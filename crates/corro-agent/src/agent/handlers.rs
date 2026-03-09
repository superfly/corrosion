//! Various handler functions
//!
//! This module is _big_ and maybe should be split up further.

use std::{
    cmp,
    collections::VecDeque,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::{
    agent::{
        bi, bootstrap, uni,
        util::{log_at_pow_10, process_multiple_changes},
        SyncClientError, ANNOUNCE_INTERVAL,
    },
    api::peer::parallel_sync,
    transport::Transport,
};
use antithesis_sdk::{assert_always, assert_sometimes};
use camino::Utf8Path;
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, Bookie, SplitPool},
    base::CrsqlSeq,
    broadcast::{BroadcastInput, BroadcastV1, ChangeSource, ChangeV1, FocaInput},
    channel::CorroReceiver,
    members::MemberAddedResult,
    sqlite::log_slow_inflight_queries,
    sync::generate_sync,
};

use bytes::Bytes;
use corro_types::broadcast::Timestamp;
use foca::OwnedNotification;
use indexmap::map::Entry;
use indexmap::IndexMap;
use metrics::{counter, gauge, histogram};
use rand::{prelude::IteratorRandom, rngs::StdRng, SeedableRng};
use rangemap::RangeInclusiveSet;
use serde_json::json;
use spawn::spawn_counted;
use tokio::time::sleep;
use tokio::{
    sync::mpsc::Receiver as TokioReceiver,
    task::{block_in_place, JoinHandle},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{debug, debug_span, error, info, trace, warn, Instrument};
use tripwire::{Outcome, PreemptibleFutureExt, TimeoutFutureExt, Tripwire};

/// Spawn a tree of tasks that handles incoming gossip server
/// connections, streams, and their respective payloads.
pub fn spawn_gossipserver_handler(
    agent: &Agent,
    bookie: &Bookie,
    tripwire: &Tripwire,
    gossip_server_endpoint: quinn::Endpoint,
) {
    spawn_counted({
        let agent = agent.clone();
        let bookie = bookie.clone();
        let mut tripwire = tripwire.clone();
        async move {
            loop {
                let incoming = match gossip_server_endpoint
                    .accept()
                    .preemptible(&mut tripwire)
                    .await
                {
                    Outcome::Completed(Some(incoming)) => incoming,
                    Outcome::Completed(None) => return,
                    Outcome::Preempted(_) => break,
                };

                // Spawn incoming connection handlers
                spawn_incoming_connection_handlers(&agent, &bookie, &tripwire, incoming);
            }

            // graceful shutdown
            let idle = gossip_server_endpoint
                .wait_idle()
                .with_timeout(Duration::from_secs(5));
            tokio::pin!(idle);

            loop {
                tokio::select! {
                    _ = &mut idle => {
                        break;
                    }
                    incoming = gossip_server_endpoint.accept() => {
                        if let Some(inc) = incoming {
                            inc.refuse();
                        }
                    }
                }
            }

            gossip_server_endpoint.close(0u32.into(), b"shutting down");
        }
    });
    info!("gossipserver_handler is done");
}

/// Spawn a task which handles all state and interactions for a given
/// incoming connection.  This function spawns many futures!
pub fn spawn_incoming_connection_handlers(
    agent: &Agent,
    bookie: &Bookie,
    tripwire: &Tripwire,
    connecting: quinn::Incoming,
) {
    let agent = agent.clone();
    let bookie = bookie.clone();
    let tripwire = tripwire.clone();
    tokio::spawn(async move {
        let remote_addr = connecting.remote_address();
        // let local_ip = connecting.local_ip().unwrap();
        trace!("got a connection from {remote_addr}");

        let conn = match connecting.await {
            Ok(conn) => conn,
            Err(e) => {
                error!("could not handshake connection from {remote_addr}: {e}");
                return;
            }
        };

        counter!("corro.peer.connection.accept.total").increment(1);

        trace!("accepted a QUIC conn from {remote_addr}");

        // Spawn handler tasks for this connection
        spawn_foca_handler(&agent, &tripwire, &conn);
        uni::spawn_unipayload_handler(
            &tripwire,
            &conn,
            agent.cluster_id(),
            agent.tx_changes().clone(),
        );
        bi::spawn_bipayload_handler(&agent, &bookie, &tripwire, &conn);
    });
}

/// Spawn a single task that accepts chunks from a receiver and
/// updates cluster member round-trip-times in the agent state.
pub fn spawn_rtt_handler(
    agent: &Agent,
    rtt_rx: TokioReceiver<(SocketAddr, Duration)>,
    tripwire: Tripwire,
) {
    spawn_counted({
        let agent = agent.clone();
        let mut tripwire = tripwire.clone();
        async move {
            let stream = ReceiverStream::new(rtt_rx);
            // we can handle a lot of them I think...
            let chunker = stream.chunks_timeout(1024, Duration::from_secs(1));
            tokio::pin!(chunker);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut tripwire => {
                        break;
                    }
                    chunks = chunker.next() => {
                        if let Some(chunks) = chunks {
                            let mut members = agent.members().write();
                            for (addr, rtt) in chunks {
                                members.add_rtt(addr, rtt);
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    });
}

/// Spawn a single task to listen for `Datagram`s from the transport
/// and apply FOCA messages to the local SWIM statemachine.
pub fn spawn_foca_handler(agent: &Agent, tripwire: &Tripwire, conn: &quinn::Connection) {
    tokio::spawn({
        let conn = conn.clone();
        let mut tripwire = tripwire.clone();
        let foca_tx = agent.tx_foca().clone();
        async move {
            loop {
                let b = tokio::select! {
                    b_res = conn.read_datagram() => match b_res {
                        Ok(b) => {
                            counter!("corro.peer.datagram.recv.total").increment(1);
                            counter!("corro.peer.datagram.bytes.recv.total").increment(b.len() as u64);
                            b
                        },
                        Err(e) => {
                            debug!("could not read datagram from connection: {e}");
                            return;
                        }
                    },
                    _ = &mut tripwire => {
                        debug!("connection cancelled");
                        return;
                    }
                };

                if let Err(e) = foca_tx.send(FocaInput::Data(b)).await {
                    error!("could not send data foca input: {e}");
                }
            }
        }
    });
}

/// Announce this node to other random nodes (according to SWIM)
///
/// We use an exponential backoff to announce aggressively in the
/// beginning, get a full picture of the cluster, then stop spamming
/// everyone.
///
///
pub fn spawn_swim_announcer(agent: &Agent, gossip_addr: SocketAddr, tripwire: Tripwire) {
    spawn_counted({
        let agent = agent.clone();
        let mut tripwire = tripwire.clone();
        async move {
            let mut boff = backoff::Backoff::new(10)
                .timeout_range(Duration::from_secs(5), Duration::from_secs(120))
                .iter();
            let timer = tokio::time::sleep(Duration::new(0, 0));
            tokio::pin!(timer);

            loop {
                tokio::select! {
                    _ = &mut tripwire => {
                        break;
                    }
                    _ = timer.as_mut() => {}
                }

                // TODO: find way to find and filter out addrs with a different membership id
                match bootstrap::generate_bootstrap(
                    agent.config().gossip.bootstrap.as_slice(),
                    gossip_addr,
                    agent.pool(),
                )
                .await
                {
                    Ok(addrs) => {
                        for addr in addrs.iter() {
                            debug!("Bootstrapping w/ {addr}");
                            if let Err(e) = agent
                                .tx_foca()
                                .send(FocaInput::Announce((*addr).into()))
                                .await
                            {
                                error!("could not send foca Announce message: {e}");
                            } else {
                                debug!("successfully sent announce message");
                            }

                            assert_sometimes!(true, "Corrosion bootstraps with other nodes")
                        }
                    }
                    Err(e) => {
                        error!("could not find nodes to announce ourselves to: {e}");
                    }
                }

                let dur = boff.next().unwrap_or(ANNOUNCE_INTERVAL);
                timer.as_mut().reset(tokio::time::Instant::now() + dur);
            }
        }
    });
}

/// A central dispatcher for SWIM cluster management messages
// TODO: we may be able to inline this code where it is needed
pub async fn handle_gossip_to_send(
    transport: Transport,
    mut swim_to_send_rx: CorroReceiver<(Actor, Bytes)>,
    mut tripwire: Tripwire,
) {
    let spawn_sender_fn = |(actor, data): (Actor, Bytes)| {
        trace!("got gossip to send to {actor:?}");

        let addr = actor.addr();
        let actor_id = actor.id();

        let transport = transport.clone();

        let len = data.len();
        spawn_counted(
            async move {
                if let Err(e) = transport.send_datagram(addr, data).await {
                    error!("could not write datagram {addr}: {e}");
                    return;
                }
                counter!("corro.peer.datagram.sent.total", "actor_id" => actor_id.to_string())
                    .increment(1);
                counter!("corro.peer.datagram.bytes.sent.total").increment(len as u64);
            }
            .instrument(debug_span!("send_swim_payload", %addr, %actor_id, buf_size = len)),
        );
    };

    while let Outcome::Completed(Some((actor, data))) =
        swim_to_send_rx.recv().preemptible(&mut tripwire).await
    {
        spawn_sender_fn((actor, data));
    }
    if tripwire.is_shutting_down() {
        // Send any remaining messages
        while let Ok((actor, data)) = swim_to_send_rx.try_recv() {
            spawn_sender_fn((actor, data));
        }
    }
}

/// Poll for updates from the cluster membership system (`foca`/ SWIM)
/// and apply any incoming changes to the local actor/ agent state.
pub async fn handle_notifications(
    agent: Agent,
    mut notification_rx: CorroReceiver<OwnedNotification<Actor>>,
    mut tripwire: Tripwire,
) {
    while let Outcome::Completed(Some(notification)) =
        notification_rx.recv().preemptible(&mut tripwire).await
    {
        trace!("handle notification");
        match notification {
            OwnedNotification::MemberUp(actor) => {
                let member_added_res = agent.members().write().add_member(&actor);
                info!("Member Up {actor:?} (result: {member_added_res:?})");

                match member_added_res {
                    MemberAddedResult::NewMember | MemberAddedResult::Removed => {
                        if matches!(member_added_res, MemberAddedResult::Removed) {
                            debug!("Member Removed {actor:?} due to member id mismatch");
                            counter!("corro.gossip.member.removed", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string()).increment(1);
                        } else {
                            debug!("Member Added {actor:?}");
                            counter!("corro.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string()).increment(1);
                        }

                        let members_len = { agent.members().read().states.len() as u32 };

                        // actually added a member
                        // notify of new cluster size
                        if let Ok(size) = members_len.try_into() {
                            if let Err(e) = agent.tx_foca().send(FocaInput::ClusterSize(size)).await
                            {
                                error!("could not send new foca cluster size: {e}");
                            }
                        }
                    }
                    MemberAddedResult::Updated => {
                        debug!("Member Updated {actor:?}");
                        // anything else to do here?
                    }
                    MemberAddedResult::Ignored => {
                        // TODO: it's unclear if this is needed or
                        // not.  We removed it to debug a foca member
                        // state issue.  It may be needed again.

                        // if let Err(e) = agent
                        //     .tx_foca()
                        //     .send(FocaInput::ApplyMany(vec![foca::Member::new(
                        //         actor.clone(),
                        //         foca::Incarnation::default(),
                        //         foca::State::Down,
                        //     )]))
                        //     .await
                        // {
                        //     warn!(?actor, "could not manually declare actor as down! {e}");
                        // }
                    }
                }
                counter!("corro.swim.notification", "type" => "memberup").increment(1);
            }
            OwnedNotification::MemberDown(actor) => {
                let removed = { agent.members().write().remove_member(&actor) };
                info!("Member Down {actor:?} (removed: {removed})");
                if removed {
                    debug!("Member Down {actor:?}");
                    counter!("corro.gossip.member.removed", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string()).increment(1);
                    // actually removed a member
                    // notify of new cluster size
                    let member_len = { agent.members().read().states.len() as u32 };
                    if let Ok(size) = member_len.try_into() {
                        if let Err(e) = agent.tx_foca().send(FocaInput::ClusterSize(size)).await {
                            error!("could not send new foca cluster size: {e}");
                        }
                    }
                }
                counter!("corro.swim.notification", "type" => "memberdown").increment(1);
            }
            OwnedNotification::Rename(a, b) => {
                let mut lock = agent.members().write();
                let del_res = lock.remove_member(&a);
                let add_res = lock.add_member(&b);
                info!("Member Rename {a:?} to {b:?} (del_res: {del_res:?}, add_res: {add_res:?})");
            }
            OwnedNotification::Active => {
                info!("Current node is considered ACTIVE");
                counter!("corro.swim.notification", "type" => "active").increment(1);
            }
            OwnedNotification::Idle => {
                warn!("Current node is considered IDLE");
                counter!("corro.swim.notification", "type" => "idle").increment(1);
            }
            // this happens when we leave the cluster
            OwnedNotification::Defunct => {
                debug!("Current node is considered DEFUNCT");
                counter!("corro.swim.notification", "type" => "defunct").increment(1);
            }
            OwnedNotification::Rejoin(id) => {
                info!("Rejoined the cluster with id: {id:?}");
                counter!("corro.swim.notification", "type" => "rejoin").increment(1);
            }
        }
    }
}

/// We keep a write-ahead-log, which under write-pressure can grow to
/// multiple gigabytes and needs periodic truncation.  We don't want
/// to schedule this task too often since it locks the whole DB.
// TODO: can we get around the lock somehow?
fn wal_checkpoint(conn: &rusqlite::Connection, timeout: u64) -> eyre::Result<()> {
    debug!("handling db_cleanup (WAL truncation)");
    let start = Instant::now();

    assert_sometimes!(true, "Corrosion truncates WAL");
    let orig: u64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
    conn.pragma_update(None, "busy_timeout", timeout)?;

    let busy: bool = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
    if busy {
        warn!("could not truncate sqlite WAL, database busy - with timeout: {timeout}");
        log_slow_inflight_queries();
        counter!("corro.db.wal.truncate.busy").increment(1);
    } else {
        debug!("successfully truncated sqlite WAL!");
        histogram!("corro.db.wal.truncate.seconds").record(start.elapsed().as_secs_f64());
    }

    _ = conn.pragma_update(None, "busy_timeout", orig);

    Ok::<_, eyre::Report>(())
}

/// If the number of unused free pages is above the provided limit,
/// This function continously runs an incremental_vacuum
/// until it is below the limit
///
async fn vacuum_db(pool: &SplitPool, lim: u64) -> eyre::Result<()> {
    let mut freelist: u64 = {
        let conn = pool.read().await?;

        let vacuum: u64 = conn.pragma_query_value(None, "auto_vacuum", |row| row.get(0))?;
        if vacuum != 2 {
            warn!("auto_vacuum isn't set to INCREMENTAL");
            return Err(rusqlite::Error::ModuleError(
                "auto_vacuum has to be set to INCREMENTAL".to_string(),
            )
            .into());
        }

        conn.pragma_query_value(None, "freelist_count", |row| row.get(0))?
    };

    debug!("freelist count: {freelist:?}");

    let (busy_timeout, cache_size) = {
        // update settings in write conn
        let conn = pool.write_low().await?;
        let orig: u64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
        let cache_size: i64 = conn.pragma_query_value(None, "cache_size", |row| row.get(0))?;
        conn.pragma_update(None, "cache_size", 100000)?;
        (orig, cache_size)
    };

    while freelist >= lim {
        let conn = pool.write_low().await?;

        block_in_place(|| {
            let start = Instant::now();

            let mut prepped = conn.prepare("pragma incremental_vacuum(1000)")?;
            let mut rows = prepped.query([])?;

            while let Ok(Some(_)) = rows.next() {}
            histogram!("corro.db.incremental.vacuum.seconds").record(start.elapsed().as_secs_f64());

            freelist = conn.pragma_query_value(None, "freelist_count", |row| row.get(0))?;
            debug!("freelist count after incremental vacuum: {freelist:?}");

            Ok::<(), eyre::Error>(())
        })?;

        drop(conn);
        sleep(Duration::from_secs(1)).await;
    }

    let conn = pool.write_low().await?;
    conn.pragma_update(None, "busy_timeout", busy_timeout)?;
    conn.pragma_update(None, "cache_size", cache_size)?;

    Ok::<_, eyre::Report>(())
}

/// See `db_cleanup` and `vacuum_db`
pub fn spawn_handle_db_maintenance(agent: &Agent) {
    let mut wal_path = agent.config().db.path.clone();
    let wal_threshold = agent.config().perf.wal_threshold_mb as u64;
    wal_path.set_extension(format!("{}-wal", wal_path.extension().unwrap_or_default()));

    let pool = agent.pool().clone();

    let on_antithesis = std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok();

    // reduce interval if we are running in antithesis
    let interval_secs = if on_antithesis { 30 } else { 5 * 60 };

    tokio::spawn(async move {
        let truncate_wal_threshold: u64 = wal_threshold * 1024 * 1024;

        // try to initially truncate the WAL
        match wal_checkpoint_over_threshold(wal_path.as_path(), &pool, truncate_wal_threshold).await
        {
            Ok(truncated) if truncated => {
                info!("initially truncated WAL");
            }
            Err(e) => {
                error!("could not initially truncate WAL: {e}");
            }
            _ => {}
        }

        // large sleep right at the start to give node time to sync

        if !on_antithesis {
            sleep(Duration::from_secs(60)).await;
        }

        let mut vacuum_interval = tokio::time::interval(Duration::from_secs(interval_secs));

        const MAX_DB_FREE_PAGES: u64 = 10000;

        loop {
            vacuum_interval.tick().await;
            if let Err(e) = vacuum_db(&pool, MAX_DB_FREE_PAGES).await {
                error!("could not check freelist and vacuum: {e}");
            }

            match wal_checkpoint_over_threshold(wal_path.as_path(), &pool, truncate_wal_threshold)
                .await
            {
                Err(e) => {
                    error!("could not wal_checkpoint truncate: {e}");
                }
                Ok(truncated) => {
                    info!("truncated WAL: {truncated}");
                }
                _ => {}
            }
        }
    });
}

async fn wal_checkpoint_over_threshold(
    wal_path: &Utf8Path,
    pool: &SplitPool,
    threshold: u64,
) -> eyre::Result<bool> {
    let wal_size = wal_path.metadata()?.len();
    let should_truncate = wal_size > threshold;

    let details = json!({
        "wal_size": wal_size,
        "threshold": threshold,
        "should_truncate": should_truncate,
    });
    assert_sometimes!(true, "Corrosion attempts to truncate WAL", &details);
    if should_truncate {
        let conn = if wal_size > (5 * threshold) {
            warn!("wal_size is over 5x the threshold, trying to get a priority conn");
            pool.write_priority().await?
        } else {
            pool.write_low().await?
        };

        let timeout = calc_busy_timeout(wal_path.metadata()?.len(), threshold);
        block_in_place(|| wal_checkpoint(&conn, timeout))?;
    }
    Ok(should_truncate)
}

fn calc_busy_timeout(wal_size: u64, threshold: u64) -> u64 {
    let wal_size_gb = wal_size / (1024 * 1024 * 1024);
    let threshold_gb = threshold / (1024 * 1024 * 1024);
    let base_timeout = 30000;
    if wal_size_gb <= threshold_gb {
        return base_timeout;
    }

    // Double the timeout every 5gb and cap at 16 minutes
    let diff = cmp::min(5, (wal_size_gb - threshold_gb) / 5);
    // add extra (five * diff) seconds for every extra 1gb over 4gb
    let linear_increase = (wal_size_gb % 5) * 5000 * (diff + 1);
    let timeout = base_timeout * 2_u64.pow(diff as u32) + linear_increase;
    // we are using a 16min timeout, something is wrong if we get here
    if diff >= 5 {
        warn!("WAL size is too large, setting busy timeout {timeout}ms");
    }
    timeout
}

/// State for dynamic batch processing
struct HandleChangesState {
    queue: VecDeque<(ChangeV1, ChangeSource, Instant)>,
    buf_cost: usize,
    current_batch_size: usize,
    processing_task: Option<JoinHandle<Result<(), corro_types::agent::ChangeError>>>,
    max_wait: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    drop_log_count: u64,

    // Configuration
    max_queue_len: usize,
    min_batch_size: usize,
    step_base: usize,
    max_batch_size: usize,
    batch_threshold_ratio: f64,
    timeout_duration: Duration,
    tx_timeout: Duration,
}

impl HandleChangesState {
    fn new(
        min_batch_size: usize,
        step_base: usize,
        max_batch_size: usize,
        batch_threshold_ratio: f64,
        timeout_duration: Duration,
        tx_timeout: Duration,
        max_queue_len: usize,
    ) -> Self {
        Self {
            queue: VecDeque::with_capacity(max_queue_len),
            buf_cost: 0,
            current_batch_size: min_batch_size,
            processing_task: None,
            max_wait: Some(Box::pin(tokio::time::sleep(timeout_duration))),
            min_batch_size,
            step_base,
            max_batch_size,
            batch_threshold_ratio,
            timeout_duration,
            tx_timeout,
            max_queue_len,
            drop_log_count: 0,
        }
    }

    /// Calculate exponential batch size based on cost
    fn calculate_batch_size(&self, cost: usize) -> usize {
        if self.step_base == 0 || cost < self.step_base {
            self.min_batch_size
        } else {
            let size = self.step_base * (1 << (cost / self.step_base).ilog2());
            size.clamp(self.min_batch_size, self.max_batch_size)
        }
    }

    /// Get the threshold for immediate spawning based on configured ratio
    fn batch_threshold(&self) -> usize {
        (self.current_batch_size as f64 * self.batch_threshold_ratio) as usize
    }

    /// Drain a batch from the queue and spawn processing task
    fn drain_and_spawn(
        &mut self,
        agent: &Agent,
        bookie: &Bookie,
        target_size: usize,
        reason: &str,
    ) -> Option<usize> {
        // Must complete previous task before spawning a new one
        if self.processing_task.is_some() {
            return None;
        }

        let mut batch = Vec::with_capacity(self.current_batch_size);
        let mut batch_cost = 0;

        while let Some((change, src, queued_at)) = self.queue.pop_front() {
            let cost = change.processing_cost();
            batch_cost += cost;
            self.buf_cost -= cost;
            batch.push((change, src, queued_at));
            if batch_cost >= target_size {
                break;
            }
        }

        if batch.is_empty() {
            return None;
        }

        debug!(count = %batch_cost, reason = %reason, batch_size = %self.current_batch_size, "spawning batch processing task");

        let agent_clone = agent.clone();
        let bookie_clone = bookie.clone();
        self.processing_task = Some(tokio::spawn(process_multiple_changes(
            agent_clone,
            bookie_clone,
            batch,
            self.tx_timeout,
        )));
        counter!("corro.agent.changes.batch.spawned").increment(1);
        gauge!("corro.agent.changes.batch_size").set(self.current_batch_size as f64);

        Some(batch_cost)
    }

    /// Handle task completion and decide whether to spawn next batch
    fn handle_task_completion(
        &mut self,
        agent: &Agent,
        bookie: &Bookie,
        result: Result<Result<(), corro_types::agent::ChangeError>, tokio::task::JoinError>,
    ) {
        // Handle task result
        match result {
            Ok(Ok(())) => {
                debug!("batch processing completed successfully");
            }
            Ok(Err(e)) => {
                let err_str = e.to_string();
                error!("error processing batch: {e}");

                // Check for memory errors and emergency reduce batch size
                // TODO: requeue the changes
                if err_str.contains("SQLITE_NOMEM") || err_str.contains("out of memory") {
                    error!("memory error detected, halving batch size");
                    self.current_batch_size =
                        (self.current_batch_size / 2).max(self.min_batch_size);
                }
            }
            Err(e) => {
                error!("batch processing task panicked: {e}");
            }
        }

        self.processing_task = None;

        // Should we spawn immediately with queued changes?
        if self.buf_cost >= self.batch_threshold() {
            // YES - enough changes queued, spawn immediately
            // Check if we might want to burst
            if self.buf_cost > self.current_batch_size {
                self.current_batch_size = self.calculate_batch_size(self.buf_cost);
            }
            if self
                .drain_and_spawn(
                    agent,
                    bookie,
                    self.current_batch_size,
                    "immediate-after-completion",
                )
                .is_none()
            {
                unreachable!("Must spawn a batch");
            }
        } else {
            // NO - not enough changes yet, start timeout
            self.max_wait = Some(Box::pin(tokio::time::sleep(self.timeout_duration)));
        }
    }

    /// Handle timeout - spawn whatever we have
    /// The amount of changes MUST be smaller than the threshold
    /// as otherwise other code paths would have spawned a batch
    fn handle_timeout(&mut self, agent: &Agent, bookie: &Bookie) {
        self.max_wait = None;

        if !self.queue.is_empty() {
            let actual_cost = self.buf_cost;
            let threshold = self.batch_threshold();
            // Other code paths should have spawned a batch by now!
            assert_always!(actual_cost < threshold, "Timeout with too many changes");
            // Just process whatever we have and then downsize the batch size
            if self
                .drain_and_spawn(agent, bookie, usize::MAX, "timeout")
                .is_none()
            {
                unreachable!("Must spawn a batch");
            }

            assert_sometimes!(true, "Corrosion processes changes");
            // Adjust batch size based on what we had processed
            // This will decrease the batch size unless it's already at the minimum
            self.current_batch_size = self.calculate_batch_size(actual_cost);
        } else if self.processing_task.is_none() && self.max_wait.is_none() {
            // Empty queue, no task running, no timeout set
            self.current_batch_size = self.calculate_batch_size(0);
            // If no task is running and no timeout is set, start timeout
            // This ensures changes get processed even if they don't hit the threshold
            self.max_wait = Some(Box::pin(tokio::time::sleep(self.timeout_duration)));
        }
    }

    // Drops oldest items when the queue is full
    fn maybe_drop_old_change(&mut self) -> Option<ChangeV1> {
        let mut dropped_count = 0;

        let maybe_dropped_change = if self.queue.len() >= self.max_queue_len {
            if let Some((dropped_change, _, _)) = self.queue.pop_front() {
                self.buf_cost -= dropped_change.processing_cost();
                dropped_count += 1;

                Some(dropped_change)
            } else {
                None
            }
        } else {
            None
        };

        counter!("corro.agent.changes.dropped").increment(dropped_count);
        log_at_pow_10("dropped old change from queue", &mut self.drop_log_count);
        maybe_dropped_change
    }

    /// Handle new change arrival - check if we should spawn immediately
    fn handle_new_change(
        &mut self,
        agent: &Agent,
        bookie: &Bookie,
        change: ChangeV1,
        src: ChangeSource,
    ) {
        let cost = change.processing_cost();
        self.queue.push_back((change, src, Instant::now()));
        self.buf_cost += cost;

        // Check if we should spawn immediately (threshold-based)
        if self.buf_cost >= self.batch_threshold() && self.processing_task.is_none() {
            // Cancel any pending timeout
            self.max_wait = None;

            // Check if we might want to burst
            if self.buf_cost > self.current_batch_size {
                self.current_batch_size = self.calculate_batch_size(self.buf_cost);
            }

            // Drain and spawn
            if self
                .drain_and_spawn(
                    agent,
                    bookie,
                    self.current_batch_size,
                    "size-threshold-on-new-change",
                )
                .is_none()
            {
                unreachable!("Must spawn a batch");
            }
        }
    }
}

/// Bundle incoming changes to optimise transaction sizes with SQLite
///
/// *Performance tradeoff*: introduce latency (with a max timeout) to
/// apply changes more efficiently.
///
/// This function used by broadcast receivers and sync receivers
pub async fn handle_changes(
    agent: Agent,
    bookie: Bookie,
    mut rx_changes: CorroReceiver<(ChangeV1, ChangeSource)>,
    mut tripwire: Tripwire,
) {
    let max_queue_len: usize = agent.config().perf.processing_queue_len;

    // Initialize batch processor state
    let mut state = HandleChangesState::new(
        agent.config().perf.apply_queue_min_batch_size,
        agent.config().perf.apply_queue_step_base,
        agent.config().perf.apply_queue_max_batch_size,
        agent.config().perf.apply_queue_batch_threshold_ratio,
        Duration::from_millis(agent.config().perf.apply_queue_timeout as u64),
        Duration::from_secs(agent.config().perf.sql_tx_timeout as u64),
        max_queue_len,
    );

    let max_seen_cache_len: usize = max_queue_len;
    let metrics_tracker = agent.metrics_tracker();

    // unlikely, but max_seen_cache_len can be less than 10, in that case we want to just clear the whole cache
    // (todo): put some validation in config instead
    let keep_seen_cache_size: usize = if max_seen_cache_len > 10 {
        cmp::max(10, max_seen_cache_len / 10)
    } else {
        0
    };
    let mut seen: IndexMap<_, RangeInclusiveSet<CrsqlSeq>> = IndexMap::new();

    loop {
        let (change, src) = tokio::select! {
            biased;

            // Processing task finished
            res = async { state.processing_task.as_mut().unwrap().await }, if state.processing_task.is_some() => {
                state.handle_task_completion(&agent, &bookie, res);
                continue;
            },

            // New changes arrive
            maybe_change_src = rx_changes.recv() => match maybe_change_src {
                Some((change, src)) => (change, src),
                None => break,
            },

            // Timeout fires
            _ = async { state.max_wait.as_mut().unwrap().await }, if state.max_wait.is_some() => {
                // Emit metrics
                gauge!("corro.agent.changes.in_queue").set(state.buf_cost as f64);
                gauge!("corro.agent.changesets.in_queue").set(state.queue.len() as f64);
                gauge!("corro.agent.changes.processing.jobs").set(if state.processing_task.is_some() { 1.0 } else { 0.0 });
                metrics_tracker.observe_queue_size(state.queue.len() as u64);

                state.handle_timeout(&agent, &bookie);

                // Cleanup seen cache
                if seen.len() > max_seen_cache_len {
                    seen.drain(..seen.len() - keep_seen_cache_size);
                }

                continue;
            },

            // Corrosion is shutting down
            _ = &mut tripwire => {
                break;
            }
        };

        let change_len = change.len();
        counter!("corro.agent.changes.recv").increment(std::cmp::max(change_len, 1) as u64); // count empties...

        // Skip changes from ourselves
        if change.actor_id == agent.actor_id() {
            continue;
        }

        // Skip changes we've already seen recently in the seen cache
        if let Some(mut seqs) = change.seqs() {
            let v = change.versions().start();
            if let Some(seen_seqs) = seen.get(&(change.actor_id, v)) {
                if seqs.all(|seq| seen_seqs.contains(&seq)) {
                    continue;
                }
            }
        } else if change
            .versions()
            .all(|v| seen.contains_key(&(change.actor_id, v)))
        {
            if matches!(src, ChangeSource::Broadcast) {
                counter!("corro.broadcast.duplicate.count", "from" => "cache").increment(1);
            }
            continue;
        }

        // Update logical clock if needed
        let src_str: &'static str = src.into();
        let recv_lag = change.ts().and_then(|ts| {
            let mut our_ts = Timestamp::from(agent.clock().new_timestamp());
            if ts > our_ts {
                if let Err(e) = agent.update_clock_with_timestamp(change.actor_id, ts) {
                    error!("could not update clock from actor {}: {e}", change.actor_id);
                    return None;
                }
                counter!("corro.agent.clock.update", "source" => src_str).increment(1);
                // update our_ts to the new timestamp
                our_ts = Timestamp::from(agent.clock().new_timestamp());
            }
            Some((our_ts.0 - ts.0).to_duration())
        });

        if matches!(src, ChangeSource::Broadcast) {
            counter!("corro.broadcast.recv.count", "kind" => "change").increment(1);
        }

        // Skip changes we've already seen in the bookie
        let booked = bookie.get(&change.actor_id);
        if let Some(booked) = booked {
            if booked.read().contains_all(change.versions(), change.seqs()) {
                trace!("already seen, stop disseminating");
                if matches!(src, ChangeSource::Broadcast) {
                    counter!("corro.broadcast.duplicate.count", "from" => "bookie").increment(1);
                }
                continue;
            }
        }

        if let Some(recv_lag) = recv_lag {
            histogram!("corro.agent.changes.recv.lag.seconds", "source" => src_str)
                .record(recv_lag.as_secs_f64());
        }

        // If we need to drop an old change, drop it from the seen cache aswell
        while let Some(dropped_change) = state.maybe_drop_old_change() {
            for v in dropped_change.versions() {
                if let Entry::Occupied(mut entry) = seen.entry((change.actor_id, v)) {
                    if let Some(seqs) = dropped_change.seqs() {
                        entry.get_mut().remove(seqs.into());
                    } else {
                        entry.swap_remove_entry();
                    }
                };
            }
        }

        // Register the new change in the seen cache
        // this will only run once for a non-empty changeset
        for v in change.versions() {
            let entry = seen.entry((change.actor_id, v)).or_default();
            if let Some(seqs) = change.seqs() {
                entry.extend([seqs.into()]);
            }
        }

        assert_sometimes!(
            matches!(src, ChangeSource::Sync),
            "Corrosion receives changes through sync"
        );
        // Rebroadcast changes received from broadcast
        if matches!(src, ChangeSource::Broadcast) && !change.is_empty() {
            assert_sometimes!(true, "Corrosion rebroadcasts changes");
            if let Err(_e) =
                agent
                    .tx_bcast()
                    .try_send(BroadcastInput::Rebroadcast(BroadcastV1::Change(
                        change.clone(),
                    )))
            {
                debug!("broadcasts are full or done!");
            }
        }

        // Handle the new change - queue it and potentially spawn a batch
        state.handle_new_change(&agent, &bookie, change, src);
    }
}

/// Start a new sync with multiple other nodes
///
/// Choose members to sync with based on the current RTT and how many
/// (known) versions we need from that peer.  Add randomness to taste.
#[tracing::instrument(skip_all, err, level = "debug")]
pub async fn handle_sync(
    agent: &Agent,
    bookie: &Bookie,
    transport: &Transport,
) -> Result<(), SyncClientError> {
    let sync_state = generate_sync(bookie, agent.actor_id()).await;

    for (actor_id, needed) in sync_state.need.iter() {
        gauge!("corro.sync.client.needed", "actor_id" => actor_id.to_string())
            .set(needed.len() as f64);
    }
    for (actor_id, version) in sync_state.heads.iter() {
        gauge!("corro.sync.client.head", "actor_id" => actor_id.to_string()).set(version.0 as f64);
    }

    let chosen: Vec<(ActorId, SocketAddr)> = {
        let candidates = {
            let members = agent.members().read();

            members
                .states
                .iter()
                // Filter out self
                .filter(|(id, state)| {
                    **id != agent.actor_id()
                        && state.cluster_id == agent.cluster_id()
                        && state.member_id == agent.member_id()
                })
                // Grab a ring-buffer index to the member RTT range
                .map(|(id, state)| {
                    (
                        *id,
                        state.ring.unwrap_or(255),
                        state.addr,
                        state.last_sync_ts,
                    )
                })
                .collect::<Vec<(ActorId, u8, SocketAddr, Option<Timestamp>)>>()
        };

        if candidates.is_empty() {
            return Ok(());
        }

        debug!("found {} candidates to synchronize with", candidates.len());

        assert_sometimes!(true, "Corrosion syncs with other nodes");
        let desired_count = (candidates.len() / 100).clamp(3, 10);
        debug!("Selected {desired_count} nodes to sync with");

        let mut rng = StdRng::from_os_rng();

        let mut choices = candidates
            .into_iter()
            .choose_multiple(&mut rng, desired_count * 2);

        choices.sort_by(|a, b| {
            // most missing actors first
            sync_state
                .need_len_for_actor(&b.0)
                .cmp(&sync_state.need_len_for_actor(&a.0))
                // if equal, look at last sync time
                .then_with(|| a.3.cmp(&b.3))
                // if equal, look at proximity (via `ring`)
                .then_with(|| a.1.cmp(&b.1))
        });

        choices.truncate(desired_count);
        choices
            .into_iter()
            .map(|(actor_id, _, addr, _)| (actor_id, addr))
            .collect()
    };

    trace!("Sync set: {chosen:?}");
    if chosen.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let n = match parallel_sync(agent, transport, chosen.clone(), sync_state).await {
        Ok(n) => n,
        Err(e) => {
            error!("failed to execute parallel sync: {e:?}");
            return Err(e.into());
        }
    };

    let elapsed = start.elapsed();
    if n > 0 {
        info!(
            "synced {n} changes w/ {} in {}s @ {} changes/s",
            chosen
                .clone()
                .into_iter()
                .map(|(actor_id, _)| actor_id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            elapsed.as_secs_f64(),
            n as f64 / elapsed.as_secs_f64()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::agent::setup;
    use crate::api::public::api_v1_db_schema;

    use super::*;
    use axum::{http::StatusCode, Extension, Json};
    use corro_tests::TEST_SCHEMA;
    use corro_types::api::{ColumnName, TableName};
    use corro_types::{
        base::{dbsr, dbvr, CrsqlDbVersion},
        broadcast::Changeset,
        change::Change,
        config::Config,
        pubsub::pack_columns,
    };
    use rusqlite::Connection;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    use tokio::time::{timeout, Duration};

    #[test]
    fn ensure_truncate_works() -> eyre::Result<()> {
        let tmpdir = tempfile::tempdir()?;

        let conn = rusqlite::Connection::open(tmpdir.path().join("db.sqlite"))?;
        let pragma_value = 12345u64;
        conn.pragma_update(None, "busy_timeout", pragma_value)?;

        wal_checkpoint(&conn, 60000)?;
        assert_eq!(
            conn.pragma_query_value(None, "busy_timeout", |row| row.get::<_, u64>(0))?,
            pragma_value
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_loadshed_handle_changes() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();
        let dir = tempfile::tempdir()?;

        let mut config = Config::builder()
            .db_path(dir.path().join("corrosion.db").display().to_string())
            .gossip_addr("127.0.0.1:0".parse()?)
            .api_addr("127.0.0.1:0".parse()?)
            .build()?;
        config.perf.processing_queue_len = 3;
        config.perf.apply_queue_min_batch_size = 1;
        config.perf.apply_queue_max_batch_size = 1;
        config.perf.apply_queue_step_base = 0;
        config.perf.changes_channel_len = 1;

        let (agent, agent_options) = setup(config, tripwire.clone()).await?;

        let (status_code, _res) =
            api_v1_db_schema(Extension(agent.clone()), Json(vec![TEST_SCHEMA.to_owned()])).await;
        assert_eq!(status_code, StatusCode::OK);

        let other_actor = ActorId(uuid::Uuid::new_v4());
        let bookie = Bookie::new(Default::default());
        tokio::spawn(handle_changes(
            agent.clone(),
            bookie.clone(),
            agent_options.rx_changes,
            tripwire,
        ));

        {
            // hold write connection so that max_concurrency is reached
            let _conn = agent.pool().write_normal().await?;

            // queue size is very small - only three changes
            // 10-6 are stuck proecessing because we hold the write conn
            // next two versions, 3-5, enter the queue
            // last version 2-1, displace 4 and 5 from the queue and
            // they never get processed
            for i in (1i64..=10i64).rev() {
                let crsql_row = Change {
                    table: TableName("tests".into()),
                    pk: pack_columns(&vec![i.into()])?,
                    cid: ColumnName("text".into()),
                    val: "two override".into(),
                    col_version: 1,
                    db_version: CrsqlDbVersion(i as u64),
                    seq: CrsqlSeq(0),
                    site_id: other_actor.to_bytes(),
                    cl: 1,
                };

                let change = (
                    ChangeV1 {
                        actor_id: other_actor,
                        changeset: Changeset::Full {
                            version: CrsqlDbVersion(i as u64),
                            changes: vec![crsql_row.clone()],
                            seqs: dbsr!(0, 0),
                            last_seq: CrsqlSeq(0),
                            ts: agent.clock().new_timestamp().into(),
                        },
                    },
                    ChangeSource::Sync,
                );

                agent.tx_changes().send(change).await?;
            }
        }

        sleep(Duration::from_secs(2)).await;

        let booked = bookie.get(&other_actor).unwrap().read();

        // With batch_size=1 and queue_len=3, the behavior is:
        // - Version 10 processes immediately (first change, hits threshold)
        // - While 10 is blocked on write lock, versions 9-1 arrive
        // - Queue fills (max 3): holds 9,8,7 then 8,7,6 then 7,6,5 then 6,5,4 then 5,4,3 then 4,3,2 then 3,2,1
        // - After 10 completes, versions 3,2,1 process (what's left in queue)
        // - Versions 9,8,7,6,5,4 were dropped (load shedding)
        assert!(booked.contains_version(&CrsqlDbVersion(10)));
        assert!(booked.contains_all(dbvr!(1, 3), None));
        assert!(!booked.contains_version(&CrsqlDbVersion(4)));
        assert!(!booked.contains_version(&CrsqlDbVersion(5)));
        assert!(!booked.contains_version(&CrsqlDbVersion(6)));
        assert!(!booked.contains_version(&CrsqlDbVersion(7)));
        assert!(!booked.contains_version(&CrsqlDbVersion(8)));
        assert!(!booked.contains_version(&CrsqlDbVersion(9)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn ensure_vacuum_works() -> eyre::Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.keep().join("db.sqlite");

        {
            let db_conn = Connection::open(db_path.clone())?;
            db_conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")?;
        }

        println!("temp db: {db_path:?}");
        let write_sema = Arc::new(Semaphore::new(1));
        let pool = SplitPool::create(db_path, write_sema.clone(), -1048576).await?;

        {
            let mut conn = pool.write_priority().await?;
            conn.execute(
                r#"
            CREATE TABLE test (
                id BIGINT NOT NULL PRIMARY KEY,
                col1 TEXT,
                col2 TEXT,
                col3 TEXT,
                col4 TEXT
            )
        "#,
                [],
            )?;

            let tx = conn.transaction()?;
            // create 1m rows
            for i in 1..100000 {
                tx.execute(
                    r"INSERT INTO test VALUES (?, ?, ?, ?, ?)",
                    (i, "colunm 1", "column 2", "column 3", "column 4"),
                )?;
            }
            tx.commit()?;

            conn.execute("DELETE FROM test", [])?;
            let freelist: u64 =
                conn.pragma_query_value(None, "freelist_count", |row| row.get(0))?;
            assert!(freelist > 1000);
        }

        timeout(Duration::from_secs(2), vacuum_db(&pool, 1000)).await??;

        let conn = pool.read().await?;
        assert!(
            conn.pragma_query_value(None, "freelist_count", |row| row.get::<_, u64>(0))? < 1000
        );

        Ok(())
    }
    #[test]
    fn check_busy_timeout() {
        // Base timeout (30s) applies up to threshold
        assert_eq!(calc_busy_timeout(to_bytes(1), to_bytes(5)), 30000); // 30s
        assert_eq!(calc_busy_timeout(to_bytes(4), to_bytes(5)), 30000); // 30s
        assert_eq!(calc_busy_timeout(to_bytes(5), to_bytes(5)), 30000); // 30s
        assert_eq!(calc_busy_timeout(to_bytes(9), to_bytes(5)), 50000); // 50s

        // At 10GB we hit first doubling + linear increases (10s per GB)
        assert_eq!(calc_busy_timeout(to_bytes(10), to_bytes(5)), 60000); // 1m
        assert_eq!(calc_busy_timeout(to_bytes(11), to_bytes(5)), 70000); // 1m10s

        // At 15GB we hit second doubling + linear increases (15s per GB)
        assert_eq!(calc_busy_timeout(to_bytes(15), to_bytes(5)), 120000); // 2m
        assert_eq!(calc_busy_timeout(to_bytes(17), to_bytes(5)), 150000); // 2m30s
        assert_eq!(calc_busy_timeout(to_bytes(19), to_bytes(5)), 180000); // 3m

        // At 20GB we hit third doubling + linear increases (20s per GB)
        assert_eq!(calc_busy_timeout(to_bytes(20), to_bytes(5)), 240000); // 4m
        assert_eq!(calc_busy_timeout(to_bytes(21), to_bytes(5)), 260000); // 4m20s

        // At 25GB we hit third doubling + linear increases (25s per GB)
        assert_eq!(calc_busy_timeout(to_bytes(25), to_bytes(5)), 480000); // 8m
        assert_eq!(calc_busy_timeout(to_bytes(27), to_bytes(5)), 530000); // 8m50s

        // At 30GB we hit third doubling + linear increases (30s per GB)
        assert_eq!(calc_busy_timeout(to_bytes(30), to_bytes(5)), 960000); // 16m
        assert_eq!(calc_busy_timeout(to_bytes(31), to_bytes(5)), 990000); // 16m30s

        // At 40GB we hit the cap + linear increases (40s per GB)
        assert_eq!(calc_busy_timeout(to_bytes(40), to_bytes(5)), 960000); // 16m
        assert_eq!(calc_busy_timeout(to_bytes(41), to_bytes(5)), 990000); // 16m30s

        // At 50GB we hit fifth doubling and cap at 16m
        assert_eq!(calc_busy_timeout(to_bytes(50), to_bytes(5)), 960000); // 16m
        assert_eq!(calc_busy_timeout(to_bytes(51), to_bytes(5)), 990000); // 16m25s (capped)
        assert_eq!(calc_busy_timeout(to_bytes(100), to_bytes(5)), 960000); // 16m (capped)
    }

    fn to_bytes(gb: u64) -> u64 {
        gb * 1024 * 1024 * 1024
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_handle_changes_batch_size_bursting() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();
        let dir = tempfile::tempdir()?;

        let mut config = Config::builder()
            .db_path(dir.path().join("corrosion.db").display().to_string())
            .gossip_addr("127.0.0.1:0".parse()?)
            .api_addr("127.0.0.1:0".parse()?)
            .build()?;

        // Configure for bursting test:
        // - min_batch_size: 100
        // - step_base: 500
        // - max_batch_size: 16000
        // - threshold_ratio: 0.9
        config.perf.apply_queue_min_batch_size = 100;
        config.perf.apply_queue_step_base = 500;
        config.perf.apply_queue_max_batch_size = 16000;
        config.perf.apply_queue_batch_threshold_ratio = 0.9;
        config.perf.apply_queue_timeout = 10; // 10ms timeout
        config.perf.processing_queue_len = 50000; // Large queue to avoid dropping old changes

        let (agent, _agent_options) = setup(config.clone(), tripwire.clone()).await?;

        let (status_code, _res) =
            api_v1_db_schema(Extension(agent.clone()), Json(vec![TEST_SCHEMA.to_owned()])).await;
        assert_eq!(status_code, StatusCode::OK);

        let other_actor = ActorId(uuid::Uuid::new_v4());
        let bookie = Bookie::new(Default::default());

        // Create a HandleChangesState instance directly
        let mut state = HandleChangesState::new(
            config.perf.apply_queue_min_batch_size,
            config.perf.apply_queue_step_base,
            config.perf.apply_queue_max_batch_size,
            config.perf.apply_queue_batch_threshold_ratio,
            Duration::from_millis(config.perf.apply_queue_timeout as u64),
            Duration::from_secs(config.perf.sql_tx_timeout as u64),
            config.perf.processing_queue_len,
        );

        // Helper to create a mock change
        let create_change = |version: u64| -> ChangeV1 {
            let crsql_row = Change {
                table: TableName("tests".into()),
                pk: pack_columns(&vec![(version as i64).into()]).unwrap(),
                cid: ColumnName("text".into()),
                val: "test value".into(),
                col_version: 1,
                db_version: CrsqlDbVersion(version),
                seq: CrsqlSeq(0),
                site_id: other_actor.to_bytes(),
                cl: 1,
            };

            let change = ChangeV1 {
                actor_id: other_actor,
                changeset: Changeset::Full {
                    version: CrsqlDbVersion(version),
                    changes: vec![crsql_row],
                    seqs: dbsr!(0, 0),
                    last_seq: CrsqlSeq(0),
                    ts: agent.clock().new_timestamp().into(),
                },
            };

            assert!(change.processing_cost() == 1);

            change
        };

        let mut version = 1;
        let mut simulate_burst = |state: &mut HandleChangesState, count: u64| {
            for _ in 1..=count {
                state.handle_new_change(
                    &agent,
                    &bookie,
                    create_change(version),
                    ChangeSource::Sync,
                );
                version += 1;
            }
        };

        // Initial batch size should be min_batch_size
        // Timer should be spawned
        assert_eq!(state.current_batch_size, 100);
        assert!(state.max_wait.is_some());
        assert!(state.processing_task.is_none());
        assert!(state.buf_cost == 0);
        assert!(state.queue.is_empty());

        // Timing out with no changes should not spawn a task
        // The timer should be reset
        state.handle_timeout(&agent, &bookie);
        assert!(state.processing_task.is_none());
        assert!(state.max_wait.is_some());
        assert!(state.buf_cost == 0);
        assert!(state.queue.is_empty());

        // Phase 1: Add a few changes (5) - not enough to hit threshold
        // Threshold = 100 * 0.9 = 90, we'll add 5 changes (cost = 5)
        simulate_burst(&mut state, 5);

        // Should not have spawned (buf_cost = 5 < threshold = 90)
        assert!(state.processing_task.is_none());
        assert_eq!(state.buf_cost, 5);
        assert_eq!(state.queue.len(), 5);

        // Phase 2: Trigger timeout processing
        state.handle_timeout(&agent, &bookie);

        // Should have spawned a task now
        assert!(state.processing_task.is_some());
        assert_eq!(state.buf_cost, 0); // All drained
        assert_eq!(state.queue.len(), 0);

        // Batch size should decrease because actual_cost (5) < threshold (90)
        // when hitting the timeout. We're already at the min batch size, so it
        // should stay at it
        assert_eq!(state.current_batch_size, 100);

        // Phase 3: While task is running, simulate a burst of changes
        // Add 2001 changes (cost = 2001) - this is a burst!
        simulate_burst(&mut state, 2001);

        // Task is still running, so changes should be queued
        assert!(state.processing_task.is_some());
        assert_eq!(state.buf_cost, 2001);
        assert_eq!(state.queue.len(), 2001);

        // Phase 4: Complete the task successfully
        // After completion with buf_cost=2001 >= threshold=90:
        // - Should spawn immediately
        // - Should burst to batch_size 2000
        // - No timer should be spawned

        let task = state.processing_task.take().unwrap();
        let task_result = task.await;
        state.handle_task_completion(&agent, &bookie, task_result);
        assert_eq!(state.buf_cost, 1);
        assert_eq!(state.queue.len(), 1); // Drained ~100
        assert_eq!(state.current_batch_size, 2000);
        assert!(state.max_wait.is_none());

        // Phase 5: Complete the second task
        // - We should not spawn another task cause we are below the threshold of 1800
        // - The timer should be spawned
        let task = state.processing_task.take().unwrap();
        let task_result = task.await;
        state.handle_task_completion(&agent, &bookie, task_result);
        assert!(state.processing_task.is_none());
        assert_eq!(state.buf_cost, 1);
        assert_eq!(state.queue.len(), 1);
        assert_eq!(state.current_batch_size, 2000);
        assert!(state.max_wait.is_some());

        // Phase 6: Simulate another burst of changes
        // As the batch size is 2000, we should spawn a new task after 1800 changes
        // The timer should be cancelled
        simulate_burst(&mut state, 1900);
        assert!(state.processing_task.is_some());
        assert_eq!(state.buf_cost, 101);
        assert_eq!(state.queue.len(), 101);
        assert_eq!(state.current_batch_size, 2000);
        assert!(state.max_wait.is_none());

        // Phase 7: Complete the third task
        // - We should not spawn another task cause we are below the threshold of 1800
        // - The timer should be spawned
        let task = state.processing_task.take().unwrap();
        let task_result = task.await;
        state.handle_task_completion(&agent, &bookie, task_result);
        assert!(state.processing_task.is_none());
        assert_eq!(state.buf_cost, 101);
        assert_eq!(state.queue.len(), 101);
        assert_eq!(state.current_batch_size, 2000);
        assert!(state.max_wait.is_some());

        // Phase 8: Burst below threshold
        simulate_burst(&mut state, 1000);
        assert!(state.processing_task.is_none());
        assert_eq!(state.buf_cost, 1101);
        assert_eq!(state.queue.len(), 1101);
        assert_eq!(state.current_batch_size, 2000);
        assert!(state.max_wait.is_some()); // Timer is still running

        // Phase 9: We timed out, so the batch size should get decreased
        state.handle_timeout(&agent, &bookie);
        assert!(state.processing_task.is_some());
        assert_eq!(state.buf_cost, 0);
        assert_eq!(state.queue.len(), 0);
        assert_eq!(state.current_batch_size, 1000);
        assert!(state.max_wait.is_none());

        // Phase 10: Complete the task
        let task = state.processing_task.take().unwrap();
        let task_result = task.await;
        state.handle_task_completion(&agent, &bookie, task_result);
        // No instant spawn
        assert!(state.processing_task.is_none());
        assert_eq!(state.buf_cost, 0);
        assert_eq!(state.queue.len(), 0);
        assert_eq!(state.current_batch_size, 1000);
        // Timer should be spawned
        assert!(state.max_wait.is_some());

        // Phase 11: Batch size should return to normal
        state.handle_timeout(&agent, &bookie);
        assert!(state.processing_task.is_none());
        assert_eq!(state.buf_cost, 0);
        assert_eq!(state.queue.len(), 0);
        assert_eq!(state.current_batch_size, 100);
        assert!(state.max_wait.is_some());

        Ok(())
    }
}
