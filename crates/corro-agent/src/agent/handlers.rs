//! Various handler functions
//!
//! This module is _big_ and maybe should be split up further.

use std::collections::HashMap;
use std::ops::RangeInclusive;

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
use camino::Utf8Path;
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, Bookie, SplitPool},
    base::CrsqlSeq,
    broadcast::{BroadcastInput, BroadcastV1, ChangeSource, ChangeV1, Changeset, FocaInput},
    channel::CorroReceiver,
    members::MemberAddedResult,
    sync::generate_sync,
};

use bytes::Bytes;
use corro_types::agent::ChangeError;
use corro_types::base::CrsqlDbVersion;
use corro_types::broadcast::Timestamp;
use foca::Notification;
use indexmap::map::Entry;
use indexmap::IndexMap;
use metrics::{counter, gauge, histogram};
use rand::{prelude::IteratorRandom, rngs::StdRng, SeedableRng};
use rangemap::RangeInclusiveSet;
use spawn::spawn_counted;
use tokio::time::sleep;
use tokio::{
    sync::mpsc::Receiver as TokioReceiver,
    task::{block_in_place, JoinSet},
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
                let connecting = match gossip_server_endpoint
                    .accept()
                    .preemptible(&mut tripwire)
                    .await
                {
                    Outcome::Completed(Some(connecting)) => connecting,
                    Outcome::Completed(None) => return,
                    Outcome::Preempted(_) => break,
                };

                // Spawn incoming connection handlers
                spawn_incoming_connection_handlers(&agent, &bookie, &tripwire, connecting);
            }

            // graceful shutdown
            gossip_server_endpoint.reject_new_connections();
            _ = gossip_server_endpoint
                .wait_idle()
                .with_timeout(Duration::from_secs(5))
                .await;
            gossip_server_endpoint.close(0u32.into(), b"shutting down");
        }
    });
}

/// Spawn a task which handles all state and interactions for a given
/// incoming connection.  This function spawns many futures!
pub fn spawn_incoming_connection_handlers(
    agent: &Agent,
    bookie: &Bookie,
    tripwire: &Tripwire,
    connecting: quinn::Connecting,
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
pub fn spawn_rtt_handler(agent: &Agent, rtt_rx: TokioReceiver<(SocketAddr, Duration)>) {
    tokio::spawn({
        let agent = agent.clone();
        async move {
            let stream = ReceiverStream::new(rtt_rx);
            // we can handle a lot of them I think...
            let chunker = stream.chunks_timeout(1024, Duration::from_secs(1));
            tokio::pin!(chunker);
            while let Some(chunks) = StreamExt::next(&mut chunker).await {
                let mut members = agent.members().write();
                for (addr, rtt) in chunks {
                    members.add_rtt(addr, rtt);
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
pub fn spawn_swim_announcer(agent: &Agent, gossip_addr: SocketAddr) {
    tokio::spawn({
        let agent = agent.clone();
        async move {
            let mut boff = backoff::Backoff::new(10)
                .timeout_range(Duration::from_secs(5), Duration::from_secs(120))
                .iter();
            let timer = tokio::time::sleep(Duration::new(0, 0));
            tokio::pin!(timer);

            loop {
                timer.as_mut().await;

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
) {
    // TODO: use tripwire and drain messages to send when that happens...
    while let Some((actor, data)) = swim_to_send_rx.recv().await {
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
    }
}

/// Poll for updates from the cluster membership system (`foca`/ SWIM)
/// and apply any incoming changes to the local actor/ agent state.
pub async fn handle_notifications(
    agent: Agent,
    mut notification_rx: CorroReceiver<Notification<Actor>>,
) {
    while let Some(notification) = notification_rx.recv().await {
        trace!("handle notification");
        match notification {
            Notification::MemberUp(actor) => {
                let member_added_res = agent.members().write().add_member(&actor);
                info!("Member Up {actor:?} (result: {member_added_res:?})");

                match member_added_res {
                    MemberAddedResult::NewMember => {
                        debug!("Member Added {actor:?}");
                        counter!("corro.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string()).increment(1);

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
            Notification::MemberDown(actor) => {
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
            Notification::Active => {
                info!("Current node is considered ACTIVE");
                counter!("corro.swim.notification", "type" => "active").increment(1);
            }
            Notification::Idle => {
                warn!("Current node is considered IDLE");
                counter!("corro.swim.notification", "type" => "idle").increment(1);
            }
            // this happens when we leave the cluster
            Notification::Defunct => {
                debug!("Current node is considered DEFUNCT");
                counter!("corro.swim.notification", "type" => "defunct").increment(1);
            }
            Notification::Rejoin(id) => {
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
fn wal_checkpoint(conn: &rusqlite::Connection) -> eyre::Result<()> {
    debug!("handling db_cleanup (WAL truncation)");
    let start = Instant::now();

    let orig: u64 = conn.pragma_query_value(None, "busy_timeout", |row| row.get(0))?;
    conn.pragma_update(None, "busy_timeout", 60000)?;

    let busy: bool = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
    if busy {
        warn!("could not truncate sqlite WAL, database busy");
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
        conn.pragma_update(None, "busy_timeout", 60000)?;

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
    let wal_threshold = agent.config().perf.wal_threshold_gb as u64;
    wal_path.set_extension(format!("{}-wal", wal_path.extension().unwrap_or_default()));

    let pool = agent.pool().clone();

    tokio::spawn(async move {
        let truncate_wal_threshold: u64 = wal_threshold * 1024 * 1024 * 1024;

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
        sleep(Duration::from_secs(60)).await;

        let mut vacuum_interval = tokio::time::interval(Duration::from_secs(60 * 5));

        const MAX_DB_FREE_PAGES: u64 = 10000;

        loop {
            vacuum_interval.tick().await;
            if let Err(e) = vacuum_db(&pool, MAX_DB_FREE_PAGES).await {
                error!("could not check freelist and vacuum: {e}");
            }

            if let Err(e) =
                wal_checkpoint_over_threshold(wal_path.as_path(), &pool, truncate_wal_threshold)
                    .await
            {
                error!("could not wal_checkpoint truncate: {e}");
            }
        }
    });
}

async fn wal_checkpoint_over_threshold(
    wal_path: &Utf8Path,
    pool: &SplitPool,
    threshold: u64,
) -> eyre::Result<bool> {
    let should_truncate = wal_path.metadata()?.len() > threshold;
    if should_truncate {
        let conn = pool.write_low().await?;
        block_in_place(|| wal_checkpoint(&conn))?;
    }
    Ok(should_truncate)
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
    let max_changes_chunk: usize = agent.config().perf.apply_queue_len;
    let max_queue_len: usize = agent.config().perf.processing_queue_len;
    let mut queue: VecDeque<(ChangeV1, ChangeSource, Instant)> = VecDeque::new();
    let mut buf = vec![];
    let mut buf_cost = 0;

    const MAX_CONCURRENT: usize = 5;
    let mut join_set = JoinSet::new();

    let mut max_wait = tokio::time::interval(Duration::from_millis(
        agent.config().perf.apply_queue_timeout as u64,
    ));

    let max_seen_cache_len: usize = max_queue_len;
    let keep_seen_cache_size: usize = cmp::max(10, max_seen_cache_len / 10);
    let mut seen: IndexMap<_, RangeInclusiveSet<CrsqlSeq>> = IndexMap::new();

    let mut drop_log_count: u64 = 0;
    // complicated loop to process changes efficiently w/ a max concurrency
    // and a minimum chunk size for bigger and faster SQLite transactions
    loop {
        while buf_cost >= max_changes_chunk && join_set.len() < MAX_CONCURRENT {
            // we're already bigger than the minimum size of changes batch
            // so we want to accumulate at least that much and process them
            // concurrently bvased on MAX_CONCURRENCY
            let mut tmp_cost = 0;
            while let Some((change, src, queued_at)) = queue.pop_front() {
                tmp_cost += change.processing_cost();
                buf.push((change, src, queued_at));
                if tmp_cost >= max_changes_chunk {
                    break;
                }
            }

            if buf.is_empty() {
                break;
            }

            debug!(count = %tmp_cost, "spawning processing multiple changes from beginning of loop");
            let changes = std::mem::take(&mut buf);
            let agent = agent.clone();
            let bookie = bookie.clone();
            join_set.spawn(process_multiple_changes(agent, bookie, changes.clone()));
            counter!("corro.agent.changes.batch.spawned").increment(1);

            buf_cost -= tmp_cost;
        }

        let (change, src) = tokio::select! {
            biased;

            // process these first, we don't care about the result,
            // but we need to drain it to free up concurrency
            res = join_set.join_next(), if !join_set.is_empty() => {
                debug!("processed multiple changes concurrently");
                if let Some(Ok(Err(e))) = res {
                    error!("could not process multiple changes: {e}");
                }
                continue;
            },

            maybe_change_src = rx_changes.recv() => match maybe_change_src {
                Some((change, src)) => (change, src),
                None => break,
            },

            _ = max_wait.tick() => {
                // got a wait interval tick...

                gauge!("corro.agent.changes.in_queue").set(buf_cost as f64);
                gauge!("corro.agent.changesets.in_queue").set(queue.len() as f64);
                gauge!("corro.agent.changes.processing.jobs").set(join_set.len() as f64);

                if buf_cost < max_changes_chunk && !queue.is_empty() && join_set.len() < MAX_CONCURRENT {
                    // we can process this right away
                    debug!(%buf_cost, "spawning processing multiple changes from max wait interval");
                    let changes: Vec<_> = queue.drain(..).collect();
                    let agent = agent.clone();
                    let bookie = bookie.clone();
                    join_set.spawn(process_multiple_changes(agent, bookie, changes.clone()));
                    counter!("corro.agent.changes.batch.spawned").increment(1);
                    buf_cost = 0;
                }

                if seen.len() > max_seen_cache_len {
                    // we don't want to keep too many entries in here.
                    seen = seen.split_off(seen.len() - keep_seen_cache_size);
                }
                continue
            },

            _ = &mut tripwire => {
                break;
            }
        };

        let change_len = change.len();
        counter!("corro.agent.changes.recv").increment(std::cmp::max(change_len, 1) as u64); // count empties...

        if change.actor_id == agent.actor_id() {
            continue;
        }

        if let Some(mut seqs) = change.seqs().cloned() {
            let v = *change.versions().start();
            if let Some(seen_seqs) = seen.get(&(change.actor_id, v)) {
                if seqs.all(|seq| seen_seqs.contains(&seq)) {
                    continue;
                }
            }
        } else {
            // empty versions
            if change
                .versions()
                .all(|v| seen.contains_key(&(change.actor_id, v)))
            {
                continue;
            }
        }

        let recv_lag = change
            .ts()
            .map(|ts| (agent.clock().new_timestamp().get_time() - ts.0).to_duration());

        if matches!(src, ChangeSource::Broadcast) {
            counter!("corro.broadcast.recv.count", "kind" => "change").increment(1);
        }

        let booked = {
            bookie
                .read("handle_change(get)", change.actor_id.as_simple())
                .await
                .get(&change.actor_id)
                .cloned()
        };

        if let Some(booked) = booked {
            if booked
                .read("handle_change(contains?)", change.actor_id.as_simple())
                .await
                .contains_all(change.versions(), change.seqs())
            {
                trace!("already seen, stop disseminating");
                continue;
            }
        }

        // drop old items when the queue is full.
        if queue.len() >= max_queue_len {
            let mut dropped_count = 0;
            if let Some((dropped_change, _, _)) = queue.pop_front() {
                for v in dropped_change.versions() {
                    if let Entry::Occupied(mut entry) = seen.entry((change.actor_id, v)) {
                        if let Some(seqs) = dropped_change.seqs().cloned() {
                            entry.get_mut().remove(seqs);
                        } else {
                            entry.remove_entry();
                        }
                    };
                }

                buf_cost -= dropped_change.processing_cost();
                dropped_count += 1;
            }
            counter!("corro.agent.changes.dropped").increment(dropped_count);

            log_at_pow_10("dropped old change from queue", &mut drop_log_count);
        }

        if let Some(recv_lag) = recv_lag {
            let src_str: &'static str = src.into();
            histogram!("corro.agent.changes.recv.lag.seconds", "source" => src_str)
                .record(recv_lag.as_secs_f64());
        }

        // this will only run once for a non-empty changeset
        for v in change.versions() {
            let entry = seen.entry((change.actor_id, v)).or_default();
            if let Some(seqs) = change.seqs().cloned() {
                entry.extend([seqs]);
            }
        }

        if matches!(src, ChangeSource::Broadcast) && !change.is_empty() {
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

        let cost = change.processing_cost();
        queue.push_back((change, src, Instant::now()));

        buf_cost += cost; // tracks the cost, not number of changes
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
                    **id != agent.actor_id() && state.cluster_id == agent.cluster_id()
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

        let desired_count = (candidates.len() / 100).clamp(3, 10);
        debug!("Selected {desired_count} nodes to sync with");

        let mut rng = StdRng::from_entropy();

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
    use corro_types::api::{Change, ColumnName, TableName};
    use corro_types::{base::CrsqlDbVersion, config::Config, pubsub::pack_columns};
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

        wal_checkpoint(&conn)?;
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
        config.perf.apply_queue_len = 1;
        config.perf.processing_queue_len = 3;

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
                    db_version: CrsqlDbVersion(4),
                    seq: CrsqlSeq(0),
                    site_id: agent.actor_id().to_bytes(),
                    cl: 1,
                };

                let change = (
                    ChangeV1 {
                        actor_id: other_actor,
                        changeset: Changeset::Full {
                            version: CrsqlDbVersion(i as u64),
                            changes: vec![crsql_row.clone()],
                            seqs: CrsqlSeq(0)..=CrsqlSeq(0),
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

        let bookie = bookie.read::<&str, _>("read booked", None).await;
        let booked = bookie
            .get(&other_actor)
            .unwrap()
            .read::<&str, _>("test", None)
            .await;
        assert!(booked.contains_all(CrsqlDbVersion(6)..=CrsqlDbVersion(10), None));
        assert!(booked.contains_all(CrsqlDbVersion(1)..=CrsqlDbVersion(3), None));
        assert!(!booked.contains_version(&CrsqlDbVersion(5)));
        assert!(!booked.contains_version(&CrsqlDbVersion(4)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn ensure_vacuum_works() -> eyre::Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.into_path().join("db.sqlite");

        {
            let db_conn = Connection::open(db_path.clone())?;
            db_conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")?;
        }

        println!("temp db: {:?}", db_path);
        let write_sema = Arc::new(Semaphore::new(1));
        let pool = SplitPool::create(db_path, write_sema.clone()).await?;

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
}
