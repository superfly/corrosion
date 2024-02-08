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
    agent::{bi, bootstrap, uni, SyncClientError, ANNOUNCE_INTERVAL},
    api::peer::parallel_sync,
    change,
    transport::Transport,
};
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, Bookie, SplitPool},
    base::CrsqlSeq,
    broadcast::{BroadcastInput, BroadcastV1, ChangeSource, ChangeV1, FocaInput},
    channel::CorroReceiver,
    sync::generate_sync,
};

use bytes::Bytes;
use foca::Notification;
use indexmap::IndexMap;
use metrics::{counter, gauge, histogram};
use rand::{prelude::IteratorRandom, rngs::StdRng, SeedableRng};
use rangemap::RangeInclusiveSet;
use spawn::spawn_counted;
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
        debug!("got a connection from {remote_addr}");

        let conn = match connecting.await {
            Ok(conn) => conn,
            Err(e) => {
                error!("could not handshake connection from {remote_addr}: {e}");
                return;
            }
        };

        counter!("corro.peer.connection.accept.total").increment(1);

        debug!("accepted a QUIC conn from {remote_addr}");

        // Spawn handler tasks for this connection
        spawn_foca_handler(&agent, &tripwire, &conn);
        uni::spawn_unipayload_handler(&agent, &bookie, &tripwire, &conn);
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
                let (added, same) = { agent.members().write().add_member(&actor) };
                trace!("Member Up {actor:?} (added: {added})");
                if added {
                    debug!("Member Up {actor:?}");
                    counter!("corro.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string()).increment(1);
                    // actually added a member
                    // notify of new cluster size
                    let members_len = { agent.members().read().states.len() as u32 };
                    if let Ok(size) = members_len.try_into() {
                        if let Err(e) = agent.tx_foca().send(FocaInput::ClusterSize(size)).await {
                            error!("could not send new foca cluster size: {e}");
                        }
                    }
                } else if !same {
                    // had a older timestamp!
                    if let Err(e) = agent
                        .tx_foca()
                        .send(FocaInput::ApplyMany(vec![foca::Member::new(
                            actor.clone(),
                            foca::Incarnation::default(),
                            foca::State::Down,
                        )]))
                        .await
                    {
                        warn!(?actor, "could not manually declare actor as down! {e}");
                    }
                }
                counter!("corro.swim.notification", "type" => "memberup").increment(1);
            }
            Notification::MemberDown(actor) => {
                let removed = { agent.members().write().remove_member(&actor) };
                trace!("Member Down {actor:?} (removed: {removed})");
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
async fn db_cleanup(pool: &SplitPool) -> eyre::Result<()> {
    debug!("handling db_cleanup (WAL truncation)");
    let conn = pool.write_low().await?;
    block_in_place(move || {
        let start = Instant::now();

        let busy: bool =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
        if busy {
            warn!("could not truncate sqlite WAL, database busy");
            counter!("corro.db.wal.truncate.busy").increment(1);
        } else {
            debug!("successfully truncated sqlite WAL!");
            histogram!("corro.db.wal.truncate.seconds").record(start.elapsed().as_secs_f64());
        }
        Ok::<_, eyre::Report>(())
    })?;
    debug!("done handling db_cleanup");
    Ok(())
}

/// See `db_cleanup`
pub fn spawn_handle_db_cleanup(pool: SplitPool) {
    tokio::spawn(async move {
        let mut db_cleanup_interval = tokio::time::interval(Duration::from_secs(60 * 15));
        loop {
            db_cleanup_interval.tick().await;

            if let Err(e) = db_cleanup(&pool).await {
                error!("could not truncate db: {e}");
            }
        }
    });
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
    const MIN_CHANGES_CHUNK: usize = 2000;
    let mut queue: VecDeque<(ChangeV1, ChangeSource, Instant)> = VecDeque::new();
    let mut buf = vec![];
    let mut count = 0;

    const MAX_CONCURRENT: usize = 3;
    let mut join_set = JoinSet::new();

    let mut max_wait = tokio::time::interval(Duration::from_millis(500));

    const MAX_SEEN_CACHE_LEN: usize = 10000;
    const KEEP_SEEN_CACHE_SIZE: usize = 1000;
    let mut seen: IndexMap<_, RangeInclusiveSet<CrsqlSeq>> = IndexMap::new();

    // complicated loop to process changes efficiently w/ a max concurrency
    // and a minimum chunk size for bigger and faster SQLite transactions
    loop {
        while count >= MIN_CHANGES_CHUNK && join_set.len() < MAX_CONCURRENT {
            // we're already bigger than the minimum size of changes batch
            // so we want to accumulate at least that much and process them
            // concurrently bvased on MAX_CONCURRENCY
            let mut tmp_count = 0;
            while let Some((change, src, queued_at)) = queue.pop_front() {
                tmp_count += change.len();
                buf.push((change, src, queued_at));
                if tmp_count >= MIN_CHANGES_CHUNK {
                    break;
                }
            }

            if buf.is_empty() {
                break;
            }

            debug!(count = %tmp_count, "spawning processing multiple changes from beginning of loop");
            join_set.spawn(change::process_multiple_changes(
                agent.clone(),
                bookie.clone(),
                std::mem::take(&mut buf),
            ));

            count -= tmp_count;
        }

        tokio::select! {
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

            Some((change, src)) = rx_changes.recv() => {
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
                    if change.versions().all(|v| seen.contains_key(&(change.actor_id, v))) {
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
                        .read(format!(
                            "handle_change(get):{}",
                            change.actor_id.as_simple()
                        ))
                        .await
                        .get(&change.actor_id)
                        .cloned()
                };

                if let Some(booked) = booked {
                    if booked
                        .read(format!(
                            "handle_change(contains?):{}",
                            change.actor_id.as_simple()
                        ))
                        .await
                        .contains_all(change.versions(), change.seqs())
                    {
                        trace!("already seen, stop disseminating");
                        continue;
                    }
                }

                if let Some(recv_lag) = recv_lag {
                    let src_str: &'static str = src.into();
                    histogram!("corro.agent.changes.recv.lag.seconds", "source" => src_str).record(recv_lag.as_secs_f64());
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
                            .try_send(BroadcastInput::Rebroadcast(BroadcastV1::Change(change.clone())))
                    {
                        debug!("broadcasts are full or done!");
                    }
                }

                queue.push_back((change, src, Instant::now()));

                count += change_len; // track number of individual changes, not changesets
            },

            _ = max_wait.tick() => {
                // got a wait interval tick...

                gauge!("corro.agent.changes.in_queue").set(count as f64);
                gauge!("corro.agent.changesets.in_queue").set(queue.len() as f64);
                gauge!("corro.agent.changes.processing.jobs").set(join_set.len() as f64);

                if count < MIN_CHANGES_CHUNK && !queue.is_empty() && join_set.len() < MAX_CONCURRENT {
                    // we can process this right away
                    debug!(%count, "spawning processing multiple changes from max wait interval");
                    join_set.spawn(change::process_multiple_changes(
                        agent.clone(),
                        bookie.clone(),
                        queue.drain(..).collect(),
                    ));
                    count = 0;
                }

                if seen.len() > MAX_SEEN_CACHE_LEN {
                    // we don't want to keep too many entries in here.
                    seen = seen.split_off(seen.len() - KEEP_SEEN_CACHE_SIZE);
                }
            },

            _ = &mut tripwire => {
                break;
            }

            else => {
                break;
            }
        }
    }

    info!("Draining changes receiver...");

    // drain!
    while let Ok((change, src)) = rx_changes.try_recv() {
        let changes_count = std::cmp::max(change.len(), 1);
        counter!("corro.agent.changes.recv").increment(changes_count as u64);
        count += changes_count;
        queue.push_back((change, src, Instant::now()));
        if count >= MIN_CHANGES_CHUNK {
            // drain and process current changes!
            if let Err(e) = change::process_multiple_changes(
                agent.clone(),
                bookie.clone(),
                queue.drain(..).collect(),
            )
            .await
            {
                error!("could not process last multiple changes: {e}");
            }

            // reset count
            count = 0;
        }
    }

    // process the last changes we got!
    if let Err(e) =
        change::process_multiple_changes(agent, bookie, queue.into_iter().collect()).await
    {
        error!("could not process multiple changes: {e}");
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
                .map(|(id, state)| (*id, state.ring.unwrap_or(255), state.addr))
                .collect::<Vec<(ActorId, u8, SocketAddr)>>()
        };

        if candidates.is_empty() {
            return Ok(());
        }

        debug!("found {} candidates to synchronize with", candidates.len());

        let desired_count = cmp::max(cmp::min(candidates.len() / 100, 10), 3);

        let mut rng = StdRng::from_entropy();

        let mut choices = candidates
            .into_iter()
            .choose_multiple(&mut rng, desired_count * 2);

        choices.sort_by(|a, b| {
            // most missing actors first
            sync_state
                .need_len_for_actor(&b.0)
                .cmp(&sync_state.need_len_for_actor(&a.0))
                // if equal, look at proximity (via `ring`)
                .then_with(|| a.1.cmp(&b.1))
        });

        choices.truncate(desired_count);
        choices
            .into_iter()
            .map(|(actor_id, _, addr)| (actor_id, addr))
            .collect()
    };

    if chosen.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let n = parallel_sync(agent, transport, chosen.clone(), sync_state).await?;

    let elapsed = start.elapsed();
    if n > 0 {
        info!(
            "synced {n} changes w/ {} in {}s @ {} changes/s",
            chosen
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
