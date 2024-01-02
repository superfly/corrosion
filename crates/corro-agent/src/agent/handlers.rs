//! Various handler functions
//!
//! This module is _big_ and maybe should be split up further.

use std::{
    cmp,
    net::SocketAddr,
    time::{Duration, Instant},
};

use crate::{
    agent::{bi, bootstrap, uni, util, SyncClientError, ANNOUNCE_INTERVAL, MIN_CHANGES_CHUNK},
    api::peer::parallel_sync,
    transport::Transport,
};
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, Bookie, SplitPool},
    broadcast::{BroadcastV1, ChangeSource, ChangeV1, FocaInput, UniPayload},
    sync::generate_sync,
};

use bytes::Bytes;
use foca::Notification;
use metrics::{counter, gauge, histogram, increment_counter};
use rand::{prelude::IteratorRandom, rngs::StdRng, SeedableRng};
use spawn::spawn_counted;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::block_in_place,
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
    process_uni_tx: Sender<UniPayload>,
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
                spawn_incoming_connection_handlers(
                    &agent,
                    &bookie,
                    &tripwire,
                    process_uni_tx.clone(),
                    connecting,
                );
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
    process_uni_tx: Sender<UniPayload>,
    connecting: quinn::Connecting,
) {
    let process_uni_tx = process_uni_tx.clone();
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

        increment_counter!("corro.peer.connection.accept.total");

        debug!("accepted a QUIC conn from {remote_addr}");

        // Spawn handler tasks for this connection
        spawn_foca_handler(&agent, &tripwire, &conn);
        uni::spawn_unipayload_handler(&tripwire, &conn, process_uni_tx);
        bi::spawn_bipayload_handler(&agent, &bookie, &tripwire, &conn);
    });
}

/// Spawn a single task that accepts chunks from a receiver and
/// updates cluster member round-trip-times in the agent state.
pub fn spawn_rtt_handler(agent: &Agent, rtt_rx: Receiver<(SocketAddr, Duration)>) {
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
                            increment_counter!("corro.peer.datagram.recv.total");
                            counter!("corro.peer.datagram.bytes.recv.total", b.len() as u64);
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

// DOCME: provide function context
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

// DOCME: provide function context
pub async fn handle_gossip_to_send(transport: Transport, mut to_send_rx: Receiver<(Actor, Bytes)>) {
    // TODO: use tripwire and drain messages to send when that happens...
    while let Some((actor, data)) = to_send_rx.recv().await {
        trace!("got gossip to send to {actor:?}");

        let addr = actor.addr();
        let actor_id = actor.id();

        let transport = transport.clone();

        let len = data.len();
        spawn_counted(async move {
            if let Err(e) = transport.send_datagram(addr, data).await {
                error!("could not write datagram {addr}: {e}");
                return;
            }
            increment_counter!("corro.peer.datagram.sent.total", "actor_id" => actor_id.to_string());
            counter!("corro.peer.datagram.bytes.sent.total", len as u64);
        }.instrument(debug_span!("send_swim_payload", %addr, %actor_id, buf_size = len)));
    }
}

// DOCME: provide function context
pub async fn handle_broadcasts(agent: Agent, mut bcast_rx: Receiver<BroadcastV1>) {
    while let Some(bcast) = bcast_rx.recv().await {
        increment_counter!("corro.broadcast.recv.count");
        match bcast {
            BroadcastV1::Change(change) => {
                if let Err(_e) = agent
                    .tx_changes()
                    .send((change, ChangeSource::Broadcast))
                    .await
                {
                    error!("changes channel is closed");
                    break;
                }
            }
        }
    }
}

// DOCME: provide function context
pub async fn handle_notifications(
    agent: Agent,
    mut notification_rx: Receiver<Notification<Actor>>,
) {
    while let Some(notification) = notification_rx.recv().await {
        trace!("handle notification");
        match notification {
            Notification::MemberUp(actor) => {
                let (added, same) = { agent.members().write().add_member(&actor) };
                trace!("Member Up {actor:?} (added: {added})");
                if added {
                    debug!("Member Up {actor:?}");
                    increment_counter!("corro.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
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
                increment_counter!("corro.swim.notification", "type" => "memberup");
            }
            Notification::MemberDown(actor) => {
                let removed = { agent.members().write().remove_member(&actor) };
                trace!("Member Down {actor:?} (removed: {removed})");
                if removed {
                    debug!("Member Down {actor:?}");
                    increment_counter!("corro.gossip.member.removed", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually removed a member
                    // notify of new cluster size
                    let member_len = { agent.members().read().states.len() as u32 };
                    if let Ok(size) = member_len.try_into() {
                        if let Err(e) = agent.tx_foca().send(FocaInput::ClusterSize(size)).await {
                            error!("could not send new foca cluster size: {e}");
                        }
                    }
                }
                increment_counter!("corro.swim.notification", "type" => "memberdown");
            }
            Notification::Active => {
                info!("Current node is considered ACTIVE");
                increment_counter!("corro.swim.notification", "type" => "active");
            }
            Notification::Idle => {
                warn!("Current node is considered IDLE");
                increment_counter!("corro.swim.notification", "type" => "idle");
            }
            // this happens when we leave the cluster
            Notification::Defunct => {
                debug!("Current node is considered DEFUNCT");
                increment_counter!("corro.swim.notification", "type" => "defunct");
            }
            Notification::Rejoin(id) => {
                info!("Rejoined the cluster with id: {id:?}");
                increment_counter!("corro.swim.notification", "type" => "rejoin");
            }
        }
    }
}

async fn db_cleanup(pool: &SplitPool) -> eyre::Result<()> {
    debug!("handling db_cleanup (WAL truncation)");
    let conn = pool.write_low().await?;
    block_in_place(move || {
        let start = Instant::now();

        let busy: bool =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
        if busy {
            warn!("could not truncate sqlite WAL, database busy");
            increment_counter!("corro.db.wal.truncate.busy");
        } else {
            debug!("successfully truncated sqlite WAL!");
            histogram!(
                "corro.db.wal.truncate.seconds",
                start.elapsed().as_secs_f64()
            );
        }
        Ok::<_, eyre::Report>(())
    })?;
    debug!("done handling db_cleanup");
    Ok(())
}

// Every now and then, we need to truncate the WAL.
// It can get enormous when under write pressure.
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

// DOCME: provide function context
pub async fn handle_changes(
    agent: Agent,
    bookie: Bookie,
    mut rx_changes: Receiver<(ChangeV1, ChangeSource)>,
    mut tripwire: Tripwire,
) {
    let mut buf = vec![];
    let mut count = 0;

    let mut max_wait = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            Some((change, src)) = rx_changes.recv() => {
                counter!("corro.agent.changes.recv", std::cmp::max(change.len(), 1) as u64); // count empties...
                count += change.len(); // don't count empties
                buf.push((change, src));
                if count < MIN_CHANGES_CHUNK {
                    continue;
                }
            },
            _ = max_wait.tick() => {
                // got a wait interval tick...
                if buf.is_empty() {
                    continue;
                }
            },
            _ = &mut tripwire => {
                break;
            }
            else => {
                break;
            }
        }

        // drain and process current changes!
        #[allow(clippy::drain_collect)]
        if let Err(e) =
            util::process_multiple_changes(&agent, &bookie, buf.drain(..).collect()).await
        {
            error!("could not process multiple changes: {e}");
        }

        // reset count
        count = 0;
    }

    info!("Draining changes receiver...");

    // drain!
    while let Ok((change, src)) = rx_changes.try_recv() {
        let changes_count = std::cmp::max(change.len(), 1);
        counter!("corro.agent.changes.recv", changes_count as u64);
        count += changes_count;
        buf.push((change, src));
        if count >= MIN_CHANGES_CHUNK {
            // drain and process current changes!
            #[allow(clippy::drain_collect)]
            if let Err(e) =
                util::process_multiple_changes(&agent, &bookie, buf.drain(..).collect()).await
            {
                error!("could not process last multiple changes: {e}");
            }

            // reset count
            count = 0;
        }
    }

    // process the last changes we got!
    if let Err(e) = util::process_multiple_changes(&agent, &bookie, buf).await {
        error!("could not process multiple changes: {e}");
    }
}

// DOCME: add context
#[tracing::instrument(skip_all, err, level = "debug")]
pub async fn handle_sync(
    agent: &Agent,
    bookie: &Bookie,
    transport: &Transport,
) -> Result<(), SyncClientError> {
    let sync_state = generate_sync(bookie, agent.actor_id()).await;

    for (actor_id, needed) in sync_state.need.iter() {
        gauge!("corro.sync.client.needed", needed.len() as f64, "actor_id" => actor_id.to_string());
    }
    for (actor_id, version) in sync_state.heads.iter() {
        gauge!("corro.sync.client.head", version.0 as f64, "actor_id" => actor_id.to_string());
    }

    let chosen: Vec<(ActorId, SocketAddr)> = {
        let candidates = {
            let members = agent.members().read();

            members
                .states
                .iter()
                .filter(|(id, _state)| **id != agent.actor_id())
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
