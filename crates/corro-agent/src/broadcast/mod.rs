use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use bincode::DefaultOptions;
use bytes::{BufMut, Bytes, BytesMut};
use foca::{BincodeCodec, Foca, NoCustomBroadcast, Notification, Timer};
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Future,
};
use metrics::{counter, gauge};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rusqlite::params;
use spawn::spawn_counted;
use speedy::Writable;
use strum::EnumDiscriminants;
use tokio::{
    sync::mpsc,
    task::{block_in_place, LocalSet},
    time::interval,
};
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{debug, error, log::info, trace, warn};
use tripwire::Tripwire;

use corro_types::{
    actor::{Actor, ActorId},
    agent::Agent,
    broadcast::{BroadcastInput, DispatchRuntime, FocaCmd, FocaInput, UniPayload, UniPayloadV1},
    channel::{bounded, CorroReceiver, CorroSender},
};

use crate::transport::Transport;

#[derive(Clone)]
struct TimerSpawner {
    send: mpsc::UnboundedSender<(Duration, Timer<Actor>)>,
}

impl TimerSpawner {
    pub fn new(timer_tx: mpsc::Sender<(Timer<Actor>, Instant)>) -> Self {
        let (send, mut recv) = mpsc::unbounded_channel();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        std::thread::spawn({
            move || {
                let local = LocalSet::new();

                local.spawn_local(async move {
                    while let Some((duration, timer)) = recv.recv().await {
                        let seq = Instant::now() + duration;

                        let timer_tx = timer_tx.clone();
                        tokio::task::spawn_local(async move {
                            tokio::time::sleep(duration).await;
                            timer_tx.send((timer, seq)).await.ok();
                        });
                    }
                    // If the while loop returns, then all the LocalSpawner
                    // objects have been dropped.
                });

                // This will return once all senders are dropped and all
                // spawned tasks have returned.
                rt.block_on(local);
            }
        });

        Self { send }
    }

    pub fn spawn(&self, task: (Duration, Timer<Actor>)) {
        self.send
            .send(task)
            .expect("Thread with LocalSet has shut down.");
    }
}

fn handle_timer(
    foca: &mut Foca<Actor, BincodeCodec<DefaultOptions>, StdRng, NoCustomBroadcast>,
    runtime: &mut DispatchRuntime<Actor>,
    timer_rx: &mut mpsc::Receiver<(Timer<Actor>, Instant)>,
    timer: Timer<Actor>,
    seq: Instant,
) {
    // trace!("handling Branch::HandleTimer");
    let mut v = vec![(timer, seq)];

    // drain the channel, in case there's a race among timers
    while let Ok((timer, seq)) = timer_rx.try_recv() {
        v.push((timer, seq));
    }

    // sort by instant these were scheduled
    v.sort_by(|a, b| a.1.cmp(&b.1));

    for (timer, _) in v {
        if let Err(e) = foca.handle_timer(timer, &mut *runtime) {
            error!("foca: error handling timer: {e}");
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn runtime_loop(
    actor: Actor,
    agent: Agent,
    transport: Transport,
    mut rx_foca: CorroReceiver<FocaInput>,
    mut rx_bcast: CorroReceiver<BroadcastInput>,
    to_send_tx: CorroSender<(Actor, Bytes)>,
    notifications_tx: CorroSender<Notification<Actor>>,
    mut tripwire: Tripwire,
) {
    debug!("starting runtime loop for actor: {actor:?}");
    let rng = StdRng::from_entropy();
    let actor_id = actor.id();

    let config = Arc::new(RwLock::new(make_foca_config(1.try_into().unwrap())));

    let mut foca = Foca::with_custom_broadcast(
        actor,
        config.read().clone(),
        rng,
        foca::BincodeCodec(bincode::DefaultOptions::new()),
        NoCustomBroadcast,
    );

    let (to_schedule_tx, mut to_schedule_rx) = bounded(10240, "to_schedule");

    let mut runtime: DispatchRuntime<Actor> =
        DispatchRuntime::new(to_send_tx, to_schedule_tx, notifications_tx);

    let (timer_tx, mut timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(timer_tx);

    tokio::spawn(async move {
        while let Some((duration, timer)) = to_schedule_rx.recv().await {
            timer_spawner.spawn((duration, timer));
        }
    });

    // foca SWIM operations loop.
    // NOTE: every turn of that loop should be fast or else we risk being a down suspect
    spawn_counted({
        let config = config.clone();
        let agent = agent.clone();
        let mut tripwire = tripwire.clone();
        async move {
            let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_cluster_size = unsafe { NonZeroU32::new_unchecked(1) };

            let mut last_states = HashMap::new();
            let mut diff_last_states_every = tokio::time::interval(Duration::from_secs(60));

            #[derive(EnumDiscriminants)]
            #[strum_discriminants(derive(strum::IntoStaticStr))]
            enum Branch {
                Foca(FocaInput),
                HandleTimer(Timer<Actor>, Instant),
                DiffMembers,
                Metrics,
            }

            loop {
                let branch = tokio::select! {
                    biased;
                    _ = &mut tripwire => {
                        break
                    },
                    timer = timer_rx.recv() => match timer {
                        Some((timer, seq)) => {
                            Branch::HandleTimer(timer, seq)
                        },
                        None => {
                            warn!("no more foca timers, breaking");
                            break;
                        }
                    },
                    input = rx_foca.recv() => match input {
                        Some(input) => {
                            Branch::Foca(input)
                        },
                        None => {
                            warn!("no more foca inputs");
                            break;
                        }
                    },
                    _ = metrics_interval.tick() => {
                        Branch::Metrics
                    },
                    _ = diff_last_states_every.tick() => {
                        Branch::DiffMembers
                    }
                };

                let start = Instant::now();
                let discriminant = BranchDiscriminants::from(&branch);

                match branch {
                    Branch::Foca(input) => match input {
                        FocaInput::Announce(actor) => {
                            trace!("handling FocaInput::Announce");
                            trace!("announcing actor: {actor:?}");
                            if let Err(e) = foca.announce(actor, &mut runtime) {
                                error!("foca announce error: {e}");
                            }
                        }
                        FocaInput::Data(data) => {
                            trace!("handling FocaInput::Data");
                            if let Err(e) = foca.handle_data(&data, &mut runtime) {
                                error!("error handling foca data: {e}");
                            }
                        }
                        FocaInput::ClusterSize(size) => {
                            trace!("handling FocaInput::ClusterSize");
                            // let diff: i64 =
                            //     (size.get() as i64 - last_cluster_size.get() as i64).abs();

                            // debug!("received cluster size update: {size}, last size: {last_cluster_size}, diff: {diff}");

                            if size != last_cluster_size {
                                debug!("Adjusting cluster size to {size}");
                                let new_config = make_foca_config(size);
                                if let Err(e) = foca.set_config(new_config.clone()) {
                                    error!("foca set_config error: {e}");
                                } else {
                                    last_cluster_size = size;
                                    let mut config = config.write();
                                    *config = new_config;
                                }
                            }
                        }
                        FocaInput::ApplyMany(updates) => {
                            trace!("handling FocaInput::ApplyMany");
                            if let Err(e) = foca.apply_many(updates.into_iter(), &mut runtime) {
                                error!("foca apply_many error: {e}");
                            }
                        }
                        FocaInput::Cmd(cmd) => match cmd {
                            FocaCmd::MembershipStates(sender) => {
                                for member in foca.iter_membership_state() {
                                    if let Err(e) = sender.send(member.clone()).await {
                                        error!("could not send back foca membership: {e}");
                                        break;
                                    }
                                }
                            }
                        },
                    },
                    Branch::HandleTimer(timer, seq) => {
                        handle_timer(&mut foca, &mut runtime, &mut timer_rx, timer, seq);
                    }
                    Branch::Metrics => {
                        trace!("handling Branch::Metrics");
                        {
                            gauge!("corro.gossip.members").set(foca.num_members() as f64);
                            gauge!("corro.gossip.member.states")
                                .set(foca.iter_membership_state().count() as f64);
                            gauge!("corro.gossip.updates_backlog")
                                .set(foca.updates_backlog() as f64);
                        }
                        {
                            let config = config.read();
                            gauge!("corro.gossip.config.max_transmissions")
                                .set(config.max_transmissions.get() as f64);
                            gauge!("corro.gossip.config.num_indirect_probes")
                                .set(config.num_indirect_probes.get() as f64);
                        }
                        gauge!("corro.gossip.cluster_size").set(last_cluster_size.get() as f64);
                    }
                    Branch::DiffMembers => {
                        diff_member_states(&agent, &foca, &mut last_states);
                    }
                }

                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    let to_s: &'static str = discriminant.into();
                    warn!("took {elapsed:?} to execute branch: {to_s}");
                }
            }

            info!("foca loop is done, leaving cluster");

            // leave the cluster gracefully
            if let Err(e) = foca.leave_cluster(&mut runtime) {
                error!("could not leave cluster: {e}");
            }

            let leave_deadline = tokio::time::sleep(Duration::from_secs(5));
            tokio::pin!(leave_deadline);

            let mut foca_done = false;
            let mut timer_done = false;

            loop {
                tokio::select! {
                    biased;
                    _ = &mut leave_deadline => {
                        break;
                    },
                    timer = timer_rx.recv(), if !timer_done => match timer {
                        Some((timer, seq)) => {
                            handle_timer(&mut foca, &mut runtime, &mut timer_rx, timer, seq);
                        },
                        None => {
                            timer_done = true;
                        }
                    },
                    input = rx_foca.recv(), if !foca_done => match input {
                        Some(FocaInput::Data(data)) => {
                            _ = foca.handle_data(&data, &mut runtime);
                        },
                        None => {
                            foca_done = true
                        }
                        _ => {

                        }
                    },
                    else => {
                        break;
                    }
                }
            }

            if let Some(handle) = diff_member_states(&agent, &foca, &mut last_states) {
                info!("Waiting on task to update member states...");
                if let Err(e) = handle.await {
                    error!("could not await task to update member states: {e}");
                }
            }
        }
    });

    tokio::spawn(async move {
        const BROADCAST_CUTOFF: usize = 64 * 1024;

        let mut bcast_codec = LengthDelimitedCodec::new();

        let mut bcast_buf = BytesMut::new();
        let mut local_bcast_buf = BytesMut::new();
        let mut single_bcast_buf = BytesMut::new();

        let mut metrics_interval = interval(Duration::from_secs(10));

        let mut rng = StdRng::from_entropy();

        let mut idle_pendings = FuturesUnordered::<
            Pin<Box<dyn Future<Output = PendingBroadcast> + Send + 'static>>,
        >::new();

        let mut bcast_interval = interval(Duration::from_millis(500));

        enum Branch {
            Broadcast(BroadcastInput),
            BroadcastTick,
            WokePendingBroadcast(PendingBroadcast),
            Tripped,
            Metrics,
        }

        let mut tripped = false;
        let mut ser_buf = BytesMut::new();

        let mut to_broadcast = vec![];

        loop {
            let branch = tokio::select! {
                biased;
                input = rx_bcast.recv() => match input {
                    Some(input) => {
                        Branch::Broadcast(input)
                    },
                    None => {
                        warn!("no more swim inputs");
                        break;
                    }
                },
                _ = bcast_interval.tick() => {
                    Branch::BroadcastTick
                },
                maybe_woke = idle_pendings.next(), if !idle_pendings.is_terminated() => match maybe_woke {
                    Some(woke) => Branch::WokePendingBroadcast(woke),
                    None => {
                        trace!("idle pendings returned None");
                        // I guess?
                        continue;
                    }
                },

                _ = &mut tripwire, if !tripped => {
                    tripped = true;
                    Branch::Tripped
                },
                _ = metrics_interval.tick() => {
                    Branch::Metrics
                }
            };

            match branch {
                Branch::Tripped => {
                    // nothing to do here, yet!
                }
                Branch::BroadcastTick => {
                    if !bcast_buf.is_empty() {
                        to_broadcast.push(PendingBroadcast::new(bcast_buf.split().freeze()));
                    }
                    if !local_bcast_buf.is_empty() {
                        to_broadcast.push(PendingBroadcast::new_local(
                            local_bcast_buf.split().freeze(),
                        ));
                    }
                }
                Branch::Broadcast(input) => {
                    trace!("handling Branch::Broadcast");
                    let (bcast, is_local) = match input {
                        BroadcastInput::Rebroadcast(bcast) => (bcast, false),
                        BroadcastInput::AddBroadcast(bcast) => (bcast, true),
                    };
                    trace!("adding broadcast: {bcast:?}, local? {is_local}");

                    if let Err(e) = UniPayload::V1(UniPayloadV1::Broadcast(bcast.clone()))
                        .write_to_stream((&mut ser_buf).writer())
                    {
                        error!("could not encode UniPayload::V1 Broadcast: {e}");
                        ser_buf.clear();
                        continue;
                    }
                    trace!("ser buf len: {}", ser_buf.len());

                    if is_local {
                        if let Err(e) =
                            bcast_codec.encode(ser_buf.split().freeze(), &mut single_bcast_buf)
                        {
                            error!("could not encode local broadcast: {e}");
                            single_bcast_buf.clear();
                            continue;
                        }

                        let payload = single_bcast_buf.split().freeze();

                        local_bcast_buf.extend_from_slice(&payload);

                        let members = agent.members().read();
                        for addr in members.ring0() {
                            // this spawns, so we won't be holding onto the read lock for long
                            tokio::spawn(transmit_broadcast(
                                payload.clone(),
                                transport.clone(),
                                addr,
                            ));
                        }

                        if local_bcast_buf.len() >= BROADCAST_CUTOFF {
                            to_broadcast.push(PendingBroadcast::new_local(
                                local_bcast_buf.split().freeze(),
                            ));
                        }
                    } else {
                        if let Err(e) = bcast_codec.encode(ser_buf.split().freeze(), &mut bcast_buf)
                        {
                            error!("could not encode broadcast: {e}");
                            bcast_buf.clear();
                            continue;
                        }

                        if bcast_buf.len() >= BROADCAST_CUTOFF {
                            to_broadcast.push(PendingBroadcast::new(bcast_buf.split().freeze()));
                        }
                    }
                }
                Branch::WokePendingBroadcast(pending) => {
                    trace!("handling Branch::WokePendingBroadcast");
                    to_broadcast.push(pending);
                }
                Branch::Metrics => {
                    trace!("handling Branch::Metrics");
                    gauge!("corro.broadcast.pending.count").set(idle_pendings.len() as f64);
                    gauge!("corro.broadcast.buffer.capacity").set(bcast_buf.capacity() as f64);
                    gauge!("corro.broadcast.serialization.buffer.capacity")
                        .set(ser_buf.capacity() as f64);
                }
            }

            for mut pending in to_broadcast.drain(..) {
                trace!("{} to broadcast: {pending:?}", actor_id);

                let (member_count, max_transmissions) = {
                    let config = config.read();
                    (
                        config.num_indirect_probes.get(),
                        config.max_transmissions.get(),
                    )
                };

                let broadcast_to = {
                    agent
                        .members()
                        .read()
                        .states
                        .iter()
                        .filter_map(|(member_id, state)| {
                            // don't broadcast to ourselves... or ring0 if local broadcast
                            if *member_id == actor_id || (pending.is_local && state.is_ring0()) {
                                None
                            } else {
                                Some(state.addr)
                            }
                        })
                        .choose_multiple(&mut rng, member_count)
                };

                for addr in broadcast_to {
                    debug!(actor = %actor_id, "broadcasting {} bytes to: {addr} (send count: {})", pending.payload.len(), pending.send_count);

                    tokio::spawn(transmit_broadcast(
                        pending.payload.clone(),
                        transport.clone(),
                        addr,
                    ));
                }

                pending.send_count = pending.send_count.wrapping_add(1);

                trace!(
                    "send_count: {}, max_transmissions: {max_transmissions}",
                    pending.send_count
                );

                if pending.send_count < max_transmissions {
                    debug!("queueing for re-send");
                    idle_pendings.push(Box::pin(async move {
                        // FIXME: calculate sleep duration based on send count
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        pending
                    }));
                }
            }
        }
        info!("broadcasts are done");
    });
}

fn diff_member_states(
    agent: &Agent,
    foca: &Foca<Actor, BincodeCodec<DefaultOptions>, StdRng, NoCustomBroadcast>,
    last_states: &mut HashMap<ActorId, (foca::Member<Actor>, Option<u64>)>,
) -> Option<tokio::task::JoinHandle<()>> {
    let mut foca_states = HashMap::new();
    for member in foca.iter_membership_state() {
        match foca_states.entry(member.id().id()) {
            Entry::Occupied(mut entry) => {
                let prev: &mut &foca::Member<Actor> = entry.get_mut();
                if prev.id().ts() < member.id().ts() {
                    *prev = member;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(member);
            }
        }
    }

    let members = agent.members().read();

    let to_update = foca_states
        .iter()
        .filter_map(|(id, member)| {
            let member = *member;
            let rtt = members
                .rtts
                .get(&member.id().addr())
                .and_then(|rtts| rtts.buf.iter().min().copied());
            match last_states.entry(*id) {
                Entry::Occupied(mut entry) => {
                    let (prev_member, prev_rtt) = entry.get();
                    if prev_member != member || *prev_rtt != rtt {
                        entry.insert((member.clone(), rtt));
                        Some((member.clone(), rtt))
                    } else {
                        None
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert((member.clone(), rtt));
                    Some((member.clone(), rtt))
                }
            }
        })
        .collect::<Vec<_>>();

    let mut to_delete = vec![];

    last_states.retain(|id, _v| {
        if foca_states.contains_key(id) {
            true
        } else {
            to_delete.push(*id);
            false
        }
    });

    if to_update.is_empty() && to_delete.is_empty() {
        return None;
    }

    info!(
        "Scheduling cluster membership state update for {} members (delete: {})",
        to_update.len(),
        to_delete.len()
    );

    let updated_at = time::OffsetDateTime::now_utc();

    let pool = agent.pool().clone();
    Some(tokio::spawn(async move {
        let mut conn = match pool.write_low().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("could not acquire a r/w conn to process member events: {e}");
                return;
            }
        };

        let mut upserted = 0;
        let mut deleted = 0;

        let res = block_in_place(|| {
            let tx = conn.immediate_transaction()?;

            for (member, rtt_min) in to_update {
                let foca_state = serde_json::to_string(&member).unwrap();

                upserted += tx
                    .prepare_cached(
                        "
                    INSERT INTO __corro_members (actor_id, address, foca_state, rtt_min, updated_at)
                        VALUES                  (?,        ?,       ?,          ?,       ?)
                        ON CONFLICT (actor_id)
                            DO UPDATE SET
                                foca_state = excluded.foca_state,
                                address = excluded.address,
                                rtt_min = CASE excluded.rtt_min WHEN NULL THEN rtt_min ELSE excluded.rtt_min END,
                                updated_at = excluded.updated_at
                            WHERE excluded.updated_at > updated_at
                ",
                    )?
                    .execute(params![
                        member.id().id(),
                        member.id().addr().to_string(),
                        foca_state,
                        rtt_min,
                        updated_at
                    ])?;
            }

            for id in to_delete {
                deleted += tx
                    .prepare_cached(
                        "DELETE FROM __corro_members WHERE actor_id = ? AND updated_at < ?",
                    )?
                    .execute(params![id, updated_at])?;
            }

            tx.commit()?;

            Ok::<_, rusqlite::Error>(())
        });

        if let Err(e) = res {
            error!("could not insert state changes from SWIM cluster into sqlite: {e}");
        }

        info!("Membership states changed! upserted: {upserted}, deleted {deleted}");
    }))
}

fn make_foca_config(cluster_size: NonZeroU32) -> foca::Config {
    let mut config = foca::Config::new_wan(cluster_size);
    config.remove_down_after = Duration::from_secs(2 * 24 * 60 * 60);

    // max payload size for udp datagrams, use a safe value here...
    // TODO: calculate from smallest max datagram size for all QUIC conns
    config.max_packet_size = 1178.try_into().unwrap();

    config
}

#[derive(Debug)]
struct PendingBroadcast {
    payload: Bytes,
    is_local: bool,
    send_count: u8,
}

impl PendingBroadcast {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            is_local: false,
            send_count: 0,
        }
    }

    pub fn new_local(payload: Bytes) -> Self {
        Self {
            payload,
            is_local: true,
            send_count: 0,
        }
    }
}

#[tracing::instrument(skip(payload, transport), fields(buf_size = payload.len()), level = "debug")]
async fn transmit_broadcast(payload: Bytes, transport: Transport, addr: SocketAddr) {
    trace!("singly broadcasting to {addr}");

    let len = payload.len();
    match tokio::time::timeout(Duration::from_secs(5), transport.send_uni(addr, payload)).await {
        Err(_e) => {
            warn!("timed out writing broadcast to uni stream");
        }
        Ok(Err(e)) => {
            error!("could not write to uni stream to {addr}: {e}");
        }
        Ok(Ok(_)) => {
            counter!("corro.peer.stream.bytes.sent.total", "type" => "uni").increment(len as u64);
        }
    }
}
