use std::{
    cmp,
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    net::SocketAddr,
    num::NonZeroU32,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bincode::DefaultOptions;
use bytes::{BufMut, Bytes, BytesMut};
use foca::{BincodeCodec, Foca, Identity, NoCustomBroadcast, Notification, Timer};
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Future,
};
use governor::{Quota, RateLimiter};
use metrics::{counter, gauge};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rusqlite::params;
use spawn::spawn_counted;
use speedy::Writable;
use strum::EnumDiscriminants;
use tokio::{
    sync::mpsc,
    task::{block_in_place, JoinSet, LocalSet},
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

use crate::{agent::util::log_at_pow_10, transport::Transport};

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
    rx_bcast: CorroReceiver<BroadcastInput>,
    to_send_tx: CorroSender<(Actor, Bytes)>,
    notifications_tx: CorroSender<Notification<Actor>>,
    tripwire: Tripwire,
) {
    debug!("starting runtime loop for actor: {actor:?}");
    let rng = StdRng::from_entropy();

    let config = Arc::new(RwLock::new(make_foca_config(1.try_into().unwrap())));

    let mut foca = Foca::with_custom_broadcast(
        actor,
        config.read().clone(),
        rng,
        foca::BincodeCodec(bincode::DefaultOptions::new()),
        NoCustomBroadcast,
    );

    let (to_schedule_tx, mut to_schedule_rx) =
        bounded(agent.config().perf.schedule_channel_len, "to_schedule");

    let mut runtime: DispatchRuntime<Actor> =
        DispatchRuntime::new(to_send_tx, to_schedule_tx, notifications_tx);

    let (timer_tx, mut timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(timer_tx);

    tokio::spawn(async move {
        while let Some((duration, timer)) = to_schedule_rx.recv().await {
            timer_spawner.spawn((duration, timer));
        }
    });

    let cluster_size = Arc::new(AtomicU32::new(1));

    // foca SWIM operations loop.
    // NOTE: every turn of that loop should be fast or else we risk being a down suspect
    spawn_counted({
        let config = config.clone();
        let agent = agent.clone();
        let cluster_size = cluster_size.clone();
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
                        info!("tripped runtime loop, breaking");
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

                            cluster_size.store(size.get(), Ordering::Release);
                        }
                        FocaInput::ApplyMany(updates) => {
                            trace!("handling FocaInput::ApplyMany");
                            if let Err(e) = foca.apply_many(updates.into_iter(), &mut runtime) {
                                error!("foca apply_many error: {e}");
                            }
                        }
                        FocaInput::Cmd(cmd) => match cmd {
                            FocaCmd::Rejoin(callback) => {
                                let renewed = foca.identity().renew().unwrap();
                                debug!("handling FocaInput::Rejoin {renewed:?}");

                                let new_id = foca.change_identity(renewed, &mut runtime);
                                info!("New identity: {new_id:?}");

                                if callback.send(new_id).is_err() {
                                    warn!("could not send back result after rejoining cluster");
                                }
                            }
                            FocaCmd::MembershipStates(sender) => {
                                for member in foca.iter_membership_state() {
                                    if let Err(e) = sender.send(member.clone()).await {
                                        error!("could not send back foca membership: {e}");
                                        break;
                                    }
                                }
                            }
                            FocaCmd::ChangeIdentity(id, callback) => {
                                if callback
                                    .send(foca.change_identity(id, &mut runtime))
                                    .is_err()
                                {
                                    warn!("could not send back result after changing identity");
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
            info!("foca runtime loop is done, leaving cluster");
        }
    });

    tokio::spawn(handle_broadcasts(
        agent,
        rx_bcast,
        transport,
        config,
        tripwire,
        Default::default(),
    ));
}

type BroadcastRateLimiter = RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::QuantaClock,
    governor::middleware::StateInformationMiddleware,
>;

#[derive(Debug, Clone, Copy)]
pub struct BroadcastOpts {
    pub interval: Duration,
    pub bcast_cutoff: usize,
}

impl Default for BroadcastOpts {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            bcast_cutoff: 64 * 1024,
        }
    }
}

async fn handle_broadcasts(
    agent: Agent,
    mut rx_bcast: CorroReceiver<BroadcastInput>,
    transport: Transport,
    config: Arc<RwLock<foca::Config>>,
    mut tripwire: Tripwire,
    opts: BroadcastOpts,
) {
    let actor_id = agent.actor_id();

    // max broadcast size
    let broadcast_cutoff: usize = opts.bcast_cutoff;

    let mut bcast_codec = LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();

    let mut bcast_buf = BytesMut::new();
    let mut local_bcast_buf = BytesMut::new();
    let mut single_bcast_buf = BytesMut::new();

    let mut metrics_interval = interval(Duration::from_secs(10));

    let mut rng = StdRng::from_entropy();

    let mut idle_pendings =
        FuturesUnordered::<Pin<Box<dyn Future<Output = PendingBroadcast> + Send + 'static>>>::new();

    let mut bcast_interval = interval(opts.interval);

    enum Branch {
        Broadcast(BroadcastInput),
        BroadcastDeadline,
        WokePendingBroadcast(PendingBroadcast),
        Tripped,
        Metrics,
    }

    let mut tripped = false;
    let mut ser_buf = BytesMut::new();

    let mut join_set = JoinSet::new();
    let max_queue_len = agent.config().perf.processing_queue_len;
    const MAX_INFLIGHT_BROADCAST: usize = 500;
    let mut to_broadcast = VecDeque::new();
    let mut to_local_broadcast = VecDeque::new();
    let mut log_count = 0;

    let mut limited_log_count = 0;

    let bytes_per_sec: BroadcastRateLimiter = RateLimiter::direct(Quota::per_second(unsafe {
        NonZeroU32::new_unchecked(10 * 1024 * 1024)
    }))
    .with_middleware();

    let mut rate_limited = false;

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
            _ = join_set.join_next(), if !join_set.is_empty() => {
                // drains the joinset
                continue;
            },
            _ = bcast_interval.tick() => {
                Branch::BroadcastDeadline
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
                warn!("tripped broadcast loop");
                break;
            }
            Branch::BroadcastDeadline => {
                if !bcast_buf.is_empty() {
                    to_broadcast.push_front(PendingBroadcast::new(bcast_buf.split().freeze()));
                }
                if !local_bcast_buf.is_empty() {
                    to_broadcast.push_front(PendingBroadcast::new_local(
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

                if let Err(e) = (UniPayload::V1 {
                    data: UniPayloadV1::Broadcast(bcast.clone()),
                    cluster_id: agent.cluster_id(),
                })
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

                    to_local_broadcast.push_front(payload);

                    if local_bcast_buf.len() >= broadcast_cutoff {
                        to_broadcast.push_front(PendingBroadcast::new_local(
                            local_bcast_buf.split().freeze(),
                        ));
                    }
                } else {
                    if let Err(e) = bcast_codec.encode(ser_buf.split().freeze(), &mut bcast_buf) {
                        error!("could not encode broadcast: {e}");
                        bcast_buf.clear();
                        continue;
                    }

                    if bcast_buf.len() >= broadcast_cutoff {
                        to_broadcast.push_front(PendingBroadcast::new(bcast_buf.split().freeze()));
                    }
                }
            }
            Branch::WokePendingBroadcast(pending) => {
                trace!("handling Branch::WokePendingBroadcast");
                to_broadcast.push_front(pending);
            }
            Branch::Metrics => {
                trace!("handling Branch::Metrics");
                gauge!("corro.broadcast.pending.count").set(idle_pendings.len() as f64);
                gauge!("corro.broadcast.processing.jobs").set(join_set.len() as f64);
                gauge!("corro.broadcast.buffer.capacity").set(bcast_buf.capacity() as f64);
                gauge!("corro.broadcast.serialization.buffer.capacity")
                    .set(ser_buf.capacity() as f64);
            }
        }

        let prev_rate_limited = rate_limited;

        // start with local broadcasts, they're higher priority
        let mut ring0 = HashSet::new();
        while !to_local_broadcast.is_empty() && join_set.len() < MAX_INFLIGHT_BROADCAST {
            // UNWRAP: we just checked that it wasn't empty
            let payload = to_local_broadcast.pop_front().unwrap();

            let members = agent.members().read();
            let mut spawn_count = 0;
            let mut ring0_count = 0;
            for addr in members.ring0(agent.cluster_id()) {
                if join_set.len() >= MAX_INFLIGHT_BROADCAST {
                    debug!(
                        "breaking, max inflight broadcast reached: {}",
                        MAX_INFLIGHT_BROADCAST
                    );
                    break;
                }
                ring0_count += 1;
                ring0.insert(addr);

                match try_transmit_broadcast(
                    &bytes_per_sec,
                    payload.clone(),
                    transport.clone(),
                    addr,
                ) {
                    Err(e) => {
                        log_at_pow_10(
                            "could not spawn broadcast transmission: {e}",
                            &mut limited_log_count,
                        );
                        match e {
                            TransmitError::TooBig(_) | TransmitError::InsufficientCapacity(_) => {
                                // not sure this would ever happen
                                error!("could not spawn broadcast transmission: {e}");
                                continue;
                            }
                            TransmitError::QuotaExceeded(_) => {
                                // exceeded our quota, stop trying to send this through
                                rate_limited = true;
                                counter!("corro.broadcast.rate_limited").increment(1);
                                log_at_pow_10("broadcasts rate limited", &mut limited_log_count);
                                break;
                            }
                        }
                    }
                    Ok(fut) => {
                        join_set.spawn(fut);
                        spawn_count += 1;
                    }
                }
            }

            // couldn't send it anywhere!
            if rate_limited && spawn_count == 0 && ring0_count > 0 {
                // push it back in front since this got nowhere and it's still the
                // freshest item we have in the queue
                to_local_broadcast.push_front(payload);
                break;
            }

            counter!("corro.broadcast.spawn", "type" => "local").increment(spawn_count);
        }

        if !rate_limited && !to_broadcast.is_empty() && join_set.len() < MAX_INFLIGHT_BROADCAST {
            let (members_count, ring0_count) = {
                let members = agent.members().read();
                let members_count = members.states.len();
                let ring0_count = members.ring0(agent.cluster_id()).count();
                (members_count, ring0_count)
            };

            let (choose_count, max_transmissions) = {
                let config = config.read();
                let max_transmissions = config.max_transmissions.get();
                let dynamic_count =
                    (members_count - ring0_count) / (max_transmissions as usize * 10);
                let count = cmp::max(config.num_indirect_probes.get(), dynamic_count);

                if prev_rate_limited {
                    // we've been rate limited on the last loop, try sending to less nodes...
                    (cmp::min(count, dynamic_count / 2), max_transmissions / 2)
                } else {
                    (count, max_transmissions)
                }
            };

            debug!(
                "choosing {} broadcasts, ring0 count: {}, MAX_INFLIGHT_BROADCAST: {}",
                choose_count, ring0_count, MAX_INFLIGHT_BROADCAST
            );
            while !to_broadcast.is_empty() && join_set.len() < MAX_INFLIGHT_BROADCAST {
                let mut pending = to_broadcast.pop_front().unwrap();

                let broadcast_to = {
                    agent
                        .members()
                        .read()
                        .states
                        .iter()
                        .filter_map(|(member_id, state)| {
                            // don't broadcast to ourselves... or ring0 if local broadcast
                            // (ring0 could have changed since the time we sent the local broadcast
                            // so we check the ring0 variable that's created at start of local_broacast
                            // instead of state.is_ring0())
                            if *member_id == actor_id
                                || state.cluster_id != agent.cluster_id()
                                || (pending.is_local && ring0.contains(&state.addr))
                                || pending.sent_to.contains(&state.addr)
                            // don't broadcast to this peer
                            {
                                None
                            } else {
                                Some(state.addr)
                            }
                        })
                        .choose_multiple(
                            &mut rng,
                            // prevent going over max count
                            cmp::min(
                                choose_count,
                                MAX_INFLIGHT_BROADCAST.saturating_sub(join_set.len()),
                            ),
                        )
                };

                let pending_sent_instance = pending.sent_to.len();

                let mut spawn_count = 0;
                trace!("broadcasting to: {:?}", broadcast_to);
                for addr in broadcast_to {
                    match try_transmit_broadcast(
                        &bytes_per_sec,
                        pending.payload.clone(),
                        transport.clone(),
                        addr,
                    ) {
                        Err(e) => {
                            warn!("could not spawn broadcast transmission: {e}");
                            match e {
                                TransmitError::TooBig(_)
                                | TransmitError::InsufficientCapacity(_) => {
                                    // not sure this would ever happen
                                    continue;
                                }
                                TransmitError::QuotaExceeded(_) => {
                                    // exceeded our quota, stop trying to send this through
                                    counter!("corro.broadcast.rate_limited").increment(1);
                                    log_at_pow_10(
                                        "broadcasts rate limited",
                                        &mut limited_log_count,
                                    );
                                    break;
                                }
                            }
                        }
                        Ok(fut) => {
                            debug!(actor = %actor_id, "broadcasting {} bytes to: {addr}", pending.payload.len());
                            join_set.spawn(fut);
                            pending.sent_to.insert(addr);
                            spawn_count += 1;
                        }
                    }
                }

                counter!("corro.broadcast.spawn", "type" => "global").increment(spawn_count);

                if pending_sent_instance != pending.sent_to.len() {
                    // we've sent this to at least 1 member...

                    if let Some(send_count) = pending.send_count.checked_add(1) {
                        trace!("send_count: {send_count}, max_transmissions: {max_transmissions}");
                        pending.send_count = send_count;

                        if send_count < max_transmissions {
                            debug!("queueing for re-send");
                            idle_pendings.push(Box::pin(async move {
                                // slow our send pace if we've been previously rate limited
                                let sleep_ms_base = if prev_rate_limited { 500 } else { 100 };
                                // send with increasing latency as we've already sent the updates out
                                tokio::time::sleep(Duration::from_millis(
                                    sleep_ms_base * send_count as u64,
                                ))
                                .await;
                                pending
                            }));
                        }
                    }
                }
            }
        }

        if drop_oldest_broadcast(&mut to_broadcast, &mut to_local_broadcast, max_queue_len)
            .is_some()
        {
            log_at_pow_10("dropped old change from broadcast queue", &mut log_count);
            counter!("corro.broadcast.dropped").increment(1);
        }
    }

    info!("broadcasts are done");
}

// Drop the oldest, most sent item or the oldest local item
fn drop_oldest_broadcast(
    queue: &mut VecDeque<PendingBroadcast>,
    local_queue: &mut VecDeque<Bytes>,
    max: usize,
) -> Option<PendingBroadcast> {
    if queue.len() + local_queue.len() > max {
        // start by dropping from global queue
        let max_sent: Option<(_, _)> = queue
            .iter()
            .enumerate()
            .max_by_key(|(_, val)| val.send_count);
        return if let Some((i, _)) = max_sent {
            queue.remove(i)
        } else {
            local_queue.pop_back().map(PendingBroadcast::new_local)
        };
    }

    None
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
                    let member_differed = prev_member != member;
                    let rtt_differed = *prev_rtt != rtt;
                    if member_differed || rtt_differed {
                        debug!("member differed? {member_differed}, rtt differed? {rtt_differed} (prev: {prev_rtt:?}, new: {rtt:?})");
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
    config.remove_down_after = Duration::from_secs(2 * 24 * 3600);

    // max payload size for udp datagrams, use a safe value here...
    // TODO: calculate from smallest max datagram size for all QUIC conns
    config.max_packet_size = 1178.try_into().unwrap();

    config
}

#[derive(Debug)]
struct PendingBroadcast {
    payload: Bytes,
    is_local: bool,
    sent_to: HashSet<SocketAddr>,
    send_count: u8,
}

impl PendingBroadcast {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            is_local: false,
            sent_to: Default::default(),
            send_count: 0,
        }
    }

    pub fn new_local(payload: Bytes) -> Self {
        Self {
            payload,
            is_local: true,
            sent_to: Default::default(),
            send_count: 0,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum TransmitError {
    #[error("payload > u32::MAX: {0}")]
    TooBig(usize),
    #[error(transparent)]
    InsufficientCapacity(#[from] governor::InsufficientCapacity),
    #[error("{0}")]
    QuotaExceeded(governor::NotUntil<governor::clock::QuantaInstant>),
}

#[tracing::instrument(skip(payload, transport), fields(buf_size = payload.len()), level = "debug")]
fn try_transmit_broadcast(
    bytes_per_sec: &BroadcastRateLimiter,
    payload: Bytes,
    transport: Transport,
    addr: SocketAddr,
) -> Result<Pin<Box<dyn Future<Output = ()> + Send>>, TransmitError> {
    trace!("singly broadcasting to {addr}");

    let len = payload.len();

    let len_u32 = match len.try_into().ok().and_then(NonZeroU32::new) {
        Some(len) => len,
        None => {
            return Err(TransmitError::TooBig(len));
        }
    };

    match bytes_per_sec.check_n(len_u32) {
        Ok(Ok(state)) => {
            gauge!("corro.broadcast.limiter.remaining_burst")
                .set(state.remaining_burst_capacity() as f64);
        }
        Ok(Err(e)) => return Err(TransmitError::QuotaExceeded(e)),
        Err(e) => return Err(e.into()),
    }

    Ok(Box::pin(async move {
        match tokio::time::timeout(Duration::from_secs(5), transport.send_uni(addr, payload)).await
        {
            Err(_e) => {
                warn!("timed out writing broadcast to uni stream {:?}", addr);
            }
            Ok(Err(e)) => {
                error!("could not write to uni stream to {addr}: {e}");
            }
            Ok(Ok(_)) => {
                counter!("corro.peer.stream.bytes.sent.total", "type" => "uni")
                    .increment(len as u64);
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::spawn_unipayload_handler;
    use corro_tests::launch_test_agent;
    use corro_types::{
        base::{CrsqlDbVersion, CrsqlSeq},
        broadcast::{BroadcastV1, ChangeV1, Changeset},
    };
    use uuid::Uuid;

    #[test]
    fn test_behaviour_when_queue_is_full() -> eyre::Result<()> {
        let max = 4;
        let mut queue = VecDeque::new();
        let mut local_queue = VecDeque::new();

        assert!(drop_oldest_broadcast(&mut queue, &mut local_queue, max).is_none());

        queue.push_front(build_broadcast(1, 0));
        queue.push_front(build_broadcast(2, 3));
        queue.push_front(build_broadcast(3, 1));
        queue.push_front(build_broadcast(4, 1));
        queue.push_front(build_broadcast(5, 2));
        queue.push_front(build_broadcast(6, 1));
        queue.push_front(build_broadcast(7, 3));
        queue.push_front(build_broadcast(8, 0));

        // drop oldest item with highest send count
        let dropped = drop_oldest_broadcast(&mut queue, &mut local_queue, max).unwrap();
        assert_eq!(dropped.send_count, 3);
        assert_eq!(2_i64.to_be_bytes(), dropped.payload.as_ref());

        let dropped = drop_oldest_broadcast(&mut queue, &mut local_queue, max).unwrap();
        assert_eq!(dropped.send_count, 3);
        assert_eq!(7_i64.to_be_bytes(), dropped.payload.as_ref());

        let dropped = drop_oldest_broadcast(&mut queue, &mut local_queue, max).unwrap();
        assert_eq!(dropped.send_count, 2);
        assert_eq!(5_i64.to_be_bytes(), dropped.payload.as_ref());

        let dropped = drop_oldest_broadcast(&mut queue, &mut local_queue, max).unwrap();
        assert_eq!(dropped.send_count, 1);
        assert_eq!(3_i64.to_be_bytes(), dropped.payload.as_ref());

        // queue is still at max now, no item gets dropped
        assert!(drop_oldest_broadcast(&mut queue, &mut local_queue, max).is_none());

        Ok(())
    }

    fn build_broadcast(id: u64, send_count: u8) -> PendingBroadcast {
        PendingBroadcast {
            payload: Bytes::copy_from_slice(&id.to_be_bytes()),
            is_local: false,
            send_count,
            sent_to: HashSet::new(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_broadcast_order() -> eyre::Result<()> {
        let _ = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

        let (tx_bcast, rx_bcast) = bounded(100, "bcast");
        let (tx_rtt, _) = mpsc::channel(100);

        let config = Arc::new(RwLock::new(make_foca_config(1.try_into().unwrap())));
        let transport = Transport::new(&ta1.config.gossip, tx_rtt).await?;

        let server_config = quinn_plaintext::server_config();
        let endpoint = quinn::Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap())?;
        let ta2_gossip_addr = endpoint.local_addr()?;
        println!("listening on {ta2_gossip_addr}");

        let ta2_actor = Actor::new(
            ActorId(Uuid::new_v4()),
            ta2_gossip_addr,
            Default::default(),
            ta1.agent.cluster_id(),
        );
        ta1.agent.members().write().add_member(&ta2_actor);

        let bcast = BroadcastV1::Change(ChangeV1 {
            actor_id: ta1.agent.actor_id(),
            changeset: Changeset::Full {
                version: CrsqlDbVersion(0),
                changes: vec![],
                seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                last_seq: CrsqlSeq(0),
                ts: Default::default(),
            },
        });
        let mut ser_buf = BytesMut::new();
        let _ = UniPayload::V1 {
            data: UniPayloadV1::Broadcast(bcast),
            cluster_id: ta1.agent.cluster_id(),
        }
        .write_to_stream((&mut ser_buf).writer())?;
        let estimated_size = ser_buf.len();

        tokio::spawn(handle_broadcasts(
            ta1.agent.clone(),
            rx_bcast,
            transport,
            config,
            tripwire.clone(),
            BroadcastOpts {
                interval: Duration::from_secs(2),
                bcast_cutoff: 5 * estimated_size,
            },
        ));

        let actor_id = ta1.agent.actor_id();
        for i in 0..5 {
            tx_bcast
                .send(BroadcastInput::Rebroadcast(BroadcastV1::Change(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Full {
                        version: CrsqlDbVersion(i),
                        changes: vec![],
                        seqs: CrsqlSeq(0)..=CrsqlSeq(0),
                        last_seq: CrsqlSeq(0),
                        ts: Default::default(),
                    },
                })))
                .await?;
        }

        if let Some(conn) = endpoint.accept().await {
            info!("accepting connection");
            let conn = conn.await.unwrap();

            let (tx_changes, mut rx_changes) = bounded(100, "changes");
            spawn_unipayload_handler(&tripwire, &conn, ta1.agent.cluster_id(), tx_changes);

            // we should receive five items starting from the biggest version
            for i in (0..5).rev() {
                let changes = tokio::time::timeout(Duration::from_secs(5), rx_changes.recv())
                    .await?
                    .unwrap();
                assert_eq!(changes.0.versions(), CrsqlDbVersion(i)..=CrsqlDbVersion(i));
            }
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        spawn::wait_for_all_pending_handles().await;

        Ok(())
    }
}
