use std::{
    cmp,
    collections::{hash_map::Entry, HashMap, HashSet, VecDeque},
    net::SocketAddr,
    num::{NonZeroU32, NonZeroUsize},
    pin::Pin,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use foca::{BincodeCodec, Foca, Identity, Member, NoCustomBroadcast, OwnedNotification, Timer};
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
use rand::seq::{IndexedRandom, SliceRandom};
use tokio_stream::StreamExt;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{debug, error, log::info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};

use corro_types::{
    actor::{Actor, ActorId},
    agent::Agent,
    broadcast::{
        BroadcastInput, DispatchRuntime, FocaCmd, FocaInput, PlumtreeMsg, UniPayload, UniPayloadV1,
    },
    channel::{bounded, CorroReceiver, CorroSender},
    sqlite::unnest_param,
};

use crate::{
    agent::util::log_at_pow_10,
    transport::{Transport, TransportExt},
};
use plum_foca::Payload;

#[derive(Clone)]
struct TimerSpawner<T: Send + 'static> {
    send: mpsc::UnboundedSender<(Duration, T)>,
}

impl<T: Send + 'static> TimerSpawner<T> {
    pub fn new(timer_tx: mpsc::Sender<(T, Instant)>) -> Self {
        let (send, mut recv) = mpsc::unbounded_channel::<(Duration, T)>();

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

    pub fn spawn(&self, task: (Duration, T)) {
        self.send
            .send(task)
            .expect("Thread with LocalSet has shut down.");
    }
}

fn handle_timer(
    foca: &mut Foca<Actor, BincodeCodec<bincode::config::Configuration>, StdRng, NoCustomBroadcast>,
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
    notifications_tx: CorroSender<OwnedNotification<Actor>>,
    tripwire: Tripwire,
    member_states: Vec<(SocketAddr, Member<Actor>)>,
) {
    debug!("starting runtime loop for actor: {actor:?}");
    let rng = StdRng::from_os_rng();

    let max_mtu = agent.config().gossip.max_mtu;
    let config = Arc::new(RwLock::new(make_foca_config(
        1.try_into().unwrap(),
        max_mtu,
    )));

    let mut foca = Foca::with_custom_broadcast(
        actor,
        config.read().clone(),
        rng,
        foca::BincodeCodec(bincode::config::Configuration::default()),
        NoCustomBroadcast,
    );

    let (to_schedule_tx, mut to_schedule_rx) =
        bounded(agent.config().perf.schedule_channel_len, "to_schedule");

    let mut runtime: DispatchRuntime<Actor> =
        DispatchRuntime::new(to_send_tx, to_schedule_tx, notifications_tx.clone());

    let (timer_tx, mut timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(timer_tx);

    {
        let mut tripwire = tripwire.clone();
        spawn_counted(async move {
            while let Outcome::Completed(Some((duration, timer))) =
                to_schedule_rx.recv().preemptible(&mut tripwire).await
            {
                timer_spawner.spawn((duration, timer));
            }
        });
    }

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

            let mut last_states = member_states
                .into_iter()
                .map(|(_, member)| (member.id().id(), (member, None)))
                .collect::<HashMap<_, _>>();
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
                                let new_config = make_foca_config(size, max_mtu);
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
                            if let Err(e) = foca.apply_many(updates.into_iter(), true, &mut runtime)
                            {
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
                        diff_member_states(&agent, &foca, &mut last_states, &notifications_tx);
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

            if let Some(handle) =
                diff_member_states(&agent, &foca, &mut last_states, &notifications_tx)
            {
                info!("Waiting on task to update member states...");
                if let Err(e) = handle.await {
                    error!("could not await task to update member states: {e}");
                }
            }
            info!("foca runtime loop is done, leaving cluster");
        }
    });

    spawn_counted(handle_broadcasts(
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
    transport: impl TransportExt + Clone + Send + 'static,
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

    let mut rng = StdRng::from_os_rng();

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
        rate_limited = false;

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
    foca: &Foca<Actor, BincodeCodec<bincode::config::Configuration>, StdRng, NoCustomBroadcast>,
    last_states: &mut HashMap<ActorId, (foca::Member<Actor>, Option<u64>)>,
    notifications_tx: &CorroSender<foca::OwnedNotification<Actor>>,
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
    let member_id = agent.config().gossip.member_id;

    let to_update = foca_states
        .iter()
        .filter_map(|(id, member)| {
            let member = *member;

            if member.id().member_id() != member_id {
                return None;
            }

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
        let foca_state = foca_states.get(id);
        if foca_state.is_some() && foca_state.unwrap().id().member_id() == member_id {
            true
        } else {
            to_delete.push(*id);
            false
        }
    });

    let mut foca_notifications = vec![];
    foca_notifications.extend(foca_states.iter().filter_map(|(id, member)| {
        let member = *member;
        if foca_state_is_active(&member.state()) && members.get(id).is_none() {
            Some(foca::OwnedNotification::MemberUp(member.id().clone()))
        } else {
            None
        }
    }));

    foca_notifications.extend(members.states.iter().filter_map(|(id, member_state)| {
        match foca_states.get(id) {
            Some(foca_state) => {
                if !foca_state_is_active(&foca_state.state()) {
                    Some(foca::OwnedNotification::MemberDown(foca_state.id().clone()))
                } else {
                    None
                }
            }
            None => Some(foca::OwnedNotification::MemberDown(
                member_state.to_actor(*id),
            )),
        }
    }));

    drop(members);

    if !foca_notifications.is_empty() {
        info!(
            "Sending out {} foca notifications for members",
            foca_notifications.len()
        );
        // best effort update since we'd retry this function.
        for notification in foca_notifications {
            if let Err(e) = notifications_tx.try_send(notification) {
                error!("error dispatching notifications from diff_member_states: {e}");
                counter!("corro.channel.error", "type" => "full", "name" => "dispatch.notifications")
                    .increment(1);
            }
        }
    }

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

            upserted += tx
                .prepare_cached(
                    "
                INSERT INTO __corro_members (actor_id, address, foca_state, rtt_min, updated_at)
                    SELECT                   value0,   value1,  value2,     value3, value4
                    FROM              unnest(?,        ?,       ?,          ?,       ?)
                    -- Otherwise sqlite will think ON CONFLICT is part of a JOIN
                    WHERE true
                    ON CONFLICT (actor_id)
                        DO UPDATE SET
                            foca_state = excluded.foca_state,
                            address = excluded.address,
                            rtt_min = COALESCE(excluded.rtt_min, rtt_min),
                            updated_at = excluded.updated_at
                        WHERE excluded.updated_at > updated_at
            ",
                )?
                .execute(params![
                    unnest_param(to_update.iter().map(|(member, _)| member.id().id())),
                    unnest_param(
                        to_update
                            .iter()
                            .map(|(member, _)| member.id().addr().to_string())
                    ),
                    unnest_param(
                        to_update
                            .iter()
                            .map(|(member, _)| serde_json::to_string(&member).unwrap())
                    ),
                    unnest_param(to_update.iter().map(|(_, rtt_min)| rtt_min)),
                    unnest_param(to_update.iter().map(|_| updated_at)),
                ])?;

            deleted += tx
                .prepare_cached(
                    r#"DELETE FROM __corro_members 
                            WHERE actor_id IN (SELECT value0 FROM UNNEST(?)) 
                            AND updated_at < ?
                        "#,
                )?
                .execute(params![unnest_param(to_delete.iter()), updated_at])?;

            tx.commit()?;

            Ok::<_, rusqlite::Error>(())
        });

        if let Err(e) = res {
            error!("could not insert state changes from SWIM cluster into sqlite: {e}");
        }

        info!("Membership states changed! upserted: {upserted}, deleted {deleted}");
    }))
}

fn foca_state_is_active(state: &foca::State) -> bool {
    matches!(*state, foca::State::Alive | foca::State::Suspect)
}

const QUIC_SHORT_HEADER_OVERHEAD: u16 = 13;

fn make_foca_config(cluster_size: NonZeroU32, max_mtu: Option<u16>) -> foca::Config {
    let mut config = foca::Config::new_wan(cluster_size);
    config.remove_down_after = Duration::from_secs(2 * 24 * 3600);

    config.max_packet_size = max_mtu
        .and_then(|mtu| mtu.checked_sub(QUIC_SHORT_HEADER_OVERHEAD))
        .and_then(|size| NonZeroUsize::new(size as usize))
        .unwrap_or_else(|| NonZeroUsize::new(1178).unwrap());

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
    transport: impl TransportExt + Send + 'static,
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
                counter!("corro.peer.stream.bytes.sent.total", "traffic" => "broadcast")
                    .increment(len as u64);
            }
        }
    }))
}

use corro_types::broadcast::{ChangeId, ChangeSource, ChangeV1, PlumtreeInput, PlumtreeMsgV1};
use plum_foca::PlumtreeState;

/// Implements `plum_foca::Runtime` for Corrosion, bridging the generic protocol
/// to Corrosion's transport, change processing, and timer infrastructure.
struct CorrosionPlumtreeRuntime<T: TransportExt> {
    agent: Agent,
    transport: T,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId>>,
    bcast_codec: LengthDelimitedCodec,
    ser_buf: BytesMut,
}

impl<T: TransportExt> CorrosionPlumtreeRuntime<T> {
    fn resolve_addr(&self, peer: &ActorId) -> Option<std::net::SocketAddr> {
        let members = self.agent.members().read();
        members.states.get(peer).map(|m| m.addr)
    }
}

impl<T: TransportExt + Clone + Send + 'static> plum_foca::Runtime<ChangeId, ChangeV1, ActorId>
    for CorrosionPlumtreeRuntime<T>
{
    fn send(&mut self, to: ActorId, msg: PlumtreeMsgV1) {
        let addr = match self.resolve_addr(&to) {
            Some(a) => a,
            None => {
                debug!("plumtree: no address for peer {to}, dropping message");
                return;
            }
        };

        let payload = UniPayload::V1 {
            data: UniPayloadV1::PlumTree(PlumtreeMsg::V1 { data: msg }),
            cluster_id: self.agent.cluster_id(),
        };

        self.ser_buf.clear();
        if let Err(e) = payload.write_to_stream((&mut self.ser_buf).writer()) {
            error!("plumtree: failed to serialize wire msg: {e}");
            return;
        }

        let mut frame_buf = BytesMut::new();
        if let Err(e) = self
            .bcast_codec
            .encode(self.ser_buf.split().freeze(), &mut frame_buf)
        {
            error!("plumtree: failed to frame wire msg: {e}");
            return;
        }

        let data = frame_buf.freeze();
        let transport = self.transport.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(5), transport.send_uni(addr, data)).await
            {
                Err(_) => warn!("plumtree: timed out sending to {addr}"),
                Ok(Err(e)) => debug!("plumtree: send error to {addr}: {e}"),
                Ok(Ok(())) => {
                    counter!("corro.plumtree.send.total").increment(1);
                }
            }
        });
    }

    fn deliver(
        &mut self,
        _id: ChangeId,
        payload: ChangeV1,
        _sender: ActorId,
        _round: plum_foca::Round,
    ) {
        let tx = self.tx_changes.clone();
        tokio::spawn(async move {
            if let Err(e) = tx.send((payload, ChangeSource::Broadcast)).await {
                error!("plumtree: could not deliver change: {e}");
            }
        });
    }

    fn schedule(&mut self, timer: plum_foca::Timer<ChangeId>, after: Duration) {
        self.timer_spawner.spawn((after, timer));
    }

    fn notify(&mut self, notification: plum_foca::Notification<ChangeId, ActorId>) {
        trace!("plumtree notification: {notification:?}");
    }
}

/// The main Plumtree event loop. Runs alongside the existing `handle_broadcasts`.
pub async fn plumtree_loop<T: TransportExt + Clone + Send + 'static>(
    agent: Agent,
    transport: T,
    mut rx_plumtree: CorroReceiver<PlumtreeInput>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    mut tripwire: Tripwire,
    stats: Arc<(AtomicU64, AtomicU64, AtomicU64, AtomicU64)>,
) {
    let config = plum_foca::Config {
        ihave_timeout: Duration::from_millis(500),
        optimization_threshold: 5,
        max_cached_payloads: 4096,
        max_eager: 5,
        max_lazy: 10,
        prune_threshold: 2,
    };

    let mut state: PlumtreeState<ChangeId, ChangeV1, ActorId> =
        PlumtreeState::new(agent.actor_id(), config);

    // Seed eager set with current ring0 members
    {
        let mut actors = agent.members().read().states.keys().cloned().collect::<Vec<_>>();
        let mut rng = StdRng::from_os_rng();
        actors.shuffle(&mut rng);
        for actor_id in actors {
            if actor_id != agent.actor_id() {
                state.peer_up(actor_id);
            }
        }
        let (eager, lazy) = (state.eager_peers().len(), state.lazy_peers().len());
        info!("plumtree: seeded with {eager} eager + {lazy} lazy peers from existing members");
    }

    let (plumtree_timer_tx, mut plumtree_timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(plumtree_timer_tx);

    let mut tick_interval = interval(Duration::from_millis(200));
    // do we need this if we aren't batching?
    let bcast_codec = LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();

    let mut rt = CorrosionPlumtreeRuntime {
        agent: agent,
        transport: transport,
        tx_changes: tx_changes,
        timer_spawner: timer_spawner,
        bcast_codec: bcast_codec,
        ser_buf: BytesMut::with_capacity(10 * 1_024 * 1_024),
    };

    loop {
        tokio::select! {
            biased;
            _ = &mut tripwire => {
                info!("plumtree_loop: tripwire fired, shutting down");
                break;
            }
            input = rx_plumtree.recv() => match input {
                Some(PlumtreeInput::Wire(msg)) => {
                    match msg {
                        PlumtreeMsgV1::Gossip(g) => {
                            state.handle_gossip(g, &mut rt);
                            stats.0.fetch_add(1, Ordering::Relaxed);
                        }
                        PlumtreeMsgV1::IHave(ih) => {
                            state.handle_ihave(ih, &mut rt);
                            stats.1.fetch_add(1, Ordering::Relaxed);
                        }
                        PlumtreeMsgV1::Graft(g) => {
                            state.handle_graft(g, &mut rt);
                            stats.2.fetch_add(1, Ordering::Relaxed);
                        }
                        PlumtreeMsgV1::Prune(p) => { state.handle_prune(p, &mut rt);
                            stats.3.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Some(PlumtreeInput::MemberUp(actor_id)) => {
                    state.peer_up(actor_id);
                }
                Some(PlumtreeInput::MemberDown(actor_id)) => {
                    state.peer_down(&actor_id, &mut rt);
                }
                Some(PlumtreeInput::Broadcast(change)) => {
                    let id = change.message_id();
                    state.broadcast(id, change, &mut rt);
                }
                None => {
                    warn!("plumtree_loop: input channel closed");
                    break;
                }
            },
            Some((timer, _seq)) = plumtree_timer_rx.recv() => {
                state.timer_fired(timer, &mut rt);
            }
            _ = tick_interval.tick() => {
                state.tick(&mut rt);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{setup, spawn_unipayload_handler};
    use crate::transport::{TransportError, TransportExt};
    use async_trait::async_trait;
    use corro_tests::{launch_test_agent, test_config};
    use corro_types::{
        base::{dbsr, CrsqlDbVersion, CrsqlSeq},
        broadcast::{BroadcastV1, ChangeV1, Changeset},
        members::Members,
    };
    // use plum_foca::PlumtreeMsg;
    use rand::seq::{IndexedRandom, SliceRandom};
    use rangemap::RangeInclusiveSet;
    use speedy::Readable;
    use std::collections::HashSet;
    // use std::hash::Hash;
    use std::sync::atomic::AtomicU64;
    use tokio::task::JoinSet;
    use tokio_util::codec::FramedRead;
    use tokio_util::sync::CancellationToken;
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

        let config = Arc::new(RwLock::new(make_foca_config(1.try_into().unwrap(), None)));
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
            None,
        );
        ta1.agent.members().write().add_member(&ta2_actor);

        let bcast = BroadcastV1::Change(ChangeV1 {
            actor_id: ta1.agent.actor_id(),
            changeset: Changeset::Full {
                version: CrsqlDbVersion(0),
                changes: vec![],
                seqs: dbsr!(0, 0),
                last_seq: CrsqlSeq(0),
                ts: Default::default(),
            },
        });
        let mut ser_buf = BytesMut::new();
        UniPayload::V1 {
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
                        seqs: dbsr!(0, 0),
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
            let (tx_plumtree, _rx_plumtree) = bounded(100, "plumtree_test");
            spawn_unipayload_handler(
                &tripwire,
                &conn,
                ta1.agent.cluster_id(),
                tx_changes,
                tx_plumtree,
            );

            // we should receive five items starting from the biggest version
            for i in (0..5).rev() {
                let changes = tokio::time::timeout(Duration::from_secs(5), rx_changes.recv())
                    .await?
                    .unwrap();
                assert_eq!(
                    changes.0.versions(),
                    (CrsqlDbVersion(i)..=CrsqlDbVersion(i)).into()
                );
            }
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        spawn::wait_for_all_pending_handles().await;

        Ok(())
    }

    #[derive(Clone, Debug)]
    pub struct TestTransport {
        nodes: Arc<RwLock<HashMap<SocketAddr, mpsc::Sender<Bytes>>>>,
    }

    impl TestTransport {
        pub fn new() -> Self {
            Self {
                nodes: Arc::new(RwLock::new(HashMap::new())),
            }
        }

        pub async fn add_node(&self, addr: SocketAddr) -> mpsc::Receiver<Bytes> {
            let (tx, rx) = mpsc::channel(50000);
            self.nodes.write().insert(addr, tx);
            rx
        }
    }

    #[async_trait]
    impl TransportExt for TestTransport {
        async fn send_datagram(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
            let tx = self.nodes.write().get(&addr).unwrap().clone();
            tokio::spawn(async move {
                tx.send(data)
                    .await
                    .map_err(|_| TransportError::SendError("channel closed".to_string()))
            });
            Ok(())
        }

        async fn send_uni(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
            let tx = self.nodes.write().get(&addr).unwrap().clone();
            tokio::spawn(async move {
                tx.send(data)
                    .await
                    .map_err(|_| TransportError::SendError("channel closed".to_string()))
            });
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_broadcast_spread() -> eyre::Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _, _) = Tripwire::new_simple();
        let opts = BroadcastOpts {
            interval: Duration::from_secs(2),
            bcast_cutoff: 5 * 1024,
        };
        let num_nodes = 800;
        let num_changes = 2000;
        let transport = TestTransport::new();
        let config = Arc::new(RwLock::new(foca::Config::new_wan(
            num_nodes.try_into().unwrap(),
        )));

        println!("config: {:?}", config.read().max_transmissions);
        let cancel_token = CancellationToken::new();
        let mut join_set = JoinSet::new();

        let mut tas: Vec<_> = Vec::new();
        let mut members = Members::default();
        for _ in 0..num_nodes {
            let (_, test_conf) = test_config(|conf| conf.build())?;
            let (agent, _) = setup(test_conf.clone(), tripwire.clone()).await?;
            members.add_member(&Actor::new(
                agent.actor_id(),
                agent.gossip_addr(),
                agent.clock().new_timestamp().into(),
                agent.cluster_id(),
            ));
            tas.push(agent);
        }

        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let mut tx_bcasts: Vec<_> = Vec::new();
        for agent in tas.iter() {
            let (tx_bcast, rx_bcast) = bounded(1000, "bcast");
            let transport_rx: mpsc::Receiver<Bytes> = transport.add_node(agent.gossip_addr()).await;
            tx_bcasts.push((agent.actor_id(), tx_bcast));

            let agent_clone = agent.clone();
            agent_clone.members().write().states = members.states.clone();
            agent_clone.members().write().by_addr = members.by_addr.clone();
            agent_clone.members().write().rtts = members.rtts.clone();

            let cancel_token = cancel_token.clone();
            join_set.spawn(process_broadcasts(
                agent_clone.actor_id(),
                transport_rx,
                cancel_token,
            ));
            tokio::spawn(handle_broadcasts(
                agent_clone.clone(),
                rx_bcast,
                transport.clone(),
                config.clone(),
                tripwire.clone(),
                opts,
            ));
        }
        let mut rng = StdRng::from_os_rng();

        let mut local: HashMap<ActorId, HashSet<u64>> = HashMap::new();
        for i in 1..=num_changes {
            let ring0 = tx_bcasts.choose_multiple(&mut rng, 10).collect::<Vec<_>>();
            for (actor_id, tx_bcast) in ring0 {
                tx_bcast
                    .send(BroadcastInput::AddBroadcast(BroadcastV1::Change(
                        ChangeV1 {
                            actor_id: *actor_id,
                            changeset: Changeset::Full {
                                version: CrsqlDbVersion(i),
                                changes: vec![],
                                seqs: dbsr!(0, 0),
                                last_seq: CrsqlSeq(0),
                                ts: Default::default(),
                            },
                        },
                    )))
                    .await
                    .unwrap();

                local
                    .entry(*actor_id)
                    .or_insert(Default::default())
                    .insert(i);
            }
        }

        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_token.cancel();

        println!("sending cancel signal");
        tripwire_tx.send(()).await.unwrap();
        tripwire_worker.await;
        spawn::wait_for_all_pending_handles().await;

        let results = join_set.join_all().await;
        let total = num_nodes as u64 * num_changes as u64;
        let mut total_extra_recvs = 0;
        let mut total_seen = 0;
        for (actor_id, extra_recvs, mut seen) in results {
            if let Some(local_seen) = local.get(&actor_id) {
                seen.extend(local_seen.iter().cloned());
            }
            total_extra_recvs += extra_recvs;
            total_seen += seen.len();
            let output = RangeInclusiveSet::from_iter(seen.iter().cloned().map(|v| v..=v));
            println!("actor_id: {actor_id}, extra_recvs: {extra_recvs}, seen: {output:?}");
        }

        println!("total extra recvs: {total_extra_recvs}");
        println!("total seen: {total_seen} ");
        println!("total: {total}");

        let percent_received = (total_seen as f64 / total as f64) * 100.0;
        println!("percentage received: {percent_received}");

        let extras = (total_extra_recvs as f64 / total as f64) * 100.0;
        println!("percentage extras: {extras}");
        assert!(percent_received > 90.0);
        Ok(())
    }

    async fn process_broadcasts(
        actor_id: ActorId,
        mut transport_rx: mpsc::Receiver<Bytes>,
        cancel_token: CancellationToken,
    ) -> (ActorId, usize, HashSet<u64>) {
        let mut extra_recvs = 0;
        let mut seen = HashSet::new();
        loop {
            tokio::select! {
                biased;
                maybe_bytes = transport_rx.recv() => {
                    match maybe_bytes {
                        Some(b) => {
                            let mut framed = FramedRead::new(
                                b.as_ref(),
                                LengthDelimitedCodec::builder()
                                    .max_frame_length(100 * 1_024 * 1_024)
                                    .new_codec(),
                            );

                            while let Some(Ok(b)) = framed.next().await {
                                let bcast = UniPayload::read_from_buffer(&b).unwrap();
                                match bcast {
                                    UniPayload::V1 {
                                        data: UniPayloadV1::Broadcast(BroadcastV1::Change(change)),
                                        ..
                                    } => {
                                        for version in change.versions() {
                                            if seen.contains(&version.0) {
                                                extra_recvs += 1;
                                                continue;
                                            }
                                            seen.insert(version.0);
                                        }
                                    }
                                    _ => {
                                        panic!("unexpected broadcast type");
                                    }
                                }
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
                _ = cancel_token.cancelled() => {
                    break;
                }
            }
        }

        (actor_id, extra_recvs, seen)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_plumtree_broadcast_spread() -> eyre::Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _, _) = Tripwire::new_simple();
        let num_nodes = 100;
        let num_changes = 2000;
        let transport = TestTransport::new();
        let config = Arc::new(RwLock::new(foca::Config::new_wan(
            num_nodes.try_into().unwrap(),
        )));

        println!("config: {:?}", config.read().max_transmissions);
        let mut join_set = JoinSet::new();

        let mut tas: Vec<_> = Vec::new();
        let mut send_tas: Vec<_> = Vec::new();
        let mut members = Members::default();
        let mut plum_stats = HashMap::new();
        for _ in 0..num_nodes {
            let (_, test_conf) = test_config(|conf| conf.build())?;
            let (agent, opts) = setup(test_conf.clone(), tripwire.clone()).await?;
            members.add_member(&Actor::new(
                agent.actor_id(),
                agent.gossip_addr(),
                agent.clock().new_timestamp().into(),
                agent.cluster_id(),
            ));
            plum_stats.insert(
                agent.actor_id(),
                Arc::new((
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                    AtomicU64::new(0),
                )),
            );
            send_tas.push((agent.actor_id(), agent.tx_plumtree().clone()));
            tas.push((agent, opts));
        }

        let plum_config = plum_foca::Config {
            ihave_timeout: Duration::from_millis(500),
            optimization_threshold: 5,
            max_cached_payloads: 4096,
            max_eager: 5,
            max_lazy: 10,
            prune_threshold: 2,
        };

        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        // let mut tx_bcasts: Vec<_> = Vec::new();
        let cancel_token = CancellationToken::new();

        for (agent, mut opts) in tas.into_iter() {
            let agent_clone = agent.clone();
            agent_clone.members().write().states = members.states.clone();
            agent_clone.members().write().by_addr = members.by_addr.clone();
            agent_clone.members().write().rtts = members.rtts.clone();

            let cancel = cancel_token.clone();

            // let (tx_bcast, rx_bcast) = bounded(1000, "bcast");
            // let (tx_plumtree, rx_plumtree) = bounded(1000, "plumtree");
            let mut transport_rx: mpsc::Receiver<Bytes> =
                transport.add_node(agent_clone.gossip_addr()).await;
            // tx_bcasts.push((agent_clone.actor_id(), tx_bcast));

            tokio::spawn(plumtree_loop(
                agent_clone.clone(),
                transport.clone(),
                opts.rx_plumtree,
                agent_clone.tx_changes().clone(),
                tripwire.clone(),
                plum_stats.get(&agent_clone.actor_id()).unwrap().clone(),
            ));

            // load up all the members into plumtree
            // println!("loading up members into plumtree");
            // let mut member_ids = members.states.keys().collect::<Vec<_>>();
            // member_ids.shuffle(&mut StdRng::from_os_rng());
            // println!("local_id={:?} member up: {:?}", agent_clone.actor_id(), member_ids);
            // for actor_id in member_ids {
            //     if *actor_id != agent_clone.actor_id() {
            //         agent_clone.tx_plumtree().send(PlumtreeInput::MemberUp(*actor_id)).await.ok();
            //     }
            // }
            // println!("done loading up members into plumtree");

            let cancel_clone = cancel.clone();
            let tx_plumtree = agent_clone.tx_plumtree().clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel_clone.cancelled() => break,
                        Some(b) = transport_rx.recv() => {
                            let mut framed = FramedRead::new(
                                b.as_ref(),
                                LengthDelimitedCodec::builder()
                                    .max_frame_length(100 * 1_024 * 1_024)
                                    .new_codec(),
                            );
                            while let Some(Ok(frame)) = framed.next().await {
                                if let Ok(UniPayload::V1 {
                                    data: UniPayloadV1::PlumTree(msg),
                                    cluster_id: cid,
                                }) = UniPayload::read_from_buffer(&frame)
                                {
                                    // if cid != cluster_id {
                                    //     continue;
                                    // }
                                    tx_plumtree.send(PlumtreeInput::Wire(msg)).await.ok();
                                }
                            }
                        }
                    }
                }
            });

            // let agent_clone: Agent = agent.clone();
            join_set.spawn(async move {
                let actor_id = agent.actor_id();
                let mut seen_map: RangeInclusiveSet<CrsqlDbVersion> = RangeInclusiveSet::new();
                let mut duplicate = 0;
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        Some(changes) = opts.rx_changes.recv() => {
                            match changes {
                                (
                                    ChangeV1 {
                                        actor_id,
                                        changeset,
                                    },
                                    ChangeSource::Broadcast,
                                ) => {
                                    if seen_map.contains(&changeset.versions().start()) {
                                        duplicate += 1;
                                        continue;
                                    }
                                    seen_map.insert(changeset.versions().start()..=changeset.versions().end());
                                }
                                _ => {
                                    warn!("unexpected change source: {:?}", changes);
                                }
                            }
                        }
                    }
                }

                return (actor_id, duplicate, seen_map);
            });
        }

        println!("done spawning processing threads");
        let mut rng = StdRng::from_os_rng();
        let chunk_size = 10u64;
        let chunk_pause = Duration::from_millis(1000);
        let mut all_changes = Vec::new();
        for chunk_start in (0..num_changes).step_by(chunk_size as usize) {
            let chunk_end = (chunk_start + chunk_size).min(num_changes);
            for i in chunk_start..chunk_end {
                let (actor_id, tx_plumtree) = send_tas.choose(&mut rng).unwrap();
                let change = ChangeV1 {
                    actor_id: *actor_id,
                    changeset: Changeset::Full {
                        version: CrsqlDbVersion(i),
                        changes: vec![],
                        seqs: dbsr!(0, 0),
                        last_seq: CrsqlSeq(0),
                        ts: Default::default(),
                    },
                };
                all_changes.push(change.clone());
                tx_plumtree.send(PlumtreeInput::Broadcast(BroadcastV1::Change(change))).await.unwrap();
            }

            if chunk_end < num_changes {
                // if chunk_start ==  {
                //     tokio::time::sleep(Duration::from_secs(1)).await;
                // } else {
                    tokio::time::sleep(chunk_pause).await;
                // }
            }
        }

        println!("done sending changes");
        tokio::time::sleep(Duration::from_secs(10)).await;
        cancel_token.cancel();
        println!("done cancelling");
        drop(send_tas);

        println!("sending cancel signal");
        tripwire_tx.send(()).await.unwrap();
        tripwire_worker.await;
        // spawn::wait_for_all_pending_handles().await;

        let results = join_set.join_all().await;


        let mut total_map = results.into_iter().map(|(actor_id, duplicate, seen_map)| (actor_id, (duplicate, seen_map))).collect::<HashMap<_, _>>();
        for change in all_changes {
            let (_, seen_map) = total_map.entry(change.actor_id).or_insert((0, RangeInclusiveSet::new()));
            seen_map.insert(change.changeset.versions().start()..=change.changeset.versions().end());
        }

        let total_expected = num_nodes as u64 * num_changes;
        let total_seen: u64 = total_map.values().map(|(_, seen_map)| seen_map.iter().map(|v| v.end().0 - v.start().0 + 1).sum::<u64>() as u64).sum();
        let extra_recvs: u64 = total_map.values().map(|(duplicate, _)| *duplicate as u64).sum();

        let (total_gossip, total_ihave, total_graft, total_prune) = plum_stats.iter().fold(
            (0, 0, 0, 0),
            |(acc_gossip, acc_ihave, acc_graft, acc_prune), (_, stats)| {
                (
                    acc_gossip + stats.0.load(Ordering::Relaxed),
                    acc_ihave + stats.1.load(Ordering::Relaxed),
                    acc_graft + stats.2.load(Ordering::Relaxed),
                    acc_prune + stats.3.load(Ordering::Relaxed),
                )
            },
        );
        // let total_gossip = stats.gossip.load(Ordering::Relaxed);
        // let total_ihave = stats.ihave.load(Ordering::Relaxed);
        // let total_graft = stats.graft.load(Ordering::Relaxed);
        // let total_prune = stats.prune.load(Ordering::Relaxed);
        let total_control = total_ihave + total_graft + total_prune;

        println!();
        println!("--- Plumtree spread results (async) ---");
        println!("nodes: {num_nodes}, changes: {num_changes}");
        println!();

        for (actor_id, (duplicate, seen_map)) in total_map {
            println!("actor_id: {actor_id}, duplicate: {duplicate}, seen: {seen_map:?}");
        }

        println!("delivery: {total_seen} / {total_expected}");
        println!(
            "delivery %: {:.2}",
            total_seen as f64 / total_expected as f64 * 100.0
        );
        println!("duplicate deliveries: {extra_recvs}");
        println!(
            "duplicate %: {:.4}",
            extra_recvs as f64 / total_expected as f64 * 100.0
        );
        println!();
        println!("gossip messages:  {total_gossip}");
        println!(
            "  per change:     {:.1}",
            total_gossip as f64 / total_expected as f64
        );

        println!(
            "gossip duplicates: {:.1}",
            total_gossip as f64 / total_expected as f64 * 100.0
        );
        println!("ihave messages:   {total_ihave}");
        println!("graft messages:   {total_graft}");
        println!("prune messages:   {total_prune}");
        println!("total control:    {total_control}");
        println!(
            "control per change: {:.1}",
            total_control as f64 / num_changes as f64
        );

        assert!(
            total_seen as f64 / total_expected as f64 > 0.99,
            "delivery rate too low: {total_seen}/{total_expected}"
        );
        Ok(())
    }
}
