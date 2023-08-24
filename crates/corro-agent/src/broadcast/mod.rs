use std::{
    net::SocketAddr,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use foca::{Foca, NoCustomBroadcast, Notification, Timer};
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Future,
};
use metrics::{gauge, histogram};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rusqlite::params;
use speedy::Writable;
use strum::EnumDiscriminants;
use tokio::{
    sync::mpsc::{self, channel, Receiver, Sender},
    task::{block_in_place, LocalSet},
    time::interval,
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, ReceiverStream},
    StreamExt,
};
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{debug, error, log::info, trace, warn};
use tripwire::Tripwire;

use corro_types::{
    actor::Actor,
    agent::Agent,
    broadcast::{
        BroadcastInput, BroadcastV1, DispatchRuntime, FocaInput, UniPayload, UniPayloadV1,
    },
    members::MemberEvent,
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
            let timer_tx = timer_tx.clone();
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

pub fn runtime_loop(
    actor: Actor,
    agent: Agent,
    transport: Transport,
    mut rx_foca: Receiver<FocaInput>,
    mut rx_bcast: Receiver<BroadcastInput>,
    member_events: tokio::sync::broadcast::Receiver<MemberEvent>,
    to_send_tx: Sender<(Actor, Bytes)>,
    notifications_tx: Sender<Notification<Actor>>,
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

    let (to_schedule_tx, mut to_schedule_rx) = channel(10240);

    let mut runtime: DispatchRuntime<Actor> =
        DispatchRuntime::new(to_send_tx, to_schedule_tx, notifications_tx);

    let (timer_tx, mut timer_rx) = channel(10);
    let timer_spawner = TimerSpawner::new(timer_tx);

    tokio::spawn(async move {
        while let Some((duration, timer)) = to_schedule_rx.recv().await {
            timer_spawner.spawn((duration, timer));
        }
    });

    // foca SWIM operations loop.
    // NOTE: every turn of that loop should be fast or else we risk being suspected of being down
    tokio::spawn({
        let config = config.clone();
        let agent = agent.clone();
        let mut tripwire = tripwire.clone();
        async move {
            let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_cluster_size = unsafe { NonZeroU32::new_unchecked(1) };

            let member_events_chunks =
                tokio_stream::wrappers::BroadcastStream::new(member_events.resubscribe())
                    .chunks_timeout(100, Duration::from_secs(30));
            tokio::pin!(member_events_chunks);

            #[derive(EnumDiscriminants)]
            #[strum_discriminants(derive(strum::IntoStaticStr))]
            enum Branch {
                Foca(FocaInput),
                HandleTimer(Timer<Actor>, Instant),
                MemberEvents(Vec<Result<MemberEvent, BroadcastStreamRecvError>>),
                Metrics,
                Tripped,
            }

            let mut tripped = false;
            let mut member_events_done = false;

            loop {
                let branch = tokio::select! {
                    biased;
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
                    evts =  member_events_chunks.next(), if !member_events_done && !tripped => match evts {
                        Some(evts) if !evts.is_empty() => Branch::MemberEvents(evts),
                        Some(_) => {
                            continue;
                        }
                        None => {
                            member_events_done = true;
                            continue;
                        }
                    },
                    _ = metrics_interval.tick() => {
                        Branch::Metrics
                    },
                    _ = &mut tripwire, if !tripped => {
                        tripped = true;
                        Branch::Tripped
                    },
                };

                let start = Instant::now();
                let discriminant = BranchDiscriminants::from(&branch);

                match branch {
                    Branch::Tripped => {
                        trace!("handling Branch::Tripped");
                        // collect all current states

                        let states: Vec<_> = {
                            let members = agent.members().read();
                            foca.iter_members()
                                .filter_map(|member| {
                                    members.get(&member.id().id()).and_then(|state| {
                                        match serde_json::to_string(member) {
                                            Ok(foca_state) => Some((
                                                member.id().id(),
                                                state.addr,
                                                "up",
                                                foca_state,
                                            )),
                                            Err(e) => {
                                                error!(
                                                    "could not serialize foca member state: {e}"
                                                );
                                                None
                                            }
                                        }
                                    })
                                })
                                .collect()
                        };

                        // leave the cluster gracefully

                        if let Err(e) = foca.leave_cluster(&mut runtime) {
                            error!("could not leave cluster: {e}");
                        }

                        // write the states to the DB for a faster rejoin

                        {
                            match agent.pool().write_priority().await {
                                Ok(mut conn) => block_in_place(|| match conn.transaction() {
                                    Ok(tx) => {
                                        for (id, address, state, foca_state) in states {
                                            let db_res = tx.prepare_cached(
                                                    "
                                                INSERT INTO __corro_members (id, address, state, foca_state)
                                                    VALUES (?, ?, ?, ?)
                                                ON CONFLICT (id) DO UPDATE SET
                                                    address = excluded.address,
                                                    state = excluded.state,
                                                    foca_state = excluded.foca_state;
                                            ",
                                                )
                                                .and_then(|mut prepped| {
                                                    prepped.execute(params![
                                                        id,
                                                        address.to_string(),
                                                        state,
                                                        foca_state
                                                    ])
                                                });

                                            if let Err(e) = db_res {
                                                error!("could not upsert member state: {e}");
                                            }
                                        }
                                        if let Err(e) = tx.commit() {
                                            error!("could not commit member states upsert tx: {e}");
                                        }
                                    }

                                    Err(e) => {
                                        error!("could not start transaction to update member events: {e}");
                                    }
                                }),
                                Err(e) => {
                                    error!(
                                    "could not acquire a r/w conn to process member events: {e}"
                                );
                                }
                            }
                        }

                        break;
                    }
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
                            histogram!("corro.gossip.recv.bytes", data.len() as f64);
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
                    },
                    Branch::MemberEvents(evts) => {
                        trace!("handling Branch::MemberEvents");
                        let splitted: Vec<_> = evts
                            .iter()
                            .flatten()
                            .filter_map(|evt| {
                                let actor = evt.actor();
                                let foca_state = {
                                    // need to bind this...
                                    let foca_state = foca
                                        .iter_members()
                                        .find(|member| member.id().id() == actor.id())
                                        .and_then(|member| match serde_json::to_string(member) {
                                            Ok(foca_state) => Some(foca_state),
                                            Err(e) => {
                                                error!(
                                                    "could not serialize foca member state: {e}"
                                                );
                                                None
                                            }
                                        });
                                    foca_state
                                };

                                foca_state.map(|foca_state| {
                                    (actor.id(), actor.addr(), evt.as_str(), foca_state)
                                })
                            })
                            .collect();

                        let pool = agent.pool().clone();
                        tokio::spawn(async move {
                            let mut conn = match pool.write_low().await {
                                Ok(conn) => conn,
                                Err(e) => {
                                    error!(
                                        "could not acquire a r/w conn to process member events: {e}"
                                    );
                                    return;
                                }
                            };

                            let res = block_in_place(|| {
                                let tx = conn.transaction()?;

                                for (id, address, state, foca_state) in splitted {
                                    tx.prepare_cached(
                                        "
                                            INSERT INTO __corro_members (id, address, state, foca_state)
                                                VALUES (?, ?, ?, ?)
                                            ON CONFLICT (id) DO UPDATE SET
                                                address = excluded.address,
                                                state = excluded.state,
                                                foca_state = excluded.foca_state;
                                        ",
                                    )?
                                    .execute(params![
                                        id,
                                        address.to_string(),
                                        state,
                                        foca_state
                                    ])?;
                                }

                                tx.commit()?;

                                Ok::<_, rusqlite::Error>(())
                            });

                            if let Err(e) = res {
                                error!(
                                    "could not insert state changes from SWIM cluster into sqlite: {e}"
                                );
                            }
                        });
                    }
                    Branch::HandleTimer(timer, seq) => {
                        trace!("handling Branch::HandleTimer");
                        let mut v = vec![(timer, seq)];

                        // drain the channel, in case there's a race among timers
                        while let Ok((timer, seq)) = timer_rx.try_recv() {
                            v.push((timer, seq));
                        }

                        // sort by instant these were scheduled
                        v.sort_by(|a, b| a.1.cmp(&b.1));

                        for (timer, _) in v {
                            if let Err(e) = foca.handle_timer(timer, &mut runtime) {
                                error!("foca: error handling timer: {e}");
                            }
                        }
                    }
                    Branch::Metrics => {
                        trace!("handling Branch::Metrics");
                        {
                            gauge!("corro.gossip.members", foca.num_members() as f64);
                            gauge!(
                                "corro.gossip.updates_backlog",
                                foca.updates_backlog() as f64
                            );
                        }
                        {
                            let config = config.read();
                            gauge!(
                                "corro.gossip.config.max_transmissions",
                                config.max_transmissions.get() as f64
                            );
                            gauge!(
                                "corro.gossip.config.num_indirect_probes",
                                config.num_indirect_probes.get() as f64
                            );
                        }
                        gauge!("corro.gossip.cluster_size", last_cluster_size.get() as f64);
                    }
                }

                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(1) {
                    let to_s: &'static str = discriminant.into();
                    warn!("took {elapsed:?} to execute branch: {to_s}");
                }
            }

            info!("foca is done");
        }
    });

    tokio::spawn(async move {
        let (bcast_tx, bcast_rx) = channel::<BroadcastV1>(10240);

        let mut bcast_buf = BytesMut::new();
        let mut bcast_codec = LengthDelimitedCodec::new();

        let mut metrics_interval = interval(Duration::from_secs(10));

        let mut rng = StdRng::from_entropy();

        let mut idle_pendings = FuturesUnordered::<
            Pin<Box<dyn Future<Output = PendingBroadcast> + Send + 'static>>,
        >::new();

        let chunked_bcast =
            ReceiverStream::new(bcast_rx).chunks_timeout(4, Duration::from_millis(100));
        tokio::pin!(chunked_bcast);

        enum Branch {
            Broadcast(BroadcastInput),
            SendBroadcast(Vec<BroadcastV1>),
            WokePendingBroadcast(PendingBroadcast),
            Tripped,
            Metrics,
        }

        let mut tripped = false;
        let mut ser_buf = BytesMut::new();

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
                Some(payloads) = chunked_bcast.next() => {
                    Branch::SendBroadcast(payloads)
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

            let mut to_broadcast = None;

            match branch {
                Branch::Tripped => {
                    // nothing to here, yet!
                }
                Branch::Broadcast(input) => {
                    trace!("handling Branch::Broadcast");
                    match input {
                        BroadcastInput::Rebroadcast(payload) => {
                            let bcast_tx = bcast_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = bcast_tx.send(payload).await {
                                    error!("could not send broadcast to receiver: {e:?}");
                                }
                            });
                        }
                        BroadcastInput::AddBroadcast(payload) => {
                            // queue this to be broadcasted normally..
                            let bcast_tx = bcast_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = bcast_tx.send(payload).await {
                                    error!("could not send broadcast (from priority) to receiver: {e:?}");
                                }
                            });
                            // }
                        }
                    }
                }
                Branch::SendBroadcast(broadcasts) => {
                    trace!("handling Branch::SendBroadcast");

                    for bcast in broadcasts {
                        if let Err(e) = UniPayload::V1(UniPayloadV1::Broadcast(bcast))
                            .write_to_stream((&mut ser_buf).writer())
                        {
                            error!("could not encode UniPayload::V1 Broadcast: {e}");
                            continue;
                        }
                        trace!("ser buf len: {}", ser_buf.len());
                        if let Err(e) = bcast_codec.encode(ser_buf.split().freeze(), &mut bcast_buf)
                        {
                            error!("could not encode broadcast: {e}");
                            continue;
                        }
                        // bcast_buf.extend_from_slice(&ser_buf.split());
                    }

                    trace!("bcast_buf len: {}", bcast_buf.len());

                    to_broadcast = Some(PendingBroadcast::new(bcast_buf.split().freeze()));
                }
                Branch::WokePendingBroadcast(pending) => {
                    trace!("handling Branch::WokePendingBroadcast");
                    to_broadcast = Some(pending)
                }
                Branch::Metrics => {
                    trace!("handling Branch::Metrics");
                    gauge!(
                        "corro.gossip.broadcast.channel.capacity",
                        bcast_tx.capacity() as f64
                    );
                    gauge!("corro.broadcast.pending.count", idle_pendings.len() as f64);
                    gauge!(
                        "corro.broadcast.buffer.capacity",
                        bcast_buf.capacity() as f64
                    );
                    gauge!(
                        "corro.broadcast.serialization.buffer.capacity",
                        ser_buf.capacity() as f64
                    );
                }
            }

            if let Some(mut pending) = to_broadcast.take() {
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
                            (*member_id != actor_id).then(|| state.addr)
                        })
                        .choose_multiple(&mut rng, member_count as usize)
                };

                for addr in broadcast_to {
                    debug!(actor = %actor_id, "broadcasting {} bytes to: {addr} (send count: {})", pending.payload.len(), pending.send_count);

                    transmit_broadcast(pending.payload.clone(), transport.clone(), addr);
                }

                pending.send_count = pending.send_count.wrapping_add(1);

                debug!(
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

fn make_foca_config(cluster_size: NonZeroU32) -> foca::Config {
    let mut config = foca::Config::new_wan(cluster_size);
    config.remove_down_after = Duration::from_secs(2 * 24 * 60 * 60);

    // max payload size for udp datagrams, use a safe value here...
    // TODO: calculate from smallest max datagram size for all QUIC conns
    config.max_packet_size = 1200.try_into().unwrap();

    config
}

#[derive(Debug)]
struct PendingBroadcast {
    payload: Bytes,
    send_count: u8,
}

impl PendingBroadcast {
    pub fn new(payload: Bytes) -> Self {
        Self {
            payload,
            send_count: 0,
        }
    }
}

fn transmit_broadcast(payload: Bytes, transport: Transport, addr: SocketAddr) {
    tokio::spawn(async move {
        trace!("singly broadcasting to {addr}");
        let mut stream = match transport.open_uni(addr).await {
            Ok(s) => s,
            Err(e) => {
                error!("could not open unidirectional stream to {addr}: {e}");
                return;
            }
        };

        match tokio::time::timeout(Duration::from_secs(5), stream.write_all(&payload)).await {
            Err(_e) => {
                warn!("timed out writing broadcast to uni stream");
                return;
            }
            Ok(Err(e)) => {
                error!("could not write to uni stream to {addr}: {e}");
                return;
            }
            _ => {}
        }

        if let Err(e) = stream.finish().await {
            warn!("error finishing broadcast uni stream to {addr}: {e}");
        }
    });
}
