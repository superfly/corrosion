use std::{
    cmp,
    iter::Peekable,
    net::SocketAddr,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use backoff::Backoff;
use bytes::{BufMut, Bytes, BytesMut};
use foca::{Foca, NoCustomBroadcast, Notification, Timer};
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Future, FutureExt,
};
use hyper::client::HttpConnector;
use metrics::{gauge, histogram, increment_counter};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rusqlite::params;
use strum::{EnumDiscriminants, EnumString};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{channel, Receiver, Sender},
    task::block_in_place,
    time::{interval, timeout, MissedTickBehavior},
};
use tokio_stream::{wrappers::errors::BroadcastStreamRecvError, StreamExt};
use tracing::{debug, error, log::info, trace, warn};
use tripwire::Tripwire;

use corro_types::{
    actor::Actor,
    agent::Agent,
    broadcast::{BroadcastInput, DispatchRuntime, FocaInput, Message},
    members::MemberEvent,
};

// TODO: do not hard code...
pub const FRAGMENTS_AT: usize = 1420 // wg0 MTU
                              - 40 // 40 bytes IPv6 header
                              - 8; // UDP header bytes
pub const EFFECTIVE_CAP: usize = FRAGMENTS_AT - 1; // fragmentation cap - 1 for the message type byte
pub const HTTP_BROADCAST_SIZE: usize = 64 * 1024;
pub const EFFECTIVE_HTTP_BROADCAST_SIZE: usize = HTTP_BROADCAST_SIZE - 1;

const BIG_PAYLOAD_THRESHOLD: usize = 32 * 1024;

pub type ClientPool = hyper::Client<HttpConnector, hyper::Body>;

pub fn runtime_loop(
    actor: Actor,
    agent: Agent,
    socket: Arc<UdpSocket>,
    mut rx_foca: Receiver<FocaInput>,
    mut rx_bcast: Receiver<BroadcastInput>,
    member_events: tokio::sync::broadcast::Receiver<MemberEvent>,
    to_send_tx: Sender<(Actor, Bytes)>,
    notifications_tx: Sender<Notification<Actor>>,
    client: ClientPool,
    clock: Arc<uhlc::HLC>,
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

    let (timer_tx, mut timer_rx) = channel(1);
    tokio::spawn(async move {
        while let Some((duration, timer)) = to_schedule_rx.recv().await {
            let timer_tx = timer_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(duration).await;
                timer_tx.send(timer).await.ok();
            });
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
                HandleTimer(Timer<Actor>),
                MemberEvents(Vec<Result<MemberEvent, BroadcastStreamRecvError>>),
                Metrics,
                Tripped,
            }

            let mut tripped = false;
            let mut member_events_done = false;

            loop {
                let branch = tokio::select! {
                    biased;
                    input = rx_foca.recv() => match input {
                        Some(input) => {
                            Branch::Foca(input)
                        },
                        None => {
                            warn!("no more foca inputs");
                            break;
                        }
                    },
                    timer = timer_rx.recv() => match timer {
                        Some(timer) => {
                            Branch::HandleTimer(timer)
                        },
                        None => {
                            warn!("no more foca timers, breaking");
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
                            trace!("announcing actor: {actor:?}");
                            if let Err(e) = foca.announce(actor, &mut runtime) {
                                error!("foca announce error: {e}");
                            }
                        }
                        FocaInput::Data(data, from) => {
                            histogram!("corro.gossip.recv.bytes", data.len() as f64);
                            if let Err(e) = foca.handle_data(&data, &mut runtime) {
                                error!("error handling foca data from {from:?}: {e}");
                            }
                        }
                        FocaInput::ClusterSize(size) => {
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
                            if let Err(e) = foca.apply_many(updates.into_iter(), &mut runtime) {
                                error!("foca apply_many error: {e}");
                            }
                        }
                    },
                    Branch::MemberEvents(evts) => {
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
                    Branch::HandleTimer(timer) => {
                        if let Err(e) = foca.handle_timer(timer, &mut runtime) {
                            error!("foca: error handling timer: {e}");
                        }
                    }
                    Branch::Metrics => {
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
        let mut bcast_interval = interval(Duration::from_millis(200));
        // don't want this to go apeshit when delay is created...
        bcast_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut http_bcast_interval = interval(Duration::from_millis(200));
        // don't want this to go apeshit when delay is created...
        http_bcast_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let (bcast_tx, mut bcast_rx) = channel(10240);

        let mut bcast_buf = BytesMut::with_capacity(FRAGMENTS_AT);
        // prepare for first broadcast..
        bcast_buf.put_u8(1);

        let mut http_bcast_buf = BytesMut::with_capacity(HTTP_BROADCAST_SIZE);
        http_bcast_buf.put_u8(1);

        let mut metrics_interval = interval(Duration::from_secs(10));

        let mut rng = StdRng::from_entropy();

        let mut idle_pendings = FuturesUnordered::<
            Pin<Box<dyn Future<Output = PendingBroadcast> + Send + 'static>>,
        >::new();

        enum Branch {
            Broadcast(BroadcastInput),
            SendBroadcast(Message),
            BroadcastDeadline,
            HttpBroadcastDeadline,
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
                bytes = bcast_rx.recv() => match bytes {
                    Some(msg) => {
                        trace!("queueing broadcast: {msg:?}");
                        Branch::SendBroadcast(msg)
                    },
                    None => {
                        warn!("no more broadcast data");
                        break;
                    }
                },
                _ = bcast_interval.tick() => {
                    Branch::BroadcastDeadline
                },
                _ = http_bcast_interval.tick() => {
                    Branch::HttpBroadcastDeadline
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
                    match input {
                        BroadcastInput::Rebroadcast(msg) => {
                            let bcast_tx = bcast_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = bcast_tx.send(msg).await {
                                    error!("could not send broadcast to receiver: {e:?}");
                                }
                            });
                        }
                        BroadcastInput::AddBroadcast(msg) => {
                            // TODO: bring back priority broadcasting
                            // trace!("PRIORITY BROADCASTING: {msg:?}");
                            // if let Err(e) = handle_priority_broadcast(
                            //     &msg,
                            //     &foca.read(),
                            //     // &agent,
                            //     &socket,
                            //     &client,
                            // ) {
                            //     error!("priority broadcast error: {e}");
                            // }
                            // for msg in changes {
                            // queue this to be broadcasted normally..
                            let bcast_tx = bcast_tx.clone();
                            tokio::spawn(async move {
                                if let Err(e) = bcast_tx.send(msg).await {
                                    error!("could not send broadcast (from priority) to receiver: {e:?}");
                                }
                            });
                            // }
                        }
                    }
                }
                Branch::SendBroadcast(msg) => {
                    match serialize_broadcast(&msg, &mut ser_buf) {
                        Ok(bytes) => {
                            let config = config.read();
                            if bytes.len() > EFFECTIVE_CAP {
                                trace!(
                                    "broadcast too big for UDP (len: {}), sending via HTTP",
                                    bytes.len()
                                );
                                if bytes.len() > HTTP_BROADCAST_SIZE - http_bcast_buf.len() {
                                    to_broadcast = Some(PendingBroadcast::from_buf(
                                        &mut http_bcast_buf,
                                        config.num_indirect_probes.get(),
                                        config.max_transmissions.get(),
                                        true,
                                    ));
                                    increment_counter!("corro.broadcast.added.count", "transport" => "http", "reason" => "broadcast");

                                    // reset this since we already reached the max buffer len we care about
                                    http_bcast_interval.reset();
                                };
                                http_bcast_buf.put_slice(&bytes);
                            } else {
                                if bytes.len() > FRAGMENTS_AT - bcast_buf.len() {
                                    to_broadcast = Some(PendingBroadcast::from_buf(
                                        &mut bcast_buf,
                                        config.num_indirect_probes.get(),
                                        config.max_transmissions.get(),
                                        false,
                                    ));

                                    increment_counter!("corro.broadcast.added.count", "transport" => "udp", "reason" => "broadcast");

                                    // reset this since we already reached the max buffer len we care about
                                    bcast_interval.reset();
                                }
                                bcast_buf.put_slice(&bytes);
                            }
                        }
                        Err(e) => {
                            error!("could not serialize low-priority broadcast: {e}");
                        }
                    }
                }
                Branch::BroadcastDeadline => {
                    let config = config.read();
                    if bcast_buf.len() > 1 {
                        to_broadcast = Some(PendingBroadcast::from_buf(
                            &mut bcast_buf,
                            config.num_indirect_probes.get(),
                            config.max_transmissions.get(),
                            false,
                        ));
                        increment_counter!("corro.broadcast.added.count", "transport" => "udp", "reason" => "deadline");
                    }
                }
                Branch::HttpBroadcastDeadline => {
                    let config = config.read();
                    if http_bcast_buf.len() > 1 {
                        to_broadcast = Some(PendingBroadcast::from_buf(
                            &mut http_bcast_buf,
                            config.num_indirect_probes.get(),
                            config.max_transmissions.get(),
                            true,
                        ));
                        increment_counter!("corro.broadcast.added.count", "transport" => "http", "reason" => "deadline");
                    }
                }
                Branch::WokePendingBroadcast(pending) => to_broadcast = Some(pending),
                Branch::Metrics => {
                    gauge!(
                        "corro.gossip.broadcast.channel.capacity",
                        bcast_tx.capacity() as f64
                    );
                    gauge!("corro.broadcast.pending.count", idle_pendings.len() as f64);
                }
            }

            if let Some(mut pending) = to_broadcast.take() {
                trace!("{} to broadcast: {pending:?}", actor_id);
                let broadcast_to = {
                    agent
                        .members()
                        .read()
                        .states
                        .iter()
                        .filter_map(|(member_id, state)| {
                            (*member_id != actor_id).then(|| state.addr)
                        })
                        .choose_multiple(&mut rng, pending.count)
                };

                for addr in broadcast_to {
                    trace!(actor = %actor_id, "broadcasting {} bytes to: {addr} (count: {}, send count: {})", pending.payload.len(), pending.count, pending.send_count());
                    if pending.http {
                        single_http_broadcast(pending.payload.clone(), &client, addr, &clock);
                    } else {
                        single_broadcast(pending.payload.clone(), socket.clone(), addr);
                    }
                }

                if let Some(dur) = pending.backoff.next() {
                    idle_pendings.push(Box::pin(async move {
                        tokio::time::sleep(dur).await;
                        pending
                    }));
                }
            }
        }
    });
}

fn make_foca_config(cluster_size: NonZeroU32) -> foca::Config {
    let mut config = foca::Config::new_wan(cluster_size);
    config.remove_down_after = Duration::from_secs(2 * 24 * 60 * 60);

    // max payload size for udp over ipv6 wg - 1 for payload type
    config.max_packet_size = EFFECTIVE_CAP.try_into().unwrap();

    config
}

fn split_broadcast(buf: &mut BytesMut) -> Bytes {
    let bytes = buf.split().freeze();

    // set the buffer to send a byte of 1 first, custom broadcast stuff!
    buf.put_u8(1);

    bytes
}

#[derive(Debug)]
struct PendingBroadcast {
    http: bool,
    payload: Bytes,
    count: usize,
    backoff: backoff::Iter,
}

impl PendingBroadcast {
    pub fn send_count(&self) -> u32 {
        self.backoff.retry_count()
    }

    pub fn from_buf(
        buf: &mut BytesMut,
        mut count: usize,
        mut max_transmissions: u8,
        http: bool,
    ) -> Self {
        trace!("creating pending broadcast for {count} members");

        let payload = split_broadcast(buf);

        if payload.len() >= BIG_PAYLOAD_THRESHOLD {
            count /= 2;
            max_transmissions /= 2;
        }

        max_transmissions = cmp::max(max_transmissions, 1);

        // do at least a few!
        // max_transmissions = cmp::max(max_transmissions, 20);

        // debug!("using max transmissions: {max_transmissions}");

        Self {
            http,
            payload,
            count,
            backoff: Backoff::new(max_transmissions as u32)
                .timeout_range(Duration::from_millis(200), Duration::from_secs(10))
                .iter(),
        }
    }
}

fn serialize_broadcast<'a>(msg: &Message, buf: &mut BytesMut) -> eyre::Result<Bytes> {
    trace!("serializing change {msg:?}");

    msg.encode(buf)?;

    Ok(buf.split().freeze())
}

pub struct BroadcastIter<'b, I: Iterator<Item = &'b Bytes>> {
    buf: &'b mut BytesMut,
    chunks: Peekable<I>,
    byte: u8,
    cap: usize,
}

impl<'b, I> BroadcastIter<'b, I>
where
    I: Iterator<Item = &'b Bytes>,
{
    pub fn new(buf: &'b mut BytesMut, chunks: I, cap: usize) -> Self {
        Self {
            buf,
            chunks: chunks.peekable(),
            byte: 1,
            cap,
        }
    }
    pub fn new_priority(buf: &'b mut BytesMut, chunks: I, cap: usize) -> Self {
        Self {
            buf,
            chunks: chunks.peekable(),
            byte: 2,
            cap,
        }
    }
}

impl<'b, I> Iterator for BroadcastIter<'b, I>
where
    I: Iterator<Item = &'b Bytes>,
{
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next_wont_fit = self
                .chunks
                .peek()
                .map(|bytes| bytes.len() > self.cap - self.buf.len())
                .unwrap_or(false);

            if !next_wont_fit {
                if let Some(bytes) = self.chunks.next() {
                    if self.buf.is_empty() {
                        self.buf.put_u8(self.byte);
                    }
                    self.buf.put_slice(bytes.as_ref());
                    continue;
                }
            }

            if self.buf.is_empty() {
                break None;
            }
            break Some(self.buf.split().freeze());
        }
    }
}

fn single_broadcast(payload: Bytes, socket: Arc<UdpSocket>, addr: SocketAddr) {
    tokio::spawn(async move {
        trace!("singly broadcasting to {addr}");
        match socket.send_to(payload.as_ref(), addr).await {
            Ok(n) => {
                histogram!("corro.broadcast.sent.bytes", n as f64, "transport" => "udp");
                trace!("sent {n} bytes to {addr}");
            }
            Err(e) => {
                error!(
                    "could not send UDP broadcast to {addr} (len: {}): {e}",
                    payload.len()
                );
            }
        }
    });
}

fn single_http_broadcast(payload: Bytes, client: &ClientPool, addr: SocketAddr, clock: &uhlc::HLC) {
    let start = Instant::now();
    trace!("single http broadcast with first byte: {}", payload[0]);
    let len = payload.len();
    let req = client
        .request(http_broadcast_request(
            addr,
            clock,
            hyper::Body::from(payload),
        ))
        .inspect(move |res| match res {
            Ok(_) => {
                histogram!("corro.broadcast.sent.bytes", len as f64, "transport" => "http");
            }
            Err(e) => {
                error!(
                    "could not send single http broadcast to addr: '{addr}' (len: {}): {e}",
                    len
                );
            }
        });
    tokio::spawn(async move {
        match req.await {
            Ok(res) => {
                histogram!("corro.gossip.broadcast.response.time.seconds", start.elapsed().as_secs_f64(), "status" => res.status().to_string(), "kind" => "single");
            }
            Err(e) => error!("error sending priority broadcast (single): {e}"),
        }
    });
}

fn http_broadcast_request(
    addr: SocketAddr,
    clock: &uhlc::HLC,
    body: hyper::Body,
) -> hyper::Request<hyper::Body> {
    hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(format!("http://{addr}/v1/broadcast"))
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::ACCEPT, "application/json")
        .header(
            "corro-clock",
            serde_json::to_string(&clock.new_timestamp()).expect("could not serialize clock"),
        )
        .body(body)
        .expect("could not build request")
}
