use std::{
    iter::Peekable,
    net::SocketAddr,
    num::NonZeroU32,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use backoff::Backoff;
use bytes::{BufMut, Bytes, BytesMut};
use foca::{Foca, Member, NoCustomBroadcast, Notification, Timer};
use futures::{
    stream::{FusedStream, FuturesUnordered},
    Future, FutureExt, StreamExt,
};
use hyper::client::HttpConnector;
use metrics::{gauge, histogram, increment_counter};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use speedy::{Readable, Writable};
use tokio::{
    net::UdpSocket,
    sync::mpsc::{channel, Receiver, Sender},
    time::{interval, MissedTickBehavior},
};
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

use crate::{
    actor::{Actor, ActorId},
    types::change::Change,
};

use self::runtime::DispatchRuntime;

pub mod runtime;

// TODO: do not hard code...
pub const FRAGMENTS_AT: usize = 1420 // wg0 MTU
                              - 40 // 40 bytes IPv6 header
                              - 8; // UDP header bytes
pub const EFFECTIVE_CAP: usize = FRAGMENTS_AT - 1; // fragmentation cap - 1 for the message type byte
pub const HTTP_BROADCAST_SIZE: usize = 64 * 1024;
pub const EFFECTIVE_HTTP_BROADCAST_SIZE: usize = HTTP_BROADCAST_SIZE - 1;

const BIG_PAYLOAD_THRESHOLD: usize = 32 * 1024;

pub type ClientPool = hyper::Client<HttpConnector, hyper::Body>;

#[derive(Debug)]
pub enum BroadcastSrc {
    Http(SocketAddr),
    Udp(SocketAddr),
}

#[derive(Debug)]
pub enum FocaInput {
    Announce(Actor),
    Data(Bytes, BroadcastSrc),
    ClusterSize(NonZeroU32),
    ApplyMany(Vec<Member<Actor>>),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum Message {
    V1(MessageV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum MessageV1 {
    Change {
        actor_id: ActorId,
        start_version: i64,
        end_version: i64,
        changeset: Vec<Change>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum MessageEncodeError {
    #[error(transparent)]
    Serialize(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Message {
    pub fn from_slice<S: AsRef<[u8]>>(slice: S) -> Result<Self, speedy::Error> {
        Self::read_from_buffer(slice.as_ref())
    }

    pub fn encode(&self, buf: &mut BytesMut) -> Result<(), MessageEncodeError> {
        self.write_to_stream(buf.writer())?;
        let bytes = buf.split().freeze();

        let hash = crc32fast::hash(&bytes);

        let mut codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_codec();
        codec.encode(bytes, buf)?;

        buf.put_u32(hash);

        Ok(())
    }
}

#[derive(Debug)]
pub enum BroadcastInput {
    Rebroadcast(Message),
    AddBroadcast(Message),
}

pub fn runtime_loop(
    actor: Actor,
    socket: Arc<UdpSocket>,
    mut rx_foca: Receiver<FocaInput>,
    mut rx_bcast: Receiver<BroadcastInput>,
    to_send_tx: Sender<(Actor, Bytes)>,
    notifications_tx: Sender<Notification<Actor>>,
    client: ClientPool,
    clock: Arc<uhlc::HLC>,
    mut _tripwire: Tripwire,
) {
    debug!("starting runtime loop for actor: {actor:?}");
    let rng = StdRng::from_entropy();
    let actor_id = actor.id();

    let config = Arc::new(RwLock::new(make_foca_config(1.try_into().unwrap())));

    let foca = Arc::new(RwLock::new(Foca::with_custom_broadcast(
        actor,
        config.read().clone(),
        rng,
        foca::BincodeCodec(bincode::DefaultOptions::new()),
        NoCustomBroadcast,
    )));

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

    tokio::spawn({
        let foca = foca.clone();
        let config = config.clone();
        async move {
            let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
            let mut last_cluster_size = unsafe { NonZeroU32::new_unchecked(1) };

            #[derive(Debug)]
            enum Branch {
                Foca(FocaInput),
                HandleTimer(Timer<Actor>),
                Metrics,
            }

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
                    _ = metrics_interval.tick() => {
                        Branch::Metrics
                    }
                };

                trace!("handling foca branch: {branch:?}");

                match branch {
                    Branch::Foca(input) => match input {
                        FocaInput::Announce(actor) => {
                            trace!("announcing actor: {actor:?}");
                            if let Err(e) = foca.write().announce(actor, &mut runtime) {
                                error!("foca announce error: {e}");
                            }
                        }
                        FocaInput::Data(data, from) => {
                            histogram!("corrosion.gossip.recv.bytes", data.len() as f64);
                            if let Err(e) = foca.write().handle_data(&data, &mut runtime) {
                                error!("error handling foca data from {from:?}: {e}");
                            }
                        }
                        FocaInput::ClusterSize(size) => {
                            let diff: i64 =
                                (size.get() as i64 - last_cluster_size.get() as i64).abs();
                            if diff > 5 {
                                debug!("Adjusting cluster size to {size}");
                                let new_config = make_foca_config(size);
                                if let Err(e) = foca.write().set_config(new_config.clone()) {
                                    error!("foca set_config error: {e}");
                                } else {
                                    last_cluster_size = size;
                                    let mut config = config.write();
                                    *config = new_config;
                                }
                            }
                        }
                        FocaInput::ApplyMany(updates) => {
                            if let Err(e) =
                                foca.write().apply_many(updates.into_iter(), &mut runtime)
                            {
                                error!("foca apply_many error: {e}");
                            }
                        }
                    },
                    Branch::HandleTimer(timer) => {
                        if let Err(e) = foca.write().handle_timer(timer, &mut runtime) {
                            error!("foca: error handling timer: {e}");
                        }
                    }
                    Branch::Metrics => {
                        {
                            let foca = foca.read();
                            gauge!("corrosion.gossip.members", foca.num_members() as f64);
                            gauge!(
                                "corrosion.gossip.updates_backlog",
                                foca.updates_backlog() as f64
                            );
                        }
                        {
                            let config = config.read();
                            gauge!(
                                "corrosion.gossip.config.max_transmissions",
                                config.max_transmissions.get() as f64
                            );
                            gauge!(
                                "corrosion.gossip.config.num_indirect_probes",
                                config.num_indirect_probes.get() as f64
                            );
                        }
                        gauge!(
                            "corrosion.gossip.cluster_size",
                            last_cluster_size.get() as f64
                        );
                    }
                }
            }

            warn!("foca is done");
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

        //     // let member_events_chunks =
        //     //     tokio_stream::wrappers::BroadcastStream::new(member_events.resubscribe())
        //     //         .chunks_timeout(100, Duration::from_secs(30));
        //     // tokio::pin!(member_events_chunks);

        enum Branch {
            Broadcast(BroadcastInput),
            SendBroadcast(Message),
            BroadcastDeadline,
            HttpBroadcastDeadline,
            WokePendingBroadcast(PendingBroadcast),
            // MemberEvents(Vec<Result<MemberEvent, BroadcastStreamRecvError>>),
            // Tripped,
            Metrics,
        }

        //     let mut tripped = false;
        //     let mut member_events_done = false;

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
                // evts =  member_events_chunks.next(), if !member_events_done && !tripped => match evts {
                //     Some(evts) if !evts.is_empty() => Branch::MemberEvents(evts),
                //     Some(_) => {
                //         continue;
                //     }
                //     None => {
                //         member_events_done = true;
                //         continue;
                //     }
                // },
                // _ = &mut tripwire, if !tripped => {
                //     tripped = true;
                //     Branch::Tripped
                // },
                _ = metrics_interval.tick() => {
                    Branch::Metrics
                }
            };

            let mut to_broadcast = None;

            match branch {
                // TODO: track cluster membership
                // Branch::Tripped => {
                // }
                // Branch::MemberEvents(evts) => {
                // }
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
                    // TODO: optimize allocs
                    let mut buf = BytesMut::new();
                    match serialize_broadcast(&msg, &mut buf) {
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
                                    increment_counter!("corrosion.broadcast.added.count", "transport" => "http", "reason" => "broadcast");

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

                                    increment_counter!("corrosion.broadcast.added.count", "transport" => "udp", "reason" => "broadcast");

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
                        increment_counter!("corrosion.broadcast.added.count", "transport" => "udp", "reason" => "deadline");
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
                        increment_counter!("corrosion.broadcast.added.count", "transport" => "http", "reason" => "deadline");
                    }
                }
                Branch::WokePendingBroadcast(pending) => to_broadcast = Some(pending),
                Branch::Metrics => {
                    gauge!(
                        "corrosion.gossip.broadcast.channel.capacity",
                        bcast_tx.capacity() as f64
                    );
                    gauge!(
                        "corrosion.broadcast.pending.count",
                        idle_pendings.len() as f64
                    );
                }
            }

            if let Some(mut pending) = to_broadcast.take() {
                trace!("{} to broadcast: {pending:?}", actor_id.hyphenated());
                let broadcast_to = {
                    foca.read()
                        .iter_members()
                        .filter(|member| matches!(member.state(), foca::State::Alive))
                        .map(|member| member.id())
                        .filter(|a| a.id() != actor_id)
                        .map(|a| a.addr())
                        .choose_multiple(&mut rng, pending.count)
                };

                for addr in broadcast_to {
                    trace!("broadcasting to: {addr}");
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

// fn handle_priority_broadcast<
//     B: BroadcastHandler<Actor, Broadcast = &'static [u8], Error = BroadcastsDisabledError>,
// >(
//     msg: &Message,
//     foca: &Foca<Actor, BincodeCodec<DefaultOptions>, StdRng, B>,
//     // agent: &Agent,
//     socket: &Arc<UdpSocket>,
//     client: &ClientPool,
//     // sent_at: Instant,
// ) -> eyre::Result<()> {
//     // TODO: bring that from upstream to reduce allocs
//     let mut buf = BytesMut::new();
//     let serialized = serialize_broadcast(msg, &mut buf)?;

//     let big = serialized.len() > EFFECTIVE_CAP;

//     priority_broadcast_serialized(serialized, foca, socket, client, big);

//     Ok(())
// }

// fn priority_broadcast_serialized<
//     B: BroadcastHandler<Actor, Broadcast = &'static [u8], Error = BroadcastsDisabledError>,
// >(
//     serialized: Bytes,
//     _foca: &Foca<Actor, BincodeCodec<DefaultOptions>, StdRng, B>,
//     // agent: &Agent,
//     _socket: &Arc<UdpSocket>,
//     _client: &ClientPool,
//     http: bool,
//     // sent_at: Instant,
// ) {
//     let cap = if http {
//         HTTP_BROADCAST_SIZE
//     } else {
//         FRAGMENTS_AT
//     };

//     let mut buf = BytesMut::with_capacity(cap);

//     // // first, send broadcast to subscribing nodes
//     // for actor in foca.iter_members().map(|member| member.id()) {
//     //     trace!("checking actor: {actor:?} for priority broadcast");
//     //     let subs = {
//     //         let members = agent.0.members.read();
//     //         let subs = members.subscriptions(&actor.id());
//     //         // save some time
//     //         if subs.is_empty() {
//     //             // trace!("actor {} has no subs", actor.name());
//     //             continue;
//     //         }

//     //         if actor.id() == agent.actor_id() {
//     //             // trace!("actor {} is us!", actor.name());
//     //             continue;
//     //         }
//     //         subs.to_vec()
//     //     };

//     //     trace!("actor {} has subs!", actor.name());

//     //     let iter = BroadcastIter::new_priority(
//     //         &mut buf,
//     //         serialized.iter().filter_map(|(bytes, ctx)| {
//     //             if actor.region() == agent.region() {
//     //                 None
//     //             } else {
//     //                 let ctx_ref = ctx.as_ref()?;
//     //                 subs.iter().find_map(|filter| {
//     //                     match_expr(filter.input(), filter.expr(), ctx_ref).then_some(bytes)
//     //                 })
//     //             }
//     //         }),
//     //         cap,
//     //     );

//     //     debug!(
//     //         "priority broadcasting a matching broadcast to {}",
//     //         actor.name()
//     //     );
//     //     if http {
//     //         let chunks: Vec<Bytes> = iter.collect();
//     //         counter!("corrosion.broadcast.send.count", chunks.len() as u64, "reason" => "subscription", "target" => actor.name().to_string());
//     //         streaming_http_broadcast(chunks, client, actor.addr(), agent.clock());
//     //     } else {
//     //         for bytes in iter {
//     //             increment_counter!("corrosion.broadcast.send.count", "reason" => "subscription", "target" => actor.name().to_string());
//     //             single_broadcast(bytes, socket.clone(), actor.addr());
//     //         }
//     //     }
//     // }

//     // // send everything to all regional members
//     // let regional: Vec<&Actor> = foca
//     //     .iter_members()
//     //     .map(|member| member.id())
//     //     .filter(|actor| actor.id() != agent.actor_id())
//     //     .filter(|actor| actor.region() == agent.region())
//     //     .collect();

//     let iter = BroadcastIter::new_priority(&mut buf, vec![serialized].iter(), cap);

//     // if http {
//     //     let chunks: Vec<Bytes> = iter.collect();
//     //     for actor in regional.iter() {
//     //         counter!("corrosion.broadcast.send.count", chunks.len() as u64, "reason" => "region", "target" => actor.name().to_string());
//     //         streaming_http_broadcast(chunks.clone(), client, actor.addr(), agent.clock());
//     //     }
//     // } else {
//     //     for bytes in iter {
//     //         for actor in regional.iter() {
//     //             increment_counter!("corrosion.broadcast.send.count", "reason" => "region", "target" => actor.name().to_string());
//     //             single_broadcast(bytes.clone(), socket.clone(), actor.addr());
//     //         }
//     //     }
//     // }
//     // histogram!("corrosion.payload.send.lag.seconds", sent_at.elapsed().as_secs_f64(), "priority" => "high");
// }

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
                histogram!("corrosion.broadcast.sent.bytes", n as f64, "transport" => "udp");
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

// fn streaming_http_broadcast(
//     chunks: Vec<Bytes>,
//     client: &ClientPool,
//     addr: SocketAddr,
//     clock: &uhlc::HLC,
// ) {
//     let start = Instant::now();
//     debug!(
//         "streaming http broadcast, first bytes: {:?}",
//         chunks.iter().map(|chunk| chunk[0]).collect::<Vec<u8>>()
//     );

//     let req = client.request(http_broadcast_request(
//         addr,
//         clock,
//         hyper::Body::wrap_stream(futures::StreamExt::inspect(
//             futures::stream::iter(chunks.into_iter().map(Ok::<_, std::io::Error>)),
//             |r| {
//                 if let Ok(b) = r {
//                     histogram!("corrosion.broadcast.sent.bytes", b.len() as f64, "transport" => "http-stream");
//                 }
//             },
//         )),
//     ));

//     tokio::spawn(async move {
//         match req.await {
//             Ok(res) => {
//                 histogram!("corrosion.gossip.broadcast.response.time.seconds", start.elapsed().as_secs_f64(), "status" => res.status().to_string(), "kind" => "streaming");
//             }
//             Err(e) => error!("error sending priority broadcast (streaming): {e}"),
//         }
//     });
// }

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
                histogram!("corrosion.broadcast.sent.bytes", len as f64, "transport" => "http");
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
                histogram!("corrosion.gossip.broadcast.response.time.seconds", start.elapsed().as_secs_f64(), "status" => res.status().to_string(), "kind" => "single");
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
