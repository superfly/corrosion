use std::{
    cmp,
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    num::NonZeroU32,
    ops::RangeInclusive,
    time::Duration,
};

use bytes::{BufMut, Bytes, BytesMut};
use corro_types::{
    actor::{ActorId, ClusterId},
    agent::{Agent, Bookie},
    base::{CrsqlDbVersion, CrsqlSeq},
    broadcast::{
        ChangeId, ChangeSource, ChangeV1, ChangesetId, PlumtreeInput, PlumtreeMsg, PlumtreeMsgV1,
        PlumtreeUpdates, UniPayload, UniPayloadV1,
    },
    channel::{bounded, CorroReceiver, CorroSender},
};
use governor::{Quota, RateLimiter};
use indexmap::IndexMap;
use metrics::counter;
use plum_foca::{
    Payload, PeerTopologyInfo, PlumPrio, PlumtreeState, PlumtreeStats, SeenStore, Timer,
};
use rangemap::RangeInclusiveSet;
use speedy::Writable;
use strum::EnumDiscriminants;
use tokio::{sync::mpsc, task::JoinSet, time::interval};
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{error, info, trace, warn};
use tripwire::Tripwire;

use crate::{
    agent::util::log_at_pow_10,
    broadcast::{try_transmit_uni, TimerSpawner, TransmitError, TransmitRateLimiter},
    transport::TransportExt,
};

#[derive(Debug)]
struct SeenEntry {
    seqs: Option<RangeInclusiveSet<CrsqlSeq>>,
    last_seq: Option<CrsqlSeq>,
    round: plum_foca::Round,
    duplicate_count: u32,
}

struct ChangeSeenStore {
    entries: IndexMap<(ActorId, CrsqlDbVersion), SeenEntry>,
    max_entries: usize,
    bookie: Bookie,
}

impl ChangeSeenStore {
    fn new(max_entries: usize, bookie: Bookie) -> Self {
        Self {
            entries: IndexMap::new(),
            max_entries,
            bookie,
        }
    }
}

impl SeenStore<ChangeId> for ChangeSeenStore {
    fn evict_if_needed(&mut self) {
        if self.entries.len() > self.max_entries {
            self.entries.drain(0..self.entries.len() - self.max_entries);
        }
    }

    fn contains(&self, id: &ChangeId) -> bool {
        self.contains_local(id) || self.contains_booked(id)
    }

    fn observe(&mut self, id: ChangeId, round: plum_foca::Round) -> Option<u32> {
        let actor_id = id.actor_id;
        match &id.changeset_id {
            ChangesetId::Full {
                version,
                seqs,
                last_seq,
            } => match self.entries.entry((actor_id, *version)) {
                indexmap::map::Entry::Vacant(e) => {
                    let incoming = RangeInclusiveSet::from_iter([RangeInclusive::from(*seqs)]);
                    e.insert(SeenEntry {
                        seqs: Some(incoming),
                        last_seq: Some(*last_seq),
                        round,
                        duplicate_count: 0,
                    });

                    return None;
                }
                indexmap::map::Entry::Occupied(mut e) => {
                    let entry = e.get_mut();
                    entry.round = cmp::max(entry.round, round);

                    if entry.seqs.is_none() {
                        entry.duplicate_count += 1;
                        return Some(entry.duplicate_count);
                    }

                    let incoming = RangeInclusive::from(*seqs);
                    let stored = entry.seqs.as_mut().unwrap();
                    if stored.gaps(&incoming).next().is_none() {
                        let full = CrsqlSeq(0)..=*last_seq;
                        let is_complete = stored.gaps(&full).next().is_none();

                        // if we seen this partial change, but we are still incomplete. We don't want partials to cause a prune
                        if !is_complete {
                            return Some(0);
                        }

                        entry.duplicate_count += 1;
                        return Some(entry.duplicate_count);
                    }

                    stored.insert(incoming);
                    return None;
                }
            },
            ChangesetId::Empty { versions } => {
                let min_seen = versions
                    .clone()
                    .map(|version| {
                        if let Some(entry) = self.entries.get_mut(&(actor_id, version)) {
                            entry.last_seq = None;
                            entry.duplicate_count += 1;
                            return entry.duplicate_count;
                        }

                        self.entries.insert(
                            (actor_id, version),
                            SeenEntry {
                                seqs: None,
                                last_seq: None,
                                round: round,
                                duplicate_count: 0,
                            },
                        );

                        return 0;
                    })
                    .min()
                    .unwrap_or(0);

                // there's at least one version are seeing for the first time
                if min_seen == 0 {
                    return None;
                } else {
                    return Some(min_seen);
                }
            }
        };
    }
}

impl ChangeSeenStore {
    fn contains_booked(&self, id: &ChangeId) -> bool {
        let actor_id = id.actor_id;
        if let Some(bookie) = self.bookie.get(&actor_id) {
            match &id.changeset_id {
                ChangesetId::Full { version, seqs, .. } => {
                    bookie.read().contains(*version, Some(*seqs))
                }
                ChangesetId::Empty { versions, .. } => versions
                    .clone()
                    .all(|version| bookie.read().contains(version, None)),
            }
        } else {
            false
        }
    }

    fn contains_local(&self, id: &ChangeId) -> bool {
        let actor_id = id.actor_id;
        match &id.changeset_id {
            ChangesetId::Full { version, seqs, .. } => {
                let entry = self.entries.get(&(actor_id, *version));
                let Some(entry) = entry else {
                    return false;
                };
                entry
                    .seqs
                    .as_ref()
                    .map(|old_seqs| {
                        let incoming = RangeInclusive::from(*seqs);
                        old_seqs.gaps(&incoming).count() == 0
                    })
                    .unwrap_or(false)
            }
            ChangesetId::Empty { versions, .. } => versions.clone().all(|version| {
                self.entries
                    .get(&(actor_id, version))
                    .map(|e| e.seqs.is_none())
                    .unwrap_or(false)
            }),
        }
    }
}

/// Implements `plum_foca::Runtime` for Corrosion, bridging the generic protocol
/// to Corrosion's transport, change processing, and timer infrastructure.
struct CorrosionPlumtreeRuntime {
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId, ActorId>>,
    tx_msgs: CorroSender<(PlumPrio, Vec<ActorId>, PlumtreeMsgV1)>,
}

impl CorrosionPlumtreeRuntime {
    fn new(
        tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
        timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId, ActorId>>,
        tx_msgs: CorroSender<(PlumPrio, Vec<ActorId>, PlumtreeMsgV1)>,
    ) -> Self {
        Self {
            tx_changes,
            timer_spawner,
            tx_msgs,
        }
    }
}

impl plum_foca::Runtime<ChangeId, ChangeV1, ActorId> for CorrosionPlumtreeRuntime {
    fn send_all(
        &mut self,
        peers: Vec<ActorId>,
        msg: plum_foca::PlumtreeMsg<ChangeId, ChangeV1, ActorId>,
        priority: PlumPrio,
    ) {
        if let Err(e) = self.tx_msgs.try_send((priority, peers, msg)) {
            error!("plumtree: could not send message: {e}");
        }
    }

    fn send(&mut self, to: ActorId, msg: PlumtreeMsgV1, prio: PlumPrio) {
        if let Err(e) = self.tx_msgs.try_send((prio, vec![to], msg)) {
            error!("plumtree: could not send message: {e}");
        }
    }

    fn deliver(&mut self, payload: ChangeV1) {
        let tx = self.tx_changes.clone();
        tokio::spawn(async move {
            if let Err(e) = tx.send((payload, ChangeSource::Broadcast)).await {
                error!("plumtree: could not deliver change: {e}");
            }
        });
    }

    fn schedule(&mut self, timer: plum_foca::Timer<ChangeId, ActorId>, after: Duration) {
        self.timer_spawner.spawn((after, timer));
    }

    fn notify(&mut self, notification: plum_foca::Notification<ChangeId, ActorId>) {
        trace!("plumtree notification: {notification:?}");
    }
}

pub async fn spawn_plumtree_loop<T: TransportExt + Clone + Send + 'static>(
    agent: Agent,
    transport: T,
    rx_plumtree: CorroReceiver<PlumtreeInput>,
    rx_plumtree_updates: CorroReceiver<PlumtreeUpdates>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    tripwire: Tripwire,
) {
    let config = plum_foca::Config {
        ihave_timeout: Duration::from_millis(200),
        optimization_threshold: Some(5),
        max_cached_payloads: 4096,
        // todo: have plumtree decide on these values
        max_eager: 5,
        max_lazy: 10,
        min_lazy: 5,
        prune_threshold: 3,
        max_received_entries: 10000,
    };

    plumtree_loop(
        agent,
        transport,
        rx_plumtree,
        rx_plumtree_updates,
        tx_changes,
        config,
        tripwire,
    )
    .await;
}

/// The main Plumtree event loop. Runs alongside the existing `handle_broadcasts`.
pub async fn plumtree_loop<T: TransportExt + Clone + Send + 'static>(
    agent: Agent,
    transport: T,
    mut rx_plumtree: CorroReceiver<PlumtreeInput>,
    mut rx_plumtree_updates: CorroReceiver<PlumtreeUpdates>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    config: plum_foca::Config,
    mut tripwire: Tripwire,
) -> PlumtreeStats {
    let seen = ChangeSeenStore::new(config.max_received_entries, agent.bookie().clone());
    let mut state: PlumtreeState<ChangeId, ChangeV1, ActorId, ChangeSeenStore> =
        PlumtreeState::new_with_store(agent.actor_id(), config, seen);

    let (plumtree_timer_tx, mut plumtree_timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(plumtree_timer_tx);

    let (tx_msgs, rx_msgs) = bounded(agent.config().perf.bcast_channel_len, "plumtree_msgs");

    let send_agent = agent.clone();
    let send_transport = transport.clone();
    let send_tripwire = tripwire.clone();
    tokio::spawn(send_messages_loop(
        send_agent,
        send_transport,
        rx_msgs,
        send_tripwire,
    ));

    let mut tick_interval = interval(Duration::from_millis(200));
    let mut maintenance_interval = interval(Duration::from_secs(10));

    let actors_keys: Vec<ActorId> = agent.members().read().states.keys().cloned().collect();

    let mut rt = CorrosionPlumtreeRuntime::new(tx_changes, timer_spawner, tx_msgs);

    let len = actors_keys.len();
    state.add_peers_bulk(actors_keys, &mut rt);
    info!("added {} peers to plumtree", len);

    #[derive(EnumDiscriminants)]
    #[strum_discriminants(derive(strum::IntoStaticStr))]
    enum Branch {
        Input(PlumtreeInput),
        Updates(PlumtreeUpdates),
        HandleTimer(Timer<ChangeId, ActorId>),
        IHaveTick,
        MaintenanceTick,
        // Metrics,
    }

    loop {
        let branch = tokio::select! {
            biased;
            _ = &mut tripwire => {
                info!("plumtree_loop: tripwire fired, shutting down");
                break;
            },
            updates = rx_plumtree_updates.recv() => match updates {
                Some(updates) => Branch::Updates(updates),
                None => {
                    warn!("plumtree_loop: updates channel closed");
                    break;
                }
            },
            input = rx_plumtree.recv() => match input {
                Some(input) => Branch::Input(input),
                None => {
                    warn!("plumtree_loop: input channel closed");
                    break;
                }
            },
            Some((timer, _seq)) = plumtree_timer_rx.recv() => {
                Branch::HandleTimer(timer)
            }
            // todo batch this on all timers
            _ = tick_interval.tick() => {
                Branch::IHaveTick
            }
            _ = maintenance_interval.tick() => {
                Branch::MaintenanceTick
            }
            // _ = metrics_interval.tick() => {
            //     Branch::Metrics
            // }
        };

        match branch {
            Branch::Input(input) => match input {
                PlumtreeInput::Wire(msg) => match msg {
                    PlumtreeMsgV1::Gossip(g) => {
                        state.handle_gossip(g, &mut rt);
                    }
                    PlumtreeMsgV1::IHave(ih) => {
                        state.handle_ihave(ih, &mut rt);
                    }
                    PlumtreeMsgV1::Graft(g) => {
                        state.handle_graft(g, &mut rt);
                    }
                    PlumtreeMsgV1::Prune(p) => {
                        state.handle_prune(p, &mut rt);
                    }
                },
                PlumtreeInput::Broadcast(change) => {
                    let id = change.message_id();
                    state.broadcast(id, change, &mut rt);
                }
            },
            Branch::Updates(updates) => match updates {
                PlumtreeUpdates::MemberUp { actor_id, addr: _ } => {
                    state.peer_up(actor_id, &mut rt);
                }
                PlumtreeUpdates::MemberDown(actor_id) => {
                    state.peer_down(&actor_id, &mut rt);
                }
            },
            Branch::HandleTimer(timer) => {
                state.timer_fired(timer, &mut rt);
            }
            Branch::IHaveTick => {
                state.tick(&mut rt);
            }
            Branch::MaintenanceTick => {
                let topo = plumtree_topology_map(&agent);
                state.maintain_topology(&mut rt, topo);
                state.seen_evict_if_needed();
            }
        }
    }

    return state.stats().clone();
}

#[derive(Debug)]
struct PendingPlumtreeSend {
    peers: Vec<ActorId>,
    payload: Bytes,
}

async fn send_messages_loop<T: TransportExt + Clone + Send + 'static>(
    agent: Agent,
    transport: T,
    mut rx_msgs: CorroReceiver<(PlumPrio, Vec<ActorId>, PlumtreeMsgV1)>,
    mut tripwire: Tripwire,
) {
    let cluster_id = agent.cluster_id();
    let max_queue_len = agent.config().perf.processing_queue_len;
    const MAX_INFLIGHT: usize = 500;

    let mut codec = LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();
    let mut ser_buf = BytesMut::new();
    let mut frame_buf = BytesMut::new();

    let mut p0_queue: VecDeque<PendingPlumtreeSend> = VecDeque::new();
    let mut p1_queue: VecDeque<PendingPlumtreeSend> = VecDeque::new();
    let mut join_set = JoinSet::new();
    let mut limited_log_count = 0;
    let mut drop_log_count = 0;

    let bytes_per_sec: TransmitRateLimiter = RateLimiter::direct(Quota::per_second(unsafe {
        NonZeroU32::new_unchecked(10 * 1024 * 1024)
    }))
    .with_middleware();

    let mut rate_limited = false;

    loop {
        let recv = tokio::select! {
            biased;
            _ = &mut tripwire => {
                info!("plumtree send loop: tripwire fired, shutting down");
                break;
            },
            _ = join_set.join_next(), if !join_set.is_empty() => {
                continue;
            },
            msg = rx_msgs.recv() => match msg {
                Some(msg) => Some(msg),
                None => {
                    warn!("plumtree send loop: message channel closed");
                    break;
                }
            },
        };

        if let Some((prio, peers, msg)) = recv {
            let payload = match encode_plumtree_wire(
                cluster_id,
                &mut codec,
                &mut ser_buf,
                &mut frame_buf,
                msg,
            ) {
                Ok(payload) => payload,
                Err(()) => continue,
            };
            let pending = PendingPlumtreeSend { peers, payload };
            match prio {
                PlumPrio::P0 => p0_queue.push_back(pending),
                PlumPrio::P1 => p1_queue.push_back(pending),
            }
        }

        drain_plumtree_queue(
            &agent,
            &transport,
            &bytes_per_sec,
            &mut join_set,
            &mut p0_queue,
            MAX_INFLIGHT,
            &mut rate_limited,
            &mut limited_log_count,
        );
        if !rate_limited {
            drain_plumtree_queue(
                &agent,
                &transport,
                &bytes_per_sec,
                &mut join_set,
                &mut p1_queue,
                MAX_INFLIGHT,
                &mut rate_limited,
                &mut limited_log_count,
            );
        }

        if drop_oldest_plumtree_send(&mut p0_queue, &mut p1_queue, max_queue_len).is_some() {
            log_at_pow_10(
                "dropped old plumtree message from send queue",
                &mut drop_log_count,
            );
            counter!("corro.plumtree.send.dropped").increment(1);
        }
    }

    info!("plumtree send loop is done");
}

fn encode_plumtree_wire(
    cluster_id: ClusterId,
    codec: &mut LengthDelimitedCodec,
    ser_buf: &mut BytesMut,
    frame_buf: &mut BytesMut,
    msg: PlumtreeMsgV1,
) -> Result<Bytes, ()> {
    ser_buf.clear();
    if let Err(e) = (UniPayload::V1 {
        data: UniPayloadV1::PlumTree(PlumtreeMsg::V1 { data: msg }),
        cluster_id,
    })
    .write_to_stream(ser_buf.writer())
    {
        error!("plumtree: failed to serialize wire msg: {e}");
        return Err(());
    }

    frame_buf.clear();
    if let Err(e) = codec.encode(ser_buf.split().freeze(), frame_buf) {
        error!("plumtree: failed to frame wire msg: {e}");
        return Err(());
    }

    Ok(frame_buf.split().freeze())
}

fn resolve_peer_addrs(agent: &Agent, peers: &[ActorId]) -> Vec<SocketAddr> {
    let members = agent.members().read();
    peers
        .iter()
        .filter_map(|id| members.states.get(id).map(|st| st.addr))
        .collect()
}

fn drain_plumtree_queue<T: TransportExt + Clone + Send + 'static>(
    agent: &Agent,
    transport: &T,
    bytes_per_sec: &TransmitRateLimiter,
    join_set: &mut JoinSet<()>,
    queue: &mut VecDeque<PendingPlumtreeSend>,
    max_inflight: usize,
    rate_limited: &mut bool,
    limited_log_count: &mut u64,
) {
    while !queue.is_empty() && join_set.len() < max_inflight {
        let pending = queue.pop_front().unwrap();
        let addrs = resolve_peer_addrs(agent, &pending.peers);
        if addrs.is_empty() {
            trace!(
                peers = ?pending.peers,
                "plumtree: no addresses for peers, dropping message"
            );
            continue;
        }

        let mut spawn_count = 0;
        let addr_count = addrs.len();
        for addr in addrs {
            if join_set.len() >= max_inflight {
                break;
            }

            match try_transmit_uni(
                bytes_per_sec,
                pending.payload.clone(),
                transport.clone(),
                addr,
            ) {
                Err(e) => match e {
                    TransmitError::TooBig(_) | TransmitError::InsufficientCapacity(_) => {
                        error!("plumtree: could not spawn transmission: {e}");
                    }
                    TransmitError::QuotaExceeded(_) => {
                        *rate_limited = true;
                        counter!("corro.plumtree.send.rate_limited").increment(1);
                        log_at_pow_10("plumtree sends rate limited", limited_log_count);
                        break;
                    }
                },
                Ok(fut) => {
                    join_set.spawn(async move {
                        fut.await;
                        counter!("corro.plumtree.send.total").increment(1);
                    });
                    spawn_count += 1;
                }
            }
        }

        if *rate_limited && spawn_count == 0 && addr_count > 0 {
            queue.push_front(pending);
            break;
        }

        counter!("corro.plumtree.send.spawn").increment(spawn_count);
    }
}

fn drop_oldest_plumtree_send(
    p0_queue: &mut VecDeque<PendingPlumtreeSend>,
    p1_queue: &mut VecDeque<PendingPlumtreeSend>,
    max: usize,
) -> Option<PendingPlumtreeSend> {
    if p0_queue.len() + p1_queue.len() <= max {
        return None;
    }
    // drop from low-priority queue first
    p1_queue.pop_back().or_else(|| p0_queue.pop_back())
}

/// Ring + RTT snapshot from [`Members`] for [`PlumtreeState::maintain_topology`].
fn plumtree_topology_map(agent: &Agent) -> HashMap<ActorId, PeerTopologyInfo> {
    let members = agent.members().read();
    members
        .states
        .iter()
        .map(|(id, st)| {
            let rtt_ms = members.avg_rtt_ms(id).unwrap_or(u64::MAX);
            (
                *id,
                PeerTopologyInfo {
                    ring: st.ring,
                    rtt_ms,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::setup;
    use crate::transport::{TransportError, TransportExt};
    use async_trait::async_trait;
    use bytes::Bytes;
    use corro_tests::test_config;
    use corro_types::{
        actor::Actor,
        base::{dbsr, CrsqlDbVersion, CrsqlSeq},
        broadcast::{ChangeV1, Changeset},
        members::Members,
    };
    use parking_lot::RwLock;

    use std::{collections::HashMap, net::SocketAddr, sync::Arc};
    // use plum_foca::PlumtreeMsg;
    use rand::seq::IndexedRandom;
    use rangemap::RangeInclusiveSet;
    use speedy::Readable;
    use tokio::task::JoinSet;
    use tokio_stream::StreamExt;
    use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
    use tokio_util::sync::CancellationToken;

    use rand::{rngs::StdRng, SeedableRng};

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
                let _ = tx.send(data).await;
            });
            Ok(())
        }

        async fn send_uni(&self, addr: SocketAddr, data: Bytes) -> Result<(), TransportError> {
            let tx = self.nodes.write().get(&addr).unwrap().clone();
            tokio::spawn(async move {
                let _ = tx.send(data).await;
            });
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_plumtree_broadcast_spread() -> eyre::Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _, _) = Tripwire::new_simple();
        let num_nodes = 10;
        let num_changes = 200;
        let transport = TestTransport::new();
        let config = Arc::new(RwLock::new(foca::Config::new_wan(
            num_nodes.try_into().unwrap(),
        )));

        println!("config: {:?}", config.read().max_transmissions);
        let mut processed_join_set = JoinSet::new();
        let mut plumtree_join_set = JoinSet::new();

        let mut tas: Vec<_> = Vec::new();
        let mut send_tas: Vec<_> = Vec::new();
        let mut members = Members::default();
        for _ in 0..num_nodes {
            let (_, test_conf) = test_config(|conf| conf.build())?;
            let (agent, opts) = setup(test_conf.clone(), tripwire.clone()).await?;
            members.add_member(&Actor::new(
                agent.actor_id(),
                agent.gossip_addr(),
                agent.clock().new_timestamp().into(),
                agent.cluster_id(),
                None,
            ));
            send_tas.push((agent.actor_id(), agent.tx_plumtree().clone()));
            tas.push((agent, opts));
        }

        let plum_config = plum_foca::Config {
            ihave_timeout: Duration::from_millis(500),
            optimization_threshold: Some(5),
            max_cached_payloads: 4096,
            max_eager: 5,
            min_lazy: 2,
            max_lazy: 10,
            prune_threshold: 2,
            max_received_entries: 10000,
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

            let mut transport_rx: mpsc::Receiver<Bytes> =
                transport.add_node(agent_clone.gossip_addr()).await;

            let tx_plumtree = agent_clone.tx_plumtree().clone();
            let transport_clone = transport.clone();
            let tripwire_clone = tripwire.clone();
            plumtree_join_set.spawn(async move {
                let agent_id = agent_clone.actor_id();
                let config = plum_foca::Config {
                    ihave_timeout: Duration::from_millis(500),
                    optimization_threshold: Some(5),
                    max_cached_payloads: 4096,
                    max_eager: 3,
                    min_lazy: 5,
                    max_lazy: 10,
                    prune_threshold: 3,
                    max_received_entries: 10000,
                };
                let stats = plumtree_loop(
                    agent_clone.clone(),
                    transport_clone,
                    opts.rx_plumtree,
                    opts.rx_plumtree_updates,
                    agent_clone.tx_changes().clone(),
                    config,
                    tripwire_clone,
                )
                .await;

                return (agent_id, stats);
            });

            let cancel_clone = cancel.clone();
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
                                    ..
                                }) = UniPayload::read_from_buffer(&frame)
                                {
                                    let PlumtreeMsg::V1 { data } = msg;
                                    tx_plumtree
                                        .send(PlumtreeInput::Wire(data))
                                        .await
                                        .ok();
                                }
                            }
                        }
                    }
                }
            });

            // let agent_clone: Agent = agent.clone();
            processed_join_set.spawn(async move {
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
                                        actor_id: _,
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
        let chunk_pause = Duration::from_millis(500);
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
                tx_plumtree
                    .send(PlumtreeInput::Broadcast(change))
                    .await
                    .unwrap();
            }

            tokio::time::sleep(chunk_pause).await;
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

        let results = processed_join_set.join_all().await;
        let plumtree_results = plumtree_join_set.join_all().await;

        let mut total_map = results
            .into_iter()
            .map(|(actor_id, duplicate, seen_map)| (actor_id, (duplicate, seen_map)))
            .collect::<HashMap<_, _>>();

        let plumstats_map = plumtree_results
            .into_iter()
            .map(|(agent_id, stats)| (agent_id, stats))
            .collect::<HashMap<_, _>>();
        for change in all_changes {
            let (_, seen_map) = total_map
                .entry(change.actor_id)
                .or_insert((0, RangeInclusiveSet::new()));
            seen_map
                .insert(change.changeset.versions().start()..=change.changeset.versions().end());
        }

        let total_expected = num_nodes as u64 * num_changes;
        let total_seen: u64 = total_map
            .values()
            .map(|(_, seen_map)| {
                seen_map
                    .iter()
                    .map(|v| v.end().0 - v.start().0 + 1)
                    .sum::<u64>() as u64
            })
            .sum();
        let extra_recvs: u64 = total_map
            .values()
            .map(|(duplicate, _)| *duplicate as u64)
            .sum();

        let (total_gossip, total_ihave, total_graft, total_prune) = plumstats_map.iter().fold(
            (0, 0, 0, 0),
            |(acc_gossip, acc_ihave, acc_graft, acc_prune), (_, stats)| {
                (
                    acc_gossip + stats.gossip,
                    acc_ihave + stats.ihave,
                    acc_graft + stats.graft,
                    acc_prune + stats.prune,
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
