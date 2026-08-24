use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    num::NonZeroU32,
    ops::RangeInclusive,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwapOption;
use bytes::{BufMut, Bytes, BytesMut};
use corro_types::{
    actor::{ActorId, ClusterId},
    agent::{Agent, Bookie},
    base::{CrsqlDbVersion, CrsqlSeq},
    broadcast::{
        BroadcastV1, ChangeId, ChangeSource, ChangeV1, ChangesetId, PlumtreeInput, PlumtreeMsgV1,
        PlumtreePayload, PlumtreeStats, PlumtreeUpdates, PlumtreeWire, UniPayload, UniPayloadV1,
    },
    channel::{bounded, CorroReceiver, CorroSender},
    compress::ZstdDicts,
};
use governor::{Quota, RateLimiter};
use indexmap::{IndexMap, IndexSet};
use metrics::{counter, gauge, histogram};
use plum_foca::{Payload, PlumtreeState, Round, RttInfo, SeenStore, Timer};
use rangemap::RangeInclusiveSet;
use speedy::Writable;
use tokio::{
    sync::mpsc,
    task::JoinSet,
    time::{interval, MissedTickBehavior},
};
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{error, info, trace, warn};
use tripwire::Tripwire;

use crate::{
    agent::util::log_at_pow_10,
    broadcast::{try_transmit_uni, TimerSpawner, TransmitError, TransmitRateLimiter},
    transport::Transport,
};

#[derive(Debug)]
struct SeenEntry {
    seqs: Option<RangeInclusiveSet<CrsqlSeq>>,
    last_seq: Option<CrsqlSeq>,
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
            counter!("corro.plumtree.cache.drained").increment(1);
            self.entries.drain(0..self.entries.len() - self.max_entries);
        }
    }

    fn contains(&self, id: &ChangeId) -> bool {
        self.contains_local(id) || self.contains_booked(id)
    }

    fn size(&self) -> usize {
        self.entries.len()
    }

    fn observe(&mut self, id: ChangeId, _round: plum_foca::Round) -> Option<u32> {
        // Already applied via sync/apply or entry was dropped
        let already_booked = !self.has_cache_entry(&id) && self.contains_booked(&id);

        let actor_id = id.actor_id;
        let result = match &id.changeset_id {
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
                        duplicate_count: 0,
                    });

                    None
                }
                indexmap::map::Entry::Occupied(mut e) => {
                    let entry = e.get_mut();

                    if entry.seqs.is_none() {
                        entry.duplicate_count += 1;
                        return Some(entry.duplicate_count);
                    }

                    let incoming = RangeInclusive::from(*seqs);
                    let stored = entry.seqs.as_mut().unwrap();

                    if stored.gaps(&incoming).next().is_none() {
                        // we already seen this seqs, increase duplicate count if change is complete
                        let full = CrsqlSeq(0)..=*last_seq;
                        let is_complete = stored.gaps(&full).next().is_none();

                        // if we seen this partial change, but we are still incomplete, return zero duplicates.
                        // We don't want partials to cause a prune while we are in the middle of receiving the change.
                        if !is_complete {
                            counter!("corro.plumtree.partial.duplicate").increment(1);
                            return Some(0);
                        }

                        entry.duplicate_count += 1;
                        return Some(entry.duplicate_count);
                    }

                    stored.insert(incoming);
                    None
                }
            },
            // Empty changesets are actually not used rn
            ChangesetId::Empty { versions } => {
                let min_seen = versions
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
                                duplicate_count: 0,
                            },
                        );

                        0
                    })
                    .min()
                    .unwrap_or(0);

                // there's at least one version are seeing for the first time
                if min_seen == 0 {
                    None
                } else {
                    Some(min_seen)
                }
            }
        };

        if already_booked {
            // return zero so it isn't re-delivered
            counter!("corro.plumtree.change.synced").increment(1);
            Some(0)
        } else {
            result
        }
    }
}

impl ChangeSeenStore {
    fn contains_booked(&self, id: &ChangeId) -> bool {
        let actor_id = id.actor_id;
        self.bookie.get(&actor_id).is_some_and(|booked| {
            let bookedr = booked.read();
            match &id.changeset_id {
                ChangesetId::Full { version, seqs, .. } => bookedr.contains(*version, Some(*seqs)),
                ChangesetId::Empty { versions, .. } => versions
                    .clone()
                    .all(|version| bookedr.contains(version, None)),
            }
        })
    }

    fn has_cache_entry(&self, id: &ChangeId) -> bool {
        match &id.changeset_id {
            ChangesetId::Full { version, .. } => {
                self.entries.contains_key(&(id.actor_id, *version))
            }
            ChangesetId::Empty { versions, .. } => versions
                .clone()
                .any(|version| self.entries.contains_key(&(id.actor_id, version))),
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
    tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
    change_dict: Arc<ArcSwapOption<ZstdDicts>>,
    timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId, ActorId>>,
    tx_msgs: CorroSender<(Vec<ActorId>, PlumtreeMsgV1)>,
}

impl CorrosionPlumtreeRuntime {
    fn new(
        tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
        change_dict: Arc<ArcSwapOption<ZstdDicts>>,
        timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId, ActorId>>,
        tx_msgs: CorroSender<(Vec<ActorId>, PlumtreeMsgV1)>,
    ) -> Self {
        Self {
            tx_changes,
            change_dict,
            timer_spawner,
            tx_msgs,
        }
    }
}

impl plum_foca::Runtime<ChangeId, PlumtreePayload, ActorId> for CorrosionPlumtreeRuntime {
    fn send_all(
        &mut self,
        peers: Vec<ActorId>,
        msg: plum_foca::PlumtreeMsg<ChangeId, PlumtreePayload, ActorId>,
    ) {
        if let Err(e) = self.tx_msgs.try_send((peers, msg)) {
            error!("plumtree: could not send message: {e}");
        }
    }

    fn send(&mut self, to: ActorId, msg: PlumtreeMsgV1) {
        if let Err(e) = self.tx_msgs.try_send((vec![to], msg)) {
            error!("plumtree: could not send message: {e}");
        }
    }

    fn deliver(&mut self, payload: PlumtreePayload) {
        let dicts = self.change_dict.load_full();
        let compressed = payload.bcast.is_compressed();
        let change = match payload.bcast.into_change(dicts.as_deref()) {
            Ok(change) => change,
            Err(e) => {
                error!("plumtree: could not decode delivered broadcast: {e}");
                return;
            }
        };
        let original_bcast = compressed.then_some(payload.bcast);
        let tx = self.tx_changes.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(
                Duration::from_secs(1),
                tx.send((change, ChangeSource::Broadcast, original_bcast)),
            )
            .await
            {
                Ok(Err(e)) => error!("plumtree: could not deliver change: {e}"),
                Err(_) => error!("plumtree: timed out delivering change after 1s"),
                Ok(Ok(())) => {}
            }
        });
    }

    fn schedule(&mut self, timer: plum_foca::Timer<ChangeId, ActorId>, after: Duration) {
        self.timer_spawner.spawn((after, timer));
    }

    fn notify(&mut self, notification: plum_foca::Notification<'_, ChangeId, ActorId>) {
        trace!("plumtree notification: {notification:?}");
        match notification {
            plum_foca::Notification::PeerMovedToEager(_) => {
                counter!("corro.plumtree.peer_to_eager").increment(1);
            }
            plum_foca::Notification::PeerMovedToLazy(_) => {
                counter!("corro.plumtree.peer_to_lazy").increment(1);
            }
            plum_foca::Notification::PeerDroppedFromEager(_) => {
                counter!("corro.plumtree.peer_dropped_from_eager").increment(1);
            }
            plum_foca::Notification::PeerEvictedFromLazy(_) => {
                counter!("corro.plumtree.peer_evicted").increment(1);
            }
            plum_foca::Notification::DuplicateMessage(_) => {
                counter!("corro.plumtree.duplicate_message").increment(1);
            }
            plum_foca::Notification::PayloadNotCached(_) => {
                counter!("corro.plumtree.payload_not_cached").increment(1);
            }
            plum_foca::Notification::MessageMissing(count) => {
                counter!("corro.plumtree.message_missing").increment(count as u64);
            }
            plum_foca::Notification::PruneSuppressed(_) => {
                counter!("corro.plumtree.prune_suppressed").increment(1);
            }
            plum_foca::Notification::Rebalance => {
                counter!("corro.plumtree.rebalance.total").increment(1);
            }
        }
    }

    fn now(&self) -> Instant {
        Instant::now()
    }
}

pub async fn spawn_plumtree_loop(
    agent: Agent,
    transport: Transport,
    rx_plumtree: CorroReceiver<PlumtreeInput>,
    rx_plumtree_updates: CorroReceiver<PlumtreeUpdates>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
    tripwire: Tripwire,
) {
    let plumtree_config = agent
        .config()
        .gossip
        .plumtree()
        .cloned()
        .unwrap_or_default();

    let max_queue_len = agent.config().perf.processing_queue_len;

    let config = plum_foca::Config {
        ihave_timeout: Duration::from_millis(150),
        optimization_threshold: plumtree_config.optimization_threshold,
        max_cached_payloads: max_queue_len,
        num_eager: None,
        min_lazy: None,
        max_lazy: None,
        prune_threshold: plumtree_config.prune_threshold,
        max_received_entries: max_queue_len,
        prune_throttle: plumtree_config.prune_throttle_secs.map(Duration::from_secs),
        eager_ratios: plumtree_config.eager_ratios,
        ring_locked_radius: plumtree_config.ring_locked_radius,
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

pub async fn plumtree_loop(
    agent: Agent,
    transport: Transport,
    mut rx_plumtree: CorroReceiver<PlumtreeInput>,
    mut rx_plumtree_updates: CorroReceiver<PlumtreeUpdates>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
    config: plum_foca::Config,
    mut tripwire: Tripwire,
) {
    let seen = ChangeSeenStore::new(config.max_received_entries, agent.bookie().clone());
    let mut state: PlumtreeState<ChangeId, PlumtreePayload, ActorId, ChangeSeenStore> =
        PlumtreeState::new_with_store(agent.actor_id(), config, seen);

    let (plumtree_timer_tx, mut plumtree_timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(plumtree_timer_tx);

    let (tx_msgs, rx_msgs) = bounded(agent.config().perf.bcast_channel_len, "plumtree_msgs");

    let send_agent = agent.clone();
    let send_transport = transport.clone();
    let send_msgs_handle = tokio::spawn(send_messages_loop(send_agent, send_transport, rx_msgs));

    // send out ihave digests to lazy peers
    let mut ihave_tick_interval = interval(Duration::from_millis(150));
    let mut maintenance_interval = interval(Duration::from_secs(60));
    maintenance_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut rt =
        CorrosionPlumtreeRuntime::new(tx_changes, agent.change_dict_slot(), timer_spawner, tx_msgs);

    let peers: Vec<_> = plumtree_topology_map(&agent).into_iter().collect();
    info!("added {} peers to plumtree from members", peers.len());
    state.add_peers_bulk_with_rtt(peers, &mut rt);

    enum Branch {
        Input(PlumtreeInput),
        Updates(PlumtreeUpdates),
        HandleTimer(Timer<ChangeId, ActorId>),
        IHaveTick,
        MaintenanceTick,
    }

    loop {
        let branch = tokio::select! {
            biased;
            _ = &mut tripwire => {
                info!("plumtree_loop: tripwire fired, sending shutdown prunes");
                state.handle_shutdown(&mut rt);
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
            _ = ihave_tick_interval.tick() => {
                Branch::IHaveTick
            }
            _ = maintenance_interval.tick() => {
                Branch::MaintenanceTick
            }
        };

        match branch {
            Branch::Input(input) => match input {
                PlumtreeInput::Wire(msg) => {
                    let msg_type: &'static str = (&msg).into();
                    trace!("plumtree: received {msg_type} message");
                    counter!("corro.plumtree.messages", "msg_type" => msg_type).increment(1);
                    match msg {
                        PlumtreeMsgV1::Gossip(g) => {
                            trace!("handling plumtree gossip");
                            histogram!("corro.plumtree.gossip.round").record(g.round as f64);
                            state.handle_gossip(g, &mut rt);
                        }
                        PlumtreeMsgV1::IHave(ih) => {
                            trace!("handling plumtree ihave");
                            state.handle_ihave(ih, &mut rt);
                        }
                        PlumtreeMsgV1::Graft(g) => {
                            trace!("handling plumtree graft");
                            let reason = if g.send {
                                "ihave_timeout"
                            } else {
                                "optimization"
                            };
                            counter!("corro.plumtree.graft", "reason" => reason).increment(1);
                            state.handle_graft(g, &mut rt);
                        }
                        PlumtreeMsgV1::Prune(p) => {
                            trace!("handling plumtree prune");
                            state.handle_prune(p, &mut rt);
                        }
                    }
                }
                PlumtreeInput::Broadcast(change) => {
                    let id = change.message_id();
                    // compress payload if needed (same as gossip broadcast path)
                    let compression_config = agent.config().gossip.compression_config();
                    let bcast = BroadcastV1::Change(change);
                    let bcast = if compression_config.enabled {
                        let level = compression_config.level;
                        let dict = agent.change_dict();
                        match tokio::task::spawn_blocking(move || {
                            bcast.compress_for_wire(level, dict.as_deref())
                        })
                        .await
                        {
                            Ok(bcast) => bcast,
                            Err(e) => {
                                error!("plumtree: compress_for_wire task panicked: {e}");
                                continue;
                            }
                        }
                    } else {
                        bcast
                    };

                    trace!("plumtree: broadcasting change: {id:?}");
                    state.broadcast(id.clone(), PlumtreePayload { id, bcast }, &mut rt);
                }
                PlumtreeInput::QueryStats(reply) => {
                    let stats = PlumtreeStats {
                        eager_peers: state.eager_peers().iter().copied().collect(),
                        lazy_peers: state.lazy_peers().iter().copied().collect(),
                        ring_locked_peers: state.ring_locked_peers().iter().copied().collect(),
                        known_peers: state.known_peers().len(),
                        num_eager_target: state.num_eager(),
                        min_lazy_target: state.min_lazy(),
                        max_lazy_target: state.max_lazy(),
                        lazy_queue_len: state.lazy_queue().len(),
                        seen_cache_size: state.seen_cache_size(),
                        payload_cache_size: state.payload_cache_size(),
                    };
                    let _ = reply.send(stats);
                }
            },
            Branch::Updates(updates) => match updates {
                PlumtreeUpdates::MemberUp {
                    actor_id,
                    addr: _,
                    ring,
                } => {
                    info!("plumtree: receieved member up: {actor_id}, ring: {ring:?}");
                    state.peer_up(actor_id, Some(RttInfo { ring }), &mut rt);
                }
                PlumtreeUpdates::MemberDown(actor_id) => {
                    info!("plumtree: receieved member down: {actor_id}");
                    state.peer_down(&actor_id, &mut rt);
                }
            },
            Branch::HandleTimer(timer) => {
                state.timer_fired(timer, &mut rt);
            }
            Branch::IHaveTick => {
                trace!("plumtree: sending out ihave digests");
                state.tick(&mut rt);
            }
            Branch::MaintenanceTick => {
                trace!("plumtree: updating peer topology");
                state.update_peer_topology(plumtree_topology_map(&agent), &mut rt);
                state.cache_evict_if_needed(&mut rt);

                gauge!("corro.plumtree.eager_peers").set(state.eager_peers().len() as f64);
                gauge!("corro.plumtree.lazy_peers").set(state.lazy_peers().len() as f64);
                gauge!("corro.plumtree.ring_locked_peers")
                    .set(state.ring_locked_peers().len() as f64);
                gauge!("corro.plumtree.known_peers").set(state.known_peers().len() as f64);
                gauge!("corro.plumtree.lazy_queue").set(state.lazy_queue().len() as f64);

                gauge!("corro.plumtree.payload_cache_size").set(state.payload_cache_size() as f64);
                gauge!("corro.plumtree.seen_cache_size").set(state.seen_cache_size() as f64);
            }
        }
    }

    drop(rt);
    if let Err(e) = send_msgs_handle.await {
        error!("plumtree send loop task failed to join: {e}");
    }
}

struct PendingPlumtreeSend {
    peers: Vec<ActorId>,
    payload: BytesMut,
    shed_key: ShedKey,
}

/// Kind of a queued message, used to break ties between equal rounds
/// when dropping queue items.
/// mirrors `PlumtreeMsg`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ShedKind {
    Gossip,
    Graft,
    IHave,
    Prune,
}

/// Shed priority for a queued send. Round first then kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShedKey(Round, ShedKind);

impl ShedKey {
    /// Classify a message: `true` means it belongs on the local queue.
    ///
    fn of(msg: &PlumtreeMsgV1) -> (bool, Self) {
        match msg {
            // Round 0 gossip is ours: either a fresh local broadcast or a
            // graft reply serving our own cached payload.
            PlumtreeMsgV1::Gossip(g) if g.round == 0 => (true, Self(0, ShedKind::Gossip)),
            PlumtreeMsgV1::Gossip(g) => (false, Self(g.round, ShedKind::Gossip)),
            PlumtreeMsgV1::IHave(m) => {
                let round = m
                    .digests
                    .iter()
                    .map(|d| d.round)
                    .min()
                    .unwrap_or(Round::MAX);
                (false, Self(round, ShedKind::IHave))
            }
            PlumtreeMsgV1::Graft(m) => {
                let round = m
                    .requests
                    .iter()
                    .map(|r| r.round)
                    .min()
                    .unwrap_or(Round::MAX);
                (false, Self(round, ShedKind::Graft))
            }
            PlumtreeMsgV1::Prune(_) => (false, Self(Round::MAX, ShedKind::Prune)),
        }
    }

    fn label(&self, is_local: bool) -> &'static str {
        if is_local {
            return "local_gossip";
        }
        match self.1 {
            ShedKind::Gossip => "forwarded_gossip",
            ShedKind::Graft => "graft",
            ShedKind::IHave => "i_have",
            ShedKind::Prune => "prune",
        }
    }
}

async fn send_messages_loop(
    agent: Agent,
    transport: Transport,
    mut rx_msgs: CorroReceiver<(Vec<ActorId>, PlumtreeMsgV1)>,
) {
    const MAX_INFLIGHT: usize = 700;
    const GOSSIP_BATCH_INTERVAL: Duration = Duration::from_millis(10);
    const GOSSIP_BATCH_CUTOFF: usize = 1024 * 1024;

    let cluster_id = agent.cluster_id();
    let max_queue_len = agent.config().perf.plumtree_send_queue_len;
    let batch_gossip = agent
        .config()
        .gossip
        .plumtree()
        .map(|p| p.batch_gossip)
        .unwrap_or(false);

    let mut codec = LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();
    let mut ser_buf = BytesMut::new();
    let mut frame_buf = BytesMut::new();

    let mut local_queue: VecDeque<PendingPlumtreeSend> = VecDeque::new();
    let mut remote_queue: VecDeque<PendingPlumtreeSend> = VecDeque::new();

    let mut gossip_batch_peers: IndexSet<ActorId> = IndexSet::new();
    let mut gossip_batch_payload = BytesMut::new();
    let mut batch_shed_key = ShedKey(Round::MAX, ShedKind::Gossip);
    let mut gossip_batch_interval = interval(GOSSIP_BATCH_INTERVAL);

    let mut metrics_interval = interval(Duration::from_secs(10));
    metrics_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut join_set = JoinSet::new();
    let mut limited_log_count = 0;
    let mut drop_log_count = 0;

    let bytes_per_sec: TransmitRateLimiter = RateLimiter::direct(Quota::per_second(unsafe {
        NonZeroU32::new_unchecked(10 * 1024 * 1024)
    }))
    .with_middleware();

    loop {
        let msg = tokio::select! {
            biased;
            _ = metrics_interval.tick() => {
                gauge!("corro.plumtree.send_queue.len", "queue" => "local").set(local_queue.len() as f64);
                gauge!("corro.plumtree.send_queue.len", "queue" => "remote").set(remote_queue.len() as f64);
                gauge!("corro.plumtree.send.inflight").set(join_set.len() as f64);
                continue;
            },
            _ = join_set.join_next(), if !join_set.is_empty() => {
                continue;
            },
            _ = gossip_batch_interval.tick(), if batch_gossip => {
                if !gossip_batch_payload.is_empty() {
                    remote_queue.push_front(PendingPlumtreeSend {
                        peers: gossip_batch_peers.drain(..).collect(),
                        payload: std::mem::take(&mut gossip_batch_payload),
                        shed_key: std::mem::replace(
                            &mut batch_shed_key,
                            ShedKey(Round::MAX, ShedKind::Gossip),
                        ),
                    });
                }
                continue;
            },
            msg = rx_msgs.recv() => match msg {
                Some(msg) => msg,
                None => {
                    warn!("plumtree send loop: message channel closed");
                    break;
                }
            },
        };

        let mut rate_limited = false;
        let (peers, msg) = msg;
        trace!("plumtree: msg: {msg:?}, peers: {peers:?}");
        let (is_local, shed_key) = ShedKey::of(&msg);

        let batchable = !is_local && matches!(&msg, PlumtreeMsgV1::Gossip(_));
        let payload =
            match encode_plumtree_wire(cluster_id, &mut codec, &mut ser_buf, &mut frame_buf, msg) {
                Ok(payload) => payload,
                Err(()) => continue,
            };

        if batch_gossip && batchable {
            gossip_batch_peers.extend(peers);
            gossip_batch_payload.extend_from_slice(&payload);
            // A batch is only as sheddable as its most valuable member.
            batch_shed_key = batch_shed_key.min(shed_key);
            if gossip_batch_payload.len() >= GOSSIP_BATCH_CUTOFF {
                remote_queue.push_front(PendingPlumtreeSend {
                    peers: gossip_batch_peers.drain(..).collect(),
                    payload: std::mem::take(&mut gossip_batch_payload),
                    shed_key: std::mem::replace(
                        &mut batch_shed_key,
                        ShedKey(Round::MAX, ShedKind::Gossip),
                    ),
                });
            }
        } else {
            let pending = PendingPlumtreeSend {
                peers,
                payload: BytesMut::from(payload),
                shed_key,
            };
            if is_local {
                local_queue.push_front(pending);
            } else {
                remote_queue.push_front(pending);
            }
        }

        drain_plumtree_queue(
            &agent,
            &transport,
            &bytes_per_sec,
            &mut join_set,
            &mut local_queue,
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
                &mut remote_queue,
                MAX_INFLIGHT,
                &mut rate_limited,
                &mut limited_log_count,
            );
        }

        if let Some((was_local, shed)) =
            shed_plumtree_send(&mut local_queue, &mut remote_queue, max_queue_len)
        {
            log_at_pow_10("shed plumtree message from send queue", &mut drop_log_count);
            counter!("corro.plumtree.send.dropped", "kind" => shed.shed_key.label(was_local))
                .increment(1);
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
        data: UniPayloadV1::Plumtree(PlumtreeWire::V1 { data: msg }),
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

#[allow(clippy::too_many_arguments)]
fn drain_plumtree_queue(
    agent: &Agent,
    transport: &Transport,
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
            warn!(
                peers = ?pending.peers,
                "plumtree: no addresses for peers, dropping message"
            );
            continue;
        }

        trace!("plumtree: sending plumtree msg to {addrs:?}");
        let mut spawn_count = 0;
        let addr_count = addrs.len();
        for addr in addrs {
            match try_transmit_uni(
                bytes_per_sec,
                pending.payload.clone().freeze(),
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
                        log_at_pow_10("plumtree broadcasts rate limited", limited_log_count);
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

/// Drop items from queue, we drop from remote queue (based on ShedKey) first,
/// then local queue
fn shed_plumtree_send(
    local_queue: &mut VecDeque<PendingPlumtreeSend>,
    remote_queue: &mut VecDeque<PendingPlumtreeSend>,
    max: usize,
) -> Option<(bool, PendingPlumtreeSend)> {
    if local_queue.len() + remote_queue.len() <= max {
        return None;
    }

    let (was_local, queue) = if remote_queue.is_empty() {
        (true, local_queue)
    } else {
        (false, remote_queue)
    };

    // no need to loop over the full queue, look at last 1000 items
    let oldest_window = queue.len().saturating_sub(1000);
    let victim = queue
        .iter()
        .enumerate()
        .skip(oldest_window)
        // Highest shed key wins; the larger index breaks ties toward the oldest.
        .max_by_key(|(i, pending)| (pending.shed_key, *i))
        .map(|(i, _)| i)?;
    queue.remove(victim).map(|pending| (was_local, pending))
}

/// Ring + RTT snapshot from in-memory [`Members`].
fn plumtree_topology_map(agent: &Agent) -> HashMap<ActorId, RttInfo> {
    let members = agent.members().read();
    members
        .states
        .iter()
        .map(|(id, st)| (*id, RttInfo { ring: st.ring }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use corro_types::{
        actor::ActorId,
        base::{CrsqlDbVersion, CrsqlSeq, CrsqlSeqRange},
        broadcast::ChangesetId,
    };
    use rangemap::RangeInclusiveSet;

    fn full_change_id(
        actor_id: ActorId,
        version: u64,
        seq_start: u64,
        seq_end: u64,
        last_seq: u64,
    ) -> ChangeId {
        ChangeId {
            actor_id,
            changeset_id: ChangesetId::Full {
                version: CrsqlDbVersion(version),
                seqs: CrsqlSeqRange::new(CrsqlSeq(seq_start), CrsqlSeq(seq_end)),
                last_seq: CrsqlSeq(last_seq),
            },
        }
    }

    fn mark_booked(bookie: &Bookie, actor_id: ActorId, version: CrsqlDbVersion) {
        let booked = bookie.ensure(actor_id);
        let guard = bookie.write_lock_blocking();
        let mut tx = guard.write_tx(&booked);
        tx.compute_and_apply_gaps(RangeInclusiveSet::from_iter([version..=version]));
        tx.commit();
    }

    #[test]
    fn test_seen_store_duplicate_handling() {
        let actor_id = ActorId(uuid::Uuid::new_v4());
        let bookie = Bookie::new(Default::default());
        let mut store = ChangeSeenStore::new(100, bookie.clone());
        let round: plum_foca::Round = 0;

        // First complete gossip is new.
        let complete = full_change_id(actor_id, 1, 0, 0, 0);
        assert_eq!(store.observe(complete.clone(), round), None);

        // Exact duplicates of a complete change increase the dup count.
        assert_eq!(store.observe(complete.clone(), round), Some(1));
        assert_eq!(store.observe(complete.clone(), round), Some(2));

        // Incomplete partials: first seq is new, repeating it must not prune (Some(0)).
        let partial_a = full_change_id(actor_id, 2, 0, 0, 2);
        assert_eq!(store.observe(partial_a.clone(), round), None);
        assert_eq!(store.observe(partial_a, round), Some(0));

        // Filling remaining seqs is still new; only a full duplicate increments.
        let partial_b = full_change_id(actor_id, 2, 1, 2, 2);
        assert_eq!(store.observe(partial_b, round), None);
        let complete_v2 = full_change_id(actor_id, 2, 0, 2, 2);
        assert_eq!(store.observe(complete_v2, round), Some(1));

        // Bookie already has the change but cache is cold: seed cache and
        // report Some(0) so gossip is not re-delivered.
        mark_booked(&bookie, actor_id, CrsqlDbVersion(3));
        let booked_id = full_change_id(actor_id, 3, 0, 0, 0);
        assert!(!store.has_cache_entry(&booked_id));
        assert!(store.contains_booked(&booked_id));
        assert_eq!(store.observe(booked_id.clone(), round), Some(0));
        assert!(store.has_cache_entry(&booked_id));

        // After seeding, a second gossip for the same change is a real duplicate.
        assert_eq!(store.observe(booked_id, round), Some(1));
    }

    fn pending(round: Round, kind: ShedKind, tag: u8) -> PendingPlumtreeSend {
        PendingPlumtreeSend {
            peers: vec![ActorId(uuid::Uuid::new_v4())],
            payload: BytesMut::from(&[tag][..]),
            shed_key: ShedKey(round, kind),
        }
    }

    fn gossip(round: Round, tag: u8) -> PendingPlumtreeSend {
        pending(round, ShedKind::Gossip, tag)
    }

    fn prune(tag: u8) -> PendingPlumtreeSend {
        pending(Round::MAX, ShedKind::Prune, tag)
    }

    fn tag(shed: Option<(bool, PendingPlumtreeSend)>) -> u8 {
        shed.expect("expected a shed").1.payload[0]
    }

    #[test]
    fn shed_does_nothing_under_capacity() {
        let mut local = VecDeque::from(vec![gossip(0, 1)]);
        let mut remote = VecDeque::from(vec![gossip(9, 2)]);
        assert!(shed_plumtree_send(&mut local, &mut remote, 2).is_none());
        assert_eq!(local.len(), 1);
        assert_eq!(remote.len(), 1);
    }

    #[test]
    fn shed_picks_highest_round_first() {
        // Rounds queued out of order; the most-travelled one must go first.
        let mut local = VecDeque::new();
        let mut remote = VecDeque::from(vec![gossip(3, 1), gossip(12, 2), gossip(7, 3)]);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 2)), 2);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 1)), 3);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 0)), 1);
    }

    #[test]
    fn shed_breaks_ties_toward_oldest() {
        let mut local = VecDeque::new();
        let mut remote = VecDeque::new();
        for t in [1u8, 2, 3] {
            remote.push_front(gossip(5, t));
        }
        assert_eq!(
            tag(shed_plumtree_send(&mut local, &mut remote, 2)),
            1,
            "equal rounds should shed the oldest entry"
        );
    }

    #[test]
    fn shed_prune_goes_first_regardless_of_round() {
        // A PRUNE carries no round, so it must shed ahead of even the
        // highest-round gossip.
        let mut local = VecDeque::new();
        let mut remote = VecDeque::from(vec![gossip(Round::MAX, 1), prune(2), gossip(500, 3)]);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 2)), 2);
    }

    #[test]
    fn shed_orders_gossip_graft_ihave_at_equal_round() {
        // Same round: ihave sheds first, then graft, and gossip is kept longest.
        let mut local = VecDeque::new();
        let mut remote = VecDeque::from(vec![
            pending(4, ShedKind::Gossip, 1),
            pending(4, ShedKind::Graft, 2),
            pending(4, ShedKind::IHave, 3),
        ]);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 2)), 3);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 1)), 2);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 0)), 1);
    }

    #[test]
    fn shed_round_outranks_kind() {
        // A low-round ihave must survive a high-round gossip: round is primary.
        let mut local = VecDeque::new();
        let mut remote = VecDeque::from(vec![
            pending(1, ShedKind::IHave, 1),
            pending(30, ShedKind::Gossip, 2),
        ]);
        assert_eq!(tag(shed_plumtree_send(&mut local, &mut remote, 1)), 2);
    }

    #[test]
    fn shed_never_touches_local_while_remote_has_anything() {
        let mut local = VecDeque::from(vec![gossip(0, 1)]);
        let mut remote = VecDeque::from(vec![gossip(2, 2)]);
        let shed = shed_plumtree_send(&mut local, &mut remote, 1).expect("should shed");
        assert!(!shed.0, "should come off the remote queue");
        assert_eq!(shed.1.payload[0], 2);
        assert_eq!(local.len(), 1, "our own broadcast must survive");
    }

    #[test]
    fn shed_key_classifies_messages() {
        use corro_types::broadcast::PlumtreePayload;
        use plum_foca::{GossipMsg, GraftMsg, GraftRequest, IHaveDigest, IHaveMsg, PruneMsg};

        let actor = ActorId(uuid::Uuid::new_v4());
        let id = full_change_id(actor, 1, 0, 0, 0);

        let prune: PlumtreeMsgV1 = plum_foca::PlumtreeMsg::Prune(PruneMsg {
            sender: actor,
            triggered_by: None,
        });
        assert_eq!(
            ShedKey::of(&prune),
            (false, ShedKey(Round::MAX, ShedKind::Prune))
        );

        // IHave and Graft take the minimum round of their batch: a batch is
        // only as sheddable as its most valuable member.
        let ihave: PlumtreeMsgV1 = plum_foca::PlumtreeMsg::IHave(IHaveMsg {
            sender: actor,
            digests: vec![
                IHaveDigest {
                    id: id.clone(),
                    round: 9,
                },
                IHaveDigest {
                    id: id.clone(),
                    round: 3,
                },
            ],
        });
        assert_eq!(ShedKey::of(&ihave), (false, ShedKey(3, ShedKind::IHave)));

        let graft: PlumtreeMsgV1 = plum_foca::PlumtreeMsg::Graft(GraftMsg {
            sender: actor,
            send: true,
            requests: vec![
                GraftRequest {
                    id: id.clone(),
                    round: 7,
                },
                GraftRequest {
                    id: id.clone(),
                    round: 2,
                },
            ],
        });
        assert_eq!(ShedKey::of(&graft), (false, ShedKey(2, ShedKind::Graft)));

        // round 0 gossip is ours and routes to the local queue
        let payload = PlumtreePayload {
            id: id.clone(),
            bcast: corro_types::broadcast::BroadcastV1::CompressedChange(vec![0u8; 4]),
        };
        let local: PlumtreeMsgV1 = plum_foca::PlumtreeMsg::Gossip(GossipMsg {
            round: 0,
            sender: actor,
            payload: payload.clone(),
        });
        assert_eq!(ShedKey::of(&local), (true, ShedKey(0, ShedKind::Gossip)));

        let forwarded: PlumtreeMsgV1 = plum_foca::PlumtreeMsg::Gossip(GossipMsg {
            round: 6,
            sender: actor,
            payload,
        });
        assert_eq!(
            ShedKey::of(&forwarded),
            (false, ShedKey(6, ShedKind::Gossip))
        );
    }
}
