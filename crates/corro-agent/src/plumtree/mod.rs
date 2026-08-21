use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    num::NonZeroU32,
    ops::RangeInclusive,
    time::{Duration, Instant},
};

use std::sync::Arc;

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
use indexmap::IndexMap;
use metrics::{counter, gauge, histogram};
use plum_foca::{Payload, PlumPrio, PlumtreeState, RttInfo, SeenStore, Timer};
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
    tx_msgs: CorroSender<(PlumPrio, Vec<ActorId>, PlumtreeMsgV1)>,
}

impl CorrosionPlumtreeRuntime {
    fn new(
        tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
        change_dict: Arc<ArcSwapOption<ZstdDicts>>,
        timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId, ActorId>>,
        tx_msgs: CorroSender<(PlumPrio, Vec<ActorId>, PlumtreeMsgV1)>,
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

pub(super) struct PlumtreeActor {
    agent: Agent,
    transport: Transport,
    rx_plumtree: CorroReceiver<PlumtreeInput>,
    rx_plumtree_updates: CorroReceiver<PlumtreeUpdates>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
}

impl PlumtreeActor {
    pub(super) fn new(
        agent: Agent,
        transport: Transport,
        rx_plumtree: CorroReceiver<PlumtreeInput>,
        rx_plumtree_updates: CorroReceiver<PlumtreeUpdates>,
        tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
    ) -> Self {
        Self {
            agent,
            transport,
            rx_plumtree,
            rx_plumtree_updates,
            tx_changes,
        }
    }

    pub(super) async fn run(&mut self, tripwire: Tripwire) -> eyre::Result<()> {
        let plumtree_config = self
            .agent
            .config()
            .gossip
            .plumtree()
            .cloned()
            .unwrap_or_default();

        let max_queue_len = self.agent.config().perf.processing_queue_len;

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
        };

        plumtree_loop(
            self.agent.clone(),
            self.transport.clone(),
            &mut self.rx_plumtree,
            &mut self.rx_plumtree_updates,
            self.tx_changes.clone(),
            config,
            tripwire,
        )
        .await
    }
}

pub async fn plumtree_loop(
    agent: Agent,
    transport: Transport,
    rx_plumtree: &mut CorroReceiver<PlumtreeInput>,
    rx_plumtree_updates: &mut CorroReceiver<PlumtreeUpdates>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
    config: plum_foca::Config,
    mut tripwire: Tripwire,
) -> eyre::Result<()> {
    let seen = ChangeSeenStore::new(config.max_received_entries, agent.bookie().clone());
    let mut state: PlumtreeState<ChangeId, PlumtreePayload, ActorId, ChangeSeenStore> =
        PlumtreeState::new_with_store(agent.actor_id(), config, seen);

    let (plumtree_timer_tx, mut plumtree_timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(plumtree_timer_tx);

    let (tx_msgs, rx_msgs) = bounded(agent.config().perf.bcast_channel_len, "plumtree_msgs");

    let send_agent = agent.clone();
    let send_transport = transport.clone();

    let mut sender_tasks = JoinSet::new();
    sender_tasks.spawn(send_messages_loop(send_agent, send_transport, rx_msgs));

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

            sender_result = sender_tasks.join_next(),
                if !sender_tasks.is_empty() =>
            {
                match sender_result {
                    Some(Ok(())) => {
                        return Err(eyre::eyre!(
                            "plumtree send loop exited unexpectedly"
                        ));
                    }
                    Some(Err(e)) => {
                        return Err(eyre::eyre!(
                            "plumtree send loop task failed: {e}"
                        ));
                    }
                    None => {
                        return Err(eyre::eyre!(
                            "plumtree sender task set became empty"
                        ));
                    }
                }
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

    while let Some(result) = sender_tasks.join_next().await {
        if let Err(e) = result {
            error!("plumtree send loop task failed to join: {e}");
        }
    }

    Ok(())
}

struct PendingPlumtreeSend {
    peers: Vec<ActorId>,
    payload: BytesMut,
}

async fn send_messages_loop(
    agent: Agent,
    transport: Transport,
    mut rx_msgs: CorroReceiver<(PlumPrio, Vec<ActorId>, PlumtreeMsgV1)>,
) {
    const MAX_INFLIGHT: usize = 500;
    const P1_GOSSIP_BATCH_INTERVAL: Duration = Duration::from_millis(10);
    const P1_GOSSIP_BATCH_CUTOFF: usize = 1024 * 1024;

    let cluster_id = agent.cluster_id();
    let max_queue_len = agent.config().perf.processing_queue_len;
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

    let mut p0_queue: VecDeque<PendingPlumtreeSend> = VecDeque::new();
    let mut p1_queue: VecDeque<PendingPlumtreeSend> = VecDeque::new();
    let mut p1_gossip_batch = PendingPlumtreeSend {
        peers: Vec::new(),
        payload: BytesMut::new(),
    };
    let mut gossip_batch_interval = interval(P1_GOSSIP_BATCH_INTERVAL);
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
            _ = join_set.join_next(), if !join_set.is_empty() => {
                continue;
            },
            _ = gossip_batch_interval.tick(), if batch_gossip => {
                if !p1_gossip_batch.payload.is_empty() {
                    p1_queue.push_back(PendingPlumtreeSend {
                        peers: std::mem::take(&mut p1_gossip_batch.peers),
                        payload: std::mem::take(&mut p1_gossip_batch.payload),
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
        let (prio, peers, msg) = msg;
        trace!("plumtree: msg: {msg:?}, peers: {peers:?}");
        let p1_gossip = matches!(&msg, PlumtreeMsgV1::Gossip(_));
        let payload =
            match encode_plumtree_wire(cluster_id, &mut codec, &mut ser_buf, &mut frame_buf, msg) {
                Ok(payload) => payload,
                Err(()) => continue,
            };

        if batch_gossip && p1_gossip {
            // gossip is sent to latest eager peers
            p1_gossip_batch.peers = peers;
            p1_gossip_batch.payload.extend_from_slice(&payload);
            if p1_gossip_batch.payload.len() >= P1_GOSSIP_BATCH_CUTOFF {
                p1_queue.push_back(PendingPlumtreeSend {
                    peers: std::mem::take(&mut p1_gossip_batch.peers),
                    payload: std::mem::take(&mut p1_gossip_batch.payload),
                });
            }
        } else {
            let pending = PendingPlumtreeSend {
                peers,
                payload: BytesMut::from(payload),
            };
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
            if join_set.len() >= max_inflight {
                break;
            }

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
}
