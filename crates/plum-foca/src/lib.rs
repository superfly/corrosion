use indexmap::{IndexMap, IndexSet};
use rand::Rng;
use rand::seq::IteratorRandom;
use speedy::{Readable, Writable};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use tracing::{debug, trace};

pub trait MessageId: Clone + Eq + Hash + Debug + Send + 'static {}

pub trait Payload: Clone + Debug + Send + 'static {
    type MessageId: MessageId;
    type NodeId: NodeId;
    fn message_id(&self) -> Self::MessageId;
    fn origin(&self) -> Self::NodeId;
}

pub trait NodeId: Clone + Eq + Hash + Ord + Debug + Send + 'static {}

impl<T> MessageId for T where T: Clone + Eq + Hash + Debug + Send + 'static {}
impl<T> NodeId for T where T: Clone + Eq + Hash + Ord + Debug + Send + 'static {}

pub type Round = u32;

#[derive(Clone, Copy)]
pub enum PlumPrio {
    P0,
    P1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RttInfo {
    pub ring: Option<u8>,
    pub rtt_ms: u64,
}

impl Default for RttInfo {
    fn default() -> Self {
        Self {
            ring: None,
            rtt_ms: u64::MAX,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RingBucket {
    Near,
    Mid,
    Far,
}

impl RingBucket {
    fn of(info: RttInfo) -> Self {
        match info.ring {
            Some(0 | 1) => Self::Near,
            Some(2 | 3) => Self::Mid,
            Some(_) | None => Self::Far,
        }
    }
}

/// Tunable parameters for the Plumtree protocol.
#[derive(Debug, Clone)]
pub struct Config {
    /// How long to wait after receiving an IHave before sending a GRAFT
    /// if the full message hasn't arrived yet.
    pub ihave_timeout: Duration,
    /// Optimization threshold in rounds. Controls when a node receiving
    /// a GOSSIP will attempt to GRAFT a shorter-path peer (via IHave)
    pub optimization_threshold: Option<Round>,
    /// Number of eager peers (fanout).
    pub num_eager: usize,
    /// Minimum number of lazy peers.
    pub min_lazy: usize,
    /// Maximum number of lazy peers.
    pub max_lazy: usize,
    /// Maximum number of times a message can be received before pruning sender.
    /// We trade some duplication for tree stability.
    pub prune_threshold: u32,
    /// cap on seen entries; oldest are evicted when exceeded.
    pub max_received_entries: usize,
    /// cap on cached payload used to respond to GRAFT requests.
    pub max_cached_payloads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ihave_timeout: Duration::from_secs(1),
            optimization_threshold: Some(3),
            num_eager: 5,
            min_lazy: 10,
            max_lazy: 15,
            prune_threshold: 1,
            max_received_entries: 10000,
            max_cached_payloads: 8192,
        }
    }
}

pub trait SeenStore<I: MessageId> {
    fn evict_if_needed(&mut self);
    fn contains(&self, id: &I) -> bool;
    fn observe(&mut self, id: I, round: Round) -> Option<u32>;
}

/// Timers produced by the protocol, these are handle with the `schedule` method.
/// The runtime schedules these externally and calls `PlumtreeState::timer_fired` when they expire.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Timer<I: MessageId, N: NodeId> {
    /// Fires after `config.ihave_timeout`. For each id still missing, the node
    /// sends a GRAFT request to senders.
    IHaveTimeoutBatch {
        ids: Vec<I>,
        retries: u32,
        senders: Vec<N>,
    },
}

/// Observable protocol events for metrics, logging, or diagnostics.
#[derive(Debug, Clone)]
pub enum Notification<I: MessageId, N: NodeId> {
    PeerMovedToEager(N),
    PeerMovedToLazy(N),
    DuplicateMessage(I),
    PayloadNotCached(I),
    MessageMissing(usize),
}

pub trait Runtime<I: MessageId, P: Payload<MessageId = I, NodeId = N>, N: NodeId> {
    /// Send a protocol message to a specific peer.
    fn send(&mut self, to: N, msg: PlumtreeMsg<I, P, N>, prio: PlumPrio);

    // Send a message to a group of peers
    fn send_all(&mut self, peers: Vec<N>, msg: PlumtreeMsg<I, P, N>, prio: PlumPrio);

    /// Deliver a received message to the application layer.
    fn deliver(&mut self, payload: P);

    /// Request the caller to schedule a timer. When the duration elapses,
    /// the caller must invoke `PlumtreeState::timer_fired`.
    fn schedule(&mut self, timer: Timer<I, N>, after: Duration);

    /// Observable protocol event.
    fn notify(&mut self, notification: Notification<I, N>);
}

/// Messages exchanged between Plumtree peers.
#[derive(Debug, Clone, Readable, Writable, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum PlumtreeMsg<I, P, N>
where
    I: MessageId,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
{
    Gossip(GossipMsg<I, P, N>),
    IHave(IHaveMsg<I, N>),
    Graft(GraftMsg<I, N>),
    Prune(PruneMsg<I, N>),
}

/// Full payload sent immediately to eager peers.
#[derive(Debug, Clone, Readable, Writable)]
pub struct GossipMsg<I, P, N>
where
    I: MessageId,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
{
    pub round: Round,
    pub sender: N,
    pub payload: P,
}

/// Digest-only batch sent to lazy peers on each tick.
#[derive(Debug, Clone, Readable, Writable)]
pub struct IHaveMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    pub digests: Vec<IHaveDigest<I>>,
}

#[derive(Debug, Clone, Readable, Writable)]
pub struct IHaveDigest<I: MessageId> {
    pub id: I,
    pub round: Round,
}

#[derive(Debug, Clone, Readable, Writable)]
pub struct GraftRequest<I: MessageId> {
    pub id: I,
    pub round: Round,
}

#[derive(Debug, Clone, Readable, Writable)]
pub struct GraftMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    /// When true, respond with GOSSIP for each cached request id.
    pub send: bool,
    pub requests: Vec<GraftRequest<I>>,
}

#[derive(Debug, Clone, Readable, Writable)]
pub struct PruneMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    pub triggered_by: I,
}

#[derive(Debug, Clone)]
struct MissingEntry<N: NodeId> {
    ihave_sender: N,
    round: Round,
}

#[derive(Debug)]
struct PayloadCache<I: MessageId, P: Payload> {
    entries: IndexMap<I, (P, Round)>,
    max_size: usize,
}

impl<I: MessageId, P: Payload> PayloadCache<I, P> {
    fn new(max_size: usize) -> Self {
        Self {
            entries: IndexMap::with_capacity(max_size),
            max_size,
        }
    }

    fn insert(&mut self, id: I, payload: P, round: Round) {
        self.entries.insert(id.clone(), (payload, round));
    }

    fn get(&self, id: &I) -> Option<&(P, Round)> {
        self.entries.get(id)
    }

    fn evict_if_needed(&mut self) {
        if self.entries.len() > self.max_size {
            self.entries.drain(0..self.entries.len() - self.max_size);
        }
    }
}

/// Full Plumtree protocol state for one local node.
#[derive(Debug)]
pub struct PlumtreeState<
    I: MessageId,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
    S: SeenStore<I>,
> {
    local_id: N,
    config: Config,

    eager_peers: HashSet<N>,
    lazy_peers: IndexSet<N>,
    known_peers: HashSet<N>,
    ring_locked: HashSet<N>,
    peer_topology: HashMap<N, RttInfo>,

    lazy_queue: Vec<IHaveDigest<I>>,
    missing: HashMap<I, MissingEntry<N>>,

    seen: S,
    cache: PayloadCache<I, P>,
}

impl<I: MessageId, P: Payload<MessageId = I, NodeId = N>, N: NodeId, S: SeenStore<I>>
    PlumtreeState<I, P, N, S>
{
    pub fn new_with_store(local_id: N, config: Config, seen: S) -> Self {
        let cache_size = config.max_cached_payloads;
        Self {
            local_id,
            config,
            eager_peers: HashSet::new(),
            lazy_peers: IndexSet::new(),
            known_peers: HashSet::new(),
            ring_locked: HashSet::new(),
            peer_topology: HashMap::new(),
            lazy_queue: Vec::new(),
            missing: HashMap::new(),
            seen,
            cache: PayloadCache::new(cache_size),
        }
    }

    pub fn ring_locked_peers(&self) -> &HashSet<N> {
        &self.ring_locked
    }

    pub fn has_message(&self, id: &I) -> bool {
        self.seen.contains(id)
    }

    pub fn eager_peers(&self) -> &HashSet<N> {
        &self.eager_peers
    }

    pub fn lazy_peers(&self) -> &IndexSet<N> {
        &self.lazy_peers
    }

    pub fn known_peers(&self) -> &HashSet<N> {
        &self.known_peers
    }

    pub fn lazy_queue(&self) -> &Vec<IHaveDigest<I>> {
        &self.lazy_queue
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn local_id(&self) -> &N {
        &self.local_id
    }

    // --- Protocol methods ---

    /// Handle an incoming GOSSIP message carrying a full payload.
    ///
    /// Unlike the paper, we do NOT promote the sender to eager on receipt.
    /// In a multi-sender network, the peer that forwarded sender A's
    /// message fast may be a poor path for sender B. Eager promotion
    /// only happens through intentional GRAFT (IHave timeout or
    /// optimization path).
    pub fn handle_gossip(&mut self, msg: GossipMsg<I, P, N>, rt: &mut impl Runtime<I, P, N>) {
        let GossipMsg {
            round,
            sender,
            payload,
        } = msg;
        let id = payload.message_id();

        if let Some(duplicates) = self.seen.observe(id.clone(), round) {
            if duplicates > self.config.prune_threshold && !self.ring_locked.contains(&sender) {
                trace!(?self.local_id, ?sender, "sending PRUNE due to duplicate gossip");
                rt.send(
                    sender.clone(),
                    PlumtreeMsg::Prune(PruneMsg {
                        sender: self.local_id.clone(),
                        triggered_by: id.clone(),
                    }),
                    PlumPrio::P1,
                );
                self.move_to_lazy(&sender, rt);
            }
            rt.notify(Notification::DuplicateMessage(id));
            return;
        }

        trace!(
            ?self.local_id, ?sender, ?id, "forwarding gossip to eager peers",
        );

        self.cache.insert(id.clone(), payload.clone(), round);
        rt.deliver(payload.clone());

        let next_round = round + 1;
        let peers: Vec<N> = self
            .eager_peers
            .iter()
            .filter(|p| **p != sender && **p != self.local_id && payload.origin() != **p)
            .cloned()
            .collect();
        let fwd_count = peers.len();
        if fwd_count > 0 {
            rt.send_all(
                peers,
                PlumtreeMsg::Gossip(GossipMsg {
                    round: next_round,
                    sender: self.local_id.clone(),
                    payload: payload.clone(),
                }),
                PlumPrio::P1,
            );
        }
        trace!(?self.local_id, ?id, fwd_count, "gossip forwarded to eager peers");

        self.enqueue_ihave(id.clone(), round);

        // if peer is not eager, ensure they are in lazy
        if !self.eager_peers.contains(&sender) {
            self.ensure_in_lazy(&sender);
        }

        if let Some(entry) = self.missing.remove(&id) {
            if let Some(optimization_threshold) = self.config.optimization_threshold {
                if entry.round + optimization_threshold < round && entry.ihave_sender != sender {
                    debug!(?id,
                        ihave_round = entry.round, gossip_round = round,
                        ihave_sender = ?entry.ihave_sender,
                        "local={:?} optimization → GRAFT shorter path", self.local_id);
                    rt.send(
                        entry.ihave_sender.clone(),
                        PlumtreeMsg::Graft(GraftMsg {
                            sender: self.local_id.clone(),
                            send: false,
                            requests: vec![GraftRequest {
                                id: id.clone(),
                                round: entry.round,
                            }],
                        }),
                        PlumPrio::P0,
                    );
                    self.move_to_eager(&entry.ihave_sender, rt);
                }
            }
        }
    }

    /// Handle an incoming IHave digest batch.
    ///
    /// For each digest we haven't already received, record it in the
    /// `missing` set and schedule one `IHaveTimeoutBatch` timer. If the full
    /// GOSSIP doesn't arrive before the timer fires, we'll GRAFT.
    pub fn handle_ihave(&mut self, msg: IHaveMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        let IHaveMsg { sender, digests } = msg;
        let count = digests.len();

        let mut senders = vec![sender.clone()];
        senders.extend(self.random_eager_peers(2));

        let mut new_ids = Vec::new();
        for digest in digests {
            if self.seen.contains(&digest.id) {
                continue;
            }
            if self.missing.contains_key(&digest.id) {
                continue;
            }

            new_ids.push(digest.id.clone());
            self.missing.insert(
                digest.id,
                MissingEntry {
                    ihave_sender: sender.clone(),
                    round: digest.round,
                },
            );
        }

        if !new_ids.is_empty() {
            debug!(
                local = ?self.local_id,
                ?sender,
                count,
                new_missing = new_ids.len(),
                total_missing = self.missing.len(),
                "handle_ihave → scheduled graft timeout batch"
            );
            rt.schedule(
                Timer::IHaveTimeoutBatch {
                    ids: new_ids,
                    retries: 0,
                    senders,
                },
                self.config.ihave_timeout,
            );
        }
    }

    /// Handle an incoming GRAFT request.
    ///
    /// The sender is asking us to add them back to our eager set and
    /// (re)send the full payload for each requested message.
    pub fn handle_graft(&mut self, msg: GraftMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        let GraftMsg {
            sender,
            send,
            requests,
        } = msg;

        self.move_to_eager(&sender, rt);

        if !send {
            return;
        }

        for req in requests {
            if let Some((payload, round)) = self.cache.get(&req.id).cloned() {
                debug!(
                    id = ?req.id,
                    "local={:?} sender={:?} graft → sending cached payload",
                    self.local_id,
                    sender
                );
                rt.send(
                    sender.clone(),
                    PlumtreeMsg::Gossip(GossipMsg {
                        round,
                        sender: self.local_id.clone(),
                        payload,
                    }),
                    PlumPrio::P0,
                );
            } else {
                debug!(
                    "local={:?} sender={:?} graft → NOT CACHED",
                    self.local_id, sender
                );
                rt.notify(Notification::PayloadNotCached(req.id));
            }
        }
    }

    /// Handle an incoming PRUNE.
    ///
    pub fn handle_prune(&mut self, msg: PruneMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        debug!(local = ?self.local_id, sender = ?msg.sender, triggered_by = ?msg.triggered_by, "handle_prune");
        if !self.ring_locked.contains(&msg.sender) {
            self.move_to_lazy(&msg.sender, rt);
        }
    }

    /// Originate a new message from this node.
    ///
    /// Marks it as received, caches the payload, sends GOSSIP to all
    /// eager peers (round 0), and enqueues IHave for lazy peers.
    pub fn broadcast(&mut self, id: I, payload: P, rt: &mut impl Runtime<I, P, N>) {
        debug!(
            ?id,
            eager = self.eager_peers.len(),
            lazy = self.lazy_peers.len(),
            "local={:?} broadcast originate",
            self.local_id
        );

        self.seen.observe(id.clone(), 0);
        self.cache.insert(id.clone(), payload.clone(), 0);

        let peers = self
            .eager_peers
            .iter()
            .filter(|p| **p != self.local_id)
            .cloned()
            .collect::<Vec<_>>();
        rt.send_all(
            peers,
            PlumtreeMsg::Gossip(GossipMsg {
                round: 0,
                sender: self.local_id.clone(),
                payload: payload.clone(),
            }),
            PlumPrio::P0,
        );

        self.enqueue_ihave(id, 0);
    }

    /// Handle a fired timer.
    ///
    /// `IHaveTimeoutBatch`: for each id still missing, send GRAFT and promote
    /// the target peer to eager; schedule retry at a shorter timeout.
    pub fn timer_fired(&mut self, timer: Timer<I, N>, rt: &mut impl Runtime<I, P, N>) {
        match timer {
            Timer::IHaveTimeoutBatch {
                ids,
                retries,
                senders,
            } => self.handle_ihave_timeout(ids, retries, senders, rt),
        }
    }

    fn handle_ihave_timeout(
        &mut self,
        ids: Vec<I>,
        retries: u32,
        senders: Vec<N>,
        rt: &mut impl Runtime<I, P, N>,
    ) {
        if senders.is_empty() {
            return;
        }

        // we weren't able to get a response from sender
        if retries >= senders.len() as u32 {
            rt.notify(Notification::MessageMissing(ids.len()));
            for id in ids {
                self.missing.remove(&id);
            }
            return;
        }

        let send_to = senders[retries as usize].clone();
        let mut graft_requests = Vec::new();
        let mut still_missing = Vec::new();

        for id in ids {
            if self.seen.contains(&id) {
                trace!("missing change already received, noop");
                self.missing.remove(&id);
                continue;
            }

            let Some(entry) = self.missing.get(&id).cloned() else {
                trace!("no longer missing, noop");
                continue;
            };

            graft_requests.push(GraftRequest {
                id: id.clone(),
                round: entry.round,
            });
            still_missing.push(id);
        }

        if !graft_requests.is_empty() {
            debug!(
                count = graft_requests.len(),
                ?send_to,
                "local={:?} IHave timeout → GRAFT batch",
                self.local_id
            );
            // todo: maybe move this after we actually receive a response?
            self.move_to_eager(&send_to, rt);
            for chunk in graft_requests.chunks(10) {
                rt.send(
                    send_to.clone(),
                    PlumtreeMsg::Graft(GraftMsg {
                        sender: self.local_id.clone(),
                        send: true,
                        requests: chunk.to_vec(),
                    }),
                    PlumPrio::P0,
                );
            }
        }

        if !still_missing.is_empty() {
            rt.schedule(
                Timer::IHaveTimeoutBatch {
                    ids: still_missing,
                    retries: retries + 1,
                    senders,
                },
                self.config.ihave_timeout / 2,
            );
        }
    }

    /// Periodic maintenance — flush all pending IHave digests to lazy peers.
    ///
    /// The caller should invoke this on a regular interval.
    pub fn tick(&mut self, rt: &mut impl Runtime<I, P, N>) {
        let peers: Vec<N> = self.lazy_peers.iter().cloned().collect();
        if let Some(digests) = self.drain_lazy_queue() {
            rt.send_all(
                peers,
                PlumtreeMsg::IHave(IHaveMsg {
                    sender: self.local_id.clone(),
                    digests,
                }),
                PlumPrio::P1,
            );
        }
    }

    fn random_eager_peers(&self, count: usize) -> Vec<N> {
        let mut rng = rand::rng();
        self.eager_peers
            .iter()
            .cloned()
            .choose_multiple(&mut rng, count)
    }

    // --- Peer membership ---

    /// Add multiple peers at once (e.g. during bootstrap).
    pub fn add_peers_bulk(&mut self, peers: Vec<N>, rt: &mut impl Runtime<I, P, N>) {
        let entries = peers.into_iter().map(|p| (p, RttInfo::default())).collect();
        self.add_peers_bulk_with_rtt(entries, rt);
    }

    /// Bootstrap peers together with RTT ring info for [`Self::rebalance`].
    pub fn add_peers_bulk_with_rtt(
        &mut self,
        peers: Vec<(N, RttInfo)>,
        rt: &mut impl Runtime<I, P, N>,
    ) {
        for (peer, info) in peers {
            if peer != self.local_id {
                self.known_peers.insert(peer.clone());
                self.peer_topology.insert(peer, info);
            }
        }
        self.rebalance(rt);
    }

    /// A new peer has come online and wants to join the overlay.
    pub fn peer_up(&mut self, peer: N, rtt: Option<RttInfo>, rt: &mut impl Runtime<I, P, N>) {
        if peer == self.local_id || self.known_peers.contains(&peer) {
            return;
        }

        self.peer_topology
            .insert(peer.clone(), rtt.unwrap_or_default());
        self.known_peers.insert(peer);
        self.rebalance(rt);
    }

    /// Refresh RTT ring info for known peers (e.g. after members recalculates rings).
    pub fn update_peer_topology(
        &mut self,
        updates: impl IntoIterator<Item = (N, RttInfo)>,
        rt: &mut impl Runtime<I, P, N>,
    ) {
        let mut changed = false;
        for (peer, info) in updates {
            let existing = self.peer_topology.get(&peer);
            if existing.is_none() || existing.unwrap().ring != info.ring {
                self.peer_topology.insert(peer, info);
                changed = true;
            }
        }

        if changed {
            self.rebalance(rt);
        }
    }

    pub fn peer_down(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        let was_eager = self.eager_peers.remove(peer);
        let was_lazy = self.lazy_peers.swap_remove(peer);
        self.known_peers.remove(peer);
        if was_eager || was_lazy {
            self.rebalance(rt);
        }
    }

    // --- Rebalance ---

    /// Clears existing lazy / eager peers and selects them again.
    ///
    /// Ring neighbors are always eager. Remaining eager slots are split across
    /// near, mid, and far RTT bucket. Locked peers count toward the bucket targets.
    ///
    /// TODO: leave special slots with for unknown rtt peers
    fn rebalance(&mut self, _rt: &mut impl Runtime<I, P, N>) {
        self.eager_peers.clear();
        self.lazy_peers.clear();

        if self.known_peers.is_empty() {
            return;
        }

        self.set_ring_neighbors();

        if self.known_peers.len() <= self.config.num_eager {
            self.eager_peers.extend(self.known_peers.iter().cloned());
            return;
        }

        // ring-locked peers are always eager so a node is never isolated.
        for p in self.ring_locked.iter().cloned() {
            self.eager_peers.insert(p);
        }

        let num_eager = self.config.num_eager - self.ring_locked.len();
        let near_pool = self.bucket_candidates(RingBucket::Near);
        let mid_pool = self.bucket_candidates(RingBucket::Mid);
        let far_pool = self.bucket_candidates(RingBucket::Far);

        // we mostly want eager peers to be low rtt, but throw in a mix of mid and far peers;
        // percentage is roughly 50% near, 30% mid, 20% far
        let (eager_near, eager_mid, eager_far) =
            Self::bucket_targets(num_eager, near_pool.len(), mid_pool.len(), far_pool.len());

        self.eager_peers
            .extend(near_pool.iter().take(eager_near).cloned());
        self.eager_peers
            .extend(mid_pool.iter().take(eager_mid).cloned());
        self.eager_peers
            .extend(far_pool.iter().take(eager_far).cloned());

        // same selection logic for lazy peers since they are the eager candidates
        let num_lazy = self.config.min_lazy;
        let (lazy_near, lazy_mid, lazy_far) = Self::bucket_targets(
            num_lazy,
            near_pool.len() - eager_near,
            mid_pool.len() - eager_mid,
            far_pool.len() - eager_far,
        );

        self.lazy_peers
            .extend(near_pool.iter().skip(eager_near).take(lazy_near).cloned());
        self.lazy_peers
            .extend(mid_pool.iter().skip(eager_mid).take(lazy_mid).cloned());
        self.lazy_peers
            .extend(far_pool.iter().skip(eager_far).take(lazy_far).cloned());

        trace!(
            local = ?self.local_id,
            eager = self.eager_peers.len(),
            lazy = self.lazy_peers.len(),
            ring_locked = self.ring_locked.len(),
            known = self.known_peers.len(),
            "rebalance complete"
        );
    }

    fn bucket_targets(
        num_eager: usize,
        near_cap: usize,
        mid_cap: usize,
        far_cap: usize,
    ) -> (usize, usize, usize) {
        let near = (num_eager * 55).div_ceil(100).min(near_cap);
        let mid = (num_eager.saturating_sub(near) * 70)
            .div_ceil(100)
            .min(mid_cap);
        let far = num_eager.saturating_sub(near + mid).min(far_cap);
        (near, mid, far)
    }

    fn peer_bucket(&self, p: &N) -> RingBucket {
        RingBucket::of(self.peer_topology.get(p).copied().unwrap_or_default())
    }

    fn peer_rtt(&self, p: &N) -> u64 {
        self.peer_topology
            .get(p)
            .map(|r| r.rtt_ms)
            .unwrap_or(u64::MAX)
    }

    fn bucket_candidates(&self, bucket: RingBucket) -> Vec<N> {
        let mut peers: Vec<N> = self
            .known_peers
            .iter()
            .filter(|p| !self.eager_peers.contains(*p) && self.peer_bucket(p) == bucket)
            .cloned()
            .collect();
        peers.sort_by_key(|p| (self.peer_rtt(p), p.clone()));
        peers
    }

    fn enqueue_ihave(&mut self, id: I, round: Round) {
        let digest = IHaveDigest { id, round };
        self.lazy_queue.push(digest);
    }

    fn drain_lazy_queue(&mut self) -> Option<Vec<IHaveDigest<I>>> {
        if self.lazy_queue.is_empty() {
            return None;
        }
        Some(std::mem::take(&mut self.lazy_queue))
    }

    /// Recompute ring neighbors from scratch based on current known_peers.
    ///
    /// Clears old ring_locked set and recalculates the two peers that
    /// are immediately before and after local_id in sorted order.
    fn set_ring_neighbors(&mut self) {
        self.ring_locked.clear();

        let mut peers: Vec<N> = self.known_peers.iter().cloned().collect();
        peers.push(self.local_id.clone());
        if peers.len() <= 1 {
            return;
        }

        peers.sort();
        if let Some(position) = peers.iter().position(|p| p == &self.local_id) {
            let len = peers.len();
            let after_idx = (position + 1) % len;
            let before_idx = (position + len - 1) % len;

            self.ring_locked.insert(peers[before_idx].clone());
            self.ring_locked.insert(peers[after_idx].clone());
        }
    }

    fn move_to_eager(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.contains(peer)
            || (!self.ring_locked.contains(peer) && self.eager_peers.len() >= self.config.num_eager)
        {
            return;
        }

        self.eager_peers.insert(peer.clone());
        let was_lazy = self.lazy_peers.swap_remove(peer);
        trace!(
            ?self.local_id,
            ?peer,
            was_lazy,
            "peer moved to eager"
        );
        rt.notify(Notification::PeerMovedToEager(peer.clone()));
    }

    fn move_to_lazy(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.ring_locked.contains(peer) {
            trace!(
                local = ?self.local_id,
                ?peer,
                "move_to_lazy skipped (ring-locked eager peer)"
            );
            return;
        }
        let was_eager = self.eager_peers.remove(peer);
        self.insert_into_lazy(peer.clone());
        trace!(
            ?self.local_id,
            ?peer,
            was_eager,
            "peer moved to lazy"
        );
        rt.notify(Notification::PeerMovedToLazy(peer.clone()));
    }

    /// Ensure a peer is at least in the lazy set so they receive IHave
    /// digests. Does nothing if the peer is already in eager or lazy.
    fn ensure_in_lazy(&mut self, peer: &N) {
        if self.eager_peers.contains(peer) || self.lazy_peers.contains(peer) {
            return;
        }
        self.insert_into_lazy(peer.clone());
    }

    /// Insert a peer into the lazy set, evicting a random non-ring-locked
    /// peer if at capacity. Returns true if the peer was inserted.
    ///
    fn insert_into_lazy(&mut self, peer: N) -> bool {
        if self.lazy_peers.contains(&peer) {
            return true;
        }

        if self.lazy_peers.len() < self.config.max_lazy {
            self.lazy_peers.insert(peer);
            return true;
        }

        // Lazy is full — evict a random non-ring-locked peer.
        if let Some(evicted) = self.evict_from_lazy() {
            trace!(
                local = ?self.local_id,
                ?evicted,
                new = ?peer,
                "lazy full → evicted peer to make room"
            );
            self.lazy_peers.insert(peer);
            true
        } else {
            debug!(
                local = ?self.local_id,
                ?peer,
                "lazy full and all ring-locked, peer dropped"
            );
            false
        }
    }

    /// Evict a random non-ring-locked peer from the lazy set.
    fn evict_from_lazy(&mut self) -> Option<N> {
        let evictable: Vec<usize> = self
            .lazy_peers
            .iter()
            .enumerate()
            .filter(|(_, p)| !self.ring_locked.contains(*p))
            .map(|(i, _)| i)
            .collect();

        if evictable.is_empty() {
            return None;
        }

        let idx = evictable[rand::rng().random_range(0..evictable.len())];
        let evicted = self.lazy_peers.swap_remove_index(idx);
        evicted
    }

    pub fn cache_evict_if_needed(&mut self) {
        self.seen.evict_if_needed();
        self.cache.evict_if_needed();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub(crate) struct TestMsgId(pub u64);

    pub(crate) type TestNodeId = u8;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct TestPayload(pub Vec<u8>);

    impl Payload for TestPayload {
        type MessageId = TestMsgId;
        type NodeId = TestNodeId;
        fn message_id(&self) -> Self::MessageId {
            TestMsgId(self.0[0] as u64)
        }

        fn origin(&self) -> Self::NodeId {
            0
        }
    }

    #[derive(Debug, Clone, Default)]
    struct TestSeenStore {
        entries: HashMap<TestMsgId, (Round, u32)>,
    }

    impl SeenStore<TestMsgId> for TestSeenStore {
        fn evict_if_needed(&mut self) {
            // no-op
        }

        fn contains(&self, id: &TestMsgId) -> bool {
            self.entries.contains_key(id)
        }

        fn observe(&mut self, id: TestMsgId, round: Round) -> Option<u32> {
            let existing = self.entries.get_mut(&id);
            println!("existing: {:?}", existing);
            if let Some((existing, seen)) = existing {
                *existing = round;
                *seen += 1;
                return Some(*seen);
            }

            self.entries.insert(id, (round, 1));
            None
        }
    }

    pub(crate) fn test_config() -> Config {
        Config {
            ihave_timeout: Duration::from_secs(3),
            optimization_threshold: Some(3),
            max_cached_payloads: 128,
            num_eager: 5,
            min_lazy: 10,
            max_lazy: 15,
            prune_threshold: 1,
            max_received_entries: 10000,
        }
    }

    fn graft_msg(
        sender: TestNodeId,
        send: bool,
        requests: Vec<(TestMsgId, Round)>,
    ) -> GraftMsg<TestMsgId, TestNodeId> {
        GraftMsg {
            sender,
            send,
            requests: requests
                .into_iter()
                .map(|(id, round)| GraftRequest { id, round })
                .collect(),
        }
    }

    fn state() -> PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore> {
        PlumtreeState::new_with_store(0u8, test_config(), TestSeenStore::default())
    }

    /// Accumulates all runtime calls for assertion in tests.
    #[derive(Debug, Default)]
    pub(crate) struct AccumulatingRuntime {
        pub sent: Vec<(TestNodeId, PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>)>,
        pub delivered: Vec<TestPayload>,
        pub scheduled: Vec<(Timer<TestMsgId, TestNodeId>, Duration)>,
        pub notifications: Vec<Notification<TestMsgId, TestNodeId>>,
    }

    impl Runtime<TestMsgId, TestPayload, TestNodeId> for AccumulatingRuntime {
        fn send_all(
            &mut self,
            peers: Vec<TestNodeId>,
            msg: PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
            _prio: PlumPrio,
        ) {
            for peer in peers {
                self.send(peer, msg.clone(), PlumPrio::P0);
            }
        }

        fn send(
            &mut self,
            to: TestNodeId,
            msg: PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
            _prio: PlumPrio,
        ) {
            self.sent.push((to, msg));
        }

        fn deliver(&mut self, payload: TestPayload) {
            self.delivered.push(payload);
        }

        fn schedule(&mut self, timer: Timer<TestMsgId, TestNodeId>, after: Duration) {
            self.scheduled.push((timer, after));
        }

        fn notify(&mut self, notification: Notification<TestMsgId, TestNodeId>) {
            self.notifications.push(notification);
        }
    }

    // --- helpers ---
    fn msg(id: u64) -> TestMsgId {
        TestMsgId(id)
    }

    fn payload(v: u8) -> TestPayload {
        TestPayload(vec![v])
    }

    /// Extract the inner GossipMsg from a PlumtreeMsg, panicking otherwise.
    fn unwrap_gossip(
        m: &PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
    ) -> &GossipMsg<TestMsgId, TestPayload, TestNodeId> {
        match m {
            PlumtreeMsg::Gossip(g) => g,
            other => panic!("expected Gossip, got {other:?}"),
        }
    }

    fn unwrap_prune(
        m: &PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
    ) -> &PruneMsg<TestMsgId, TestNodeId> {
        match m {
            PlumtreeMsg::Prune(p) => p,
            other => panic!("expected Prune, got {other:?}"),
        }
    }

    fn unwrap_graft(
        m: &PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
    ) -> &GraftMsg<TestMsgId, TestNodeId> {
        match m {
            PlumtreeMsg::Graft(g) => g,
            other => panic!("expected Graft, got {other:?}"),
        }
    }

    fn unwrap_ihave(
        m: &PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
    ) -> &IHaveMsg<TestMsgId, TestNodeId> {
        match m {
            PlumtreeMsg::IHave(ih) => ih,
            other => panic!("expected IHave, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------------
    // Membership
    // -----------------------------------------------------------------------

    #[test]
    fn peer_up_starts_eager() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        assert!(s.eager_peers.contains(&1));
        assert!(!s.lazy_peers.contains(&1));
    }

    #[test]
    fn peer_up_overflow_to_lazy() {
        let mut s = state(); // num_eager=5
        let mut rt = AccumulatingRuntime::default();
        for i in 1..=5 {
            s.peer_up(i, None, &mut rt);
        }
        assert_eq!(s.eager_peers.len(), 5);

        s.peer_up(6, None, &mut rt);
        assert!(s.known_peers.contains(&6));
        // Sixth peer may be ring-locked (must be eager) or placed in lazy.
        assert!(s.eager_peers.contains(&6) || s.lazy_peers.contains(&6));
    }

    #[test]
    fn peer_up_both_full_ignored() {
        let mut cfg = test_config();
        cfg.num_eager = 2;
        cfg.min_lazy = 2;
        cfg.max_lazy = 2;
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore> =
            PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, None, &mut rt); // eager
        s.peer_up(2, None, &mut rt); // eager
        s.peer_up(3, None, &mut rt); // lazy
        s.peer_up(4, None, &mut rt); // lazy
        s.peer_up(5, None, &mut rt); // one peer remains only in known_peers (caps exceeded)

        assert_eq!(s.known_peers.len(), 5);
        assert_eq!(s.eager_peers.len(), 2);
        assert_eq!(s.lazy_peers.len(), 2);
        let orphan = s
            .known_peers
            .iter()
            .filter(|p| !s.eager_peers.contains(*p) && !s.lazy_peers.contains(*p))
            .count();
        assert_eq!(orphan, 1);
    }

    #[test]
    fn peer_up_idempotent() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        s.peer_up(1, None, &mut rt);
        assert_eq!(s.eager_peers.len(), 1);
        assert_eq!(s.lazy_peers.len(), 0);
    }

    #[test]
    fn peer_down_removes_from_both_sets() {
        let mut cfg = test_config();
        cfg.num_eager = 5;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        s.peer_up(2, None, &mut rt);
        s.peer_down(&1, &mut rt);
        assert_eq!(s.eager_peers.len(), 1);
        assert!(s.eager_peers.contains(&2));
        s.peer_down(&2, &mut rt);
        assert!(s.eager_peers.is_empty());
        assert!(s.lazy_peers.is_empty());
    }

    #[test]
    fn handle_prune_does_not_demote_ring_locked_peer() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        assert!(s.ring_locked_peers().contains(&1));
        assert!(s.eager_peers.contains(&1));
        s.handle_prune(
            PruneMsg {
                sender: 1,
                triggered_by: TestMsgId(0),
            },
            &mut rt,
        );
        assert!(
            s.eager_peers.contains(&1),
            "ring-locked eager peer must stay eager after PRUNE"
        );
    }

    #[test]
    fn reconcile_ring_locked_promotes_lazy_locked_peer() {
        let mut cfg = test_config();
        cfg.num_eager = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        s.peer_up(2, None, &mut rt);
        assert_eq!(s.eager_peers.len(), 2);
        s.peer_up(3, None, &mut rt);
        assert!(s.ring_locked_peers().contains(&3));
        assert!(
            s.eager_peers.contains(&3),
            "ring successor/predecessor must stay eager"
        );
        assert!(!s.lazy_peers.contains(&3));
    }

    #[test]
    fn replenish_eager_after_prune() {
        let mut cfg = test_config();
        cfg.num_eager = 3;
        cfg.min_lazy = 5;
        cfg.max_lazy = 5;
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore> =
            PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, None, &mut rt); // eager
        s.peer_up(2, None, &mut rt); // eager
        s.peer_up(3, None, &mut rt); // eager (full)
        s.peer_up(4, None, &mut rt); // lazy
        s.peer_up(5, None, &mut rt); // lazy
        assert_eq!(s.eager_peers.len(), 3);
        assert_eq!(s.lazy_peers.len(), 2);

        // Receive a PRUNE from the non-locked eager peer (who that is depends on shuffle).
        let prune_sender = *s
            .eager_peers
            .iter()
            .find(|p| !s.ring_locked_peers().contains(*p))
            .expect("one non-locked eager slot-filled peer");
        assert!(!s.ring_locked_peers().contains(&prune_sender));
        s.handle_prune(
            PruneMsg {
                sender: prune_sender,
                triggered_by: msg(99),
            },
            &mut rt,
        );
        assert_eq!(s.eager_peers.len(), 2, "peer should be dropped from eager");
        assert_eq!(s.lazy_peers.len(), 3);
        assert!(!s.eager_peers.contains(&prune_sender));
        assert!(s.lazy_peers.contains(&prune_sender));
    }

    // -----------------------------------------------------------------------
    // broadcast (originate)
    // -----------------------------------------------------------------------

    #[test]
    fn broadcast_sends_to_eager_and_enqueues_lazy() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, None, &mut rt); // eager
        s.peer_up(2, None, &mut rt); // eager
        // manually move 3 to lazy
        s.lazy_peers.insert(3);

        s.broadcast(msg(42), payload(42), &mut rt);

        // Should have sent GOSSIP to eager peers 1 and 2
        assert_eq!(rt.sent.len(), 2);
        for (to, m) in &rt.sent {
            let g = unwrap_gossip(m);
            assert_eq!(g.payload.message_id(), msg(42));
            assert_eq!(g.round, 0);
            assert_eq!(g.sender, 0); // local_id
            assert!(s.eager_peers.contains(to));
        }

        // Lazy queue for peer 3 should have one digest
        assert_eq!(s.lazy_queue.len(), 1);

        // Message should be marked received
        assert!(s.has_message(&msg(42)));
    }

    #[test]
    fn handle_gossip_new_delivers_and_forwards() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(2, None, &mut rt); // eager
        s.peer_up(3, None, &mut rt); // eager
        s.lazy_peers.insert(4);

        // Receive a GOSSIP from peer 1 (not yet in our peer set)
        s.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 1,
                payload: payload(10),
            },
            &mut rt,
        );

        // Delivered once
        assert_eq!(rt.delivered.len(), 1);
        assert_eq!(rt.delivered[0].message_id(), msg(10));

        // Forwarded to eager peers 2, 3 (not sender 1)
        let gossip_targets: Vec<u8> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Gossip(_)))
            .map(|(to, _)| *to)
            .collect();
        assert!(gossip_targets.contains(&2));
        assert!(gossip_targets.contains(&3));
        assert!(!gossip_targets.contains(&1)); // not back to sender

        // Forwarded gossips have round + 1
        for (_, m) in &rt.sent {
            if let PlumtreeMsg::Gossip(g) = m {
                assert_eq!(g.round, 2);
                assert_eq!(g.sender, 0); // local_id
            }
        }

        // Sender 1 at least lazy (sync path does not promote gossip senders to eager).
        assert!(s.lazy_peers.contains(&1));

        // Lazy peer 4 has IHave enqueued
        assert_eq!(s.lazy_queue.len(), 1);

        // Marked as received
        assert!(s.has_message(&msg(10)));
    }

    #[test]
    fn handle_gossip_duplicate_sends_prune() {
        let mut cfg = test_config();
        cfg.num_eager = 3;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(10, None, &mut rt);
        // First receive
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 10,
                payload: payload(1),
            },
            &mut rt,
        );
        rt.sent.clear();
        rt.delivered.clear();
        rt.notifications.clear();

        // Known [10..=13]: ring locks 10 and 13; 11 is eager and not ring-locked.
        s.peer_up(11, None, &mut rt);
        s.peer_up(12, None, &mut rt);
        s.peer_up(13, None, &mut rt);
        s.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 11,
                payload: payload(1),
            },
            &mut rt,
        );

        // No delivery
        assert!(rt.delivered.is_empty());

        // PRUNE sent to duplicate sender 11
        assert_eq!(rt.sent.len(), 1);
        let (to, m) = &rt.sent[0];
        assert_eq!(*to, 11);
        let prune = unwrap_prune(m);
        assert_eq!(prune.sender, 0);
        assert_eq!(prune.triggered_by, msg(1));

        // Peer 11 demoted to lazy (12 replenished eager)
        assert!(s.lazy_peers.contains(&11));
        assert!(!s.eager_peers.contains(&11));
    }

    #[test]
    fn handle_gossip_optimization_grafts_shorter_path() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        // optimization_threshold = 3

        // Peer 5 tells us about msg(42) via IHave at round 1
        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(42),
                    round: 1,
                }],
            },
            &mut rt,
        );
        assert!(s.missing.contains_key(&msg(42)));
        rt.sent.clear();
        rt.scheduled.clear();

        // Now the GOSSIP arrives from peer 1 at round 10 (1 + 3 < 10)
        s.peer_up(1, None, &mut rt);
        s.handle_gossip(
            GossipMsg {
                round: 10,
                sender: 1,
                payload: payload(42),
            },
            &mut rt,
        );

        // Should have sent GRAFT to peer 5 (shorter path)
        let grafts: Vec<_> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Graft(_)))
            .collect();
        assert_eq!(grafts.len(), 1);
        let (to, m) = &grafts[0];
        assert_eq!(*to, 5);
        let graft = unwrap_graft(m);
        assert_eq!(graft.requests[0].round, 1);
        assert!(!graft.send);

        // Peer 5 promoted to eager
        assert!(s.eager_peers.contains(&5));

        // Missing entry removed
        assert!(!s.missing.contains_key(&msg(42)));
    }

    #[test]
    fn handle_gossip_no_optimization_when_round_close() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        // IHave at round 5, GOSSIP arrives at round 7 (5 + 3 = 8 > 7, no graft)
        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 5,
                }],
            },
            &mut rt,
        );
        rt.sent.clear();

        s.handle_gossip(
            GossipMsg {
                round: 7,
                sender: 1,
                payload: payload(1),
            },
            &mut rt,
        );

        // No GRAFT sent
        let grafts: Vec<_> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Graft(_)))
            .collect();
        assert!(grafts.is_empty());

        // Missing entry still removed
        assert!(!s.missing.contains_key(&msg(1)));
    }

    // -----------------------------------------------------------------------
    // handle_ihave
    // -----------------------------------------------------------------------

    #[test]
    fn handle_ihave_schedules_timer() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![
                    IHaveDigest {
                        id: msg(1),
                        round: 0,
                    },
                    IHaveDigest {
                        id: msg(2),
                        round: 1,
                    },
                ],
            },
            &mut rt,
        );

        assert_eq!(s.missing.len(), 2);
        assert_eq!(rt.scheduled.len(), 1);
        assert_eq!(
            rt.scheduled[0].0,
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1), msg(2)],
                retries: 0,
                senders: vec![5],
            }
        );
        assert_eq!(rt.scheduled[0].1, Duration::from_secs(3));
    }

    #[test]
    fn handle_ihave_skips_already_received() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.seen.observe(msg(1), 0);

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 0,
                }],
            },
            &mut rt,
        );

        assert!(s.missing.is_empty());
        assert!(rt.scheduled.is_empty());
    }

    #[test]
    fn handle_ihave_skips_already_missing() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 0,
                }],
            },
            &mut rt,
        );
        assert_eq!(rt.scheduled.len(), 1);

        // Second IHave for the same id from a different peer
        s.handle_ihave(
            IHaveMsg {
                sender: 6,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 0,
                }],
            },
            &mut rt,
        );
        // No additional timer or missing entry
        assert_eq!(rt.scheduled.len(), 1);
        assert_eq!(s.missing.len(), 1);
    }

    // -----------------------------------------------------------------------
    // handle_graft
    // -----------------------------------------------------------------------

    #[test]
    fn handle_graft_sends_cached_payload() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        // First, broadcast so the payload is cached
        s.broadcast(msg(1), payload(42), &mut rt);
        rt.sent.clear();

        // Peer 5 sends GRAFT
        s.handle_graft(graft_msg(5, true, vec![(msg(1), 0)]), &mut rt);

        // Peer 5 promoted to eager
        assert!(s.eager_peers.contains(&5));

        // GOSSIP sent back with cached payload
        assert_eq!(rt.sent.len(), 1);
        let (to, m) = &rt.sent[0];
        assert_eq!(*to, 5);
        let g = unwrap_gossip(m);
        assert_eq!(g.payload, payload(42));
    }

    #[test]
    fn handle_graft_notifies_when_not_cached() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_graft(graft_msg(5, true, vec![(msg(999), 0)]), &mut rt);

        // No GOSSIP sent
        let gossips: Vec<_> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Gossip(_)))
            .collect();
        assert!(gossips.is_empty());

        // PayloadNotCached notification
        assert!(
            rt.notifications
                .iter()
                .any(|n| matches!(n, Notification::PayloadNotCached(id) if *id == msg(999)))
        );
    }

    // -----------------------------------------------------------------------
    // handle_prune
    // -----------------------------------------------------------------------

    #[test]
    fn handle_prune_demotes_to_lazy() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        // Ring locks min/max only; exactly one discretionary eager → no shuffle ambiguity.
        s.peer_up(100, None, &mut rt);
        s.peer_up(200, None, &mut rt);
        s.peer_up(210, None, &mut rt);
        assert!(s.eager_peers.contains(&200));
        assert!(!s.ring_locked_peers().contains(&200));
        s.handle_prune(
            PruneMsg {
                sender: 200,
                triggered_by: msg(1),
            },
            &mut rt,
        );

        assert!(!s.eager_peers.contains(&200));
        assert!(s.lazy_peers.contains(&200));
    }

    // -----------------------------------------------------------------------
    // timer_fired
    // -----------------------------------------------------------------------

    #[test]
    fn timer_fired_ihave_timeout_sends_graft() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 2,
                }],
            },
            &mut rt,
        );
        rt.sent.clear();
        rt.scheduled.clear();

        // Fire the timer
        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1)],
                retries: 0,
                senders: vec![5],
            },
            &mut rt,
        );

        // GRAFT sent to peer 5
        assert_eq!(rt.sent.len(), 1);
        let (to, m) = &rt.sent[0];
        assert_eq!(*to, 5);
        let graft = unwrap_graft(m);
        assert_eq!(graft.requests.len(), 1);
        assert_eq!(graft.requests[0].id, msg(1));
        assert_eq!(graft.requests[0].round, 2);

        // Peer 5 promoted to eager
        assert!(s.eager_peers.contains(&5));

        // Still missing until received or retries exhausted; retry batch scheduled
        assert!(s.missing.contains_key(&msg(1)));
        assert_eq!(rt.scheduled.len(), 1);
        assert_eq!(
            rt.scheduled[0].0,
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1)],
                retries: 1,
                senders: vec![5],
            }
        );
        assert_eq!(rt.scheduled[0].1, Duration::from_secs(3) / 2);
    }

    #[test]
    fn handle_ihave_timeout_partial_noop() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![
                    IHaveDigest {
                        id: msg(1),
                        round: 0,
                    },
                    IHaveDigest {
                        id: msg(2),
                        round: 0,
                    },
                ],
            },
            &mut rt,
        );
        s.seen.observe(msg(1), 0);
        rt.sent.clear();
        rt.scheduled.clear();

        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1), msg(2)],
                retries: 0,
                senders: vec![5],
            },
            &mut rt,
        );

        assert_eq!(rt.sent.len(), 1);
        let graft = unwrap_graft(&rt.sent[0].1);
        assert_eq!(graft.requests.len(), 1);
        assert_eq!(graft.requests[0].id, msg(2));
        assert!(!s.missing.contains_key(&msg(1)));
        assert!(s.missing.contains_key(&msg(2)));
        assert_eq!(rt.scheduled.len(), 1);
        assert_eq!(
            rt.scheduled[0].0,
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(2)],
                retries: 1,
                senders: vec![5],
            }
        );
    }

    #[test]
    fn timer_fired_ihave_timeout_chunks_graft_requests() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        let digests: Vec<_> = (1..=15)
            .map(|n| IHaveDigest {
                id: msg(n),
                round: 0,
            })
            .collect();

        s.handle_ihave(IHaveMsg { sender: 5, digests }, &mut rt);
        rt.sent.clear();
        rt.scheduled.clear();

        let ids: Vec<_> = (1..=15).map(msg).collect();
        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids,
                retries: 0,
                senders: vec![5],
            },
            &mut rt,
        );

        assert_eq!(rt.sent.len(), 2);
        let g0 = unwrap_graft(&rt.sent[0].1);
        let g1 = unwrap_graft(&rt.sent[1].1);
        assert_eq!(g0.requests.len(), 10);
        assert_eq!(g1.requests.len(), 5);
        assert!(rt.sent.iter().all(|(to, _)| *to == 5));
    }

    #[test]
    fn timer_fired_retries_race_multiple_senders() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 0,
                }],
            },
            &mut rt,
        );
        rt.sent.clear();
        rt.scheduled.clear();

        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1)],
                retries: 1,
                senders: vec![5, 6, 7, 8],
            },
            &mut rt,
        );

        let targets: Vec<_> = rt.sent.iter().map(|(to, _)| *to).collect();
        assert_eq!(targets, vec![6]);
        assert_eq!(
            rt.scheduled[0].0,
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1)],
                retries: 2,
                senders: vec![5, 6, 7, 8],
            }
        );
    }

    #[test]
    fn handle_graft_batch_sends_multiple_cached_payloads() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.broadcast(msg(1), payload(1), &mut rt);
        s.broadcast(msg(2), payload(2), &mut rt);
        rt.sent.clear();

        s.handle_graft(graft_msg(5, true, vec![(msg(1), 0), (msg(2), 0)]), &mut rt);

        let gossips: Vec<_> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Gossip(_)))
            .collect();
        assert_eq!(gossips.len(), 2);
    }

    #[test]
    fn timer_fired_noop_if_already_received() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: msg(1),
                    round: 0,
                }],
            },
            &mut rt,
        );

        // GOSSIP arrives before timer fires, removing from missing
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 2,
                payload: payload(1),
            },
            &mut rt,
        );
        rt.sent.clear();

        // Timer fires — but missing entry already removed
        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(1)],
                retries: 0,
                senders: vec![5],
            },
            &mut rt,
        );
        assert!(rt.sent.is_empty());
    }

    // -----------------------------------------------------------------------
    // tick
    // -----------------------------------------------------------------------

    #[test]
    fn tick_flushes_lazy_queues() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.lazy_peers.insert(3);
        s.lazy_peers.insert(4);

        s.enqueue_ihave(msg(1), 0);
        s.enqueue_ihave(msg(2), 1);

        s.tick(&mut rt);

        // IHave sent to both lazy peers
        assert_eq!(rt.sent.len(), 2);
        for (to, m) in &rt.sent {
            let ih = unwrap_ihave(m);
            assert_eq!(ih.sender, 0);
            assert_eq!(ih.digests.len(), 2);
            assert!(s.lazy_peers.contains(to));
        }

        // Queue is drained
        assert!(s.drain_lazy_queue().is_none());
    }

    #[test]
    fn tick_no_sends_when_queue_empty() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.lazy_peers.insert(1);

        s.tick(&mut rt);
        assert!(rt.sent.is_empty());
    }

    // -----------------------------------------------------------------------
    // Fanout / capacity limits
    // -----------------------------------------------------------------------

    // #[test]
    // fn rebalance_uses_weighted_ring_buckets_and_explore_slot() {
    //     let mut cfg = test_config();
    //     cfg.num_eager = 8;
    //     cfg.num_lazy = 10;
    //     let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
    //     let mut rt = AccumulatingRuntime::default();

    //     let peers = (1u8..=12)
    //         .map(|peer| {
    //             let ring = match peer {
    //                 1..=4 => Some(0),
    //                 5..=8 => Some(2),
    //                 _ => Some(5),
    //             };
    //             (
    //                 peer,
    //                 RttInfo {
    //                     ring,
    //                     rtt_ms: u64::from(peer),
    //                 },
    //             )
    //         })
    //         .collect();
    //     s.add_peers_bulk_with_rtt(peers, &mut rt);

    //     assert_eq!(s.eager_peers.len(), 8);
    //     assert!(
    //         s.ring_locked_peers()
    //             .iter()
    //             .all(|p| s.eager_peers.contains(p))
    //     );

    //     let near = s.eager_bucket_count(RingBucket::Near);
    //     let mid = s.eager_bucket_count(RingBucket::Mid);
    //     let far = s.eager_bucket_count(RingBucket::Far);
    //     assert!(near >= 4, "expected near slots to be favored: {near}");
    //     assert!(mid >= 2, "expected mid bucket coverage: {mid}");
    //     assert!(far >= 1, "expected far bucket coverage: {far}");
    // }

    #[test]
    fn move_to_eager_respects_num_eager() {
        let mut cfg = test_config();
        cfg.num_eager = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, None, &mut rt); // eager
        s.peer_up(2, None, &mut rt); // eager (full)
        s.lazy_peers.insert(3);

        // handle_prune will try move_to_lazy then handle_gossip tries
        // move_to_eager for the sender. Let's test directly via a gossip
        // from peer 3 — it will try to promote 3 to eager but can't.
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 3,
                payload: payload(1),
            },
            &mut rt,
        );

        // Peer 3 should stay in lazy (eager is full)
        assert!(s.lazy_peers.contains(&3));
        assert!(!s.eager_peers.contains(&3));
        assert_eq!(s.eager_peers.len(), 2);
    }

    #[test]
    fn move_to_lazy_respects_max_lazy() {
        let mut cfg = test_config();
        cfg.num_eager = 3;
        // Need room for both prior lazy (6) and demoted peer (5) so 6 is not evicted from lazy.
        cfg.min_lazy = 2;
        cfg.max_lazy = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        // Ring locks 7 and 4; pool {5,6} → one eager, one lazy (shuffle picks which).
        s.peer_up(4, None, &mut rt);
        s.peer_up(5, None, &mut rt);
        s.peer_up(6, None, &mut rt);
        s.peer_up(7, None, &mut rt);

        let dup_sender = *s
            .eager_peers
            .iter()
            .find(|p| !s.ring_locked_peers().contains(*p))
            .expect("one non-locked eager");
        assert!(s.lazy_peers().len() == 1);
        let lazy_peer = s.lazy_peers()[0];
        assert_ne!(dup_sender, lazy_peer);

        // Duplicate gossip from non-locked eager → demote;
        s.seen.observe(msg(1), 0);
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: dup_sender,
                payload: payload(1),
            },
            &mut rt,
        );

        assert!(!s.eager_peers.contains(&dup_sender));
        assert!(s.lazy_peers.contains(&dup_sender));
    }

    // -----------------------------------------------------------------------
    // Full tree optimization cycle (multi-node simulation)
    // -----------------------------------------------------------------------

    #[test]
    fn full_prune_graft_cycle() {
        // Simulate: A broadcasts, B and C both receive from A (eager).
        // B also receives a duplicate from C → B sends PRUNE to C.
        // Later C has a message B missed → B GRAFTs C back.

        // Node B (id=2). Ring [1,2,4,10,30]: neighbors of 2 are 1 and 4 — C (30) is not locked.
        let cfg = test_config();
        let mut b =
            PlumtreeState::<TestMsgId, TestPayload, TestNodeId, TestSeenStore>::new_with_store(
                2,
                cfg.clone(),
                TestSeenStore::default(),
            );
        let mut rt_b = AccumulatingRuntime::default();

        b.peer_up(1, None, &mut rt_b); // A
        b.peer_up(4, None, &mut rt_b);
        b.peer_up(10, None, &mut rt_b);
        b.peer_up(30, None, &mut rt_b); // C — not successor/predecessor of B on the ring

        // B receives GOSSIP from A
        b.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 1,
                payload: payload(1),
            },
            &mut rt_b,
        );
        assert!(b.has_message(&msg(1)));
        rt_b.sent.clear();

        // B receives duplicate from C → sends PRUNE to C
        b.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 30,
                payload: payload(1),
            },
            &mut rt_b,
        );
        assert_eq!(rt_b.sent.len(), 1);
        let (to, m) = &rt_b.sent[0];
        assert_eq!(*to, 30);
        unwrap_prune(m); // verifies it's a Prune
        assert!(b.lazy_peers.contains(&30));
        rt_b.sent.clear();

        // Now C ticks and sends IHave for msg(2) to B (lazy peer)
        // Simulated: B receives the IHave
        b.handle_ihave(
            IHaveMsg {
                sender: 30,
                digests: vec![IHaveDigest {
                    id: msg(2),
                    round: 0,
                }],
            },
            &mut rt_b,
        );

        // Timer fires — B sends GRAFT to C
        b.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![msg(2)],
                retries: 0,
                senders: vec![30],
            },
            &mut rt_b,
        );
        assert_eq!(rt_b.sent.len(), 1);
        let (to, m) = &rt_b.sent[0];
        assert_eq!(*to, 30);
        unwrap_graft(m);

        // C is back in eager
        assert!(b.eager_peers.contains(&30));
        assert!(!b.lazy_peers.contains(&30));
    }
}
