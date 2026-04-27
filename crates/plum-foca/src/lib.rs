use indexmap::IndexSet;
use rand::Rng;
use rand::prelude::IndexedRandom;
use rand::seq::SliceRandom;
use speedy::{Readable, Writable};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;
use tracing::{debug, trace};

/// Identifies a unique message in the network.
pub trait MessageId: Clone + Eq + Hash + Debug + Send + 'static {}

/// The payload carried by a Gossip message.
pub trait Payload: Clone + Debug + Send + 'static {
    type MessageId: MessageId;
    type NodeId: NodeId;
    fn message_id(&self) -> Self::MessageId;
    fn origin(&self) -> Self::NodeId;
}

/// Identifies a peer/node for routing purposes.
pub trait NodeId: Clone + Eq + Hash + Ord + Debug + Send + 'static {}

impl<T> MessageId for T where T: Clone + Eq + Hash + Debug + Send + 'static {}
impl<T> NodeId for T where T: Clone + Eq + Hash + Ord + Debug + Send + 'static {}

/// Hop count from the original sender, incremented on each forward.
pub type Round = u32;

/// Tunable parameters for the Plumtree protocol.
#[derive(Debug, Clone)]
pub struct Config {
    /// How long to wait after receiving an IHave before sending a GRAFT
    /// if the full message hasn't arrived yet.
    pub ihave_timeout: Duration,
    /// Optimization threshold in rounds. Controls when a node receiving
    /// a duplicate GOSSIP will attempt to GRAFT a shorter-path peer
    /// discovered via IHave.
    pub optimization_threshold: Option<Round>,
    /// Maximum number of eager peers (fanout). Peers beyond this limit
    /// are placed in the lazy set instead. Paper recommends ~4-5.
    pub max_eager: usize,
    /// Minimum number of lazy peers. When lazy falls below this floor,
    /// rebalance proactively pulls from known_peers to maintain a
    /// minimum IHave fanout so the repair mechanism can function.
    pub min_lazy: usize,
    /// Maximum number of lazy peers. Peers beyond this limit are
    /// evicted randomly. Recommended: 4 * max_eager.
    pub max_lazy: usize,
    /// Maximum number of times a message can be received before pruning sender.
    /// We trade of some duplication for tree stability.
    pub prune_threshold: u32,
    /// cap on seen entries; oldest are evicted when exceeded.
    pub max_received_entries: usize,
    // cap on cached payload used to respond to GRAFT requests.
    pub max_cached_payloads: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ihave_timeout: Duration::from_secs(1),
            optimization_threshold: Some(3),
            max_cached_payloads: 8192,
            max_eager: 5,
            min_lazy: 5,
            max_lazy: 20,
            prune_threshold: 1,
            max_received_entries: 10000,
        }
    }
}

pub trait SeenStore<I: MessageId>: std::fmt::Debug {
    fn contains(&self, id: &I) -> bool;
    fn observe(&mut self, id: I, round: Round) -> Option<u32>;
}

/// Timers produced by the protocol. The caller schedules these externally
/// (e.g. via tokio) and calls `PlumtreeState::timer_fired` when they expire.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Timer<I: MessageId> {
    /// Fires after `config.ihave_timeout`. If the message still hasn't
    /// arrived, the node sends a GRAFT to the IHave sender.
    IHaveTimeout(I),
}

/// Observable protocol events for metrics, logging, or diagnostics.
#[derive(Debug, Clone)]
pub enum Notification<I: MessageId, N: NodeId> {
    PeerMovedToEager(N),
    PeerMovedToLazy(N),
    /// Received a duplicate GOSSIP (already delivered).
    MessageAlreadyReceived(I),
    /// A GRAFT arrived for a message whose payload was evicted from cache.
    PayloadNotCached(I),
}

pub trait Runtime<I: MessageId, P: Payload<MessageId = I, NodeId = N>, N: NodeId> {
    /// Send a protocol message to a specific peer.
    fn send(&mut self, to: N, msg: PlumtreeMsg<I, P, N>);

    /// Deliver a received message to the application layer.
    fn deliver(&mut self, payload: P);

    /// Request the caller to schedule a timer. When the duration elapses,
    /// the caller must invoke `PlumtreeState::timer_fired`.
    fn schedule(&mut self, timer: Timer<I>, after: Duration);

    /// Observable protocol event.
    fn notify(&mut self, notification: Notification<I, N>);
}

/// Wire messages exchanged between Plumtree peers.
#[derive(Debug, Clone, Readable, Writable)]
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

/// Full payload — sent immediately to eager peers.
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

/// Digest-only batch — sent to lazy peers on each tick.
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
pub struct GraftMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    pub id: I,
    pub round: Round,
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

/// A message we heard about (via IHave) but haven't received yet.
/// Keyed by message id in `PlumtreeState::missing`.
#[derive(Debug)]
struct MissingEntry<N: NodeId> {
    ihave_sender: N,
    round: Round,
}

#[derive(Debug)]
struct PayloadCache<I: MessageId, P: Payload> {
    entries: HashMap<I, (P, Round)>,
    order: VecDeque<I>,
    max_size: usize,
}

impl<I: MessageId, P: Payload> PayloadCache<I, P> {
    fn new(max_size: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(max_size.min(1024)),
            order: VecDeque::with_capacity(max_size.min(1024)),
            max_size,
        }
    }

    fn insert(&mut self, id: I, payload: P, round: Round) {
        if self.entries.contains_key(&id) {
            return;
        }
        while self.entries.len() >= self.max_size {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        self.entries.insert(id.clone(), (payload, round));
        self.order.push_back(id);
    }

    fn get(&self, id: &I) -> Option<&(P, Round)> {
        self.entries.get(id)
    }
}

#[derive(Debug, Default, Clone)]
pub struct PlumtreeStats {
    pub gossip: u64,
    pub ihave: u64,
    pub graft: u64,
    pub prune: u64,

    pub peer_up: u64,
    pub peer_down: u64,
    pub lazy_peers: u64,
    pub eager_peers: u64,
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

    lazy_queue: HashMap<N, Vec<IHaveDigest<I>>>,
    missing: HashMap<I, MissingEntry<N>>,

    seen: S,
    cache: PayloadCache<I, P>,
    stats: PlumtreeStats,
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
            lazy_queue: HashMap::new(),
            missing: HashMap::new(),
            seen,
            cache: PayloadCache::new(cache_size),
            stats: PlumtreeStats::default(),
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

    pub fn stats(&self) -> &PlumtreeStats {
        &self.stats
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
    /// Adapted from Paper §3.2 eagerPush procedure for multi-sender:
    /// - Duplicate → PRUNE sender (after threshold), demote to lazy.
    /// - New → deliver, forward to eager peers, enqueue IHave for lazy.
    ///
    /// Unlike the paper, we do NOT promote the sender to eager on receipt.
    /// In a multi-sender network, the peer that forwarded sender A's
    /// message fast may be a poor path for sender B. Eager promotion
    /// only happens through intentional GRAFT (IHave timeout or
    /// optimization path), which proves the peer is genuinely needed.
    pub fn handle_gossip(&mut self, msg: GossipMsg<I, P, N>, rt: &mut impl Runtime<I, P, N>) {
        let GossipMsg {
            round,
            sender,
            payload,
        } = msg;
        let id = payload.message_id();

        self.stats.gossip += 1;
        if let Some(duplicates) = self.seen.observe(id.clone(), round) {
            if duplicates > self.config.prune_threshold && !self.ring_locked.contains(&sender) {
                trace!(
                    ?id,
                    ?round,
                    "local={:?} sender={:?} gossip dup → PRUNE sender",
                    self.local_id,
                    sender
                );
                rt.send(
                    sender.clone(),
                    PlumtreeMsg::Prune(PruneMsg {
                        sender: self.local_id.clone(),
                        triggered_by: id.clone(),
                    }),
                );
                self.move_to_lazy(&sender, rt);
            }
            rt.notify(Notification::MessageAlreadyReceived(id));
            return;
        }

        trace!(
            ?id,
            round,
            eager = self.eager_peers.len(),
            lazy = self.lazy_peers.len(),
            "local={:?} sender={:?} gossip NEW → deliver + forward",
            self.local_id,
            sender
        );

        self.cache.insert(id.clone(), payload.clone(), round);
        rt.deliver(payload.clone());

        let next_round = round + 1;
        let mut fwd_count = 0u32;
        for peer in &self.eager_peers {
            if *peer == sender || *peer == self.local_id || payload.origin() == *peer {
                continue;
            }
            rt.send(
                peer.clone(),
                PlumtreeMsg::Gossip(GossipMsg {
                    round: next_round,
                    sender: self.local_id.clone(),
                    payload: payload.clone(),
                }),
            );
            fwd_count += 1;
        }
        trace!(?id, fwd_count, "local={:?} gossip forwarded", self.local_id);

        self.enqueue_ihave(id.clone(), round);

        // Do NOT promote sender to eager here. In multi-sender networks,
        // one successful delivery doesn't make a peer a good eager
        // candidate for all senders. Eager promotion happens only via
        // intentional GRAFT (IHave timeout or optimization).
        //
        // But do ensure the sender is at least in lazy so they receive
        // IHaves and can be grafted later if they prove useful.
        self.ensure_in_lazy(&sender);

        // this
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
                            id: id.clone(),
                            round: entry.round,
                        }),
                    );
                    self.move_to_eager(&entry.ihave_sender, rt);
                }
            }
        }
    }

    /// Handle an incoming IHave digest batch.
    ///
    /// For each digest we haven't already received, record it in the
    /// `missing` set and schedule an `IHaveTimeout` timer. If the full
    /// GOSSIP doesn't arrive before the timer fires, we'll GRAFT.
    pub fn handle_ihave(&mut self, msg: IHaveMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        self.stats.ihave += 1;
        let IHaveMsg { sender, digests } = msg;
        let count = digests.len();
        let mut new_missing = 0u32;

        for digest in digests {
            if self.seen.contains(&digest.id) {
                continue;
            }
            if self.missing.contains_key(&digest.id) {
                continue;
            }

            new_missing += 1;
            self.missing.insert(
                digest.id.clone(),
                MissingEntry {
                    ihave_sender: sender.clone(),
                    round: digest.round,
                },
            );
            rt.schedule(Timer::IHaveTimeout(digest.id), self.config.ihave_timeout);
        }
        if new_missing > 0 {
            debug!(local = ?self.local_id, ?sender, count, new_missing,
                   total_missing = self.missing.len(), "handle_ihave → scheduled grafts");
        }
    }

    /// Handle an incoming GRAFT request.
    ///
    /// The sender is asking us to add them back to our eager set and
    /// (re)send the full payload for a specific message.
    pub fn handle_graft(&mut self, msg: GraftMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        self.stats.graft += 1;
        let GraftMsg { sender, id, .. } = msg;

        self.move_to_eager(&sender, rt);

        if let Some((payload, round)) = self.cache.get(&id).cloned() {
            debug!(
                ?id,
                "local={:?} sender={:?} graft → sending cached payload", self.local_id, sender
            );
            rt.send(
                sender,
                PlumtreeMsg::Gossip(GossipMsg {
                    round,
                    sender: self.local_id.clone(),
                    payload,
                }),
            );
        } else {
            debug!(
                ?id,
                cache_size = self.cache.entries.len(),
                "local={:?} sender={:?} graft → NOT CACHED",
                self.local_id,
                sender
            );
            rt.notify(Notification::PayloadNotCached(id));
        }
    }

    /// Handle an incoming PRUNE.
    ///
    /// The sender already has the message, so demote them from our
    /// eager set to lazy — we'll only send them IHave digests.
    pub fn handle_prune(&mut self, msg: PruneMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        self.stats.prune += 1;
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
        if self.seen.contains(&id) {
            debug!(
                ?id,
                "local={:?} broadcast skipped (already received)", self.local_id
            );
            return;
        }

        debug!(
            ?id,
            eager = self.eager_peers.len(),
            lazy = self.lazy_peers.len(),
            "local={:?} broadcast originate",
            self.local_id
        );
        self.seen.observe(id.clone(), 0);
        self.cache.insert(id.clone(), payload.clone(), 0);

        for peer in &self.eager_peers {
            if *peer == self.local_id {
                continue;
            }
            rt.send(
                peer.clone(),
                PlumtreeMsg::Gossip(GossipMsg {
                    round: 0,
                    sender: self.local_id.clone(),
                    payload: payload.clone(),
                }),
            );
        }

        self.enqueue_ihave(id, 0);
    }

    /// Handle a fired timer.
    ///
    /// `IHaveTimeout`: if the message is still missing, send GRAFT to
    /// the peer that told us about it and promote them to eager.
    pub fn timer_fired(&mut self, timer: Timer<I>, rt: &mut impl Runtime<I, P, N>) {
        match timer {
            Timer::IHaveTimeout(ref id) => {
                if let Some(entry) = self.missing.remove(id) {
                    debug!(
                        ?id,
                        "local={:?} send_to={:?} IHave timeout → GRAFT",
                        self.local_id,
                        entry.ihave_sender
                    );
                    rt.send(
                        entry.ihave_sender.clone(),
                        PlumtreeMsg::Graft(GraftMsg {
                            sender: self.local_id.clone(),
                            id: id.clone(),
                            round: entry.round,
                        }),
                    );
                    self.move_to_eager(&entry.ihave_sender, rt);
                } else {
                    trace!(
                        ?id,
                        "local={:?} IHave timeout → already received, noop", self.local_id
                    );
                }
            }
        }
    }

    /// Periodic maintenance — flush all pending IHave digests to lazy peers.
    ///
    /// The caller should invoke this on a regular interval (e.g. 200-500ms).
    pub fn tick(&mut self, rt: &mut impl Runtime<I, P, N>) {
        let peers: Vec<N> = self.lazy_peers.iter().cloned().collect();
        for peer in peers {
            if let Some(digests) = self.drain_lazy_queue(&peer) {
                rt.send(
                    peer,
                    PlumtreeMsg::IHave(IHaveMsg {
                        sender: self.local_id.clone(),
                        digests,
                    }),
                );
            }
        }
    }

    // --- Peer membership ---

    /// Add multiple peers at once (e.g. during bootstrap).
    pub fn add_peers_bulk(&mut self, peers: Vec<N>, rt: &mut impl Runtime<I, P, N>) {
        for peer in peers {
            if peer != self.local_id {
                self.known_peers.insert(peer);
            }
        }
        self.rebalance(rt);
    }

    /// A new peer has come online and wants to join the overlay.
    pub fn peer_up(&mut self, peer: N, rt: &mut impl Runtime<I, P, N>) {
        if peer == self.local_id || self.known_peers.contains(&peer) {
            return;
        }

        self.stats.peer_up += 1;
        self.known_peers.insert(peer);
        self.rebalance(rt);
    }

    /// Peer leaves or is detected as failed.
    pub fn peer_down(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        self.stats.peer_down += 1;
        self.eager_peers.remove(peer);
        self.lazy_peers.swap_remove(peer);
        self.lazy_queue.remove(peer);
        self.known_peers.remove(peer);
        self.rebalance(rt);
    }

    // --- Rebalance ---

    /// Single function that restores all overlay invariants after any
    /// change to known_peers. Called by peer_up, peer_down, and
    /// add_peers_bulk.
    ///
    /// Invariants maintained:
    /// 1. ring_locked is recomputed from current known_peers.
    /// 2. All ring-locked peers are in eager (forced, even above max_eager).
    /// 3. Eager is filled up to max_eager from lazy, then known_peers.
    /// 4. Non-ring eager peers above max_eager are demoted to lazy.
    /// 5. Lazy is filled up to min_lazy from known_peers if below floor.
    /// 6. Lazy never exceeds max_lazy (insert_into_lazy handles eviction).
    /// 7. Stale peers no longer in known_peers are removed from eager/lazy.
    fn rebalance(&mut self, rt: &mut impl Runtime<I, P, N>) {
        self.eager_peers.clear();
        self.lazy_peers.clear();

        // under 5? append all peers to eager
        if self.known_peers.len() <= 5 {
            self.eager_peers.extend(self.known_peers.iter().cloned());
        }

        // 1. Recompute ring neighbor.
        self.set_ring_neighbors();

        self.eager_peers.extend(self.ring_locked.iter().cloned());

        let remaining_eager = self.config.max_eager - self.eager_peers.len();

        // 2. Force ring-locked peers into eager.
        let must_eager: Vec<N> = self
            .ring_locked
            .iter()
            .filter(|p| !self.eager_peers.contains(p))
            .cloned()
            .collect();

        for peer in must_eager {
            self.lazy_peers.swap_remove(&peer);
            self.eager_peers.insert(peer.clone());
            rt.notify(Notification::PeerMovedToEager(peer));
        }

        let mut remaining_peers = self
            .known_peers
            .iter()
            .cloned()
            .filter(|p| !self.eager_peers.contains(&p))
            .collect::<Vec<_>>();
        remaining_peers.shuffle(&mut rand::rng());

        self.eager_peers
            .extend(remaining_peers.iter().take(remaining_eager).cloned());
        self.lazy_peers.extend(
            remaining_peers
                .iter()
                .skip(remaining_eager)
                .take(self.config.min_lazy)
                .cloned(),
        );

        trace!(
            local = ?self.local_id,
            eager = self.eager_peers.len(),
            lazy = self.lazy_peers.len(),
            ring_locked = self.ring_locked.len(),
            known = self.known_peers.len(),
            "rebalance complete"
        );
    }

    // fn maintain_eager_lazy_peers(&mut self, rt: &mut impl Runtime<I, P, N>, rtt_scorer: impl Fn() -> HashMap<N, u64>) {
    //     // we only want to do this if things are not in the desired state
    //     if self.eager_peers.len() <= self.config.max_eager
    //         && self.lazy_peers.len() >= self.config.min_lazy
    //         && self.lazy_peers.len() <= self.config.max_lazy
    //     {
    //         return;
    //     }

    //     // ensure ring neighbors are in eager
    //     let ring_locked = self.ring_locked.iter().cloned().collect::<Vec<_>>();
    //     for peer in ring_locked {
    //         self.move_to_eager(&peer, rt);
    //     }

    //     let rtt_scores = rtt_scorer();
    //     if self.eager_peers.len() < self.config.max_eager {
    //         // promote based  on ring data
    //     }

    //     // demote based on ring0 data

    //     // promote based on ring0 data

    //     // fix lazy

    // }

    // --- Internal helpers ---

    fn enqueue_ihave(&mut self, id: I, round: Round) {
        let digest = IHaveDigest { id, round };
        for peer in &self.lazy_peers {
            self.lazy_queue
                .entry(peer.clone())
                .or_default()
                .push(digest.clone());
        }
    }

    fn drain_lazy_queue(&mut self, peer: &N) -> Option<Vec<IHaveDigest<I>>> {
        let queue = self.lazy_queue.get_mut(peer)?;
        if queue.is_empty() {
            return None;
        }
        Some(std::mem::take(queue))
    }

    /// Recompute ring neighbors from scratch based on current known_peers.
    ///
    /// Clears old ring_locked set and recalculates the two peers that
    /// are immediately before and after local_id in sorted order.
    fn set_ring_neighbors(&mut self) {
        self.ring_locked.clear();

        let mut sorted_peers: Vec<N> = self.known_peers.iter().cloned().collect();
        sorted_peers.push(self.local_id.clone());
        if sorted_peers.len() <= 1 {
            return;
        }

        sorted_peers.sort();
        if let Some(position) = sorted_peers.iter().position(|p| p == &self.local_id) {
            let len = sorted_peers.len();
            let after_idx = (position + 1) % len;
            let before_idx = (position + len - 1) % len;

            self.ring_locked.insert(sorted_peers[before_idx].clone());
            self.ring_locked.insert(sorted_peers[after_idx].clone());
        }
    }

    fn move_to_eager(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.contains(peer) || (!self.ring_locked.contains(peer) && self.eager_peers.len() >= self.config.max_eager) {
            return;
        }

        self.eager_peers.insert(peer.clone());
        if self.lazy_peers.contains(peer) {
            self.lazy_peers.swap_remove(peer);
            trace!(
                eager = self.eager_peers.len(),
                "local={:?} peer={:?} promoted from lazy", self.local_id, peer
            );
        } else {
            trace!(
                eager = self.eager_peers.len(),
                "local={:?} peer={:?} promoted", self.local_id, peer
            );
        }
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
        if self.eager_peers.remove(peer) {
            debug!(
                eager = self.eager_peers.len(),
                lazy = self.lazy_peers.len(),
                "local={:?} peer={:?} demoted_to_lazy",
                self.local_id,
                peer
            );
            self.insert_into_lazy(peer.clone());
            rt.notify(Notification::PeerMovedToLazy(peer.clone()));
        }
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
    /// This is the single point of lazy insertion — all code paths that
    /// add to lazy go through here so the eviction policy lives in one place.
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
    /// Cleans up their lazy_queue. Returns the evicted peer if any.
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
        if let Some(ref evicted) = evicted {
            self.lazy_queue.remove(evicted);
        }
        evicted
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
        fn contains(&self, id: &TestMsgId) -> bool {
            self.entries.contains_key(id)
        }

        fn observe(&mut self, id: TestMsgId, round: Round) -> Option<u32> {
            let existing = self.entries.get_mut(&id);
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
            max_eager: 5,
            min_lazy: 5,
            max_lazy: 10,
            prune_threshold: 1,
            max_received_entries: 10000,
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
        pub scheduled: Vec<(Timer<TestMsgId>, Duration)>,
        pub notifications: Vec<Notification<TestMsgId, TestNodeId>>,
    }

    impl Runtime<TestMsgId, TestPayload, TestNodeId> for AccumulatingRuntime {
        fn send(&mut self, to: TestNodeId, msg: PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>) {
            self.sent.push((to, msg));
        }

        fn deliver(&mut self, payload: TestPayload) {
            self.delivered.push(payload);
        }

        fn schedule(&mut self, timer: Timer<TestMsgId>, after: Duration) {
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
        s.peer_up(1, &mut rt);
        assert!(s.eager_peers.contains(&1));
        assert!(!s.lazy_peers.contains(&1));
    }

    #[test]
    fn peer_up_overflow_to_lazy() {
        let mut s = state(); // max_eager=5
        let mut rt = AccumulatingRuntime::default();
        for i in 1..=5 {
            s.peer_up(i, &mut rt);
        }
        assert_eq!(s.eager_peers.len(), 5);

        s.peer_up(6, &mut rt);
        assert!(!s.eager_peers.contains(&6));
        assert!(s.lazy_peers.contains(&6));
    }

    #[test]
    fn peer_up_both_full_ignored() {
        let mut cfg = test_config();
        cfg.max_eager = 2;
        cfg.max_lazy = 2;
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore> =
            PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, &mut rt); // eager
        s.peer_up(2, &mut rt); // eager
        s.peer_up(3, &mut rt); // lazy
        s.peer_up(4, &mut rt); // lazy
        s.peer_up(5, &mut rt); // ignored

        assert_eq!(s.eager_peers.len(), 2);
        assert_eq!(s.lazy_peers.len(), 2);
        assert!(!s.eager_peers.contains(&5));
        assert!(!s.lazy_peers.contains(&5));
    }

    #[test]
    fn peer_up_idempotent() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, &mut rt);
        s.peer_up(1, &mut rt);
        assert_eq!(s.eager_peers.len(), 1);
        assert_eq!(s.lazy_peers.len(), 0);
    }

    #[test]
    fn peer_down_removes_from_both_sets() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, &mut rt);
        s.lazy_peers.insert(2);
        s.peer_down(&1, &mut rt);
        // peer 2 should have been promoted from lazy to eager
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
        s.peer_up(1, &mut rt);
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
        cfg.max_eager = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, &mut rt);
        s.peer_up(2, &mut rt);
        assert_eq!(s.eager_peers.len(), 2);
        s.peer_up(3, &mut rt);
        assert!(s.ring_locked_peers().contains(&3));
        assert!(s.lazy_peers.contains(&3));
        assert!(
            s.eager_peers.contains(&3),
            "locked peer 3 should be promoted from lazy"
        );
    }

    #[test]
    fn peer_down_cleans_lazy_queue() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.lazy_peers.insert(1);
        s.enqueue_ihave(msg(1), 0);
        assert!(s.lazy_queue.contains_key(&1));

        s.peer_down(&1, &mut rt);
        assert!(!s.lazy_queue.contains_key(&1));
    }

    #[test]
    fn replenish_eager_after_prune() {
        let mut cfg = test_config();
        cfg.max_eager = 3;
        cfg.max_lazy = 5;
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore> =
            PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, &mut rt); // eager
        s.peer_up(2, &mut rt); // eager
        s.peer_up(3, &mut rt); // eager (full)
        s.peer_up(4, &mut rt); // lazy
        s.peer_up(5, &mut rt); // lazy
        assert_eq!(s.eager_peers.len(), 3);
        assert_eq!(s.lazy_peers.len(), 2);

        // Receive a PRUNE from peer 1 → eager shrinks, lazy peer promoted
        s.handle_prune(
            PruneMsg {
                sender: 1,
                triggered_by: msg(99),
            },
            &mut rt,
        );
        assert_eq!(s.eager_peers.len(), 3, "eager should be replenished");
        // one lazy promoted to eager, then peer 1 added to lazy → net same
        assert_eq!(s.lazy_peers.len(), 2);
        assert!(!s.eager_peers.contains(&1));
        assert!(s.lazy_peers.contains(&1));
    }

    // -----------------------------------------------------------------------
    // broadcast (originate)
    // -----------------------------------------------------------------------

    #[test]
    fn broadcast_sends_to_eager_and_enqueues_lazy() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, &mut rt); // eager
        s.peer_up(2, &mut rt); // eager
        // manually move 3 to lazy
        s.lazy_peers.insert(3);

        s.broadcast(msg(100), payload(42), &mut rt);

        // Should have sent GOSSIP to eager peers 1 and 2
        assert_eq!(rt.sent.len(), 2);
        for (to, m) in &rt.sent {
            let g = unwrap_gossip(m);
            assert_eq!(g.payload.message_id(), msg(100));
            assert_eq!(g.round, 0);
            assert_eq!(g.sender, 0); // local_id
            assert!(s.eager_peers.contains(to));
        }

        // Lazy queue for peer 3 should have one digest
        assert_eq!(s.lazy_queue[&3].len(), 1);

        // Message should be marked received
        assert!(s.has_message(&msg(100)));
    }

    #[test]
    fn broadcast_dedup_ignores_second_call() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, &mut rt);

        s.broadcast(msg(1), payload(1), &mut rt);
        assert_eq!(rt.sent.len(), 1);

        rt.sent.clear();
        s.broadcast(msg(1), payload(99), &mut rt);
        assert!(rt.sent.is_empty());
    }

    // -----------------------------------------------------------------------
    // handle_gossip
    // -----------------------------------------------------------------------

    #[test]
    fn handle_gossip_new_delivers_and_forwards() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(2, &mut rt); // eager
        s.peer_up(3, &mut rt); // eager
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

        // Sender 1 promoted to eager
        assert!(s.eager_peers.contains(&1));

        // Lazy peer 4 has IHave enqueued
        assert_eq!(s.lazy_queue[&4].len(), 1);

        // Marked as received
        assert!(s.has_message(&msg(10)));
    }

    #[test]
    fn handle_gossip_duplicate_sends_prune() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, &mut rt);
        // First receive
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 1,
                payload: payload(1),
            },
            &mut rt,
        );
        rt.sent.clear();
        rt.delivered.clear();
        rt.notifications.clear();

        // Duplicate from peer 2
        s.peer_up(2, &mut rt);
        s.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 2,
                payload: payload(1),
            },
            &mut rt,
        );

        // No delivery
        assert!(rt.delivered.is_empty());

        // PRUNE sent to sender 2
        assert_eq!(rt.sent.len(), 1);
        let (to, m) = &rt.sent[0];
        assert_eq!(*to, 2);
        let prune = unwrap_prune(m);
        assert_eq!(prune.sender, 0);
        assert_eq!(prune.triggered_by, msg(1));

        // Peer 2 demoted to lazy
        assert!(s.lazy_peers.contains(&2));
        assert!(!s.eager_peers.contains(&2));
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
        s.peer_up(1, &mut rt);
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
        assert_eq!(graft.round, 1);

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
        assert_eq!(rt.scheduled.len(), 2);
        assert_eq!(rt.scheduled[0].0, Timer::IHaveTimeout(msg(1)));
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
        s.handle_graft(
            GraftMsg {
                sender: 5,
                id: msg(1),
                round: 0,
            },
            &mut rt,
        );

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

        s.handle_graft(
            GraftMsg {
                sender: 5,
                id: msg(999),
                round: 0,
            },
            &mut rt,
        );

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

        s.peer_up(1, &mut rt); // eager
        s.handle_prune(
            PruneMsg {
                sender: 1,
                triggered_by: msg(1),
            },
            &mut rt,
        );

        assert!(!s.eager_peers.contains(&1));
        assert!(s.lazy_peers.contains(&1));
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

        // Fire the timer
        s.timer_fired(Timer::IHaveTimeout(msg(1)), &mut rt);

        // GRAFT sent to peer 5
        assert_eq!(rt.sent.len(), 1);
        let (to, m) = &rt.sent[0];
        assert_eq!(*to, 5);
        let graft = unwrap_graft(m);
        assert_eq!(graft.id, msg(1));
        assert_eq!(graft.round, 2);

        // Peer 5 promoted to eager
        assert!(s.eager_peers.contains(&5));

        // Missing entry removed
        assert!(!s.missing.contains_key(&msg(1)));
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
        s.timer_fired(Timer::IHaveTimeout(msg(1)), &mut rt);
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
        assert!(s.drain_lazy_queue(&3).is_none());
        assert!(s.drain_lazy_queue(&4).is_none());
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

    #[test]
    fn move_to_eager_respects_max_eager() {
        let mut cfg = test_config();
        cfg.max_eager = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, &mut rt); // eager
        s.peer_up(2, &mut rt); // eager (full)
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
        cfg.max_eager = 5;
        cfg.max_lazy = 1;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, &mut rt); // eager
        s.peer_up(2, &mut rt); // eager
        s.lazy_peers.insert(3); // lazy (full, max_lazy=1)

        // Duplicate gossip from peer 2 → should try to demote 2 to lazy
        s.seen.observe(msg(1), 0);
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 2,
                payload: payload(1),
            },
            &mut rt,
        );

        // Peer 3 promoted from lazy to eager (replenish), making room for
        // peer 2 in lazy.
        assert!(!s.eager_peers.contains(&2));
        assert!(s.eager_peers.contains(&3));
        assert!(s.lazy_peers.contains(&2));
    }

    // -----------------------------------------------------------------------
    // Full tree optimization cycle (multi-node simulation)
    // -----------------------------------------------------------------------

    #[test]
    fn full_prune_graft_cycle() {
        // Simulate: A broadcasts, B and C both receive from A (eager).
        // B also receives a duplicate from C → B sends PRUNE to C.
        // Later C has a message B missed → B GRAFTs C back.

        // Node A (id=1), Node B (id=2), Node C (id=3)
        let cfg = test_config();
        let mut b =
            PlumtreeState::<TestMsgId, TestPayload, TestNodeId, TestSeenStore>::new_with_store(
                2,
                cfg.clone(),
                TestSeenStore::default(),
            );
        let mut rt_b = AccumulatingRuntime::default();

        b.peer_up(1, &mut rt_b); // A
        b.peer_up(3, &mut rt_b); // C

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
                sender: 3,
                payload: payload(1),
            },
            &mut rt_b,
        );
        assert_eq!(rt_b.sent.len(), 1);
        let (to, m) = &rt_b.sent[0];
        assert_eq!(*to, 3);
        unwrap_prune(m); // verifies it's a Prune
        assert!(b.lazy_peers.contains(&3));
        rt_b.sent.clear();

        // Now C ticks and sends IHave for msg(2) to B (lazy peer)
        // Simulated: B receives the IHave
        b.handle_ihave(
            IHaveMsg {
                sender: 3,
                digests: vec![IHaveDigest {
                    id: msg(2),
                    round: 0,
                }],
            },
            &mut rt_b,
        );

        // Timer fires — B sends GRAFT to C
        b.timer_fired(Timer::IHaveTimeout(msg(2)), &mut rt_b);
        assert_eq!(rt_b.sent.len(), 1);
        let (to, m) = &rt_b.sent[0];
        assert_eq!(*to, 3);
        unwrap_graft(m);

        // C is back in eager
        assert!(b.eager_peers.contains(&3));
        assert!(!b.lazy_peers.contains(&3));
    }

    // -----------------------------------------------------------------------
    // Payload cache
    // -----------------------------------------------------------------------

    #[test]
    fn payload_cache_fifo_eviction() {
        let mut cache = PayloadCache::<TestMsgId, TestPayload>::new(3);
        cache.insert(msg(1), payload(1), 0);
        cache.insert(msg(2), payload(2), 0);
        cache.insert(msg(3), payload(3), 0);
        assert!(cache.get(&msg(1)).is_some());

        cache.insert(msg(4), payload(4), 0);
        assert!(cache.get(&msg(1)).is_none());
        assert!(cache.get(&msg(2)).is_some());
        assert!(cache.get(&msg(4)).is_some());
    }

    #[test]
    fn payload_cache_dedup_insert() {
        let mut cache = PayloadCache::<TestMsgId, TestPayload>::new(3);
        cache.insert(msg(1), payload(1), 0);
        cache.insert(msg(1), payload(99), 0);
        assert_eq!(cache.get(&msg(1)).unwrap().0, payload(1));
        assert_eq!(cache.entries.len(), 1);
    }

    #[test]
    fn graft_after_cache_eviction_notifies() {
        let mut cfg = test_config();
        cfg.max_cached_payloads = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        // Cache 3 messages (capacity 2, so msg(1) gets evicted)
        s.broadcast(msg(1), payload(1), &mut rt);
        s.broadcast(msg(2), payload(2), &mut rt);
        s.broadcast(msg(3), payload(3), &mut rt);
        rt.sent.clear();
        rt.notifications.clear();

        // GRAFT for evicted msg(1)
        s.handle_graft(
            GraftMsg {
                sender: 5,
                id: msg(1),
                round: 0,
            },
            &mut rt,
        );

        // No GOSSIP sent (payload gone)
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
                .any(|n| matches!(n, Notification::PayloadNotCached(id) if *id == msg(1)))
        );
    }
}
