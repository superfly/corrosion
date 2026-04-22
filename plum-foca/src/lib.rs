use indexmap::IndexSet;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::hash::Hash;
use std::time::Duration;

#[cfg(feature = "tracing")]
use tracing::{debug, trace};

#[cfg(not(feature = "tracing"))]
macro_rules! debug {
    ($($t:tt)*) => {};
}
#[cfg(not(feature = "tracing"))]
macro_rules! trace {
    ($($t:tt)*) => {};
}

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
pub trait NodeId: Clone + Eq + Hash + Debug + Send + 'static {}

impl<T> MessageId for T where T: Clone + Eq + Hash + Debug + Send + 'static {}
impl<T> NodeId for T where T: Clone + Eq + Hash + Debug + Send + 'static {}

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
    /// Paper recommends ~3 for single-sender, ~7 for multi-sender.
    pub optimization_threshold: Round,
    /// Maximum payloads kept cached for GRAFT responses.
    /// Older entries are evicted FIFO.
    pub max_cached_payloads: usize,
    /// Maximum number of eager peers (fanout). Peers beyond this limit
    /// are placed in the lazy set instead. Paper recommends ~4-5.
    pub max_eager: usize,
    /// Maximum number of lazy peers. Peers beyond this limit are not
    /// tracked. Recommended: 2 * max_eager.
    pub max_lazy: usize,
    /// Maximum number of times a message can be received before being pruned.
    pub prune_threshold: u32,
    /// Optional cap on seen entries; oldest are evicted when exceeded.
    pub max_received_entries: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ihave_timeout: Duration::from_secs(1),
            optimization_threshold: 3,
            max_cached_payloads: 8192,
            max_eager: 5,
            max_lazy: 10,
            prune_threshold: 1,
            max_received_entries: None,
        }
    }
}

pub trait SeenStore<I: MessageId>: std::fmt::Debug {
    fn contains(&self, id: &I) -> bool;
    fn observe(&mut self, id: I, round: Round) -> Option<u32>;
    fn record_local(&mut self, id: I);
}

#[derive(Debug)]
pub struct DefaultSeenStore<I: MessageId> {
    entries: HashMap<I, (Round, u32)>,
    order: VecDeque<I>,
    max_entries: Option<usize>,
}

impl<I: MessageId> DefaultSeenStore<I> {
    pub fn new(max_entries: Option<usize>) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_entries,
        }
    }

    fn touch_new(&mut self, id: I, round: Round) {
        self.entries.insert(id.clone(), (round, 1));
        self.order.push_back(id);
        if let Some(max_entries) = self.max_entries {
            while self.entries.len() > max_entries {
                if let Some(old) = self.order.pop_front() {
                    self.entries.remove(&old);
                } else {
                    break;
                }
            }
        }
    }
}

impl<I> SeenStore<I> for DefaultSeenStore<I>
where
    I: MessageId,
{
    fn contains(&self, id: &I) -> bool {
        self.entries.contains_key(id)
    }

    fn observe(&mut self, id: I, round: Round) -> Option<u32> {
        if let Some((_, count)) = self.entries.get_mut(&id) {
            *count = count.saturating_add(1);
            return Some(*count);
        }

        self.touch_new(id, round);
        None
    }

    fn record_local(&mut self, id: I) {
        if self.entries.contains_key(&id) {
            return;
        }
        self.touch_new(id, 0);
    }
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

/// Callback interface for I/O and scheduling. Passed by `&mut` reference
/// to each protocol method — never stored on `PlumtreeState`.
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
#[derive(Debug, Clone)]
#[cfg_attr(feature = "speedy", derive(speedy::Readable, speedy::Writable))]
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
#[derive(Debug, Clone)]
#[cfg_attr(feature = "speedy", derive(speedy::Readable, speedy::Writable))]
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
#[derive(Debug, Clone)]
#[cfg_attr(feature = "speedy", derive(speedy::Readable, speedy::Writable))]
pub struct IHaveMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    pub digests: Vec<IHaveDigest<I>>,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "speedy", derive(speedy::Readable, speedy::Writable))]
pub struct IHaveDigest<I: MessageId> {
    pub id: I,
    pub round: Round,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "speedy", derive(speedy::Readable, speedy::Writable))]
pub struct GraftMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    pub id: I,
    pub round: Round,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "speedy", derive(speedy::Readable, speedy::Writable))]
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

// ---------------------------------------------------------------------------
// PlumtreeState
// ---------------------------------------------------------------------------

/// Full Plumtree protocol state for one local node.
///
/// Pure state machine — owns no tasks, channels, or I/O.
/// The driving task calls transition methods, passing a `&mut impl Runtime`
/// for the library to emit sends, deliveries, and timer requests.
#[derive(Debug)]
pub struct PlumtreeState<
    I: MessageId,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
    S = DefaultSeenStore<I>,
> where
    S: SeenStore<I>,
{
    local_id: N,
    config: Config,
    eager_peers: HashSet<N>,
    lazy_peers: IndexSet<N>,
    seen: S,
    lazy_queue: HashMap<N, Vec<IHaveDigest<I>>>,
    missing: HashMap<I, MissingEntry<N>>,
    cache: PayloadCache<I, P>,
}

impl<I: MessageId, P: Payload<MessageId = I, NodeId = N>, N: NodeId>
    PlumtreeState<I, P, N, DefaultSeenStore<I>>
{
    pub fn new(local_id: N, config: Config) -> Self {
        let seen = DefaultSeenStore::new(config.max_received_entries);
        Self::new_with_store(local_id, config, seen)
    }
}

impl<I, P, N, S> PlumtreeState<I, P, N, S>
where
    I: MessageId,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
    S: SeenStore<I>,
{
    pub fn new_with_store(local_id: N, config: Config, seen: S) -> Self {
        let cache_size = config.max_cached_payloads;
        Self {
            local_id,
            config,
            eager_peers: HashSet::new(),
            lazy_peers: IndexSet::new(),
            seen,
            lazy_queue: HashMap::new(),
            missing: HashMap::new(),
            cache: PayloadCache::new(cache_size),
        }
    }

    // --- Membership ---

    /// New peer joins. Starts as eager if there's room, otherwise lazy.
    /// Ignored entirely if both sets are at capacity.
    pub fn peer_up(&mut self, peer: N) {
        if self.eager_peers.contains(&peer) || self.lazy_peers.contains(&peer) {
            return;
        }
        if self.eager_peers.len() < self.config.max_eager {
            trace!(local = ?self.local_id, ?peer, "peer_up → eager");
            self.eager_peers.insert(peer);
        } else if self.lazy_peers.len() < self.config.max_lazy {
            trace!(local = ?self.local_id, ?peer, max_lazy = self.config.max_lazy, "peer_up → lazy");
            self.lazy_peers.insert(peer);
        } else {
            debug!(local = ?self.local_id, ?peer, eager = self.eager_peers.len(), lazy = self.lazy_peers.len(), "peer_up → dropped (both full)");
        }
    }

    /// Peer leaves or is detected as failed.
    pub fn peer_down(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        let was_eager = self.eager_peers.remove(peer);
        self.lazy_peers.swap_remove(peer);
        self.lazy_queue.remove(peer);
        if was_eager {
            self.replenish_eager(rt);
        }
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

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn local_id(&self) -> &N {
        &self.local_id
    }

    // --- Protocol methods ---

    /// Handle an incoming GOSSIP message carrying a full payload.
    ///
    /// Paper §3.2 eagerPush procedure:
    /// - Duplicate → PRUNE sender, demote to lazy.
    /// - New → deliver, forward to eager peers, enqueue IHave for lazy,
    ///   promote sender to eager, and optionally GRAFT a shorter-path
    ///   peer discovered via a prior IHave.
    pub fn handle_gossip(&mut self, msg: GossipMsg<I, P, N>, rt: &mut impl Runtime<I, P, N>) {
        let GossipMsg {
            round,
            sender,
            payload,
        } = msg;
        let id = payload.message_id();
        if let Some(duplicates) = self.seen.observe(id.clone(), round) {
            if duplicates > self.config.prune_threshold {
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

        // already recorded as new by seen.observe above
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
        // TODO: check all the moves to verify.
        self.move_to_eager(&sender, rt);

        // TODO: understand effect of this optimization.
        if let Some(entry) = self.missing.remove(&id) {
            if entry.round + self.config.optimization_threshold < round
                && entry.ihave_sender != sender
            {
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

    /// Handle an incoming IHave digest batch.
    ///
    /// For each digest we haven't already received, record it in the
    /// `missing` set and schedule an `IHaveTimeout` timer. If the full
    /// GOSSIP doesn't arrive before the timer fires, we'll GRAFT.
    pub fn handle_ihave(&mut self, msg: IHaveMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
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
        debug!(local = ?self.local_id, sender = ?msg.sender, triggered_by = ?msg.triggered_by, "handle_prune");
        self.move_to_lazy(&msg.sender, rt);
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

    fn move_to_eager(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.contains(peer) || self.eager_peers.len() >= self.config.max_eager {
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
        if self.eager_peers.remove(peer) {
            debug!(
                eager = self.eager_peers.len(),
                lazy = self.lazy_peers.len(),
                "local={:?} peer={:?} demoted_to_lazy, replenishing",
                self.local_id,
                peer
            );
            self.replenish_eager(rt);
            if self.lazy_peers.len() < self.config.max_lazy {
                self.lazy_peers.insert(peer.clone());
            } else {
                debug!(
                    "local={:?} peer={:?} lazy full, peer dropped from overlay",
                    self.local_id, peer
                );
            }
            rt.notify(Notification::PeerMovedToLazy(peer.clone()));
        }
    }

    fn replenish_eager(&mut self, rt: &mut impl Runtime<I, P, N>) {
        while self.eager_peers.len() < self.config.max_eager {
            let peer = match self.lazy_peers.iter().next() {
                Some(p) => p.clone(),
                None => {
                    debug!(
                        eager = self.eager_peers.len(),
                        max = self.config.max_eager,
                        "local={:?} replenish_eager: no lazy peers left",
                        self.local_id
                    );
                    break;
                }
            };
            debug!(
                eager = self.eager_peers.len() + 1,
                max = self.config.max_eager,
                "local={:?} peer={:?} replenish_eager: promoting lazy → eager",
                self.local_id,
                peer
            );
            self.lazy_peers.swap_remove(&peer);
            self.eager_peers.insert(peer.clone());
            rt.notify(Notification::PeerMovedToEager(peer.clone()));
        }
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

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct TestPayload(pub Vec<u8>);

    pub(crate) type TestNodeId = u8;

    impl Payload for TestPayload {
        type MessageId = TestMsgId;
        type NodeId = TestNodeId;
        fn message_id(&self) -> Self::MessageId {
            TestMsgId(0)
        }

        fn origin(&self) -> Self::NodeId {
            0
        }
    }

    pub(crate) fn test_config() -> Config {
        Config {
            ihave_timeout: Duration::from_secs(3),
            optimization_threshold: 3,
            max_cached_payloads: 128,
            max_eager: 5,
            max_lazy: 10,
            prune_threshold: 1,
            max_received_entries: None,
        }
    }

    fn state() -> PlumtreeState<TestMsgId, TestPayload, TestNodeId> {
        PlumtreeState::new(0u8, test_config())
    }

    /// Accumulates all runtime calls for assertion in tests.
    #[derive(Debug, Default)]
    pub(crate) struct AccumulatingRuntime {
        pub sent: Vec<(TestNodeId, PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>)>,
        pub delivered: Vec<(TestMsgId, TestPayload, TestNodeId, Round)>,
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
        s.peer_up(1);
        assert!(s.eager_peers.contains(&1));
        assert!(!s.lazy_peers.contains(&1));
    }

    #[test]
    fn peer_up_overflow_to_lazy() {
        let mut s = state(); // max_eager=5
        for i in 1..=5 {
            s.peer_up(i);
        }
        assert_eq!(s.eager_peers.len(), 5);

        s.peer_up(6);
        assert!(!s.eager_peers.contains(&6));
        assert!(s.lazy_peers.contains(&6));
    }

    #[test]
    fn peer_up_both_full_ignored() {
        let mut cfg = test_config();
        cfg.max_eager = 2;
        cfg.max_lazy = 2;
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId> = PlumtreeState::new(0u8, cfg);

        s.peer_up(1); // eager
        s.peer_up(2); // eager
        s.peer_up(3); // lazy
        s.peer_up(4); // lazy
        s.peer_up(5); // ignored

        assert_eq!(s.eager_peers.len(), 2);
        assert_eq!(s.lazy_peers.len(), 2);
        assert!(!s.eager_peers.contains(&5));
        assert!(!s.lazy_peers.contains(&5));
    }

    #[test]
    fn peer_up_idempotent() {
        let mut s = state();
        s.peer_up(1);
        s.peer_up(1);
        assert_eq!(s.eager_peers.len(), 1);
        assert_eq!(s.lazy_peers.len(), 0);
    }

    #[test]
    fn peer_down_removes_from_both_sets() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1);
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
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId> = PlumtreeState::new(0u8, cfg);
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1); // eager
        s.peer_up(2); // eager
        s.peer_up(3); // eager (full)
        s.peer_up(4); // lazy
        s.peer_up(5); // lazy
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

        s.peer_up(1); // eager
        s.peer_up(2); // eager
        // manually move 3 to lazy
        s.lazy_peers.insert(3);

        s.broadcast(msg(100), payload(42), &mut rt);

        // Should have sent GOSSIP to eager peers 1 and 2
        assert_eq!(rt.sent.len(), 2);
        for (to, m) in &rt.sent {
            let g = unwrap_gossip(m);
            assert_eq!(g.id, msg(100));
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
        s.peer_up(1);

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

        s.peer_up(2); // eager
        s.peer_up(3); // eager
        s.lazy_peers.insert(4);

        // Receive a GOSSIP from peer 1 (not yet in our peer set)
        s.handle_gossip(
            GossipMsg {
                id: msg(10),
                round: 1,
                sender: 1,
                payload: payload(10),
            },
            &mut rt,
        );

        // Delivered once
        assert_eq!(rt.delivered.len(), 1);
        assert_eq!(rt.delivered[0].0, msg(10));
        assert_eq!(rt.delivered[0].3, 1); // round

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

        s.peer_up(1);
        // First receive
        s.handle_gossip(
            GossipMsg {
                id: msg(1),
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
        s.peer_up(2);
        s.handle_gossip(
            GossipMsg {
                id: msg(1),
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
        s.peer_up(1);
        s.handle_gossip(
            GossipMsg {
                id: msg(42),
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
                id: msg(1),
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

        s.seen.record_local(msg(1), &payload(1));

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

        s.peer_up(1); // eager
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
                id: msg(1),
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
        let mut s = PlumtreeState::new(0u8, cfg);
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1); // eager
        s.peer_up(2); // eager (full)
        s.lazy_peers.insert(3);

        // handle_prune will try move_to_lazy then handle_gossip tries
        // move_to_eager for the sender. Let's test directly via a gossip
        // from peer 3 — it will try to promote 3 to eager but can't.
        s.handle_gossip(
            GossipMsg {
                id: msg(1),
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
        let mut s = PlumtreeState::new(0u8, cfg);
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1); // eager
        s.peer_up(2); // eager
        s.lazy_peers.insert(3); // lazy (full, max_lazy=1)

        // Duplicate gossip from peer 2 → should try to demote 2 to lazy
        s.seen.record_local(msg(1), &payload(1));
        s.handle_gossip(
            GossipMsg {
                id: msg(1),
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
        let mut b = PlumtreeState::<TestMsgId, TestPayload, TestNodeId>::new(2, cfg.clone());
        let mut rt_b = AccumulatingRuntime::default();

        b.peer_up(1); // A
        b.peer_up(3); // C

        // B receives GOSSIP from A
        b.handle_gossip(
            GossipMsg {
                id: msg(1),
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
                id: msg(1),
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
        let mut s = PlumtreeState::new(0u8, cfg);
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
