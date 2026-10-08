use indexmap::{IndexMap, IndexSet};
use rand::rngs::SmallRng;
use rand::seq::{IndexedRandom, IteratorRandom, SliceRandom};
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use std::cmp::{Ordering, Reverse};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, info, trace, warn};

pub trait MessageId: Clone + Eq + Hash + Debug + Send + 'static {
    type NodeId: NodeId;
    fn origin(&self) -> Self::NodeId;
}

pub trait Payload: Clone + Debug + Send + 'static {
    type MessageId: MessageId<NodeId = Self::NodeId>;
    type NodeId: NodeId;
    fn message_id(&self) -> Self::MessageId;
    fn origin(&self) -> Self::NodeId;
}

pub trait NodeId: Copy + Eq + Hash + Ord + Debug + Send {}

impl<T> NodeId for T where T: Copy + Eq + Hash + Ord + Debug + Send + 'static {}

pub type Round = u32;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RttInfo {
    pub ring: Option<u8>,
    // pub rtt_ms: u64,
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

/// Split eager (and lazy) peers across Near/Mid/Far RTT buckets.
/// Values are percentages of the target fanout; default values are (~50% near, ~30% mid, ~20% far).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct EagerRatios {
    pub near_pct: u8,
    pub mid_pct: u8,
    pub far_pct: u8,
}

impl Default for EagerRatios {
    fn default() -> Self {
        Self {
            near_pct: 50,
            mid_pct: 30,
            far_pct: 20,
        }
    }
}

impl EagerRatios {
    pub fn validate(&self) -> Result<(), EagerRatiosError> {
        let sum = self.near_pct as u16 + self.mid_pct as u16 + self.far_pct as u16;
        if sum != 100 {
            return Err(EagerRatiosError {
                near_pct: self.near_pct,
                mid_pct: self.mid_pct,
                far_pct: self.far_pct,
                sum,
            });
        }
        Ok(())
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error(
    "eager ratios must sum to 100, got near={near_pct}, mid={mid_pct}, far={far_pct} (sum={sum})"
)]
pub struct EagerRatiosError {
    pub near_pct: u8,
    pub mid_pct: u8,
    pub far_pct: u8,
    pub sum: u16,
}

/// How eager and lazy peers follow membership changes. Only
/// `FullRebalance` is used in production; the others are compared against
/// it in `tests/sim.rs`. Under the others, a joining peer that is not made
/// eager starts lazy while the lazy set has room.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum PeerSelection {
    /// Membership changes flag a rebalance; the next maintenance tick clears
    /// both sets and refills them from shuffled bucket pools.
    #[default]
    FullRebalance,
    /// Rendezvous hashing: each node ranks every bucket by a salted hash and
    /// takes eager, then lazy peers from the top. A change only moves the
    /// peers whose label (eager, lazy or neither) changed.
    Hrw,
    /// A joining peer becomes eager with probability ~k/n, demoting a random
    /// eager peer if the set is full. A departing peer's slot is refilled at
    /// random from its bucket.
    IncrementalRandom,
}

/// Tunable parameters for the Plumtree protocol.
#[derive(Debug, Clone)]
pub struct Config {
    /// How long to wait after receiving an IHave before sending a GRAFT
    /// if the full message hasn't arrived yet.
    pub ihave_timeout: Duration,
    /// Optimization threshold in rounds. This is the minimum amount by which a lazy peer's
    /// IHAVE round must beat our eager-delivery round before we attempt to graft the lazy peer.
    pub optimization_threshold: Option<Round>,
    /// Number of eager peers (fanout). `None` derives it from cluster size:
    /// `round(log10(known_peers + 1) * 3)` (min 3).
    pub num_eager: Option<usize>,
    /// Minimum number of lazy peers. `None` defaults to `num_eager * 1.5`.
    pub min_lazy: Option<usize>,
    /// Maximum number of lazy peers. `None` defaults to `num_eager * 2`.
    pub max_lazy: Option<usize>,
    /// Maximum number of times a message can be received before pruning sender.
    /// We trade some duplication for tree stability.
    pub prune_threshold: u32,
    /// cap on seen entries; oldest are evicted when exceeded.
    pub max_received_entries: usize,
    /// cap on cached payload used to respond to GRAFT requests.
    pub max_cached_payloads: usize,
    /// If set, suppress repeat PRUNEs to the same peer within this window.
    pub prune_throttle: Option<Duration>,
    /// Near/Mid/Far split when selecting eager and lazy peers during rebalance.
    pub eager_ratios: EagerRatios,
    /// Neighbors locked eager on each side of the identity ring.
    /// Total locked peers is `2 * radius` (default 1 → 2).
    pub ring_locked_radius: usize,
    /// How the sets follow membership changes.
    pub peer_selection: PeerSelection,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ihave_timeout: Duration::from_secs(1),
            optimization_threshold: Some(3),
            num_eager: None,
            min_lazy: None,
            max_lazy: None,
            prune_threshold: 1,
            max_received_entries: 10000,
            max_cached_payloads: 8192,
            prune_throttle: Some(Duration::from_secs(1)),
            eager_ratios: EagerRatios::default(),
            ring_locked_radius: 1,
            peer_selection: PeerSelection::FullRebalance,
        }
    }
}

/// Resolved eager/lazy peer targets used by the protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FanoutTargets {
    num_eager: usize,
    min_lazy: usize,
    max_lazy: usize,
}

fn resolve_fanout(known_peers: usize, config: &Config) -> FanoutTargets {
    let num_eager = config.num_eager.unwrap_or_else(|| {
        let cluster = (known_peers + 1) as f64;
        ((cluster.log10() * 3.0).round() as usize).max(3)
    });
    FanoutTargets {
        num_eager,
        min_lazy: config.min_lazy.unwrap_or(num_eager * 3 / 2),
        max_lazy: config.max_lazy.unwrap_or(num_eager * 2),
    }
}

/// Rendezvous score of `peer` for the node with `salt`. A fixed function
/// rather than `DefaultHasher`, whose output may change between Rust
/// releases.
fn rank<N: Hash>(salt: u64, peer: &N) -> u64 {
    let mut hasher = RankHasher(salt);
    peer.hash(&mut hasher);
    hasher.finish()
}

struct RankHasher(u64);

impl Hasher for RankHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.0 = mix64(self.0 ^ u64::from_le_bytes(word));
        }
    }
}

/// One SplitMix64 step: the gamma increment, then the output mix.
fn mix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Eager and lazy peers picked by a full selection.
#[derive(Debug, Clone)]
struct Targets<N> {
    eager: IndexSet<N>,
    lazy: IndexSet<N>,
}

impl<N> Default for Targets<N> {
    fn default() -> Self {
        Self {
            eager: IndexSet::new(),
            lazy: IndexSet::new(),
        }
    }
}

/// A full selection's split of one RTT bucket.
#[derive(Debug, Clone, Copy)]
struct BucketShare {
    /// Ring-locked peers in the bucket; they count toward its eager share.
    locked: usize,
    /// The other known peers in the bucket.
    pool: usize,
    /// How many pool peers are made eager, and lazy.
    eager: usize,
    lazy: usize,
}

/// What happened to a peer, for [`PlumtreeState::apply_change`].
#[derive(Debug, Clone, Copy)]
enum PeerChange {
    Joined,
    Left {
        bucket: RingBucket,
        was_eager: bool,
        was_lazy: bool,
    },
    MovedBucket {
        from: RingBucket,
    },
}

pub trait SeenStore<I: MessageId> {
    fn evict_if_needed(&mut self);
    fn contains(&self, id: &I) -> bool;
    fn observe(&mut self, id: I, round: Round) -> Option<u32>;
    fn size(&self) -> usize;
}

/// Timers produced by the protocol, these are handled with the `schedule` method.
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
#[derive(Debug, PartialEq, Eq)]
pub enum Notification<'a, I: MessageId, N: NodeId> {
    PeerMovedToEager(&'a N),
    PeerDroppedFromEager(&'a N),
    PeerMovedToLazy(&'a N),
    PeerEvictedFromLazy(&'a N),
    DuplicateMessage(&'a I),
    PayloadNotCached(&'a I),
    MessageMissing(usize),
    PruneSuppressed(&'a N),
    Rebalance,
}

pub trait Runtime<I: MessageId, P: Payload<MessageId = I, NodeId = N>, N: NodeId> {
    /// Send a protocol message to a specific peer.
    ///
    /// The transport decides send and shed order from the message itself --
    /// `GossipMsg::round` and the message variant -- so no separate priority
    /// is passed down from the protocol.
    fn send(&mut self, to: N, msg: PlumtreeMsg<I, P, N>);

    // Send a message to a group of peers
    fn send_all(&mut self, peers: Vec<N>, msg: PlumtreeMsg<I, P, N>);

    /// Deliver a received message to the application layer.
    fn deliver(&mut self, payload: P);

    /// Request the caller to schedule a timer. When the duration elapses,
    /// the caller must invoke `PlumtreeState::timer_fired`.
    fn schedule(&mut self, timer: Timer<I, N>, after: Duration);

    /// Observable protocol event.
    fn notify(&mut self, notification: Notification<'_, I, N>);

    /// Current time. The state machine is sans-IO(sans-io.readthedocs.io) and owns no clock, so the
    /// runtime supplies one.
    fn now(&self) -> Instant;
}

/// A protocol message exchanged between Plumtree peers.
///
/// Each peer keeps two overlays over the same neighbor set: an eager set that
/// forms a spanning tree and receives full payloads (push), and a lazy set
/// that only receives message_id digests and pulls the payload on demand.
/// `Graft` and `Prune` move a link between the two sets to repair and optimize
/// the tree.
#[derive(Debug, Clone, Readable, Writable, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum PlumtreeMsg<I, P, N>
where
    I: MessageId,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
{
    /// Full payload pushed to an eager-set peer. This is how messages actually
    /// propagate along the spanning tree.
    Gossip(GossipMsg<I, P, N>),
    /// Lazy-push digest: contains batch of recently-seen message ids that is sent to lazy
    /// peers on a tick. Lets a peer notice a message it never received eagerly
    /// and `Graft` to fetch it.
    IHave(IHaveMsg<I, N>),
    /// Ask the recipient to add us to its eager set (and, when `send` is set,
    /// to reply with the full payload). Sent when an `IHave` reveals a message
    /// that never arrived eagerly (tree repair) or to adopt a shorter path
    /// (optimization).
    Graft(GraftMsg<I, N>),
    /// Ask the recipient to remove us from its eager set, demoting the link to
    /// lazy. Sent after a duplicate `Gossip` arrives once the duplicate count
    /// crosses `Config::prune_threshold`, which signals a redundant tree edge.
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

#[derive(Debug, Clone)]
pub struct PruneMsg<I, N>
where
    I: MessageId,
    N: NodeId,
{
    pub sender: N,
    pub triggered_by: Option<I>,
}

// NOTE: speedy's derive can't express the bounds needed to (de)serialize an
// `Option<I>` field over a generic `I`, so we implement the traits by hand.
impl<'a, C, I, N> speedy::Readable<'a, C> for PruneMsg<I, N>
where
    C: speedy::Context,
    I: MessageId + speedy::Readable<'a, C>,
    N: NodeId + speedy::Readable<'a, C>,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        Ok(Self {
            sender: reader.read_value()?,
            triggered_by: reader.read_value()?,
        })
    }

    fn minimum_bytes_needed() -> usize {
        <N as speedy::Readable<'a, C>>::minimum_bytes_needed()
            + <Option<I> as speedy::Readable<'a, C>>::minimum_bytes_needed()
    }
}

impl<C, I, N> speedy::Writable<C> for PruneMsg<I, N>
where
    C: speedy::Context,
    I: MessageId + speedy::Writable<C>,
    N: NodeId + speedy::Writable<C>,
{
    fn write_to<T: ?Sized + speedy::Writer<C>>(&self, writer: &mut T) -> Result<(), C::Error> {
        writer.write_value(&self.sender)?;
        writer.write_value(&self.triggered_by)?;
        Ok(())
    }
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
        self.entries.insert(id, (payload, round));
    }

    fn get(&self, id: &I) -> Option<&(P, Round)> {
        self.entries.get(id)
    }

    fn evict_if_needed(&mut self) {
        if self.entries.len() > self.max_size {
            self.entries.drain(0..self.entries.len() - self.max_size);
        }
    }

    fn size(&self) -> usize {
        self.entries.len()
    }
}

// Mostly a dupe of ThrottleMap where we provide the time
#[derive(Debug)]
struct PruneThrottle<N: NodeId> {
    until: HashMap<N, Instant>,
}

impl<N: NodeId> Default for PruneThrottle<N> {
    fn default() -> Self {
        Self {
            until: HashMap::new(),
        }
    }
}

impl<N: NodeId> PruneThrottle<N> {
    fn throttled(&self, peer: &N, now: Instant) -> bool {
        self.until.get(peer).is_some_and(|t| now < *t)
    }

    fn record(&mut self, peer: N, now: Instant, ttl: Duration) {
        self.until.insert(peer, now + ttl);
    }

    fn clear_expired(&mut self, now: Instant) {
        self.until.retain(|_, t| *t > now);
    }
}

// The current rtt measurements from quic are very prone to spikes and can move
// bucket frequently in a short window. We don't want to rebalance until we are
// sure a peer has actually moved rings.
// TODO: implement better rtt measurements in corrosion
const RING_EXTRA_CONFIRMATIONS: u32 = 8;

/// Full Plumtree protocol state for one local node.
#[derive(Debug)]
pub struct PlumtreeState<
    I: MessageId<NodeId = N>,
    P: Payload<MessageId = I, NodeId = N>,
    N: NodeId,
    S: SeenStore<I>,
> {
    /// This node's own id, excluded from every peer set.
    local_id: N,
    /// Tunables (fanout sizes, thresholds, throttles).
    config: Config,

    /// Peers on the spanning tree: they get full `Gossip` payloads (push).
    eager_peers: IndexSet<N>,
    /// Lazy peers only get `IHave` digests (lazy push).
    lazy_peers: IndexSet<N>,
    /// Every peer we know about, eager or lazy; drives.
    known_peers: IndexSet<N>,
    /// Eager peers pinned to the tree because they are our ring neighbors
    /// within `Config::ring_locked_radius`; they are never pruned.
    ring_locked: IndexSet<N>,
    /// Per-per rtt information.
    peer_topology: HashMap<N, RttInfo>,
    /// Candidate topology change awaiting confirmation
    pending_topology: HashMap<N, (RttInfo, u32)>,

    /// Digests buffered since the last tick, flushed to lazy peers as `IHave`.
    lazy_queue: Vec<IHaveDigest<I>>,
    /// Messages announced via `IHave` but not yet received
    missing: HashMap<I, MissingEntry<N>>,

    /// Dedup store: message ids we've already delivered, to drop duplicates.
    seen: S,
    /// Recently-delivered payloads, kept so we can answer inbound `Graft`s.
    cache: PayloadCache<I, P>,
    /// PRNG for random peer selection (seedable for deterministic tests).
    /// The peer sets are `IndexSet`s so that their iteration order, which
    /// decides what this RNG picks and the order of sends, depends only on
    /// the sequence of inserts and removes, not on the hasher.
    rng: SmallRng,
    /// Rate-limits outbound `Prune`s per peer to avoid flapping links.
    prune_throttle: PruneThrottle<N>,
    /// set on membership updates so we'd rebalance peers on next tick.
    needs_rebalance: bool,
    // target number of eager and lazy peers, calculated based on config
    // or cluster size if config is not set.
    fanout: FanoutTargets,
    /// `PeerSelection::Hrw`: this node's salt for `rank`, and the sets the
    /// last selection picked. A change moves only the peers whose label
    /// (eager, lazy, neither) differs between that and the new selection.
    rank_salt: u64,
    targets: Targets<N>,
}

impl<I: MessageId<NodeId = N>, P: Payload<MessageId = I, NodeId = N>, N: NodeId, S: SeenStore<I>>
    PlumtreeState<I, P, N, S>
{
    pub fn new_with_store(local_id: N, config: Config, seen: S) -> Self {
        Self::new_with_store_seeded(local_id, config, seen, rand::rng().random())
    }

    /// Like [`Self::new_with_store`] but with a deterministic RNG seed.
    ///
    /// All randomized decisions (eager/lazy selection, lazy eviction, graft
    /// sender choice) draw from this RNG, so simulations and tests are
    /// reproducible given the same seed and event order.
    pub fn new_with_store_seeded(local_id: N, config: Config, seen: S, seed: u64) -> Self {
        let cache_size = config.max_cached_payloads;
        let fanout = resolve_fanout(0, &config);
        Self {
            local_id,
            config,
            eager_peers: IndexSet::new(),
            lazy_peers: IndexSet::new(),
            known_peers: IndexSet::new(),
            ring_locked: IndexSet::new(),
            peer_topology: HashMap::new(),
            pending_topology: HashMap::new(),
            lazy_queue: Vec::new(),
            missing: HashMap::new(),
            seen,
            cache: PayloadCache::new(cache_size),
            rng: SmallRng::seed_from_u64(seed),
            prune_throttle: PruneThrottle::default(),
            needs_rebalance: false,
            fanout,
            // derived from the seed without drawing from `rng`
            rank_salt: mix64(seed),
            targets: Targets::default(),
        }
    }

    pub fn ring_locked_peers(&self) -> &IndexSet<N> {
        &self.ring_locked
    }

    pub fn has_message(&self, id: &I) -> bool {
        self.seen.contains(id)
    }

    pub fn eager_peers(&self) -> &IndexSet<N> {
        &self.eager_peers
    }

    pub fn lazy_peers(&self) -> &IndexSet<N> {
        &self.lazy_peers
    }

    pub fn known_peers(&self) -> &IndexSet<N> {
        &self.known_peers
    }

    pub fn lazy_queue(&self) -> &Vec<IHaveDigest<I>> {
        &self.lazy_queue
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn num_eager(&self) -> usize {
        self.fanout.num_eager
    }

    pub fn min_lazy(&self) -> usize {
        self.fanout.min_lazy
    }

    pub fn max_lazy(&self) -> usize {
        self.fanout.max_lazy
    }

    pub fn payload_cache_size(&self) -> usize {
        self.cache.size()
    }

    pub fn seen_cache_size(&self) -> usize {
        self.seen.size()
    }
    /// Recompute cached fanout when `config.num_eager` is unset and cluster
    /// size produces a new target. Returns `true` if any effective value changed.
    fn maybe_recompute_fanout(&mut self) -> bool {
        if self.config.num_eager.is_some() {
            return false;
        }
        let new = resolve_fanout(self.known_peers.len(), &self.config);
        if new == self.fanout {
            return false;
        }
        self.fanout = new;
        true
    }

    // used only in sim tests
    pub fn force_eager(&mut self, peer: N) {
        if self.known_peers.contains(&peer) {
            self.lazy_peers.swap_remove(&peer);
            self.eager_peers.insert(peer);
        }
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

        let self_actor_id = self.local_id;
        if payload.origin() == self.local_id {
            return;
        }

        if let Some(duplicates) = self.seen.observe(id.clone(), round) {
            if duplicates > self.config.prune_threshold && !self.ring_locked.contains(&sender) {
                // TODO: suppress prunes for recently grafted peers? this would help
                // with tree stability
                let suppressed = self
                    .config
                    .prune_throttle
                    .is_some_and(|_| self.prune_throttle.throttled(&sender, rt.now()));
                if suppressed {
                    rt.notify(Notification::PruneSuppressed(&sender));
                } else {
                    trace!(
                        ?self_actor_id,
                        ?sender,
                        "sending PRUNE due to duplicate gossip, triggered_by: {id:?}"
                    );

                    if let Some(ttl) = self.config.prune_throttle {
                        self.prune_throttle.record(sender, rt.now(), ttl);
                    }

                    rt.send(
                        sender,
                        PlumtreeMsg::Prune(PruneMsg {
                            sender: self.local_id,
                            triggered_by: Some(id.clone()),
                        }),
                    );
                    self.move_to_lazy(&sender, rt);
                }
            }
            rt.notify(Notification::DuplicateMessage(&id));
            return;
        }

        self.cache.insert(id.clone(), payload.clone(), round);
        rt.deliver(payload.clone());

        let next_round = round.saturating_add(1);
        if next_round == round {
            warn!(?self_actor_id, ?id, "plumtree gossip round overflowed");
        }

        let peers: Vec<N> = self
            .eager_peers
            .iter()
            .filter(|p| **p != sender && **p != self.local_id && payload.origin() != **p)
            .copied()
            .collect();
        trace!(
            ?self_actor_id,
            ?sender,
            "forwarding gossip to {} eager peers (id: {id:?}, round: {next_round}, peers: {peers:?})",
            peers.len(),
        );
        if !peers.is_empty() {
            rt.send_all(
                peers,
                PlumtreeMsg::Gossip(GossipMsg {
                    round: next_round,
                    sender: self.local_id,
                    payload,
                }),
            );
        }

        // if peer is not eager, ensure they are in lazy
        if !self.eager_peers.contains(&sender) {
            self.ensure_in_lazy(&sender, rt);
        }

        if let Some(entry) = self.missing.remove(&id)
            && let Some(optimization_threshold) = self.config.optimization_threshold
            && entry.round + optimization_threshold < round
            && entry.ihave_sender != sender
        {
            let sender = &entry.ihave_sender;
            debug!(
                ?self_actor_id,
                "sending graft to {sender:?} (optimization from id {id:?} with round {round})"
            );
            rt.send(
                *sender,
                PlumtreeMsg::Graft(GraftMsg {
                    sender: self.local_id,
                    send: false,
                    requests: vec![GraftRequest {
                        id: id.clone(),
                        round: entry.round,
                    }],
                }),
            );

            // paper has a prune here but we might prune a good path for different sender,
            // possibly need more info to determine if we should prune.
        }

        self.enqueue_ihave(id, round);
    }

    /// Handle an incoming IHave digest batch.
    ///
    /// For each digest we haven't already received, record it in the
    /// `missing` set and schedule one `IHaveTimeoutBatch` timer. If the full
    /// GOSSIP doesn't arrive before the timer fires, we'll GRAFT.
    pub fn handle_ihave(&mut self, msg: IHaveMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        let IHaveMsg { sender, digests } = msg;

        let mut senders = vec![sender];
        senders.extend(self.random_eager_peers(1, &sender));

        let mut new_ids = Vec::new();
        for digest in digests {
            // Same as gossip: we originated this message, nothing to pull.
            if digest.id.origin() == self.local_id {
                continue;
            }
            if self.seen.contains(&digest.id) {
                continue;
            }
            // todo: maybe we can store peers that have sent ihave's
            // and use them as a fallback
            if self.missing.contains_key(&digest.id) {
                continue;
            }

            new_ids.push(digest.id.clone());
            self.missing.insert(
                digest.id,
                MissingEntry {
                    ihave_sender: sender,
                    round: digest.round,
                },
            );
        }

        let self_actor_id = &self.local_id;
        if !new_ids.is_empty() {
            trace!(
                ?self_actor_id,
                ?sender,
                "handle_ihave, scheduling graft timeout for {}",
                new_ids.len(),
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

        let self_actor_id = &self.local_id;
        for req in requests {
            if let Some((payload, round)) = self.cache.get(&req.id).cloned() {
                debug!(
                    ?self_actor_id,
                    ?sender,
                    "sending cached payloads requested through graft ({:?})",
                    req.id
                );
                rt.send(
                    sender,
                    PlumtreeMsg::Gossip(GossipMsg {
                        round,
                        sender: self.local_id,
                        payload,
                    }),
                );
            } else {
                debug!(
                    ?self_actor_id,
                    ?sender,
                    "requested payload no longer cached ({:?})",
                    req.id,
                );
                rt.notify(Notification::PayloadNotCached(&req.id));
            }
        }
    }

    /// Handle an incoming PRUNE.
    ///
    pub fn handle_prune(&mut self, msg: PruneMsg<I, N>, rt: &mut impl Runtime<I, P, N>) {
        debug!(self_actor_id = ?self.local_id, sender = ?msg.sender, "received prune, triggered by {:?}", msg.triggered_by);
        if !self.ring_locked.contains(&msg.sender) {
            self.move_to_lazy(&msg.sender, rt);
        }
    }

    /// Handle a graceful shutdown.
    ///
    /// Sends a PRUNE to every eager peer so they stop treating us as part of
    /// the spanning tree and re-route around us.
    pub fn handle_shutdown(&mut self, rt: &mut impl Runtime<I, P, N>) {
        info!(self_actor_id = ?self.local_id, "sending prunes to eager peers due to shutdown");
        let peers = self.eager_peers.iter().copied().collect::<Vec<_>>();
        rt.send_all(
            peers,
            PlumtreeMsg::Prune(PruneMsg {
                sender: self.local_id,
                triggered_by: None,
            }),
        );
    }

    /// Originate a new message from this node.
    ///
    /// Marks it as received, caches the payload, sends GOSSIP to all
    /// eager peers (round 0), and enqueues IHave for lazy peers.
    pub fn broadcast(&mut self, id: I, payload: P, rt: &mut impl Runtime<I, P, N>) {
        debug!(
            self_actor_id = ?self.local_id,
            "broadcasting message to {} eager and {} lazy",
            self.eager_peers.len(),
            self.lazy_peers.len()
        );

        self.cache.insert(id.clone(), payload.clone(), 0);

        let peers = self
            .eager_peers
            .iter()
            .filter(|p| **p != self.local_id)
            .copied()
            .collect::<Vec<_>>();
        rt.send_all(
            peers,
            PlumtreeMsg::Gossip(GossipMsg {
                round: 0,
                sender: self.local_id,
                payload,
            }),
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
        if senders.is_empty() || retries >= senders.len() as u32 {
            return;
        }

        let send_to = senders[retries as usize];
        let mut graft_requests = Vec::new();

        for id in ids {
            if id.origin() == self.local_id {
                self.missing.remove(&id);
                continue;
            }
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
        }

        if !graft_requests.is_empty() {
            debug!(
                self_actor_id = ?self.local_id,
                "Handling ihave time out for {} requests", graft_requests.len(),
            );

            for chunk in graft_requests.chunks(10) {
                rt.send(
                    send_to,
                    PlumtreeMsg::Graft(GraftMsg {
                        sender: self.local_id,
                        send: true,
                        requests: chunk.to_vec(),
                    }),
                );
            }
            // supress prunes for a recently grafted peers
            self.prune_throttle
                .record(send_to, rt.now(), Duration::from_secs(3 * 60));

            let ids = graft_requests.iter().map(|r| r.id.clone()).collect();
            // reschedule a retry if we still have more senders
            if retries + 1 < senders.len() as u32 {
                rt.schedule(
                    Timer::IHaveTimeoutBatch {
                        ids,
                        retries: retries + 1,
                        senders,
                    },
                    self.config.ihave_timeout,
                );
            } else {
                rt.notify(Notification::MessageMissing(ids.len()));
                for id in ids {
                    self.missing.remove(&id);
                }
            }
        }
    }

    /// Periodic sends for all pending IHave digests to lazy peers.
    ///
    /// The caller should invoke this on a regular interval.
    pub fn tick(&mut self, rt: &mut impl Runtime<I, P, N>) {
        let digests = self.drain_lazy_queue();
        if self.lazy_peers.is_empty() || digests.is_none() {
            return;
        }
        let peers: Vec<N> = self.lazy_peers.iter().copied().collect();
        rt.send_all(
            peers,
            PlumtreeMsg::IHave(IHaveMsg {
                sender: self.local_id,
                digests: digests.unwrap(),
            }),
        );
    }

    fn random_eager_peers(&mut self, count: usize, exclude: &N) -> Vec<N> {
        self.eager_peers
            .iter()
            .filter(|p| *p != exclude)
            .choose_multiple(&mut self.rng, count)
            .into_iter()
            .copied()
            .collect()
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
                self.commit_topology(peer, info);
            }
        }
        self.maybe_recompute_fanout();
        self.rebalance(rt);
    }

    /// A new peer has come online and wants to join the overlay.
    pub fn peer_up(&mut self, peer: N, rtt: Option<RttInfo>, rt: &mut impl Runtime<I, P, N>) {
        if peer == self.local_id || self.known_peers.contains(&peer) {
            return;
        }

        let info = self.cached_ring_or(&peer, rtt.unwrap_or_default());
        self.commit_topology(peer, info);
        if self.config.peer_selection != PeerSelection::FullRebalance {
            self.apply_change(peer, PeerChange::Joined, rt);
            return;
        }
        self.maybe_recompute_fanout();
        if self.eager_peers.len() < self.num_eager() {
            self.move_to_eager(&peer, rt);
        } else {
            self.insert_into_lazy(peer, rt);
        }
        self.needs_rebalance = true;
    }

    /// Reconcile the overlay with a membership snapshot, then run the
    /// deferred rebalance if one is due — either a membership change flagged
    /// it (`needs_rebalance`), or an overlay peer crossed a bucket boundary
    /// (Near/Mid/Far).
    ///
    /// Members that are not known are added, whether they joined for the
    /// first time or rejoined after a `peer_down`. Known peers missing from
    /// the snapshot are removed as if `peer_down` had been called. Known
    /// peers present in both only move buckets after
    /// `RING_EXTRA_CONFIRMATIONS` consecutive runs. `peer_topology` keeps an
    /// entry for every peer ever seen: it is the RTT cache that places a
    /// rejoining peer before a fresh ring is known.
    ///
    /// Outside `PeerSelection::FullRebalance`, every change found here updates
    /// the sets right away and the run ends with `top_up` instead of a
    /// rebalance.
    pub fn update_peer_topology(
        &mut self,
        updates: impl IntoIterator<Item = (N, RttInfo)>,
        rt: &mut impl Runtime<I, P, N>,
    ) {
        let mut topology_changed = false;

        let prev_count = self.known_peers().len();
        let mut present: HashSet<N> = HashSet::with_capacity(prev_count);
        for (peer, info) in updates {
            if peer == self.local_id {
                continue;
            }
            present.insert(peer);
            match self.peer_topology.get(&peer).copied() {
                Some(existing) if self.known_peers.contains(&peer) => {
                    let old_bucket = RingBucket::of(existing);
                    let new_bucket = RingBucket::of(info);

                    // change topology if we get a ring for the first time
                    if existing.ring.is_none() && info.ring.is_some() {
                        self.pending_topology.remove(&peer);
                        self.peer_topology.insert(peer, info);
                        if new_bucket != old_bucket {
                            topology_changed = true;
                            self.apply_change(
                                peer,
                                PeerChange::MovedBucket { from: old_bucket },
                                rt,
                            );
                        }
                        continue;
                    }

                    if new_bucket == old_bucket {
                        self.pending_topology.remove(&peer);
                        continue;
                    }

                    let confirmations = match self.pending_topology.get(&peer) {
                        Some((pending_info, count))
                            if RingBucket::of(*pending_info) == new_bucket =>
                        {
                            count + 1
                        }
                        _ => 1,
                    };

                    if confirmations >= RING_EXTRA_CONFIRMATIONS {
                        info!(
                            "topology changed for peer {:?}, old: {:?}, new: {:?} (stable for {confirmations} runs)",
                            peer, existing, info
                        );
                        self.commit_topology(peer, info);
                        topology_changed = true;
                        self.apply_change(peer, PeerChange::MovedBucket { from: old_bucket }, rt);
                    } else {
                        // Not yet stable, record the candidate and wait.
                        self.pending_topology.insert(peer, (info, confirmations));
                    }
                }
                // first join, or a rejoin whose peer_up never arrived
                _ => {
                    let info = self.cached_ring_or(&peer, info);
                    info!("topology added peer {:?}, new: {:?}", peer, info);
                    self.commit_topology(peer, info);
                    topology_changed = true;
                    self.apply_change(peer, PeerChange::Joined, rt);
                }
            }
        }

        let departed: Vec<N> = self
            .known_peers
            .iter()
            .filter(|peer| !present.contains(peer))
            .copied()
            .collect();
        for peer in departed {
            info!("topology removed peer {:?}, absent from members", peer);
            self.peer_down(&peer, rt);
            topology_changed = true;
        }

        if self.known_peers().len() != prev_count {
            self.maybe_recompute_fanout();
        }

        if self.config.peer_selection != PeerSelection::FullRebalance {
            self.top_up(rt);
        } else if self.needs_rebalance || topology_changed {
            info!(
                "rebalancing peers: topology_changed: {topology_changed}, needs rebalance: {:?}",
                self.needs_rebalance
            );
            self.rebalance(rt);
            self.needs_rebalance = false;
        }
    }

    fn commit_topology(&mut self, peer: N, info: RttInfo) {
        self.peer_topology.insert(peer, info);
        self.known_peers.insert(peer);
        self.pending_topology.remove(&peer);
    }

    /// Fresh info wins. Without a ring, fall back to the last one cached
    /// in `peer_topology` so a rejoining peer is placed where it was.
    fn cached_ring_or(&self, peer: &N, info: RttInfo) -> RttInfo {
        match (info.ring, self.peer_topology.get(peer)) {
            (None, Some(cached)) => *cached,
            _ => info,
        }
    }

    /// Removes the peer from the overlay. `peer_topology` is left alone:
    /// it is the RTT cache consulted when the peer comes back.
    pub fn peer_down(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        let was_eager = self.eager_peers.swap_remove(peer);
        let was_lazy = self.lazy_peers.swap_remove(peer);
        self.known_peers.swap_remove(peer);
        self.ring_locked.swap_remove(peer);
        self.pending_topology.remove(peer);
        if self.config.peer_selection != PeerSelection::FullRebalance {
            let change = PeerChange::Left {
                bucket: self.peer_bucket(peer),
                was_eager,
                was_lazy,
            };
            self.apply_change(*peer, change, rt);
            return;
        }
        let fanout_changed = self.maybe_recompute_fanout();
        if was_eager || was_lazy || fanout_changed {
            self.needs_rebalance = true;
        }
    }

    // --- Hrw and IncrementalRandom ---

    /// Updates the sets right away for a peer that joined, left or moved
    /// bucket. `FullRebalance` instead flags a rebalance for the next tick,
    /// see the callers.
    fn apply_change(&mut self, peer: N, change: PeerChange, rt: &mut impl Runtime<I, P, N>) {
        if self.config.peer_selection == PeerSelection::FullRebalance {
            return;
        }
        let prev_num_eager = self.num_eager();
        self.maybe_recompute_fanout();
        if self.config.peer_selection == PeerSelection::Hrw {
            self.apply_ranked_targets(rt);
            if let PeerChange::Joined = change {
                self.default_to_lazy(peer, rt);
            }
        } else {
            self.place_randomly(peer, change, prev_num_eager, rt);
        }
    }

    /// A joining peer that is not eager starts lazy, so that it gets IHaves
    /// and can GRAFT. With a full lazy set it is only tracked: evicting
    /// another entry would change two entries instead of none.
    fn default_to_lazy(&mut self, peer: N, rt: &mut impl Runtime<I, P, N>) {
        if self.lazy_peers.len() < self.max_lazy() {
            self.ensure_in_lazy(&peer, rt);
        }
    }

    /// The maintenance tick outside `FullRebalance`: an idempotent repair
    /// instead of a rebalance.
    fn top_up(&mut self, rt: &mut impl Runtime<I, P, N>) {
        match self.config.peer_selection {
            PeerSelection::FullRebalance => {}
            PeerSelection::Hrw => self.apply_ranked_targets(rt),
            PeerSelection::IncrementalRandom => {
                self.lock_ring_neighbors(rt);
                // Refills lazy only: eager below the fanout is where PRUNE
                // left it, and refilling it every tick would undo that.
                while self.lazy_peers.len() < self.min_lazy().min(self.max_lazy()) {
                    let untracked = self.untracked_peers(None);
                    let Some(peer) = self.pick(untracked, None) else {
                        break;
                    };
                    self.insert_into_lazy(peer, rt);
                }
            }
        }
    }

    /// `Hrw`: moves only the peers whose label (eager, lazy or neither)
    /// differs between the previous and the new ranked selection. Every other
    /// entry stays as GRAFT, PRUNE or a gossip sender left it.
    fn apply_ranked_targets(&mut self, rt: &mut impl Runtime<I, P, N>) {
        self.set_ring_neighbors();
        let new = self.select_targets();
        let old = std::mem::replace(&mut self.targets, new.clone());

        // Removals first, then promotions, so that lazy inserts find room.
        for p in old.eager.iter().chain(&old.lazy) {
            if new.eager.contains(p) || new.lazy.contains(p) {
                continue;
            }
            if !old.eager.contains(p) {
                if self.lazy_peers.swap_remove(p) {
                    rt.notify(Notification::PeerEvictedFromLazy(p));
                }
            } else if self.eager_peers.swap_remove(p) {
                rt.notify(Notification::PeerDroppedFromEager(p));
            }
        }
        for p in new.eager.difference(&old.eager) {
            self.move_to_eager(p, rt);
        }
        for p in new.lazy.difference(&old.lazy) {
            if old.eager.contains(p) {
                self.move_to_lazy(p, rt);
            } else {
                self.ensure_in_lazy(p, rt);
            }
        }
        // A new ring neighbor may keep its eager label while an earlier
        // PRUNE left it in lazy.
        for p in self.ring_locked.clone() {
            self.move_to_eager(&p, rt);
        }
    }

    /// `IncrementalRandom`: places the one peer that changed and refills or
    /// trims a slot; every other entry stays.
    fn place_randomly(
        &mut self,
        peer: N,
        change: PeerChange,
        prev_num_eager: usize,
        rt: &mut impl Runtime<I, P, N>,
    ) {
        self.lock_ring_neighbors(rt);
        match change {
            PeerChange::Joined => self.admit_randomly(peer, rt),
            PeerChange::Left {
                bucket,
                was_eager,
                was_lazy,
            } => self.refill(peer, bucket, was_eager, was_lazy, rt),
            // a bucket change is a departure from the old bucket and a
            // join to the new one
            PeerChange::MovedBucket { from } => {
                let was_eager =
                    !self.ring_locked.contains(&peer) && self.eager_peers.swap_remove(&peer);
                if was_eager {
                    rt.notify(Notification::PeerDroppedFromEager(&peer));
                }
                let was_lazy = self.lazy_peers.swap_remove(&peer);
                if was_lazy {
                    rt.notify(Notification::PeerEvictedFromLazy(&peer));
                }
                self.refill(peer, from, was_eager, was_lazy, rt);
                self.admit_randomly(peer, rt);
            }
        }

        // A fanout change adds or removes one eager peer, from any bucket,
        // if the eager set is not already at the new size.
        let eager = self.eager_peers.len();
        match self.num_eager().cmp(&prev_num_eager) {
            Ordering::Greater if eager < self.num_eager() => {
                let candidates = self.non_eager_peers(None);
                if let Some(p) = self.pick(candidates, None) {
                    self.move_to_eager(&p, rt);
                }
            }
            Ordering::Less if eager > self.num_eager() => self.demote_random(None, rt),
            _ => {}
        }
    }

    /// One draw: eager with probability ~k/n, else lazy. k and n are taken
    /// per bucket (its eager share and its known peers, ring neighbors
    /// included), so that the Near/Mid/Far mix holds; with one bucket this is
    /// exactly k/n.
    fn admit_randomly(&mut self, peer: N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.contains(&peer) {
            // a new ring neighbor
            return;
        }
        let bucket = self.peer_bucket(&peer);
        let mut pool = [0; 3];
        for p in self.known_peers.iter() {
            if !self.ring_locked.contains(p) {
                pool[self.peer_bucket(p) as usize] += 1;
            }
        }
        let share = self.bucket_shares(pool)[bucket as usize];
        let known = (share.locked + share.pool).max(1) as f64;
        let eager = (share.locked + share.eager) as f64 / known;

        if self.rng.random::<f64>() < eager {
            self.make_eager(peer, rt);
        } else {
            self.default_to_lazy(peer, rt);
        }
    }

    /// Makes `peer` eager; with a full eager set, first demotes a random
    /// eager peer to lazy.
    fn make_eager(&mut self, peer: N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.len() >= self.num_eager() {
            self.demote_random(Some(self.peer_bucket(&peer)), rt);
        }
        self.move_to_eager(&peer, rt);
    }

    /// Demotes a random eager peer that is not ring-locked to lazy, from
    /// `bucket` if it has one.
    fn demote_random(&mut self, bucket: Option<RingBucket>, rt: &mut impl Runtime<I, P, N>) {
        let candidates = self
            .eager_peers
            .iter()
            .filter(|p| !self.ring_locked.contains(*p))
            .copied()
            .collect();
        if let Some(p) = self.pick(candidates, bucket) {
            self.move_to_lazy(&p, rt);
        }
    }

    /// Refills the slot `left` held, below the fanout or `min_lazy`, with a
    /// random peer from `bucket` if it has one.
    fn refill(
        &mut self,
        left: N,
        bucket: RingBucket,
        was_eager: bool,
        was_lazy: bool,
        rt: &mut impl Runtime<I, P, N>,
    ) {
        if was_eager && self.eager_peers.len() < self.num_eager() {
            let candidates = self.non_eager_peers(Some(left));
            if let Some(p) = self.pick(candidates, Some(bucket)) {
                self.move_to_eager(&p, rt);
            }
        }
        if was_lazy && self.lazy_peers.len() < self.min_lazy() {
            let candidates = self.untracked_peers(Some(left));
            if let Some(p) = self.pick(candidates, Some(bucket)) {
                self.insert_into_lazy(p, rt);
            }
        }
    }

    /// Recomputes the ring neighbors and makes new ones eager. A former
    /// neighbor keeps its place as an ordinary entry.
    fn lock_ring_neighbors(&mut self, rt: &mut impl Runtime<I, P, N>) {
        self.set_ring_neighbors();
        let unlocked: Vec<N> = self
            .ring_locked
            .iter()
            .filter(|p| !self.eager_peers.contains(*p))
            .copied()
            .collect();
        for p in unlocked {
            self.make_eager(p, rt);
        }
    }

    fn non_eager_peers(&self, except: Option<N>) -> Vec<N> {
        self.known_peers
            .iter()
            .filter(|p| !self.eager_peers.contains(*p) && Some(**p) != except)
            .copied()
            .collect()
    }

    /// Known peers in neither set.
    fn untracked_peers(&self, except: Option<N>) -> Vec<N> {
        self.non_eager_peers(except)
            .into_iter()
            .filter(|p| !self.lazy_peers.contains(p))
            .collect()
    }

    /// A random candidate, from `bucket` if one is there.
    fn pick(&mut self, mut candidates: Vec<N>, bucket: Option<RingBucket>) -> Option<N> {
        if let Some(bucket) = bucket {
            let same: Vec<N> = candidates
                .iter()
                .copied()
                .filter(|p| self.peer_bucket(p) == bucket)
                .collect();
            if !same.is_empty() {
                candidates = same;
            }
        }
        candidates.choose(&mut self.rng).copied()
    }

    // --- Rebalance ---

    /// Clears existing lazy / eager peers and selects them again.
    ///
    /// Ring neighbors are always eager. Remaining eager slots are split across
    /// near, mid, and far RTT bucket. Locked peers count toward the bucket targets.
    ///
    /// todo: maybe re-implement rebalance so we don't clear exisiting eager/lazy peers.
    fn rebalance(&mut self, rt: &mut impl Runtime<I, P, N>) {
        rt.notify(Notification::Rebalance);

        self.eager_peers.clear();
        self.lazy_peers.clear();
        self.ring_locked.clear();

        self.set_ring_neighbors();
        let targets = self.select_targets();
        self.eager_peers.extend(targets.eager.iter().copied());
        self.lazy_peers.extend(targets.lazy.iter().copied());
        self.targets = targets;
        trace!(
            self_actor_id = ?self.local_id,
            eager = self.eager_peers.len(),
            lazy = self.lazy_peers.len(),
            ring_locked = self.ring_locked.len(),
            known = self.known_peers.len(),
            "rebalance complete (eager: {:?}, lazy: {:?})", self.eager_peers, self.lazy_peers
        );
    }

    /// The eager and lazy peers of a full selection, given the current ring
    /// neighbors: every known peer if they fit in the fanout, else the ring
    /// neighbors plus each bucket's eager share from the front of its pool,
    /// and the lazy share right after it.
    fn select_targets(&mut self) -> Targets<N> {
        let mut targets = Targets::default();
        if self.known_peers.len() <= self.num_eager() {
            targets.eager.extend(self.known_peers.iter().copied());
            return targets;
        }

        // ring-locked peers are always eager so a node is never isolated.
        targets.eager.extend(self.ring_locked.iter().copied());

        // pools exclude ring_locked peers so they aren't reselected.
        let pools = self.bucket_pools();
        let shares = self.bucket_shares(pools.each_ref().map(Vec::len));
        for (pool, share) in pools.iter().zip(shares) {
            targets.eager.extend(pool.iter().take(share.eager).copied());
        }

        // same selection logic for lazy peers since they are eager candidates.
        for (pool, share) in pools.iter().zip(shares) {
            targets
                .lazy
                .extend(pool.iter().skip(share.eager).take(share.lazy).copied());
        }
        targets
    }

    /// Known peers that are not ring-locked, per bucket (Near, Mid, Far), in
    /// selection order: ranked for `Hrw`, shuffled otherwise.
    fn bucket_pools(&mut self) -> [Vec<N>; 3] {
        let mut pools: [Vec<N>; 3] = Default::default();
        for p in self.known_peers.iter() {
            if !self.ring_locked.contains(p) {
                pools[self.peer_bucket(p) as usize].push(*p);
            }
        }

        let salt = self.rank_salt;
        for pool in &mut pools {
            if self.config.peer_selection == PeerSelection::Hrw {
                pool.sort_by_cached_key(|p| Reverse(rank(salt, p)));
            } else {
                pool.shuffle(&mut self.rng);
            }
        }
        pools
    }

    /// Splits the fanout across the buckets for pools of the given sizes,
    /// with the Near/Mid/Far ratios of `EagerRatios`.
    fn bucket_shares(&self, pool: [usize; 3]) -> [BucketShare; 3] {
        let mut locked = [0; 3];
        for p in self.ring_locked.iter() {
            locked[self.peer_bucket(p) as usize] += 1;
        }
        let (near, mid, far) = Self::bucket_targets(
            self.num_eager(),
            pool[0] + locked[0],
            pool[1] + locked[1],
            pool[2] + locked[2],
            self.config.eager_ratios,
        );
        let eager = [
            near.saturating_sub(locked[0]),
            mid.saturating_sub(locked[1]),
            far.saturating_sub(locked[2]),
        ];
        let (near, mid, far) = Self::bucket_targets(
            self.min_lazy(),
            pool[0].saturating_sub(eager[0]),
            pool[1].saturating_sub(eager[1]),
            pool[2].saturating_sub(eager[2]),
            self.config.eager_ratios,
        );
        let lazy = [near, mid, far];
        std::array::from_fn(|b| BucketShare {
            locked: locked[b],
            pool: pool[b],
            eager: eager[b],
            lazy: lazy[b],
        })
    }

    fn bucket_targets(
        num_eager: usize,
        near_cap: usize,
        mid_cap: usize,
        far_cap: usize,
        ratios: EagerRatios,
    ) -> (usize, usize, usize) {
        debug_assert!(ratios.validate().is_ok());
        let near_pct = ratios.near_pct as usize;
        let mid_pct = ratios.mid_pct as usize;
        let near = (num_eager * near_pct).div_ceil(100).min(near_cap);
        let mid = (num_eager * mid_pct).div_ceil(100).min(mid_cap);
        let far = num_eager.saturating_sub(near + mid).min(far_cap);

        // update near and mid, if we still have some room
        let near = near_cap.min(num_eager.saturating_sub(mid + far));
        let mid = mid_cap.min(num_eager.saturating_sub(near + far));
        (near, mid, far)
    }

    fn peer_bucket(&self, p: &N) -> RingBucket {
        RingBucket::of(self.peer_topology.get(p).copied().unwrap_or_default())
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

    /// Recompute ring neighbors based on current known_peers.
    ///
    /// Locks `ring_locked_radius` peers on each side of `local_id` in sorted order.
    fn set_ring_neighbors(&mut self) {
        self.ring_locked.clear();

        let mut peers: Vec<_> = self.known_peers.iter().collect();
        peers.push(&self.local_id);
        if peers.len() <= 1 {
            return;
        }

        peers.sort();
        let Some(position) = peers.iter().position(|p| *p == &self.local_id) else {
            return;
        };
        let len = peers.len();
        let radius = self.config.ring_locked_radius.max(1);
        for i in 1..=radius {
            let after = *peers[(position + i) % len];
            let before = *peers[(position + len - i) % len];
            if after != self.local_id {
                self.ring_locked.insert(after);
            }
            if before != self.local_id {
                self.ring_locked.insert(before);
            }
        }
    }

    fn move_to_eager(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.contains(peer) || !self.known_peers.contains(peer) {
            return;
        }

        self.eager_peers.insert(*peer);
        let was_lazy = self.lazy_peers.swap_remove(peer);
        trace!(
            self_actor_id = ?self.local_id,
            ?peer,
            "peer moved to eager (was_lazy: {was_lazy})"
        );
        rt.notify(Notification::PeerMovedToEager(peer));
    }

    fn move_to_lazy(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.ring_locked.contains(peer) {
            warn!(
                self_actor_id = ?self.local_id,
                ?peer,
                "move_to_lazy skipped (ring-locked eager peer)"
            );
            return;
        }

        if self.lazy_peers.contains(peer) || !self.known_peers.contains(peer) {
            return;
        }

        let was_eager = self.eager_peers.swap_remove(peer);
        if was_eager {
            rt.notify(Notification::PeerDroppedFromEager(peer));
        }

        self.insert_into_lazy(*peer, rt);
        trace!(
            self_actor_id = ?self.local_id,
            ?peer,
            "peer moved to lazy: (was_eager: {was_eager})"
        );
    }

    /// Ensure a peer is at least in the lazy set so they receive IHave
    /// digests. Does nothing if the peer is already in eager or lazy.
    fn ensure_in_lazy(&mut self, peer: &N, rt: &mut impl Runtime<I, P, N>) {
        if self.eager_peers.contains(peer)
            || self.lazy_peers.contains(peer)
            || !self.known_peers.contains(peer)
        {
            return;
        }
        self.insert_into_lazy(*peer, rt);
    }

    /// Insert a peer into the lazy set, evicting a random non-ring-locked
    /// peer if at capacity. Returns true if the peer was inserted.
    ///
    fn insert_into_lazy(&mut self, peer: N, rt: &mut impl Runtime<I, P, N>) -> bool {
        if self.lazy_peers.contains(&peer) {
            return true;
        }

        if self.lazy_peers.len() < self.max_lazy() {
            self.lazy_peers.insert(peer);
            rt.notify(Notification::PeerMovedToLazy(&peer));
            return true;
        }

        // Lazy is full — evict a random lazy peer.
        let idx = self.rng.random_range(0..self.lazy_peers().len());
        let evicted = self.lazy_peers.swap_remove_index(idx);

        if let Some(evicted) = evicted {
            trace!(
                self_actor_id = ?self.local_id,
                ?peer,
                "inserted into lazy, evicted peer {evicted:?} to make room"
            );
            self.lazy_peers.insert(peer);
            rt.notify(Notification::PeerMovedToLazy(&peer));
            rt.notify(Notification::PeerEvictedFromLazy(&evicted));
            true
        } else {
            debug!(
                self_actor_id = ?self.local_id,
                ?peer,
                "unable to insert into lazy, peer dropped"
            );
            false
        }
    }

    pub fn cache_evict_if_needed(&mut self, rt: &mut impl Runtime<I, P, N>) {
        self.seen.evict_if_needed();
        self.cache.evict_if_needed();
        self.prune_throttle.clear_expired(rt.now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter;

    /// Packed as `(origin << 8) | seq`.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub(crate) struct TestMsgId(u16);

    impl TestMsgId {
        fn new(origin: TestNodeId, seq: u8) -> Self {
            Self(((origin as u16) << 8) | seq as u16)
        }
    }

    fn tid(origin: TestNodeId, seq: u8) -> TestMsgId {
        TestMsgId::new(origin, seq)
    }

    pub(crate) type TestNodeId = u8;

    #[derive(Debug, Clone, PartialEq)]
    pub(crate) struct TestPayload(pub Vec<u8>);

    impl Payload for TestPayload {
        type MessageId = TestMsgId;
        type NodeId = TestNodeId;
        fn message_id(&self) -> Self::MessageId {
            TestMsgId::new(self.0[0], self.0[1])
        }

        fn origin(&self) -> Self::NodeId {
            self.0[0]
        }
    }

    impl MessageId for TestMsgId {
        type NodeId = TestNodeId;
        fn origin(&self) -> TestNodeId {
            (self.0 >> 8) as u8
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

        fn size(&self) -> usize {
            self.entries.len()
        }

        fn contains(&self, id: &TestMsgId) -> bool {
            self.entries.contains_key(id)
        }

        fn observe(&mut self, id: TestMsgId, round: Round) -> Option<u32> {
            let existing = self.entries.get_mut(&id);
            if let Some((_, seen)) = existing {
                // *existing = round;
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
            num_eager: Some(5),
            min_lazy: Some(10),
            max_lazy: Some(15),
            prune_threshold: 1,
            max_received_entries: 10000,
            prune_throttle: None,
            eager_ratios: EagerRatios::default(),
            ring_locked_radius: 1,
            peer_selection: PeerSelection::FullRebalance,
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
    }

    impl AccumulatingRuntime {
        fn clear(&mut self) {
            self.sent.clear();
            self.delivered.clear();
            self.scheduled.clear();
        }
    }

    impl Runtime<TestMsgId, TestPayload, TestNodeId> for AccumulatingRuntime {
        fn send_all(
            &mut self,
            peers: Vec<TestNodeId>,
            msg: PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>,
        ) {
            for peer in peers {
                self.send(peer, msg.clone());
            }
        }

        fn send(&mut self, to: TestNodeId, msg: PlumtreeMsg<TestMsgId, TestPayload, TestNodeId>) {
            self.sent.push((to, msg));
        }

        fn deliver(&mut self, payload: TestPayload) {
            self.delivered.push(payload);
        }

        fn schedule(&mut self, timer: Timer<TestMsgId, TestNodeId>, after: Duration) {
            self.scheduled.push((timer, after));
        }

        fn notify(&mut self, _notification: Notification<'_, TestMsgId, TestNodeId>) {}

        fn now(&self) -> Instant {
            Instant::now()
        }
    }

    fn payload(node: u8, v: u8) -> TestPayload {
        TestPayload(vec![node, v])
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

    #[test]
    fn derived_fanout_updates_with_cluster_growth() {
        let mut cfg = test_config();
        cfg.num_eager = None;
        cfg.min_lazy = None;
        cfg.max_lazy = None;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        assert_eq!(s.num_eager(), 3);
        assert_eq!(s.min_lazy(), 4);
        assert_eq!(s.max_lazy(), 6);

        // 99 known peers -> cluster size 100 -> log10(100)*3 = 6 eager.
        let peers: Vec<_> = (1..=99).collect();
        s.add_peers_bulk(peers, &mut rt);
        assert_eq!(s.known_peers().len(), 99);
        assert_eq!(s.num_eager(), 6);
        assert_eq!(s.min_lazy(), 9);
        assert_eq!(s.max_lazy(), 12);
        assert!(s.eager_peers().len() <= s.num_eager());
    }

    #[test]
    fn test_membership_events() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        assert!(s.eager_peers.contains(&1));
        assert!(!s.lazy_peers.contains(&1));

        for i in 1..=5 {
            s.peer_up(i, None, &mut rt);
        }
        assert_eq!(s.eager_peers.len(), 5);

        // lazy sets gets peers when eager is full
        s.peer_up(6, None, &mut rt);
        assert!(s.known_peers.contains(&6));
        // Sixth peer may be ring-locked (must be eager) or placed in lazy.
        assert!(s.eager_peers.contains(&6) || s.lazy_peers.contains(&6));

        let prev_eager = s.eager_peers().clone();
        let prev_lazy = s.lazy_peers().clone();
        // duplicate peer up events should be ignored
        s.peer_up(1, None, &mut rt);
        assert_eq!(s.eager_peers().clone(), prev_eager);
        assert_eq!(s.lazy_peers().clone(), prev_lazy);

        // peer_up only flags a rebalance; flush it so ring neighbors are picked.
        s.update_peer_topology(known_snapshot(&s), &mut rt);

        // test that ring-locked peers are eager
        // since id is zero, ring locked peers are 1 and 6
        assert!(s.ring_locked_peers().contains(&1));
        assert!(s.ring_locked_peers().contains(&6));

        assert!(s.eager_peers().contains(&1));
        assert!(s.eager_peers().contains(&6));
        assert!(!s.lazy_peers().contains(&1));
        assert!(!s.lazy_peers().contains(&6));

        // new peer that takes a locked position gets ring-locked after rebalance
        s.peer_up(7, None, &mut rt);
        s.update_peer_topology(known_snapshot(&s), &mut rt);
        assert!(s.ring_locked_peers().contains(&7));
        assert!(s.eager_peers().contains(&7));
        assert!(!s.lazy_peers().contains(&7));
        assert_eq!(s.known_peers().len(), 7);

        // removal of a ring-locked peer flags a rebalance; flush selects new ones
        s.peer_down(&7, &mut rt);
        s.update_peer_topology(known_snapshot(&s), &mut rt);
        assert!(!s.ring_locked_peers().contains(&7));
        assert!(!s.eager_peers().contains(&7));
        assert!(!s.lazy_peers().contains(&7));
        assert_eq!(s.known_peers().len(), 6);

        // prunes should never delete ring-locked peers
        s.handle_prune(
            PruneMsg {
                sender: 1,
                triggered_by: Some(tid(1, 0)),
            },
            &mut rt,
        );
        assert!(s.eager_peers().contains(&1));

        // prunes should move non-ring-locked peers to lazy
        let rand_eager = *s
            .eager_peers()
            .iter()
            .find(|p| !s.ring_locked_peers().contains(*p))
            .unwrap();
        s.handle_prune(
            PruneMsg {
                sender: rand_eager,
                triggered_by: Some(tid(1, 0)),
            },
            &mut rt,
        );
        assert!(!s.eager_peers().contains(&rand_eager));
        assert!(s.lazy_peers().contains(&rand_eager));

        // peer down removes member from any internal sets
        s.peer_down(&6, &mut rt);
        assert!(!s.eager_peers().contains(&6));
        assert!(!s.lazy_peers().contains(&6));
        assert_eq!(s.known_peers().len(), 5);
    }

    #[test]
    fn ring_locked_radius_locks_both_sides() {
        let mut cfg = test_config();
        cfg.ring_locked_radius = 2;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();
        for i in 1..=6 {
            s.peer_up(i, None, &mut rt);
        }
        s.update_peer_topology(known_snapshot(&s), &mut rt);

        // sorted ring: 0,1,2,3,4,5,6 → radius 2 locks 5,6 and 1,2
        assert_eq!(s.ring_locked_peers().len(), 4);
        for p in [1u8, 2, 5, 6] {
            assert!(s.ring_locked_peers().contains(&p));
            assert!(s.eager_peers().contains(&p));
        }
        assert!(!s.ring_locked_peers().contains(&3));
        assert!(!s.ring_locked_peers().contains(&4));
    }

    #[test]
    fn peer_up_both_full_ignored() {
        let mut cfg = test_config();
        cfg.num_eager = Some(2);
        cfg.min_lazy = Some(2);
        cfg.max_lazy = Some(2);
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
    fn prune_drops_eager_peer() {
        let mut cfg = test_config();
        cfg.num_eager = Some(3);
        cfg.min_lazy = Some(5);
        cfg.max_lazy = Some(5);
        let mut s: PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore> =
            PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, None, &mut rt); // eager
        s.peer_up(2, None, &mut rt); // eager
        s.peer_up(3, None, &mut rt); // eager (full)
        s.peer_up(4, None, &mut rt); // lazy
        s.peer_up(5, None, &mut rt); // lazy
        s.update_peer_topology(known_snapshot(&s), &mut rt);
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
                triggered_by: Some(tid(1, 99)),
            },
            &mut rt,
        );
        assert_eq!(s.eager_peers.len(), 2, "peer should be dropped from eager");
        assert_eq!(s.lazy_peers.len(), 3);
        assert!(!s.eager_peers.contains(&prune_sender));
        assert!(s.lazy_peers.contains(&prune_sender));

        // prune for ring-locked peer shouldn't succeed
        s.handle_prune(
            PruneMsg {
                sender: 1,
                triggered_by: Some(tid(1, 99)),
            },
            &mut rt,
        );
        assert!(s.eager_peers().contains(&1));
    }

    #[test]
    fn broadcast_sends_to_eager_and_enqueues_lazy() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(1, None, &mut rt); // eager
        s.peer_up(2, None, &mut rt); // eager
        // manually move 3 to lazy
        s.lazy_peers.insert(3);

        s.broadcast(tid(0, 42), payload(0, 42), &mut rt);

        // Should have sent GOSSIP to eager peers 1 and 2
        assert_eq!(rt.sent.len(), 2);
        for (to, m) in &rt.sent {
            let g = unwrap_gossip(m);
            assert_eq!(g.payload.message_id(), tid(0, 42));
            assert_eq!(g.round, 0);
            assert_eq!(g.sender, 0); // local_id
            assert!(s.eager_peers.contains(to));
        }

        // Lazy queue for peer 3 should have one digest
        assert_eq!(s.lazy_queue.len(), 1);
        assert!(
            s.lazy_queue
                .iter()
                .any(|d| d.id == tid(0, 42) && d.round == 0)
        );

        // we don't track local messages
        assert!(!s.has_message(&tid(0, 42)));

        // Receive a GOSSIP from peer 4 (not yet in our peer set)
        rt.clear();
        s.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 1,
                payload: payload(1, 10),
            },
            &mut rt,
        );
        // Delivered once
        assert_eq!(rt.delivered.len(), 1);
        assert_eq!(rt.delivered[0].message_id(), tid(1, 10));

        // Forwarded to eager peers 2, 3 (not sender 1)
        let gossip_targets: Vec<u8> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Gossip(_)))
            .map(|(to, _)| *to)
            .collect();
        assert!(gossip_targets.contains(&2));
        assert!(!gossip_targets.contains(&1)); // not back to sender

        // Forwarded gossips have round + 1
        for (_, m) in &rt.sent {
            if let PlumtreeMsg::Gossip(g) = m {
                assert_eq!(g.round, 2);
                assert_eq!(g.sender, 0);
            }
        }

        assert!(!s.lazy_peers.contains(&1));
        // lazy queue
        assert_eq!(s.lazy_queue.len(), 2);
        assert!(s.has_message(&tid(1, 10)));

        rt.clear();

        // unknown peer message gets processed but isn't added to peer set
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 5,
                payload: payload(5, 15),
            },
            &mut rt,
        );
        assert_eq!(s.lazy_queue.len(), 3);
        assert!(s.has_message(&tid(5, 15)));
        assert!(!s.lazy_peers.contains(&5));

        // duplicate gossip gets pruned
        rt.clear();
        s.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 11,
                payload: payload(5, 15),
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
        assert_eq!(prune.triggered_by, Some(tid(5, 15)));

        assert!(!s.lazy_peers.contains(&11));
        assert!(!s.eager_peers.contains(&11));
    }

    #[test]
    fn handle_gossip_ignores_own_origin() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.peer_up(1, None, &mut rt);
        rt.clear();

        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: 1,
                payload: payload(0, 7), // origin == local_id
            },
            &mut rt,
        );

        assert!(rt.delivered.is_empty());
        assert!(rt.sent.is_empty());
        assert!(!s.has_message(&tid(0, 7)));
        assert!(s.lazy_queue.is_empty());
    }

    #[test]
    fn handle_ihave_ignores_own_origin() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![
                    IHaveDigest {
                        id: tid(0, 1), // own origin
                        round: 0,
                    },
                    IHaveDigest {
                        id: tid(1, 2),
                        round: 0,
                    },
                ],
            },
            &mut rt,
        );

        assert!(!s.missing.contains_key(&tid(0, 1)));
        assert!(s.missing.contains_key(&tid(1, 2)));
        assert_eq!(s.missing.len(), 1);
    }

    #[test]
    fn handle_gossip_optimization_grafts_shorter_path() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        // optimization_threshold = 3

        // Peer 5 tells us about msg(42) via IHave at round 1
        s.peer_up(5, None, &mut rt);
        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![
                    IHaveDigest {
                        id: tid(10, 42),
                        round: 1,
                    },
                    IHaveDigest {
                        id: tid(10, 45),
                        round: 2,
                    },
                ],
            },
            &mut rt,
        );
        assert!(s.missing.contains_key(&tid(10, 42)));
        rt.clear();

        // Now the GOSSIP arrives from peer 1 at round 10 (1 + 3 < 10)
        s.peer_up(1, None, &mut rt);
        s.handle_gossip(
            GossipMsg {
                round: 10,
                sender: 1,
                payload: payload(10, 42),
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
        assert!(!s.missing.contains_key(&tid(10, 42)));

        // check that nothing gets triggered if round is very close
        rt.clear();
        s.handle_gossip(
            GossipMsg {
                round: 3,
                sender: 1,
                payload: payload(10, 45),
            },
            &mut rt,
        );

        let grafts: Vec<_> = rt
            .sent
            .iter()
            .filter(|(_, m)| matches!(m, PlumtreeMsg::Graft(_)))
            .collect();

        assert!(grafts.is_empty());
        assert!(!s.missing.contains_key(&tid(10, 45)));
    }

    // -----------------------------------------------------------------------
    // handle_ihave
    // -----------------------------------------------------------------------

    #[test]
    fn handle_ihave_schedules_timer() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.seen.observe(tid(1, 3), 0);
        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![
                    IHaveDigest {
                        id: tid(1, 1),
                        round: 0,
                    },
                    IHaveDigest {
                        id: tid(1, 2),
                        round: 1,
                    },
                    IHaveDigest {
                        id: tid(1, 3),
                        round: 1,
                    },
                ],
            },
            &mut rt,
        );

        assert_eq!(s.missing.len(), 2);
        assert_eq!(rt.scheduled.len(), 1);
        // already received changes aren't added to missing
        assert!(!s.missing.contains_key(&tid(1, 3)));
        assert_eq!(
            rt.scheduled[0].0,
            Timer::IHaveTimeoutBatch {
                ids: vec![tid(1, 1), tid(1, 2)],
                retries: 0,
                senders: vec![5],
            }
        );
        assert_eq!(rt.scheduled[0].1, Duration::from_secs(3));
    }

    #[test]
    fn handle_graft_sends_cached_payload() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        // First, broadcast so the payload is cached
        s.broadcast(tid(42, 1), payload(42, 1), &mut rt);
        rt.clear();

        // Peer 5 sends GRAFT
        s.known_peers.insert(5);
        s.handle_graft(graft_msg(5, true, vec![(tid(42, 1), 0)]), &mut rt);

        // Peer 5 promoted to eager
        assert!(s.eager_peers.contains(&5));

        // GOSSIP sent back with cached payload
        assert_eq!(rt.sent.len(), 1);
        let (to, m) = &rt.sent[0];
        assert_eq!(*to, 5);
        let g = unwrap_gossip(m);
        assert_eq!(g.payload, payload(42, 1));

        rt.clear();
        s.seen.observe(tid(1, 99), 0);
        s.handle_graft(graft_msg(5, true, vec![(tid(1, 99), 0)]), &mut rt);

        // seen but not cached — graft must not send gossip back
        assert!(s.has_message(&tid(1, 99)));
        assert!(rt.sent.is_empty());
    }

    #[test]
    fn handle_ihave_timeout_partial_noop() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.peer_up(5, None, &mut rt);
        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![
                    IHaveDigest {
                        id: tid(1, 1),
                        round: 0,
                    },
                    IHaveDigest {
                        id: tid(1, 2),
                        round: 0,
                    },
                ],
            },
            &mut rt,
        );
        s.seen.observe(tid(1, 1), 0);
        rt.sent.clear();
        rt.scheduled.clear();

        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![tid(1, 1), tid(1, 2)],
                retries: 0,
                senders: vec![5, 6],
            },
            &mut rt,
        );

        assert_eq!(rt.sent.len(), 1);
        {
            let (to, m) = &rt.sent[0];
            let graft = unwrap_graft(m);
            assert_eq!(*to, 5);
            assert_eq!(graft.requests.len(), 1);
            assert_eq!(graft.requests[0].id, tid(1, 2));
            assert_eq!(graft.requests[0].round, 0);
        }

        assert!(s.eager_peers.contains(&5));
        assert!(!s.missing.contains_key(&tid(1, 1)));
        assert!(s.missing.contains_key(&tid(1, 2)));
        assert_eq!(rt.scheduled.len(), 1);
        // schedule retry
        let scheduled = Timer::IHaveTimeoutBatch {
            ids: vec![tid(1, 2)],
            retries: 1,
            senders: vec![5, 6],
        };
        assert_eq!(rt.scheduled[0].0, scheduled,);

        rt.sent.clear();
        rt.scheduled.clear();

        s.timer_fired(scheduled, &mut rt);

        let graft = unwrap_graft(&rt.sent[0].1);
        assert_eq!(graft.requests.len(), 1);
        assert_eq!(rt.sent[0].0, 6);
        assert!(!s.missing.contains_key(&tid(1, 2)));
        assert_eq!(rt.scheduled.len(), 0);
    }

    #[test]
    fn timer_fired_ihave_timeout_chunks_graft_requests() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        let digests: Vec<_> = (1..=15)
            .map(|n| IHaveDigest {
                id: tid(1, n),
                round: 0,
            })
            .collect();

        s.handle_ihave(IHaveMsg { sender: 5, digests }, &mut rt);
        rt.sent.clear();
        rt.scheduled.clear();

        let ids: Vec<_> = (1..=15).map(|n| tid(1, n)).collect();
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
    fn timer_fired_noop_if_already_received() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.handle_ihave(
            IHaveMsg {
                sender: 5,
                digests: vec![IHaveDigest {
                    id: tid(2, 1),
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
                payload: payload(2, 1),
            },
            &mut rt,
        );
        rt.sent.clear();

        // Timer fires — but missing entry already removed
        s.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![tid(2, 1)],
                retries: 0,
                senders: vec![5],
            },
            &mut rt,
        );
        assert!(rt.sent.is_empty());
    }

    #[test]
    fn tick_flushes_lazy_queues() {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();

        s.lazy_peers.insert(3);
        s.lazy_peers.insert(4);

        s.enqueue_ihave(tid(1, 1), 0);
        s.enqueue_ihave(tid(1, 2), 1);

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
    fn rebalance_uses_weighted_ring_buckets() {
        let mut cfg = test_config();
        cfg.num_eager = Some(8);
        cfg.min_lazy = Some(10);
        cfg.max_lazy = Some(10);
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        let peers = (1u8..=12)
            .map(|peer| {
                let ring = match peer {
                    1..=4 => Some(0),
                    5 => Some(2),
                    _ => Some(5),
                };
                (peer, RttInfo { ring })
            })
            .collect();
        s.add_peers_bulk_with_rtt(peers, &mut rt);

        assert_eq!(s.eager_peers.len(), 8);
        assert!(
            s.ring_locked_peers()
                .iter()
                .all(|p| s.eager_peers.contains(p))
        );

        let near = s
            .eager_peers
            .iter()
            .filter(|p| s.peer_bucket(p) == RingBucket::Near)
            .count();
        let mid = s
            .eager_peers
            .iter()
            .filter(|p| s.peer_bucket(p) == RingBucket::Mid)
            .count();
        let far = s
            .eager_peers
            .iter()
            .filter(|p| s.peer_bucket(p) == RingBucket::Far)
            .count();
        assert!(near == 4, "expected near slots to be favored: {near}");
        assert!(
            mid == 1,
            "expected mid bucket coverage to have just one peer: {mid}"
        );
        assert!(far == 3, "expected far bucket coverage: {far}");

        // rest should get added to lazy
        assert_eq!(s.lazy_peers().len(), 4);
    }

    #[test]
    fn move_to_lazy_respects_max_lazy() {
        let mut cfg = test_config();
        cfg.num_eager = Some(3);
        // Need room for both prior lazy (6) and demoted peer (5) so 6 is not evicted from lazy.
        cfg.min_lazy = Some(2);
        cfg.max_lazy = Some(2);
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
        s.seen.observe(tid(dup_sender, 1), 0);
        s.handle_gossip(
            GossipMsg {
                round: 0,
                sender: dup_sender,
                payload: payload(dup_sender, 1),
            },
            &mut rt,
        );

        assert!(!s.eager_peers.contains(&dup_sender));
        assert!(s.lazy_peers.contains(&dup_sender));
    }

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
                payload: payload(1, 1),
            },
            &mut rt_b,
        );
        assert!(b.has_message(&tid(1, 1)));
        rt_b.sent.clear();

        // B receives duplicate from C → sends PRUNE to C
        b.handle_gossip(
            GossipMsg {
                round: 1,
                sender: 30,
                payload: payload(1, 1),
            },
            &mut rt_b,
        );
        assert_eq!(rt_b.sent.len(), 1);
        let (to, m) = &rt_b.sent[0];
        assert_eq!(*to, 30);
        unwrap_prune(m); // verifies it's a Prune
        assert!(b.lazy_peers.contains(&30));
        rt_b.sent.clear();

        // Now C ticks and sends IHave for tid(1, 2) to B (lazy peer)
        // Simulated: B receives the IHave
        b.handle_ihave(
            IHaveMsg {
                sender: 30,
                digests: vec![IHaveDigest {
                    id: tid(1, 2),
                    round: 0,
                }],
            },
            &mut rt_b,
        );

        // Timer fires — B sends GRAFT to C
        b.timer_fired(
            Timer::IHaveTimeoutBatch {
                ids: vec![tid(1, 2)],
                retries: 0,
                senders: vec![30],
            },
            &mut rt_b,
        );
        assert_eq!(rt_b.sent.len(), 1);
        let (to, m) = &rt_b.sent[0];
        assert_eq!(*to, 30);
        unwrap_graft(m);
    }

    // --- Maintenance reconciliation (update_peer_topology) ---

    fn ring(r: Option<u8>) -> RttInfo {
        RttInfo { ring: r }
    }

    /// A state bootstrapped the way `plumtree_loop` does it: every member
    /// with its ring, then one reconcile run over the same snapshot.
    fn reconciled(
        members: &[(TestNodeId, Option<u8>)],
    ) -> (
        PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore>,
        AccumulatingRuntime,
    ) {
        let mut s = state();
        let mut rt = AccumulatingRuntime::default();
        s.add_peers_bulk_with_rtt(snapshot(members), &mut rt);
        s.update_peer_topology(snapshot(members), &mut rt);
        (s, rt)
    }

    fn snapshot(members: &[(TestNodeId, Option<u8>)]) -> Vec<(TestNodeId, RttInfo)> {
        members.iter().map(|(n, r)| (*n, ring(*r))).collect()
    }

    /// The membership snapshot that matches what the state already knows,
    /// so a reconcile run only flushes the deferred rebalance.
    fn known_snapshot(
        s: &PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore>,
    ) -> Vec<(TestNodeId, RttInfo)> {
        s.known_peers
            .iter()
            .map(|p| (*p, s.peer_topology.get(p).copied().unwrap_or_default()))
            .collect()
    }

    fn in_overlay(
        s: &PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore>,
        p: &TestNodeId,
    ) -> bool {
        s.eager_peers.contains(p) || s.lazy_peers.contains(p)
    }

    const THREE: [(TestNodeId, Option<u8>); 3] = [(1, Some(0)), (2, Some(0)), (3, Some(0))];

    #[test]
    fn reconcile_removes_departed_peer() {
        let (mut s, mut rt) = reconciled(&THREE);
        assert!(s.known_peers.contains(&3));

        // 3 left the member map and its MemberDown never reached plumtree.
        s.update_peer_topology(snapshot(&THREE[..2]), &mut rt);

        assert!(!s.known_peers.contains(&3));
        assert!(!in_overlay(&s, &3));
        assert!(!s.ring_locked.contains(&3));
        assert!(!s.pending_topology.contains_key(&3));
        // the RTT cache keeps the entry on purpose
        assert_eq!(s.peer_topology.get(&3), Some(&ring(Some(0))));
    }

    #[test]
    fn reconcile_relocks_ring_neighbors_after_departure() {
        // local id 0, sorted ring 0,1,2,3: neighbors are 1 and 3.
        let (mut s, mut rt) = reconciled(&THREE);
        assert_eq!(s.ring_locked, IndexSet::from([1, 3]));

        s.update_peer_topology(snapshot(&THREE[..2]), &mut rt);

        assert_eq!(s.ring_locked, IndexSet::from([1, 2]));
        assert!(s.eager_peers.contains(&2));
    }

    #[test]
    fn reconcile_readds_rejoined_peer_after_peer_down() {
        let (mut s, mut rt) = reconciled(&THREE);
        s.peer_down(&3, &mut rt);
        s.update_peer_topology(snapshot(&THREE[..2]), &mut rt);
        assert!(!s.known_peers.contains(&3));

        // 3 is back in the member map with the same ring; its MemberUp was lost.
        s.update_peer_topology(snapshot(&THREE), &mut rt);

        assert!(s.known_peers.contains(&3));
        assert!(in_overlay(&s, &3));
    }

    #[test]
    fn reconcile_readds_rejoined_peer_whose_ring_appears() {
        let (mut s, mut rt) = reconciled(&[(1, Some(0)), (2, Some(0)), (3, None)]);
        s.peer_down(&3, &mut rt);
        s.update_peer_topology(snapshot(&THREE[..2]), &mut rt);
        assert!(!s.known_peers.contains(&3));

        s.update_peer_topology(snapshot(&THREE), &mut rt);

        assert!(s.known_peers.contains(&3));
        assert_eq!(s.peer_topology.get(&3), Some(&ring(Some(0))));
    }

    #[test]
    fn reconcile_places_rejoined_peer_with_cached_ring() {
        // 3 was known in the Far bucket. It rejoins before any RTT sample
        // exists, so the snapshot carries ring None. The cached ring wins.
        let (mut s, mut rt) = reconciled(&[(1, Some(0)), (2, Some(0)), (3, Some(5))]);
        s.peer_down(&3, &mut rt);
        s.update_peer_topology(snapshot(&THREE[..2]), &mut rt);

        s.update_peer_topology(snapshot(&[(1, Some(0)), (2, Some(0)), (3, None)]), &mut rt);

        assert!(s.known_peers.contains(&3));
        assert_eq!(s.peer_topology.get(&3), Some(&ring(Some(5))));
        assert_eq!(s.peer_bucket(&3), RingBucket::Far);
    }

    #[test]
    fn peer_up_places_rejoined_peer_with_cached_ring() {
        let (mut s, mut rt) = reconciled(&[(1, Some(0)), (2, Some(0)), (3, Some(5))]);
        s.peer_down(&3, &mut rt);

        s.peer_up(3, None, &mut rt);

        assert!(s.known_peers.contains(&3));
        assert_eq!(s.peer_topology.get(&3), Some(&ring(Some(5))));
    }

    #[test]
    fn reconcile_keeps_bucket_confirmation_for_known_peers() {
        // A known peer that moves bucket still needs RING_EXTRA_CONFIRMATIONS runs.
        let (mut s, mut rt) = reconciled(&THREE);
        let moved = [(1, Some(0)), (2, Some(0)), (3, Some(5))];
        for _ in 1..RING_EXTRA_CONFIRMATIONS {
            s.update_peer_topology(snapshot(&moved), &mut rt);
            assert_eq!(s.peer_topology.get(&3), Some(&ring(Some(0))));
        }
        s.update_peer_topology(snapshot(&moved), &mut rt);
        assert_eq!(s.peer_topology.get(&3), Some(&ring(Some(5))));
        assert!(s.known_peers.contains(&3));
    }

    #[test]
    fn reconcile_ignores_local_id() {
        let mut s = state(); // local id 0
        let mut rt = AccumulatingRuntime::default();

        s.update_peer_topology(snapshot(&[(0, Some(0)), (1, Some(0))]), &mut rt);

        assert!(!s.known_peers.contains(&0));
        assert!(!s.peer_topology.contains_key(&0));
        assert!(s.known_peers.contains(&1));
    }

    #[test]
    fn reconcile_with_empty_snapshot_removes_every_peer() {
        let (mut s, mut rt) = reconciled(&THREE);

        s.update_peer_topology(iter::empty::<(TestNodeId, RttInfo)>(), &mut rt);

        assert!(s.known_peers.is_empty());
        assert!(s.eager_peers.is_empty());
        assert!(s.lazy_peers.is_empty());
        assert!(s.ring_locked.is_empty());
    }

    #[test]
    fn peer_down_recomputes_fanout_for_overlay_peers() {
        let mut cfg = test_config();
        cfg.num_eager = None;
        cfg.min_lazy = None;
        cfg.max_lazy = None;
        let mut s = PlumtreeState::new_with_store(0u8, cfg, TestSeenStore::default());
        let mut rt = AccumulatingRuntime::default();

        // 68 peers -> cluster 69 -> round(log10(69) * 3) = 6 eager.
        s.add_peers_bulk((1..=68u8).collect(), &mut rt);
        assert_eq!(s.num_eager(), 6);

        let overlay: Vec<u8> = s
            .eager_peers
            .iter()
            .chain(s.lazy_peers.iter())
            .copied()
            .collect();
        for p in overlay {
            s.peer_down(&p, &mut rt);
        }

        let expected = resolve_fanout(s.known_peers.len(), s.config()).num_eager;
        assert_eq!(s.num_eager(), expected);
        assert!(s.needs_rebalance);
    }

    // --- Peer selection strategies ---

    type TestState = PlumtreeState<TestMsgId, TestPayload, TestNodeId, TestSeenStore>;

    /// Rings 0..=5 by id, so the peers spread over Near, Mid and Far.
    fn ring_of(p: TestNodeId) -> RttInfo {
        ring(Some(p % 6))
    }

    /// A state with a derived fanout, bootstrapped with `peers`.
    fn selecting(
        selection: PeerSelection,
        peers: impl IntoIterator<Item = (TestNodeId, RttInfo)>,
    ) -> (TestState, AccumulatingRuntime) {
        selecting_seeded(selection, peers, 7)
    }

    fn selecting_seeded(
        selection: PeerSelection,
        peers: impl IntoIterator<Item = (TestNodeId, RttInfo)>,
        seed: u64,
    ) -> (TestState, AccumulatingRuntime) {
        let mut cfg = test_config();
        cfg.num_eager = None;
        cfg.min_lazy = None;
        cfg.max_lazy = None;
        cfg.peer_selection = selection;
        let mut s = PlumtreeState::new_with_store_seeded(0, cfg, TestSeenStore::default(), seed);
        let mut rt = AccumulatingRuntime::default();
        s.add_peers_bulk_with_rtt(peers.into_iter().collect(), &mut rt);
        (s, rt)
    }

    fn sets(s: &TestState) -> (IndexSet<TestNodeId>, IndexSet<TestNodeId>) {
        (s.eager_peers.clone(), s.lazy_peers.clone())
    }

    /// Eager and lazy entries changed since `before`.
    fn changed(s: &TestState, before: &(IndexSet<TestNodeId>, IndexSet<TestNodeId>)) -> usize {
        before.0.symmetric_difference(&s.eager_peers).count()
            + before.1.symmetric_difference(&s.lazy_peers).count()
    }

    /// 300 random joins and departures over ids 1..=150, starting from
    /// 1..=120 known. `check` runs after each one.
    fn random_churn(selection: PeerSelection, mut check: impl FnMut(&mut TestState, usize)) {
        let (mut s, mut rt) = selecting(selection, (1..=120).map(|p| (p, ring_of(p))));
        let mut events = SmallRng::seed_from_u64(1);
        for _ in 0..300 {
            let p = events.random_range(1..=150);
            let before = sets(&s);
            if s.known_peers.contains(&p) {
                s.peer_down(&p, &mut rt);
            } else {
                s.peer_up(p, Some(ring_of(p)), &mut rt);
            }
            let changed = changed(&s, &before);
            check(&mut s, changed);
        }
    }

    #[test]
    fn hrw_flap_restores_the_eager_set() {
        let (mut s, mut rt) = selecting(PeerSelection::Hrw, (1..=120).map(|p| (p, ring_of(p))));
        let (eager, lazy) = sets(&s);
        let mut started_lazy = IndexSet::new();
        // eager, lazy, untracked and ring-locked (1 and 120) peers alike
        for p in 1..=120 {
            if !in_overlay(&s, &p) {
                started_lazy.insert(p);
            }
            s.peer_down(&p, &mut rt);
            s.peer_up(p, Some(ring_of(p)), &mut rt);
            assert_eq!(s.eager_peers, eager, "flap of {p}");
            // an untracked peer comes back lazy while there is room
            assert!(lazy.is_subset(&s.lazy_peers), "flap of {p}");
            assert!(
                s.lazy_peers
                    .difference(&lazy)
                    .all(|q| started_lazy.contains(q))
            );
        }
        assert_eq!(s.lazy_peers.len(), s.max_lazy());
        s.update_peer_topology(known_snapshot(&s), &mut rt);
        assert_eq!(s.eager_peers, eager);
    }

    #[test]
    fn graft_learned_eager_peer_survives_unrelated_joins() {
        // Members have even ids and joins odd ones in between, so the ring
        // neighbors stay the same. Under Hrw the joins rank around the
        // grafted Near peers. IncrementalRandom demotes from the newcomer's
        // bucket first: Far joins that are not drawn eager leave again, so
        // the Far bucket stays small and many joins evict a Far peer.
        for (selection, join_ring) in [
            (PeerSelection::Hrw, 0),
            (PeerSelection::IncrementalRandom, 4),
        ] {
            let far = [40, 80, 120, 160];
            let members = (1..=100).map(|i| {
                let p = 2 * i;
                let info = if far.contains(&p) {
                    ring(Some(4))
                } else {
                    ring(Some(p % 4))
                };
                (p, info)
            });
            let (mut s, mut rt) = selecting(selection, members);
            let near = |p: &&u8| s.peer_bucket(p) == RingBucket::Near;
            let untracked = *s
                .known_peers
                .iter()
                .filter(near)
                .find(|p| !in_overlay(&s, p))
                .unwrap();
            let lazy = *s.lazy_peers.iter().rfind(near).unwrap();
            for p in [untracked, lazy] {
                s.handle_graft(graft_msg(p, false, vec![]), &mut rt);
            }

            let mut evictions = 0;
            for p in (101..=199).step_by(2) {
                let before = s.eager_peers.clone();
                s.peer_up(p, Some(ring(Some(join_ring))), &mut rt);
                evictions += before.difference(&s.eager_peers).count();
                if selection == PeerSelection::IncrementalRandom && !s.eager_peers.contains(&p) {
                    s.peer_down(&p, &mut rt);
                }
                for grafted in [untracked, lazy] {
                    assert!(
                        s.eager_peers.contains(&grafted),
                        "{selection:?}: join of {p} dropped {grafted}"
                    );
                }
            }
            s.update_peer_topology(known_snapshot(&s), &mut rt);
            assert!(s.eager_peers.contains(&untracked) && s.eager_peers.contains(&lazy));
            if selection == PeerSelection::Hrw {
                // the joins did push the lazy one out of its ranked slot
                assert!(!s.targets.lazy.contains(&lazy) && !s.targets.eager.contains(&lazy));
            } else {
                assert!(evictions >= 5, "{evictions} evictions");
            }
        }
    }

    #[test]
    fn membership_change_moves_few_entries() {
        for selection in [PeerSelection::Hrw, PeerSelection::IncrementalRandom] {
            let (mut total, mut max) = (0, 0);
            random_churn(selection, |_, changed| {
                total += changed;
                max = max.max(changed);
            });
            // Guards against changes that touch most of the sets: a full
            // rebuild changes up to 2 * (num_eager + min_lazy) = 30 entries
            // here. One change moves the peer itself, the slice boundaries it
            // shifts, a ring lock that changes bucket, and a fanout step that
            // moves every bucket's slice. Over 300 event seeds Hrw reaches 10
            // per change and 307 per 300 changes.
            assert!(max <= 12, "{selection:?}: {max} entries in one change");
            assert!(
                total <= 350,
                "{selection:?}: {total} entries in 300 changes"
            );
        }
    }

    #[test]
    fn ring_neighbors_stay_locked_and_eager() {
        for selection in [
            PeerSelection::FullRebalance,
            PeerSelection::Hrw,
            PeerSelection::IncrementalRandom,
        ] {
            random_churn(selection, |s, _| {
                if selection == PeerSelection::FullRebalance {
                    // relocks on the tick
                    let mut rt = AccumulatingRuntime::default();
                    s.update_peer_topology(known_snapshot(s), &mut rt);
                }
                // local id 0: the smallest and the largest known id
                let neighbors = IndexSet::from([
                    *s.known_peers.iter().min().unwrap(),
                    *s.known_peers.iter().max().unwrap(),
                ]);
                assert_eq!(s.ring_locked, neighbors, "{selection:?}");
                assert!(s.ring_locked.is_subset(&s.eager_peers), "{selection:?}");
            });
        }
    }

    #[test]
    fn new_ring_neighbor_is_eager_after_an_earlier_prune() {
        for selection in [PeerSelection::Hrw, PeerSelection::IncrementalRandom] {
            let (mut s, mut rt) = selecting(selection, (1..=120).map(|p| (p, ring_of(p))));
            let p = *s
                .eager_peers
                .iter()
                .filter(|p| !s.ring_locked.contains(*p))
                .max()
                .unwrap();
            s.handle_prune(
                PruneMsg {
                    sender: p,
                    triggered_by: None,
                },
                &mut rt,
            );
            assert!(s.lazy_peers.contains(&p));
            // p becomes the largest id, a ring neighbor of 0
            for q in (p + 1)..=120 {
                s.peer_down(&q, &mut rt);
            }
            assert!(s.ring_locked.contains(&p), "{selection:?}");
            assert!(s.eager_peers.contains(&p), "{selection:?}");
            s.update_peer_topology(known_snapshot(&s), &mut rt);
            assert!(s.ring_locked.is_subset(&s.eager_peers), "{selection:?}");
        }
    }

    /// Each node ranks with its own salt, so nodes with the same members pick
    /// different eager peers and no peer is eager everywhere.
    #[test]
    fn hrw_rankings_differ_between_nodes() {
        let mut picked: HashMap<TestNodeId, u32> = HashMap::new();
        for seed in 0..40 {
            let members = (1..=120).map(|p| (p, ring_of(p)));
            let (s, _) = selecting_seeded(PeerSelection::Hrw, members, seed);
            for p in s.eager_peers.difference(&s.ring_locked) {
                *picked.entry(*p).or_default() += 1;
            }
        }
        // 4 non-locked eager peers out of 118 per node: about 1.4 picks per
        // peer over 40 nodes; one shared ranking would give 40.
        let max = picked.values().max().unwrap();
        assert!(*max <= 10, "one peer is eager at {max} of 40 nodes");
    }

    #[test]
    fn ring_neighbor_stays_eager_when_its_bucket_changes() {
        for selection in [PeerSelection::Hrw, PeerSelection::IncrementalRandom] {
            let mut members: Vec<_> = (1..=120).map(|p| (p, ring_of(p))).collect();
            let (mut s, mut rt) = selecting(selection, members.clone());
            // 1 is a ring neighbor of 0 and moves from Near to Far
            assert!(s.ring_locked.contains(&1));
            let before = sets(&s);
            members[0].1 = ring(Some(5));
            for _ in 0..RING_EXTRA_CONFIRMATIONS {
                s.update_peer_topology(members.clone(), &mut rt);
            }
            assert_eq!(s.peer_bucket(&1), RingBucket::Far);
            assert!(s.ring_locked.contains(&1), "{selection:?}");
            assert!(s.eager_peers.contains(&1), "{selection:?}");
            if selection == PeerSelection::IncrementalRandom {
                // a locked peer is not dropped and drawn again
                assert_eq!(sets(&s), before);
            }
        }
    }

    #[test]
    fn hrw_sets_depend_only_on_membership() {
        let (mut s, mut rt) = selecting(PeerSelection::Hrw, (1..=120).map(|p| (p, ring_of(p))));
        for p in [3, 10, 64, 120] {
            s.peer_down(&p, &mut rt);
        }
        for p in [121, 122, 130] {
            s.peer_up(p, Some(ring_of(p)), &mut rt);
        }
        // 7 moves from Near to Far once the move is confirmed
        let mut members = known_snapshot(&s);
        for (p, info) in members.iter_mut() {
            if *p == 7 {
                *info = ring(Some(5));
            }
        }
        for _ in 0..RING_EXTRA_CONFIRMATIONS {
            s.update_peer_topology(members.clone(), &mut rt);
        }
        assert_eq!(s.peer_bucket(&7), RingBucket::Far);

        let (fresh, _) = selecting(PeerSelection::Hrw, members);
        assert_eq!(s.eager_peers, fresh.eager_peers);
        // plus the joins that started lazy
        assert!(fresh.lazy_peers.is_subset(&s.lazy_peers));
        assert!(
            s.lazy_peers
                .difference(&fresh.lazy_peers)
                .all(|p| [121, 122, 130].contains(p))
        );
    }

    #[test]
    fn incremental_random_bucket_change_is_a_leave_and_a_join() {
        let members: Vec<_> = (1..=120).map(|p| (p, ring_of(p))).collect();
        let (mut s, mut rt) = selecting(PeerSelection::IncrementalRandom, members.clone());
        // a non-locked Near peer from each set moves to Far
        let moved = [
            *s.eager_peers
                .iter()
                .find(|p| !s.ring_locked.contains(*p) && s.peer_bucket(p) == RingBucket::Near)
                .unwrap(),
            *s.lazy_peers
                .iter()
                .find(|p| s.peer_bucket(p) == RingBucket::Near)
                .unwrap(),
        ];
        let mut members = members;
        for (p, info) in members.iter_mut() {
            if moved.contains(p) {
                *info = ring(Some(5));
            }
        }
        let before = sets(&s);
        for _ in 0..RING_EXTRA_CONFIRMATIONS {
            s.update_peer_topology(members.clone(), &mut rt);
        }
        for p in moved {
            assert_eq!(s.peer_bucket(&p), RingBucket::Far);
        }
        // each move: the peer, a refill and an eviction at most
        assert!(
            changed(&s, &before) <= 2 * 5,
            "{} entries",
            changed(&s, &before)
        );
        assert!(s.eager_peers.len() <= s.num_eager());
        assert!(s.ring_locked.is_subset(&s.eager_peers));
    }

    /// Every event keeps the eager set at the derived fanout: an eager
    /// admission demotes one, an eager departure is refilled, and a fanout
    /// change adds or removes one. Known peers move across 30/31, where the
    /// fanout steps between 4 and 5.
    #[test]
    fn incremental_random_keeps_the_eager_set_at_the_fanout() {
        let (mut s, mut rt) = selecting(
            PeerSelection::IncrementalRandom,
            (1..=31).map(|p| (p, ring_of(p))),
        );
        let mut events = SmallRng::seed_from_u64(3);
        let mut fanout_changes = 0;
        for _ in 0..400 {
            let p = events.random_range(1..=40);
            let num_eager = s.num_eager();
            if s.known_peers.contains(&p) {
                s.peer_down(&p, &mut rt);
            } else {
                s.peer_up(p, Some(ring_of(p)), &mut rt);
            }
            fanout_changes += (s.num_eager() != num_eager) as u32;
            let derived = resolve_fanout(s.known_peers.len(), s.config()).num_eager;
            assert_eq!(s.num_eager(), derived);
            assert_eq!(s.eager_peers.len(), s.num_eager(), "after {p}");
        }
        assert!(fanout_changes >= 4, "{fanout_changes} fanout changes");
    }

    /// The draw admits a joining peer as eager with its bucket's eager share
    /// over its known peers: 0.075 for Near and 0.025 for Far with 120
    /// peers over three buckets, 0.05 with one bucket.
    #[test]
    fn incremental_random_admission_odds_per_bucket() {
        let three = ring_of as fn(TestNodeId) -> RttInfo;
        let one = (|_| ring(Some(0))) as fn(TestNodeId) -> RttInfo;
        for (rings, joiner, expected) in [(three, 60, 0.075), (three, 64, 0.025), (one, 60, 0.05)] {
            let (mut s, mut rt) = selecting(
                PeerSelection::IncrementalRandom,
                (1..=120).map(|p| (p, rings(p))),
            );
            let trials = 4000;
            let mut eager = 0;
            for _ in 0..trials {
                s.peer_down(&joiner, &mut rt);
                s.peer_up(joiner, Some(rings(joiner)), &mut rt);
                eager += s.eager_peers.contains(&joiner) as u32;
            }
            let observed = eager as f64 / trials as f64;
            let sigma = (expected * (1.0 - expected) / trials as f64).sqrt();
            assert!(
                (observed - expected).abs() < 4.0 * sigma,
                "joiner {joiner}: {observed:.4} eager, expected {expected}"
            );
        }
    }

    /// A full eager set demotes a peer from the newcomer's bucket, an eager
    /// departure is refilled from the departed peer's bucket, and a joining
    /// peer that is not drawn eager starts lazy.
    #[test]
    fn incremental_random_evicts_and_refills_within_the_bucket() {
        // 60 peers over three buckets; ring neighbors 1 and 60 are Near, so
        // one non-locked Near peer is eager next to two Mid ones.
        let (mut s, mut rt) = selecting(
            PeerSelection::IncrementalRandom,
            (1..=60).map(|p| (p, ring_of(p))),
        );
        let num_eager = s.num_eager();
        assert_eq!(s.eager_peers.len(), num_eager);
        let (mut evictions, mut refills) = (0, 0);
        for round in 0..20 {
            for p in 2..60 {
                if s.ring_locked.contains(&p) || s.peer_bucket(&p) != RingBucket::Near {
                    continue;
                }
                let bucket = s.peer_bucket(&p);
                let was_eager = s.eager_peers.contains(&p);
                let before = s.eager_peers.clone();
                s.peer_down(&p, &mut rt);
                assert_eq!(s.eager_peers.len(), num_eager, "round {round}, down {p}");
                for q in s.eager_peers.difference(&before) {
                    assert!(was_eager);
                    assert_eq!(s.peer_bucket(q), bucket, "refilled {p} with {q}");
                    refills += 1;
                }

                let before = s.eager_peers.clone();
                let lazy_full = s.lazy_peers.len() >= s.max_lazy();
                s.peer_up(p, Some(ring_of(p)), &mut rt);
                assert_eq!(s.eager_peers.len(), num_eager, "round {round}, up {p}");
                // not drawn eager: lazy while there is room
                assert!(s.eager_peers.contains(&p) || lazy_full || s.lazy_peers.contains(&p));
                for q in before.difference(&s.eager_peers) {
                    assert_eq!(s.peer_bucket(q), bucket, "{p} demoted {q}");
                    evictions += 1;
                }
            }
        }
        assert!(
            evictions >= 10 && refills >= 5,
            "{evictions} evictions, {refills} refills"
        );
    }

    #[test]
    fn incremental_random_refills_lazy() {
        let (mut s, mut rt) = selecting(
            PeerSelection::IncrementalRandom,
            (1..=120).map(|p| (p, ring_of(p))),
        );
        // a lazy departure is refilled from its bucket
        let left = *s.lazy_peers.first().unwrap();
        let before = s.lazy_peers.clone();
        s.peer_down(&left, &mut rt);
        assert_eq!(s.lazy_peers.len(), s.min_lazy());
        for p in s.lazy_peers.difference(&before) {
            assert_eq!(s.peer_bucket(p), s.peer_bucket(&left));
        }

        // the tick tops lazy up after GRAFTs took some
        let grafted: Vec<_> = s.lazy_peers.iter().take(3).copied().collect();
        for p in grafted {
            s.handle_graft(graft_msg(p, false, vec![]), &mut rt);
        }
        assert_eq!(s.lazy_peers.len(), s.min_lazy() - 3);
        s.update_peer_topology(known_snapshot(&s), &mut rt);
        assert_eq!(s.lazy_peers.len(), s.min_lazy());
    }

    #[test]
    fn top_up_ends_when_min_lazy_exceeds_max_lazy() {
        let mut cfg = test_config();
        cfg.peer_selection = PeerSelection::IncrementalRandom;
        cfg.min_lazy = Some(20);
        cfg.max_lazy = Some(15);
        let mut s = PlumtreeState::new_with_store_seeded(0, cfg, TestSeenStore::default(), 7);
        let mut rt = AccumulatingRuntime::default();
        s.add_peers_bulk_with_rtt((1..=120).map(|p| (p, ring_of(p))).collect(), &mut rt);
        let grafted: Vec<_> = s.lazy_peers.iter().take(8).copied().collect();
        for p in grafted {
            s.handle_graft(graft_msg(p, false, vec![]), &mut rt);
        }
        assert_eq!(s.lazy_peers.len(), 12);
        s.update_peer_topology(known_snapshot(&s), &mut rt);
        assert_eq!(s.lazy_peers.len(), 15);
    }
}
