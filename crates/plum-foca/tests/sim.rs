//! Deterministic discrete-event simulation of the Plumtree protocol at
//! cluster scale.
//!
//! Instead of spawning tasks and channels, every node is a bare
//! [`PlumtreeState`] and all effects (sends, timers, ticks) are events in a
//! single priority queue ordered by virtual time. Message latency is derived
//! from a geographic model: one (lat, lon) per region,
//! RTT ≈ great-circle-km / 75 (speed of light in fiber + routing overhead),
//! validated against measured region-to-region RTTs. Number of nodes in a re
//! region is weighted.
//!
//! ```text
//! cargo test --release -p plum-foca --test sim -- --ignored --nocapture
//! ```

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

/// Fixed epoch so the sim can map virtual µs to a monotonic `Instant` for
/// `Runtime::now()` — keeps time-windowed protocol logic deterministic.
static SIM_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

use indexmap::IndexMap;
use plum_foca::{
    Config, EagerRatios, Notification, Payload, PlumtreeMsg, PlumtreeState, Round, RttInfo,
    Runtime, SeenStore, Timer,
};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

type NId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MId(u64);

#[derive(Debug, Clone)]
struct SimPayload {
    origin: NId,
    seq: u32,
}

impl SimPayload {
    fn id(&self) -> MId {
        MId(((self.origin as u64) << 32) | self.seq as u64)
    }
}

/// Decode the origin node encoded in a `SimPayload::id()`/gossip `mid`
/// (both sims use the same `(origin << 32) | seq` encoding).
fn origin_of(mid: MId) -> NId {
    (mid.0 >> 32) as NId
}

impl plum_foca::MessageId for MId {
    type NodeId = NId;
    fn origin(&self) -> NId {
        origin_of(*self)
    }
}

impl Payload for SimPayload {
    type MessageId = MId;
    type NodeId = NId;

    fn message_id(&self) -> MId {
        self.id()
    }

    fn origin(&self) -> NId {
        self.origin
    }
}

struct SimSeenStore {
    entries: IndexMap<MId, u32>,
    cap: usize,
}

impl SimSeenStore {
    fn new(cap: usize) -> Self {
        Self {
            entries: IndexMap::with_capacity(cap.min(64 * 1024)),
            cap,
        }
    }
}

impl SeenStore<MId> for SimSeenStore {
    fn evict_if_needed(&mut self) {
        if self.entries.len() > self.cap {
            let excess = self.entries.len() - self.cap;
            self.entries.drain(0..excess);
        }
    }

    fn contains(&self, id: &MId) -> bool {
        self.entries.contains_key(id)
    }

    fn observe(&mut self, id: MId, _round: Round) -> Option<u32> {
        if let Some(count) = self.entries.get_mut(&id) {
            *count += 1;
            return Some(*count);
        }
        self.entries.insert(id, 1);
        None
    }

    fn size(&self) -> usize {
        self.entries.len()
    }
}

struct Region {
    #[allow(dead_code)]
    name: &'static str,
    lat: f64,
    lon: f64,
    weight: u32,
}

#[rustfmt::skip]
const REGIONS: &[Region] = &[
    Region { name: "ams", lat: 52.31, lon: 4.76, weight: 274 },
    Region { name: "arn", lat: 59.65, lon: 17.92, weight: 20 },
    Region { name: "bom", lat: 19.09, lon: 72.87, weight: 33 },
    Region { name: "cdg", lat: 49.01, lon: 2.55, weight: 175 },
    Region { name: "dfw", lat: 32.90, lon: -97.04, weight: 528 },
    Region { name: "ewr", lat: 40.69, lon: -74.17, weight: 245 },
    Region { name: "fra", lat: 50.03, lon: 8.57, weight: 347 },
    Region { name: "gru", lat: -23.43, lon: -46.47, weight: 111 },
    Region { name: "iad", lat: 38.94, lon: -77.46, weight: 442 },
    Region { name: "jnb", lat: -26.14, lon: 28.25, weight: 16 },
    Region { name: "lax", lat: 33.94, lon: -118.41, weight: 306 },
    Region { name: "lhr", lat: 51.47, lon: -0.45, weight: 74 },
    Region { name: "nrt", lat: 35.77, lon: 140.39, weight: 76 },
    Region { name: "ord", lat: 41.97, lon: -87.91, weight: 559 },
    Region { name: "qmx", lat: 19.43, lon: -99.13, weight: 42 },
    Region { name: "sin", lat: 1.36, lon: 103.99, weight: 209 },
    Region { name: "sjc", lat: 37.36, lon: -121.93, weight: 682 },
    Region { name: "syd", lat: -33.95, lon: 151.18, weight: 200 },
    Region { name: "yyz", lat: 43.68, lon: -79.63, weight: 114 },
];

fn haversine_km(a: &Region, b: &Region) -> f64 {
    let (la1, la2) = (a.lat.to_radians(), b.lat.to_radians());
    let dlat = (b.lat - a.lat).to_radians();
    let dlon = (b.lon - a.lon).to_radians();
    let h = (dlat / 2.0).sin().powi(2) + la1.cos() * la2.cos() * (dlon / 2.0).sin().powi(2);
    2.0 * 6371.0 * h.sqrt().asin()
}

/// RTT between two regions in ms: 4ms routing floor + distance at ~2/3 c.
/// Matches measured Fly region-to-region RTTs within ~15%.
fn region_rtt_ms(a: usize, b: usize) -> u64 {
    if a == b {
        return 1;
    }
    (4.0 + haversine_km(&REGIONS[a], &REGIONS[b]) / 75.0) as u64
}

/// Mirrors `corro_types::members::RING_BUCKETS`.
const RING_BUCKETS: [std::ops::Range<u64>; 6] = [0..6, 6..15, 15..50, 50..100, 100..200, 200..300];

fn ring_from_rtt_ms(rtt_ms: u64) -> Option<u8> {
    RING_BUCKETS
        .iter()
        .position(|range| range.contains(&rtt_ms))
        .map(|ring| ring as u8)
}

#[derive(Default)]
struct NotificationStats {
    duplicates: u64,
    missing: u64,
    not_cached: u64,
    promotions: u64,
    demotions: u64,
    prune_suppressed: u64,
}

#[derive(Default)]
struct SimRuntime {
    // Collects outgoing protocol messages
    // (DstNodeId, PlumtreeMsg<MessageId, .., SrcNodeId>)
    outbox: Vec<(NId, PlumtreeMsg<MId, SimPayload, NId>)>,
    timers: Vec<(Timer<MId, NId>, Duration)>,
    // Payloads the protocol delivered this step.
    inbox: Vec<SimPayload>,
    notification_stats: NotificationStats,
    /// current virtual time in µs, kept in sync with `Sim::now`
    now_us: u64,
}

impl Runtime<MId, SimPayload, NId> for SimRuntime {
    fn send(&mut self, to: NId, msg: PlumtreeMsg<MId, SimPayload, NId>) {
        self.outbox.push((to, msg));
    }

    fn send_all(&mut self, peers: Vec<NId>, msg: PlumtreeMsg<MId, SimPayload, NId>) {
        for peer in peers {
            self.outbox.push((peer, msg.clone()));
        }
    }

    fn deliver(&mut self, payload: SimPayload) {
        self.inbox.push(payload);
    }

    fn schedule(&mut self, timer: Timer<MId, NId>, after: Duration) {
        self.timers.push((timer, after));
    }

    fn notify(&mut self, notification: Notification<'_, MId, NId>) {
        match notification {
            Notification::DuplicateMessage(_) => self.notification_stats.duplicates += 1,
            Notification::MessageMissing(count) => self.notification_stats.missing += count as u64,
            Notification::PayloadNotCached(_) => self.notification_stats.not_cached += 1,
            Notification::PeerMovedToEager(_) => self.notification_stats.promotions += 1,
            Notification::PeerDroppedFromEager(_) => self.notification_stats.demotions += 1,
            Notification::PeerMovedToLazy(_) | Notification::PeerEvictedFromLazy(_) => {}
            Notification::PruneSuppressed(_) => self.notification_stats.prune_suppressed += 1,
            Notification::Rebalance => {}
        }
    }

    fn now(&self) -> Instant {
        *SIM_EPOCH + Duration::from_micros(self.now_us)
    }
}

enum Event {
    Recv {
        to: NId,
        msg: PlumtreeMsg<MId, SimPayload, NId>,
    },
    TimerFired {
        node: NId,
        timer: Timer<MId, NId>,
    },
    /// Flush this node's lazy IHave queue (`PlumtreeState::tick`).
    Tick {
        node: NId,
    },
    Originate {
        node: NId,
        seq: u32,
    },
    /// Node stops responding. `restart_after: None` is a permanent crash;
    /// `Some(d)` means it rejoins after `d` with a fresh `PlumtreeState`,
    /// like a real process restart.
    NodeDown {
        node: NId,
        restart_after: Option<Duration>,
    },
    /// A previously-down node rejoins (only reachable via `NodeDown {
    /// restart_after: Some(_) }`).
    NodeUp {
        node: NId,
    },
    /// `observer`'s foca instance finally notices `subject` went down, after
    /// a random per-observer detection delay.
    MemberDown {
        observer: NId,
        subject: NId,
    },
    /// `observer`'s foca instance finally notices `subject` came back.
    MemberUp {
        observer: NId,
        subject: NId,
    },
}

struct Scheduled<E> {
    at: u64,
    seq: u64,
    ev: E,
}

impl<E> PartialEq for Scheduled<E> {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl<E> Eq for Scheduled<E> {}
impl<E> PartialOrd for Scheduled<E> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<E> Ord for Scheduled<E> {
    // reversed so the BinaryHeap pops the earliest event first; seq breaks
    // ties deterministically.
    fn cmp(&self, other: &Self) -> Ordering {
        (other.at, other.seq).cmp(&(self.at, self.seq))
    }
}

// ---------------------------------------------------------------------------
// Simulation
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Params {
    n: usize,
    num_eager: usize,
    msgs_per_node: u32,
    /// Window over which broadcasts are staggered (uniformly at random).
    broadcast_window: Duration,
    /// Give nodes real RTT ring info; `false` leaves every peer in the Far
    /// bucket (ring: None), i.e. topology-blind eager selection.
    rtt_aware: bool,
    /// Probability that any protocol message is dropped in flight.
    loss: f64,
    /// PRUNE throttle window; `None` = prune every over-threshold duplicate.
    prune_throttle: Option<Duration>,
    /// Extra eager peers force-added per node after bootstrap, modelling an
    /// over-full eager set (e.g. admission without trimming). 0 = none.
    eager_inflate: usize,
    seed: u64,
    eager_ratios: EagerRatios,
    /// Number of randomly chosen nodes that crash permanently partway
    /// through the run (never send or receive again).
    crash_count: usize,
    /// Number of randomly chosen nodes that go down and later rejoin with a
    /// fresh `PlumtreeState`, modeling a real process restart.
    flap_count: usize,
    /// How long a flapping node stays down before rejoining.
    flap_downtime: Duration,
    /// Range for the random per-observer delay before a `peer_down`/`peer_up`
    /// is delivered, modeling staggered SWIM failure detection across the
    /// cluster (real nodes don't all notice a change at the same instant).
    detection_delay_min: Duration,
    detection_delay_max: Duration,
}

impl Params {
    fn new(n: usize, num_eager: usize) -> Self {
        Self {
            n,
            num_eager,
            msgs_per_node: 1,
            // ~200 broadcasts/s cluster-wide regardless of N
            broadcast_window: Duration::from_millis(n as u64 * 5),
            rtt_aware: true,
            loss: 0.0,
            prune_throttle: Some(SIM_PRUNE_THROTTLE),
            eager_inflate: 0,
            seed: 42,
            eager_ratios: EagerRatios::default(),
            crash_count: 0,
            flap_count: 0,
            flap_downtime: Duration::from_secs(5),
            detection_delay_min: Duration::from_secs(2),
            detection_delay_max: Duration::from_secs(8),
        }
    }

    fn with_eager_ratios(mut self, eager_ratios: EagerRatios) -> Self {
        self.eager_ratios = eager_ratios;
        self
    }

    /// `crash_count` nodes crash permanently, `flap_count` nodes go down and
    /// rejoin after `flap_downtime`; all are chosen randomly and scheduled
    /// uniformly over `broadcast_window`, overlapping the broadcast storm.
    fn with_failures(
        mut self,
        crash_count: usize,
        flap_count: usize,
        flap_downtime: Duration,
    ) -> Self {
        self.crash_count = crash_count;
        self.flap_count = flap_count;
        self.flap_downtime = flap_downtime;
        self
    }

    fn with_detection_delay(mut self, min: Duration, max: Duration) -> Self {
        self.detection_delay_min = min;
        self.detection_delay_max = max;
        self
    }

    /// Production config from `spawn_plumtree_loop`, with `num_eager`
    /// parametrized and the seen-store sized so it never evicts mid-run.
    fn config(&self) -> Config {
        Config {
            ihave_timeout: Duration::from_millis(200),
            optimization_threshold: Some(5),
            num_eager: None,
            min_lazy: None,
            max_lazy: None,
            prune_threshold: 5,
            max_received_entries: (self.n as u32 * self.msgs_per_node) as usize + 1,
            max_cached_payloads: 4096,
            prune_throttle: self.prune_throttle,
            eager_ratios: self.eager_ratios,
        }
    }
}

const TICK_INTERVAL: Duration = Duration::from_millis(200);
/// Default prune throttle in sim: shorter than production (1s) but still
/// spans a few tick intervals at sim event density.
const SIM_PRUNE_THROTTLE: Duration = Duration::from_millis(500);
/// A gossip send crossing a link slower than this counts as long-haul.
const LONGHAUL_RTT_MS: u64 = 150;

struct MsgStat {
    sent_at: u64,
    gossip_sends: u64,
    deliveries: u32,
    last_delivery_at: u64,
    /// Nodes that have received this message (Plumtree sim only; used to
    /// check survivor delivery under node failures). Always empty in the
    /// gossip baseline sim.
    delivered_to: HashSet<NId>,
}

#[derive(Default)]
struct Stats {
    sent_gossip: u64,
    sent_ihave: u64,
    sent_graft: u64,
    sent_prune: u64,
    longhaul_gossip: u64,
    dropped: u64,
    duplicates: u64,
    missing: u64,
    not_cached: u64,
    promotions: u64,
    demotions: u64,
    prune_suppressed: u64,
    /// One entry per delivery: number of gossip hops from the origin.
    hops: Vec<u32>,
    /// One entry per delivery: broadcast-to-delivery latency in µs.
    latencies_us: Vec<u64>,
}

struct Sim {
    params: Params,
    now: u64, // virtual time, µs
    next_seq: u64,
    queue: BinaryHeap<Scheduled<Event>>,
    states: Vec<PlumtreeState<MId, SimPayload, NId, SimSeenStore>>,
    region_of: Vec<u8>,
    rtt_ms: Vec<Vec<u64>>,
    rng: StdRng,
    runtime: SimRuntime,
    tick_pending: Vec<bool>,
    /// Round of the gossip message currently being handled, for hop
    /// attribution of the deliveries it produces.
    gossip_round: Option<Round>,
    msgs: HashMap<MId, MsgStat>,
    stats: Stats,
    /// avg eager-set size right after bootstrap+inflation, before the run.
    start_avg_eager: f64,
    /// Nodes currently unreachable (crashed or mid-flap).
    down: Vec<bool>,
    /// Nodes that crashed permanently (subset of nodes still `down` once the
    /// queue fully drains).
    crashed: HashSet<NId>,
    /// Nodes that went down and rejoined at least once.
    flapped: HashSet<NId>,
    /// Ground truth, per recipient, of every message it could never have
    /// received because it was down — recorded directly at the moment it's
    /// true, not inferred from timing: either the recipient was already
    /// down when the message originated, or a real delivery attempt reached
    /// it while down and got dropped. See `failure_miss_stats`.
    missed_during_downtime: HashMap<NId, HashSet<MId>>,
}

impl Sim {
    fn new(params: Params) -> Self {
        let mut rng = StdRng::seed_from_u64(params.seed);
        let n = params.n;

        let nregions = REGIONS.len();
        let rtt_ms: Vec<Vec<u64>> = (0..nregions)
            .map(|a| (0..nregions).map(|b| region_rtt_ms(a, b)).collect())
            .collect();

        // Assign nodes to regions, weighted by real per-region populations.
        let total_weight: u32 = REGIONS.iter().map(|r| r.weight).sum();
        let region_of: Vec<u8> = (0..n)
            .map(|_| {
                let mut pick = rng.random_range(0..total_weight);
                for (i, r) in REGIONS.iter().enumerate() {
                    if pick < r.weight {
                        return i as u8;
                    }
                    pick -= r.weight;
                }
                unreachable!()
            })
            .collect();

        let config = params.config();
        let mut states: Vec<_> = (0..n)
            .map(|i| {
                PlumtreeState::new_with_store_seeded(
                    i as NId,
                    config.clone(),
                    SimSeenStore::new(config.max_received_entries),
                    params.seed.wrapping_add(i as u64),
                )
            })
            .collect();

        // Full-membership bootstrap: every node knows every other node, the
        // way corrosion clusters work today.
        let mut rt = SimRuntime::default();
        for i in 0..n {
            let peers: Vec<(NId, RttInfo)> = (0..n)
                .filter(|&j| j != i)
                .map(|j| {
                    let rtt = rtt_ms[region_of[i] as usize][region_of[j] as usize];
                    let info = if params.rtt_aware {
                        RttInfo {
                            ring: ring_from_rtt_ms(rtt),
                        }
                    } else {
                        RttInfo::default()
                    };
                    (j as NId, info)
                })
                .collect();
            states[i].add_peers_bulk_with_rtt(peers, &mut rt);
        }
        // bootstrap produces no traffic
        assert!(rt.outbox.is_empty() && rt.timers.is_empty());

        let mut sim = Self {
            now: 0,
            next_seq: 0,
            queue: BinaryHeap::new(),
            states,
            region_of,
            rtt_ms,
            rng,
            runtime: rt,
            tick_pending: vec![false; n],
            gossip_round: None,
            msgs: HashMap::new(),
            stats: Stats::default(),
            params,
            start_avg_eager: 0.0,
            down: vec![false; n],
            crashed: HashSet::new(),
            flapped: HashSet::new(),
            missed_during_downtime: HashMap::new(),
        };

        // Optionally over-fill each node's eager set (model admission without
        // trimming); prune should converge it back over the run.
        if sim.params.eager_inflate > 0 {
            for i in 0..n {
                let mut added = 0;
                let mut attempts = 0;
                while added < sim.params.eager_inflate && attempts < sim.params.eager_inflate * 50 {
                    attempts += 1;
                    let p = sim.rng.random_range(0..n as NId);
                    if p as usize == i || sim.states[i].eager_peers().contains(&p) {
                        continue;
                    }
                    sim.states[i].force_eager(p);
                    added += 1;
                }
            }
        }
        sim.start_avg_eager = sim
            .states
            .iter()
            .map(|s| s.eager_peers().len())
            .sum::<usize>() as f64
            / n as f64;

        // Every node broadcasts, staggered over the window.
        let window_us = sim.params.broadcast_window.as_micros() as u64;
        for node in 0..n as NId {
            for seq in 0..sim.params.msgs_per_node {
                let at = sim.rng.random_range(0..window_us.max(1));
                sim.push(at, Event::Originate { node, seq });
            }
        }

        // Inject node failures (crashes and down/up flaps), scheduled
        // uniformly over the same window so churn overlaps the broadcast
        // storm, the way a real failure would.
        let fail_count = sim.params.crash_count + sim.params.flap_count;
        debug_assert!(
            fail_count <= n,
            "not enough nodes for the requested failure counts"
        );
        if fail_count > 0 {
            let mut candidates: Vec<NId> = (0..n as NId).collect();
            candidates.shuffle(&mut sim.rng);
            for &node in &candidates[..sim.params.crash_count] {
                let at = sim.rng.random_range(0..window_us.max(1));
                sim.push(
                    at,
                    Event::NodeDown {
                        node,
                        restart_after: None,
                    },
                );
            }
            for &node in &candidates[sim.params.crash_count..fail_count] {
                let at = sim.rng.random_range(0..window_us.max(1));
                sim.push(
                    at,
                    Event::NodeDown {
                        node,
                        restart_after: Some(sim.params.flap_downtime),
                    },
                );
            }
        }

        sim
    }

    fn push(&mut self, at: u64, ev: Event) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push(Scheduled { at, seq, ev });
    }

    /// One-way flight time in µs between two nodes, with jitter.
    fn flight_us(&mut self, from: NId, to: NId) -> u64 {
        let rtt = self.link_rtt_ms(from, to);
        let jitter = self.rng.random_range(1.0..1.15);
        ((rtt * 1000) as f64 / 2.0 * jitter) as u64
    }

    fn link_rtt_ms(&self, from: NId, to: NId) -> u64 {
        self.rtt_ms[self.region_of[from as usize] as usize][self.region_of[to as usize] as usize]
    }

    fn rtt_info(&self, from: NId, to: NId) -> RttInfo {
        if self.params.rtt_aware {
            RttInfo {
                ring: ring_from_rtt_ms(self.link_rtt_ms(from, to)),
            }
        } else {
            RttInfo::default()
        }
    }

    /// Random per-observer delay before a `peer_down`/`peer_up` is
    /// delivered, modeling staggered SWIM failure detection.
    fn random_detection_delay(&mut self) -> u64 {
        let min = self.params.detection_delay_min.as_micros() as u64;
        let max = self.params.detection_delay_max.as_micros() as u64;
        if max <= min {
            return min;
        }
        self.rng.random_range(min..max)
    }

    /// Run until the event queue drains (the protocol quiesces).
    fn run(mut self) -> Report {
        self.drain_queue_to_quiescence();
        self.check_invariants();
        self.report()
    }

    /// Run the event queue to quiescence. Split out from `run` so failure
    /// tests can inspect `Sim`'s private state afterward instead of only
    /// the aggregate `Report`.
    fn drain_queue_to_quiescence(&mut self) {
        while let Some(Scheduled { at, ev, .. }) = self.queue.pop() {
            debug_assert!(at >= self.now);
            self.now = at;
            self.runtime.now_us = at;
            match ev {
                Event::Originate { node, seq } => {
                    if self.down[node as usize] {
                        continue;
                    }
                    let payload = SimPayload { origin: node, seq };
                    let id = payload.id();
                    self.msgs.insert(
                        id,
                        MsgStat {
                            sent_at: self.now,
                            gossip_sends: 0,
                            deliveries: 0,
                            last_delivery_at: 0,
                            delivered_to: HashSet::new(),
                        },
                    );
                    // Ground truth: any node already down when this message
                    // is born could never receive it, full stop.
                    for r in 0..self.params.n as NId {
                        if r != node && self.down[r as usize] {
                            self.missed_during_downtime.entry(r).or_default().insert(id);
                        }
                    }
                    self.states[node as usize].broadcast(id, payload, &mut self.runtime);
                    self.drain(node);
                }
                Event::Recv { to, msg } => {
                    if self.down[to as usize] {
                        self.stats.dropped += 1;
                        // Ground truth: this specific broadcast really did
                        // try to reach `to` and got dropped because it was
                        // down — only `Gossip` carries a broadcast payload;
                        // IHave/Graft/Prune are protocol control traffic.
                        if let PlumtreeMsg::Gossip(g) = &msg {
                            self.missed_during_downtime
                                .entry(to)
                                .or_default()
                                .insert(g.payload.id());
                        }
                        continue;
                    }
                    let state = &mut self.states[to as usize];
                    match msg {
                        PlumtreeMsg::Gossip(g) => {
                            self.gossip_round = Some(g.round);
                            state.handle_gossip(g, &mut self.runtime);
                        }
                        PlumtreeMsg::IHave(ih) => state.handle_ihave(ih, &mut self.runtime),
                        PlumtreeMsg::Graft(g) => state.handle_graft(g, &mut self.runtime),
                        PlumtreeMsg::Prune(p) => state.handle_prune(p, &mut self.runtime),
                    }
                    self.drain(to);
                    self.gossip_round = None;
                }
                Event::TimerFired { node, timer } => {
                    if self.down[node as usize] {
                        continue;
                    }
                    self.states[node as usize].timer_fired(timer, &mut self.runtime);
                    self.drain(node);
                }
                Event::Tick { node } => {
                    self.tick_pending[node as usize] = false;
                    if self.down[node as usize] {
                        continue;
                    }
                    let state = &mut self.states[node as usize];
                    state.cache_evict_if_needed(&mut self.runtime);
                    state.tick(&mut self.runtime);
                    self.drain(node);
                }
                Event::NodeDown {
                    node,
                    restart_after,
                } => {
                    self.down[node as usize] = true;
                    if restart_after.is_none() {
                        self.crashed.insert(node);
                    }
                    let n = self.params.n as NId;
                    for observer in 0..n {
                        if observer == node || self.down[observer as usize] {
                            continue;
                        }
                        let delay = self.random_detection_delay();
                        let at = self.now + delay;
                        self.push(
                            at,
                            Event::MemberDown {
                                observer,
                                subject: node,
                            },
                        );
                    }
                    if let Some(downtime) = restart_after {
                        let at = self.now + downtime.as_micros() as u64;
                        self.push(at, Event::NodeUp { node });
                    }
                }
                Event::NodeUp { node } => {
                    self.down[node as usize] = false;
                    self.flapped.insert(node);
                    let config = self.params.config();
                    let seed = self.rng.random();
                    self.states[node as usize] = PlumtreeState::new_with_store_seeded(
                        node,
                        config.clone(),
                        SimSeenStore::new(config.max_received_entries),
                        seed,
                    );
                    self.tick_pending[node as usize] = false;

                    // Instant rejoin bootstrap: the restarted node learns the
                    // current alive membership immediately (like joining
                    // fresh off the gossip seed addrs).
                    let n: u32 = self.params.n as NId;
                    let peers: Vec<(NId, RttInfo)> = (0..n)
                        .filter(|&j| j != node && !self.down[j as usize])
                        .map(|j| (j, self.rtt_info(node, j)))
                        .collect();
                    self.states[node as usize].add_peers_bulk_with_rtt(peers, &mut self.runtime);
                    self.drain(node);

                    // Every other alive node's foca instance notices the
                    // rejoin after its own random detection delay.
                    for observer in 0..n {
                        if observer == node || self.down[observer as usize] {
                            continue;
                        }
                        let delay = self.random_detection_delay();
                        let at = self.now + delay;
                        self.push(
                            at,
                            Event::MemberUp {
                                observer,
                                subject: node,
                            },
                        );
                    }
                }
                Event::MemberDown { observer, subject } => {
                    if !self.down[observer as usize] {
                        self.states[observer as usize].peer_down(&subject, &mut self.runtime);
                        self.drain(observer);
                    }
                }
                Event::MemberUp { observer, subject } => {
                    if !self.down[observer as usize] && !self.down[subject as usize] {
                        let info = self.rtt_info(observer, subject);
                        self.states[observer as usize].peer_up(
                            subject,
                            Some(info),
                            &mut self.runtime,
                        );
                        self.drain(observer);
                    }
                }
            }
        }
    }

    /// Apply everything the node just asked the runtime to do.
    fn drain(&mut self, from: NId) {
        for payload in std::mem::take(&mut self.runtime.inbox) {
            let stat = self
                .msgs
                .get_mut(&payload.id())
                .expect("delivered unknown msg");
            stat.deliveries += 1;
            stat.last_delivery_at = self.now;
            stat.delivered_to.insert(from);
            self.stats.latencies_us.push(self.now - stat.sent_at);
            // round 0 = received directly from the origin = 1 hop
            let round = self.gossip_round.expect("delivery outside handle_gossip");
            self.stats.hops.push(round + 1);
        }

        let notification_stats = std::mem::take(&mut self.runtime.notification_stats);
        self.stats.duplicates += notification_stats.duplicates;
        self.stats.missing += notification_stats.missing;
        self.stats.not_cached += notification_stats.not_cached;
        self.stats.promotions += notification_stats.promotions;
        self.stats.demotions += notification_stats.demotions;
        self.stats.prune_suppressed += notification_stats.prune_suppressed;

        for (timer, after) in std::mem::take(&mut self.runtime.timers) {
            let at = self.now + after.as_micros() as u64;
            self.push(at, Event::TimerFired { node: from, timer });
        }

        for (to, msg) in std::mem::take(&mut self.runtime.outbox) {
            match &msg {
                PlumtreeMsg::Gossip(g) => {
                    self.stats.sent_gossip += 1;
                    if self.link_rtt_ms(from, to) > LONGHAUL_RTT_MS {
                        self.stats.longhaul_gossip += 1;
                    }
                    if let Some(stat) = self.msgs.get_mut(&g.payload.id()) {
                        stat.gossip_sends += 1;
                    }
                }
                PlumtreeMsg::IHave(_) => self.stats.sent_ihave += 1,
                PlumtreeMsg::Graft(_) => self.stats.sent_graft += 1,
                PlumtreeMsg::Prune(_) => self.stats.sent_prune += 1,
            }
            if self.params.loss > 0.0 && self.rng.random_bool(self.params.loss) {
                self.stats.dropped += 1;
                continue;
            }
            let at = self.now + self.flight_us(from, to);
            self.push(at, Event::Recv { to, msg });
        }

        // Mirror the agent's periodic IHave flush, but only schedule a tick
        // when there is something to flush so the queue can drain.
        if !self.tick_pending[from as usize] && !self.states[from as usize].lazy_queue().is_empty()
        {
            self.tick_pending[from as usize] = true;
            let at = self.now + TICK_INTERVAL.as_micros() as u64;
            self.push(at, Event::Tick { node: from });
        }
    }

    fn check_invariants(&self) {
        // By the time the queue fully drains, every flap has resolved (its
        // `NodeUp` always fires eventually), so anything still `down` here
        // must be a permanent crash.
        for (i, node) in (0..self.params.n as NId).enumerate() {
            if self.down[i] {
                assert!(
                    self.crashed.contains(&node),
                    "node {node} still down at drain but not a permanent crash"
                );
                continue;
            }
            let state = &self.states[i];
            let eager = state.eager_peers();
            let lazy = state.lazy_peers();
            let known = state.known_peers();
            for p in state.ring_locked_peers() {
                assert!(eager.contains(p), "ring-locked peer not eager");
            }
            for p in eager {
                assert!(!lazy.contains(p), "peer in both eager and lazy");
                assert!(known.contains(p), "eager peer not known");
            }
            for p in lazy {
                assert!(known.contains(p), "lazy peer not known");
            }
            assert!(lazy.len() <= state.max_lazy(), "lazy set exceeds max_lazy");
            assert!(state.lazy_queue().is_empty(), "lazy queue not drained");
            assert_eq!(
                known.len(),
                self.params.n - 1 - self.crashed.len(),
                "full membership not reconverged for node {node}"
            );
        }
    }

    fn report(&self) -> Report {
        // Not `n * msgs_per_node`: under failure injection a node's own
        // origin is skipped if it's already down when its broadcast fires
        // (see `Event::Originate`), so the true count of originated messages
        // can be less. Identical to `n * msgs_per_node` whenever nothing
        // gets skipped (i.e. every non-failure run).
        let expected = self.msgs.len() as u64 * (self.params.n as u64 - 1);
        let delivered = self.stats.latencies_us.len() as u64;

        let mut hops = self.stats.hops.clone();
        hops.sort_unstable();
        let mut lat = self.stats.latencies_us.clone();
        lat.sort_unstable();

        // time for a fully-delivered message to reach the whole cluster
        let mut full: Vec<u64> = self
            .msgs
            .values()
            .filter(|m| m.deliveries as u64 == self.params.n as u64 - 1)
            .map(|m| m.last_delivery_at - m.sent_at)
            .collect();
        full.sort_unstable();

        // duplicate copies delivered per message, in broadcast order, to show
        // tree convergence. 0 = perfect spanning tree (no duplicates); >0 = extra
        // copies each node receives on average.
        let mut by_time: Vec<&MsgStat> = self.msgs.values().collect();
        by_time.sort_unstable_by_key(|m| m.sent_at);
        let dups_per_msg = |msgs: &[&MsgStat]| -> f64 {
            if msgs.is_empty() {
                return 0.0;
            }
            let sends: u64 = msgs.iter().map(|m| m.gossip_sends).sum();
            sends as f64 / (msgs.len() as f64 * (self.params.n - 1) as f64) - 1.0
        };
        let decile = (by_time.len() / 10).max(1);
        let dups_per_msg_first = dups_per_msg(&by_time[..decile]);
        let dups_per_msg_last = dups_per_msg(&by_time[by_time.len() - decile..]);

        let avg_eager = self
            .states
            .iter()
            .map(|s| s.eager_peers().len())
            .sum::<usize>() as f64
            / self.params.n as f64;

        let (missed_due_to_failure, unexplained_missing) =
            failure_miss_stats(self.params.n, &self.msgs, &self.missed_during_downtime);

        Report {
            label: format!(
                "n={} num_eager={} eager={}/{}/{} rtt_aware={} loss={:.1}% seed={}",
                self.params.n,
                self.params.num_eager,
                self.params.eager_ratios.near_pct,
                self.params.eager_ratios.mid_pct,
                self.params.eager_ratios.far_pct,
                self.params.rtt_aware,
                self.params.loss * 100.0,
                self.params.seed,
            ),
            expected,
            delivered,
            missing: self.stats.missing,
            hops_p50: pct(&hops, 0.50),
            hops_p99: pct(&hops, 0.99),
            hops_max: hops.last().copied().unwrap_or(0),
            lat_p50_ms: pct(&lat, 0.50) / 1000,
            lat_p99_ms: pct(&lat, 0.99) / 1000,
            full_p50_ms: pct(&full, 0.50) / 1000,
            full_p99_ms: pct(&full, 0.99) / 1000,
            full_max_ms: full.last().copied().unwrap_or(0) / 1000,
            dups_per_msg_first,
            dups_per_msg_last,
            sent_gossip: self.stats.sent_gossip,
            sent_ihave: self.stats.sent_ihave,
            sent_graft: self.stats.sent_graft,
            sent_prune: self.stats.sent_prune,
            prune_suppressed: self.stats.prune_suppressed,
            longhaul_frac: self.stats.longhaul_gossip as f64 / self.stats.sent_gossip.max(1) as f64,
            duplicates: self.stats.duplicates,
            dropped: self.stats.dropped,
            not_cached: self.stats.not_cached,
            avg_eager,
            start_avg_eager: self.start_avg_eager,
            crashed: self.crashed.len(),
            flapped: self.flapped.len(),
            missed_due_to_failure,
            unexplained_missing,
        }
    }
}

fn pct<T: Copy + Default>(sorted: &[T], p: f64) -> T {
    if sorted.is_empty() {
        return T::default();
    }
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// For every message and every would-be recipient (all nodes except the
/// origin) that never appears in `delivered_to`, classify the miss using
/// `missed_during_downtime` — ground truth recorded live during the run (see
/// its doc comment), not inferred from timing after the fact:
///   - `missed_due_to_failure`: the recipient's entry in `missed_during_downtime`
///     names this message — it was already down when the message was born, or
///     a real delivery attempt reached it while down and got dropped.
///   - `unexplained_missing`: no such record exists — nothing ever even
///     tried to reach this recipient while it was reachable, or it was
///     never down at all. A real signal, expected to be 0 in a healthy run
///     (modulo gossip's own inherent small residual non-convergence, which
///     is unrelated to any failure and pre-exists this metric).
///     Short-circuits to `(0, 0)` when nothing was ever recorded, so non-failure
///     runs (including 2k-scale ones) pay nothing for this.
fn failure_miss_stats(
    n: usize,
    msgs: &HashMap<MId, MsgStat>,
    missed_during_downtime: &HashMap<NId, HashSet<MId>>,
) -> (u64, u64) {
    if missed_during_downtime.is_empty() {
        return (0, 0);
    }
    let mut missed_due_to_failure = 0u64;
    let mut unexplained_missing = 0u64;
    for (&mid, stat) in msgs.iter() {
        let origin = origin_of(mid);
        for r in 0..n as NId {
            if r == origin || stat.delivered_to.contains(&r) {
                continue;
            }
            let excused = missed_during_downtime
                .get(&r)
                .is_some_and(|mids| mids.contains(&mid));
            if excused {
                missed_due_to_failure += 1;
            } else {
                unexplained_missing += 1;
            }
        }
    }
    (missed_due_to_failure, unexplained_missing)
}

struct Report {
    label: String,
    expected: u64,
    delivered: u64,
    missing: u64,
    hops_p50: u32,
    hops_p99: u32,
    hops_max: u32,
    lat_p50_ms: u64,
    lat_p99_ms: u64,
    full_p50_ms: u64,
    full_p99_ms: u64,
    full_max_ms: u64,
    dups_per_msg_first: f64,
    dups_per_msg_last: f64,
    sent_gossip: u64,
    sent_ihave: u64,
    sent_graft: u64,
    sent_prune: u64,
    prune_suppressed: u64,
    longhaul_frac: f64,
    duplicates: u64,
    dropped: u64,
    not_cached: u64,
    avg_eager: f64,
    start_avg_eager: f64,
    /// Nodes that crashed permanently.
    crashed: usize,
    /// Nodes that went down and rejoined at least once.
    flapped: usize,
    /// Non-deliveries fully explained by a recipient being down (crashed, or
    /// not yet (re)joined) when the message was sent — see `failure_miss_stats`.
    missed_due_to_failure: u64,
    /// Non-deliveries that aren't explained by any known failure — should be
    /// 0 in a healthy run; a nonzero value signals a real protocol miss.
    unexplained_missing: u64,
}

impl Report {
    fn delivery_pct(&self) -> f64 {
        self.delivered as f64 / self.expected as f64 * 100.0
    }

    /// Delivery percentage after excluding the gaps that failures fully
    /// explain — i.e. delivery among the recipients that could actually have
    /// received each message.
    fn adjusted_delivery_pct(&self) -> f64 {
        self.delivered as f64 / (self.expected - self.missed_due_to_failure).max(1) as f64 * 100.0
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "--- {}", self.label)?;
        writeln!(
            f,
            "  delivery   {:>10.4}% ({}/{})  missing={} not_cached={}",
            self.delivery_pct(),
            self.delivered,
            self.expected,
            self.missing,
            self.not_cached,
        )?;
        writeln!(
            f,
            "  failures   crashed={} flapped={} missed_due_to_failure={} ({:.2}%) \
             unexplained_missing={}  adjusted_delivery={:.4}%",
            self.crashed,
            self.flapped,
            self.missed_due_to_failure,
            self.missed_due_to_failure as f64 / self.expected.max(1) as f64 * 100.0,
            self.unexplained_missing,
            self.adjusted_delivery_pct(),
        )?;
        writeln!(
            f,
            "  hops       p50={} p99={} max={}",
            self.hops_p50, self.hops_p99, self.hops_max
        )?;
        writeln!(
            f,
            "  latency    p50={}ms p99={}ms   full-cluster: p50={}ms p99={}ms max={}ms",
            self.lat_p50_ms, self.lat_p99_ms, self.full_p50_ms, self.full_p99_ms, self.full_max_ms
        )?;
        writeln!(
            f,
            "  duplicates per msg first-decile={:.2} last-decile={:.2}  duplicates={}",
            self.dups_per_msg_first, self.dups_per_msg_last, self.duplicates
        )?;
        writeln!(
            f,
            "  traffic    gossip={} ihave={} graft={} prune={} prune_suppressed={} dropped={}  longhaul-gossip={:.1}%",
            self.sent_gossip,
            self.sent_ihave,
            self.sent_graft,
            self.sent_prune,
            self.prune_suppressed,
            self.dropped,
            self.longhaul_frac * 100.0,
        )?;
        writeln!(
            f,
            "  topology   avg_eager start={:.1} end={:.1}",
            self.start_avg_eager, self.avg_eager
        )
    }
}

// ---------------------------------------------------------------------------
// Gossip (foca) broadcast baseline
// ---------------------------------------------------------------------------
//
// Recreates the dissemination logic of `corro-agent`'s `handle_broadcasts`
// (the `BroadcastMethod::Gossip` path that plumtree is meant to replace) so we
// have concrete numbers to compare plumtree against on the same topology.
//
// The model (see crates/corro-agent/src/broadcast/mod.rs):
//   * Every node originates one change, exactly like the plumtree sim, so the
//     two are measured on an identical workload. We deliberately do NOT model
//     the byte-batching / broadcast_cutoff buffering — that only packs several
//     changes into one datagram, it doesn't change *who* receives a change — so
//     messages disseminate independently of one another. That independence lets
//     `run` process originators in memory-bounded chunks: identical per-message
//     results, bounded peak RAM at 2k scale (all-at-once would be >1GB).
//   * Originator (`AddBroadcast`, is_local): floods ALL of its ring0 peers once
//     (the "local broadcast"), plus a global pending that EXCLUDES ring0.
//   * Any node, on first receipt of a change that isn't its own
//     (`Rebroadcast`, is_local=false): a global pending only; ring0 is NOT
//     excluded and is NOT specially flooded.
//   * A global pending sends to `choose_count` freshly-chosen random peers
//     (deduped via `sent_to`), then re-queues itself up to `max_transmissions`
//     times with `100ms * send_count` backoff.
//       choose_count   = max(num_indirect_probes=3, (N - ring0)/(max_tx*10))
//       max_transmissions = foca Config::compute_max_tx(cluster_size)
//   * We don't model the 10MB/s rate limiter (messages are tiny) or the
//     anti-entropy SYNC path that mops up gossip stragglers — so the reported
//     delivery% is the broadcast layer alone, and may sit just under 100%.

const NUM_INDIRECT_PROBES: usize = 3;
/// `sleep_ms_base` between successive transmissions of a pending broadcast.
const GOSSIP_RESEND_BASE: Duration = Duration::from_millis(100);

/// foca `Config::compute_max_tx`: log10(cluster_size+1) * 4, clamped to [1,255].
fn compute_max_tx(cluster_size: u32) -> u8 {
    let max_tx = f64::from(cluster_size.saturating_add(1)).log10() * 4.0;
    if max_tx <= 1.0 {
        1
    } else if max_tx >= 255.0 {
        255
    } else {
        max_tx as u8
    }
}

#[derive(Clone)]
struct GossipParams {
    n: usize,
    /// With RTT info each node has a ring0 (peers < 6ms) to flood; blind means
    /// no ring assignment, so ring0 is empty and everything is random gossip.
    rtt_aware: bool,
    loss: f64,
    seed: u64,
    /// Window over which origins are staggered by `run_staggered` (mirrors
    /// `Params::broadcast_window`; unused by the chunked `run`).
    broadcast_window: Duration,
    /// See `Params::crash_count`/`flap_count`/`flap_downtime`.
    crash_count: usize,
    flap_count: usize,
    flap_downtime: Duration,
    /// See `Params::detection_delay_min`/`detection_delay_max`.
    detection_delay_min: Duration,
    detection_delay_max: Duration,
}

impl GossipParams {
    fn new(n: usize) -> Self {
        Self {
            n,
            rtt_aware: true,
            loss: 0.0,
            seed: 42,
            broadcast_window: Duration::from_millis(n as u64 * 5),
            crash_count: 0,
            flap_count: 0,
            flap_downtime: Duration::from_secs(5),
            detection_delay_min: Duration::from_secs(2),
            detection_delay_max: Duration::from_secs(8),
        }
    }

    /// See `Params::with_failures`.
    fn with_failures(
        mut self,
        crash_count: usize,
        flap_count: usize,
        flap_downtime: Duration,
    ) -> Self {
        self.crash_count = crash_count;
        self.flap_count = flap_count;
        self.flap_downtime = flap_downtime;
        self
    }

    fn with_detection_delay(mut self, min: Duration, max: Duration) -> Self {
        self.detection_delay_min = min;
        self.detection_delay_max = max;
        self
    }
}

enum GEvent {
    Originate {
        node: NId,
        mid: MId,
    },
    /// One transmission round of a pending broadcast on `node`.
    Send {
        node: NId,
        mid: MId,
        send_count: u8,
        sent_to: std::collections::HashSet<NId>,
        hop: u32,
    },
    Recv {
        to: NId,
        mid: MId,
        hop: u32,
    },
    /// See `Event::NodeDown`.
    NodeDown {
        node: NId,
        restart_after: Option<Duration>,
    },
    /// See `Event::NodeUp`.
    NodeUp {
        node: NId,
    },
    /// See `Event::MemberDown`.
    MemberDown {
        observer: NId,
        subject: NId,
    },
    /// See `Event::MemberUp`.
    MemberUp {
        observer: NId,
        subject: NId,
    },
}

struct GossipSim {
    params: GossipParams,
    now: u64,
    next_seq: u64,
    queue: BinaryHeap<Scheduled<GEvent>>,
    region_of: Vec<u8>,
    rtt_ms: Vec<Vec<u64>>,
    /// ring0 peers (RTT ring 0) per node; empty when topology-blind.
    ring0: Vec<Vec<NId>>,
    seen: Vec<std::collections::HashSet<MId>>,
    rng: StdRng,
    max_tx: u8,
    msgs: HashMap<MId, MsgStat>,
    stats: Stats,
    /// Nodes currently unreachable (crashed or mid-flap). See `Sim::down`.
    down: Vec<bool>,
    /// Nodes that crashed permanently.
    crashed: HashSet<NId>,
    /// Nodes that went down and rejoined at least once.
    flapped: HashSet<NId>,
    /// See `Sim::missed_during_downtime`.
    missed_during_downtime: HashMap<NId, HashSet<MId>>,
    /// Per-node view of which other nodes it currently believes are down —
    /// gossip's equivalent of Plumtree's adaptive `known_peers`, since gossip
    /// otherwise always samples targets from the full `0..n` regardless of
    /// membership. Updated via delayed `MemberDown`/`MemberUp`, same as
    /// `Sim`'s `peer_down`/`peer_up`.
    believed_down: Vec<HashSet<NId>>,
}

impl GossipSim {
    fn new(params: GossipParams) -> Self {
        let mut rng = StdRng::seed_from_u64(params.seed);
        let n = params.n;

        let nregions = REGIONS.len();
        let rtt_ms: Vec<Vec<u64>> = (0..nregions)
            .map(|a| (0..nregions).map(|b| region_rtt_ms(a, b)).collect())
            .collect();

        let total_weight: u32 = REGIONS.iter().map(|r| r.weight).sum();
        let region_of: Vec<u8> = (0..n)
            .map(|_| {
                let mut pick = rng.random_range(0..total_weight);
                for (i, r) in REGIONS.iter().enumerate() {
                    if pick < r.weight {
                        return i as u8;
                    }
                    pick -= r.weight;
                }
                unreachable!()
            })
            .collect();

        // ring0(i) = peers within ring 0 (RTT < 6ms). Blind => no rings => empty.
        let ring0: Vec<Vec<NId>> = (0..n)
            .map(|i| {
                if !params.rtt_aware {
                    return Vec::new();
                }
                (0..n)
                    .filter(|&j| j != i)
                    .filter(|&j| {
                        let rtt = rtt_ms[region_of[i] as usize][region_of[j] as usize];
                        ring_from_rtt_ms(rtt) == Some(0)
                    })
                    .map(|j| j as NId)
                    .collect()
            })
            .collect();

        let mut sim = Self {
            now: 0,
            next_seq: 0,
            queue: BinaryHeap::new(),
            region_of,
            rtt_ms,
            ring0,
            seen: vec![std::collections::HashSet::new(); n],
            rng,
            max_tx: compute_max_tx(n as u32),
            msgs: HashMap::new(),
            stats: Stats::default(),
            down: vec![false; n],
            crashed: HashSet::new(),
            flapped: HashSet::new(),
            missed_during_downtime: HashMap::new(),
            believed_down: vec![HashSet::new(); n],
            params,
        };

        // Inject the same failures as `Sim::new`, scheduled uniformly over
        // `broadcast_window` so both sims can be compared under an identical
        // failure timeline regardless of which `run*` method is used.
        let window_us = sim.params.broadcast_window.as_micros() as u64;
        let fail_count = sim.params.crash_count + sim.params.flap_count;
        debug_assert!(
            fail_count <= n,
            "not enough nodes for the requested failure counts"
        );
        if fail_count > 0 {
            let mut candidates: Vec<NId> = (0..n as NId).collect();
            candidates.shuffle(&mut sim.rng);
            for &node in &candidates[..sim.params.crash_count] {
                let at = sim.rng.random_range(0..window_us.max(1));
                sim.push(
                    at,
                    GEvent::NodeDown {
                        node,
                        restart_after: None,
                    },
                );
            }
            for &node in &candidates[sim.params.crash_count..fail_count] {
                let at = sim.rng.random_range(0..window_us.max(1));
                sim.push(
                    at,
                    GEvent::NodeDown {
                        node,
                        restart_after: Some(sim.params.flap_downtime),
                    },
                );
            }
        }

        sim
    }

    fn push(&mut self, at: u64, ev: GEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.queue.push(Scheduled { at, seq, ev });
    }

    fn link_rtt_ms(&self, from: NId, to: NId) -> u64 {
        self.rtt_ms[self.region_of[from as usize] as usize][self.region_of[to as usize] as usize]
    }

    fn flight_us(&mut self, from: NId, to: NId) -> u64 {
        let rtt = self.link_rtt_ms(from, to);
        let jitter = self.rng.random_range(1.0..1.15);
        ((rtt * 1000) as f64 / 2.0 * jitter) as u64
    }

    /// Transmit one copy of `mid` from->to, applying loss; schedules the Recv.
    fn send_one(&mut self, from: NId, to: NId, mid: MId, hop: u32) {
        self.stats.sent_gossip += 1;
        if self.link_rtt_ms(from, to) > LONGHAUL_RTT_MS {
            self.stats.longhaul_gossip += 1;
        }
        if let Some(s) = self.msgs.get_mut(&mid) {
            s.gossip_sends += 1;
        }
        if self.params.loss > 0.0 && self.rng.random_bool(self.params.loss) {
            self.stats.dropped += 1;
            return;
        }
        let at = self.now + self.flight_us(from, to);
        self.push(at, GEvent::Recv { to, mid, hop });
    }

    /// `choose_count` random peers not already in `exclude`.
    fn sample_excluding(
        &mut self,
        exclude: &std::collections::HashSet<NId>,
        take: usize,
    ) -> Vec<NId> {
        let mut out = Vec::with_capacity(take);
        let mut picked = std::collections::HashSet::new();
        let n = self.params.n as NId;
        let mut attempts = 0usize;
        // rejection sampling: sent_to stays far below N, so collisions are rare.
        while out.len() < take && attempts < take.saturating_mul(50).max(1) {
            attempts += 1;
            let c = self.rng.random_range(0..n);
            if exclude.contains(&c) || !picked.insert(c) {
                continue;
            }
            out.push(c);
        }
        out
    }

    /// See `Sim::random_detection_delay`.
    fn random_detection_delay(&mut self) -> u64 {
        let min = self.params.detection_delay_min.as_micros() as u64;
        let max = self.params.detection_delay_max.as_micros() as u64;
        if max <= min {
            return min;
        }
        self.rng.random_range(min..max)
    }

    fn run(mut self) -> Report {
        // Every node originates one change (matching the plumtree sim). Because
        // gossip messages disseminate independently (no shared tree, no modeled
        // bandwidth contention), originators are processed in memory-bounded
        // chunks: identical per-message results, bounded peak RAM at 2k scale.
        const CHUNK: usize = 128;
        let mut origins: Vec<NId> = (0..self.params.n as NId).collect();
        // shuffle so a chunk isn't correlated with region/node-id ordering.
        origins.shuffle(&mut self.rng);

        for chunk in origins.chunks(CHUNK) {
            for &node in chunk {
                let mid = MId((node as u64) << 32);
                let at = self.now;
                self.push(at, GEvent::Originate { node, mid });
            }
            self.drain_queue();
            // messages are independent across chunks; clear per-node seen so
            // memory doesn't grow with the number of chunks processed.
            for s in &mut self.seen {
                s.clear();
            }
        }
        self.report()
    }

    /// Like `run`, but stages one origin per node across `broadcast_window`
    /// — the same continuous-time model `Sim::run` uses — instead of firing
    /// a whole chunk at the same instant. This is what makes the failure
    /// timeline (`GossipParams::with_failures`, scheduled in `new` over the
    /// same window) meaningfully comparable to `Sim`'s. Small-scale only:
    /// unlike `run`, there's no chunking, so `seen` grows for every node —
    /// don't use this at 2k+ node scale.
    fn run_staggered(mut self) -> Report {
        let n = self.params.n as NId;
        let window_us = self.params.broadcast_window.as_micros() as u64;
        for node in 0..n {
            let mid = MId((node as u64) << 32);
            let at = self.rng.random_range(0..window_us.max(1));
            self.push(at, GEvent::Originate { node, mid });
        }
        self.drain_queue();
        self.report()
    }

    /// Run the event queue to quiescence for the currently-pending messages.
    fn drain_queue(&mut self) {
        while let Some(Scheduled { at, ev, .. }) = self.queue.pop() {
            debug_assert!(at >= self.now);
            self.now = at;
            match ev {
                GEvent::Originate { node, mid } => {
                    if self.down[node as usize] {
                        continue;
                    }
                    self.seen[node as usize].insert(mid);
                    self.msgs.insert(
                        mid,
                        MsgStat {
                            sent_at: self.now,
                            gossip_sends: 0,
                            deliveries: 0,
                            last_delivery_at: 0,
                            delivered_to: HashSet::new(),
                        },
                    );
                    // Ground truth: any node already down when this message
                    // is born could never receive it, full stop.
                    for r in 0..self.params.n as NId {
                        if r != node && self.down[r as usize] {
                            self.missed_during_downtime
                                .entry(r)
                                .or_default()
                                .insert(mid);
                        }
                    }
                    // local broadcast: flood every ring0 peer once (hop 1),
                    // skipping any peer this node currently believes is down.
                    // `sent_to` still excludes the *full* ring0 (believed-down
                    // or not) from the random pool below, so a peer we've
                    // stopped flooding isn't picked again at random either.
                    let r0 = self.ring0[node as usize].clone();
                    for &peer in &r0 {
                        if !self.believed_down[node as usize].contains(&peer) {
                            self.send_one(node, peer, mid, 1);
                        }
                    }
                    // global pending excluding ring0 (is_local=true).
                    let mut sent_to: std::collections::HashSet<NId> = r0.into_iter().collect();
                    sent_to.insert(node);
                    self.do_send(node, mid, 0, sent_to, 1);
                }
                GEvent::Send {
                    node,
                    mid,
                    send_count,
                    sent_to,
                    hop,
                } => {
                    self.do_send(node, mid, send_count, sent_to, hop);
                }
                GEvent::Recv { to, mid, hop, .. } => {
                    if self.down[to as usize] {
                        self.stats.dropped += 1;
                        // Ground truth: this send really did try to reach
                        // `to` and got dropped because it was down.
                        self.missed_during_downtime
                            .entry(to)
                            .or_default()
                            .insert(mid);
                        continue;
                    }
                    if self.seen[to as usize].contains(&mid) {
                        self.stats.duplicates += 1;
                        continue;
                    }
                    self.seen[to as usize].insert(mid);
                    let stat = self.msgs.get_mut(&mid).expect("recv unknown msg");
                    stat.deliveries += 1;
                    stat.last_delivery_at = self.now;
                    stat.delivered_to.insert(to);
                    self.stats.latencies_us.push(self.now - stat.sent_at);
                    self.stats.hops.push(hop);
                    // rebroadcast (is_local=false): global pending, ring0 NOT excluded.
                    let mut sent_to = std::collections::HashSet::new();
                    sent_to.insert(to);
                    self.do_send(to, mid, 0, sent_to, hop + 1);
                }
                GEvent::NodeDown {
                    node,
                    restart_after,
                } => {
                    self.down[node as usize] = true;
                    if restart_after.is_none() {
                        self.crashed.insert(node);
                    }
                    let n = self.params.n as NId;
                    for observer in 0..n {
                        if observer == node || self.down[observer as usize] {
                            continue;
                        }
                        let delay = self.random_detection_delay();
                        let at = self.now + delay;
                        self.push(
                            at,
                            GEvent::MemberDown {
                                observer,
                                subject: node,
                            },
                        );
                    }
                    if let Some(downtime) = restart_after {
                        let at = self.now + downtime.as_micros() as u64;
                        self.push(at, GEvent::NodeUp { node });
                    }
                }
                GEvent::NodeUp { node } => {
                    self.down[node as usize] = false;
                    self.flapped.insert(node);
                    // Fresh process: empty dedup cache, and an instant
                    // rejoin-bootstrap belief that mirrors currently-down
                    // membership (see `Sim`'s `NodeUp` handler).
                    self.seen[node as usize].clear();
                    let n = self.params.n as NId;
                    self.believed_down[node as usize] = (0..n)
                        .filter(|&j| j != node && self.down[j as usize])
                        .collect();

                    for observer in 0..n {
                        if observer == node || self.down[observer as usize] {
                            continue;
                        }
                        let delay = self.random_detection_delay();
                        let at = self.now + delay;
                        self.push(
                            at,
                            GEvent::MemberUp {
                                observer,
                                subject: node,
                            },
                        );
                    }
                }
                GEvent::MemberDown { observer, subject } => {
                    if !self.down[observer as usize] {
                        self.believed_down[observer as usize].insert(subject);
                    }
                }
                GEvent::MemberUp { observer, subject } => {
                    if !self.down[observer as usize] && !self.down[subject as usize] {
                        self.believed_down[observer as usize].remove(&subject);
                    }
                }
            }
        }
    }

    /// One transmission round of a pending broadcast, then re-queue with backoff.
    fn do_send(
        &mut self,
        node: NId,
        mid: MId,
        send_count: u8,
        mut sent_to: std::collections::HashSet<NId>,
        hop: u32,
    ) {
        if self.down[node as usize] {
            // Dead sender: no more retransmissions.
            return;
        }
        let ring0_count = self.ring0[node as usize].len();
        let dynamic = self.params.n.saturating_sub(ring0_count) / (self.max_tx as usize * 10);
        let choose_count = NUM_INDIRECT_PROBES.max(dynamic);
        let take = choose_count.min(self.params.n.saturating_sub(sent_to.len()));
        // Don't waste a pick on a peer we already believe is down.
        let mut exclude = sent_to.clone();
        exclude.extend(self.believed_down[node as usize].iter().copied());
        for p in self.sample_excluding(&exclude, take) {
            sent_to.insert(p);
            self.send_one(node, p, mid, hop);
        }

        let next = send_count + 1;
        if next < self.max_tx {
            let at = self.now + GOSSIP_RESEND_BASE.as_micros() as u64 * next as u64;
            self.push(
                at,
                GEvent::Send {
                    node,
                    mid,
                    send_count: next,
                    sent_to,
                    hop,
                },
            );
        }
    }

    fn report(&self) -> Report {
        // Every node originates once, so expected deliveries = M*(N-1) where
        // M is how many actually got to originate. Not `params.n`: under
        // failure injection a node's own origin is skipped if it's already
        // down when its broadcast fires (see `GEvent::Originate`), so M can
        // be less than N. Identical to `n` whenever nothing gets skipped.
        let expected = self.msgs.len() as u64 * (self.params.n as u64 - 1);
        let delivered = self.stats.latencies_us.len() as u64;

        let mut hops = self.stats.hops.clone();
        hops.sort_unstable();
        let mut lat = self.stats.latencies_us.clone();
        lat.sort_unstable();

        let mut full: Vec<u64> = self
            .msgs
            .values()
            .filter(|m| m.deliveries as u64 == self.params.n as u64 - 1)
            .map(|m| m.last_delivery_at - m.sent_at)
            .collect();
        full.sort_unstable();

        // measure duplicate sends over all messages.
        let dups_per_msg = if self.msgs.is_empty() {
            0.0
        } else {
            let total_sends: u64 = self.msgs.values().map(|m| m.gossip_sends).sum();
            total_sends as f64 / (self.msgs.len() as f64 * (self.params.n - 1) as f64) - 1.0
        };

        let avg_ring0 =
            self.ring0.iter().map(|r| r.len()).sum::<usize>() as f64 / self.params.n.max(1) as f64;

        let (missed_due_to_failure, unexplained_missing) =
            failure_miss_stats(self.params.n, &self.msgs, &self.missed_during_downtime);

        Report {
            label: format!(
                "GOSSIP n={} all-broadcast rtt_aware={} loss={:.1}% seed={} max_tx={}",
                self.params.n,
                self.params.rtt_aware,
                self.params.loss * 100.0,
                self.params.seed,
                self.max_tx,
            ),
            expected,
            delivered,
            missing: 0,
            hops_p50: pct(&hops, 0.50),
            hops_p99: pct(&hops, 0.99),
            hops_max: hops.last().copied().unwrap_or(0),
            lat_p50_ms: pct(&lat, 0.50) / 1000,
            lat_p99_ms: pct(&lat, 0.99) / 1000,
            full_p50_ms: pct(&full, 0.50) / 1000,
            full_p99_ms: pct(&full, 0.99) / 1000,
            full_max_ms: full.last().copied().unwrap_or(0) / 1000,
            // gossip has no tree to converge; report the single value in both slots.
            dups_per_msg_first: dups_per_msg,
            dups_per_msg_last: dups_per_msg,
            sent_gossip: self.stats.sent_gossip,
            sent_ihave: 0,
            sent_graft: 0,
            sent_prune: 0,
            prune_suppressed: 0,
            longhaul_frac: self.stats.longhaul_gossip as f64 / self.stats.sent_gossip.max(1) as f64,
            duplicates: self.stats.duplicates,
            dropped: self.stats.dropped,
            not_cached: 0,
            // no eager set in gossip; report the avg ring0 (guaranteed-flood) size.
            avg_eager: avg_ring0,
            start_avg_eager: avg_ring0,
            crashed: self.crashed.len(),
            flapped: self.flapped.len(),
            missed_due_to_failure,
            unexplained_missing,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Install a tracing subscriber driven by `RUST_LOG` (no-op if RUST_LOG is
/// unset or a subscriber is already installed). The plumtree state machine
/// logs every graft/prune and eager<->lazy move via `trace!`, independent of
/// the sim's runtime, so this surfaces them. Each line carries `self_actor_id`
/// and `peer`, so repeated churn on the same pair is visible.
fn init_trace_logging() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        // virtual time, not wall-clock — line order is the sim's causal order.
        .without_time()
        .try_init();
}

/// Emit plum-foca trace logs for a single 2k plumtree run so tree stability —
/// whether we keep grafting/pruning the same node pairs — can be inspected.
/// Full `plum_foca=trace` is ~18M lines (dominated by per-forward gossip), so
/// filter to the topology-churn events. Run with:
///
/// ```text
/// RUST_LOG=plum_foca=trace cargo test --release -p plum-foca --test sim \
///   sim_2k_tree_stability -- --ignored --nocapture 2>/dev/null \
///   | grep -E 'moved to eager|moved to lazy|PRUNE|graft|evicted' \
///   > /tmp/plumtree_2k_stability.log
/// ```
#[test]
#[ignore = "emits trace logs; run in release with RUST_LOG=plum_foca=trace"]
fn sim_2k_tree_stability() {
    init_trace_logging();
    let report = Sim::new(Params::new(2000, 8)).run();
    eprintln!("{report}");
}

/// Every node broadcasts once; every message must reach every other node.
#[test]
fn sim_small_full_delivery() {
    let report = Sim::new(Params::new(200, 5)).run();
    println!("{report}");
    assert_eq!(report.delivered, report.expected, "incomplete delivery");
    assert_eq!(report.missing, 0);
}

/// With message loss, the IHave -> timeout -> GRAFT path must recover.
#[test]
fn sim_small_loss_recovery() {
    let mut params = Params::new(200, 5);
    params.loss = 0.02;
    let report = Sim::new(params).run();
    println!("{report}");
    assert!(report.dropped > 0, "loss knob had no effect");
    assert!(
        report.delivery_pct() > 99.9,
        "delivery {:.4}% under 2% loss",
        report.delivery_pct()
    );
    assert!(report.sent_graft > 0, "recovery never used the graft path");
}

/// RTT-aware eager selection should keep most gossip off long-haul links
/// compared to topology-blind selection.
#[test]
fn sim_small_rtt_awareness_pays_off() {
    let aware = Sim::new(Params::new(300, 8)).run();
    let mut blind_params = Params::new(300, 8);
    blind_params.rtt_aware = false;
    let blind = Sim::new(blind_params).run();
    println!("rtt-aware:\n{aware}");
    println!("topology-blind:\n{blind}");

    assert_eq!(aware.delivered, aware.expected);
    assert_eq!(blind.delivered, blind.expected);
    assert!(
        aware.longhaul_frac < blind.longhaul_frac,
        "rtt-aware long-haul fraction {:.3} not lower than blind {:.3}",
        aware.longhaul_frac,
        blind.longhaul_frac
    );
}

/// 2k nodes, all broadcasting, across a sweep of num_eager values.
/// Run with: cargo test --release -p plum-foca --test sim -- --ignored --nocapture
#[test]
#[ignore = "slow; run in release mode"]
fn sim_2k_num_eager_sweep() {
    let mut hops_p50 = Vec::new();
    for num_eager in [3, 5, 8, 12] {
        let report = Sim::new(Params::new(2000, num_eager)).run();
        println!("{report}");
        assert_eq!(
            report.delivered, report.expected,
            "incomplete delivery at num_eager={num_eager}"
        );
        assert_eq!(report.missing, 0);
        hops_p50.push(report.hops_p50);
    }
    // more eager peers -> wider tree -> fewer hops
    assert!(
        hops_p50.last() <= hops_p50.first(),
        "hops did not improve with fanout: {hops_p50:?}"
    );
}

/// 2k nodes: compare near/mid/far eager ratio splits.
/// Run with: cargo test --release -p plum-foca --test sim sim_2k_eager_ratios_sweep -- --ignored --nocapture
#[test]
#[ignore = "slow; run in release mode"]
fn sim_2k_eager_ratios_sweep() {
    const N: usize = 2000;
    let cases: [(&str, EagerRatios); 4] = [
        (
            "near-heavy (default)",
            EagerRatios {
                near_pct: 50,
                mid_pct: 30,
                far_pct: 20,
            },
        ),
        (
            "balanced",
            EagerRatios {
                near_pct: 40,
                mid_pct: 30,
                far_pct: 30,
            },
        ),
        (
            "far-heavy",
            EagerRatios {
                near_pct: 30,
                mid_pct: 35,
                far_pct: 35,
            },
        ),
        (
            "near small",
            EagerRatios {
                near_pct: 20,
                mid_pct: 40,
                far_pct: 40,
            },
        ),
    ];

    let mut reports = Vec::new();
    for (name, ratios) in cases {
        let report = Sim::new(Params::new(N, 8).with_eager_ratios(ratios)).run();
        println!("{name}:\n{report}");
        assert_eq!(
            report.delivered, report.expected,
            "incomplete delivery for {name}"
        );
        assert_eq!(report.missing, 0, "missing messages for {name}");
        reports.push((name, report));
    }
}

/// Baseline: foca gossip broadcast at small scale. Characterizes the redundancy
/// (duplicate copies per message) and delivery of the current production path on
/// the same topology.
#[test]
fn sim_gossip_small() {
    let report = GossipSim::new(GossipParams::new(200)).run();
    println!("{report}");
    // epidemic gossip reaches ~everyone, though the broadcast layer alone
    // (no anti-entropy sync) can leave a tiny straggler tail.
    assert!(
        report.delivery_pct() > 99.0,
        "gossip delivery {:.2}% unexpectedly low",
        report.delivery_pct()
    );
    // each relaying node sprays a large fraction of the cluster: dups/msg is high.
    assert!(
        report.dups_per_msg_first > 1.0,
        "gossip dups/msg {:.2} unexpectedly low",
        report.dups_per_msg_first
    );
}

/// Head-to-head: the current foca gossip broadcast vs plumtree at 2k, on the
/// same topology, printed as one comparison table. This is the "what does
/// plumtree buy us" test. dups/msg is the key number (redundant copies delivered
/// per message); `sends/msg` = (dups/msg + 1)·(N-1) is the absolute per-change cost.
/// Run with: cargo test --release -p plum-foca --test sim -- --ignored --nocapture
#[test]
#[ignore = "slow; run in release mode"]
fn sim_gossip_vs_plumtree_2k() {
    const N: usize = 2000;
    let gossip = GossipSim::new(GossipParams::new(N)).run();
    let plum = Sim::new(Params::new(N, 8)).run();

    println!("gossip (current production path):\n{gossip}");
    println!("plumtree (num_eager=8):\n{plum}");

    let row = |name: &str, g: String, p: String| println!("  {name:<22}{g:>16}{p:>16}");
    let sends_per_msg = |r: &Report| (r.dups_per_msg_first + 1.0) * (N as f64 - 1.0);
    println!("\n=== gossip vs plumtree (n={N}, full membership) ===");
    row("metric", "gossip".into(), "plumtree".into());
    row(
        "delivery %",
        format!("{:.3}", gossip.delivery_pct()),
        format!("{:.3}", plum.delivery_pct()),
    );
    row(
        "hops p50/p99",
        format!("{}/{}", gossip.hops_p50, gossip.hops_p99),
        format!("{}/{}", plum.hops_p50, plum.hops_p99),
    );
    row(
        "full-cluster p50 ms",
        gossip.full_p50_ms.to_string(),
        plum.full_p50_ms.to_string(),
    );
    row(
        "full-cluster p99 ms",
        gossip.full_p99_ms.to_string(),
        plum.full_p99_ms.to_string(),
    );
    row(
        "dups/msg (redundancy)",
        format!("{:.1}", gossip.dups_per_msg_first),
        format!("{:.1}", plum.dups_per_msg_last),
    );
    row(
        "sends/change",
        format!("{:.0}", sends_per_msg(&gossip)),
        format!("{:.0}", sends_per_msg(&plum)),
    );
    row(
        "longhaul gossip %",
        format!("{:.1}", gossip.longhaul_frac * 100.0),
        format!("{:.1}", plum.longhaul_frac * 100.0),
    );
    println!(
        "  => plumtree uses ~{:.0}x fewer transmissions per change",
        sends_per_msg(&gossip) / sends_per_msg(&plum).max(1.0)
    );

    // The whole point of plumtree: drastically lower redundancy at comparable
    // convergence. Guard the headline result so a regression is caught.
    assert!(
        plum.dups_per_msg_last < gossip.dups_per_msg_first / 5.0,
        "plumtree dups/msg {:.1} not dramatically below gossip dups/msg {:.1}",
        plum.dups_per_msg_last,
        gossip.dups_per_msg_first
    );
}

/// 2k-node gossip baseline, to compare directly against the plumtree numbers
/// (`sim_2k_num_eager_sweep` / `sim_2k_rtt_awareness`). Every node broadcasts
/// once, the same workload as the plumtree 2k tests.
/// Run with: cargo test --release -p plum-foca --test sim -- --ignored --nocapture
#[test]
#[ignore = "slow; run in release mode"]
fn sim_gossip_2k() {
    let aware = GossipSim::new(GossipParams::new(2000)).run();
    let mut blind_params = GossipParams::new(2000);
    blind_params.rtt_aware = false;
    let blind = GossipSim::new(blind_params).run();
    println!("rtt-aware (ring0 flood):\n{aware}");
    println!("topology-blind (pure random gossip):\n{blind}");
}

/// 2k nodes: topology-aware vs topology-blind eager selection.
#[test]
#[ignore = "slow; run in release mode"]
fn sim_2k_rtt_awareness() {
    init_trace_logging();
    let aware = Sim::new(Params::new(2000, 8)).run();
    let mut blind_params = Params::new(2000, 8);
    blind_params.rtt_aware = false;
    let blind = Sim::new(blind_params).run();
    println!("rtt-aware:\n{aware}");
    println!("topology-blind:\n{blind}");

    assert_eq!(aware.delivered, aware.expected);
    assert_eq!(blind.delivered, blind.expected);
    assert!(aware.longhaul_frac < blind.longhaul_frac);
}

/// Measure the prune-throttle (B2): same 2k run with throttling off vs the
/// sim default (500ms) per-peer PRUNE window. Expect prune traffic to drop
/// and full-cluster latency preserved (the in-flight duplicate bursts no longer
/// each draw a PRUNE). Run with:
///   cargo test --release -p plum-foca --test sim sim_2k_prune_throttle -- --ignored --nocapture
#[test]
#[ignore = "slow; run in release mode"]
fn sim_2k_prune_throttle() {
    let mut baseline_params = Params::new(2000, 8);
    baseline_params.prune_throttle = None;
    let baseline = Sim::new(baseline_params).run();
    let throttled = Sim::new(Params::new(2000, 8)).run();
    println!("prune-throttle OFF:\n{baseline}");
    println!("prune-throttle 500ms:\n{throttled}");

    // delivery must be unharmed
    assert_eq!(
        throttled.delivered, throttled.expected,
        "throttle broke delivery"
    );
    assert_eq!(throttled.missing, 0);
    // prune traffic should fall substantially
    assert!(
        throttled.sent_prune < baseline.sent_prune / 2,
        "throttle didn't cut prune traffic: {} -> {}",
        baseline.sent_prune,
        throttled.sent_prune
    );
    assert!(throttled.prune_suppressed > 0, "nothing was suppressed");
    // convergence shouldn't regress (allow a small margin for timing shifts)
    assert!(
        throttled.full_p99_ms <= baseline.full_p99_ms + baseline.full_p99_ms / 10,
        "full-cluster p99 regressed: {}ms -> {}ms",
        baseline.full_p99_ms,
        throttled.full_p99_ms
    );
}

/// Some nodes crash permanently mid-run (stop sending/receiving for good).
/// Observers only learn of the crash after a random per-observer delay
/// (`MemberDown`), mirroring staggered SWIM detection. The tree must
/// self-heal around the dead nodes: since the sim always runs to full
/// quiescence, every node that never crashes must eventually receive every
/// message, and every surviving node must fully reconverge on membership
/// (checked by `check_invariants`, which already accounts for `crashed`).
#[test]
fn sim_small_crash_resilience() {
    const N: usize = 200;
    let params = Params::new(N, 5).with_failures(10, 0, Duration::from_secs(5));
    let mut sim = Sim::new(params);
    sim.drain_queue_to_quiescence();
    sim.check_invariants();
    let report = sim.report();
    println!("{report}");

    assert_eq!(report.crashed, 10, "crash count knob had no effect");
    // Every gap must be explained by a recipient having crashed — anything
    // left over would mean the tree failed to route around a dead node.
    assert_eq!(
        report.unexplained_missing, 0,
        "some non-delivery isn't explained by a crash — real protocol miss"
    );
}

/// Some nodes go down and later rejoin with a fresh `PlumtreeState` (a real
/// process restart), again with a randomly-delayed `MemberDown`/`MemberUp`
/// per observer. Plumtree never replays already-delivered messages to a
/// late (re)joiner, so we can't assert full history delivery — instead, we
/// wait for every flap + detection delay to resolve, then fire a fresh
/// broadcast wave and require it reach the *entire* cluster, including the
/// nodes that just came back.
#[test]
fn sim_small_flap_recovery() {
    const N: usize = 200;
    let flap_downtime = Duration::from_secs(5);
    let params = Params::new(N, 5)
        .with_failures(0, 10, flap_downtime)
        .with_detection_delay(Duration::from_secs(2), Duration::from_secs(4));
    let broadcast_window = params.broadcast_window;
    let detection_delay_max = params.detection_delay_max;
    let mut sim = Sim::new(params);

    // Worst case: a flap is scheduled right at the end of the window, then
    // takes `flap_downtime` to come back, then up to `detection_delay_max`
    // for every observer to notice. Double that last term for margin.
    let settle_at = broadcast_window.as_micros() as u64
        + flap_downtime.as_micros() as u64
        + detection_delay_max.as_micros() as u64 * 2;

    // Post-recovery wave, using `seq: 1` (distinct from the `seq: 0` wave
    // `Sim::new` already scheduled from the default `msgs_per_node: 1`) so
    // these message ids don't collide with — and get treated as duplicates
    // of — the earlier ones.
    for node in 0..N as NId {
        sim.push(settle_at, Event::Originate { node, seq: 1 });
    }

    sim.drain_queue_to_quiescence();
    sim.check_invariants();
    let report = sim.report();
    println!("{report}");

    assert_eq!(report.flapped, 10, "flap count knob had no effect");
    // Every gap must be explained by a recipient not having (re)joined yet
    // when a message was sent — in particular, every node is alive for the
    // post-recovery wave above, so any gap there would show up here as a
    // real protocol miss.
    assert_eq!(
        report.unexplained_missing, 0,
        "some non-delivery isn't explained by a node still being down — real protocol miss"
    );
}

/// Head-to-head: plumtree vs the gossip baseline under an *identical* crash
/// timeline (same crash count, seed, detection-delay range, broadcast
/// window) — the no-failure comparison is `sim_gossip_vs_plumtree_2k`; this
/// is its churn counterpart. Both must fully explain their delivery gap as
/// crash-caused, not as an unexplained protocol miss.
#[test]
#[ignore = "slow; run in release mode"]
fn sim_gossip_vs_plumtree_failures() {
    const N: usize = 500;
    const CRASH_COUNT: usize = 20;
    let flap_downtime = Duration::from_secs(4);
    let detection_delay = (Duration::from_secs(2), Duration::from_secs(5));

    let mut plum = Sim::new(
        Params::new(N, 5)
            .with_failures(CRASH_COUNT, 0, flap_downtime)
            .with_detection_delay(detection_delay.0, detection_delay.1),
    );
    plum.drain_queue_to_quiescence();
    plum.check_invariants();
    let plum_report = plum.report();

    let gossip_report = GossipSim::new(
        GossipParams::new(N)
            .with_failures(CRASH_COUNT, 0, flap_downtime)
            .with_detection_delay(detection_delay.0, detection_delay.1),
    )
    .run_staggered();

    println!("gossip under failures:\n{gossip_report}");
    println!("plumtree under failures:\n{plum_report}");

    let row = |name: &str, g: String, p: String| println!("  {name:<24}{g:>16}{p:>16}");
    println!("\n=== gossip vs plumtree under failures (n={N}, crash={CRASH_COUNT}) ===");
    row("metric", "gossip".into(), "plumtree".into());
    row(
        "delivery %",
        format!("{:.3}", gossip_report.delivery_pct()),
        format!("{:.3}", plum_report.delivery_pct()),
    );
    row(
        "delivered/expected",
        format!("{}/{}", gossip_report.delivered, gossip_report.expected),
        format!("{}/{}", plum_report.delivered, plum_report.expected),
    );
    row(
        "adjusted delivery %",
        format!("{:.3}", gossip_report.adjusted_delivery_pct()),
        format!("{:.3}", plum_report.adjusted_delivery_pct()),
    );
    row(
        "duplicates",
        gossip_report.duplicates.to_string(),
        plum_report.duplicates.to_string(),
    );
    row(
        "dups/msg",
        format!("{:.2}", gossip_report.dups_per_msg_first),
        format!("{:.2}", plum_report.dups_per_msg_last),
    );
    row(
        "latency p50/p99 ms",
        format!("{}/{}", gossip_report.lat_p50_ms, gossip_report.lat_p99_ms),
        format!("{}/{}", plum_report.lat_p50_ms, plum_report.lat_p99_ms),
    );
    row(
        "missed_due_to_failure",
        gossip_report.missed_due_to_failure.to_string(),
        plum_report.missed_due_to_failure.to_string(),
    );
    row(
        "unexplained_missing",
        gossip_report.unexplained_missing.to_string(),
        plum_report.unexplained_missing.to_string(),
    );

    assert_eq!(gossip_report.crashed, CRASH_COUNT);
    assert_eq!(plum_report.crashed, CRASH_COUNT);
    assert_eq!(
        gossip_report.unexplained_missing, 0,
        "gossip has non-delivery not explained by the injected crashes"
    );
    assert_eq!(
        plum_report.unexplained_missing, 0,
        "plumtree has non-delivery not explained by the injected crashes"
    );
}
