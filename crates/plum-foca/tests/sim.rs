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
use std::collections::{BinaryHeap, HashMap};
use std::fmt;
use std::time::Duration;

use indexmap::IndexMap;
use plum_foca::{
    Config, Notification, Payload, PlumPrio, PlumtreeMsg, PlumtreeState, Round, RttInfo, Runtime,
    SeenStore, Timer,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

type NId = u32;
type MId = u64;

#[derive(Debug, Clone)]
struct SimPayload {
    origin: NId,
    seq: u32,
}

impl SimPayload {
    fn id(&self) -> MId {
        ((self.origin as u64) << 32) | self.seq as u64
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

/// Bounded seen-store, equivalent in behavior to the in-memory part of
/// corro-agent's `ChangeSeenStore`: tracks how many times each id was seen.
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
}

struct Region {
    #[allow(dead_code)]
    name: &'static str,
    lat: f64,
    lon: f64,
    weight: u32,
}

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
struct Outbox {
    sent: Vec<(NId, PlumtreeMsg<MId, SimPayload, NId>)>,
    scheduled: Vec<(Timer<MId, NId>, Duration)>,
    delivered: Vec<SimPayload>,
    notifications: Vec<Notification<MId, NId>>,
}

impl Runtime<MId, SimPayload, NId> for Outbox {
    fn send(&mut self, to: NId, msg: PlumtreeMsg<MId, SimPayload, NId>, _prio: PlumPrio) {
        self.sent.push((to, msg));
    }

    fn send_all(&mut self, peers: Vec<NId>, msg: PlumtreeMsg<MId, SimPayload, NId>, _prio: PlumPrio) {
        for peer in peers {
            self.sent.push((peer, msg.clone()));
        }
    }

    fn deliver(&mut self, payload: SimPayload) {
        self.delivered.push(payload);
    }

    fn schedule(&mut self, timer: Timer<MId, NId>, after: Duration) {
        self.scheduled.push((timer, after));
    }

    fn notify(&mut self, notification: Notification<MId, NId>) {
        self.notifications.push(notification);
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
    Tick { node: NId },
    Originate { node: NId, seq: u32 },
}

struct Scheduled {
    at: u64,
    seq: u64,
    ev: Event,
}

impl PartialEq for Scheduled {
    fn eq(&self, other: &Self) -> bool {
        (self.at, self.seq) == (other.at, other.seq)
    }
}
impl Eq for Scheduled {}
impl PartialOrd for Scheduled {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Scheduled {
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
    seed: u64,
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
            seed: 42,
        }
    }

    /// Production config from `spawn_plumtree_loop`, with `num_eager`
    /// parametrized and the seen-store sized so it never evicts mid-run.
    fn config(&self) -> Config {
        Config {
            ihave_timeout: Duration::from_millis(200),
            optimization_threshold: Some(5),
            num_eager: self.num_eager,
            min_lazy: 15,
            max_lazy: 30,
            prune_threshold: 5,
            max_received_entries: (self.n as u32 * self.msgs_per_node) as usize + 1,
            max_cached_payloads: 4096,
        }
    }

    fn total_msgs(&self) -> u64 {
        self.n as u64 * self.msgs_per_node as u64
    }
}

const TICK_INTERVAL: Duration = Duration::from_millis(200);
/// A gossip send crossing a link slower than this counts as long-haul.
const LONGHAUL_RTT_MS: u64 = 150;

struct MsgStat {
    sent_at: u64,
    gossip_sends: u64,
    deliveries: u32,
    last_delivery_at: u64,
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
    /// One entry per delivery: number of gossip hops from the origin.
    hops: Vec<u32>,
    /// One entry per delivery: broadcast-to-delivery latency in µs.
    latencies_us: Vec<u64>,
}

struct Sim {
    params: Params,
    now: u64, // virtual time, µs
    next_seq: u64,
    queue: BinaryHeap<Scheduled>,
    states: Vec<PlumtreeState<MId, SimPayload, NId, SimSeenStore>>,
    region_of: Vec<u8>,
    rtt_ms: Vec<Vec<u64>>,
    rng: StdRng,
    outbox: Outbox,
    tick_pending: Vec<bool>,
    /// Round of the gossip message currently being handled, for hop
    /// attribution of the deliveries it produces.
    gossip_round: Option<Round>,
    msgs: HashMap<MId, MsgStat>,
    stats: Stats,
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
        let mut outbox = Outbox::default();
        for i in 0..n {
            let peers: Vec<(NId, RttInfo)> = (0..n)
                .filter(|&j| j != i)
                .map(|j| {
                    let rtt = rtt_ms[region_of[i] as usize][region_of[j] as usize];
                    let info = if params.rtt_aware {
                        RttInfo {
                            ring: ring_from_rtt_ms(rtt),
                            rtt_ms: rtt,
                        }
                    } else {
                        RttInfo::default()
                    };
                    (j as NId, info)
                })
                .collect();
            states[i].add_peers_bulk_with_rtt(peers, &mut outbox);
        }
        // bootstrap produces no traffic
        assert!(outbox.sent.is_empty() && outbox.scheduled.is_empty());

        let mut sim = Self {
            now: 0,
            next_seq: 0,
            queue: BinaryHeap::new(),
            states,
            region_of,
            rtt_ms,
            rng,
            outbox,
            tick_pending: vec![false; n],
            gossip_round: None,
            msgs: HashMap::new(),
            stats: Stats::default(),
            params,
        };

        // Every node broadcasts, staggered over the window.
        let window_us = sim.params.broadcast_window.as_micros() as u64;
        for node in 0..n as NId {
            for seq in 0..sim.params.msgs_per_node {
                let at = sim.rng.random_range(0..window_us.max(1));
                sim.push(at, Event::Originate { node, seq });
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

    /// Run until the event queue drains (the protocol quiesces).
    fn run(mut self) -> Report {
        while let Some(Scheduled { at, ev, .. }) = self.queue.pop() {
            debug_assert!(at >= self.now);
            self.now = at;
            match ev {
                Event::Originate { node, seq } => {
                    let payload = SimPayload { origin: node, seq };
                    let id = payload.id();
                    self.msgs.insert(
                        id,
                        MsgStat {
                            sent_at: self.now,
                            gossip_sends: 0,
                            deliveries: 0,
                            last_delivery_at: 0,
                        },
                    );
                    self.states[node as usize].broadcast(id, payload, &mut self.outbox);
                    self.drain(node);
                }
                Event::Recv { to, msg } => {
                    let state = &mut self.states[to as usize];
                    match msg {
                        PlumtreeMsg::Gossip(g) => {
                            self.gossip_round = Some(g.round);
                            state.handle_gossip(g, &mut self.outbox);
                        }
                        PlumtreeMsg::IHave(ih) => state.handle_ihave(ih, &mut self.outbox),
                        PlumtreeMsg::Graft(g) => state.handle_graft(g, &mut self.outbox),
                        PlumtreeMsg::Prune(p) => state.handle_prune(p, &mut self.outbox),
                    }
                    self.drain(to);
                    self.gossip_round = None;
                }
                Event::TimerFired { node, timer } => {
                    self.states[node as usize].timer_fired(timer, &mut self.outbox);
                    self.drain(node);
                }
                Event::Tick { node } => {
                    self.tick_pending[node as usize] = false;
                    let state = &mut self.states[node as usize];
                    state.cache_evict_if_needed();
                    state.tick(&mut self.outbox);
                    self.drain(node);
                }
            }
        }
        self.check_invariants();
        self.report()
    }

    /// Apply everything the node just asked the runtime to do.
    fn drain(&mut self, from: NId) {
        for payload in std::mem::take(&mut self.outbox.delivered) {
            let stat = self.msgs.get_mut(&payload.id()).expect("delivered unknown msg");
            stat.deliveries += 1;
            stat.last_delivery_at = self.now;
            self.stats.latencies_us.push(self.now - stat.sent_at);
            // round 0 = received directly from the origin = 1 hop
            let round = self.gossip_round.expect("delivery outside handle_gossip");
            self.stats.hops.push(round + 1);
        }

        for notification in std::mem::take(&mut self.outbox.notifications) {
            match notification {
                Notification::DuplicateMessage(_) => self.stats.duplicates += 1,
                Notification::MessageMissing(count) => self.stats.missing += count as u64,
                Notification::PayloadNotCached(_) => self.stats.not_cached += 1,
                Notification::PeerMovedToEager(_) => self.stats.promotions += 1,
                Notification::PeerMovedToLazy(_) => self.stats.demotions += 1,
            }
        }

        for (timer, after) in std::mem::take(&mut self.outbox.scheduled) {
            let at = self.now + after.as_micros() as u64;
            self.push(at, Event::TimerFired { node: from, timer });
        }

        for (to, msg) in std::mem::take(&mut self.outbox.sent) {
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
        let max_lazy = self.params.config().max_lazy;
        for state in &self.states {
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
            assert!(lazy.len() <= max_lazy, "lazy set exceeds max_lazy");
            assert!(state.lazy_queue().is_empty(), "lazy queue not drained");
            assert_eq!(known.len(), self.params.n - 1, "full membership lost");
        }
    }

    fn report(&self) -> Report {
        let expected = self.params.total_msgs() * (self.params.n as u64 - 1);
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

        // RMR per message, in broadcast order, to show tree convergence
        let mut by_time: Vec<&MsgStat> = self.msgs.values().collect();
        by_time.sort_unstable_by_key(|m| m.sent_at);
        let rmr = |msgs: &[&MsgStat]| -> f64 {
            if msgs.is_empty() {
                return 0.0;
            }
            let sends: u64 = msgs.iter().map(|m| m.gossip_sends).sum();
            sends as f64 / (msgs.len() as f64 * (self.params.n - 1) as f64) - 1.0
        };
        let decile = (by_time.len() / 10).max(1);
        let rmr_first = rmr(&by_time[..decile]);
        let rmr_last = rmr(&by_time[by_time.len() - decile..]);

        let avg_eager = self
            .states
            .iter()
            .map(|s| s.eager_peers().len())
            .sum::<usize>() as f64
            / self.params.n as f64;

        Report {
            label: format!(
                "n={} num_eager={} rtt_aware={} loss={:.1}% seed={}",
                self.params.n,
                self.params.num_eager,
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
            rmr_first,
            rmr_last,
            sent_gossip: self.stats.sent_gossip,
            sent_ihave: self.stats.sent_ihave,
            sent_graft: self.stats.sent_graft,
            sent_prune: self.stats.sent_prune,
            longhaul_frac: self.stats.longhaul_gossip as f64 / self.stats.sent_gossip.max(1) as f64,
            duplicates: self.stats.duplicates,
            dropped: self.stats.dropped,
            not_cached: self.stats.not_cached,
            avg_eager,
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
    rmr_first: f64,
    rmr_last: f64,
    sent_gossip: u64,
    sent_ihave: u64,
    sent_graft: u64,
    sent_prune: u64,
    longhaul_frac: f64,
    duplicates: u64,
    dropped: u64,
    not_cached: u64,
    avg_eager: f64,
}

impl Report {
    fn delivery_pct(&self) -> f64 {
        self.delivered as f64 / self.expected as f64 * 100.0
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
            "  redundancy rmr first-decile={:.2} last-decile={:.2}  duplicates={}",
            self.rmr_first, self.rmr_last, self.duplicates
        )?;
        writeln!(
            f,
            "  traffic    gossip={} ihave={} graft={} prune={} dropped={}  longhaul-gossip={:.1}%",
            self.sent_gossip,
            self.sent_ihave,
            self.sent_graft,
            self.sent_prune,
            self.dropped,
            self.longhaul_frac * 100.0,
        )?;
        writeln!(f, "  topology   avg_eager={:.1}", self.avg_eager)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

/// 2k nodes: topology-aware vs topology-blind eager selection.
#[test]
#[ignore = "slow; run in release mode"]
fn sim_2k_rtt_awareness() {
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
