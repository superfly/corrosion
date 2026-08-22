use super::*;
use stateright::{Checker, Model, Property};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::LazyLock;
use std::time::Instant;

type NodeId = u8;

const NODE_COUNT: usize = 3;
const ORIGIN: NodeId = 0;
const INITIAL_EAGER_RECEIVER: NodeId = 1;
const MESSAGE: MsgId = MsgId(ORIGIN);

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct MsgId(NodeId);

impl MessageId for MsgId {
    type NodeId = NodeId;

    fn origin(&self) -> Self::NodeId {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TestPayload(MsgId);

impl Payload for TestPayload {
    type MessageId = MsgId;
    type NodeId = NodeId;

    fn message_id(&self) -> Self::MessageId {
        self.0
    }

    fn origin(&self) -> Self::NodeId {
        self.0.origin()
    }
}

#[derive(Debug, Default)]
struct ModelSeen {
    entries: BTreeMap<MsgId, u32>,
}

impl SeenStore<MsgId> for ModelSeen {
    fn evict_if_needed(&mut self) {}

    fn contains(&self, id: &MsgId) -> bool {
        self.entries.contains_key(id)
    }

    fn observe(&mut self, id: MsgId, _round: Round) -> Option<u32> {
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

type ProtocolState = PlumtreeState<MsgId, TestPayload, NodeId, ModelSeen>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Packet {
    to: NodeId,
    msg: WireMsg,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum WireMsg {
    Gossip {
        round: Round,
        sender: NodeId,
        payload: TestPayload,
    },
    IHave {
        sender: NodeId,
        digests: Vec<(MsgId, Round)>,
    },
    Graft {
        sender: NodeId,
        send: bool,
        requests: Vec<(MsgId, Round)>,
    },
    Prune {
        sender: NodeId,
        triggered_by: Option<MsgId>,
    },
}

impl From<PlumtreeMsg<MsgId, TestPayload, NodeId>> for WireMsg {
    fn from(msg: PlumtreeMsg<MsgId, TestPayload, NodeId>) -> Self {
        match msg {
            PlumtreeMsg::Gossip(msg) => Self::Gossip {
                round: msg.round,
                sender: msg.sender,
                payload: msg.payload,
            },
            PlumtreeMsg::IHave(msg) => Self::IHave {
                sender: msg.sender,
                digests: msg
                    .digests
                    .into_iter()
                    .map(|digest| (digest.id, digest.round))
                    .collect(),
            },
            PlumtreeMsg::Graft(msg) => Self::Graft {
                sender: msg.sender,
                send: msg.send,
                requests: msg
                    .requests
                    .into_iter()
                    .map(|request| (request.id, request.round))
                    .collect(),
            },
            PlumtreeMsg::Prune(msg) => Self::Prune {
                sender: msg.sender,
                triggered_by: msg.triggered_by,
            },
        }
    }
}

impl WireMsg {
    fn into_protocol(self) -> PlumtreeMsg<MsgId, TestPayload, NodeId> {
        match self {
            Self::Gossip {
                round,
                sender,
                payload,
            } => PlumtreeMsg::Gossip(GossipMsg {
                round,
                sender,
                payload,
            }),
            Self::IHave { sender, digests } => PlumtreeMsg::IHave(IHaveMsg {
                sender,
                digests: digests
                    .into_iter()
                    .map(|(id, round)| IHaveDigest { id, round })
                    .collect(),
            }),
            Self::Graft {
                sender,
                send,
                requests,
            } => PlumtreeMsg::Graft(GraftMsg {
                sender,
                send,
                requests: requests
                    .into_iter()
                    .map(|(id, round)| GraftRequest { id, round })
                    .collect(),
            }),
            Self::Prune {
                sender,
                triggered_by,
            } => PlumtreeMsg::Prune(PruneMsg {
                sender,
                triggered_by,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct TimerEvent {
    node: NodeId,
    ids: Vec<MsgId>,
    retries: u32,
    senders: Vec<NodeId>,
}

impl TimerEvent {
    fn from_timer(node: NodeId, timer: Timer<MsgId, NodeId>) -> Self {
        match timer {
            Timer::IHaveTimeoutBatch {
                ids,
                retries,
                senders,
            } => Self {
                node,
                ids,
                retries,
                senders,
            },
        }
    }

    fn into_protocol(self) -> Timer<MsgId, NodeId> {
        Timer::IHaveTimeoutBatch {
            ids: self.ids,
            retries: self.retries,
            senders: self.senders,
        }
    }
}

#[derive(Default)]
struct CaptureRuntime {
    outbox: Vec<Packet>,
    timers: Vec<Timer<MsgId, NodeId>>,
    delivered: Vec<TestPayload>,
}

impl Runtime<MsgId, TestPayload, NodeId> for CaptureRuntime {
    fn send(&mut self, to: NodeId, msg: PlumtreeMsg<MsgId, TestPayload, NodeId>, _prio: PlumPrio) {
        self.outbox.push(Packet {
            to,
            msg: msg.into(),
        });
    }

    fn send_all(
        &mut self,
        peers: Vec<NodeId>,
        msg: PlumtreeMsg<MsgId, TestPayload, NodeId>,
        prio: PlumPrio,
    ) {
        for peer in peers {
            self.send(peer, msg.clone(), prio);
        }
    }

    fn deliver(&mut self, payload: TestPayload) {
        self.delivered.push(payload);
    }

    fn schedule(&mut self, timer: Timer<MsgId, NodeId>, _after: std::time::Duration) {
        self.timers.push(timer);
    }

    fn notify(&mut self, _notification: Notification<'_, MsgId, NodeId>) {}

    fn now(&self) -> Instant {
        *EPOCH
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NodeSnapshot {
    eager: Vec<NodeId>,
    lazy: Vec<NodeId>,
    lazy_queue: Vec<(MsgId, Round)>,
    missing: Vec<(MsgId, NodeId, Round)>,
    seen: Vec<(MsgId, u32)>,
    cache: Vec<(MsgId, TestPayload, Round)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultScenario {
    None,
    AnySinglePacketLoss,
    InitialEagerGossipLoss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FaultProgress {
    Disabled,
    Armed,
    PacketDropped,
    InitialGossipDropped,
    GraftRequested,
    Recovered,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Snapshot {
    nodes: Vec<NodeSnapshot>,
    pending: Vec<Packet>,
    timers: Vec<TimerEvent>,
    delivered: [u8; NODE_COUNT],
    fault_progress: FaultProgress,
}

impl Snapshot {
    fn quiescent(&self) -> bool {
        self.pending.is_empty()
            && self.timers.is_empty()
            && self.nodes.iter().all(|node| node.lazy_queue.is_empty())
    }

    fn delivered_to_every_receiver(&self) -> bool {
        self.delivered
            .iter()
            .enumerate()
            .all(|(node, count)| node == ORIGIN as usize || *count == 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Action {
    Deliver(Packet),
    Drop(Packet),
    Fire(TimerEvent),
    Tick(NodeId),
}

#[derive(Debug, Clone)]
struct ModelState {
    trace: Vec<Action>,
    snapshot: Snapshot,
}

// This model has one message and fixed membership/fanout. Prune throttling is
// disabled, and every random choice has at most one candidate.
impl PartialEq for ModelState {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot == other.snapshot
    }
}

impl Eq for ModelState {}

impl Hash for ModelState {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.snapshot.hash(state);
    }
}

struct System {
    nodes: Vec<ProtocolState>,
    pending: Vec<Packet>,
    timers: Vec<TimerEvent>,
    delivered: [u8; NODE_COUNT],
    fault_progress: FaultProgress,
}

impl System {
    fn new(fault_scenario: FaultScenario) -> Self {
        let mut nodes = Vec::with_capacity(NODE_COUNT);

        for node in 0..NODE_COUNT as NodeId {
            let mut state =
                ProtocolState::new_with_store_seeded(node, model_config(), ModelSeen::default(), 0);
            let mut runtime = CaptureRuntime::default();

            // Incremental joins give each node one eager and one lazy peer.
            for peer in 0..NODE_COUNT as NodeId {
                if peer != node {
                    state.peer_up(peer, None, &mut runtime);
                }
            }

            assert!(runtime.outbox.is_empty());
            assert!(runtime.timers.is_empty());
            assert!(runtime.delivered.is_empty());

            nodes.push(state);
        }

        assert!(
            nodes
                .iter()
                .all(|state| state.ring_locked_peers().is_empty())
        );

        let origin = &nodes[ORIGIN as usize];
        assert_eq!(origin.eager_peers().len(), 1);
        assert!(origin.eager_peers().contains(&1));
        assert_eq!(origin.lazy_peers().len(), 1);
        assert!(origin.lazy_peers().contains(&2));

        let mut system = Self {
            nodes,
            pending: Vec::new(),
            timers: Vec::new(),
            delivered: [0; NODE_COUNT],
            fault_progress: match fault_scenario {
                FaultScenario::None | FaultScenario::InitialEagerGossipLoss => {
                    FaultProgress::Disabled
                }
                FaultScenario::AnySinglePacketLoss => FaultProgress::Armed,
            },
        };

        let mut runtime = CaptureRuntime::default();
        system.nodes[ORIGIN as usize].broadcast(MESSAGE, TestPayload(MESSAGE), &mut runtime);
        system.drain(ORIGIN, runtime);

        if fault_scenario == FaultScenario::InitialEagerGossipLoss {
            let matching = system
                .pending
                .iter()
                .filter(|packet| is_initial_eager_gossip(packet))
                .count();
            assert_eq!(matching, 1);

            let pos = system
                .pending
                .iter()
                .position(is_initial_eager_gossip)
                .expect("initial eager Gossip is pending");
            system.pending.remove(pos);
            system.fault_progress = FaultProgress::InitialGossipDropped;
        }

        system
    }

    fn apply(&mut self, action: &Action) -> bool {
        match action {
            Action::Deliver(packet) => {
                let Some(pos) = self.pending.iter().position(|item| item == packet) else {
                    return false;
                };
                let packet = self.pending.remove(pos);
                let node = packet.to as usize;
                let mut runtime = CaptureRuntime::default();

                match packet.msg.into_protocol() {
                    PlumtreeMsg::Gossip(msg) => {
                        self.nodes[node].handle_gossip(msg, &mut runtime);
                    }
                    PlumtreeMsg::IHave(msg) => {
                        let fallback_count = self.nodes[node]
                            .eager_peers()
                            .iter()
                            .filter(|peer| **peer != msg.sender)
                            .count();
                        assert!(
                            fallback_count <= 1,
                            "model topology introduced a random IHave fallback"
                        );
                        self.nodes[node].handle_ihave(msg, &mut runtime);
                    }
                    PlumtreeMsg::Graft(msg) => {
                        self.nodes[node].handle_graft(msg, &mut runtime);
                    }
                    PlumtreeMsg::Prune(msg) => {
                        self.nodes[node].handle_prune(msg, &mut runtime);
                    }
                }

                self.drain(packet.to, runtime);
                true
            }
            Action::Drop(packet) => {
                if self.fault_progress != FaultProgress::Armed {
                    return false;
                }

                let Some(pos) = self.pending.iter().position(|item| item == packet) else {
                    return false;
                };

                self.pending.remove(pos);
                self.fault_progress = FaultProgress::PacketDropped;
                true
            }
            Action::Fire(event) => {
                let Some(pos) = self.timers.iter().position(|item| item == event) else {
                    return false;
                };
                let event = self.timers.remove(pos);
                let node = event.node;
                let timer = event.into_protocol();
                let mut runtime = CaptureRuntime::default();

                self.nodes[node as usize].timer_fired(timer, &mut runtime);
                self.drain(node, runtime);
                true
            }
            Action::Tick(node) => {
                let node_index = *node as usize;
                if node_index >= self.nodes.len() || self.nodes[node_index].lazy_queue().is_empty()
                {
                    return false;
                }

                let mut runtime = CaptureRuntime::default();
                self.nodes[node_index].tick(&mut runtime);
                self.drain(*node, runtime);
                true
            }
        }
    }

    fn drain(&mut self, node: NodeId, runtime: CaptureRuntime) {
        if self.fault_progress == FaultProgress::InitialGossipDropped
            && runtime.outbox.iter().any(|packet| {
                matches!(
                    &packet.msg,
                    WireMsg::Graft {
                        send: true,
                        requests,
                        ..
                    } if requests.iter().any(|(id, _)| *id == MESSAGE)
                )
            })
        {
            self.fault_progress = FaultProgress::GraftRequested;
        }

        self.pending.extend(runtime.outbox);

        self.timers.extend(
            runtime
                .timers
                .into_iter()
                .map(|timer| TimerEvent::from_timer(node, timer)),
        );

        for payload in runtime.delivered {
            assert_eq!(payload.0, MESSAGE);
            self.delivered[node as usize] += 1;
        }

        if self.fault_progress == FaultProgress::GraftRequested
            && self
                .delivered
                .iter()
                .enumerate()
                .all(|(node, count)| node == ORIGIN as usize || *count == 1)
        {
            self.fault_progress = FaultProgress::Recovered;
        }

        self.pending.sort();
        self.timers.sort();
    }

    fn snapshot(&self) -> Snapshot {
        Snapshot {
            nodes: self.nodes.iter().map(snapshot_node).collect(),
            pending: self.pending.clone(),
            timers: self.timers.clone(),
            delivered: self.delivered,
            fault_progress: self.fault_progress,
        }
    }
}

fn snapshot_node(state: &ProtocolState) -> NodeSnapshot {
    let mut eager = state.eager_peers.iter().copied().collect::<Vec<_>>();
    eager.sort_unstable();

    let mut lazy = state.lazy_peers.iter().copied().collect::<Vec<_>>();
    lazy.sort_unstable();

    let lazy_queue = state
        .lazy_queue
        .iter()
        .map(|digest| (digest.id, digest.round))
        .collect();

    let mut missing = state
        .missing
        .iter()
        .map(|(id, entry)| (*id, entry.ihave_sender, entry.round))
        .collect::<Vec<_>>();
    missing.sort_unstable();

    let seen = state
        .seen
        .entries
        .iter()
        .map(|(id, count)| (*id, *count))
        .collect();

    let mut cache = state
        .cache
        .entries
        .iter()
        .map(|(id, (payload, round))| (*id, *payload, *round))
        .collect::<Vec<_>>();
    cache.sort_unstable();

    NodeSnapshot {
        eager,
        lazy,
        lazy_queue,
        missing,
        seen,
        cache,
    }
}

fn model_config() -> Config {
    Config {
        num_eager: Some(1),
        min_lazy: Some(1),
        max_lazy: Some(2),
        optimization_threshold: None,
        max_received_entries: 8,
        max_cached_payloads: 8,
        prune_throttle: None,
        ..Config::default()
    }
}

fn is_initial_eager_gossip(packet: &Packet) -> bool {
    packet.to == INITIAL_EAGER_RECEIVER
        && matches!(
            &packet.msg,
            WireMsg::Gossip {
                sender,
                payload,
                ..
            } if *sender == ORIGIN && payload.0 == MESSAGE
        )
}

fn replay(trace: &[Action], fault_scenario: FaultScenario) -> Option<System> {
    let mut system = System::new(fault_scenario);

    for action in trace {
        if !system.apply(action) {
            return None;
        }
    }

    Some(system)
}

struct PlumtreeModel {
    fault_scenario: FaultScenario,
}

impl Model for PlumtreeModel {
    type State = ModelState;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        let system = System::new(self.fault_scenario);
        vec![ModelState {
            trace: Vec::new(),
            snapshot: system.snapshot(),
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let mut pending = state.snapshot.pending.clone();
        pending.dedup();
        actions.extend(pending.iter().cloned().map(Action::Deliver));

        if state.snapshot.fault_progress == FaultProgress::Armed {
            actions.extend(pending.into_iter().map(Action::Drop));
        }

        let mut timers = state.snapshot.timers.clone();
        timers.dedup();
        actions.extend(timers.into_iter().map(Action::Fire));

        for (node, snapshot) in state.snapshot.nodes.iter().enumerate() {
            if !snapshot.lazy_queue.is_empty() {
                actions.push(Action::Tick(node as NodeId));
            }
        }
    }

    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut system = replay(&state.trace, self.fault_scenario)?;

        if !system.apply(&action) {
            return None;
        }

        let mut trace = state.trace.clone();
        trace.push(action);

        Some(ModelState {
            trace,
            snapshot: system.snapshot(),
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always(
                "peer sets are disjoint",
                |_: &PlumtreeModel, state: &ModelState| {
                    state
                        .snapshot
                        .nodes
                        .iter()
                        .all(|node| node.eager.iter().all(|peer| !node.lazy.contains(peer)))
                },
            ),
            Property::always(
                "payload is delivered at most once",
                |_: &PlumtreeModel, state: &ModelState| {
                    state.snapshot.delivered.iter().all(|count| *count <= 1)
                },
            ),
            Property::always(
                "origin does not redeliver its payload",
                |_: &PlumtreeModel, state: &ModelState| {
                    state.snapshot.delivered[ORIGIN as usize] == 0
                },
            ),
        ];

        match self.fault_scenario {
            FaultScenario::None => {
                properties.push(Property::always(
                    "lossless quiescence delivers to every receiver",
                    |_: &PlumtreeModel, state: &ModelState| {
                        !state.snapshot.quiescent() || state.snapshot.delivered_to_every_receiver()
                    },
                ));
                properties.push(Property::sometimes(
                    "lossless broadcast reaches delivered quiescence",
                    |_: &PlumtreeModel, state: &ModelState| {
                        state.snapshot.quiescent() && state.snapshot.delivered_to_every_receiver()
                    },
                ));
            }
            FaultScenario::AnySinglePacketLoss => {
                properties.push(Property::sometimes(
                    "single packet loss is explored",
                    |_: &PlumtreeModel, state: &ModelState| {
                        state.snapshot.fault_progress == FaultProgress::PacketDropped
                    },
                ));
            }
            FaultScenario::InitialEagerGossipLoss => {
                properties.push(Property::always(
                    "initial eager Gossip loss is repaired at quiescence",
                    |_: &PlumtreeModel, state: &ModelState| {
                        !state.snapshot.quiescent()
                            || state.snapshot.fault_progress == FaultProgress::Recovered
                    },
                ));
                properties.push(Property::sometimes(
                    "initial eager Gossip loss reaches repaired quiescence",
                    |_: &PlumtreeModel, state: &ModelState| {
                        state.snapshot.quiescent()
                            && state.snapshot.fault_progress == FaultProgress::Recovered
                    },
                ));
            }
        }

        properties
    }
}

fn check_model(fault_scenario: FaultScenario) {
    let checker = PlumtreeModel { fault_scenario }
        .checker()
        .threads(1)
        .spawn_dfs()
        .join();

    checker.assert_properties();
}

#[test]
fn model_check_three_node_broadcast() {
    check_model(FaultScenario::None);
}

#[test]
fn model_check_three_node_any_single_packet_loss_safety() {
    check_model(FaultScenario::AnySinglePacketLoss);
}

#[test]
fn model_check_three_node_initial_eager_gossip_loss_repair() {
    check_model(FaultScenario::InitialEagerGossipLoss);
}
