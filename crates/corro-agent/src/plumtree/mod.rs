use std::{cmp, collections::HashMap, net::SocketAddr, ops::RangeInclusive, time::Duration};

use bytes::{BufMut, BytesMut};
use corro_types::{
    actor::ActorId,
    agent::Agent,
    base::{CrsqlDbVersion, CrsqlSeq},
    broadcast::{
        ChangeId, ChangeSource, ChangeV1, ChangesetId, PlumtreeInput, PlumtreeMsg, PlumtreeMsgV1,
        PlumtreeUpdates, UniPayload, UniPayloadV1,
    },
    channel::{CorroReceiver, CorroSender},
};
use indexmap::IndexMap;
use metrics::counter;
use plum_foca::{Payload, PlumtreeState, PlumtreeStats, SeenStore, Timer};
use rangemap::RangeInclusiveSet;
use speedy::Writable;
use strum::EnumDiscriminants;
use tokio::{sync::mpsc, time::interval};
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use tracing::{debug, error, info, trace, warn};
use tripwire::Tripwire;

use crate::{broadcast::TimerSpawner, transport::TransportExt};

#[derive(Debug)]
struct SeenEntry {
    seqs: Option<RangeInclusiveSet<CrsqlSeq>>,
    last_seq: Option<CrsqlSeq>,
    round: plum_foca::Round,
    duplicate_count: u32,
}

#[derive(Debug)]
struct ChangeSeenStore {
    entries: IndexMap<(ActorId, CrsqlDbVersion), SeenEntry>,
    max_entries: usize,
}

impl ChangeSeenStore {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: IndexMap::new(),
            max_entries,
        }
    }

    fn evict_if_needed(&mut self) {
        if self.entries.len() > self.max_entries {
            self.entries.drain(0..self.entries.len() - self.max_entries);
        }
    }
}

impl SeenStore<ChangeId> for ChangeSeenStore {
    fn contains(&self, id: &ChangeId) -> bool {
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

/// Implements `plum_foca::Runtime` for Corrosion, bridging the generic protocol
/// to Corrosion's transport, change processing, and timer infrastructure.
struct CorrosionPlumtreeRuntime<T: TransportExt> {
    agent: Agent,
    transport: T,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    timer_spawner: TimerSpawner<plum_foca::Timer<ChangeId>>,
    bcast_codec: LengthDelimitedCodec,
    ser_buf: BytesMut,
    actors: HashMap<ActorId, SocketAddr>,
}

impl<T: TransportExt> CorrosionPlumtreeRuntime<T> {
    fn update_actor(&mut self, actor_id: ActorId, addr: SocketAddr) {
        self.actors.insert(actor_id, addr);
    }

    fn remove_actor(&mut self, actor_id: ActorId) {
        self.actors.remove(&actor_id);
    }
}

impl<T: TransportExt + Clone + Send + 'static> plum_foca::Runtime<ChangeId, ChangeV1, ActorId>
    for CorrosionPlumtreeRuntime<T>
{
    fn send(&mut self, to: ActorId, msg: PlumtreeMsgV1) {
        let addr = match self.actors.get(&to) {
            Some(a) => *a,
            None => {
                debug!("plumtree: no address for peer {to}, dropping message");
                return;
            }
        };

        let payload = UniPayload::V1 {
            data: UniPayloadV1::PlumTree(PlumtreeMsg::V1 { data: msg }),
            cluster_id: self.agent.cluster_id(),
        };

        self.ser_buf.clear();
        if let Err(e) = payload.write_to_stream((&mut self.ser_buf).writer()) {
            error!("plumtree: failed to serialize wire msg: {e}");
            return;
        }

        let mut frame_buf = BytesMut::new();
        if let Err(e) = self
            .bcast_codec
            .encode(self.ser_buf.split().freeze(), &mut frame_buf)
        {
            error!("plumtree: failed to frame wire msg: {e}");
            return;
        }

        let data = frame_buf.freeze();
        let transport = self.transport.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(Duration::from_secs(5), transport.send_uni(addr, data)).await
            {
                Err(_) => warn!("plumtree: timed out sending to {addr}"),
                Ok(Err(e)) => debug!("plumtree: send error to {addr}: {e}"),
                Ok(Ok(())) => {
                    counter!("corro.plumtree.send.total").increment(1);
                }
            }
        });
    }

    fn deliver(&mut self, payload: ChangeV1) {
        let tx = self.tx_changes.clone();
        tokio::spawn(async move {
            if let Err(e) = tx.send((payload, ChangeSource::Broadcast)).await {
                error!("plumtree: could not deliver change: {e}");
            }
        });
    }

    fn schedule(&mut self, timer: plum_foca::Timer<ChangeId>, after: Duration) {
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
    let seen = ChangeSeenStore::new(config.max_received_entries);
    let mut state: PlumtreeState<ChangeId, ChangeV1, ActorId, ChangeSeenStore> =
        PlumtreeState::new_with_store(agent.actor_id(), config, seen);

    let (plumtree_timer_tx, mut plumtree_timer_rx) = mpsc::channel(10);
    let timer_spawner = TimerSpawner::new(plumtree_timer_tx);

    let mut tick_interval = interval(Duration::from_millis(200));
    // do we need this if we aren't batching?
    let bcast_codec = LengthDelimitedCodec::builder()
        .max_frame_length(10 * 1_024 * 1_024)
        .new_codec();

    let actors: HashMap<ActorId, SocketAddr> = agent
        .members()
        .read()
        .states
        .iter()
        .map(|(id, state)| (id.clone(), state.addr))
        .collect();

    let actors_keys: Vec<ActorId> = actors.keys().cloned().collect();
    let mut rt = CorrosionPlumtreeRuntime {
        agent: agent.clone(),
        transport: transport,
        tx_changes: tx_changes,
        timer_spawner: timer_spawner,
        bcast_codec: bcast_codec,
        ser_buf: BytesMut::with_capacity(10 * 1_024 * 1_024),
        actors,
    };

    let len = actors_keys.len();
    state.add_peers_bulk(actors_keys, &mut rt);
    info!("added {} peers to plumtree", len);

    #[derive(EnumDiscriminants)]
    #[strum_discriminants(derive(strum::IntoStaticStr))]
    enum Branch {
        PlumtreeInput(PlumtreeInput),
        PlumtreeUpdates(PlumtreeUpdates),
        HandleTimer(Timer<ChangeId>),
        PlumtreeTick,
        Metrics,
    }

    loop {
        let branch = tokio::select! {
            biased;
            _ = &mut tripwire => {
                info!("plumtree_loop: tripwire fired, shutting down");
                break;
            },
            updates = rx_plumtree_updates.recv() => match updates {
                Some(updates) => Branch::PlumtreeUpdates(updates),
                None => {
                    warn!("plumtree_loop: updates channel closed");
                    break;
                }
            },
            input = rx_plumtree.recv() => match input {
                Some(input) => Branch::PlumtreeInput(input),
                None => {
                    warn!("plumtree_loop: input channel closed");
                    break;
                }
            },
            Some((timer, _seq)) = plumtree_timer_rx.recv() => {
                Branch::HandleTimer(timer)
            }
            _ = tick_interval.tick() => {
                Branch::PlumtreeTick
            }
            // _ = metrics_interval.tick() => {
            //     Branch::Metrics
            // }
        };

        match branch {
            Branch::PlumtreeInput(input) => match input {
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
            Branch::PlumtreeUpdates(updates) => match updates {
                PlumtreeUpdates::MemberUp { actor_id, addr } => {
                    rt.update_actor(actor_id, addr);
                    state.peer_up(actor_id, &mut rt);
                }
                PlumtreeUpdates::MemberDown(actor_id) => {
                    rt.remove_actor(actor_id);
                    state.peer_down(&actor_id, &mut rt);
                }
            },
            Branch::HandleTimer(timer) => {
                state.timer_fired(timer, &mut rt);
            }
            Branch::PlumtreeTick => {
                state.tick(&mut rt);
            }
            Branch::Metrics => {
                todo!("metrics");
            }
        }
    }

    return state.stats().clone();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{setup, spawn_unipayload_handler};
    use crate::transport::{TransportError, TransportExt};
    use async_trait::async_trait;
    use bytes::{BufMut, Bytes, BytesMut};
    use corro_tests::{launch_test_agent, test_config};
    use corro_types::{
        actor::Actor,
        base::{dbsr, CrsqlDbVersion, CrsqlSeq},
        broadcast::{BroadcastV1, ChangeV1, Changeset},
        members::Members,
    };
    use parking_lot::RwLock;

    use std::{collections::HashMap, net::SocketAddr, sync::Arc};
    // use plum_foca::PlumtreeMsg;
    use rand::seq::{IndexedRandom, SliceRandom};
    use rangemap::RangeInclusiveSet;
    use speedy::Readable;
    use std::collections::HashSet;
    // use std::hash::Hash;
    use std::sync::atomic::AtomicU64;
    use tokio::task::JoinSet;
    use tokio_stream::StreamExt;
    use tokio_util::codec::{Encoder, FramedRead, LengthDelimitedCodec};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

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
                                        actor_id,
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

        let mut plumstats_map = plumtree_results
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
