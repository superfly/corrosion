pub mod actor;
pub mod api;
pub mod broadcast;
pub mod config;
pub mod filters;
pub mod json_schema;
pub mod pubsub;
pub mod sqlite;
pub mod types;

use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    fmt,
    fs::DirEntry,
    io::{self, Read, Write},
    mem::size_of,
    net::SocketAddr,
    ops::RangeInclusive,
    path::Path,
    sync::{atomic::AtomicI64, Arc},
    time::{Duration, Instant},
};

use crate::{
    actor::Actor,
    api::{http::api_v1_db_execute, peer::peer_api_v1_broadcast},
    broadcast::{runtime_loop, MessageV1},
    config::DEFAULT_GOSSIP_PORT,
    sqlite::{init_cr_conn, CrConn},
    types::{agent::AgentInner, members::Members},
};

use self::config::Config;
use actor::ActorId;
use axum::{
    error_handling::HandleErrorLayer, extract::DefaultBodyLimit, routing::post, BoxError,
    Extension, Router,
};
use broadcast::{BroadcastInput, BroadcastSrc, FocaInput, Message, FRAGMENTS_AT};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use consul::Client as ConsulClient;
use fallible_iterator::FallibleIterator;
use foca::Notification;
use futures::{FutureExt, TryFutureExt};
use hyper::{server::conn::AddrIncoming, StatusCode};
use metrics::{counter, histogram, increment_counter};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rangemap::RangeInclusiveSet;
use rusqlite::{params, Connection, OptionalExtension, ToSql, Transaction};
use spawn::spawn_counted;
use sqlite::{CrConnManager, SqlitePool};
use sqlite3_parser::{
    ast::{Cmd, CreateTableBody, Stmt},
    lexer::{sql::Parser, Input},
};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::mpsc::{channel, Receiver, Sender},
    task::block_in_place,
    time::timeout,
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::codec::{Decoder, LengthDelimitedCodec};
use tower::{buffer::BufferLayer, limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, trace, warn};
use tripwire::{PreemptibleFutureExt, Tripwire};
use trust_dns_resolver::{
    error::ResolveErrorKind,
    proto::rr::{RData, RecordType},
};
use types::agent::Agent;
use uuid::Uuid;

// const MAX_SYNC_BACKOFF: Duration = Duration::from_secs(60); // 1 minute oughta be enough, we're constantly getting broadcasts randomly + targetted
// const CONSUL_PULL_INTERVAL: Duration = Duration::from_secs(1);
const RANDOM_NODES_CHOICES: usize = 10;
// const GOSSIP_HANDLE_THRESHOLD: Duration = Duration::from_secs(4);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    Ok(())
}

#[derive(Default, Clone)]
pub struct Bookie(Arc<RwLock<HashMap<ActorId, Arc<RwLock<RangeInclusiveSet<i64>>>>>>);

impl Bookie {
    pub fn add(&self, actor_id: ActorId, start: i64, end: i64) {
        if let Some(booked) = { self.0.read().get(&actor_id).cloned() } {
            booked.write().insert(start..=end);
            return;
        }
        let mut w = self.0.write();
        let booked = w.entry(actor_id).or_default();
        booked.write().insert(start..=end);
    }

    pub fn contains(&self, actor_id: ActorId, version: i64) -> bool {
        self.0
            .read()
            .get(&actor_id)
            .map(|booked| booked.read().contains(&version))
            .unwrap_or(false)
    }

    // check if a whole range is fully contained in bookkeeping
    pub fn contains_range(&self, actor_id: ActorId, mut range: RangeInclusive<i64>) -> bool {
        let booked = self.0.read().get(&actor_id).cloned();

        booked
            .map(|booked| {
                let r = booked.read();
                range.all(|v| r.contains(&v))
            })
            .unwrap_or(false)
    }

    pub fn last(&self, actor_id: &ActorId) -> Option<i64> {
        self.0
            .read()
            .get(&actor_id)
            .and_then(|booked| booked.read().iter().map(|range| range.end()).max().copied())
    }
}

pub struct AgentOptions {
    actor_id: ActorId,
    gossip_listener: TcpListener,
    api_listener: Option<TcpListener>,
    bootstrap: Vec<String>,
    clock: Arc<uhlc::HLC>,
    rx_bcast: Receiver<BroadcastInput>,
    tripwire: Tripwire,
}

pub async fn setup(conf: Config, tripwire: Tripwire) -> eyre::Result<(Agent, AgentOptions)> {
    debug!("setting up corrosion @ {}", conf.base_path);
    let state_path = conf.base_path.join("state");
    if !state_path.exists() {
        std::fs::create_dir_all(&state_path)?;
    }

    let state_db_path = state_path.join("state.sqlite");

    let actor_uuid = {
        let mut actor_id_file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(conf.base_path.join("actor_id"))?;

        let mut uuid_str = String::new();
        actor_id_file.read_to_string(&mut uuid_str)?;

        let actor_uuid = if uuid_str.is_empty() {
            let uuid = Uuid::new_v4();
            actor_id_file.write_all(uuid.to_string().as_bytes())?;
            uuid
        } else {
            uuid_str.parse()?
        };

        debug!("actor_id file's uuid: {actor_uuid}");

        {
            let mut conn = CrConn(Connection::open(&state_db_path)?);
            init_cr_conn(&mut conn)?;
        }

        let conn = Connection::open(&state_db_path)?;

        trace!("got actor_id setup conn");
        let crsql_siteid: Uuid = conn
            .query_row("SELECT site_id FROM __crsql_siteid LIMIT 1;", [], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .optional()?
            .and_then(|raw| Uuid::from_slice(&raw).ok())
            .unwrap_or(Uuid::nil());

        debug!("crsql_siteid: {crsql_siteid:?}");

        if crsql_siteid != actor_uuid {
            warn!(
                "mismatched crsql_siteid {} and actor_id from file {}, override crsql's",
                crsql_siteid.hyphenated(),
                actor_uuid.hyphenated()
            );
            conn.execute("UPDATE __crsql_siteid SET site_id = ?;", [actor_uuid])?;
        }

        actor_uuid
    };

    info!("Actor ID: {}", actor_uuid);

    let actor_id = ActorId(actor_uuid);

    let rw_pool = bb8::Builder::new()
        .max_size(1)
        .build(CrConnManager::new(&state_db_path))
        .await?;

    apply_schema(&rw_pool, conf.base_path.join("schema")).await?;

    debug!("built RW pool");

    let ro_pool = bb8::Builder::new()
        .max_size(5)
        .min_idle(Some(1))
        .max_lifetime(Some(Duration::from_secs(30)))
        .build_unchecked(CrConnManager::new(&state_db_path));
    debug!("built RO pool");

    let mut bk: HashMap<ActorId, RangeInclusiveSet<i64>> = HashMap::new();

    {
        let conn = ro_pool.get().await?;
        let mut prepped = conn.prepare_cached(
            "SELECT actor_id, start_version, end_version FROM __corro_bookkeeping",
        )?;
        let mut rows = prepped.query([])?;

        loop {
            let row = rows.next()?;
            match row {
                None => break,
                Some(row) => {
                    let ranges = bk.entry(ActorId(row.get::<_, Uuid>(0)?)).or_default();
                    ranges.insert(row.get(1)?..=row.get(2)?);
                }
            }
        }
    }

    let bookie = Bookie(Arc::new(RwLock::new(
        bk.into_iter()
            .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
            .collect(),
    )));

    let gossip_listener = TcpListener::bind(conf.gossip_addr).await?;
    let gossip_addr = gossip_listener.local_addr()?;

    let (api_listener, api_addr) = match conf.api_addr {
        Some(api_addr) => {
            let ln = TcpListener::bind(api_addr).await?;
            let addr = ln.local_addr()?;
            (Some(ln), Some(addr))
        }
        None => (None, None),
    };

    let clock = Arc::new(
        uhlc::HLCBuilder::default()
            .with_id(actor_id.0.into())
            .with_max_delta(Duration::from_millis(300))
            .build(),
    );

    let (tx_bcast, rx_bcast) = channel(10240);

    let agent = Agent(Arc::new(AgentInner {
        actor_id,
        ro_pool,
        rw_pool,
        gossip_addr,
        api_addr,
        members: RwLock::new(Members::default()),
        clock: clock.clone(),
        consul: conf.consul.map(|c| ConsulClient::new(c).unwrap()),
        subscribers: Default::default(),
        bookie,
        tx_bcast,
    }));

    let opts = AgentOptions {
        actor_id,
        gossip_listener,
        api_listener,
        bootstrap: conf.bootstrap,
        clock,
        rx_bcast,
        tripwire,
    };

    Ok((agent, opts))
}

pub async fn start(conf: Config, tripwire: Tripwire) -> eyre::Result<Agent> {
    let (agent, opts) = setup(conf, tripwire.clone()).await?;

    tokio::spawn(run(agent.clone(), opts).inspect(|_| info!("corrosion agent run is done")));

    Ok(agent)
}

pub async fn run(agent: Agent, opts: AgentOptions) -> eyre::Result<()> {
    let AgentOptions {
        actor_id,
        gossip_listener,
        api_listener,
        bootstrap,
        mut tripwire,
        rx_bcast,
        clock,
    } = opts;
    info!("Current Actor ID: {}", actor_id.hyphenated());

    let (to_send_tx, to_send_rx) = channel(10240);
    let (notifications_tx, notifications_rx) = channel(10240);

    let (bcast_msg_tx, bcast_rx) = channel::<Message>(10240);

    let client = hyper::Client::builder()
        .pool_max_idle_per_host(1)
        .pool_idle_timeout(Duration::from_secs(5))
        .build_http();

    let gossip_addr = gossip_listener.local_addr()?;
    let udp_gossip = Arc::new(UdpSocket::bind(gossip_addr).await?);
    info!("Started UDP gossip listener on {gossip_addr}");

    let (foca_tx, foca_rx) = channel(10240);

    runtime_loop(
        Actor::new(actor_id, agent.gossip_addr()),
        // agent.clone(),
        udp_gossip.clone(),
        foca_rx,
        rx_bcast,
        to_send_tx,
        notifications_tx,
        client.clone(),
        agent.clock().clone(),
        tripwire.clone(),
    );

    let peer_api = Router::new()
        // .route(
        //     "/v1/sync",
        //     post(peer_api_v1_sync_post).route_layer(
        //         tower::ServiceBuilder::new()
        //             .layer(HandleErrorLayer::new(|_error: BoxError| async {
        //                 increment_counter!("corrosion.api.peer.shed.count", "route" => "POST /v1/sync");
        //                 Ok::<_, Infallible>((StatusCode::SERVICE_UNAVAILABLE, "sync has reached its concurrency limit".to_string()))
        //             }))
        //             // only allow 2 syncs at the same time...
        //             .layer(LoadShedLayer::new())
        //             .layer(ConcurrencyLimitLayer::new(3)),
        //     ),
        // )
        .route(
            "/v1/broadcast",
            post(peer_api_v1_broadcast).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        increment_counter!("corrosion.api.peer.shed.count", "route" => "POST /v1/broadcast");
                        Ok::<_, Infallible>((StatusCode::SERVICE_UNAVAILABLE, "broadcast has reached its concurrency limit".to_string()))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(BufferLayer::new(1024))
                    .layer(ConcurrencyLimitLayer::new(512)),
            ),
        )
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(actor_id))
                .layer(Extension(foca_tx.clone()))
                .layer(Extension(agent.tx_bcast().clone()))
                .layer(Extension(agent.bookie().clone()))
                .layer(Extension(bcast_msg_tx.clone()))
                .layer(Extension(clock.clone()))
                .layer(Extension(tripwire.clone())),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http());

    tokio::spawn({
        let foca_tx = foca_tx.clone();
        let socket = udp_gossip.clone();
        let bookie = agent.bookie().clone();
        async move {
            let mut recv_buf = vec![0u8; FRAGMENTS_AT];

            loop {
                match socket.recv_from(&mut recv_buf).await {
                    Ok((len, from_addr)) => {
                        trace!("udp received len: {len} from {from_addr}");
                        let recv = recv_buf[..len].to_vec().into();
                        let foca_tx = foca_tx.clone();
                        let bookie = bookie.clone();
                        let bcast_msg_tx = bcast_msg_tx.clone();
                        tokio::spawn(async move {
                            let mut codec = LengthDelimitedCodec::builder()
                                .length_field_type::<u32>()
                                .new_codec();
                            let mut databuf = BytesMut::new();
                            if let Err(e) = handle_payload(
                                None,
                                BroadcastSrc::Udp(from_addr),
                                &mut databuf,
                                &mut codec,
                                recv,
                                &foca_tx,
                                actor_id,
                                &bookie,
                                &bcast_msg_tx,
                            )
                            .await
                            {
                                error!("could not handle UDP payload from '{from_addr}': {e}");
                            }
                        });
                    }
                    Err(e) => {
                        error!("error receiving on gossip udp socket: {e}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    });

    info!("Starting gossip server on {gossip_addr}");

    tokio::spawn(
        axum::Server::builder(AddrIncoming::from_listener(gossip_listener)?)
            .serve(peer_api.into_make_service_with_connect_info::<SocketAddr>())
            .preemptible(tripwire.clone()),
    );

    tokio::spawn({
        let pool = agent.read_only_pool().clone();
        let foca_tx = foca_tx.clone();
        async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));

            loop {
                interval.tick().await;

                match generate_bootstrap(bootstrap.as_slice(), gossip_addr, &pool).await {
                    Ok(addrs) => {
                        for addr in addrs.iter() {
                            info!("Bootstrapping w/ {addr}");
                            foca_tx.send(FocaInput::Announce((*addr).into())).await.ok();
                        }
                    }
                    Err(e) => {
                        error!("could not find nodes to announce ourselves to: {e}");
                    }
                }
            }
        }
    });

    let api = Router::new()
        .route(
            "/db/execute",
            post(api_v1_db_execute).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(128)),
            ),
        )
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(Arc::new(AtomicI64::new(0))))
                .layer(Extension(actor_id))
                .layer(Extension(agent.read_write_pool().clone()))
                .layer(Extension(agent.clock().clone()))
                .layer(Extension(agent.tx_bcast().clone()))
                .layer(Extension(agent.subscribers().clone()))
                .layer(Extension(agent.bookie().clone()))
                .layer(Extension(tripwire.clone())),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http());

    if let Some(api_listener) = api_listener {
        let api_addr = api_listener.local_addr()?;
        info!("Starting public API server on {api_addr}");
        spawn_counted(
            axum::Server::builder(AddrIncoming::from_listener(api_listener)?)
                .executor(CountedExecutor)
                .serve(
                    api.clone()
                        .into_make_service_with_connect_info::<SocketAddr>(),
                )
                .with_graceful_shutdown(
                    tripwire
                        .clone()
                        .inspect(move |_| info!("corrosion api http tripped {api_addr}")),
                )
                .inspect(|_| info!("corrosion api is done")),
        );
    }

    // TODO: consul loop

    // let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));
    let mut db_cleanup_interval = tokio::time::interval(Duration::from_secs(60 * 15));

    tokio::spawn(handle_gossip_to_send(udp_gossip.clone(), to_send_rx));
    tokio::spawn(handle_notifications(
        agent.clone(),
        notifications_rx,
        foca_tx.clone(),
    ));

    let gossip_chunker =
        ReceiverStream::new(bcast_rx).chunks_timeout(512, Duration::from_millis(500));
    tokio::pin!(gossip_chunker);

    loop {
        tokio::select! {
            biased;
            msg = gossip_chunker.next() => match msg {
                Some(msg) => {
                    spawn_counted(
                        handle_gossip(agent.clone(), msg, false)
                            .inspect_err(|e| error!("error handling gossip: {e}")).preemptible(tripwire.clone()).complete_or_else(|_| {
                                warn!("preempted a gossip");
                                eyre::eyre!("preempted a gossip")
                            })
                    );
                },
                None => {
                    error!("NO MORE PARSED MESSAGES");
                    break;
                }
            },
            _ = db_cleanup_interval.tick() => {
                tokio::spawn(handle_db_cleanup(agent.read_write_pool().clone()).preemptible(tripwire.clone()));
            },
            // _ = metrics_interval.tick() => {
            //     let agent = agent.clone();
            //     tokio::spawn(tokio::runtime::Handle::current().spawn_blocking(move ||collect_metrics(agent)));
            // },
            _ = &mut tripwire => {
                debug!("tripped corrosion");
                break;
            }
        }
    }

    Ok(())
}

async fn handle_gossip_to_send(socket: Arc<UdpSocket>, mut to_send_rx: Receiver<(Actor, Bytes)>) {
    let mut buf = BytesMut::with_capacity(FRAGMENTS_AT);

    while let Some((actor, payload)) = to_send_rx.recv().await {
        debug!("got gossip to send to {actor:?}");
        let addr = actor.addr();
        buf.put_u8(0);
        buf.put_slice(&payload);

        let bytes = buf.split().freeze();
        let socket = socket.clone();
        spawn_counted(async move {
            trace!("in gossip send task");
            match timeout(Duration::from_secs(5), socket.send_to(bytes.as_ref(), addr)).await {
                Ok(Ok(n)) => {
                    trace!("successfully sent gossip to {addr}");
                    histogram!("corrosion.gossip.sent.bytes", n as f64, "actor_id" => actor.id().hyphenated().to_string());
                }
                Ok(Err(e)) => {
                    error!("could not send SWIM message via udp to {addr}: {e}");
                }
                Err(_e) => {
                    error!("could not send SWIM message via udp to {addr}: timed out");
                }
            }
        });
        increment_counter!("corrosion.gossip.send.count", "actor_id" => actor.id().hyphenated().to_string());
    }
}

// async fn handle_one_gossip()

async fn handle_gossip(
    agent: Agent,
    messages: Vec<Message>,
    high_priority: bool,
) -> eyre::Result<()> {
    let priority_label = if high_priority { "high" } else { "normal" };
    counter!("corrosion.broadcast.recv.count", messages.len() as u64, "priority" => priority_label);

    let bookie = agent.bookie();

    let mut rebroadcast = vec![];

    for msg in messages {
        match msg {
            Message::V1(MessageV1::Change {
                actor_id,
                start_version,
                end_version,
                ref changeset,
            }) => {
                if bookie.contains_range(actor_id, start_version..=end_version) {
                    trace!("already seen this one!");
                    continue;
                }
                let mut conn = agent.read_write_pool().get().await?;

                block_in_place(move || {
                    let tx = conn.transaction()?;

                    for change in changeset.iter() {
                        tx.execute(
                            r#"
                            INSERT INTO crsql_changes
                                ("table", pk, cid, val, col_version, db_version, site_id)
                            VALUES
                                (?,       ?,  ?,   ?,   ?,           ?,          ?);"#,
                            params![
                                change.table,
                                change.pk,
                                change.cid,
                                change.val,
                                change.col_version,
                                change.db_version,
                                change.site_id
                            ],
                        )?;
                    }

                    tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) VALUES (?, ?, ?);", params![actor_id.0, start_version, end_version])?;

                    tx.commit()?;

                    Ok::<_, eyre::Report>(())
                })?;

                bookie.add(actor_id, start_version, end_version);
            }
        }
        rebroadcast.push(msg);
    }

    for msg in rebroadcast {
        if let Err(e) = agent
            .tx_bcast()
            .send(BroadcastInput::Rebroadcast(msg))
            .await
        {
            error!("could not send message rebroadcast: {e}");
        }
    }

    Ok(())
}

async fn handle_notifications(
    agent: Agent,
    mut notification_rx: Receiver<Notification<Actor>>,
    foca_tx: Sender<FocaInput>,
) {
    while let Some(notification) = notification_rx.recv().await {
        trace!("handle notification");
        match notification {
            Notification::MemberUp(actor) => {
                let added = { agent.0.members.write().add_member(&actor) };
                info!("Member Up {actor:?} (added: {added})");
                if added {
                    increment_counter!("corrosion.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually added a member
                    // notify of new cluster size
                    let members_len = { agent.0.members.read().states.len() as u32 };
                    if let Ok(size) = members_len.try_into() {
                        foca_tx.send(FocaInput::ClusterSize(size)).await.ok();
                    }

                    // TODO: update SWIM state
                }
            }
            Notification::MemberDown(actor) => {
                let removed = { agent.0.members.write().remove_member(&actor) };
                info!("Member Down {actor:?} (removed: {removed})");
                if removed {
                    increment_counter!("corrosion.gossip.member.removed", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually removed a member
                    // notify of new cluster size
                    let member_len = { agent.0.members.read().states.len() as u32 };
                    if let Ok(size) = member_len.try_into() {
                        foca_tx.send(FocaInput::ClusterSize(size)).await.ok();
                    }
                    // TODO: update SWIM state
                }
            }
            Notification::Active => {
                info!("Current node is considered ACTIVE");
            }
            Notification::Idle => {
                warn!("Current node is considered IDLE");
            }
            Notification::Defunct => {
                error!("Current node is considered DEFUNCT");
                // TODO: reissue identity
            }
            Notification::Rejoin(id) => {
                info!("Rejoined the cluster with id: {id:?}");
            }
        }
    }
}

async fn handle_db_cleanup(rw_pool: SqlitePool) -> eyre::Result<()> {
    let conn = rw_pool.get().await?;
    block_in_place(move || {
        let start = Instant::now();

        let busy: bool =
            conn.query_row("PRAGMA wal_checkpoint(TRUNCATE);", [], |row| row.get(0))?;
        if busy {
            warn!("could not truncate sqlite WAL, database busy");
            increment_counter!("corrosion.db.wal.truncate.busy");
        } else {
            debug!("successfully truncated sqlite WAL!");
            histogram!(
                "corrosion.db.wal.truncate.seconds",
                start.elapsed().as_secs_f64()
            );
        }
        Ok::<_, eyre::Report>(())
    })?;
    Ok(())
}

#[derive(Clone)]
pub struct CountedExecutor;

impl<F> hyper::rt::Executor<F> for CountedExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, fut: F) {
        spawn::spawn_counted(fut);
    }
}

async fn generate_bootstrap(
    bootstrap: &[String],
    our_addr: SocketAddr,
    pool: &SqlitePool,
) -> eyre::Result<Vec<SocketAddr>> {
    let mut addrs = match resolve_bootstrap(bootstrap, our_addr).await {
        Ok(addrs) => addrs,
        Err(e) => {
            warn!("could not resolve bootstraps, falling back to in-db nodes: {e}");
            HashSet::new()
        }
    };

    if addrs.is_empty() {
        // fallback to in-db nodes
        let conn = pool.get().await?;
        let mut prepped = conn.prepare("select address from __corro_members limit 5")?;
        let node_addrs = prepped.query_map([], |row| row.get::<_, String>(0))?;
        addrs = node_addrs
            .flatten()
            .flat_map(|addr| addr.parse())
            .filter(|addr| match (our_addr, addr) {
                (SocketAddr::V6(our_ip), SocketAddr::V6(ip)) if our_ip != *ip => true,
                (SocketAddr::V4(our_ip), SocketAddr::V4(ip)) if our_ip != *ip => true,
                _ => {
                    info!("ignore node with addr: {addr}");
                    false
                }
            })
            .collect()
    }

    let mut rng = StdRng::from_entropy();

    Ok(addrs
        .into_iter()
        .choose_multiple(&mut rng, RANDOM_NODES_CHOICES))
}

async fn resolve_bootstrap(
    bootstrap: &[String],
    our_addr: SocketAddr,
) -> eyre::Result<HashSet<SocketAddr>> {
    use trust_dns_resolver::config::{NameServerConfigGroup, ResolverConfig, ResolverOpts};
    use trust_dns_resolver::{AsyncResolver, TokioAsyncResolver};

    let mut addrs = HashSet::new();

    if bootstrap.is_empty() {
        return Ok(addrs);
    }

    let system_resolver = AsyncResolver::tokio_from_system_conf()?;

    for s in bootstrap {
        if let Ok(addr) = s.parse() {
            addrs.insert(addr);
        } else {
            debug!("attempting to resolve {s}");
            let mut host_port_dns_server = s.split('@');
            let mut host_port = host_port_dns_server.next().unwrap().split(':');
            let mut resolver = None;
            if let Some(dns_server) = host_port_dns_server.next() {
                debug!("attempting to use resolver: {dns_server}");
                let (ip, port) = if let Ok(addr) = dns_server.parse::<SocketAddr>() {
                    (addr.ip(), addr.port())
                } else {
                    (dns_server.parse()?, 53)
                };
                resolver = Some(TokioAsyncResolver::tokio(
                    ResolverConfig::from_parts(
                        None,
                        vec![],
                        NameServerConfigGroup::from_ips_clear(&[ip], port, true),
                    ),
                    ResolverOpts::default(),
                )?);
                debug!("using resolver: {dns_server}");
            }
            if let Some(hostname) = host_port.next() {
                info!("Resolving '{hostname}' to an IP");
                match resolver
                    .as_ref()
                    .unwrap_or(&system_resolver)
                    .lookup(
                        hostname,
                        if our_addr.is_ipv6() {
                            RecordType::AAAA
                        } else {
                            RecordType::A
                        },
                    )
                    .await
                {
                    Ok(response) => {
                        debug!("Successfully resolved things: {response:?}");
                        let port: u16 = host_port
                            .next()
                            .and_then(|p| p.parse().ok())
                            .unwrap_or(DEFAULT_GOSSIP_PORT);
                        for addr in response.iter().filter_map(|rdata| match rdata {
                            RData::A(ip) => Some(SocketAddr::from((*ip, port))),
                            RData::AAAA(ip) => Some(SocketAddr::from((*ip, port))),
                            _ => None,
                        }) {
                            match (our_addr, addr) {
                                (SocketAddr::V4(our_ip), SocketAddr::V4(ip)) if our_ip != ip => {}
                                (SocketAddr::V6(our_ip), SocketAddr::V6(ip)) if our_ip != ip => {}
                                _ => {
                                    info!("ignore node with addr: {addr}");
                                    continue;
                                }
                            }
                            addrs.insert(addr);
                        }
                    }
                    Err(e) => match e.kind() {
                        ResolveErrorKind::NoRecordsFound { .. } => {
                            // do nothing, that might be fine!
                        }
                        _ => {
                            error!("could not resolve '{hostname}': {e}");
                            return Err(e.into());
                        }
                    },
                }
            }
        }
    }

    Ok(addrs)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PayloadKind {
    Swim,
    Broadcast,
    PriorityBroadcast,

    Unknown(u8),
}

impl fmt::Display for PayloadKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadKind::Swim => write!(f, "swim"),
            PayloadKind::Broadcast => write!(f, "broadcast"),
            PayloadKind::PriorityBroadcast => write!(f, "priority-broadcast"),
            PayloadKind::Unknown(b) => write!(f, "unknown({b})"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_payload(
    mut kind: Option<PayloadKind>,
    from: BroadcastSrc,
    buf: &mut BytesMut,
    codec: &mut LengthDelimitedCodec,
    mut payload: Bytes,
    foca_tx: &Sender<FocaInput>,
    actor_id: ActorId,
    bookie: &Bookie,
    bcast_tx: &Sender<Message>,
) -> eyre::Result<PayloadKind> {
    let kind = kind.take().unwrap_or_else(|| {
        let kind = match payload.get_u8() {
            0 => PayloadKind::Swim,
            1 => PayloadKind::Broadcast,
            2 => PayloadKind::PriorityBroadcast,
            n => PayloadKind::Unknown(n),
        };
        increment_counter!("corrosion.payload.recv.count", "kind" => kind.to_string());
        kind
    });

    trace!("got payload kind: {kind:?}");

    match kind {
        // SWIM
        PayloadKind::Swim => {
            trace!("received SWIM gossip");
            // And simply forward it to foca
            _ = foca_tx.send(FocaInput::Data(payload, from)).await;
        }
        // broadcast!
        PayloadKind::Broadcast | PayloadKind::PriorityBroadcast => {
            trace!("received broadcast gossip");
            // put it back in there...
            buf.put_slice(payload.as_ref());
            handle_broadcast(
                buf, codec, actor_id, bookie,
                // if kind == PayloadKind::PriorityBroadcast {
                //     bcast_priority_tx
                // } else {
                bcast_tx, // },
            )
            .await?;
        }
        // unknown
        PayloadKind::Unknown(n) => {
            return Err(eyre::eyre!("unknown payload kind: {n}"));
        }
    }
    Ok(kind)
}

pub async fn handle_broadcast(
    buf: &mut BytesMut,
    codec: &mut LengthDelimitedCodec,
    self_actor_id: ActorId,
    bookie: &Bookie,
    bcast_tx: &Sender<Message>,
) -> eyre::Result<()> {
    histogram!("corrosion.broadcast.recv.bytes", buf.len() as f64);
    loop {
        // decode a length-delimited "frame"
        match codec.decode(buf) {
            Ok(Some(bytes)) => {
                trace!("successfully decoded a frame, len: {}", bytes.len());
                // let mut cursor = bytes.freeze();

                if buf.remaining() >= size_of::<u32>() {
                    let crc = buf.get_u32();
                    let new_crc = crc32fast::hash(&bytes);
                    trace!("crc: {crc}, new_crc: {new_crc}");
                    if crc != new_crc {
                        eyre::bail!("mistmatched crc32! {crc} != {new_crc}");
                    }
                } else {
                    warn!("not enough bytes to crc32 (remaining: {})", buf.remaining());
                }

                let msg = Message::from_slice(&bytes)?;

                trace!("broadcast: {msg:?}");

                match msg {
                    Message::V1(v1) => match v1 {
                        MessageV1::Change {
                            actor_id,
                            start_version,
                            end_version,
                            changeset,
                        } => {
                            increment_counter!("corrosion.broadcast.recv.count", "kind" => "operation");

                            if bookie.contains_range(actor_id, start_version..=end_version) {
                                trace!("already seen, stop disseminating");
                                continue;
                            }

                            if actor_id != self_actor_id {
                                bcast_tx
                                    .send(Message::V1(MessageV1::Change {
                                        actor_id,
                                        start_version,
                                        end_version,
                                        changeset,
                                    }))
                                    .await
                                    .ok();
                            }
                        }
                    },
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                error!(
                    "error decoding bytes from gossip body: {e} (len: {})",
                    buf.len()
                );
                break;
            }
        }
    }
    Ok(())
}

pub async fn apply_schema<P: AsRef<Path>>(w_pool: &SqlitePool, schema_path: P) -> eyre::Result<()> {
    let mut conn = w_pool.get().await?;

    let migrations: Vec<Box<dyn Migration>> = vec![Box::new(
        init_migration as fn(&Transaction) -> eyre::Result<()>,
    )];

    let target_version = migrations.len();

    let current_version = user_version(&conn)?;
    {
        let tx = conn.transaction()?;
        for (i, migration) in migrations.into_iter().enumerate() {
            let new_version = i + 1;
            if new_version <= current_version {
                continue;
            }
            migration.migrate(&tx)?;
        }
        set_user_version(&tx, target_version)?;
        tx.commit()?;
    }

    let dir = std::fs::read_dir(schema_path)?;
    let entries: Vec<DirEntry> = dir.collect::<Result<Vec<_>, io::Error>>()?;
    let mut entries: Vec<DirEntry> = entries
        .into_iter()
        .filter_map(|entry| {
            entry
                .path()
                .extension()
                .and_then(|ext| if ext == "sql" { Some(entry) } else { None })
        })
        .collect();
    entries.sort_by_key(|entry| entry.path());

    let tx = conn.transaction()?;
    for entry in entries {
        println!("applying schema file: {}", entry.path().display());

        let mut q = String::new();
        std::fs::File::open(entry.path())?.read_to_string(&mut q)?;

        let cmds = prepare_sql(q.as_bytes())?;

        tx.execute_batch(
            &cmds
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join("\n"),
        )?;
        // tx.execute_batch(&q)?;
    }
    tx.commit()?;

    Ok::<_, eyre::Report>(())
}

trait Migration {
    fn migrate(&self, tx: &Transaction) -> eyre::Result<()>;
}

impl Migration for fn(&Transaction) -> eyre::Result<()> {
    fn migrate(&self, tx: &Transaction) -> eyre::Result<()> {
        self(tx)
    }
}

fn init_migration(tx: &Transaction) -> Result<(), eyre::Report> {
    tx.execute_batch(
        r#"
    CREATE TABLE __corro_bookkeeping (
        actor_id BLOB NOT NULL,
        start_version INTEGER NOT NULL,
        end_version INTEGER NOT NULL,
        PRIMARY KEY (actor_id, start_version)
    ) WITHOUT ROWID;
    
    CREATE INDEX __corro_bookkeeping_end_version ON __corro_bookkeeping (end_version);
    
    CREATE TABLE __corro_members (
        id BLOB PRIMARY KEY NOT NULL,
        address TEXT NOT NULL,
    
        state TEXT NOT NULL DEFAULT 'down',
    
        foca_state JSON
    ) WITHOUT ROWID;
    "#,
    )?;
    Ok(())
}

fn prepare_sql<I: Input>(input: I) -> eyre::Result<Vec<Cmd>> {
    let mut cmds = vec![];
    let mut parser = sqlite3_parser::lexer::sql::Parser::new(input);
    loop {
        match parser.next() {
            Ok(None) => break,
            Err(err) => {
                eprintln!("Err: {err}");
                return Err(err.into());
            }
            Ok(Some(mut cmd)) => {
                let extra_cmd = if let Cmd::Stmt(Stmt::CreateTable {
                    ref tbl_name,
                    ref mut body,
                    ..
                }) = cmd
                {
                    if let CreateTableBody::ColumnsAndConstraints {
                        // ref mut columns,
                        ..
                    } = body
                    {
                        if !tbl_name.name.0.contains("crsql")
                            & !tbl_name.name.0.contains("sqlite")
                            & !tbl_name.name.0.starts_with("__corro")
                        {
                            // let mut sub_schema = JsonSchema::default();
                            // sub_schema.id = Some(format!("/data/{}", tbl_name.name));

                            // for def in columns.iter_mut() {
                            //     add_prop(&mut sub_schema, def);
                            // }

                            // sub_schema.additional_properties = Some(true);
                            // schema.defs.insert(tbl_name.name.to_string(), sub_schema);

                            println!("SELECTING crsql_as_crr for {}", tbl_name.name.0);

                            let select = format!("SELECT crsql_as_crr('{}');", tbl_name.name.0);
                            let mut select_parser = Parser::new(select.as_bytes());
                            Some(
                                select_parser
                                    .next()?
                                    .ok_or(eyre::eyre!("could not parse select crsql"))?,
                            )
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                cmds.push(cmd);
                if let Some(extra_cmd) = extra_cmd {
                    cmds.push(extra_cmd);
                }
            }
        }
    }

    Ok(cmds)
}

// Read user version field from the SQLite db
fn user_version(conn: &Connection) -> Result<usize, rusqlite::Error> {
    #[allow(deprecated)] // To keep compatibility with lower rusqlite versions
    conn.query_row::<_, &[&dyn ToSql], _>("PRAGMA user_version", &[], |row| row.get(0))
        .map(|v: i64| v as usize)
}

// Set user version field from the SQLite db
fn set_user_version(conn: &Connection, v: usize) -> rusqlite::Result<()> {
    let v = v as u32;
    conn.pragma_update(None, "user_version", &v)?;
    Ok(())
}

#[cfg(test)]
pub mod tests {
    use std::net::SocketAddr;

    use serde::Deserialize;
    use serde_json::json;
    use spawn::wait_for_all_pending_handles;
    use tempfile::TempDir;
    use tokio::time::sleep;
    use uuid::Uuid;

    use crate::config::ConfigBuilder;
    use corro_types::api::{RqliteResponse, Statement};

    use super::*;

    const TEST_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;
    "#;

    #[derive(Clone)]
    pub struct TestAgent {
        pub agent: Agent,
        _tmpdir: Arc<TempDir>,
    }

    pub async fn launch_test_agent(
        id: &str,
        bootstrap: Vec<String>,
        tripwire: Tripwire,
    ) -> eyre::Result<TestAgent> {
        launch_test_agent_w_gossip(id, "127.0.0.1:0".parse()?, bootstrap, "test", tripwire).await
    }

    pub async fn launch_test_agent_with_region(
        id: &str,
        bootstrap: Vec<String>,
        region: &str,
        tripwire: Tripwire,
    ) -> eyre::Result<TestAgent> {
        launch_test_agent_w_gossip(id, "127.0.0.1:0".parse()?, bootstrap, region, tripwire).await
    }

    pub async fn launch_test_agent_w_builder<F: FnOnce(ConfigBuilder) -> eyre::Result<Config>>(
        f: F,
        tripwire: Tripwire,
    ) -> eyre::Result<TestAgent> {
        let tmpdir = tempfile::tempdir()?;

        let conf = f(Config::builder().base_path(tmpdir.path().display().to_string()))?;

        let schema_path = tmpdir.path().join("schema");
        tokio::fs::create_dir(&schema_path).await?;
        tokio::fs::write(schema_path.join("tests.sql"), TEST_SCHEMA.as_bytes()).await?;

        start(conf, tripwire).await.map(|agent| TestAgent {
            agent,
            _tmpdir: Arc::new(tmpdir),
        })
    }

    pub async fn launch_test_agent_w_gossip(
        id: &str,
        gossip_addr: SocketAddr,
        bootstrap: Vec<String>,
        _region: &str,
        tripwire: Tripwire,
    ) -> eyre::Result<TestAgent> {
        trace!("launching test agent {id}");
        launch_test_agent_w_builder(
            move |conf| {
                Ok(conf
                    .gossip_addr(gossip_addr)
                    .api_addr("127.0.0.1:0".parse()?)
                    .bootstrap(bootstrap)
                    .build()?)
            },
            tripwire,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn insert_rows_and_gossip() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta1 = launch_test_agent("test1", vec![], tripwire.clone()).await?;
        let ta2 = launch_test_agent(
            "test2",
            vec![ta1.agent.gossip_addr().to_string()],
            tripwire.clone(),
        )
        .await?;

        let client = hyper::Client::builder()
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(300))
            .build_http::<hyper::Body>();

        let req_body: Vec<Statement> = serde_json::from_value(json!([[
            "INSERT INTO tests (id,text) VALUES (?,?)",
            1,
            "hello world 1"
        ]]))?;

        println!("stmts: {req_body:?}");

        let res = client
            .request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!(
                        "http://{}/db/execute",
                        ta1.agent.api_addr().unwrap()
                    ))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            )
            .await?;

        let body: RqliteResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");

        #[derive(Debug, Deserialize)]
        struct TestRecord {
            id: i64,
            text: String,
        }

        let svc: TestRecord = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT id, text FROM tests WHERE id = 1;",
            [],
            |row| {
                Ok(TestRecord {
                    id: row.get(0)?,
                    text: row.get(1)?,
                })
            },
        )?;

        assert_eq!(svc.id, 1);
        assert_eq!(svc.text, "hello world 1");

        sleep(Duration::from_secs(1)).await;

        let svc: TestRecord = ta2.agent.read_only_pool().get().await?.query_row(
            "SELECT id, text FROM tests WHERE id = 1;",
            [],
            |row| {
                Ok(TestRecord {
                    id: row.get(0)?,
                    text: row.get(1)?,
                })
            },
        )?;

        assert_eq!(svc.id, 1);
        assert_eq!(svc.text, "hello world 1");

        // assert_eq!(
        //     bod["outcomes"],
        //     serde_json::json!([{"result": "applied", "version": 1}])
        // );

        let req_body: Vec<Statement> = serde_json::from_value(json!([[
            "INSERT INTO tests (id,text) VALUES (?,?)",
            2,
            "hello world 2"
        ]]))?;

        println!("stmts: {req_body:?}");

        let res = client
            .request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!(
                        "http://{}/db/execute",
                        ta1.agent.api_addr().unwrap()
                    ))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            )
            .await?;

        let body: RqliteResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");

        let bk = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT actor_id, start_version, end_version FROM __corro_bookkeeping",
            [],
            |row| {
                Ok((
                    row.get::<_, Uuid>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )?;

        assert_eq!(bk, (ta1.agent.actor_id().0, 0, 2));

        let svc: TestRecord = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT id, text FROM tests WHERE id = 2;",
            [],
            |row| {
                Ok(TestRecord {
                    id: row.get(0)?,
                    text: row.get(1)?,
                })
            },
        )?;

        assert_eq!(svc.id, 2);
        assert_eq!(svc.text, "hello world 2");

        sleep(Duration::from_secs(1)).await;

        let svc: TestRecord = ta2.agent.read_only_pool().get().await?.query_row(
            "SELECT id, text FROM tests WHERE id = 2;",
            [],
            |row| {
                Ok(TestRecord {
                    id: row.get(0)?,
                    text: row.get(1)?,
                })
            },
        )?;

        assert_eq!(svc.id, 2);
        assert_eq!(svc.text, "hello world 2");

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
