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
    error::Error,
    fmt,
    fs::DirEntry,
    io::{self, Read, Write},
    net::SocketAddr,
    path::Path,
    sync::{atomic::AtomicI64, Arc},
    time::{Duration, Instant},
};

use crate::{
    actor::Actor,
    api::{
        http::api_v1_db_execute,
        peer::{peer_api_v1_broadcast, peer_api_v1_sync_post},
    },
    broadcast::{runtime_loop, MessageV1},
    config::DEFAULT_GOSSIP_PORT,
    sqlite::{init_cr_conn, CrConn},
    types::{agent::AgentInner, members::Members},
};

use self::config::Config;
use actor::ActorId;
use api::peer::{generate_sync, SyncMessage};
use axum::{
    error_handling::HandleErrorLayer, extract::DefaultBodyLimit, routing::post, BoxError,
    Extension, Router,
};
use broadcast::{
    BroadcastInput, BroadcastSrc, ClientPool, FocaInput, Message, MessageDecodeError, FRAGMENTS_AT,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use consul::Client as ConsulClient;
use fallible_iterator::FallibleIterator;
use foca::Notification;
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use hyper::{server::conn::AddrIncoming, StatusCode};
use metrics::{counter, gauge, histogram, increment_counter};
use parking_lot::{RwLock, RwLockReadGuard};
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rangemap::RangeInclusiveMap;
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
use tokio_util::{codec::LengthDelimitedCodec, io::StreamReader};
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

const MAX_SYNC_BACKOFF: Duration = Duration::from_secs(60); // 1 minute oughta be enough, we're constantly getting broadcasts randomly + targetted
const RANDOM_NODES_CHOICES: usize = 10;
// const GOSSIP_HANDLE_THRESHOLD: Duration = Duration::from_secs(4);

#[tokio::main]
async fn main() -> eyre::Result<()> {
    Ok(())
}

#[derive(Default, Clone)]
pub struct Booked(Arc<RwLock<RangeInclusiveMap<i64, Option<i64>>>>);

impl Booked {
    pub fn insert(&self, version: i64, db_version: Option<i64>) {
        self.0.write().insert(version..=version, db_version);
    }

    pub fn contains(&self, version: i64) -> bool {
        self.0.read().contains_key(&version)
    }

    pub fn last(&self) -> Option<i64> {
        self.0.read().iter().map(|(k, _v)| *k.end()).max()
    }

    pub fn read(&self) -> RwLockReadGuard<RangeInclusiveMap<i64, Option<i64>>> {
        self.0.read()
    }
}

#[derive(Default, Clone)]
pub struct Bookie(Arc<RwLock<HashMap<ActorId, Booked>>>);

impl Bookie {
    pub fn add(&self, actor_id: ActorId, version: i64, db_version: Option<i64>) {
        {
            if let Some(booked) = self.0.read().get(&actor_id) {
                booked.insert(version, db_version);
                return;
            }
        };

        let mut w = self.0.write();
        let booked = w.entry(actor_id).or_default();
        booked.insert(version, db_version);
    }

    pub fn contains(&self, actor_id: ActorId, version: i64) -> bool {
        self.0
            .read()
            .get(&actor_id)
            .map(|booked| booked.contains(version))
            .unwrap_or(false)
    }

    pub fn last(&self, actor_id: &ActorId) -> Option<i64> {
        self.0
            .read()
            .get(&actor_id)
            .and_then(|booked| booked.last())
    }

    pub fn read(&self) -> RwLockReadGuard<HashMap<ActorId, Booked>> {
        self.0.read()
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
        .build_unchecked(CrConnManager::new_read_only(&state_db_path));
    debug!("built RO pool");

    let mut bk: HashMap<ActorId, RangeInclusiveMap<i64, Option<i64>>> = HashMap::new();

    {
        let conn = ro_pool.get().await?;
        let mut prepped =
            conn.prepare_cached("SELECT actor_id, version, db_version FROM __corro_bookkeeping")?;
        let mut rows = prepped.query([])?;

        loop {
            let row = rows.next()?;
            match row {
                None => break,
                Some(row) => {
                    let ranges = bk.entry(ActorId(row.get::<_, Uuid>(0)?)).or_default();
                    let v = row.get(1)?;
                    ranges.insert(v..=v, row.get(2)?);
                }
            }
        }
    }

    let bookie = Bookie(Arc::new(RwLock::new(
        bk.into_iter()
            .map(|(k, v)| (k, Booked(Arc::new(RwLock::new(v)))))
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
        .route(
            "/v1/sync",
            post(peer_api_v1_sync_post).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        increment_counter!("corrosion.api.peer.shed.count", "route" => "POST /v1/sync");
                        Ok::<_, Infallible>((StatusCode::SERVICE_UNAVAILABLE, "sync has reached its concurrency limit".to_string()))
                    }))
                    // only allow 2 syncs at the same time...
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(3)),
            ),
        )
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
                .layer(Extension(agent.read_only_pool().clone()))
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

    spawn_counted(
        sync_loop(agent.clone(), client.clone(), tripwire.clone())
            .inspect(|_| info!("corrosion agent sync loop is done")),
    );

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
        trace!("got gossip to send to {actor:?}");
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
        if process_msg(&msg, bookie, agent.read_write_pool()).await? {
            rebroadcast.push(msg);
        }
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
        match Message::decode(codec, buf) {
            Ok(Some(msg)) => {
                trace!("broadcast: {msg:?}");

                match msg {
                    Message::V1(v1) => match v1 {
                        MessageV1::Change {
                            actor_id,
                            version,
                            changeset,
                        } => {
                            increment_counter!("corrosion.broadcast.recv.count", "kind" => "operation");

                            if bookie.contains(actor_id, version) {
                                trace!("already seen, stop disseminating");
                                continue;
                            }

                            if actor_id != self_actor_id {
                                bcast_tx
                                    .send(Message::V1(MessageV1::Change {
                                        actor_id,
                                        version,
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

async fn process_msg(
    msg: &Message,
    bookie: &Bookie,
    pool: &SqlitePool,
) -> Result<bool, bb8::RunError<bb8_rusqlite::Error>> {
    match msg {
        Message::V1(MessageV1::Change {
            actor_id,
            version,
            changeset,
        }) => {
            if bookie.contains(*actor_id, *version) {
                trace!(
                    "already seen this one! from: {}, version: {version}",
                    actor_id.hyphenated()
                );
                return Ok(false);
            }

            trace!(
                "received {} changes to process from: {}, version: {version}",
                changeset.len(),
                actor_id.hyphenated()
            );

            let mut conn = pool.get().await?;

            let db_version = block_in_place(move || {
                let tx = conn.transaction()?;

                let start_version: i64 =
                    tx.query_row("SELECT crsql_dbversion()", (), |row| row.get(0))?;

                for change in changeset.iter() {
                    tx.prepare_cached(
                        r#"
                    INSERT INTO crsql_changes
                        ("table", pk, cid, val, col_version, db_version, site_id)
                    VALUES
                        (?,       ?,  ?,   ?,   ?,           ?,          ?)"#,
                    )?
                    .execute(params![
                        change.table.as_str(),
                        change.pk.as_str(),
                        change.cid.as_str(),
                        &change.val,
                        change.col_version,
                        change.db_version,
                        &change.site_id
                    ])?;
                }

                let end_version: i64 = tx
                    .prepare_cached("SELECT MAX(db_version) FROM crsql_changes;")?
                    .query_row((), |row| row.get(0))?;

                let db_version = if end_version > start_version {
                    Some(end_version)
                } else {
                    None
                };

                tx.prepare_cached("INSERT INTO __corro_bookkeeping (actor_id, version, db_version) VALUES (?, ?, ?);")?.execute(params![actor_id.0, version, db_version])?;

                tx.commit()?;

                Ok::<_, bb8_rusqlite::Error>(db_version)
            })?;

            bookie.add(*actor_id, *version, db_version);
        }
    }
    Ok(true)
}

#[derive(Debug, thiserror::Error)]
pub enum SyncClientError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("bad status code: {0}")]
    Status(StatusCode),
    #[error("service unavailable right now")]
    Unavailable,
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error("request timed out")]
    RequestTimedOut,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pool(#[from] bb8::RunError<bb8_rusqlite::Error>),
    #[error("no good candidates found")]
    NoGoodCandidate,
    #[error("could not decode message: {0}")]
    Decoded(#[from] MessageDecodeError),
}

impl SyncClientError {
    pub fn is_unavailable(&self) -> bool {
        matches!(self, SyncClientError::Unavailable)
    }
}

struct SyncWith {
    actor_id: ActorId,
    addr: SocketAddr,
}

async fn handle_sync(agent: &Agent, client: &ClientPool) -> Result<(), SyncClientError> {
    let sync = generate_sync(agent.bookie(), agent.actor_id());
    for (actor_id, needed) in sync.need.iter() {
        gauge!("corrosion.sync.client.needed", needed.len() as f64, "actor_id" => actor_id.0.to_string());
    }
    for (actor_id, version) in sync.heads.iter() {
        gauge!("corrosion.sync.client.head", *version as f64, "actor_id" => actor_id.to_string());
    }

    let sync = Arc::new(sync);

    let mut boff = backoff::Backoff::new(5)
        .timeout_range(Duration::from_millis(100), Duration::from_secs(1))
        .iter();

    loop {
        let (actor_id, addr) = {
            let low_rtt_candidates = {
                let members = agent.0.members.read();

                members
                    .states
                    .iter()
                    .filter(|(id, _state)| **id != agent.actor_id())
                    .map(|(id, state)| (*id, state.addr))
                    .collect::<Vec<(ActorId, SocketAddr)>>()
            };

            if low_rtt_candidates.is_empty() {
                warn!("could not find any good candidate for sync");
                return Err(SyncClientError::NoGoodCandidate);
            }

            // low_rtt_candidates.truncate(low_rtt_candidates.len() / 2);

            let mut rng = StdRng::from_entropy();

            let mut choices = low_rtt_candidates.into_iter().choose_multiple(&mut rng, 2);

            choices.sort_by(|a, b| {
                sync.need_len_for_actor(&b.0)
                    .cmp(&sync.need_len_for_actor(&a.0))
            });

            if let Some(chosen) = choices.get(0).cloned() {
                chosen
            } else {
                return Err(SyncClientError::NoGoodCandidate);
            }
        };

        info!(
            "syncing from: {} to: {}, need len: {}",
            sync.actor_id.hyphenated(),
            actor_id.hyphenated(),
            sync.need_len(),
        );

        let start = Instant::now();
        let res =
            handle_sync_receive(agent, client, &SyncWith { actor_id, addr }, sync.clone()).await;

        match res {
            Ok(n) => {
                let elapsed = start.elapsed();
                info!(
                    "synced {n} ops w/ {} in {}s @ {} ops/s",
                    actor_id.hyphenated(),
                    elapsed.as_secs_f64(),
                    n as f64 / elapsed.as_secs_f64()
                );
                return Ok(());
            }
            Err(e) => {
                if e.is_unavailable() {
                    increment_counter!("corrosion.sync.client.busy.servers");
                    if let Some(dur) = boff.next() {
                        tokio::time::sleep(dur).await;
                        continue;
                    }
                }
                error!(?actor_id, ?addr, "could not properly sync: {e}");
                return Err(e);
            }
        }
    }
}

async fn handle_sync_receive(
    agent: &Agent,
    client: &ClientPool,
    with: &SyncWith,
    sync: Arc<SyncMessage>,
) -> Result<usize, SyncClientError> {
    let SyncWith { actor_id, addr } = with;
    let actor_id = *actor_id;
    // println!("syncing with {actor_id:?}");

    increment_counter!("corrosion.sync.client.member", "id" => actor_id.0.to_string(), "addr" => addr.to_string());

    histogram!(
        "corrosion.sync.client.request.operations.need.count",
        sync.need.len() as f64
    );
    let data = serde_json::to_vec(&*sync)?;

    gauge!(
        "corrosion.sync.client.request.size.bytes",
        data.len() as f64
    );

    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(format!("http://{addr}/v1/sync"))
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .header(hyper::header::ACCEPT, "application/json")
        .header(
            "corro-clock",
            serde_json::to_string(&agent.clock().new_timestamp())
                .expect("could not serialize clock"),
        )
        .body(hyper::Body::wrap_stream(futures::stream::iter(
            data.chunks(4 * 1024 * 1024)
                .map(|v| Bytes::from(v.to_vec()))
                .collect::<Vec<Bytes>>()
                .into_iter()
                .map(Ok::<_, std::io::Error>),
        )))
        .unwrap();

    increment_counter!("corrosion.sync.client.request.count", "id" => actor_id.0.to_string(), "addr" => addr.to_string());

    let start = Instant::now();
    let res = match timeout(Duration::from_secs(15), client.request(req)).await {
        Ok(Ok(res)) => {
            histogram!("corrosion.sync.client.response.time.seconds", start.elapsed().as_secs_f64(), "id" => actor_id.0.to_string(), "addr" => addr.to_string(), "status" => res.status().to_string());
            res
        }
        Ok(Err(e)) => {
            increment_counter!("corrosion.sync.client.request.error", "id" => actor_id.0.to_string(), "addr" => addr.to_string(), "error" => e.to_string());
            return Err(e.into());
        }
        Err(_e) => {
            increment_counter!("corrosion.sync.client.request.error", "id" => actor_id.0.to_string(), "addr" => addr.to_string(), "error" => "timed out waiting for headers");
            return Err(SyncClientError::RequestTimedOut);
        }
    };

    let status = res.status();
    if status != hyper::StatusCode::OK {
        if status == hyper::StatusCode::SERVICE_UNAVAILABLE {
            return Err(SyncClientError::Unavailable);
        }
        return Err(SyncClientError::Status(status));
    }

    let body = StreamReader::new(res.into_body().map_err(|e| {
        if let Some(io_error) = e
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
        {
            return io::Error::from(io_error.kind());
        }
        io::Error::new(io::ErrorKind::Other, e)
    }));

    let mut framed = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .new_read(body)
        .inspect_ok(|b| counter!("corrosion.sync.client.chunk.recv.bytes", b.len() as u64));

    let mut count = 0;

    while let Some(buf_res) = framed.next().await {
        let mut buf = buf_res?;
        let msg = Message::from_buf(&mut buf)?;
        process_msg(&msg, agent.bookie(), agent.read_write_pool()).await?;
        match msg {
            Message::V1(MessageV1::Change { ref changeset, .. }) => count += changeset.len(),
        }
    }

    Ok(count)
}

async fn sync_loop(agent: Agent, client: ClientPool, mut tripwire: Tripwire) {
    let mut sync_backoff = backoff::Backoff::new(0)
        .timeout_range(Duration::from_secs(1), MAX_SYNC_BACKOFF)
        .iter();
    let next_sync_at = tokio::time::sleep(sync_backoff.next().unwrap());
    tokio::pin!(next_sync_at);

    loop {
        tokio::select! {
            _ = &mut next_sync_at => {
                // ignoring here, there is trying and logging going on inside
                match handle_sync(&agent, &client).preemptible(&mut tripwire).await {
                    tripwire::Outcome::Preempted(_) => {
                        warn!("aborted sync by tripwire");
                        break;
                    },
                    tripwire::Outcome::Completed(_res) => {

                    }
                }
                next_sync_at.as_mut().reset(tokio::time::Instant::now() + sync_backoff.next().unwrap());
            },
            _ = &mut tripwire => {
                break;
            }
        }
    }
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
                version INTEGER NOT NULL,
                db_version INTEGER,
                PRIMARY KEY (actor_id, version)
            ) WITHOUT ROWID;
                        
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

    use futures::{StreamExt, TryStreamExt};
    use serde::Deserialize;
    use serde_json::json;
    use spawn::wait_for_all_pending_handles;
    use tempfile::TempDir;
    use tokio::time::{sleep, MissedTickBehavior};
    use tracing::info_span;
    use uuid::Uuid;

    use crate::{api::peer::generate_sync, config::ConfigBuilder};
    use corro_types::api::{RqliteResponse, Statement};

    use super::*;

    const TEST_SCHEMA: &str = r#"
        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS tests2 (
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
        ],]))?;

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

        let dbversion: i64 = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT crsql_dbversion();",
            (),
            |row| row.get(0),
        )?;
        assert_eq!(dbversion, 1);

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

        let bk: Vec<(Uuid, i64, i64)> = ta1
            .agent
            .read_only_pool()
            .get()
            .await?
            .prepare("SELECT actor_id, version, db_version FROM __corro_bookkeeping")?
            .query_map((), |row| {
                Ok((
                    row.get::<_, Uuid>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;

        assert_eq!(
            bk,
            vec![
                (ta1.agent.actor_id().0, 1, 1),
                (ta1.agent.actor_id().0, 2, 2)
            ]
        );

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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn stress_test() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let agents = futures::stream::iter(
            (0..10).map(|n| "127.0.0.1:0".parse().map(move |addr| (n, addr))),
        )
        .try_chunks(50)
        .try_fold(vec![], {
            let tripwire = tripwire.clone();
            move |mut agents: Vec<TestAgent>, to_launch| {
                let tripwire = tripwire.clone();
                async move {
                    for (n, gossip_ln) in to_launch {
                        println!("LAUNCHING AGENT #{n}");
                        let mut rng = StdRng::from_entropy();
                        let bootstrap = agents
                            .iter()
                            .map(|ta| ta.agent.gossip_addr())
                            .choose_multiple(&mut rng, 10);
                        agents.push(
                            launch_test_agent_w_gossip(
                                &format!("test-{n}"),
                                gossip_ln,
                                bootstrap.iter().map(SocketAddr::to_string).collect(),
                                "test",
                                tripwire.clone(),
                            )
                            .await
                            .unwrap(),
                        );
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    Ok(agents)
                }
            }
        })
        .await?;

        let client: hyper::Client<_, hyper::Body> = hyper::Client::builder().build_http();

        let addrs: Vec<SocketAddr> = agents.iter().filter_map(|ta| ta.agent.api_addr()).collect();

        let count = 200;

        let iter = (0..count).flat_map(|n| {
            serde_json::from_value::<Vec<Statement>>(json!([
                [
                    "INSERT INTO tests (id,text) VALUES (?,?)",
                    n,
                    "hello world {n}"
                ],
                [
                    "INSERT INTO tests2 (id,text) VALUES (?,?)",
                    n,
                    "hello world {n}"
                ],
                [
                    "INSERT INTO tests (id,text) VALUES (?,?)",
                    n + 10000,
                    "hello world {n}"
                ],
                [
                    "INSERT INTO tests2 (id,text) VALUES (?,?)",
                    n + 10000,
                    "hello world {n}"
                ]
            ]))
            .unwrap()
        });

        tokio::spawn(async move {
            tokio_stream::StreamExt::map(futures::stream::iter(iter).chunks(5), {
                let addrs = addrs.clone();
                let client = client.clone();
                move |statements| {
                    let addrs = addrs.clone();
                    let client = client.clone();
                    Ok(async move {
                        let mut rng = StdRng::from_entropy();
                        let chosen = addrs.iter().choose(&mut rng).unwrap();

                        let res = client
                            .request(
                                hyper::Request::builder()
                                    .method(hyper::Method::POST)
                                    .uri(format!("http://{chosen}/db/execute?transaction"))
                                    .header(hyper::header::CONTENT_TYPE, "application/json")
                                    .body(serde_json::to_vec(&statements)?.into())?,
                            )
                            .await?;

                        Ok::<_, eyre::Report>(res)
                    })
                }
            })
            .try_buffer_unordered(10)
            .try_collect::<Vec<hyper::Response<hyper::Body>>>()
            .await?;
            Ok::<_, eyre::Report>(())
        });

        let ops_count = 4 * count;

        println!("expecting {ops_count} ops");

        let start = Instant::now();

        let mut interval = tokio::time::interval(Duration::from_secs(3));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            println!("checking status after {}s", start.elapsed().as_secs_f32());
            let mut v = vec![];
            for ta in agents.iter() {
                let span = info_span!("consistency", actor_id = %ta.agent.actor_id().0);
                let _entered = span.enter();

                let conn = ta.agent.read_only_pool().get().await?;
                let counts: HashMap<Uuid, i64> = conn
                    .prepare_cached(
                        "SELECT COALESCE(site_id, crsql_siteid()), count(*) FROM crsql_changes GROUP BY site_id;",
                    )?
                    .query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<_>>()?;

                info!("versions count: {counts:?}");

                let actual_count: i64 =
                    conn.query_row("SELECT count(*) FROM crsql_changes;", (), |row| row.get(0))?;
                info!("actual count: {actual_count}");

                let bookie = ta.agent.bookie();

                info!("last version: {:?}", bookie.last(&ta.agent.actor_id()));

                let sync = generate_sync(bookie, ta.agent.actor_id());
                let needed = sync.need_len();

                info!("generated sync: {sync:?}");
                info!("needed: {needed}");

                v.push((counts.values().sum::<i64>(), needed));
            }
            if v.len() != agents.len() {
                println!("got {} actors, expecting {}", v.len(), agents.len());
            }
            if v.len() == agents.len()
                && v.iter().all(|(n, needed)| *n == ops_count && *needed == 0)
            {
                break;
            }

            if start.elapsed() > Duration::from_secs(60) {
                panic!(
                    "failed to disseminate all updates to all nodes in {}s",
                    start.elapsed().as_secs_f32()
                );
            }
        }
        println!("fully disseminated in {}s", start.elapsed().as_secs_f32());

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
