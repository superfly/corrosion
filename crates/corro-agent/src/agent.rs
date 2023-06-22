use std::{
    collections::{BTreeMap, HashMap, HashSet},
    convert::Infallible,
    error::Error,
    fmt, io,
    net::SocketAddr,
    ops::RangeInclusive,
    sync::{atomic::AtomicI64, Arc},
    time::{Duration, Instant},
};

use crate::{
    api::{
        http::{api_v1_db_execute, api_v1_db_query, api_v1_db_schema},
        peer::{peer_api_v1_broadcast, peer_api_v1_sync_post},
        pubsub::api_v1_subscribe_ws,
    },
    broadcast::{runtime_loop, ClientPool, FRAGMENTS_AT},
};

use arc_swap::ArcSwap;
use corro_types::{
    actor::{Actor, ActorId},
    agent::{Agent, AgentInner, Booked, BookedVersions, Bookie, KnownDbVersion},
    broadcast::{
        BroadcastInput, BroadcastSrc, ChangeV1, Changeset, FocaInput, Message, MessageV1, Timestamp,
    },
    change::Change,
    config::{Config, DEFAULT_GOSSIP_PORT},
    filters::{match_expr, AggregateChange},
    members::{MemberEvent, Members},
    pubsub::{SubscriptionEvent, SubscriptionMessage},
    schema::{apply_schema, init_schema},
    sqlite::{init_cr_conn, CrConn, CrConnManager, Migration, SqlitePool},
    sync::{
        generate_sync, SyncMessage, SyncMessageDecodeError, SyncMessageEncodeError, SyncMessageV1,
        SyncStateV1,
    },
};

use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    routing::{get, post},
    BoxError, Extension, Router,
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use foca::{Member, Notification};
use futures::{FutureExt, TryFutureExt, TryStreamExt};
use hyper::{server::conn::AddrIncoming, StatusCode};
use metrics::{counter, gauge, histogram, increment_counter};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rangemap::RangeInclusiveSet;
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use spawn::spawn_counted;
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::mpsc::{channel, Receiver, Sender},
    task::block_in_place,
    time::{sleep, timeout},
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
use uuid::Uuid;

const MAX_SYNC_BACKOFF: Duration = Duration::from_secs(60); // 1 minute oughta be enough, we're constantly getting broadcasts randomly + targetted
const RANDOM_NODES_CHOICES: usize = 10;

pub struct AgentOptions {
    actor_id: ActorId,
    gossip_listener: TcpListener,
    api_listener: TcpListener,
    bootstrap: Vec<String>,
    rx_bcast: Receiver<BroadcastInput>,
    tripwire: Tripwire,
}

pub async fn setup(
    actor_id: ActorId,
    conf: Config,
    tripwire: Tripwire,
) -> eyre::Result<(Agent, AgentOptions)> {
    debug!("setting up corrosion @ {}", conf.db_path);

    if let Some(parent) = conf.db_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    {
        {
            let mut conn = CrConn(Connection::open(&conf.db_path)?);
            init_cr_conn(&mut conn)?;
        }

        let conn = Connection::open(&conf.db_path)?;

        trace!("got actor_id setup conn");
        let crsql_siteid = conn
            .query_row("SELECT site_id FROM __crsql_siteid LIMIT 1;", [], |row| {
                row.get::<_, ActorId>(0)
            })
            .optional()?
            .unwrap_or(ActorId(Uuid::nil()));

        debug!("crsql_siteid as ActorId: {crsql_siteid:?}");

        if crsql_siteid != actor_id {
            warn!(
                "mismatched crsql_siteid {} and actor_id from file {}, override crsql's",
                crsql_siteid, actor_id
            );
            conn.execute(
                "UPDATE __crsql_siteid SET site_id = ?;",
                [actor_id.to_bytes()],
            )?;
        }
    }

    info!("Actor ID: {}", actor_id);

    let rw_pool = bb8::Builder::new()
        .max_size(1)
        .build(CrConnManager::new(&conf.db_path))
        .await?;

    debug!("built RW pool");

    let ro_pool = bb8::Builder::new()
        .max_size(5)
        .min_idle(Some(1))
        .max_lifetime(Some(Duration::from_secs(30)))
        .build_unchecked(CrConnManager::new_read_only(&conf.db_path));
    debug!("built RO pool");

    let schema = {
        let mut conn = rw_pool.get().await?;
        migrate(&mut conn)?;
        let schema = init_schema(&conn)?;
        apply_schema(&mut conn, &conf.schema_paths, &schema)?
    };

    let mut bk: HashMap<ActorId, BookedVersions> = HashMap::new();

    {
        debug!("getting read-only conn for pull bookkeeping rows");
        let conn = ro_pool.get().await?;

        debug!("getting bookkept rows");

        let mut prepped = conn.prepare_cached(
            "SELECT actor_id, start_version, end_version, db_version, last_seq, ts
                FROM __corro_bookkeeping AS bk",
        )?;
        let mut rows = prepped.query([])?;

        loop {
            let row = rows.next()?;
            match row {
                None => break,
                Some(row) => {
                    let ranges = bk.entry(row.get(0)?).or_default();
                    let start_v = row.get(1)?;
                    let end_v: Option<i64> = row.get(2)?;
                    ranges.insert(
                        start_v..=end_v.unwrap_or(start_v),
                        match row.get(3)? {
                            Some(db_version) => KnownDbVersion::Current {
                                db_version,
                                last_seq: row.get(4)?,
                                ts: row.get(5)?,
                            },
                            None => KnownDbVersion::Cleared,
                        },
                    );
                }
            }
        }

        let mut partials: HashMap<(ActorId, i64), (RangeInclusiveSet<i64>, i64, Timestamp)> =
            HashMap::new();

        debug!("getting seq bookkept rows");

        let mut prepped = conn.prepare_cached(
            "SELECT site_id, version, start_seq, end_seq, last_seq, ts
                FROM __corro_seq_bookkeeping",
        )?;
        let mut rows = prepped.query([])?;

        loop {
            let row = rows.next()?;
            match row {
                None => break,
                Some(row) => {
                    let (range, last_seq, ts) =
                        partials.entry((row.get(0)?, row.get(1)?)).or_default();

                    range.insert(row.get(2)?..=row.get(3)?);
                    *last_seq = row.get(4)?;
                    *ts = row.get(5)?;
                }
            }
        }

        debug!("filling up partial known versions");

        for ((actor_id, version), (seqs, last_seq, ts)) in partials {
            let ranges = bk.entry(actor_id).or_default();
            ranges.insert(
                version..=version,
                KnownDbVersion::Partial { seqs, last_seq, ts },
            );
        }
    }

    debug!("done building bookkeeping");

    let bookie = Bookie::new(Arc::new(RwLock::new(
        bk.into_iter()
            .map(|(k, v)| (k, Booked::new(Arc::new(RwLock::new(v)))))
            .collect(),
    )));

    let gossip_listener = TcpListener::bind(conf.gossip_addr).await?;
    let gossip_addr = gossip_listener.local_addr()?;

    let api_listener = TcpListener::bind(conf.api_addr).await?;
    let api_addr = api_listener.local_addr()?;

    let clock = Arc::new(
        uhlc::HLCBuilder::default()
            .with_id(actor_id.0.into())
            .with_max_delta(Duration::from_millis(300))
            .build(),
    );

    let (tx_bcast, rx_bcast) = channel(10240);

    let opts = AgentOptions {
        actor_id,
        gossip_listener,
        api_listener,
        bootstrap: conf.bootstrap.clone(),
        rx_bcast,
        tripwire,
    };

    let agent = Agent(Arc::new(AgentInner {
        actor_id,
        ro_pool,
        rw_pool,
        config: ArcSwap::from_pointee(conf),
        gossip_addr,
        api_addr,
        members: RwLock::new(Members::default()),
        clock: clock.clone(),
        subscribers: Default::default(),
        bookie,
        tx_bcast,
        schema: RwLock::new(schema),
    }));

    Ok((agent, opts))
}

pub async fn start(actor_id: ActorId, conf: Config, tripwire: Tripwire) -> eyre::Result<Agent> {
    let (agent, opts) = setup(actor_id, conf, tripwire.clone()).await?;

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
    } = opts;
    info!("Current Actor ID: {}", actor_id);

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
    let (member_events_tx, member_events_rx) = tokio::sync::broadcast::channel::<MemberEvent>(512);

    runtime_loop(
        Actor::new(actor_id, agent.gossip_addr()),
        agent.clone(),
        udp_gossip.clone(),
        foca_rx,
        rx_bcast,
        member_events_rx.resubscribe(),
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
                        increment_counter!("corro.api.peer.shed.count", "route" => "POST /v1/sync");
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
                        increment_counter!("corro.api.peer.shed.count", "route" => "POST /v1/broadcast");
                        Ok::<_, Infallible>((StatusCode::SERVICE_UNAVAILABLE, "broadcast has reached its concurrency limit".to_string()))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(BufferLayer::new(1024))
                    .layer(ConcurrencyLimitLayer::new(512)),
            ),
        )
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(foca_tx.clone()))
                .layer(Extension(agent.clone()))
                .layer(Extension(bcast_msg_tx.clone()))
                ,
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

    tokio::spawn({
        let self_actor_id = agent.actor_id();
        let rw_pool = agent.read_write_pool().clone();
        let bookie = agent.bookie().clone();
        async move {
            loop {
                sleep(Duration::from_secs(300)).await;

                let to_check: Vec<ActorId> = { bookie.read().keys().copied().collect() };

                for actor_id in to_check {
                    // we need to acquire this first, annoyingly
                    let mut conn = match rw_pool.get().await {
                        Ok(conn) => conn,
                        Err(e) => {
                            error!("could not checkout pooled conn for compacting in-memory actor versions: {e}");
                            continue;
                        }
                    };

                    let booked = bookie.for_actor(actor_id);

                    // get a write lock so nothing else may handle changes while we do this
                    let mut bookedw = booked.write();

                    let versions = bookedw.current_versions();

                    if versions.is_empty() {
                        continue;
                    }

                    let site_id = if actor_id == self_actor_id {
                        None
                    } else {
                        Some(actor_id.to_bytes())
                    };

                    let res = block_in_place(|| {
                        match compact_booked_for_actor(&conn, site_id, &versions) {
                            Ok(to_clear) => {
                                if to_clear.is_empty() {
                                    return Ok(());
                                }
                                let len = to_clear.len();
                                for db_version in to_clear {
                                    if let Some(version) = versions.get(&db_version) {
                                        bookedw.insert(*version, KnownDbVersion::Cleared);
                                    }
                                }
                                info!("compacted in-memory cache by clearing {len} db versions for actor {actor_id}");
                            }
                            Err(e) => {
                                error!("could not compute difference between known live and still alive versions for actor {actor_id}: {e}");
                                return Ok(());
                            }
                        }

                        let tx = conn.transaction()?;

                        let deleted = tx
                            .prepare_cached("DELETE FROM __corro_bookkeeping WHERE actor_id = ?")?
                            .execute([actor_id])?;

                        let mut inserted = 0;

                        for (range, known) in bookedw.inner().iter() {
                            match known {
                                KnownDbVersion::Current {
                                    db_version,
                                    last_seq,
                                    ts,
                                } => {
                                    inserted += tx.prepare_cached("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts) VALUES (?,?,?,?,?)")?.execute(params![actor_id, range.start(), db_version, last_seq, ts])?;
                                }
                                KnownDbVersion::Cleared => {
                                    inserted += tx.prepare_cached("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) VALUES (?,?,?)")?.execute(params![actor_id, range.start(), range.end()])?;
                                }
                                KnownDbVersion::Partial { .. } => {
                                    // do nothing, not stored in that table!
                                }
                            }
                        }

                        tx.commit()?;

                        info!("compacted in-db version state for actor {actor_id}, deleted: {deleted}, inserted: {inserted}");

                        Ok::<_, rusqlite::Error>(())
                    });

                    if let Err(e) = res {
                        error!("could not compact versions for actor {actor_id}: {e}");
                    }
                }
            }
        }
    });

    let states = match agent.read_only_pool().get().await {
        Ok(conn) => {
            match conn.prepare("SELECT foca_state FROM __corro_members") {
                Ok(mut prepped) => {
                    match prepped
                    .query_map([], |row| row.get::<_, String>(0))
                    .and_then(|rows| rows.collect::<rusqlite::Result<Vec<String>>>())
                {
                    Ok(foca_states) => {
                        foca_states.iter().filter_map(|state| match serde_json::from_str(state.as_str()) {
                            Ok(fs) => Some(fs),
                            Err(e) => {
                                error!("could not deserialize foca member state: {e} (json: {state})");
                                None
                            }
                        }).collect::<Vec<Member<Actor>>>()
                    }
                    Err(e) => {
                        error!("could not query for foca member states: {e}");
                        vec![]
                    },
                }
                }
                Err(e) => {
                    error!("could not prepare query for foca member states: {e}");
                    vec![]
                }
            }
        }
        Err(e) => {
            error!("could not acquire conn for foca member states: {e}");
            vec![]
        }
    };

    if !states.is_empty() {
        // let cluster_size = states.len();
        foca_tx.send(FocaInput::ApplyMany(states)).await.ok();
        // foca_tx
        //     .send(FocaInput::ClusterSize(
        //         (cluster_size as u32).try_into().unwrap(),
        //     ))
        //     .await
        //     .ok();
    }

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
        .route(
            "/db/query",
            post(api_v1_db_query).route_layer(
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
        .route(
            "/db/schema",
            post(api_v1_db_schema).route_layer(
                tower::ServiceBuilder::new()
                    .layer(HandleErrorLayer::new(|_error: BoxError| async {
                        Ok::<_, Infallible>((
                            StatusCode::SERVICE_UNAVAILABLE,
                            "max concurrency limit reached".to_string(),
                        ))
                    }))
                    .layer(LoadShedLayer::new())
                    .layer(ConcurrencyLimitLayer::new(4)),
            ),
        )
        .route("/v1/subscribe", get(api_v1_subscribe_ws))
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(Arc::new(AtomicI64::new(0))))
                .layer(Extension(agent.clone()))
                .layer(Extension(tripwire.clone())),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http());

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
        member_events_tx,
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

fn compact_booked_for_actor(
    conn: &Connection,
    site_id: Option<[u8; 16]>,
    versions: &BTreeMap<i64, i64>,
) -> eyre::Result<HashSet<i64>> {
    // TODO: optimize that in a single query once cr-sqlite supports aggregation
    conn.prepare_cached("CREATE TEMP TABLE __temp_count_lookup AS SELECT db_version, site_id FROM crsql_changes WHERE site_id IS ? AND db_version >= ? AND db_version <= ?;")?.execute(params![site_id, versions.first_key_value().map(|(v, _db_v)| *v), versions.last_key_value().map(|(v, _db_v)| *v)])?;

    let still_live: HashSet<i64> = conn
        .prepare_cached("SELECT db_version, count(*) FROM __temp_count_lookup GROUP BY db_version")?
        .query_map((), |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;

    conn.prepare_cached("DROP TABLE __temp_count_lookup;")?
        .execute(())?;

    let keys: HashSet<i64> = versions.keys().copied().collect();

    Ok(keys.difference(&still_live).copied().collect())
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
                    histogram!("corro.gossip.sent.bytes", n as f64, "actor_id" => actor.id().to_string());
                }
                Ok(Err(e)) => {
                    error!("could not send SWIM message via udp to {addr}: {e}");
                }
                Err(_e) => {
                    error!("could not send SWIM message via udp to {addr}: timed out");
                }
            }
        });
        increment_counter!("corro.gossip.send.count", "actor_id" => actor.id().to_string());
    }
}

// async fn handle_one_gossip()

async fn handle_gossip(
    agent: Agent,
    messages: Vec<Message>,
    high_priority: bool,
) -> eyre::Result<()> {
    let priority_label = if high_priority { "high" } else { "normal" };
    counter!("corro.broadcast.recv.count", messages.len() as u64, "priority" => priority_label);

    let mut rebroadcast = vec![];

    for msg in messages {
        if let Some(msg) = process_msg(&agent, msg).await? {
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
    member_events: tokio::sync::broadcast::Sender<MemberEvent>,
) {
    while let Some(notification) = notification_rx.recv().await {
        trace!("handle notification");
        match notification {
            Notification::MemberUp(actor) => {
                let added = { agent.0.members.write().add_member(&actor) };
                info!("Member Up {actor:?} (added: {added})");
                if added {
                    increment_counter!("corro.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually added a member
                    // notify of new cluster size
                    let members_len = { agent.0.members.read().states.len() as u32 };
                    if let Ok(size) = members_len.try_into() {
                        foca_tx.send(FocaInput::ClusterSize(size)).await.ok();
                    }

                    member_events.send(MemberEvent::Up(actor.clone())).ok();
                }
            }
            Notification::MemberDown(actor) => {
                let removed = { agent.0.members.write().remove_member(&actor) };
                info!("Member Down {actor:?} (removed: {removed})");
                if removed {
                    increment_counter!("corro.gossip.member.removed", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually removed a member
                    // notify of new cluster size
                    let member_len = { agent.0.members.read().states.len() as u32 };
                    if let Ok(size) = member_len.try_into() {
                        foca_tx.send(FocaInput::ClusterSize(size)).await.ok();
                    }
                    member_events.send(MemberEvent::Down(actor.clone())).ok();
                }
            }
            Notification::Active => {
                info!("Current node is considered ACTIVE");
            }
            Notification::Idle => {
                warn!("Current node is considered IDLE");
            }
            // this happens when we leave the cluster
            Notification::Defunct => {
                debug!("Current node is considered DEFUNCT");
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
            increment_counter!("corro.db.wal.truncate.busy");
        } else {
            debug!("successfully truncated sqlite WAL!");
            histogram!(
                "corro.db.wal.truncate.seconds",
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
        increment_counter!("corro.payload.recv.count", "kind" => kind.to_string());
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
    histogram!("corro.broadcast.recv.bytes", buf.len() as f64);
    loop {
        // decode a length-delimited "frame"
        match Message::decode(codec, buf) {
            Ok(Some(msg)) => {
                trace!("broadcast: {msg:?}");

                match msg {
                    Message::V1(MessageV1::Change {
                        actor_id,
                        version,
                        changeset,
                    }) => {
                        increment_counter!("corro.broadcast.recv.count", "kind" => "operation");

                        if bookie.contains(actor_id, version, changeset.seqs()) {
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

fn store_empty_changeset(
    tx: Transaction,
    actor_id: ActorId,
    version: i64,
) -> Result<(), rusqlite::Error> {
    // TODO: make sure this makes sense
    tx.prepare_cached(
        "
        INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, ts)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (actor_id, start_version) DO NOTHING;
    ",
    )?
    .execute(params![
        actor_id,
        version,
        rusqlite::types::Null,
        rusqlite::types::Null
    ])?;

    tx.prepare_cached("DELETE FROM __corro_seq_bookkeeping WHERE site_id = ? AND version = ?")?
        .execute(params![actor_id, version,])?;

    tx.prepare_cached("DELETE FROM __corro_buffered_changes WHERE site_id = ? AND version = ?")?
        .execute(params![actor_id, version,])?;

    tx.commit()?;
    return Ok(());
}

async fn process_single_version(
    agent: &Agent,
    change: ChangeV1,
) -> Result<Option<Changeset>, bb8::RunError<bb8_rusqlite::Error>> {
    let bookie = agent.bookie();

    let is_complete = change.is_complete();

    let ChangeV1 {
        actor_id,
        version,
        changeset,
    } = change;

    if bookie.contains(actor_id, version, changeset.seqs()) {
        trace!(
            "already seen this one! from: {}, version: {version}",
            actor_id
        );
        return Ok(None);
    }

    trace!(
        "received {} changes to process from: {actor_id}, version: {version}, seqs: {:?} (last_seq: {:?})",
        changeset.len(),
        changeset.seqs(),
        changeset.last_seq()
    );

    let mut conn = agent.read_write_pool().get().await?;

    let booked = bookie.for_actor(actor_id);
    let (db_version, changeset) = {
        let mut booked_write = booked.write();

        // check again, might've changed since we acquired the lock
        if booked_write.contains(version, changeset.seqs()) {
            trace!("previously unknown versions are now deemed known, aborting inserts");
            return Ok(None);
        }

        let (known_version, changeset, db_version) = block_in_place(move || {
            let tx = conn.transaction()?;

            let (changes, seqs, last_seq, ts) = match changeset.into_parts() {
                None => {
                    store_empty_changeset(tx, actor_id, version)?;
                    return Ok((KnownDbVersion::Cleared, Changeset::Empty, None));
                }
                Some(parts) => parts,
            };

            // if not a full range!
            if !is_complete {
                // debug!(actor = %agent.actor_id(), "changes len: {}", changes.len());
                if changes.len() != 50 {
                    debug!(actor = %agent.actor_id(), "changes len: {}", changes.len());
                }
                let mut inserted = 0;
                for change in changes.iter() {
                    trace!("buffering change! {change:?}");

                    // insert change, do nothing on conflict
                    inserted += tx.prepare_cached(
                        r#"
                            INSERT INTO __corro_buffered_changes
                                ("table", pk, cid, val, col_version, db_version, site_id, seq, version)
                            VALUES
                                (?,       ?,  ?,   ?,   ?,           ?,          ?,       ?,   ?)
                            ON CONFLICT (site_id, db_version, version, seq)
                                DO NOTHING
                        "#,
                    )?
                    .execute(params![
                        change.table.as_str(),
                        change.pk.as_str(),
                        change.cid.as_str(),
                        &change.val,
                        change.col_version,
                        change.db_version,
                        &change.site_id,
                        change.seq,
                        version,
                    ])?;
                }

                if changes.len() != inserted {
                    debug!(actor = %agent.actor_id(), "did not insert as many changes... {inserted}");
                }

                // calculate all known sequences for the actor + version combo
                let mut seqs_recorded: RangeInclusiveSet<i64> = tx
                    .prepare_cached(
                        "
                    SELECT start_seq, end_seq
                        FROM __corro_seq_bookkeeping
                        WHERE site_id = ?
                          AND version = ?
                ",
                    )?
                    .query_map(params![actor_id, version], |row| {
                        Ok(row.get(0)?..=row.get(1)?)
                    })?
                    .collect::<rusqlite::Result<_>>()?;

                // immediately add this new range to the recorded seqs ranges
                seqs_recorded.insert(seqs.clone());

                let full_seqs_range = 0..=last_seq;

                // figure out how many seq gaps we have between 0 and the last seq for this version
                let gaps = seqs_recorded.gaps(&full_seqs_range);
                let seq_gaps: Vec<RangeInclusive<i64>> = gaps.collect();

                trace!(actor = %agent.actor_id(), "still missing seq {seq_gaps:?}");

                // if we have no gaps, then we can apply all these changes.
                let known_version = if seq_gaps.is_empty() {
                    info!(actor = %agent.actor_id(), "moving buffered changes to crsql_changes (actor: {actor_id}, version: {version}, last_seq: {last_seq})");

                    let count: i64 = tx
                        .prepare_cached(
                            "
                            SELECT count(*)
                                FROM __corro_buffered_changes
                                    WHERE site_id = ?
                                      AND version = ?
                                    ",
                        )?
                        .query_row(params![actor_id.as_bytes(), version], |row| row.get(0))?;
                    debug!(actor = %agent.actor_id(), "total buffered rows: {count}");

                    // insert all buffered changes into crsql_changes directly from the buffered changes table
                    let count = tx
                        .prepare_cached(
                            "
                        INSERT INTO crsql_changes
                            SELECT \"table\", pk, cid, val, col_version, db_version, site_id
                                FROM __corro_buffered_changes
                                    WHERE site_id = ?
                                      AND version = ?
                                    ORDER BY db_version ASC, seq ASC
                                    ",
                        )?
                        .execute(params![actor_id.as_bytes(), version])?;
                    info!(actor = %agent.actor_id(), "inserted {count} rows from buffered into crsql_changes");

                    // remove all buffered changes for cleanup purposes
                    let count = tx.prepare_cached(
                        "DELETE FROM __corro_buffered_changes WHERE site_id = ? AND version = ?",
                    )?
                    .execute(params![actor_id.as_bytes(), version])?;
                    info!(actor = %agent.actor_id(), "deleted {count} buffered changes");

                    // delete all bookkept sequences for this version
                    let count = tx
                        .prepare_cached(
                            "DELETE FROM __corro_seq_bookkeeping WHERE site_id = ? AND version = ?",
                        )?
                        .execute(params![actor_id, version])?;
                    info!(actor = %agent.actor_id(), "deleted {count} sequences in bookkeeping");

                    let rows_impacted: i64 = tx
                        .prepare_cached("SELECT crsql_rows_impacted()")?
                        .query_row((), |row| row.get(0))?;

                    info!(actor = %agent.actor_id(), "rows impacted by changes: {rows_impacted}");

                    if rows_impacted > 0 {
                        let db_version: i64 =
                            tx.query_row("SELECT crsql_nextdbversion()", (), |row| row.get(0))?;
                        debug!("db version: {db_version}");
                        KnownDbVersion::Current {
                            db_version,
                            last_seq,
                            ts,
                        }
                    } else {
                        KnownDbVersion::Cleared
                    }
                } else {
                    tx.prepare_cached(
                        "DELETE FROM __corro_seq_bookkeeping WHERE site_id = ? AND version = ?",
                    )?
                    .execute(params![actor_id, version])?;

                    for range in seqs_recorded.iter() {
                        tx.prepare_cached("INSERT INTO __corro_seq_bookkeeping (site_id, version, start_seq, end_seq, last_seq, ts)
                        VALUES (?, ?, ?, ?, ?, ?)
                            ",
                        )?
                        .execute(params![
                            actor_id,
                            version,
                            range.start(),
                            range.end(),
                            last_seq,
                            ts
                        ])?;
                    }

                    KnownDbVersion::Partial {
                        seqs: seqs_recorded,
                        last_seq,
                        ts,
                    }
                };

                tx.commit()?;

                return Ok((
                    known_version,
                    Changeset::Full {
                        changes,
                        seqs,
                        last_seq,
                        ts,
                    },
                    None,
                ));
            }

            let db_version: i64 =
                tx.query_row("SELECT crsql_nextdbversion()", (), |row| row.get(0))?;

            let mut impactful_changeset = vec![];

            for change in changes {
                trace!("inserting change! {change:?}");
                tx.prepare_cached(
                    r#"
                        INSERT INTO crsql_changes
                            ("table", pk, cid, val, col_version, db_version, seq, site_id)
                        VALUES
                            (?,       ?,  ?,   ?,   ?,           ?,          ?,    ?)
                    "#,
                )?
                .execute(params![
                    change.table.as_str(),
                    change.pk.as_str(),
                    change.cid.as_str(),
                    &change.val,
                    change.col_version,
                    change.db_version,
                    change.seq,
                    &change.site_id
                ])?;
                let rows_impacted: i64 = tx
                    .prepare_cached("SELECT crsql_rows_impacted()")?
                    .query_row((), |row| row.get(0))?;

                debug!(actor = %agent.actor_id(), "inserted {rows_impacted} into crsql_changes");
                if rows_impacted > 0 {
                    impactful_changeset.push(change);
                }
            }

            debug!(
                "inserting bookkeeping row: {}, start: {}, end: {:?}, ts: {:?}",
                actor_id, version, db_version, ts
            );

            tx.prepare_cached("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts) VALUES (?, ?, ?, ?, ?);")?.execute(params![actor_id, version, db_version, last_seq, ts])?;

            let (known_version, new_changeset, db_version) = if impactful_changeset.is_empty() {
                (KnownDbVersion::Cleared, Changeset::Empty, None)
            } else {
                (
                    KnownDbVersion::Current {
                        db_version,
                        last_seq,
                        ts,
                    },
                    Changeset::Full {
                        changes: impactful_changeset,
                        seqs,
                        last_seq,
                        ts,
                    },
                    Some(db_version),
                )
            };

            tx.commit()?;

            Ok::<_, bb8_rusqlite::Error>((known_version, new_changeset, db_version))
        })?;

        booked_write.insert(version, known_version);
        (db_version, changeset)
    };

    if let Some(db_version) = db_version {
        process_subs(agent, changeset.changes(), db_version);
    }

    Ok(Some(changeset))
}

async fn process_msg(
    agent: &Agent,
    msg: Message,
) -> Result<Option<Message>, bb8::RunError<bb8_rusqlite::Error>> {
    Ok(match msg {
        Message::V1(MessageV1::Change {
            actor_id,
            version,
            changeset,
        }) => {
            let changeset = process_single_version(
                agent,
                ChangeV1 {
                    actor_id,
                    version,
                    changeset,
                },
            )
            .await?;

            changeset.map(|changeset| {
                Message::V1(MessageV1::Change {
                    actor_id,
                    version,
                    changeset,
                })
            })
        }
    })
}

pub fn process_subs(agent: &Agent, changeset: &[Change], db_version: i64) {
    trace!("process subs...");
    let schema = agent.0.schema.read();
    let aggs = AggregateChange::from_changes(changeset.iter(), &schema, db_version);
    trace!("agg changes: {aggs:?}");
    for agg in aggs {
        let subscribers = agent.subscribers().read();
        trace!("subs: {subscribers:?}");
        for (sub, subscriptions) in subscribers.iter() {
            trace!("looking at sub {sub}");
            let subs = subscriptions.read();
            if let Some((subs, sender)) = subs.as_local() {
                for (id, info) in subs.iter() {
                    if let Some(filter) = info.filter.as_ref() {
                        if match_expr(filter, &agg) {
                            trace!("matched subscriber: {id} w/ info: {info:?}!");
                            if let Err(e) = sender.send(SubscriptionMessage::Event {
                                id: id.clone(),
                                event: SubscriptionEvent::Change(agg.to_owned()),
                            }) {
                                error!("could not send sub message: {e}")
                            }
                        }
                    }
                }
            }
        }
    }
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
    Decoded(#[from] SyncMessageDecodeError),
    #[error("could not encode message: {0}")]
    Encoded(#[from] SyncMessageEncodeError),
    #[error("unexpected sync message")]
    UnexpectedSyncMessage,
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
    let sync_state = generate_sync(agent.bookie(), agent.actor_id());
    for (actor_id, needed) in sync_state.need.iter() {
        gauge!("corro.sync.client.needed", needed.len() as f64, "actor_id" => actor_id.0.to_string());
    }
    for (actor_id, version) in sync_state.heads.iter() {
        gauge!("corro.sync.client.head", *version as f64, "actor_id" => actor_id.to_string());
    }

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
                sync_state
                    .need_len_for_actor(&b.0)
                    .cmp(&sync_state.need_len_for_actor(&a.0))
            });

            if let Some(chosen) = choices.get(0).cloned() {
                chosen
            } else {
                return Err(SyncClientError::NoGoodCandidate);
            }
        };

        info!(
            "syncing {} with: {}, need len: {}",
            agent.actor_id(),
            actor_id,
            sync_state.need_len(),
        );

        debug!(actor = %agent.actor_id(), "sync message: {sync_state:?}");

        let start = Instant::now();
        let res = handle_sync_receive(
            agent,
            client,
            &SyncWith { actor_id, addr },
            sync_state.clone(),
        )
        .await;

        match res {
            Ok(n) => {
                let elapsed = start.elapsed();
                info!(
                    "synced {n} changes w/ {} in {}s @ {} changes/s",
                    actor_id,
                    elapsed.as_secs_f64(),
                    n as f64 / elapsed.as_secs_f64()
                );
                return Ok(());
            }
            Err(e) => {
                if e.is_unavailable() {
                    increment_counter!("corro.sync.client.busy.servers");
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
    sync: SyncStateV1,
) -> Result<usize, SyncClientError> {
    let SyncWith { actor_id, addr } = with;
    let actor_id = *actor_id;
    // println!("syncing with {actor_id:?}");

    increment_counter!("corro.sync.client.member", "id" => actor_id.0.to_string(), "addr" => addr.to_string());

    histogram!(
        "corro.sync.client.request.operations.need.count",
        sync.need.len() as f64
    );

    let sync_msg: SyncMessage = sync.into();
    let mut buf = BytesMut::new();

    sync_msg.encode(&mut buf)?;
    let data = buf.split().freeze();

    gauge!("corro.sync.client.request.size.bytes", data.len() as f64);

    let req = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(format!("http://{addr}/v1/sync"))
        .header(hyper::header::CONTENT_TYPE, "application/speedy")
        .header(hyper::header::ACCEPT, "application/octet-stream")
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

    increment_counter!("corro.sync.client.request.count", "id" => actor_id.0.to_string(), "addr" => addr.to_string());

    let start = Instant::now();
    let res = match timeout(Duration::from_secs(15), client.request(req)).await {
        Ok(Ok(res)) => {
            histogram!("corro.sync.client.response.time.seconds", start.elapsed().as_secs_f64(), "id" => actor_id.0.to_string(), "addr" => addr.to_string(), "status" => res.status().to_string());
            res
        }
        Ok(Err(e)) => {
            increment_counter!("corro.sync.client.request.error", "id" => actor_id.0.to_string(), "addr" => addr.to_string(), "error" => e.to_string());
            return Err(e.into());
        }
        Err(_e) => {
            increment_counter!("corro.sync.client.request.error", "id" => actor_id.0.to_string(), "addr" => addr.to_string(), "error" => "timed out waiting for headers");
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
        .inspect_ok(|b| counter!("corro.sync.client.chunk.recv.bytes", b.len() as u64));

    let mut count = 0;

    while let Some(buf_res) = framed.next().await {
        let mut buf = buf_res?;
        let msg = SyncMessage::from_buf(&mut buf)?;
        let len = match msg {
            SyncMessage::V1(SyncMessageV1::Changeset(change)) => {
                let len = change.len();
                process_single_version(agent, change).await?;
                len
            }
            SyncMessage::V1(SyncMessageV1::State(_)) => {
                return Err(SyncClientError::UnexpectedSyncMessage);
            }
        };
        count += len;
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

pub fn migrate(conn: &mut CrConn) -> rusqlite::Result<()> {
    let migrations: Vec<Box<dyn Migration>> = vec![Box::new(
        init_migration as fn(&Transaction) -> rusqlite::Result<()>,
    )];

    corro_types::sqlite::migrate(conn, migrations)
}

fn init_migration(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        r#"
            -- internal bookkeeping
            CREATE TABLE __corro_bookkeeping (
                actor_id TEXT NOT NULL,
                start_version INTEGER NOT NULL,
                end_version INTEGER,
                db_version INTEGER,

                last_seq INTEGER,

                ts TEXT,

                PRIMARY KEY (actor_id, start_version)
            ) WITHOUT ROWID;

            -- internal per-db-version seq bookkeeping
            CREATE TABLE __corro_seq_bookkeeping (
                -- remote actor / site id
                site_id BLOB NOT NULL,
                -- remote internal version
                version INTEGER NOT NULL,
                
                -- start and end seq for this bookkept record
                start_seq INTEGER NOT NULL,
                end_seq INTEGER NOT NULL,

                last_seq INTEGER NOT NULL,

                -- timestamp, need to propagate...
                ts TEXT NOT NULL,

                PRIMARY KEY (site_id, version, start_seq)
            ) WITHOUT ROWID;

            -- buffered changes (similar schema as crsql_changes)
            CREATE TABLE __corro_buffered_changes (
                "table" TEXT NOT NULL,
                pk TEXT NOT NULL,
                cid TEXT NOT NULL,
                val ANY, -- shouldn't matter I don't think
                col_version INTEGER NOT NULL,
                db_version INTEGER NOT NULL,
                site_id BLOB NOT NULL, -- this differs from crsql_changes, we'll never buffer our own
                seq INTEGER NOT NULL,

                version INTEGER NOT NULL,

                PRIMARY KEY (site_id, db_version, version, seq)
            ) WITHOUT ROWID;
            
            -- SWIM memberships
            CREATE TABLE __corro_members (
                id TEXT PRIMARY KEY NOT NULL,
                address TEXT NOT NULL,
            
                state TEXT NOT NULL DEFAULT 'down',
            
                foca_state JSON
            ) WITHOUT ROWID;

            -- tracked corrosion schema, useful for non-file schema changes
            CREATE TABLE __corro_schema (
                tbl_name TEXT NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                sql TEXT NOT NULL,
            
                source TEXT NOT NULL,
            
                PRIMARY KEY (tbl_name, type, name)
            ) WITHOUT ROWID;

            -- all subscriptions ever
            CREATE TABLE __corro_subs (
                actor_id TEXT NOT NULL,
                id TEXT NOT NULL,
            
                filter TEXT NOT NULL DEFAULT "",
                active INTEGER NOT NULL DEFAULT 1,

                ts TEXT,
            
                PRIMARY KEY (actor_id, id)
            ) WITHOUT ROWID;

            -- that's how we'll propagate subscription changes
            SELECT crsql_as_crr('__corro_subs');
        "#,
    )?;

    Ok(())
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        time::{Duration, Instant},
    };

    use futures::{SinkExt, StreamExt, TryStreamExt};
    use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
    use serde::Deserialize;
    use serde_json::json;
    use spawn::wait_for_all_pending_handles;
    use tokio::time::{sleep, MissedTickBehavior};
    use tokio_tungstenite::tungstenite::Message;
    use tracing::{info, info_span};
    use tripwire::Tripwire;

    use crate::api::http::make_broadcastable_changes;

    use super::*;

    use corro_types::{
        agent::reload,
        api::{RqliteResponse, Statement},
        change::SqliteValue,
        filters::{ChangeEvent, OwnedAggregateChange},
        pubsub::{Subscription, SubscriptionId},
        sqlite::CrConnManager,
    };

    use corro_tests::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn insert_rows_and_gossip() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
        let ta2 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                    .build()
            },
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

        let res = timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/db/execute", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

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

        let res = client
            .request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/db/execute", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            )
            .await?;

        let body: RqliteResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");

        let bk: Vec<(ActorId, i64, i64)> = ta1
            .agent
            .read_only_pool()
            .get()
            .await?
            .prepare("SELECT actor_id, start_version, db_version FROM __corro_bookkeeping")?
            .query_map((), |row| {
                Ok((
                    row.get::<_, ActorId>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;

        assert_eq!(
            bk,
            vec![(ta1.agent.actor_id(), 1, 1), (ta1.agent.actor_id(), 2, 2)]
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

        let values: Vec<serde_json::Value> = (3..1000)
            .map(|id| {
                serde_json::json!([
                    "INSERT INTO tests (id,text) VALUES (?,?)",
                    id,
                    format!("hello world #{id}"),
                ])
            })
            .collect();

        let req_body: Vec<Statement> = serde_json::from_value(json!(values))?;

        let res = timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/db/execute", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

        let body: RqliteResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        let dbversion: i64 = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT crsql_dbversion();",
            (),
            |row| row.get(0),
        )?;
        assert_eq!(dbversion, 3);

        println!("body: {body:?}");

        let expected_count: i64 = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT COUNT(*) FROM tests",
            (),
            |row| row.get(0),
        )?;

        sleep(Duration::from_secs(2)).await;

        let got_count: i64 = ta2.agent.read_only_pool().get().await?.query_row(
            "SELECT COUNT(*) FROM tests",
            (),
            |row| row.get(0),
        )?;

        assert_eq!(expected_count, got_count);

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
                    for (n, gossip_addr) in to_launch {
                        println!("LAUNCHING AGENT #{n}");
                        let mut rng = StdRng::from_entropy();
                        let bootstrap = agents
                            .iter()
                            .map(|ta| ta.agent.gossip_addr())
                            .choose_multiple(&mut rng, 10);
                        agents.push(
                            launch_test_agent(
                                |conf| {
                                    conf.gossip_addr(gossip_addr)
                                        .bootstrap(
                                            bootstrap
                                                .iter()
                                                .map(SocketAddr::to_string)
                                                .collect::<Vec<String>>(),
                                        )
                                        .build()
                                },
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

        let addrs: Vec<SocketAddr> = agents.iter().map(|ta| ta.agent.api_addr()).collect();

        let count = 200;

        let iter = (0..count).flat_map(|n| {
            serde_json::from_value::<Vec<Statement>>(json!([
                [
                    "INSERT INTO tests (id,text) VALUES (?,?)",
                    n,
                    format!("hello world {n}")
                ],
                [
                    "INSERT INTO tests2 (id,text) VALUES (?,?)",
                    n,
                    format!("hello world {n}")
                ],
                [
                    "INSERT INTO tests (id,text) VALUES (?,?)",
                    n + 10000,
                    format!("hello world {n}")
                ],
                [
                    "INSERT INTO tests2 (id,text) VALUES (?,?)",
                    n + 10000,
                    format!("hello world {n}")
                ]
            ]))
            .unwrap()
        });

        tokio::spawn(async move {
            tokio_stream::StreamExt::map(futures::stream::iter(iter).chunks(20), {
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
                let counts: HashMap<ActorId, i64> = conn
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn schema_application() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir()?;
        let schema_path = dir.path().join("schema");
        tokio::fs::create_dir_all(&schema_path).await?;

        tokio::fs::write(schema_path.join("test.sql"), corro_tests::TEST_SCHEMA).await?;

        let pool = bb8::Pool::builder()
            .max_size(1)
            .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite")));

        let schema = {
            let mut conn = pool.get().await?;
            migrate(&mut conn)?;
            let schema = init_schema(&conn)?;
            apply_schema(&mut conn, &[&schema_path], &schema)?
        };

        println!("initial schema: {schema:#?}");

        println!("SECOND ATTEMPT ------");

        tokio::fs::remove_file(schema_path.join("test.sql")).await?;

        tokio::fs::write(
            schema_path.join("consul.sql"),
            r#"

        CREATE TABLE IF NOT EXISTS tests (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT "",
            meta JSON NOT NULL DEFAULT '{}',

            app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)),
            instance_id TEXT AS (
                COALESCE(
                    JSON_EXTRACT(meta, '$.machine_id'),
                    SUBSTR(JSON_EXTRACT(meta, '$.alloc_id'), 1, 8),
                    CASE
                        WHEN INSTR(id, '_nomad-task-') = 1 THEN SUBSTR(id, 13, 8)
                        ELSE NULL
                    END
                )
            )
        ) WITHOUT ROWID;


        CREATE TABLE IF NOT EXISTS tests2 (
            id INTEGER NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS testsblob (
            id BLOB NOT NULL PRIMARY KEY,
            text TEXT NOT NULL DEFAULT ""
        ) WITHOUT ROWID;
        
        
        CREATE TABLE IF NOT EXISTS consul_checks (
            node TEXT NOT NULL DEFAULT "",
            id TEXT NOT NULL DEFAULT "",
            service_id TEXT NOT NULL DEFAULT "",
            service_name TEXT NOT NULL DEFAULT "",
            name TEXT NOT NULL DEFAULT "",
            status TEXT,
            output TEXT NOT NULL DEFAULT "",
            updated_at BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (node, id)
        ) WITHOUT ROWID;
        
        CREATE INDEX consul_checks_node_id_updated_at ON consul_checks (node, id, updated_at);

        "#,
        )
        .await?;

        let _new_schema = {
            let mut conn = pool.get().await?;
            apply_schema(&mut conn, &[schema_path], &schema)?
        };

        // println!("new schema: {new_schema:#?}");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn basic_pubsub() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta = launch_test_agent(
            |conf| conf.api_addr("127.0.0.1:0".parse().unwrap()).build(),
            tripwire.clone(),
        )
        .await?;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("ws://{}/v1/subscribe", ta.agent.api_addr()))
                .await?;

        let id = SubscriptionId("blah".into());

        ws.send(Message::binary(
            serde_json::to_vec(&Subscription::Add {
                id: id.clone(),
                where_clause: Some("tbl_name = 'testsblob'".into()),
                from_db_version: None,
                is_priority: true,
            })
            .unwrap(),
        ))
        .await?;

        println!("sent message!");

        make_broadcastable_changes(&ta.agent, |tx| {
            tx.execute(
                "INSERT INTO testsblob (id,text) VALUES (?,?)",
                params![&[1u8, 2u8, 3u8], "hello"],
            )?;
            Ok(())
        })
        .await
        .unwrap();

        let msg = ws.next().await.unwrap().unwrap();

        let parsed: SubscriptionMessage = serde_json::from_slice(&msg.into_data()).unwrap();

        println!("{parsed:#?}");

        assert_eq!(
            parsed,
            SubscriptionMessage::Event {
                id,
                event: SubscriptionEvent::Change(OwnedAggregateChange {
                    actor_id: ta.agent.actor_id(),
                    version: 1,
                    evt_type: ChangeEvent::Insert,
                    table: "testsblob".into(),
                    pk: vec![(String::from("id"), SqliteValue::Blob(vec![1, 2, 3]))]
                        .into_iter()
                        .collect(),
                    data: vec![(String::from("text"), SqliteValue::Text("hello".into()))]
                        .into_iter()
                        .collect(),
                })
            }
        );

        let conn = ta.agent.read_only_pool().get().await?;

        let changes = conn
                    .prepare_cached(
                        r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()) FROM crsql_changes"#,
                    )?
                    .query_map([], |row| {
                        Ok(Change {
                            table: row.get(0)?,
                            pk: row.get(1)?,
                            cid: row.get(2)?,
                            val: row.get(3)?,
                            col_version: row.get(4)?,
                            db_version: row.get(5)?,
                            seq: row.get(6)?,
                            site_id: row.get(7)?,
                        })
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;

        println!("changes: {changes:#?}");

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn reloading() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta = launch_test_agent(
            |conf| conf.api_addr("127.0.0.1:0".parse().unwrap()).build(),
            tripwire.clone(),
        )
        .await?;

        let dir = tempfile::tempdir()?;
        tokio::fs::write(
            dir.path().join("test2.sql"),
            br#"CREATE TABLE IF NOT EXISTS consul_checks (
            node TEXT NOT NULL DEFAULT "",
            id TEXT NOT NULL DEFAULT "",
            service_id TEXT NOT NULL DEFAULT "",
            service_name TEXT NOT NULL DEFAULT "",
            name TEXT NOT NULL DEFAULT "",
            status TEXT,
            output TEXT NOT NULL DEFAULT "",
            updated_at BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (node, id)
        ) WITHOUT ROWID;
        
        CREATE INDEX consul_checks_node_id_updated_at ON consul_checks (node, id, updated_at);"#,
        )
        .await?;

        let mut conf: Config = ta.agent.config().as_ref().clone();
        conf.schema_paths
            .push(dir.path().display().to_string().into());

        assert!(ta
            .agent
            .0
            .schema
            .read()
            .tables
            .get("consul_checks")
            .is_none());

        reload(&ta.agent, conf).await?;

        assert!(ta
            .agent
            .0
            .schema
            .read()
            .tables
            .get("consul_checks")
            .is_some());

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }

    #[test]
    fn test_in_memory_versions_compaction() -> eyre::Result<()> {
        let mut conn = rusqlite::Connection::open_in_memory()?;

        init_cr_conn(&mut conn)?;

        conn.execute_batch(
            "CREATE TABLE foo (a INTEGER PRIMARY KEY, b INTEGER); SELECT crsql_as_crr('foo');",
        )?;

        // db version 1
        conn.execute("INSERT INTO foo (a) VALUES (1)", ())?;
        // db version 2
        conn.execute("UPDATE foo SET b = 2 WHERE a = 1;", ())?;

        let db_version: i64 = conn.query_row(
            "SELECT db_version FROM crsql_changes WHERE site_id IS NULL",
            (),
            |row| row.get(0),
        )?;

        assert_eq!(db_version, 2);

        let diff = compact_booked_for_actor(&conn, None, &vec![(1, 1)].into_iter().collect())?;

        assert!(diff.contains(&1));
        assert!(!diff.contains(&2));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn large_tx_sync() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

        let client = hyper::Client::builder()
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(300))
            .build_http::<hyper::Body>();

        let req_body: Vec<Statement> = serde_json::from_value(json!(["INSERT INTO tests  WITH RECURSIVE    cte(id) AS (       SELECT random()       UNION ALL       SELECT random()         FROM cte        LIMIT 100000  ) SELECT id, \"hello\" as text FROM cte;"]))?;

        let res = timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/db/execute", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

        let body: RqliteResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");

        let dbversion: i64 = ta1.agent.read_only_pool().get().await?.query_row(
            "SELECT crsql_dbversion();",
            (),
            |row| row.get(0),
        )?;
        assert_eq!(dbversion, 1);

        sleep(Duration::from_secs(5)).await;

        let ta2 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;
        let ta3 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta2.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;
        let ta4 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta3.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        sleep(Duration::from_secs(20)).await;

        {
            let conn = ta2.agent.read_only_pool().get().await?;

            let count: i64 = conn
                .prepare_cached("SELECT COUNT(*) FROM tests;")?
                .query_row((), |row| row.get(0))?;

            assert_eq!(
                count,
                100000,
                "actor {} did not reach 100K rows",
                ta2.agent.actor_id()
            );
        }

        {
            let conn = ta3.agent.read_only_pool().get().await?;

            let count: i64 = conn
                .prepare_cached("SELECT COUNT(*) FROM tests;")?
                .query_row((), |row| row.get(0))?;

            assert_eq!(
                count,
                100000,
                "actor {} did not reach 100K rows",
                ta3.agent.actor_id()
            );
        }
        {
            let conn = ta4.agent.read_only_pool().get().await?;

            let count: i64 = conn
                .prepare_cached("SELECT COUNT(*) FROM tests;")?
                .query_row((), |row| row.get(0))?;

            assert_eq!(
                count,
                100000,
                "actor {} did not reach 100K rows",
                ta4.agent.actor_id()
            );
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
