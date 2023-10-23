use std::{
    cmp,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    convert::Infallible,
    net::SocketAddr,
    ops::RangeInclusive,
    sync::{atomic::AtomicI64, Arc},
    time::{Duration, Instant},
};

use crate::{
    api::{
        peer::{gossip_server_endpoint, parallel_sync, serve_sync, SyncError},
        public::{
            api_v1_db_schema, api_v1_queries, api_v1_transactions,
            pubsub::{
                api_v1_sub_by_id, api_v1_subs, process_sub_channel, MatcherBroadcastCache,
                MatcherIdCache,
            },
        },
    },
    broadcast::runtime_loop,
    transport::{Transport, TransportError},
};

use arc_swap::ArcSwap;
use corro_types::{
    actor::{Actor, ActorId},
    agent::{
        Agent, AgentConfig, BookedVersions, Bookie, ChangeError, KnownDbVersion, PartialVersion,
        SplitPool,
    },
    broadcast::{
        BiPayload, BiPayloadV1, BroadcastInput, BroadcastV1, ChangeSource, ChangeV1, Changeset,
        ChangesetParts, FocaInput, Timestamp, UniPayload, UniPayloadV1,
    },
    change::Change,
    config::{AuthzConfig, Config, DEFAULT_GOSSIP_PORT},
    members::{MemberEvent, Members, Rtt},
    pubsub::{migrate_subs, Matcher},
    schema::init_schema,
    sqlite::{CrConn, Migration, SqlitePoolError},
    sync::{generate_sync, SyncMessageDecodeError, SyncMessageEncodeError},
};

use axum::{
    error_handling::HandleErrorLayer,
    extract::DefaultBodyLimit,
    headers::{authorization::Bearer, Authorization},
    routing::{get, post},
    BoxError, Extension, Router, TypedHeader,
};
use bytes::Bytes;
use foca::{Member, Notification};
use futures::{FutureExt, StreamExt};
use hyper::{server::conn::AddrIncoming, StatusCode};
use itertools::Itertools;
use metrics::{counter, gauge, histogram, increment_counter};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{named_params, params, Connection, OptionalExtension, Transaction};
use spawn::spawn_counted;
use speedy::Readable;
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
    task::block_in_place,
    time::{error::Elapsed, sleep, timeout},
};
use tokio_stream::{wrappers::ReceiverStream, StreamExt as TokioStreamExt};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};
use tower::{limit::ConcurrencyLimitLayer, load_shed::LoadShedLayer};
use tower_http::trace::TraceLayer;
use tracing::{debug, debug_span, error, info, trace, warn, Instrument};
use tripwire::{Outcome, PreemptibleFutureExt, TimeoutFutureExt, Tripwire};
use trust_dns_resolver::{
    error::ResolveErrorKind,
    proto::rr::{RData, RecordType},
};

const MAX_SYNC_BACKOFF: Duration = Duration::from_secs(15); // 1 minute oughta be enough, we're constantly getting broadcasts randomly + targetted
const RANDOM_NODES_CHOICES: usize = 10;
const COMPACT_BOOKED_INTERVAL: Duration = Duration::from_secs(300);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(300);

pub struct AgentOptions {
    pub actor_id: ActorId,
    pub gossip_server_endpoint: quinn::Endpoint,
    pub transport: Transport,
    pub api_listener: TcpListener,
    pub rx_bcast: Receiver<BroadcastInput>,
    pub rx_apply: Receiver<(ActorId, i64)>,
    pub rx_empty: Receiver<(ActorId, RangeInclusive<i64>)>,
    pub rx_changes: Receiver<(ChangeV1, ChangeSource)>,
    pub rx_foca: Receiver<FocaInput>,
    pub rtt_rx: Receiver<(SocketAddr, Duration)>,
    pub tripwire: Tripwire,
}

pub async fn setup(conf: Config, tripwire: Tripwire) -> eyre::Result<(Agent, AgentOptions)> {
    debug!("setting up corrosion @ {}", conf.db.path);

    if let Some(parent) = conf.db.path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // do this early to error earlier
    let members = Members::default();

    let actor_id = {
        let conn = CrConn::init(Connection::open(&conf.db.path)?)?;
        conn.query_row("SELECT crsql_site_id();", [], |row| {
            row.get::<_, ActorId>(0)
        })?
    };

    info!("Actor ID: {}", actor_id);

    let subscriptions_db_path = conf.db.subscriptions_db_path();

    if let Some(parent) = subscriptions_db_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let pool = SplitPool::create(&conf.db.path, &subscriptions_db_path, tripwire.clone()).await?;

    let schema = {
        let mut conn = pool.write_priority().await?;
        migrate(&mut conn)?;
        init_schema(&conn)?
    };

    {
        let mut conn = rusqlite::Connection::open(&subscriptions_db_path)?;
        conn.execute_batch(
            r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
            "#,
        )?;
        migrate_subs(&mut conn)?;
    }

    let (tx_apply, rx_apply) = channel(10240);

    let mut bk: HashMap<ActorId, BookedVersions> = HashMap::new();

    {
        debug!("getting read-only conn for pull bookkeeping rows");
        let conn = pool.read().await?;

        debug!("getting bookkept rows");

        let mut prepped = conn.prepare_cached(
            "SELECT actor_id, start_version, end_version, db_version, last_seq, ts
                FROM __corro_bookkeeping",
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
                    ranges.insert_many(
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
            debug!(%actor_id, version, "looking at partials (seq: {seqs:?}, last_seq: {last_seq})");
            let ranges = bk.entry(actor_id).or_default();

            if let Some(known) = ranges.get(&version) {
                warn!(%actor_id, version, "found partial data that has been applied, clearing buffered meta, known: {known:?}");

                let mut conn = pool.write_priority().await?;
                let tx = conn.transaction()?;
                clear_buffered_meta(&tx, actor_id, version..=version)?;
                tx.commit()?;
                continue;
            }

            let gaps_count = seqs.gaps(&(0..=last_seq)).count();
            ranges.insert(version, KnownDbVersion::Partial { seqs, last_seq, ts });

            if gaps_count == 0 {
                info!(%actor_id, version, "found fully buffered, unapplied, changes! scheduling apply");
                tx_apply.send((actor_id, version)).await?;
            }
        }
    }

    debug!("done building bookkeeping");

    let bookie = Bookie::new(bk);

    let gossip_server_endpoint = gossip_server_endpoint(&conf.gossip).await?;
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    let (rtt_tx, rtt_rx) = channel(128);

    let transport = Transport::new(&conf.gossip, rtt_tx).await?;

    let api_listener = TcpListener::bind(conf.api.bind_addr).await?;
    let api_addr = api_listener.local_addr()?;

    let clock = Arc::new(
        uhlc::HLCBuilder::default()
            .with_id(actor_id.try_into().unwrap())
            .with_max_delta(Duration::from_millis(300))
            .build(),
    );

    let (tx_bcast, rx_bcast) = channel(10240);
    let (tx_empty, rx_empty) = channel(10240);
    let (tx_changes, rx_changes) = channel(5192);
    let (tx_foca, rx_foca) = channel(10240);

    let opts = AgentOptions {
        actor_id,
        gossip_server_endpoint,
        transport,
        api_listener,
        rx_bcast,
        rx_apply,
        rx_empty,
        rx_changes,
        rx_foca,
        rtt_rx,
        tripwire: tripwire.clone(),
    };

    let agent = Agent::new(AgentConfig {
        actor_id,
        pool,
        gossip_addr,
        api_addr,
        members: RwLock::new(members),
        config: ArcSwap::from_pointee(conf),
        clock,
        bookie,
        tx_bcast,
        tx_apply,
        tx_empty,
        tx_changes,
        tx_foca,
        schema: RwLock::new(schema),
        tripwire,
    });

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
        gossip_server_endpoint,
        transport,
        api_listener,
        mut tripwire,
        rx_bcast,
        rx_apply,
        rx_empty,
        rx_changes,
        rx_foca,
        rtt_rx,
    } = opts;

    let mut matcher_id_cache = MatcherIdCache::default();
    let mut matcher_bcast_cache = MatcherBroadcastCache::default();

    {
        // open database and set its journal to WAL
        let rows = {
            let subscriptions_db_path = agent.config().db.subscriptions_db_path();
            let mut conn = rusqlite::Connection::open(&subscriptions_db_path)?;
            conn.execute_batch(
                r#"
                    PRAGMA journal_mode = WAL;
                    PRAGMA synchronous = NORMAL;
                "#,
            )?;

            migrate_subs(&mut conn)?;

            let res = conn
                .prepare("SELECT id, sql FROM subs")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<Vec<(uuid::Uuid, String)>>>()?;

            // the let is required or else we get a lifetime error
            #[allow(clippy::let_and_return)]
            res
        };

        for (id, sql) in rows {
            let conn = agent.pool().dedicated().await?;
            let (evt_tx, evt_rx) = channel(512);
            match Matcher::restore(id, &agent.schema().read(), conn, evt_tx, &sql) {
                Ok(handle) => {
                    agent.matchers().write().insert(id, handle);
                    let (sub_tx, _) = tokio::sync::broadcast::channel(10240);
                    tokio::spawn(process_sub_channel(
                        agent.clone(),
                        id,
                        sub_tx.clone(),
                        evt_rx,
                    ));
                    matcher_id_cache.insert(sql, id);
                    matcher_bcast_cache.insert(id, sub_tx);
                }
                Err(e) => {
                    error!("could not restore subscription {id}: {e}");
                }
            }
        }
    };

    let matcher_id_cache = Arc::new(tokio::sync::RwLock::new(matcher_id_cache));
    let matcher_bcast_cache = Arc::new(tokio::sync::RwLock::new(matcher_bcast_cache));

    let (to_send_tx, to_send_rx) = channel(10240);
    let (notifications_tx, notifications_rx) = channel(10240);

    let (bcast_msg_tx, bcast_rx) = channel::<BroadcastV1>(10240);

    let gossip_addr = gossip_server_endpoint.local_addr()?;

    let (member_events_tx, member_events_rx) = tokio::sync::broadcast::channel::<MemberEvent>(512);

    runtime_loop(
        Actor::new(
            actor_id,
            agent.gossip_addr(),
            agent.clock().new_timestamp().into(),
        ),
        agent.clone(),
        transport.clone(),
        rx_foca,
        rx_bcast,
        member_events_rx.resubscribe(),
        to_send_tx,
        notifications_tx,
        tripwire.clone(),
    );

    tokio::spawn({
        let agent = agent.clone();
        async move {
            let stream = ReceiverStream::new(rtt_rx);
            // we can handle a lot of them I think...
            let chunker = stream.chunks_timeout(1024, Duration::from_secs(1));
            tokio::pin!(chunker);
            while let Some(chunks) = StreamExt::next(&mut chunker).await {
                let mut members = agent.members().write();
                for (addr, rtt) in chunks {
                    members.add_rtt(addr, rtt);
                }
            }
        }
    });

    let (process_uni_tx, mut process_uni_rx) = channel(10240);

    // async message decoder task
    tokio::spawn({
        let agent = agent.clone();
        async move {
            while let Some(payload) = process_uni_rx.recv().await {
                match payload {
                    UniPayload::V1(UniPayloadV1::Broadcast(bcast)) => {
                        handle_change(&agent, bcast, &bcast_msg_tx).await
                    }
                }
            }

            info!("uni payload process loop is done!");
        }
    });

    spawn_counted({
        let agent = agent.clone();
        let mut tripwire = tripwire.clone();
        async move {
            loop {
                let connecting = match gossip_server_endpoint
                    .accept()
                    .preemptible(&mut tripwire)
                    .await
                {
                    Outcome::Completed(Some(connecting)) => connecting,
                    Outcome::Completed(None) => return,
                    Outcome::Preempted(_) => break,
                };

                let process_uni_tx = process_uni_tx.clone();
                let agent = agent.clone();
                let tripwire = tripwire.clone();
                tokio::spawn(async move {
                    let remote_addr = connecting.remote_address();
                    // let local_ip = connecting.local_ip().unwrap();
                    debug!("got a connection from {remote_addr}");

                    let conn = match connecting.await {
                        Ok(conn) => conn,
                        Err(e) => {
                            error!("could not connection from {remote_addr}: {e}");
                            return;
                        }
                    };

                    increment_counter!("corro.peer.connection.accept.total");

                    debug!("accepted a QUIC conn from {remote_addr}");

                    tokio::spawn({
                        let conn = conn.clone();
                        let mut tripwire = tripwire.clone();
                        let foca_tx = agent.tx_foca().clone();
                        async move {
                            loop {
                                let b = tokio::select! {
                                    b_res = conn.read_datagram() => match b_res {
                                        Ok(b) => {
                                            increment_counter!("corro.peer.datagram.recv.total");
                                            counter!("corro.peer.datagram.bytes.recv.total", b.len() as u64);
                                            b
                                        },
                                        Err(e) => {
                                            debug!("could not read datagram from connection: {e}");
                                            return;
                                        }
                                    },
                                    _ = &mut tripwire => {
                                        debug!("connection cancelled");
                                        return;
                                    }
                                };

                                if let Err(e) = foca_tx.send(FocaInput::Data(b)).await {
                                    error!("could not send data foca input: {e}");
                                }
                            }
                        }
                    });

                    tokio::spawn({
                        let conn = conn.clone();
                        let mut tripwire = tripwire.clone();
                        async move {
                            loop {
                                let rx = tokio::select! {
                                    rx_res = conn.accept_uni() => match rx_res {
                                        Ok(rx) => rx,
                                        Err(e) => {
                                            debug!("could not accept unidirectional stream from connection: {e}");
                                            return;
                                        }
                                    },
                                    _ = &mut tripwire => {
                                        debug!("connection cancelled");
                                        return;
                                    }
                                };

                                increment_counter!("corro.peer.stream.accept.total", "type" => "uni");

                                debug!(
                                    "accepted a unidirectional stream from {}",
                                    conn.remote_address()
                                );

                                tokio::spawn({
                                    let process_uni_tx = process_uni_tx.clone();
                                    async move {
                                        let mut framed =
                                            FramedRead::new(rx, LengthDelimitedCodec::new());

                                        loop {
                                            match StreamExt::next(&mut framed).await {
                                                Some(Ok(b)) => {
                                                    counter!("corro.peer.stream.bytes.recv.total", b.len() as u64, "type" => "uni");
                                                    match UniPayload::read_from_buffer(&b) {
                                                        Ok(payload) => {
                                                            trace!("parsed a payload: {payload:?}");

                                                            if let Err(e) =
                                                                process_uni_tx.send(payload).await
                                                            {
                                                                error!("could not send UniPayload for processing: {e}");
                                                                // this means we won't be able to process more...
                                                                return;
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "could not decode UniPayload: {e}"
                                                            );
                                                            continue;
                                                        }
                                                    }
                                                }
                                                Some(Err(e)) => {
                                                    error!("decode error: {e}");
                                                }
                                                None => break,
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    });

                    tokio::spawn(async move {
                        let mut tripwire = tripwire.clone();
                        loop {
                            let (tx, rx) = tokio::select! {
                                tx_rx_res = conn.accept_bi() => match tx_rx_res {
                                    Ok(tx_rx) => tx_rx,
                                    Err(e) => {
                                        debug!("could not accept bidirectional stream from connection: {e}");
                                        return;
                                    }
                                },
                                _ = &mut tripwire => {
                                    debug!("connection cancelled");
                                    return;
                                }
                            };

                            increment_counter!("corro.peer.stream.accept.total", "type" => "bi");

                            debug!(
                                "accepted a bidirectional stream from {}",
                                conn.remote_address()
                            );

                            // TODO: implement concurrency limit for sync requests
                            tokio::spawn({
                                let agent = agent.clone();
                                async move {
                                    let mut framed =
                                        FramedRead::new(rx, LengthDelimitedCodec::new());

                                    loop {
                                        match timeout(
                                            Duration::from_secs(5),
                                            StreamExt::next(&mut framed),
                                        )
                                        .await
                                        {
                                            Err(_e) => {
                                                warn!("timed out receiving bidirectional frame");
                                                return;
                                            }
                                            Ok(None) => {
                                                return;
                                            }
                                            Ok(Some(res)) => match res {
                                                Ok(b) => {
                                                    match BiPayload::read_from_buffer(&b) {
                                                        Ok(payload) => {
                                                            match payload {
                                                                BiPayload::V1(
                                                                    BiPayloadV1::SyncStart {
                                                                        actor_id,
                                                                        trace_ctx,
                                                                    },
                                                                ) => {
                                                                    trace!("framed read buffer len: {}", framed.read_buffer().len());
                                                                    // println!("got sync state: {state:?}");
                                                                    if let Err(e) = serve_sync(
                                                                        &agent, actor_id,
                                                                        trace_ctx, framed, tx,
                                                                    )
                                                                    .await
                                                                    {
                                                                        warn!("could not complete receiving sync: {e}");
                                                                    }
                                                                    break;
                                                                }
                                                            }
                                                        }

                                                        Err(e) => {
                                                            warn!(
                                                                "could not decode BiPayload: {e}"
                                                            );
                                                        }
                                                    }
                                                }

                                                Err(e) => {
                                                    error!("could not read framed payload from bidirectional stream: {e}");
                                                }
                                            },
                                        }
                                    }
                                }
                            });
                        }
                    });
                });
            }

            // graceful shutdown
            gossip_server_endpoint.reject_new_connections();
            _ = gossip_server_endpoint
                .wait_idle()
                .with_timeout(Duration::from_secs(5))
                .await;
            gossip_server_endpoint.close(0u32.into(), b"shutting down");
        }
    });

    info!("Starting peer API on udp/{gossip_addr} (QUIC)");

    tokio::spawn({
        let agent = agent.clone();
        async move {
            let mut boff = backoff::Backoff::new(10)
                .timeout_range(Duration::from_secs(5), Duration::from_secs(120))
                .iter();
            let timer = tokio::time::sleep(Duration::new(0, 0));
            tokio::pin!(timer);

            loop {
                timer.as_mut().await;

                match generate_bootstrap(
                    agent.config().gossip.bootstrap.as_slice(),
                    gossip_addr,
                    agent.pool(),
                )
                .await
                {
                    Ok(addrs) => {
                        for addr in addrs.iter() {
                            debug!("Bootstrapping w/ {addr}");
                            if let Err(e) = agent
                                .tx_foca()
                                .send(FocaInput::Announce((*addr).into()))
                                .await
                            {
                                error!("could not send foca Announce message: {e}");
                            } else {
                                debug!("successfully sent announce message");
                            }
                        }
                    }
                    Err(e) => {
                        error!("could not find nodes to announce ourselves to: {e}");
                    }
                }

                let dur = boff.next().unwrap_or(ANNOUNCE_INTERVAL);
                timer.as_mut().reset(tokio::time::Instant::now() + dur);
            }
        }
    });

    tokio::spawn(clear_overwritten_versions(agent.clone()));

    let states = match agent.pool().read().await {
        Ok(conn) => block_in_place(|| {
            match conn.prepare("SELECT address,foca_state,rtts FROM __corro_members") {
                Ok(mut prepped) => {
                    match prepped
                    .query_map([], |row| Ok((
                            row.get::<_, String>(0)?.parse().map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?,
                            row.get::<_, String>(1)?,
                            serde_json::from_str(&row.get::<_, String>(2)?).map_err(|e| rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, Box::new(e)))?
                        ))
                    )
                    .and_then(|rows| rows.collect::<rusqlite::Result<Vec<(SocketAddr, String, Vec<u64>)>>>())
                {
                    Ok(members) => {
                        members.into_iter().filter_map(|(address, state, rtts)| match serde_json::from_str::<foca::Member<Actor>>(state.as_str()) {
                            Ok(fs) => Some((address, fs, rtts)),
                            Err(e) => {
                                error!("could not deserialize foca member state: {e} (json: {state})");
                                None
                            }
                        }).collect::<Vec<(SocketAddr, Member<Actor>, Vec<u64>)>>()
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
        }),
        Err(e) => {
            error!("could not acquire conn for foca member states: {e}");
            vec![]
        }
    };

    if !states.is_empty() {
        let mut foca_states = Vec::with_capacity(states.len());

        {
            // block to drop the members write lock
            let mut members = agent.members().write();
            for (address, foca_state, rtts) in states {
                let mut rtt = Rtt::default();
                for v in rtts {
                    rtt.buf.push_back(v);
                }
                members.rtts.insert(address, rtt);
                members.by_addr.insert(address, foca_state.id().id());
                if matches!(foca_state.state(), foca::State::Suspect) {
                    continue;
                }
                foca_states.push(foca_state);
            }
        }

        agent
            .tx_foca()
            .send(FocaInput::ApplyMany(foca_states))
            .await
            .ok();
    }

    let api = Router::new()
        .route(
            "/v1/transactions",
            post(api_v1_transactions).route_layer(
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
            "/v1/queries",
            post(api_v1_queries).route_layer(
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
            "/v1/subscriptions",
            post(api_v1_subs).route_layer(
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
            "/v1/subscriptions/:id",
            get(api_v1_sub_by_id).route_layer(
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
            "/v1/migrations",
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
        .layer(axum::middleware::from_fn(require_authz))
        .layer(
            tower::ServiceBuilder::new()
                .layer(Extension(Arc::new(AtomicI64::new(0))))
                .layer(Extension(agent.clone()))
                .layer(Extension(matcher_id_cache))
                .layer(Extension(matcher_bcast_cache))
                .layer(Extension(tripwire.clone())),
        )
        .layer(DefaultBodyLimit::disable())
        .layer(TraceLayer::new_for_http());

    let api_addr = api_listener.local_addr()?;
    info!("Starting public API server on tcp/{api_addr}");
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

    spawn_counted(handle_changes(agent.clone(), rx_changes, tripwire.clone()));

    spawn_counted(
        sync_loop(
            agent.clone(),
            transport.clone(),
            rx_apply,
            rx_empty,
            tripwire.clone(),
        )
        .inspect(|_| info!("corrosion agent sync loop is done")),
    );

    let mut db_cleanup_interval = tokio::time::interval(Duration::from_secs(60 * 15));

    tokio::spawn(handle_gossip_to_send(transport.clone(), to_send_rx));
    tokio::spawn(handle_notifications(
        agent.clone(),
        notifications_rx,
        member_events_tx,
    ));
    tokio::spawn(metrics_loop(agent.clone(), transport));

    tokio::spawn(handle_broadcasts(agent.clone(), bcast_rx));

    loop {
        tokio::select! {
            biased;
            _ = db_cleanup_interval.tick() => {
                tokio::spawn(handle_db_cleanup(agent.pool().clone()).preemptible(tripwire.clone()));
            },
            _ = &mut tripwire => {
                debug!("tripped corrosion");
                break;
            }
        }
    }

    Ok(())
}

async fn require_authz<B>(
    Extension(agent): Extension<Agent>,
    maybe_authz_header: Option<TypedHeader<Authorization<Bearer>>>,
    request: axum::http::Request<B>,
    next: axum::middleware::Next<B>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let passed = if let Some(ref authz) = agent.config().api.authorization {
        match authz {
            AuthzConfig::BearerToken(token) => maybe_authz_header
                .map(|h| h.token() == token)
                .unwrap_or(false),
        }
    } else {
        true
    };

    if !passed {
        return Err(axum::http::StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(request).await)
}

async fn clear_overwritten_versions(agent: Agent) {
    let pool = agent.pool();
    let bookie = agent.bookie();

    let mut interval = Duration::new(0, 0);

    loop {
        sleep(interval).await;

        if interval != COMPACT_BOOKED_INTERVAL {
            interval = COMPACT_BOOKED_INTERVAL;
        }

        info!("Starting compaction...");
        let start = Instant::now();

        let mut to_check: BTreeMap<i64, (ActorId, i64)> = BTreeMap::new();

        {
            let booked = bookie.read("clear_overwritten_versions").await;
            for (actor_id, booked) in booked.iter() {
                let versions = {
                    match timeout(
                        Duration::from_secs(1),
                        booked.read(format!(
                            "clear_overwritten_versions:{}",
                            actor_id.as_simple()
                        )),
                    )
                    .await
                    {
                        Ok(booked) => booked.current_versions(),
                        Err(_) => {
                            info!(%actor_id, "timed out acquiring read lock on bookkeeping, skipping for now");
                            continue;
                        }
                    }
                };
                if versions.is_empty() {
                    continue;
                }
                for (db_v, v) in versions {
                    to_check.insert(db_v, (*actor_id, v));
                }
            }
        }

        debug!("got actors and their versions");

        let cleared_versions: BTreeSet<i64> = {
            match pool.read().await {
                Ok(mut conn) => {
                    let res = block_in_place(|| {
                        let tx = conn.transaction()?;
                        find_cleared_db_versions(&tx)
                    });
                    match res {
                        Ok(cleared) => cleared,
                        Err(e) => {
                            error!("could not get cleared versions: {e}");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!("could not get read connection: {e}");
                    continue;
                }
            }
        };

        let mut to_clear_by_actor: BTreeMap<ActorId, Vec<(i64, i64)>> = BTreeMap::new();

        for db_v in cleared_versions {
            if let Some((actor_id, v)) = to_check.remove(&db_v) {
                to_clear_by_actor
                    .entry(actor_id)
                    .or_default()
                    .push((db_v, v));
            }
        }

        let mut deleted = 0;
        let mut inserted = 0;

        for (actor_id, to_clear) in to_clear_by_actor {
            info!(%actor_id, "Clearing {} versions", to_clear.len());
            let booked = {
                bookie
                    .write(format!("to_clear_get_booked:{}", actor_id.as_simple()))
                    .await
                    .for_actor(actor_id)
            };

            let mut bookedw = booked
                .write(format!("clearing:{}", actor_id.as_simple()))
                .await;

            for (_db_v, v) in to_clear.iter() {
                if bookedw.contains_current(v) {
                    bookedw.insert(*v, KnownDbVersion::Cleared);
                    deleted += 1;
                }
            }

            for range in to_clear
                .iter()
                .filter_map(|(_, v)| bookedw.cleared.get(v))
                .dedup()
            {
                if let Err(e) = agent.tx_empty().try_send((actor_id, range.clone())) {
                    error!("could not schedule version to be cleared: {e}");
                } else {
                    inserted += 1;
                }
            }
        }

        info!(
            "Compaction done, cleared {} DB bookkeeping table rows in {:?}",
            deleted - inserted,
            start.elapsed()
        );
    }
}

async fn metrics_loop(agent: Agent, transport: Transport) {
    let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        metrics_interval.tick().await;

        block_in_place(|| collect_metrics(&agent, &transport));
    }
}

// const MAX_COUNT_TO_HASH: i64 = 500_000;

fn collect_metrics(agent: &Agent, transport: &Transport) {
    agent.pool().emit_metrics();
    transport.emit_metrics();

    let schema = agent.schema().read();

    let conn = match agent.pool().read_blocking() {
        Ok(conn) => conn,
        Err(e) => {
            error!("could not acquire read connection for metrics purposes: {e}");
            return;
        }
    };

    // let mut low_count_tables = vec![];

    for table in schema.tables.keys() {
        match conn
            .prepare_cached(&format!("SELECT count(*) FROM {table}"))
            .and_then(|mut prepped| prepped.query_row([], |row| row.get::<_, i64>(0)))
        {
            Ok(count) => {
                gauge!("corro.db.table.rows.total", count as f64, "table" => table.clone());
                // if count <= MAX_COUNT_TO_HASH {
                //     low_count_tables.push(table);
                // }
            }
            Err(e) => {
                error!("could not query count for table {table}: {e}");
                continue;
            }
        }
    }

    match conn
        .prepare_cached("SELECT actor_id, (select count(site_id) FROM __corro_buffered_changes WHERE site_id = actor_id) FROM __corro_members")
        .and_then(|mut prepped| {
            prepped
                .query_map((), |row| {
                    Ok((row.get::<_, ActorId>(0)?, row.get::<_, i64>(1)?))
                })
                .and_then(|mapped| mapped.collect::<Result<Vec<_>, _>>())
        }) {
        Ok(mapped) => {
            for (actor_id, count) in mapped {
                gauge!("corro.db.buffered.changes.rows.total", count as f64, "actor_id" => actor_id.to_string())
            }
        }
        Err(e) => {
            error!("could not query count for buffered changes: {e}");
        }
    }
}

pub async fn handle_change(agent: &Agent, bcast: BroadcastV1, bcast_msg_tx: &Sender<BroadcastV1>) {
    match bcast {
        BroadcastV1::Change(change) => {
            let diff = if let Some(ts) = change.ts() {
                if let Ok(id) = change.actor_id.try_into() {
                    Some(
                        agent
                            .clock()
                            .new_timestamp()
                            .get_diff_duration(&uhlc::Timestamp::new(ts.0, id)),
                    )
                } else {
                    None
                }
            } else {
                None
            };

            increment_counter!("corro.broadcast.recv.count", "kind" => "change");

            trace!("handling {} changes", change.len());

            let booked = {
                agent
                    .bookie()
                    .write(format!(
                        "handle_change(for_actor):{}",
                        change.actor_id.as_simple()
                    ))
                    .await
                    .for_actor(change.actor_id)
            };

            if booked
                .read(format!(
                    "handle_change(contains?):{}",
                    change.actor_id.as_simple()
                ))
                .await
                .contains_all(change.versions(), change.seqs())
            {
                trace!("already seen, stop disseminating");
                return;
            }

            if change.actor_id == agent.actor_id() {
                return;
            }

            if let Some(diff) = diff {
                histogram!("corro.broadcast.recv.lag.seconds", diff.as_secs_f64());
            }

            if let Err(e) = bcast_msg_tx.send(BroadcastV1::Change(change)).await {
                error!("could not send change message through broadcast channel: {e}");
            }
        }
    }
}

#[tracing::instrument(skip_all)]
fn find_cleared_db_versions(tx: &Transaction) -> rusqlite::Result<BTreeSet<i64>> {
    let tables = tx
        .prepare_cached(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '%__crsql_clock'",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<Result<BTreeSet<String>, _>>()?;

    if tables.is_empty() {
        return Ok(BTreeSet::new());
    }

    let to_clear_query = format!(
        "SELECT DISTINCT(db_version) FROM __corro_bookkeeping WHERE db_version IS NOT NULL
            EXCEPT SELECT db_version FROM ({});",
        tables
            .iter()
            .map(|table| format!("SELECT DISTINCT(db_version) FROM {table}"))
            .collect::<Vec<_>>()
            .join(" UNION ")
    );

    let start = Instant::now();
    let cleared_db_versions: BTreeSet<i64> = tx
        .prepare_cached(&to_clear_query)?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;

    info!(
        "Aggregated {} DB versions to clear in {:?}",
        cleared_db_versions.len(),
        start.elapsed()
    );

    Ok(cleared_db_versions)
}

async fn handle_gossip_to_send(transport: Transport, mut to_send_rx: Receiver<(Actor, Bytes)>) {
    // TODO: use tripwire and drain messages to send when that happens...
    while let Some((actor, data)) = to_send_rx.recv().await {
        trace!("got gossip to send to {actor:?}");

        let addr = actor.addr();
        let actor_id = actor.id();

        let transport = transport.clone();

        let len = data.len();
        spawn_counted(async move {
            if let Err(e) = transport.send_datagram(addr, data).await {
                error!("could not write datagram {addr}: {e}");
                return;
            }
            increment_counter!("corro.peer.datagram.sent.total", "actor_id" => actor_id.to_string());
            counter!("corro.peer.datagram.bytes.sent.total", len as u64);
        }.instrument(debug_span!("send_swim_payload", %addr, %actor_id, buf_size = len)));
    }
}

async fn handle_broadcasts(agent: Agent, mut bcast_rx: Receiver<BroadcastV1>) {
    while let Some(bcast) = bcast_rx.recv().await {
        increment_counter!("corro.broadcast.recv.count");
        match bcast {
            BroadcastV1::Change(change) => {
                if let Err(_e) = agent
                    .tx_changes()
                    .send((change, ChangeSource::Broadcast))
                    .await
                {
                    error!("changes channel is closed");
                    break;
                }
            }
        }
    }
}

async fn handle_notifications(
    agent: Agent,
    mut notification_rx: Receiver<Notification<Actor>>,
    member_events: tokio::sync::broadcast::Sender<MemberEvent>,
) {
    while let Some(notification) = notification_rx.recv().await {
        trace!("handle notification");
        match notification {
            Notification::MemberUp(actor) => {
                let (added, same) = { agent.members().write().add_member(&actor) };
                trace!("Member Up {actor:?} (added: {added})");
                if added {
                    debug!("Member Up {actor:?}");
                    increment_counter!("corro.gossip.member.added", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually added a member
                    // notify of new cluster size
                    let members_len = { agent.members().read().states.len() as u32 };
                    if let Ok(size) = members_len.try_into() {
                        if let Err(e) = agent.tx_foca().send(FocaInput::ClusterSize(size)).await {
                            error!("could not send new foca cluster size: {e}");
                        }
                    }

                    member_events.send(MemberEvent::Up(actor.clone())).ok();
                } else if !same {
                    // had a older timestamp!
                    if let Err(e) = agent
                        .tx_foca()
                        .send(FocaInput::ApplyMany(vec![foca::Member::new(
                            actor.clone(),
                            foca::Incarnation::default(),
                            foca::State::Down,
                        )]))
                        .await
                    {
                        warn!(?actor, "could not manually declare actor as down! {e}");
                    }
                }
                increment_counter!("corro.swim.notification", "type" => "memberup");
            }
            Notification::MemberDown(actor) => {
                let removed = { agent.members().write().remove_member(&actor) };
                trace!("Member Down {actor:?} (removed: {removed})");
                if removed {
                    debug!("Member Down {actor:?}");
                    increment_counter!("corro.gossip.member.removed", "id" => actor.id().0.to_string(), "addr" => actor.addr().to_string());
                    // actually removed a member
                    // notify of new cluster size
                    let member_len = { agent.members().read().states.len() as u32 };
                    if let Ok(size) = member_len.try_into() {
                        if let Err(e) = agent.tx_foca().send(FocaInput::ClusterSize(size)).await {
                            error!("could not send new foca cluster size: {e}");
                        }
                    }
                    member_events.send(MemberEvent::Down(actor.clone())).ok();
                }
                increment_counter!("corro.swim.notification", "type" => "memberdown");
            }
            Notification::Active => {
                info!("Current node is considered ACTIVE");
                increment_counter!("corro.swim.notification", "type" => "active");
            }
            Notification::Idle => {
                warn!("Current node is considered IDLE");
                increment_counter!("corro.swim.notification", "type" => "idle");
            }
            // this happens when we leave the cluster
            Notification::Defunct => {
                debug!("Current node is considered DEFUNCT");
                increment_counter!("corro.swim.notification", "type" => "defunct");
            }
            Notification::Rejoin(id) => {
                info!("Rejoined the cluster with id: {id:?}");
                increment_counter!("corro.swim.notification", "type" => "rejoin");
            }
        }
    }
}

async fn handle_db_cleanup(pool: SplitPool) -> eyre::Result<()> {
    debug!("handling db_cleanup (WAL truncation)");
    let conn = pool.write_low().await?;
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
    debug!("done handling db_cleanup");
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
    pool: &SplitPool,
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
        let conn = pool.read().await?;
        addrs = block_in_place(|| {
            let mut prepped = conn.prepare("SELECT address FROM __corro_members LIMIT 5")?;
            let node_addrs = prepped.query_map([], |row| row.get::<_, String>(0))?;
            Ok::<_, rusqlite::Error>(
                node_addrs
                    .flatten()
                    .flat_map(|addr| addr.parse())
                    .filter(|addr| match (our_addr, addr) {
                        (SocketAddr::V6(our_ip), SocketAddr::V6(ip)) if our_ip != *ip => true,
                        (SocketAddr::V4(our_ip), SocketAddr::V4(ip)) if our_ip != *ip => true,
                        _ => {
                            debug!("ignore node with addr: {addr}");
                            false
                        }
                    })
                    .collect(),
            )
        })?;
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
                debug!("Resolving '{hostname}' to an IP");
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
                                    debug!("ignore node with addr: {addr}");
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

fn store_empty_changeset(
    tx: &Transaction,
    actor_id: ActorId,
    versions: RangeInclusive<i64>,
) -> Result<usize, rusqlite::Error> {
    // first, delete "current" versions, they're now gone!
    let deleted = tx.prepare_cached("DELETE FROM __corro_bookkeeping WHERE actor_id = ? AND start_version >= ? AND start_version <= ? AND end_version IS NULL")?.execute(params![actor_id, versions.start(), versions.end()])?;

    if deleted > 0 {
        debug!("deleted {deleted} still-live versions from database's bookkeeping");
    }

    // insert cleared versions
    let inserted = tx
        .prepare_cached(
            "
                INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version, db_version, last_seq, ts)
                    VALUES (?, ?, ?, NULL, NULL, NULL)
                    ON CONFLICT (actor_id, start_version) DO UPDATE SET
                        end_version = excluded.end_version,
                        db_version = NULL,
                        last_seq = NULL,
                        ts = NULL
                    WHERE end_version < excluded.end_version;
            ",
        )?
        .execute(params![actor_id, versions.start(), versions.end()])?;

    // remove any buffered data or seqs bookkeeping for these versions
    clear_buffered_meta(tx, actor_id, versions)?;

    Ok(inserted)
}

fn clear_buffered_meta(
    tx: &Transaction,
    actor_id: ActorId,
    versions: RangeInclusive<i64>,
) -> rusqlite::Result<()> {
    // remove all buffered changes for cleanup purposes
    let count = tx
        .prepare_cached("DELETE FROM __corro_buffered_changes WHERE site_id = ? AND version >= ? AND version <= ?")?
        .execute(params![actor_id, versions.start(), versions.end()])?;
    debug!(%actor_id, ?versions, "deleted {count} buffered changes");

    // delete all bookkept sequences for this version
    let count = tx
        .prepare_cached("DELETE FROM __corro_seq_bookkeeping WHERE site_id = ? AND version >= ? AND version <= ?")?
        .execute(params![actor_id, versions.start(), versions.end()])?;
    debug!(%actor_id, ?versions, "deleted {count} sequences in bookkeeping");

    Ok(())
}

#[tracing::instrument(skip(agent), err)]
async fn process_fully_buffered_changes(
    agent: &Agent,
    actor_id: ActorId,
    version: i64,
) -> Result<bool, ChangeError> {
    let mut conn = agent.pool().write_normal().await?;
    debug!(%actor_id, version, "acquired write (normal) connection to process fully buffered changes");

    let booked = {
        agent
            .bookie()
            .write(format!(
                "process_fully_buffered(for_actor):{}",
                actor_id.as_simple()
            ))
            .await
            .for_actor(actor_id)
    };

    let mut bookedw = booked
        .write(format!(
            "process_fully_buffered(booked writer):{}",
            actor_id.as_simple()
        ))
        .await;
    debug!(%actor_id, version, "acquired Booked write lock to process fully buffered changes");

    let inserted = block_in_place(|| {
        let (last_seq, ts) = {
            match bookedw.partials.get(&version) {
                Some(PartialVersion { seqs, last_seq, ts }) => {
                    if seqs.gaps(&(0..=*last_seq)).count() != 0 {
                        error!(%actor_id, version, "found sequence gaps: {:?}, aborting!", seqs.gaps(&(0..=*last_seq)).collect::<RangeInclusiveSet<i64>>());
                        // TODO: return an error here
                        return Ok(false);
                    }
                    (*last_seq, *ts)
                }
                None => {
                    warn!(%actor_id, %version, "version not found in cache, returning");
                    return Ok(false);
                }
            }
        };

        let tx = conn.transaction()?;

        info!(%actor_id, version, "Processing buffered changes to crsql_changes (actor: {actor_id}, version: {version}, last_seq: {last_seq})");

        let max_db_version: Option<i64> = tx.prepare_cached("SELECT MAX(db_version) FROM __corro_buffered_changes WHERE site_id = ? AND version = ?")?.query_row(params![actor_id.as_bytes(), version], |row| row.get(0)).optional()?;

        let start = Instant::now();

        if let Some(max_db_version) = max_db_version {
            // insert all buffered changes into crsql_changes directly from the buffered changes table
            let count = tx
            .prepare_cached(
                r#"
                INSERT INTO crsql_changes ("table", pk, cid, val, col_version, db_version, site_id, cl, seq)
                    SELECT                 "table", pk, cid, val, col_version, ? as db_version, site_id, cl, seq
                        FROM __corro_buffered_changes
                            WHERE site_id = ?
                              AND version = ?
                            ORDER BY db_version ASC, seq ASC
                            "#,
            )?
            .execute(params![max_db_version, actor_id.as_bytes(), version])?;
            info!(%actor_id, version, "Inserted {count} rows from buffered into crsql_changes in {:?}", start.elapsed());
        } else {
            info!(%actor_id, version, "No buffered rows, skipped insertion into crsql_changes");
        }

        clear_buffered_meta(&tx, actor_id, version..=version)?;

        let rows_impacted: i64 = tx
            .prepare_cached("SELECT crsql_rows_impacted()")?
            .query_row((), |row| row.get(0))?;

        debug!(%actor_id, version, "rows impacted by buffered changes insertion: {rows_impacted}");

        let known_version = if rows_impacted > 0 {
            let db_version: i64 =
                tx.query_row("SELECT crsql_next_db_version()", [], |row| row.get(0))?;
            debug!("db version: {db_version}");

            tx.prepare_cached(
                "
                INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                    VALUES (
                        :actor_id,
                        :version,
                        :db_version,
                        :last_seq,
                        :ts
                    );",
            )?
            .execute(named_params! {
                ":actor_id": actor_id,
                ":version": version,
                ":db_version": db_version,
             // ":start_version": 0,
                ":last_seq": last_seq,
                ":ts": ts
            })?;

            debug!(%actor_id, version, "inserted bookkeeping row after buffered insert");

            Some(KnownDbVersion::Current {
                db_version,
                last_seq,
                ts,
            })
        } else {
            store_empty_changeset(&tx, actor_id, version..=version)?;

            debug!(%actor_id, version, "inserted CLEARED bookkeeping row after buffered insert");
            Some(KnownDbVersion::Cleared)
        };

        tx.commit()?;

        let inserted = if let Some(known_version) = known_version {
            let db_version = if let KnownDbVersion::Current { db_version, .. } = &known_version {
                Some(*db_version)
            } else {
                None
            };

            bookedw.insert(version, known_version);

            drop(bookedw);

            if let Some(db_version) = db_version {
                // TODO: write changes into a queueing table
                process_subs_by_db_version(agent, &conn, db_version);
            }

            true
        } else {
            false
        };

        Ok::<_, rusqlite::Error>(inserted)
    })?;

    Ok(inserted)
}

#[tracing::instrument(skip(agent, changes), err)]
pub async fn process_multiple_changes(
    agent: &Agent,
    changes: Vec<(ChangeV1, ChangeSource)>,
) -> Result<(), ChangeError> {
    debug!(self_actor_id = %agent.actor_id(), "processing multiple changes, len: {}", changes.iter().map(|(change, _)| cmp::max(change.len(), 1)).sum::<usize>());

    let bookie = agent.bookie();

    let mut seen = HashSet::new();
    let mut unknown_changes = Vec::with_capacity(changes.len());
    for (change, src) in changes {
        let versions = change.versions();
        let seqs = change.seqs();
        if !seen.insert((change.actor_id, versions, seqs.cloned())) {
            continue;
        }
        if bookie
            .write(format!(
                "process_multiple_changes(for_actor):{}",
                change.actor_id.as_simple()
            ))
            .await
            .for_actor(change.actor_id)
            .read(format!(
                "process_multiple_changes(contains?):{}",
                change.actor_id.as_simple()
            ))
            .await
            .contains_all(change.versions(), change.seqs())
        {
            continue;
        }

        unknown_changes.push((change, src));
    }

    unknown_changes.sort_by_key(|(change, _src)| change.actor_id);

    let mut conn = agent.pool().write_normal().await?;

    block_in_place(|| {
        let start = Instant::now();
        let tx = conn.transaction()?;

        let mut knowns: BTreeMap<ActorId, Vec<_>> = BTreeMap::new();
        let mut changesets = vec![];

        let mut last_db_version = None;

        for (actor_id, changes) in unknown_changes
            .into_iter()
            .group_by(|(change, _src)| change.actor_id)
            .into_iter()
        {
            // get a lock on the actor id's booked writer if we didn't already
            {
                let booked = {
                    bookie
                        .blocking_write(format!(
                            "process_multiple_changes(for_actor_blocking):{}",
                            actor_id.as_simple()
                        ))
                        .for_actor(actor_id)
                };
                let booked_write = booked.blocking_write(format!(
                    "process_multiple_changes(booked writer):{}",
                    actor_id.as_simple()
                ));

                let mut seen = RangeInclusiveMap::new();

                for (change, src) in changes {
                    let seqs = change.seqs();
                    if booked_write.contains_all(change.versions(), change.seqs()) {
                        trace!(
                            "previously unknown versions are now deemed known, aborting inserts"
                        );
                        continue;
                    }

                    let versions = change.versions();

                    // check if we've seen this version here...
                    if versions.clone().all(|version| match seqs {
                        Some(check_seqs) => match seen.get(&version) {
                            Some(known) => match known {
                                KnownDbVersion::Partial { seqs, .. } => {
                                    check_seqs.clone().all(|seq| seqs.contains(&seq))
                                }
                                KnownDbVersion::Current { .. } | KnownDbVersion::Cleared => true,
                            },
                            None => false,
                        },
                        None => seen.contains_key(&version),
                    }) {
                        continue;
                    }

                    // optimizing this, insert later!
                    let (known, changeset) = if change.is_complete() && change.is_empty() {
                        // we never want to block here
                        if let Err(e) = agent.tx_empty().try_send((actor_id, change.versions())) {
                            error!("could not send empty changed versions into channel: {e}");
                        }
                        (
                            KnownDbVersion::Cleared,
                            Changeset::Empty {
                                versions: change.versions(),
                            },
                        )
                    } else {
                        let (known, changeset) = match process_single_version(
                            &tx,
                            last_db_version,
                            change,
                        ) {
                            Ok((known, changeset)) => {
                                if let KnownDbVersion::Current { db_version, .. } = &known {
                                    last_db_version = Some(*db_version);
                                }
                                (known, changeset)
                            }
                            Err(e) => {
                                error!(%actor_id, ?versions, "could not process single change: {e}");
                                continue;
                            }
                        };
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), versions = ?changeset.versions(), "got known to insert: {known:?}");
                        (known, changeset)
                    };

                    seen.insert(versions.clone(), known.clone());

                    changesets.push((actor_id, changeset, src));
                    knowns.entry(actor_id).or_default().push((versions, known));
                }
            }
        }

        let mut count = 0;

        for (actor_id, knowns) in knowns.iter_mut() {
            debug!(%actor_id, self_actor_id = %agent.actor_id(), "processing {} knowns", knowns.len());
            for (versions, known) in knowns.iter_mut() {
                match known {
                    KnownDbVersion::Partial { .. } => {
                        continue;
                    }
                    KnownDbVersion::Current {
                        db_version,
                        last_seq,
                        ts,
                    } => {
                        count += 1;
                        let version = versions.start();
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), version, "inserting bookkeeping row db_version: {db_version}, ts: {ts:?}");
                        tx.prepare_cached("
                            INSERT INTO __corro_bookkeeping ( actor_id,  start_version,  db_version,  last_seq,  ts)
                                                    VALUES  (:actor_id, :start_version, :db_version, :last_seq, :ts);")?
                            .execute(named_params!{
                                ":actor_id": actor_id,
                                ":start_version": *version,
                                ":db_version": *db_version,
                                ":last_seq": *last_seq,
                                ":ts": *ts
                            })?;
                    }
                    KnownDbVersion::Cleared => {
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserting CLEARED bookkeeping");
                        store_empty_changeset(&tx, *actor_id, versions.clone())?;
                    }
                }
                debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserted bookkeeping row");
            }
        }

        debug!("inserted {count} new changesets");

        tx.commit()?;

        debug!("committed {count} changes in {:?}", start.elapsed());

        for (actor_id, knowns) in knowns {
            let booked = {
                bookie
                    .blocking_write(format!(
                        "process_multiple_changes(for_actor_blocking):{}",
                        actor_id.as_simple()
                    ))
                    .for_actor(actor_id)
            };
            let mut booked_write = booked.blocking_write(format!(
                "process_multiple_changes(booked writer, post commit):{}",
                actor_id.as_simple()
            ));

            for (versions, known) in knowns {
                if let KnownDbVersion::Partial { seqs, last_seq, .. } = &known {
                    let full_seqs_range = 0..=*last_seq;
                    let gaps_count = seqs.gaps(&full_seqs_range).count();
                    let version = *versions.start();
                    if gaps_count == 0 {
                        // if we have no gaps, then we can schedule applying all these changes.
                        debug!(%actor_id, version, "we now have all versions, notifying for background jobber to insert buffered changes! seqs: {seqs:?}, expected full seqs: {full_seqs_range:?}");
                        let tx_apply = agent.tx_apply().clone();
                        tokio::spawn(async move {
                            if let Err(e) = tx_apply.send((actor_id, version)).await {
                                error!("could not send trigger for applying fully buffered changes later: {e}");
                            }
                        });
                    } else {
                        debug!(%actor_id, version, "still have {gaps_count} gaps in partially buffered seqs");
                    }
                }
                booked_write.insert_many(versions, known);
            }
        }

        for (actor_id, changeset, src) in changesets {
            process_subs(agent, changeset.changes());
            if matches!(src, ChangeSource::Broadcast) && !changeset.is_empty() {
                if let Err(_e) =
                    agent
                        .tx_bcast()
                        .try_send(BroadcastInput::Rebroadcast(BroadcastV1::Change(ChangeV1 {
                            actor_id,
                            changeset,
                        })))
                {
                    debug!("broadcasts are full or done!");
                }
            }
        }

        Ok::<_, ChangeError>(())
    })?;

    Ok(())
}

#[tracing::instrument(skip(tx, parts), err)]
fn process_incomplete_version(
    tx: &Transaction,
    actor_id: ActorId,
    parts: &ChangesetParts,
) -> rusqlite::Result<KnownDbVersion> {
    let ChangesetParts {
        version,
        changes,
        seqs,
        last_seq,
        ts,
    } = parts;

    debug!(%actor_id, version, "incomplete change, seqs: {seqs:?}, last_seq: {last_seq:?}, len: {}", changes.len());
    let mut inserted = 0;
    for change in changes.iter() {
        trace!("buffering change! {change:?}");

        // insert change, do nothing on conflict
        inserted += tx.prepare_cached(
            r#"
                INSERT INTO __corro_buffered_changes
                    ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, version)
                VALUES
                    (:table, :pk, :cid, :val, :col_version, :db_version, :site_id, :cl, :seq, :version)
                ON CONFLICT (site_id, db_version, version, seq)
                    DO NOTHING
            "#,
        )?
        .execute(named_params!{
            ":table": change.table.as_str(),
            ":pk": change.pk,
            ":cid": change.cid.as_str(),
            ":val": &change.val,
            ":col_version": change.col_version,
            ":db_version": change.db_version,
            ":site_id": &change.site_id,
            ":cl": change.cl,
            ":seq": change.seq,
            ":version": version,
        })?;
    }

    debug!(%actor_id, version, "buffered {inserted} changes");

    // calculate all known sequences for the actor + version combo
    let mut seqs_in_bookkeeping: RangeInclusiveSet<i64> = tx
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

    // immediately add the new range to the recorded seqs ranges
    seqs_in_bookkeeping.insert(seqs.clone());

    tx.prepare_cached("DELETE FROM __corro_seq_bookkeeping WHERE site_id = ? AND version = ?")?
        .execute(params![actor_id, version])?;

    for range in seqs_in_bookkeeping.iter() {
        tx.prepare_cached(
            "
            INSERT INTO __corro_seq_bookkeeping (site_id, version, start_seq, end_seq, last_seq, ts)
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

    Ok(KnownDbVersion::Partial {
        seqs: seqs_in_bookkeeping.clone(),
        last_seq: *last_seq,
        ts: *ts,
    })
}

#[tracing::instrument(skip(tx, last_db_version, parts), err)]
fn process_complete_version(
    tx: &Transaction,
    actor_id: ActorId,
    last_db_version: Option<i64>,
    versions: RangeInclusive<i64>,
    parts: ChangesetParts,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangesetParts {
        version,
        changes,
        seqs,
        last_seq,
        ts,
    } = parts;

    let len = changes.len();

    let max_db_version = changes.iter().map(|c| c.db_version).max().unwrap_or(0);

    debug!(%actor_id, version, "complete change, applying right away! seqs: {seqs:?}, last_seq: {last_seq}, changes len: {len}, max db version: {max_db_version}");

    debug_assert!(len <= (seqs.end() - seqs.start() + 1) as usize);

    let mut impactful_changeset = vec![];

    let mut last_rows_impacted = 0;

    let mut changes_per_table = BTreeMap::new();

    // we need to manually increment the next db version for each changeset
    tx
        .prepare_cached("SELECT CASE WHEN COALESCE(?, crsql_db_version()) >= ? THEN crsql_next_db_version(crsql_next_db_version() + 1) END")?
        .query_row(params![last_db_version, max_db_version], |_row| Ok(()))?;

    for change in changes {
        trace!("inserting change! {change:?}");

        tx.prepare_cached(
            r#"
                INSERT INTO crsql_changes
                    ("table", pk, cid, val, col_version, db_version, site_id, cl, seq)
                VALUES
                    (?,       ?,  ?,   ?,   ?,           ?,          ?,       ?,  ?)
            "#,
        )?
        .execute(params![
            change.table.as_str(),
            change.pk,
            change.cid.as_str(),
            &change.val,
            change.col_version,
            change.db_version,
            &change.site_id,
            change.cl,
            // increment the seq by the start_seq or else we'll have multiple change rows with the same seq
            change.seq,
        ])?;
        let rows_impacted: i64 = tx
            .prepare_cached("SELECT crsql_rows_impacted()")?
            .query_row((), |row| row.get(0))?;

        if rows_impacted > last_rows_impacted {
            trace!("inserted the change into crsql_changes");
            impactful_changeset.push(change);
            if let Some(c) = impactful_changeset.last() {
                if let Some(counter) = changes_per_table.get_mut(&c.table) {
                    *counter += 1;
                } else {
                    changes_per_table.insert(c.table.clone(), 1);
                }
            }
        }
        last_rows_impacted = rows_impacted;
    }

    let (known_version, new_changeset) = if impactful_changeset.is_empty() {
        (KnownDbVersion::Cleared, Changeset::Empty { versions })
    } else {
        // TODO: find a way to avoid this...
        let db_version: i64 = tx
            .prepare_cached("SELECT crsql_next_db_version()")?
            .query_row([], |row| row.get(0))?;
        (
            KnownDbVersion::Current {
                db_version,
                last_seq,
                ts,
            },
            Changeset::Full {
                version,
                changes: impactful_changeset,
                seqs,
                last_seq,
                ts,
            },
        )
    };

    // in case we got both buffered data and a complete set of changes
    clear_buffered_meta(tx, actor_id, version..=version)?;

    for (table_name, count) in changes_per_table {
        counter!("corro.changes.committed", count, "table" => table_name.to_string(), "source" => "remote");
    }

    Ok::<_, rusqlite::Error>((known_version, new_changeset))
}

#[tracing::instrument(skip(tx, last_db_version, change), err)]
fn process_single_version(
    tx: &Transaction,
    last_db_version: Option<i64>,
    change: ChangeV1,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangeV1 {
        actor_id,
        changeset,
    } = change;

    let versions = changeset.versions();

    let (known, changeset) = if changeset.is_complete() {
        process_complete_version(
            tx,
            actor_id,
            last_db_version,
            versions,
            changeset
                .into_parts()
                .expect("no changeset parts, this shouldn't be happening!"),
        )?
    } else {
        let parts = changeset.into_parts().unwrap();
        let known = process_incomplete_version(tx, actor_id, &parts)?;

        (known, parts.into())
    };

    Ok((known, changeset))
}

pub fn process_subs(agent: &Agent, changeset: &[Change]) {
    trace!("process subs...");

    let mut matchers_to_delete = vec![];

    {
        let matchers = agent.matchers().read();
        for (id, matcher) in matchers.iter() {
            if let Err(e) = matcher.process_change(changeset) {
                error!("could not process change w/ matcher {id}, it is probably defunct! {e}");
                matchers_to_delete.push(*id);
            }
        }
    }

    for id in matchers_to_delete {
        agent.matchers().write().remove(&id);
    }
}

pub fn process_subs_by_db_version(agent: &Agent, conn: &Connection, db_version: i64) {
    trace!("process subs by db version...");

    let mut matchers_to_delete = vec![];

    {
        let matchers = agent.matchers().read();
        for (id, matcher) in matchers.iter() {
            if let Err(e) = matcher.process_changes_from_db_version(conn, db_version) {
                error!("could not process change w/ matcher {id}, it is probably defunct! {e}");
                matchers_to_delete.push(*id);
            }
        }
    }

    for id in matchers_to_delete {
        agent.matchers().write().remove(&id);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SyncClientError {
    #[error("bad status code: {0}")]
    Status(StatusCode),
    #[error("service unavailable right now")]
    Unavailable,
    #[error(transparent)]
    Connect(#[from] TransportError),
    #[error("request timed out")]
    RequestTimedOut,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error("could not decode message: {0}")]
    Decoded(#[from] SyncMessageDecodeError),
    #[error("could not encode message: {0}")]
    Encoded(#[from] SyncMessageEncodeError),

    #[error(transparent)]
    Sync(#[from] SyncError),
}

impl SyncClientError {
    pub fn is_unavailable(&self) -> bool {
        matches!(self, SyncClientError::Unavailable)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SyncRecvError {
    #[error("could not decode message: {0}")]
    Decoded(#[from] SyncMessageDecodeError),
    #[error(transparent)]
    Change(#[from] ChangeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("expected sync state message, received something else")]
    ExpectedSyncState,
    #[error("unexpected end of stream")]
    UnexpectedEndOfStream,
    #[error("expected sync clock message, received something else")]
    ExpectedClockMessage,
    #[error("timed out waiting for sync message")]
    TimedOut(#[from] Elapsed),
    #[error("changes channel is closed")]
    ChangesChannelClosed,
    #[error("requests channel is closed")]
    RequestsChannelClosed,
}

#[tracing::instrument(skip_all, err, level = "debug")]
async fn handle_sync(agent: &Agent, transport: &Transport) -> Result<(), SyncClientError> {
    let sync_state = generate_sync(agent.bookie(), agent.actor_id()).await;

    for (actor_id, needed) in sync_state.need.iter() {
        gauge!("corro.sync.client.needed", needed.len() as f64, "actor_id" => actor_id.to_string());
    }
    for (actor_id, version) in sync_state.heads.iter() {
        gauge!("corro.sync.client.head", *version as f64, "actor_id" => actor_id.to_string());
    }

    let chosen: Vec<(ActorId, SocketAddr)> = {
        let candidates = {
            let members = agent.members().read();

            members
                .states
                .iter()
                .filter(|(id, _state)| **id != agent.actor_id())
                .map(|(id, state)| (*id, state.ring.unwrap_or(255), state.addr))
                .collect::<Vec<(ActorId, u8, SocketAddr)>>()
        };

        if candidates.is_empty() {
            return Ok(());
        }

        debug!("found {} candidates to synchronize with", candidates.len());

        let desired_count = cmp::max(cmp::min(candidates.len() / 100, 10), 3);

        let mut rng = StdRng::from_entropy();

        let mut choices = candidates
            .into_iter()
            .choose_multiple(&mut rng, desired_count * 2);

        choices.sort_by(|a, b| {
            // most missing actors first
            sync_state
                .need_len_for_actor(&b.0)
                .cmp(&sync_state.need_len_for_actor(&a.0))
                // if equal, look at proximity (via `ring`)
                .then_with(|| a.1.cmp(&b.1))
        });

        choices.truncate(desired_count);
        choices
            .into_iter()
            .map(|(actor_id, _, addr)| (actor_id, addr))
            .collect()
    };

    if chosen.is_empty() {
        return Ok(());
    }

    let start = Instant::now();
    let n = parallel_sync(agent, transport, chosen.clone(), sync_state).await?;

    let elapsed = start.elapsed();
    if n > 0 {
        info!(
            "synced {n} changes w/ {} in {}s @ {} changes/s",
            chosen
                .into_iter()
                .map(|(actor_id, _)| actor_id.to_string())
                .collect::<Vec<_>>()
                .join(", "),
            elapsed.as_secs_f64(),
            n as f64 / elapsed.as_secs_f64()
        );
    }
    Ok(())
}

const MIN_CHANGES_CHUNK: usize = 1000;

async fn handle_changes(
    agent: Agent,
    mut rx_changes: Receiver<(ChangeV1, ChangeSource)>,
    mut tripwire: Tripwire,
) {
    let mut buf = vec![];
    let mut count = 0;

    let mut max_wait = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            Some((change, src)) = rx_changes.recv() => {
                counter!("corro.agent.changes.recv", std::cmp::max(change.len(), 1) as u64); // count empties...
                count += change.len(); // don't count empties
                buf.push((change, src));
                if count < MIN_CHANGES_CHUNK {
                    continue;
                }
            },
            _ = max_wait.tick() => {
                // got a wait interval tick...
                if buf.is_empty() {
                    continue;
                }
            },
            _ = &mut tripwire => {
                break;
            }
            else => {
                break;
            }
        }

        // drain and process current changes!
        if let Err(e) = process_multiple_changes(&agent, buf.drain(..).collect()).await {
            error!("could not process multiple changes: {e}");
        }

        // reset count
        count = 0;
    }

    info!("Draining changes receiver...");

    // drain!
    while let Ok((change, src)) = rx_changes.try_recv() {
        let changes_count = std::cmp::max(change.len(), 1);
        counter!("corro.agent.changes.recv", changes_count as u64);
        count += changes_count;
        buf.push((change, src));
        if count >= MIN_CHANGES_CHUNK {
            // drain and process current changes!
            if let Err(e) = process_multiple_changes(&agent, buf.drain(..).collect()).await {
                error!("could not process multiple changes: {e}");
            }

            // reset count
            count = 0;
        }
    }

    // process the last changes we got!
    if let Err(e) = process_multiple_changes(&agent, buf).await {
        error!("could not process multiple changes: {e}");
    }
}

const CHECK_EMPTIES_TO_INSERT_AFTER: Duration = Duration::from_secs(120);

async fn write_empties_loop(
    agent: Agent,
    mut rx_empty: Receiver<(ActorId, RangeInclusive<i64>)>,
    mut tripwire: Tripwire,
) {
    let mut empties: BTreeMap<ActorId, RangeInclusiveSet<i64>> = BTreeMap::new();

    let next_empties_check = tokio::time::sleep(CHECK_EMPTIES_TO_INSERT_AFTER);
    tokio::pin!(next_empties_check);

    loop {
        tokio::select! {
            maybe_empty = rx_empty.recv() => match maybe_empty {
                Some((actor_id, versions)) => {
                    empties.entry(actor_id).or_default().insert(versions);
                    continue;
                },
                None => {
                    debug!("empties queue is done");
                    break;
                }
            },
            _ = &mut next_empties_check => {
                next_empties_check.as_mut().reset(tokio::time::Instant::now() + CHECK_EMPTIES_TO_INSERT_AFTER);
                if empties.is_empty() {
                    continue;
                }
            },
            _ = &mut tripwire => break
        }

        if let Err(e) = process_completed_empties(&agent, &mut empties).await {
            error!("could not process empties: {e}");
        }
    }
    info!("Draining empty versions to process...");
    // drain empties channel
    while let Ok((actor_id, versions)) = rx_empty.try_recv() {
        empties.entry(actor_id).or_default().insert(versions);
    }

    if !empties.is_empty() {
        info!("Inserting last unprocessed empties before shut down");
        if let Err(e) = process_completed_empties(&agent, &mut empties).await {
            error!("could not process empties: {e}");
        }
    }
}

async fn sync_loop(
    agent: Agent,
    transport: Transport,
    mut rx_apply: Receiver<(ActorId, i64)>,
    rx_empty: Receiver<(ActorId, RangeInclusive<i64>)>,
    mut tripwire: Tripwire,
) {
    let mut sync_backoff = backoff::Backoff::new(0)
        .timeout_range(Duration::from_secs(1), MAX_SYNC_BACKOFF)
        .iter();
    let next_sync_at = tokio::time::sleep(sync_backoff.next().unwrap());
    tokio::pin!(next_sync_at);

    // TODO: move this elsewhere, doesn't have be spawned here...
    spawn_counted(write_empties_loop(
        agent.clone(),
        rx_empty,
        tripwire.clone(),
    ));

    loop {
        enum Branch {
            Tick,
            BackgroundApply { actor_id: ActorId, version: i64 },
        }

        let branch = tokio::select! {
            biased;

            maybe_item = rx_apply.recv() => match maybe_item {
                Some((actor_id, version)) => Branch::BackgroundApply{actor_id, version},
                None => {
                    debug!("background applies queue is closed, breaking out of loop");
                    break;
                }
            },

            _ = &mut next_sync_at => {
                Branch::Tick
            },
            _ = &mut tripwire => {
                break;
            }
        };

        match branch {
            Branch::Tick => {
                // ignoring here, there is trying and logging going on inside
                match handle_sync(&agent, &transport)
                    .preemptible(&mut tripwire)
                    .await
                {
                    tripwire::Outcome::Preempted(_) => {
                        warn!("aborted sync by tripwire");
                        break;
                    }
                    tripwire::Outcome::Completed(res) => {
                        if let Err(e) = res {
                            error!("could not sync: {e}");
                            // keep syncing until we successfully sync
                            continue;
                        }
                    }
                }
                next_sync_at
                    .as_mut()
                    .reset(tokio::time::Instant::now() + sync_backoff.next().unwrap());
            }
            Branch::BackgroundApply { actor_id, version } => {
                debug!(%actor_id, version, "picked up background apply of buffered changes");
                match process_fully_buffered_changes(&agent, actor_id, version).await {
                    Ok(false) => {
                        warn!(%actor_id, version, "did not apply buffered changes");
                    }
                    Ok(true) => {
                        debug!(%actor_id, version, "succesfully applied buffered changes");
                    }
                    Err(e) => {
                        error!(%actor_id, version, "could not apply fully buffered changes: {e}");
                    }
                }
            }
        }
    }
}

#[tracing::instrument(skip_all, err)]
async fn process_completed_empties(
    agent: &Agent,
    empties: &mut BTreeMap<ActorId, RangeInclusiveSet<i64>>,
) -> eyre::Result<()> {
    debug!(
        "processing empty versions (count: {})",
        empties.values().map(RangeInclusiveSet::len).sum::<usize>()
    );

    let mut inserted = 0;

    let start = Instant::now();
    while let Some((actor_id, empties)) = empties.pop_first() {
        let v = empties.into_iter().collect::<Vec<_>>();

        for ranges in v.chunks(50) {
            let mut conn = agent.pool().write_low().await?;
            block_in_place(|| {
                let tx = conn.transaction()?;

                for range in ranges {
                    inserted += store_empty_changeset(&tx, actor_id, range.clone())?;
                }

                tx.commit()?;

                Ok::<_, eyre::Report>(())
            })?;
        }
    }

    debug!(
        "upserted {inserted} empty version ranges in {:?}",
        start.elapsed()
    );

    Ok(())
}

pub fn migrate(conn: &mut Connection) -> rusqlite::Result<()> {
    let migrations: Vec<Box<dyn Migration>> = vec![
        Box::new(init_migration as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(v0_2_0_migration as fn(&Transaction) -> rusqlite::Result<()>),
    ];

    corro_types::sqlite::migrate(conn, migrations)
}

fn init_migration(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        r#"
            -- key/value for internal corrosion data (e.g. 'schema_version' => INT)
            CREATE TABLE __corro_state (key TEXT NOT NULL PRIMARY KEY, value);

            -- internal bookkeeping
            CREATE TABLE __corro_bookkeeping (
                actor_id BLOB NOT NULL,
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
                pk BLOB NOT NULL,
                cid TEXT NOT NULL,
                val ANY, -- shouldn't matter I don't think
                col_version INTEGER NOT NULL,
                db_version INTEGER NOT NULL,
                site_id BLOB NOT NULL, -- this differs from crsql_changes, we'll never buffer our own
                seq INTEGER NOT NULL,
                cl INTEGER NOT NULL, -- causal length

                version INTEGER NOT NULL,

                PRIMARY KEY (site_id, db_version, version, seq)
            ) WITHOUT ROWID;
            
            -- SWIM memberships
            CREATE TABLE __corro_members (
                actor_id BLOB PRIMARY KEY NOT NULL,
                address TEXT NOT NULL,
            
                state TEXT NOT NULL DEFAULT 'down',
                foca_state JSON,

                rtts JSON DEFAULT '[]'
            ) WITHOUT ROWID;

            -- tracked corrosion schema
            CREATE TABLE __corro_schema (
                tbl_name TEXT NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                sql TEXT NOT NULL,
            
                source TEXT NOT NULL,
            
                PRIMARY KEY (tbl_name, type, name)
            ) WITHOUT ROWID;
        "#,
    )?;

    Ok(())
}

fn v0_2_0_migration(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        "
        CREATE INDEX __corro_bookkeeping_db_version ON __corro_bookkeeping (db_version);
    ",
    )
}

#[cfg(test)]
pub mod tests {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        time::{Duration, Instant},
    };

    use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
    use rand::{
        distributions::Uniform, prelude::Distribution, rngs::StdRng, seq::IteratorRandom,
        SeedableRng,
    };
    use serde::Deserialize;
    use serde_json::json;
    use spawn::wait_for_all_pending_handles;
    use tokio::time::{sleep, timeout, MissedTickBehavior};
    use tracing::info_span;
    use tripwire::Tripwire;

    use super::*;

    use corro_types::api::{ExecResponse, ExecResult, Statement};

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
            [1, "hello world 1"]
        ],]))?;

        let res = timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

        let body: ExecResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        let db_version: i64 =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
        assert_eq!(db_version, 1);

        println!("body: {body:?}");

        #[derive(Debug, Deserialize)]
        struct TestRecord {
            id: i64,
            text: String,
        }

        let svc: TestRecord = ta1.agent.pool().read().await?.query_row(
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

        let svc: TestRecord = ta2.agent.pool().read().await?.query_row(
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

        let req_body: Vec<Statement> = serde_json::from_value(json!([[
            "INSERT INTO tests (id,text) VALUES (?,?)",
            [2, "hello world 2"]
        ]]))?;

        let res = client
            .request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            )
            .await?;

        let body: ExecResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");

        #[allow(clippy::type_complexity)]
        let bk: Vec<(ActorId, i64, Option<i64>, i64, Option<i64>)> = ta1
            .agent
            .pool()
            .read()
            .await?
            .prepare("SELECT actor_id, start_version, end_version, db_version, last_seq FROM __corro_bookkeeping")?
            .query_map((), |row| {
                Ok((
                    row.get::<_, ActorId>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;

        assert_eq!(
            bk,
            vec![
                (ta1.agent.actor_id(), 1, None, 1, Some(0)),
                (ta1.agent.actor_id(), 2, None, 2, Some(0))
            ]
        );

        let svc: TestRecord = ta1.agent.pool().read().await?.query_row(
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

        let svc: TestRecord = ta2.agent.pool().read().await?.query_row(
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
                    [id, format!("hello world #{id}")],
                ])
            })
            .collect();

        let req_body: Vec<Statement> = serde_json::from_value(json!(values))?;

        timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

        let db_version: i64 =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
        assert_eq!(db_version, 3);

        let expected_count: i64 =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT COUNT(*) FROM tests", (), |row| row.get(0))?;

        sleep(Duration::from_secs(5)).await;

        let got_count: i64 =
            ta2.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT COUNT(*) FROM tests", (), |row| row.get(0))?;

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
                    [n, format!("hello world {n}")]
                ],
                [
                    "INSERT INTO tests2 (id,text) VALUES (?,?)",
                    [n, format!("hello world {n}")]
                ],
                [
                    "INSERT INTO tests (id,text) VALUES (?,?)",
                    [n + 10000, format!("hello world {n}")]
                ],
                [
                    "INSERT INTO tests2 (id,text) VALUES (?,?)",
                    [n + 10000, format!("hello world {n}")]
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
                                    .uri(format!("http://{chosen}/v1/transactions"))
                                    .header(hyper::header::CONTENT_TYPE, "application/json")
                                    .body(serde_json::to_vec(&statements)?.into())?,
                            )
                            .await?;

                        if res.status() != StatusCode::OK {
                            eyre::bail!("unexpected status code: {}", res.status());
                        }

                        let body: ExecResponse =
                            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

                        for (i, statement) in statements.iter().enumerate() {
                            if !matches!(
                                body.results[i],
                                ExecResult::Execute {
                                    rows_affected: 1,
                                    ..
                                }
                            ) {
                                eyre::bail!(
                                    "unexpected exec result for statement {i}: {statement:?}"
                                );
                            }
                        }

                        Ok::<_, eyre::Report>(())
                    })
                }
            })
            .try_buffer_unordered(10)
            .try_collect::<Vec<()>>()
            .await?;
            Ok::<_, eyre::Report>(())
        });

        let changes_count = 4 * count;

        println!("expecting {changes_count} ops");

        let start = Instant::now();

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            println!("checking status after {}s", start.elapsed().as_secs_f32());
            let mut v = vec![];
            for ta in agents.iter() {
                let span = info_span!("consistency", actor_id = %ta.agent.actor_id().0);
                let _entered = span.enter();

                let conn = ta.agent.pool().read().await?;
                let counts: HashMap<ActorId, i64> = conn
                    .prepare_cached(
                        "SELECT COALESCE(site_id, crsql_site_id()), count(*) FROM crsql_changes GROUP BY site_id;",
                    )?
                    .query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                        ))
                    })?
                    .collect::<rusqlite::Result<_>>()?;

                debug!("versions count: {counts:?}");

                let actual_count: i64 =
                    conn.query_row("SELECT count(*) FROM crsql_changes;", (), |row| row.get(0))?;
                debug!("actual count: {actual_count}");

                let bookie = ta.agent.bookie();

                debug!(
                    "last version: {:?}",
                    bookie
                        .write("test")
                        .await
                        .for_actor(ta.agent.actor_id())
                        .read("test")
                        .await
                        .last()
                );

                let sync = generate_sync(bookie, ta.agent.actor_id()).await;
                let needed = sync.need_len();

                debug!("generated sync: {sync:?}");

                v.push((counts.values().sum::<i64>(), needed));
            }
            if v.len() != agents.len() {
                println!("got {} actors, expecting {}", v.len(), agents.len());
            }
            if v.len() == agents.len()
                && v.iter()
                    .all(|(n, needed)| *n == changes_count && *needed == 0)
            {
                break;
            }

            if start.elapsed() > Duration::from_secs(30) {
                let conn = agents[0].agent.pool().read().await?;
                let mut prepped = conn.prepare("SELECT * FROM crsql_changes;")?;
                let mut rows = prepped.query(())?;

                while let Ok(Some(row)) = rows.next() {
                    println!("row: {row:?}");
                }

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

    #[test]
    fn test_in_memory_versions_compaction() -> eyre::Result<()> {
        let mut conn = CrConn::init(rusqlite::Connection::open_in_memory()?)?;

        migrate(&mut conn)?;

        conn.execute_batch(
            "
            CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER);
            SELECT crsql_as_crr('foo');

            CREATE TABLE foo2 (a INTEGER NOT NULL PRIMARY KEY, b INTEGER);
            SELECT crsql_as_crr('foo2');
            ",
        )?;

        // db version 1
        conn.execute("INSERT INTO foo (a) VALUES (1)", ())?;

        // invalid, but whatever
        conn.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 1, crsql_db_version()", [])?;

        // db version 2
        conn.execute("DELETE FROM foo;", ())?;

        // invalid, but whatever
        conn.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 2, crsql_db_version()", [])?;

        let db_version: i64 = conn.query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;

        assert_eq!(db_version, 2);

        {
            let mut prepped = conn.prepare("SELECT * FROM __corro_bookkeeping")?;
            let mut rows = prepped.query([])?;

            while let Ok(Some(row)) = rows.next() {
                println!("row: {row:?}");
            }
        }

        {
            let mut prepped = conn.prepare("SELECT DISTINCT(db_version) FROM foo2__crsql_clock UNION SELECT DISTINCT(db_version) FROM foo__crsql_clock;")?;
            let mut rows = prepped.query([])?;

            while let Ok(Some(row)) = rows.next() {
                println!("row: {row:?}");
            }
        }

        let tx = conn.transaction()?;

        let to_clear = find_cleared_db_versions(&tx)?;

        println!("to_clear: {to_clear:?}");

        assert!(to_clear.contains(&1));
        assert!(!to_clear.contains(&2));

        tx.execute("DELETE FROM __corro_bookkeeping WHERE db_version = 1", [])?;
        tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) SELECT crsql_site_id(), 1, 1", [])?;

        let to_clear = find_cleared_db_versions(&tx)?;
        assert!(to_clear.is_empty());

        tx.execute("INSERT INTO foo2 (a) VALUES (2)", ())?;

        // invalid, but whatever
        tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 3, crsql_db_version()", [])?;

        tx.commit()?;

        let tx = conn.transaction()?;
        let to_clear = find_cleared_db_versions(&tx)?;
        assert!(to_clear.is_empty());

        tx.execute("INSERT INTO foo (a) VALUES (1)", ())?;
        tx.commit()?;

        let tx = conn.transaction()?;
        let to_clear = find_cleared_db_versions(&tx)?;

        assert!(to_clear.contains(&2));
        assert!(!to_clear.contains(&3));
        assert!(!to_clear.contains(&4));

        tx.execute("DELETE FROM __corro_bookkeeping WHERE db_version = 2", [])?;
        tx.execute(
            "UPDATE __corro_bookkeeping SET end_version = 2 WHERE start_version = 1;",
            [],
        )?;
        let to_clear = find_cleared_db_versions(&tx)?;

        assert!(to_clear.is_empty());

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

        let req_body: Vec<Statement> = serde_json::from_value(json!(["INSERT INTO tests  WITH RECURSIVE    cte(id) AS (       SELECT random()       UNION ALL       SELECT random()         FROM cte        LIMIT 10000  ) SELECT id, \"hello\" as text FROM cte;"]))?;

        let res = timeout(
            Duration::from_secs(5),
            client.request(
                hyper::Request::builder()
                    .method(hyper::Method::POST)
                    .uri(format!("http://{}/v1/transactions", ta1.agent.api_addr()))
                    .header(hyper::header::CONTENT_TYPE, "application/json")
                    .body(serde_json::to_vec(&req_body)?.into())?,
            ),
        )
        .await??;

        let body: ExecResponse =
            serde_json::from_slice(&hyper::body::to_bytes(res.into_body()).await?)?;

        println!("body: {body:?}");

        let db_version: i64 =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
        assert_eq!(db_version, 1);

        sleep(Duration::from_secs(2)).await;

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

        sleep(Duration::from_secs(5)).await;

        {
            let conn = ta2.agent.pool().read().await?;

            let count: i64 = conn
                .prepare_cached("SELECT COUNT(*) FROM tests;")?
                .query_row((), |row| row.get(0))?;

            println!(
                "{:#?}",
                generate_sync(ta2.agent.bookie(), ta2.agent.actor_id()).await
            );

            assert_eq!(
                count,
                10000,
                "actor {} did not reach 100K rows",
                ta2.agent.actor_id()
            );
        }

        {
            let conn = ta3.agent.pool().read().await?;

            let count: i64 = conn
                .prepare_cached("SELECT COUNT(*) FROM tests;")?
                .query_row((), |row| row.get(0))?;

            println!(
                "{:#?}",
                generate_sync(ta3.agent.bookie(), ta3.agent.actor_id()).await
            );

            assert_eq!(
                count,
                10000,
                "actor {} did not reach 100K rows",
                ta3.agent.actor_id()
            );
        }
        {
            let conn = ta4.agent.pool().read().await?;

            let count: i64 = conn
                .prepare_cached("SELECT COUNT(*) FROM tests;")?
                .query_row((), |row| row.get(0))?;

            println!(
                "{:#?}",
                generate_sync(ta4.agent.bookie(), ta4.agent.actor_id()).await
            );

            assert_eq!(
                count,
                10000,
                "actor {} did not reach 100K rows",
                ta4.agent.actor_id()
            );
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn many_small_changes() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let agents = futures::StreamExt::fold(futures::stream::iter(0..10).chunks(50), vec![], {
            let tripwire = tripwire.clone();
            move |mut agents: Vec<TestAgent>, to_launch| {
                let tripwire = tripwire.clone();
                async move {
                    for n in to_launch {
                        println!("LAUNCHING AGENT #{n}");
                        let mut rng = StdRng::from_entropy();
                        let bootstrap = agents
                            .iter()
                            .map(|ta| ta.agent.gossip_addr())
                            .choose_multiple(&mut rng, 10);
                        agents.push(
                            launch_test_agent(
                                |conf| {
                                    conf.gossip_addr("127.0.0.1:0".parse().unwrap())
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
                    agents
                }
            }
        })
        .await;

        let mut start_id = 0;

        FuturesUnordered::from_iter(agents.iter().map(|ta| {
            let ta = ta.clone();
            start_id += 100000;
            async move {
                tokio::spawn(async move {
                    let client: hyper::Client<_, hyper::Body> =
                        hyper::Client::builder().build_http();

                    let durs = {
                        let between = Uniform::from(100..=1000);
                        let mut rng = rand::thread_rng();
                        (0..100)
                            .map(|_| between.sample(&mut rng))
                            .collect::<Vec<_>>()
                    };

                    let api_addr = ta.agent.api_addr();
                    let actor_id = ta.agent.actor_id();

                    FuturesUnordered::from_iter(durs.into_iter().map(|dur| {
                        let client = client.clone();
                        start_id += 1;
                        async move {
                            sleep(Duration::from_millis(dur)).await;

                            let req_body = serde_json::from_value::<Vec<Statement>>(json!([[
                                "INSERT INTO tests (id,text) VALUES (?,?)",
                                [start_id, format!("hello from {actor_id}")]
                            ],]))?;

                            let res = client
                                .request(
                                    hyper::Request::builder()
                                        .method(hyper::Method::POST)
                                        .uri(format!("http://{api_addr}/v1/transactions"))
                                        .header(hyper::header::CONTENT_TYPE, "application/json")
                                        .body(serde_json::to_vec(&req_body)?.into())?,
                                )
                                .await?;

                            if res.status() != StatusCode::OK {
                                eyre::bail!("bad status code: {}", res.status());
                            }

                            let body: ExecResponse = serde_json::from_slice(
                                &hyper::body::to_bytes(res.into_body()).await?,
                            )?;

                            match &body.results[0] {
                                ExecResult::Execute { .. } => {}
                                ExecResult::Error { error } => {
                                    eyre::bail!("error: {error}");
                                }
                            }

                            Ok::<_, eyre::Report>(())
                        }
                    }))
                    .try_collect()
                    .await?;

                    Ok::<_, eyre::Report>(())
                })
                .await??;
                Ok::<_, eyre::Report>(())
            }
        }))
        .try_collect()
        .await?;

        sleep(Duration::from_secs(10)).await;

        for ta in agents {
            let conn = ta.agent.pool().read().await?;
            let count: i64 = conn.query_row("SELECT count(*) FROM tests", (), |row| row.get(0))?;

            println!("actor: {}, count: {count}", ta.agent.actor_id());
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
