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
            pubsub::{api_v1_sub_by_id, api_v1_subs, process_sub_channel, MatcherBroadcastCache},
        },
    },
    broadcast::runtime_loop,
    transport::{Transport, TransportError},
};

use arc_swap::ArcSwap;
use corro_types::{
    actor::{Actor, ActorId},
    agent::{
        migrate, Agent, AgentConfig, BookedVersions, Bookie, ChangeError, CurrentVersion,
        KnownDbVersion, PartialVersion, SplitPool,
    },
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{
        BiPayload, BiPayloadV1, BroadcastInput, BroadcastV1, ChangeSource, ChangeV1, Changeset,
        ChangesetParts, FocaInput, Timestamp, UniPayload, UniPayloadV1,
    },
    config::{AuthzConfig, Config, DEFAULT_GOSSIP_PORT},
    members::Members,
    pubsub::{Matcher, SubsManager},
    schema::init_schema,
    sqlite::{CrConn, SqlitePoolError},
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
use futures::{FutureExt, StreamExt, TryFutureExt};
use hyper::{server::conn::AddrIncoming, StatusCode};
use itertools::Itertools;
use metrics::{counter, gauge, histogram, increment_counter};
use parking_lot::RwLock;
use rand::{rngs::StdRng, seq::IteratorRandom, SeedableRng};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{
    named_params, params, params_from_iter, Connection, OptionalExtension, ToSql, Transaction,
};
use spawn::spawn_counted;
use speedy::Readable;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Semaphore,
    },
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
    pub rx_apply: Receiver<(ActorId, Version)>,
    pub rx_empty: Receiver<(ActorId, RangeInclusive<Version>)>,
    pub rx_clear_buf: Receiver<(ActorId, RangeInclusive<Version>)>,
    pub rx_changes: Receiver<(ChangeV1, ChangeSource)>,
    pub rx_foca: Receiver<FocaInput>,
    pub rtt_rx: Receiver<(SocketAddr, Duration)>,
    pub subs_manager: SubsManager,
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

    let write_sema = Arc::new(Semaphore::new(1));

    let pool = SplitPool::create(&conf.db.path, write_sema.clone()).await?;

    let schema = {
        let mut conn = pool.write_priority().await?;
        migrate(&mut conn)?;
        let mut schema = init_schema(&conn)?;
        schema.constrain()?;

        info!("Ensuring clock table indexes for fast compaction");
        let start = Instant::now();
        for table in schema.tables.keys() {
            conn.execute_batch(&format!("CREATE INDEX IF NOT EXISTS corro_{table}__crsql_clock_site_id_dbv ON {table}__crsql_clock (site_id, db_version);"))?;
        }
        info!("Ensured indexes in {:?}", start.elapsed());

        schema
    };

    let (tx_apply, rx_apply) = channel(20480);
    let (tx_clear_buf, rx_clear_buf) = channel(10240);

    let mut bk: HashMap<ActorId, BookedVersions> = HashMap::new();

    {
        debug!("getting read-only conn for pull bookkeeping rows");
        let mut conn = pool.write_priority().await?;

        debug!("getting bookkept rows");

        let mut cleared_rows: BTreeMap<ActorId, usize> = BTreeMap::new();

        {
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
                        let actor_id = row.get(0)?;
                        let ranges = bk.entry(actor_id).or_default();
                        let start_v = row.get(1)?;
                        let end_v: Option<Version> = row.get(2)?;
                        ranges.insert_many(
                            start_v..=end_v.unwrap_or(start_v),
                            match row.get(3)? {
                                Some(db_version) => KnownDbVersion::Current(CurrentVersion {
                                    db_version,
                                    last_seq: row.get(4)?,
                                    ts: row.get(5)?,
                                }),
                                None => {
                                    *cleared_rows.entry(actor_id).or_default() += 1;
                                    KnownDbVersion::Cleared
                                }
                            },
                        );
                    }
                }
            }

            let mut partials: HashMap<
                (ActorId, Version),
                (RangeInclusiveSet<CrsqlSeq>, CrsqlSeq, Timestamp),
            > = HashMap::new();

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
                debug!(%actor_id, %version, "looking at partials (seq: {seqs:?}, last_seq: {last_seq})");
                let ranges = bk.entry(actor_id).or_default();

                if let Some(known) = ranges.get(&version) {
                    warn!(%actor_id, %version, "found partial data that has been applied, clearing buffered meta, known: {known:?}");

                    _ = tx_clear_buf.try_send((actor_id, version..=version));
                    continue;
                }

                let gaps_count = seqs.gaps(&(CrsqlSeq(0)..=last_seq)).count();

                ranges.insert(
                    version,
                    KnownDbVersion::Partial(PartialVersion { seqs, last_seq, ts }),
                );

                if gaps_count == 0 {
                    info!(%actor_id, %version, "found fully buffered, unapplied, changes! scheduling apply");
                    // spawn this because it can block if the channel gets full, nothing is draining it yet!
                    tokio::spawn({
                        let tx_apply = tx_apply.clone();
                        async move {
                            let _ = tx_apply.send((actor_id, version)).await;
                        }
                    });
                }
            }
        }

        for (actor_id, booked) in bk.iter() {
            if let Some(clear_count) = cleared_rows.get(actor_id).copied() {
                if clear_count > booked.cleared.len() {
                    warn!(%actor_id, "cleared bookkept rows count ({clear_count}) in DB was bigger than in-memory entries count ({}), compacting...", booked.cleared.len());
                    let tx = conn.immediate_transaction()?;
                    let deleted = tx.execute("DELETE FROM __corro_bookkeeping WHERE actor_id = ? AND end_version IS NOT NULL", [actor_id])?;
                    info!("deleted {deleted} rows that had an end_version");
                    let mut inserted = 0;
                    for range in booked.cleared.iter() {
                        inserted += tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) VALUES (?, ?, ?)", params![actor_id, range.start(), range.end()])?;
                    }
                    info!("inserted {inserted} cleared version rows");
                    tx.commit()?;
                }
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

    let subs_manager = SubsManager::default();

    let opts = AgentOptions {
        actor_id,
        gossip_server_endpoint,
        transport,
        api_listener,
        rx_bcast,
        rx_apply,
        rx_empty,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        rtt_rx,
        subs_manager: subs_manager.clone(),
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
        tx_clear_buf,
        tx_changes,
        tx_foca,
        write_sema,
        schema: RwLock::new(schema),
        subs_manager,
        tripwire,
    });

    Ok((agent, opts))
}

pub async fn start(conf: Config, tripwire: Tripwire) -> eyre::Result<Agent> {
    let (agent, opts) = setup(conf, tripwire.clone()).await?;

    tokio::spawn({
        let agent = agent.clone();
        async move {
            match run(agent, opts).await {
                Ok(_) => info!("corrosion agent run is done"),
                Err(e) => error!("running corrosion agent failed: {e}"),
            }
        }
    });

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
        rx_clear_buf,
        rx_changes,
        rx_foca,
        subs_manager,
        rtt_rx,
    } = opts;

    let mut subs_bcast_cache = MatcherBroadcastCache::default();

    {
        let subs_path = agent.config().db.subscriptions_path();

        let mut to_cleanup = vec![];

        if let Ok(mut dir) = tokio::fs::read_dir(&subs_path).await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let path_str = entry.path().display().to_string();
                if let Some(sub_id_str) = path_str.strip_prefix(subs_path.as_str()) {
                    if let Ok(sub_id) = sub_id_str.trim_matches('/').parse() {
                        let (_, created) = match subs_manager.restore(
                            sub_id,
                            &subs_path,
                            &agent.schema().read(),
                            agent.pool(),
                            tripwire.clone(),
                        ) {
                            Ok(res) => res,
                            Err(e) => {
                                error!(%sub_id, "could not restore subscription: {e}");
                                to_cleanup.push(sub_id);
                                continue;
                            }
                        };

                        info!(%sub_id, "Restored subscription");

                        let (sub_tx, _) = tokio::sync::broadcast::channel(10240);

                        tokio::spawn(process_sub_channel(
                            subs_manager.clone(),
                            sub_id,
                            sub_tx.clone(),
                            created.evt_rx,
                        ));

                        subs_bcast_cache.insert(sub_id, sub_tx);
                    }
                }
            }
        }

        for id in to_cleanup {
            info!(sub_id = %id, "Cleaning up unclean subscription");
            Matcher::cleanup(id, Matcher::sub_path(subs_path.as_path(), id))?;
        }
    };

    let subs_bcast_cache = Arc::new(tokio::sync::RwLock::new(subs_bcast_cache));

    if let Some(pg_conf) = agent.config().api.pg.clone() {
        info!("Starting PostgreSQL wire-compatible server");
        let pg_server = corro_pg::start(agent.clone(), pg_conf, tripwire.clone()).await?;
        info!(
            "Started PostgreSQL wire-compatible server, listening at {}",
            pg_server.local_addr
        );
    }

    let (to_send_tx, to_send_rx) = channel(10240);
    let (notifications_tx, notifications_rx) = channel(10240);

    let (bcast_msg_tx, bcast_rx) = channel::<BroadcastV1>(10240);

    let gossip_addr = gossip_server_endpoint.local_addr()?;

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
                            error!("could not handshake connection from {remote_addr}: {e}");
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
            match conn.prepare("SELECT address,foca_state FROM __corro_members") {
                Ok(mut prepped) => {
                    match prepped
                    .query_map([], |row| Ok((
                            row.get::<_, String>(0)?.parse().map_err(|e| rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e)))?,
                            row.get::<_, String>(1)?
                        ))
                    )
                    .and_then(|rows| rows.collect::<rusqlite::Result<Vec<(SocketAddr, String)>>>())
                {
                    Ok(members) => {
                        members.into_iter().filter_map(|(address, state)| match serde_json::from_str::<foca::Member<Actor>>(state.as_str()) {
                            Ok(fs) => Some((address, fs)),
                            Err(e) => {
                                error!("could not deserialize foca member state: {e} (json: {state})");
                                None
                            }
                        }).collect::<Vec<(SocketAddr, Member<Actor>)>>()
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
            for (address, foca_state) in states {
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
                .layer(Extension(subs_bcast_cache))
                .layer(Extension(subs_manager))
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

    spawn_counted(write_empties_loop(
        agent.clone(),
        rx_empty,
        tripwire.clone(),
    ));

    tokio::spawn(clear_buffered_meta_loop(agent.clone(), rx_clear_buf));

    spawn_counted(
        sync_loop(agent.clone(), transport.clone(), rx_apply, tripwire.clone())
            .inspect(|_| info!("corrosion agent sync loop is done")),
    );

    tokio::spawn(handle_gossip_to_send(transport.clone(), to_send_rx));
    tokio::spawn(handle_notifications(agent.clone(), notifications_rx));
    tokio::spawn(metrics_loop(agent.clone(), transport));

    tokio::spawn(handle_broadcasts(agent.clone(), bcast_rx));

    let mut db_cleanup_interval = tokio::time::interval(Duration::from_secs(60 * 15));

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

    loop {
        sleep(COMPACT_BOOKED_INTERVAL).await;

        info!("Starting compaction...");
        let start = Instant::now();

        let bookie_clone = {
            bookie
                .read("gather_booked_for_compaction")
                .await
                .iter()
                .map(|(actor_id, booked)| (*actor_id, booked.clone()))
                .collect::<HashMap<ActorId, _>>()
        };

        let mut inserted = 0;
        let mut deleted = 0;

        let mut db_elapsed = Duration::new(0, 0);

        for (actor_id, booked) in bookie_clone {
            // pull the current db version -> version map at the present time
            // these are only updated _after_ a transaction has been committed, via a write lock
            // so it should be representative of the current state.
            let mut versions = {
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

            // we're using a read connection here, starting a read-only transaction
            // this should be representative of the state of current versions from the actor
            let cleared_versions = match pool.read().await {
                Ok(mut conn) => {
                    let start = Instant::now();
                    let res = block_in_place(|| {
                        let tx = conn.transaction()?;
                        find_cleared_db_versions(&tx, &actor_id)
                    });
                    db_elapsed += start.elapsed();
                    match res {
                        Ok(cleared) => {
                            debug!(
                                actor_id = %actor_id,
                                "Aggregated {} DB versions to clear in {:?}",
                                cleared.len(),
                                start.elapsed()
                            );
                            cleared
                        }
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
            };

            if !cleared_versions.is_empty() {
                let mut to_clear = Vec::new();

                for db_v in cleared_versions {
                    if let Some(v) = versions.remove(&db_v) {
                        to_clear.push((db_v, v))
                    }
                }

                if !to_clear.is_empty() {
                    // use a write lock here so we can mutate the bookkept state
                    let mut bookedw = booked
                        .write(format!("clearing:{}", actor_id.as_simple()))
                        .await;

                    for (_db_v, v) in to_clear.iter() {
                        // only remove when confirming that version is still considered "current"
                        if bookedw.contains_current(v) {
                            // set it as cleared right away
                            bookedw.insert(*v, KnownDbVersion::Cleared);
                            deleted += 1;
                        }
                    }

                    // find any affected cleared ranges
                    for range in to_clear
                        .iter()
                        .filter_map(|(_, v)| bookedw.cleared.get(v))
                        .dedup()
                    {
                        // schedule for clearing in the background task
                        if let Err(e) = agent.tx_empty().try_send((actor_id, range.clone())) {
                            error!("could not schedule version to be cleared: {e}");
                        } else {
                            inserted += 1;
                        }
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        info!(
            "Compaction done, cleared {} DB bookkeeping table rows (wall time: {:?}, db time: {db_elapsed:?})",
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
fn find_cleared_db_versions(
    tx: &Transaction,
    actor_id: &ActorId,
) -> rusqlite::Result<BTreeSet<CrsqlDbVersion>> {
    let clock_site_id: Option<u64> = match tx
        .prepare_cached("SELECT ordinal FROM crsql_site_id WHERE site_id = ?")?
        .query_row([actor_id], |row| row.get(0))
        .optional()?
    {
        Some(0) => None, // this is the current crsql_site_id which is going to be NULL in clock tables
        Some(ordinal) => Some(ordinal),
        None => {
            warn!(actor_id = %actor_id, "could not find crsql ordinal for actor");
            return Ok(Default::default());
        }
    };

    let tables = tx
        .prepare_cached(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name LIKE '%__crsql_clock'",
        )?
        .query_map([], |row| row.get(0))?
        .collect::<Result<BTreeSet<String>, _>>()?;

    if tables.is_empty() {
        // means there's no schema trakced by cr-sqlite or corrosion.
        return Ok(BTreeSet::new());
    }

    let mut params: Vec<&dyn ToSql> = vec![actor_id];
    let to_clear_query = format!(
        "SELECT DISTINCT db_version FROM __corro_bookkeeping WHERE actor_id = ? AND db_version IS NOT NULL
            EXCEPT SELECT db_version FROM ({});",
        tables
            .iter()
            .map(|table| {
                params.push(&clock_site_id);
                format!("SELECT DISTINCT db_version FROM {table} WHERE site_id IS ?")
            })
            .collect::<Vec<_>>()
            .join(" UNION ")
    );

    let cleared_db_versions: BTreeSet<CrsqlDbVersion> = tx
        .prepare_cached(&to_clear_query)?
        .query_map(params_from_iter(params.into_iter()), |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;

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

async fn handle_notifications(agent: Agent, mut notification_rx: Receiver<Notification<Actor>>) {
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
            let mut prepped =
                conn.prepare("SELECT address FROM __corro_members ORDER BY RANDOM() LIMIT 5")?;
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
    conn: &Connection,
    actor_id: ActorId,
    versions: RangeInclusive<Version>,
) -> eyre::Result<usize> {
    // first, delete "current" versions, they're now gone!
    let deleted: Vec<RangeInclusive<Version>> = conn
        .prepare_cached(
            "
        DELETE FROM __corro_bookkeeping 
            WHERE
                actor_id = :actor_id AND
                (
                    -- start_version is between start and end of range AND no end_version
                    ( start_version BETWEEN :start AND :end AND end_version IS NULL ) OR
                    
                    -- start_version and end_version are within the range
                    ( start_version >= :start AND end_version <= :end ) OR

                    -- range being inserted is partially contained within another
                    ( start_version <= :end AND end_version >= :end ) OR

                    -- start_version = end + 1 (to collapse ranges)
                    ( start_version = :end + 1 AND end_version IS NOT NULL ) OR

                    -- end_version = start - 1 (to collapse ranges)
                    ( end_version = :start - 1 )
                )
            RETURNING start_version, end_version",
        )?
        .query_map(
            named_params![
                ":actor_id": actor_id,
                ":start": versions.start(),
                ":end": versions.end(),
            ],
            |row| {
                let start = row.get(0)?;
                Ok(start..=row.get::<_, Option<Version>>(1)?.unwrap_or(start))
            },
        )
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    if !deleted.is_empty() {
        debug!(
            "deleted {} still-live versions from database's bookkeeping",
            deleted.len()
        );
    }

    // re-compute the ranges
    let mut new_ranges = RangeInclusiveSet::from_iter(deleted);
    new_ranges.insert(versions);

    // we should never have deleted non-contiguous ranges, abort!
    if new_ranges.len() > 1 {
        warn!("deleted non-contiguous ranges! {new_ranges:?}");
        // this serves as a failsafe
        eyre::bail!("deleted non-contiguous ranges: {new_ranges:?}");
    }

    let mut inserted = 0;

    for range in new_ranges {
        // insert cleared versions
        inserted += conn
        .prepare_cached(
            "
                INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version, db_version, last_seq, ts)
                    VALUES (?, ?, ?, NULL, NULL, NULL);
            ",
        )?
        .execute(params![actor_id, range.start(), range.end()])?;
    }

    Ok(inserted)
}

#[tracing::instrument(skip(agent), err)]
async fn process_fully_buffered_changes(
    agent: &Agent,
    actor_id: ActorId,
    version: Version,
) -> Result<bool, ChangeError> {
    let mut conn = agent.pool().write_normal().await?;
    debug!(%actor_id, %version, "acquired write (normal) connection to process fully buffered changes");

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
    debug!(%actor_id, %version, "acquired Booked write lock to process fully buffered changes");

    let inserted = block_in_place(|| {
        let (last_seq, ts) = {
            match bookedw.partials.get(&version) {
                Some(PartialVersion { seqs, last_seq, ts }) => {
                    if seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).count() != 0 {
                        error!(%actor_id, %version, "found sequence gaps: {:?}, aborting!", seqs.gaps(&(CrsqlSeq(0)..=*last_seq)).collect::<RangeInclusiveSet<CrsqlSeq>>());
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

        let tx = conn.immediate_transaction()?;

        info!(%actor_id, %version, "Processing buffered changes to crsql_changes (actor: {actor_id}, version: {version}, last_seq: {last_seq})");

        let max_db_version: Option<Option<CrsqlDbVersion>> = tx.prepare_cached("SELECT MAX(db_version) FROM __corro_buffered_changes WHERE site_id = ? AND version = ?")?.query_row(params![actor_id.as_bytes(), version], |row| row.get(0)).optional()?;

        let start = Instant::now();

        if let Some(max_db_version) = max_db_version.flatten() {
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
            info!(%actor_id, %version, "Inserted {count} rows from buffered into crsql_changes in {:?}", start.elapsed());
        } else {
            info!(%actor_id, %version, "No buffered rows, skipped insertion into crsql_changes");
        }

        if let Err(e) = agent.tx_clear_buf().try_send((actor_id, version..=version)) {
            error!("could not schedule buffered data clear: {e}");
        }

        let rows_impacted: i64 = tx
            .prepare_cached("SELECT crsql_rows_impacted()")?
            .query_row((), |row| row.get(0))?;

        debug!(%actor_id, %version, "rows impacted by buffered changes insertion: {rows_impacted}");

        let known_version = if rows_impacted > 0 {
            let db_version: CrsqlDbVersion =
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

            debug!(%actor_id, %version, "inserted bookkeeping row after buffered insert");

            Some(KnownDbVersion::Current(CurrentVersion {
                db_version,
                last_seq,
                ts,
            }))
        } else {
            if let Err(e) = agent.tx_empty().try_send((actor_id, version..=version)) {
                error!(%actor_id, "could not schedule empties for clear: {e}");
            }

            debug!(%actor_id, %version, "inserted CLEARED bookkeeping row after buffered insert");
            Some(KnownDbVersion::Cleared)
        };

        tx.commit()?;

        let inserted = if let Some(known_version) = known_version {
            bookedw.insert(version, known_version);

            drop(bookedw);

            true
        } else {
            false
        };

        Ok::<_, rusqlite::Error>(inserted)
    }).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(actor_id), version: Some(version)})?;

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

    let changesets = block_in_place(|| {
        let start = Instant::now();
        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

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
                    trace!("handling a single changeset: {change:?}");
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
                                KnownDbVersion::Partial(PartialVersion { seqs, .. }) => {
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
                    let known = if change.is_complete() && change.is_empty() {
                        // we never want to block here
                        if let Err(e) = agent.tx_empty().try_send((actor_id, change.versions())) {
                            error!("could not send empty changed versions into channel: {e}");
                        }

                        KnownDbVersion::Cleared
                    } else {
                        if let Some(seqs) = change.seqs() {
                            if seqs.end() < seqs.start() {
                                warn!(%actor_id, versions = ?change.versions(), "received an invalid change, seqs start is greater than seqs end: {seqs:?}");
                                continue;
                            }
                        }

                        let (known, versions) = match process_single_version(
                            agent,
                            &tx,
                            last_db_version,
                            change,
                        ) {
                            Ok((known, changeset)) => {
                                let versions = changeset.versions();
                                if let KnownDbVersion::Current(CurrentVersion {
                                    db_version, ..
                                }) = &known
                                {
                                    last_db_version = Some(*db_version);
                                    changesets.push((actor_id, changeset, *db_version, src));
                                }
                                (known, versions)
                            }
                            Err(e) => {
                                error!(%actor_id, ?versions, "could not process single change: {e}");
                                continue;
                            }
                        };
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "got known to insert: {known:?}");
                        known
                    };

                    seen.insert(versions.clone(), known.clone());
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
                    KnownDbVersion::Current(CurrentVersion {
                        db_version,
                        last_seq,
                        ts,
                    }) => {
                        count += 1;
                        let version = versions.start();
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), %version, "inserting bookkeeping row db_version: {db_version}, ts: {ts:?}");
                        tx.prepare_cached("
                            INSERT INTO __corro_bookkeeping ( actor_id,  start_version,  db_version,  last_seq,  ts)
                                                    VALUES  (:actor_id, :start_version, :db_version, :last_seq, :ts);").map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(*actor_id), version: Some(*version)})?
                            .execute(named_params!{
                                ":actor_id": actor_id,
                                ":start_version": *version,
                                ":db_version": *db_version,
                                ":last_seq": *last_seq,
                                ":ts": *ts
                            }).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(*actor_id), version: Some(*version)})?;
                    }
                    KnownDbVersion::Cleared => {
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserting CLEARED bookkeeping");
                        if let Err(e) = agent.tx_empty().try_send((*actor_id, versions.clone())) {
                            error!("could not schedule version to be cleared: {e}");
                        }
                    }
                }
                debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserted bookkeeping row");
            }
        }

        debug!("inserted {count} new changesets");

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: None,
            version: None,
        })?;

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
                let version = *versions.start();
                // this merges partial version seqs
                if let Some(PartialVersion { seqs, last_seq, .. }) =
                    booked_write.insert_many(versions, known)
                {
                    let full_seqs_range = CrsqlSeq(0)..=last_seq;
                    let gaps_count = seqs.gaps(&full_seqs_range).count();
                    if gaps_count == 0 {
                        // if we have no gaps, then we can schedule applying all these changes.
                        debug!(%actor_id, %version, "we now have all versions, notifying for background jobber to insert buffered changes! seqs: {seqs:?}, expected full seqs: {full_seqs_range:?}");
                        let tx_apply = agent.tx_apply().clone();
                        tokio::spawn(async move {
                            if let Err(e) = tx_apply.send((actor_id, version)).await {
                                error!("could not send trigger for applying fully buffered changes later: {e}");
                            }
                        });
                    } else {
                        debug!(%actor_id, %version, "still have {gaps_count} gaps in partially buffered seqs");
                    }
                }
            }
        }

        Ok::<_, ChangeError>(changesets)
    })?;

    for (actor_id, changeset, db_version, src) in changesets {
        agent
            .subs_manager()
            .match_changes(changeset.changes(), db_version);

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

    let mut changes_per_table = BTreeMap::new();

    debug!(%actor_id, %version, "incomplete change, seqs: {seqs:?}, last_seq: {last_seq:?}, len: {}", changes.len());
    let mut inserted = 0;
    for change in changes.iter() {
        trace!("buffering change! {change:?}");

        // insert change, do nothing on conflict
        let new_insertion = tx.prepare_cached(
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

        inserted += new_insertion;

        if let Some(counter) = changes_per_table.get_mut(&change.table) {
            *counter += 1;
        } else {
            changes_per_table.insert(change.table.clone(), 1);
        }
    }

    debug!(%actor_id, %version, "buffered {inserted} changes");

    let deleted: Vec<RangeInclusive<CrsqlSeq>> = tx
        .prepare_cached(
            "
            DELETE FROM __corro_seq_bookkeeping
                WHERE site_id = :actor_id AND version = :version AND
                (
                    -- start_seq is between start and end of range AND no end_seq
                    ( start_seq BETWEEN :start AND :end AND end_seq IS NULL ) OR
                    
                    -- start_seq and end_seq are within the range
                    ( start_seq >= :start AND end_seq <= :end ) OR

                    -- range being inserted is partially contained within another
                    ( start_seq <= :end AND end_seq >= :end ) OR

                    -- start_seq = end + 1 (to collapse ranges)
                    ( start_seq = :end + 1 AND end_seq IS NOT NULL ) OR

                    -- end_seq = start - 1 (to collapse ranges)
                    ( end_seq = :start - 1 )
                )
        ",
        )?
        .query_map(
            named_params![
                ":actor_id": actor_id,
                ":version": version,
                ":start": seqs.start(),
                ":end": seqs.end(),
            ],
            |row| {
                let start = row.get(0)?;
                Ok(start..=row.get::<_, Option<CrsqlSeq>>(1)?.unwrap_or(start))
            },
        )
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

    // re-compute the ranges
    let mut new_ranges = RangeInclusiveSet::from_iter(deleted);
    new_ranges.insert(seqs.clone());

    // we should never have deleted non-contiguous seq ranges, abort!
    if new_ranges.len() > 1 {
        warn!("deleted non-contiguous ranges! {new_ranges:?}");
        // this serves as a failsafe
        return Err(rusqlite::Error::StatementChangedRows(new_ranges.len()));
    }

    // insert new seq ranges, there should only be one...
    for range in new_ranges.clone() {
        tx
        .prepare_cached(
            "
                INSERT INTO __corro_seq_bookkeeping (site_id, version, start_seq, end_seq, last_seq, ts)
                    VALUES (?, ?, ?, ?, ?, ?);
            ",
        )?
        .execute(params![actor_id, version, range.start(), range.end(), last_seq, ts])?;
    }

    for (table_name, count) in changes_per_table {
        counter!("corro.changes.committed", count, "table" => table_name.to_string(), "source" => "remote");
    }

    Ok(KnownDbVersion::Partial(PartialVersion {
        seqs: new_ranges,
        last_seq: *last_seq,
        ts: *ts,
    }))
}

#[tracing::instrument(skip(tx, last_db_version, parts), err)]
fn process_complete_version(
    tx: &Transaction,
    actor_id: ActorId,
    last_db_version: Option<CrsqlDbVersion>,
    versions: RangeInclusive<Version>,
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

    let max_db_version = changes
        .iter()
        .map(|c| c.db_version)
        .max()
        .unwrap_or(CrsqlDbVersion(0));

    debug!(%actor_id, %version, "complete change, applying right away! seqs: {seqs:?}, last_seq: {last_seq}, changes len: {len}, max db version: {max_db_version}");

    debug_assert!(len <= (seqs.end().0 - seqs.start().0 + 1) as usize);

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
        let db_version: CrsqlDbVersion = tx
            .prepare_cached("SELECT crsql_next_db_version()")?
            .query_row([], |row| row.get(0))?;
        (
            KnownDbVersion::Current(CurrentVersion {
                db_version,
                last_seq,
                ts,
            }),
            Changeset::Full {
                version,
                changes: impactful_changeset,
                seqs,
                last_seq,
                ts,
            },
        )
    };

    for (table_name, count) in changes_per_table {
        counter!("corro.changes.committed", count, "table" => table_name.to_string(), "source" => "remote");
    }

    Ok::<_, rusqlite::Error>((known_version, new_changeset))
}

fn check_buffered_meta_to_clear(
    conn: &Connection,
    actor_id: ActorId,
    versions: RangeInclusive<Version>,
) -> rusqlite::Result<bool> {
    let should_clear: bool = conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM __corro_buffered_changes WHERE site_id = ? AND version >= ? AND version <= ?)")?.query_row(params![actor_id, versions.start(), versions.end()], |row| row.get(0))?;
    if should_clear {
        return Ok(true);
    }

    conn.prepare_cached("SELECT EXISTS(SELECT 1 FROM __corro_seq_bookkeeping WHERE site_id = ? AND version >= ? AND version <= ?)")?.query_row(params![actor_id, versions.start(), versions.end()], |row| row.get(0))
}

#[tracing::instrument(skip_all, err)]
fn process_single_version(
    agent: &Agent,
    tx: &Transaction,
    last_db_version: Option<CrsqlDbVersion>,
    change: ChangeV1,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangeV1 {
        actor_id,
        changeset,
    } = change;

    let versions = changeset.versions();

    let (known, changeset) = if changeset.is_complete() {
        let (known, changeset) = process_complete_version(
            tx,
            actor_id,
            last_db_version,
            versions,
            changeset
                .into_parts()
                .expect("no changeset parts, this shouldn't be happening!"),
        )?;

        if check_buffered_meta_to_clear(tx, actor_id, changeset.versions())? {
            if let Err(e) = agent
                .tx_clear_buf()
                .try_send((actor_id, changeset.versions()))
            {
                error!("could not schedule buffered meta clear: {e}");
            }
        }

        (known, changeset)
    } else {
        let parts = changeset.into_parts().unwrap();
        let known = process_incomplete_version(tx, actor_id, &parts)?;

        (known, parts.into())
    };

    Ok((known, changeset))
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
        gauge!("corro.sync.client.head", version.0 as f64, "actor_id" => actor_id.to_string());
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
        #[allow(clippy::drain_collect)]
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
            #[allow(clippy::drain_collect)]
            if let Err(e) = process_multiple_changes(&agent, buf.drain(..).collect()).await {
                error!("could not process last multiple changes: {e}");
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
    mut rx_empty: Receiver<(ActorId, RangeInclusive<Version>)>,
    mut tripwire: Tripwire,
) {
    let mut empties: BTreeMap<ActorId, RangeInclusiveSet<Version>> = BTreeMap::new();

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

        let empties_to_process = std::mem::take(&mut empties);
        spawn_counted(
            process_completed_empties(agent.clone(), empties_to_process)
                .inspect_err(|e| error!("could not process empties: {e}")),
        );
    }
    info!("Draining empty versions to process...");
    // drain empties channel
    while let Ok((actor_id, versions)) = rx_empty.try_recv() {
        empties.entry(actor_id).or_default().insert(versions);
    }

    if !empties.is_empty() {
        info!("Inserting last unprocessed empties before shut down");
        if let Err(e) = process_completed_empties(agent, empties).await {
            error!("could not process empties: {e}");
        }
    }
}

const TO_CLEAR_COUNT: usize = 1000;

async fn clear_buffered_meta_loop(
    agent: Agent,
    mut rx_partials: Receiver<(ActorId, RangeInclusive<Version>)>,
) {
    while let Some((actor_id, versions)) = rx_partials.recv().await {
        let pool = agent.pool().clone();
        let self_actor_id = agent.actor_id();
        tokio::spawn(async move {
            loop {
                let res = {
                    let mut conn = pool.write_low().await?;

                    block_in_place(|| {
                        let tx = conn.immediate_transaction()?;

                        // TODO: delete buffered changes from deleted sequences only (maybe, it's kind of hard and may not be necessary)

                        let seq_count = tx
                            .prepare_cached("DELETE FROM __corro_seq_bookkeeping WHERE (site_id, version, start_seq) IN (SELECT site_id, version, start_seq FROM __corro_seq_bookkeeping WHERE site_id = ? AND version >= ? AND version <= ? LIMIT ?)")?
                            .execute(params![actor_id, versions.start(), versions.end(), TO_CLEAR_COUNT])?;

                        let buf_count = tx
                            .prepare_cached("DELETE FROM __corro_buffered_changes WHERE (site_id, db_version, version, seq) IN (SELECT site_id, db_version, version, seq FROM __corro_buffered_changes WHERE site_id = ? AND version >= ? AND version <= ? LIMIT ?)")?
                            .execute(params![actor_id, versions.start(), versions.end(), TO_CLEAR_COUNT])?;

                        tx.commit()?;

                        Ok::<_, rusqlite::Error>((buf_count, seq_count))
                    })
                };

                match res {
                    Ok((buf_count, seq_count)) => {
                        if buf_count + seq_count > 0 {
                            info!(%actor_id, %self_actor_id, "cleared {} buffered meta rows for versions {versions:?}", buf_count + seq_count);
                        }
                        if buf_count < TO_CLEAR_COUNT && seq_count < TO_CLEAR_COUNT {
                            break;
                        }
                    }
                    Err(e) => {
                        error!(%actor_id, "could not clear buffered meta for versions {versions:?}: {e}");
                    }
                }

                tokio::time::sleep(Duration::from_secs(2)).await;
            }

            Ok::<_, eyre::Report>(())
        });
    }
}

async fn sync_loop(
    agent: Agent,
    transport: Transport,
    mut rx_apply: Receiver<(ActorId, Version)>,
    mut tripwire: Tripwire,
) {
    let mut sync_backoff = backoff::Backoff::new(0)
        .timeout_range(Duration::from_secs(1), MAX_SYNC_BACKOFF)
        .iter();
    let next_sync_at = tokio::time::sleep(sync_backoff.next().unwrap());
    tokio::pin!(next_sync_at);

    loop {
        enum Branch {
            Tick,
            BackgroundApply { actor_id: ActorId, version: Version },
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
                debug!(%actor_id, %version, "picked up background apply of buffered changes");
                match process_fully_buffered_changes(&agent, actor_id, version).await {
                    Ok(false) => {
                        warn!(%actor_id, %version, "did not apply buffered changes");
                    }
                    Ok(true) => {
                        debug!(%actor_id, %version, "succesfully applied buffered changes");
                    }
                    Err(e) => {
                        error!(%actor_id, %version, "could not apply fully buffered changes: {e}");
                    }
                }
            }
        }
    }
}

#[tracing::instrument(skip_all, err)]
async fn process_completed_empties(
    agent: Agent,
    empties: BTreeMap<ActorId, RangeInclusiveSet<Version>>,
) -> eyre::Result<()> {
    debug!(
        "processing empty versions (count: {})",
        empties.values().map(RangeInclusiveSet::len).sum::<usize>()
    );

    let mut inserted = 0;

    let start = Instant::now();
    for (actor_id, empties) in empties {
        let v = empties.into_iter().collect::<Vec<_>>();

        for ranges in v.chunks(50) {
            let mut conn = agent.pool().write_low().await?;
            block_in_place(|| {
                let mut tx = conn.immediate_transaction()?;

                for range in ranges {
                    let mut sp = tx.savepoint()?;
                    match store_empty_changeset(&sp, actor_id, range.clone()) {
                        Ok(count) => {
                            inserted += count;
                            sp.commit()?;
                        }
                        Err(e) => {
                            error!(%actor_id, "could not store empty changeset for versions {range:?}: {e}");
                            sp.rollback()?;
                            continue;
                        }
                    }
                    if let Err(e) = agent.tx_clear_buf().try_send((actor_id, range.clone())) {
                        error!(%actor_id, "could not schedule buffered meta clear: {e}");
                    }
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

        let db_version: CrsqlDbVersion =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
        assert_eq!(db_version, CrsqlDbVersion(1));

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
        let bk: Vec<(ActorId, Version, Option<Version>, CrsqlDbVersion, Option<CrsqlSeq>)> = ta1
            .agent
            .pool()
            .read()
            .await?
            .prepare("SELECT actor_id, start_version, end_version, db_version, last_seq FROM __corro_bookkeeping")?
            .query_map((), |row| {
                Ok((
                    row.get::<_, ActorId>(0)?,
                    row.get::<_, Version>(1)?,
                    row.get::<_, Option<Version>>(2)?,
                    row.get::<_, CrsqlDbVersion>(3)?,
                    row.get::<_, Option<CrsqlSeq>>(4)?,
                ))
            })?
            .collect::<rusqlite::Result<_>>()?;

        assert_eq!(
            bk,
            vec![
                (
                    ta1.agent.actor_id(),
                    Version(1),
                    None,
                    CrsqlDbVersion(1),
                    Some(CrsqlSeq(0))
                ),
                (
                    ta1.agent.actor_id(),
                    Version(2),
                    None,
                    CrsqlDbVersion(2),
                    Some(CrsqlSeq(0))
                )
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

        let db_version: CrsqlDbVersion =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
        assert_eq!(db_version, CrsqlDbVersion(3));

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

            CREATE INDEX fooclock ON foo__crsql_clock (site_id, db_version);
            CREATE INDEX foo2clock ON foo2__crsql_clock (site_id, db_version);
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

        let db_version: CrsqlDbVersion =
            conn.query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;

        assert_eq!(db_version, CrsqlDbVersion(2));

        {
            let mut prepped = conn.prepare("SELECT * FROM __corro_bookkeeping")?;
            let mut rows = prepped.query([])?;

            println!("bookkeeping rows:");
            while let Ok(Some(row)) = rows.next() {
                println!("row: {row:?}");
            }
        }

        {
            let mut prepped = conn
                .prepare("SELECT * FROM foo2__crsql_clock UNION SELECT * FROM foo__crsql_clock;")?;
            let mut rows = prepped.query([])?;

            println!("all clock rows:");
            while let Ok(Some(row)) = rows.next() {
                println!("row: {row:?}");
            }
        }

        {
            let mut prepped = conn.prepare("EXPLAIN QUERY PLAN SELECT DISTINCT db_version FROM foo2__crsql_clock WHERE site_id IS ? UNION SELECT DISTINCT db_version FROM foo__crsql_clock WHERE site_id IS ?;")?;
            let mut rows = prepped.query([rusqlite::types::Null, rusqlite::types::Null])?;

            println!("matching clock rows:");
            while let Ok(Some(row)) = rows.next() {
                println!("row: {row:?}");
            }
        }

        let tx = conn.immediate_transaction()?;
        let actor_id: ActorId = tx.query_row("SELECT crsql_site_id()", [], |row| row.get(0))?;

        let to_clear = find_cleared_db_versions(&tx, &actor_id)?;

        println!("to_clear: {to_clear:?}");

        assert!(to_clear.contains(&CrsqlDbVersion(1)));
        assert!(!to_clear.contains(&CrsqlDbVersion(2)));

        tx.execute("DELETE FROM __corro_bookkeeping WHERE db_version = 1", [])?;
        tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) SELECT crsql_site_id(), 1, 1", [])?;

        let to_clear = find_cleared_db_versions(&tx, &actor_id)?;
        assert!(to_clear.is_empty());

        tx.execute("INSERT INTO foo2 (a) VALUES (2)", ())?;

        // invalid, but whatever
        tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version) SELECT crsql_site_id(), 3, crsql_db_version()", [])?;

        tx.commit()?;

        let tx = conn.immediate_transaction()?;
        let to_clear = find_cleared_db_versions(&tx, &actor_id)?;
        assert!(to_clear.is_empty());

        tx.execute("INSERT INTO foo (a) VALUES (1)", ())?;
        tx.commit()?;

        let tx = conn.immediate_transaction()?;
        let to_clear = find_cleared_db_versions(&tx, &actor_id)?;

        assert!(to_clear.contains(&CrsqlDbVersion(2)));
        assert!(!to_clear.contains(&CrsqlDbVersion(3)));
        assert!(!to_clear.contains(&CrsqlDbVersion(4)));

        tx.execute("DELETE FROM __corro_bookkeeping WHERE db_version = 2", [])?;
        tx.execute(
            "UPDATE __corro_bookkeeping SET end_version = 2 WHERE start_version = 1;",
            [],
        )?;
        let to_clear = find_cleared_db_versions(&tx, &actor_id)?;

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

        let counts = [
            10000, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600,
            500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900,
            800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200,
            100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500,
            400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100, 1000, 900, 800,
            700, 600, 500, 400, 300, 200, 100, 1000, 900, 800, 700, 600, 500, 400, 300, 200, 100,
        ];

        for n in counts.iter() {
            let req_body: Vec<Statement> = serde_json::from_value(json!([format!("INSERT INTO testsbool (id) WITH RECURSIVE    cte(id) AS (       SELECT random()       UNION ALL       SELECT random()         FROM cte        LIMIT {n}  ) SELECT id FROM cte;")]))?;

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
        }

        let expected_count = counts.into_iter().sum::<usize>();

        let db_version: CrsqlDbVersion =
            ta1.agent
                .pool()
                .read()
                .await?
                .query_row("SELECT crsql_db_version();", (), |row| row.get(0))?;
        assert_eq!(db_version, CrsqlDbVersion(counts.len() as u64));

        sleep(Duration::from_secs(2)).await;

        let ta2 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta1.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        sleep(Duration::from_secs(1)).await;

        let ta3 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta2.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        sleep(Duration::from_secs(1)).await;

        let ta4 = launch_test_agent(
            |conf| {
                conf.bootstrap(vec![ta3.agent.gossip_addr().to_string()])
                    .build()
            },
            tripwire.clone(),
        )
        .await?;

        sleep(Duration::from_secs(20)).await;

        for agent in [ta2.agent, ta3.agent, ta4.agent] {
            let conn = agent.pool().read().await?;

            let count: u64 = conn
                .prepare_cached("SELECT COUNT(*) FROM testsbool;")?
                .query_row((), |row| row.get(0))?;

            println!(
                "{:#?}",
                generate_sync(agent.bookie(), agent.actor_id()).await
            );

            if count as usize != expected_count {
                let buf_count: u64 =
                    conn.query_row("select count(*) from __corro_buffered_changes", [], |row| {
                        row.get(0)
                    })?;
                println!(
                    "BUFFERED COUNT: {buf_count} (actor_id: {})",
                    agent.actor_id()
                );

                let ranges = conn
                    .prepare("select start_seq, end_seq from __corro_seq_bookkeeping")?
                    .query_map([], |row| Ok(row.get::<_, u64>(0)?..=row.get::<_, u64>(1)?))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                println!("ranges: {ranges:?}");
            }

            assert_eq!(
                count as usize,
                expected_count,
                "actor {} did not reach 10K rows",
                agent.actor_id()
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

    #[test]
    fn test_store_empty_changeset() -> eyre::Result<()> {
        let mut conn = Connection::open_in_memory()?;

        corro_types::sqlite::setup_conn(&mut conn)?;
        migrate(&mut conn)?;

        let actor_id = ActorId(uuid::Uuid::new_v4());

        #[derive(Debug, Eq, PartialEq)]
        struct CorroBook {
            actor_id: ActorId,
            start_version: Version,
            end_version: Option<Version>,
        }

        conn.execute(
            "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 1)",
            [actor_id],
        )?;

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(1)..=Version(2))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        assert_eq!(rows.len(), 1);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(2))
            }
        );

        conn.execute(
            "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 3)",
            [actor_id],
        )?;

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(5)..=Version(7))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        assert_eq!(rows.len(), 3);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(2))
            }
        );
        assert_eq!(
            rows[1],
            CorroBook {
                actor_id,
                start_version: Version(3),
                end_version: None
            }
        );
        assert_eq!(
            rows[2],
            CorroBook {
                actor_id,
                start_version: Version(5),
                end_version: Some(Version(7))
            }
        );

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(3)..=Version(6))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        println!("rows: {rows:?}");

        assert_eq!(rows.len(), 1);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(7))
            }
        );

        conn.execute(
            "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 12)",
            [actor_id],
        )?;

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(1)..=Version(10))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        println!("rows: {rows:?}");

        assert_eq!(rows.len(), 2);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(10))
            }
        );

        assert_eq!(
            rows[1],
            CorroBook {
                actor_id,
                start_version: Version(12),
                end_version: None
            }
        );

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(1)..=Version(11))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        println!("rows: {rows:?}");

        assert_eq!(rows.len(), 2);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(11))
            }
        );

        assert_eq!(
            rows[1],
            CorroBook {
                actor_id,
                start_version: Version(12),
                end_version: None
            }
        );

        conn.execute(
            "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 13)",
            [actor_id],
        )?;

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(14)..=Version(14))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        println!("rows: {rows:?}");

        assert_eq!(rows.len(), 4);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(11))
            }
        );

        assert_eq!(
            rows[1],
            CorroBook {
                actor_id,
                start_version: Version(12),
                end_version: None
            }
        );
        assert_eq!(
            rows[2],
            CorroBook {
                actor_id,
                start_version: Version(13),
                end_version: None
            }
        );

        assert_eq!(
            rows[3],
            CorroBook {
                actor_id,
                start_version: Version(14),
                end_version: Some(Version(14))
            }
        );

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(12)..=Version(14))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        println!("rows: {rows:?}");

        assert_eq!(rows.len(), 1);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(14))
            }
        );

        conn.execute(
            "INSERT INTO __corro_bookkeeping (actor_id, start_version) VALUES (?, 15)",
            [actor_id],
        )?;
        conn.execute(
            "INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) VALUES (?, 16, 18)",
            [actor_id],
        )?;

        {
            let tx = conn.transaction()?;
            assert_eq!(
                store_empty_changeset(&tx, actor_id, Version(15)..=Version(15))?,
                1
            );
            tx.commit()?;
        }

        let rows = conn
            .prepare("SELECT actor_id, start_version, end_version FROM __corro_bookkeeping")?
            .query_map([], |row| {
                Ok(CorroBook {
                    actor_id: row.get(0)?,
                    start_version: row.get(1)?,
                    end_version: row.get(2)?,
                })
            })
            .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?;

        println!("rows: {rows:?}");

        assert_eq!(rows.len(), 1);

        assert_eq!(
            rows[0],
            CorroBook {
                actor_id,
                start_version: Version(1),
                end_version: Some(Version(18))
            }
        );

        Ok(())
    }
}
