//! Setup main agent state

// External crates
use arc_swap::ArcSwap;
use futures::lock;
use parking_lot::RwLock;
use rangemap::RangeInclusiveSet;
use rusqlite::{params, Connection};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    ops::RangeInclusive,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel, Receiver},
        Semaphore,
    },
};
use tracing::{debug, info, warn};
use tripwire::Tripwire;

// Internals
use crate::{api::peer::gossip_server_endpoint, transport::Transport};
use corro_types::{
    actor::ActorId,
    agent::{
        migrate, Agent, AgentConfig, Booked, BookedVersions, Bookie, CurrentVersion,
        KnownDbVersion, LockRegistry, PartialVersion, SplitPool,
    },
    base::{CrsqlSeq, Version},
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput, Timestamp},
    config::Config,
    members::Members,
    pubsub::SubsManager,
    schema::init_schema,
    sqlite::CrConn,
};

/// Runtime state for the Corrosion agent
pub struct AgentOptions {
    pub actor_id: ActorId,
    pub lock_registry: LockRegistry,
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

/// Setup an agent runtime and state with a configuration
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

        schema
    };

    let (tx_apply, rx_apply) = channel(20480);
    let (tx_clear_buf, rx_clear_buf) = channel(10240);

    let lock_registry = LockRegistry::default();
    let booked = {
        let conn = pool.read().await?;
        Booked::new(
            BookedVersions::from_conn(&conn, actor_id)?,
            lock_registry.clone(),
        )
    };

    // let mut bk: HashMap<ActorId, BookedVersions> = HashMap::new();

    // {
    //     debug!("getting read-only conn for pull bookkeeping rows");
    //     let mut conn = pool.write_priority().await?;

    //     debug!("getting bookkept rows");

    //     let mut cleared_rows: BTreeMap<ActorId, usize> = BTreeMap::new();

    //     {
    //         let mut prepped = conn.prepare_cached(
    //             "SELECT actor_id, start_version, end_version, db_version, last_seq, ts
    //             FROM __corro_bookkeeping",
    //         )?;
    //         let mut rows = prepped.query([])?;

    //         loop {
    //             let row = rows.next()?;
    //             match row {
    //                 None => break,
    //                 Some(row) => {
    //                     let actor_id = row.get(0)?;
    //                     let ranges = bk.entry(actor_id).or_default();
    //                     let start_v = row.get(1)?;
    //                     let end_v: Option<Version> = row.get(2)?;
    //                     ranges.insert_many(
    //                         start_v..=end_v.unwrap_or(start_v),
    //                         match row.get(3)? {
    //                             Some(db_version) => KnownDbVersion::Current(CurrentVersion {
    //                                 db_version,
    //                                 last_seq: row.get(4)?,
    //                                 ts: row.get(5)?,
    //                             }),
    //                             None => {
    //                                 *cleared_rows.entry(actor_id).or_default() += 1;
    //                                 KnownDbVersion::Cleared
    //                             }
    //                         },
    //                     );
    //                 }
    //             }
    //         }

    //         let mut partials: HashMap<
    //             (ActorId, Version),
    //             (RangeInclusiveSet<CrsqlSeq>, CrsqlSeq, Timestamp),
    //         > = HashMap::new();

    //         debug!("getting seq bookkept rows");

    //         let mut prepped = conn.prepare_cached(
    //             "SELECT site_id, version, start_seq, end_seq, last_seq, ts
    //             FROM __corro_seq_bookkeeping",
    //         )?;
    //         let mut rows = prepped.query([])?;

    //         loop {
    //             let row = rows.next()?;
    //             match row {
    //                 None => break,
    //                 Some(row) => {
    //                     let (range, last_seq, ts) =
    //                         partials.entry((row.get(0)?, row.get(1)?)).or_default();

    //                     range.insert(row.get(2)?..=row.get(3)?);
    //                     *last_seq = row.get(4)?;
    //                     *ts = row.get(5)?;
    //                 }
    //             }
    //         }

    //         debug!("filling up partial known versions");

    //         for ((actor_id, version), (seqs, last_seq, ts)) in partials {
    //             debug!(%actor_id, %version, "looking at partials (seq: {seqs:?}, last_seq: {last_seq})");
    //             let ranges = bk.entry(actor_id).or_default();

    //             if let Some(known) = ranges.get(&version) {
    //                 warn!(%actor_id, %version, "found partial data that has been applied, clearing buffered meta, known: {known:?}");

    //                 _ = tx_clear_buf.try_send((actor_id, version..=version));
    //                 continue;
    //             }

    //             let gaps_count = seqs.gaps(&(CrsqlSeq(0)..=last_seq)).count();

    //             ranges.insert(
    //                 version,
    //                 KnownDbVersion::Partial(PartialVersion { seqs, last_seq, ts }),
    //             );

    //             if gaps_count == 0 {
    //                 info!(%actor_id, %version, "found fully buffered, unapplied, changes! scheduling apply");
    //                 // spawn this because it can block if the channel gets full, nothing is draining it yet!
    //                 tokio::spawn({
    //                     let tx_apply = tx_apply.clone();
    //                     async move {
    //                         let _ = tx_apply.send((actor_id, version)).await;
    //                     }
    //                 });
    //             }
    //         }
    //     }

    //     for (actor_id, booked) in bk.iter() {
    //         if let Some(clear_count) = cleared_rows.get(actor_id).copied() {
    //             if clear_count > booked.cleared.len() {
    //                 warn!(%actor_id, "cleared bookkept rows count ({clear_count}) in DB was bigger than in-memory entries count ({}), compacting...", booked.cleared.len());
    //                 let tx = conn.immediate_transaction()?;
    //                 let deleted = tx.execute("DELETE FROM __corro_bookkeeping WHERE actor_id = ? AND end_version IS NOT NULL", [actor_id])?;
    //                 info!("deleted {deleted} rows that had an end_version");
    //                 let mut inserted = 0;
    //                 for range in booked.cleared.iter() {
    //                     inserted += tx.execute("INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version) VALUES (?, ?, ?)", params![actor_id, range.start(), range.end()])?;
    //                 }
    //                 info!("inserted {inserted} cleared version rows");
    //                 tx.commit()?;
    //             }
    //         }
    //     }
    // }

    // debug!("done building bookkeeping");

    // let bookie = Bookie::new(bk);

    let gossip_server_endpoint = gossip_server_endpoint(&conf.gossip).await?;
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    let external_addr = conf.gossip.external_addr;

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
        lock_registry,
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
        external_addr,
        api_addr,
        members: RwLock::new(members),
        config: ArcSwap::from_pointee(conf),
        clock,
        booked,
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
