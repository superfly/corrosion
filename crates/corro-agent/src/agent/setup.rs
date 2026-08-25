//! Setup main agent state

// External crates
use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use parking_lot::RwLock;
use rusqlite::{Connection, OptionalExtension};
use std::{
    io::{self, Read, Seek, SeekFrom},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel as tokio_channel, Receiver as TokioReceiver},
        RwLock as TokioRwLock, Semaphore,
    },
    time::Instant,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tripwire::Tripwire;
use zstd;

// Internals
use crate::{
    api::{
        peer::gossip_server_endpoint,
        public::{
            pubsub::{process_sub_channel, MatcherBroadcastCache, SharedMatcherBroadcastCache},
            update::SharedUpdateBroadcastCache,
        },
    },
    transport::Transport,
};
use corro_types::{
    actor::ActorId,
    agent::{migrate, Agent, AgentConfig, BookedVersions, Bookie, SplitPool},
    base::{CrsqlDbVersion, CrsqlDbVersionRange},
    broadcast::{BroadcastInput, BroadcastV1, ChangeSource, ChangeV1, FocaInput, Timestamp},
    channel::{bounded, CorroReceiver},
    compress::ZstdDicts,
    config::{CompressionConfig, Config, CrsqliteConfig},
    members::Members,
    metrics_tracker::MetricsTracker,
    pubsub::{Matcher, SubsManager},
    schema::{init_schema, Schema},
    sqlite::CrConn,
    updates::UpdatesManager,
};

/// Runtime state for the Corrosion agent
pub struct AgentOptions {
    pub gossip_server_endpoint: quinn::Endpoint,
    pub transport: Transport,
    pub api_listeners: Vec<TcpListener>,
    pub rx_bcast: CorroReceiver<BroadcastInput>,
    pub rx_apply: CorroReceiver<(ActorId, CrsqlDbVersion)>,
    pub rx_clear_buf: CorroReceiver<(ActorId, CrsqlDbVersionRange)>,
    pub rx_changes: CorroReceiver<(ChangeV1, ChangeSource, Option<BroadcastV1>)>,
    pub rx_foca: CorroReceiver<FocaInput>,
    pub rtt_rx: TokioReceiver<(SocketAddr, Duration)>,
    pub subs_manager: SubsManager,
    pub subs_bcast_cache: SharedMatcherBroadcastCache,
    pub updates_bcast_cache: SharedUpdateBroadcastCache,
    pub tripwire: Tripwire,
}

/// Setup an agent runtime and state with a configuration
pub async fn setup(conf: Config, tripwire: Tripwire) -> eyre::Result<(Agent, AgentOptions)> {
    debug!("setting up corrosion @ {}", conf.db.path);

    if let Some(parent) = conf.db.path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    // do this early to error earlier
    let members = Members::new(conf.gossip.member_id);

    let actor_id = {
        // we need to set auto_vacuum before any tables are created
        let db_conn = Connection::open(&conf.db.path)?;
        db_conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL")?;

        let conn = CrConn::init(db_conn)?;
        conn.query_row("SELECT crsql_site_id();", [], |row| {
            row.get::<_, ActorId>(0)
        })
    }?;

    info!("Actor ID: {actor_id}");

    let write_sema = Arc::new(Semaphore::new(1));

    let pool = SplitPool::create(&conf.db.path, write_sema.clone(), conf.db.cache_size_kib).await?;

    let clock = Arc::new(
        uhlc::HLCBuilder::default()
            .with_id(actor_id.try_into().unwrap())
            .with_max_delta(Duration::from_millis(300))
            .build(),
    );

    let schema = {
        let mut conn = pool.write_priority().await?;
        migrate(clock.clone(), &mut conn)?;

        // Apply crsqlite metadata version config transitions if configured.
        // This must happen after schema migration but before the agent starts
        // processing changes, so the metadata version is consistent.
        if let Some(crsqlite_conf) = &conf.crsqlite {
            apply_crsqlite_config(&conn, crsqlite_conf, &clock)?;
        }

        let mut schema = init_schema(&conn)?;
        schema.constrain()?;

        schema
    };

    let subs_manager = SubsManager::default();

    let updates_manager = UpdatesManager::default();
    // Setup subscription handlers, this is before we start processing changes.
    let subs_bcast_cache = setup_spawn_subscriptions(
        &subs_manager,
        conf.db.subscriptions_path(),
        &pool,
        &schema,
        &tripwire,
    )
    .await?;

    let updates_bcast_cache = SharedUpdateBroadcastCache::default();

    let cluster_id = {
        let conn = pool.read().await?;
        conn.query_row(
            "SELECT value FROM __corro_state WHERE key = 'cluster_id'",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or_default()
    };

    info!("Cluster ID: {cluster_id}");

    let (tx_apply, rx_apply) = bounded(conf.perf.apply_channel_len, "apply");
    let (tx_clear_buf, rx_clear_buf) = bounded(conf.perf.clearbuf_channel_len, "clear_buf");

    let gossip_server_endpoint = gossip_server_endpoint(&conf.gossip).await?;
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    let external_addr = conf.gossip.external_addr;

    // RTT handling interacts with the tokio ReceiverStream and as
    // such needs a raw tokio channel
    let (rtt_tx, rtt_rx) = tokio_channel(128);

    let transport = Transport::new(&conf.gossip, rtt_tx).await?;

    let mut api_listeners = Vec::with_capacity(conf.api.bind_addr.len());
    for addr in conf.api.bind_addr.iter() {
        api_listeners.push(TcpListener::bind(addr).await?);
    }
    let api_addr = api_listeners.first().unwrap().local_addr()?;

    let (tx_bcast, rx_bcast) = bounded(conf.perf.bcast_channel_len, "bcast");
    let (tx_changes, rx_changes) = bounded(conf.perf.changes_channel_len, "changes");
    let (tx_foca, rx_foca) = bounded(conf.perf.foca_channel_len, "foca");

    // Load all actors' bookie state synchronously.
    let start = Instant::now();
    let all_booked = {
        let conn = pool.read().await?;
        BookedVersions::load_all_from_conn(&conn)?
    };
    info!("Loaded booked versions in {:?}", start.elapsed());

    let bookie = Bookie::new(all_booked);
    let booked = bookie.ensure(actor_id);

    let metrics_tracker = MetricsTracker::new(Duration::from_secs(120), 5)?;

    let change_dict = load_change_dicts(&conf.gossip.compression_config())?;

    let opts = AgentOptions {
        gossip_server_endpoint,
        transport: transport.clone(),
        api_listeners,
        rx_bcast,
        rx_apply,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        rtt_rx,
        subs_manager: subs_manager.clone(),
        subs_bcast_cache,
        updates_bcast_cache,
        tripwire: tripwire.clone(),
    };

    let agent = Agent::new(AgentConfig {
        actor_id,
        pool: pool.clone(),
        gossip_addr,
        external_addr,
        api_addr,
        members: RwLock::new(members),
        config: ArcSwap::from_pointee(conf),
        clock,
        booked,
        bookie,
        tx_bcast,
        tx_apply,
        tx_clear_buf,
        tx_changes,
        tx_foca,
        change_dict,
        write_sema,
        schema: RwLock::new(schema),
        cluster_id,
        subs_manager,
        updates_manager,
        metrics_tracker,
        tripwire,
        fatal_issue: Default::default(),
        shutdown_token: CancellationToken::new(),
    });

    Ok((agent, opts))
}

/// Load trained zstd dictionaries from `gossip.compression.dict_dir`.
///
/// Every valid dictionary file in the directory is registered for decoding.
/// When `dict_file` is set, that file is also used as the encoder dictionary.
pub fn load_change_dicts(
    compression_config: &CompressionConfig,
) -> eyre::Result<Option<Arc<ZstdDicts>>> {
    let Some(dir) = &compression_config.dict_dir else {
        return Ok(None);
    };

    let encode_name = compression_config.dict_file.as_deref();
    let mut encoder_bytes: Option<Vec<u8>> = None;
    let mut decoder_dicts = Vec::new();

    let entries = std::fs::read_dir(dir)
        .map_err(|e| eyre::eyre!("could not read compression dict dir {dir}: {e}"))?;
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let entry_path = entry.path();
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();

        let mut file = match std::fs::File::open(&entry_path) {
            Ok(file) => file,
            Err(e) => {
                warn!("could not open candidate compression dict {entry_path:?}: {e}");
                continue;
            }
        };

        match load_dictionary(&mut file) {
            Ok(Some(b)) => {
                if encode_name == Some(file_name.as_ref()) {
                    info!(
                        path = %entry_path.display(),
                        dict_id = ?zstd::zstd_safe::get_dict_id_from_dict(&b),
                        "using compression dictionary for encoding and decoding"
                    );
                    encoder_bytes = Some(b.clone());
                } else {
                    info!(
                        path = %entry_path.display(),
                        dict_id = ?zstd::zstd_safe::get_dict_id_from_dict(&b),
                        "using compression dictionary for decoding"
                    );
                }
                decoder_dicts.push(b);
            }
            _ => {
                warn!("could not read candidate compression dict {entry_path:?}");
            }
        }
    }

    if let Some(name) = encode_name {
        if encoder_bytes.is_none() {
            eyre::bail!(
                "gossip.compression.dict_file `{name}` not found in {dir} \
                 or is not a valid zstd dictionary"
            );
        }
    }

    Ok(Some(Arc::new(ZstdDicts::new(
        encoder_bytes.as_deref(),
        compression_config.level,
        decoder_dicts,
    ))))
}

/// Rescan `gossip.compression.dict_dir` and swap the agent's dictionaries.
pub fn reload_change_dicts(agent: &Agent) -> eyre::Result<usize> {
    let dicts = load_change_dicts(&agent.config().gossip.compression_config())?;
    let decoder_count = dicts.as_ref().map(|d| d.decoder_count()).unwrap_or(0);
    agent.set_change_dict(dicts);
    Ok(decoder_count)
}

/// Initialise subscription state and tasks
///
/// 1. Get subscriptions state directory from config
/// 2. Load existing subscriptions and restore them in SubsManager
/// 3. Spawn subscription processor task
async fn setup_spawn_subscriptions(
    subs_manager: &SubsManager,
    subs_path: Utf8PathBuf,
    pool: &SplitPool,
    schema: &Schema,
    tripwire: &Tripwire,
) -> eyre::Result<SharedMatcherBroadcastCache> {
    let mut subs_bcast_cache = MatcherBroadcastCache::default();
    let mut to_cleanup = vec![];

    if let Ok(mut dir) = tokio::fs::read_dir(&subs_path).await {
        while let Ok(Some(entry)) = dir.next_entry().await {
            let path_str = entry.path().display().to_string();
            if let Some(sub_id_str) = path_str.strip_prefix(subs_path.as_str()) {
                if let Ok(sub_id) = sub_id_str.trim_matches('/').parse() {
                    let (_, created) = match subs_manager.restore(
                        sub_id,
                        &subs_path,
                        schema,
                        pool,
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

    Ok(Arc::new(TokioRwLock::new(subs_bcast_cache)))
}

/// Apply crsqlite metadata version config transitions on startup.
///
/// For each configured version setting, if the current DB value differs,
/// call `crsql_config_set` to transition. Crsqlite validates transitions
/// and will error on invalid jumps (e.g. 1→3 when CRR tables exist).
///
/// The transitions are applied in order: write-version, use-version, sync-log-version,
/// since there are dependencies between them (e.g. sync-log v2 requires use-version v2).
fn apply_crsqlite_config(
    conn: &CrConn,
    config: &CrsqliteConfig,
    clock: &Arc<uhlc::HLC>,
) -> eyre::Result<()> {
    let ts = Timestamp::from(clock.new_timestamp());

    // V2 operations require a non-zero ts
    conn.execute_batch(&format!("SELECT crsql_set_ts({})", ts.as_u64()))?;

    // Helper: read current value, transition if different
    fn try_transition(
        conn: &CrConn,
        config_name: &str,
        desired: i64,
    ) -> eyre::Result<()> {
        let current: i64 = conn.query_row(
            &format!("SELECT crsql_config_get('{}')", config_name),
            [],
            |row| row.get(0),
        )?;

        if current == desired {
            debug!(config_name, current, "crsqlite config already at target");
            return Ok(());
        }

        info!(
            config_name,
            current,
            desired,
            "transitioning crsqlite metadata version"
        );

        let sql = format!("SELECT crsql_config_set('{}', {})", config_name, desired);
        match conn.query_row::<String, _, _>(&sql, [], |row| row.get(0)) {
            Ok(_) => {
                info!(config_name, desired, "crsqlite config transition complete");
                Ok(())
            }
            Err(e) => {
                Err(eyre::eyre!(
                    "failed to transition crsql_config_set('{}', {}): {e}",
                    config_name,
                    desired
                ))
            }
        }
    }

    // Apply in dependency order
    if let Some(wv) = config.metadata_write_version {
        try_transition(conn, "metadata-write-version", wv)?;
    }
    if let Some(uv) = config.metadata_use_version {
        try_transition(conn, "metadata-use-version", uv)?;
    }
    if let Some(slv) = config.sync_log_version {
        try_transition(conn, "sync-log-version", slv)?;
    }

    Ok(())
}

fn load_dictionary(file: &mut std::fs::File) -> io::Result<Option<Vec<u8>>> {
    let mut prefix = [0u8; 4];
    let peeked = file.read(&mut prefix)?;
    let is_dict = peeked == 4 && u32::from_le_bytes(prefix) == zstd::zstd_safe::MAGIC_DICTIONARY;
    if !is_dict {
        return Ok(None);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok(Some(bytes))
}
