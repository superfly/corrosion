//! Setup main agent state

// External crates
use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use parking_lot::RwLock;
use rusqlite::{Connection, OptionalExtension};
use std::{
    net::SocketAddr,
    ops::{DerefMut, RangeInclusive},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel as tokio_channel, Receiver as TokioReceiver},
        RwLock as TokioRwLock, Semaphore,
    },
};
use tracing::{debug, error, info};
use tripwire::Tripwire;

// Internals
use crate::{
    api::{
        peer::gossip_server_endpoint,
        public::pubsub::{process_sub_channel, MatcherBroadcastCache, SharedMatcherBroadcastCache},
    },
    transport::Transport,
};
use corro_types::{
    actor::ActorId,
    agent::{migrate, Agent, AgentConfig, Booked, BookedVersions, LockRegistry, SplitPool},
    base::Version,
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput},
    channel::{bounded, CorroReceiver},
    config::Config,
    members::Members,
    pubsub::{Matcher, SubsManager},
    schema::{init_schema, Schema},
    sqlite::CrConn,
};

/// Runtime state for the Corrosion agent
pub struct AgentOptions {
    pub lock_registry: LockRegistry,
    pub gossip_server_endpoint: quinn::Endpoint,
    pub transport: Transport,
    pub api_listeners: Vec<TcpListener>,
    pub rx_bcast: CorroReceiver<BroadcastInput>,
    pub rx_apply: CorroReceiver<(ActorId, Version)>,
    pub rx_clear_buf: CorroReceiver<(ActorId, RangeInclusive<Version>)>,
    pub rx_changes: CorroReceiver<(ChangeV1, ChangeSource)>,
    pub rx_foca: CorroReceiver<FocaInput>,
    pub rtt_rx: TokioReceiver<(SocketAddr, Duration)>,
    pub subs_manager: SubsManager,
    pub subs_bcast_cache: SharedMatcherBroadcastCache,
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
        })
    }?;

    info!("Actor ID: {actor_id}");

    let write_sema = Arc::new(Semaphore::new(1));

    let pool = SplitPool::create(&conf.db.path, write_sema.clone()).await?;

    let schema = {
        let mut conn = pool.write_priority().await?;
        migrate(&mut conn)?;
        let mut schema = init_schema(&conn)?;
        schema.constrain()?;

        schema
    };

    let subs_manager = SubsManager::default();

    // Setup subscription handlers
    let subs_bcast_cache = setup_spawn_subscriptions(
        &subs_manager,
        conf.db.subscriptions_path(),
        &pool,
        &schema,
        &tripwire,
    )
    .await?;

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

    let clock = Arc::new(
        uhlc::HLCBuilder::default()
            .with_id(actor_id.try_into().unwrap())
            .with_max_delta(Duration::from_millis(300))
            .build(),
    );

    let (tx_bcast, rx_bcast) = bounded(conf.perf.bcast_channel_len, "bcast");
    let (tx_changes, rx_changes) = bounded(conf.perf.changes_channel_len, "changes");
    let (tx_foca, rx_foca) = bounded(conf.perf.foca_channel_len, "foca");

    let lock_registry = LockRegistry::default();

    // make an empty booked!
    let booked = Booked::new(BookedVersions::new(actor_id), lock_registry.clone());

    // asynchronously load it up!
    tokio::spawn({
        let pool = pool.clone();
        // acquiring the lock here means everything will have to wait for it to be ready
        let mut booked = booked.write_owned("init").await;
        async move {
            let conn = pool.read().await?;
            *booked.deref_mut().deref_mut() =
                tokio::task::block_in_place(|| BookedVersions::from_conn(&conn, actor_id))?;
            Ok::<_, eyre::Report>(())
        }
    });

    let opts = AgentOptions {
        gossip_server_endpoint,
        transport,
        api_listeners,
        lock_registry,
        rx_bcast,
        rx_apply,
        rx_clear_buf,
        rx_changes,
        rx_foca,
        rtt_rx,
        subs_manager: subs_manager.clone(),
        subs_bcast_cache,
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
        tx_clear_buf,
        tx_changes,
        tx_foca,
        write_sema,
        schema: RwLock::new(schema),
        cluster_id,
        subs_manager,
        tripwire,
    });

    Ok((agent, opts))
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
