//! Setup main agent state

// External crates
use arc_swap::ArcSwap;
use parking_lot::RwLock;
use rusqlite::{Connection, OptionalExtension};
use std::{net::SocketAddr, ops::RangeInclusive, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel as tokio_channel, Receiver as TokioReceiver},
        Semaphore,
    },
};
use tracing::{debug, info};
use tripwire::Tripwire;

// Internals
use crate::{api::peer::gossip_server_endpoint, transport::Transport};
use corro_types::{
    actor::ActorId,
    agent::{migrate, Agent, AgentConfig, Booked, BookedVersions, LockRegistry, SplitPool},
    base::Version,
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput},
    channel::{bounded, CorroReceiver},
    config::Config,
    members::Members,
    pubsub::SubsManager,
    schema::init_schema,
    sqlite::CrConn,
};

/// Runtime state for the Corrosion agent
pub struct AgentOptions {
    pub lock_registry: LockRegistry,
    pub gossip_server_endpoint: quinn::Endpoint,
    pub transport: Transport,
    pub api_listener: TcpListener,
    pub rx_bcast: CorroReceiver<BroadcastInput>,
    pub rx_apply: CorroReceiver<(ActorId, Version)>,
    pub rx_empty: CorroReceiver<(ActorId, RangeInclusive<Version>)>,
    pub rx_clear_buf: CorroReceiver<(ActorId, RangeInclusive<Version>)>,
    pub rx_changes: CorroReceiver<(ChangeV1, ChangeSource)>,
    pub rx_foca: CorroReceiver<FocaInput>,
    pub rtt_rx: TokioReceiver<(SocketAddr, Duration)>,
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

    let (tx_apply, rx_apply) = bounded(2048, "apply");
    let (tx_clear_buf, rx_clear_buf) = bounded(512, "clear_buf");

    let lock_registry = LockRegistry::default();
    let booked = {
        let conn = pool.read().await?;
        Booked::new(
            BookedVersions::from_conn(&conn, actor_id)?,
            lock_registry.clone(),
        )
    };

    let gossip_server_endpoint = gossip_server_endpoint(&conf.gossip).await?;
    let gossip_addr = gossip_server_endpoint.local_addr()?;

    let external_addr = conf.gossip.external_addr;

    // RTT handling interacts with the tokio ReceiverStream and as
    // such needs a raw tokio channel
    let (rtt_tx, rtt_rx) = tokio_channel(128);

    let transport = Transport::new(&conf.gossip, rtt_tx).await?;

    let api_listener = TcpListener::bind(conf.api.bind_addr).await?;
    let api_addr = api_listener.local_addr()?;

    let clock = Arc::new(
        uhlc::HLCBuilder::default()
            .with_id(actor_id.try_into().unwrap())
            .with_max_delta(Duration::from_millis(300))
            .build(),
    );

    let (tx_bcast, rx_bcast) = bounded(512, "bcast");
    let (tx_empty, rx_empty) = bounded(512, "empty");
    let (tx_changes, rx_changes) = bounded(1024, "changes");
    let (tx_foca, rx_foca) = bounded(256, "foca");

    let subs_manager = SubsManager::default();

    let opts = AgentOptions {
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
        cluster_id,
        subs_manager,
        tripwire,
    });

    Ok((agent, opts))
}
