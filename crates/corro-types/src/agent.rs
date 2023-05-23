use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use parking_lot::{RwLock, RwLockReadGuard};
use rangemap::RangeInclusiveMap;
use tokio::{sync::mpsc::Sender, task::block_in_place};
use tracing::warn;

use crate::{
    actor::ActorId,
    broadcast::{BroadcastInput, Timestamp},
    config::Config,
    pubsub::Subscribers,
    schema::{apply_schema, NormalizedSchema, SchemaError},
    sqlite::SqlitePool,
};

use super::members::Members;

#[derive(Clone)]
pub struct Agent(pub Arc<AgentInner>);

pub struct AgentInner {
    pub actor_id: ActorId,
    pub ro_pool: SqlitePool,
    pub rw_pool: SqlitePool,
    pub config: ArcSwap<Config>,
    pub gossip_addr: SocketAddr,
    pub api_addr: Option<SocketAddr>,
    pub members: RwLock<Members>,
    pub clock: Arc<uhlc::HLC>,
    pub bookie: Bookie,
    pub subscribers: Subscribers,
    pub tx_bcast: Sender<BroadcastInput>,
    pub schema: RwLock<NormalizedSchema>,
}

impl Agent {
    /// Return a borrowed [SqlitePool]
    pub fn read_only_pool(&self) -> &SqlitePool {
        &self.0.ro_pool
    }

    pub fn read_write_pool(&self) -> &SqlitePool {
        &self.0.rw_pool
    }

    pub fn actor_id(&self) -> ActorId {
        self.0.actor_id
    }

    pub fn clock(&self) -> &Arc<uhlc::HLC> {
        &self.0.clock
    }

    pub fn gossip_addr(&self) -> SocketAddr {
        self.0.gossip_addr
    }
    pub fn api_addr(&self) -> Option<SocketAddr> {
        self.0.api_addr
    }
    pub fn subscribers(&self) -> &Subscribers {
        &self.0.subscribers
    }

    pub fn tx_bcast(&self) -> &Sender<BroadcastInput> {
        &self.0.tx_bcast
    }

    pub fn bookie(&self) -> &Bookie {
        &self.0.bookie
    }

    pub fn base_path(&self) -> Utf8PathBuf {
        self.0.config.load().base_path.clone()
    }

    pub fn config(&self) -> arc_swap::Guard<Arc<Config>, arc_swap::strategy::DefaultStrategy> {
        self.0.config.load()
    }

    pub fn set_config(&self, new_conf: Config) {
        self.0.config.store(Arc::new(new_conf))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Pool(#[from] bb8::RunError<bb8_rusqlite::Error>),
}

pub async fn reload(agent: &Agent, new_conf: Config) -> Result<(), ReloadError> {
    let old_conf = agent.config();

    if old_conf.base_path != new_conf.base_path {
        warn!("reloaded ineffectual change: base_path");
    }
    if old_conf.gossip_addr != new_conf.gossip_addr {
        warn!("reloaded ineffectual change: gossip_addr");
    }
    if old_conf.api_addr != new_conf.api_addr {
        warn!("reloaded ineffectual change: api_addr");
    }
    if old_conf.metrics_addr != new_conf.metrics_addr {
        warn!("reloaded ineffectual change: metrics_addr");
    }
    if old_conf.bootstrap != new_conf.bootstrap {
        warn!("reloaded ineffectual change: bootstrap");
    }
    if old_conf.log_format != new_conf.log_format {
        warn!("reloaded ineffectual change: log_format");
    }

    let mut conn = agent.read_write_pool().get().await?;
    let new_schema =
        block_in_place(|| apply_schema(&mut conn, &new_conf.schema_paths, &agent.0.schema.read()))?;

    *agent.0.schema.write() = new_schema;
    agent.set_config(new_conf);

    Ok(())
}

pub type BookedVersion = RangeInclusiveMap<i64, (Option<i64>, Timestamp)>;
pub type BookedInner = Arc<RwLock<BookedVersion>>;

#[derive(Default, Clone)]
pub struct Booked(BookedInner);

impl Booked {
    pub fn new(inner: BookedInner) -> Self {
        Self(inner)
    }

    pub fn insert(&self, version: i64, db_version: Option<i64>, ts: Timestamp) {
        self.0.write().insert(version..=version, (db_version, ts));
    }

    pub fn contains(&self, version: i64) -> bool {
        self.0.read().contains_key(&version)
    }

    pub fn last(&self) -> Option<i64> {
        self.0.read().iter().map(|(k, _v)| *k.end()).max()
    }

    pub fn read(&self) -> RwLockReadGuard<BookedVersion> {
        self.0.read()
    }
}

pub type BookieInner = Arc<RwLock<HashMap<ActorId, Booked>>>;

#[derive(Default, Clone)]
pub struct Bookie(BookieInner);

impl Bookie {
    pub fn new(inner: BookieInner) -> Self {
        Self(inner)
    }

    pub fn add(&self, actor_id: ActorId, version: i64, db_version: Option<i64>, ts: Timestamp) {
        {
            if let Some(booked) = self.0.read().get(&actor_id) {
                booked.insert(version, db_version, ts);
                return;
            }
        };

        let mut w = self.0.write();
        let booked = w.entry(actor_id).or_default();
        booked.insert(version, db_version, ts);
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
