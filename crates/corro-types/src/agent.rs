use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use parking_lot::{RwLock, RwLockReadGuard};
use rangemap::RangeInclusiveMap;
use tokio::sync::mpsc::Sender;

use crate::{
    actor::ActorId,
    broadcast::BroadcastInput,
    config::Config,
    pubsub::Subscribers,
    sqlite::{NormalizedSchema, SqlitePool},
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

pub type BookedVersion = RangeInclusiveMap<i64, (Option<i64>, u64)>;
pub type BookedInner = Arc<RwLock<BookedVersion>>;

#[derive(Default, Clone)]
pub struct Booked(BookedInner);

impl Booked {
    pub fn new(inner: BookedInner) -> Self {
        Self(inner)
    }

    pub fn insert(&self, version: i64, db_version: Option<i64>, ts: u64) {
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

    pub fn add(&self, actor_id: ActorId, version: i64, db_version: Option<i64>, ts: u64) {
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
