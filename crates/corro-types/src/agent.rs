use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    ops::{Deref, DerefMut, RangeInclusive},
    sync::Arc,
};

use arc_swap::ArcSwap;
use bb8::PooledConnection;
use camino::Utf8PathBuf;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use spawn::spawn_counted;
use tokio::{
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
    task::block_in_place,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{error, warn};
use tripwire::Tripwire;

use crate::{
    actor::ActorId,
    broadcast::{BroadcastInput, Timestamp},
    config::Config,
    pubsub::Subscribers,
    schema::{apply_schema, NormalizedSchema, SchemaError},
    sqlite::{CrConnManager, SqlitePool},
};

use super::members::Members;

#[derive(Clone)]
pub struct Agent(Arc<AgentInner>);

pub struct AgentConfig {
    pub actor_id: ActorId,
    pub ro_pool: SqlitePool,
    pub rw_pool: SqlitePool,
    pub config: ArcSwap<Config>,
    pub gossip_addr: SocketAddr,
    pub api_addr: SocketAddr,
    pub members: RwLock<Members>,
    pub clock: Arc<uhlc::HLC>,
    pub bookie: Bookie,
    pub subscribers: Subscribers,
    pub tx_bcast: Sender<BroadcastInput>,
    pub tx_apply: Sender<(ActorId, i64)>,

    pub schema: RwLock<NormalizedSchema>,
    pub tripwire: Tripwire,
}

pub struct AgentInner {
    actor_id: ActorId,
    pool: SplitPool,
    config: ArcSwap<Config>,
    gossip_addr: SocketAddr,
    api_addr: SocketAddr,
    members: RwLock<Members>,
    clock: Arc<uhlc::HLC>,
    bookie: Bookie,
    subscribers: Subscribers,
    tx_bcast: Sender<BroadcastInput>,
    tx_apply: Sender<(ActorId, i64)>,
    schema: RwLock<NormalizedSchema>,
}

impl Agent {
    pub fn new(config: AgentConfig) -> Self {
        Self(Arc::new(AgentInner {
            actor_id: config.actor_id,
            pool: SplitPool::new(config.ro_pool, config.rw_pool, config.tripwire),
            config: config.config,
            gossip_addr: config.gossip_addr,
            api_addr: config.api_addr,
            members: config.members,
            clock: config.clock,
            bookie: config.bookie,
            subscribers: config.subscribers,
            tx_bcast: config.tx_bcast,
            tx_apply: config.tx_apply,
            schema: config.schema,
        }))
    }

    /// Return a borrowed [SqlitePool]
    pub fn pool(&self) -> &SplitPool {
        &self.0.pool
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
    pub fn api_addr(&self) -> SocketAddr {
        self.0.api_addr
    }
    pub fn subscribers(&self) -> &Subscribers {
        &self.0.subscribers
    }

    pub fn tx_bcast(&self) -> &Sender<BroadcastInput> {
        &self.0.tx_bcast
    }

    pub fn tx_apply(&self) -> &Sender<(ActorId, i64)> {
        &self.0.tx_apply
    }

    pub fn bookie(&self) -> &Bookie {
        &self.0.bookie
    }

    pub fn members(&self) -> &RwLock<Members> {
        &self.0.members
    }

    pub fn schema(&self) -> &RwLock<NormalizedSchema> {
        &self.0.schema
    }

    pub fn db_path(&self) -> Utf8PathBuf {
        self.0.config.load().db_path.clone()
    }

    pub fn config(&self) -> arc_swap::Guard<Arc<Config>, arc_swap::strategy::DefaultStrategy> {
        self.0.config.load()
    }

    pub fn set_config(&self, new_conf: Config) {
        self.0.config.store(Arc::new(new_conf))
    }
}

#[derive(Debug, Clone)]
pub struct SplitPool(Arc<SplitPoolInner>);

#[derive(Debug)]
struct SplitPoolInner {
    read: SqlitePool,
    write: SqlitePool,

    priority_tx: Sender<oneshot::Sender<DropGuard>>,
    normal_tx: Sender<oneshot::Sender<DropGuard>>,
    low_tx: Sender<oneshot::Sender<DropGuard>>,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error(transparent)]
    Pool(#[from] bb8::RunError<bb8_rusqlite::Error>),
    #[error("queue is closed")]
    QueueClosed,
    #[error("callback is closed")]
    CallbackClosed,
}

#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    #[error("could not acquire pooled connection: {0}")]
    Pool(#[from] PoolError),
    #[error("rusqlite: {0}")]
    Rusqlite(#[from] rusqlite::Error),
}

impl SplitPool {
    pub fn new(read: SqlitePool, write: SqlitePool, mut tripwire: Tripwire) -> Self {
        let (priority_tx, mut priority_rx) = channel(256);
        let (normal_tx, mut normal_rx) = channel(512);
        let (low_tx, mut low_rx) = channel(1024);

        spawn_counted(async move {
            loop {
                let tx: oneshot::Sender<DropGuard> = tokio::select! {
                    biased;

                    _ = &mut tripwire => {
                        break
                    }

                    Some(tx) = priority_rx.recv() => tx,
                    Some(tx) = normal_rx.recv() => tx,
                    Some(tx) = low_rx.recv() => tx,
                };

                wait_conn_drop(tx).await
            }

            // keep processing priority messages
            // NOTE: not using recv because it'll wait indefinitely, this only waits until all
            //       current conn requests are done
            while let Ok(tx) = priority_rx.try_recv() {
                wait_conn_drop(tx).await
            }
        });

        Self(Arc::new(SplitPoolInner {
            read,
            write,
            priority_tx,
            normal_tx,
            low_tx,
        }))
    }

    // get a read-only connection
    pub async fn read(
        &self,
    ) -> Result<PooledConnection<CrConnManager>, bb8::RunError<bb8_rusqlite::Error>> {
        self.0.read.get().await
    }

    // get a high priority write connection (e.g. client input)
    pub async fn write_priority(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.priority_tx).await
    }

    // get a normal priority write connection (e.g. sync process)
    pub async fn write_normal(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.normal_tx).await
    }

    // get a low priority write connection (e.g. background tasks)
    pub async fn write_low(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.low_tx).await
    }

    async fn write_inner(
        &self,
        chan: &Sender<oneshot::Sender<DropGuard>>,
    ) -> Result<WriteConn, PoolError> {
        let (tx, rx) = oneshot::channel();
        chan.send(tx).await.map_err(|_| PoolError::QueueClosed)?;
        let _drop_guard = rx.await.map_err(|_| PoolError::CallbackClosed)?;
        let conn = self.0.write.get().await?;
        Ok(WriteConn { conn, _drop_guard })
    }
}

async fn wait_conn_drop(tx: oneshot::Sender<DropGuard>) {
    let cancel = CancellationToken::new();
    let drop_guard = cancel.clone().drop_guard();

    if let Err(_e) = tx.send(drop_guard) {
        error!("could not send back drop guard for pooled conn, oneshot channel likely closed");
        return;
    }

    cancel.cancelled().await
}

pub struct WriteConn<'a> {
    conn: PooledConnection<'a, CrConnManager>,
    _drop_guard: DropGuard,
}

impl<'a> Deref for WriteConn<'a> {
    type Target = PooledConnection<'a, CrConnManager>;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<'a> DerefMut for WriteConn<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Pool(#[from] PoolError),
}

pub async fn reload(agent: &Agent, new_conf: Config) -> Result<(), ReloadError> {
    let old_conf = agent.config();

    if old_conf.db_path != new_conf.db_path {
        warn!("reloaded ineffectual change: db_path");
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

    let mut conn = agent.pool().write_normal().await?;
    let mut schema_write = agent.0.schema.write();

    let new_schema =
        block_in_place(|| apply_schema(&mut conn, &new_conf.schema_paths, &schema_write))?;

    agent.set_config(new_conf);
    *schema_write = new_schema;

    Ok(())
}

#[derive(Clone, Eq, PartialEq)]
pub enum KnownDbVersion {
    Current {
        db_version: i64,
        last_seq: i64,
        ts: Timestamp,
    },
    Partial {
        seqs: RangeInclusiveSet<i64>,
        last_seq: i64,
        ts: Timestamp,
    },
    Cleared,
}

pub type BookedVersions = RangeInclusiveMap<i64, KnownDbVersion>;
pub type BookedInner = Arc<RwLock<BookedVersions>>;

#[derive(Default, Clone)]
pub struct Booked(BookedInner);

impl Booked {
    pub fn new(inner: BookedInner) -> Self {
        Self(inner)
    }

    pub fn insert(&self, version: i64, db_version: KnownDbVersion) {
        self.0.write().insert(version..=version, db_version);
    }

    pub fn contains(&self, version: i64, seqs: Option<&RangeInclusive<i64>>) -> bool {
        match seqs {
            Some(check_seqs) => {
                let read = self.0.read();
                match read.get(&version) {
                    Some(known) => match known {
                        KnownDbVersion::Partial { seqs, .. } => {
                            check_seqs.clone().all(|seq| seqs.contains(&seq))
                        }
                        KnownDbVersion::Current { .. } | KnownDbVersion::Cleared => true,
                    },
                    None => false,
                }
            }
            None => self.0.read().contains_key(&version),
        }
    }

    pub fn last(&self) -> Option<i64> {
        self.0.read().iter().map(|(k, _v)| *k.end()).max()
    }

    pub fn read(&self) -> RwLockReadGuard<BookedVersions> {
        self.0.read()
    }

    pub fn write(&self) -> BookWriter {
        BookWriter(self.0.write())
    }
}

pub struct BookWriter<'a>(RwLockWriteGuard<'a, BookedVersions>);

impl<'a> BookWriter<'a> {
    pub fn insert(&mut self, version: i64, known_version: KnownDbVersion) {
        self.0.insert(version..=version, known_version);
    }

    pub fn contains(&self, version: i64, seqs: Option<&RangeInclusive<i64>>) -> bool {
        match seqs {
            Some(check_seqs) => match self.0.get(&version) {
                Some(known) => match known {
                    KnownDbVersion::Partial { seqs, .. } => {
                        check_seqs.clone().all(|seq| seqs.contains(&seq))
                    }
                    KnownDbVersion::Current { .. } | KnownDbVersion::Cleared => true,
                },
                None => false,
            },
            None => self.0.contains_key(&version),
        }
    }

    pub fn last(&self) -> Option<i64> {
        self.0.iter().map(|(k, _v)| *k.end()).max()
    }

    pub fn current_versions(&self) -> BTreeMap<i64, i64> {
        self.0
            .iter()
            .filter_map(|(range, known)| {
                if let KnownDbVersion::Current { db_version, .. } = known {
                    Some((*db_version, *range.start()))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn inner(&self) -> &RwLockWriteGuard<'a, BookedVersions> {
        &self.0
    }
}

pub type BookieInner = Arc<RwLock<HashMap<ActorId, Booked>>>;

#[derive(Default, Clone)]
pub struct Bookie(BookieInner);

impl Bookie {
    pub fn new(inner: BookieInner) -> Self {
        Self(inner)
    }

    pub fn add(&self, actor_id: ActorId, version: i64, db_version: KnownDbVersion) {
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

    pub fn for_actor(&self, actor_id: ActorId) -> Booked {
        let mut w = self.0.write();
        w.entry(actor_id).or_default().clone()
    }

    pub fn contains(
        &self,
        actor_id: &ActorId,
        version: i64,
        seqs: Option<&RangeInclusive<i64>>,
    ) -> bool {
        self.0
            .read()
            .get(actor_id)
            .map(|booked| booked.contains(version, seqs))
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
