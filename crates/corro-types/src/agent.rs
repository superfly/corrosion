use std::{
    cmp,
    collections::{btree_map, BTreeMap, HashMap, HashSet},
    io,
    net::SocketAddr,
    ops::{Deref, DerefMut, RangeInclusive},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use compact_str::CompactString;
use indexmap::IndexMap;
use metrics::{gauge, histogram};
use parking_lot::RwLock;
use rangemap::RangeInclusiveSet;
use rusqlite::{named_params, Connection, Transaction};
use serde::{Deserialize, Serialize};
use tokio::sync::{
    AcquireError, OwnedRwLockWriteGuard as OwnedTokioRwLockWriteGuard, OwnedSemaphorePermit,
    RwLock as TokioRwLock, RwLockReadGuard as TokioRwLockReadGuard,
    RwLockWriteGuard as TokioRwLockWriteGuard,
};
use tokio::{
    runtime::Handle,
    sync::{oneshot, Semaphore},
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

use crate::{
    actor::{Actor, ActorId, ClusterId},
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput, Timestamp},
    channel::{bounded, CorroSender},
    config::Config,
    pubsub::SubsManager,
    schema::Schema,
    sqlite::{rusqlite_to_crsqlite, setup_conn, CrConn, Migration, SqlitePool, SqlitePoolError},
};

use super::members::Members;

#[derive(Clone)]
pub struct Agent(Arc<AgentInner>);

pub struct AgentConfig {
    pub actor_id: ActorId,
    pub pool: SplitPool,
    pub config: ArcSwap<Config>,
    pub gossip_addr: SocketAddr,
    pub external_addr: Option<SocketAddr>,
    pub api_addr: SocketAddr,
    pub members: RwLock<Members>,
    pub clock: Arc<uhlc::HLC>,

    pub booked: Booked,

    pub tx_bcast: CorroSender<BroadcastInput>,
    pub tx_apply: CorroSender<(ActorId, Version)>,
    pub tx_clear_buf: CorroSender<(ActorId, RangeInclusive<Version>)>,
    pub tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    pub tx_foca: CorroSender<FocaInput>,

    pub write_sema: Arc<Semaphore>,

    pub schema: RwLock<Schema>,
    pub cluster_id: ClusterId,

    pub subs_manager: SubsManager,

    pub tripwire: Tripwire,
}

pub struct AgentInner {
    actor_id: ActorId,
    pool: SplitPool,
    config: ArcSwap<Config>,
    gossip_addr: SocketAddr,
    external_addr: Option<SocketAddr>,
    api_addr: SocketAddr,
    members: RwLock<Members>,
    clock: Arc<uhlc::HLC>,
    booked: Booked,
    tx_bcast: CorroSender<BroadcastInput>,
    tx_apply: CorroSender<(ActorId, Version)>,
    tx_clear_buf: CorroSender<(ActorId, RangeInclusive<Version>)>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    tx_foca: CorroSender<FocaInput>,
    write_sema: Arc<Semaphore>,
    schema: RwLock<Schema>,
    cluster_id: ArcSwap<ClusterId>,
    limits: Limits,
    subs_manager: SubsManager,
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub sync: Arc<Semaphore>,
}

impl Agent {
    pub fn new(config: AgentConfig) -> Self {
        Self(Arc::new(AgentInner {
            actor_id: config.actor_id,
            pool: config.pool,
            config: config.config,
            gossip_addr: config.gossip_addr,
            external_addr: config.external_addr,
            api_addr: config.api_addr,
            members: config.members,
            clock: config.clock,
            booked: config.booked,
            tx_bcast: config.tx_bcast,
            tx_apply: config.tx_apply,
            tx_clear_buf: config.tx_clear_buf,
            tx_changes: config.tx_changes,
            tx_foca: config.tx_foca,
            write_sema: config.write_sema,
            schema: config.schema,
            cluster_id: ArcSwap::from_pointee(config.cluster_id),
            limits: Limits {
                sync: Arc::new(Semaphore::new(3)),
            },
            subs_manager: config.subs_manager,
        }))
    }

    pub fn actor<C: Into<Option<ClusterId>>>(&self, cluster_id: C) -> Actor {
        Actor::new(
            self.0.actor_id,
            self.external_addr().unwrap_or_else(|| self.gossip_addr()),
            self.clock().new_timestamp().into(),
            cluster_id.into().unwrap_or_else(|| self.cluster_id()),
        )
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

    pub fn external_addr(&self) -> Option<SocketAddr> {
        self.0.external_addr
    }

    pub fn api_addr(&self) -> SocketAddr {
        self.0.api_addr
    }

    pub fn tx_bcast(&self) -> &CorroSender<BroadcastInput> {
        &self.0.tx_bcast
    }

    pub fn tx_apply(&self) -> &CorroSender<(ActorId, Version)> {
        &self.0.tx_apply
    }

    pub fn tx_changes(&self) -> &CorroSender<(ChangeV1, ChangeSource)> {
        &self.0.tx_changes
    }

    pub fn tx_clear_buf(&self) -> &CorroSender<(ActorId, RangeInclusive<Version>)> {
        &self.0.tx_clear_buf
    }

    pub fn tx_foca(&self) -> &CorroSender<FocaInput> {
        &self.0.tx_foca
    }

    pub fn write_sema(&self) -> &Arc<Semaphore> {
        &self.0.write_sema
    }

    pub async fn write_permit(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        self.0.write_sema.clone().acquire_owned().await
    }

    pub fn write_permit_blocking(&self) -> Result<OwnedSemaphorePermit, AcquireError> {
        Handle::current().block_on(self.0.write_sema.clone().acquire_owned())
    }

    pub fn booked(&self) -> &Booked {
        &self.0.booked
    }

    pub fn members(&self) -> &RwLock<Members> {
        &self.0.members
    }

    pub fn schema(&self) -> &RwLock<Schema> {
        &self.0.schema
    }

    pub fn db_path(&self) -> Utf8PathBuf {
        self.0.config.load().db.path.clone()
    }

    pub fn config(&self) -> arc_swap::Guard<Arc<Config>, arc_swap::strategy::DefaultStrategy> {
        self.0.config.load()
    }

    pub fn set_config(&self, new_conf: Config) {
        self.0.config.store(Arc::new(new_conf))
    }

    pub fn limits(&self) -> &Limits {
        &self.0.limits
    }

    pub fn subs_manager(&self) -> &SubsManager {
        &self.0.subs_manager
    }

    pub fn set_cluster_id(&self, cluster_id: ClusterId) {
        self.0.cluster_id.store(Arc::new(cluster_id));
    }

    pub fn cluster_id(&self) -> ClusterId {
        *self.0.cluster_id.load().as_ref()
    }
}

pub fn migrate(conn: &mut Connection) -> rusqlite::Result<()> {
    let migrations: Vec<Box<dyn Migration>> = vec![
        Box::new(init_migration as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(bookkeeping_db_version_index as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(create_corro_subs as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(refactor_corro_members as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(crsqlite_v0_16_migration as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(create_bookkeeping_gaps as fn(&Transaction) -> rusqlite::Result<()>),
    ];

    crate::sqlite::migrate(conn, migrations)
}

fn create_bookkeeping_gaps(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        r#"
        -- store known needed versions
        CREATE TABLE IF NOT EXISTS __corro_bookkeeping_gaps (
            actor_id BLOB NOT NULL,
            start INTEGER NOT NULL,
            end INTEGER NOT NULL,

            PRIMARY KEY (actor_id, start)
        ) WITHOUT ROWID;
    "#,
    )
}

// since crsqlite 0.16, site_id is NOT NULL in clock tables
// also sets the new 'merge-equal-values' config to true.
fn crsqlite_v0_16_migration(tx: &Transaction) -> rusqlite::Result<()> {
    let tables: Vec<String> = tx.prepare("SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name LIKE '%__crsql_clock'")?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<Vec<_>>>()?;

    for table in tables {
        let indexes: Vec<String> = tx
            .prepare(&format!(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name LIKE '{table}%'"
            ))?
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        tx.execute_batch(
            &format!(r#"
                CREATE TABLE {table}_new (
                    key INTEGER NOT NULL,
                    col_name TEXT NOT NULL,
                    col_version INTEGER NOT NULL,
                    db_version INTEGER NOT NULL,
                    site_id INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    PRIMARY KEY (key, col_name)
                ) WITHOUT ROWID, STRICT;

                INSERT INTO {table}_new SELECT key, col_name, col_version, db_version, COALESCE(site_id, 0), seq FROM {table};

                ALTER TABLE {table} RENAME TO {table}_old;
                ALTER TABLE {table}_new RENAME TO {table};

                DROP TABLE {table}_old;

                CREATE INDEX IF NOT EXISTS corro_{table}_site_id_dbv ON {table} (site_id, db_version);
            "#),
        )?;

        // recreate the indexes
        for sql in indexes {
            tx.execute_batch(&sql)?;
        }
    }

    // we want this to be true or else we'll assuredly make our DB inconsistent.
    let _value: i64 = tx.query_row(
        "SELECT crsql_config_set('merge-equal-values', 1);",
        [],
        |row| row.get(0),
    )?;

    Ok(())
}

fn refactor_corro_members(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        r#"
        -- remove state
        ALTER TABLE __corro_members DROP COLUMN state;
        -- remove rtts
        ALTER TABLE __corro_members DROP COLUMN rtts;
        -- add computed rtt_min
        ALTER TABLE __corro_members ADD COLUMN rtt_min INTEGER;
        -- add updated_at
        ALTER TABLE __corro_members ADD COLUMN updated_at DATETIME NOT NULL DEFAULT 0;
    "#,
    )
}

fn create_corro_subs(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        r#"
        -- where subscriptions are stored
        CREATE TABLE __corro_subs (
            id BLOB PRIMARY KEY NOT NULL,
            sql TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'created'
        ) WITHOUT ROWID;
    "#,
    )
}

fn bookkeeping_db_version_index(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        "
        CREATE INDEX __corro_bookkeeping_db_version ON __corro_bookkeeping (db_version);
        ",
    )
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

#[derive(Debug, Clone)]
pub struct SplitPool(Arc<SplitPoolInner>);

#[derive(Debug)]
struct SplitPoolInner {
    path: PathBuf,
    write_sema: Arc<Semaphore>,

    read: SqlitePool,
    write: SqlitePool,

    priority_tx: CorroSender<oneshot::Sender<CancellationToken>>,
    normal_tx: CorroSender<oneshot::Sender<CancellationToken>>,
    low_tx: CorroSender<oneshot::Sender<CancellationToken>>,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error("queue is closed")]
    QueueClosed,
    #[error("callback is closed")]
    CallbackClosed,
    #[error("could not acquire write permit")]
    Permit(#[from] AcquireError),
}

#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    #[error("could not acquire pooled connection: {0}")]
    Pool(#[from] PoolError),
    #[error("rusqlite: {source} (actor_id: {actor_id:?}, version: {version:?})")]
    Rusqlite {
        source: rusqlite::Error,
        actor_id: Option<ActorId>,
        version: Option<Version>,
    },
    #[error("non-contiguous empties range delete")]
    NonContiguousDelete,
}

#[derive(Debug, thiserror::Error)]
pub enum SplitPoolCreateError {
    #[error(transparent)]
    Pool(#[from] sqlite_pool::CreatePoolError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
}

impl SplitPool {
    pub async fn create<P: AsRef<Path>>(
        path: P,
        write_sema: Arc<Semaphore>,
    ) -> Result<Self, SplitPoolCreateError> {
        let rw_pool = sqlite_pool::Config::new(path.as_ref())
            .max_size(1)
            .create_pool_transform(rusqlite_to_crsqlite)?;

        debug!("built RW pool");

        let ro_pool = sqlite_pool::Config::new(path.as_ref())
            .read_only()
            .max_size(20)
            .create_pool_transform(rusqlite_to_crsqlite)?;
        debug!("built RO pool");

        Ok(Self::new(
            path.as_ref().to_owned(),
            write_sema,
            ro_pool,
            rw_pool,
        ))
    }

    fn new(path: PathBuf, write_sema: Arc<Semaphore>, read: SqlitePool, write: SqlitePool) -> Self {
        let (priority_tx, mut priority_rx) = bounded(256, "priority");
        let (normal_tx, mut normal_rx) = bounded(512, "normal");
        let (low_tx, mut low_rx) = bounded(1024, "low");

        tokio::spawn(async move {
            loop {
                let tx: oneshot::Sender<CancellationToken> = tokio::select! {
                    biased;

                    Some(tx) = priority_rx.recv() => tx,
                    Some(tx) = normal_rx.recv() => tx,
                    Some(tx) = low_rx.recv() => tx,
                };

                wait_conn_drop(tx).await
            }
        });

        Self(Arc::new(SplitPoolInner {
            path,
            write_sema,
            read,
            write,
            priority_tx,
            normal_tx,
            low_tx,
        }))
    }

    pub fn emit_metrics(&self) {
        let read_state = self.0.read.status();
        gauge!("corro.sqlite.pool.read.connections").set(read_state.size as f64);
        gauge!("corro.sqlite.pool.read.connections.available").set(read_state.available as f64);
        gauge!("corro.sqlite.pool.read.connections.waiting").set(read_state.waiting as f64);

        let write_state = self.0.write.status();
        gauge!("corro.sqlite.pool.write.connections").set(write_state.size as f64);
        gauge!("corro.sqlite.pool.write.connections.available").set(write_state.available as f64);
        gauge!("corro.sqlite.pool.write.connections.waiting").set(write_state.waiting as f64);
    }

    // get a read-only connection
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn read(&self) -> Result<sqlite_pool::Connection<CrConn>, SqlitePoolError> {
        self.0.read.get().await
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn read_blocking(&self) -> Result<sqlite_pool::Connection<CrConn>, SqlitePoolError> {
        Handle::current().block_on(self.0.read.get())
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn dedicated(&self) -> rusqlite::Result<Connection> {
        let mut conn = rusqlite::Connection::open(&self.0.path)?;
        setup_conn(&mut conn)?;
        Ok(conn)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn client_dedicated(&self) -> rusqlite::Result<CrConn> {
        let conn = rusqlite::Connection::open(&self.0.path)?;
        rusqlite_to_crsqlite(conn)
    }

    // get a high priority write connection (e.g. client input)
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn write_priority(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.priority_tx, "priority").await
    }

    // get a normal priority write connection (e.g. sync process)
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn write_normal(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.normal_tx, "normal").await
    }

    // get a low priority write connection (e.g. background tasks)
    #[tracing::instrument(skip(self), level = "debug")]
    pub async fn write_low(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.low_tx, "low").await
    }

    async fn write_inner(
        &self,
        chan: &CorroSender<oneshot::Sender<CancellationToken>>,
        queue: &'static str,
    ) -> Result<WriteConn, PoolError> {
        let (tx, rx) = oneshot::channel();
        chan.send(tx).await.map_err(|_| PoolError::QueueClosed)?;
        let start = Instant::now();
        let token = rx.await.map_err(|_| PoolError::CallbackClosed)?;
        histogram!("corro.sqlite.pool.queue.seconds", "queue" => queue)
            .record(start.elapsed().as_secs_f64());
        let conn = self.0.write.get().await?;

        let start = Instant::now();
        let _permit = self.0.write_sema.clone().acquire_owned().await?;
        histogram!("corro.sqlite.write_permit.acquisition.seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(WriteConn {
            conn,
            _drop_guard: token.drop_guard(),
            _permit,
        })
    }
}

async fn wait_conn_drop(tx: oneshot::Sender<CancellationToken>) {
    let cancel = CancellationToken::new();

    if let Err(_e) = tx.send(cancel.clone()) {
        error!("could not send back drop guard for pooled conn, oneshot channel likely closed");
        return;
    }

    cancel.cancelled().await
}

pub struct WriteConn {
    conn: sqlite_pool::Connection<CrConn>,
    _drop_guard: DropGuard,
    _permit: OwnedSemaphorePermit,
}

impl Deref for WriteConn {
    type Target = sqlite_pool::Connection<CrConn>;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for WriteConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

pub struct CountedTokioRwLock<T> {
    registry: LockRegistry,
    lock: Arc<TokioRwLock<T>>,
}

impl<T> CountedTokioRwLock<T> {
    fn new(registry: LockRegistry, value: T) -> Self {
        Self {
            registry,
            lock: Arc::new(TokioRwLock::new(value)),
        }
    }

    #[tracing::instrument(skip(self, label), level = "debug")]
    pub async fn write<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockWriteGuard<'_, T> {
        self.registry.acquire_write(label, &self.lock).await
    }

    #[tracing::instrument(skip(self, label), level = "debug")]
    pub async fn write_owned<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        self.registry
            .acquire_write_owned(label, self.lock.clone())
            .await
    }

    #[tracing::instrument(skip(self, label), level = "debug")]
    pub fn blocking_write<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockWriteGuard<'_, T> {
        self.registry.acquire_blocking_write(label, &self.lock)
    }

    #[tracing::instrument(skip(self, label), level = "debug")]
    pub fn blocking_write_owned<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        self.registry
            .acquire_blocking_write_owned(label, self.lock.clone())
    }

    #[tracing::instrument(skip(self, label), level = "debug")]
    pub fn blocking_read<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockReadGuard<'_, T> {
        self.registry.acquire_blocking_read(label, &self.lock)
    }

    #[tracing::instrument(skip(self, label), level = "debug")]
    pub async fn read<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockReadGuard<'_, T> {
        self.registry.acquire_read(label, &self.lock).await
    }

    pub fn registry(&self) -> &LockRegistry {
        &self.registry
    }
}

pub struct CountedTokioRwLockWriteGuard<'a, T> {
    lock: TokioRwLockWriteGuard<'a, T>,
    _tracker: LockTracker,
}

impl<'a, T> Deref for CountedTokioRwLockWriteGuard<'a, T> {
    type Target = TokioRwLockWriteGuard<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.lock
    }
}

impl<'a, T> DerefMut for CountedTokioRwLockWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lock
    }
}

pub struct CountedOwnedTokioRwLockWriteGuard<T> {
    lock: OwnedTokioRwLockWriteGuard<T>,
    _tracker: LockTracker,
}

impl<T> Deref for CountedOwnedTokioRwLockWriteGuard<T> {
    type Target = OwnedTokioRwLockWriteGuard<T>;

    fn deref(&self) -> &Self::Target {
        &self.lock
    }
}

impl<T> DerefMut for CountedOwnedTokioRwLockWriteGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lock
    }
}

pub struct CountedTokioRwLockReadGuard<'a, T> {
    lock: TokioRwLockReadGuard<'a, T>,
    _tracker: LockTracker,
}

impl<'a, T> Deref for CountedTokioRwLockReadGuard<'a, T> {
    type Target = TokioRwLockReadGuard<'a, T>;

    fn deref(&self) -> &Self::Target {
        &self.lock
    }
}

impl<'a, T> DerefMut for CountedTokioRwLockReadGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.lock
    }
}

type LockId = usize;

#[derive(Debug, Clone)]
pub struct LockMeta {
    pub label: CompactString,
    pub kind: LockKind,
    pub state: LockState,
    pub started_at: Instant,
}

#[derive(Default, Clone)]
pub struct LockRegistry {
    id_gen: Arc<AtomicUsize>,
    pub map: Arc<RwLock<IndexMap<LockId, LockMeta>>>,
}

impl LockRegistry {
    fn remove(&self, id: &LockId) {
        self.map.write().remove(id);
    }

    async fn acquire_write<'a, T, C: Into<CompactString>>(
        &self,
        label: C,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockWriteGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label: label.into(),
                kind: LockKind::Write,
                state: LockState::Acquiring,
                started_at: Instant::now(),
            },
        );
        let _tracker = LockTracker {
            id,
            registry: self.clone(),
        };
        let w = lock.write().await;
        self.set_lock_state(&id, LockState::Locked);
        CountedTokioRwLockWriteGuard { lock: w, _tracker }
    }

    async fn acquire_write_owned<T, C: Into<CompactString>>(
        &self,
        label: C,
        lock: Arc<TokioRwLock<T>>,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label: label.into(),
                kind: LockKind::Write,
                state: LockState::Acquiring,
                started_at: Instant::now(),
            },
        );
        let _tracker = LockTracker {
            id,
            registry: self.clone(),
        };
        let w = lock.write_owned().await;
        self.set_lock_state(&id, LockState::Locked);
        CountedOwnedTokioRwLockWriteGuard { lock: w, _tracker }
    }

    fn acquire_blocking_write<'a, T, C: Into<CompactString>>(
        &self,
        label: C,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockWriteGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label: label.into(),
                kind: LockKind::Write,
                state: LockState::Acquiring,
                started_at: Instant::now(),
            },
        );
        let _tracker = LockTracker {
            id,
            registry: self.clone(),
        };
        let w = lock.blocking_write();
        self.set_lock_state(&id, LockState::Locked);
        CountedTokioRwLockWriteGuard { lock: w, _tracker }
    }

    fn acquire_blocking_write_owned<T, C: Into<CompactString>>(
        &self,
        label: C,
        lock: Arc<TokioRwLock<T>>,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label: label.into(),
                kind: LockKind::Write,
                state: LockState::Acquiring,
                started_at: Instant::now(),
            },
        );
        let _tracker = LockTracker {
            id,
            registry: self.clone(),
        };
        let w = loop {
            if let Ok(w) = lock.clone().try_write_owned() {
                break w;
            }
            // don't instantly loop
            std::thread::sleep(Duration::from_millis(1));
        };
        self.set_lock_state(&id, LockState::Locked);
        CountedOwnedTokioRwLockWriteGuard { lock: w, _tracker }
    }

    async fn acquire_read<'a, T, C: Into<CompactString>>(
        &self,
        label: C,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockReadGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label: label.into(),
                kind: LockKind::Read,
                state: LockState::Acquiring,
                started_at: Instant::now(),
            },
        );
        let _tracker = LockTracker {
            id,
            registry: self.clone(),
        };
        let w = lock.read().await;
        self.set_lock_state(&id, LockState::Locked);
        CountedTokioRwLockReadGuard { lock: w, _tracker }
    }

    fn acquire_blocking_read<'a, T, C: Into<CompactString>>(
        &self,
        label: C,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockReadGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label: label.into(),
                kind: LockKind::Read,
                state: LockState::Acquiring,
                started_at: Instant::now(),
            },
        );
        let _tracker = LockTracker {
            id,
            registry: self.clone(),
        };
        let w = lock.blocking_read();
        self.set_lock_state(&id, LockState::Locked);
        CountedTokioRwLockReadGuard { lock: w, _tracker }
    }

    fn set_lock_state(&self, id: &LockId, state: LockState) {
        if let Some(meta) = self.map.write().get_mut(id) {
            meta.state = state
        }
    }

    fn insert_lock(&self, id: LockId, meta: LockMeta) {
        self.map.write().insert(id, meta);
    }

    fn gen_id(&self) -> LockId {
        self.id_gen.fetch_add(1, Ordering::Release) + 1
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockState {
    Acquiring,
    Locked,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockKind {
    Read,
    Write,
}

struct LockTracker {
    id: LockId,
    registry: LockRegistry,
}

impl Drop for LockTracker {
    fn drop(&mut self) {
        self.registry.remove(&self.id)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartialVersion {
    // range of sequences recorded
    pub seqs: RangeInclusiveSet<CrsqlSeq>,
    // actual last sequence originally produced
    pub last_seq: CrsqlSeq,
    // timestamp when the change was produced by the source
    pub ts: Timestamp,
}

impl PartialVersion {
    pub fn is_complete(&self) -> bool {
        self.seqs.gaps(&self.full_range()).count() == 0
    }

    pub fn full_range(&self) -> RangeInclusive<CrsqlSeq> {
        CrsqlSeq(1)..=self.last_seq
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CurrentVersion {
    pub db_version: CrsqlDbVersion,
    pub last_seq: CrsqlSeq,
    pub ts: Timestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum KnownDbVersion {
    Cleared,
    Current(CurrentVersion),
    Partial(PartialVersion),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookedVersions {
    actor_id: ActorId,
    pub partials: BTreeMap<Version, PartialVersion>,
    needed: RangeInclusiveSet<Version>,
    max: Option<Version>,
}

impl BookedVersions {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            partials: Default::default(),
            needed: Default::default(),
            max: Default::default(),
        }
    }

    pub fn from_conn(conn: &Connection, actor_id: ActorId) -> rusqlite::Result<Self> {
        let mut bv = BookedVersions::new(actor_id);

        // fetch the biggest version we know, a partial version might override
        // this below
        bv.max = conn
            .prepare_cached(
                "SELECT MAX(start_version) FROM __corro_bookkeeping WHERE actor_id = ?",
            )?
            .query_row([actor_id], |row| row.get(0))?;

        let mut changes = GapsChanges {
            actor_id,
            max: bv.max,
            insert_set: Default::default(),
            remove_ranges: Default::default(),
        };

        // fetch the sync's needed version gaps
        let mut prepped = conn
            .prepare_cached("SELECT start, end FROM __corro_bookkeeping_gaps WHERE actor_id = ?")?;
        let mut rows = prepped.query([actor_id])?;

        loop {
            let row = rows.next()?;
            match row {
                None => break,
                Some(row) => {
                    let start_v = row.get(0)?;
                    let end_v = row.get(1)?;

                    changes.insert_set.insert(start_v..=end_v);
                }
            }
        }

        bv.apply_needed_changes(changes);

        // fetch known partial sequences
        let mut prepped = conn.prepare_cached(
            "SELECT version, start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping WHERE site_id = ?",
        )?;
        let mut rows = prepped.query([actor_id])?;

        loop {
            let row = rows.next()?;
            match row {
                None => break,
                Some(row) => {
                    let version = row.get(0)?;
                    bv.max = std::cmp::max(Some(version), bv.max);
                    // NOTE: use normal insert logic to have a consistent behavior
                    bv.insert_partial(
                        version,
                        PartialVersion {
                            seqs: RangeInclusiveSet::from_iter(vec![row.get(1)?..=row.get(2)?]),
                            last_seq: row.get(3)?,
                            ts: row.get(4)?,
                        },
                    );
                }
            }
        }

        Ok(bv)
    }

    pub fn contains_version(&self, version: &Version) -> bool {
        // corrosion knows about a version if...

        // it's not in the list of needed versions
        !self.needed.contains(version) &&
        // and the last known version is bigger than the requested version
        self.max.unwrap_or_default() >= *version
        // we don't need to look at partials because if we have a partial
        // then it fulfills the previous conditions
    }

    pub fn get_partial(&self, version: &Version) -> Option<&PartialVersion> {
        self.partials.get(version)
    }

    pub fn contains(&self, version: Version, seqs: Option<&RangeInclusive<CrsqlSeq>>) -> bool {
        self.contains_version(&version)
            && seqs
                .map(|check_seqs| match self.partials.get(&version) {
                    Some(partial) => check_seqs.clone().all(|seq| partial.seqs.contains(&seq)),
                    // if `contains_version` is true but we don't have a partial version,
                    // then we must have it as a fully applied or cleared version
                    None => true,
                })
                .unwrap_or(true)
    }

    pub fn contains_all(
        &self,
        mut versions: RangeInclusive<Version>,
        seqs: Option<&RangeInclusive<CrsqlSeq>>,
    ) -> bool {
        versions.all(|version| self.contains(version, seqs))
    }

    pub fn last(&self) -> Option<Version> {
        self.max
    }

    pub fn apply_needed_changes(&mut self, mut changes: GapsChanges) {
        for range in std::mem::take(&mut changes.remove_ranges) {
            for version in range.clone() {
                self.partials.remove(&version);
            }
            self.needed.remove(range);
        }

        for range in std::mem::take(&mut changes.insert_set) {
            self.needed.insert(range);
        }

        self.max = changes.max.take();
    }

    // used when the commit has succeeded
    pub fn insert_partial(&mut self, version: Version, partial: PartialVersion) -> PartialVersion {
        debug!(actor_id = %self.actor_id, "insert partial {version:?}");

        match self.partials.entry(version) {
            btree_map::Entry::Vacant(entry) => entry.insert(partial).clone(),
            btree_map::Entry::Occupied(mut entry) => {
                let got = entry.get_mut();
                got.seqs.extend(partial.seqs);
                got.clone()
            }
        }
    }

    pub fn insert_db(
        &mut self,         // only because we want 1 mt a time here
        conn: &Connection, // usually a `Transaction`
        versions: RangeInclusiveSet<Version>,
    ) -> rusqlite::Result<GapsChanges> {
        trace!("wants to insert into db {versions:?}");
        let changes = self.compute_gaps_change(versions);

        trace!(actor_id = %self.actor_id, "delete: {:?}", changes.remove_ranges);
        trace!(actor_id = %self.actor_id, "new: {:?}", changes.insert_set);

        // those are actual ranges we had stored and will change, remove them from the DB
        for range in changes.remove_ranges.iter() {
            debug!(actor_id = %self.actor_id, "deleting {range:?}");
            let count = conn
                .prepare_cached("DELETE FROM __corro_bookkeeping_gaps WHERE actor_id = :actor_id AND start = :start AND end = :end")?
                .execute(named_params! {
                    ":actor_id": self.actor_id,
                    ":start": range.start(),
                    ":end": range.end()
                })?;
            if count != 1 {
                warn!(actor_id = %self.actor_id, "did not delete gap from db: {range:?}");
            }
            debug_assert_eq!(count, 1, "ineffective deletion of gaps in-db: {range:?}");
        }

        for range in changes.insert_set.iter() {
            debug!(actor_id = %self.actor_id, "inserting {range:?}");
            let res = conn
                .prepare_cached(
                    "INSERT INTO __corro_bookkeeping_gaps VALUES (:actor_id, :start, :end)",
                )?
                .execute(named_params! {
                    ":actor_id": self.actor_id,
                    ":start": range.start(),
                    ":end": range.end()
                });

            if let Err(e) = res {
                let (actor_id, start, end) : (ActorId, Version, Version) = conn.query_row("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps WHERE actor_id = :actor_id AND start = :start", named_params! {
                    ":actor_id": self.actor_id,
                    ":start": range.start(),
                }, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

                warn!("already had gaps entry! actor_id: {actor_id}, start: {start}, end: {end}");

                return Err(e);
            }
        }

        Ok(changes)
    }

    fn compute_gaps_change(&self, versions: RangeInclusiveSet<Version>) -> GapsChanges {
        trace!("needed: {:?}", self.needed);

        let mut changes = GapsChanges {
            // for logging purposes
            actor_id: self.actor_id,
            // set as the current max
            max: self.max,

            insert_set: Default::default(),
            remove_ranges: Default::default(),
        };

        for versions in versions {
            // only update the max if it's bigger
            changes.max = cmp::max(changes.max, Some(*versions.end()));

            let overlapping = self.needed.overlapping(&versions);

            // iterate all partially or fully overlapping changes
            for range in overlapping {
                trace!(actor_id = %self.actor_id, "overlapping: {range:?}");
                // insert the overlapping range in the set (collapses ajoining ranges)
                changes.insert_set.insert(range.clone());
                // remove the range, they'll be added back from the insert set ^
                changes.remove_ranges.insert(range.clone());
            }

            // check if there's a previous range with an end version = start version - 1
            if let Some(range) = self.needed.get(&Version(versions.start().0 - 1)) {
                trace!(actor_id = %self.actor_id, "got a start - 1: {range:?}");
                // insert the collapsible range
                changes.insert_set.insert(range.clone());
                // remove the collapsible range
                changes.remove_ranges.insert(range.clone());
            }

            // check if there's a next range with an start version = end version + 1
            if let Some(range) = self.needed.get(&Version(versions.end().0 + 1)) {
                trace!(actor_id = %self.actor_id, "got a end + 1: {range:?}");
                // insert the collapsible range
                changes.insert_set.insert(range.clone());
                // remove the collapsible range
                changes.remove_ranges.insert(range.clone());
            }

            // either a max or 0
            // TODO: figure out if we want to use 0 instead of None in the struct by default
            let current_max = self.max.unwrap_or_default();

            // check if there's a gap created between our current max and the start version we just inserted
            let gap_start = current_max + 1;
            if gap_start < *versions.start() {
                let range = gap_start..=*versions.start();
                trace!("inserting gap between max + 1 and start: {range:?}");
                changes.insert_set.insert(range.clone());
                for range in self.needed.overlapping(&range) {
                    changes.insert_set.insert(range.clone());
                    changes.remove_ranges.insert(range.clone());
                }
            }

            // we now know the applied versions
            changes.insert_set.remove(versions.clone());
        }
        changes
    }

    pub fn needed(&self) -> &RangeInclusiveSet<Version> {
        &self.needed
    }
}

#[derive(Debug)]
pub struct GapsChanges {
    actor_id: ActorId,
    max: Option<Version>,
    insert_set: RangeInclusiveSet<Version>,
    remove_ranges: HashSet<RangeInclusive<Version>>,
}

// this struct must be drained!
impl Drop for GapsChanges {
    fn drop(&mut self) {
        debug_assert!(
            self.insert_set.is_empty(),
            "gaps changes insert set was not drained!"
        );
        if !self.insert_set.is_empty() {
            warn!(actor_id = %self.actor_id, "gaps: did not properly drain inserted versions set: {:?}", self.insert_set);
        }
        debug_assert!(
            self.remove_ranges.is_empty(),
            "gaps changes remove ranges was not drained!"
        );
        if !self.remove_ranges.is_empty() {
            warn!(actor_id = %self.actor_id, "gaps: did not properly drain remove ranges: {:?}", self.remove_ranges);
        }
        debug_assert!(self.max.is_none(), "max value was not applied");
    }
}

pub type BookedInner = Arc<CountedTokioRwLock<BookedVersions>>;

#[derive(Clone)]
pub struct Booked(BookedInner);

impl Booked {
    pub fn new(versions: BookedVersions, registry: LockRegistry) -> Self {
        Self(Arc::new(CountedTokioRwLock::new(registry, versions)))
    }

    pub async fn read<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockReadGuard<'_, BookedVersions> {
        self.0.read(label).await
    }

    pub async fn write<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockWriteGuard<'_, BookedVersions> {
        self.0.write(label).await
    }

    pub async fn write_owned<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedOwnedTokioRwLockWriteGuard<BookedVersions> {
        self.0.write_owned(label).await
    }

    pub fn blocking_write<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockWriteGuard<'_, BookedVersions> {
        self.0.blocking_write(label)
    }

    pub fn blocking_read<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockReadGuard<'_, BookedVersions> {
        self.0.blocking_read(label)
    }

    pub fn blocking_write_owned<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedOwnedTokioRwLockWriteGuard<BookedVersions> {
        self.0.blocking_write_owned(label)
    }
}

#[derive(Default)]
pub struct BookieInner {
    map: HashMap<ActorId, Booked>,
    registry: LockRegistry,
}

impl BookieInner {
    pub fn ensure(&mut self, actor_id: ActorId) -> Booked {
        self.map
            .entry(actor_id)
            .or_insert_with(|| {
                Booked(Arc::new(CountedTokioRwLock::new(
                    self.registry.clone(),
                    BookedVersions::new(actor_id),
                )))
            })
            .clone()
    }

    pub fn replace_actor(&mut self, actor_id: ActorId, bv: BookedVersions) {
        self.map.insert(
            actor_id,
            Booked(Arc::new(CountedTokioRwLock::new(self.registry.clone(), bv))),
        );
    }
}

impl Deref for BookieInner {
    type Target = HashMap<ActorId, Booked>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl DerefMut for BookieInner {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

#[derive(Clone)]
pub struct Bookie(Arc<CountedTokioRwLock<BookieInner>>);

impl Bookie {
    pub fn new(map: HashMap<ActorId, BookedVersions>) -> Self {
        let registry = LockRegistry::default();
        Self::new_with_registry(map, registry)
    }

    pub fn new_with_registry(
        map: HashMap<ActorId, BookedVersions>,
        registry: LockRegistry,
    ) -> Self {
        Self(Arc::new(CountedTokioRwLock::new(
            registry.clone(),
            BookieInner {
                map: map
                    .into_iter()
                    .map(|(k, v)| (k, Booked::new(v, registry.clone())))
                    .collect(),
                registry,
            },
        )))
    }

    pub async fn read<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockReadGuard<BookieInner> {
        self.0.read(label).await
    }

    pub async fn write<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockWriteGuard<BookieInner> {
        self.0.write(label).await
    }

    pub fn blocking_write<L: Into<CompactString>>(
        &self,
        label: L,
    ) -> CountedTokioRwLockWriteGuard<BookieInner> {
        self.0.blocking_write(label)
    }

    pub fn registry(&self) -> &LockRegistry {
        self.0.registry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_booked_insert_db() -> rusqlite::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let mut conn = CrConn::init(Connection::open_in_memory()?)?;
        migrate(&mut conn)?;

        let actor_id = ActorId::default();
        let mut bv = BookedVersions::new(actor_id);

        let mut all = RangeInclusiveSet::new();

        insert_everywhere(&conn, &mut bv, &mut all, Version(1)..=Version(20))?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(&conn, &mut bv, &mut all, Version(1)..=Version(10))?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // try from an empty state again
        let mut bv = BookedVersions::new(actor_id);
        let mut all = RangeInclusiveSet::new();

        // insert a non-1 first version
        insert_everywhere(&conn, &mut bv, &mut all, Version(5)..=Version(20))?;
        expect_gaps(&conn, &bv, &all, vec![Version(1)..=Version(4)])?;

        // insert a further change that does not overlap a gap
        insert_everywhere(&conn, &mut bv, &mut all, Version(6)..=Version(7))?;
        expect_gaps(&conn, &bv, &all, vec![Version(1)..=Version(4)])?;

        // insert a further change that does overlap a gap
        insert_everywhere(&conn, &mut bv, &mut all, Version(3)..=Version(7))?;
        expect_gaps(&conn, &bv, &all, vec![Version(1)..=Version(2)])?;

        insert_everywhere(&conn, &mut bv, &mut all, Version(1)..=Version(2))?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(&conn, &mut bv, &mut all, Version(25)..=Version(25))?;
        expect_gaps(&conn, &bv, &all, vec![Version(21)..=Version(24)])?;

        insert_everywhere(&conn, &mut bv, &mut all, Version(30)..=Version(35))?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![Version(21)..=Version(24), Version(26)..=Version(29)],
        )?;

        // NOTE: overlapping partially from the end

        insert_everywhere(&conn, &mut bv, &mut all, Version(19)..=Version(22))?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![Version(23)..=Version(24), Version(26)..=Version(29)],
        )?;

        // NOTE: overlapping partially from the start

        insert_everywhere(&conn, &mut bv, &mut all, Version(24)..=Version(25))?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![Version(23)..=Version(23), Version(26)..=Version(29)],
        )?;

        // NOTE: overlapping 2 ranges

        insert_everywhere(&conn, &mut bv, &mut all, Version(23)..=Version(27))?;
        expect_gaps(&conn, &bv, &all, vec![Version(28)..=Version(29)])?;

        // NOTE: ineffective insert of already known ranges

        insert_everywhere(&conn, &mut bv, &mut all, Version(1)..=Version(20))?;
        expect_gaps(&conn, &bv, &all, vec![Version(28)..=Version(29)])?;

        // NOTE: overlapping no ranges, but encompassing a full range

        insert_everywhere(&conn, &mut bv, &mut all, Version(27)..=Version(30))?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // NOTE: touching multiple ranges, partially

        // create gap 36..=39
        insert_everywhere(&conn, &mut bv, &mut all, Version(40)..=Version(45))?;
        // create gap 46..=49
        insert_everywhere(&conn, &mut bv, &mut all, Version(50)..=Version(55))?;

        insert_everywhere(&conn, &mut bv, &mut all, Version(38)..=Version(47))?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![Version(36)..=Version(37), Version(48)..=Version(49)],
        )?;

        // test loading a bv from the conn, they should be identical!
        let mut bv2 = BookedVersions::from_conn(&conn, actor_id)?;
        // manually set the last version because there's nothing in `__corro_bookkeeping`
        bv2.max = Some(Version(55));

        assert_eq!(bv, bv2);

        Ok(())
    }

    fn insert_everywhere(
        conn: &Connection,
        bv: &mut BookedVersions,
        all_versions: &mut RangeInclusiveSet<Version>,
        versions: RangeInclusive<Version>,
    ) -> rusqlite::Result<()> {
        all_versions.insert(versions.clone());
        let changes = bv.insert_db(conn, RangeInclusiveSet::from([versions]))?;
        bv.apply_needed_changes(changes);
        Ok(())
    }

    fn expect_gaps(
        conn: &Connection,
        bv: &BookedVersions,
        all_versions: &RangeInclusiveSet<Version>,
        expected: Vec<RangeInclusive<Version>>,
    ) -> rusqlite::Result<()> {
        let gaps: Vec<(ActorId, Version, Version)> = conn
            .prepare_cached("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(
            gaps,
            expected
                .clone()
                .into_iter()
                .map(|expected| (bv.actor_id, *expected.start(), *expected.end()))
                .collect::<Vec<_>>()
        );

        for range in all_versions.iter() {
            assert!(bv.contains_all(range.clone(), None));
        }

        for range in expected {
            for v in range {
                assert!(!bv.contains(v, None), "expected not to contain {v}");
                assert!(bv.needed.contains(&v), "expected needed to contain {v}");
            }
        }

        assert_eq!(
            bv.max,
            all_versions.iter().last().map(|range| *range.end()),
            "expected last version not to increment"
        );

        Ok(())
    }
}
