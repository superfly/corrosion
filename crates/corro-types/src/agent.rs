use std::{
    cmp,
    collections::{btree_map, BTreeMap, HashMap, HashSet},
    fmt,
    future::Future,
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

use antithesis_sdk::{assert_always, assert_unreachable};
use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use compact_str::{CompactString, ToCompactString};
use indexmap::IndexMap;
use metrics::{gauge, histogram};
use parking_lot::RwLock;
use rangemap::RangeInclusiveSet;
use rusqlite::{named_params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    runtime::Handle,
    sync::{oneshot, Semaphore},
};
use tokio::{
    sync::{
        AcquireError, OwnedRwLockWriteGuard as OwnedTokioRwLockWriteGuard, OwnedSemaphorePermit,
        RwLock as TokioRwLock, RwLockReadGuard as TokioRwLockReadGuard,
        RwLockWriteGuard as TokioRwLockWriteGuard,
    },
    time::timeout,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

use crate::{
    actor::{Actor, ActorId, ClusterId},
    base::{CrsqlDbVersion, CrsqlSeq},
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput, Timestamp},
    channel::{bounded, CorroSender},
    config::Config,
    pubsub::SubsManager,
    schema::Schema,
    sqlite::{
        rusqlite_to_crsqlite, rusqlite_to_crsqlite_write, setup_conn, CrConn, Migration,
        SqlitePool, SqlitePoolError,
    },
    updates::UpdatesManager,
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
    pub tx_apply: CorroSender<(ActorId, CrsqlDbVersion)>,
    pub tx_clear_buf: CorroSender<(ActorId, RangeInclusive<CrsqlDbVersion>)>,
    pub tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    pub tx_foca: CorroSender<FocaInput>,

    pub write_sema: Arc<Semaphore>,

    pub schema: RwLock<Schema>,
    pub cluster_id: ClusterId,

    pub subs_manager: SubsManager,

    pub updates_manager: UpdatesManager,

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
    tx_apply: CorroSender<(ActorId, CrsqlDbVersion)>,
    tx_clear_buf: CorroSender<(ActorId, RangeInclusive<CrsqlDbVersion>)>,
    tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    tx_foca: CorroSender<FocaInput>,
    write_sema: Arc<Semaphore>,
    schema: RwLock<Schema>,
    cluster_id: ArcSwap<ClusterId>,
    limits: Limits,
    subs_manager: SubsManager,
    updates_manager: UpdatesManager,
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
            updates_manager: config.updates_manager,
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

    pub fn tx_apply(&self) -> &CorroSender<(ActorId, CrsqlDbVersion)> {
        &self.0.tx_apply
    }

    pub fn tx_changes(&self) -> &CorroSender<(ChangeV1, ChangeSource)> {
        &self.0.tx_changes
    }

    pub fn tx_clear_buf(&self) -> &CorroSender<(ActorId, RangeInclusive<CrsqlDbVersion>)> {
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

    pub fn updates_manager(&self) -> &UpdatesManager {
        &self.0.updates_manager
    }

    pub fn set_cluster_id(&self, cluster_id: ClusterId) {
        self.0.cluster_id.store(Arc::new(cluster_id));
    }

    pub fn cluster_id(&self) -> ClusterId {
        *self.0.cluster_id.load().as_ref()
    }

    pub fn update_clock_with_timestamp(
        &self,
        actor_id: ActorId,
        ts: Timestamp,
    ) -> Result<(), String> {
        let id = actor_id
            .try_into()
            .map_err(|e| format!("could not convert ActorId to uhlc ID: {e}"))?;
        self.clock()
            .update_with_timestamp(&uhlc::Timestamp::new(ts.0, id))
    }
}

pub fn migrate(clock: Arc<uhlc::HLC>, conn: &mut Connection) -> rusqlite::Result<()> {
    let migrations: Vec<Box<dyn Migration>> = vec![
        Box::new(init_migration as fn(&Transaction) -> rusqlite::Result<()>),
        Box::new(crsqlite_v0_17_migration(clock)),
    ];

    crate::sqlite::migrate(conn, migrations)
}

fn init_migration(tx: &Transaction) -> rusqlite::Result<()> {
    tx.execute_batch(
        r#"
            -- key/value for internal corrosion data (e.g. 'schema_version' => INT)
            -- can be pre-created before corrosion starts so we get a cluster id.
            CREATE TABLE IF NOT EXISTS __corro_state (key TEXT NOT NULL PRIMARY KEY, value);

            -- internal per-db-version seq bookkeeping
            CREATE TABLE  __corro_seq_bookkeeping (
                -- remote actor / site id
                site_id BLOB NOT NULL,
                -- remote internal version
                db_version INTEGER NOT NULL,

                -- start and end seq for this bookkept record
                start_seq INTEGER NOT NULL,
                end_seq INTEGER NOT NULL,

                last_seq INTEGER NOT NULL,

                -- timestamp, need to propagate...
                ts TEXT NOT NULL,

                PRIMARY KEY (site_id, db_version, start_seq)
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
                ts TEXT NOT NULL,

                PRIMARY KEY (site_id, db_version, seq)
            ) WITHOUT ROWID;

            -- SWIM memberships
            CREATE TABLE __corro_members (
                actor_id BLOB PRIMARY KEY NOT NULL,
                address TEXT NOT NULL,

                foca_state JSON,

                rtt_min INTEGER,
                updated_at DATETIME NOT NULL DEFAULT 0
            ) WITHOUT ROWID;

            -- tracked corrosion schema
            CREATE TABLE IF NOT EXISTS __corro_schema (
                tbl_name TEXT NOT NULL,
                type TEXT NOT NULL,
                name TEXT NOT NULL,
                sql TEXT NOT NULL,

                source TEXT NOT NULL,

                PRIMARY KEY (tbl_name, type, name)
            ) WITHOUT ROWID;

            -- store known needed versions
            CREATE TABLE __corro_bookkeeping_gaps (
                actor_id BLOB NOT NULL,
                start INTEGER NOT NULL,
                end INTEGER NOT NULL,

                PRIMARY KEY (actor_id, start)
            ) WITHOUT ROWID;
        "#,
    )?;

    let _value: i64 = tx.query_row(
        "SELECT crsql_config_set('merge-equal-values', 1);",
        [],
        |row| row.get(0),
    )?;

    Ok(())
}

// since crsqlite 0.17, ts is now stored as TEXT in clock tables
// also sets the new 'merge-equal-values' config to true.
fn crsqlite_v0_17_migration(
    clock: Arc<uhlc::HLC>,
) -> impl Fn(&Transaction) -> rusqlite::Result<()> {
    let ts = Timestamp::from(clock.new_timestamp()).as_u64().to_string();

    move |tx: &Transaction| -> rusqlite::Result<()> {
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
                        ts TEXT NOT NULL DEFAULT '0',
                        PRIMARY KEY (key, col_name)
                    ) WITHOUT ROWID, STRICT;

                    INSERT INTO {table}_new (key, col_name, col_version, db_version, site_id, seq, ts) SELECT key, col_name, col_version, db_version, site_id, seq, '{ts}' FROM {table};

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

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SplitPool(Arc<SplitPoolInner>);

#[derive(Debug)]
struct SplitPoolInner {
    path: PathBuf,
    write_sema: Arc<Semaphore>,

    read: SqlitePool,
    write: SqlitePool,

    priority_tx: CorroSender<oneshot::Sender<DropGuard>>,
    normal_tx: CorroSender<oneshot::Sender<DropGuard>>,
    low_tx: CorroSender<oneshot::Sender<DropGuard>>,
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
    #[error("timed out acquiring write permit while {op:?}")]
    TimedOut { op: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    #[error("could not acquire pooled write connection: {0}")]
    Pool(#[from] PoolError),
    #[error("could not acquire pooled read connection: {0}")]
    SqlitePool(#[from] SqlitePoolError),
    #[error("rusqlite: {source} (actor_id: {actor_id:?}, version: {version:?})")]
    Rusqlite {
        source: rusqlite::Error,
        actor_id: Option<ActorId>,
        version: Option<CrsqlDbVersion>,
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
            .create_pool_transform(rusqlite_to_crsqlite_write)?;

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
                let (tx, channel) = tokio::select! {
                    biased;

                    Some(tx) = priority_rx.recv() => (tx, "priority"),
                    Some(tx) = normal_rx.recv() => (tx, "normal"),
                    Some(tx) = low_rx.recv() => (tx, "low"),
                };

                wait_conn_drop(tx, channel).await
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

        let available_permit = self.0.write_sema.available_permits();
        gauge!("corro.sqlite.write.permits.available").set(available_permit as f64);
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

    pub fn drain_read(&self) {
        self.0.read.retain(|_, _| false);
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn dedicated(&self) -> rusqlite::Result<Connection> {
        let conn = rusqlite::Connection::open(&self.0.path)?;
        setup_conn(&conn)?;
        Ok(conn)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn client_dedicated(&self) -> rusqlite::Result<CrConn> {
        let conn = rusqlite::Connection::open(&self.0.path)?;
        rusqlite_to_crsqlite_write(conn)
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
        chan: &CorroSender<oneshot::Sender<DropGuard>>,
        queue: &'static str,
    ) -> Result<WriteConn, PoolError> {
        let (tx, rx) = oneshot::channel();
        let max_timeout = Duration::from_secs(5 * 60);

        timeout_fut("tx to oneshot channel", max_timeout, chan.send(tx))
            .await?
            .map_err(|_| PoolError::QueueClosed)?;

        let start = Instant::now();

        let _drop_guard = timeout_fut("rx from oneshot channel", max_timeout, rx)
            .await?
            .map_err(|_| PoolError::CallbackClosed)?;

        histogram!("corro.sqlite.pool.queue.seconds", "queue" => queue)
            .record(start.elapsed().as_secs_f64());
        let conn = timeout_fut("acquiring write conn", max_timeout, self.0.write.get()).await??;

        let start = Instant::now();
        let _permit = timeout_fut(
            "acquiring write semaphore",
            max_timeout,
            self.0.write_sema.clone().acquire_owned(),
        )
        .await??;

        histogram!("corro.sqlite.write_permit.acquisition.seconds")
            .record(start.elapsed().as_secs_f64());

        Ok(WriteConn {
            conn,
            _drop_guard,
            _permit,
        })
    }
}

async fn wait_conn_drop(tx: oneshot::Sender<DropGuard>, channel: &'static str) {
    let cancel = CancellationToken::new();

    if let Err(_e) = tx.send(cancel.clone().drop_guard()) {
        error!("could not send back drop guard for pooled conn, oneshot channel likely closed");
        return;
    }

    let mut interval = tokio::time::interval(Duration::from_secs(5 * 60));
    // skip first tick
    interval.tick().await;
    let start = Instant::now();

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                break;
            }
            _ = interval.tick() => {
                let details = json!({
                    "channel": channel,
                    "elapsed": start.elapsed().as_secs_f64()
                });
                assert_unreachable!(
                    "wait_conn_drop has been running for too long",
                    &details
                );

                let elapsed = start.elapsed();
                warn!("wait_conn_drop has been running since {elapsed:?}, token_is_cancelled - {:?}, channel - {channel}", cancel.is_cancelled());
                continue;
            }
        }
    }
}

async fn timeout_fut<T, F>(op: &'static str, duration: Duration, fut: F) -> Result<T, PoolError>
where
    F: Future<Output = T>,
{
    timeout(duration, fut)
        .await
        .map_err(|_| PoolError::TimedOut { op: op.to_string() })
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

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn write<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockWriteGuard<'_, T> {
        self.registry.acquire_write(label, extra, &self.lock).await
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn write_owned<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        self.registry
            .acquire_write_owned(label, extra, self.lock.clone())
            .await
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub fn blocking_write<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockWriteGuard<'_, T> {
        self.registry
            .acquire_blocking_write(label, extra, &self.lock)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub fn blocking_write_owned<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        self.registry
            .acquire_blocking_write_owned(label, extra, self.lock.clone())
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub fn blocking_read<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockReadGuard<'_, T> {
        self.registry
            .acquire_blocking_read(label, extra, &self.lock)
    }

    #[tracing::instrument(skip_all, level = "debug")]
    pub async fn read<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockReadGuard<'_, T> {
        self.registry.acquire_read(label, extra, &self.lock).await
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
    pub label: &'static str,
    pub extra: Option<CompactString>,
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

    async fn acquire_write<'a, T, C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockWriteGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label,
                extra: extra.into().map(|d| d.to_compact_string()),
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

    async fn acquire_write_owned<T, C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
        lock: Arc<TokioRwLock<T>>,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label,
                extra: extra.into().map(|d| d.to_compact_string()),
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

    fn acquire_blocking_write<'a, T, C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockWriteGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label,
                extra: extra.into().map(|d| d.to_compact_string()),
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

    fn acquire_blocking_write_owned<T, C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
        lock: Arc<TokioRwLock<T>>,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label,
                extra: extra.into().map(|d| d.to_compact_string()),
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

    async fn acquire_read<'a, T, C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockReadGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label,
                extra: extra
                    .into()
                    .map(|d| d.to_compact_string())
                    .map(|d| d.to_compact_string()),
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

    fn acquire_blocking_read<'a, T, C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
        lock: &'a TokioRwLock<T>,
    ) -> CountedTokioRwLockReadGuard<'a, T> {
        let id = self.gen_id();
        self.insert_lock(
            id,
            LockMeta {
                label,
                extra: extra.into().map(|d| d.to_compact_string()),
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

#[derive(Debug)]
pub struct VersionsSnapshot {
    actor_id: ActorId,
    needed: RangeInclusiveSet<CrsqlDbVersion>,
    partials: BTreeMap<CrsqlDbVersion, PartialVersion>,
    max: Option<CrsqlDbVersion>,
}

impl VersionsSnapshot {
    pub fn needed(&self) -> &RangeInclusiveSet<CrsqlDbVersion> {
        &self.needed
    }

    pub fn insert_gaps(&mut self, db_versions: RangeInclusiveSet<CrsqlDbVersion>) {
        self.needed.extend(db_versions);
    }

    pub fn insert_db(
        &mut self,         // only because we want 1 mt a time here
        conn: &Connection, // usually a `Transaction`
        db_versions: RangeInclusiveSet<CrsqlDbVersion>,
    ) -> rusqlite::Result<()> {
        trace!("wants to insert into db {db_versions:?}");
        let mut changes = self.compute_gaps_change(db_versions);

        trace!(actor_id = %self.actor_id, "delete: {:?}", changes.remove_ranges);
        trace!(actor_id = %self.actor_id, "new: {:?}", changes.insert_set);

        // those are actual ranges we had stored and will change, remove them from the DB
        for range in std::mem::take(&mut changes.remove_ranges) {
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
            let details = json!({"count": count, "range": range});
            assert_always!(count == 1, "ineffective deletion of gaps in-db", &details);
            for version in range.clone() {
                self.partials.remove(&version);
            }
            self.needed.remove(range);
        }

        for range in std::mem::take(&mut changes.insert_set) {
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
                let (actor_id, start, end) : (ActorId, CrsqlDbVersion, CrsqlDbVersion) = conn.query_row("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps WHERE actor_id = :actor_id AND start = :start", named_params! {
                    ":actor_id": self.actor_id,
                    ":start": range.start(),
                }, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;

                warn!("already had gaps entry! actor_id: {actor_id}, start: {start}, end: {end}");

                return Err(e);
            }
            self.needed.insert(range);
        }

        self.max = changes.max.take();

        Ok(())
    }

    fn compute_gaps_change(&self, versions: RangeInclusiveSet<CrsqlDbVersion>) -> GapsChanges {
        trace!("needed: {:?}", self.needed);

        let mut changes = GapsChanges {
            // set as the current max
            max: self.max,

            insert_set: Default::default(),
            remove_ranges: Default::default(),
        };

        for versions in versions.clone() {
            // only update the max if it's bigger
            changes.max = cmp::max(changes.max, Some(*versions.end()));

            // iterate all partially or fully overlapping changes
            for range in self.needed.overlapping(&versions) {
                trace!(actor_id = %self.actor_id, "overlapping: {range:?}");
                // insert the overlapping range in the set (collapses ajoining ranges)
                changes.insert_set.insert(range.clone());
                // remove the range, they'll be added back from the insert set ^
                changes.remove_ranges.insert(range.clone());
            }

            // check if there's a previous range with an end version = start version - 1
            if let Some(range) = self.needed.get(&CrsqlDbVersion(versions.start().0 - 1)) {
                trace!(actor_id = %self.actor_id, "got a start - 1: {range:?}");
                // insert the collapsible range
                changes.insert_set.insert(range.clone());
                // remove the collapsible range
                changes.remove_ranges.insert(range.clone());
            }

            // check if there's a next range with an start version = end version + 1
            if let Some(range) = self.needed.get(&CrsqlDbVersion(versions.end().0 + 1)) {
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
        }

        for versions in versions {
            // we now know the applied versions
            changes.insert_set.remove(versions.clone());
        }

        changes
    }
}

// this struct must be drained!
impl Drop for VersionsSnapshot {
    fn drop(&mut self) {
        trace!("dropping snapshot: {self:?}");
        if !self.needed.is_empty() {
            warn!(actor_id = %self.actor_id, "needed versions from snapshot were not drained: {:?}", self.needed);
        }
        debug_assert!(
            self.needed.is_empty(),
            "needed versions from snapshot were not drained"
        );
        if !self.partials.is_empty() {
            warn!(actor_id = %self.actor_id, "partials were not drained: {:?}", self.partials);
        }
        debug_assert!(
            self.partials.is_empty(),
            "partials from snapshot were not drained!"
        );
        debug_assert!(self.max.is_none(), "max value was not applied");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookedVersions {
    actor_id: ActorId,
    pub partials: BTreeMap<CrsqlDbVersion, PartialVersion>,
    needed: RangeInclusiveSet<CrsqlDbVersion>,
    max: Option<CrsqlDbVersion>,
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

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    pub fn from_conn(conn: &Connection, actor_id: ActorId) -> rusqlite::Result<Self> {
        trace!("from_conn");
        let mut bv = BookedVersions::new(actor_id);

        // fetch the biggest version we know, a partial version might override
        // this below
        bv.max = conn
            .prepare_cached("SELECT db_version FROM crsql_db_versions WHERE site_id = ?")?
            .query_row([actor_id], |row| row.get(0))
            .optional()?;

        {
            // fetch known partial sequences
            let mut prepped = conn.prepare_cached(
            "SELECT db_version, start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping WHERE site_id = ?",
            )?;
            let mut rows = prepped.query([actor_id])?;

            loop {
                let row = rows.next()?;
                match row {
                    None => break,
                    Some(row) => {
                        let version = row.get(0)?;
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
        }

        let mut snap = bv.snapshot();

        {
            // fetch the sync's needed version gaps
            let mut prepped = conn.prepare_cached(
                "SELECT start, end FROM __corro_bookkeeping_gaps WHERE actor_id = ?",
            )?;
            let mut rows = prepped.query([actor_id])?;

            loop {
                let row = rows.next()?;
                match row {
                    None => break,
                    Some(row) => {
                        let start_v = row.get(0)?;
                        let end_v = row.get(1)?;

                        // TODO: don't do this manually...
                        snap.needed.insert(start_v..=end_v);
                        // max for booked versions shouldn't come from gaps
                        if Some(end_v) > snap.max {
                            warn!(%actor_id, %start_v, %end_v, max = ?snap.max, "max for actor is less than gap");
                        }
                    }
                }
            }
        }

        bv.commit_snapshot(snap);

        Ok(bv)
    }

    pub fn contains_version(&self, version: &CrsqlDbVersion) -> bool {
        // corrosion knows about a version if...

        // it's not in the list of needed versions
        !self.needed.iter().any(|range| range.start() <= version && version <= range.end()) &&
        // and the last known version is bigger than the requested version
        self.max.unwrap_or_default() >= *version
        // we don't need to look at partials because if we have a partial
        // then it fulfills the previous conditions
    }

    pub fn get_partial(&self, version: &CrsqlDbVersion) -> Option<&PartialVersion> {
        self.partials.get(version)
    }

    pub fn contains(
        &self,
        version: CrsqlDbVersion,
        seqs: Option<&RangeInclusive<CrsqlSeq>>,
    ) -> bool {
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
        mut versions: RangeInclusive<CrsqlDbVersion>,
        seqs: Option<&RangeInclusive<CrsqlSeq>>,
    ) -> bool {
        versions.all(|version| self.contains(version, seqs))
    }

    pub fn last(&self) -> Option<CrsqlDbVersion> {
        self.max
    }

    pub fn commit_snapshot(&mut self, mut snap: VersionsSnapshot) {
        debug!("comitting snapshot");
        self.needed = std::mem::take(&mut snap.needed);
        self.partials = std::mem::take(&mut snap.partials);
        self.max = snap.max.take();
    }

    pub fn snapshot(&self) -> VersionsSnapshot {
        trace!("creating snapshot");
        VersionsSnapshot {
            actor_id: self.actor_id,
            needed: self.needed.clone(),
            partials: self.partials.clone(),
            max: self.max,
        }
    }

    // used when the commit has succeeded
    pub fn insert_partial(
        &mut self,
        version: CrsqlDbVersion,
        partial: PartialVersion,
    ) -> PartialVersion {
        debug!(actor_id = %self.actor_id, "insert partial {version:?}");

        match self.partials.entry(version) {
            btree_map::Entry::Vacant(entry) => {
                self.max = cmp::max(self.max, Some(version));
                entry.insert(partial).clone()
            }
            btree_map::Entry::Occupied(mut entry) => {
                let got = entry.get_mut();
                got.seqs.extend(partial.seqs);
                got.clone()
            }
        }
    }

    pub fn needed(&self) -> &RangeInclusiveSet<CrsqlDbVersion> {
        &self.needed
    }
}

#[derive(Debug)]
pub struct GapsChanges {
    max: Option<CrsqlDbVersion>,
    insert_set: RangeInclusiveSet<CrsqlDbVersion>,
    remove_ranges: HashSet<RangeInclusive<CrsqlDbVersion>>,
}

pub type BookedInner = Arc<CountedTokioRwLock<BookedVersions>>;

#[derive(Clone)]
pub struct Booked(BookedInner);

impl Booked {
    pub fn new(versions: BookedVersions, registry: LockRegistry) -> Self {
        Self(Arc::new(CountedTokioRwLock::new(registry, versions)))
    }

    pub async fn read<D: fmt::Display, E: Into<Option<D>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockReadGuard<'_, BookedVersions> {
        self.0.read(label, extra).await
    }

    pub async fn write<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockWriteGuard<'_, BookedVersions> {
        self.0.write(label, extra).await
    }

    pub async fn write_owned<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedOwnedTokioRwLockWriteGuard<BookedVersions> {
        self.0.write_owned(label, extra).await
    }

    pub fn blocking_write<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockWriteGuard<'_, BookedVersions> {
        self.0.blocking_write(label, extra)
    }

    pub fn blocking_read<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockReadGuard<'_, BookedVersions> {
        self.0.blocking_read(label, extra)
    }

    pub fn blocking_write_owned<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedOwnedTokioRwLockWriteGuard<BookedVersions> {
        self.0.blocking_write_owned(label, extra)
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

    pub async fn read<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockReadGuard<BookieInner> {
        self.0.read(label, extra).await
    }

    pub async fn write<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockWriteGuard<BookieInner> {
        self.0.write(label, extra).await
    }

    pub fn blocking_write<C: fmt::Display, E: Into<Option<C>>>(
        &self,
        label: &'static str,
        extra: E,
    ) -> CountedTokioRwLockWriteGuard<BookieInner> {
        self.0.blocking_write(label, extra)
    }

    pub fn registry(&self) -> &LockRegistry {
        self.0.registry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rangemap::range_inclusive_set;

    #[test]
    fn test_booked_insert_db() -> rusqlite::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let mut conn = CrConn::init(Connection::open_in_memory()?)?;
        setup_conn(&conn)?;
        let clock = Arc::new(uhlc::HLC::default());
        migrate(clock, &mut conn)?;

        let actor_id = ActorId::default();
        let mut bv = BookedVersions::new(actor_id);

        let mut all = RangeInclusiveSet::new();

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(1)..=CrsqlDbVersion(20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(1)..=CrsqlDbVersion(10)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // try from an empty state again
        let mut bv = BookedVersions::new(actor_id);
        let mut all = RangeInclusiveSet::new();

        // create 2:=3 gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![
                CrsqlDbVersion(1)..=CrsqlDbVersion(1),
                CrsqlDbVersion(4)..=CrsqlDbVersion(4)
            ],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(2)..=CrsqlDbVersion(3)],
        )?;

        // fill gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![
                CrsqlDbVersion(3)..=CrsqlDbVersion(3),
                CrsqlDbVersion(2)..=CrsqlDbVersion(2)
            ],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // try from an empty state again
        let mut bv = BookedVersions::new(actor_id);
        let mut all = RangeInclusiveSet::new();

        // insert a non-1 first version
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(5)..=CrsqlDbVersion(20)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(1)..=CrsqlDbVersion(4)],
        )?;

        // insert a further change that does not overlap a gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(6)..=CrsqlDbVersion(7)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(1)..=CrsqlDbVersion(4)],
        )?;

        // insert a further change that does overlap a gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(3)..=CrsqlDbVersion(7)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(1)..=CrsqlDbVersion(2)],
        )?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(1)..=CrsqlDbVersion(2)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(25)..=CrsqlDbVersion(25)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(21)..=CrsqlDbVersion(24)],
        )?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(30)..=CrsqlDbVersion(35)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![
                CrsqlDbVersion(21)..=CrsqlDbVersion(24),
                CrsqlDbVersion(26)..=CrsqlDbVersion(29),
            ],
        )?;

        // NOTE: overlapping partially from the end

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(19)..=CrsqlDbVersion(22)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![
                CrsqlDbVersion(23)..=CrsqlDbVersion(24),
                CrsqlDbVersion(26)..=CrsqlDbVersion(29),
            ],
        )?;

        // NOTE: overlapping partially from the start

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(24)..=CrsqlDbVersion(25)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![
                CrsqlDbVersion(23)..=CrsqlDbVersion(23),
                CrsqlDbVersion(26)..=CrsqlDbVersion(29),
            ],
        )?;

        // NOTE: overlapping 2 ranges

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(23)..=CrsqlDbVersion(27)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(28)..=CrsqlDbVersion(29)],
        )?;

        // NOTE: ineffective insert of already known ranges

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(1)..=CrsqlDbVersion(20)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![CrsqlDbVersion(28)..=CrsqlDbVersion(29)],
        )?;

        // NOTE: overlapping no ranges, but encompassing a full range

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(27)..=CrsqlDbVersion(30)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // NOTE: touching multiple ranges, partially

        // create gap 36..=39
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(40)..=CrsqlDbVersion(45)],
        )?;
        // create gap 46..=49
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(50)..=CrsqlDbVersion(55)],
        )?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![CrsqlDbVersion(38)..=CrsqlDbVersion(47)],
        )?;
        expect_gaps(
            &conn,
            &bv,
            &all,
            vec![
                CrsqlDbVersion(36)..=CrsqlDbVersion(37),
                CrsqlDbVersion(48)..=CrsqlDbVersion(49),
            ],
        )?;

        // test loading a bv from the conn, they should be identical!
        let mut bv2 = BookedVersions::from_conn(&conn, actor_id)?;
        // manually set the last version because there's nothing in `__corro_bookkeeping`
        bv2.max = Some(CrsqlDbVersion(55));

        assert_eq!(bv, bv2);

        Ok(())
    }

    fn insert_everywhere(
        conn: &Connection,
        bv: &mut BookedVersions,
        all_versions: &mut RangeInclusiveSet<CrsqlDbVersion>,
        versions: RangeInclusiveSet<CrsqlDbVersion>,
    ) -> rusqlite::Result<()> {
        all_versions.extend(versions.clone());
        let mut snap = bv.snapshot();
        snap.insert_db(conn, versions)?;
        bv.commit_snapshot(snap);
        Ok(())
    }

    fn expect_gaps(
        conn: &Connection,
        bv: &BookedVersions,
        all_versions: &RangeInclusiveSet<CrsqlDbVersion>,
        expected: Vec<RangeInclusive<CrsqlDbVersion>>,
    ) -> rusqlite::Result<()> {
        let gaps: Vec<(ActorId, CrsqlDbVersion, CrsqlDbVersion)> = conn
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
                assert!(bv.needed.contains(&v), "expected needed to contain {v:?}");
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
