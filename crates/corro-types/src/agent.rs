use std::{
    future::Future,
    io,
    net::SocketAddr,
    ops::{Deref, DerefMut, RangeInclusive},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use antithesis_sdk::assert_unreachable;
use arc_swap::ArcSwap;
use camino::Utf8PathBuf;
use metrics::{gauge, histogram};
use parking_lot::RwLock;
use rangemap::RangeInclusiveSet;
use rusqlite::{Connection, OpenFlags, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlite_pool::SqliteConn;
use tokio::{
    runtime::Handle,
    sync::{oneshot, Semaphore},
};
use tokio::{
    sync::{AcquireError, OwnedSemaphorePermit},
    time::timeout,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, warn};
use tripwire::Tripwire;

use crate::{
    actor::{Actor, ActorId, ClusterId, MemberId},
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq},
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput, Timestamp},
    channel::{bounded, CorroSender},
    config::Config,
    metrics_tracker::MetricsTracker,
    pubsub::SubsManager,
    schema::Schema,
    sqlite::{
        rusqlite_to_crsqlite, rusqlite_to_crsqlite_write, setup_conn, trace_heavy_queries, CrConn,
        Migration, SqlitePool, SqlitePoolError,
    },
    updates::UpdatesManager,
};

pub use crate::bookie::{Booked, BookedVersions, BookedWriteTx, Bookie, BookieWriteGuard};

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
    pub bookie: Bookie,

    pub tx_bcast: CorroSender<BroadcastInput>,
    pub tx_apply: CorroSender<(ActorId, CrsqlDbVersion)>,
    pub tx_clear_buf: CorroSender<(ActorId, CrsqlDbVersionRange)>,
    pub tx_changes: CorroSender<(ChangeV1, ChangeSource)>,
    pub tx_foca: CorroSender<FocaInput>,

    pub write_sema: Arc<Semaphore>,

    pub schema: RwLock<Schema>,
    pub cluster_id: ClusterId,

    pub subs_manager: SubsManager,

    pub updates_manager: UpdatesManager,

    pub metrics_tracker: MetricsTracker,

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
    metrics_tracker: MetricsTracker,
    clock: Arc<uhlc::HLC>,
    booked: Booked,
    bookie: Bookie,
    tx_bcast: CorroSender<BroadcastInput>,
    tx_apply: CorroSender<(ActorId, CrsqlDbVersion)>,
    tx_clear_buf: CorroSender<(ActorId, CrsqlDbVersionRange)>,
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
            metrics_tracker: config.metrics_tracker,
            clock: config.clock,
            booked: config.booked,
            bookie: config.bookie,
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

    pub fn actor<C: Into<Option<ClusterId>>, M: Into<Option<MemberId>>>(
        &self,
        cluster_id: C,
        member_id: M,
    ) -> Actor {
        Actor::new(
            self.0.actor_id,
            self.external_addr().unwrap_or_else(|| self.gossip_addr()),
            self.clock().new_timestamp().into(),
            cluster_id.into().unwrap_or_else(|| self.cluster_id()),
            member_id.into(),
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

    pub fn tx_clear_buf(&self) -> &CorroSender<(ActorId, CrsqlDbVersionRange)> {
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

    pub fn bookie(&self) -> &Bookie {
        &self.0.bookie
    }

    pub fn members(&self) -> &RwLock<Members> {
        &self.0.members
    }

    pub fn schema(&self) -> &RwLock<Schema> {
        &self.0.schema
    }

    pub fn metrics_tracker(&self) -> &MetricsTracker {
        &self.0.metrics_tracker
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

    pub fn member_id(&self) -> Option<MemberId> {
        self.0.config.load().gossip.member_id
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

// TODO: enable this down the line
// fn crsqlite_ts_index() -> impl Fn(&Transaction) -> rusqlite::Result<()> {
//     |tx: &Transaction| -> rusqlite::Result<()> {
//         let tables: Vec<String> = tx.prepare("SELECT tbl_name FROM sqlite_master WHERE type='table' AND tbl_name LIKE '%__crsql_clock'")?.query_map([], |row| row.get(0))?.collect::<rusqlite::Result<Vec<_>>>()?;

//         for table in tables {
//             tx.execute(
//                 &format!("CREATE INDEX IF NOT EXISTS {table}_ts ON {table} (ts)"),
//                 [],
//             )?;
//         }

//         Ok(())
//     }
// }

#[derive(Debug, Clone)]
pub struct SplitPool(Arc<SplitPoolInner>);

#[derive(Debug)]
struct SplitPoolInner {
    path: PathBuf,
    write_sema: Arc<Semaphore>,
    cache_size_kib: i64,

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
        cache_size_kib: i64,
    ) -> Result<Self, SplitPoolCreateError> {
        let rw_pool = sqlite_pool::Config::new(path.as_ref())
            .max_size(1)
            .create_pool_transform(move |conn| rusqlite_to_crsqlite_write(conn, cache_size_kib))?;

        debug!("built RW pool");

        let ro_pool = sqlite_pool::Config::new(path.as_ref())
            .read_only()
            .max_size(20)
            .create_pool_transform(rusqlite_to_crsqlite)?;
        debug!("built RO pool");

        Ok(Self::new(
            path.as_ref().to_owned(),
            write_sema,
            cache_size_kib,
            ro_pool,
            rw_pool,
        ))
    }

    fn new(
        path: PathBuf,
        write_sema: Arc<Semaphore>,
        cache_size_kib: i64,
        read: SqlitePool,
        write: SqlitePool,
    ) -> Self {
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
            cache_size_kib,
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

    pub fn db_path(&self) -> &Path {
        &self.0.path
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn dedicated(&self) -> rusqlite::Result<Connection> {
        let conn = rusqlite::Connection::open(&self.0.path)?;
        setup_conn(&conn)?;
        trace_heavy_queries(&conn)?;
        Ok(conn)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn client_dedicated(&self) -> rusqlite::Result<CrConn> {
        let conn = rusqlite::Connection::open(&self.0.path)?;
        let cr_conn = rusqlite_to_crsqlite_write(conn, self.0.cache_size_kib)?;
        trace_heavy_queries(cr_conn.conn())?;
        Ok(cr_conn)
    }

    #[tracing::instrument(skip(self), level = "debug")]
    pub fn client_dedicated_readonly(&self) -> rusqlite::Result<CrConn> {
        let conn = rusqlite::Connection::open_with_flags(
            &self.0.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let cr_conn = rusqlite_to_crsqlite(conn)?;
        trace_heavy_queries(cr_conn.conn())?;
        Ok(cr_conn)
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
        CrsqlSeq(0)..=self.last_seq
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
