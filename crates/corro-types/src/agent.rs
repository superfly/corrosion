use std::{
    cmp,
    collections::{btree_map, BTreeMap, HashMap, HashSet},
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
use papaya::{Guard, HashMap as PapayaHashMap};
use parking_lot::{ArcMutexGuard, Mutex, RawMutex, RwLock};
use rangemap::RangeInclusiveSet;
use rusqlite::{named_params, Connection, OpenFlags, OptionalExtension, Transaction};
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
use tracing::{debug, error, trace, warn};
use tripwire::Tripwire;

use crate::{
    actor::{Actor, ActorId, ClusterId, MemberId},
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq, CrsqlSeqRange},
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput, Timestamp},
    channel::{bounded, CorroSender},
    config::Config,
    metrics_tracker::MetricsTracker,
    pubsub::SubsManager,
    schema::Schema,
    sqlite::{
        rusqlite_to_crsqlite, rusqlite_to_crsqlite_write, setup_conn, trace_heavy_queries,
        unnest_param, CrConn, Migration, SqlitePool, SqlitePoolError,
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

impl BookedVersions {
    pub fn needed(&self) -> &RangeInclusiveSet<CrsqlDbVersion> {
        &self.needed
    }

    pub fn insert_gaps(&mut self, db_versions: RangeInclusiveSet<CrsqlDbVersion>) {
        self.needed.extend(db_versions);
    }

    pub fn insert_db(
        &mut self,
        conn: &Connection, // usually a `Transaction`
        db_versions: RangeInclusiveSet<CrsqlDbVersion>,
    ) -> rusqlite::Result<()> {
        trace!("wants to insert into db {db_versions:?}");
        let mut changes = self.compute_gaps_change(db_versions);

        trace!(actor_id = %self.actor_id, "delete: {:?}", changes.remove_ranges);
        trace!(actor_id = %self.actor_id, "new: {:?}", changes.insert_set);

        // those are actual ranges we had stored and will change, remove them from the DB
        {
            let remove_ranges = std::mem::take(&mut changes.remove_ranges);
            let actors = unnest_param(remove_ranges.iter().map(|_| self.actor_id));
            let starts = unnest_param(remove_ranges.iter().map(|r| r.start()));
            let ends = unnest_param(remove_ranges.iter().map(|r| r.end()));
            // TODO: use returning to discover which ranges were actually deleted
            let count = conn
                .prepare_cached(
                    "
                    DELETE FROM __corro_bookkeeping_gaps WHERE (actor_id, start, end) 
                    IN (SELECT value0, value1, value2 FROM unnest(:actors, :starts, :ends))
                    ",
                )?
                .execute(named_params! {
                    ":actors": actors,
                    ":starts": starts,
                    ":ends": ends,
                })?;
            if count != remove_ranges.len() {
                warn!(actor_id = %self.actor_id, "did not delete some gaps from db: {remove_ranges:?}");
                let details: serde_json::Value = json!({"count": count, "ranges": remove_ranges});
                assert_unreachable!("ineffective deletion of gaps in-db", &details);
            }

            for range in remove_ranges {
                for version in CrsqlDbVersionRange::from(&range) {
                    self.partials.remove(&version);
                }
                self.needed.remove(range);
            }
        }

        {
            let insert_set = std::mem::take(&mut changes.insert_set);
            let actors = unnest_param(insert_set.iter().map(|_| self.actor_id));
            let starts = unnest_param(insert_set.iter().map(|r| r.start()));
            let ends = unnest_param(insert_set.iter().map(|r| r.end()));
            debug!(actor_id = %self.actor_id, "inserting {insert_set:?}");
            // TODO: use returning to discover which ranges were actually inserted
            let count = conn
                .prepare_cached(
                    "
                    INSERT OR IGNORE INTO __corro_bookkeeping_gaps (actor_id, start, end) 
                    SELECT value0, value1, value2 FROM unnest(:actors, :starts, :ends)
                    ",
                )?
                .execute(named_params! {
                    ":actors": actors,
                    ":starts": starts,
                    ":ends": ends,
                })?;
            if count != insert_set.len() {
                warn!(actor_id = %self.actor_id, "did not insert some gaps into db: {insert_set:?}");

                let existing: Vec<(ActorId, CrsqlDbVersion, CrsqlDbVersion)> = conn
                    .prepare_cached(
                        "
                    SELECT actor_id, start, end FROM __corro_bookkeeping_gaps 
                    WHERE (actor_id, start) 
                    IN (SELECT value0, value1 FROM unnest(:actors, :starts))",
                    )?
                    .query_map(
                        named_params! {
                            ":actors": actors,
                            ":starts": starts,
                        },
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;

                warn!("already had gaps entries! existing: {existing:?}");
                let details: serde_json::Value =
                    json!({"count": count, "insert_set": insert_set, "existing": existing});
                assert_unreachable!("ineffective insertion of gaps in-db", &details);
                return Err(rusqlite::Error::ModuleError(
                    "Gaps entries already present in DB".to_string(),
                ));
            }

            for range in insert_set {
                self.needed.insert(range);
            }
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
                        bv.needed.insert(start_v..=end_v);
                        // max for booked versions shouldn't come from gaps
                        if Some(end_v) > bv.max {
                            warn!(%actor_id, %start_v, %end_v, max = ?bv.max, "max for actor is less than gap");
                        }
                    }
                }
            }
        }

        Ok(bv)
    }

    pub fn load_all_from_conn(conn: &Connection) -> rusqlite::Result<HashMap<ActorId, Self>> {
        let mut map: HashMap<ActorId, Self> = HashMap::new();

        // Collect all known site_ids: remote actors we have applied changes from,
        // plus any we have buffered partial changes for, plus ourselves.
        {
            let mut prepped = conn.prepare(
                "SELECT site_id FROM crsql_site_id WHERE ordinal > 0
                        UNION
                    SELECT DISTINCT site_id FROM __corro_seq_bookkeeping
                        UNION 
                    SELECT DISTINCT actor_id FROM __corro_bookkeeping_gaps",
            )?;
            let rows = prepped.query_map([], |row| row.get(0))?;
            for actor_id in rows {
                let actor_id: ActorId = actor_id?;
                map.entry(actor_id).or_insert_with(|| Self::new(actor_id));
            }
        }

        // Populate max from crsql_db_versions
        {
            let mut prepped = conn.prepare("SELECT site_id, db_version FROM crsql_db_versions")?;
            let rows = prepped.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            for row in rows {
                let (actor_id, db_version): (ActorId, CrsqlDbVersion) = row?;
                if let Some(bv) = map.get_mut(&actor_id) {
                    bv.max = Some(db_version);
                }
            }
        }

        // Populate partials from __corro_seq_bookkeeping
        {
            let mut prepped = conn.prepare(
                "SELECT site_id, db_version, start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping",
            )?;
            let rows = prepped.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?;
            for row in rows {
                let (actor_id, version, start_seq, end_seq, last_seq, ts): (
                    ActorId,
                    CrsqlDbVersion,
                    _,
                    _,
                    _,
                    _,
                ) = row?;
                if let Some(bv) = map.get_mut(&actor_id) {
                    bv.insert_partial(
                        version,
                        PartialVersion {
                            seqs: RangeInclusiveSet::from_iter(vec![start_seq..=end_seq]),
                            last_seq,
                            ts,
                        },
                    );
                }
            }
        }

        // Populate needed gaps from __corro_bookkeeping_gaps
        {
            let mut prepped =
                conn.prepare("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps")?;
            let rows = prepped.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            for row in rows {
                let (actor_id, start_v, end_v): (ActorId, CrsqlDbVersion, CrsqlDbVersion) = row?;
                if let Some(bv) = map.get_mut(&actor_id) {
                    bv.needed.insert(start_v..=end_v);
                    if Some(end_v) > bv.max {
                        warn!(%actor_id, %start_v, %end_v, max = ?bv.max, "max for actor is less than gap");
                    }
                }
            }
        }

        Ok(map)
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

    pub fn contains(&self, version: CrsqlDbVersion, seqs: Option<CrsqlSeqRange>) -> bool {
        self.contains_version(&version)
            && seqs
                .map(|mut check_seqs| match self.partials.get(&version) {
                    Some(partial) => check_seqs.all(|seq| partial.seqs.contains(&seq)),
                    // if `contains_version` is true but we don't have a partial version,
                    // then we must have it as a fully applied or cleared version
                    None => true,
                })
                .unwrap_or(true)
    }

    #[inline]
    pub fn contains_all(
        &self,
        versions: impl Into<CrsqlDbVersionRange>,
        seqs: Option<CrsqlSeqRange>,
    ) -> bool {
        versions.into().all(|version| self.contains(version, seqs))
    }

    pub fn last(&self) -> Option<CrsqlDbVersion> {
        self.max
    }

    // Called for each partial changeset which has been successfully buffered in the database.
    // Right now db reconciliation is done when inserting a partial changeset.
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
}

#[derive(Debug)]
pub struct GapsChanges {
    max: Option<CrsqlDbVersion>,
    insert_set: RangeInclusiveSet<CrsqlDbVersion>,
    remove_ranges: HashSet<RangeInclusive<CrsqlDbVersion>>,
}

#[derive(Clone)]
pub struct Booked {
    versions: Arc<ArcSwap<BookedVersions>>,
}

pub struct BookedWriteTx {
    versions: Arc<ArcSwap<BookedVersions>>,
    working: BookedVersions,
}

impl BookedWriteTx {
    // Publishes the working copy into ArcSwap so it's visible by readers.
    // The external BookieWriteGuard controls lock lifetime.
    pub fn commit(self) {
        self.versions.store(Arc::new(self.working));
    }
}

impl Deref for BookedWriteTx {
    type Target = BookedVersions;

    fn deref(&self) -> &Self::Target {
        &self.working
    }
}

impl DerefMut for BookedWriteTx {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.working
    }
}

pub struct BookieWriteGuard {
    _writer_guard: ArcMutexGuard<RawMutex, ()>,
}

impl BookieWriteGuard {
    fn new(writer_guard: ArcMutexGuard<RawMutex, ()>) -> BookieWriteGuard {
        BookieWriteGuard {
            _writer_guard: writer_guard,
        }
    }

    pub fn write_tx(&self, booked: &Booked) -> BookedWriteTx {
        booked.write_tx()
    }
}

impl Booked {
    pub fn new(versions: BookedVersions) -> Self {
        Self {
            versions: Arc::new(ArcSwap::from_pointee(versions)),
        }
    }

    pub fn read(
        &self,
    ) -> arc_swap::Guard<Arc<BookedVersions>, arc_swap::strategy::DefaultStrategy> {
        self.versions.load()
    }

    fn write_tx(&self) -> BookedWriteTx {
        let current = self.versions.load_full();

        BookedWriteTx {
            versions: self.versions.clone(),
            working: (*current).clone(),
        }
    }
}

#[derive(Clone)]
pub struct Bookie {
    map: Arc<PapayaHashMap<ActorId, Booked>>,
    writer: Arc<Mutex<()>>,
}

impl Bookie {
    // WARNING: this method blocks the current thread while waiting for the lock.
    // In async contexts this can starve runtime workers if used outside block_in_place/spawn_blocking.
    pub fn write_lock_blocking(&self) -> BookieWriteGuard {
        BookieWriteGuard::new(self.writer.lock_arc())
    }

    pub fn new(map: HashMap<ActorId, BookedVersions>) -> Self {
        let papaya_map = PapayaHashMap::new();
        {
            let pinned_map = papaya_map.pin();
            for (actor_id, booked_versions) in map {
                pinned_map.insert(actor_id, Booked::new(booked_versions));
            }
        }

        Self {
            map: Arc::new(papaya_map),
            writer: Arc::new(Mutex::new(())),
        }
    }

    pub fn get(&self, actor_id: &ActorId) -> Option<Booked> {
        self.map.pin().get(actor_id).cloned()
    }

    pub fn owned_guard(&self) -> impl Guard + '_ {
        self.map.owned_guard()
    }

    pub fn iter<'guard>(
        &self,
        guard: &'guard impl Guard,
    ) -> impl Iterator<Item = (&'guard ActorId, &'guard Booked)> {
        self.map.iter(guard)
    }

    pub fn ensure(&self, actor_id: ActorId) -> Booked {
        self.map
            .pin()
            .get_or_insert(actor_id, Booked::new(BookedVersions::new(actor_id)))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::{dbvr, dbvri};
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
            range_inclusive_set![dbvri!(1, 20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(1, 10)],
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
            range_inclusive_set![dbvri!(1, 1), dbvri!(4, 4)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(2, 3)])?;

        // fill gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(3, 3), dbvri!(2, 2)],
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
            range_inclusive_set![dbvri!(5, 20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(1, 4)])?;

        // insert a further change that does not overlap a gap
        insert_everywhere(&conn, &mut bv, &mut all, range_inclusive_set![dbvri!(6, 7)])?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(1, 4)])?;

        // insert a further change that does overlap a gap
        insert_everywhere(&conn, &mut bv, &mut all, range_inclusive_set![dbvri!(3, 7)])?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(1, 2)])?;

        insert_everywhere(&conn, &mut bv, &mut all, range_inclusive_set![dbvri!(1, 2)])?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(25, 25)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(21, 24)])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(30, 35)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(21, 24), dbvr!(26, 29)])?;

        // NOTE: overlapping partially from the end

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(19, 22)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(23, 24), dbvr!(26, 29)])?;

        // NOTE: overlapping partially from the start

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(24, 25)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(23, 23), dbvr!(26, 29)])?;

        // NOTE: overlapping 2 ranges

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(23, 27)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(28, 29)])?;

        // NOTE: ineffective insert of already known ranges

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(1, 20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(28, 29)])?;

        // NOTE: overlapping no ranges, but encompassing a full range

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(27, 30)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // NOTE: touching multiple ranges, partially

        // create gap 36..=39
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(40, 45)],
        )?;
        // create gap 46..=49
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(50, 55)],
        )?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(38, 47)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(36, 37), dbvr!(48, 49)])?;

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
        bv.insert_db(conn, versions)
    }

    fn expect_gaps(
        conn: &Connection,
        bv: &BookedVersions,
        all_versions: &RangeInclusiveSet<CrsqlDbVersion>,
        expected: Vec<CrsqlDbVersionRange>,
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
                .map(|expected| (bv.actor_id, expected.start(), expected.end()))
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
            all_versions.iter().next_back().map(|range| *range.end()),
            "expected last version not to increment"
        );

        Ok(())
    }
}
