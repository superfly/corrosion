use std::{
    collections::{BTreeMap, HashMap},
    io,
    net::SocketAddr,
    ops::{Deref, DerefMut, RangeInclusive},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use bb8::PooledConnection;
use camino::Utf8PathBuf;
use metrics::{gauge, histogram, increment_counter};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rangemap::{RangeInclusiveMap, RangeInclusiveSet};
use rusqlite::{Connection, InterruptHandle};
use tempfile::TempDir;
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{channel, Sender},
        oneshot,
    },
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, info, trace, warn};
use tripwire::Tripwire;

use crate::{
    actor::ActorId,
    broadcast::{BroadcastInput, Timestamp},
    config::Config,
    pubsub::Matcher,
    schema::NormalizedSchema,
    sqlite::{CrConnManager, RusqliteConnManager, SqlitePool, SqlitePoolError},
};

use super::members::Members;

pub type Matchers = HashMap<uuid::Uuid, Matcher>;

#[derive(Clone)]
pub struct Agent(Arc<AgentInner>);

pub struct AgentConfig {
    pub actor_id: ActorId,
    pub pool: SplitPool,
    pub config: ArcSwap<Config>,
    pub gossip_addr: SocketAddr,
    pub api_addr: SocketAddr,
    pub members: RwLock<Members>,
    pub clock: Arc<uhlc::HLC>,
    pub bookie: Bookie,
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
    matchers: RwLock<Matchers>,
    tx_bcast: Sender<BroadcastInput>,
    tx_apply: Sender<(ActorId, i64)>,
    schema: RwLock<NormalizedSchema>,
}

impl Agent {
    pub fn new(config: AgentConfig) -> Self {
        Self(Arc::new(AgentInner {
            actor_id: config.actor_id,
            pool: config.pool,
            config: config.config,
            gossip_addr: config.gossip_addr,
            api_addr: config.api_addr,
            members: config.members,
            clock: config.clock,
            bookie: config.bookie,
            matchers: RwLock::new(HashMap::new()),
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

    pub fn matchers(&self) -> &RwLock<Matchers> {
        &self.0.matchers
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
}

#[derive(Debug, Clone)]
pub struct SplitPool(Arc<SplitPoolInner>);

#[derive(Debug)]
struct SplitPoolInner {
    read: SqlitePool,
    write: SqlitePool,

    dedicated_pool: bb8::Pool<RusqliteConnManager>,

    // need to keep this for the life of the pool!
    #[allow(dead_code)]
    tmp_db_dir: TempDir,

    priority_tx: Sender<oneshot::Sender<CancellationToken>>,
    normal_tx: Sender<oneshot::Sender<CancellationToken>>,
    low_tx: Sender<oneshot::Sender<CancellationToken>>,
}

#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
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

#[derive(Debug, thiserror::Error)]
pub enum SplitPoolCreateError {
    #[error(transparent)]
    Pool(#[from] crate::sqlite::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl SplitPool {
    pub async fn create<P: AsRef<Path>>(
        path: P,
        tripwire: Tripwire,
    ) -> Result<Self, SplitPoolCreateError> {
        let rw_pool = bb8::Builder::new()
            .max_size(1)
            .min_idle(Some(1)) // create one right away and keep it idle
            .build(CrConnManager::new(path.as_ref()))
            .await?;

        debug!("built RW pool");

        let ro_pool = bb8::Builder::new()
            .max_size(10)
            .min_idle(Some(5)) // keep a few idling
            .max_lifetime(Some(Duration::from_secs(30)))
            .build(CrConnManager::new_read_only(path.as_ref()))
            .await?;
        debug!("built RO pool");

        let tmp_db_dir = match path.as_ref().parent() {
            Some(parent) => tempfile::tempdir_in(parent)?,
            None => tempfile::tempdir()?,
        };

        let dedicated_pool = bb8::Pool::builder()
            .max_size(100)
            .build(RusqliteConnManager::new(path.as_ref()).attach(
                tmp_db_dir.path().join("watches.db").display().to_string(),
                "watches",
            ))
            .await?;

        Ok(Self::new(
            ro_pool,
            rw_pool,
            dedicated_pool,
            tmp_db_dir,
            tripwire,
        ))
    }

    pub fn new(
        read: SqlitePool,
        write: SqlitePool,
        dedicated_pool: bb8::Pool<RusqliteConnManager>,
        tmp_db_dir: TempDir,
        mut tripwire: Tripwire,
    ) -> Self {
        let (priority_tx, mut priority_rx) = channel(256);
        let (normal_tx, mut normal_rx) = channel(512);
        let (low_tx, mut low_rx) = channel(1024);

        tokio::spawn(async move {
            loop {
                let tx: oneshot::Sender<CancellationToken> = tokio::select! {
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

            info!("write loop done, draining...");

            // keep processing priority messages
            // NOTE: using `recv` would wait indefinitely, this loop only waits until all
            //       current conn requests are done
            while let Ok(tx) = priority_rx.try_recv() {
                wait_conn_drop(tx).await
            }
        });

        Self(Arc::new(SplitPoolInner {
            read,
            write,
            dedicated_pool,
            tmp_db_dir,
            priority_tx,
            normal_tx,
            low_tx,
        }))
    }

    pub fn emit_metrics(&self) {
        let read_state = self.0.read.state();
        gauge!(
            "corro.sqlite.pool.read.connections",
            read_state.connections as f64
        );
        gauge!(
            "corro.sqlite.pool.read.connections.idle",
            read_state.idle_connections as f64
        );

        let write_state = self.0.write.state();
        gauge!(
            "corro.sqlite.pool.write.connections",
            write_state.connections as f64
        );
        gauge!(
            "corro.sqlite.pool.write.connections.idle",
            write_state.idle_connections as f64
        );
    }

    // get a read-only connection
    pub async fn read(&self) -> Result<PooledConnection<CrConnManager>, SqlitePoolError> {
        self.0.read.get().await
    }

    pub fn read_blocking(&self) -> Result<PooledConnection<CrConnManager>, SqlitePoolError> {
        Handle::current().block_on(self.0.read.get_owned())
    }

    pub async fn dedicated(&self) -> Result<Connection, crate::sqlite::Error> {
        self.0.dedicated_pool.dedicated_connection().await
    }

    pub fn dedicated_pool(&self) -> &bb8::Pool<RusqliteConnManager> {
        &self.0.dedicated_pool
    }

    // get a high priority write connection (e.g. client input)
    pub async fn write_priority(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.priority_tx, "priority").await
    }

    // get a normal priority write connection (e.g. sync process)
    pub async fn write_normal(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.normal_tx, "normal").await
    }

    // get a low priority write connection (e.g. background tasks)
    pub async fn write_low(&self) -> Result<WriteConn, PoolError> {
        self.write_inner(&self.0.low_tx, "low").await
    }

    pub fn blocking_write_low(&self) -> Result<WriteConn<'static>, PoolError> {
        Handle::current().block_on(self.write_inner_owned(&self.0.low_tx, "priority"))
    }

    async fn write_inner(
        &self,
        chan: &Sender<oneshot::Sender<CancellationToken>>,
        queue: &'static str,
    ) -> Result<WriteConn, PoolError> {
        let (tx, rx) = oneshot::channel();
        chan.send(tx).await.map_err(|_| PoolError::QueueClosed)?;
        let start = Instant::now();
        let token = rx.await.map_err(|_| PoolError::CallbackClosed)?;
        histogram!("corro.sqlite.pool.queue.seconds", start.elapsed().as_secs_f64(), "queue" => queue);
        let conn = self.0.write.get().await?;

        tokio::spawn(timeout_wait(
            token.clone(),
            conn.get_interrupt_handle(),
            Duration::from_secs(30),
            queue,
        ));

        Ok(WriteConn {
            conn,
            _drop_guard: token.drop_guard(),
        })
    }

    async fn write_inner_owned(
        &self,
        chan: &Sender<oneshot::Sender<CancellationToken>>,
        queue: &'static str,
    ) -> Result<WriteConn<'static>, PoolError> {
        let (tx, rx) = oneshot::channel();
        chan.send(tx).await.map_err(|_| PoolError::QueueClosed)?;
        let start = Instant::now();
        let token = rx.await.map_err(|_| PoolError::CallbackClosed)?;
        histogram!("corro.sqlite.pool.queue.seconds", start.elapsed().as_secs_f64(), "queue" => queue);
        let conn = self.0.write.get_owned().await?;

        tokio::spawn(timeout_wait(
            token.clone(),
            conn.get_interrupt_handle(),
            Duration::from_secs(30),
            queue,
        ));

        Ok(WriteConn {
            conn,
            _drop_guard: token.drop_guard(),
        })
    }
}

async fn timeout_wait(
    token: CancellationToken,
    handle: InterruptHandle,
    timeout: Duration,
    queue: &'static str,
) {
    let start = Instant::now();
    tokio::select! {
        biased;
        _ = token.cancelled() => {
            trace!("conn dropped before timeout");
            histogram!("corro.sqlite.pool.execution.seconds", start.elapsed().as_secs_f64(), "queue" => queue);
            return;
        },
        _ = tokio::time::sleep(timeout) => {
            warn!("conn execution timed out, interrupting!");
        }
    }
    handle.interrupt();
    increment_counter!("corro.sqlite.pool.execution.timeout");
    // FIXME: do we need to cancel the token?
}

async fn wait_conn_drop(tx: oneshot::Sender<CancellationToken>) {
    let cancel = CancellationToken::new();

    if let Err(_e) = tx.send(cancel.clone()) {
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

#[derive(Debug, Clone, Eq, PartialEq)]
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

    pub fn read(&self) -> BookReader {
        BookReader(self.0.read())
    }

    pub fn write(&self) -> BookWriter {
        BookWriter(self.0.write())
    }
}

pub struct BookReader<'a>(RwLockReadGuard<'a, BookedVersions>);

impl<'a> BookReader<'a> {
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
}

impl<'a> Deref for BookReader<'a> {
    type Target = RwLockReadGuard<'a, BookedVersions>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct BookWriter<'a>(RwLockWriteGuard<'a, BookedVersions>);

impl<'a> BookWriter<'a> {
    pub fn insert(&mut self, version: i64, known_version: KnownDbVersion) {
        self.insert_many(version..=version, known_version);
    }

    pub fn insert_many(&mut self, versions: RangeInclusive<i64>, known_version: KnownDbVersion) {
        self.0.insert(versions, known_version);
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

    pub fn contains_all(
        &self,
        mut versions: RangeInclusive<i64>,
        seqs: Option<&RangeInclusive<i64>>,
    ) -> bool {
        versions.all(|version| self.contains(version, seqs))
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

    pub fn inner_mut(&mut self) -> &mut RwLockWriteGuard<'a, BookedVersions> {
        &mut self.0
    }
}

impl<'a> Deref for BookWriter<'a> {
    type Target = RwLockWriteGuard<'a, BookedVersions>;

    fn deref(&self) -> &Self::Target {
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
        mut versions: RangeInclusive<i64>,
        seqs: Option<&RangeInclusive<i64>>,
    ) -> bool {
        self.0
            .read()
            .get(actor_id)
            .map(|booked| {
                let read = booked.read();
                versions.all(|v| read.contains(v, seqs))
            })
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
