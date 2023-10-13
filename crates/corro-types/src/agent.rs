use std::{
    collections::{BTreeMap, HashMap},
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
use rusqlite::{Connection, InterruptHandle};
use serde::{Deserialize, Serialize};
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{channel, Sender},
        oneshot, Semaphore,
    },
};
use tokio::{
    sync::{
        OwnedRwLockWriteGuard as OwnedTokioRwLockWriteGuard, RwLock as TokioRwLock,
        RwLockReadGuard as TokioRwLockReadGuard, RwLockWriteGuard as TokioRwLockWriteGuard,
    },
    task::block_in_place,
};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, info};
use tripwire::Tripwire;

use crate::{
    actor::ActorId,
    broadcast::{BroadcastInput, ChangeSource, ChangeV1, FocaInput, Timestamp},
    config::Config,
    pubsub::MatcherHandle,
    schema::NormalizedSchema,
    sqlite::{rusqlite_to_crsqlite, setup_conn, AttachMap, CrConn, SqlitePool, SqlitePoolError},
};

use super::members::Members;

pub type Subs = BTreeMap<uuid::Uuid, MatcherHandle>;

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
    pub tx_empty: Sender<(ActorId, RangeInclusive<i64>)>,
    pub tx_changes: Sender<(ChangeV1, ChangeSource)>,
    pub tx_foca: Sender<FocaInput>,

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
    subs: RwLock<Subs>,
    tx_bcast: Sender<BroadcastInput>,
    tx_apply: Sender<(ActorId, i64)>,
    tx_empty: Sender<(ActorId, RangeInclusive<i64>)>,
    tx_changes: Sender<(ChangeV1, ChangeSource)>,
    tx_foca: Sender<FocaInput>,
    schema: RwLock<NormalizedSchema>,
    limits: Limits,
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub sync: Arc<Semaphore>,
}

impl Agent {
    pub fn new_w_subs(config: AgentConfig, subs: Subs) -> Self {
        Self(Arc::new(AgentInner {
            actor_id: config.actor_id,
            pool: config.pool,
            config: config.config,
            gossip_addr: config.gossip_addr,
            api_addr: config.api_addr,
            members: config.members,
            clock: config.clock,
            bookie: config.bookie,
            subs: RwLock::new(subs),
            tx_bcast: config.tx_bcast,
            tx_apply: config.tx_apply,
            tx_empty: config.tx_empty,
            tx_changes: config.tx_changes,
            tx_foca: config.tx_foca,
            schema: config.schema,
            limits: Limits {
                sync: Arc::new(Semaphore::new(3)),
            },
        }))
    }

    pub fn new(config: AgentConfig) -> Self {
        Self::new_w_subs(config, Default::default())
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

    pub fn tx_changes(&self) -> &Sender<(ChangeV1, ChangeSource)> {
        &self.0.tx_changes
    }

    pub fn tx_empty(&self) -> &Sender<(ActorId, RangeInclusive<i64>)> {
        &self.0.tx_empty
    }

    pub fn tx_foca(&self) -> &Sender<FocaInput> {
        &self.0.tx_foca
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

    pub fn matchers(&self) -> &RwLock<Subs> {
        &self.0.subs
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
}

#[derive(Debug, Clone)]
pub struct SplitPool(Arc<SplitPoolInner>);

#[derive(Debug)]
struct SplitPoolInner {
    path: PathBuf,
    attachments: HashMap<Utf8PathBuf, compact_str::CompactString>,

    read: SqlitePool,
    write: SqlitePool,

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
    Pool(#[from] sqlite_pool::CreatePoolError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
}

impl SplitPool {
    pub async fn create<P: AsRef<Path>, P2: AsRef<Path>>(
        path: P,
        subscriptions_path: P2,
        tripwire: Tripwire,
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
            vec![(
                subscriptions_path.as_ref().display().to_string().into(),
                "subscriptions".into(),
            )]
            .into_iter()
            .collect(),
            ro_pool,
            rw_pool,
            tripwire,
        ))
    }

    fn new(
        path: PathBuf,
        attachments: AttachMap,
        read: SqlitePool,
        write: SqlitePool,
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
            path,
            attachments,
            read,
            write,
            priority_tx,
            normal_tx,
            low_tx,
        }))
    }

    pub fn emit_metrics(&self) {
        let read_state = self.0.read.status();
        gauge!("corro.sqlite.pool.read.connections", read_state.size as f64);
        gauge!(
            "corro.sqlite.pool.read.connections.available",
            read_state.available as f64
        );
        gauge!(
            "corro.sqlite.pool.read.connections.waiting",
            read_state.waiting as f64
        );

        let write_state = self.0.write.status();
        gauge!(
            "corro.sqlite.pool.write.connections",
            write_state.size as f64
        );
        gauge!(
            "corro.sqlite.pool.write.connections.available",
            write_state.available as f64
        );
        gauge!(
            "corro.sqlite.pool.write.connections.waiting",
            write_state.waiting as f64
        );
    }

    // get a read-only connection
    pub async fn read(&self) -> Result<sqlite_pool::Connection<CrConn>, SqlitePoolError> {
        self.0.read.get().await
    }

    pub fn read_blocking(&self) -> Result<sqlite_pool::Connection<CrConn>, SqlitePoolError> {
        Handle::current().block_on(self.0.read.get())
    }

    pub async fn dedicated(&self) -> rusqlite::Result<Connection> {
        block_in_place(|| {
            let mut conn = rusqlite::Connection::open(&self.0.path)?;
            setup_conn(&mut conn, &self.0.attachments)?;
            Ok(conn)
        })
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
}

async fn timeout_wait(
    token: CancellationToken,
    _handle: InterruptHandle,
    _timeout: Duration,
    queue: &'static str,
) {
    let start = Instant::now();
    token.cancelled().await;
    histogram!("corro.sqlite.pool.execution.seconds", start.elapsed().as_secs_f64(), "queue" => queue);
    // tokio::select! {
    //     biased;
    //     _ = token.cancelled() => {
    //         trace!("conn dropped before timeout");
    //         histogram!("corro.sqlite.pool.execution.seconds", start.elapsed().as_secs_f64(), "queue" => queue);
    //         return;
    //     },
    //     _ = tokio::time::sleep(timeout) => {
    //         warn!("conn execution timed out, interrupting!");
    //     }
    // }
    // handle.interrupt();
    // increment_tracker!("corro.sqlite.pool.execution.timeout");
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

pub struct WriteConn {
    conn: sqlite_pool::Connection<CrConn>,
    _drop_guard: DropGuard,
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum KnownDbVersion {
    Partial {
        // range of sequences recorded
        seqs: RangeInclusiveSet<i64>,
        // actual last sequence originally produced
        last_seq: i64,
        // timestamp when the change was produced by the source
        ts: Timestamp,
    },
    Current {
        // cr-sqlite db version
        db_version: i64,
        // actual last sequence originally produced
        last_seq: i64,
        // timestamp when the change was produced by the source
        ts: Timestamp,
    },
    Cleared,
}

impl KnownDbVersion {
    pub fn is_cleared(&self) -> bool {
        matches!(self, KnownDbVersion::Cleared)
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

    pub async fn write<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockWriteGuard<'_, T> {
        self.registry.acquire_write(label, &self.lock).await
    }

    pub fn blocking_write<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockWriteGuard<'_, T> {
        self.registry.acquire_blocking_write(label, &self.lock)
    }

    pub fn blocking_write_owned<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedOwnedTokioRwLockWriteGuard<T> {
        self.registry
            .acquire_blocking_write_owned(label, self.lock.clone())
    }

    pub fn blocking_read<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockReadGuard<'_, T> {
        self.registry.acquire_blocking_read(label, &self.lock)
    }

    pub async fn read<C: Into<CompactString>>(
        &self,
        label: C,
    ) -> CountedTokioRwLockReadGuard<'_, T> {
        self.registry.acquire_read(label, &self.lock).await
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CurrentVersion {
    // cr-sqlite db version
    pub db_version: i64,
    // actual last sequence originally produced
    pub last_seq: i64,
    // timestamp when the change was produced by the source
    pub ts: Timestamp,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PartialVersion {
    // range of sequences recorded
    pub seqs: RangeInclusiveSet<i64>,
    // actual last sequence originally produced
    pub last_seq: i64,
    // timestamp when the change was produced by the source
    pub ts: Timestamp,
}

impl From<PartialVersion> for KnownDbVersion {
    fn from(PartialVersion { seqs, last_seq, ts }: PartialVersion) -> Self {
        KnownDbVersion::Partial { seqs, last_seq, ts }
    }
}

#[derive(Debug)]
pub enum KnownVersion<'a> {
    Cleared,
    Current(&'a CurrentVersion),
    Partial(&'a PartialVersion),
}

impl<'a> KnownVersion<'a> {
    pub fn is_cleared(&self) -> bool {
        matches!(self, KnownVersion::Cleared)
    }
}

impl<'a> From<KnownVersion<'a>> for KnownDbVersion {
    fn from(value: KnownVersion<'a>) -> Self {
        match value {
            KnownVersion::Cleared => KnownDbVersion::Cleared,
            KnownVersion::Current(CurrentVersion {
                db_version,
                last_seq,
                ts,
            }) => KnownDbVersion::Current {
                db_version: *db_version,
                last_seq: *last_seq,
                ts: *ts,
            },
            KnownVersion::Partial(PartialVersion { seqs, last_seq, ts }) => {
                KnownDbVersion::Partial {
                    seqs: seqs.clone(),
                    last_seq: *last_seq,
                    ts: *ts,
                }
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct BookedVersions {
    pub cleared: RangeInclusiveSet<i64>,
    pub current: BTreeMap<i64, CurrentVersion>,
    pub partials: BTreeMap<i64, PartialVersion>,
}

impl BookedVersions {
    pub fn contains_version(&self, version: &i64) -> bool {
        self.cleared.contains(version)
            || self.current.contains_key(version)
            || self.partials.contains_key(version)
    }

    pub fn get(&self, version: &i64) -> Option<KnownVersion> {
        self.cleared
            .get(version)
            .map(|_| KnownVersion::Cleared)
            .or_else(|| self.current.get(version).map(KnownVersion::Current))
            .or_else(|| self.partials.get(version).map(KnownVersion::Partial))
    }

    pub fn contains(&self, version: i64, seqs: Option<&RangeInclusive<i64>>) -> bool {
        self.contains_version(&version)
            && seqs
                .map(|check_seqs| match self.get(&version) {
                    Some(KnownVersion::Cleared) | Some(KnownVersion::Current(_)) => true,
                    Some(KnownVersion::Partial(partial)) => {
                        check_seqs.clone().all(|seq| partial.seqs.contains(&seq))
                    }
                    None => false,
                })
                .unwrap_or(true)
    }

    pub fn contains_all(
        &self,
        mut versions: RangeInclusive<i64>,
        seqs: Option<&RangeInclusive<i64>>,
    ) -> bool {
        versions.all(|version| self.contains(version, seqs))
    }

    pub fn contains_current(&self, version: &i64) -> bool {
        self.current.contains_key(version)
    }

    pub fn current_versions(&self) -> BTreeMap<i64, i64> {
        self.current
            .iter()
            .map(|(version, current)| (current.db_version, *version))
            .collect()
    }

    pub fn last(&self) -> Option<i64> {
        std::cmp::max(
            // TODO: we probably don't need to traverse all of that...
            //       maybe use `skip` based on the len
            self.cleared.iter().map(|k| *k.end()).max(),
            std::cmp::max(
                self.current.last_key_value().map(|(k, _)| *k),
                self.partials.last_key_value().map(|(k, _)| *k),
            ),
        )
    }

    pub fn insert(&mut self, version: i64, known_version: KnownDbVersion) {
        self.insert_many(version..=version, known_version);
    }

    pub fn insert_many(&mut self, versions: RangeInclusive<i64>, known_version: KnownDbVersion) {
        match known_version {
            KnownDbVersion::Partial { seqs, last_seq, ts } => {
                self.partials
                    .insert(*versions.start(), PartialVersion { seqs, last_seq, ts });
            }
            KnownDbVersion::Current {
                db_version,
                last_seq,
                ts,
            } => {
                let version = *versions.start();
                self.partials.remove(&version);
                self.current.insert(
                    version,
                    CurrentVersion {
                        db_version,
                        last_seq,
                        ts,
                    },
                );
            }
            KnownDbVersion::Cleared => {
                for version in versions.clone() {
                    self.partials.remove(&version);
                    self.current.remove(&version);
                }
                self.cleared.insert(versions);
            }
        }
    }

    pub fn all_versions(&self) -> RangeInclusiveSet<i64> {
        let mut versions = self.cleared.clone();
        versions.extend(self.current.keys().map(|key| *key..=*key));
        versions.extend(self.partials.keys().map(|key| *key..=*key));

        versions
    }
}

pub type BookedInner = Arc<CountedTokioRwLock<BookedVersions>>;

#[derive(Clone)]
pub struct Booked(BookedInner);

impl Booked {
    fn new(versions: BookedVersions, registry: LockRegistry) -> Self {
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
    pub fn for_actor(&mut self, actor_id: ActorId) -> Booked {
        self.map
            .entry(actor_id)
            .or_insert_with(|| {
                Booked(Arc::new(CountedTokioRwLock::new(
                    self.registry.clone(),
                    Default::default(),
                )))
            })
            .clone()
    }

    pub fn registry(&self) -> &LockRegistry {
        &self.registry
    }
}

impl Deref for BookieInner {
    type Target = HashMap<ActorId, Booked>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

#[derive(Clone)]
pub struct Bookie(Arc<CountedTokioRwLock<BookieInner>>);

impl Bookie {
    pub fn new(map: HashMap<ActorId, BookedVersions>) -> Self {
        let registry = LockRegistry::default();
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
}
