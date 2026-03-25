mod config;

use arc_swap::ArcSwap;
use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tracing::warn;

use deadpool::managed::{self, Object};
use metrics::counter;
use rusqlite::{CachedStatement, InterruptHandle, Params, Transaction};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

pub use deadpool::managed::reexports::*;
pub use rusqlite;

pub type Pool<T> = deadpool::managed::Pool<Manager<T>>;
pub type RusqlitePool = Pool<rusqlite::Connection>;
pub type CreatePoolError = deadpool::managed::CreatePoolError<ConfigError>;
pub type PoolBuilder<T> = deadpool::managed::PoolBuilder<Manager<T>, Object<Manager<T>>>;
pub type PoolError = deadpool::managed::PoolError<rusqlite::Error>;

pub type Hook<T> = deadpool::managed::Hook<Manager<T>>;
pub type HookError = deadpool::managed::HookError<rusqlite::Error>;

pub type Connection<T> = deadpool::managed::Object<Manager<T>>;
pub type RusqliteConnection = Connection<rusqlite::Connection>;

#[inline]
pub fn noop_transform(conn: rusqlite::Connection) -> rusqlite::Result<rusqlite::Connection> {
    Ok(conn)
}

pub use self::config::{Config, ConfigError};

pub type TransformFn<T> = dyn Fn(rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + Sync;

/// [`Manager`] for creating and recycling SQLite [`Connection`]s.
///
/// [`Manager`]: managed::Manager
pub struct Manager<T> {
    config: Config,
    recycle_count: AtomicUsize,
    transform: Box<TransformFn<T>>,
}

impl<T> Manager<T> {
    /// Creates a new [`Manager`] using the given [`Config`] backed by the
    /// specified [`Runtime`].
    #[must_use]
    pub fn from_config(
        config: &Config,
        transform: impl Fn(rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + Sync + 'static,
    ) -> Self {
        Self {
            config: config.clone(),
            recycle_count: AtomicUsize::new(0),
            transform: Box::new(transform),
        }
    }
}

impl<T> fmt::Debug for Manager<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Manager")
            .field("config", &self.config)
            .field("recycle_count", &self.recycle_count)
            .finish()
    }
}

pub trait SqliteConn: Send {
    fn conn(&self) -> &rusqlite::Connection;
}

impl SqliteConn for rusqlite::Connection {
    fn conn(&self) -> &rusqlite::Connection {
        self
    }
}

impl<T> managed::Manager for Manager<T>
where
    T: SqliteConn,
{
    type Type = T;
    type Error = rusqlite::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        (self.transform)(rusqlite::Connection::open_with_flags(
            &self.config.path,
            self.config.open_flags,
        )?)
    }

    async fn recycle(
        &self,
        _conn: &mut Self::Type,
        _: &Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        let _ = self.recycle_count.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct TimeoutGuard {
    cancel_token: CancellationToken,
}

impl Drop for TimeoutGuard {
    fn drop(&mut self) {
        self.cancel_token.cancel();
    }
}

#[derive(Clone)]
pub struct InterruptHandler {
    interrupt_hdl: Arc<InterruptHandle>,
    current_sql: Arc<ArcSwap<Option<String>>>,
    timeout: Option<Duration>,
}

impl InterruptHandler {
    pub fn new(
        interrupt_hdl: Arc<InterruptHandle>,
        current_sql: Arc<ArcSwap<Option<String>>>,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            interrupt_hdl,
            current_sql,
            timeout,
        }
    }

    fn timeout_guard(&self) -> TimeoutGuard {
        let cancel_token = CancellationToken::new();

        if let Some(timeout) = self.timeout {
            let cloned_token = cancel_token.clone();
            let interrupt_hdl = self.interrupt_hdl.clone();
            let current_sql = self.current_sql.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cloned_token.cancelled() => {}
                    _ = sleep(timeout) => {
                        warn!("sql call took more than {timeout:?}, interrupting.. {:?}", current_sql);
                        interrupt_hdl.interrupt();
                        counter!("corro.sqlite.interrupt", "source" => "timeout").increment(1);
                    }
                }
            });
        }

        TimeoutGuard { cancel_token }
    }
}

pub struct InterruptibleTransaction<T> {
    conn: T,
    timeout: Option<Duration>,
    int_hdlr: InterruptHandler,
    source: &'static str,
    current_sql: Arc<ArcSwap<Option<String>>>,
}

impl<T> InterruptibleTransaction<T>
where
    T: Deref<Target = rusqlite::Connection> + Committable,
{
    pub fn new(conn: T, timeout: Option<Duration>, source: &'static str) -> Self {
        let interrupt_hdl = Arc::new(conn.get_interrupt_handle());
        let query_store: Arc<ArcSwap<Option<String>>> = Arc::new(ArcSwap::new(Arc::new(None)));
        let int_hdlr = InterruptHandler::new(interrupt_hdl, query_store.clone(), timeout);
        Self {
            conn,
            timeout,
            int_hdlr,
            source,
            current_sql: query_store,
        }
    }

    pub fn execute(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<usize, rusqlite::Error> {
        let _guard = self.int_hdlr.timeout_guard();
        self.current_sql.store(Arc::new(Some(sql.to_string())));
        self.conn.execute(sql, params)
    }

    pub fn commit(self) -> Result<(), rusqlite::Error> {
        let _guard = self.int_hdlr.timeout_guard();
        self.conn.commit()
    }

    pub fn prepare(
        &self,
        sql: &str,
    ) -> Result<InterruptibleStatement<Statement<'_>>, rusqlite::Error> {
        let stmt = self.conn.prepare(sql)?;
        self.current_sql.store(Arc::new(Some(sql.to_string())));
        Ok(InterruptibleStatement::new(
            Statement(stmt),
            self.int_hdlr.clone(),
        ))
    }

    pub fn prepare_cached(
        &self,
        sql: &str,
    ) -> Result<InterruptibleStatement<CachedStatement<'_>>, rusqlite::Error> {
        let stmt = self.conn.prepare_cached(sql)?;
        self.current_sql.store(Arc::new(Some(sql.to_string())));
        Ok(InterruptibleStatement::new(stmt, self.int_hdlr.clone()))
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), rusqlite::Error> {
        let _guard = self.int_hdlr.timeout_guard();
        self.current_sql.store(Arc::new(Some(sql.to_string())));
        self.conn.execute_batch(sql)
    }

    pub fn savepoint(
        &mut self,
    ) -> Result<InterruptibleTransaction<rusqlite::Savepoint<'_>>, rusqlite::Error> {
        let sp = self.conn.savepoint()?;
        Ok(InterruptibleTransaction::new(sp, self.timeout, self.source))
    }
}

impl<T> Deref for InterruptibleTransaction<T>
where
    T: Deref<Target = rusqlite::Connection>,
{
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl<T> DerefMut for InterruptibleTransaction<T>
where
    T: DerefMut<Target = rusqlite::Connection>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

pub struct InterruptibleStatement<T> {
    stmt: T,
    int_hdlr: InterruptHandler,
}

impl<'conn, 'a, T> InterruptibleStatement<T>
where
    T: Deref<Target = rusqlite::Statement<'conn>> + DerefMut<Target = rusqlite::Statement<'conn>>,
{
    pub fn new(stmt: T, int_hdlr: InterruptHandler) -> Self {
        Self { stmt, int_hdlr }
    }

    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize, rusqlite::Error> {
        let _guard = self.int_hdlr.timeout_guard();
        self.stmt.execute(params)
    }

    pub fn query<'rows, P: Params>(
        &'a mut self,
        params: P,
    ) -> Result<rusqlite::Rows<'rows>, rusqlite::Error>
    where
        'conn: 'rows,
        'a: 'rows,
    {
        let _guard = self.int_hdlr.timeout_guard();
        self.stmt.query(params)
    }

    pub fn query_map<P: Params, S, F>(
        &'a mut self,
        params: P,
        f: F,
    ) -> rusqlite::Result<rusqlite::MappedRows<'a, F>>
    where
        F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<S>,
        'conn: 'a,
    {
        let _guard = self.int_hdlr.timeout_guard();
        self.stmt.query_map(params, f)
    }
}

impl<'conn, T: Deref<Target = rusqlite::Statement<'conn>>> Deref for InterruptibleStatement<T> {
    type Target = rusqlite::Statement<'conn>;

    fn deref(&self) -> &Self::Target {
        &self.stmt
    }
}

impl<'conn, T: DerefMut<Target = rusqlite::Statement<'conn>>> DerefMut
    for InterruptibleStatement<T>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stmt
    }
}

pub trait Committable {
    fn commit(self) -> Result<(), rusqlite::Error>;
    fn savepoint(&mut self) -> Result<rusqlite::Savepoint<'_>, rusqlite::Error>;
}

impl Committable for Transaction<'_> {
    fn commit(self) -> Result<(), rusqlite::Error> {
        self.commit()
    }

    fn savepoint(&mut self) -> Result<rusqlite::Savepoint<'_>, rusqlite::Error> {
        self.savepoint()
    }
}

impl Committable for rusqlite::Savepoint<'_> {
    fn commit(self) -> Result<(), rusqlite::Error> {
        self.commit()
    }

    fn savepoint(&mut self) -> Result<rusqlite::Savepoint<'_>, rusqlite::Error> {
        self.savepoint()
    }
}

// No-op for plain connections
impl Committable for rusqlite::Connection {
    fn commit(self) -> Result<(), rusqlite::Error> {
        Ok(())
    }

    fn savepoint(&mut self) -> Result<rusqlite::Savepoint<'_>, rusqlite::Error> {
        Err(rusqlite::Error::ModuleError(String::from(
            "cannot create savepoint from connection",
        )))
    }
}

pub struct Statement<'conn>(pub rusqlite::Statement<'conn>);

impl<'conn> Deref for Statement<'conn> {
    type Target = rusqlite::Statement<'conn>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'conn> DerefMut for Statement<'conn> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct InterruptibleMappedRows<'a, F> {
    rows: rusqlite::MappedRows<'a, F>,
    int_hdlr: InterruptHandler,
}

impl<'a, F> InterruptibleMappedRows<'a, F> {
    pub fn new(rows: rusqlite::MappedRows<'a, F>, int_hdlr: InterruptHandler) -> Self {
        Self { rows, int_hdlr }
    }
}

impl<'a, F, T> Iterator for InterruptibleMappedRows<'a, F>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
{
    type Item = rusqlite::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let _guard = self.int_hdlr.timeout_guard();
        self.rows.next()
    }
}

#[cfg(test)]
mod tests {}
