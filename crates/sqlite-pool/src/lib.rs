mod config;

use std::{
    fmt,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use deadpool::{
    async_trait,
    managed::{self, Object},
};
use metrics::counter;
use rusqlite::{CachedStatement, InterruptHandle, Params, Transaction};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

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

#[async_trait]
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

pub struct InterruptibleTransaction<T> {
    conn: T,
    timeout: Option<Duration>,
    interrupt_hdl: Arc<InterruptHandle>,
}

impl<T> InterruptibleTransaction<T>
where
    T: Deref<Target = rusqlite::Connection> + Committable,
{
    pub fn new(conn: T, timeout: Option<Duration>) -> Self {
        let interrupt_hdl = conn.get_interrupt_handle();

        Self {
            conn,
            timeout,
            interrupt_hdl: Arc::new(interrupt_hdl),
        }
    }

    pub fn execute(
        &self,
        sql: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> Result<usize, rusqlite::Error> {
        let token = self.interrupt_on_timeout();

        let res = self.conn.execute(sql, params);
        token.cancel();
        res
    }

    pub fn commit(self) -> Result<(), rusqlite::Error> {
        let token = self.interrupt_on_timeout();

        let res = self.conn.commit();
        token.cancel();
        res
    }

    pub fn prepare(&self, sql: &str) -> Result<InterruptibleStatement<Statement>, rusqlite::Error> {
        let stmt = self.conn.prepare(sql)?;
        Ok(InterruptibleStatement::new(
            Statement(stmt),
            self.interrupt_hdl.clone(),
            self.timeout,
        ))
    }

    pub fn prepare_cached(
        &self,
        sql: &str,
    ) -> Result<InterruptibleStatement<CachedStatement>, rusqlite::Error> {
        let stmt = self.conn.prepare_cached(sql)?;
        Ok(InterruptibleStatement::new(
            stmt,
            self.interrupt_hdl.clone(),
            self.timeout,
        ))
    }

    pub fn execute_batch(&self, sql: &str) -> Result<(), rusqlite::Error> {
        let token = self.interrupt_on_timeout();

        let res = self.conn.execute_batch(sql);
        token.cancel();
        res
    }

    pub fn savepoint(
        &mut self,
    ) -> Result<InterruptibleTransaction<rusqlite::Savepoint<'_>>, rusqlite::Error> {
        let sp = self.conn.savepoint()?;
        Ok(InterruptibleTransaction::new(sp, self.timeout))
    }

    pub fn interrupt_on_timeout(&self) -> CancellationToken {
        let token = CancellationToken::new();
        if let Some(timeout) = self.timeout {
            let cloned_token = token.clone();
            let interrupt_hdl = self.interrupt_hdl.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cloned_token.cancelled() => {}
                    _ = sleep(timeout) => {
                        warn!("sql call took more than {timeout:?}, interrupting..");
                        interrupt_hdl.interrupt();
                        counter!("corro.sqlite.interrupt").increment(1);
                    }
                };
            });
        }

        token
    }

    pub fn interrupt_on_cancel(&self, cancel: CancellationToken) {
        let interrupt_hdl = self.interrupt_hdl.clone();
        tokio::spawn(async move {
            cancel.cancelled().await;
            debug!("interruptting sqlite connection");
            interrupt_hdl.interrupt();
            counter!("corro.sqlite.interrupt").increment(1);
        });
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
    timeout: Option<Duration>,
    interrupt_hdl: Arc<InterruptHandle>,
}

impl<'conn, T> InterruptibleStatement<T>
where
    T: Deref<Target = rusqlite::Statement<'conn>> + DerefMut<Target = rusqlite::Statement<'conn>>,
{
    pub fn new(stmt: T, interrupt_hdl: Arc<InterruptHandle>, timeout: Option<Duration>) -> Self {
        Self {
            stmt,
            timeout,
            interrupt_hdl,
        }
    }

    pub fn execute<P: Params>(&mut self, params: P) -> Result<usize, rusqlite::Error> {
        let token = self.interrupt_on_timeout();
        let res = self.stmt.execute(params);
        token.cancel();
        res
    }

    pub fn interrupt_on_timeout(&self) -> CancellationToken {
        let token = CancellationToken::new();
        if let Some(timeout) = self.timeout {
            let cloned_token = token.clone();
            let interrupt_hdl = self.interrupt_hdl.clone();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cloned_token.cancelled() => {}
                    _ = sleep(timeout) => {
                        warn!("sql call took more than {timeout:?}, interrupting..");
                        interrupt_hdl.interrupt();
                        counter!("corro.sqlite.interrupt").increment(1);
                    }
                };
            });
        }

        token
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

pub struct Statement<'conn>(rusqlite::Statement<'conn>);

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

#[cfg(test)]
mod tests {}
