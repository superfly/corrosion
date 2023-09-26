mod config;

use std::{
    fmt,
    sync::atomic::{AtomicUsize, Ordering},
};

use deadpool::{
    async_trait,
    managed::{self, Object, RecycleError},
};

pub use deadpool::managed::reexports::*;
pub use rusqlite;
use tokio::task::block_in_place;

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
        conn: &mut Self::Type,
        _: &Metrics,
    ) -> managed::RecycleResult<Self::Error> {
        let recycle_count = self.recycle_count.fetch_add(1, Ordering::Relaxed);
        let n: usize = block_in_place(|| {
            conn.conn()
                .query_row("SELECT $1", [recycle_count], |row| row.get(0))
                .map_err(|e| RecycleError::Message(format!("{}", e)))
        })?;
        if n == recycle_count {
            Ok(())
        } else {
            Err(RecycleError::StaticMessage("Recycle count mismatch"))
        }
    }
}
