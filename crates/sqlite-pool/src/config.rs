use std::{convert::Infallible, path::PathBuf, time::Duration};

use deadpool::managed::{QueueMode, Timeouts};
use rusqlite::OpenFlags;

use crate::{
    noop_transform, CreatePoolError, Manager, Pool, PoolBuilder, PoolConfig, Runtime, RusqlitePool,
    SqliteConn,
};

/// Configuration object.
#[derive(Clone, Debug, Default)]
pub struct Config {
    /// Path to SQLite database file.
    pub path: PathBuf,

    /// Open flags for SQLite
    pub open_flags: OpenFlags,

    /// [`Pool`] configuration.
    pub pool: PoolConfig,
}

impl Config {
    /// Create a new [`Config`] with the given `path` of SQLite database file.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            open_flags: OpenFlags::default(),
            pool: PoolConfig {
                max_size: 20,
                timeouts: Timeouts {
                    wait: Some(Duration::from_secs(30)),
                    ..Default::default()
                },
                queue_mode: QueueMode::default(),
            },
        }
    }

    pub fn read_only(mut self) -> Self {
        self.open_flags =
            OpenFlags::empty() | OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        self
    }

    pub fn max_size(mut self, value: usize) -> Self {
        self.pool.max_size = value;
        self
    }

    pub fn create_pool(&self) -> Result<RusqlitePool, CreatePoolError> {
        self.builder(noop_transform)
            .map_err(CreatePoolError::Config)?
            .build()
            .map_err(CreatePoolError::Build)
    }

    pub fn create_pool_transform<T: SqliteConn>(
        &self,
        f: impl Fn(rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + Sync + 'static,
    ) -> Result<Pool<T>, CreatePoolError> {
        self.builder(f)
            .map_err(CreatePoolError::Config)?
            .build()
            .map_err(CreatePoolError::Build)
    }

    /// Creates a new [`PoolBuilder`] using this [`Config`].
    ///
    /// # Errors
    ///
    /// See [`ConfigError`] for details.
    ///
    /// [`RedisError`]: redis::RedisError
    pub fn builder<T: SqliteConn>(
        &self,
        f: impl Fn(rusqlite::Connection) -> Result<T, rusqlite::Error> + Send + Sync + 'static,
    ) -> Result<PoolBuilder<T>, ConfigError> {
        let manager = Manager::from_config(self, f);
        Ok(Pool::builder(manager)
            .config(self.get_pool_config())
            .runtime(Runtime::Tokio1))
    }

    /// Returns [`deadpool::managed::PoolConfig`] which can be used to construct
    /// a [`deadpool::managed::Pool`] instance.
    #[must_use]
    pub fn get_pool_config(&self) -> PoolConfig {
        self.pool
    }
}

/// This error is returned if there is something wrong with the SQLite configuration.
///
/// This is just a type alias to [`Infallible`] at the moment as there
/// is no validation happening at the configuration phase.
pub type ConfigError = Infallible;
