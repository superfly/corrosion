use std::{convert::Infallible, path::PathBuf};

use rusqlite::OpenFlags;

use crate::{
    noop_transform, CreatePoolError, Manager, Pool, PoolBuilder, PoolConfig, Runtime, RusqlitePool,
    SqliteConn,
};

/// Configuration object.
///
/// # Example (from environment)
///
/// By enabling the `serde` feature you can read the configuration using the
/// [`config`](https://crates.io/crates/config) crate as following:
/// ```env
/// SQLITE__PATH=db.sqlite3
/// SQLITE__POOL__MAX_SIZE=16
/// SQLITE__POOL__TIMEOUTS__WAIT__SECS=5
/// SQLITE__POOL__TIMEOUTS__WAIT__NANOS=0
/// ```
/// ```rust
/// # use serde_1 as serde;
/// #
/// #[derive(serde::Deserialize, serde::Serialize)]
/// # #[serde(crate = "serde_1")]
/// struct Config {
///     sqlite: deadpool_sqlite::Config,
/// }
/// impl Config {
///     pub fn from_env() -> Result<Self, config::ConfigError> {
///         let mut cfg = config::Config::builder()
///            .add_source(config::Environment::default().separator("__"))
///            .build()?;
///            cfg.try_deserialize()
///     }
/// }
/// ```
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(serde_1::Deserialize, serde_1::Serialize))]
#[cfg_attr(feature = "serde", serde(crate = "serde_1"))]
pub struct Config {
    /// Path to SQLite database file.
    pub path: PathBuf,

    /// Open flags for SQLite
    pub open_flags: OpenFlags,

    /// [`Pool`] configuration.
    pub pool: Option<PoolConfig>,
}

impl Config {
    /// Create a new [`Config`] with the given `path` of SQLite database file.
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            open_flags: OpenFlags::default(),
            pool: None,
        }
    }

    pub fn read_only(mut self) -> Self {
        self.open_flags =
            OpenFlags::empty() | OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
        self
    }

    pub fn max_size(mut self, value: usize) -> Self {
        self.pool.get_or_insert_with(Default::default).max_size = value;
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
        self.pool.unwrap_or_default()
    }
}

/// This error is returned if there is something wrong with the SQLite configuration.
///
/// This is just a type alias to [`Infallible`] at the moment as there
/// is no validation happening at the configuration phase.
pub type ConfigError = Infallible;
