use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::Arc,
};

use bb8::ManageConnection;
use camino::Utf8PathBuf;
use compact_str::CompactString;
use enquote::enquote;
use once_cell::sync::Lazy;
use rusqlite::{Connection, OpenFlags, ToSql, Transaction};
use tempfile::TempDir;
use tracing::{error, trace};

pub type SqlitePool = bb8::Pool<CrConnManager>;
pub type SqlitePoolError = bb8::RunError<Error>;

const CRSQL_EXT_GENERIC_NAME: &str = "crsqlite";

#[cfg(target_os = "macos")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.dylib";
#[cfg(target_os = "linux")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.so";

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-darwin-aarch64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-darwin-x86_64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-linux-x86_64.so");
#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-linux-aarch64.so");

// TODO: support windows

// need to keep this alive!
static CRSQL_EXT_DIR: Lazy<TempDir> = Lazy::new(|| {
    let dir = TempDir::new().expect("could not create temp dir!");
    std::fs::write(dir.path().join(CRSQL_EXT_GENERIC_NAME), CRSQL_EXT)
        .expect("could not write crsql ext file");
    dir
});

#[derive(Debug, Clone)]
struct ConnectionOptions {
    mode: OpenMode,
    path: PathBuf,
    attach: HashMap<Utf8PathBuf, CompactString>,
}

#[derive(Debug, Clone)]
enum OpenMode {
    Plain,
    WithFlags { flags: rusqlite::OpenFlags },
}

pub struct CrConnManager(Arc<ConnectionOptions>);

impl CrConnManager {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self(Arc::new(ConnectionOptions {
            mode: OpenMode::Plain,
            path: path.as_ref().into(),
            attach: Default::default(),
        }))
    }

    pub fn new_read_only<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self::new_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
    }

    pub fn new_with_flags<P>(path: P, flags: OpenFlags) -> Self
    where
        P: AsRef<Path>,
    {
        Self(Arc::new(ConnectionOptions {
            mode: OpenMode::WithFlags { flags },
            path: path.as_ref().into(),
            attach: Default::default(),
        }))
    }

    pub fn with_flags(self, flags: OpenFlags) -> Self {
        let mut opts = self.0.as_ref().clone();
        opts.mode = OpenMode::WithFlags { flags };
        Self(Arc::new(opts))
    }
}

#[async_trait::async_trait]
impl ManageConnection for CrConnManager {
    type Connection = CrConn;

    type Error = Error;

    /// Attempts to create a new connection.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let options = self.0.clone();

        // Technically, we don't need to use spawn_blocking() here, but doing so
        // means we won't inadvertantly block this task for any length of time,
        // since rusqlite is inherently synchronous.
        let mut conn = tokio::task::spawn_blocking(move || match &options.mode {
            OpenMode::Plain => rusqlite::Connection::open(&options.path),
            OpenMode::WithFlags { flags } => {
                rusqlite::Connection::open_with_flags(&options.path, *flags)
            }
        })
        .await??;

        init_cr_conn(&mut conn)?;
        setup_conn(&mut conn, &self.0.attach)?;
        Ok(CrConn(conn))
    }

    #[inline]
    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // no real need for this I don't think.
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // no concept of broken conns for sqlite afaik
        false
    }
}

#[derive(Debug)]
pub struct CrConn(Connection);

impl CrConn {
    pub fn init(mut conn: Connection) -> Result<Self, rusqlite::Error> {
        init_cr_conn(&mut conn)?;
        Ok(Self(conn))
    }

    pub fn transaction(&mut self) -> rusqlite::Result<Transaction> {
        self.0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
    }
}

impl Deref for CrConn {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CrConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for CrConn {
    fn drop(&mut self) {
        if let Err(e) = self.execute_batch("select crsql_finalize();") {
            error!("could not crsql_finalize: {e}");
        }
    }
}

fn init_cr_conn(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let ext_dir = &CRSQL_EXT_DIR;
    trace!(
        "loading crsqlite extension from path: {}",
        ext_dir.path().display()
    );
    unsafe {
        trace!("enabled loading extension");
        conn.load_extension_enable()?;
        conn.load_extension(
            ext_dir.path().join(CRSQL_EXT_GENERIC_NAME),
            Some("sqlite3_crsqlite_init"),
        )?;
        conn.load_extension_disable()?;
    }
    trace!("loaded crsqlite extension");

    Ok(())
}

pub(crate) fn setup_conn(
    conn: &mut Connection,
    attach: &HashMap<Utf8PathBuf, CompactString>,
) -> Result<(), rusqlite::Error> {
    // WAL journal mode and synchronous NORMAL for best performance / crash resilience compromise
    conn.execute_batch(
        r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA recursive_triggers = ON;
        "#,
    )?;

    for (path, name) in attach.iter() {
        conn.execute_batch(&format!(
            "ATTACH DATABASE {} AS {}",
            enquote('\'', path.as_str()),
            name
        ))?;
    }

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A rusqlite error.
    #[error("rusqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),

    /// A tokio join handle error.
    #[error("tokio join error")]
    TokioJoin(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
pub struct RusqliteConnManager(Arc<ConnectionOptions>);

impl RusqliteConnManager {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self(Arc::new(ConnectionOptions {
            mode: OpenMode::Plain,
            path: path.as_ref().into(),
            attach: Default::default(),
        }))
    }

    pub fn new_read_only<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self::new_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
    }

    pub fn new_with_flags<P>(path: P, flags: OpenFlags) -> Self
    where
        P: AsRef<Path>,
    {
        Self(Arc::new(ConnectionOptions {
            mode: OpenMode::WithFlags { flags },
            path: path.as_ref().into(),
            attach: Default::default(),
        }))
    }

    pub fn attach<P: Into<Utf8PathBuf>, S: Into<CompactString>>(self, path: P, name: S) -> Self {
        let mut opts = self.0.as_ref().clone();
        opts.attach.insert(path.into(), name.into());
        Self(Arc::new(opts))
    }
}

#[async_trait::async_trait]
impl ManageConnection for RusqliteConnManager {
    type Connection = Connection;

    type Error = Error;

    /// Attempts to create a new connection.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let options = self.0.clone();

        // Technically, we don't need to use spawn_blocking() here, but doing so
        // means we won't inadvertantly block this task for any length of time,
        // since rusqlite is inherently synchronous.
        let mut conn = tokio::task::spawn_blocking(move || match &options.mode {
            OpenMode::Plain => rusqlite::Connection::open(&options.path),
            OpenMode::WithFlags { flags } => {
                rusqlite::Connection::open_with_flags(&options.path, *flags)
            }
        })
        .await??;

        setup_conn(&mut conn, &self.0.attach)?;
        Ok(conn)
    }

    #[inline]
    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // no real need for this I don't think.
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // no concept of broken conns for sqlite afaik
        false
    }
}

pub trait Migration {
    fn migrate(&self, tx: &Transaction) -> rusqlite::Result<()>;
}

impl Migration for fn(&Transaction) -> rusqlite::Result<()> {
    fn migrate(&self, tx: &Transaction) -> rusqlite::Result<()> {
        self(tx)
    }
}

// Read user version field from the SQLite db
pub fn user_version(conn: &Connection) -> Result<usize, rusqlite::Error> {
    #[allow(deprecated)] // To keep compatibility with lower rusqlite versions
    conn.query_row::<_, &[&dyn ToSql], _>("PRAGMA user_version", &[], |row| row.get(0))
        .map(|v: i64| v as usize)
}

// Set user version field from the SQLite db
pub fn set_user_version(conn: &Connection, v: usize) -> rusqlite::Result<()> {
    let v = v as u32;
    conn.pragma_update(None, "user_version", v)?;
    Ok(())
}

// should be a noop if up to date!
pub fn migrate(conn: &mut Connection, migrations: Vec<Box<dyn Migration>>) -> rusqlite::Result<()> {
    let target_version = migrations.len();

    let current_version = user_version(conn)?;
    {
        let tx = conn.transaction()?;
        for (i, migration) in migrations.into_iter().enumerate() {
            let new_version = i + 1;
            if new_version <= current_version {
                continue;
            }
            migration.migrate(&tx)?;
        }
        set_user_version(&tx, target_version)?;
        tx.commit()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::{stream::FuturesUnordered, TryStreamExt};
    use tokio::task::block_in_place;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::TempDir::new()?;

        let pool = bb8::Builder::new()
            .max_size(1)
            .min_idle(Some(1)) // create one right away and keep it idle
            .build(CrConnManager::new(tmpdir.path().join("test.db")))
            .await?;

        {
            let conn = pool.get().await?;

            conn.execute_batch(
                "
                CREATE TABLE foo (a INTEGER PRIMARY KEY, b INTEGER);
                SELECT crsql_as_crr('foo');
            ",
            )?;
        }

        let total: i64 = 1000;
        let per_worker: i64 = 5;

        let futs = FuturesUnordered::from_iter((0..total).map(|_| {
            let pool = pool.clone();
            async move {
                tokio::spawn(async move {
                    FuturesUnordered::from_iter((0..per_worker).map(|_| {
                        let pool = pool.clone();
                        async move {
                            let conn = pool.get().await?;
                            block_in_place(|| {
                                conn.prepare_cached(
                                    "INSERT INTO foo (a, b) VALUES (random(), random())",
                                )?
                                .execute(())?;
                                Ok::<_, TestError>(())
                            })?;
                            Ok::<_, TestError>(())
                        }
                    }))
                    .try_collect()
                    .await?;
                    Ok::<_, TestError>(())
                })
                .await??;
                Ok::<_, TestError>(())
            }
        }));

        futs.try_collect().await?;

        let conn = pool.get().await?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM foo;", (), |row| row.get(0))?;

        assert_eq!(count, total * per_worker);

        Ok(())
    }

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Rusqlite(#[from] rusqlite::Error),
        #[error(transparent)]
        Bb8Rusqlite(#[from] SqlitePoolError),
        #[error(transparent)]
        Join(#[from] tokio::task::JoinError),
    }
}
