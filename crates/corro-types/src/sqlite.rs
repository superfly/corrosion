use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use bb8::ManageConnection;
use bb8_rusqlite::RusqliteConnectionManager;
use once_cell::sync::Lazy;
use rusqlite::{Connection, OpenFlags, ToSql, Transaction};
use tempfile::TempDir;
use tracing::{error, trace};

pub type SqlitePool = bb8::Pool<CrConnManager>;
pub type SqlitePoolError = bb8::RunError<bb8_rusqlite::Error>;

const CRSQL_EXT_GENERIC_NAME: &str = "crsqlite";

#[cfg(target_os = "macos")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.dylib";
#[cfg(target_os = "linux")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.so";

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../../../crsqlite-darwin-aarch64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../../../crsqlite-darwin-x86_64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../../../crsqlite-linux-x86_64.so");

// TODO: support windows

// need to keep this alive!
static CRSQL_EXT_DIR: Lazy<TempDir> = Lazy::new(|| {
    let dir = TempDir::new().expect("could not create temp dir!");
    std::fs::write(dir.path().join(CRSQL_EXT_GENERIC_NAME), CRSQL_EXT)
        .expect("could not write crsql ext file");
    dir
});

pub struct CrConnManager(RusqliteConnectionManager);

impl CrConnManager {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self(RusqliteConnectionManager::new(path))
    }

    pub fn new_read_only<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        Self(RusqliteConnectionManager::new_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        ))
    }
}

#[async_trait::async_trait]
impl ManageConnection for CrConnManager {
    type Connection = CrConn;

    type Error = bb8_rusqlite::Error;

    /// Attempts to create a new connection.
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut conn = self.0.connect().await?;
        init_cr_conn(&mut conn)?;
        Ok(CrConn(conn))
    }
    /// Determines if the connection is still connected to the database.
    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        self.0.is_valid(conn).await
    }
    /// Synchronously determine if the connection is no longer usable, if possible.
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        self.0.has_broken(conn)
    }
}

pub struct CrConn(pub Connection);

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

#[derive(Debug)]
pub struct CrSqlite;

pub fn init_cr_conn(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let ext_dir = &CRSQL_EXT_DIR;
    trace!(
        "loading crsqlite extension from path: {}",
        ext_dir.path().display()
    );
    unsafe {
        conn.load_extension_enable()?;
        conn.load_extension(
            ext_dir.path().join(CRSQL_EXT_GENERIC_NAME),
            Some("sqlite3_crsqlite_init"),
        )?;
        conn.load_extension_disable()?;
    }
    trace!("loaded crsqlite extension");

    // WAL journal mode and synchronous NORMAL for best performance / crash resilience compromise
    conn.execute_batch(
        r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
        "#,
    )?;

    Ok(())
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
    conn.pragma_update(None, "user_version", &v)?;
    Ok(())
}

// should be a noop if up to date!
pub fn migrate(conn: &mut Connection, migrations: Vec<Box<dyn Migration>>) -> rusqlite::Result<()> {
    let target_version = migrations.len();

    let current_version = user_version(&conn)?;
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
