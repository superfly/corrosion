use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use bb8::ManageConnection;
use bb8_rusqlite::RusqliteConnectionManager;
use once_cell::sync::Lazy;
use rusqlite::{Connection, OpenFlags};
use tempfile::TempDir;
use tracing::{error, trace};

pub type SqlitePool = bb8::Pool<CrConnManager>;

const CRSQL_EXT_GENERIC_NAME: &str = "crsqlite";

#[cfg(target_os = "macos")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.dylib";
#[cfg(target_os = "linux")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.so";

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../../../crsqlite-darwin-aarch64.dylib");
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
