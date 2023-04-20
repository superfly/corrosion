use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use bb8::ManageConnection;
use bb8_rusqlite::RusqliteConnectionManager;
use rusqlite::{Connection, OpenFlags};
use tracing::{error, trace};

pub type SqlitePool = bb8::Pool<CrConnManager>;

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

// #[async_trait::async_trait]
// impl CustomizeConnection<CrConn, bb8_rusqlite::Error> for CrSqlite {
//     async fn on_acquire(&self, conn: &mut CrConn) -> Result<(), bb8_rusqlite::Error> {
//         Ok(init_cr_conn(conn)?)
//     }
// }

pub fn init_cr_conn(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    unsafe {
        conn.load_extension_enable()?;
        conn.load_extension(
            "../../crsqlite-linux-x86_64.so",
            Some("sqlite3_crsqlite_init"),
        )?;
        conn.load_extension_disable()?;
    }
    trace!("loaded crsqlite extension");

    // WAL journal mode and NORMAL synchronous for best performance / crash resilience
    conn.execute_batch(
        r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
        "#,
    )?;

    Ok(())
}
