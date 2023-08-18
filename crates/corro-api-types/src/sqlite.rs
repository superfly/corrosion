use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use bb8::ManageConnection;
use camino::Utf8PathBuf;
use compact_str::CompactString;
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

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
        conn.execute_batch(&format!("ATTACH DATABASE {} AS {}", path.as_str(), name))?;
    }

    Ok(())
}
