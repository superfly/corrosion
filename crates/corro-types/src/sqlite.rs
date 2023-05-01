use std::{
    ops::{Deref, DerefMut},
    path::Path,
};

use bb8::ManageConnection;
use bb8_rusqlite::RusqliteConnectionManager;
use fallible_iterator::FallibleIterator;
use once_cell::sync::Lazy;
use rusqlite::{types::Type, Connection, OpenFlags};
use sqlite3_parser::{
    ast::{Cmd, ColumnConstraint, CreateTableBody, Stmt},
    lexer::{
        sql::{Parser, ParserError},
        Input,
    },
};
use tempfile::TempDir;
use tracing::{debug, error, trace};

use crate::filters::{Column, Schema};

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

#[derive(Debug, thiserror::Error)]
pub enum PrepareSqlError {
    #[error(transparent)]
    Parse(#[from] sqlite3_parser::lexer::sql::Error),
}

pub fn prepare_sql<I: Input>(input: I, schema: &mut Schema) -> Result<Vec<Cmd>, PrepareSqlError> {
    let mut cmds = vec![];
    let mut parser = sqlite3_parser::lexer::sql::Parser::new(input);
    loop {
        match parser.next() {
            Ok(None) => break,
            Err(err) => {
                eprintln!("Err: {err}");
                return Err(err.into());
            }
            Ok(Some(mut cmd)) => {
                let extra_cmd = if let Cmd::Stmt(Stmt::CreateTable {
                    ref tbl_name,
                    ref mut body,
                    ..
                }) = cmd
                {
                    if let CreateTableBody::ColumnsAndConstraints { ref columns, .. } = body {
                        if !tbl_name.name.0.contains("crsql")
                            & !tbl_name.name.0.contains("sqlite")
                            & !tbl_name.name.0.starts_with("__corro")
                        {
                            let fields: Vec<_> = columns
                                .iter()
                                .map(|def| {
                                    Column {
                                        name: def.col_name.0.clone(),
                                        sqlite_type: match def
                                            .col_type
                                            .as_ref()
                                            .map(|t| t.name.to_ascii_uppercase())
                                            .as_deref()
                                        {
                                            // TODO: magic JSON...
                                            // Some("JSON") => {
                                            //     Type::Map(Box::new(Type::String), Box::new(Type::Infer))
                                            // }

                                            // 1. If the declared type contains the string "INT" then it is assigned INTEGER affinity.
                                            Some(s) if s.contains("INT") => Type::Integer,
                                            // 2. If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
                                            Some(s)
                                                if s.contains("CHAR")
                                                    || s.contains("CLOB")
                                                    || s.contains("TEXT")
                                                    || s == "JSON" =>
                                            {
                                                Type::Text
                                            }

                                            // 3. If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
                                            Some(s) if s.contains("BLOB") || s == "JSONB" => {
                                                Type::Blob
                                            }
                                            None => Type::Blob,

                                            // 4. If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
                                            Some(s)
                                                if s.contains("REAL")
                                                    || s.contains("FLOA")
                                                    || s.contains("DOUB")
                                                    || s == "ANY" =>
                                            {
                                                Type::Real
                                            }

                                            // 5. Otherwise, the affinity is NUMERIC.
                                            Some(_s) => Type::Real,
                                        },
                                        primary_key: def.constraints.iter().any(|constraint| {
                                            matches!(
                                                constraint.constraint,
                                                ColumnConstraint::PrimaryKey { .. }
                                            )
                                        }),
                                        nullable: def.constraints.iter().any(|constraint| {
                                            matches!(
                                                constraint.constraint,
                                                ColumnConstraint::NotNull { nullable: true, .. }
                                            )
                                        }),
                                    }
                                })
                                .collect();

                            schema.insert(tbl_name.name.0.clone(), fields);

                            debug!("SELECTING crsql_as_crr for {}", tbl_name.name.0);

                            let select = format!("SELECT crsql_as_crr('{}');", tbl_name.name.0);
                            let mut select_parser = Parser::new(select.as_bytes());
                            Some(select_parser.next()?.expect("could not parse crsql_as_crr"))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                cmds.push(cmd);
                if let Some(extra_cmd) = extra_cmd {
                    cmds.push(extra_cmd);
                }
            }
        }
    }

    Ok(cmds)
}
