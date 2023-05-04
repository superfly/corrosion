use std::{
    fmt,
    ops::{Deref, DerefMut},
    path::Path,
};

use bb8::ManageConnection;
use bb8_rusqlite::RusqliteConnectionManager;
use fallible_iterator::FallibleIterator;
use indexmap::IndexMap;
use once_cell::sync::Lazy;
use rusqlite::{Connection, OpenFlags, ToSql, Transaction};
use sqlite3_parser::{
    ast::{
        Cmd, ColumnConstraint, ColumnDefinition, CreateTableBody, Expr, Name, NamedTableConstraint,
        QualifiedName, SortedColumn, Stmt, TableConstraint, TableOptions, ToTokens,
    },
    lexer::{sql::Parser, Input},
};
use tempfile::TempDir;
use tracing::{debug, error, info, trace};

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
pub enum ParseSqlError {
    #[error(transparent)]
    Parse(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("nothing to parse")]
    NothingParsed,
    #[error("unsupported statement: {0}")]
    UnsupportedCmd(Cmd),
    #[error("unique indexes are not supported: {0}")]
    UniqueIndex(Cmd),
    #[error("temporary tables are not supported: {0}")]
    TemporaryTable(Cmd),
    #[error("table as select are not supported: {0}")]
    TableAsSelect(Cmd),
    #[error("not nullable column '{name}' on table '{tbl_name}' needs a default value for forward schema compatibility")]
    NotNullableColumnNeedsDefault { tbl_name: String, name: String },
    #[error("foreign keys are not supported (table: '{tbl_name}', column: '{name}')")]
    ForeignKey { tbl_name: String, name: String },
    #[error("missing table for index (table: '{tbl_name}', index: '{name}')")]
    IndexWithoutTable { tbl_name: String, name: String },
}

pub fn prepare_sql<I: Input>(input: I, schema: &mut Schema) -> Result<Vec<Cmd>, ParseSqlError> {
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
                    if let CreateTableBody::ColumnsAndConstraints {
                        ref columns,
                        ref constraints,
                        ..
                    } = body
                    {
                        if !tbl_name.name.0.contains("crsql")
                            & !tbl_name.name.0.contains("sqlite")
                            & !tbl_name.name.0.starts_with("__corro")
                        {
                            let fields: Vec<_> = columns
                                .iter()
                                .map(|def| {
                                    Column {
                                        name: def.col_name.0.clone(),
                                        sql_type: match def
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
                                        primary_key: def.constraints.iter().any(|named| {
                                            matches!(
                                                named.constraint,
                                                ColumnConstraint::PrimaryKey { .. }
                                            )
                                        }) || constraints
                                            .as_ref()
                                            .map(|ref constraints| {
                                                constraints.iter().any(|ref named| {
                                                    match &named.constraint {
                                                        TableConstraint::PrimaryKey {
                                                            columns,
                                                            ..
                                                        } => columns.iter().any(|col| {
                                                            match &col.expr {
                                                                Expr::Id(id) => {
                                                                    id.0 == def.col_name.0
                                                                }
                                                                _ => false,
                                                            }
                                                        }),
                                                        _ => false,
                                                    }
                                                })
                                            })
                                            .unwrap_or(false),
                                        nullable: def.constraints.iter().any(|named| {
                                            matches!(
                                                named.constraint,
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

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NormalizedColumn {
    pub name: String,
    pub sql_type: Type,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub generated: Option<String>,
    pub primary_key: bool,
    pub raw: ColumnDefinition,
}

impl std::hash::Hash for NormalizedColumn {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.sql_type.hash(state);
        self.nullable.hash(state);
        self.default_value.hash(state);
        self.generated.hash(state);
        self.primary_key.hash(state);
    }
}

impl fmt::Display for NormalizedColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.to_fmt(f)
    }
}

/// SQLite data types.
/// See [Fundamental Datatypes](https://sqlite.org/c3ref/c_blob.html).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Type {
    /// NULL
    Null,
    /// 64-bit signed integer
    Integer,
    /// 64-bit IEEE floating point number
    Real,
    /// String
    Text,
    /// BLOB
    Blob,
}

#[derive(Debug, Clone)]
pub struct NormalizedTable {
    pub name: String,
    pub columns: IndexMap<String, NormalizedColumn>,
    pub indexes: IndexMap<String, NormalizedIndex>,
    pub raw: CreateTableBody,
}

impl fmt::Display for NormalizedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Cmd::Stmt(Stmt::CreateTable {
            temporary: false,
            if_not_exists: false,
            tbl_name: QualifiedName::single(Name(self.name.clone())),
            body: self.raw.clone(),
        })
        .to_fmt(f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NormalizedIndex {
    pub name: String,
    pub tbl_name: String,
    pub columns: Vec<SortedColumn>,
    pub where_clause: Option<Expr>,
}

#[derive(Debug, Clone, Default)]
pub struct NormalizedSchema {
    pub tables: IndexMap<String, NormalizedTable>,
}

pub fn parse_sql(sql: &str) -> Result<NormalizedSchema, ParseSqlError> {
    let mut schema = NormalizedSchema::default();
    info!("parsing {sql}");
    let mut parser = sqlite3_parser::lexer::sql::Parser::new(sql.as_bytes());

    loop {
        match parser.next() {
            Ok(None) => break,
            Err(err) => {
                eprintln!("Err: {err}");
                return Err(err.into());
            }
            Ok(Some(ref cmd @ Cmd::Stmt(ref stmt))) => match stmt {
                Stmt::CreateIndex { unique: true, .. } => {
                    return Err(ParseSqlError::UniqueIndex(cmd.clone()))
                }
                Stmt::CreateTable {
                    temporary: true, ..
                } => return Err(ParseSqlError::TemporaryTable(cmd.clone())),
                Stmt::CreateTable {
                    body: CreateTableBody::AsSelect(_),
                    ..
                } => return Err(ParseSqlError::TemporaryTable(cmd.clone())),
                Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: _,
                    tbl_name,
                    body:
                        CreateTableBody::ColumnsAndConstraints {
                            columns,
                            constraints,
                            options,
                        },
                } => {
                    if let Some(table) =
                        prepare_table(tbl_name, columns, constraints.as_ref(), options)?
                    {
                        schema.tables.insert(tbl_name.name.0.clone(), table);
                        info!("inserted table: {}", tbl_name.name.0);
                    } else {
                        info!("skipped table: {}", tbl_name.name.0);
                    }
                }
                Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: _,
                    idx_name,
                    tbl_name,
                    columns,
                    where_clause,
                } => {
                    if let Some(table) = schema.tables.get_mut(tbl_name.0.as_str()) {
                        if let Some(index) =
                            prepare_index(idx_name, tbl_name, columns, where_clause.as_ref())?
                        {
                            table.indexes.insert(idx_name.name.0.clone(), index);
                        }
                    } else {
                        return Err(ParseSqlError::IndexWithoutTable {
                            tbl_name: tbl_name.0.clone(),
                            name: idx_name.name.0.clone(),
                        });
                    }
                }
                _ => return Err(ParseSqlError::UnsupportedCmd(cmd.clone())),
            },
            Ok(Some(cmd)) => return Err(ParseSqlError::UnsupportedCmd(cmd)),
        }
    }

    Ok(schema)
}

fn prepare_index(
    name: &QualifiedName,
    tbl_name: &Name,
    columns: &Vec<SortedColumn>,
    where_clause: Option<&Expr>,
) -> Result<Option<NormalizedIndex>, ParseSqlError> {
    info!("preparing index: {}", name.name.0);
    if tbl_name.0.contains("crsql")
        & tbl_name.0.contains("sqlite")
        & tbl_name.0.starts_with("__corro")
    {
        return Ok(None);
    }

    Ok(Some(NormalizedIndex {
        name: name.name.0.clone(),
        tbl_name: tbl_name.0.clone(),
        columns: columns.clone(),
        where_clause: where_clause.cloned(),
    }))
}

fn prepare_table(
    tbl_name: &QualifiedName,
    columns: &Vec<ColumnDefinition>,
    constraints: Option<&Vec<NamedTableConstraint>>,
    options: &TableOptions,
) -> Result<Option<NormalizedTable>, ParseSqlError> {
    info!("preparing table: {}", tbl_name.name.0);
    if tbl_name.name.0.contains("crsql")
        & tbl_name.name.0.contains("sqlite")
        & tbl_name.name.0.starts_with("__corro")
    {
        info!("skipping table because of name");
        return Ok(None);
    }

    Ok(Some(NormalizedTable {
        name: tbl_name.name.0.clone(),
        indexes: IndexMap::new(),
        columns: columns
            .iter()
            .map(|def| {
                info!("visiting column: {}", def.col_name.0);
                let default_value = def.constraints.iter().find_map(|named| {
                    if let ColumnConstraint::Default(ref expr) = named.constraint {
                        Some(expr.to_string())
                    } else {
                        None
                    }
                });

                let not_nullable = def.constraints.iter().any(|named| {
                    matches!(
                        named.constraint,
                        ColumnConstraint::NotNull {
                            nullable: false,
                            ..
                        }
                    )
                });
                let nullable = !not_nullable;

                let primary_key =
                    def.constraints.iter().any(|named| {
                        matches!(named.constraint, ColumnConstraint::PrimaryKey { .. })
                    }) || constraints
                        .as_ref()
                        .map(|ref constraints| {
                            constraints.iter().any(|ref named| match &named.constraint {
                                TableConstraint::PrimaryKey { columns, .. } => {
                                    columns.iter().any(|col| match &col.expr {
                                        Expr::Id(id) => id.0 == def.col_name.0,
                                        _ => false,
                                    })
                                }
                                _ => false,
                            })
                        })
                        .unwrap_or(false);

                if !primary_key && (!nullable && default_value.is_none()) {
                    return Err(ParseSqlError::NotNullableColumnNeedsDefault {
                        tbl_name: tbl_name.name.0.clone(),
                        name: def.col_name.0.clone(),
                    });
                }

                if def
                    .constraints
                    .iter()
                    .any(|named| matches!(named.constraint, ColumnConstraint::ForeignKey { .. }))
                {
                    return Err(ParseSqlError::ForeignKey {
                        tbl_name: tbl_name.name.0.clone(),
                        name: def.col_name.0.clone(),
                    });
                }

                Ok((
                    def.col_name.0.clone(),
                    NormalizedColumn {
                        name: def.col_name.0.clone(),
                        sql_type: match def
                            .col_type
                            .as_ref()
                            .map(|t| t.name.to_ascii_uppercase())
                            .as_deref()
                        {
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
                            Some(s) if s.contains("BLOB") || s == "JSONB" => Type::Blob,
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
                        primary_key,
                        nullable,
                        default_value,
                        generated: def.constraints.iter().find_map(|named| {
                            if let ColumnConstraint::Generated { ref expr, .. } = named.constraint {
                                Some(expr.to_string())
                            } else {
                                None
                            }
                        }),
                        raw: def.clone(),
                    },
                ))
            })
            .collect::<Result<IndexMap<_, _>, ParseSqlError>>()?,
        raw: CreateTableBody::ColumnsAndConstraints {
            columns: columns.clone(),
            constraints: constraints.cloned(),
            options: options.clone(),
        },
    }))
}
