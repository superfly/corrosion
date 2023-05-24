use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::Read,
    path::Path,
    time::{Instant, SystemTime},
};

use fallible_iterator::FallibleIterator;
use indexmap::{IndexMap, IndexSet};
use rusqlite::{Connection, Transaction};
use sqlite3_parser::ast::{
    Cmd, ColumnConstraint, ColumnDefinition, CreateTableBody, Expr, Name, NamedTableConstraint,
    QualifiedName, SortedColumn, Stmt, TableConstraint, TableOptions, ToTokens,
};
use tracing::{debug, info};

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
    pub pk: IndexSet<String>,
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

#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
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
    #[error("table as select arenot supported: {0}")]
    TableAsSelect(Cmd),
    #[error("not nullable column '{name}' on table '{tbl_name}' needs a default value for forward schema compatibility")]
    NotNullableColumnNeedsDefault { tbl_name: String, name: String },
    #[error("foreign keys are not supported (table: '{tbl_name}', column: '{name}')")]
    ForeignKey { tbl_name: String, name: String },
    #[error("missing table for index (table: '{tbl_name}', index: '{name}')")]
    IndexWithoutTable { tbl_name: String, name: String },
    #[error("expr used as primary")]
    PrimaryKeyExpr,
    #[error("won't drop table without the destructive flag set (table: '{0}')")]
    DropTableWithoutDestructiveFlag(String),
    #[error("won't drop table without the destructive flag set (table: '{0}', column: '{1}')")]
    DropColumnWithoutDestructiveFlag(String, String),
    #[error("can't add a primary key (table: '{0}', column: '{1}')")]
    AddPrimaryKey(String, String),
    #[error("can't modify primary keys (table: '{0}')")]
    ModifyPrimaryKeys(String),
}

pub fn init_schema(conn: &Connection) -> Result<NormalizedSchema, SchemaError> {
    let mut dump = String::new();

    let tables: HashMap<String, String> = conn
            .prepare(
                r#"SELECT name, sql FROM sqlite_schema
    WHERE type = "table" AND name != "sqlite_sequence" AND name NOT LIKE '__corro_%' AND name NOT LIKE '%crsql%' ORDER BY tbl_name"#,
            )?
            .query_map((), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;

    for sql in tables.values() {
        dump.push_str(sql.as_str());
        dump.push(';');
    }

    let indexes: HashMap<String, String> = conn
            .prepare(
                r#"SELECT name, sql FROM sqlite_schema
    WHERE type = "index" AND name NOT LIKE 'sqlite_autoindex_%' AND name NOT LIKE '__corro_%' AND name NOT LIKE '%crsql%' ORDER BY tbl_name"#,
            )?
            .query_map((), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;

    for sql in indexes.values() {
        dump.push_str(sql.as_str());
        dump.push(';');
    }

    Ok(parse_sql(dump.as_str())?)
}

pub fn make_schema<P: AsRef<Path>>(
    tx: &Transaction,
    schema_paths: &[P],
    schema: &NormalizedSchema,
) -> Result<NormalizedSchema, SchemaError> {
    let mut new_sql = String::new();

    for schema_path in schema_paths.iter() {
        let mut dir = std::fs::read_dir(schema_path)?;

        let mut entries = vec![];

        while let Some(entry) = dir.next() {
            entries.push(entry?);
        }

        let mut entries: Vec<_> = entries
            .into_iter()
            .filter_map(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|ext| if ext == "sql" { Some(entry) } else { None })
            })
            .collect();

        entries.sort_by_key(|entry| entry.path());

        for entry in entries.iter() {
            std::fs::File::open(entry.path())?.read_to_string(&mut new_sql)?;
        }
    }

    let new_schema = parse_sql(&new_sql)?;

    // iterate over dropped tables
    for name in schema
        .tables
        .keys()
        .collect::<HashSet<_>>()
        .difference(&new_schema.tables.keys().collect::<HashSet<_>>())
    {
        // TODO: add options and check flag
        return Err(SchemaError::DropTableWithoutDestructiveFlag(
            (*name).clone(),
        ));
    }

    let new_table_names = new_schema
        .tables
        .keys()
        .collect::<HashSet<_>>()
        .difference(&schema.tables.keys().collect::<HashSet<_>>())
        .cloned()
        .collect::<HashSet<_>>();

    debug!("new table names: {new_table_names:?}");

    let new_tables_iter = new_schema
        .tables
        .iter()
        .filter(|(table, _)| new_table_names.contains(table));

    for (name, table) in new_tables_iter {
        info!("creating table '{name}'");
        tx.execute_batch(
            &Cmd::Stmt(Stmt::CreateTable {
                temporary: false,
                if_not_exists: false,
                tbl_name: QualifiedName::single(Name(name.clone())),
                body: table.raw.clone(),
            })
            .to_string(),
        )?;

        tx.execute_batch(&format!("SELECT crsql_as_crr('{name}');"))?;

        for (idx_name, index) in table.indexes.iter() {
            info!("creating index '{idx_name}'");
            tx.execute_batch(
                &Cmd::Stmt(Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: false,
                    idx_name: QualifiedName::single(Name(idx_name.clone())),
                    tbl_name: Name(index.tbl_name.clone()),
                    columns: index.columns.clone(),
                    where_clause: index.where_clause.clone(),
                })
                .to_string(),
            )?;
        }
    }

    // iterate intersecting tables
    for name in new_schema
        .tables
        .keys()
        .collect::<HashSet<_>>()
        .intersection(&schema.tables.keys().collect::<HashSet<_>>())
        .cloned()
    {
        debug!("processing table '{name}'");
        let table = schema.tables.get(name).unwrap();
        debug!(
            "current cols: {:?}",
            table.columns.keys().collect::<Vec<&String>>()
        );
        let new_table = new_schema.tables.get(name).unwrap();
        debug!(
            "new cols: {:?}",
            new_table.columns.keys().collect::<Vec<&String>>()
        );

        // 1. Check column drops... don't allow unless flag is passed

        let dropped_cols = table
            .columns
            .keys()
            .collect::<HashSet<_>>()
            .difference(&new_table.columns.keys().collect::<HashSet<_>>())
            .cloned()
            .collect::<HashSet<_>>();

        debug!("dropped cols: {dropped_cols:?}");

        for col_name in dropped_cols {
            // TODO: add options and check flag
            return Err(SchemaError::DropColumnWithoutDestructiveFlag(
                name.clone(),
                col_name.clone(),
            ));
        }

        // 2. check for changed columns

        let changed_cols: HashMap<String, NormalizedColumn> = table
            .columns
            .iter()
            .filter_map(|(name, col)| {
                new_table
                    .columns
                    .get(name)
                    .and_then(|new_col| (new_col != col).then(|| (name.clone(), col.clone())))
            })
            .collect();

        debug!(
            "changed cols: {:?}",
            changed_cols.keys().collect::<Vec<_>>()
        );

        let new_col_names = new_table
            .columns
            .keys()
            .collect::<HashSet<_>>()
            .difference(&table.columns.keys().collect::<HashSet<_>>())
            .cloned()
            .collect::<HashSet<_>>();

        info!("new columns: {new_col_names:?}");

        let new_cols_iter = new_table
            .columns
            .iter()
            .filter(|(col_name, _)| new_col_names.contains(col_name));

        if changed_cols.is_empty() {
            // 2.1. no changed columns, add missing ones

            tx.execute_batch(&format!("SELECT crsql_begin_alter('{name}');"))?;

            for (col_name, col) in new_cols_iter {
                info!("adding column '{col_name}'");
                if col.primary_key {
                    return Err(SchemaError::AddPrimaryKey(name.clone(), col_name.clone()));
                }
                if !col.nullable && col.default_value.is_none() {
                    return Err(SchemaError::NotNullableColumnNeedsDefault {
                        tbl_name: name.clone(),
                        name: col_name.clone(),
                    });
                }
                tx.execute_batch(&format!("ALTER TABLE {name} ADD COLUMN {}", col))?;
            }
            tx.execute_batch(&format!("SELECT crsql_commit_alter('{name}');"))?;
        } else {
            // 2.2 we do have changed columns, try to do something about that

            let primary_keys = table
                .columns
                .values()
                .filter_map(|col| col.primary_key.then(|| &col.name))
                .collect::<Vec<&String>>();

            let new_primary_keys = new_table
                .columns
                .values()
                .filter_map(|col| col.primary_key.then(|| &col.name))
                .collect::<Vec<&String>>();

            if primary_keys != new_primary_keys {
                return Err(SchemaError::ModifyPrimaryKeys(name.clone()));
            }

            // "12-step" process to modifying a table

            // first, create our new table with a temp name
            let tmp_name = format!(
                "{name}_{}",
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            );

            let create_tmp_table = Cmd::Stmt(Stmt::CreateTable {
                temporary: false,
                if_not_exists: false,
                tbl_name: QualifiedName::single(Name(tmp_name.clone())),
                body: new_table.raw.clone(),
            });

            tx.execute_batch("SELECT crsql_begin_alter('{name}');")?;

            info!("creating tmp table '{tmp_name}'");
            tx.execute_batch(&create_tmp_table.to_string())?;

            let col_names = table
                .columns
                .keys()
                .cloned()
                .collect::<Vec<String>>()
                .join(",");

            info!("inserting data from '{name}' into '{tmp_name}'");
            let inserted = tx.execute(
                &format!("INSERT INTO {tmp_name} ({col_names}) SELECT {col_names} FROM {name}"),
                (),
            )?;

            info!("re-inserted {inserted} rows into the new table for {name}");

            info!("dropping old table '{name}', renaming '{tmp_name}' to '{name}'");
            tx.execute_batch(&format!(
                "DROP TABLE {name};
                 ALTER TABLE {tmp_name} RENAME TO {name}"
            ))?;

            tx.execute_batch(&format!("SELECT crsql_commit_alter('{name}');"))?;
        }

        let new_index_names = new_table
            .indexes
            .keys()
            .collect::<HashSet<_>>()
            .difference(&table.indexes.keys().collect::<HashSet<_>>())
            .cloned()
            .collect::<HashSet<_>>();

        let new_indexes_iter = new_table
            .indexes
            .iter()
            .filter(|(index, _)| new_index_names.contains(index));

        for (idx_name, index) in new_indexes_iter {
            info!("creating new index '{idx_name}'");
            tx.execute_batch(
                &Cmd::Stmt(Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: false,
                    idx_name: QualifiedName::single(Name(idx_name.clone())),
                    tbl_name: Name(index.tbl_name.clone()),
                    columns: index.columns.clone(),
                    where_clause: index.where_clause.clone(),
                })
                .to_string(),
            )?;
        }

        let dropped_indexes = table
            .indexes
            .keys()
            .collect::<HashSet<_>>()
            .difference(&new_table.indexes.keys().collect::<HashSet<_>>())
            .cloned()
            .collect::<HashSet<_>>();

        for idx_name in dropped_indexes {
            info!("dropping index '{idx_name}'");
            tx.execute_batch(&format!("DROP INDEX {idx_name}"))?;
        }

        let changed_indexes_iter = table.indexes.iter().filter_map(|(idx_name, index)| {
            let pindex = new_table.indexes.get(idx_name)?;
            if pindex != index {
                Some((idx_name, pindex))
            } else {
                None
            }
        });

        for (idx_name, index) in changed_indexes_iter {
            info!("replacing index '{idx_name}' (drop + create)");
            tx.execute_batch(&format!(
                "DROP INDEX {idx_name}; {}",
                &Cmd::Stmt(Stmt::CreateIndex {
                    unique: false,
                    if_not_exists: false,
                    idx_name: QualifiedName::single(Name(idx_name.clone())),
                    tbl_name: Name(index.tbl_name.clone()),
                    columns: index.columns.clone(),
                    where_clause: index.where_clause.clone(),
                })
                .to_string(),
            ))?;
        }
    }

    Ok(new_schema)
}

pub fn apply_schema<P: AsRef<Path>>(
    conn: &mut Connection,
    schema_paths: &[P],
    schema: &NormalizedSchema,
) -> Result<NormalizedSchema, SchemaError> {
    info!("Applying schema changes...");
    let start = Instant::now();

    let tx = conn.transaction()?;
    let new_schema = make_schema(&tx, schema_paths, schema)?;

    tx.commit()?;

    info!("Done applying schema changes (took: {:?})", start.elapsed());

    Ok(new_schema)
}

pub fn parse_sql(sql: &str) -> Result<NormalizedSchema, SchemaError> {
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
                    return Err(SchemaError::UniqueIndex(cmd.clone()))
                }
                Stmt::CreateTable {
                    temporary: true, ..
                } => return Err(SchemaError::TemporaryTable(cmd.clone())),
                Stmt::CreateTable {
                    body: CreateTableBody::AsSelect(_),
                    ..
                } => return Err(SchemaError::TemporaryTable(cmd.clone())),
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
                        debug!("inserted table: {}", tbl_name.name.0);
                    } else {
                        debug!("skipped table: {}", tbl_name.name.0);
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
                        return Err(SchemaError::IndexWithoutTable {
                            tbl_name: tbl_name.0.clone(),
                            name: idx_name.name.0.clone(),
                        });
                    }
                }
                _ => return Err(SchemaError::UnsupportedCmd(cmd.clone())),
            },
            Ok(Some(cmd)) => return Err(SchemaError::UnsupportedCmd(cmd)),
        }
    }

    Ok(schema)
}

fn prepare_index(
    name: &QualifiedName,
    tbl_name: &Name,
    columns: &Vec<SortedColumn>,
    where_clause: Option<&Expr>,
) -> Result<Option<NormalizedIndex>, SchemaError> {
    debug!("preparing index: {}", name.name.0);
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
) -> Result<Option<NormalizedTable>, SchemaError> {
    debug!("preparing table: {}", tbl_name.name.0);
    if tbl_name.name.0.contains("crsql")
        & tbl_name.name.0.contains("sqlite")
        & tbl_name.name.0.starts_with("__corro")
    {
        debug!("skipping table because of name");
        return Ok(None);
    }

    let pk = constraints
        .and_then(|constraints| {
            constraints
                .iter()
                .find_map(|named| match &named.constraint {
                    TableConstraint::PrimaryKey { columns, .. } => Some(
                        columns
                            .iter()
                            .map(|col| match &col.expr {
                                Expr::Id(id) => Ok(id.0.clone()),
                                _ => Err(SchemaError::PrimaryKeyExpr),
                            })
                            .collect::<Result<IndexSet<_>, SchemaError>>(),
                    ),
                    _ => None,
                })
        })
        .unwrap_or_else(|| {
            Ok(columns
                .iter()
                .filter_map(|def| {
                    def.constraints
                        .iter()
                        .any(|named| {
                            matches!(named.constraint, ColumnConstraint::PrimaryKey { .. })
                        })
                        .then(|| def.col_name.0.clone())
                })
                .collect())
        })?;

    Ok(Some(NormalizedTable {
        name: tbl_name.name.0.clone(),
        indexes: IndexMap::new(),
        columns: columns
            .iter()
            .map(|def| {
                debug!("visiting column: {}", def.col_name.0);
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

                let primary_key = pk.contains(&def.col_name.0);

                if !primary_key && (!nullable && default_value.is_none()) {
                    return Err(SchemaError::NotNullableColumnNeedsDefault {
                        tbl_name: tbl_name.name.0.clone(),
                        name: def.col_name.0.clone(),
                    });
                }

                if def
                    .constraints
                    .iter()
                    .any(|named| matches!(named.constraint, ColumnConstraint::ForeignKey { .. }))
                {
                    return Err(SchemaError::ForeignKey {
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
            .collect::<Result<IndexMap<_, _>, SchemaError>>()?,
        pk,
        raw: CreateTableBody::ColumnsAndConstraints {
            columns: columns.clone(),
            constraints: constraints.cloned(),
            options: options.clone(),
        },
    }))
}
