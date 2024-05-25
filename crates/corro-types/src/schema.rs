use std::{
    collections::{HashMap, HashSet},
    fmt,
    time::{Instant, SystemTime},
};

use enquote::unquote;
use fallible_iterator::FallibleIterator;
use indexmap::{IndexMap, IndexSet};
use rusqlite::{Connection, Transaction};
use serde::{Deserialize, Serialize};
use sqlite3_parser::ast::{
    Cmd, ColumnConstraint, ColumnDefinition, CreateTableBody, Expr, Name, NamedTableConstraint,
    QualifiedName, SortedColumn, Stmt, TableConstraint, TableOptions, ToTokens,
};
use tracing::{debug, info, trace};

use crate::agent::create_clock_change_trigger;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Column {
    pub name: String,
    pub sql_type: (SqliteType, Option<String>),
    pub nullable: bool,
    pub default_value: Option<String>,
    pub generated: Option<String>,
    pub primary_key: bool,
    pub raw: ColumnDefinition,
}

impl Column {
    pub fn sql_type(&self) -> (SqliteType, Option<&str>) {
        (self.sql_type.0, self.sql_type.1.as_deref())
    }
}

impl std::hash::Hash for Column {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.sql_type.hash(state);
        self.nullable.hash(state);
        self.default_value.hash(state);
        self.generated.hash(state);
        self.primary_key.hash(state);
    }
}

impl fmt::Display for Column {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.raw.to_fmt(f)
    }
}

/// SQLite data types.
/// See [Fundamental Datatypes](https://sqlite.org/c3ref/c_blob.html).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SqliteType {
    /// NULL
    Null,
    /// String
    Text,
    /// Integer or Real (default affinity)
    Numeric,
    /// 64-bit signed integer
    Integer,
    /// 64-bit IEEE floating point number
    Real,
    /// BLOB
    Blob,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub pk: IndexSet<String>,
    pub columns: IndexMap<String, Column>,
    pub indexes: IndexMap<String, Index>,
    pub raw: CreateTableBody,
}

impl fmt::Display for Table {
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
pub struct Index {
    pub name: String,
    pub tbl_name: String,
    pub columns: Vec<SortedColumn>,
    pub where_clause: Option<Expr>,
    pub unique: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: IndexMap<String, Table>,
}

impl Schema {
    pub fn constrain(&mut self) -> Result<(), ConstrainedSchemaError> {
        self.tables.retain(|name, _table| {
            !(name.contains("crsql") && name.contains("sqlite") && name.starts_with("__corro"))
        });

        for (tbl_name, table) in self.tables.iter() {
            // this should always be the case...
            if let CreateTableBody::ColumnsAndConstraints {
                columns: _,
                constraints,
                options: _,
            } = &table.raw
            {
                if let Some(constraints) = constraints {
                    for named in constraints.iter() {
                        if let TableConstraint::PrimaryKey { columns, .. } = &named.constraint {
                            for column in columns.iter() {
                                if !matches!(column.expr, Expr::Id(_)) {
                                    return Err(ConstrainedSchemaError::PrimaryKeyExpr);
                                }
                            }
                        }
                    }
                }
            } else {
                // error here!
            }

            for (name, column) in table.columns.iter() {
                if !column.primary_key && !column.nullable && column.default_value.is_none() {
                    return Err(ConstrainedSchemaError::NotNullableColumnNeedsDefault {
                        tbl_name: tbl_name.clone(),
                        name: name.clone(),
                    });
                }

                if column
                    .raw
                    .constraints
                    .iter()
                    .any(|named| matches!(named.constraint, ColumnConstraint::ForeignKey { .. }))
                {
                    return Err(ConstrainedSchemaError::ForeignKey {
                        tbl_name: tbl_name.clone(),
                        name: name.clone(),
                    });
                }
            }

            for (name, index) in table.indexes.iter() {
                if index.unique {
                    return Err(ConstrainedSchemaError::UniqueIndex(name.clone()));
                }
            }
        }

        Ok(())
    }

    pub fn create_changes_view(&self, conn: &Connection) -> rusqlite::Result<()> {
        conn.execute_batch(&format!(
            "DROP VIEW IF EXISTS __corro_changes; {}",
            self.view_stmt()
        ))
    }

    fn view_stmt(&self) -> String {
        if self.tables.is_empty() {
            return r#"CREATE VIEW __corro_changes AS SELECT
                NULL as "table",
                NULL as pk,
                NULL as cid,
                NULL as val,
                NULL as col_version,
                NULL as db_version,
                NULL as seq,
                NULL as site_id,
                NULL as cl
                FROM sqlite_schema
                LIMIT 0
                "#
            .into();
        }

        let unions: Vec<String> = self
            .tables
            .iter()
            .map(|(name, table)| {
                let pk_list = table
                    .pk
                    .iter()
                    .map(|pk| format!("pk_tbl.{pk}"))
                    .collect::<Vec<_>>()
                    .join(",");

                let pk_names = table
                .pk
                .iter().map(|pk| format!("'{}'", escape_ident_as_value(pk))).collect::<Vec<_>>().join(",");

                format!(
                    "SELECT
                      '{table_name_val}' as tbl,
                      crsql_pack_columns({pk_list}) as pk,
                      t1.col_name as cid,
                      corro_get_value('{table_name_val}', t1.col_name, {pk_names}, {pk_list}) as val,
                      t1.col_version as col_version,
                      t1.db_version as db_version,
                      site_tbl.site_id as site_id,
                      t1.seq as seq,
                      COALESCE(t2.col_version, 1) as cl
                  FROM \"{table_name_ident}__crsql_clock\" AS t1
                  JOIN \"{table_name_ident}__crsql_pks\" AS pk_tbl ON t1.key = pk_tbl.__crsql_key
                  LEFT JOIN crsql_site_id AS site_tbl ON t1.site_id = site_tbl.ordinal
                  LEFT JOIN \"{table_name_ident}__crsql_clock\" AS t2 ON
                  t1.key = t2.key AND t2.col_name = '{sentinel}'",
                    table_name_val = escape_ident_as_value(name),
                    table_name_ident = escape_ident(name),
                    sentinel = "-1"
                )
            })
            .collect();

        format!(
            r#"CREATE VIEW __corro_changes AS SELECT tbl AS "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM ({})"#,
            unions.join(" UNION ALL ")
        )
    }
}

pub fn escape_ident(ident: &str) -> String {
    ident.replace('"', "\"\"")
}

pub fn escape_ident_as_value(ident: &str) -> String {
    ident.replace('\'', "''")
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
    #[error("unsupported command: {0}")]
    UnsupportedCmd(Cmd),
    #[error("missing table for index (table: '{tbl_name}', index: '{name}')")]
    IndexWithoutTable { tbl_name: String, name: String },
    #[error("temporary tables are not supported: {0}")]
    TemporaryTable(Cmd),
}

#[derive(Debug, thiserror::Error)]
pub enum ConstrainedSchemaError {
    #[error("unique indexes are not supported: {0}")]
    UniqueIndex(String),
    #[error("table as select arenot supported: {0}")]
    TableAsSelect(Cmd),
    #[error("not nullable column '{name}' on table '{tbl_name}' needs a default value for forward schema compatibility")]
    NotNullableColumnNeedsDefault { tbl_name: String, name: String },
    #[error("foreign keys are not supported (table: '{tbl_name}', column: '{name}')")]
    ForeignKey { tbl_name: String, name: String },
    #[error("expr used as primary")]
    PrimaryKeyExpr,
}

#[allow(clippy::result_large_err)]
pub fn init_schema(conn: &Connection) -> Result<Schema, SchemaError> {
    let mut dump = String::new();

    let tables: HashMap<String, String> = conn
        .prepare(r#"SELECT name, sql FROM __corro_schema WHERE type = "table" ORDER BY tbl_name"#)?
        .query_map((), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    for sql in tables.values() {
        dump.push_str(sql.as_str());
        dump.push(';');
    }

    let indexes: HashMap<String, String> = conn
        .prepare(r#"SELECT name, sql FROM __corro_schema WHERE type = "index" ORDER BY tbl_name"#)?
        .query_map((), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    for sql in indexes.values() {
        dump.push_str(sql.as_str());
        dump.push(';');
    }

    parse_sql(dump.as_str())
}

#[derive(Debug, thiserror::Error)]
pub enum ApplySchemaError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    ConstrainedSchema(#[from] ConstrainedSchemaError),
    #[error("won't drop table without the destructive flag set (table: '{0}')")]
    DropTableWithoutDestructiveFlag(String),
    #[error("won't remove column without the destructive flag set (table: '{0}', column: '{1}')")]
    RemoveColumnWithoutDestructiveFlag(String, String),
    #[error("can't add a primary key (table: '{0}', column: '{1}')")]
    AddPrimaryKey(String, String),
    #[error("can't modify primary keys (table: '{0}')")]
    ModifyPrimaryKeys(String),

    #[error("tried importing an existing schema for table '{0}' due to a failed CREATE TABLE but didn't find anything (this should never happen)")]
    ImportedSchemaNotFound(String),

    #[error("existing schema for table '{tbl_name}' primary keys mismatched, expected: {expected:?}, got: {got:?}")]
    ImportedSchemaPkMismatch {
        tbl_name: String,
        expected: IndexSet<String>,
        got: IndexSet<String>,
    },

    #[error("existing schema for table '{tbl_name}' columns mismatched, expected: {expected:?}, got: {got:?}")]
    ImportedSchemaColumnsMismatch {
        tbl_name: String,
        expected: IndexMap<String, Column>,
        got: IndexMap<String, Column>,
    },
}

#[allow(clippy::result_large_err)]
pub fn apply_schema(
    tx: &Transaction,
    schema: &Schema,
    new_schema: &mut Schema,
) -> Result<(), ApplySchemaError> {
    if let Some(name) = schema
        .tables
        .keys()
        .collect::<HashSet<_>>()
        .difference(&new_schema.tables.keys().collect::<HashSet<_>>())
        .next()
    {
        // TODO: add options and check flag
        return Err(ApplySchemaError::DropTableWithoutDestructiveFlag(
            (*name).clone(),
        ));
    }

    let mut schema_to_merge = Schema::default();

    {
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
            let create_table_res = tx.execute_batch(
                &Cmd::Stmt(Stmt::CreateTable {
                    temporary: false,
                    if_not_exists: false,
                    tbl_name: QualifiedName::single(Name(name.clone())),
                    body: table.raw.clone(),
                })
                .to_string(),
            );

            if let Err(e) = create_table_res {
                debug!("could not create table '{name}', trying to reconcile schema if table already exists");
                let sql: Vec<String> = tx
                .prepare(
                    "SELECT sql FROM sqlite_schema WHERE tbl_name = ? AND type IN ('table', 'index') AND name IS NOT NULL AND sql IS NOT NULL")?.query_map(
                    [name],
                    |row| row.get(0),
                )?.collect::<rusqlite::Result<Vec<_>>>()?;

                if sql.is_empty() {
                    return Err(e.into());
                }

                let sql = sql.join(";");
                info!("found existing schema for '{name}'");

                let parsed_table = parse_sql(&sql)?
                    .tables
                    .remove(name)
                    .ok_or_else(|| ApplySchemaError::ImportedSchemaNotFound(name.clone()))?;

                if parsed_table.pk != table.pk {
                    return Err(ApplySchemaError::ImportedSchemaPkMismatch {
                        tbl_name: name.clone(),
                        expected: table.pk.clone(),
                        got: parsed_table.pk,
                    });
                }

                if parsed_table.columns != table.columns {
                    return Err(ApplySchemaError::ImportedSchemaColumnsMismatch {
                        tbl_name: name.clone(),
                        expected: table.columns.clone(),
                        got: parsed_table.columns,
                    });
                }

                schema_to_merge.tables.insert(name.clone(), parsed_table);
            }

            tx.execute_batch(&format!("SELECT crsql_as_crr('{name}'); CREATE INDEX IF NOT EXISTS corro_{name}__crsql_clock_site_id_dbv ON {name}__crsql_clock (site_id, db_version);"))?;

            if schema_to_merge.tables.contains_key(name) {
                // just merged!
                continue;
            }

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

            // create the trigger for this table, in case it does not exist
            create_clock_change_trigger(tx, name)?;
        }
    }

    for (name, table) in schema_to_merge.tables {
        new_schema.tables.insert(name, table);
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

        if let Some(col_name) = dropped_cols.into_iter().next() {
            return Err(ApplySchemaError::RemoveColumnWithoutDestructiveFlag(
                name.clone(),
                col_name.clone(),
            ));
        }

        // 2. check for changed columns

        let changed_cols: HashMap<String, Column> = table
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

        if changed_cols.is_empty() {
            // 2.1. no changed columns, add missing ones

            if new_col_names.is_empty() {
                // nothing to do
            } else {
                info!("Altering crsql for table {}", table.name);
                let start = Instant::now();
                let new_cols = new_table
                    .columns
                    .iter()
                    .filter(|(col_name, _)| new_col_names.contains(col_name))
                    .collect::<Vec<_>>();

                // if all columns are generated, we don't need a migration
                let require_migration = !new_cols.iter().all(|(_, col)| col.generated.is_some());

                if require_migration {
                    tx.execute_batch(&format!("SELECT crsql_begin_alter('{name}');"))?;
                }

                for (col_name, col) in new_cols {
                    info!("adding column '{col_name}'");
                    if col.primary_key {
                        return Err(ApplySchemaError::AddPrimaryKey(
                            name.clone(),
                            col_name.clone(),
                        ));
                    }
                    if !col.nullable && col.default_value.is_none() {
                        return Err(ConstrainedSchemaError::NotNullableColumnNeedsDefault {
                            tbl_name: name.clone(),
                            name: col_name.clone(),
                        }
                        .into());
                    }
                    tx.execute_batch(&format!("ALTER TABLE {name} ADD COLUMN {}", col))?;
                }

                if require_migration {
                    tx.execute_batch(&format!("SELECT crsql_commit_alter('{name}');"))?;
                }
                info!(
                    "Altering crsql for table {} took {:?}",
                    table.name,
                    start.elapsed()
                );
            }
        } else {
            // 2.2 we do have changed columns, try to do something about that

            info!("Columns have changed... replacing table {}", table.name);
            let start = Instant::now();

            let primary_keys = table
                .columns
                .values()
                .filter_map(|col| col.primary_key.then_some(&col.name))
                .collect::<Vec<&String>>();

            let new_primary_keys = new_table
                .columns
                .values()
                .filter_map(|col| col.primary_key.then_some(&col.name))
                .collect::<Vec<&String>>();

            if primary_keys != new_primary_keys {
                return Err(ApplySchemaError::ModifyPrimaryKeys(name.clone()));
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
            info!("Replacing table {} took {:?}", table.name, start.elapsed());
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

    new_schema.create_changes_view(&tx)?;

    Ok(())
}

#[allow(clippy::result_large_err)]
pub fn parse_sql_to_schema(schema: &mut Schema, sql: &str) -> Result<(), SchemaError> {
    trace!("parsing {sql}");
    let mut parser = sqlite3_parser::lexer::sql::Parser::new(sql.as_bytes());

    loop {
        match parser.next() {
            Ok(None) => break,
            Err(err) => {
                eprintln!("Err: {err}");
                return Err(err.into());
            }
            Ok(Some(ref cmd @ Cmd::Stmt(ref stmt))) => match stmt {
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
                    let table = prepare_table(tbl_name, columns, constraints.as_ref(), options);
                    schema.tables.insert(table.name.clone(), table);
                    trace!("inserted table: {}", tbl_name.name.0);
                }
                Stmt::CreateIndex {
                    unique,
                    idx_name,
                    tbl_name,
                    columns,
                    where_clause,
                    ..
                } => {
                    let tbl_name =
                        unquote(tbl_name.0.as_str()).unwrap_or_else(|_| tbl_name.0.clone());
                    let idx_name = unquote(idx_name.name.0.as_str())
                        .unwrap_or_else(|_| idx_name.name.0.clone());
                    if let Some(table) = schema.tables.get_mut(tbl_name.as_str()) {
                        table.indexes.insert(
                            idx_name.clone(),
                            Index {
                                name: idx_name,
                                tbl_name,
                                columns: columns.to_vec(),
                                where_clause: where_clause.clone(),
                                unique: *unique,
                            },
                        );
                    } else {
                        return Err(SchemaError::IndexWithoutTable {
                            tbl_name: tbl_name.clone(),
                            name: idx_name.clone(),
                        });
                    }
                }
                _ => return Err(SchemaError::UnsupportedCmd(cmd.clone())),
            },
            Ok(Some(cmd)) => return Err(SchemaError::UnsupportedCmd(cmd)),
        }
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
pub fn parse_sql(sql: &str) -> Result<Schema, SchemaError> {
    let mut schema = Schema::default();

    parse_sql_to_schema(&mut schema, sql)?;

    Ok(schema)
}

#[allow(clippy::result_large_err)]
fn prepare_table(
    tbl_name: &QualifiedName,
    columns: &[ColumnDefinition],
    constraints: Option<&Vec<NamedTableConstraint>>,
    options: &TableOptions,
) -> Table {
    let pk = constraints
        .and_then(|constraints| {
            constraints
                .iter()
                .find_map(|named| match &named.constraint {
                    TableConstraint::PrimaryKey { columns, .. } => Some(
                        columns
                            .iter()
                            .filter_map(|col| match &col.expr {
                                Expr::Id(id) => {
                                    Some(unquote(&id.0).unwrap_or_else(|_| id.0.clone()))
                                }
                                _ => None,
                            })
                            .collect::<IndexSet<_>>(),
                    ),
                    _ => None,
                })
        })
        .unwrap_or_else(|| {
            columns
                .iter()
                .filter(|&def| {
                    def.constraints.iter().any(|named| {
                        matches!(named.constraint, ColumnConstraint::PrimaryKey { .. })
                    })
                })
                .map(|def| unquote(&def.col_name.0).unwrap_or_else(|_| def.col_name.0.clone()))
                .collect()
        });

    Table {
        name: unquote(&tbl_name.name.0).unwrap_or_else(|_| tbl_name.name.0.clone()),
        indexes: IndexMap::new(),
        columns: columns
            .iter()
            .map(|def| {
                trace!("visiting column: {}", def.col_name.0);
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

                let col_name = unquote(&def.col_name.0).unwrap_or_else(|_| def.col_name.0.clone());

                (
                    col_name.clone(),
                    Column {
                        name: col_name.clone(),
                        sql_type: match def.col_type.as_ref().map(|t| t.name.to_ascii_uppercase()) {
                            // 1. If the declared type contains the string "INT" then it is assigned INTEGER affinity.
                            Some(s) if s.contains("INT") => (SqliteType::Integer, Some(s)),
                            // 2. If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
                            Some(s)
                                if s.contains("CHAR")
                                    || s.contains("CLOB")
                                    || s.contains("TEXT")
                                    || s == "JSON" =>
                            {
                                (SqliteType::Text, Some(s))
                            }

                            // 3. If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
                            Some(s) if s.contains("BLOB") || s == "JSONB" => {
                                (SqliteType::Blob, Some(s))
                            }
                            None => (SqliteType::Blob, None),

                            // 4. If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
                            Some(s)
                                if s.contains("REAL")
                                    || s.contains("FLOA")
                                    || s.contains("DOUB")
                                    || s == "ANY" =>
                            {
                                (SqliteType::Real, Some(s))
                            }

                            // 5. Otherwise, the affinity is NUMERIC.
                            Some(s) => (SqliteType::Numeric, Some(s)),
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
                )
            })
            .collect::<IndexMap<_, _>>(),
        pk,
        raw: CreateTableBody::ColumnsAndConstraints {
            columns: columns.to_vec(),
            constraints: constraints.cloned(),
            options: *options,
        },
    }
}
