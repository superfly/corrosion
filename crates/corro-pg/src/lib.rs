mod codec;
pub mod sql_state;
mod ssl;
pub mod utils;
mod vtab;

use codec::VecFromSqlText;
use eyre::WrapErr;
use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fmt,
    net::SocketAddr,
    rc::Rc,
    str::{FromStr, Utf8Error},
    sync::{
        atomic::{AtomicI32, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::NaiveDateTime;
use compact_str::CompactString;
use corro_types::{
    agent::{Agent, BookieWriteGuard, ChangeError},
    broadcast::{broadcast_changes, Timestamp},
    change::{insert_local_changes, InsertChangesInfo},
    config::PgConfig,
    persistent_gauge,
    schema::{parse_sql, Column, Schema, SchemaError, SqliteType, Table},
    sqlite::CrConn,
};
use fallible_iterator::FallibleIterator;
use futures::{SinkExt, StreamExt};
use metrics::counter;
use pgwire::{
    api::results::{DataRowEncoder, FieldFormat, FieldInfo, Tag},
    api::DefaultClient,
    error::{ErrorInfo, PgWireError},
    messages::{
        data::{NoData, ParameterDescription, RowDescription},
        extendedquery::{BindComplete, CloseComplete, ParseComplete, PortalSuspended},
        response::{
            CommandComplete, EmptyQueryResponse, ErrorResponse, ReadyForQuery, TransactionStatus,
        },
        startup::ParameterStatus,
        PgWireBackendMessage, PgWireFrontendMessage,
    },
    tokio::server::PgWireMessageServerCodec,
    types::{format::FormatOptions, FromSqlText},
};
use postgres_types::{FromSql, Type};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rusqlite::{
    ffi::SQLITE_CONSTRAINT_UNIQUE,
    functions::{Aggregate, Context as SqliteCtx, FunctionFlags},
    types::ValueRef,
    vtab::eponymous_only_module,
    Connection, Statement,
};
use rustls::ServerConfig;
use socket2::{SockRef, TcpKeepalive};
use spawn::spawn_counted;
use sqlite3_parser::ast::{
    As, Cmd, ColumnDefinition, CreateTableBody, Expr, FromClause, Id, InsertBody, Limit, Literal,
    Name, OneSelect, QualifiedName, ResultColumn, Select, SelectBody, SelectTable, Stmt, With,
};
use sqlparser::ast::Statement as PgStatement;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{channel, Sender},
        AcquireError, OwnedSemaphorePermit, RwLock as TokioRwLock,
    },
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::{codec::Framed, either::Either, sync::CancellationToken};
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, TimeoutFutureExt, Tripwire};

use crate::{
    sql_state::SqlState,
    utils::CountedTcpStream,
    vtab::{
        empty_catalog::{
            EmptyCatalogTable, PG_DESCRIPTION_DDL, PG_EXTENSION_DDL, PG_SHDESCRIPTION_DDL,
            PG_STATIO_USER_TABLES_DDL,
        },
        information_schema_columns::{
            load_information_schema_columns, InformationSchemaColumnsTable,
        },
        information_schema_key_column_usage::{
            load_information_schema_key_column_usage, InformationSchemaKeyColumnUsageTable,
        },
        information_schema_table_constraints::{
            load_information_schema_table_constraints, InformationSchemaTableConstraintsTable,
        },
        information_schema_tables::{
            load_information_schema_table_names, InformationSchemaTablesTable,
        },
        information_schema_triggers::{
            load_information_schema_triggers, InformationSchemaTriggersTable,
        },
        pg_am::PgAmTable,
        pg_attribute::{load_pg_attributes, PgAttributeTable},
        pg_class::{load_pg_class_entries, PgClassTable},
        pg_constraint::{load_pg_constraint_entries, PgConstraintTable},
        pg_database::{PgDatabase, PgDatabaseTable},
        pg_index::{load_pg_index_entries, PgIndexTable},
        pg_language::{load_pg_language_entries, PgLanguageTable},
        pg_namespace::PgNamespaceTable,
        pg_proc::{load_pg_proc_entries, PgProcTable},
        pg_range::PgRangeTable,
        pg_type::PgTypeTable,
    },
};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub struct PgServer {
    pub local_addr: SocketAddr,
}

enum BackendResponse {
    Message {
        message: PgWireBackendMessage,
        flush: bool,
    },
    Flush,
}

impl From<(PgWireBackendMessage, bool)> for BackendResponse {
    fn from((message, flush): (PgWireBackendMessage, bool)) -> Self {
        Self::Message { message, flush }
    }
}

#[derive(Clone, Copy, Debug)]
enum StmtTag {
    Select,
    InsertAsSelect,

    Insert,
    Update,
    Delete,

    Alter,
    Analyze,
    Attach,
    Begin,
    Commit,
    Create,
    Detach,
    Drop,
    Pragma,
    Reindex,
    Release,
    Rollback,
    Savepoint,
    Vacuum,

    Other,
}

impl StmtTag {
    fn into_command_complete(self, rows: usize, changes: usize) -> CommandComplete {
        if self.returns_num_rows() {
            self.tag(Some(rows)).into()
        } else if self.returns_rows_affected() {
            if matches!(self, StmtTag::Insert) {
                CommandComplete::new(format!("INSERT 0 {changes}"))
            } else {
                self.tag(Some(changes)).into()
            }
        } else {
            self.tag(None).into()
        }
    }

    fn returns_rows_affected(&self) -> bool {
        matches!(self, StmtTag::Insert | StmtTag::Update | StmtTag::Delete)
    }
    fn returns_num_rows(&self) -> bool {
        matches!(self, StmtTag::Select | StmtTag::InsertAsSelect)
    }
    pub fn tag(&self, rows: Option<usize>) -> Tag {
        let tag = match self {
            StmtTag::Select => Tag::new("SELECT"),
            StmtTag::InsertAsSelect | StmtTag::Insert => Tag::new("INSERT"),
            StmtTag::Update => Tag::new("UPDATE"),
            StmtTag::Delete => Tag::new("DELETE"),
            StmtTag::Alter => Tag::new("ALTER"),
            StmtTag::Analyze => Tag::new("ANALYZE"),
            StmtTag::Attach => Tag::new("ATTACH"),
            StmtTag::Begin => Tag::new("BEGIN"),
            StmtTag::Commit => Tag::new("COMMIT"),
            StmtTag::Create => Tag::new("CREATE"),
            StmtTag::Detach => Tag::new("DETACH"),
            StmtTag::Drop => Tag::new("DROP"),
            StmtTag::Pragma => Tag::new("PRAGMA"),
            StmtTag::Reindex => Tag::new("REINDEX"),
            StmtTag::Release => Tag::new("RELEASE"),
            StmtTag::Rollback => Tag::new("ROLLBACK"),
            StmtTag::Savepoint => Tag::new("SAVEPOINT"),
            StmtTag::Vacuum => Tag::new("VACUUM"),
            StmtTag::Other => Tag::new("OK"),
        };

        if let Some(r) = rows {
            tag.with_rows(r)
        } else {
            tag
        }
    }
}

enum Prepared {
    Empty,
    NonEmpty {
        sql: String,
        param_types: Vec<Type>,
        fields: Vec<FieldInfo>,
        cmd: Box<ParsedCmd>,
    },
}

enum Portal<'a> {
    Empty {
        stmt_name: CompactString,
    },
    Parsed {
        stmt_name: CompactString,
        stmt: Statement<'a>,
        result_formats: Vec<FieldFormat>,
        cmd: Box<ParsedCmd>,
    },
}

impl<'a> Portal<'a> {
    fn stmt_name(&self) -> &str {
        match self {
            Portal::Empty { stmt_name } | Portal::Parsed { stmt_name, .. } => stmt_name.as_str(),
        }
    }
}

#[derive(Clone, Debug)]
#[allow(clippy::large_enum_variant)]
enum ParsedCmd {
    Sqlite(Cmd),
    Postgres(PgStatement),
}

impl ParsedCmd {
    pub fn is_begin(&self) -> bool {
        matches!(
            self,
            ParsedCmd::Sqlite(Cmd::Stmt(Stmt::Begin(_, _)))
                | ParsedCmd::Postgres(PgStatement::StartTransaction { .. })
        )
    }

    pub fn is_commit(&self) -> bool {
        matches!(
            self,
            ParsedCmd::Sqlite(Cmd::Stmt(Stmt::Commit(_)))
                | ParsedCmd::Postgres(PgStatement::Commit { .. })
        )
    }

    pub fn is_rollback(&self) -> bool {
        matches!(self, ParsedCmd::Sqlite(Cmd::Stmt(Stmt::Rollback { .. })))
    }

    pub fn is_pg(&self) -> bool {
        matches!(self, ParsedCmd::Postgres(_))
    }

    pub fn is_show(&self) -> bool {
        matches!(self, ParsedCmd::Postgres(PgStatement::ShowVariable { .. }))
    }

    pub fn is_set(&self) -> bool {
        matches!(self, ParsedCmd::Postgres(PgStatement::Set { .. }))
    }

    fn tag(&self) -> StmtTag {
        match &self {
            ParsedCmd::Sqlite(Cmd::Stmt(stmt)) => match stmt {
                Stmt::Select(_) => StmtTag::Select,
                Stmt::CreateTable {
                    body: CreateTableBody::AsSelect(_),
                    ..
                } => StmtTag::InsertAsSelect,
                Stmt::AlterTable(_, _) => StmtTag::Alter,
                Stmt::Analyze(_) => StmtTag::Analyze,
                Stmt::Attach { .. } => StmtTag::Attach,
                Stmt::Begin(_, _) => StmtTag::Begin,
                Stmt::Commit(_) => StmtTag::Commit,
                Stmt::CreateIndex { .. }
                | Stmt::CreateTable { .. }
                | Stmt::CreateTrigger { .. }
                | Stmt::CreateView { .. }
                | Stmt::CreateVirtualTable { .. } => StmtTag::Create,
                Stmt::Delete { .. } => StmtTag::Delete,
                Stmt::Detach(_) => StmtTag::Detach,
                Stmt::DropIndex { .. }
                | Stmt::DropTable { .. }
                | Stmt::DropTrigger { .. }
                | Stmt::DropView { .. } => StmtTag::Drop,
                Stmt::Insert { .. } => StmtTag::Insert,
                Stmt::Pragma(_, _) => StmtTag::Pragma,
                Stmt::Reindex { .. } => StmtTag::Reindex,
                Stmt::Release(_) => StmtTag::Release,
                Stmt::Rollback { .. } => StmtTag::Rollback,
                Stmt::Savepoint(_) => StmtTag::Savepoint,

                Stmt::Update { .. } => StmtTag::Update,
                Stmt::Vacuum(_, _) => StmtTag::Vacuum,
            },
            ParsedCmd::Postgres(stmt) => match stmt {
                PgStatement::StartTransaction { .. } => StmtTag::Begin,
                PgStatement::Commit { .. } => StmtTag::Commit,
                _ => StmtTag::Other,
            },
            _ => StmtTag::Other,
        }
    }
}

impl fmt::Display for ParsedCmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParsedCmd::Sqlite(cmd) => cmd.fmt(f),
            ParsedCmd::Postgres(stmt) => stmt.fmt(f),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("pg: {0}")]
    Postgres(#[from] sqlparser::parser::ParserError),
}

/// Convert PostgreSQL-style backreferences (\1, \2, ...) in a replacement
/// string to the `regex` crate's format ($1, $2, ...).
fn convert_pg_backrefs(replacement: &str) -> String {
    let mut result = String::with_capacity(replacement.len());
    let mut chars = replacement.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&next) = chars.peek() {
                if next.is_ascii_digit() {
                    chars.next();
                    result.push('$');
                    result.push(next);
                    continue;
                }
            }
        }
        result.push(c);
    }
    result
}

/// Accumulator for string_agg(value, delimiter).
struct StringAggCtx {
    parts: Vec<String>,
    delimiter: String,
}

/// Aggregate for string_agg(value, delimiter).
struct StringAgg;

impl Aggregate<StringAggCtx, String> for StringAgg {
    fn init(&self, _ctx: &mut SqliteCtx<'_>) -> rusqlite::Result<StringAggCtx> {
        Ok(StringAggCtx {
            parts: Vec::new(),
            delimiter: String::new(),
        })
    }

    fn step(&self, ctx: &mut SqliteCtx<'_>, acc: &mut StringAggCtx) -> rusqlite::Result<()> {
        let value: Option<String> = ctx.get(0).unwrap_or(None);
        if acc.delimiter.is_empty() {
            acc.delimiter = ctx.get(1).unwrap_or_default();
        }
        if let Some(v) = value {
            acc.parts.push(v);
        }
        Ok(())
    }

    fn finalize(
        &self,
        _ctx: &mut SqliteCtx<'_>,
        acc: Option<StringAggCtx>,
    ) -> rusqlite::Result<String> {
        let acc = acc.unwrap_or(StringAggCtx {
            parts: Vec::new(),
            delimiter: String::new(),
        });
        Ok(acc.parts.join(&acc.delimiter))
    }
}

fn parse_query(sql: &str) -> Result<(String, VecDeque<ParsedCmd>), ParseError> {
    let mut cmds = VecDeque::new();

    let normalized = sql.trim_matches(';').trim();
    if normalized.is_empty() {
        return Ok((normalized.to_string(), cmds));
    }

    // First, try parsing with the SQLite parser directly — this is the fast
    // path for all SQLite-compatible SQL.
    let mut parser = sqlite3_parser::lexer::sql::Parser::new(normalized.as_bytes());
    loop {
        match parser.next() {
            Ok(Some(cmd)) => {
                cmds.push_back(ParsedCmd::Sqlite(cmd));
            }
            Ok(None) => {
                return Ok((normalized.to_string(), cmds));
            }
            Err(e) => {
                debug!("could not parse statement ({sql:?}) as sqlite: {e}");
                break;
            }
        }
    }

    // The SQLite parser failed.  Try parsing with the PostgreSQL parser, then
    // strip PG-specific syntax from the AST (e.g. `::type` casts and
    // `pg_catalog.function()` prefixes) and re-serialize to a SQLite-compatible
    // SQL string.  If that string can be parsed by the SQLite parser, we use
    // it — otherwise we fall back to the PG-parsed statement (which will only
    // work for a limited set of statements like BEGIN/COMMIT/SET/SHOW).

    let stmts = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::PostgreSqlDialect {},
        normalized,
    )?;

    // Try to convert each PG statement to a SQLite-compatible string.
    let mut sqlite_compat_sql = String::new();
    let mut all_convertible = true;
    for stmt in &stmts {
        match pg_stmt_to_sqlite_sql(stmt) {
            Some(converted) => {
                if !sqlite_compat_sql.is_empty() {
                    sqlite_compat_sql.push_str("; ");
                }
                sqlite_compat_sql.push_str(&converted);
            }
            None => {
                all_convertible = false;
                break;
            }
        }
    }

    if all_convertible && !sqlite_compat_sql.is_empty() {
        // Try parsing the converted SQL with the SQLite parser.
        let mut parser = sqlite3_parser::lexer::sql::Parser::new(sqlite_compat_sql.as_bytes());
        let mut sqlite_cmds = VecDeque::new();
        let mut success = true;
        loop {
            match parser.next() {
                Ok(Some(cmd)) => {
                    sqlite_cmds.push_back(ParsedCmd::Sqlite(cmd));
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    debug!(
                        "could not parse PG-converted statement ({sqlite_compat_sql:?}) as sqlite: {e}"
                    );
                    success = false;
                    break;
                }
            }
        }
        if success && !sqlite_cmds.is_empty() {
            return Ok((sqlite_compat_sql, sqlite_cmds));
        }
    }

    // Fall back to the PG-parsed statements.
    for stmt in stmts {
        cmds.push_back(ParsedCmd::Postgres(stmt));
    }

    Ok((normalized.to_string(), cmds))
}

/// Attempts to convert a `sqlparser` AST statement into a SQLite-compatible SQL
/// string by stripping PostgreSQL-specific constructs.  Returns `None` if the
/// statement type is not supported for conversion.
///
/// The main transformations are:
///   - `expr::type` casts → just `expr` (SQLite is dynamically typed)
///   - `pg_catalog.function(...)` → `function(...)` (SQLite doesn't support
///     schema-qualified function calls)
fn pg_stmt_to_sqlite_sql(stmt: &PgStatement) -> Option<String> {
    use sqlparser::ast::Statement as S;

    match stmt {
        S::Query(query) => {
            let mut query = query.clone();
            strip_pg_ast_query(&mut query);
            Some(query.to_string())
        }
        // Other statement types are handled as PG statements elsewhere
        // (BEGIN/COMMIT/SET/SHOW) or are not supported.
        _ => None,
    }
}

/// Recursively strips PG-specific syntax from a `Query` AST.
fn strip_pg_ast_query(query: &mut sqlparser::ast::Query) {
    strip_pg_ast_set_expr(&mut query.body);
    if let Some(ref mut order_by) = query.order_by {
        if let sqlparser::ast::OrderByKind::Expressions(exprs) = &mut order_by.kind {
            for item in exprs.iter_mut() {
                strip_pg_ast_expr(&mut item.expr);
            }
        }
    }
}

/// Recursively strips PG-specific syntax from a `SetExpr`.
fn strip_pg_ast_set_expr(set_expr: &mut sqlparser::ast::SetExpr) {
    use sqlparser::ast::SetExpr;
    match set_expr {
        SetExpr::Select(select) => {
            for item in select.projection.iter_mut() {
                strip_pg_ast_select_item(item);
            }
            for table_with_joins in select.from.iter_mut() {
                strip_pg_ast_table_with_joins(table_with_joins);
            }
            if let Some(ref mut expr) = select.selection {
                strip_pg_ast_expr(expr);
            }
            if let Some(ref mut expr) = select.having {
                strip_pg_ast_expr(expr);
            }
        }
        SetExpr::Query(query) => strip_pg_ast_query(query),
        SetExpr::SetOperation { left, right, .. } => {
            strip_pg_ast_set_expr(left);
            strip_pg_ast_set_expr(right);
        }
        _ => {}
    }
}

/// Quotes an identifier if it is a SQLite reserved keyword, so that it can be
/// used as an alias or column name without causing a syntax error.
fn quote_sqlite_keyword(ident: &mut sqlparser::ast::Ident) {
    if ident.quote_style.is_some() {
        return; // already quoted
    }
    if is_sqlite_keyword(&ident.value) {
        ident.quote_style = Some('"');
    }
}

/// Returns true if the given identifier (case-insensitive) is a SQLite reserved
/// keyword that cannot be used as an unquoted alias.
fn is_sqlite_keyword(s: &str) -> bool {
    // This is not the full SQLite keyword list — just the ones that are
    // commonly used as column/alias names by PostgreSQL tools like TablePlus
    // and that conflict with SQLite's parser.
    matches!(
        s.to_ascii_uppercase().as_str(),
        "CHECK"
            | "CONSTRAINT"
            | "KEY"
            | "INDEX"
            | "TABLE"
            | "COLUMN"
            | "DEFAULT"
            | "NULL"
            | "PRIMARY"
            | "UNIQUE"
            | "FOREIGN"
            | "REFERENCES"
            | "CAST"
            | "COLLATE"
            | "CONFLICT"
            | "FILTER"
            | "ROW"
            | "ROWS"
            | "VALUES"
            | "VIEW"
            | "TEMP"
            | "TEMPORARY"
            | "TRIGGER"
            | "BEGIN"
            | "COMMIT"
            | "ROLLBACK"
            | "END"
            | "EXPLAIN"
            | "PRAGMA"
            | "REPLACE"
            | "UNION"
            | "EXCEPT"
            | "INTERSECT"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "INNER"
            | "OUTER"
            | "CROSS"
            | "NATURAL"
            | "JOIN"
            | "ON"
            | "USING"
            | "GROUP"
            | "ORDER"
            | "HAVING"
            | "WHERE"
            | "FROM"
            | "INTO"
            | "SET"
            | "BY"
            | "AS"
            | "AND"
            | "OR"
            | "NOT"
            | "IN"
            | "IS"
            | "LIKE"
            | "GLOB"
            | "BETWEEN"
            | "ESCAPE"
            | "EXISTS"
            | "DISTINCT"
            | "ALL"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "IF"
            | "MATCH"
            | "ASC"
            | "DESC"
            | "LIMIT"
            | "OFFSET"
            | "AUTOINCREMENT"
            | "ADD"
            | "ALTER"
            | "DROP"
            | "RENAME"
            | "TO"
            | "CREATE"
            | "SELECT"
            | "INSERT"
            | "UPDATE"
            | "DELETE"
            | "WITH"
            | "RECURSIVE"
            | "MATERIALIZED"
            | "WINDOW"
            | "OVER"
            | "PARTITION"
            | "RETURNING"
            | "RAISE"
            | "ABORT"
            | "ACTION"
            | "AFTER"
            | "BEFORE"
            | "FAIL"
            | "IGNORE"
            | "RESTRICT"
            | "CASCADE"
            | "DEFERRABLE"
            | "DEFERRED"
            | "IMMEDIATE"
            | "INITIALLY"
            | "DEFER"
            | "EXCLUSIVE"
            | "SHARED"
            | "UNLOCKED"
    )
}

/// Builds a JSON array string from a slice of strings, properly escaping
/// each element.  Used by `string_to_array` and `parse_ident` to represent
/// PG arrays as JSON arrays (since SQLite has no native array type).
fn json_array(parts: &[String]) -> String {
    let items: Vec<String> = parts
        .iter()
        .map(|s| {
            let escaped = s
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t");
            format!("\"{escaped}\"")
        })
        .collect();
    format!("[{}]", items.join(","))
}

/// Converts a PG array literal like `{a,b,c}` to a JSON array string.
fn pg_array_to_json(s: &str) -> String {
    let trimmed = s.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return format!("[\"{}\"]", trimmed.replace('"', "\\\""));
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    if inner.is_empty() {
        return "[]".to_string();
    }
    let parts: Vec<String> = inner.split(',').map(String::from).collect();
    json_array(&parts)
}

/// Parses a PostgreSQL qualified identifier string (e.g. `"schema"."table"`
/// or `schema.table`) into its component parts.
fn parse_pg_ident(s: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut chars = s.chars().peekable();
    while chars.peek().is_some() {
        // Skip whitespace
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }
        if chars.peek() == Some(&'"') {
            // Quoted identifier
            chars.next(); // consume opening quote
            let mut part = String::new();
            while let Some(&c) = chars.peek() {
                chars.next();
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        // Escaped double quote
                        chars.next();
                        part.push('"');
                    } else {
                        break;
                    }
                } else {
                    part.push(c);
                }
            }
            parts.push(part);
        } else {
            // Unquoted identifier — read until dot or whitespace
            let mut part = String::new();
            while let Some(&c) = chars.peek() {
                if c == '.' || c.is_whitespace() {
                    break;
                }
                part.push(c);
                chars.next();
            }
            parts.push(part);
        }
        // Skip whitespace
        while chars.peek().is_some_and(|c| c.is_whitespace()) {
            chars.next();
        }
        // Consume dot separator
        if chars.peek() == Some(&'.') {
            chars.next();
        }
    }
    parts
}

/// Strips PG-specific syntax from a `SelectItem`.
fn strip_pg_ast_select_item(item: &mut sqlparser::ast::SelectItem) {
    use sqlparser::ast::SelectItem;
    match item {
        SelectItem::ExprWithAlias { expr, alias } => {
            strip_pg_ast_expr(expr);
            quote_sqlite_keyword(alias);
        }
        SelectItem::UnnamedExpr(expr) => strip_pg_ast_expr(expr),
        _ => {}
    }
}

/// Strips PG-specific syntax from a `TableWithJoins`.
fn strip_pg_ast_table_with_joins(table_with_joins: &mut sqlparser::ast::TableWithJoins) {
    strip_pg_ast_table_factor(&mut table_with_joins.relation);
    for join in table_with_joins.joins.iter_mut() {
        strip_pg_ast_table_factor(&mut join.relation);
        match &mut join.join_operator {
            sqlparser::ast::JoinOperator::Join(constraint)
            | sqlparser::ast::JoinOperator::Inner(constraint)
            | sqlparser::ast::JoinOperator::Left(constraint)
            | sqlparser::ast::JoinOperator::LeftOuter(constraint)
            | sqlparser::ast::JoinOperator::Right(constraint)
            | sqlparser::ast::JoinOperator::RightOuter(constraint)
            | sqlparser::ast::JoinOperator::FullOuter(constraint)
            | sqlparser::ast::JoinOperator::Semi(constraint)
            | sqlparser::ast::JoinOperator::LeftSemi(constraint)
            | sqlparser::ast::JoinOperator::RightSemi(constraint)
            | sqlparser::ast::JoinOperator::Anti(constraint)
            | sqlparser::ast::JoinOperator::LeftAnti(constraint)
            | sqlparser::ast::JoinOperator::RightAnti(constraint) => {
                if let sqlparser::ast::JoinConstraint::On(expr) = constraint {
                    strip_pg_ast_expr(expr);
                }
            }
            _ => {}
        }
    }
}

/// Strips PG-specific syntax from a `TableFactor` (mainly schema-qualified
/// function calls in table function context).
fn strip_pg_ast_table_factor(table_factor: &mut sqlparser::ast::TableFactor) {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, TableFactor};

    // Helper: convert a scalar function used as a table factor into a
    // derived subquery `(SELECT func(args) AS col) alias`.
    // Also converts `generate_series(start, end) AS i` to
    // `(SELECT value AS i FROM generate_series(start, end)) AS i`
    // so that the column is accessible by the alias name.
    // Returns true if the conversion was performed.
    fn try_convert_scalar_table_func(
        table_factor: &mut TableFactor,
        name: &sqlparser::ast::ObjectName,
        args: &[FunctionArg],
        alias: &Option<sqlparser::ast::TableAlias>,
    ) -> bool {
        let func_name = name
            .0
            .last()
            .and_then(|p| p.as_ident())
            .map(|p| p.value.to_ascii_lowercase())
            .unwrap_or_default();
        const SCALAR_FUNCS: &[&str] = &[
            "string_to_array",
            "current_setting",
            "parse_ident",
            "array_to_string",
        ];

        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| "col".to_string());

        let subquery_sql = if SCALAR_FUNCS.contains(&func_name.as_str()) {
            // Scalar function: wrap in (SELECT func(args) AS alias_name)
            let func_call = format!(
                "{}({})",
                name,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            format!("SELECT {} AS \"{}\"", func_call, alias_name)
        } else if func_name == "generate_series" {
            // generate_series: wrap in (SELECT value AS alias_name FROM generate_series(...))
            let func_call = format!(
                "{}({})",
                name,
                args.iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            format!("SELECT value AS \"{}\" FROM {}", alias_name, func_call)
        } else {
            return false;
        };

        // Parse the subquery and replace the table factor
        if let Ok(sub_stmts) = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            &subquery_sql,
        ) {
            if let Some(sqlparser::ast::Statement::Query(sub_query)) = sub_stmts.into_iter().next()
            {
                *table_factor = TableFactor::Derived {
                    lateral: false,
                    subquery: sub_query,
                    alias: alias.clone(),
                };
                return true;
            }
        }
        false
    }

    match table_factor {
        TableFactor::Function {
            name, args, alias, ..
        } => {
            strip_pg_catalog_prefix(name);
            // Process args first
            for arg in args.iter_mut() {
                let func_arg_expr = match arg {
                    FunctionArg::Named { arg, .. } => arg,
                    FunctionArg::ExprNamed { arg, .. } => arg,
                    FunctionArg::Unnamed(expr) => expr,
                };
                if let FunctionArgExpr::Expr(e) = func_arg_expr {
                    strip_pg_ast_expr(e);
                }
            }
            let name_clone = name.clone();
            let args_clone = args.clone();
            let alias_clone = alias.clone();
            try_convert_scalar_table_func(table_factor, &name_clone, &args_clone, &alias_clone);
        }
        TableFactor::Table {
            name,
            args: Some(table_args),
            alias,
            ..
        } => {
            strip_pg_catalog_prefix(name);
            // Process args
            for arg in table_args.args.iter_mut() {
                let func_arg_expr = match arg {
                    FunctionArg::Named { arg, .. } => arg,
                    FunctionArg::ExprNamed { arg, .. } => arg,
                    FunctionArg::Unnamed(expr) => expr,
                };
                if let FunctionArgExpr::Expr(e) = func_arg_expr {
                    strip_pg_ast_expr(e);
                }
            }
            let name_clone = name.clone();
            let args_clone = table_args.args.clone();
            let alias_clone = alias.clone();
            try_convert_scalar_table_func(table_factor, &name_clone, &args_clone, &alias_clone);
        }
        TableFactor::Table {
            name: _,
            args: None,
            ..
        } => {
            // Don't strip pg_catalog. prefix from regular table references —
            // SQLite supports schema.table syntax and the pg_catalog vtabs
            // are registered with that schema prefix.
        }
        TableFactor::Derived { subquery, .. } => {
            strip_pg_ast_query(subquery);
        }
        _ => {}
    }
}

/// Strips the `pg_catalog.` (or other schema) prefix from an `ObjectName` if
/// it has more than one part, leaving just the last part (the function name).
fn strip_pg_catalog_prefix(name: &mut sqlparser::ast::ObjectName) {
    if name.0.len() > 1 {
        // Keep only the last part (the function/table name).
        let last = name.0.pop().unwrap();
        name.0 = vec![last];
    }
}

/// Recursively strips PG-specific syntax from function arguments.
fn strip_pg_ast_function_args(args: &mut sqlparser::ast::FunctionArguments) {
    use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
    if let FunctionArguments::List(arg_list) = args {
        for arg in arg_list.args.iter_mut() {
            let func_arg_expr = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::ExprNamed { arg, .. } => arg,
                FunctionArg::Unnamed(expr) => expr,
            };
            if let FunctionArgExpr::Expr(e) = func_arg_expr {
                strip_pg_ast_expr(e);
            }
        }
    }
}
/// Maps a PostgreSQL `DataType` to a SQLite-compatible `DataType`.
///
/// SQLite's CAST only cares about type affinity (INTEGER, REAL, TEXT, BLOB,
/// NUMERIC), but the corrosion engine's `name_to_type` function maps a
/// specific set of type names to PostgreSQL wire types.  This function
/// converts PG-specific type aliases (e.g. `INT8`, `FLOAT8`) to the
/// canonical names that `name_to_type` recognizes (e.g. `BIGINT`, `DOUBLE
/// PRECISION`).
fn pg_data_type_to_sqlite(dt: sqlparser::ast::DataType) -> sqlparser::ast::DataType {
    use sqlparser::ast::DataType;
    match dt {
        // Integer types → BIGINT (maps to Type::INT8 in name_to_type)
        DataType::Int2(_) | DataType::Int4(_) | DataType::Int8(_) | DataType::SmallInt(_) => {
            DataType::BigInt(None)
        }
        // Floating-point types → DoublePrecision (maps to Type::FLOAT8)
        DataType::Real | DataType::Float(_) | DataType::Double(_) | DataType::Float8 => {
            DataType::DoublePrecision
        }
        // Boolean → BOOLEAN (maps to Type::BOOL)
        DataType::Bool => DataType::Boolean,
        // Bytea → BLOB (maps to Type::BYTEA)
        DataType::Bytea => DataType::Blob(None),
        // Text types → TEXT (maps to Type::TEXT)
        DataType::Varchar(_) | DataType::CharVarying(_) | DataType::CharacterVarying(_) => {
            DataType::Text
        }
        // Timestamp → TIMESTAMP (maps to Type::TIMESTAMP)
        DataType::Timestamp(_, _) => DataType::Timestamp(None, sqlparser::ast::TimezoneInfo::None),
        // Pass through types that are already SQLite-compatible
        other => other,
    }
}

/// Builds a simple `Function` AST node for a function call with unnamed args.
fn make_sqlite_function(name: &str, args: Vec<sqlparser::ast::Expr>) -> sqlparser::ast::Function {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName, ObjectNamePart,
    };
    Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    }
}

/// Recursively strips PG-specific syntax from an `Expr`.  The main
/// transformation is converting `::type` casts to `CAST(expr AS type)` with
/// SQLite-compatible type names.
fn strip_pg_ast_expr(outer_expr: &mut sqlparser::ast::Expr) {
    use sqlparser::ast::Expr;

    match outer_expr {
        // Convert `expr::type` (DoubleColon) casts to `CAST(expr AS type)`
        // which SQLite understands, and map PG-specific type names to
        // SQLite-compatible ones.  `CAST(...)` casts are also normalized.
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            ..
        } => {
            strip_pg_ast_expr(inner);
            *kind = sqlparser::ast::CastKind::Cast;
            *data_type = pg_data_type_to_sqlite(data_type.clone());
        }
        Expr::Function(func) => {
            strip_pg_catalog_prefix(&mut func.name);
            // In PG, `user` is a special keyword equivalent to `current_user`.
            // The PG parser produces it as Expr::Function with no args.
            // SQLite doesn't have this, so convert to a string literal.
            if let Some(ident) = func.name.0.last().and_then(|p| p.as_ident()) {
                if ident.value.eq_ignore_ascii_case("user")
                    && ident.quote_style.is_none()
                    && matches!(func.args, sqlparser::ast::FunctionArguments::None)
                {
                    *outer_expr = Expr::Value(
                        sqlparser::ast::Value::SingleQuotedString("corro".into()).into(),
                    );
                    return;
                }
            }
            strip_pg_ast_function_args(&mut func.args);
            if let Some(ref mut filter) = func.filter {
                strip_pg_ast_expr(filter);
            }
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Wildcard(_) => {}
        Expr::BinaryOp { left, right, .. } => {
            strip_pg_ast_expr(left);
            strip_pg_ast_expr(right);
        }
        Expr::UnaryOp { expr, .. } => strip_pg_ast_expr(expr),
        Expr::IsNull(expr) | Expr::IsNotNull(expr) | Expr::IsTrue(expr) | Expr::IsFalse(expr) => {
            strip_pg_ast_expr(expr)
        }
        Expr::InList { expr, list, .. } => {
            strip_pg_ast_expr(expr);
            for e in list {
                strip_pg_ast_expr(e);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            strip_pg_ast_expr(expr);
            strip_pg_ast_query(subquery);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            strip_pg_ast_expr(expr);
            strip_pg_ast_expr(low);
            strip_pg_ast_expr(high);
        }
        Expr::Like { expr, pattern, .. } => {
            strip_pg_ast_expr(expr);
            strip_pg_ast_expr(pattern);
        }
        Expr::ILike { expr, pattern, .. } => {
            strip_pg_ast_expr(expr);
            strip_pg_ast_expr(pattern);
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                strip_pg_ast_expr(op);
            }
            for case_when in conditions.iter_mut() {
                strip_pg_ast_expr(&mut case_when.condition);
                strip_pg_ast_expr(&mut case_when.result);
            }
            if let Some(er) = else_result {
                strip_pg_ast_expr(er);
            }
        }
        Expr::Subquery(query) => strip_pg_ast_query(query),
        Expr::Exists { subquery, .. } => strip_pg_ast_query(subquery),
        Expr::Nested(inner) => strip_pg_ast_expr(inner),
        Expr::Extract { expr, .. } => strip_pg_ast_expr(expr),
        Expr::Trim {
            expr,
            trim_what,
            trim_characters,
            ..
        } => {
            strip_pg_ast_expr(expr);
            if let Some(tw) = trim_what {
                strip_pg_ast_expr(tw);
            }
            if let Some(tcs) = trim_characters {
                for tc in tcs {
                    strip_pg_ast_expr(tc);
                }
            }
        }
        Expr::Overlay {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
        } => {
            strip_pg_ast_expr(expr);
            strip_pg_ast_expr(overlay_what);
            strip_pg_ast_expr(overlay_from);
            if let Some(of) = overlay_for {
                strip_pg_ast_expr(of);
            }
        }
        Expr::AtTimeZone {
            timestamp,
            time_zone,
        } => {
            strip_pg_ast_expr(timestamp);
            strip_pg_ast_expr(time_zone);
        }
        Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
            strip_pg_ast_expr(left);
            strip_pg_ast_expr(right);
        }
        Expr::IsUnknown(expr) | Expr::IsNotUnknown(expr) => strip_pg_ast_expr(expr),
        // Convert `POSITION(substr IN str)` to `INSTR(str, substr)` which
        // SQLite understands.  SQLite doesn't support the POSITION...IN syntax.
        Expr::Position {
            expr: substr,
            r#in: in_str,
        } => {
            strip_pg_ast_expr(substr);
            strip_pg_ast_expr(in_str);
            let in_expr = (**in_str).clone();
            let substr_expr = (**substr).clone();
            *outer_expr = Expr::Function(make_sqlite_function("INSTR", vec![in_expr, substr_expr]));
        }
        // Convert PG array subscript `arr[i]` (parsed as JsonAccess) to `json_extract(arr, '$[' || (i - 1) || ']')`.
        // PG arrays are 1-indexed; JSON arrays are 0-indexed, so we subtract 1.
        Expr::JsonAccess { value, path } => {
            strip_pg_ast_expr(value);
            if path.path.len() == 1 {
                if let sqlparser::ast::JsonPathElem::Bracket { key } = &path.path[0] {
                    let mut idx_expr = key.clone();
                    strip_pg_ast_expr(&mut idx_expr);
                    let arr_expr = (**value).clone();
                    // Build json_extract(arr, '$[' || ((idx) - 1) || ']')
                    let idx_minus_1 = Expr::Nested(Box::new(Expr::BinaryOp {
                        left: Box::new(idx_expr),
                        op: sqlparser::ast::BinaryOperator::Minus,
                        right: Box::new(Expr::Value(
                            sqlparser::ast::Value::Number("1".into(), false).into(),
                        )),
                    }));
                    let path_expr = Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Value(
                                sqlparser::ast::Value::SingleQuotedString("$[".into()).into(),
                            )),
                            op: sqlparser::ast::BinaryOperator::StringConcat,
                            right: Box::new(idx_minus_1),
                        }),
                        op: sqlparser::ast::BinaryOperator::StringConcat,
                        right: Box::new(Expr::Value(
                            sqlparser::ast::Value::SingleQuotedString("]".into()).into(),
                        )),
                    };
                    *outer_expr = Expr::Function(make_sqlite_function(
                        "json_extract",
                        vec![arr_expr, path_expr],
                    ));
                }
            }
        }
        // Convert PG array subscript `arr[i]` to `json_extract(arr, '$[i-1]')`.
        // PG arrays are 1-indexed; JSON arrays are 0-indexed, so we subtract 1.
        // Only handles a single subscript index (not slices).
        Expr::CompoundFieldAccess { root, access_chain } => {
            strip_pg_ast_expr(root);
            if access_chain.len() == 1 {
                if let sqlparser::ast::AccessExpr::Subscript(sqlparser::ast::Subscript::Index {
                    index,
                }) = &access_chain[0]
                {
                    let mut idx_expr = index.clone();
                    strip_pg_ast_expr(&mut idx_expr);
                    let arr_expr = (**root).clone();
                    // Build json_extract(arr, '$[' || (idx - 1) || ']')
                    // PG arrays are 1-indexed; JSON arrays are 0-indexed.
                    let idx_minus_1 = Expr::Nested(Box::new(Expr::BinaryOp {
                        left: Box::new(idx_expr),
                        op: sqlparser::ast::BinaryOperator::Minus,
                        right: Box::new(Expr::Value(
                            sqlparser::ast::Value::Number("1".into(), false).into(),
                        )),
                    }));
                    // '$[' || (idx - 1) || ']'
                    let path_expr = Expr::BinaryOp {
                        left: Box::new(Expr::BinaryOp {
                            left: Box::new(Expr::Value(
                                sqlparser::ast::Value::SingleQuotedString("$[".into()).into(),
                            )),
                            op: sqlparser::ast::BinaryOperator::StringConcat,
                            right: Box::new(idx_minus_1),
                        }),
                        op: sqlparser::ast::BinaryOperator::StringConcat,
                        right: Box::new(Expr::Value(
                            sqlparser::ast::Value::SingleQuotedString("]".into()).into(),
                        )),
                    };
                    *outer_expr = Expr::Function(make_sqlite_function(
                        "json_extract",
                        vec![arr_expr, path_expr],
                    ));
                    return;
                }
            }
            // Fallback: just recurse into the root
            for access in access_chain.iter_mut() {
                match access {
                    sqlparser::ast::AccessExpr::Dot(e) => strip_pg_ast_expr(e),
                    sqlparser::ast::AccessExpr::Subscript(sqlparser::ast::Subscript::Index {
                        index,
                    }) => strip_pg_ast_expr(index),
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::pg_stmt_to_sqlite_sql;

    #[test]
    fn check_normal_pg_dialect_array_subscript() {
        // Verify the normal PostgreSqlDialect parses s[i] as
        // CompoundFieldAccess and our walker transforms it to json_extract
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            "SELECT s[i] FROM t",
        )
        .unwrap();
        let converted = pg_stmt_to_sqlite_sql(&stmts[0]).unwrap();
        assert!(
            converted.contains("json_extract(s,"),
            "expected json_extract in: {converted}"
        );
    }

    #[test]
    fn strip_pg_casts_via_ast() {
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            "SELECT reltuples::int8 FROM pg_class",
        )
        .unwrap();
        let converted = pg_stmt_to_sqlite_sql(&stmts[0]).unwrap();
        // The ::int8 cast should be converted to CAST(reltuples AS BIGINT)
        // (INT8 is a PG alias for BIGINT; SQLite + name_to_type understand BIGINT)
        assert!(!converted.contains("::"));
        assert!(converted.contains("CAST(reltuples AS BIGINT)"));
    }

    #[test]
    fn strip_pg_catalog_function_prefix_via_ast() {
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            "SELECT pg_catalog.col_description(16395, ordinal_position) FROM t",
        )
        .unwrap();
        let converted = pg_stmt_to_sqlite_sql(&stmts[0]).unwrap();
        assert!(!converted.contains("pg_catalog.col_description"));
        assert!(converted.contains("col_description"));
    }

    #[test]
    fn pg_catalog_table_reference_preserved() {
        let stmts = sqlparser::parser::Parser::parse_sql(
            &sqlparser::dialect::PostgreSqlDialect {},
            "SELECT * FROM pg_catalog.pg_class",
        )
        .unwrap();
        let converted = pg_stmt_to_sqlite_sql(&stmts[0]).unwrap();
        // Table references should keep pg_catalog. prefix since SQLite supports schema.table
        assert!(converted.contains("pg_catalog.pg_class"));
    }
}

#[derive(Default)]
enum TxState {
    Started {
        kind: OpenTxKind,
        permits: Option<(OwnedSemaphorePermit, BookieWriteGuard)>,
    },
    #[default]
    Ended,
}

impl TxState {
    fn implicit() -> Self {
        Self::Started {
            kind: OpenTxKind::Implicit,
            permits: None,
        }
    }
    fn explicit() -> Self {
        Self::Started {
            kind: OpenTxKind::Explicit,
            permits: None,
        }
    }

    fn is_writing(&self) -> bool {
        matches!(
            self,
            TxState::Started {
                permits: Some(_),
                ..
            }
        )
    }

    fn set_write_context(&mut self, permit: OwnedSemaphorePermit, bookie_write: BookieWriteGuard) {
        match self {
            TxState::Started { permits, .. } => {
                *permits = Some((permit, bookie_write));
            }
            TxState::Ended => {
                // do nothing, maybe bomb?
            }
        }
    }

    fn is_implicit(&self) -> bool {
        matches!(
            self,
            TxState::Started {
                kind: OpenTxKind::Implicit,
                ..
            }
        )
    }
    fn is_explicit(&self) -> bool {
        matches!(
            self,
            TxState::Started {
                kind: OpenTxKind::Explicit,
                ..
            }
        )
    }
    fn is_ended(&self) -> bool {
        matches!(self, TxState::Ended)
    }

    fn start_implicit(&mut self) {
        *self = Self::implicit()
    }

    fn start_explicit(&mut self) {
        *self = Self::explicit()
    }

    fn end(&mut self) -> Option<(OwnedSemaphorePermit, BookieWriteGuard)> {
        let permits = match self {
            TxState::Started { permits, .. } => permits.take(),
            TxState::Ended => None,
        };
        *self = TxState::Ended;
        permits
    }
}

#[derive(Debug)]
enum OpenTxKind {
    Implicit,
    Explicit,
}

#[derive(Debug, Clone)]
struct CancelInfo {
    cancel: CancellationToken,
    secret_key: i32,
}

#[derive(Debug, Clone, Default)]
pub struct PgTaskCancellation(Arc<TokioRwLock<HashMap<i32, CancelInfo>>>);

impl PgTaskCancellation {
    pub async fn insert(&self, conn_id: i32, cancel: CancellationToken, secret_key: i32) {
        self.0
            .write()
            .await
            .insert(conn_id, CancelInfo { cancel, secret_key });
    }

    pub async fn remove(&self, conn_id: i32) {
        self.0.write().await.remove(&conn_id);
    }

    pub async fn get_and_verify(&self, conn_id: i32, secret_key: i32) -> Option<CancellationToken> {
        if let Some(cancel_info) = self.0.read().await.get(&conn_id).cloned() {
            if cancel_info.secret_key == secret_key {
                return Some(cancel_info.cancel);
            }
        }
        None
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PgStartError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    PgTlsError(#[from] eyre::Error),
}

async fn setup_tls(pg: PgConfig) -> eyre::Result<(Option<TlsAcceptor>, bool)> {
    use eyre::ContextCompat as _;
    use rustls::pki_types::pem::PemObject as _;

    let tls = match pg.tls {
        Some(tls) => tls,
        None => {
            return Ok((None, false));
        }
    };

    let ssl_required = tls.verify_client;

    let key_data = tokio::fs::read(&tls.key_file).await?;
    let key = if tls.key_file.extension() == Some("der") {
        rustls::pki_types::PrivateKeyDer::try_from(key_data).map_err(|e| eyre::eyre!("{e}"))?
    } else {
        rustls::pki_types::PrivateKeyDer::from_pem_slice(&key_data)?
    };

    let certs = tokio::fs::read(&tls.cert_file).await?;
    let certs = if tls.cert_file.extension() == Some("der") {
        vec![rustls::pki_types::CertificateDer::from(certs)]
    } else {
        rustls::pki_types::CertificateDer::pem_slice_iter(&certs)
            .map(|res| res.wrap_err_with(|| format!("failed to read certs from {}", tls.key_file)))
            .collect::<eyre::Result<Vec<_>>>()?
    };

    let server_crypto = ServerConfig::builder();

    let server_crypto = if ssl_required {
        let ca_file = tls
            .ca_file
            .as_ref()
            .context("ca_file required in tls config for server client cert auth verification")?;

        let ca_certs = tokio::fs::read(&ca_file).await?;

        let mut root_store = rustls::RootCertStore::empty();

        if ca_file.extension() == Some("der") {
            root_store.add(rustls::pki_types::CertificateDer::from_slice(&ca_certs))?;
        } else {
            for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&ca_certs) {
                root_store
                    .add(cert.wrap_err_with(|| format!("failed to read certs from {ca_file}"))?)?;
            }
        }

        server_crypto.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store)).build()?,
        )
    } else {
        server_crypto.with_no_client_auth()
    };

    let config = server_crypto.with_single_cert(certs, key)?;
    Ok((Some(TlsAcceptor::from(Arc::new(config))), ssl_required))
}

pub async fn start(
    agent: Agent,
    pg: PgConfig,
    tripwire: Tripwire,
) -> Result<PgServer, PgStartError> {
    let readonly = pg.readonly;
    let server = TcpListener::bind(pg.bind_addr).await?;
    let (tls_acceptor, ssl_required) = setup_tls(pg).await?;
    let local_addr = server.local_addr()?;
    let conn_gauge = persistent_gauge!("corro.api.active.streams",
    "source" => "postgres",
    "protocol" => "pg",
    "readonly" => readonly.to_string(),
    );
    let conn_counter = AtomicI32::new(0);
    let task_cancellation = PgTaskCancellation::default();

    spawn_counted(async move {
        let mut conn_tripwire = tripwire.clone();
        loop {
            let (tcp_conn, remote_addr) =
                match server.accept().preemptible(&mut conn_tripwire).await {
                    Outcome::Completed(res) => res?,
                    Outcome::Preempted(_) => break,
                };
            let conn = CountedTcpStream::wrap(tcp_conn, conn_gauge.clone());
            let tls_acceptor = tls_acceptor.clone();
            debug!("Accepted a PostgreSQL connection (from: {remote_addr})");

            counter!("corro.api.connection.count", "protocol" => "pg", "readonly" => readonly.to_string()).increment(1);

            let agent = agent.clone();
            let tripwire = tripwire.clone();
            // Don't use spawn_counted here
            // Until the connection gets fully established we don't need to gracefully close it
            let conn_id = conn_counter.fetch_add(1, Ordering::SeqCst);
            let task_cancellation = task_cancellation.clone();
            tokio::spawn(async move {
                conn.stream.set_nodelay(true)?;
                {
                    let sock = SockRef::from(&conn.stream);
                    let ka = TcpKeepalive::new()
                        .with_time(Duration::from_secs(10))
                        .with_interval(Duration::from_secs(10))
                        .with_retries(4);
                    sock.set_tcp_keepalive(&ka)?;
                }

                let mut tcp_socket = Framed::new(
                    tokio::io::BufStream::new(conn),
                    PgWireMessageServerCodec::<()>::new(DefaultClient::new(local_addr, false)),
                );

                let negotiation =
                    ssl::negotiate_ssl(&mut tcp_socket, tls_acceptor.is_some()).await?;

                let (mut framed, secured, maybe_next_msg) =
                    if matches!(negotiation, ssl::SslNegotiationType::None(_)) {
                        if ssl_required {
                            debug!("rejecting non-ssl connection");
                            return Ok(());
                        }

                        let maybe_next_msg = match negotiation {
                            ssl::SslNegotiationType::None(Some(msg)) => Some(msg),
                            _ => None,
                        };

                        (Either::Left(tcp_socket), false, maybe_next_msg)
                    } else if let Some(tls) = tls_acceptor {
                        let tls_socket = tls.accept(tcp_socket.into_inner()).await?;

                        if matches!(negotiation, ssl::SslNegotiationType::Direct) {
                            ssl::check_alpn_for_direct_ssl(&tls_socket)?;
                        }

                        let framed = Framed::new(
                            tokio::io::BufStream::new(tls_socket),
                            PgWireMessageServerCodec::new(DefaultClient::<()>::new(
                                local_addr, true,
                            )),
                        );

                        (Either::Right(framed), true, None)
                    } else {
                        trace!("received SSL connection attempt without a TLS acceptor configured");
                        return Ok(());
                    };

                trace!("SSL ? {secured}");

                use crate::codec::SetState;

                trace!("maybe_next_msg: {maybe_next_msg:?}");
                let msg = match maybe_next_msg {
                    Some(msg) => msg,
                    None => {
                        framed.set_state(pgwire::api::PgWireConnectionState::AwaitingStartup);
                        match framed.next().await {
                            Some(msg) => msg?,
                            None => return Ok(()),
                        }
                    }
                };

                match msg {
                    PgWireFrontendMessage::Startup(startup) => {
                        debug!("received startup message: {startup:?}");
                    }
                    PgWireFrontendMessage::CancelRequest(cancel_request) => {
                        debug!("received cancel request: {cancel_request:?}");

                        if let Some(secret_key) = cancel_request.secret_key.as_i32() {
                            if let Some(cancel) = task_cancellation
                                .get_and_verify(cancel_request.pid, secret_key)
                                .await
                            {
                                cancel.cancel();
                            } else {
                                warn!("invalid secret key for cancel request");
                            }
                        }
                        return Ok(());
                    }
                    _ => {
                        framed
                            .send(PgWireBackendMessage::ErrorResponse(
                                ErrorInfo::new(
                                    "FATAL".into(),
                                    SqlState::PROTOCOL_VIOLATION.code().into(),
                                    "expected startup message".into(),
                                )
                                .into(),
                            ))
                            .await?;
                        return Ok(());
                    }
                }

                framed.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);

                framed
                    .feed(PgWireBackendMessage::Authentication(
                        pgwire::messages::startup::Authentication::Ok,
                    ))
                    .await?;

                framed
                    .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                        "server_version".into(),
                        "14.0.0".into(),
                    )))
                    .await?;

                let mut rng = StdRng::from_os_rng();
                let secret_key: i32 = rng.random::<i32>();

                let cancel = CancellationToken::new();
                task_cancellation
                    .insert(conn_id, cancel.clone(), secret_key)
                    .await;

                framed
                    .feed(PgWireBackendMessage::BackendKeyData(
                        pgwire::messages::startup::BackendKeyData::new(
                            conn_id,
                            pgwire::messages::startup::SecretKey::I32(secret_key),
                        ),
                    ))
                    .await?;

                framed
                    .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                        TransactionStatus::Idle,
                    )))
                    .await?;

                framed.flush().await?;

                trace!("sent auth ok and ReadyForQuery");

                let (front_tx, mut front_rx) = channel(1024);
                let (back_tx, mut back_rx) = channel(1024);

                let (mut sink, mut stream) = framed.split();

                // If we're shutting down corrosion, both frontend and backend tasks will finish
                let mut frontend_task = spawn_counted({
                    // Use a weak sender here; it should NOT hold the backend channel (and half-connection) open
                    let back_tx = back_tx.clone().downgrade();
                    let cancel = cancel.clone();
                    let mut tripwire = tripwire.clone();
                    async move {
                        // cancel stuff if this loop breaks
                        let _drop_guard = cancel.drop_guard();

                        match async move {
                            while let Some(decode_res) = stream.next().await {
                                let msg = match decode_res {
                                    Ok(msg) => msg,
                                    Err(PgWireError::IoError(io_error)) => {
                                        debug!("postgres io error: {io_error}");
                                        break;
                                    }
                                    Err(e) => {
                                        warn!("could not receive pg frontend message: {e}");
                                        // attempt to send this...
                                        if let Some(back_tx) = back_tx.upgrade() {
                                            _ = back_tx.try_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "FATAL".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            );
                                        }
                                        break;
                                    }
                                };

                                front_tx.send(msg).await?;
                            }
                            Ok::<_, BoxError>(())
                        }
                        .preemptible(&mut tripwire)
                        .await
                        {
                            Outcome::Completed(res) => res?,
                            Outcome::Preempted(_) => {}
                        }
                        debug!("frontend stream is done");

                        Ok::<_, BoxError>(())
                    }
                });

                let mut backend_task = spawn_counted({
                    let cancel = cancel.clone();
                    let mut tripwire = tripwire.clone();
                    async move {
                        let _drop_guard = cancel.drop_guard();
                        match async {
                            while let Some(back) = back_rx.recv().await {
                                match back {
                                    BackendResponse::Message { message, flush } => {
                                        if let PgWireBackendMessage::ErrorResponse(e) = &message {
                                            warn!("sending: {e:?}");
                                        } else {
                                            debug!("sending: {message:?}");
                                        }
                                        sink.feed(message).await?;
                                        if flush {
                                            sink.flush().await?;
                                        }
                                    }
                                    BackendResponse::Flush => {
                                        sink.flush().await?;
                                    }
                                }
                            }
                            Ok::<_, std::io::Error>(())
                        }
                        .preemptible(&mut tripwire)
                        .await
                        {
                            Outcome::Completed(res) => res?,
                            Outcome::Preempted(_) => {}
                        }
                        if tripwire.is_shutting_down() {
                            debug!("Closing connection due to corrosion shutdown");
                            // Give 1s for graceful shutdown of the connection
                            timeout(Duration::from_millis(1000), async move {
                                let _ = sink
                                    .feed(PgWireBackendMessage::ErrorResponse(
                                        ErrorInfo::new(
                                            "ERROR".to_owned(),
                                            sql_state::SqlState::ADMIN_SHUTDOWN.code().into(),
                                            "Corrosion is shutting down".into(),
                                        )
                                        .into(),
                                    ))
                                    .await;
                                let _ = sink.flush().await;
                                let _ = sink.close().await;
                            })
                            .await?;
                        } else {
                            debug!("Closing connection due to client disconnection");
                            // If we get here, we know that `back_rx` has been fully drained.
                            // Close the sink, this calls shutdown() on the underlying TCP socket
                            // If the other side behaves correctly, the frontend task will eventually receive an EOF
                            // and will also complete; by that point we know all messages have been sent successfully over TCP.
                            // However, if this is not handled correctly we time out later.
                            //
                            // If we are shutting down when the client disconnects, we just exit. Don't need to timeout here
                            let _ = sink.close().preemptible(&mut tripwire).await;
                        }
                        Ok::<_, std::io::Error>(())
                    }
                });

                let res = tokio::task::spawn_blocking({
                    let back_tx = back_tx.clone();
                    move || {
                        let conn = if readonly {
                            agent.pool().client_dedicated_readonly().unwrap()
                        } else {
                            agent.pool().client_dedicated().unwrap()
                        };
                        trace!("opened connection");

                        let int_handle = conn.get_interrupt_handle();
                        tokio::spawn(async move {
                            cancel.cancelled().await;
                            int_handle.interrupt();
                        });

                        conn.execute_batch(
                            "ATTACH ':memory:' AS pg_catalog;
                             ATTACH ':memory:' AS information_schema;",
                        )?;

                        let dbs = Arc::new(vec![PgDatabase::new("state".into())]);
                        let table_names = load_information_schema_table_names(&conn)?;
                        let columns = Arc::new(load_information_schema_columns(&conn, &table_names)?);
                        let table_constraints = Arc::new(
                            load_information_schema_table_constraints(columns.as_slice()),
                        );
                        let key_column_usage = Arc::new(
                            load_information_schema_key_column_usage(columns.as_slice()),
                        );
                        let pg_class_entries_vec = load_pg_class_entries(&conn, &table_names)?;

                        // Build OID maps for pg_index loading.
                        let table_oid_map: std::collections::HashMap<String, i64> =
                            pg_class_entries_vec
                                .iter()
                                .filter(|e| e.relkind == "r")
                                .map(|e| (e.relname.clone(), e.oid))
                                .collect();
                        let index_oid_map: std::collections::HashMap<String, i64> =
                            pg_class_entries_vec
                                .iter()
                                .filter(|e| e.relkind == "i")
                                .map(|e| (e.relname.clone(), e.oid))
                                .collect();
                        let pg_index_entries = Arc::new(load_pg_index_entries(
                            &conn,
                            &table_oid_map,
                            &index_oid_map,
                        )?);

                        // Populate a temp table mapping pg_class OIDs to
                        // relnames, so pg_get_indexdef can look up index SQL
                        // by OID.
                        conn.execute(
                            "CREATE TEMP TABLE temp_pg_class_oid_map (oid INTEGER, relname TEXT, relkind TEXT)",
                            [],
                        )?;
                        {
                            let mut stmt = conn.prepare(
                                "INSERT INTO temp_pg_class_oid_map (oid, relname, relkind) VALUES (?1, ?2, ?3)",
                            )?;
                            for entry in pg_class_entries_vec.iter() {
                                stmt.execute(rusqlite::params![
                                    entry.oid,
                                    &entry.relname,
                                    entry.relkind
                                ])?;
                            }
                        }

                        let pg_class_entries = Arc::new(pg_class_entries_vec);

                        let pg_attributes = Arc::new(load_pg_attributes(
                            columns.as_slice(),
                            pg_class_entries.as_slice(),
                        ));
                        let triggers = Arc::new(load_information_schema_triggers(&conn)?);
                        let table_names = Arc::new(table_names);

                        conn.create_module(
                            "pg_database",
                            eponymous_only_module::<PgDatabaseTable>(),
                            Some(dbs),
                        )?;
                        conn.create_module(
                            "pg_type",
                            eponymous_only_module::<PgTypeTable>(),
                            None,
                        )?;
                        conn.create_module(
                            "pg_range",
                            eponymous_only_module::<PgRangeTable>(),
                            None,
                        )?;
                        conn.create_module(
                            "pg_namespace",
                            eponymous_only_module::<PgNamespaceTable>(),
                            None,
                        )?;
                        conn.create_module(
                            "pg_class",
                            eponymous_only_module::<PgClassTable>(),
                            Some(pg_class_entries),
                        )?;
                        conn.create_module(
                            "pg_attribute",
                            eponymous_only_module::<PgAttributeTable>(),
                            Some(pg_attributes),
                        )?;
                        conn.create_module(
                            "tables",
                            eponymous_only_module::<InformationSchemaTablesTable>(),
                            Some(table_names.clone()),
                        )?;
                        conn.create_module(
                            "columns",
                            eponymous_only_module::<InformationSchemaColumnsTable>(),
                            Some(columns),
                        )?;
                        conn.create_module(
                            "table_constraints",
                            eponymous_only_module::<InformationSchemaTableConstraintsTable>(),
                            Some(table_constraints),
                        )?;
                        conn.create_module(
                            "key_column_usage",
                            eponymous_only_module::<InformationSchemaKeyColumnUsageTable>(),
                            Some(key_column_usage),
                        )?;
                        conn.create_module(
                            "triggers",
                            eponymous_only_module::<InformationSchemaTriggersTable>(),
                            Some(triggers),
                        )?;

                        // pg_index — populated from sqlite_master indexes.
                        conn.create_module(
                            "pg_index",
                            eponymous_only_module::<PgIndexTable>(),
                            Some(pg_index_entries),
                        )?;

                        // pg_constraint — populated from PK and unique constraints.
                        let pg_constraint_entries = Arc::new(load_pg_constraint_entries(
                            &conn,
                            &table_names,
                            &table_oid_map,
                            &index_oid_map,
                        )?);
                        conn.create_module(
                            "pg_constraint",
                            eponymous_only_module::<PgConstraintTable>(),
                            Some(pg_constraint_entries.clone()),
                        )?;

                        // pg_get_constraintdef(oid) – returns the definition
                        // text of a constraint.
                        conn.execute(
                            "CREATE TEMP TABLE IF NOT EXISTS temp_pg_constraint_map (oid INTEGER, consrc TEXT)",
                            [],
                        )?;
                        for entry in pg_constraint_entries.iter() {
                            conn.execute(
                                "INSERT INTO temp_pg_constraint_map VALUES (?1, ?2)",
                                rusqlite::params![entry.oid, entry.consrc],
                            )?;
                        }
                        conn.create_scalar_function(
                            "pg_get_constraintdef",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let oid: i64 = ctx.get(0).unwrap_or(0);
                                let conn = unsafe { ctx.get_connection()? };
                                let result: Option<String> = conn
                                    .query_row(
                                        "SELECT consrc FROM temp_pg_constraint_map WHERE oid = ?1",
                                        [oid],
                                        |row| row.get(0),
                                    )
                                    .unwrap_or(None);
                                Ok(result)
                            },
                        )?;

                        // pg_am — SQLite only uses btree (OID 403).
                        conn.create_module(
                            "pg_am",
                            eponymous_only_module::<PgAmTable>(),
                            None,
                        )?;
                        // pg_proc — populated from pragma_function_list().
                        let pg_proc_entries = Arc::new(load_pg_proc_entries(&conn)?);
                        conn.create_module(
                            "pg_proc",
                            eponymous_only_module::<PgProcTable>(),
                            Some(pg_proc_entries),
                        )?;
                        // pg_language — static list of languages (internal, c, sql).
                        let pg_language_entries = Arc::new(load_pg_language_entries());
                        conn.create_module(
                            "pg_language",
                            eponymous_only_module::<PgLanguageTable>(),
                            Some(pg_language_entries),
                        )?;
                        // Empty catalog tables that are referenced in JOINs
                        // but not yet populated.  Returning empty lets tools
                        // degrade gracefully.
                        conn.create_module(
                            "pg_extension",
                            eponymous_only_module::<EmptyCatalogTable>(),
                            Some(PG_EXTENSION_DDL),
                        )?;
                        conn.create_module(
                            "pg_statio_user_tables",
                            eponymous_only_module::<EmptyCatalogTable>(),
                            Some(PG_STATIO_USER_TABLES_DDL),
                        )?;
                        conn.create_module(
                            "pg_description",
                            eponymous_only_module::<EmptyCatalogTable>(),
                            Some(PG_DESCRIPTION_DDL),
                        )?;
                        conn.create_module(
                            "pg_shdescription",
                            eponymous_only_module::<EmptyCatalogTable>(),
                            Some(PG_SHDESCRIPTION_DDL),
                        )?;

                        conn.create_scalar_function(
                            "version",
                            0,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok("PostgreSQL 14.9"),
                        )?;

                        conn.create_scalar_function(
                            "pg_my_temp_schema",
                            0,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok(0i64),
                        )?;

                        conn.create_scalar_function(
                            "pg_is_other_temp_schema",
                            1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok(false),
                        )?;

                        // format_type(oid, typmod) – returns the PostgreSQL
                        // type name for a given type OID.  This is used by
                        // TablePlus and other tools when introspecting columns.
                        conn.create_scalar_function(
                            "format_type",
                            2,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let oid: i64 = ctx.get(0)?;
                                Ok(format_type_oid(oid as u32))
                            },
                        )?;

                        // col_description(table_oid, column_number) – returns
                        // the comment for a column.  We don't store comments
                        // so always return NULL.
                        conn.create_scalar_function(
                            "col_description",
                            2,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok::<Option<String>, rusqlite::Error>(None),
                        )?;

                        // obj_description(oid, catalog) – returns the comment
                        // for a database object.  Always return NULL.
                        conn.create_scalar_function(
                            "obj_description",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok::<Option<String>, rusqlite::Error>(None),
                        )?;

                        // pg_get_indexdef(index_oid) – returns the CREATE INDEX
                        // statement for an index.  For explicit indexes, return
                        // the SQL from sqlite_master.  For auto-indexes
                        // (sqlite_autoindex_*, sql IS NULL), synthesize a
                        // CREATE INDEX statement from pragma_index_info.
                        //
                        // pg_get_indexdef(index_oid, colno, pretty) – returns
                        // just the colno-th column name (1-based) of the index.
                        conn.create_scalar_function(
                            "pg_get_indexdef",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let n = ctx.len();
                                let oid: i64 = ctx.get(0).unwrap_or(0);
                                let conn = unsafe { ctx.get_connection()? };

                                // Look up the index name and table name.
                                let (index_name, tbl_name): (String, String) = conn
                                    .query_row(
                                        "SELECT o.relname, m.tbl_name \
                                         FROM temp_pg_class_oid_map o \
                                         JOIN sqlite_master m ON m.name = o.relname \
                                         WHERE o.oid = ?1 AND m.type = 'index'",
                                        [oid],
                                        |row| Ok((row.get(0)?, row.get(1)?)),
                                    )
                                    .unwrap_or_default();

                                if index_name.is_empty() {
                                    return Ok::<Option<String>, rusqlite::Error>(None);
                                }

                                // Get column names from pragma_index_info.
                                let mut stmt = conn.prepare(
                                    "SELECT name FROM pragma_index_info(?1) ORDER BY seqno",
                                )?;
                                let cols: Vec<String> = stmt
                                    .query_map([&index_name], |row| row.get::<_, String>(0))?
                                    .filter_map(|r| r.ok())
                                    .collect();

                                // 3-arg form: return the colno-th column name.
                                if n >= 3 {
                                    let colno: i64 = ctx.get(1).unwrap_or(0);
                                    if colno < 1 || colno as usize > cols.len() {
                                        return Ok(None);
                                    }
                                    return Ok(Some(cols[(colno - 1) as usize].clone()));
                                }

                                // 1-arg form: return full CREATE INDEX statement.

                                // Try to get the explicit CREATE INDEX SQL.
                                let sql: Option<String> = conn
                                    .query_row(
                                        "SELECT sql FROM sqlite_master WHERE name = ?1 AND type = 'index'",
                                        [&index_name],
                                        |row| row.get(0),
                                    )
                                    .unwrap_or(None);

                                if let Some(s) = sql {
                                    return Ok(Some(s));
                                }

                                if cols.is_empty() {
                                    return Ok(None);
                                }

                                // Auto-index: synthesize from pragma_index_info.
                                // Check if it's unique via pragma_index_list.
                                let is_unique: bool = conn
                                    .query_row(
                                        "SELECT \"unique\" FROM pragma_index_list(?1) WHERE name = ?2",
                                        rusqlite::params![&tbl_name, &index_name],
                                        |row| row.get::<_, i64>(0),
                                    )
                                    .map(|v| v != 0)
                                    .unwrap_or(false);

                                let unique_kw = if is_unique { "UNIQUE " } else { "" };
                                let cols_str = cols.join(", ");
                                Ok(Some(format!(
                                    "CREATE {unique_kw}INDEX {index_name} ON {tbl_name} ({cols_str})"
                                )))
                            },
                        )?;

                        // pg_get_function_identity_arguments(oid) – returns
                        // the argument list of a function as a string.
                        // We look up the function in pg_proc and return a
                        // comma-separated list of argument type names.
                        conn.create_scalar_function(
                            "pg_get_function_identity_arguments",
                            1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let oid: i64 = ctx.get(0).unwrap_or(0);
                                let conn = unsafe { ctx.get_connection()? };
                                let nargs: i64 = conn
                                    .query_row(
                                        "SELECT pronargs FROM pg_proc WHERE oid = ?1",
                                        [oid],
                                        |row| row.get(0),
                                    )
                                    .unwrap_or(0);
                                if nargs == 0 {
                                    return Ok::<String, rusqlite::Error>(String::new());
                                }
                                // Return placeholder argument types
                                Ok((0..nargs)
                                    .map(|_| "anyelement")
                                    .collect::<Vec<_>>()
                                    .join(", "))
                            },
                        )?;

                        // pg_get_userbyid(oid) – returns the username for a
                        // role OID.  We always return "postgres" (OID 10).
                        conn.create_scalar_function(
                            "pg_get_userbyid",
                            1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok::<String, rusqlite::Error>("postgres".to_string()),
                        )?;

                        // pg_get_functiondef(oid) – returns the CREATE FUNCTION
                        // statement for a function.  We synthesize a basic one.
                        conn.create_scalar_function(
                            "pg_get_functiondef",
                            1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let oid: i64 = ctx.get(0).unwrap_or(0);
                                let conn = unsafe { ctx.get_connection()? };
                                let (proname, nargs): (String, i64) = conn
                                    .query_row(
                                        "SELECT proname, pronargs FROM pg_proc WHERE oid = ?1",
                                        [oid],
                                        |row| Ok((row.get(0)?, row.get(1)?)),
                                    )
                                    .unwrap_or_default();
                                if proname.is_empty() {
                                    return Ok::<Option<String>, rusqlite::Error>(None);
                                }
                                let args = if nargs > 0 {
                                    (0..nargs)
                                        .map(|i| format!("arg{i}"))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                } else {
                                    String::new()
                                };
                                Ok(Some(format!(
                                    "CREATE OR REPLACE FUNCTION {proname}({args}) RETURNS text LANGUAGE sql AS $$ SELECT NULL $$"
                                )))
                            },
                        )?;

                        // pg_get_expr(pg_node_tree, relation_oid) – returns
                        // the expression text.  We don't have pg_node_tree,
                        // so return NULL (no partial index expressions).
                        conn.create_scalar_function(
                            "pg_get_expr",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok::<Option<String>, rusqlite::Error>(None),
                        )?;

                        // array_to_string(array, delimiter[, null_string])
                        // – joins array elements with delimiter.  Our arrays
                        // are PG-style text like "{a,b,c}".  Return empty
                        // string for NULL arrays.
                        conn.create_scalar_function(
                            "array_to_string",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let n = ctx.len();
                                if n < 2 {
                                    return Ok::<String, rusqlite::Error>(String::new());
                                }
                                let arr: Option<String> = ctx.get(0).unwrap_or(None);
                                let delimiter: String = ctx.get(1).unwrap_or_default();
                                let arr = match arr {
                                    Some(a) => a,
                                    None => return Ok(String::new()),
                                };
                                // Parse PG array literal "{elem1,elem2,...}"
                                let inner = arr
                                    .strip_prefix('{')
                                    .and_then(|s| s.strip_suffix('}'))
                                    .unwrap_or(&arr);
                                let parts: Vec<&str> = if inner.is_empty() {
                                    Vec::new()
                                } else {
                                    inner.split(',').collect()
                                };
                                Ok(parts.join(&delimiter))
                            },
                        )?;

                        // string_agg(value, delimiter) – aggregate that
                        // concatenates values with a delimiter.  SQLite has
                        // group_concat which is similar, but TablePlus calls
                        // string_agg directly.
                        conn.create_aggregate_function(
                            "string_agg",
                            2,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            StringAgg,
                        )?;

                        // pg_total_relation_size(oid) / pg_table_size(oid) /
                        // pg_indexes_size(oid) – return the on-disk size of a
                        // table.  Return 0 as an approximation.
                        for name in ["pg_total_relation_size", "pg_table_size", "pg_indexes_size"] {
                            conn.create_scalar_function(
                                name,
                                -1,
                                FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                                |_ctx| Ok(0i64),
                            )?;
                        }

                        // regexp_replace(source, pattern, replacement[, flags])
                        // – PostgreSQL regex replace function used by TablePlus
                        // to extract column names from index definitions.
                        // Implemented using the `regex` crate with POSIX-style
                        // patterns (greedy by default, like PostgreSQL).
                        conn.create_scalar_function(
                            "regexp_replace",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let n = ctx.len();
                                if n < 3 {
                                    return Ok::<String, rusqlite::Error>(String::new());
                                }
                                let source: String = ctx.get(0).unwrap_or_default();
                                let pattern: String = ctx.get(1).unwrap_or_default();
                                let replacement: String = ctx.get(2).unwrap_or_default();
                                // Optional 4th arg: flags ('g' for global, 'i' for case-insensitive)
                                let flags: String = if n >= 4 {
                                    ctx.get(3).unwrap_or_default()
                                } else {
                                    String::new()
                                };
                                let global = flags.contains('g');
                                let case_insensitive = flags.contains('i');

                                // Build the regex.  PostgreSQL uses POSIX
                                // extended regex; the `regex` crate is close
                                // enough for the patterns TablePlus uses.
                                let mut builder = regex::RegexBuilder::new(&pattern);
                                builder.case_insensitive(case_insensitive);
                                // PostgreSQL defaults to greedy matching.
                                builder.swap_greed(false);

                                let Ok(re) = builder.build() else {
                                    // If the pattern is invalid, return the source unchanged.
                                    return Ok(source);
                                };

                                // PostgreSQL uses \1, \2, etc. for backreferences
                                // in the replacement string.  The `regex` crate
                                // uses $1, $2.  Convert PG-style backrefs.
                                let repl = convert_pg_backrefs(&replacement);

                                if global {
                                    Ok(re.replace_all(&source, repl.as_str()).into_owned())
                                } else {
                                    Ok(re.replace(&source, repl.as_str()).into_owned())
                                }
                            },
                        )?;

                        // current_setting(name) – returns the current value
                        // of a PostgreSQL configuration parameter.  We stub
                        // the few settings that Grafana and other tools query.
                        conn.create_scalar_function(
                            "current_setting",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let name: String = ctx.get(0).unwrap_or_default();
                                let val = match name.as_str() {
                                    "server_version_num" => "140000".to_string(),
                                    "server_version" => "14.0.0".to_string(),
                                    "search_path" => "main".to_string(),
                                    "standard_conforming_strings" => "on".to_string(),
                                    "TimeZone" => "UTC".to_string(),
                                    "integer_datetimes" => "on".to_string(),
                                    "client_encoding" => "UTF8".to_string(),
                                    "application_name" => String::new(),
                                    _ => String::new(),
                                };
                                Ok(val)
                            },
                        )?;

                        // quote_ident(str) – quotes an identifier if it would
                        // need quoting in PostgreSQL (contains special chars,
                        // is a reserved word, etc.).  We wrap in double quotes
                        // when needed; otherwise return as-is.
                        conn.create_scalar_function(
                            "quote_ident",
                            1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let s: String = ctx.get(0).unwrap_or_default();
                                let needs_quoting = s.is_empty()
                                    || s.chars().any(|c| {
                                        !c.is_ascii_alphanumeric() && c != '_'
                                    })
                                    || s.chars().next().is_none_or(|c| c.is_ascii_digit())
                                    || is_sqlite_keyword(&s);
                                if needs_quoting {
                                    // Escape any embedded double quotes
                                    let escaped = s.replace('"', "\"\"");
                                    Ok(format!("\"{escaped}\""))
                                } else {
                                    Ok(s)
                                }
                            },
                        )?;

                        // string_to_array(str, delimiter) – splits a string
                        // by delimiter and returns a JSON array string (since
                        // SQLite has no native array type).  PG arrays are
                        // represented as JSON arrays throughout our emulation.
                        conn.create_scalar_function(
                            "string_to_array",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let s: String = ctx.get(0).unwrap_or_default();
                                let delim: String = ctx.get(1).unwrap_or_default();
                                if delim.is_empty() {
                                    // PG: empty delimiter splits into characters
                                    let chars: Vec<String> =
                                        s.chars().map(|c| c.to_string()).collect();
                                    return Ok(json_array(&chars));
                                }
                                let parts: Vec<String> = s.split(&delim).map(String::from).collect();
                                Ok(json_array(&parts))
                            },
                        )?;

                        // array_length(arr, dim) – returns the length of an
                        // array dimension.  We delegate to SQLite's builtin
                        // json_array_length since we represent PG arrays as
                        // JSON arrays.  dim is ignored (always 1).
                        conn.create_scalar_function(
                            "array_length",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let arr: String = ctx.get(0).unwrap_or_default();
                                let json_arr = if arr.starts_with('[') {
                                    arr
                                } else {
                                    pg_array_to_json(&arr)
                                };
                                let conn = unsafe { ctx.get_connection()? };
                                let n: i64 = conn
                                    .query_row(
                                        "SELECT json_array_length(?1)",
                                        [&json_arr],
                                        |row| row.get(0),
                                    )
                                    .unwrap_or(0);
                                Ok(n)
                            },
                        )?;

                        // array_lower(arr, dim) – returns the lower bound of
                        // an array dimension.  PG arrays are 1-indexed.
                        conn.create_scalar_function(
                            "array_lower",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |_ctx| Ok(1i64),
                        )?;

                        // array_upper(arr, dim) – returns the upper bound of
                        // an array dimension.  Same as array_length for 1-indexed.
                        conn.create_scalar_function(
                            "array_upper",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let arr: String = ctx.get(0).unwrap_or_default();
                                let json_arr = if arr.starts_with('[') {
                                    arr
                                } else {
                                    pg_array_to_json(&arr)
                                };
                                let conn = unsafe { ctx.get_connection()? };
                                let n: i64 = conn
                                    .query_row(
                                        "SELECT json_array_length(?1)",
                                        [&json_arr],
                                        |row| row.get(0),
                                    )
                                    .unwrap_or(0);
                                Ok(n)
                            },
                        )?;

                        // parse_ident(str) – parses a possibly-qualified
                        // identifier (e.g. "schema"."table") into an array.
                        // Returns a JSON array of the identifier parts.
                        conn.create_scalar_function(
                            "parse_ident",
                            -1,
                            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                            |ctx| {
                                let s: String = ctx.get(0).unwrap_or_default();
                                let parts = parse_pg_ident(&s);
                                Ok(json_array(&parts))
                            },
                        )?;

                        let schema = match compute_schema(&conn) {
                            Ok(schema) => schema,
                            Err(e) => {
                                error!("could not parse schema: {e}");
                                back_tx.blocking_send(
                                    (
                                        PgWireBackendMessage::ErrorResponse(
                                            ErrorInfo::new(
                                                "FATAL".into(),
                                                "XX000".into(),
                                                "could not parse database schema".into(),
                                            )
                                            .into(),
                                        ),
                                        true,
                                    )
                                        .into(),
                                )?;
                                return Ok(());
                            }
                        };

                        let mut session = Session {
                            agent,
                            conn: &conn,
                            tx_state: TxState::default(),
                        };

                        let mut prepared: HashMap<CompactString, Prepared> = HashMap::new();

                        let mut portals: HashMap<CompactString, Portal> = HashMap::new();

                        let mut discard_until_sync = false;

                        'outer: while let Some(msg) = front_rx.blocking_recv() {
                            debug!("msg: {msg:?}");

                            if discard_until_sync
                                && !matches!(
                                    msg,
                                    PgWireFrontendMessage::Sync(_)
                                        | PgWireFrontendMessage::Flush(_)
                                )
                            {
                                debug!("discarding message due to previous error");
                                continue;
                            }

                            match msg {
                                PgWireFrontendMessage::Startup(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "FATAL".into(),
                                                    SqlState::PROTOCOL_VIOLATION.code().into(),
                                                    "unexpected startup message".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                }
                                PgWireFrontendMessage::Parse(parse) => {
                                    let name: &str = parse.name.as_deref().unwrap_or_default();
                                    tracing::info!(target: "TOOL_DEBUG", query = %parse.query, "parse query");
                                    let (stripped_sql, mut cmds) = match parse_query(&parse.query) {
                                        Ok(cmds) => cmds,
                                        Err(e) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            discard_until_sync = true;
                                            continue;
                                        }
                                    };

                                    match cmds.pop_front() {
                                        None => {
                                            prepared.insert(name.into(), Prepared::Empty);
                                        }
                                        Some(parsed_cmd) => {
                                            if !cmds.is_empty() {
                                                back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            sql_state::SqlState::PROTOCOL_VIOLATION
                                                                .code()
                                                                .into(),
                                                            "only 1 command per Parse is allowed"
                                                                .into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                                discard_until_sync = true;
                                                continue;
                                            }

                                            trace!("parsed cmd: {parsed_cmd:#?}");

                                            let prepped = match session.conn.prepare(&stripped_sql) {
                                                Ok(prepped) => prepped,
                                                Err(e) => {
                                                    back_tx.blocking_send(
                                                        (
                                                            PgWireBackendMessage::ErrorResponse(
                                                                ErrorInfo::new(
                                                                    "ERROR".to_owned(),
                                                                    "XX000".to_owned(),
                                                                    e.to_string(),
                                                                )
                                                                .into(),
                                                            ),
                                                            true,
                                                        )
                                                            .into(),
                                                    )?;
                                                    discard_until_sync = true;
                                                    continue;
                                                }
                                            };

                                            let mut param_types: Vec<Type> = parse
                                                .type_oids
                                                .iter()
                                                .filter_map(|oid| Type::from_oid(*oid))
                                                .collect();

                                            debug!("params types {param_types:?}");
                                            debug!("prepped parameter count: {}", prepped.parameter_count());

                                            if param_types.len() != prepped.parameter_count() {
                                                let extracted_types = parameter_types(&schema, &parsed_cmd);


                                                param_types = match extracted_types {
                                                    Ok(extracted_types) => {
                                                        extracted_types.params
                                                    .into_iter()
                                                    .map(|param| {
                                                        trace!("got param: {param:?}");
                                                        match (param.sqlite_type, param.source) {
                                                            (SqliteType::Null, Some("TEXT[]")) => Type::TEXT_ARRAY,
                                                            (SqliteType::Null, Some("INT[]")) => Type::INT8_ARRAY,
                                                            (SqliteType::Null, Some("REAL[]")) => Type::FLOAT8_ARRAY,
                                                            (SqliteType::Null, Some("BLOB[]")) => Type::BYTEA_ARRAY,
                                                            (SqliteType::Null, Some("BOOL[]")) => Type::BOOL_ARRAY,
                                                            (SqliteType::Null, _) => unreachable!(),
                                                            (SqliteType::Text, src) => match src {
                                                                Some("JSON") => Type::JSON,
                                                                _ => Type::TEXT,
                                                            },
                                                            (SqliteType::Numeric, Some(src)) => {
                                                                match src {
                                                                    "BOOLEAN" | "BOOL" => {
                                                                        Type::BOOL
                                                                    }
                                                                    "DATETIME" => Type::TIMESTAMP,
                                                                    _ => Type::FLOAT8,
                                                                }
                                                            }
                                                            (SqliteType::Numeric, None) => {
                                                                Type::FLOAT8
                                                            }
                                                            (SqliteType::Integer, _src) => {
                                                                Type::INT8
                                                            }
                                                            (SqliteType::Real, _src) => {
                                                                Type::FLOAT8
                                                            }
                                                            (SqliteType::Blob, src) => match src {
                                                                Some("JSONB") => Type::JSONB,
                                                                _ => Type::BYTEA,
                                                            },
                                                        }
                                                    })
                                                    .collect()
                                                    }
                                                    Err(e) => {
                                                        back_tx.blocking_send(BackendResponse::Message {
                                                            message: e.into(),
                                                            flush: true,
                                                        })?;
                                                        discard_until_sync = true;
                                                        continue;
                                                    }
                                                };
                                            }

                                            let fields = match field_types(
                                                &prepped,
                                                &parsed_cmd,
                                                FieldFormats::All(FieldFormat::Text),
                                            ) {
                                                Ok(fields) => fields,
                                                Err(e) => {
                                                    back_tx
                                                        .blocking_send((e.into(), true).into())?;
                                                    discard_until_sync = true;
                                                    continue 'outer;
                                                }
                                            };

                                            prepared.insert(
                                                name.into(),
                                                Prepared::NonEmpty {
                                                    sql: stripped_sql.clone(),
                                                    param_types,
                                                    fields,
                                                    cmd: Box::new(parsed_cmd),
                                                },
                                            );
                                        }
                                    }

                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ParseComplete(
                                                ParseComplete::new(),
                                            ),
                                            false,
                                        )
                                            .into(),
                                    )?;
                                }
                                PgWireFrontendMessage::Describe(desc) => {
                                    let name = desc.name.as_deref().unwrap_or_default();
                                    match desc.target_type {
                                        // statement
                                        b'S' => match prepared.get(name) {
                                            None => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ErrorResponse(
                                                            ErrorInfo::new(
                                                                "ERROR".into(),
                                                                "XX000".into(),
                                                                "statement not found".into(),
                                                            )
                                                            .into(),
                                                        ),
                                                        true,
                                                    )
                                                        .into(),
                                                )?;
                                                discard_until_sync = true;
                                            }
                                            Some(Prepared::Empty) => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::NoData(NoData::new()),
                                                        false,
                                                    )
                                                        .into(),
                                                )?;
                                            }
                                            Some(Prepared::NonEmpty {
                                                param_types,
                                                fields,
                                                ..
                                            }) => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ParameterDescription(
                                                            ParameterDescription::new(
                                                                param_types
                                                                    .iter()
                                                                    .map(|t| t.oid())
                                                                    .collect(),
                                                            ),
                                                        ),
                                                        false,
                                                    )
                                                        .into(),
                                                )?;

                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::RowDescription(
                                                            RowDescription::new(
                                                                fields
                                                                    .iter()
                                                                    .map(Into::into)
                                                                    .collect(),
                                                            ),
                                                        ),
                                                        false,
                                                    )
                                                        .into(),
                                                )?;
                                            }
                                        },
                                        // portal
                                        b'P' => match portals.get(name) {
                                            None => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ErrorResponse(
                                                            ErrorInfo::new(
                                                                "ERROR".into(),
                                                                "XX000".into(),
                                                                "portal not found".into(),
                                                            )
                                                            .into(),
                                                        ),
                                                        true,
                                                    )
                                                        .into(),
                                                )?;
                                                discard_until_sync = true;
                                            }
                                            Some(Portal::Empty { .. }) => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::NoData(NoData::new()),
                                                        false,
                                                    )
                                                        .into(),
                                                )?;
                                            }
                                            Some(Portal::Parsed {
                                                stmt,
                                                result_formats,
                                                cmd,
                                                ..
                                            }) => {
                                                let fields = match field_types(
                                                    stmt,
                                                    cmd,
                                                    FieldFormats::Each(result_formats),
                                                ) {
                                                    Ok(fields) => fields,
                                                    Err(e) => {
                                                        back_tx.blocking_send(
                                                            (
                                                                PgWireBackendMessage::ErrorResponse(
                                                                    e.into(),
                                                                ),
                                                                true,
                                                            )
                                                                .into(),
                                                        )?;
                                                        continue 'outer;
                                                    }
                                                };

                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::RowDescription(
                                                            RowDescription::new(
                                                                fields
                                                                    .iter()
                                                                    .map(Into::into)
                                                                    .collect(),
                                                            ),
                                                        ),
                                                        false,
                                                    )
                                                        .into(),
                                                )?;
                                            }
                                        },
                                        _ => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "FATAL".into(),
                                                            SqlState::PROTOCOL_VIOLATION
                                                                .code()
                                                                .into(),
                                                            "unexpected describe type".into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            discard_until_sync = true;
                                            continue;
                                        }
                                    }
                                }
                                PgWireFrontendMessage::Bind(bind) => {
                                    let portal_name = bind
                                        .portal_name
                                        .as_deref()
                                        .map(CompactString::from)
                                        .unwrap_or_default();

                                    let stmt_name = bind.statement_name.as_deref().unwrap_or_default();

                                    match prepared.get(stmt_name) {
                                        None => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            "statement not found".into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            discard_until_sync = true;
                                            continue;
                                        }
                                        Some(Prepared::Empty) => {
                                            portals.insert(
                                                portal_name,
                                                Portal::Empty {
                                                    stmt_name: stmt_name.into(),
                                                },
                                            );
                                        }
                                        Some(Prepared::NonEmpty {
                                            sql,
                                            param_types,
                                            cmd,
                                            ..
                                        }) => {
                                            let mut prepped = match session.conn.prepare(sql) {
                                                Ok(prepped) => prepped,
                                                Err(e) => {
                                                    back_tx.blocking_send(
                                                        (
                                                            PgWireBackendMessage::ErrorResponse(
                                                                ErrorInfo::new(
                                                                    "ERROR".to_owned(),
                                                                    "XX000".to_owned(),
                                                                    e.to_string(),
                                                                )
                                                                .into(),
                                                            ),
                                                            true,
                                                        )
                                                            .into(),
                                                    )?;
                                                    discard_until_sync = true;
                                                    continue;
                                                }
                                            };

                                            trace!(
                                                "bind params count: {}, statement params count: {}",
                                                bind.parameters.len(),
                                                prepped.parameter_count()
                                            );

                                            debug!("bind param types: {param_types:?}");

                                            let mut format_codes = match bind
                                            .parameter_format_codes
                                            .iter()
                                            .map(|code| {
                                                Ok(match *code {
                                                    0 => FormatCode::Text,
                                                    1 => FormatCode::Binary,
                                                    n => return Err(UnknownFormatCode(n)),
                                                })
                                            })
                                            .collect::<Result<Vec<FormatCode>, UnknownFormatCode>>()
                                        {
                                            Ok(v) => v,
                                            Err(e) => {
                                                back_tx.blocking_send(
                                                    (
                                                        PgWireBackendMessage::ErrorResponse(
                                                            ErrorInfo::new(
                                                                "ERROR".to_owned(),
                                                                "XX000".to_owned(),
                                                                e.to_string(),
                                                            )
                                                            .into(),
                                                        ),
                                                        true,
                                                    )
                                                        .into(),
                                                )?;
                                                discard_until_sync = true;
                                                continue;
                                            }
                                        };
                                            if format_codes.is_empty() {
                                                // no format codes? default to text
                                                format_codes =
                                                    vec![FormatCode::Text; bind.parameters.len()];
                                            } else if format_codes.len() == 1 {
                                                // single code means we should use it for all others
                                                format_codes =
                                                    vec![format_codes[0]; bind.parameters.len()];
                                            }

                                            for (i, param) in bind.parameters.iter().enumerate() {
                                                let idx = i + 1;
                                                let b = match param {
                                                    None => {
                                                        trace!("binding idx {idx} w/ NULL");
                                                        if let Err(e) = prepped.raw_bind_parameter(
                                                            idx,
                                                            rusqlite::types::Null,
                                                        ) {
                                                            back_tx.blocking_send(
                                                            (
                                                                PgWireBackendMessage::ErrorResponse(
                                                                    ErrorInfo::new(
                                                                        "ERROR".to_owned(),
                                                                        "XX000".to_owned(),
                                                                        e.to_string(),
                                                                    )
                                                                    .into(),
                                                                ),
                                                                true,
                                                            )
                                                                .into(),
                                                        )?;
                                                            discard_until_sync = true;
                                                            continue 'outer;
                                                        }
                                                        continue;
                                                    }
                                                    Some(b) => b,
                                                };

                                                trace!("got param bytes: {b:?}");

                                                match param_types.get(i) {
                                                    None => {
                                                        trace!("no param type found!");
                                                        back_tx.blocking_send(
                                                            (
                                                                PgWireBackendMessage::ErrorResponse(
                                                                    ErrorInfo::new(
                                                                        "ERROR".to_owned(),
                                                                        "XX000".to_owned(),
                                                                        "missing parameter type"
                                                                            .into(),
                                                                    )
                                                                    .into(),
                                                                ),
                                                                true,
                                                            )
                                                                .into(),
                                                        )?;
                                                        discard_until_sync = true;
                                                        continue 'outer;
                                                    }
                                                    Some(param_type) => {
                                                        let format_code = format_codes[i];
                                                        trace!("parsing param_type {param_type:?}, format_code: {format_code:?}, bytes: {b:?}");
                                                        match param_type {
                                                            t @ &Type::BOOL => {
                                                                let value: bool =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::INT2 => {
                                                                let value: i16 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::INT4 => {
                                                                let value: i32 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::INT8 => {
                                                                let value: i64 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }

                                                            t @ &Type::TEXT
                                                            | t @ &Type::VARCHAR
                                                            | t @ &Type::JSON => {
                                                                let value: &str = match format_code
                                                                {
                                                                    FormatCode::Text => {
                                                                        std::str::from_utf8(b)?
                                                                    }
                                                                    FormatCode::Binary => {
                                                                        FromSql::from_sql(t, b)?
                                                                    }
                                                                };
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::FLOAT4 => {
                                                                let value: f32 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }
                                                            t @ &Type::FLOAT8 => {
                                                                let value: f64 =
                                                                    from_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ value: {value}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, value,
                                                                )?;
                                                            }

                                                            &Type::BYTEA | &Type::JSONB => {
                                                                let maybe_decoded = matches!(
                                                                    format_code,
                                                                    FormatCode::Text
                                                                )
                                                                .then(|| hex::decode(b).ok())
                                                                .flatten();

                                                                trace!("binding idx {idx} w/ decoded value: {maybe_decoded:?} (bytes: {b:?})");
                                                                prepped.raw_bind_parameter(
                                                                    idx,
                                                                    maybe_decoded
                                                                        .as_deref()
                                                                        .unwrap_or(b),
                                                                )?;
                                                            }

                                                            t @ &Type::TIMESTAMP => {
                                                                let dt = match format_code {
                                                                FormatCode::Text => {
                                                                    let s =
                                                                        String::from_utf8_lossy(b);
                                                                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f").map_err(ToParamError::Parse)?
                                                                }
                                                                FormatCode::Binary => {
                                                                    NaiveDateTime::from_sql(t, b)
                                                                        .map_err(
                                                                            ToParamError::<
                                                                                chrono::format::ParseError
                                                                            >::FromSql,
                                                                        )?
                                                                }
                                                            };
                                                                prepped
                                                                    .raw_bind_parameter(idx, dt)?;
                                                            }
                                                            t @ &Type::INT8_ARRAY => {
                                                                let value: Vec<i64> =
                                                                    from_array_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ array value: {value:?}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, Rc::new(value.into_iter().map(|v| v.into()).collect::<Vec<rusqlite::types::Value>>()),
                                                                )?;
                                                            }
                                                            t @ &Type::TEXT_ARRAY => {
                                                                let value: Vec<String> =
                                                                    from_array_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ array value: {value:?}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, Rc::new(value.into_iter().map(|v| v.into()).collect::<Vec<rusqlite::types::Value>>()),
                                                                )?;
                                                            }
                                                            t @ &Type::BYTEA_ARRAY => {
                                                                let value: Vec<Vec<u8>> =
                                                                    from_array_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ array value: {value:?}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, Rc::new(value.into_iter().map(|v| v.into()).collect::<Vec<rusqlite::types::Value>>()),
                                                                )?;
                                                            }
                                                            t @ &Type::FLOAT8_ARRAY => {
                                                                let value: Vec<f64> =
                                                                    from_array_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ array value: {value:?}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, Rc::new(value.into_iter().map(|v| v.into()).collect::<Vec<rusqlite::types::Value>>()),
                                                                )?;
                                                            }
                                                            t @ &Type::BOOL_ARRAY => {
                                                                let value: Vec<bool> =
                                                                    from_array_type_and_format(
                                                                        t,
                                                                        b,
                                                                        format_code,
                                                                    )?;
                                                                trace!("binding idx {idx} w/ array value: {value:?}");
                                                                prepped.raw_bind_parameter(
                                                                    idx, Rc::new(value.into_iter().map(|v| v.into()).collect::<Vec<rusqlite::types::Value>>()),
                                                                )?;
                                                            }
                                                        t => {
                                                            warn!("unsupported type: {t:?}");
                                                            back_tx.blocking_send(
                                                                (
                                                                    PgWireBackendMessage::ErrorResponse(
                                                                        ErrorInfo::new(
                                                                            "ERROR".to_owned(),
                                                                            "XX000".to_owned(),
                                                                            format!(
                                                                            "unsupported type {t} at index {i}"
                                                                        ),
                                                                        )
                                                                        .into(),
                                                                    ),
                                                                    true,
                                                                ).into(),
                                                            )?;
                                                                discard_until_sync = true;
                                                                continue 'outer;
                                                            }
                                                        }
                                                    }
                                                }
                                            }

                                            debug!("EXPANDED SQL: {:?}", prepped.expanded_sql());

                                            portals.insert(
                                                portal_name,
                                                Portal::Parsed {
                                                    stmt_name: stmt_name.into(),
                                                    stmt: prepped,
                                                    result_formats: bind
                                                        .result_column_format_codes
                                                        .iter()
                                                        .copied()
                                                        .map(FieldFormat::from)
                                                        .collect(),
                                                    cmd: cmd.clone(),
                                                },
                                            );
                                        }
                                    }

                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::BindComplete(BindComplete::new()),
                                            false,
                                        )
                                            .into(),
                                    )?;
                                }
                                PgWireFrontendMessage::Sync(_) => {
                                    send_ready(&mut session, discard_until_sync, &back_tx)?;

                                    // reset this
                                    discard_until_sync = false;
                                }
                                PgWireFrontendMessage::Execute(execute) => {
                                    let name = execute.name.as_deref().unwrap_or_default();
                                    let (prepped, result_formats, cmd) = match portals.get_mut(name)
                                    {
                                        Some(Portal::Empty { .. }) => {
                                            trace!("empty portal");
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::EmptyQueryResponse(
                                                        EmptyQueryResponse::new(),
                                                    ),
                                                    false,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                        Some(Portal::Parsed {
                                            stmt,
                                            result_formats,
                                            cmd,
                                            ..
                                        }) => (stmt, result_formats, cmd),
                                        None => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".into(),
                                                            "XX000".into(),
                                                            "portal not found".into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            discard_until_sync = true;
                                            continue;
                                        }
                                    };

                                    trace!("non-empty portal!");

                                    let max_rows = if execute.max_rows <= 0 {
                                        usize::MAX
                                    } else {
                                        execute.max_rows as usize
                                    };

                                    if let Err(e) = session.handle_execute(
                                        prepped,
                                        result_formats,
                                        cmd,
                                        max_rows,
                                        &back_tx,
                                    ) {
                                        debug!("error in execute: {e}");

                                        back_tx.blocking_send(BackendResponse::Message {
                                            message: e.try_into()?,
                                            flush: true,
                                        })?;

                                        discard_until_sync = true;

                                        send_ready(
                                            &mut session,
                                            discard_until_sync,
                                            &back_tx,
                                        )?;
                                        continue;
                                    }
                                }
                                PgWireFrontendMessage::Query(query) => {
                                    tracing::info!(target: "TOOL_DEBUG", query = %query.query, "simple query");
                                    let (_stripped_sql, parsed_query) = match parse_query(&query.query) {
                                        Ok(q) => q,
                                        Err(e) => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            send_ready(
                                                &mut session,
                                                discard_until_sync,
                                                &back_tx,
                                            )?;
                                            continue;
                                        }
                                    };

                                    if parsed_query.is_empty() {
                                        back_tx.blocking_send(
                                            (
                                                PgWireBackendMessage::EmptyQueryResponse(
                                                    EmptyQueryResponse::new(),
                                                ),
                                                false,
                                            )
                                                .into(),
                                        )?;

                                        send_ready(
                                            &mut session,
                                            discard_until_sync,
                                            &back_tx,
                                        )?;
                                        continue;
                                    }

                                    for cmd in parsed_query.into_iter() {
                                        if let Err(e) =
                                            session.handle_query(&cmd, &back_tx, true)
                                        {
                                            back_tx.blocking_send(BackendResponse::Message {
                                                message: e.try_into()?,
                                                flush: true,
                                            })?;
                                            send_ready(
                                                &mut session,
                                                discard_until_sync,
                                                &back_tx,
                                            )?;
                                            continue 'outer;
                                        }
                                    }

                                    // automatically commit an implicit tx
                                    if session.tx_state.is_implicit() {
                                        trace!("committing IMPLICIT tx");
                                        let permits = session.tx_state.end();

                                        let commit_res = if let Some((_permit, bookie_write)) = permits {
                                            session.handle_commit(bookie_write)
                                        } else {
                                            session.commit_db()
                                        };

                                        if let Err(e) = commit_res {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "ERROR".to_owned(),
                                                            "XX000".to_owned(),
                                                            e.to_string(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            send_ready(
                                                &mut session,
                                                discard_until_sync,
                                                &back_tx,
                                            )?;
                                            continue;
                                        }
                                        trace!("committed IMPLICIT tx");
                                    }

                                    send_ready(&mut session, discard_until_sync, &back_tx)?;
                                }
                                PgWireFrontendMessage::Terminate(_) => {
                                    break;
                                }

                                PgWireFrontendMessage::PasswordMessageFamily(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "PasswordMessage is not implemented".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                }
                                PgWireFrontendMessage::Close(close) => {
                                    let name = close.name.as_deref().unwrap_or_default();
                                    match close.target_type {
                                        // statement
                                        b'S' => {
                                            if prepared.remove(name).is_some() {
                                                portals
                                                    .retain(|_, portal| portal.stmt_name() != name);
                                            }
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::CloseComplete(
                                                        CloseComplete::new(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            continue;
                                        }
                                        // portal
                                        b'P' => {
                                            portals.remove(name);
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::CloseComplete(
                                                        CloseComplete::new(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                        }
                                        _ => {
                                            back_tx.blocking_send(
                                                (
                                                    PgWireBackendMessage::ErrorResponse(
                                                        ErrorInfo::new(
                                                            "FATAL".into(),
                                                            SqlState::PROTOCOL_VIOLATION
                                                                .code()
                                                                .into(),
                                                            "unexpected Close target_type".into(),
                                                        )
                                                        .into(),
                                                    ),
                                                    true,
                                                )
                                                    .into(),
                                            )?;
                                            discard_until_sync = true;
                                            continue;
                                        }
                                    }
                                }
                                PgWireFrontendMessage::Flush(_) => {
                                    back_tx.blocking_send(BackendResponse::Flush)?;
                                }
                                PgWireFrontendMessage::CopyData(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "CopyData is not implemented".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                }
                                PgWireFrontendMessage::CopyFail(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "CopyFail is not implemented".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                }
                                PgWireFrontendMessage::CopyDone(_) => {
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "CopyDone is not implemented".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                }
                                PgWireFrontendMessage::CancelRequest(_) => {
                                    // cancel should be sent as first message on a new connection.
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "Unexpected Cancel message".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;
                                },
                                PgWireFrontendMessage::SslNegotiation(_) => {
                                    // SSL Negotiation should be sent as first message on a new connection.
                                    back_tx.blocking_send(
                                        (
                                            PgWireBackendMessage::ErrorResponse(
                                                ErrorInfo::new(
                                                    "ERROR".into(),
                                                    "XX000".to_owned(),
                                                    "Unexpected SSL Negotiation message".into(),
                                                )
                                                .into(),
                                            ),
                                            true,
                                        )
                                            .into(),
                                    )?;
                                    continue;

                                },
                                PgWireFrontendMessage::PortalSuspended(_) => {
                                    // this shouldn't happen, backend sends this msg.
                                },
                            }
                        }

                        Ok::<_, BoxError>(())
                    }
                }).await;

                match res {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        error!("connection failed: {e}");
                        _ = back_tx
                            .send(
                                (
                                    PgWireBackendMessage::ErrorResponse(
                                        ErrorInfo::new(
                                            "FATAL".to_owned(),
                                            "XX000".to_owned(),
                                            e.to_string(),
                                        )
                                        .into(),
                                    ),
                                    true,
                                )
                                    .into(),
                            )
                            .await;
                    }
                    Err(e) => {
                        error!("spawn_blocking failed: {e}");
                        _ = back_tx
                            .send(
                                (
                                    PgWireBackendMessage::ErrorResponse(
                                        ErrorInfo::new(
                                            "FATAL".to_owned(),
                                            "XX000".to_owned(),
                                            e.to_string(),
                                        )
                                        .into(),
                                    ),
                                    true,
                                )
                                    .into(),
                            )
                            .await;
                    }
                }

                // The message-handling loop has completed, make sure we also abort the tasks
                // handling the TCP connection
                // Firstly we attempt a graceful shutdown -- dropping back_tx will cause

                // backend_task to complete once it writes all content to the TCP socket
                // Then, frontend_task will eventually receive an EOF if clients behave properly
                // Note that this should be the only reference of back_tx at this point:
                // the one in frontend_task is weak, and the one cloned into the message-handling
                // thread should have been dropped.
                task_cancellation.remove(conn_id).await;
                assert_eq!(back_tx.strong_count(), 1);
                drop(back_tx);

                // Now we wait for both front and back to complete; if however frontend_task never
                // receives an EOF, instead of relying on half-open timeout we just abort both tasks
                // after 1 minute.
                match async { tokio::join!(&mut frontend_task, &mut backend_task) }
                    .with_timeout(Duration::from_secs(60))
                    .await
                {
                    Outcome::Preempted(_) => {
                        frontend_task.abort();
                        backend_task.abort();
                    }
                    Outcome::Completed(_) => {}
                }

                Ok::<_, BoxError>(())
            });
        }

        info!("postgres server done");

        Ok::<_, BoxError>(())
    });

    Ok(PgServer { local_addr })
}

struct Session<'conn> {
    agent: Agent,
    conn: &'conn CrConn,
    tx_state: TxState,
}

impl<'conn> Session<'conn> {
    fn handle_query(
        &mut self,
        cmd: &ParsedCmd,
        back_tx: &Sender<BackendResponse>,
        send_row_desc: bool,
    ) -> Result<(), QueryError> {
        if cmd.is_show() {
            back_tx
                .blocking_send(
                    (
                        PgWireBackendMessage::CommandComplete(CommandComplete::new("SHOW".into())),
                        true,
                    )
                        .into(),
                )
                .map_err(|_| QueryError::BackendResponseSendFailed)?;
            return Ok(());
        }

        if cmd.is_set() {
            back_tx
                .blocking_send(
                    (
                        PgWireBackendMessage::CommandComplete(CommandComplete::new("SET".into())),
                        true,
                    )
                        .into(),
                )
                .map_err(|_| QueryError::BackendResponseSendFailed)?;
            return Ok(());
        }

        // need to start an implicit transaction
        if self.tx_state.is_ended() && !cmd.is_begin() {
            self.conn.execute_batch("BEGIN")?;
            trace!("started IMPLICIT tx");
            self.tx_state.start_implicit();
        } else if self.tx_state.is_implicit() && cmd.is_begin() {
            trace!("committing IMPLICIT tx");
            let permits = self.tx_state.end();

            if let Some((_permit, bookie_write)) = permits {
                self.handle_commit(bookie_write)?;
            } else {
                self.commit_db()?;
            }
            trace!("committed IMPLICIT tx");
        }

        let tag = cmd.tag();

        let mut changes = 0usize;

        let count = if cmd.is_begin() {
            self.conn.execute_batch("BEGIN")?;
            self.tx_state.start_explicit();
            0
        } else if cmd.is_commit() {
            let permits = self.tx_state.end();
            if let Some((_permit, bookie_write)) = permits {
                self.handle_commit(bookie_write)?;
            } else {
                self.commit_db()?;
            }
            0
        } else if cmd.is_rollback() {
            let _permits = self.tx_state.end();
            self.conn.execute_batch("ROLLBACK")?;
            0
        } else {
            let mut prepped = if cmd.is_pg() {
                return Err(QueryError::NotSqlite);
            } else {
                self.conn.prepare(&cmd.to_string())?
            };

            let fields = field_types(&prepped, cmd, FieldFormats::All(FieldFormat::Text))?;

            if send_row_desc {
                back_tx
                    .blocking_send(
                        (
                            PgWireBackendMessage::RowDescription(RowDescription::new(
                                fields.iter().map(Into::into).collect(),
                            )),
                            true,
                        )
                            .into(),
                    )
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            let schema = Arc::new(fields);

            if !self.tx_state.is_writing() && !prepped.readonly() {
                trace!("query statement writes, acquiring permit...");
                let write_permit = self.agent.write_permit_blocking()?;
                let bookie_permit = self.agent.bookie().write_lock_blocking();
                self.tx_state.set_write_context(write_permit, bookie_permit);

                counter!("corro.acquired.write.permit.count", "protocol" => "pg").increment(1);
                self.set_ts()?;
            }

            let mut rows = prepped.raw_query();
            let ncols = schema.len();

            let mut count = 0;
            while let Some(row) = rows.next()? {
                count += 1;
                let mut encoder = DataRowEncoder::new(schema.clone());
                for idx in 0..ncols {
                    let data = row.get_ref_unwrap::<usize>(idx);
                    match data {
                        ValueRef::Null => encoder.encode_field(&None::<i8>).unwrap(),
                        ValueRef::Integer(i) => {
                            encoder.encode_field(&i).unwrap();
                        }
                        ValueRef::Real(f) => {
                            encoder.encode_field(&f).unwrap();
                        }
                        ValueRef::Text(t) => {
                            encoder
                                .encode_field(&String::from_utf8_lossy(t).as_ref())
                                .unwrap();
                        }
                        ValueRef::Blob(b) => {
                            encoder.encode_field(&b).unwrap();
                        }
                    }
                }
                let data_row = encoder.take_row();
                back_tx
                    .blocking_send((PgWireBackendMessage::DataRow(data_row), false).into())
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            if tag.returns_rows_affected() {
                changes = self.conn.changes() as usize;
            }

            count
        };

        back_tx
            .blocking_send(
                (
                    PgWireBackendMessage::CommandComplete(
                        tag.into_command_complete(count, changes),
                    ),
                    true,
                )
                    .into(),
            )
            .map_err(|_| QueryError::BackendResponseSendFailed)?;

        if cmd.is_begin() {
            trace!("setting EXPLICIT tx");
            // explicit tx
            self.tx_state.start_explicit();
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_execute(
        &mut self,
        prepped: &mut Statement<'conn>,
        result_formats: &[FieldFormat],
        cmd: &ParsedCmd,
        max_rows: usize,
        back_tx: &Sender<BackendResponse>,
    ) -> Result<(), QueryError> {
        // TODO: maybe we don't need to recompute this...
        let fields = field_types(prepped, cmd, FieldFormats::Each(result_formats))?;

        trace!("fields: {fields:?}");

        let schema = Arc::new(fields);

        // we need to know because we'll commit it right away
        let mut opened_implicit_tx = false;

        if self.tx_state.is_ended() {
            debug!("tx is_ended");
            if !cmd.is_begin() && !prepped.readonly() {
                debug!("tx is_ended && !cmd.is_begin() && !prepped.readonly()");
                // NOT in a tx and statement mutates DB...
                self.conn.execute_batch("BEGIN")?;

                self.tx_state.start_implicit();
                opened_implicit_tx = true;
            } else if cmd.is_begin() {
                debug!("cmd is BEGIN");
                self.conn.execute_batch("BEGIN")?;
                self.tx_state.start_explicit();
                debug!("started EXPLICIT tx");
            }
        }

        let tag = cmd.tag();

        let mut count = 0;
        let mut changes = 0usize;

        if cmd.is_commit() {
            let permits = self.tx_state.end();
            if let Some((_permit, bookie_write)) = permits {
                self.handle_commit(bookie_write)?;
            } else {
                self.commit_db()?;
            }
        } else if cmd.is_begin() {
            // do nothing
            debug!("cmd is BEGIN");
        } else {
            if !self.tx_state.is_writing() && !prepped.readonly() {
                trace!("statement writes, acquiring permit...");
                let write_permit = self.agent.write_permit_blocking()?;
                let bookie_permit = self.agent.bookie().write_lock_blocking();
                self.tx_state.set_write_context(write_permit, bookie_permit);

                self.set_ts()?;
            }
            let mut rows = prepped.raw_query();
            loop {
                if count >= max_rows {
                    trace!("attained max rows");
                    // forget the Rows iterator here so as to not reset the statement!
                    std::mem::forget(rows);
                    back_tx
                        .blocking_send(
                            (
                                PgWireBackendMessage::PortalSuspended(PortalSuspended::new()),
                                true,
                            )
                                .into(),
                        )
                        .map_err(|_| QueryError::BackendResponseSendFailed)?;
                    return Ok(());
                }

                let row = match rows.next()? {
                    Some(row) => {
                        trace!("got a row: {row:?}");
                        row
                    }
                    None => {
                        trace!("done w/ rows");
                        break;
                    }
                };

                count += 1;

                let mut encoder = DataRowEncoder::new(schema.clone());
                for (idx, field) in schema.iter().enumerate() {
                    trace!("processing field: {field:?}");
                    let format = field.format();
                    let format_opts = field.format_options().as_ref();
                    match field.datatype() {
                        &Type::ANY => {
                            let data = row.get_ref_unwrap(idx);
                            match data {
                                ValueRef::Null => encoder
                                    .encode_field_with_type_and_format(
                                        &None::<i8>,
                                        &Type::ANY,
                                        format,
                                        format_opts,
                                    )
                                    .unwrap(),
                                ValueRef::Integer(i) => {
                                    encoder
                                        .encode_field_with_type_and_format(
                                            &i,
                                            &Type::INT8,
                                            format,
                                            format_opts,
                                        )
                                        .unwrap();
                                }
                                ValueRef::Real(f) => {
                                    encoder
                                        .encode_field_with_type_and_format(
                                            &f,
                                            &Type::FLOAT8,
                                            format,
                                            format_opts,
                                        )
                                        .unwrap();
                                }
                                ValueRef::Text(t) => {
                                    encoder
                                        .encode_field_with_type_and_format(
                                            &String::from_utf8_lossy(t).as_ref(),
                                            &Type::TEXT,
                                            format,
                                            format_opts,
                                        )
                                        .unwrap();
                                }
                                ValueRef::Blob(b) => {
                                    encoder
                                        .encode_field_with_type_and_format(
                                            &b,
                                            &Type::BYTEA,
                                            format,
                                            format_opts,
                                        )
                                        .unwrap();
                                }
                            }
                        }
                        t @ &Type::BOOL => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<bool>>(idx)?,
                                    t,
                                    format,
                                    format_opts,
                                )
                                .unwrap();
                        }
                        t @ &Type::INT8 => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<i64>>(idx)?,
                                    t,
                                    format,
                                    format_opts,
                                )
                                .unwrap();
                        }
                        t @ &Type::TIMESTAMP => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<NaiveDateTime>>(idx)?,
                                    t,
                                    format,
                                    format_opts,
                                )
                                .unwrap();
                        }
                        t @ &Type::VARCHAR | t @ &Type::TEXT | t @ &Type::JSON => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<String>>(idx)?,
                                    t,
                                    format,
                                    format_opts,
                                )
                                .unwrap();
                        }
                        t @ &Type::BYTEA | t @ &Type::JSONB => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<Vec<u8>>>(idx)?,
                                    t,
                                    format,
                                    format_opts,
                                )
                                .unwrap();
                        }
                        t @ &Type::FLOAT8 => {
                            encoder
                                .encode_field_with_type_and_format(
                                    &row.get::<_, Option<f64>>(idx)?,
                                    t,
                                    format,
                                    format_opts,
                                )
                                .unwrap();
                        }
                        _ => {
                            return Err(
                                UnsupportedSqliteToPostgresType(field.name().to_owned()).into()
                            )
                        }
                    }
                }

                let data_row = encoder.take_row();
                back_tx
                    .blocking_send((PgWireBackendMessage::DataRow(data_row), false).into())
                    .map_err(|_| QueryError::BackendResponseSendFailed)?;
            }

            if tag.returns_rows_affected() {
                changes = self.conn.changes() as usize;
            }

            if opened_implicit_tx {
                let permits = self.tx_state.end();
                if let Some((_permit, bookie_write)) = permits {
                    self.handle_commit(bookie_write)?;
                } else {
                    self.commit_db()?;
                }
            }
        }

        trace!("done w/ rows, computing tag: {tag:?}");

        // done!
        back_tx
            .blocking_send(
                (
                    PgWireBackendMessage::CommandComplete(
                        tag.into_command_complete(count, changes),
                    ),
                    true,
                )
                    .into(),
            )
            .map_err(|_| QueryError::BackendResponseSendFailed)?;

        Ok(())
    }

    fn commit_db(&self) -> Result<(), ChangeError> {
        let actor_id = self.agent.actor_id();
        self.conn
            .execute_batch("COMMIT")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;
        Ok(())
    }

    fn handle_commit(&self, bookie_write: BookieWriteGuard) -> Result<(), ChangeError> {
        trace!("HANDLE COMMIT");

        let actor_id = self.agent.actor_id();

        let mut book_writer = bookie_write.write_tx(self.agent.booked());

        let insert_info = insert_local_changes(&self.agent, self.conn, &mut book_writer)?;
        self.conn
            .execute_batch("COMMIT")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        if let Some(InsertChangesInfo {
            db_version,
            last_seq,
            ts,
        }) = insert_info
        {
            trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

            book_writer.commit();

            let agent = self.agent.clone();

            spawn_counted(async move { broadcast_changes(agent, db_version, last_seq, ts).await });
        }

        Ok(())
    }

    fn set_ts(&self) -> Result<(), rusqlite::Error> {
        let ts = Timestamp::from(self.agent.clock().new_timestamp());

        let _ = self
            .conn
            .prepare_cached("SELECT crsql_set_ts(?)")?
            .query_row([&ts], |row| row.get::<_, String>(0))?;

        Ok(())
    }
}

impl<'conn> Drop for Session<'conn> {
    fn drop(&mut self) {
        if !self.tx_state.is_ended() {
            let _permits = self.tx_state.end();
            if let Err(e) = self.conn.execute_batch("ROLLBACK") {
                warn!("failed to rollback tx: {e}");
            } else {
                debug!("rolled back tx");
            }
        }
    }
}

fn send_ready(
    session: &mut Session,
    discard_until_sync: bool,
    back_tx: &Sender<BackendResponse>,
) -> Result<(), BoxError> {
    let ready_status = if session.tx_state.is_implicit() {
        let permits = session.tx_state.end(); // do this first, in case of failure
        if discard_until_sync {
            // an error occurred, rollback implicit tx!
            warn!("receive Sync message w/ an error to send, rolling back implicit tx");
            session.conn.execute_batch("ROLLBACK")?;
        } else {
            // no error, commit implicit tx
            warn!("receive Sync message, committing implicit tx");
            if let Some((_permit, bookie_write)) = permits {
                session.handle_commit(bookie_write)?;
            } else {
                session.commit_db()?;
            }
        }

        TransactionStatus::Idle
    } else if session.tx_state.is_explicit() {
        if discard_until_sync {
            TransactionStatus::Error
        } else {
            TransactionStatus::Transaction
        }
    } else {
        TransactionStatus::Idle
    };

    back_tx.blocking_send(
        (
            PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(ready_status)),
            true,
        )
            .into(),
    )?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum QueryError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Unsupported(#[from] UnsupportedSqliteToPostgresType),
    #[error(transparent)]
    UntypedUnnest(#[from] UntypedUnnestParameter),
    #[error("statement is not parsable as SQLite-flavored SQL")]
    NotSqlite,
    #[error(transparent)]
    PgWire(#[from] PgWireError),
    #[error("backend response channel is closed")]
    BackendResponseSendFailed,
    #[error("could not acquire write permit")]
    PermitAcquire(#[from] AcquireError),
    #[error(transparent)]
    Change(#[from] ChangeError),
}

#[derive(Debug, thiserror::Error)]
#[error("channel is closed")]
struct ChannelClosed;

impl TryFrom<QueryError> for PgWireBackendMessage {
    type Error = ChannelClosed;

    fn try_from(value: QueryError) -> Result<Self, Self::Error> {
        Ok(PgWireBackendMessage::ErrorResponse(match value {
            QueryError::Rusqlite(e) => match &e {
                rusqlite::Error::SqliteFailure(sqlite_error, _maybe_sqlite_message)
                    if sqlite_error.extended_code == SQLITE_CONSTRAINT_UNIQUE =>
                {
                    ErrorInfo::new(
                        "ERROR".to_owned(),
                        SqlState::UNIQUE_VIOLATION.code().into(),
                        e.to_string(),
                    )
                    .into()
                }
                _ => ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into(),
            },
            QueryError::Unsupported(e) => e.into(),
            QueryError::UntypedUnnest(e) => e.into(),
            e @ QueryError::NotSqlite => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            QueryError::PgWire(e) => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            e @ QueryError::PermitAcquire(_) => {
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
            QueryError::BackendResponseSendFailed => return Err(ChannelClosed),
            QueryError::Change(e) => {
                ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string()).into()
            }
        }))
    }
}

#[derive(Clone, Copy, Debug)]
#[repr(i16)]
pub enum FormatCode {
    Text = 0,
    Binary,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown format code {0}")]
pub struct UnknownFormatCode(i16);

#[derive(Debug, thiserror::Error)]
pub enum ToParamError<E> {
    #[error("conversion from bytes to types failed: {0}")]
    FromSql(Box<dyn std::error::Error + Sync + Send>),
    #[error(transparent)]
    Utf8(#[from] Utf8Error),
    #[error("parse error: {0}")]
    Parse(E),
}

fn from_type_and_format<'a, E, T: FromSql<'a> + FromStr<Err = E>>(
    t: &Type,
    b: &'a [u8],
    format_code: FormatCode,
) -> Result<T, ToParamError<E>>
// where
//     FromStr::Err =,
{
    Ok(match format_code {
        FormatCode::Text => {
            T::from_str(std::str::from_utf8(b)?).map_err(|e| ToParamError::<E>::Parse(e))?
        }
        FormatCode::Binary => T::from_sql(t, b).map_err(ToParamError::FromSql)?,
    })
}

fn from_array_type_and_format<'a, T>(
    t: &Type,
    b: &'a [u8],
    format_code: FormatCode,
) -> Result<Vec<T>, ToParamError<String>>
where
    T: FromSql<'a> + for<'b> FromSqlText<'b>,
{
    let format_opts = FormatOptions::default();
    Ok(match format_code {
        FormatCode::Text => {
            Vec::<T>::from_vec_sql_text(t, b, &format_opts).map_err(ToParamError::FromSql)?
        }
        FormatCode::Binary => Vec::<T>::from_sql(t, b).map_err(ToParamError::FromSql)?,
    })
}

#[derive(Debug, thiserror::Error)]
#[error("Unsupported data type: {0}")]
struct UnsupportedSqliteToPostgresType(String);

impl From<UnsupportedSqliteToPostgresType> for PgWireBackendMessage {
    fn from(value: UnsupportedSqliteToPostgresType) -> Self {
        PgWireBackendMessage::ErrorResponse(value.into())
    }
}

impl From<UnsupportedSqliteToPostgresType> for ErrorResponse {
    fn from(value: UnsupportedSqliteToPostgresType) -> Self {
        ErrorInfo::new("ERROR".to_owned(), "42846".to_owned(), value.to_string()).into()
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Untyped array argument for unnest() (or corro_unnest()), please use CAST($N AS T) where T is one of: TEXT[] BLOB[] INT[] INTEGER[] BIGINT[] REAL[] FLOAT[] DOUBLE[] BOOL[] BOOLEAN[]")]
struct UntypedUnnestParameter;

impl From<UntypedUnnestParameter> for PgWireBackendMessage {
    fn from(value: UntypedUnnestParameter) -> Self {
        PgWireBackendMessage::ErrorResponse(value.into())
    }
}

impl From<UntypedUnnestParameter> for ErrorResponse {
    fn from(value: UntypedUnnestParameter) -> Self {
        ErrorInfo::new("ERROR".to_owned(), "42804".to_owned(), value.to_string()).into()
    }
}

#[allow(clippy::result_large_err)]
fn name_to_type(name: &str) -> Result<Type, UnsupportedSqliteToPostgresType> {
    // Strip any type modifiers (e.g., "VARCHAR(255)" -> "VARCHAR") so that
    // parameterized types map correctly.
    let base = name.split('(').next().unwrap_or(name).trim().to_uppercase();
    Ok(match base.as_ref() {
        "ANY" => Type::ANY,
        "INT" | "INTEGER" | "BIGINT" => Type::INT8,
        "DATETIME" | "TIMESTAMP" => Type::TIMESTAMP,
        "VARCHAR" | "CHARACTER VARYING" | "CHAR VARYING" => Type::VARCHAR,
        "TEXT" => Type::TEXT,
        "BINARY" | "BLOB" => Type::BYTEA,
        "JSONB" => Type::JSONB,
        "JSON" => Type::JSON,
        "FLOAT" | "REAL" | "DOUBLE" | "DOUBLE PRECISION" => Type::FLOAT8,
        "BOOL" | "BOOLEAN" => Type::BOOL,
        "NUMERIC" | "DECIMAL" => Type::NUMERIC,
        "CHAR" | "CHARACTER" => Type::BPCHAR,
        "CLOB" => Type::TEXT,
        "DATE" => Type::DATE,
        "TIME" => Type::TIME,
        _ => return Err(UnsupportedSqliteToPostgresType(name.to_string())),
    })
}

/// Maps a PostgreSQL type OID to its human-readable type name, matching the
/// behaviour of PostgreSQL's `format_type()` builtin.  Used by tools like
/// TablePlus to display column types.
fn format_type_oid(oid: u32) -> String {
    match oid {
        16 => "boolean".into(),
        17 => "bytea".into(),
        18 => "character".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        25 => "text".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1042 => "character".into(),
        1043 => "character varying".into(),
        1082 => "date".into(),
        1083 => "time without time zone".into(),
        1114 => "timestamp without time zone".into(),
        114 => "json".into(),
        3802 => "jsonb".into(),
        1700 => "numeric".into(),
        _ => "unknown".into(),
    }
}

fn compute_schema(conn: &Connection) -> Result<Schema, Box<SchemaError>> {
    fn dump_sql(conn: &Connection) -> Result<String, rusqlite::Error> {
        let mut dump = String::new();

        let tables: HashMap<String, String> = conn
            .prepare(r#"SELECT name, sql FROM sqlite_schema WHERE type = "table" AND name IS NOT NULL AND sql IS NOT NULL ORDER BY tbl_name"#)?
            .query_map((), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;

        for sql in tables.values() {
            dump.push_str(sql.as_str());
            dump.push(';');
        }

        let indexes: HashMap<String, String> = conn
            .prepare(r#"SELECT name, sql FROM sqlite_schema WHERE type = "index" AND name IS NOT NULL AND sql IS NOT NULL ORDER BY tbl_name"#)?
            .query_map((), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<_>>()?;

        for sql in indexes.values() {
            dump.push_str(sql.as_str());
            dump.push(';');
        }

        Ok(dump)
    }

    let dump = dump_sql(conn).map_err(|err| Box::new(SchemaError::from(err)))?;
    parse_sql(&dump)
}

#[derive(Debug)]
enum ParamKind<'a> {
    Named(&'a str),
    Positional,
}

fn as_param(expr: &Expr) -> Option<ParamKind<'_>> {
    if let Expr::Variable(name) = expr {
        if name.is_empty() {
            Some(ParamKind::Positional)
        } else {
            Some(ParamKind::Named(name.as_str()))
        }
    } else {
        None
    }
}

enum SqliteNameRef<'a> {
    Id(&'a Id),
    Name(&'a Name),
    Qualified(&'a Name, &'a Name),
    DoublyQualified(&'a Name, &'a Name, &'a Name),
}

impl<'a> SqliteNameRef<'a> {
    fn to_owned(&self) -> SqliteName {
        match self {
            SqliteNameRef::Id(id) => SqliteName::Id((*id).clone()),
            SqliteNameRef::Name(name) => SqliteName::Name((*name).clone()),
            SqliteNameRef::Qualified(n0, n1) => SqliteName::Qualified((*n0).clone(), (*n1).clone()),
            SqliteNameRef::DoublyQualified(n0, n1, n2) => {
                SqliteName::DoublyQualified((*n0).clone(), (*n1).clone(), (*n2).clone())
            }
        }
    }
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
enum SqliteName {
    Id(Id),
    Name(Name),
    Qualified(Name, Name),
    DoublyQualified(Name, Name, Name),
}

fn expr_to_name(expr: &Expr) -> Option<SqliteNameRef<'_>> {
    match expr {
        Expr::Id(id) => Some(SqliteNameRef::Id(id)),
        Expr::Name(name) => Some(SqliteNameRef::Name(name)),
        Expr::Qualified(n0, n1) => Some(SqliteNameRef::Qualified(n0, n1)),
        Expr::DoublyQualified(n0, n1, n2) => Some(SqliteNameRef::DoublyQualified(n0, n1, n2)),
        _ => None,
    }
}

fn handle_lhs_rhs<'stmt>(
    lhs: &'stmt Expr,
    rhs: &'stmt Expr,
) -> Option<(SqliteName, ParamKind<'stmt>)> {
    match (
        (expr_to_name(lhs), as_param(lhs)),
        (expr_to_name(rhs), as_param(rhs)),
    ) {
        ((Some(name), _), (_, Some(kind))) | ((_, Some(kind)), (Some(name), _)) => {
            Some((name.to_owned(), kind))
        }
        _ => None,
    }
}

fn extract_params<'schema, 'stmt>(
    schema: &'schema Schema,
    expr: &'stmt Expr,
    tables: &HashMap<String, &'schema Table>,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Result<(), UntypedUnnestParameter> {
    match expr {
        // expr BETWEEN expr AND expr
        Expr::Between {
            lhs: _,
            start: _,
            end: _,
            not: _,
        } => {}

        // expr operator expr
        Expr::Binary(lhs, _, rhs) => {
            if let Some((name, kind)) = handle_lhs_rhs(lhs, rhs) {
                match name {
                    // not aliased!
                    SqliteName::Id(id) => {
                        // find the first one to match
                        for (_, table) in tables.iter() {
                            if let Some(col) = table.columns.get(&id.0) {
                                let (sqlite_type, source) = col.sql_type();
                                params.insert(Param {
                                    kind,
                                    sqlite_type,
                                    source,
                                });
                                break;
                            }
                        }
                    }
                    SqliteName::Name(_) => {}
                    SqliteName::Qualified(tbl_name, col_name)
                    | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                        trace!("looking tbl {} for col {}", tbl_name.0, col_name.0);
                        if let Some(table) = tables.get(&tbl_name.0) {
                            trace!("found table! {}", table.name);
                            let col_name = if col_name.0.starts_with('"') {
                                rem_first_and_last(&col_name.0)
                            } else {
                                &col_name.0
                            };

                            if let Some(col) = table.columns.get(col_name) {
                                let (sqlite_type, source) = col.sql_type();
                                params.insert(Param {
                                    kind,
                                    sqlite_type,
                                    source,
                                });
                            }
                        }
                    }
                }
            } else {
                extract_params(schema, lhs, tables, params)?;
                extract_params(schema, rhs, tables, params)?;
            }
        }

        // CASE expr [WHEN expr THEN expr, ..., ELSE expr]
        Expr::Case {
            base: _,
            when_then_pairs: _,
            else_expr: _,
        } => {}

        // CAST ( expr AS type-name )
        Expr::Cast {
            expr: _,
            type_name: _,
        } => {}

        // expr COLLATE collation-name
        Expr::Collate(_, _) => {}

        // schema-name.table-name.column-name
        Expr::DoublyQualified(_, _, _) => {}

        // EXISTS ( select )
        Expr::Exists(select) => handle_select(schema, select, params)?,

        // function-name ( [DISTINCT] expr, ... ) filter-clause over-clause
        Expr::FunctionCall {
            name: _,
            distinctness: _,
            args,
            filter_over: _,
            order_by: _,
        } => {
            if let Some(args) = args {
                for expr in args.iter() {
                    extract_params(schema, expr, tables, params)?
                }
            }
        }

        Expr::FunctionCallStar {
            name: _,
            filter_over: _,
        } => {}

        // id
        Expr::Id(_) => {}

        // expr IN ( expr, ... )
        Expr::InList { lhs, not: _, rhs } => {
            if let Some(rhs) = rhs {
                for expr in rhs.iter() {
                    if let Some((name, kind)) = handle_lhs_rhs(lhs, expr) {
                        trace!("HANDLED LHS RHS: {name:?}");
                        match name {
                            // not aliased!
                            SqliteName::Id(id) => {
                                // find the first one to match
                                for (_, table) in tables.iter() {
                                    if let Some(col) = table.columns.get(&id.0) {
                                        let (sqlite_type, source) = col.sql_type();
                                        params.insert(Param {
                                            kind,
                                            sqlite_type,
                                            source,
                                        });
                                        break;
                                    }
                                }
                            }
                            SqliteName::Name(_) => {}
                            SqliteName::Qualified(tbl_name, col_name)
                            | SqliteName::DoublyQualified(_, tbl_name, col_name) => {
                                let col_name = if col_name.0.starts_with('"') {
                                    rem_first_and_last(&col_name.0)
                                } else {
                                    &col_name.0
                                };
                                if let Some(table) = tables.get(&tbl_name.0) {
                                    if let Some(col) = table.columns.get(col_name) {
                                        let (sqlite_type, source) = col.sql_type();
                                        params.insert(Param {
                                            kind,
                                            sqlite_type,
                                            source,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // expr IN ( select )
        Expr::InSelect {
            lhs: _,
            not: _,
            rhs,
        } => {
            // TODO: check LHS here
            handle_select(schema, rhs.as_ref(), params)?;
        }

        // expr IN schema-name.table-name | schema-name.table-function ( expr, ... )
        Expr::InTable {
            lhs: _,
            not: _,
            rhs: _,
            args: _,
        } => {}

        // expr IS NULL
        Expr::IsNull(_) => {}

        // expr [NOT] LIKE | GLOB | REGEXP | MATCH expr
        Expr::Like {
            lhs: _,
            not: _,
            op: _,
            rhs: _,
            escape: _,
        } => {}

        // NULL | integer | float | text | blob
        Expr::Literal(_) => {
            // nothing to do
        }

        // TODO:
        Expr::Name(_) => {}

        // expr NOT NULL
        Expr::NotNull(_) => {}

        // ( expr, ... )
        Expr::Parenthesized(exprs) => {
            for expr in exprs.iter() {
                extract_params(schema, expr, tables, params)?
            }
        }

        // schema-name.table-name
        Expr::Qualified(_, _) => {}

        // RAISE ( IGNORE | ROLLBACK | ABORT | FAIL [ error ] )
        Expr::Raise(_, _) => {}

        // SELECT
        Expr::Subquery(select) => handle_select(schema, select, params)?,

        // NOT | ~ | - | + expr
        Expr::Unary(_, _) => {}

        // ? | $ | :
        Expr::Variable(_) => {}
    }
    Ok(())
}

fn rem_first_and_last(value: &str) -> &str {
    let mut chars = value.chars();
    chars.next();
    chars.next_back();
    chars.as_str()
}

fn handle_select<'schema, 'stmt>(
    schema: &'schema Schema,
    select: &'stmt Select,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Result<(), UntypedUnnestParameter> {
    let tables = match &select.body.select {
        OneSelect::Select {
            columns,
            from,
            where_clause,
            distinctness: _,
            group_by: _,
            window_clause: _,
        } => {
            let tables = if let Some(from) = from {
                let tables = handle_from(schema, from, params)?;
                if let Some(where_clause) = where_clause {
                    trace!("WHERE CLAUSE: {where_clause:?}");
                    extract_params(schema, where_clause, &tables, params)?;
                }
                tables
            } else {
                HashMap::new()
            };
            for col in columns.iter() {
                if let ResultColumn::Expr(expr, _) = col {
                    // TODO: check against table if we can...
                    if let Some(kind) = as_param(expr) {
                        params.insert(Param {
                            kind,
                            sqlite_type: SqliteType::Text,
                            source: None,
                        });
                    }
                }
            }
            tables
        }
        OneSelect::Values(values_values) => {
            for values in values_values.iter() {
                for value in values.iter() {
                    if let Some(kind) = as_param(value) {
                        params.insert(Param {
                            kind,
                            sqlite_type: SqliteType::Text,
                            source: None,
                        });
                    }
                }
            }
            HashMap::new()
        }
    };
    if let Some(limit) = &select.limit {
        handle_limit(schema, limit, &tables, params)?;
    }
    Ok(())
}

/// Handle parameters in table function calls like unnest()
/// Returns an error if unnest() is called with an untyped parameter
///
/// TODO: Perhaps we should enable chaining let expressions in the compiler
///       to avoid the nesting here
fn handle_table_call_params<'schema, 'stmt>(
    qname: &QualifiedName,
    args: &'stmt Option<Vec<Expr>>,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Result<(), UntypedUnnestParameter> {
    if let Some(exprs) = args {
        let is_unnest = qname.name.0.eq_ignore_ascii_case("CORRO_UNNEST")
            || qname.name.0.eq_ignore_ascii_case("UNNEST");

        for expr in exprs.iter() {
            // If not unnest, just extract params
            // TODO: handle expressions more generally
            if !is_unnest {
                if let Some(kind) = as_param(expr) {
                    params.insert(Param {
                        kind,
                        sqlite_type: SqliteType::Text,
                        source: None,
                    });
                }
                continue;
            }

            // For unnest we force "CAST($1 AS type[])" for parameters
            // We can't use the ANYARRAY postgres type here as it doesn't work with client libraries
            if let Expr::Cast {
                expr: inner_expr,
                type_name,
            } = expr
            {
                if let Some(kind) = as_param(inner_expr) {
                    let type_str = type_name.name.to_uppercase();
                    let is_array_type = type_str.ends_with("[]");
                    let base_type = type_str[..type_str.len() - 2].trim();
                    let param_source = match base_type {
                        "TEXT" => Some("TEXT[]"),
                        "BLOB" => Some("BLOB[]"),
                        "INT" | "INTEGER" | "BIGINT" => Some("INT[]"),
                        "REAL" | "FLOAT" | "DOUBLE" => Some("REAL[]"),
                        "BOOL" | "BOOLEAN" => Some("BOOL[]"),
                        _ => None,
                    };
                    if is_array_type {
                        if let Some(source) = param_source {
                            params.insert(Param {
                                kind,
                                sqlite_type: SqliteType::Null,
                                source: Some(source),
                            });
                            continue;
                        }
                    }
                }
            }

            return Err(UntypedUnnestParameter);
        }
    }
    Ok(())
}

fn handle_from<'schema, 'stmt>(
    schema: &'schema Schema,
    from: &'stmt FromClause,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Result<HashMap<String, &'schema Table>, UntypedUnnestParameter> {
    let mut tables: HashMap<String, &Table> = HashMap::new();
    if let Some(select) = from.select.as_deref() {
        match select {
            SelectTable::Table(qname, maybe_alias, _) => {
                let actual_tbl_name = if qname.name.0.starts_with('"') {
                    rem_first_and_last(&qname.name.0)
                } else {
                    &qname.name.0
                };

                if let Some(table) = schema.tables.get(actual_tbl_name) {
                    if let Some(alias) = maybe_alias {
                        let alias = match alias {
                            As::As(name) | As::Elided(name) => name.0.clone(),
                        };
                        tables.insert(alias, table);
                    } else {
                        tables.insert(table.name.clone(), table);
                    }
                }
            }
            SelectTable::TableCall(qname, args, _alias) => {
                handle_table_call_params(qname, args, params)?;
            }
            SelectTable::Select(select, _) => {
                handle_select(schema, select, params)?;
            }
            SelectTable::Sub(_, _) => {}
        }
    }
    if let Some(joins) = &from.joins {
        for join in joins.iter() {
            match &join.table {
                SelectTable::Table(qname, maybe_alias, _) => {
                    let actual_tbl_name = if qname.name.0.starts_with('"') {
                        rem_first_and_last(&qname.name.0)
                    } else {
                        &qname.name.0
                    };

                    if let Some(table) = schema.tables.get(actual_tbl_name) {
                        if let Some(alias) = maybe_alias {
                            let alias = match alias {
                                As::As(name) | As::Elided(name) => name.0.clone(),
                            };
                            tables.insert(alias, table);
                        } else {
                            tables.insert(table.name.clone(), table);
                        }
                    }
                }
                SelectTable::TableCall(qname, args, _alias) => {
                    handle_table_call_params(qname, args, params)?;
                }
                SelectTable::Select(select, _) => {
                    handle_select(schema, select, params)?;
                }
                SelectTable::Sub(_, _) => {}
            }
        }
    }
    Ok(tables)
}

#[derive(Debug)]
struct Param<'stmt, 'schema> {
    kind: ParamKind<'stmt>,
    sqlite_type: SqliteType,
    source: Option<&'schema str>,
}

#[derive(Default, Debug)]
struct ParamsList<'stmt, 'schema> {
    params: Vec<Param<'stmt, 'schema>>,
    named: BTreeSet<&'stmt str>,
}

impl<'stmt, 'schema> ParamsList<'stmt, 'schema> {
    pub fn insert(&mut self, param: Param<'stmt, 'schema>) {
        let should_push = if let ParamKind::Named(name) = &param.kind {
            self.named.insert(*name)
        } else {
            true
        };
        if should_push {
            self.params.push(param);
        }
    }
}

fn handle_limit<'schema, 'stmt>(
    schema: &'schema Schema,
    limit: &'stmt Limit,
    tables: &HashMap<String, &'schema Table>,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Result<(), UntypedUnnestParameter> {
    if let Some(kind) = as_param(&limit.expr) {
        trace!("limit was a param (variable), pushing Integer type");
        params.insert(Param {
            kind,
            sqlite_type: SqliteType::Integer,
            source: None,
        });
    } else {
        extract_params(schema, &limit.expr, tables, params)?;
    }
    if let Some(offset) = &limit.offset {
        if let Some(kind) = as_param(offset) {
            trace!("offset was a param (variable), pushing Integer type");
            params.insert(Param {
                kind,
                sqlite_type: SqliteType::Integer,
                source: None,
            });
        } else {
            extract_params(schema, offset, tables, params)?;
        }
    }
    Ok(())
}

fn handle_with<'schema, 'stmt>(
    schema: &'schema Schema,
    with: &'stmt With,
    params: &mut ParamsList<'stmt, 'schema>,
) -> Result<Vec<Table>, UntypedUnnestParameter> {
    let mut tables = vec![];
    for cte in with.ctes.iter() {
        handle_select(schema, &cte.select, params)?;
        tables.push(Table {
            name: cte.tbl_name.0.clone(),
            pk: Default::default(),
            columns: cte
                .columns
                .as_ref()
                .map(|columns| {
                    columns
                        .iter()
                        .map(|col| {
                            (
                                col.col_name.0.clone(),
                                Column {
                                    name: col.col_name.0.clone(),
                                    sql_type: (SqliteType::Text, None), // no idea!
                                    nullable: false,
                                    default_value: None,
                                    generated: None,
                                    primary_key: false,
                                    raw: ColumnDefinition {
                                        col_name: col.col_name.clone(),
                                        col_type: None,
                                        constraints: vec![],
                                    },
                                },
                            )
                        })
                        .collect()
                })
                .unwrap_or_default(),
            indexes: Default::default(),
            raw: CreateTableBody::AsSelect(cte.select.clone()),
        })
    }
    Ok(tables)
}

fn parameter_types<'schema, 'stmt>(
    schema: &'schema Schema,
    cmd: &'stmt ParsedCmd,
) -> Result<ParamsList<'stmt, 'schema>, UntypedUnnestParameter> {
    let mut params = ParamsList::default();

    if let ParsedCmd::Sqlite(Cmd::Stmt(stmt)) = cmd {
        match stmt {
            Stmt::Select(select) => handle_select(schema, select, &mut params)?,
            Stmt::Delete {
                with,
                tbl_name,
                where_clause,
                limit,
                ..
            } => {
                if let Some(with) = with {
                    // TODO: do something w/ the accumulated tables?
                    handle_with(schema, with, &mut params)?;
                }

                let mut tables = HashMap::new();
                if let Some(tbl) = schema.tables.get(&tbl_name.name.0) {
                    if let Some(alias) = &tbl_name.alias {
                        tables.insert(alias.0.clone(), tbl);
                    } else {
                        tables.insert(tbl_name.name.0.clone(), tbl);
                    }
                }
                if let Some(where_clause) = where_clause {
                    extract_params(schema, where_clause, &tables, &mut params)?;
                }

                if let Some(limit) = limit {
                    handle_limit(schema, limit, &tables, &mut params)?;
                }
            }
            Stmt::Insert {
                with,
                tbl_name,
                columns,
                body,
                ..
            } => {
                trace!("GOT AN INSERT TO {tbl_name:?} on columns: {columns:?} w/ body: {body:?}");

                if let Some(with) = with {
                    // TODO: do something w/ the accumulated tables?
                    handle_with(schema, with, &mut params)?;
                }

                if let Some(table) = schema.tables.get(&tbl_name.name.0) {
                    match body {
                        InsertBody::Select(select, _) => {
                            if let OneSelect::Values(values_values) = &select.body.select {
                                for values in values_values.iter() {
                                    for (i, expr) in values.iter().enumerate() {
                                        if let Some(kind) = as_param(expr) {
                                            // specified columns
                                            let col = if let Some(columns) = columns {
                                                columns
                                                    .get(i)
                                                    .and_then(|name| table.columns.get(&name.0))
                                            } else {
                                                table.columns.get_index(i).map(|(_name, col)| col)
                                            };
                                            if let Some(col) = col {
                                                let (sqlite_type, source) = col.sql_type();
                                                params.insert(Param {
                                                    kind,
                                                    sqlite_type,
                                                    source,
                                                });
                                            }
                                        }
                                    }
                                }
                            } else {
                                handle_select(schema, select, &mut params)?
                            }
                        }
                        InsertBody::DefaultValues => {
                            // nothing to do!
                        }
                    }
                }
            }
            Stmt::Update {
                with,
                or_conflict: _,
                tbl_name,
                indexed: _,
                sets,
                from,
                where_clause,
                returning: _,
                order_by: _,
                limit,
            } => {
                if let Some(with) = with {
                    // TODO: do something w/ the accumulated tables?
                    handle_with(schema, with, &mut params)?;
                }

                let mut tables: HashMap<String, &'schema Table> = Default::default();

                let table = if let Some(tbl) = schema.tables.get(&tbl_name.name.0) {
                    tables.insert(tbl_name.name.0.clone(), tbl);
                    Some(tbl)
                } else {
                    None
                };

                for set in sets.iter() {
                    if let Some(kind) = as_param(&set.expr) {
                        let (sqlite_type, source) = if let Some(col) =
                            set.col_names.first().and_then(|first_col_name| {
                                table.and_then(|table| table.columns.get(&first_col_name.0))
                            }) {
                            col.sql_type()
                        } else {
                            (SqliteType::Text, None)
                        };
                        params.insert(Param {
                            kind,
                            sqlite_type,
                            source,
                        });
                    }
                }

                if let Some(from) = from {
                    let from_tables = handle_from(schema, from, &mut params)?;

                    tables.extend(from_tables);
                }

                if let Some(where_clause) = where_clause {
                    trace!("WHERE CLAUSE: {where_clause:?}");
                    extract_params(schema, where_clause, &tables, &mut params)?;
                }
                if let Some(limit) = limit {
                    handle_limit(schema, limit, &tables, &mut params)?;
                }
            }
            _ => {
                // do nothing, there can't be bound params here!
            }
        }
    }

    Ok(params)
}

enum FieldFormats<'a> {
    All(FieldFormat),
    Each(&'a [FieldFormat]),
}

impl<'a> FieldFormats<'a> {
    fn get(&self, i: usize) -> FieldFormat {
        match self {
            FieldFormats::All(format) => *format,
            // If there is less formats than columns, use the first format for all columns
            // Default to binary codecs if there are no formats
            FieldFormats::Each(formats) => formats
                .get(i)
                .copied()
                .unwrap_or(formats.first().copied().unwrap_or(FieldFormat::Binary)),
        }
    }
}

fn field_types(
    prepped: &Statement,
    parsed_cmd: &ParsedCmd,
    field_formats: FieldFormats<'_>,
) -> Result<Vec<FieldInfo>, UnsupportedSqliteToPostgresType> {
    let mut field_type_overrides = HashMap::new();

    match parsed_cmd {
        ParsedCmd::Sqlite(Cmd::Stmt(
            Stmt::Select(Select {
                body:
                    SelectBody {
                        select: OneSelect::Select { columns: cols, .. },
                        ..
                    },
                ..
            })
            | Stmt::Delete {
                returning: Some(cols),
                ..
            }
            | Stmt::Insert {
                returning: Some(cols),
                ..
            }
            | Stmt::Update {
                returning: Some(cols),
                ..
            },
        )) => {
            for (i, col) in cols.iter().enumerate() {
                if let ResultColumn::Expr(expr, _as) = col {
                    let type_override = match expr {
                        Expr::Cast { type_name, .. } => Some(name_to_type(&type_name.name)?),
                        Expr::FunctionCall { name, .. } | Expr::FunctionCallStar { name, .. } => {
                            match name.0.as_str().to_uppercase().as_ref() {
                                "COUNT" => Some(Type::INT8),
                                _ => None,
                            }
                        }
                        Expr::Literal(lit) => match lit {
                            Literal::Numeric(s) => Some(if s.contains('.') {
                                Type::FLOAT8
                            } else {
                                Type::INT8
                            }),
                            Literal::String(_) => Some(Type::TEXT),
                            Literal::Blob(_) => Some(Type::BYTEA),
                            Literal::Keyword(_) => None,
                            Literal::Null => None,
                            Literal::CurrentDate => Some(Type::DATE),
                            Literal::CurrentTime => Some(Type::TIME),
                            Literal::CurrentTimestamp => Some(Type::TIMESTAMP),
                        },
                        _ => None,
                    };
                    if let Some(type_override) = type_override {
                        match prepped.column_name(i) {
                            Ok(col_name) => {
                                field_type_overrides.insert(col_name, type_override);
                            }
                            Err(e) => {
                                error!("col index didn't exist at {i}, attempted to override type as: {type_override}: {e}");
                            }
                        }
                    }
                } else {
                    break;
                }
            }
        }
        ParsedCmd::Postgres(_stmt) => {
            // TODO: handle type overrides here too
            // let cols = match stmt {
            //     PgStatement::Insert { returning, .. }
            //     | PgStatement::Update { returning, .. }
            //     | PgStatement::Delete { returning, .. } => {
            //         returning
            //     }
            //     PgStatement::Query(query) => {
            //         match *query.body {
            //             sqlparser::ast::SetExpr::Select(
            //                 select,
            //             ) => Some(select.projection),
            //             _ => None,
            //         }
            //     }
            //     _ => None,
            // };

            // if let Some(cols) = cols {

            // }
        }
        _ => {}
    }

    let mut fields = vec![];
    for (i, col) in prepped.columns().iter().enumerate() {
        let col_name = col.name();
        let col_type = match field_type_overrides.remove(col_name) {
            Some(t) => t,
            None => match col.decl_type() {
                None => Type::TEXT,
                Some(decl_type) => name_to_type(decl_type)?,
            },
        };
        fields.push(FieldInfo::new(
            col_name.to_string(),
            None,
            None,
            col_type,
            field_formats.get(i),
        ));
    }

    Ok(fields)
}
