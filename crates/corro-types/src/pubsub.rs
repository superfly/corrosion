use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut};
use camino::Utf8PathBuf;
use compact_str::{CompactString, ToCompactString};
use corro_api_types::{
    Change, ChangeId, ColumnName, ColumnType, RowId, SqliteValue, SqliteValueRef,
};
use enquote::unquote;
use fallible_iterator::FallibleIterator;
use indexmap::{IndexMap, IndexSet};
use rusqlite::{
    params, params_from_iter,
    types::{FromSqlError, ValueRef},
    Connection, OptionalExtension,
};
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, JoinConstraint, Name, OneSelect, Operator, QualifiedName, ResultColumn,
        Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
};
use sqlite_pool::RusqlitePool;
use tokio::{sync::mpsc, task::block_in_place};
use tokio_util::sync::{CancellationToken, DropGuard};
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt};
use uuid::Uuid;

use crate::{
    api::QueryEvent,
    schema::{Schema, Table},
};

pub use corro_api_types::sqlite::ChangeType;

#[derive(Debug, thiserror::Error)]
pub enum NormalizeStatementError {
    #[error(transparent)]
    Parse(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("unexpected statement: {0}")]
    UnexpectedStatement(Cmd),
    #[error("only 1 statement is supported")]
    Multiple,
    #[error("at least 1 statement is required")]
    NoStatement,
}

pub fn normalize_sql(sql: &str) -> Result<String, NormalizeStatementError> {
    let mut parser = Parser::new(sql.as_bytes());

    let stmt = match parser.next()? {
        Some(Cmd::Stmt(stmt)) => stmt,
        Some(cmd) => {
            return Err(NormalizeStatementError::UnexpectedStatement(cmd));
        }
        None => {
            return Err(NormalizeStatementError::NoStatement);
        }
    };

    if parser.next()?.is_some() {
        return Err(NormalizeStatementError::Multiple);
    }

    Ok(Cmd::Stmt(stmt).to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum PackError {
    #[error("abort")]
    Abort,
}

pub fn pack_columns(args: &[SqliteValue]) -> Result<Vec<u8>, PackError> {
    let mut buf = vec![];
    /*
     * Format:
     * [num_columns:u8,...[(type(0-3),num_bytes?(3-7)):u8, length?:i32, ...bytes:u8[]]]
     *
     * The byte used for column type also encodes the number of bytes used for the integer.
     * e.g.: (type(0-3),num_bytes?(3-7)):u8
     * first 3 bits are type
     * last 5 encode how long the following integer, if there is a following integer, is. 1, 2, 3, ... 8 bytes.
     *
     * Not packing an integer into the minimal number of bytes required is rather wasteful.
     * E.g., the number `0` would take 8 bytes rather than 1 byte.
     */
    let len_result: Result<u8, _> = args.len().try_into();
    if let Ok(len) = len_result {
        buf.put_u8(len);
        for value in args {
            match value {
                SqliteValue::Null => {
                    buf.put_u8(ColumnType::Null as u8);
                }
                SqliteValue::Integer(val) => {
                    let num_bytes_for_int = num_bytes_needed_i64(*val);
                    let type_byte = num_bytes_for_int << 3 | (ColumnType::Integer as u8);
                    buf.put_u8(type_byte);
                    buf.put_int(*val, num_bytes_for_int as usize);
                }
                SqliteValue::Real(v) => {
                    buf.put_u8(ColumnType::Float as u8);
                    buf.put_f64(v.0);
                }
                SqliteValue::Text(value) => {
                    let len = value.len() as i32;
                    let num_bytes_for_len = num_bytes_needed_i32(len);
                    let type_byte = num_bytes_for_len << 3 | (ColumnType::Text as u8);
                    buf.put_u8(type_byte);
                    buf.put_int(len as i64, num_bytes_for_len as usize);
                    buf.put_slice(value.as_bytes());
                }
                SqliteValue::Blob(value) => {
                    let len = value.len() as i32;
                    let num_bytes_for_len = num_bytes_needed_i32(len);
                    let type_byte = num_bytes_for_len << 3 | (ColumnType::Blob as u8);
                    buf.put_u8(type_byte);
                    buf.put_int(len as i64, num_bytes_for_len as usize);
                    buf.put_slice(value);
                }
            }
        }
        Ok(buf)
    } else {
        Err(PackError::Abort)
    }
}

fn num_bytes_needed_i64(val: i64) -> u8 {
    if val & 0xFF00000000000000u64 as i64 != 0 {
        8
    } else if val & 0x00FF000000000000 != 0 {
        7
    } else if val & 0x0000FF0000000000 != 0 {
        6
    } else if val & 0x000000FF00000000 != 0 {
        5
    } else {
        num_bytes_needed_i32(val as i32)
    }
}

fn num_bytes_needed_i32(val: i32) -> u8 {
    if val & 0xFF000000u32 as i32 != 0 {
        4
    } else if val & 0x00FF0000 != 0 {
        3
    } else if val & 0x0000FF00 != 0 {
        2
    } else if val * 0x000000FF != 0 {
        1
    } else {
        0
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UnpackError {
    #[error("abort")]
    Abort,
    #[error("misuse")]
    Misuse,
}

pub fn unpack_columns(mut buf: &[u8]) -> Result<Vec<SqliteValueRef>, UnpackError> {
    let mut ret = vec![];
    let num_columns = buf.get_u8();

    for _i in 0..num_columns {
        if !buf.has_remaining() {
            return Err(UnpackError::Abort);
        }
        let column_type_and_maybe_intlen = buf.get_u8();
        let column_type = ColumnType::from_u8(column_type_and_maybe_intlen & 0x07);
        let intlen = (column_type_and_maybe_intlen >> 3) as usize;

        match column_type {
            Some(ColumnType::Blob) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                let len = buf.get_int(intlen) as usize;
                if buf.remaining() < len {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef(ValueRef::Blob(&buf[0..len])));
                buf.advance(len);
            }
            Some(ColumnType::Float) => {
                if buf.remaining() < 8 {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef(ValueRef::Real(buf.get_f64())));
            }
            Some(ColumnType::Integer) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef(ValueRef::Integer(buf.get_int(intlen))));
            }
            Some(ColumnType::Null) => {
                ret.push(SqliteValueRef(ValueRef::Null));
            }
            Some(ColumnType::Text) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                let len = buf.get_int(intlen) as usize;
                if buf.remaining() < len {
                    return Err(UnpackError::Abort);
                }
                ret.push(SqliteValueRef(ValueRef::Text(&buf[0..len])));
                buf.advance(len);
            }
            None => return Err(UnpackError::Misuse),
        }
    }

    Ok(ret)
}

pub enum MatcherCmd {
    ProcessChange(MatchCandidates),
}

#[derive(Clone)]
pub struct MatcherHandle(Arc<InnerMatcherHandle>);

struct InnerMatcherHandle {
    id: Uuid,
    pool: sqlite_pool::RusqlitePool,
    cmd_tx: mpsc::Sender<MatcherCmd>,
    parsed: ParsedSelect,
    col_names: Vec<ColumnName>,
    cancel: CancellationToken,
}

struct MatchableChange<'a> {
    table: &'a str,
    pk: &'a [u8],
    column: &'a str,
}

type MatchCandidates = IndexMap<CompactString, IndexSet<Vec<u8>>>;

impl MatcherHandle {
    fn filter_matchable_change(
        &self,
        candidates: &mut MatchCandidates,
        change: MatchableChange,
    ) -> Result<(), MatcherError> {
        // don't double process the same pk
        if candidates
            .get(change.table)
            .map(|pks| pks.contains(change.pk))
            .unwrap_or_default()
        {
            return Ok(());
        }

        // don't consider changes that don't have both the table + col in the matcher query
        if !self
            .0
            .parsed
            .table_columns
            .get(change.table)
            .map(|cols| change.column == "-1" || cols.contains(change.column))
            .unwrap_or_default()
        {
            return Ok(());
        }

        if let Some(v) = candidates.get_mut(change.table) {
            v.insert(change.pk.to_vec());
        } else {
            candidates.insert(
                change.table.to_compact_string(),
                [change.pk.to_vec()].into(),
            );
        }

        Ok(())
    }

    pub fn process_changes_from_db_version(
        &self,
        conn: &Connection,
        db_version: i64,
    ) -> Result<(), MatcherError> {
        let mut candidates = MatchCandidates::new();

        let mut prepped = conn.prepare_cached(
            "SELECT DISTINCT \"table\", pk, cid FROM crsql_changes WHERE db_version = ? GROUP BY \"table\" ORDER BY seq",
        )?;

        let mut rows = prepped.query([db_version])?;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };

            self.filter_matchable_change(
                &mut candidates,
                MatchableChange {
                    table: row.get_ref(0)?.as_str()?,
                    pk: row.get_ref(1)?.as_blob()?,
                    column: row.get_ref(2)?.as_str()?,
                },
            )?;
        }

        if !candidates.is_empty() {
            self.0
                .cmd_tx
                .try_send(MatcherCmd::ProcessChange(candidates))
                .map_err(|_| MatcherError::ChangeQueueClosedOrFull)?;
        }

        Ok(())
    }

    pub fn process_changeset(&self, changes: &[Change]) -> Result<(), MatcherError> {
        let mut candidates = MatchCandidates::new();

        for change in changes.iter() {
            self.filter_matchable_change(
                &mut candidates,
                MatchableChange {
                    table: &change.table,
                    pk: &change.pk,
                    column: &change.cid,
                },
            )?;
        }

        if !candidates.is_empty() {
            self.0
                .cmd_tx
                .try_send(MatcherCmd::ProcessChange(candidates))
                .map_err(|_| MatcherError::ChangeQueueClosedOrFull)?;
        }

        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.0.id
    }

    pub fn parsed_columns(&self) -> &[ResultColumn] {
        &self.0.parsed.columns
    }

    pub fn col_names(&self) -> &[ColumnName] {
        &self.0.col_names
    }

    pub async fn cleanup(self) {
        self.0.cancel.cancel();
        info!(sub_id = %self.0.id, "Canceled subscription");
    }

    pub fn pool(&self) -> &RusqlitePool {
        &self.0.pool
    }

    pub fn max_change_id(&self, conn: &Connection) -> rusqlite::Result<ChangeId> {
        let mut prepped = conn.prepare_cached("SELECT COALESCE(MAX(id), 0) FROM changes")?;
        prepped.query_row([], |row| row.get(0))
    }

    pub fn max_row_id(&self, conn: &Connection) -> rusqlite::Result<RowId> {
        let mut prepped =
            conn.prepare_cached("SELECT COALESCE(MAX(__corro_rowid), 0) FROM query")?;
        prepped.query_row([], |row| row.get(0))
    }

    pub fn changes_since(
        &self,
        since: ChangeId,
        conn: &Connection,
        tx: mpsc::Sender<QueryEvent>,
    ) -> rusqlite::Result<()> {
        let mut query_cols = vec![];
        for i in 0..(self.parsed_columns().len()) {
            query_cols.push(format!("col_{i}"));
        }
        let mut prepped = conn.prepare_cached(&format!(
            "SELECT id, type, __corro_rowid, {} FROM changes WHERE id > ?",
            query_cols.join(",")
        ))?;

        let col_count = prepped.column_count();

        let mut rows = prepped.query([since])?;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };

            if let Err(e) = tx.blocking_send(QueryEvent::Change(
                row.get(1)?,
                row.get(2)?,
                (3..col_count)
                    .map(|i| row.get::<_, SqliteValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?,
                row.get(0)?,
            )) {
                error!("could not send change to channel: {e}");
                break;
            }
        }

        Ok(())
    }

    pub fn all_rows(
        &self,
        conn: &Connection,
        tx: mpsc::Sender<QueryEvent>,
    ) -> rusqlite::Result<()> {
        let mut query_cols = vec![];
        for i in 0..(self.parsed_columns().len()) {
            query_cols.push(format!("col_{i}"));
        }
        let mut prepped = conn.prepare_cached(&format!(
            "SELECT __corro_rowid, {} FROM query",
            query_cols.join(",")
        ))?;

        let col_count = prepped.column_count();

        if let Err(e) = tx.blocking_send(QueryEvent::Columns(self.col_names().to_vec())) {
            error!("could not send rows to channel: {e}");
            // TODO: bomb!
            return Ok(());
        }

        let start = Instant::now();
        let mut rows = prepped.query([])?;
        let elapsed = start.elapsed();

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };

            if let Err(e) = tx.blocking_send(QueryEvent::Row(
                row.get(0)?,
                (1..col_count)
                    .map(|i| row.get::<_, SqliteValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            )) {
                error!("could not send row to channel: {e}");
                return Ok(());
            }
        }

        if let Err(e) = tx.blocking_send(QueryEvent::EndOfQuery {
            time: elapsed.as_secs_f64(),
            change_id: Some(
                conn.prepare("SELECT COALESCE(MAX(id),0) FROM changes")?
                    .query_row([], |row| row.get(0))?,
            ),
        }) {
            error!("could not send end-of-query to channel: {e}");
            // TODO: bomb!
            return Ok(());
        }

        Ok(())
    }
}

pub struct Matcher {
    pub id: Uuid,
    pub query: Stmt,
    pub statements: HashMap<String, MatcherStmt>,
    pub pks: IndexMap<String, Vec<String>>,
    pub parsed: ParsedSelect,
    pub evt_tx: mpsc::Sender<QueryEvent>,
    pub cmd_rx: mpsc::Receiver<MatcherCmd>,
    pub col_names: Vec<ColumnName>,
    pub last_rowid: i64,
    conn: Connection,
    base_path: Utf8PathBuf,
    cancel: CancellationToken,
}

#[derive(Debug, Clone)]
pub struct MatcherStmt {
    new_query: String,
    temp_query: String,
}

const CHANGE_ID_COL: &str = "id";
const CHANGE_TYPE_COL: &str = "type";

pub const QUERY_TABLE_NAME: &str = "query";

const SUB_DB_PATH: &str = "sub.sqlite";

impl Matcher {
    fn new(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        state_conn: &Connection,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
    ) -> Result<(Matcher, MatcherHandle), MatcherError> {
        let sub_path = subs_path.join(id.as_simple().to_string());

        info!("Initializing subscription at {sub_path}");

        std::fs::create_dir_all(&sub_path)?;

        let sub_db_path = sub_path.join(SUB_DB_PATH);

        let col_names: Vec<ColumnName> = {
            state_conn
                .prepare(sql)?
                .column_names()
                .into_iter()
                .map(|s| ColumnName(s.to_compact_string()))
                .collect()
        };

        let conn = Connection::open(&sub_db_path)?;
        conn.execute_batch(
            r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
            "#,
        )?;

        state_conn.execute_batch(&format!(
            "ATTACH DATABASE {} AS __corro_sub",
            enquote::enquote('\'', sub_db_path.as_str()),
        ))?;

        let mut parser = Parser::new(sql.as_bytes());

        let (mut stmt, parsed) = match parser.next()?.ok_or(MatcherError::StatementRequired)? {
            Cmd::Stmt(stmt) => {
                let parsed = match stmt {
                    Stmt::Select(ref select) => extract_select_columns(select, schema)?,
                    _ => return Err(MatcherError::UnsupportedStatement),
                };

                (stmt, parsed)
            }
            _ => return Err(MatcherError::StatementRequired),
        };

        if parsed.table_columns.is_empty() {
            return Err(MatcherError::TableRequired);
        }

        let mut statements = HashMap::new();

        let mut pks = IndexMap::default();

        match &mut stmt {
            Stmt::Select(select) => match &mut select.body.select {
                OneSelect::Select { columns, .. } => {
                    let mut new_cols = parsed
                        .table_columns
                        .iter()
                        .filter_map(|(tbl_name, _cols)| {
                            schema.tables.get(tbl_name).map(|table| {
                                let tbl_name = parsed
                                    .aliases
                                    .iter()
                                    .find_map(|(alias, actual)| {
                                        (actual == tbl_name).then_some(alias)
                                    })
                                    .unwrap_or(tbl_name);
                                table
                                    .pk
                                    .iter()
                                    .map(|pk| {
                                        let alias = format!("__corro_pk_{tbl_name}_{pk}");
                                        let entry: &mut Vec<String> =
                                            pks.entry(table.name.clone()).or_default();
                                        entry.push(alias.clone());

                                        ResultColumn::Expr(
                                            Expr::Qualified(
                                                Name(tbl_name.clone()),
                                                Name(pk.clone()),
                                            ),
                                            Some(As::As(Name(alias))),
                                        )
                                    })
                                    .collect::<Vec<_>>()
                            })
                        })
                        .flatten()
                        .collect::<Vec<_>>();

                    new_cols.append(&mut parsed.columns.clone());
                    *columns = new_cols;
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }

        for (tbl_name, _cols) in parsed.table_columns.iter() {
            let expr = table_to_expr(
                &parsed.aliases,
                schema
                    .tables
                    .get(tbl_name)
                    .expect("this should not happen, missing table in schema"),
                tbl_name,
                id,
            )?;

            let mut stmt = stmt.clone();

            if let Stmt::Select(select) = &mut stmt {
                if let OneSelect::Select { where_clause, .. } = &mut select.body.select {
                    *where_clause = if let Some(prev) = where_clause.take() {
                        Some(Expr::Binary(Box::new(expr), Operator::And, Box::new(prev)))
                    } else {
                        Some(expr)
                    };
                }
            }

            let mut new_query = Cmd::Stmt(stmt).to_string();
            new_query.pop();

            let mut tmp_cols = pks.values().flatten().cloned().collect::<Vec<String>>();
            for i in 0..(parsed.columns.len()) {
                tmp_cols.push(format!("col_{i}"));
            }

            let pk_cols = pks
                .get(tbl_name)
                .cloned()
                .ok_or(MatcherError::MissingPrimaryKeys)?
                .to_vec()
                .join(",");

            statements.insert(
                tbl_name.clone(),
                MatcherStmt {
                    new_query,
                    temp_query: format!(
                        "SELECT {} FROM query WHERE ({}) IN subscription_{}_{}",
                        tmp_cols.join(","),
                        pk_cols,
                        id.as_simple(),
                        tbl_name,
                    ),
                },
            );
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(512);
        let cancel = CancellationToken::new();

        let handle = MatcherHandle(Arc::new(InnerMatcherHandle {
            id,
            pool: sqlite_pool::Config::new(sub_db_path.into_std_path_buf())
                .max_size(5)
                .read_only()
                .create_pool()
                .expect("could not build pool, this can't fail because we specified a runtime"),
            cmd_tx,
            parsed: parsed.clone(),
            col_names: col_names.clone(),
            cancel: cancel.clone(),
        }));

        let matcher = Self {
            id,
            query: stmt,
            statements,
            pks,
            parsed,
            evt_tx,
            cmd_rx,
            col_names,
            last_rowid: 0,
            conn,
            base_path: sub_path,
            cancel,
        };

        Ok((matcher, handle))
    }

    pub fn restore(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        state_conn: Connection,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
    ) -> Result<MatcherHandle, MatcherError> {
        let (matcher, handle) = Self::new(id, subs_path, schema, &state_conn, evt_tx, sql)?;

        tokio::spawn(matcher.run_restore(state_conn));

        Ok(handle)
    }

    pub fn create(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        mut state_conn: Connection,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
    ) -> Result<MatcherHandle, MatcherError> {
        let (mut matcher, handle) = Self::new(id, subs_path, schema, &state_conn, evt_tx, sql)?;

        let pk_cols = matcher
            .pks
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<String>>();

        let mut tmp_cols = pk_cols.clone();

        let mut actual_cols = vec![];

        for i in 0..(matcher.parsed.columns.len()) {
            let col_name = format!("col_{i}");
            tmp_cols.push(col_name.clone());
            actual_cols.push(col_name);
        }

        let n = block_in_place(|| {
            let tx = matcher.conn.transaction()?;

            let create_temp_table = format!(
                "
                CREATE TABLE query (__corro_rowid INTEGER PRIMARY KEY AUTOINCREMENT, {columns});

                CREATE UNIQUE INDEX index_{id}_pk ON query ({pks_coalesced});

                CREATE TABLE changes (
                    {CHANGE_ID_COL} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    __corro_rowid INTEGER NOT NULL,
                    {CHANGE_TYPE_COL} INTEGER NOT NULL,
                    {actual_columns}
                );
            ",
                columns = tmp_cols.join(","),
                id = id.as_simple(),
                pks_coalesced = pk_cols
                    .iter()
                    .map(|pk| format!("coalesce({pk}, \"\")"))
                    .collect::<Vec<_>>()
                    .join(","),
                actual_columns = actual_cols.join(","),
            );

            tx.execute_batch(&create_temp_table)?;

            let state_tx = state_conn.transaction()?;
            let inserted = state_tx.execute(
                "INSERT INTO __corro_subs (id, sql) VALUES (?, ?);",
                params![id, sql],
            )?;

            tx.commit()?;
            state_tx.commit()?;

            Ok::<_, rusqlite::Error>(inserted)
        })?;

        if n != 1 {
            return Err(MatcherError::InsertSub);
        }

        tokio::spawn(matcher.run(state_conn));

        Ok(handle)
    }

    async fn run_restore(mut self, state_conn: Connection) {
        let init_res = block_in_place(|| {
            let mut prepped = self
                .conn
                .prepare("SELECT COALESCE(MAX(__corro_rowid), 0) FROM query")?;
            self.last_rowid = prepped
                .query_row((), |row| row.get(0))
                .optional()?
                .unwrap_or_default();
            Ok::<_, rusqlite::Error>(())
        });

        if let Err(e) = init_res {
            error!("could not initialize subscription: {e}");
            _ = self
                .evt_tx
                .send(QueryEvent::Error(
                    format!("could not init restored subscription: {e}").into(),
                ))
                .await;
            return;
        }

        self.cmd_loop(state_conn).await
    }

    fn handle_cleanup(&mut self, state_conn: &mut Connection) -> rusqlite::Result<()> {
        info!(sub_id = %self.id, "Attempting to cleanup...");
        let state_tx = state_conn.transaction()?;
        let tx = self.conn.transaction()?;

        info!(sub_id = %self.id, "Dropping sub tables");
        tx.execute_batch("DROP TABLE query; DROP TABLE changes")?;

        info!(sub_id = %self.id, "Dropping subs entry in DB");
        state_tx.execute("DELETE FROM __corro_subs WHERE id = ?", [self.id])?;

        tx.commit()?;
        state_tx.commit()?;

        if let Err(e) = std::fs::remove_dir_all(&self.base_path) {
            error!(sub_id = %self.id, "could not delete subscription base path {} due to: {e}", self.base_path);
        }

        Ok(())
    }

    async fn cmd_loop(mut self, mut state_conn: Connection) {
        let mut purge_changes_interval = tokio::time::interval(Duration::from_secs(300));
        let mut process_changes_interval = tokio::time::interval(Duration::from_millis(100));

        let mut process_buf = Vec::new();
        let mut count = 0;

        loop {
            enum Branch {
                ProcessChanges,
                PurgeOldChanges,
            }

            let branch = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    info!(sub_id = %self.id, "Acknowledged subscription cancellation, breaking loop.");
                    break;
                }
                Some(req) = self.cmd_rx.recv() => match req {
                    MatcherCmd::ProcessChange(candidates) => {
                        count += candidates.values().map(|pks| pks.len()).sum::<usize>();
                        process_buf.push(candidates);
                        if count >= 200 {
                            Branch::ProcessChanges
                        } else {
                            continue;
                        }
                    }
                },
                _ = process_changes_interval.tick() => Branch::ProcessChanges,
                _ = purge_changes_interval.tick() => Branch::PurgeOldChanges,
                else => {
                    info!(sub_id = %self.id, "Subscription command loop is done!");
                    break;
                }
            };

            match branch {
                Branch::ProcessChanges => {
                    if process_buf.is_empty() {
                        continue;
                    }

                    if let Err(e) = block_in_place(|| {
                        self.handle_change(&mut state_conn, process_buf.drain(..))
                    }) {
                        if matches!(e, MatcherError::EventReceiverClosed) {
                            break;
                        }
                        error!("could not handle change: {e}");
                        continue;
                    }

                    // just in case...
                    process_buf.clear();

                    count = 0;
                }
                Branch::PurgeOldChanges => {
                    let res = block_in_place(|| {
                        let tx = self.conn.transaction()?;

                        let deleted = tx
                            .prepare_cached(
                                "DELETE FROM changes WHERE id < (SELECT COALESCE(MAX(id),0) - 500 FROM changes)"
                            )?
                            .execute([])?;

                        tx.commit().map(|_| deleted)
                    });

                    match res {
                        Ok(deleted) => info!(
                            "Deleted {deleted} old changes row for subscription {}",
                            self.id.as_simple()
                        ),
                        Err(e) => {
                            error!("could not delete old changes: {e}");
                        }
                    }
                }
            }
        }

        debug!(id = %self.id, "matcher loop is done");

        if !process_buf.is_empty() {
            if let Err(e) =
                block_in_place(|| self.handle_change(&mut state_conn, process_buf.drain(..)))
            {
                error!("could not handle change after loop broken: {e}");
            }
        }

        if let Err(e) = block_in_place(|| self.handle_cleanup(&mut state_conn)) {
            error!("could not handle cleanup: {e}");
        }
    }

    async fn run(mut self, state_conn: Connection) {
        if let Err(e) = self
            .evt_tx
            .send(QueryEvent::Columns(self.col_names.clone()))
            .await
        {
            error!("could not send back columns, probably means no receivers! {e}");
            return;
        }

        let mut query_cols = vec![];
        for i in 0..(self.parsed.columns.len()) {
            query_cols.push(format!("col_{i}"));
        }

        let res = block_in_place(|| {
            let tx = self.conn.transaction()?;

            let mut stmt_str = Cmd::Stmt(self.query.clone()).to_string();
            stmt_str.pop(); // remove trailing `;`

            let mut tmp_cols = self
                .pks
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<String>>();

            for i in 0..(self.parsed.columns.len()) {
                let col_name = format!("col_{i}");
                tmp_cols.push(col_name.clone());
            }

            let mut last_rowid = 0;

            // ensure drop and recreate
            tx.execute_batch(&format!(
                "DROP TABLE IF EXISTS state_rows;
                            CREATE TEMP TABLE state_rows ({})",
                tmp_cols.join(",")
            ))?;

            let elapsed = {
                println!("select stmt: {stmt_str:?}");

                let mut select = state_conn.prepare(&stmt_str)?;
                let start = Instant::now();
                let mut select_rows = {
                    let _guard = interrupt_deadline_guard(&state_conn, Duration::from_secs(15));
                    select.query(())?
                };
                let elapsed = start.elapsed();

                let insert_into = format!(
                    "INSERT INTO query ({}) VALUES ({}) RETURNING __corro_rowid,{}",
                    tmp_cols.join(","),
                    tmp_cols
                        .iter()
                        .map(|_| "?".to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    query_cols.join(","),
                );
                trace!("insert stmt: {insert_into:?}");

                let mut insert = tx.prepare(&insert_into)?;

                loop {
                    match select_rows.next() {
                        Ok(Some(row)) => {
                            for i in 0..tmp_cols.len() {
                                insert.raw_bind_parameter(
                                    i + 1,
                                    SqliteValueRef::from(row.get_ref(i)?),
                                )?;
                            }

                            let mut rows = insert.raw_query();

                            let row = match rows.next()? {
                                Some(row) => row,
                                None => continue,
                            };

                            let rowid = row.get(0)?;
                            let cells = (1..=query_cols.len())
                                .map(|i| row.get::<_, SqliteValue>(i))
                                .collect::<rusqlite::Result<Vec<_>>>()?;

                            if let Err(e) = self
                                .evt_tx
                                .blocking_send(QueryEvent::Row(RowId(rowid), cells))
                            {
                                error!("could not send back row: {e}");
                                return Err(MatcherError::EventReceiverClosed);
                            }

                            last_rowid = cmp::max(rowid, last_rowid);
                        }
                        Ok(None) => {
                            // done!
                            break;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }
                elapsed
            };

            tx.execute_batch("DROP TABLE IF EXISTS state_rows;")?;

            tx.commit()?;

            self.last_rowid = last_rowid;

            Ok::<_, MatcherError>(elapsed)
        });

        println!("done w/ block_in_place for initial query");

        match res {
            Ok(elapsed) => {
                if let Err(e) = self
                    .evt_tx
                    .send(QueryEvent::EndOfQuery {
                        time: elapsed.as_secs_f64(),
                        change_id: Some(ChangeId(0)),
                    })
                    .await
                {
                    error!("could not return end of query event: {e}");
                    return;
                }
            }
            Err(e) => {
                _ = self
                    .evt_tx
                    .send(QueryEvent::Error(e.to_compact_string()))
                    .await;
                return;
            }
        }

        if let Err(e) = res {
            _ = self
                .evt_tx
                .send(QueryEvent::Error(e.to_compact_string()))
                .await;
            return;
        }

        self.cmd_loop(state_conn).await
    }

    fn handle_change<I: Iterator<Item = MatchCandidates>>(
        &mut self,
        state_conn: &mut Connection,
        candidates: I,
    ) -> Result<(), MatcherError> {
        let mut tables = IndexSet::new();

        let tx = self.conn.transaction()?;
        for candidates in candidates {
            for (table, pks) in candidates {
                let pks = pks
                    .iter()
                    .map(|pk| unpack_columns(pk))
                    .collect::<Result<Vec<Vec<SqliteValueRef>>, _>>()?;

                let tmp_table_name = format!("subscription_{}_{table}", self.id.as_simple());
                if tables.insert(table.clone()) {
                    // create a temporary table to mix and match the data
                    tx.prepare_cached(
                        // TODO: cache the statement's string somewhere, it's always the same!
                        // NOTE: this can't be an actual CREATE TEMP TABLE or it won't be visible
                        //       from the state db
                        &format!(
                            "CREATE TABLE {tmp_table_name} ({})",
                            self.pks
                                .get(table.as_str())
                                .ok_or_else(|| MatcherError::MissingPrimaryKeys)?
                                .to_vec()
                                .join(",")
                        ),
                    )?
                    .execute(())?;
                }

                for pks in pks {
                    trace!(
                        "inserting in temp table for {table}, pks {:?}",
                        pks.iter().map(|pk| pk.to_owned()).collect::<Vec<_>>()
                    );
                    tx.prepare_cached(&format!(
                        "INSERT INTO {tmp_table_name} VALUES ({})",
                        (0..pks.len()).map(|_i| "?").collect::<Vec<_>>().join(",")
                    ))?
                    .execute(params_from_iter(pks))?;
                }
            }
        }
        // commit so it is visible to the other db!
        tx.commit()?;

        let mut actual_cols = vec![];

        let pk_cols = self
            .pks
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<String>>();

        let mut tmp_cols = pk_cols.clone();
        for i in 0..(self.parsed.columns.len()) {
            let col_name = format!("col_{i}");
            tmp_cols.push(col_name.clone());
            actual_cols.push(col_name);
        }

        // start a new tx
        let tx = self.conn.transaction()?;

        // ensure drop and recreate
        tx.execute_batch(&format!(
            "DROP TABLE IF EXISTS state_results;
            CREATE TEMP TABLE state_results ({})",
            tmp_cols.join(",")
        ))?;

        let mut new_last_rowid = self.last_rowid;

        {
            let mut tmp_insert_prepped = tx.prepare_cached(&format!(
                "INSERT INTO state_results VALUES ({})",
                (0..tmp_cols.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ))?;

            for table in tables.iter() {
                let stmt = match self.statements.get(table.as_str()) {
                    Some(stmt) => stmt,
                    None => {
                        warn!(sub_id = %self.id, "no statements pre-computed for table {table}");
                        continue;
                    }
                };

                trace!("SELECT SQL: {}", stmt.new_query);

                let mut prepped = state_conn.prepare_cached(&stmt.new_query)?;

                let col_count = prepped.column_count();

                let mut rows = prepped.raw_query();

                // insert each row in the other DB...
                while let Some(row) = rows.next()? {
                    for i in 0..col_count {
                        tmp_insert_prepped
                            .raw_bind_parameter(i + 1, SqliteValueRef::from(row.get_ref(i)?))?;
                    }
                    tmp_insert_prepped.raw_execute()?;
                }

                let sql = format!(
                    "INSERT INTO query ({insert_cols})
                        SELECT * FROM (
                            SELECT * FROM state_results
                            EXCEPT
                            {query_query}
                        ) WHERE 1
                        ON CONFLICT({conflict_clause})
                            DO UPDATE SET
                                {excluded}
                            WHERE {excluded_not_same}
                        RETURNING __corro_rowid,{return_cols}",
                    // insert into
                    insert_cols = tmp_cols.join(","),
                    query_query = stmt.temp_query,
                    conflict_clause = pk_cols
                        .iter()
                        .map(|pk| format!("coalesce({pk},\"\")"))
                        .collect::<Vec<_>>()
                        .join(","),
                    excluded = (0..(self.parsed.columns.len()))
                        .map(|i| format!("col_{i} = excluded.col_{i}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    excluded_not_same = (0..(self.parsed.columns.len()))
                        .map(|i| format!("col_{i} IS NOT excluded.col_{i}"))
                        .collect::<Vec<_>>()
                        .join(" OR "),
                    return_cols = actual_cols.join(",")
                );

                trace!("INSERT SQL: {sql}");

                let insert_prepped = tx.prepare_cached(&sql)?;

                let sql = format!(
                    "
                    DELETE FROM query WHERE ({pks}) IN (SELECT {select_pks} FROM (
                        {query_query}
                        EXCEPT
                        SELECT * FROM state_results
                    )) RETURNING __corro_rowid,{return_cols}
                ",
                    // delete from
                    pks = pk_cols.join(","),
                    select_pks = pk_cols.join(","),
                    query_query = stmt.temp_query,
                    return_cols = actual_cols.join(",")
                );

                trace!("DELETE SQL: {sql}");

                let delete_prepped = tx.prepare_cached(&sql)?;

                let mut change_insert_stmt = tx.prepare_cached(&format!(
                "INSERT INTO changes (__corro_rowid, {CHANGE_TYPE_COL}, {}) VALUES (?, ?, {}) RETURNING {CHANGE_ID_COL}",
                actual_cols.join(","),
                (0..actual_cols.len())
                    .map(|_i| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ))?;

                for (mut change_type, mut prepped) in [
                    (None, insert_prepped),
                    (Some(ChangeType::Delete), delete_prepped),
                ] {
                    let col_count = prepped.column_count();

                    let mut rows = prepped.raw_query();

                    while let Ok(Some(row)) = rows.next() {
                        let rowid: RowId = row.get(0)?;

                        let change_type = change_type.take().unwrap_or({
                            if rowid.0 > self.last_rowid {
                                ChangeType::Insert
                            } else {
                                ChangeType::Update
                            }
                        });

                        new_last_rowid = cmp::max(new_last_rowid, rowid.0);

                        match (1..col_count)
                            .map(|i| row.get::<_, SqliteValue>(i))
                            .collect::<rusqlite::Result<Vec<_>>>()
                        {
                            Ok(cells) => {
                                change_insert_stmt.raw_bind_parameter(1, rowid)?;
                                change_insert_stmt.raw_bind_parameter(2, change_type)?;
                                for (i, cell) in cells.iter().enumerate() {
                                    // increment index by 3 because that's where we're starting...
                                    change_insert_stmt.raw_bind_parameter(i + 3, cell)?;
                                }

                                let mut change_rows = change_insert_stmt.raw_query();

                                let change_id: ChangeId = change_rows
                                    .next()?
                                    .ok_or(MatcherError::NoChangeInserted)?
                                    .get(0)?;

                                trace!("got change id: {change_id}");

                                if let Err(e) = self.evt_tx.blocking_send(QueryEvent::Change(
                                    change_type,
                                    rowid,
                                    cells,
                                    change_id,
                                )) {
                                    debug!("could not send back row to matcher sub sender: {e}");
                                    return Err(MatcherError::EventReceiverClosed);
                                }
                            }
                            Err(e) => {
                                error!("could not deserialize row's cells: {e}");
                                return Ok(());
                            }
                        }
                    }
                }
            }

            // clean that up
            tx.execute_batch("DROP TABLE state_results")?;

            // clean up temporary tables immediately
            for table in tables {
                // TODO: reduce mistakes by computing this table name once
                tx.prepare_cached(&format!(
                    "DROP TABLE IF EXISTS subscription_{}_{table}",
                    self.id.as_simple(),
                ))?
                .execute(())?;
                trace!("cleaned up subscription_{}_{table}", self.id.as_simple());
            }
        }

        tx.commit()?;

        self.last_rowid = new_last_rowid;

        Ok(())
    }
}

fn interrupt_deadline_guard(conn: &Connection, dur: Duration) -> DropGuard {
    let int_handle = conn.get_interrupt_handle();
    let cancel = CancellationToken::new();
    tokio::spawn({
        let cancel = cancel.clone();
        async move {
            match tokio::time::sleep(dur)
                .preemptible(cancel.cancelled())
                .await
            {
                Outcome::Completed(_) => {
                    warn!("subscription query deadline reached, interrupting!");
                    int_handle.interrupt();
                    // no need to send any query event, it should bubble up properly and if not
                    // then it means the conn was not interrupted in time which is also fine
                }
                Outcome::Preempted(_) => {
                    debug!("deadline was canceled, not interrupting query");
                }
            }
        }
    });
    cancel.drop_guard()
}

#[derive(Debug, Default, Clone)]
pub struct ParsedSelect {
    table_columns: IndexMap<String, HashSet<String>>,
    aliases: HashMap<String, String>,
    pub columns: Vec<ResultColumn>,
    children: Vec<ParsedSelect>,
}

fn extract_select_columns(select: &Select, schema: &Schema) -> Result<ParsedSelect, MatcherError> {
    let mut parsed = ParsedSelect::default();

    if let OneSelect::Select {
        ref from,
        ref columns,
        ref where_clause,
        ..
    } = select.body.select
    {
        let from_table = match from {
            Some(from) => {
                let from_table = match &from.select {
                    Some(table) => match table.as_ref() {
                        SelectTable::Table(name, alias, _) => {
                            if schema.tables.contains_key(name.name.0.as_str()) {
                                if let Some(As::As(alias) | As::Elided(alias)) = alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                } else if let Some(ref alias) = name.alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                }
                                parsed.table_columns.entry(name.name.0.clone()).or_default();
                                Some(&name.name)
                            } else {
                                return Err(MatcherError::TableNotFound(name.name.0.clone()));
                            }
                        }
                        // TODO: add support for:
                        // TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
                        // Select(Select, Option<As>),
                        // Sub(FromClause, Option<As>),
                        t => {
                            warn!("ignoring {t:?}");
                            None
                        }
                    },
                    _ => {
                        // according to the sqlite3-parser docs, this can't really happen
                        // ignore!
                        unreachable!()
                    }
                };
                if let Some(ref joins) = from.joins {
                    for join in joins.iter() {
                        // let mut tbl_name = None;
                        let tbl_name = match &join.table {
                            SelectTable::Table(name, alias, _) => {
                                if let Some(As::As(alias) | As::Elided(alias)) = alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                } else if let Some(ref alias) = name.alias {
                                    parsed.aliases.insert(alias.0.clone(), name.name.0.clone());
                                }
                                parsed.table_columns.entry(name.name.0.clone()).or_default();
                                &name.name
                            }
                            // TODO: add support for:
                            // TableCall(QualifiedName, Option<Vec<Expr>>, Option<As>),
                            // Select(Select, Option<As>),
                            // Sub(FromClause, Option<As>),
                            t => {
                                warn!("ignoring JOIN's non-SelectTable::Table:  {t:?}");
                                continue;
                            }
                        };
                        // ON or USING
                        if let Some(constraint) = &join.constraint {
                            match constraint {
                                JoinConstraint::On(expr) => {
                                    extract_expr_columns(expr, schema, &mut parsed)?;
                                }
                                JoinConstraint::Using(names) => {
                                    let entry =
                                        parsed.table_columns.entry(tbl_name.0.clone()).or_default();
                                    for name in names.iter() {
                                        entry.insert(name.0.clone());
                                    }
                                }
                            }
                        }
                    }
                }
                if let Some(expr) = where_clause {
                    extract_expr_columns(expr, schema, &mut parsed)?;
                }
                from_table
            }
            _ => None,
        };

        extract_columns(columns.as_slice(), from_table, schema, &mut parsed)?;
    }

    Ok(parsed)
}

fn extract_expr_columns(
    expr: &Expr,
    schema: &Schema,
    parsed: &mut ParsedSelect,
) -> Result<(), MatcherError> {
    match expr {
        // simplest case
        Expr::Qualified(tblname, colname) => {
            let resolved_name = parsed.aliases.get(&tblname.0).unwrap_or(&tblname.0);
            // println!("adding column: {resolved_name} => {colname:?}");
            parsed
                .table_columns
                .entry(resolved_name.clone())
                .or_default()
                .insert(colname.0.clone());
        }
        // simplest case but also mentioning the schema
        Expr::DoublyQualified(schema_name, tblname, colname) if schema_name.0 == "main" => {
            let resolved_name = parsed.aliases.get(&tblname.0).unwrap_or(&tblname.0);
            // println!("adding column: {resolved_name} => {colname:?}");
            parsed
                .table_columns
                .entry(resolved_name.clone())
                .or_default()
                .insert(colname.0.clone());
        }

        Expr::Name(colname) => {
            let check_col_name = unquote(&colname.0).ok().unwrap_or(colname.0.clone());

            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&check_col_name) {
                        if found.is_some() {
                            return Err(MatcherError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(tbl.name.as_str());
                    }
                }
            }

            if let Some(found) = found {
                parsed
                    .table_columns
                    .entry(found.to_owned())
                    .or_default()
                    .insert(check_col_name);
            } else {
                return Err(MatcherError::TableForColumnNotFound {
                    col_name: check_col_name,
                });
            }
        }

        Expr::Id(colname) => {
            let check_col_name = unquote(&colname.0).ok().unwrap_or(colname.0.clone());

            let mut found = None;
            for tbl in parsed.table_columns.keys() {
                if let Some(tbl) = schema.tables.get(tbl) {
                    if tbl.columns.contains_key(&check_col_name) {
                        if found.is_some() {
                            return Err(MatcherError::QualificationRequired {
                                col_name: check_col_name,
                            });
                        }
                        found = Some(tbl.name.as_str());
                    }
                }
            }

            if let Some(found) = found {
                parsed
                    .table_columns
                    .entry(found.to_owned())
                    .or_default()
                    .insert(colname.0.clone());
            } else {
                if colname.0.starts_with('"') {
                    return Ok(());
                }
                return Err(MatcherError::TableForColumnNotFound {
                    col_name: colname.0.clone(),
                });
            }
        }

        Expr::Between { lhs, .. } => extract_expr_columns(lhs, schema, parsed)?,
        Expr::Binary(lhs, _, rhs) => {
            extract_expr_columns(lhs, schema, parsed)?;
            extract_expr_columns(rhs, schema, parsed)?;
        }
        Expr::Case {
            base,
            when_then_pairs,
            else_expr,
        } => {
            if let Some(expr) = base {
                extract_expr_columns(expr, schema, parsed)?;
            }
            for (when_expr, _then_expr) in when_then_pairs.iter() {
                // NOTE: should we also parse the then expr?
                extract_expr_columns(when_expr, schema, parsed)?;
            }
            if let Some(expr) = else_expr {
                extract_expr_columns(expr, schema, parsed)?;
            }
        }
        Expr::Cast { expr, .. } => extract_expr_columns(expr, schema, parsed)?,
        Expr::Collate(expr, _) => extract_expr_columns(expr, schema, parsed)?,
        Expr::Exists(select) => {
            parsed
                .children
                .push(extract_select_columns(select, schema)?);
        }
        Expr::FunctionCall { args, .. } => {
            if let Some(args) = args {
                for expr in args.iter() {
                    extract_expr_columns(expr, schema, parsed)?;
                }
            }
        }
        Expr::InList { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, parsed)?;
            if let Some(rhs) = rhs {
                for expr in rhs.iter() {
                    extract_expr_columns(expr, schema, parsed)?;
                }
            }
        }
        Expr::InSelect { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, parsed)?;
            parsed.children.push(extract_select_columns(rhs, schema)?);
        }
        expr @ Expr::InTable { .. } => {
            return Err(MatcherError::UnsupportedExpr { expr: expr.clone() })
        }
        Expr::IsNull(expr) => {
            extract_expr_columns(expr, schema, parsed)?;
        }
        Expr::Like { lhs, rhs, .. } => {
            extract_expr_columns(lhs, schema, parsed)?;
            extract_expr_columns(rhs, schema, parsed)?;
        }

        Expr::NotNull(expr) => {
            extract_expr_columns(expr, schema, parsed)?;
        }
        Expr::Parenthesized(parens) => {
            for expr in parens.iter() {
                extract_expr_columns(expr, schema, parsed)?;
            }
        }
        Expr::Subquery(select) => {
            parsed
                .children
                .push(extract_select_columns(select, schema)?);
        }
        Expr::Unary(_, expr) => {
            extract_expr_columns(expr, schema, parsed)?;
        }

        // no column names in there...
        // Expr::FunctionCallStar { name, filter_over } => todo!(),
        // Expr::Id(_) => todo!(),
        // Expr::Literal(_) => todo!(),
        // Expr::Raise(_, _) => todo!(),
        // Expr::Variable(_) => todo!(),
        _ => {}
    }

    Ok(())
}

fn extract_columns(
    columns: &[ResultColumn],
    from: Option<&Name>,
    schema: &Schema,
    parsed: &mut ParsedSelect,
) -> Result<(), MatcherError> {
    let mut i = 0;
    for col in columns.iter() {
        match col {
            ResultColumn::Expr(expr, _) => {
                // println!("extracting col: {expr:?} (as: {maybe_as:?})");
                extract_expr_columns(expr, schema, parsed)?;
                parsed.columns.push(ResultColumn::Expr(
                    expr.clone(),
                    Some(As::As(Name(format!("col_{i}")))),
                ));
                i += 1;
            }
            ResultColumn::Star => {
                if let Some(tbl_name) = from {
                    if let Some(table) = schema.tables.get(&tbl_name.0) {
                        let entry = parsed.table_columns.entry(table.name.clone()).or_default();
                        for col in table.columns.keys() {
                            entry.insert(col.clone());
                            parsed.columns.push(ResultColumn::Expr(
                                Expr::Name(Name(col.clone())),
                                Some(As::As(Name(format!("col_{i}")))),
                            ));
                            i += 1;
                        }
                    } else {
                        return Err(MatcherError::TableStarNotFound {
                            tbl_name: tbl_name.0.clone(),
                        });
                    }
                } else {
                    unreachable!()
                }
            }
            ResultColumn::TableStar(tbl_name) => {
                let name = parsed
                    .aliases
                    .get(tbl_name.0.as_str())
                    .unwrap_or(&tbl_name.0);
                if let Some(table) = schema.tables.get(name) {
                    let entry = parsed.table_columns.entry(table.name.clone()).or_default();
                    for col in table.columns.keys() {
                        entry.insert(col.clone());
                        parsed.columns.push(ResultColumn::Expr(
                            Expr::Qualified(tbl_name.clone(), Name(col.clone())),
                            Some(As::As(Name(format!("col_{i}")))),
                        ));
                        i += 1;
                    }
                } else {
                    return Err(MatcherError::TableStarNotFound {
                        tbl_name: name.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn table_to_expr(
    aliases: &HashMap<String, String>,
    tbl: &Table,
    table: &str,
    id: Uuid,
) -> Result<Expr, MatcherError> {
    let tbl_name = aliases
        .iter()
        .find_map(|(alias, actual)| (actual == table).then_some(alias))
        .cloned()
        .unwrap_or_else(|| table.to_owned());

    let expr = Expr::in_table(
        Expr::Parenthesized(
            tbl.pk
                .iter()
                .map(|pk| Expr::Qualified(Name(tbl_name.clone()), Name(pk.to_owned())))
                .collect(),
        ),
        false,
        QualifiedName::fullname(
            Name("__corro_sub".into()),
            Name(format!("subscription_{}_{table}", id.as_simple())),
        ),
        None,
    );

    Ok(expr)
}

#[derive(Debug, thiserror::Error)]
pub enum MatcherError {
    #[error(transparent)]
    Lexer(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("one statement is required for matching")]
    StatementRequired,
    #[error("unsupported statement")]
    UnsupportedStatement,
    #[error("at least 1 table is required in FROM / JOIN clause")]
    TableRequired,
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("table not found in schema: {0}")]
    TableNotFound(String),
    #[error("no primary key for table: {0}")]
    NoPrimaryKey(String),
    #[error("aggregate missing primary key {0}.{1}")]
    AggPrimaryKeyMissing(String, String),
    #[error("JOIN .. ON expression is not supported for join on table '{table}': {expr:?}")]
    JoinOnExprUnsupported { table: String, expr: Expr },
    #[error("expression is not supported: {expr:?}")]
    UnsupportedExpr { expr: Expr },
    #[error("could not find table for {tbl_name}.* in corrosion's schema")]
    TableStarNotFound { tbl_name: String },
    #[error("<tbl>.{col_name} qualification required for ambiguous column name")]
    QualificationRequired { col_name: String },
    #[error("could not find table for column {col_name}")]
    TableForColumnNotFound { col_name: String },
    #[error("missing primary keys, this shouldn't happen")]
    MissingPrimaryKeys,
    #[error("change queue has been closed or is full")]
    ChangeQueueClosedOrFull,
    #[error("no change was inserted, this is not supposed to happen")]
    NoChangeInserted,
    #[error("change receiver is closed")]
    EventReceiverClosed,
    #[error(transparent)]
    Unpack(#[from] UnpackError),
    #[error("did not insert subscription")]
    InsertSub,
    #[error(transparent)]
    FromSql(#[from] FromSqlError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use camino::Utf8PathBuf;
    use corro_api_types::row_to_change;
    use rusqlite::params;

    use crate::{
        agent::migrate,
        schema::{apply_schema, parse_sql},
        sqlite::{setup_conn, CrConn},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_matcher() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        let schema_sql = "CREATE TABLE sw (pk TEXT NOT NULL PRIMARY KEY, sandwich TEXT);";
        let mut schema = parse_sql(schema_sql)?;

        let sql = "SELECT sandwich FROM sw WHERE pk=\"mad\"";

        let id = Uuid::new_v4();

        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");
        let subscriptions_path: Utf8PathBuf =
            tmpdir.path().join("subs").display().to_string().into();

        let mut state_conn = CrConn::init(rusqlite::Connection::open(&db_path)?)?;
        println!("created base conn");

        setup_conn(&mut state_conn)?;
        migrate(&mut state_conn)?;
        println!("setup base conn");

        {
            let tx = state_conn.transaction()?;
            apply_schema(&tx, &Schema::default(), &mut schema)?;
            tx.commit()?;
        }

        let mut state_conn = rusqlite::Connection::open(&db_path)?;
        println!("re-created base conn");

        setup_conn(&mut state_conn)?;
        println!("re-setup base conn");

        let (tx, _rx) = mpsc::channel(1);
        let handle = Matcher::create(id, subscriptions_path, &schema, state_conn, tx, sql)?;
        println!("created matcher");

        handle.cleanup().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_diff() {
        _ = tracing_subscriber::fmt::try_init();
        let sql = "SELECT json_object(
            'targets', json_array(cs.address||':'||cs.port),
            'labels',  json_object(
              '__metrics_path__', JSON_EXTRACT(cs.meta, '$.path'),
              'app',            cs.app_name,
              'vm_account_id',  cs.organization_id,
              'instance',       cs.instance_id
            )
          )
          FROM consul_services cs
            LEFT JOIN machines m                   ON m.id = cs.instance_id
            LEFT JOIN machine_versions mv          ON m.id = mv.machine_id  AND m.machine_version_id = mv.id
            LEFT JOIN machine_version_statuses mvs ON m.id = mvs.machine_id AND m.machine_version_id = mvs.id
          WHERE cs.node = 'test-hostname'
            AND (mvs.status IS NULL OR mvs.status = 'started')
            AND cs.name == 'app-prometheus'";

        let schema_sql = "
          CREATE TABLE consul_services (
              node TEXT NOT NULL,
              id TEXT NOT NULL,
              name TEXT NOT NULL DEFAULT '',
              tags TEXT NOT NULL DEFAULT '[]',
              meta TEXT NOT NULL DEFAULT '{}',
              port INTEGER NOT NULL DEFAULT 0,
              address TEXT NOT NULL DEFAULT '',
              updated_at INTEGER NOT NULL DEFAULT 0,
              app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)), network_id INTEGER AS (
                  CAST(JSON_EXTRACT(meta, '$.network_id') AS INTEGER)
              ), app_name TEXT AS (JSON_EXTRACT(meta, '$.app_name')), instance_id TEXT AS (
                  COALESCE(
                      JSON_EXTRACT(meta, '$.machine_id'),
                      SUBSTR(JSON_EXTRACT(meta, '$.alloc_id'), 1, 8),
                      CASE
                          WHEN INSTR(id, '_nomad-task-') = 1 THEN SUBSTR(id, 13, 8)
                          ELSE NULL
                      END
                  )
              ), organization_id INTEGER AS (
                  CAST(
                      JSON_EXTRACT(meta, '$.organization_id') AS INTEGER
                  )
              ), protocol TEXT
          AS (JSON_EXTRACT(meta, '$.protocol')),
              PRIMARY KEY (node, id)
          );

          CREATE TABLE machines (
              id TEXT NOT NULL PRIMARY KEY,
              node TEXT NOT NULL DEFAULT '',
              name TEXT NOT NULL DEFAULT '',
              machine_version_id TEXT NOT NULL DEFAULT '',
              app_id INTEGER NOT NULL DEFAULT 0,
              organization_id INTEGER NOT NULL DEFAULT 0,
              network_id INTEGER NOT NULL DEFAULT 0,
              updated_at INTEGER NOT NULL DEFAULT 0
          );

          CREATE TABLE machine_versions (
              machine_id TEXT NOT NULL,
              id TEXT NOT NULL DEFAULT '',
              config TEXT NOT NULL DEFAULT '{}',
              updated_at INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (machine_id, id)
          );

          CREATE TABLE machine_version_statuses (
              machine_id TEXT NOT NULL,
              id TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT '',
              updated_at INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY (machine_id, id)
          );
          ";

        let mut schema = parse_sql(schema_sql).unwrap();

        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");

        let mut conn =
            CrConn::init(rusqlite::Connection::open(&db_path).expect("could not open conn"))
                .expect("could not init crsql");

        let subscriptions_path: Utf8PathBuf =
            tmpdir.path().join("subs").display().to_string().into();

        setup_conn(&mut conn).unwrap();

        migrate(&mut conn).unwrap();

        {
            let tx = conn.transaction().unwrap();
            apply_schema(&tx, &Schema::default(), &mut schema).unwrap();
            tx.commit().unwrap();
        }

        // let's seed some data in there
        {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(r#"
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-1', 'app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-1"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-1', 'mv-1');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-1', 'mv-1');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-1', 'mv-1', 'started');

                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-2', 'not-app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-2"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-2', 'mv-2');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-2', 'mv-2', 'started');
                    "#).unwrap();
            tx.commit().unwrap();
        }

        {
            let mut conn2 = CrConn::init(
                rusqlite::Connection::open(tmpdir.path().join("test2.db"))
                    .expect("could not open conn"),
            )
            .expect("could not init crsql");

            setup_conn(&mut conn2).unwrap();

            {
                let tx = conn2.transaction().unwrap();
                apply_schema(&tx, &Schema::default(), &mut schema).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([1], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            let tx = conn2.transaction().unwrap();

            for change in changes {
                tx.prepare_cached(
                    r#"
                    INSERT INTO crsql_changes
                        ("table", pk, cid, val, col_version, db_version, site_id, cl, seq)
                    VALUES
                        (?,       ?,  ?,   ?,   ?,           ?,          ?,       ?,  ?)
                "#,
                )
                .unwrap()
                .execute(params![
                    change.table.as_str(),
                    change.pk,
                    change.cid.as_str(),
                    &change.val,
                    change.col_version,
                    change.db_version,
                    &change.site_id,
                    change.cl,
                    change.seq,
                ])
                .unwrap();
            }
        }

        let id = Uuid::new_v4();

        let mut matcher_conn = rusqlite::Connection::open(&db_path).expect("could not open conn");

        setup_conn(&mut matcher_conn).unwrap();

        {
            let (tx, mut rx) = mpsc::channel(1);
            let matcher = Matcher::create(
                id,
                subscriptions_path.clone(),
                &schema,
                matcher_conn,
                tx,
                sql,
            )
            .unwrap();

            println!("matcher created w/ id: {}", id.as_simple());
            println!("parsed: {:?}", matcher.0.parsed);

            assert!(matches!(rx.recv().await.unwrap(), QueryEvent::Columns(_)));

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-1\"}}".into())];

            assert_eq!(rx.recv().await.unwrap(), QueryEvent::Row(RowId(1), cells));
            println!("received a row");
            assert!(matches!(
                rx.recv().await.unwrap(),
                QueryEvent::EndOfQuery { .. }
            ));
            println!("received end of query");

            // insert the second row
            {
                let tx = conn.transaction().unwrap();
                tx.execute_batch(r#"
                INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-3', 'app-prometheus', '127.0.0.1', 1, '{"path": "/1", "machine_id": "m-3"}');

                INSERT INTO machines (id, machine_version_id) VALUES ('m-3', 'mv-3');

                INSERT INTO machine_versions (machine_id, id) VALUES ('m-3', 'mv-3');

                INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-3', 'mv-3', 'started');
            "#).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([2], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            println!("processing change...");
            matcher.process_changeset(changes.as_slice()).unwrap();
            println!("processed changes");

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Insert, RowId(2), cells, ChangeId(1))
            );

            println!("received change");

            // delete the first row
            {
                let tx = conn.transaction().unwrap();
                tx.execute_batch(r#"
                        DELETE FROM consul_services where node = 'test-hostname' AND id = 'service-1';
                    "#).unwrap();
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([3], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    println!("change: {row:?}");
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_changeset(changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.1:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-1\"}}".into())];

            println!("waiting for a change (A)");

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Delete, RowId(1), cells, ChangeId(2))
            );

            println!("got change (A)");

            // update the second row
            {
                let tx = conn.transaction().unwrap();
                let n = tx.execute(r#"
                        UPDATE consul_services SET address = '127.0.0.2' WHERE node = 'test-hostname' AND id = 'service-3';
                    "#, ()).unwrap();
                assert_eq!(n, 1);
                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([4], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    println!("change: {row:?}");
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_changeset(changes.as_slice()).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.2:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            println!("waiting for a change (B)");

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Update, RowId(2), cells, ChangeId(3))
            );

            println!("got change (B)");

            // lots of operations

            let range = 4u32..1000u32;

            {
                let tx = conn.transaction().unwrap();

                for n in range.clone() {
                    let svc_id = format!("service-{n}");
                    let ip = Ipv4Addr::from(n).to_string();
                    let port = n;
                    let machine_id = format!("m-{n}");
                    let mv = format!("mv-{n}");
                    let meta =
                        serde_json::json!({"path": format!("/path-{n}"), "machine_id": machine_id});
                    tx.execute("
                                    INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', ?, 'app-prometheus', ?, ?, ?);
                                    ", params![svc_id, ip, port, meta]).unwrap();
                    tx.execute(
                        "
                        INSERT INTO machines (id, machine_version_id) VALUES (?, ?);
                        ",
                        params![machine_id, mv],
                    )
                    .unwrap();

                    tx.execute(
                        "
                        INSERT INTO machine_versions (machine_id, id) VALUES (?, ?);
                        ",
                        params![machine_id, mv],
                    )
                    .unwrap();

                    tx.execute("
                        INSERT INTO machine_version_statuses (machine_id, id, status) VALUES (?, ?, 'started');", params![machine_id, mv]).unwrap();
                }

                tx.commit().unwrap();
            }

            let changes = {
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS NULL AND db_version = ? ORDER BY seq ASC"#).unwrap();
                let rows = prepped.query_map([5], row_to_change).unwrap();

                let mut changes = vec![];

                for row in rows {
                    changes.push(row.unwrap());
                }
                changes
            };

            matcher.process_changeset(changes.as_slice()).unwrap();

            let start = Instant::now();
            for _ in range {
                assert!(rx.recv().await.is_some());
                // FIXME: test ordering... it's actually not right!
            }

            println!("took: {:?}", start.elapsed());
            {
                let conn = rusqlite::Connection::open(
                    subscriptions_path
                        .join(matcher.id().as_simple().to_string())
                        .join("sub.sqlite"),
                )
                .unwrap();
                let mut prepped = conn.prepare("SELECT * FROM changes").unwrap();
                let cols = prepped
                    .column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>();
                let mut rows = prepped.query([]).unwrap();
                loop {
                    let row = rows.next().unwrap();
                    let row = match row {
                        None => break,
                        Some(row) => row,
                    };

                    let s = cols
                        .iter()
                        .enumerate()
                        .map(|(idx, col)| {
                            format!("{col} = {}", row.get::<_, SqliteValue>(idx).unwrap())
                        })
                        .collect::<Vec<_>>();
                    println!("{}", s.join(", "));
                }
            }
        }
    }
}
