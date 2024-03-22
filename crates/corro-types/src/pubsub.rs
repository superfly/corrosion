use std::{
    cmp,
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::{Buf, BufMut};
use camino::{Utf8Path, Utf8PathBuf};
use compact_str::{format_compact, ToCompactString};
use corro_api_types::{
    Change, ChangeId, ColumnName, ColumnType, RowId, SqliteValue, SqliteValueRef, TableName,
};
use enquote::unquote;
use fallible_iterator::FallibleIterator;
use indexmap::{IndexMap, IndexSet};
use metrics::{counter, histogram, Histogram};
use parking_lot::{Condvar, Mutex, RwLock};
use rusqlite::{
    params_from_iter,
    types::{FromSqlError, ValueRef},
    Connection, OptionalExtension,
};
use spawn::spawn_counted;
use sqlite3_parser::{
    ast::{
        As, Cmd, Expr, FromClause, JoinConstraint, JoinOperator, JoinType, JoinedSelectTable, Name,
        OneSelect, Operator, QualifiedName, ResultColumn, Select, SelectTable, Stmt,
    },
    lexer::sql::Parser,
};
use sqlite_pool::RusqlitePool;
use tokio::{
    sync::{mpsc, watch, AcquireError},
    task::block_in_place,
};
use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFuture};
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};
use uuid::Uuid;

use crate::{
    agent::SplitPool,
    api::QueryEvent,
    base::CrsqlDbVersion,
    channel::{CorroReceiver, CorroSender},
    schema::{Schema, Table},
    sqlite::CrConn,
};

pub use corro_api_types::sqlite::ChangeType;

#[derive(Debug, Default, Clone)]
pub struct SubsManager(Arc<RwLock<InnerSubsManager>>);

#[derive(Debug, Default)]
struct InnerSubsManager {
    handles: BTreeMap<Uuid, MatcherHandle>,
    queries: HashMap<String, Uuid>,
}

// tools to bootstrap a new subscriber
pub struct MatcherCreated {
    pub evt_rx: mpsc::Receiver<QueryEvent>,
}

const SUB_EVENT_CHANNEL_CAP: usize = 512;

impl SubsManager {
    pub fn get(&self, id: &Uuid) -> Option<MatcherHandle> {
        self.0.read().get(id)
    }

    pub fn get_by_query(&self, sql: &str) -> Option<MatcherHandle> {
        self.0.read().get_by_query(sql)
    }

    pub fn get_or_insert(
        &self,
        sql: &str,
        subs_path: &Utf8Path,
        schema: &Schema,
        pool: &SplitPool,
        tripwire: Tripwire,
    ) -> Result<(MatcherHandle, Option<MatcherCreated>), MatcherError> {
        if let Some(handle) = self.get_by_query(sql) {
            return Ok((handle, None));
        }

        let mut inner = self.0.write();
        if let Some(handle) = inner.get_by_query(sql) {
            return Ok((handle, None));
        }

        let id = Uuid::new_v4();
        let (evt_tx, evt_rx) = mpsc::channel(SUB_EVENT_CHANNEL_CAP);

        let handle_res = Matcher::create(
            id,
            subs_path.to_path_buf(),
            schema,
            pool.client_dedicated()?,
            evt_tx,
            sql,
            tripwire,
        );

        let handle = match handle_res {
            Ok(handle) => handle,
            Err(e) => {
                error!(sub_id = %id, "could not create subscription: {e}");
                if let Err(e) = Matcher::cleanup(id, Matcher::sub_path(subs_path, id)) {
                    error!("could not cleanup subscription: {e}");
                }

                return Err(e);
            }
        };

        inner.handles.insert(id, handle.clone());
        inner.queries.insert(sql.to_owned(), id);

        Ok((handle, Some(MatcherCreated { evt_rx })))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        &self,
        id: Uuid,
        subs_path: &Utf8Path,
        schema: &Schema,
        pool: &SplitPool,
        tripwire: Tripwire,
    ) -> Result<(MatcherHandle, MatcherCreated), MatcherError> {
        let mut inner = self.0.write();

        if inner.handles.contains_key(&id) {
            return Err(MatcherError::CannotRestoreExisting);
        }

        let (evt_tx, evt_rx) = mpsc::channel(SUB_EVENT_CHANNEL_CAP);

        let handle = Matcher::restore(
            id,
            subs_path.to_path_buf(),
            schema,
            pool.client_dedicated()?,
            evt_tx,
            tripwire,
        )?;

        inner.handles.insert(id, handle.clone());
        inner.queries.insert(handle.inner.sql.clone(), id);

        Ok((handle, MatcherCreated { evt_rx }))
    }

    pub fn remove(&self, id: &Uuid) -> Option<MatcherHandle> {
        let mut inner = self.0.write();
        inner.remove(id)
    }

    pub fn match_changes(&self, changes: &[Change], db_version: CrsqlDbVersion) {
        trace!(
            %db_version,
            "trying to match changes to subscribers, len: {}",
            changes.len()
        );
        if changes.is_empty() {
            return;
        }
        let handles = {
            let inner = self.0.read();
            if inner.handles.is_empty() {
                return;
            }
            inner.handles.clone()
        };

        for (id, handle) in handles.iter() {
            trace!(sub_id = %id, %db_version, "attempting to match changes to a subscription");
            let mut candidates = MatchCandidates::new();
            let mut match_count = 0;
            for change in changes.iter().map(MatchableChange::from) {
                if handle.filter_matchable_change(&mut candidates, change) {
                    match_count += 1;
                }
            }

            // metrics...
            for (table, pks) in candidates.iter() {
                counter!("corro.subs.changes.matched.count", "sql_hash" => handle.inner.hash.clone(), "table" => table.to_string()).increment(pks.len() as u64);
            }

            trace!(sub_id = %id, %db_version, "found {match_count} candidates");

            if let Err(e) = handle.inner.changes_tx.try_send((candidates, db_version)) {
                error!(sub_id = %id, "could not send change candidates to subscription handler: {e}");
                match e {
                    mpsc::error::TrySendError::Full(item) => {
                        warn!("channel is full, falling back to async send");
                        let changes_tx = handle.inner.changes_tx.clone();
                        tokio::spawn(async move {
                            _ = changes_tx.send(item).await;
                        });
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        if let Some(handle) = self.remove(id) {
                            tokio::spawn(handle.cleanup());
                        }
                    }
                }
            }
        }
    }
}

struct MatchableChange<'a> {
    table: &'a TableName,
    pk: &'a [u8],
    column: &'a ColumnName,
}

impl<'a> From<&'a Change> for MatchableChange<'a> {
    fn from(value: &'a Change) -> Self {
        MatchableChange {
            table: &value.table,
            pk: &value.pk,
            column: &value.cid,
        }
    }
}

impl InnerSubsManager {
    fn get(&self, id: &Uuid) -> Option<MatcherHandle> {
        self.handles.get(id).cloned()
    }

    fn get_by_query(&self, sql: &str) -> Option<MatcherHandle> {
        self.queries
            .get(sql)
            .and_then(|id| self.handles.get(id).cloned())
    }

    fn remove(&mut self, id: &Uuid) -> Option<MatcherHandle> {
        let handle = self.handles.remove(id)?;
        self.queries.remove(&handle.inner.sql);
        Some(handle)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum MatcherState {
    Created,
    Running,
}

impl MatcherState {
    fn is_running(&self) -> bool {
        matches!(self, MatcherState::Running)
    }
}

#[derive(Clone, Debug)]
pub struct MatcherHandle {
    inner: Arc<InnerMatcherHandle>,
    state: StateLock,
}

#[derive(Debug)]
struct InnerMatcherHandle {
    id: Uuid,
    sql: String,
    hash: String,
    pool: sqlite_pool::RusqlitePool,
    parsed: ParsedSelect,
    col_names: Vec<ColumnName>,
    cancel: CancellationToken,
    changes_tx: CorroSender<(MatchCandidates, CrsqlDbVersion)>,
    last_change_rx: watch::Receiver<ChangeId>,
}

type MatchCandidates = IndexMap<TableName, IndexSet<Vec<u8>>>;

impl MatcherHandle {
    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    pub fn parsed_columns(&self) -> &[ResultColumn] {
        &self.inner.parsed.columns
    }

    pub fn col_names(&self) -> &[ColumnName] {
        &self.inner.col_names
    }

    pub async fn cleanup(self) {
        self.inner.cancel.cancel();
        info!(sub_id = %self.inner.id, "Canceled subscription");
    }

    pub fn pool(&self) -> &RusqlitePool {
        &self.inner.pool
    }

    fn wait_for_running_state(&self) {
        let (lock, cvar) = &*self.state;
        let mut state = lock.lock();
        while !state.is_running() {
            cvar.wait(&mut state);
        }
    }

    pub fn cancelled(&self) -> WaitForCancellationFuture {
        self.inner.cancel.cancelled()
    }

    pub fn max_change_id(&self, conn: &Connection) -> rusqlite::Result<ChangeId> {
        self.wait_for_running_state();
        let mut prepped = conn.prepare_cached("SELECT COALESCE(MAX(id), 0) FROM changes")?;
        prepped.query_row([], |row| row.get(0))
    }

    pub fn last_change_id_sent(&self) -> ChangeId {
        *self.inner.last_change_rx.borrow()
    }

    pub fn max_row_id(&self, conn: &Connection) -> rusqlite::Result<RowId> {
        self.wait_for_running_state();
        let mut prepped =
            conn.prepare_cached("SELECT COALESCE(MAX(__corro_rowid), 0) FROM query")?;
        prepped.query_row([], |row| row.get(0))
    }

    pub fn changes_since(
        &self,
        since: ChangeId,
        conn: &Connection,
        tx: mpsc::Sender<QueryEvent>,
    ) -> rusqlite::Result<ChangeId> {
        self.wait_for_running_state();
        let mut query_cols = vec![];
        for i in 0..(self.parsed_columns().len()) {
            query_cols.push(format!("col_{i}"));
        }
        let mut prepped = conn.prepare_cached(&format!(
            "SELECT id, type, __corro_rowid, {} FROM changes WHERE id > ? ORDER BY id ASC",
            query_cols.join(",")
        ))?;

        let col_count = prepped.column_count();

        let mut max_change_id = since;

        let mut rows = prepped.query([since])?;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };

            let change_id: ChangeId = row.get(0)?;
            if change_id.0 > max_change_id.0 {
                max_change_id = change_id;
            }

            if let Err(e) = tx.blocking_send(QueryEvent::Change(
                row.get(1)?,
                row.get(2)?,
                (3..col_count)
                    .map(|i| row.get::<_, SqliteValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?,
                change_id,
            )) {
                error!("could not send change to channel: {e}");
                break;
            }
        }

        Ok(max_change_id)
    }

    pub fn all_rows(
        &self,
        conn: &Connection,
        tx: mpsc::Sender<QueryEvent>,
    ) -> Result<ChangeId, MatcherError> {
        self.wait_for_running_state();
        let mut query_cols = vec![];
        for i in 0..(self.parsed_columns().len()) {
            query_cols.push(format!("col_{i}"));
        }
        let mut prepped = conn.prepare_cached(&format!(
            "SELECT __corro_rowid, {} FROM query",
            query_cols.join(",")
        ))?;

        let col_count = prepped.column_count();

        tx.blocking_send(QueryEvent::Columns(self.col_names().to_vec()))
            .map_err(|_| MatcherError::EventReceiverClosed)?;

        let start = Instant::now();
        let mut rows = prepped.query([])?;
        let elapsed = start.elapsed();

        let mut count = 0;

        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };

            tx.blocking_send(QueryEvent::Row(
                row.get(0)?,
                (1..col_count)
                    .map(|i| row.get::<_, SqliteValue>(i))
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            ))
            .map_err(|_| MatcherError::EventReceiverClosed)?;
            count += 1;
        }

        trace!("sent {count} rows");

        let max_change_id = conn
            .prepare("SELECT COALESCE(MAX(id),0) FROM changes")?
            .query_row([], |row| row.get(0))?;

        tx.blocking_send(QueryEvent::EndOfQuery {
            time: elapsed.as_secs_f64(),
            change_id: Some(max_change_id),
        })
        .map_err(|_| MatcherError::EventReceiverClosed)?;

        Ok(max_change_id)
    }

    fn filter_matchable_change(
        &self,
        candidates: &mut MatchCandidates,
        change: MatchableChange,
    ) -> bool {
        // don't double process the same pk
        if candidates
            .get(change.table)
            .map(|pks| pks.contains(change.pk))
            .unwrap_or_default()
        {
            return false;
        }

        // don't consider changes that don't have both the table + col in the matcher query
        if !self
            .inner
            .parsed
            .table_columns
            .get(change.table.as_str())
            .map(|cols| change.column.is_crsql_sentinel() || cols.contains(change.column.as_str()))
            .unwrap_or_default()
        {
            return false;
        }

        if let Some(v) = candidates.get_mut(change.table) {
            v.insert(change.pk.to_vec())
        } else {
            candidates.insert(change.table.clone(), [change.pk.to_vec()].into());
            true
        }
    }
}

type StateLock = Arc<(Mutex<MatcherState>, Condvar)>;

struct MatcherMetrics {
    handle_candidates_time: Histogram,
}

pub struct Matcher {
    pub id: Uuid,
    pub hash: String,
    pub query: Stmt,
    pub cached_statements: HashMap<String, MatcherStmt>,
    pub pks: IndexMap<String, Vec<String>>,
    pub parsed: ParsedSelect,
    pub evt_tx: mpsc::Sender<QueryEvent>,
    pub col_names: Vec<ColumnName>,
    pub last_rowid: u64,
    conn: Connection,
    base_path: Utf8PathBuf,
    cancel: CancellationToken,
    state: StateLock,
    last_change_tx: watch::Sender<ChangeId>,
    changes_rx: CorroReceiver<(MatchCandidates, CrsqlDbVersion)>,
    metrics: MatcherMetrics,
}

#[derive(Debug, Clone)]
pub struct MatcherStmt {
    new_query: String,
    temp_query: String,
}

const CHANGE_ID_COL: &str = "id";
const CHANGE_TYPE_COL: &str = "type";

pub const QUERY_TABLE_NAME: &str = "query";

pub const SUB_DB_PATH: &str = "sub.sqlite";

impl Matcher {
    fn new(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        state_conn: &Connection,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
    ) -> Result<(Matcher, MatcherHandle), MatcherError> {
        let sub_path = Self::sub_path(subs_path.as_path(), id);

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
                PRAGMA temp_store = memory;
            "#,
        )?;

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

        for (idx, (tbl_name, _cols)) in parsed.table_columns.iter().enumerate() {
            let expr = table_to_expr(
                &parsed.aliases,
                schema
                    .tables
                    .get(tbl_name)
                    .expect("this should not happen, missing table in schema"),
                tbl_name,
            )?;

            let mut stmt = stmt.clone();

            if let Stmt::Select(select) = &mut stmt {
                if let OneSelect::Select {
                    where_clause, from, ..
                } = &mut select.body.select
                {
                    *where_clause = if let Some(prev) = where_clause.take() {
                        Some(Expr::Binary(
                            Box::new(expr),
                            Operator::And,
                            Box::new(Expr::parenthesized(prev)),
                        ))
                    } else {
                        Some(expr)
                    };

                    match from {
                        Some(FromClause {
                            joins: Some(joins), ..
                        }) if idx > 0 => {
                            // Replace LEFT JOIN with INNER join if the target is the joined table
                            if let Some(JoinedSelectTable {
                                operator:
                                    JoinOperator::TypedJoin {
                                        join_type:
                                            join_type @ Some(JoinType::LeftOuter | JoinType::Left),
                                        ..
                                    },
                                ..
                            }) = joins.get_mut(idx - 1)
                            {
                                *join_type = Some(JoinType::Inner);
                            };

                            // Remove all custom INDEXED BY clauses for the table as the most efficient
                            // way is to query it by the primary keys
                            if let Some(JoinedSelectTable {
                                table: SelectTable::Table(_, _, indexed @ Some(_)),
                                ..
                            }) = joins.get_mut(idx - 1)
                            {
                                *indexed = None
                            };
                        }
                        _ => (),
                    };
                }
            }

            let mut new_query = Cmd::Stmt(stmt).to_string();
            new_query.pop();

            let mut all_cols = pks.values().flatten().cloned().collect::<Vec<String>>();
            for i in 0..(parsed.columns.len()) {
                all_cols.push(format!("col_{i}"));
            }

            let temp_query = format!(
                "SELECT {} FROM query WHERE ({}) IN temp_{tbl_name}",
                all_cols.join(","),
                pks.get(tbl_name)
                    .cloned()
                    .ok_or(MatcherError::MissingPrimaryKeys)?
                    .to_vec()
                    .join(","),
            );

            info!(sub_id = %id, "modified query for table '{tbl_name}': {new_query}");

            statements.insert(
                tbl_name.clone(),
                MatcherStmt {
                    new_query,
                    temp_query,
                },
            );
        }

        let cancel = CancellationToken::new();

        let state = Arc::new((Mutex::new(MatcherState::Created), Condvar::new()));

        let (last_change_tx, last_change_rx) = watch::channel(ChangeId(0));

        let sql_hash = hex::encode(seahash::hash(sql.as_bytes()).to_be_bytes());
        // big channel to not miss anything
        let (changes_tx, changes_rx) =
            crate::channel::bounded(20480, "sub_changes", sql_hash.clone());

        let handle = MatcherHandle {
            inner: Arc::new(InnerMatcherHandle {
                id,
                sql: sql.to_owned(),
                hash: sql_hash.clone(),
                pool: sqlite_pool::Config::new(sub_db_path.into_std_path_buf())
                    .max_size(5)
                    .read_only()
                    .create_pool()
                    .expect("could not build pool, this can't fail because we specified a runtime"),
                parsed: parsed.clone(),
                col_names: col_names.clone(),
                cancel: cancel.clone(),
                last_change_rx,
                changes_tx,
            }),
            state: state.clone(),
        };

        let matcher = Self {
            id,
            hash: sql_hash.clone(),
            query: stmt,
            cached_statements: statements,
            pks,
            parsed,
            evt_tx,
            col_names,
            last_rowid: 0,
            conn,
            base_path: sub_path,
            cancel,
            state,
            last_change_tx,
            changes_rx,
            metrics: MatcherMetrics {
                handle_candidates_time: histogram!("corro.subs.handle.candidates.seconds", "sql_hash" => sql_hash),
            },
        };

        Ok((matcher, handle))
    }

    pub fn cleanup(id: Uuid, sub_path: Utf8PathBuf) -> rusqlite::Result<()> {
        info!(sub_id = %id, "Attempting to cleanup...");

        block_in_place(|| {
            if let Err(e) = std::fs::remove_dir_all(&sub_path) {
                error!(sub_id = %id, "could not delete subscription base path {} due to: {e}", sub_path);
            }

            Ok(())
        })
    }

    pub fn sub_path(subs_path: &Utf8Path, id: Uuid) -> Utf8PathBuf {
        subs_path.join(id.as_simple().to_string())
    }

    pub fn sub_db_path(subs_path: &Utf8Path, id: Uuid) -> Utf8PathBuf {
        Self::sub_path(subs_path, id).join(SUB_DB_PATH)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        state_conn: CrConn,
        evt_tx: mpsc::Sender<QueryEvent>,
        tripwire: Tripwire,
    ) -> Result<MatcherHandle, MatcherError> {
        let sql: String = block_in_place(|| {
            let conn = Connection::open(Matcher::sub_db_path(&subs_path, id))?;
            let state: Option<String> = conn
                .query_row("SELECT value FROM meta WHERE key = 'state'", [], |row| {
                    row.get(0)
                })
                .optional()?;
            if !matches!(state.as_deref(), Some("running")) {
                return Err(MatcherError::NotRunning);
            }

            let sql: Option<String> = conn
                .query_row("SELECT value FROM meta WHERE key = 'sql'", [], |row| {
                    row.get(0)
                })
                .optional()?;

            match sql {
                Some(sql) => Ok(sql),
                None => Err(MatcherError::MissingSql),
            }
        })?;

        let (matcher, handle) = Self::new(id, subs_path, schema, &state_conn, evt_tx, &sql)?;

        spawn_counted(matcher.run_restore(state_conn, tripwire));

        Ok(handle)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        state_conn: CrConn,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
        tripwire: Tripwire,
    ) -> Result<MatcherHandle, MatcherError> {
        let (mut matcher, handle) = Self::new(id, subs_path, schema, &state_conn, evt_tx, sql)?;

        let pk_cols = matcher
            .pks
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<String>>();

        let mut all_cols = pk_cols.clone();

        let mut query_cols = vec![];

        for i in 0..(matcher.parsed.columns.len()) {
            let col_name = format!("col_{i}");
            all_cols.push(col_name.clone());
            query_cols.push(col_name);
        }

        block_in_place(|| {
            let tx = matcher.conn.transaction()?;

            info!(sub_id = %id, "Creating subscription database schema");
            let create_temp_table = format!(
                r#"
                CREATE TABLE query (__corro_rowid INTEGER PRIMARY KEY AUTOINCREMENT, {columns});

                CREATE UNIQUE INDEX index_{id}_pk ON query ({pks_coalesced});

                CREATE TABLE changes (
                    {CHANGE_ID_COL} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    __corro_rowid INTEGER NOT NULL,
                    {CHANGE_TYPE_COL} INTEGER NOT NULL,
                    {actual_columns}
                );

                CREATE TABLE meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value
                ) WITHOUT ROWID;

                CREATE TABLE columns (
                    "table" TEXT NOT NULL,
                    cid TEXT,

                    PRIMARY KEY ("table", cid)
                );
            "#,
                columns = all_cols.join(","),
                id = id.as_simple(),
                pks_coalesced = pk_cols
                    .iter()
                    .map(|pk| format!("coalesce({pk}, \"\")"))
                    .collect::<Vec<_>>()
                    .join(","),
                actual_columns = query_cols.join(","),
            );

            tx.execute_batch(&create_temp_table)?;
            trace!("created sub tables");

            for (table, pks) in matcher.pks.iter() {
                tx.execute(
                    &format!(
                        "CREATE INDEX index_{id}_{table}_pk ON query ({pks})",
                        id = id.as_simple(),
                        table = table,
                        pks = pks.to_vec().join(","),
                    ),
                    [],
                )?;
            }
            trace!("created query indexes");

            for (table, columns) in matcher.parsed.table_columns.iter() {
                tx.execute(
                    r#"INSERT INTO columns ("table", cid) VALUES (?, '-1')"#,
                    [table.as_str()],
                )?;
                for column in columns.iter() {
                    trace!("inserting sub column {} => {}", table, column);
                    tx.execute(
                        r#"INSERT INTO columns ("table", cid) VALUES (?, ?)"#,
                        [table.as_str(), column.as_str()],
                    )?;
                }
            }
            trace!("inserted sub columns");

            tx.execute("INSERT INTO meta (key, value) VALUES ('sql', ?)", [sql])?;
            tx.execute(
                "INSERT INTO meta (key, value) VALUES ('state', 'created')",
                [],
            )?;

            tx.commit()?;
            trace!("committed subscription");

            Ok::<_, MatcherError>(())
        })?;

        spawn_counted(matcher.run(state_conn, tripwire));

        Ok(handle)
    }

    async fn run_restore(mut self, mut state_conn: CrConn, tripwire: Tripwire) {
        info!(sub_id = %self.id, "Restoring subscription");
        let init_res = block_in_place(|| {
            self.last_rowid = self
                .conn
                .query_row(
                    "SELECT COALESCE(MAX(__corro_rowid), 0) FROM query",
                    (),
                    |row| row.get(0),
                )
                .optional()?
                .unwrap_or_default();

            let max_change_id = self
                .conn
                .prepare_cached("SELECT COALESCE(MAX(id), 0) FROM changes")?
                .query_row([], |row| row.get(0))?;

            _ = self.last_change_tx.send(max_change_id);

            Ok::<_, MatcherError>(())
        });

        if let Err(e) = init_res {
            error!(sub_id = %self.id, "could not re-initialize subscription: {e}");
            return;
        }

        if let Err(e) = block_in_place(|| self.setup(&state_conn)) {
            error!(sub_id = %self.id, "could not setup connection: {e}");
            return;
        }

        let catch_up_res = block_in_place(|| {
            let db_version: CrsqlDbVersion =
                state_conn.query_row("SELECT crsql_db_version()", [], |row| row.get(0))?;

            let last_db_version: CrsqlDbVersion = self.conn.query_row(
                "SELECT value FROM meta WHERE key = 'db_version'",
                (),
                |row| row.get(0),
            )?;

            if last_db_version < db_version {
                self.handle_change(&mut state_conn, last_db_version, db_version)?;
            }

            Ok::<_, MatcherError>(())
        });

        if let Err(e) = catch_up_res {
            error!(sub_id = %self.id, "could not catch up: {e}");
            _ = self
                .evt_tx
                .try_send(QueryEvent::Error(e.to_compact_string()));
            return;
        }

        self.cmd_loop(state_conn, tripwire).await
    }

    fn setup(&self, state_conn: &Connection) -> rusqlite::Result<()> {
        info!(sub_id = %self.id, "Attaching __corro_sub to state db");
        if let Err(e) = state_conn.execute_batch(&format!(
            "ATTACH DATABASE {} AS __corro_sub",
            enquote::enquote('\'', self.base_path.join(SUB_DB_PATH).as_str()),
        )) {
            error!(sub_id = %self.id, "could not ATTACH sub db as __corro_sub on state db: {e}");
            _ = self.evt_tx.try_send(QueryEvent::Error(format_compact!(
                "could not ATTACH subscription db: {e}"
            )));
            return Err(e);
        }

        info!(sub_id = %self.id, "Attached __corro_sub to state db");
        Ok(())
    }

    async fn cmd_loop(mut self, mut state_conn: CrConn, mut tripwire: Tripwire) {
        info!(sub_id = %self.id, "Starting loop to run the subscription");
        {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock();
            *state = MatcherState::Running;
            cvar.notify_all();
            info!(sub_id = %self.id, "Notified condvar that the subscription is 'running'");
        }
        trace!("set state!");

        let mut last_db_version = None;
        let mut buf = MatchCandidates::new();
        let mut buf_count = 0;

        const MAX_BATCHED_CHANGES: usize = 100;

        let mut purge_changes_interval = tokio::time::interval(Duration::from_secs(300));

        // max duration of aggregating candidates
        let mut process_changes_interval = tokio::time::interval(Duration::from_millis(200));

        loop {
            enum Branch {
                NewCandidates((MatchCandidates, CrsqlDbVersion)),
                PurgeOldChanges,
            }

            trace!("looping...");

            let branch = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    info!(sub_id = %self.id, "Acknowledged subscription cancellation, breaking loop.");
                    break;
                }
                Some((candidates, db_version)) = self.changes_rx.recv() => {
                    for (table, pks) in  candidates {
                        let buffed = buf.entry(table).or_default();
                        for pk in pks {
                            if buffed.insert(pk) {
                                buf_count += 1;
                            }
                        }
                    }
                    last_db_version = Some(db_version);

                    if buf_count >= MAX_BATCHED_CHANGES {
                        if let Some(db_version) = last_db_version.take() {
                            Branch::NewCandidates((std::mem::take(&mut buf), db_version))
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                },
                _ = process_changes_interval.tick() => {
                    if let Some(db_version) = last_db_version.take(){
                        Branch::NewCandidates((std::mem::take(&mut buf), db_version))
                    } else {
                        continue;
                    }
                },
                _ = &mut tripwire => {
                    trace!(sub_id = %self.id, "tripped cmd_loop, returning");
                    // just return!
                    return;
                }
                _ = purge_changes_interval.tick() => Branch::PurgeOldChanges,
                else => {
                    return;
                }
            };

            match branch {
                Branch::NewCandidates((candidates, db_version)) => {
                    if let Err(e) = block_in_place(|| {
                        self.handle_candidates(&mut state_conn, candidates, db_version)
                    }) {
                        if !matches!(e, MatcherError::EventReceiverClosed) {
                            error!(sub_id = %self.id, "could not handle change: {e}");
                        }
                        break;
                    }
                    buf_count = 0;
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
                        Ok(deleted) => {
                            info!(sub_id = %self.id, "Deleted {deleted} old changes row")
                        }
                        Err(e) => {
                            error!(sub_id = %self.id, "could not delete old changes: {e}");
                        }
                    }
                }
            }
        }

        debug!(id = %self.id, "matcher loop is done");

        if let Err(e) = Self::cleanup(self.id, self.base_path.clone()) {
            error!("could not handle cleanup: {e}");
        }
    }

    async fn run(mut self, mut state_conn: CrConn, tripwire: Tripwire) {
        info!(sub_id = %self.id, "Running initial query");
        if let Err(e) = self
            .evt_tx
            .send(QueryEvent::Columns(self.col_names.clone()))
            .await
        {
            error!(sub_id = %self.id, "could not send back columns, probably means no receivers! {e}");
            return;
        }

        let mut query_cols = vec![];
        for i in 0..(self.parsed.columns.len()) {
            query_cols.push(format!("col_{i}"));
        }

        let mut first_buffered_db_version = None;

        let mut candidates = MatchCandidates::new();
        let mut last_db_version = None;

        let res = block_in_place(|| {
            let tx = self.conn.transaction()?;

            let mut stmt_str = Cmd::Stmt(self.query.clone()).to_string();
            stmt_str.pop(); // remove trailing `;`

            let mut all_cols = self
                .pks
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<String>>();

            for i in 0..(self.parsed.columns.len()) {
                let col_name = format!("col_{i}");
                all_cols.push(col_name.clone());
            }

            let mut last_rowid = 0;

            // ensure drop and recreate
            tx.execute_batch(&format!(
                "DROP TABLE IF EXISTS state_rows;
                    CREATE TEMP TABLE state_rows ({})",
                all_cols.join(",")
            ))?;

            info!(sub_id = %self.id, "Starting state conn read transaction for initial query");
            // this is read transaction up until the end!
            let state_tx = state_conn.transaction()?;

            let elapsed = {
                debug!("select stmt: {stmt_str:?}");

                let mut select = state_tx.prepare(&stmt_str)?;
                let start = Instant::now();
                let mut select_rows = {
                    let _guard = interrupt_deadline_guard(&state_tx, Duration::from_secs(15));
                    select.query(())?
                };
                let elapsed = start.elapsed();
                info!(sub_id = %self.id, "Initial query done in {elapsed:?}");

                let insert_into = format!(
                    "INSERT INTO query ({}) VALUES ({}) RETURNING __corro_rowid,{}",
                    all_cols.join(","),
                    all_cols
                        .iter()
                        .map(|_| "?".to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    query_cols.join(","),
                );
                trace!("insert stmt: {insert_into:?}");

                {
                    let mut insert = tx.prepare(&insert_into)?;

                    loop {
                        match select_rows.next() {
                            Ok(Some(row)) => {
                                for i in 0..all_cols.len() {
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
                                    error!(sub_id = %self.id, "could not send back row: {e}");
                                    return Err(MatcherError::EventReceiverClosed);
                                }

                                last_rowid = cmp::max(rowid, last_rowid);
                            }
                            Ok(None) => {
                                info!(sub_id = %self.id, "Done iterating through rows for initial query");
                                // done!
                                break;
                            }
                            Err(e) => {
                                return Err(e.into());
                            }
                        }

                        // drain this channel so it doesn't fill up
                        while let Ok((new_candidates, db_version)) = self.changes_rx.try_recv() {
                            last_db_version = Some(db_version);
                            if first_buffered_db_version.is_none() {
                                first_buffered_db_version = Some(db_version);
                            }
                            for (table, pks) in new_candidates {
                                candidates.entry(table).or_default().extend(pks);
                            }
                        }
                    }
                }

                tx.execute_batch("DROP TABLE IF EXISTS state_rows;")?;

                let db_version: CrsqlDbVersion =
                    state_tx.query_row("SELECT crsql_db_version()", [], |row| row.get(0))?;
                update_last_db_version(&tx, db_version)?;

                tx.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('state', 'running')",
                    [],
                )?;

                tx.commit()?;

                (elapsed, db_version)
            };

            self.last_rowid = last_rowid;

            Ok::<_, MatcherError>(elapsed)
        });

        let db_version = match res {
            Ok((elapsed, db_version)) => {
                trace!(
                    "done w/ block_in_place for initial query, elapsed: {:?}",
                    elapsed
                );
                if let Err(e) = self
                    .evt_tx
                    .send(QueryEvent::EndOfQuery {
                        time: elapsed.as_secs_f64(),
                        change_id: Some(ChangeId(0)),
                    })
                    .await
                {
                    error!(sub_id = %self.id, "could not return end of query event: {e}");
                    return;
                }
                db_version
            }
            Err(e) => {
                warn!(sub_id = %self.id, "could not complete initial query: {e}");
                _ = self
                    .evt_tx
                    .send(QueryEvent::Error(e.to_compact_string()))
                    .await;

                return;
            }
        };

        if let Err(e) = block_in_place(|| self.setup(&state_conn)) {
            error!(sub_id = %self.id, "could not setup connection: {e}");
            return;
        }

        if let Some(first_db_version) = first_buffered_db_version {
            if db_version < first_db_version {
                info!(sub_id = %self.id, "processing missed changes between {db_version} and {first_db_version}");
                if let Err(e) = block_in_place(|| {
                    self.handle_change(&mut state_conn, db_version + 1, first_db_version - 1)
                }) {
                    error!(sub_id = %self.id, "could not catch up from last db version {db_version} to first buffered db version {first_db_version}: {e}");
                    _ = self
                        .evt_tx
                        .try_send(QueryEvent::Error(e.to_compact_string()));
                    return;
                }
            }
            if let Some(last_db_version) = last_db_version {
                info!(sub_id = %self.id, "handling buffered candidates while performing initial query");
                if let Err(e) = block_in_place(|| {
                    self.handle_candidates(&mut state_conn, candidates, last_db_version)
                }) {
                    error!(sub_id = %self.id, "could not catch up from buffered candidates: {e}");
                    _ = self
                        .evt_tx
                        .try_send(QueryEvent::Error(e.to_compact_string()));
                    return;
                }
            }
        }

        self.cmd_loop(state_conn, tripwire).await
    }

    fn handle_candidates(
        &mut self,
        state_conn: &mut Connection,
        candidates: MatchCandidates,
        last_db_version: CrsqlDbVersion,
    ) -> Result<(), MatcherError> {
        let start = Instant::now();
        let mut tables = IndexSet::new();

        if candidates.is_empty() {
            update_last_db_version(&self.conn, last_db_version)?;
            return Ok(());
        }

        trace!(
            "got some candidates! {:?}",
            candidates.keys().collect::<Vec<_>>()
        );

        let tx = self.conn.transaction()?;
        for (table, pks) in candidates {
            let pks = pks
                .iter()
                .map(|pk| unpack_columns(pk))
                .collect::<Result<Vec<Vec<SqliteValueRef>>, _>>()?;

            let tmp_table_name = format!("temp_{table}");
            if tables.insert(table.clone()) {
                // create a temporary table to mix and match the data
                tx.prepare_cached(
                    // TODO: cache the statement's string somewhere, it's always the same!
                    // NOTE: this can't be an actual CREATE TEMP TABLE or it won't be visible
                    //       from the state db
                    &format!(
                        "CREATE TABLE IF NOT EXISTS {tmp_table_name} ({})",
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
                tx.prepare_cached(&format!(
                    "INSERT INTO {tmp_table_name} VALUES ({})",
                    (0..pks.len()).map(|_i| "?").collect::<Vec<_>>().join(",")
                ))?
                .execute(params_from_iter(pks))?;
            }
        }
        // commit so it is visible to the other db!
        tx.commit()?;
        trace!("committed temp pk tables");

        let mut query_cols = vec![];

        let pk_cols = self
            .pks
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<String>>();

        let mut all_cols = pk_cols.clone();
        for i in 0..(self.parsed.columns.len()) {
            let col_name = format!("col_{i}");
            all_cols.push(col_name.clone());
            query_cols.push(col_name);
        }

        // start a new tx
        let tx = self.conn.transaction()?;

        // ensure drop and recreate
        tx.execute_batch(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS state_results ({})",
            all_cols.join(",")
        ))?;

        let mut new_last_rowid = self.last_rowid;

        {
            // read-only!
            let state_tx = state_conn.transaction()?;
            let mut tmp_insert_prepped = tx.prepare_cached(&format!(
                "INSERT INTO state_results VALUES ({})",
                (0..all_cols.len())
                    .map(|_| "?")
                    .collect::<Vec<_>>()
                    .join(",")
            ))?;

            for table in tables.iter() {
                let stmt = match self.cached_statements.get(table.as_str()) {
                    Some(stmt) => stmt,
                    None => {
                        warn!(sub_id = %self.id, "no statements pre-computed for table {table}");
                        continue;
                    }
                };

                trace!("SELECT SQL: {}", stmt.new_query);

                let mut prepped = state_tx.prepare_cached(&stmt.new_query)?;

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

                let coalesced_pks = pk_cols
                    .iter()
                    .map(|pk| format!("coalesce({pk},\"\")"))
                    .collect::<Vec<_>>()
                    .join(",");
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
                    insert_cols = all_cols.join(","),
                    query_query = stmt.temp_query,
                    conflict_clause = coalesced_pks,
                    excluded = (0..(self.parsed.columns.len()))
                        .map(|i| format!("col_{i} = excluded.col_{i}"))
                        .collect::<Vec<_>>()
                        .join(","),
                    excluded_not_same = (0..(self.parsed.columns.len()))
                        .map(|i| format!("col_{i} IS NOT excluded.col_{i}"))
                        .collect::<Vec<_>>()
                        .join(" OR "),
                    return_cols = query_cols.join(",")
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
                    pks = coalesced_pks,
                    select_pks = coalesced_pks,
                    query_query = stmt.temp_query,
                    return_cols = query_cols.join(",")
                );

                trace!("DELETE SQL: {sql}");

                let delete_prepped = tx.prepare_cached(&sql)?;

                let mut change_insert_stmt = tx.prepare_cached(&format!(
                    "INSERT INTO changes (__corro_rowid, {CHANGE_TYPE_COL}, {}) VALUES (?, ?, {}) RETURNING {CHANGE_ID_COL}",
                    query_cols.join(","),
                    (0..query_cols.len())
                        .map(|_i| "?")
                        .collect::<Vec<_>>()
                        .join(",")
                ))?;

                for (change_type, mut prepped) in [
                    (None, insert_prepped),
                    (Some(ChangeType::Delete), delete_prepped),
                ] {
                    let col_count = prepped.column_count();

                    let mut rows = prepped.raw_query();

                    while let Ok(Some(row)) = rows.next() {
                        let rowid: RowId = row.get(0)?;

                        let change_type = change_type.clone().take().unwrap_or({
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
                                _ = self.last_change_tx.send(change_id);
                            }
                            Err(e) => {
                                error!("could not deserialize row's cells: {e}");
                                return Ok(());
                            }
                        }
                    }
                }
                // clean that up
                tx.execute_batch("DELETE FROM state_results")?;
            }

            // clean up temporary tables immediately
            for table in tables {
                // TODO: reduce mistakes by computing this table name once
                tx.prepare_cached(&format!("DELETE FROM temp_{table}",))?
                    .execute(())?;
                trace!("cleaned up temp_{table}");
            }
        }

        update_last_db_version(&tx, last_db_version)?;

        trace!("inserted new db version: {last_db_version}");

        tx.commit()?;

        self.metrics.handle_candidates_time.record(start.elapsed());

        trace!("committed!");

        self.last_rowid = new_last_rowid;

        Ok(())
    }

    fn handle_change(
        &mut self,
        state_conn: &mut Connection,
        start_db_version: CrsqlDbVersion,
        end_db_version: CrsqlDbVersion,
    ) -> Result<(), MatcherError> {
        debug!(sub_id = %self.id, "handling change from version {start_db_version} to {end_db_version}");

        let mut candidates = MatchCandidates::new();
        {
            let mut changes_prepped = state_conn.prepare_cached(
                r#"
            SELECT DISTINCT "table", pk
                FROM crsql_changes
                    WHERE db_version > ?
                      AND db_version <= ? -- TODO: allow going over?
                      AND ("table", cid) IN __corro_sub.columns -- only care about table/columns touched by the query
                    GROUP BY "table", pk
        "#,
            )?;

            let mut rows = changes_prepped.query([start_db_version, end_db_version])?;
            while let Ok(Some(row)) = rows.next() {
                candidates
                    .entry(row.get(0)?)
                    .or_default()
                    .insert(row.get(1)?);
            }
        }

        self.handle_candidates(state_conn, candidates, end_db_version)
    }
}

fn update_last_db_version(
    conn: &Connection,
    last_db_version: CrsqlDbVersion,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO meta (key,value) VALUES ('db_version', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value WHERE excluded.value > value",
        [last_db_version],
    )?;
    Ok(())
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
        QualifiedName::fullname(Name("__corro_sub".into()), Name(format!("temp_{table}"))),
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
    #[error("could not encode changeset for queue: {0}")]
    ChangesetEncode(#[from] speedy::Error),
    #[error("queue is full")]
    QueueFull,
    #[error("cannot restore existing subscription")]
    CannotRestoreExisting,
    #[error("could not acquire write permit")]
    WritePermitAcquire(#[from] AcquireError),
    #[error("subscription is not running")]
    NotRunning,
    #[error("subscription restore is missing SQL query")]
    MissingSql,
}

impl MatcherError {
    pub fn is_event_recv_closed(&self) -> bool {
        matches!(self, MatcherError::EventReceiverClosed)
    }
}

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

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use camino::Utf8PathBuf;
    use corro_api_types::row_to_change;
    use rusqlite::params;
    use spawn::wait_for_all_pending_handles;
    use tokio::sync::Semaphore;

    use crate::{
        actor::ActorId,
        agent::migrate,
        schema::{apply_schema, parse_sql},
        sqlite::{setup_conn, CrConn},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_matcher() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let schema_sql = "CREATE TABLE sw (pk TEXT NOT NULL PRIMARY KEY, sandwich TEXT);";
        let mut schema = parse_sql(schema_sql)?;

        let sql = "SELECT sandwich FROM sw WHERE pk=\"mad\"";

        let subs = SubsManager::default();

        let tmpdir = tempfile::tempdir()?;
        let db_path = tmpdir.path().join("test.db");
        let subscriptions_path: Utf8PathBuf =
            tmpdir.path().join("subs").display().to_string().into();

        let pool = SplitPool::create(db_path, Arc::new(Semaphore::new(1))).await?;
        {
            let mut conn = pool.write_priority().await?;
            setup_conn(&mut conn)?;
            migrate(&mut conn)?;
            let tx = conn.transaction()?;
            apply_schema(&tx, &Schema::default(), &mut schema)?;
            tx.commit()?;
        }

        let (handle, maybe_created) = subs.get_or_insert(
            sql,
            subscriptions_path.as_path(),
            &schema,
            &pool,
            tripwire.clone(),
        )?;

        assert!(maybe_created.is_some());

        handle.cleanup().await;

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_diff() {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

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
        let subs = SubsManager::default();

        let tmpdir = tempfile::tempdir().unwrap();
        let db_path = tmpdir.path().join("test.db");
        let subscriptions_path: Utf8PathBuf =
            tmpdir.path().join("subs").display().to_string().into();

        let pool = SplitPool::create(&db_path, Arc::new(Semaphore::new(1)))
            .await
            .unwrap();
        let mut conn = pool.write_priority().await.unwrap();

        {
            setup_conn(&mut conn).unwrap();
            migrate(&mut conn).unwrap();
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
                let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM crsql_changes WHERE db_version = ? ORDER BY seq ASC"#).unwrap();
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

        let mut matcher_conn =
            CrConn::init(rusqlite::Connection::open(&db_path).expect("could not open conn"))
                .expect("could not init crconn");

        setup_conn(&mut matcher_conn).unwrap();

        let mut last_change_id = None;

        let id = {
            // let (db_v_tx, db_v_rx) = watch::channel(0);

            let (matcher, maybe_created) = subs
                .get_or_insert(
                    sql,
                    subscriptions_path.as_path(),
                    &schema,
                    &pool,
                    tripwire.clone(),
                )
                .unwrap();

            let mut rx = maybe_created.unwrap().evt_rx;

            println!("matcher created w/ id: {}", matcher.id().as_simple());
            println!("parsed: {:?}", matcher.inner.parsed);

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

            println!("processing change...");
            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(2)).unwrap();
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

            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(3)).unwrap();

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

            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(4)).unwrap();

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.2:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            println!("waiting for a change (B)");

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Update, RowId(2), cells, ChangeId(3))
            );

            println!("got change (B)");

            // process the same and check that it doesn't produce a change again!
            // db_v_tx.send(db_version).unwrap();

            assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));

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

            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(5)).unwrap();

            let start = Instant::now();
            for _ in range {
                if let QueryEvent::Change(change_type, _, _, change_id) = rx.recv().await.unwrap() {
                    assert_eq!(change_type, ChangeType::Insert);
                    last_change_id = Some(change_id);
                }
            }

            assert_eq!(last_change_id, Some(ChangeId(999)));

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
            subs.remove(&matcher.id());
            matcher.id()
        };

        // delete a record while nothing is matching
        {
            let tx = conn.transaction().unwrap();
            tx.execute_batch(
                r#"
                    DELETE FROM consul_services where node = 'test-hostname' AND id = 'service-3';
                "#,
            )
            .unwrap();
            tx.commit().unwrap();
        }

        tokio::time::sleep(Duration::from_secs(1)).await;

        {
            let (matcher, created) = subs
                .restore(id, &subscriptions_path, &schema, &pool, tripwire.clone())
                .unwrap();
            let mut rx = created.evt_rx;

            println!("matcher restored w/ id: {}", matcher.id().as_simple());

            let (catch_up_tx, mut catch_up_rx) = mpsc::channel(1);

            tokio::spawn({
                let matcher = matcher.clone();
                async move {
                    let mut conn = matcher.pool().get().await.unwrap();
                    block_in_place(|| {
                        let conn_tx = conn.transaction()?;

                        println!(
                            "max change id: {}",
                            matcher.max_change_id(&conn_tx).unwrap()
                        );
                        matcher.all_rows(&conn_tx, catch_up_tx)?;
                        Ok::<_, MatcherError>(())
                    })
                }
            });

            assert!(matches!(
                catch_up_rx.recv().await.unwrap(),
                QueryEvent::Columns(_)
            ));

            let mut rows_count = 0;
            let mut eoq_change_id = None;

            while eoq_change_id.is_none() {
                match catch_up_rx.recv().await.unwrap() {
                    QueryEvent::EndOfQuery { change_id, .. } => eoq_change_id = Some(change_id),
                    QueryEvent::Row(_, _) => rows_count += 1,
                    QueryEvent::Change(_, _, _, _) => {
                        panic!("received a change intertwined w/ rows");
                    }
                    _ => {}
                }
            }

            assert_eq!(rows_count, 996);
            assert_eq!(eoq_change_id, Some(Some(ChangeId(1000))));

            println!("waiting for a change (A)");

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.2:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Delete, RowId(2), cells, ChangeId(1000))
            );

            assert!(rx.try_recv().is_err());
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;
    }

    fn filter_changes_from_db(
        matcher: &MatcherHandle,
        state_conn: &Connection,
        actor_id: Option<ActorId>,
        db_version: CrsqlDbVersion,
    ) -> rusqlite::Result<()> {
        let mut candidates = MatchCandidates::new();

        let mut prepped = state_conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM crsql_changes WHERE site_id = COALESCE(?, crsql_site_id()) AND db_version = ? ORDER BY seq ASC"#)?;
        let rows = prepped
            .query_map(params![actor_id, db_version], row_to_change)
            .unwrap();

        for row in rows {
            let change = row?;
            matcher.filter_matchable_change(&mut candidates, (&change).into());
        }

        if let Err(e) = matcher.inner.changes_tx.try_send((candidates, db_version)) {
            error!(sub_id = %matcher.inner.id, "could not send candidates to matcher: {e}");
        }
        Ok(())
    }
}
