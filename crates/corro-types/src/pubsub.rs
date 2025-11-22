use std::{
    cmp,
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use camino::{Utf8Path, Utf8PathBuf};
use compact_str::ToCompactString;
use corro_api_types::{
    ChangeId, ColumnName, ColumnType, RowId, SqliteValue, SqliteValueRef, TableName,
};
use indexmap::IndexMap;
use metrics::{counter, histogram};
use parking_lot::{Condvar, Mutex, RwLock};
use rusqlite::{
    types::{FromSqlError, ValueRef},
    vtab::eponymous_only_module,
    Connection, OpenFlags, OptionalExtension,
};
use spawn::spawn_counted;
use sqlite3_parser::ast::Cmd;
use sqlite_pool::RusqlitePool;
use tokio::{
    sync::{mpsc, oneshot, watch, AcquireError},
    task::block_in_place,
};
use tokio_util::sync::{CancellationToken, DropGuard, WaitForCancellationFuture};
use tracing::{debug, error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, Tripwire};
use uuid::Uuid;

use crate::{
    agent::SplitPool,
    api::QueryEvent,
    change::Change,
    matcher::sql_analyzer::{
        prepare_subscription_sql, PreparedSubscriptionSql, QueryTableMatcherStmt, ReturnExpr,
        SqlAnalysisError, MAIN_DB_IN_SUB_DB_SCHEMA_NAME,
    },
    schema::Schema,
    sqlite::unnest_param,
    updates::HandleMetrics,
    vtab::unnest::UnnestTab,
};

use crate::updates::{Handle, Manager};
pub use corro_api_types::sqlite::ChangeType;

#[derive(Debug, Default, Clone)]
pub struct SubsManager(Arc<RwLock<InnerSubsManager>>);

#[derive(Debug, Default)]
struct InnerSubsManager {
    handles: BTreeMap<Uuid, MatcherHandle>,
    queries: HashMap<String, Uuid>,
}

// tools to bootstrap a new subscriber or notifier
#[derive(Debug)]
pub struct MatcherCreated {
    pub evt_rx: mpsc::Receiver<QueryEvent>,
}

const SUB_EVENT_CHANNEL_CAP: usize = 512;

impl Manager<MatcherHandle> for SubsManager {
    fn trait_type(&self) -> String {
        "subs".to_string()
    }

    fn get(&self, id: &Uuid) -> Option<MatcherHandle> {
        self.0.read().get(id)
    }

    fn remove(&self, id: &Uuid) -> Option<MatcherHandle> {
        let mut inner = self.0.write();
        inner.remove(id)
    }

    fn get_handles(&self) -> BTreeMap<Uuid, MatcherHandle> {
        self.0.read().handles.clone()
    }
}

impl SubsManager {
    pub fn get(&self, id: &Uuid) -> Option<MatcherHandle> {
        self.0.read().get(id)
    }

    pub fn get_by_query(&self, sql: &str) -> Option<MatcherHandle> {
        self.0.read().get_by_query(sql)
    }

    pub fn get_by_hash(&self, hash: &str) -> Option<MatcherHandle> {
        self.0.read().get_by_hash(hash)
    }

    pub fn get_handles(&self) -> BTreeMap<Uuid, MatcherHandle> {
        self.0.read().handles.clone()
    }

    pub async fn drop_handles(&self) {
        let handles = {
            let mut inner = self.0.write();
            std::mem::take(&mut inner.handles)
        };
        for (_, handle) in handles.iter() {
            handle.cleanup().await;
        }
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
            pool.db_path(),
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
            pool.db_path(),
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
}

#[derive(Debug)]
pub struct MatchableChange<'a> {
    pub table: &'a TableName,
    pub pk: &'a [u8],
    pub column: &'a ColumnName,
    pub cl: i64,
}

impl<'a> From<&'a Change> for MatchableChange<'a> {
    fn from(value: &'a Change) -> Self {
        MatchableChange {
            table: &value.table,
            pk: &value.pk,
            column: &value.cid,
            cl: value.cl,
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

    pub fn get_by_hash(&self, hash: &str) -> Option<MatcherHandle> {
        self.handles
            .values()
            .find(|x| x.inner.hash == hash)
            .cloned()
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
    prepared_subscription: PreparedSubscriptionSql,
    col_names: Vec<ColumnName>,
    cancel: CancellationToken,
    changes_tx: mpsc::Sender<MatchCandidates>,
    last_change_rx: watch::Receiver<ChangeId>,
    purge_tx: mpsc::Sender<oneshot::Sender<rusqlite::Result<usize>>>,
    // some state from the matcher so we can take a look later
    subs_path: String,
    metrics: HashMap<String, HandleMetrics>,
}

pub type MatchCandidates = IndexMap<TableName, IndexMap<Vec<u8>, i64>>;

#[async_trait]
impl Handle for MatcherHandle {
    fn id(&self) -> Uuid {
        self.inner.id
    }

    fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.inner.cancel.cancelled()
    }

    fn changes_tx(&self) -> mpsc::Sender<MatchCandidates> {
        self.inner.changes_tx.clone()
    }

    async fn cleanup(&self) {
        self.inner.cancel.cancel();
        info!(sub_id = %self.inner.id, "Canceled subscription");
    }

    fn filter_matchable_change(
        &self,
        candidates: &mut MatchCandidates,
        change: MatchableChange,
    ) -> bool {
        trace!("filtering change {change:?}");
        // don't double process the same pk
        if candidates
            .get(change.table)
            .map(|pks| pks.contains_key(change.pk))
            .unwrap_or_default()
        {
            trace!("already contained key");
            return false;
        }

        // don't consider changes that don't have both the table + col in the matcher query
        if !self
            .inner
            .prepared_subscription
            .real_tables
            .get(change.table.as_str())
            .map(|table_def| {
                change.column.is_crsql_sentinel()
                    || table_def.subscribed_cols.contains(change.column.as_str())
            })
            .unwrap_or_default()
        {
            trace!("could not match against parsed query table and columns");
            return false;
        }

        if let Some(v) = candidates.get_mut(change.table) {
            v.insert(change.pk.to_vec(), change.cl).is_none()
        } else {
            candidates.insert(
                change.table.clone(),
                [(change.pk.to_vec(), change.cl)].into(),
            );
            true
        }
    }

    fn get_counter(&self, table: &str) -> &HandleMetrics {
        self.inner.metrics.get(table).unwrap_or_else(|| {
            panic!(
                "metrics counter for table '{}' missing. subs hash {}!",
                self.inner.hash, table
            )
        })
    }
}

impl MatcherHandle {
    pub fn sql(&self) -> &String {
        &self.inner.sql
    }

    pub fn hash(&self) -> &str {
        &self.inner.hash
    }

    pub fn parsed_columns(&self) -> &[ReturnExpr] {
        &self.inner.prepared_subscription.result_set
    }

    pub fn col_names(&self) -> &[ColumnName] {
        &self.inner.col_names
    }

    pub fn subs_path(&self) -> &String {
        &self.inner.subs_path
    }

    pub fn cached_stmts(&self) -> &IndexMap<String, QueryTableMatcherStmt> {
        &self.inner.prepared_subscription.query_tables
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

    /// Purges old changes from the subscription's changes table, keeping only the most recent 500.
    /// Returns the number of rows deleted.
    ///
    /// This method sends a request to the Matcher task to perform the purge.
    pub async fn purge_old_changes(&self) -> rusqlite::Result<usize> {
        self.wait_for_running_state();

        let (tx, rx) = oneshot::channel();

        // Send purge request to the Matcher task
        self.inner
            .purge_tx
            .send(tx)
            .await
            .map_err(|_| rusqlite::Error::InvalidQuery)?;

        // Wait for the result
        rx.await.map_err(|_| rusqlite::Error::InvalidQuery)?
    }

    pub fn changes_since(
        &self,
        since: ChangeId,
        conn: &Connection,
        tx: mpsc::Sender<QueryEvent>,
    ) -> rusqlite::Result<ChangeId> {
        self.wait_for_running_state();

        let mut prepped = conn.prepare_cached("SELECT COALESCE(MIN(id), 0) FROM changes")?;
        let min_change_id: u64 = prepped.query_row([], |row| row.get(0))?;

        // return error if we've cleared changes after the received change id
        if since.0 + 1 < min_change_id {
            return Err(rusqlite::Error::ModuleError(format!(
                "subscription already deleted older changes, min change id: {min_change_id}",
            )));
        }

        let mut prepped = conn.prepare_cached(&format!(
            "SELECT id, type, __corro_rowid, {} FROM changes WHERE id > ? ORDER BY id ASC",
            self.inner.prepared_subscription.return_column_names()
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
        let mut prepped = conn.prepare_cached(&format!(
            "SELECT __corro_rowid, {} FROM query",
            self.inner.prepared_subscription.return_column_names()
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
}

type StateLock = Arc<(Mutex<MatcherState>, Condvar)>;

pub struct Matcher {
    pub id: Uuid,
    pub hash: String,
    pub prepared_subscription: PreparedSubscriptionSql,
    pub evt_tx: mpsc::Sender<QueryEvent>,
    pub col_names: Vec<ColumnName>,
    pub last_rowid: u64,
    conn: Connection,
    base_path: Utf8PathBuf,
    cancel: CancellationToken,
    state: StateLock,
    last_change_tx: watch::Sender<ChangeId>,
    changes_rx: mpsc::Receiver<MatchCandidates>,
    purge_rx: mpsc::Receiver<oneshot::Sender<rusqlite::Result<usize>>>,
}

const CHANGE_ID_COL: &str = "id";
const CHANGE_TYPE_COL: &str = "type";

pub const QUERY_TABLE_NAME: &str = "query";

pub const SUB_DB_PATH: &str = "sub.sqlite";
pub const SUB_SCHEMA_MAJOR_VERSION: i64 = 2;

impl Matcher {
    fn new(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        // Path to the main corrosion database
        main_db_path: &Path,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
    ) -> Result<(Matcher, MatcherHandle), MatcherError> {
        let sub_path = Self::sub_path(subs_path.as_path(), id);
        let sql_hash = hex::encode(seahash::hash(sql.as_bytes()).to_be_bytes());

        info!(%sql_hash, sub_id = %id, "Initializing subscription at {sub_path}");
        std::fs::create_dir_all(&sub_path)?;
        let sub_db_path = sub_path.join(SUB_DB_PATH);

        let conn = Connection::open_with_flags(
            &sub_db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                // This allows us to attach the main database in RO mode
                | OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.execute_batch(
            r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA temp_store = memory;
                PRAGMA cache_size = -32000; -- 32MB
                PRAGMA mmap_size = 536870912; -- 512MB
            "#,
        )?;
        conn.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;
        conn.execute(
            format!(
                "ATTACH DATABASE 'file:{}?mode=ro' AS {};",
                main_db_path.to_string_lossy(),
                MAIN_DB_IN_SUB_DB_SCHEMA_NAME,
            )
            .as_str(),
            [],
        )?;
        // Important sanity check! The sub DB must be opened in RW mode while the attached main DB must be in RO mode
        // If that's not the case let's bail so we don't accidentally modify the main DB
        if conn.is_readonly(rusqlite::DatabaseName::Main)? {
            return Err(MatcherError::AttachMainDbError);
        }
        if !conn.is_readonly(rusqlite::DatabaseName::Attached(
            MAIN_DB_IN_SUB_DB_SCHEMA_NAME,
        ))? {
            return Err(MatcherError::AttachMainDbError);
        }

        // Sqlite will look for table names in all schemas, starting from the temp schema, then the main DB, then the attached DB
        // this will work as long as the main DB does not contain tables with the same names as the sub DB
        // Also by first preparing the SQL using sqlite we can be sure that the parser gets valid SQL
        let col_names: Vec<ColumnName> = {
            conn.prepare(sql)?
                .column_names()
                .into_iter()
                .map(|s| ColumnName(s.to_compact_string()))
                .collect()
        };

        let prepared_subscription = prepare_subscription_sql(id, sql, schema)?;

        // This means we have an sqlite parser bug
        if col_names.len() != prepared_subscription.result_set.len() {
            return Err(MatcherError::ColumnCountMismatch);
        }

        // Create all required temp tables
        conn.execute_batch(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS state_results ({})",
            prepared_subscription.return_column_names_with_pks()
        ))?;
        for table in prepared_subscription.real_tables.values() {
            conn.execute(&table.create_table_stmt, [])?;
        }

        let cancel = CancellationToken::new();

        let state = Arc::new((Mutex::new(MatcherState::Created), Condvar::new()));

        let (last_change_tx, last_change_rx) = watch::channel(ChangeId(0));

        // big channel to not miss anything
        let (changes_tx, changes_rx) = mpsc::channel(20480);

        // channel for triggering purges
        let (purge_tx, purge_rx) = mpsc::channel(10);

        // metrics counters
        let mut counter_map = HashMap::new();
        for table in prepared_subscription.parsed.table_by_real_name.keys() {
            counter_map.insert(table.clone(), HandleMetrics{
                matched_count: counter!("corro.subs.changes.matched.count", "sql_hash" => sql_hash.clone(), "table" => table.to_string()),
            });
        }

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
                prepared_subscription: prepared_subscription.clone(),
                col_names: col_names.clone(),
                cancel: cancel.clone(),
                last_change_rx,
                changes_tx,
                purge_tx,
                subs_path: sub_path.to_string(),
                metrics: counter_map,
            }),
            state: state.clone(),
        };

        let matcher = Self {
            id,
            hash: sql_hash,
            prepared_subscription,
            evt_tx,
            col_names,
            last_rowid: 0,
            conn,
            base_path: sub_path,
            cancel,
            state,
            last_change_tx,
            changes_rx,
            purge_rx,
        };

        Ok((matcher, handle))
    }

    pub fn cleanup(id: Uuid, sub_path: Utf8PathBuf) -> rusqlite::Result<()> {
        info!(sub_id = %id, "Attempting to cleanup... {}", sub_path);

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

    /// Purges old changes from the subscription's changes table, keeping only the most recent 500.
    /// Returns the number of rows deleted.
    pub fn purge_old_changes(conn: &mut Connection) -> rusqlite::Result<usize> {
        let tx = conn.transaction()?;
        let deleted = tx
            .prepare_cached(
                "DELETE FROM changes WHERE id < (SELECT COALESCE(MAX(id),0) - 500 FROM changes)",
            )?
            .execute([])?;
        tx.commit()?;
        Ok(deleted)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        // Path to the main corrosion database
        main_db_path: &Path,
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
            if !matches!(state.as_deref(), Some("completed")) {
                return Err(MatcherError::NotRunning);
            }

            let sql: Option<String> = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = 'subscribed_query'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;

            let schema_version: Option<i64> = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = 'schema_version'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;

            if schema_version != Some(SUB_SCHEMA_MAJOR_VERSION) {
                return Err(MatcherError::SchemaVersionMismatch {
                    expected: SUB_SCHEMA_MAJOR_VERSION,
                    actual: schema_version.map(|s| s.to_string()).unwrap_or_default(),
                });
            }

            match sql {
                Some(sql) => Ok(sql),
                None => Err(MatcherError::MissingSql),
            }
        })?;

        let (matcher, handle) = Self::new(id, subs_path, schema, main_db_path, evt_tx, &sql)?;

        spawn_counted(matcher.run_restore(tripwire));

        Ok(handle)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        id: Uuid,
        subs_path: Utf8PathBuf,
        schema: &Schema,
        // Path to the main corrosion database
        main_db_path: &Path,
        evt_tx: mpsc::Sender<QueryEvent>,
        sql: &str,
        tripwire: Tripwire,
    ) -> Result<MatcherHandle, MatcherError> {
        let (mut matcher, handle) = Self::new(id, subs_path, schema, main_db_path, evt_tx, sql)?;
        block_in_place(|| {
            let tx = matcher.conn.transaction()?;

            info!(sub_id = %id, "Creating subscription database schema");
            let create_temp_table = format!(
                r#"
                CREATE TABLE query (__corro_rowid INTEGER PRIMARY KEY AUTOINCREMENT, {return_column_names_with_pks_and_type}) STRICT;

                CREATE TABLE changes (
                    {CHANGE_ID_COL} INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    __corro_rowid INTEGER NOT NULL,
                    {CHANGE_TYPE_COL} INTEGER NOT NULL,
                    {return_column_names_with_type}
                ) STRICT;

                CREATE TABLE meta (
                    key TEXT PRIMARY KEY NOT NULL,
                    value
                ) WITHOUT ROWID;

                CREATE TABLE columns (
                    "table" TEXT NOT NULL,
                    alias TEXT NOT NULL,
                    cid TEXT NOT NULL,

                    PRIMARY KEY ("table", alias, cid)
                );
            "#,
                return_column_names_with_pks_and_type = matcher
                    .prepared_subscription
                    .return_column_names_with_pks_and_type(),
                return_column_names_with_type = matcher
                    .prepared_subscription
                    .return_column_names_with_type(),
            );

            tx.execute_batch(&create_temp_table)?;
            trace!("created sub tables");

            assert!(!matcher
                .prepared_subscription
                .identity_uniq_indexes
                .is_empty());
            for def in &matcher.prepared_subscription.identity_uniq_indexes {
                tx.execute(
                    &format!(
                        "CREATE UNIQUE INDEX {} ON query ({}) {}",
                        def.index_name,
                        def.index_columns_sql(),
                        def.perhaps_where_clause()
                    ),
                    [],
                )?;
            }
            trace!("created unique identity indexes");

            for def in &matcher.prepared_subscription.required_query_indexes {
                tx.execute(
                    &format!(
                        "CREATE INDEX {} ON query ({})",
                        def.index_name,
                        def.index_columns_sql()
                    ),
                    [],
                )?;
            }
            trace!("created non unique query indexes");

            for (table, details) in matcher.prepared_subscription.parsed.tables.iter() {
                tx.execute(
                    r#"INSERT INTO columns ("table", alias, cid) VALUES (?, ?, '-1')"#,
                    [table.real_table.as_str(), table.alias.as_str()],
                )?;
                for column in details.columns.iter() {
                    trace!("inserting sub column {} => {}", table.alias, column);
                    tx.execute(
                        r#"INSERT INTO columns ("table", alias, cid) VALUES (?, ?, ?)"#,
                        [
                            table.real_table.as_str(),
                            table.alias.as_str(),
                            column.as_str(),
                        ],
                    )?;
                }
            }
            trace!("inserted sub columns");

            // In the past the query was stored under the 'sql' key but we didn't store 'schema_version'
            // To prevent old corrosion from restoring subscriptions created by new corrosion using the schema_version we changed the key
            // This way when old corrosion sees that 'sql' is missing in the new databases it will just recreate the subscription
            // Similarly new corrosion will see that 'schema_version' is missing in old databases and will know not to use it
            tx.execute(
                "INSERT INTO meta (key, value) VALUES ('subscribed_query', ?)",
                [sql],
            )?;
            tx.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                [SUB_SCHEMA_MAJOR_VERSION],
            )?;
            tx.execute(
                "INSERT INTO meta (key, value) VALUES ('state', 'created')",
                [],
            )?;

            tx.commit()?;
            trace!("committed subscription");

            Ok::<_, MatcherError>(())
        })?;

        spawn_counted(matcher.run(tripwire));

        Ok(handle)
    }

    async fn run_restore(mut self, tripwire: Tripwire) {
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

        if let Err(e) = block_in_place(|| self.dump_query_plans()) {
            error!(sub_id = %self.id, "could not dump query plans: {e}");
            return;
        }

        if let Err(e) = self.set_status("running") {
            error!(sub_id = %self.id, "could not set status: {e}");
            return;
        }

        self.cmd_loop(tripwire).await
    }

    fn set_status(&self, status: &str) -> Result<(), MatcherError> {
        self.conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('state', ?)",
            [status],
        )?;
        Ok(())
    }

    fn dump_query_plans(&mut self) -> Result<(), MatcherError> {
        for (_tbl_name, query_table) in &self.prepared_subscription.query_tables {
            let table_name = &query_table.table_def.real_table;
            let table_alias = &query_table.table_def.alias;
            if let Ok(plan) = dump_query_plan(&mut self.conn, &query_table.new_query) {
                info!(sub_id = %self.id, sql_hash = %self.hash, "query plan for getting new changes for subset of changes on '{table_name}' as '{table_alias}':\n{plan}");
            }
            if let Ok(plan) = dump_query_plan(&mut self.conn, &query_table.temp_query) {
                info!(sub_id = %self.id, sql_hash = %self.hash, "query plan for checking the current query result for subset of changes on '{table_name}' as '{table_alias}':\n{plan}");
            }
        }

        Ok(())
    }

    async fn cmd_loop(mut self, mut tripwire: Tripwire) {
        const PROCESS_CHANGES_THRESHOLD: usize = 1000;
        const PROCESSING_WARN_THRESHOLD: Duration = Duration::from_secs(5);
        const PROCESS_BUFFER_DEADLINE: Duration = Duration::from_millis(600);

        info!(sub_id = %self.id, "Starting loop to run the subscription");
        {
            let (lock, cvar) = &*self.state;
            let mut state = lock.lock();
            *state = MatcherState::Running;
            cvar.notify_all();
            info!(sub_id = %self.id, "Notified condvar that the subscription is 'running'");
        }
        trace!("set state!");

        let mut buf = MatchCandidates::new();
        let mut buf_count = 0;

        let mut purge_changes_interval = tokio::time::interval(Duration::from_secs(300));

        // max duration of aggregating candidates
        let process_changes_deadline = tokio::time::sleep(PROCESS_BUFFER_DEADLINE);
        tokio::pin!(process_changes_deadline);

        loop {
            enum Branch {
                NewCandidates(MatchCandidates),
                PurgeOldChanges(Option<oneshot::Sender<rusqlite::Result<usize>>>),
            }

            // trace!("looping...");

            let branch = tokio::select! {
                biased;
                _ = self.cancel.cancelled() => {
                    info!(sub_id = %self.id, "Acknowledged subscription cancellation, breaking loop.");
                    if let Err(e) = self.set_status("cancelled") {
                        error!(sub_id = %self.id, "could not set status during cancellation: {e}");
                    }
                    info!(sub_id = %self.id, "attempting to cleanup");
                    if let Err(e) = Self::cleanup(self.id, self.base_path.clone()) {
                        error!("could not handle cleanup: {e}");
                    }
                    return;
                }
                Some(candidates) = self.changes_rx.recv() => {
                    for (table, pks) in candidates {
                        let buffed = buf.entry(table).or_default();
                        for (pk, cl) in pks {
                            if buffed.insert(pk, cl).is_none() {
                                buf_count += 1;
                            }
                        }
                    }

                    if buf_count >= PROCESS_CHANGES_THRESHOLD {
                        buf_count = 0;
                        Branch::NewCandidates(std::mem::take(&mut buf))
                    } else {
                        continue;
                    }
                },
                _ = process_changes_deadline.as_mut() => {
                    process_changes_deadline
                        .as_mut()
                        .reset((Instant::now() + PROCESS_BUFFER_DEADLINE).into());
                    if buf_count == 0 {
                        continue;
                    }
                    Branch::NewCandidates(std::mem::take(&mut buf))
                },
                _ = &mut tripwire => {
                    info!(sub_id = %self.id, "tripped cmd_loop, returning");
                    // just return!
                    break;
                }
                _ = purge_changes_interval.tick() => Branch::PurgeOldChanges(None),
                Some(response_tx) = self.purge_rx.recv() => Branch::PurgeOldChanges(Some(response_tx)),
                else => {
                    return;
                }
            };

            match branch {
                Branch::NewCandidates(candidates) => {
                    let start = Instant::now();
                    if let Err(e) = block_in_place(|| self.handle_candidates(candidates, false)) {
                        if !matches!(e, MatcherError::EventReceiverClosed) {
                            error!(sub_id = %self.id, "could not handle change: {e}");
                        }
                        info!(sub_id = %self.id, "attempting to cleanup");
                        if let Err(e) = Self::cleanup(self.id, self.base_path.clone()) {
                            error!("could not handle cleanup: {e}");
                        }
                        return;
                    }
                    let elapsed = start.elapsed();

                    histogram!("corro.subs.changes.processing.duration.seconds", "sql_hash" => self.hash.clone()).record(elapsed);

                    if elapsed >= PROCESSING_WARN_THRESHOLD {
                        warn!(sub_id = %self.id, "processed {buf_count} changes (very slowly) for subscription in {elapsed:?}");
                    } else {
                        debug!(sub_id = %self.id, "processed {buf_count} changes for subscription in {elapsed:?}");
                    }
                    buf_count = 0;

                    // reset the deadline
                    process_changes_deadline
                        .as_mut()
                        .reset((Instant::now() + PROCESS_BUFFER_DEADLINE).into());
                }
                Branch::PurgeOldChanges(maybe_response_tx) => {
                    let start = Instant::now();
                    let res = block_in_place(|| Self::purge_old_changes(&mut self.conn));

                    match &res {
                        Ok(deleted) => {
                            info!(sub_id = %self.id, "Deleted {deleted} old changes row in {:?}", start.elapsed());
                        }
                        Err(e) => {
                            error!(sub_id = %self.id, "could not delete old changes: {e}");
                        }
                    }

                    // Maybe send the result back to the caller
                    if let Some(response_tx) = maybe_response_tx {
                        let _ = response_tx.send(res);
                    }
                }
            }
        }

        info!(sub_id = %self.id, "draining changes channel");
        while let Some(candidates) = self.changes_rx.recv().await {
            for (table, pks) in candidates {
                let buffed = buf.entry(table).or_default();
                for (pk, cl) in pks {
                    buffed.insert(pk, cl);
                }
            }
        }

        if !buf.is_empty() {
            info!(sub_id = %self.id, "handling buffered candidates");
            let start = Instant::now();
            if let Err(e) = block_in_place(|| self.handle_candidates(buf, true)) {
                error!(sub_id = %self.id, "could not handle final buffered candidates: {e}");
                if let Err(e) = Self::cleanup(self.id, self.base_path.clone()) {
                    error!(sub_id = %self.id, "could not handle cleanup: {e}");
                }
                return;
            }
            let elapsed = start.elapsed();
            info!(sub_id = %self.id, "handled final buffered candidates in {elapsed:?}");
        }

        if let Err(e) = self.set_status("completed") {
            error!(sub_id = %self.id, "could not set status: {e}");
        };

        info!(sub_id = %self.id, "matcher loop is done");
    }

    async fn run(mut self, tripwire: Tripwire) {
        info!(sub_id = %self.id, "Running initial query");
        if let Err(e) = self
            .evt_tx
            .send(QueryEvent::Columns(self.col_names.clone()))
            .await
        {
            error!(sub_id = %self.id, "could not send back columns, probably means no receivers! {e}");
            return;
        }

        let res = block_in_place(|| {
            let tx = self.conn.transaction()?;

            let mut stmt_str =
                Cmd::Stmt(self.prepared_subscription.initial_query.clone()).to_string();
            stmt_str.pop(); // remove trailing `;`

            // Add NULL for the rowid so it will use the autoincrement
            let full_query = format!(
                "
                WITH initial_rows AS ({})
                INSERT INTO query SELECT NULL, * FROM initial_rows RETURNING __corro_rowid,{}
                ",
                stmt_str,
                self.prepared_subscription.return_column_names(),
            );

            let mut last_rowid = 0;
            let elapsed = {
                debug!("full query: {full_query:?}");

                let mut rows = tx.prepare(&full_query)?;
                let start = Instant::now();
                let mut select_rows = {
                    let _guard = interrupt_deadline_guard(&tx, Duration::from_secs(15));
                    rows.query(())?
                };
                let elapsed: Duration = start.elapsed();

                info!(sub_id = %self.id, "Initial query+insert done in {elapsed:?}");

                loop {
                    match select_rows.next() {
                        Ok(Some(row)) => {
                            let rowid = row.get(0)?;
                            let cells = (1..=self.prepared_subscription.result_set.len())
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
                }

                drop(select_rows);
                drop(rows);

                tx.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES ('state', 'running')",
                    [],
                )?;

                tx.commit()?;

                elapsed
            };

            self.last_rowid = last_rowid;

            Ok::<_, MatcherError>(elapsed)
        });

        match res {
            Ok(elapsed) => {
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
                // db_version
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

        if let Err(e) = block_in_place(|| self.dump_query_plans()) {
            error!(sub_id = %self.id, "could not dump query plans: {e}");
            return;
        }

        self.cmd_loop(tripwire).await
    }

    fn handle_candidates(
        &mut self,
        candidates: MatchCandidates,
        skip_send: bool,
    ) -> Result<(), MatcherError> {
        if candidates.is_empty() {
            return Ok(());
        }

        trace!(
            "got some candidates! {:?}",
            candidates.keys().collect::<Vec<_>>()
        );

        let mut clean_pk_stmts = Vec::new();
        let mut affected_query_tables = Vec::new();
        let tx = self.conn.transaction()?;
        // Insert the affected pks into the temp tables
        for (table_name, pks) in candidates {
            let real_table = self
                .prepared_subscription
                .real_tables
                .get(table_name.as_str())
                .ok_or(MatcherError::TableMissing(table_name.0.into()))?;
            clean_pk_stmts.push(&real_table.clean_table_stmt);
            // Unpack the pks into columns
            let mut prepped = tx.prepare_cached(&real_table.unnest_insert_pks_stmt)?;
            let parameter_count = real_table.table_pk.len();
            let mut columns: Vec<Vec<SqliteValue>> = (1..=parameter_count)
                .map(|_| Vec::with_capacity(pks.len()))
                .collect::<Vec<_>>();
            for (pk, _) in pks {
                let pk_cols = unpack_columns(&pk)?;
                if pk_cols.len() != parameter_count {
                    return Err(MatcherError::MalformedPk);
                }
                for (i, col) in pk_cols.iter().enumerate() {
                    columns[i].push(col.to_owned());
                }
            }
            // Bind the columns to the prepared statement
            for idx in 1..=parameter_count {
                prepped.raw_bind_parameter(idx, unnest_param(&columns[idx - 1]))?;
            }
            // And execute it
            prepped.raw_execute()?;

            // See which aliased tables are affected by this change
            for table_def in self
                .prepared_subscription
                .parsed
                .table_by_real_name
                .get(&real_table.main_table_name)
                .unwrap()
            {
                affected_query_tables.push(table_def);
            }
        }
        trace!("inserted temp pk tables");

        let mut new_last_rowid = self.last_rowid;
        {
            for table_def in &affected_query_tables {
                let start: Instant = Instant::now();
                let query_table = self
                    .prepared_subscription
                    .query_tables
                    .get(&table_def.alias)
                    .unwrap();

                let sql = format!(
                    "WITH change_batch AS ({})
                    INSERT INTO state_results SELECT * FROM change_batch ",
                    query_table.new_query,
                );
                trace!("SELECT SQL: {}", sql);
                tx.prepare_cached(&sql)?.execute([])?;

                let upsert_body = format!(
                    "
                        DO UPDATE SET
                            {excluded}
                        WHERE {excluded_not_same}
                    ",
                    excluded = self
                        .prepared_subscription
                        .result_set
                        .iter()
                        .map(|x| format!("{col_name} = excluded.{col_name}", col_name = x.alias))
                        .collect::<Vec<_>>()
                        .join(","),
                    excluded_not_same = self
                        .prepared_subscription
                        .result_set
                        .iter()
                        .map(|x| format!(
                            "{col_name} IS NOT excluded.{col_name}",
                            col_name = x.alias
                        ))
                        .collect::<Vec<_>>()
                        .join(" OR "),
                );

                // for each uniq index
                // ON CONFLICT(...) [WHERE clause for partial index]
                // DO set_row_to_new_data...
                // WHERE row_data_actually_changed
                let multi_upsert = self
                    .prepared_subscription
                    .identity_uniq_indexes
                    .iter()
                    .map(|x| {
                        format!(
                            "{on_conflict_clause}
                                 {upsert_body}
                            ",
                            on_conflict_clause = x.on_conflict_clause(),
                            upsert_body = upsert_body
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let sql = format!(
                    "INSERT INTO query ({insert_cols})
                        SELECT * FROM (
                            SELECT * FROM state_results
                            EXCEPT
                            {query_query}
                        ) WHERE 1
                        {multi_upsert}
                        RETURNING __corro_rowid,{return_cols}",
                    // insert into
                    insert_cols = self.prepared_subscription.return_column_names_with_pks(),
                    query_query = query_table.temp_query,
                    multi_upsert = multi_upsert,
                    return_cols = self.prepared_subscription.return_column_names()
                );

                trace!("INSERT SQL: {sql}");

                let insert_prepped = tx.prepare_cached(&sql)?;

                // Build the CTE branches for each partial index
                let cte_branches: Vec<String> = self
                    .prepared_subscription
                    .identity_uniq_indexes
                    .iter()
                    .enumerate()
                    .map(|(idx, def)| {
                        let where_clause = def
                            .filter_expr_sql()
                            .map(|f| format!("AND ({f})"))
                            .unwrap_or_default();

                        format!(
                            "deleted_{idx} AS (
                            SELECT __corro_rowid FROM query
                            WHERE ({pks}) IN (SELECT {pks} FROM rows_which_dissapeared)
                            {where_clause}
                        )",
                            idx = idx,
                            pks = def.index_columns_sql(),
                            where_clause = where_clause
                        )
                    })
                    .collect();

                // Build the UNION of all rowids
                let union_selects: Vec<String> = (0..cte_branches.len())
                    .map(|idx| format!("SELECT __corro_rowid FROM deleted_{idx}"))
                    .collect();

                let sql = format!(
                    "
                    WITH rows_which_dissapeared AS (
                        {query_query}
                        EXCEPT
                        SELECT * FROM state_results
                    ),
                    {cte_branches}
                    DELETE FROM query 
                    WHERE __corro_rowid IN (
                        {union_selects}
                    ) RETURNING __corro_rowid,{return_cols}
                ",
                    cte_branches = cte_branches.join(",\n        "),
                    union_selects = union_selects.join("\n        UNION ALL\n        "),
                    query_query = query_table.temp_query,
                    return_cols = self.prepared_subscription.return_column_names()
                );

                trace!("DELETE SQL: {sql}");

                let delete_prepped = tx.prepare_cached(&sql)?;

                let mut change_insert_stmt = tx.prepare_cached(&format!(
                    "INSERT INTO changes (__corro_rowid, {CHANGE_TYPE_COL}, {}) VALUES (?, ?, {}) RETURNING {CHANGE_ID_COL}",
                    self.prepared_subscription.return_column_names(),
                    (0..self.prepared_subscription.result_set.len())
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

                        let change_type = change_type.unwrap_or({
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

                                if !skip_send {
                                    if let Err(e) = self.evt_tx.blocking_send(QueryEvent::Change(
                                        change_type,
                                        rowid,
                                        cells,
                                        change_id,
                                    )) {
                                        warn!("could not send back row to matcher sub sender: {e}");
                                        return Err(MatcherError::EventReceiverClosed);
                                    }
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
                // Clean the temporary query result table
                tx.execute_batch("DELETE FROM state_results")?;

                let elapsed = start.elapsed();
                histogram!("corro.subs.changes.processing.table.duration.seconds", "sql_hash" => self.hash.clone(), "table" => table_def.real_table.clone()).record(elapsed);
            }

            // Clean the PK tables
            for stmt in clean_pk_stmts {
                tx.prepare_cached(stmt)?.execute([])?;
            }
        }

        tx.commit()?;

        trace!("committed!");

        self.last_rowid = new_last_rowid;

        Ok(())
    }
}

fn dump_query_plan(conn: &mut Connection, query: &str) -> Result<String, MatcherError> {
    let mut prepped = conn.prepare(&format!("EXPLAIN QUERY PLAN {query}"))?;
    let mut rows = prepped.query(())?;

    let mut output = String::new();
    while let Some(row) = rows.next()? {
        output.push_str(&row.get::<_, String>("detail")?);
        output.push('\n');
    }

    Ok(output)
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

#[derive(Debug, thiserror::Error)]
pub enum MatcherError {
    #[error(transparent)]
    SqlAnalysis(#[from] SqlAnalysisError),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("could not attach the main database to the subscription database in RO mode")]
    AttachMainDbError,
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
    #[error("corrosion disagrees with sqlite on the number of columns returned by the query. This is an corrosion bug, please report it")]
    ColumnCountMismatch,
    #[error("table {0} is missing from the prepared subscription")]
    TableMissing(String),
    #[error("malformed PK")]
    MalformedPk,
    #[error("subscription schema version mismatch: expected {expected}, got {actual}")]
    SchemaVersionMismatch { expected: i64, actual: String },
}

impl MatcherError {
    pub fn is_event_recv_closed(&self) -> bool {
        matches!(self, MatcherError::EventReceiverClosed)
    }
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

pub fn unpack_columns(mut buf: &[u8]) -> Result<Vec<SqliteValueRef<'_>>, UnpackError> {
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
                let len = buf.get_uint(intlen) as usize;
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
                let unsigned = buf.get_uint(intlen);
                ret.push(SqliteValueRef(ValueRef::Integer(unsigned as i64)));
            }
            Some(ColumnType::Null) => {
                ret.push(SqliteValueRef(ValueRef::Null));
            }
            Some(ColumnType::Text) => {
                if buf.remaining() < intlen {
                    return Err(UnpackError::Abort);
                }
                let len = buf.get_uint(intlen) as usize;
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
    use rusqlite::{params, Connection};
    use spawn::wait_for_all_pending_handles;
    use tokio::sync::Semaphore;

    use crate::{
        actor::ActorId,
        agent::migrate,
        base::CrsqlDbVersion,
        change::row_to_change,
        schema::{apply_schema, parse_sql},
        sqlite::{rusqlite_to_crsqlite, setup_conn, CrConn},
    };
    use corro_tests::tempdir::TempDir;

    use super::*;

    #[test]
    fn test_pack_unpack() {
        let neg_one: i64 = -1;
        let big_neg: i64 = -2500000;
        let i8_max: i64 = i8::MAX as i64;
        let i16_max: i64 = i16::MAX as i64;
        let columns = vec![
            vec![1_i64.into(), SqliteValue::Null],
            vec![
                neg_one.into(),
                "abcdefghijklmnopqrstuvwxyz1234567890?".into(),
            ],
            vec![i64::MIN.into(), 1.0.into()],
            vec![i64::MAX.into(), vec![1, 2, 3].into()],
            vec![big_neg.into(), f64::MAX.into()],
            vec![10156800_i64.into(), f64::MIN.into()],
            vec![10000000_i64.into(), "".into()],
            vec![i8_max.into(), "a".into()],
            vec![i16_max.into(), vec![0].into()],
        ];
        for cols in columns.clone() {
            let packed = pack_columns(&cols).unwrap();
            let unpacked: Vec<_> = unpack_columns(&packed)
                .unwrap()
                .into_iter()
                .map(|v| v.to_owned())
                .collect();
            assert_eq!(cols, unpacked);
        }

        let conn = Connection::open_in_memory().unwrap();
        let cr_conn = rusqlite_to_crsqlite(conn).unwrap();
        cr_conn
            .execute_batch("CREATE TABLE foo (pk INTEGER NOT NULL PRIMARY KEY, col_1);")
            .unwrap();

        for cols in columns {
            cr_conn
                .execute(
                    "INSERT INTO foo (pk, col_1) VALUES (?, ?);",
                    params![cols[0], cols[1]],
                )
                .unwrap();
            let packed = cr_conn
                .query_row(
                    "SELECT crsql_pack_columns(pk, col_1) FROM foo where pk = ?",
                    params![cols[0]],
                    |row| row.get::<_, Vec<u8>>(0),
                )
                .unwrap();
            let unpacked: Vec<_> = unpack_columns(&packed)
                .unwrap()
                .into_iter()
                .map(|v| v.to_owned())
                .collect();
            assert_eq!(cols, unpacked);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_matcher() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let schema_sql = "CREATE TABLE sw (pk TEXT NOT NULL PRIMARY KEY, sandwich TEXT);";
        let mut schema = parse_sql(schema_sql)?;

        let sql = "SELECT sandwich FROM sw WHERE pk=\"mad\"";

        let subs = SubsManager::default();

        let tmpdir = TempDir::new(tempfile::tempdir()?);
        let db_path = tmpdir.path().join("test.db");
        println!("db_path: {}", db_path.display());
        let subscriptions_path: Utf8PathBuf =
            tmpdir.path().join("subs").display().to_string().into();

        let pool = SplitPool::create(db_path, Arc::new(Semaphore::new(1))).await?;
        let clock = Arc::new(uhlc::HLC::default());

        {
            let mut conn = pool.write_priority().await?;
            setup_conn(&conn)?;
            migrate(clock, &mut conn)?;
            let tx = conn.transaction()?;
            apply_schema(&tx, &Schema::default(), &mut schema)?;
            tx.commit()?;
        }

        {
            let (handle, maybe_created) = subs.get_or_insert(
                sql,
                subscriptions_path.as_path(),
                &schema,
                &pool,
                tripwire.clone(),
            )?;

            assert!(maybe_created.is_some());

            handle.cleanup().await;
            subs.remove(&handle.id());
        }

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

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
        let subs = SubsManager::default();

        let tmpdir = TempDir::new(tempfile::tempdir().unwrap());
        let db_path = tmpdir.path().join("test.db");
        let subscriptions_path: Utf8PathBuf =
            tmpdir.path().join("subs").display().to_string().into();

        let pool = SplitPool::create(&db_path, Arc::new(Semaphore::new(1)))
            .await
            .unwrap();
        let mut conn = pool.write_priority().await.unwrap();
        let clock = Arc::new(uhlc::HLC::default());
        {
            setup_conn(&conn).unwrap();
            migrate(clock.clone(), &mut conn).unwrap();
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

            setup_conn(&conn2).unwrap();
            migrate(clock.clone(), &mut conn2).unwrap();

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
                debug!("change: {change:?}");
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
                    change.seq
                ])
                .unwrap();
            }
        }

        let matcher_conn =
            CrConn::init(rusqlite::Connection::open(&db_path).expect("could not open conn"))
                .expect("could not init crconn");

        setup_conn(&matcher_conn).unwrap();

        let mut last_change_id = None;

        let id = {
            let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

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
            println!("parsed: {:?}", matcher.inner.prepared_subscription.parsed);

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

            // trip the wire so subs loop exits cleanly
            tripwire_tx.send(()).await.ok();
            tripwire_worker.await;
            matcher.id()
        };

        tokio::time::sleep(Duration::from_secs(1)).await;

        // Check that restoration fails when schema version is wrong
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        {
            // Change the schema version
            {
                let conn =
                    Connection::open(Matcher::sub_db_path(&subscriptions_path.to_path_buf(), id))
                        .unwrap();
                conn.execute("UPDATE meta SET value = 1 WHERE key = 'schema_version'", [])
                    .unwrap();
                conn.close().unwrap();
            }
            assert!(matches!(subs
                .restore(id, &subscriptions_path, &schema, &pool, tripwire.clone())
                .unwrap_err(),
                MatcherError::SchemaVersionMismatch { ref expected, ref actual } if *expected == SUB_SCHEMA_MAJOR_VERSION && actual == "1"));

            {
                let conn =
                    Connection::open(Matcher::sub_db_path(&subscriptions_path.to_path_buf(), id))
                        .unwrap();
                conn.execute("DELETE FROM meta WHERE key = 'schema_version'", [])
                    .unwrap();
                conn.close().unwrap();
            }
            assert!(matches!(subs
                .restore(id, &subscriptions_path, &schema, &pool, tripwire.clone())
                .unwrap_err(),
                MatcherError::SchemaVersionMismatch { ref expected, ref actual } if *expected == SUB_SCHEMA_MAJOR_VERSION && actual.is_empty()));

            {
                let conn =
                    Connection::open(Matcher::sub_db_path(&subscriptions_path.to_path_buf(), id))
                        .unwrap();
                conn.execute(
                    "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                    [SUB_SCHEMA_MAJOR_VERSION],
                )
                .unwrap();
                conn.close().unwrap();
            }
        }

        // restore subscription
        let matcher_id = {
            let (matcher, created) = subs
                .restore(id, &subscriptions_path, &schema, &pool, tripwire.clone())
                .unwrap();
            let mut rx = created.evt_rx;

            let matcher_id = matcher.id().as_simple().to_string();
            println!("matcher restored w/ id: {matcher_id}");

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

            assert_eq!(rows_count, 997);
            assert_eq!(eoq_change_id, Some(Some(ChangeId(999))));

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
            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(6)).unwrap();
            println!("waiting for a change (A)");

            let cells = vec![SqliteValue::Text("{\"targets\":[\"127.0.0.2:1\"],\"labels\":{\"__metrics_path__\":\"/1\",\"app\":null,\"vm_account_id\":null,\"instance\":\"m-3\"}}".into())];

            assert_eq!(
                rx.recv().await.unwrap(),
                QueryEvent::Change(ChangeType::Delete, RowId(2), cells, ChangeId(1000))
            );

            assert!(rx.try_recv().is_err());

            {
                let tx = conn.transaction().unwrap();
                for n in 1001..=1005 {
                    tx.execute_batch(format!(r#"
                        INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-{n}', 'app-prometheus', '127.0.0.{n}', 1, '{{"path": "/1", "machine_id": "m-{n}"}}');

                        INSERT INTO machines (id, machine_version_id) VALUES ('m-{n}', 'mv-{n}');

                        INSERT INTO machine_versions (machine_id, id) VALUES ('m-{n}', 'mv-{n}');

                        INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-{n}', 'mv-{n}', 'started');
                    "#).as_str()).unwrap();
                }
                tx.commit().unwrap();
            }
            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(7)).unwrap();

            // initiate shutdown
            tripwire_tx.send(()).await.ok();
            tripwire_worker.await;

            {
                let tx = conn.transaction().unwrap();
                for n in 1006..=1010 {
                    tx.execute_batch(format!(r#"
                        INSERT INTO consul_services (node, id, name, address, port, meta) VALUES ('test-hostname', 'service-{n}', 'app-prometheus', '127.0.0.{n}', 1, '{{"path": "/1", "machine_id": "m-{n}"}}');

                        INSERT INTO machines (id, machine_version_id) VALUES ('m-{n}', 'mv-{n}');

                        INSERT INTO machine_versions (machine_id, id) VALUES ('m-{n}', 'mv-{n}');

                        INSERT INTO machine_version_statuses (machine_id, id, status) VALUES ('m-{n}', 'mv-{n}', 'started');
                    "#).as_str()).unwrap();
                }
                tx.commit().unwrap();
            }
            filter_changes_from_db(&matcher, &conn, None, CrsqlDbVersion(8)).unwrap();

            matcher_id
        };

        subs.drop_handles().await;
        wait_for_all_pending_handles().await;
        info!("tripwire worker finished");

        // check that changes sent after tripping the wire were persisted to db

        {
            let conn = rusqlite::Connection::open(
                subscriptions_path
                    .join(matcher_id.clone())
                    .join("sub.sqlite"),
            )
            .unwrap();

            let mut prepped = conn.prepare("SELECT max(id) FROM changes").unwrap();
            let max_id = prepped.query_row([], |row| row.get::<_, i64>(0)).unwrap();
            assert_eq!(max_id, 1010);

            for n in 1001..=1010 {
                let mut prepped = conn
                    .prepare(
                        format!("SELECT 1 from query where __corro_pk_cs_id = 'service-{n}'")
                            .as_str(),
                    )
                    .unwrap();
                let max_db_version = prepped.query_row([], |row| row.get::<_, i64>(0)).unwrap();
                assert_eq!(max_db_version, 1);
            }
        }

        // a restore should start ok if we shutdown properly
        {
            let res = subs.restore(id, &subscriptions_path, &schema, &pool, tripwire.clone());
            assert!(res.is_ok());
        }

        // restore should fail if we don't shutdown properly
        {
            let conn =
                rusqlite::Connection::open(subscriptions_path.join(matcher_id).join("sub.sqlite"))
                    .unwrap();
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('state', 'running')",
                (),
            )
            .unwrap();

            let res = subs.restore(id, &subscriptions_path, &schema, &pool, tripwire.clone());
            assert!(res.is_err());
        }
    }

    fn filter_changes_from_db(
        matcher: &MatcherHandle,
        state_conn: &Connection,
        actor_id: Option<ActorId>,
        db_version: CrsqlDbVersion,
    ) -> rusqlite::Result<()> {
        let mut candidates = MatchCandidates::new();

        let mut prepped = state_conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl  FROM crsql_changes WHERE site_id = COALESCE(?, crsql_site_id()) AND db_version = ? ORDER BY seq ASC"#)?;
        let rows = prepped
            .query_map(params![actor_id, db_version], row_to_change)
            .unwrap();

        for row in rows {
            let change = row?;
            matcher.filter_matchable_change(&mut candidates, (&change).into());
        }

        if let Err(e) = matcher.inner.changes_tx.try_send(candidates) {
            error!(sub_id = %matcher.inner.id, "could not send candidates to matcher: {e}");
        }
        Ok(())
    }
}
