use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    time::{Duration, Instant},
};

use metrics::counter;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rusqlite::{
    params, trace::TraceEventCodes, vtab::eponymous_only_module, Connection, Transaction,
};
use rusqlite::{
    types::{ToSql, ToSqlOutput, Value},
    DatabaseName,
};
use sqlite_pool::{Committable, SqliteConn};
use std::rc::Rc;
use tempfile::TempDir;
use thread_local::ThreadLocal;
use tracing::{error, info, trace, warn};
use tripwire::Tripwire;

use crate::vtab::unnest::UnnestTab;

pub type SqlitePool = sqlite_pool::Pool<CrConn>;
pub type SqlitePoolError = sqlite_pool::PoolError;

// Global registry for query stats
// (sql, readonly) => (total_count, total_nanos)
type QueryStats = HashMap<(String, bool), (u64, u128)>;
static QUERY_STATS: ThreadLocal<Mutex<QueryStats>> = ThreadLocal::new();
pub async fn query_metrics_loop(mut tripwire: Tripwire) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    let mut prev_tick = interval.tick().await;
    loop {
        tokio::select! {
        t = interval.tick() => {
            let elapsed = t.duration_since(prev_tick);
            prev_tick = t;
            handle_query_metrics(elapsed);
            },
            _ = &mut tripwire => break,
        }
    }
}

// Log to stdout queries taking more than 1 second
const SLOW_QUERY_THRESHOLD: Duration = Duration::from_secs(1);
// Send utilization metrics for queries taking more than 10ms per second on average
const IMPACTFUL_QUERY_THRESHOLD_MS_PER_SECOND: f64 = 10.0;
// The default length in prometheus is 4kb but 1kb is more than enough
const MAX_QUERY_LABEL_LENGTH: usize = 1024;
fn handle_query_metrics(elapsed: Duration) {
    // Aggregate and drain stats from all threads into a single map
    let mut aggregated: QueryStats = Default::default();

    for stats_mutex in QUERY_STATS.iter() {
        let mut stats = stats_mutex.lock();
        for (key, (count, nanos)) in stats.drain() {
            let entry = aggregated.entry(key).or_insert((0u64, 0u128));
            entry.0 += count;
            entry.1 += nanos;
        }
    }

    let mut other_ro_queries_count = 0u64;
    let mut other_ro_queries_nanos = 0u128;
    let mut other_rw_queries_count = 0u64;
    let mut other_rw_queries_nanos = 0u128;
    for ((query_raw, readonly), (total_query_count, total_query_nanos)) in aggregated.into_iter() {
        let total_query_ms = (total_query_nanos / 1_000_000) as u64;
        let ms_per_second = total_query_ms as f64 / elapsed.as_secs_f64();
        if ms_per_second > IMPACTFUL_QUERY_THRESHOLD_MS_PER_SECOND {
            // For too long queries, truncate them to cap the label length
            // and append a hash to avoid collisions
            let query = if query_raw.len() > MAX_QUERY_LABEL_LENGTH {
                use std::collections::hash_map::DefaultHasher;
                use std::hash::Hash;
                use std::hash::Hasher;
                let mut h = DefaultHasher::new();
                query_raw.hash(&mut h);
                format!(
                    "{}_{:x}",
                    query_raw.chars().take(1024 - 16 - 1).collect::<String>(),
                    h.finish()
                )
            } else {
                query_raw.clone()
            };
            counter!("corro.db.query.ms", "query" => query.clone() , "readonly" => readonly.to_string()).increment(total_query_ms);
            counter!("corro.db.query.count", "query" => query.clone(), "readonly" => readonly.to_string()).increment(total_query_count);
        } else {
            // For all other queries let's just sum them up
            if readonly {
                other_ro_queries_count += total_query_count;
                other_ro_queries_nanos += total_query_nanos;
            } else {
                other_rw_queries_count += total_query_count;
                other_rw_queries_nanos += total_query_nanos;
            }
        }
    }
    let other_queries_name = "OTHER";
    let other_ro_queries_ms = (other_ro_queries_nanos / 1_000_000) as u64;
    counter!("corro.db.query.ms", "query" => other_queries_name , "readonly" => "true")
        .increment(other_ro_queries_ms);
    counter!("corro.db.query.count", "query" => other_queries_name, "readonly" => "true")
        .increment(other_ro_queries_count);
    let other_rw_queries_ms = (other_rw_queries_nanos / 1_000_000) as u64;
    counter!("corro.db.query.ms", "query" => other_queries_name , "readonly" => "false")
        .increment(other_rw_queries_ms);
    counter!("corro.db.query.count", "query" => other_queries_name, "readonly" => "false")
        .increment(other_rw_queries_count);
}

fn tracing_callback_ro(ev: rusqlite::trace::TraceEvent) {
    handle_sql_tracing_event(ev, true);
}

fn tracing_callback_rw(ev: rusqlite::trace::TraceEvent) {
    handle_sql_tracing_event(ev, false);
}

fn handle_sql_tracing_event(ev: rusqlite::trace::TraceEvent, readonly: bool) {
    if let rusqlite::trace::TraceEvent::Profile(stmt_ref, duration) = ev {
        let dur = duration.as_nanos();
        let sql = stmt_ref.sql().to_string();

        // Update per-thread stats to avoid contention on hot path
        let stats_mutex = QUERY_STATS.get_or_default();
        let mut stats = stats_mutex.lock();
        let entry = stats
            .entry((sql.clone(), readonly))
            .or_insert((0u64, 0u128));
        entry.0 += 1;
        entry.1 += dur;
        drop(stats); // Release lock quickly

        if duration >= SLOW_QUERY_THRESHOLD {
            warn!(
                "SLOW {} query {duration:?} => {}",
                if readonly { "RO" } else { "RW" },
                sql
            );
        }
    }
}

pub fn trace_heavy_queries(conn: &Connection) -> rusqlite::Result<()> {
    let readonly = conn.is_readonly(DatabaseName::Main)?;
    conn.trace_v2(
        TraceEventCodes::SQLITE_TRACE_PROFILE,
        Some(if readonly {
            tracing_callback_ro
        } else {
            tracing_callback_rw
        }),
    );
    Ok(())
}

const CRSQL_EXT_GENERIC_NAME: &str = "crsqlite";

#[cfg(target_os = "macos")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.dylib";
#[cfg(target_os = "linux")]
pub const CRSQL_EXT_FILENAME: &str = "crsqlite.so";

#[cfg(all(target_arch = "aarch64", target_os = "macos"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-darwin-aarch64.dylib");
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-linux-x86_64.so");
#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
pub const CRSQL_EXT: &[u8] = include_bytes!("../crsqlite-linux-aarch64.so");

// TODO: support windows

// need to keep this alive!
static CRSQL_EXT_DIR: Lazy<TempDir> = Lazy::new(|| {
    let dir = TempDir::new().expect("could not create temp dir!");
    std::fs::write(dir.path().join(CRSQL_EXT_GENERIC_NAME), CRSQL_EXT)
        .expect("could not write crsql ext file");
    dir
});

pub fn rusqlite_to_crsqlite_write(conn: rusqlite::Connection) -> rusqlite::Result<CrConn> {
    let conn = rusqlite_to_crsqlite(conn)?;
    conn.execute_batch("PRAGMA cache_size = -32000;")?;

    Ok(conn)
}

// Due to an unknown bug, when this query get's prepared inside process_single_change_loop
// It sometimes decides to not return any rows, even though it should
// By preparing it when initializing the connection, it just works.
pub const INSERT_CRSQL_CHANGES_QUERY: &str = r#"
    INSERT INTO crsql_changes ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
        SELECT value0, value1, value2, value3, value4, value5, value6, value7, value8, value9
        FROM unnest(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        -- WARNING: This returns a row BEFORE inserting not after
        RETURNING db_version, seq, last_insert_rowid()
    "#;

pub fn rusqlite_to_crsqlite(mut conn: rusqlite::Connection) -> rusqlite::Result<CrConn> {
    init_cr_conn(&mut conn)?;
    setup_conn(&conn)?;
    sqlite_functions::add_to_connection(&conn)?;

    // Prepare problematic queries here to avoid issues
    // DON'T TOUCH IT, There are many dragons to tackle if u remove it
    // If we don't prepare it here, there's a chance invalid VDBE will get generated
    // I spent too much time debugging, it looks like a real bug in sqlite .-.
    let _ = conn.prepare_cached(INSERT_CRSQL_CHANGES_QUERY)?;

    trace_heavy_queries(&conn)?;

    Ok(CrConn(conn))
}

#[derive(Debug)]
pub struct CrConn(Connection);

impl CrConn {
    pub fn init(mut conn: Connection) -> Result<Self, rusqlite::Error> {
        init_cr_conn(&mut conn)?;
        Ok(Self(conn))
    }

    pub fn immediate_transaction(&mut self) -> rusqlite::Result<Transaction<'_>> {
        self.0
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
    }
}

impl SqliteConn for CrConn {
    fn conn(&self) -> &rusqlite::Connection {
        &self.0
    }
}

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

impl Committable for CrConn {
    fn commit(self) -> Result<(), rusqlite::Error> {
        Ok(())
    }

    fn savepoint(&mut self) -> Result<rusqlite::Savepoint<'_>, rusqlite::Error> {
        Err(rusqlite::Error::ModuleError(String::from(
            "cannot create savepoint from connection",
        )))
    }
}

fn init_cr_conn(conn: &mut Connection) -> Result<(), rusqlite::Error> {
    let ext_dir = &CRSQL_EXT_DIR;
    trace!(
        "loading crsqlite extension from path: {}",
        ext_dir.path().display()
    );
    unsafe {
        trace!("enabled loading extension");
        conn.load_extension_enable()?;
        conn.load_extension(
            ext_dir.path().join(CRSQL_EXT_GENERIC_NAME),
            Some("sqlite3_crsqlite_init"),
        )?;
        conn.load_extension_disable()?;
    }
    trace!("loaded crsqlite extension");

    Ok(())
}

pub fn setup_conn(conn: &Connection) -> Result<(), rusqlite::Error> {
    // WAL journal mode and synchronous NORMAL for best performance / crash resilience compromise
    conn.execute_batch(
        r#"
            PRAGMA journal_mode = WAL;
            PRAGMA journal_size_limit = 1073741824;
            PRAGMA synchronous = NORMAL;
            PRAGMA recursive_triggers = ON;
            PRAGMA mmap_size = 8589934592; -- 8GB
        "#,
    )?;

    rusqlite::vtab::series::load_module(conn)?;

    // Register unnest for PostgreSQL-style multi-array unnesting
    conn.create_module("unnest", eponymous_only_module::<UnnestTab>(), None)?;

    Ok(())
}

pub trait Migration {
    fn migrate(&self, tx: &Transaction) -> rusqlite::Result<()>;
}

impl<F> Migration for F
where
    F: Fn(&Transaction) -> rusqlite::Result<()>,
{
    fn migrate(&self, tx: &Transaction) -> rusqlite::Result<()> {
        self(tx)
    }
}

const SCHEMA_VERSION_KEY: &str = "schema_version";

// Read migration version field from the SQLite db
pub fn migration_version(tx: &Transaction) -> Option<usize> {
    #[allow(deprecated)] // To keep compatibility with lower rusqlite versions
    tx.query_row(
        "SELECT value FROM __corro_state WHERE key = ?",
        [SCHEMA_VERSION_KEY],
        |row| row.get::<_, i64>(0),
    )
    .map(|v| v as usize)
    .ok()
}

// Set user version field from the SQLite db
pub fn set_migration_version(tx: &Transaction, v: usize) -> rusqlite::Result<usize> {
    tx.execute(
        "INSERT OR REPLACE INTO __corro_state VALUES (?, ?)",
        params![SCHEMA_VERSION_KEY, v],
    )
}

// should be a noop if up to date!
pub fn migrate(conn: &mut Connection, migrations: Vec<Box<dyn Migration>>) -> rusqlite::Result<()> {
    let target_version = migrations.len();

    let tx = conn.transaction()?;

    // determine how many migrations to skip (skip as many as we are at)
    let skip_n = migration_version(&tx).unwrap_or_default();

    if skip_n > migrations.len() {
        warn!("Skipping migrations, database is at migration version {skip_n} which is greater than {}", migrations.len());
        return Ok(());
    }

    for (i, migration) in migrations.into_iter().skip(skip_n).enumerate() {
        let new_version = skip_n + i;
        info!("Applying migration to v{new_version}");
        let start = Instant::now();
        migration.migrate(&tx)?;
        info!("Applied v{new_version} in {:?}", start.elapsed());
    }
    set_migration_version(&tx, target_version)?;

    tx.commit()?;
    Ok(())
}

// Converts any iterator over something convertible to SQL
// into a vector of SQL values, which can be used as a parameter
// to the `unnest` function in SQL.
// Due to limitations in rusqlite we need to use owned values
pub fn unnest_param<T, K>(iter: T) -> Rc<Vec<Value>>
where
    T: IntoIterator<Item = K>,
    K: ToSql,
{
    Rc::new(
        iter.into_iter()
            .map(|to_sql| match to_sql.to_sql() {
                Ok(ToSqlOutput::Borrowed(v)) => v.into(),
                Ok(ToSqlOutput::Owned(v)) => v,
                _ => panic!("Nope"),
            })
            .collect::<Vec<Value>>(),
    )
}

#[cfg(test)]
mod tests {
    use futures::{stream::FuturesUnordered, TryStreamExt};
    use sqlite_pool::Config;
    use sqlite_pool::InterruptibleTransaction;
    use tokio::task::block_in_place;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::TempDir::new()?;

        let pool = sqlite_pool::Config::new(tmpdir.path().join("test.db"))
            .max_size(1)
            .create_pool_transform(rusqlite_to_crsqlite)?;

        {
            let conn = pool.get().await?;

            conn.execute_batch(
                "
                CREATE TABLE foo (a INTEGER NOT NULL PRIMARY KEY, b INTEGER);
                SELECT crsql_as_crr('foo');
            ",
            )?;
        }

        let total: i64 = 1000;
        let per_worker: i64 = 5;

        let futs = FuturesUnordered::from_iter((0..total).map(|_| {
            let pool = pool.clone();
            async move {
                tokio::spawn(async move {
                    let _: () = FuturesUnordered::from_iter((0..per_worker).map(|_| {
                        let pool = pool.clone();
                        async move {
                            let conn = pool.get().await?;
                            block_in_place(|| {
                                conn.prepare_cached(
                                    "INSERT INTO foo (a, b) VALUES (random(), random())",
                                )?
                                .execute(())?;
                                Ok::<_, TestError>(())
                            })?;
                            Ok::<_, TestError>(())
                        }
                    }))
                    .try_collect()
                    .await?;
                    Ok::<_, TestError>(())
                })
                .await??;
                Ok::<_, TestError>(())
            }
        }));

        let _: () = futs.try_collect().await?;

        let conn = pool.get().await?;

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM foo;", (), |row| row.get(0))?;

        assert_eq!(count, total * per_worker);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_interruptible_transaction() -> Result<(), Box<dyn std::error::Error>> {
        let tmpdir = tempfile::tempdir()?;

        let path = tmpdir.path().join("db.sqlite");
        let pool = Config::new(path)
            .max_size(1)
            .create_pool_transform(rusqlite_to_crsqlite)?;

        let mut conn = pool.get().await.unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS testsbool (
            id INTEGER NOT NULL PRIMARY KEY,
            b boolean not null default false
        ); SELECT crsql_as_crr('testsbool')",
        )?;

        {
            let tx = conn.transaction()?;
            let timeout = Some(tokio::time::Duration::from_millis(5));
            let itx = InterruptibleTransaction::new(tx, timeout, "test_interruptible_transaction");
            let res = itx.execute("INSERT INTO testsbool (id) WITH RECURSIVE    cte(id) AS (       SELECT random()       UNION ALL       SELECT random()         FROM cte        LIMIT 100000000  ) SELECT id FROM cte;", &[]);

            assert!(res.is_err_and(
                |e| e.sqlite_error_code() == Some(rusqlite::ErrorCode::OperationInterrupted)
            ));
        }

        let count: i64 = conn.query_row("SELECT COUNT(*) FROM testsbool;", (), |row| row.get(0))?;
        assert_eq!(count, 0);
        Ok(())
    }

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Rusqlite(#[from] rusqlite::Error),
        #[error(transparent)]
        Bb8Rusqlite(#[from] SqlitePoolError),
        #[error(transparent)]
        Join(#[from] tokio::task::JoinError),
    }
}
