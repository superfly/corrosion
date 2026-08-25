use std::time::{Duration, Instant};

use corro_types::{
    agent::{Agent, PoolError},
    broadcast::Timestamp,
    sqlite::SqlitePoolError,
};

use metrics::{counter, gauge, histogram};
use tokio::{task::block_in_place, time::interval};
use tracing::{error, info, trace, warn};
use tripwire::{Outcome, PreemptibleFutureExt, TimeoutFutureExt, Tripwire};

/// Chunk size for each incremental maintenance call.
/// Controls how many rows are migrated/cleaned per tick.
const DEFAULT_CHUNK_SIZE: i64 = 100_000;

/// How often the migrator checks for pending work.
/// Kept deliberately high to let corrosion catch up on replication
/// between batches — migration writes compete with live change processing
/// for the write pool.
const DEFAULT_CHECK_INTERVAL_SECS: u64 = 15;

/// Spawn the migrator background task.
///
/// The migrator periodically calls `crsql_incremental_maintenance` to
/// process pending V1→V2 migration tasks and V1/V2 table cleanup tasks.
/// It is a no-op when there are no pending tasks (returns 0 immediately),
/// so it is safe to always spawn.
///
/// This mirrors the reaper pattern: a background task on a timer that
/// does bounded work per tick and is tripwire-aware.
pub fn spawn_migrator(agent: &Agent, mut tripwire: Tripwire) {
    info!("spawning crsqlite metadata migrator");

    let chunk_size = DEFAULT_CHUNK_SIZE;
    let check_interval = DEFAULT_CHECK_INTERVAL_SECS;
    let check_timeout = Duration::from_secs(120);
    let agent = agent.clone();

    tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(check_interval));
        // skip first tick so we don't run immediately on startup
        interval.tick().await;
        let clock = agent.clock();
        loop {
            tokio::select! {
                biased;
                _ = &mut tripwire => {
                    break;
                }
                _ = interval.tick() => {
                }
            }

            let start = Instant::now();
            let result = match run_maintenance_step(&agent, chunk_size, clock)
                .with_timeout(check_timeout)
                .preemptible(&mut tripwire)
                .await
            {
                Outcome::Preempted(()) => return,
                Outcome::Completed(Outcome::Preempted(())) => {
                    warn!("migrator timed out during maintenance step");
                    Err(MigratorError::Timeout)
                }
                Outcome::Completed(Outcome::Completed(res)) => res,
            };

            match result {
                Ok(remaining) => {
                    let elapsed = start.elapsed();
                    gauge!("corro.migrator.remaining").set(remaining as f64);
                    if remaining > 0 {
                        info!(
                            remaining,
                            chunk_size,
                            elapsed = ?elapsed,
                            "crsqlite metadata migration step: {remaining} units of work remaining"
                        );
                    } else {
                        trace!(
                            elapsed = ?elapsed,
                            "crsqlite metadata migration step: no pending work"
                        );
                    }
                    counter!("corro.migrator.steps").increment(1);
                    histogram!("corro.migrator.step.seconds").record(elapsed.as_secs_f64());

                    // Emit detailed progress metrics from crsql_master markers
                    // so they can be graphed in Grafana.
                    if let Err(e) = emit_progress_metrics(&agent).await {
                        warn!("could not emit migrator progress metrics: {e}");
                    }
                }
                Err(e) => {
                    error!("error during crsqlite metadata migration step: {e}");
                    counter!("corro.migrator.errors").increment(1);
                }
            }
        }
        info!("crsqlite metadata migrator stopped");
    });
}

/// Run a single incremental maintenance step.
///
/// Calls `crsql_incremental_maintenance(?)` which dispatches to all
/// pending migration and cleanup tasks, doing up to `chunk_size` units
/// of work. Returns the number of remaining units (0 = all done).
async fn run_maintenance_step(
    agent: &Agent,
    chunk_size: i64,
    clock: &uhlc::HLC,
) -> Result<i64, MigratorError> {
    let mut conn = agent.pool().write_low().await.map_err(MigratorError::WritePool)?;

    let ts = Timestamp::from(clock.new_timestamp());

    block_in_place(|| {
        let tx = conn.immediate_transaction()?;

        // V2 clock tables require a non-zero ts before any write.
        // Set it unconditionally — it's cheap and required by
        // crsql_incremental_maintenance when V2 tables are involved.
        tx.prepare_cached("SELECT crsql_set_ts(?)")?
            .query_row([&ts], |_| Ok(()))?;

        let remaining: i64 = tx
            .prepare_cached("SELECT crsql_incremental_maintenance(?)")?
            .query_row([chunk_size], |row| row.get(0))?;

        tx.commit()?;

        Ok::<_, MigratorError>(remaining)
    })
}

/// Emit detailed progress metrics from crsql_master markers.
///
/// This reads the internal progress markers that crsqlite's migration
/// machinery writes to `crsql_master` and exposes them as gauges so
/// they can be graphed in Grafana.
///
/// Metrics emitted:
/// - `corro.migrator.metadata_write_version` — current write version (1/2/3)
/// - `corro.migrator.metadata_use_version` — current use version (1/2)
/// - `corro.migrator.sync_log_version` — current sync log version (1/2)
/// - `corro.migrator.migration.pending_tables` — count of tables with pending V1→V2 migration
/// - `corro.migrator.cleanup.v1_pending_tables` — count of tables with pending V1 cleanup
/// - `corro.migrator.cleanup.v2_pending_tables` — count of tables with pending V2 cleanup
/// - `corro.migrator.migration.table_total{table}` — total rows to migrate per table
/// - `corro.migrator.migration.table_done{table}` — rows migrated so far per table
/// - `corro.migrator.migration.table_remaining{table}` — estimated remaining rows per table
async fn emit_progress_metrics(agent: &Agent) -> Result<(), MigratorError> {
    let conn = agent.pool().read().await.map_err(MigratorError::ReadPool)?;

    block_in_place(|| {
        // Metadata version gauges
        let write_version: i64 = conn
            .prepare_cached("SELECT crsql_config_get('metadata-write-version')")?
            .query_row([], |row| row.get(0))?;
        let use_version: i64 = conn
            .prepare_cached("SELECT crsql_config_get('metadata-use-version')")?
            .query_row([], |row| row.get(0))?;
        let sync_log_version: i64 = conn
            .prepare_cached("SELECT crsql_config_get('sync-log-version')")?
            .query_row([], |row| row.get(0))?;

        gauge!("corro.migrator.metadata_write_version").set(write_version as f64);
        gauge!("corro.migrator.metadata_use_version").set(use_version as f64);
        gauge!("corro.migrator.sync_log_version").set(sync_log_version as f64);

        // Pending migration markers count
        let migration_pending: i64 = conn
            .prepare_cached(
                "SELECT count(*) FROM crsql_master \
                 WHERE key LIKE 'migration_v1_to_v2_migration_%'",
            )?
            .query_row([], |row| row.get(0))?;
        gauge!("corro.migrator.migration.pending_tables").set(migration_pending as f64);

        // Pending V1 cleanup markers count
        let v1_cleanup_pending: i64 = conn
            .prepare_cached(
                "SELECT count(*) FROM crsql_master \
                 WHERE key LIKE 'cleanup_v1_tables_%'",
            )?
            .query_row([], |row| row.get(0))?;
        gauge!("corro.migrator.cleanup.v1_pending_tables").set(v1_cleanup_pending as f64);

        // Pending V2 cleanup markers count
        let v2_cleanup_pending: i64 = conn
            .prepare_cached(
                "SELECT count(*) FROM crsql_master \
                 WHERE key LIKE 'cleanup_v2_tables_%'",
            )?
            .query_row([], |row| row.get(0))?;
        gauge!("corro.migrator.cleanup.v2_pending_tables").set(v2_cleanup_pending as f64);

        // Per-table migration progress: total, done, remaining
        // Markers: migration_v1_to_v2_total_<table> = total rows
        //          migration_v1_to_v2_done_<table>  = cumulative done
        let mut total_stmt = conn.prepare(
            "SELECT key, value FROM crsql_master \
             WHERE key LIKE 'migration_v1_to_v2_total_%'",
        )?;
        let total_rows: Vec<(String, i64)> = total_stmt
            .query_map([], |row| {
                let key: String = row.get(0)?;
                let val: i64 = row.get(1)?;
                Ok((key, val))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        let mut done_stmt = conn.prepare(
            "SELECT key, value FROM crsql_master \
             WHERE key LIKE 'migration_v1_to_v2_done_%'",
        )?;
        let done_rows: Vec<(String, i64)> = done_stmt
            .query_map([], |row| {
                let key: String = row.get(0)?;
                let val: i64 = row.get(1)?;
                Ok((key, val))
            })?
            .collect::<Result<Vec<_>, _>>()?;

        // Build a map of table -> done count
        let done_map: std::collections::HashMap<String, i64> = done_rows
            .into_iter()
            .filter_map(|(key, val)| {
                key.strip_prefix("migration_v1_to_v2_done_")
                    .map(|table| (table.to_string(), val))
            })
            .collect();

        for (key, total) in &total_rows {
            if let Some(table) = key.strip_prefix("migration_v1_to_v2_total_") {
                let done = done_map.get(table).copied().unwrap_or(0);
                let remaining = total.saturating_sub(done);
                gauge!("corro.migrator.migration.table_total", "table" => table.to_string())
                    .set(*total as f64);
                gauge!("corro.migrator.migration.table_done", "table" => table.to_string())
                    .set(done as f64);
                gauge!("corro.migrator.migration.table_remaining", "table" => table.to_string())
                    .set(remaining as f64);
            }
        }

        Ok::<_, rusqlite::Error>(())
    })
    .map_err(MigratorError::Sqlite)
}

/// Check if there are any pending migration or cleanup tasks.
///
/// Returns true if `crsql_incremental_maintenance` would do work
/// (i.e. there are pending markers in crsql_master).
#[allow(dead_code)]
pub async fn has_pending_maintenance(agent: &Agent) -> Result<bool, MigratorError> {
    let conn = agent.pool().read().await.map_err(MigratorError::ReadPool)?;

    block_in_place(|| {
        let count: i64 = conn
            .prepare_cached(
                "SELECT count(*) FROM crsql_master \
                 WHERE key LIKE 'migration_v1_to_v2_migration_%' \
                    OR key LIKE 'cleanup_v1_tables_%' \
                    OR key LIKE 'cleanup_v2_tables_%'",
            )?
            .query_row([], |row| row.get(0))?;
        Ok(count > 0)
    })
}

/// Get the current metadata version configuration from the database.
///
/// Returns (metadata_write_version, metadata_use_version, sync_log_version).
/// These are read from crsql_master config keys, falling back to ext_data
/// via the `crsql_config_get` function.
#[allow(dead_code)]
pub async fn get_metadata_versions(
    agent: &Agent,
) -> Result<(i64, i64, i64), MigratorError> {
    let conn = agent.pool().read().await.map_err(MigratorError::ReadPool)?;

    block_in_place(|| {
        let write_version: i64 = conn
            .prepare_cached("SELECT crsql_config_get('metadata-write-version')")?
            .query_row([], |row| row.get(0))?;
        let use_version: i64 = conn
            .prepare_cached("SELECT crsql_config_get('metadata-use-version')")?
            .query_row([], |row| row.get(0))?;
        let sync_log_version: i64 = conn
            .prepare_cached("SELECT crsql_config_get('sync-log-version')")?
            .query_row([], |row| row.get(0))?;
        Ok((write_version, use_version, sync_log_version))
    })
}

#[derive(Debug, thiserror::Error)]
pub enum MigratorError {
    #[error("migrator timed out")]
    Timeout,
    #[error("read pool error: {0}")]
    ReadPool(#[from] SqlitePoolError),
    #[error("write pool error: {0}")]
    WritePool(#[from] PoolError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}
