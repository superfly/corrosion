use std::collections::HashMap;
use std::time::Duration;

use corro_types::{
    agent::{Agent, PoolError, SplitPool},
    broadcast::Timestamp,
    sqlite::{unnest_param, SqlitePoolError},
};

use eyre::eyre;
use metrics::counter;
use rusqlite::params;
use tokio::{task::block_in_place, time::interval};
use tracing::{debug, error, info, trace};

/// Parse a time string like "14d", "1m", "2h", "30s" into a Duration
fn parse_duration(s: &str) -> eyre::Result<Duration> {
    if s.is_empty() {
        return Err(eyre!("empty duration string"));
    }

    let (num_str, unit) = s.split_at(s.len() - 1);
    let num: u64 = num_str
        .parse()
        .map_err(|_| eyre!("invalid number in duration: {num_str}"))?;

    let duration = match unit {
        "s" => Duration::from_secs(num),
        "m" => Duration::from_secs(num * 60),
        "h" => Duration::from_secs(num * 60 * 60),
        "d" => Duration::from_secs(num * 60 * 60 * 24),
        "w" => Duration::from_secs(num * 60 * 60 * 24 * 7),
        "y" => Duration::from_secs(num * 60 * 60 * 24 * 365),
        _ => return Err(eyre!("unknown duration unit: {unit}")),
    };

    Ok(duration)
}

/// Spawn the reaper background task
pub fn spawn_reaper(agent: &Agent) -> eyre::Result<()> {
    let config = agent.config().reaper.clone();

    if let Some(config) = config {
        if config.tables.is_empty() {
            return Ok(());
        }

        let tables = config
            .tables
            .into_iter()
            .map(|(table_name, retention)| {
                parse_duration(&retention).map(|duration| (table_name, duration))
            })
            .collect::<Result<HashMap<String, Duration>, eyre::Error>>()?;

        let agent = agent.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(config.check_interval as u64));
            // skip first tikc
            interval.tick().await;
            let clock = agent.clock();
            loop {
                interval.tick().await;

                for (table_name, retention) in tables.iter() {
                    match reap_table(&agent.pool(), table_name, *retention, &clock).await {
                        Ok((clocks, pks)) => {
                            if clocks > 0 {
                                info!(%table_name, clocks = clocks, "deleted clocks");
                                counter!("corro.agent.deleted.clocks", "table" => table_name.to_string())
                                    .increment(clocks as u64);
                            }
                            if pks > 0 {
                                info!(%table_name, pks = pks, "deleted pks");
                                counter!("corro.agent.deleted.pks", "table" => table_name.to_string())
                                    .increment(pks as u64);
                            }
                        }
                        Err(e) => {
                            error!(%table_name, error = %e, "error during reaping");
                        }
                    }

                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        });
    };

    Ok(())
}

async fn reap_table(
    pool: &SplitPool,
    table: &str,
    retention: Duration,
    clock: &uhlc::HLC,
) -> Result<(usize, usize), ReaperError> {
    let read_conn = pool.read().await.map_err(ReaperError::ReadPool)?;

    let now = Timestamp::from(clock.new_timestamp()).to_ntp64();
    let cutoff = Timestamp::from(now - uhlc::NTP64::from(retention));

    trace!("checking table {table} for deleted clocks older than {cutoff}");
    let (pks_to_delete, mut orphaned_pks): (Vec<u64>, Vec<u64>) = block_in_place(|| {
        let sentinel_pks = read_conn
            .prepare_cached(&format!(
                "SELECT key FROM {}__crsql_clock 
                WHERE col_name = -1 AND col_version % 2 = 0
                AND ts < ?  LIMIT 100",
                table
            ))?
            .query_map([&cutoff], |row| row.get::<_, u64>(0))?
            .collect::<Result<Vec<u64>, rusqlite::Error>>()?;

        let orphaned_pks = read_conn
            .prepare_cached(&format!(
                "SELECT __crsql_key FROM {table}__crsql_pks
                WHERE NOT EXISTS 
                    (SELECT 1 FROM {table}__crsql_clock WHERE __crsql_key = key) 
                LIMIT 100",
                table = table
            ))?
            .query_map([], |row| row.get::<_, u64>(0))?
            .collect::<Result<Vec<u64>, rusqlite::Error>>()?;

        Ok::<_, rusqlite::Error>((sentinel_pks, orphaned_pks))
    })?;

    if pks_to_delete.is_empty() && orphaned_pks.is_empty() {
        trace!("no pks to delete or orphaned pks found in table {table}");
        return Ok((0, 0));
    }

    debug!(
        "found {} pks to delete and {} orphaned pks in table {table}",
        pks_to_delete.len(),
        orphaned_pks.len()
    );

    // Delete the PKs
    let mut write_conn = pool
        .write_low()
        .await
        .map_err(|e| ReaperError::WritePool(e))?;

    let (clocks, pks) = block_in_place(|| {
        let tx = write_conn.immediate_transaction()?;

        let deleted_clocks = tx
            .prepare_cached(&format!(
                "DELETE FROM {}__crsql_clock WHERE key IN (SELECT value0 FROM unnest(?))",
                table
            ))?
            .execute(params![unnest_param(pks_to_delete.clone())])?;

        orphaned_pks.extend_from_slice(&pks_to_delete);
        let deleted_pks = tx
            .prepare_cached(&format!(
                "DELETE FROM {}__crsql_pks WHERE __crsql_key IN (SELECT value0 FROM unnest(?))",
                table
            ))?
            .execute(params![unnest_param(orphaned_pks)])?;

        tx.commit()?;
        Ok::<_, rusqlite::Error>((deleted_clocks, deleted_pks))
    })
    .map_err(ReaperError::Sqlite)?;

    Ok((clocks, pks))
}

#[derive(Debug, thiserror::Error)]
pub enum ReaperError {
    #[error("read pool error: {0}")]
    ReadPool(#[from] SqlitePoolError),
    #[error("write pool error: {0}")]
    WritePool(#[from] PoolError),
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use corro_types::{agent::SplitPool, broadcast::Timestamp, sqlite::setup_conn};
    use std::sync::Arc;
    use tempfile::tempdir;
    use tokio::sync::Semaphore;
    use uhlc::HLCBuilder;

    async fn setup_test_db(path: std::path::PathBuf) -> eyre::Result<SplitPool> {
        let write_sema = Arc::new(Semaphore::new(1));
        let pool = SplitPool::create(path, write_sema).await?;

        // Create test table
        let conn = pool.write_priority().await?;
        block_in_place(|| {
            setup_conn(&conn)?;
            conn.execute_batch(
                r#"
                CREATE TABLE tests (
                    id INTEGER NOT NULL PRIMARY KEY,
                    text TEXT NOT NULL DEFAULT ""
                ) WITHOUT ROWID;
                SELECT crsql_as_crr('tests');
                "#,
            )?;
            Ok::<_, rusqlite::Error>(())
        })
        .map_err(|e: rusqlite::Error| eyre::eyre!("{e}"))?;

        Ok(pool)
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("14d").unwrap(), Duration::from_secs(1209600));
        assert_eq!(parse_duration("1w").unwrap(), Duration::from_secs(604800));
        assert_eq!(parse_duration("1y").unwrap(), Duration::from_secs(31536000));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_reaper_deletes_old_sentinel_rows() -> eyre::Result<()> {
        let tmpdir: tempfile::TempDir = tempdir()?;
        let db_path = tmpdir.path().join("test.db");
        _ = tracing_subscriber::fmt::try_init();
        let pool = setup_test_db(db_path).await?;
        let clock = HLCBuilder::default().build();

        let table = "tests";

        let now_ts = Timestamp::from(clock.new_timestamp());
        let old_ts =
            Timestamp::from(now_ts.to_ntp64() - uhlc::NTP64::from(parse_duration("1w").unwrap()));

        // Insert rows with old timestamp and delete some
        let recently_deleted: [u64; 2] = [4, 5];
        let deleted: [u64; 2] = [1, 2];
        {
            let mut conn = pool.write_priority().await?;
            block_in_place(|| {
                let tx = conn.immediate_transaction()?;

                tx.prepare_cached("SELECT crsql_set_ts(?)")?
                    .query_row([&old_ts], |_| Ok(()))?;

                // Insert rows
                tx.execute("INSERT INTO tests (id, text) VALUES (1, 'test1')", [])?;
                tx.execute("INSERT INTO tests (id, text) VALUES (2, 'test2')", [])?;
                tx.execute("INSERT INTO tests (id, text) VALUES (3, 'test3')", [])?;
                tx.execute("INSERT INTO tests (id, text) VALUES (4, 'test4')", [])?;
                tx.execute("INSERT INTO tests (id, text) VALUES (5, 'test5')", [])?;

                tx.commit()?;

                let tx = conn.immediate_transaction()?;
                tx.prepare_cached("SELECT crsql_set_ts(?)")?
                    .query_row([&old_ts], |_| Ok(()))?;

                tx.execute("DELETE FROM tests WHERE id IN (?, ?)", deleted)?;

                tx.commit()?;
                Ok::<_, rusqlite::Error>(())
            })?;
        }

        // Delete some rows with a more recent timestamp
        {
            let mut conn = pool.write_priority().await?;
            block_in_place(|| {
                let tx = conn.immediate_transaction()?;
                tx.prepare_cached("SELECT crsql_set_ts(?)")?
                    .query_row([&now_ts], |_| Ok(()))?;
                tx.execute("DELETE FROM tests WHERE id IN (?, ?)", recently_deleted)?;
                tx.commit()?;
                Ok::<_, rusqlite::Error>(())
            })?;
        }

        let (clocks, pks) = reap_table(&pool, table, parse_duration("1w").unwrap(), &clock).await?;
        assert_eq!(clocks, 2, "should delete 2 old clock entries");
        assert_eq!(pks, 2, "should delete 2 old PKs");

        let (clocks, pks) = reap_table(&pool, table, parse_duration("2d").unwrap(), &clock).await?;
        assert_eq!(clocks, 0, "no more entries to delete");
        assert_eq!(pks, 0, "no more entries to delete");

        let read_conn = pool.read().await?;
        let old_sentinel_count: i64 = read_conn.query_row(
            "SELECT COUNT(*) FROM tests__crsql_clock WHERE key IN (?, ?)",
            deleted,
            |row| row.get(0),
        )?;
        assert_eq!(
            old_sentinel_count, 0,
            "old sentinel entries should be deleted"
        );

        let old_pk_count: i64 = read_conn.query_row(
            "SELECT COUNT(*) FROM tests__crsql_pks WHERE __crsql_key IN (?, ?)",
            deleted,
            |row| row.get(0),
        )?;
        assert_eq!(old_pk_count, 0, "old PKs should be deleted");

        let recent: i64 = read_conn.query_row(
            "SELECT COUNT(*) FROM tests__crsql_clock WHERE key IN (?, ?)",
            recently_deleted,
            |row| row.get(0),
        )?;
        assert_eq!(recent, 2, "recent sentinel entries should still exist");

        Ok(())
    }
}
