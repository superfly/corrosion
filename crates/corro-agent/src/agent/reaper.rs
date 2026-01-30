use std::collections::HashMap;
use std::time::Duration;

use corro_types::{
    agent::{Agent, PoolError},
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
                    match reap_table(&agent, table_name, *retention, &clock).await {
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
    agent: &Agent,
    table: &str,
    retention: Duration,
    clock: &uhlc::HLC,
) -> Result<(usize, usize), ReaperError> {
    let read_conn = agent.pool().read().await.map_err(ReaperError::ReadPool)?;

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
    let mut write_conn = agent
        .pool()
        .write_low()
        .await
        .map_err(|e| ReaperError::WritePool(e))?;

    let (clocks, pks) = block_in_place(|| {
        let tx = write_conn.immediate_transaction()?;

        let deleted_clocks = tx
            .prepare_cached(&format!(
                "DELETE FROM {}__crsql_clock WHERE key IN (SELECT table0 FROM unnest(?))",
                table
            ))?
            .execute(params![unnest_param(pks_to_delete.clone())])?;

        orphaned_pks.extend_from_slice(&pks_to_delete);
        let deleted_pks = tx
            .prepare_cached(&format!(
                "DELETE FROM {}__crsql_pks WHERE __crsql_key IN (SELECT table0 FROM unnest(?))",
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

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("14d").unwrap(), Duration::from_secs(1209600));
        assert_eq!(parse_duration("1w").unwrap(), Duration::from_secs(604800));
        assert_eq!(parse_duration("1y").unwrap(), Duration::from_secs(31536000));
    }
}
