use crate::transport::Transport;
use antithesis_sdk::assert_always;
use corro_types::agent::Agent;
use metrics::gauge;
use serde_json::json;
use std::time::Duration;
use tokio::task::block_in_place;
use tracing::error;
use tripwire::Tripwire;

pub async fn metrics_loop(agent: Agent, transport: Transport, mut tripwire: Tripwire) {
    let mut metrics_interval = tokio::time::interval(Duration::from_secs(60));
    let on_antithesis = std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok();

    loop {
        tokio::select! {
            _ = metrics_interval.tick() => {},
            _ = &mut tripwire => break,
        }

        block_in_place(|| collect_metrics(&agent, &transport, on_antithesis));
    }
}

pub fn collect_metrics(agent: &Agent, transport: &Transport, on_antithesis: bool) {
    agent.pool().emit_metrics();
    transport.emit_metrics();

    let schema = agent.schema().read();

    let conn = match agent.pool().read_blocking() {
        Ok(conn) => conn,
        Err(e) => {
            error!("could not acquire read connection for metrics purposes: {e}");
            return;
        }
    };

    // let mut low_count_tables = vec![];

    for table in schema.tables.keys() {
        match conn
            .prepare_cached(&format!("SELECT count(*) FROM {table}"))
            .and_then(|mut prepped| prepped.query_row([], |row| row.get::<_, i64>(0)))
        {
            Ok(count) => {
                gauge!("corro.db.table.rows.total", "table" => table.clone()).set(count as f64);
            }
            Err(e) => {
                error!("could not query count for table {table}: {e}");
                continue;
            }
        }
    }

    // Buffered changes stats across all actors
    match conn
        .prepare_cached(
            "
            SELECT
                COALESCE(json_extract(m.foca_state, '$.state'), 'orphan') AS peer_state,
                count(*) AS total,
                strftime('%s', 'now') - (min(bc.ts) >> 32) as oldest_age_seconds
            FROM __corro_buffered_changes bc
            LEFT JOIN __corro_members m ON m.actor_id = bc.site_id
            GROUP BY peer_state
            ",
        )
        .and_then(|mut prepped| {
            prepped
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .and_then(|mapped| mapped.collect::<Result<Vec<_>, _>>())
        }) {
        Ok(rows) => {
            for (peer_state, total_count, oldest_age_seconds) in rows {
                // v2 has O(N) time series while the v1 metric had O(N^2) time series
                // The name got changed on purpose so the metric has a fresh namespace
                gauge!("corro.db.buffered.changes.v2.total", "peer_state" => peer_state.clone())
                    .set(total_count as f64);
                gauge!("corro.db.buffered.changes.v2.oldest_age_seconds", "peer_state" => peer_state)
                    .set(oldest_age_seconds as f64);
            }
        }
        Err(e) => {
            error!("could not query buffered changes v2 stats: {e}");
        }
    }

    match conn
        .prepare_cached(
            "
            SELECT
                COALESCE(json_extract(m.foca_state, '$.state'), 'orphan') AS peer_state,
                COALESCE(SUM((g.end - g.start) + 1), 0) AS gaps_sum
            FROM __corro_bookkeeping_gaps g
            LEFT JOIN __corro_members m ON m.actor_id = g.actor_id
            GROUP BY peer_state
            ",
        )
        .and_then(|mut prepped| {
            prepped
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })
                .and_then(|mapped| mapped.collect::<Result<Vec<_>, _>>())
        }) {
        Ok(rows) => {
            for (peer_state, sum) in rows {
                gauge!("corro.db.gaps.sum", "peer_state" => peer_state).set(sum as f64)
            }
        }
        Err(e) => {
            error!("could not query sum for gaps: {e}");
        }
    }

    match conn.pragma_query_value(None, "freelist_count", |row| row.get::<_, u64>(0)) {
        Ok(n) => {
            gauge!("corro.db.freelist.count").set(n as f64);
        }
        Err(e) => {
            error!("could not query freelist_count in db: {e}");
        }
    }

    let mut db_path = agent.config().db.path.clone();

    if let Ok(meta) = db_path.metadata() {
        gauge!("corro.db.size").set(meta.len() as f64);
    }

    if db_path.set_extension(format!("{}-wal", db_path.extension().unwrap_or_default())) {
        if let Ok(meta) = db_path.metadata() {
            gauge!("corro.db.wal.size").set(meta.len() as f64);
        }
    }

    if on_antithesis {
        let gaps = conn
            .prepare_cached(
                "SELECT COALESCE(SUM(end - start + 1), 0) FROM __corro_bookkeeping_gaps",
            )
            .and_then(|mut prepped| prepped.query_row([], |row| row.get::<_, i64>(0)));

        if let Ok(gaps) = gaps {
            const MAX_GAPS: i64 = 100;
            const MAX_QUEUE: u64 = 16_000;
            const MAX_P99_LAG: f64 = 3.0;

            let p99_lag = agent.metrics_tracker().quantile_lag(0.99).unwrap_or(0.0);
            let queue_size = agent.metrics_tracker().queue_size();
            let is_healthy = gaps <= MAX_GAPS && queue_size <= MAX_QUEUE && p99_lag <= MAX_P99_LAG;
            let details = json!({
                "gaps": gaps,
                "queue_size": queue_size,
                "p99_lag": p99_lag,
                "max_gaps": MAX_GAPS,
                "max_queue": MAX_QUEUE,
                "max_p99_lag": MAX_P99_LAG,
            });

            assert_always!(
                is_healthy,
                "Corrosion node remains healthy in antithesis",
                &details
            );
        } else if let Err(e) = gaps {
            error!("could not query gaps for antithesis health assertion: {e}");
        }
    }
}
