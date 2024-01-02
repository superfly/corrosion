use crate::transport::Transport;
use corro_types::{actor::ActorId, agent::Agent};
use metrics::gauge;
use std::time::Duration;
use tokio::task::block_in_place;
use tracing::error;

pub async fn metrics_loop(agent: Agent, transport: Transport) {
    let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        metrics_interval.tick().await;

        block_in_place(|| collect_metrics(&agent, &transport));
    }
}

pub fn collect_metrics(agent: &Agent, transport: &Transport) {
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
                gauge!("corro.db.table.rows.total", count as f64, "table" => table.clone());
            }
            Err(e) => {
                error!("could not query count for table {table}: {e}");
                continue;
            }
        }
    }

    // TODO: collect from bookie?
    match conn
        .prepare_cached("SELECT actor_id, (select count(site_id) FROM __corro_buffered_changes WHERE site_id = actor_id) FROM __corro_members")
        .and_then(|mut prepped| {
            prepped
                .query_map((), |row| {
                    Ok((row.get::<_, ActorId>(0)?, row.get::<_, i64>(1)?))
                })
                .and_then(|mapped| mapped.collect::<Result<Vec<_>, _>>())
        }) {
        Ok(mapped) => {
            for (actor_id, count) in mapped {
                gauge!("corro.db.buffered.changes.rows.total", count as f64, "actor_id" => actor_id.to_string())
            }
        }
        Err(e) => {
            error!("could not query count for buffered changes: {e}");
        }
    }
}
