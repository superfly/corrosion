use crate::transport::Transport;
use corro_types::{actor::ActorId, agent::Agent, persistent_gauge};
use metrics::gauge;
use std::time::Duration;
use tokio::task::block_in_place;
use tracing::error;
use tripwire::Tripwire;

pub async fn light_metrics_loop(agent: Agent, transport: Transport, mut tripwire: Tripwire) {
    let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

    loop {
        tokio::select! {
            _ = metrics_interval.tick() => {},
            _ = &mut tripwire => break,
        }

        block_in_place(|| collect_light_metrics(&agent, &transport));
    }
}

pub async fn heavy_metrics_loop(agent: Agent, mut tripwire: Tripwire) {
    let mut metrics_interval = tokio::time::interval(Duration::from_secs(15 * 60));

    loop {
        tokio::select! {
            _ = metrics_interval.tick() => {},
            _ = &mut tripwire => break,
        }

        block_in_place(|| collect_heavy_metrics(&agent));
    }
}

pub fn collect_light_metrics(agent: &Agent, transport: &Transport) {
    agent.pool().emit_metrics();
    transport.emit_metrics();

    let schema = agent.schema().read();

    let conn = match agent.pool().read_blocking() {
        Ok(conn) => conn,
        Err(e) => {
            error!("could not acquire read connection for light metrics purposes: {e}");
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
                gauge!("corro.db.buffered.changes.rows.total", "actor_id" => actor_id.to_string()).set(count as f64)
            }
        }
        Err(e) => {
            error!("could not query count for buffered changes: {e}");
        }
    }

    match conn
        .prepare_cached("select actor_id, sum((end - start) + 1) from __corro_bookkeeping_gaps group by actor_id")
        .and_then(|mut prepped| {
            prepped
                .query_map((), |row| {
                    Ok((row.get::<_, ActorId>(0)?, row.get::<_, u64>(1)?))
                })
                .and_then(|mapped| mapped.collect::<Result<Vec<_>, _>>())
        }) {
        Ok(mapped) => {
            for (actor_id, sum) in mapped {
                gauge!("corro.db.gaps.sum", "actor_id" => actor_id.to_string()).set(sum as f64)
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
}

pub fn collect_heavy_metrics(agent: &Agent) {
    let schema = agent.schema().read();

    let conn = match agent.pool().read_blocking() {
        Ok(conn) => conn,
        Err(e) => {
            error!("could not acquire read connection for heavy metrics purposes: {e}");
            return;
        }
    };

    // Determine real btree size and crsqlite internal bookkeeping size
    // This query should complete within 30 seconds
    let mut stmt = conn
        .prepare_cached("SELECT name, pgsize FROM dbstat WHERE aggregate = true;")
        .unwrap();
    let table_sizes = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<Result<Vec<(String, i64)>, _>>()
        .unwrap();

    for table in schema.tables.keys() {
        // The size of the table btrees + any indexes it has without the corrosion bookkeeping tables
        let real_usage = table_sizes
            .iter()
            .filter(|(name, _)| name.starts_with(table) && !name.contains("__crsql_"))
            .map(|(_, size)| size)
            .sum::<i64>();
        // The size of the corrosion bookkeeping tables including the size of indexes crsqlite uses
        let bookkeeping_usage = table_sizes
            .iter()
            .filter(|(name, _)| name.starts_with(table) && name.contains("__crsql_"))
            .map(|(_, size)| size)
            .sum::<i64>();

        // Persistent gauges as this is rarely updated as the queries are expensive
        persistent_gauge!("corro.db.table.btree.bytes", "table" => table.clone())
            .set(real_usage as f64);
        persistent_gauge!("corro.db.table.bookkeeping.btree.bytes", "table" => table.clone())
            .set(bookkeeping_usage as f64);

        // Pks and Clock counts
        match conn
            .prepare_cached(&format!("SELECT count(*) FROM {table}__crsql_pks"))
            .and_then(|mut prepped| prepped.query_row([], |row| row.get::<_, i64>(0)))
        {
            Ok(count) => {
                persistent_gauge!("corro.db.table.bookkeeping.pks.rows.total", "table" => table.clone())
                    .set(count as f64);
            }
            Err(e) => {
                error!("could not query __crsql_pks count for table {table}: {e}");
                continue;
            }
        }
        match conn
            .prepare_cached(&format!("SELECT count(*) FROM {table}__crsql_clock"))
            .and_then(|mut prepped| prepped.query_row([], |row| row.get::<_, i64>(0)))
        {
            Ok(count) => {
                persistent_gauge!("corro.db.table.bookkeeping.clock.rows.total", "table" => table.clone())
                    .set(count as f64);
            }
            Err(e) => {
                error!("could not query __crsql_clock count for table {table}: {e}");
                continue;
            }
        }
    }
}
