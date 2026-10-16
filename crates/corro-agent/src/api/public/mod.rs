use std::{
    collections::BTreeSet,
    net::SocketAddr,
    ops::Deref,
    time::{Duration, Instant},
};

use crate::api::utils::CountedBody;
use antithesis_sdk::assert_sometimes;
use axum::{
    extract::{ConnectInfo, Query},
    response::IntoResponse,
    Extension,
};
use bytes::{BufMut, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    agent::{Agent, ChangeError},
    api::{
        ColumnName, ExecResponse, ExecResult, HealthQuery, HealthResponse, QueryEvent, Statement,
        TableStatRequest, TableStatResponse,
    },
    base::CrsqlDbVersion,
    broadcast::Timestamp,
    change::{insert_local_changes, InsertChangesInfo, SqliteValue},
    persistent_gauge,
    sqlite::SqlitePoolError,
};
use hyper::StatusCode;
use metrics::{counter, histogram};
use rusqlite::{
    hooks::{AuthAction, AuthContext, Authorization},
    params_from_iter, ToSql, Transaction,
};
use serde::Deserialize;
use spawn::spawn_counted;
use sqlite_pool::{Committable, InterruptibleTransaction, SqliteConn};

use tokio::{
    sync::{
        mpsc::{self, channel},
        oneshot,
    },
    task::block_in_place,
};
use tracing::{debug, error, trace, warn};

use corro_types::broadcast::broadcast_changes;

pub mod pubsub;

pub mod update;

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct TimeoutParams {
    #[serde(default)]
    pub timeout: Option<u64>,
}

pub async fn make_broadcastable_changes<F, T>(
    agent: &Agent,
    timeout: Option<u64>,
    f: F,
) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
where
    F: FnOnce(&InterruptibleTransaction<Transaction>) -> Result<T, ChangeError>,
{
    let actor_id = agent.actor_id();
    trace!("getting conn...");
    let mut conn = agent.pool().write_priority().await?;
    trace!("got conn");

    let start = Instant::now();
    let ts = Timestamp::from(agent.clock().new_timestamp());

    block_in_place(move || {
        trace!("acquiring bookie write lock...");
        let bookie_write = agent.bookie().write_lock_blocking();
        let mut book_writer = bookie_write.write_tx(agent.booked());

        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let timeout = timeout.map(Duration::from_secs);
        let tx = InterruptibleTransaction::new(tx, timeout, "query_endpoint");

        let _ = tx
            .prepare_cached("SELECT crsql_set_ts(?)")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?
            .query_row([&ts], |row| row.get::<_, String>(0))
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        // Execute whatever might mutate state data
        let ret = f(&tx)?;

        let insert_info = insert_local_changes(agent, &tx, &mut book_writer)?;
        tx.commit().map_err(|source| {
            let ce = ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: insert_info.as_ref().map(|info| info.db_version),
            };
            if let Some(issue) = ce.fatal_db_issue() {
                error!("fatal DB issue detected: {issue}");
                agent.mark_unhealthy(issue);
            }
            ce
        })?;

        let elapsed = start.elapsed();
        histogram!("corro.agent.changes.processing.time.seconds", "source" => "local")
            .record(start.elapsed());

        match insert_info {
            None => Ok((ret, None, elapsed)),
            Some(InsertChangesInfo {
                db_version,
                last_seq,
                ts,
            }) => {
                trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

                book_writer.commit();

                let agent = agent.clone();

                spawn_counted(
                    async move { broadcast_changes(agent, db_version, last_seq, ts).await },
                );

                Ok::<_, ChangeError>((ret, Some(db_version), elapsed))
            }
        }
    })
}

#[tracing::instrument(skip_all, err)]
fn execute_statement<T>(
    tx: &InterruptibleTransaction<T>,
    stmt: &Statement,
) -> rusqlite::Result<usize>
where
    T: Deref<Target = rusqlite::Connection> + Committable,
{
    let mut prepped = tx.prepare(stmt.query())?;

    match stmt {
        Statement::Simple(_)
        | Statement::Verbose {
            params: None,
            named_params: None,
            ..
        } => prepped.execute([]),
        Statement::WithParams(_, params)
        | Statement::Verbose {
            params: Some(params),
            ..
        } => prepped.execute(params_from_iter(params)),
        Statement::WithNamedParams(_, params)
        | Statement::Verbose {
            named_params: Some(params),
            ..
        } => prepped.execute(
            params
                .iter()
                .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                .collect::<Vec<(&str, &dyn ToSql)>>()
                .as_slice(),
        ),
    }
}

#[tracing::instrument(skip_all)]
pub async fn api_v1_transactions(
    // axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Extension(agent): Extension<Agent>,
    axum::extract::Query(params): axum::extract::Query<TimeoutParams>,
    axum::extract::Json(statements): axum::extract::Json<Vec<Statement>>,
) -> (StatusCode, axum::Json<ExecResponse>) {
    let actor_id = agent.actor_id().to_string();
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ExecResponse {
                results: vec![ExecResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: 0.0,
                version: None,
                actor_id: Some(actor_id),
            }),
        );
    }

    counter!("corro.api.connection.count", "protocol" => "http").increment(1);
    assert_sometimes!(true, "Corrosion receives transactions through HTTP API");
    let res = make_broadcastable_changes(&agent, params.timeout, move |tx| {
        let mut total_rows_affected = 0;

        let results = statements
            .iter()
            .map(|stmt| {
                let start = Instant::now();
                let res = execute_statement(tx, stmt).map_err(|e| ChangeError::Rusqlite {
                    source: e,
                    actor_id: None,
                    version: None,
                });

                match res {
                    Ok(rows_affected) => {
                        total_rows_affected += rows_affected;
                        Ok(ExecResult::Execute {
                            rows_affected,
                            time: start.elapsed().as_secs_f64(),
                        })
                    }
                    Err(e) => Err(e),
                }
            })
            .collect::<Result<Vec<ExecResult>, ChangeError>>();

        results
    })
    .await;

    let (results, version, elapsed) = match res {
        Ok(res) => res,
        Err(e) => {
            error!("could not execute statement(s): {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(ExecResponse {
                    results: vec![ExecResult::Error {
                        error: e.to_string(),
                    }],
                    time: 0.0,
                    version: None,
                    actor_id: Some(actor_id),
                }),
            );
        }
    };

    (
        StatusCode::OK,
        axum::Json(ExecResponse {
            results,
            time: elapsed.as_secs_f64(),
            version: version.map(Into::into),
            actor_id: Some(actor_id),
        }),
    )
}

#[derive(Debug, thiserror::Error)]
pub enum QueryError {
    #[error("pool connection acquisition error")]
    Pool(#[from] SqlitePoolError),
    #[error("sqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),
}

fn is_safe_crsql_function(function_name: &str) -> bool {
    matches!(
        function_name.to_ascii_lowercase().as_str(),
        "crsql_config_get"
            | "crsql_db_version"
            | "crsql_fract_key_between"
            | "crsql_get_seq"
            | "crsql_get_ts"
            | "crsql_pack_columns"
            | "crsql_peek_next_db_version"
            | "crsql_rows_impacted"
            | "crsql_sha"
            | "crsql_site_id"
            | "crsql_version"
    )
}

fn is_effectful_read_pragma(pragma_name: &str) -> bool {
    matches!(
        pragma_name.to_ascii_lowercase().as_str(),
        "incremental_vacuum" | "optimize" | "shrink_memory" | "wal_checkpoint"
    )
}

fn is_effectful_pragma_table(table_name: &str) -> bool {
    table_name
        .to_ascii_lowercase()
        .strip_prefix("pragma_")
        .is_some_and(is_effectful_read_pragma)
}

fn is_safe_read_pragma(pragma_name: &str, pragma_value: Option<&str>) -> bool {
    match pragma_name.to_ascii_lowercase().as_str() {
        // These accept a target as SQLite's second authorizer argument but do
        // not change connection or database state.
        "foreign_key_check" | "foreign_key_list" | "index_info" | "index_list" | "index_xinfo"
        | "integrity_check" | "quick_check" | "table_info" | "table_list" | "table_xinfo" => true,
        // These execute work even without an assignment argument.
        name if is_effectful_read_pragma(name) => false,
        // Other argument-free PRAGMAs are getters or introspection queries.
        _ => pragma_value.is_none(),
    }
}

fn authorize_read_query(context: AuthContext<'_>) -> Authorization {
    match context.action {
        AuthAction::Read { table_name, .. } if !is_effectful_pragma_table(table_name) => {
            Authorization::Allow
        }
        AuthAction::Select | AuthAction::Recursive => Authorization::Allow,
        AuthAction::Pragma {
            pragma_name,
            pragma_value,
        } if is_safe_read_pragma(pragma_name, pragma_value) => Authorization::Allow,
        AuthAction::Function { function_name }
            if !function_name.eq_ignore_ascii_case("load_extension")
                && (!function_name
                    .get(..6)
                    .is_some_and(|prefix| prefix.eq_ignore_ascii_case("crsql_"))
                    || is_safe_crsql_function(function_name)) =>
        {
            Authorization::Allow
        }
        _ => Authorization::Deny,
    }
}

struct ReadQueryAuthorizer<'conn>(&'conn rusqlite::Connection);

impl<'conn> ReadQueryAuthorizer<'conn> {
    fn install(conn: &'conn rusqlite::Connection) -> rusqlite::Result<Self> {
        conn.authorizer(Some(authorize_read_query))?;
        Ok(Self(conn))
    }
}

impl Drop for ReadQueryAuthorizer<'_> {
    fn drop(&mut self) {
        let _ = self
            .0
            .authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
    }
}

async fn build_query_rows_response(
    agent: &Agent,
    client_addr: SocketAddr,
    data_tx: mpsc::Sender<QueryEvent>,
    stmt: Statement,
    timeout: Option<u64>,
) -> Result<(), (StatusCode, ExecResult)> {
    let (res_tx, res_rx) = oneshot::channel();

    let pool = agent.pool().clone();

    tokio::spawn(async move {
        let conn = match pool.read().await {
            Ok(conn) => conn,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ExecResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        // default timeout of 1 minute if no timeout is provided
        let timeout_secs = timeout.unwrap_or(60);
        let timeout: Option<Duration> =
            (timeout_secs > 0).then(|| Duration::from_secs(timeout_secs));

        let conn = InterruptibleTransaction::new(conn.conn(), timeout, "query");
        trace!(%client_addr, "Preparing statement {}", stmt.query());

        let _authorizer = match ReadQueryAuthorizer::install(&conn) {
            Ok(authorizer) => authorizer,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ExecResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        let prepped_res = block_in_place(|| conn.prepare(stmt.query()));

        let mut prepped = match prepped_res {
            Ok(prepped) => prepped,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::BAD_REQUEST,
                    ExecResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        if !prepped.readonly() {
            _ = res_tx.send(Err((
                StatusCode::BAD_REQUEST,
                ExecResult::Error {
                    error: "statement is not readonly".into(),
                },
            )));
            return;
        }

        block_in_place(|| {
            let col_count = prepped.column_count();
            trace!("inside block in place, col count: {col_count}");

            if let Err(e) = data_tx.blocking_send(QueryEvent::Columns(
                prepped
                    .columns()
                    .into_iter()
                    .map(|col| ColumnName(col.name().to_compact_string()))
                    .collect(),
            )) {
                error!("could not send back columns: {e}");
                return;
            }

            let start = Instant::now();

            trace!(%client_addr, "Executing statement {}", stmt.query());
            let elapsed = start.elapsed();

            let query = match &stmt {
                Statement::Simple(_)
                | Statement::Verbose {
                    params: None,
                    named_params: None,
                    ..
                } => prepped.query(()),
                Statement::WithParams(_, params)
                | Statement::Verbose {
                    params: Some(params),
                    ..
                } => prepped.query(params_from_iter(params)),
                Statement::WithNamedParams(_, params)
                | Statement::Verbose {
                    named_params: Some(params),
                    ..
                } => prepped.query(
                    params
                        .iter()
                        .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                        .collect::<Vec<(&str, &dyn ToSql)>>()
                        .as_slice(),
                ),
            };

            let mut rows = match query {
                Ok(rows) => rows,
                Err(e) => {
                    _ = res_tx.send(Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ExecResult::Error {
                            error: e.to_string(),
                        },
                    )));
                    return;
                }
            };

            trace!(%client_addr, elapsed = %elapsed.as_secs(), "Statement finished executing {}", stmt.query());

            if elapsed > Duration::from_secs(10) {
                warn!(%client_addr, elapsed = %elapsed.as_secs(), "Slow read statement {}!", stmt.query());
            }

            if let Err(_e) = res_tx.send(Ok(())) {
                error!("could not send back response through oneshot channel, aborting");
                return;
            }

            let mut rowid = 1;

            trace!("about to loop through rows!");

            loop {
                match rows.next() {
                    Ok(Some(row)) => {
                        trace!("got a row: {row:?}");
                        match (0..col_count)
                            .map(|i| row.get::<_, SqliteValue>(i))
                            .collect::<rusqlite::Result<Vec<_>>>()
                        {
                            Ok(cells) => {
                                if let Err(e) =
                                    data_tx.blocking_send(QueryEvent::Row(rowid.into(), cells))
                                {
                                    error!("could not send back row: {e}");
                                    return;
                                }
                                rowid += 1;
                            }
                            Err(e) => {
                                _ = data_tx.blocking_send(QueryEvent::Error(e.to_compact_string()));
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        // done!
                        break;
                    }
                    Err(e) => {
                        _ = data_tx.blocking_send(QueryEvent::Error(e.to_compact_string()));
                        return;
                    }
                }
            }

            _ = data_tx.blocking_send(QueryEvent::EndOfQuery {
                time: elapsed.as_secs_f64(),
                change_id: None,
            });
        });
    });

    match res_rx.await {
        Ok(res) => res,
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            ExecResult::Error {
                error: e.to_string(),
            },
        )),
    }
}

pub async fn api_v1_queries(
    Extension(agent): Extension<Agent>,
    ConnectInfo(client_addr): ConnectInfo<SocketAddr>,
    axum::extract::Query(params): axum::extract::Query<TimeoutParams>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (mut tx, body) = CountedBody::channel(
        persistent_gauge!("corro.api.active.streams", "source" => "queries", "protocol" => "http"),
    );

    counter!("corro.api.queries.count").increment(1);
    // TODO: timeout on data send instead of infinitely waiting for channel space.
    let (data_tx, mut data_rx) = channel(512);

    let start = Instant::now();
    tokio::spawn(async move {
        let mut buf = BytesMut::new();

        while let Some(row_res) = data_rx.recv().await {
            {
                let mut writer = (&mut buf).writer();
                if let Err(e) = serde_json::to_writer(&mut writer, &row_res) {
                    _ = tx
                        .send_data(
                            serde_json::to_vec(&serde_json::json!(QueryEvent::Error(
                                e.to_compact_string()
                            )))
                            .expect("could not serialize error json")
                            .into(),
                        )
                        .await;
                    return;
                }
            }

            buf.extend_from_slice(b"\n");

            if let Err(e) = tx.send_data(buf.split().freeze()).await {
                error!("could not send data through body's channel: {e}");
                return;
            }
        }
        debug!("query body channel done");
    });

    trace!("building query rows response...");
    assert_sometimes!(true, "Corrosion accepts queries");

    match build_query_rows_response(&agent, client_addr, data_tx, stmt, params.timeout).await {
        Ok(_) => {
            histogram!("corro.api.queries.processing.time.seconds", "result" => "success")
                .record(start.elapsed());
            hyper::Response::builder()
                .status(StatusCode::OK)
                .body(axum::body::Body::new(body))
                .expect("could not build query response body")
        }
        Err((status, res)) => {
            histogram!("corro.api.queries.processing.time.seconds", "result" => "error")
                .record(start.elapsed());
            hyper::Response::builder()
                .status(status)
                .body(
                    serde_json::to_vec(&res)
                        .expect("could not serialize query error response")
                        .into(),
                )
                .expect("could not build query response body")
        }
    }
}

pub async fn api_v1_health(
    Extension(agent): Extension<Agent>,
    Query(query): Query<HealthQuery>,
) -> (StatusCode, axum::Json<HealthResponse>) {
    match check_health(&agent).await {
        Ok((gaps, members)) => {
            let status = query.failure_status.unwrap_or(503);
            let error_status =
                StatusCode::from_u16(status).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
            let p99_lag = match agent.metrics_tracker().quantile_lag(0.99) {
                Some(lag) => lag,
                None => {
                    error!("no p99 lag information available");
                    return (
                        error_status,
                        axum::Json(HealthResponse::Error(
                            "no p99 lag information available".into(),
                        )),
                    );
                }
            };

            let queue_size = agent.metrics_tracker().queue_size();
            let status = if query.gaps.is_some_and(|max| gaps > max)
                || query.max_queue.is_some_and(|max| queue_size > max)
                // we use queue size and p99 lag as a stronger metric for an unhealthy node
                // since a different node that is slow to send out changes can cause worse commit lag
                // even though the node is perfectly fine.
                || (query.p99_lag.is_some_and(|max| p99_lag > max)
                    && query.queue_size.is_none_or(|max| queue_size > max))
            {
                error_status
            } else {
                StatusCode::OK
            };
            (
                status,
                axum::Json(HealthResponse::Response {
                    gaps,
                    members,
                    p99_lag,
                    queue_size,
                }),
            )
        }
        Err(e) => {
            error!("could not check health: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(HealthResponse::Error(e.to_string())),
            )
        }
    }
}

async fn check_health(agent: &Agent) -> eyre::Result<(i64, i64)> {
    let read_conn = match agent.pool().read().await {
        Ok(conn) => conn,
        Err(e) => {
            error!("could not acquire read connection for health check: {e}");
            return Err(eyre::eyre!("unable to grab write conn"));
        }
    };

    let gaps = read_conn
        .prepare_cached("SELECT COALESCE(SUM(end - start + 1), 0) FROM __corro_bookkeeping_gaps")?
        .query_row([], |row| row.get::<_, i64>(0))?;

    let members = read_conn.prepare_cached(r#"
            SELECT COALESCE(COUNT(*), 0) FROM __corro_members WHERE json_extract(foca_state, "$.state") = "Alive""#)?
        .query_row([], |row| row.get::<_, i64>(0))?;

    Ok((gaps, members))
}
/// Query the table status of the current node
///
/// Currently this endpoint only supports querying the row count for a
/// selection of provided tables.  Table names are checked for
/// existence before querying
pub async fn api_v1_table_stats(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(ts_req): axum::extract::Json<TableStatRequest>,
) -> (StatusCode, axum::Json<TableStatResponse>) {
    async fn count_table_lengths(
        agent: &Agent,
        ts_req: TableStatRequest,
    ) -> eyre::Result<(i64, Vec<String>)> {
        debug!("Querying row count for {} tables", ts_req.tables.len());
        let conn = agent.pool().read().await?;

        block_in_place(move || -> eyre::Result<(i64, Vec<String>)> {
            let valid_tables: BTreeSet<String> = conn
                .prepare_cached("select name from sqlite_schema where type = 'table'")?
                .query_map([], |row| row.get(0))?
                .filter_map(|name| name.ok())
                .collect();

            let mut invalid_tables = vec![];
            let mut total_count = 0;
            for table in ts_req.tables.into_iter() {
                if !valid_tables.contains(&table) {
                    error!("Table name {} doesn't exist!", &table);
                    invalid_tables.push(table);
                    continue;
                }

                let count: i64 = conn
                    .prepare_cached(&format!("SELECT COUNT(*) FROM {}", &table))?
                    .query_row((), |row| row.get(0))?;

                total_count += count;
            }
            Ok((total_count, invalid_tables))
        })
    }

    match count_table_lengths(&agent, ts_req).await {
        Ok((count, invalid_tables)) => (
            StatusCode::OK,
            axum::Json(TableStatResponse {
                total_row_count: count,
                invalid_tables,
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(TableStatResponse {
                total_row_count: 0,
                // Since we don't know what error occurred or if any
                // tables were valid, we just return an empty list
                invalid_tables: vec![],
            }),
        ),
    }
}

#[cfg(test)]
mod tests {
    use corro_types::{
        api::RowId,
        base::CrsqlDbVersion,
        broadcast::{ChangeV1, Changeset, PlumtreeInput},
        config::{BroadcastMethod, Config},
        schema::SqliteType,
    };
    use futures::StreamExt;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use super::*;

    use crate::{agent::setup, agent::util::execute_schema};

    const VALID_READ_QUERY_CASES: &[&str] = &[
        "SELECT 1",
        "WITH value(x) AS (VALUES (1)) SELECT x FROM value",
        "VALUES (1)",
        "EXPLAIN SELECT 1",
        "EXPLAIN QUERY PLAN SELECT 1",
        "PRAGMA foreign_keys",
        "PRAGMA compile_options",
        "PRAGMA integrity_check",
        "PRAGMA table_info('freshness')",
        "PRAGMA table_list('freshness')",
        "SELECT count(*) FROM pragma_function_list",
        "SELECT count(*) FROM pragma_module_list",
        "SELECT count(*) FROM pragma_table_info('freshness')",
        "SELECT count(*) FROM pragma_table_list('freshness')",
    ];

    const REJECTED_READ_QUERY_CASES: &[&str] = &[
        "BEGIN",
        "SAVEPOINT leaked",
        "PRAGMA query_only = ON",
        "PRAGMA read_uncommitted = ON",
        "PRAGMA locking_mode = EXCLUSIVE",
        "EXPLAIN PRAGMA query_only = ON",
        "ATTACH DATABASE ':memory:' AS leaked",
        "PRAGMA incremental_vacuum",
        "PRAGMA optimize",
        "PRAGMA shrink_memory",
        "PRAGMA wal_checkpoint",
        "SELECT * FROM pragma_optimize(0x10002)",
        "SELECT crsql_finalize()",
        "UPDATE freshness SET value = 9",
    ];

    #[test]
    fn read_query_authorizer_rejects_connection_state_statements_before_prepare() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE freshness (value INTEGER)", ())
            .unwrap();

        for sql in VALID_READ_QUERY_CASES
            .iter()
            .copied()
            .chain(["SELECT 1_000"])
        {
            let _authorizer = ReadQueryAuthorizer::install(&conn).unwrap();
            let _: rusqlite::types::Value = conn
                .query_row(sql, (), |row| row.get(0))
                .unwrap_or_else(|error| panic!("expected a read query: {sql}: {error}"));
        }

        for sql in REJECTED_READ_QUERY_CASES.iter().copied().chain([
            "COMMIT",
            "ROLLBACK",
            "RELEASE leaked",
            "EXPLAIN QUERY PLAN PRAGMA read_uncommitted = ON",
            "DETACH DATABASE leaked",
            "SELECT load_extension('not-present')",
        ]) {
            let _authorizer = ReadQueryAuthorizer::install(&conn).unwrap();
            assert!(conn.prepare(sql).is_err(), "expected rejection: {sql}");
        }

        assert!(conn.is_autocommit());
        assert_eq!(
            conn.query_row("PRAGMA query_only", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );

        {
            let _authorizer = ReadQueryAuthorizer::install(&conn).unwrap();
            let column_name = conn
                .query_row("PRAGMA table_info('freshness')", (), |row| {
                    row.get::<_, String>(1)
                })
                .unwrap();
            assert_eq!(column_name, "value");
        }
        {
            let _authorizer = ReadQueryAuthorizer::install(&conn).unwrap();
            let count = conn
                .query_row(
                    "SELECT count(*) FROM pragma_table_info('freshness')",
                    (),
                    |row| row.get::<_, i64>(0),
                )
                .unwrap();
            assert_eq!(count, 1);
        }
        assert_eq!(
            conn.query_row("PRAGMA read_uncommitted", (), |row| row.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(
            conn.query_row("PRAGMA locking_mode", (), |row| row.get::<_, String>(0))
                .unwrap(),
            "normal"
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM pragma_database_list WHERE name = 'leaked'",
                (),
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
        conn.execute_batch("BEGIN; ROLLBACK")
            .expect("the request authorizer should be removed on drop");
    }

    #[test]
    fn read_query_authorizer_blocks_mutation_during_automatic_reprepare() -> eyre::Result<()> {
        use std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        };

        fn open_mutating_reader(
            path: &std::path::Path,
            mutated: &Arc<AtomicBool>,
        ) -> eyre::Result<rusqlite::Connection> {
            let reader = rusqlite::Connection::open(path)?;
            reader.pragma_update(None, "trusted_schema", true)?;
            let mutated = mutated.clone();
            reader.create_scalar_function(
                "crsql_mutate",
                0,
                rusqlite::functions::FunctionFlags::SQLITE_UTF8,
                move |_| {
                    mutated.store(true, Ordering::SeqCst);
                    Ok(2_i64)
                },
            )?;
            Ok(reader)
        }

        let dir = tempfile::tempdir()?;
        let path = dir.path().join("reprepare.db");
        let writer = rusqlite::Connection::open(&path)?;
        writer.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE VIEW guarded AS SELECT 1 AS value;",
        )?;

        let mutated = Arc::new(AtomicBool::new(false));
        let unguarded_reader = open_mutating_reader(&path, &mutated)?;

        let mut unguarded_statement = unguarded_reader.prepare("SELECT value FROM guarded")?;
        writer.execute_batch(
            "DROP VIEW guarded;
             CREATE VIEW guarded AS SELECT crsql_mutate() AS value;",
        )?;
        assert_eq!(
            unguarded_statement.query_row((), |row| row.get::<_, i64>(0))?,
            2
        );
        assert!(mutated.swap(false, Ordering::SeqCst));
        drop(unguarded_statement);
        drop(unguarded_reader);

        writer.execute_batch(
            "DROP VIEW guarded;
             CREATE VIEW guarded AS SELECT 1 AS value;",
        )?;

        let reader = open_mutating_reader(&path, &mutated)?;

        let _authorizer = ReadQueryAuthorizer::install(&reader)?;
        let mut statement = reader.prepare("SELECT value FROM guarded")?;

        writer.execute_batch(
            "DROP VIEW guarded;
             CREATE VIEW guarded AS SELECT crsql_mutate() AS value;",
        )?;

        let error = statement
            .query_row((), |row| row.get::<_, i64>(0))
            .unwrap_err();
        assert!(
            matches!(
                error,
                rusqlite::Error::SqliteFailure(_, Some(ref message))
                    if message == "not authorized to use function: crsql_mutate"
            ),
            "unexpected reprepare error: {error:?}"
        );
        assert!(!mutated.load(Ordering::SeqCst));

        Ok(())
    }

    async fn query_events(agent: &Agent, sql: &str) -> eyre::Result<(StatusCode, Vec<QueryEvent>)> {
        let response = api_v1_queries(
            Extension(agent.clone()),
            ConnectInfo("127.0.0.1:22000".parse()?),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(Statement::Simple(sql.into())),
        )
        .await
        .into_response();

        let status = response.status();
        let mut body = response.into_body().into_data_stream();
        let mut bytes = BytesMut::new();
        while let Some(chunk) = body.next().await {
            bytes.extend_from_slice(&chunk?);
        }

        if !status.is_success() {
            return Ok((status, Vec::new()));
        }

        let events = std::str::from_utf8(&bytes)?
            .lines()
            .map(serde_json::from_str)
            .collect::<Result<Vec<QueryEvent>, _>>()?;
        Ok((status, events))
    }

    async fn query_integer(agent: &Agent, sql: &str) -> eyre::Result<i64> {
        let (status, events) = query_events(agent, sql).await?;
        assert_eq!(status, StatusCode::OK);
        events
            .into_iter()
            .find_map(|event| match event {
                QueryEvent::Row(_, mut values) => match values.remove(0) {
                    SqliteValue::Integer(value) => Some(value),
                    _ => None,
                },
                _ => None,
            })
            .ok_or_else(|| eyre::eyre!("query returned no integer row: {sql}"))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn query_handler_cannot_recycle_connection_state() -> eyre::Result<()> {
        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();
        let dir = tempfile::tempdir()?;
        let (agent, _agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        {
            let conn = agent.pool().write_priority().await?;
            conn.execute_batch(
                "CREATE TABLE freshness (
                    id INTEGER PRIMARY KEY NOT NULL,
                    value INTEGER NOT NULL DEFAULT 0
                 );
                 SELECT crsql_as_crr('freshness');
                 INSERT INTO freshness VALUES (1, 1);",
            )?;
        }

        for sql in VALID_READ_QUERY_CASES
            .iter()
            .copied()
            .chain(["SELECT corro_json_contains('{}', '{}')"])
        {
            assert_eq!(
                query_events(&agent, sql).await?.0,
                StatusCode::OK,
                "rejected {sql}"
            );
        }

        let begin_status = query_events(&agent, "BEGIN").await?.0;
        let change_value_sql = "SELECT val FROM crsql_changes
            WHERE \"table\" = 'freshness' AND cid = 'value'
            ORDER BY db_version DESC, seq DESC LIMIT 1";

        assert_eq!(
            query_integer(&agent, "SELECT value FROM freshness WHERE id = 1").await?,
            1
        );
        assert_eq!(query_integer(&agent, change_value_sql).await?, 1);
        {
            let conn = agent.pool().write_priority().await?;
            conn.execute("UPDATE freshness SET value = 2 WHERE id = 1", ())?;
        }
        assert_eq!(
            query_integer(&agent, change_value_sql).await?,
            2,
            "the handler recycled a stale crsql_changes snapshot"
        );
        assert_eq!(
            query_integer(&agent, "SELECT value FROM freshness WHERE id = 1").await?,
            2
        );
        assert_eq!(begin_status, StatusCode::BAD_REQUEST, "accepted BEGIN");

        for sql in REJECTED_READ_QUERY_CASES
            .iter()
            .copied()
            .filter(|sql| *sql != "BEGIN")
            .chain([
                "SELECT crsql_set_ts(1)",
                "SELECT crsql_as_crr('freshness')",
                "SELECT crsql_as_table('freshness')",
            ])
        {
            assert_eq!(
                query_events(&agent, sql).await?.0,
                StatusCode::BAD_REQUEST,
                "accepted {sql}"
            );
        }

        assert!(query_integer(&agent, "SELECT crsql_db_version()").await? >= 0);

        let conn = agent.pool().read().await?;
        assert!(conn.is_autocommit());
        assert_eq!(
            conn.query_row("PRAGMA query_only", (), |row| row.get::<_, i64>(0))?,
            0
        );
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM pragma_database_list WHERE name = 'leaked'",
                (),
                |row| row.get::<_, i64>(0),
            )?,
            0
        );
        conn.execute_batch("BEGIN; ROLLBACK")?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_execute() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, mut agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .broadcast_method(BroadcastMethod::Plumtree)
                .build()?,
            tripwire,
        )
        .await?;

        let rx_bcast = &mut agent_options.rx_bcast;
        let rx_plumtree = &mut agent_options.rx_plumtree;

        execute_schema(&agent, vec![corro_tests::TEST_SCHEMA.to_owned()]).await?;

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![Statement::WithParams(
                "insert into tests (id, text) values (?,?)".into(),
                vec!["service-id".into(), "service-name".into()],
            )]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        let msg = rx_plumtree
            .recv()
            .await
            .expect("not msg received on bcast channel");

        assert!(matches!(
            msg,
            PlumtreeInput::Broadcast(ChangeV1 {
                changeset: Changeset::FullV2 {
                    version: CrsqlDbVersion(1),
                    ..
                },
                ..
            })
        ));

        assert_eq!(agent.booked().read().last(), Some(CrsqlDbVersion(1)));

        println!("second req...");

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![Statement::WithParams(
                "update tests SET text = ? where id = ?".into(),
                vec!["service-name".into(), "service-id".into()],
            )]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        // no actual changes!
        assert!(matches!(rx_bcast.try_recv(), Err(TryRecvError::Empty)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_query() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, _agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        execute_schema(&agent, vec![corro_tests::TEST_SCHEMA.to_owned()]).await?;

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id".into(), "service-name".into()],
                ),
                Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-2".into(), "service-name-2".into()],
                ),
            ]),
        )
        .await;

        // println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 2);

        println!("transaction body: {body:?}");

        let res = api_v1_queries(
            Extension(agent.clone()),
            ConnectInfo("127.0.0.1:1234".parse().unwrap()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(Statement::Simple("select * from tests".into())),
        )
        .await
        .into_response();

        assert_eq!(res.status(), StatusCode::OK);

        let mut body = res.into_body().into_data_stream();

        let mut lines = LinesCodec::new();

        let mut buf = BytesMut::new();

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let cols: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(cols, QueryEvent::Columns(vec!["id".into(), "text".into()]));

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(RowId(1), vec!["service-id".into(), "service-name".into()])
        );

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(
                RowId(2),
                vec!["service-id-2".into(), "service-name-2".into()]
            )
        );

        buf.extend_from_slice(&body.next().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let query_evt: QueryEvent = serde_json::from_str(&s).unwrap();

        assert!(matches!(query_evt, QueryEvent::EndOfQuery { .. }));

        assert!(body.next().await.is_none());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_schema() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let (agent, _agent_options) = setup(
            Config::builder()
                .db_path(dir.path().join("corrosion.db").display().to_string())
                .gossip_addr("127.0.0.1:0".parse()?)
                .api_addr("127.0.0.1:0".parse()?)
                .build()?,
            tripwire,
        )
        .await?;

        execute_schema(
            &agent,
            vec![
                "CREATE TABLE tests2 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
                "CREATE TABLE tests (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
            ],
        )
        .await?;

        // scope the schema reader in here
        {
            let schema = agent.schema().read();
            let tests = schema
                .tables
                .get("tests")
                .expect("no tests table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);
        }

        execute_schema(
            &agent,
            vec![
                "CREATE TABLE tests2 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
                "CREATE TABLE tests (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
            ],
        )
        .await?;

        {
            let schema = agent.schema().read();
            let tests = schema
                .tables
                .get("tests")
                .expect("no tests table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let tests = schema
                .tables
                .get("tests2")
                .expect("no tests2 table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);
        }

        // w/ existing table!

        let create_stmt = "CREATE TABLE tests3 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT, updated_at INTEGER NOT NULL DEFAULT 0);";

        {
            // adding the table and an index
            let conn = agent.pool().write_priority().await?;
            conn.execute_batch(create_stmt)?;
            conn.execute_batch("CREATE INDEX tests3_updated_at ON tests3 (updated_at);")?;
            assert_eq!(
                conn.execute(
                    "INSERT INTO tests3 VALUES (123, 'some foo text', 123456789);",
                    ()
                )?,
                1
            );
            assert_eq!(
                conn.execute(
                    "INSERT INTO tests3 VALUES (1234, 'some foo text 2', 1234567890);",
                    ()
                )?,
                1
            );
        }

        execute_schema(&agent, vec![create_stmt.to_owned()]).await?;

        {
            let schema = agent.schema().read();

            // check that the tests table is still there!
            let tests = schema
                .tables
                .get("tests")
                .expect("no tests table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let tests = schema
                .tables
                .get("tests3")
                .expect("no tests3 table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type(), (SqliteType::Integer, Some("BIGINT")));
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type(), (SqliteType::Text, Some("TEXT")));
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let updated_at_col = tests.columns.get("updated_at").unwrap();
            assert_eq!(updated_at_col.name, "updated_at");
            assert_eq!(
                updated_at_col.sql_type(),
                (SqliteType::Integer, Some("INTEGER"))
            );
            assert!(!updated_at_col.nullable);
            assert!(!updated_at_col.primary_key);

            let updated_at_idx = tests.indexes.get("tests3_updated_at").unwrap();
            assert_eq!(updated_at_idx.name, "tests3_updated_at");
            assert_eq!(updated_at_idx.tbl_name, "tests3");
            assert_eq!(updated_at_idx.columns.len(), 1);
            assert!(updated_at_idx.where_clause.is_none());
        }

        let conn = agent.pool().read().await?;
        let count: usize =
            conn.query_row("SELECT COUNT(*) FROM tests3__crsql_clock;", (), |row| {
                row.get(0)
            })?;
        // should've created a specific qty of clock table rows, just a sanity check!
        assert_eq!(count, 4);

        Ok(())
    }
}
