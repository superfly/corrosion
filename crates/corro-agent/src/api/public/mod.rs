use std::{
    collections::BTreeSet,
    net::SocketAddr,
    ops::Deref,
    time::{Duration, Instant},
};

use crate::api::utils::CountedBody;
use antithesis_sdk::assert_sometimes;
use axum::{extract::ConnectInfo, response::IntoResponse, Extension};
use bytes::{BufMut, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    agent::{Agent, ChangeError},
    api::{
        ColumnName, ExecResponse, ExecResult, QueryEvent, Statement, TableStatRequest,
        TableStatResponse,
    },
    base::CrsqlDbVersion,
    broadcast::Timestamp,
    change::{insert_local_changes, InsertChangesInfo, SqliteValue},
    persistent_gauge,
    schema::{apply_schema, parse_sql},
    sqlite::SqlitePoolError,
};
use hyper::StatusCode;
use metrics::{counter, histogram};
use rusqlite::{params_from_iter, ToSql, Transaction};
use serde::Deserialize;
use spawn::spawn_counted;
use sqlite_pool::{Committable, InterruptibleTransaction};

use tokio::{
    sync::{
        mpsc::{self, channel},
        oneshot,
    },
    task::block_in_place,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

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
    trace!("getting conn...");
    let mut conn = agent.pool().write_priority().await?;
    trace!("got conn");

    let actor_id = agent.actor_id();
    // maybe we should do this earlier, but there can only ever be 1 write conn at a time,
    // so it probably doesn't matter too much, except for reads of internal state
    let mut book_writer = agent
        .booked()
        .write::<&str, _>("make_broadcastable_changes(booked writer)", None)
        .await;

    let start = Instant::now();
    let ts = Timestamp::from(agent.clock().new_timestamp());

    block_in_place(move || {
        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let timeout = timeout.map(Duration::from_secs);
        let tx = InterruptibleTransaction::new(tx, timeout, "local_changes");

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
        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: insert_info.as_ref().map(|info| info.db_version),
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
                snap,
            }) => {
                trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

                book_writer.commit_snapshot(snap);

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

        trace!(%client_addr, "Preparing statement {}", stmt.query());

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

        let timeout = timeout.unwrap_or(4);
        let timeout: Option<Duration> = if timeout > 0 {
            Some(Duration::from_secs(timeout * 60))
        } else {
            None
        };

        let int_handle = conn.get_interrupt_handle();
        let token = CancellationToken::new();
        if let Some(timeout) = timeout {
            let cloned_token = token.clone();
            let stmt_query = stmt.query().to_string();
            tokio::spawn(async move {
                tokio::select! {
                    _ = cloned_token.cancelled() => {}
                    _ = tokio::time::sleep(timeout) => {
                        warn!("sql call took more than {timeout:?}, interrupting stmt- {:?}", stmt_query);
                        int_handle.interrupt();
                        counter!("corro.sqlite.interrupt", "source" => "timeout").increment(1);
                    }
                };
            });
        }

        let _dropguard = token.drop_guard();
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
            #[allow(clippy::needless_return)]
            return hyper::Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("could not build query response body");
        }
        Err((status, res)) => {
            histogram!("corro.api.queries.processing.time.seconds", "result" => "error")
                .record(start.elapsed());
            #[allow(clippy::needless_return)]
            return hyper::Response::builder()
                .status(status)
                .body(
                    serde_json::to_vec(&res)
                        .expect("could not serialize query error response")
                        .into(),
                )
                .expect("could not build query response body");
        }
    }
}

pub(crate) async fn execute_schema(agent: &Agent, statements: Vec<String>) -> eyre::Result<()> {
    let new_sql: String = statements.join(";");

    let partial_schema = parse_sql(&new_sql)?;

    info!("getting write connection to update schema");
    let mut conn = agent.pool().write_priority().await?;
    info!("got write connection to update schema");

    // hold onto this lock so nothing else makes changes
    let mut schema_write = agent.schema().write();

    // clone the previous schema and apply
    let mut new_schema = {
        let mut schema = schema_write.clone();
        for (name, def) in partial_schema.tables.iter() {
            // overwrite table because users are expected to return a full table def
            schema.tables.insert(name.clone(), def.clone());
        }
        schema
    };

    new_schema.constrain()?;

    // conn.trace(Some(|sql| debug!(sql)));

    let apply_res = block_in_place(|| {
        let tx = conn.immediate_transaction()?;

        apply_schema(&tx, &schema_write, &mut new_schema)?;

        for tbl_name in partial_schema.tables.keys() {
            tx.execute("DELETE FROM __corro_schema WHERE tbl_name = ?", [tbl_name])?;

            let n = tx.execute("INSERT INTO __corro_schema SELECT tbl_name, type, name, sql, 'api' AS source FROM sqlite_schema WHERE tbl_name = ? AND type IN ('table', 'index') AND name IS NOT NULL AND sql IS NOT NULL", [tbl_name])?;
            info!("Updated {n} rows in __corro_schema for table {tbl_name}");
        }

        tx.commit()?;

        // drain the pool of RO connections because they might not get the new tables in cr-sqlite!
        agent.pool().drain_read();

        Ok::<_, eyre::Report>(())
    });

    // conn.trace(None);

    apply_res?;

    *schema_write = new_schema;

    Ok(())
}

pub async fn api_v1_db_schema(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(statements): axum::extract::Json<Vec<String>>,
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

    let start = Instant::now();

    assert_sometimes!(true, "Corrosion applies schema");
    if let Err(e) = execute_schema(&agent, statements).await {
        error!("could not merge schemas: {e}");
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

    (
        StatusCode::OK,
        axum::Json(ExecResponse {
            results: vec![],
            time: start.elapsed().as_secs_f64(),
            version: None,
            actor_id: Some(actor_id),
        }),
    )
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
                // Since we don't know what error occured or if any
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
        broadcast::{BroadcastInput, BroadcastV1, ChangeV1, Changeset},
        config::Config,
        schema::SqliteType,
    };
    use http_body::Body;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use super::*;

    use crate::agent::setup;

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
                .build()?,
            tripwire,
        )
        .await?;

        let rx_bcast = &mut agent_options.rx_bcast;

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

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

        let msg = rx_bcast
            .recv()
            .await
            .expect("not msg received on bcast channel");

        assert!(matches!(
            msg,
            BroadcastInput::AddBroadcast(BroadcastV1::Change(ChangeV1 {
                changeset: Changeset::FullV2 {
                    version: CrsqlDbVersion(1),
                    ..
                },
                ..
            }))
        ));

        assert_eq!(
            agent.booked().read::<&str, _>("test", None).await.last(),
            Some(CrsqlDbVersion(1))
        );

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

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

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

        let mut body = res.into_body();

        let mut lines = LinesCodec::new();

        let mut buf = BytesMut::new();

        buf.extend_from_slice(&body.data().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let cols: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(cols, QueryEvent::Columns(vec!["id".into(), "text".into()]));

        buf.extend_from_slice(&body.data().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(RowId(1), vec!["service-id".into(), "service-name".into()])
        );

        buf.extend_from_slice(&body.data().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(
                RowId(2),
                vec!["service-id-2".into(), "service-name-2".into()]
            )
        );

        buf.extend_from_slice(&body.data().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let query_evt: QueryEvent = serde_json::from_str(&s).unwrap();

        assert!(matches!(query_evt, QueryEvent::EndOfQuery { .. }));

        assert!(body.data().await.is_none());

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

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![
                "CREATE TABLE tests (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
            ]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

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

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![
                "CREATE TABLE tests2 (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
                "CREATE TABLE tests (id BIGINT NOT NULL PRIMARY KEY, foo TEXT);".into(),
            ]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

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

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![create_stmt.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

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
