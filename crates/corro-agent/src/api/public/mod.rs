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
    change::{
        database_has_foreign_keys, database_has_user_triggers, database_schema_version,
        insert_local_changes, InsertChangesInfo, PendingLocalChanges, SqliteValue,
    },
    persistent_gauge,
    schema::{apply_schema, parse_sql},
    sqlite::{CrConn, SqlitePoolError},
};
use hyper::StatusCode;
use metrics::{counter, histogram};
use rusqlite::fallible_iterator::FallibleIterator;
use rusqlite::{params_from_iter, ToSql, Transaction};
use serde::Deserialize;
use spawn::spawn_counted;
use sqlite3_parser::ast::{Cmd, Stmt as SqliteStmt};
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

enum SpeculativeChanges<T> {
    Completed((T, Option<CrsqlDbVersion>, Duration)),
    RequiresCanonical,
}

#[cfg(test)]
impl<T> SpeculativeChanges<T> {
    fn into_completed(self) -> (T, Option<CrsqlDbVersion>, Duration) {
        match self {
            Self::Completed(result) => result,
            Self::RequiresCanonical => panic!("speculative transaction required canonical replay"),
        }
    }
}

async fn make_broadcastable_changes<F, T>(
    agent: &Agent,
    timeout: Option<u64>,
    f: F,
) -> Result<SpeculativeChanges<T>, ChangeError>
where
    F: FnOnce(&InterruptibleTransaction<CrConn>) -> Result<T, ChangeError>,
{
    let actor_id = agent.actor_id();
    let start = Instant::now();
    let ts = Timestamp::from(agent.clock().new_timestamp());
    let timeout = timeout.map(Duration::from_secs);

    let conn = agent
        .pool()
        .client_dedicated()
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    let speculative = block_in_place(move || {
        conn.execute_batch("BEGIN CONCURRENT;")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let schema_version =
            database_schema_version(&conn).map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let replay_safe = !database_has_user_triggers(&conn).unwrap_or(true)
            && !database_has_foreign_keys(&conn).unwrap_or(true);

        if !replay_safe {
            conn.execute_batch("ROLLBACK;")
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: None,
                })?;
            return Ok(None);
        }

        let tx = InterruptibleTransaction::new(conn, timeout, "local_changes");

        if let Err(source) = tx
            .prepare_cached("SELECT crsql_set_ts(?)")
            .and_then(|mut stmt| stmt.query_row([&ts], |row| row.get::<_, String>(0)))
        {
            let _ = tx.execute_batch("ROLLBACK;");
            return Err(ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            });
        }

        let ret = match f(&tx) {
            Ok(ret) => ret,
            Err(err) => {
                let _ = tx.execute_batch("ROLLBACK;");
                return Err(err);
            }
        };

        let pending = match PendingLocalChanges::capture(&tx, actor_id) {
            Ok(pending) => pending,
            Err(source) => {
                let _ = tx.execute_batch("ROLLBACK;");
                return Err(ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: None,
                });
            }
        };

        tx.execute_batch("ROLLBACK;")
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        Ok::<_, ChangeError>(Some((ret, pending, schema_version)))
    })?;

    let Some((ret, pending, schema_version)) = speculative else {
        return Ok(SpeculativeChanges::RequiresCanonical);
    };

    if pending.is_empty() {
        let elapsed = start.elapsed();
        histogram!("corro.agent.changes.processing.time.seconds", "source" => "local")
            .record(elapsed);
        return Ok(SpeculativeChanges::Completed((ret, None, elapsed)));
    }

    let mut conn = agent.pool().write_priority().await?;
    let mut book_writer = agent
        .booked()
        .write::<&str, _>("make_broadcastable_changes(booked writer)", None)
        .await;

    let insert_info = block_in_place(move || {
        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let current_schema_version =
            database_schema_version(&tx).map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        if current_schema_version != schema_version {
            return Err(ChangeError::SchemaChanged {
                expected: schema_version,
                actual: current_schema_version,
            });
        }

        let reserved_version = pending
            .replay(&tx)
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let insert_info = insert_local_changes(agent, &tx, &mut book_writer)?;

        if reserved_version.is_some() && insert_info.is_none() {
            tx.rollback().map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: reserved_version,
            })?;
            return Ok(None);
        }

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: insert_info.as_ref().map(|info| info.db_version),
        })?;

        let committed = match insert_info {
            None => None,
            Some(InsertChangesInfo {
                db_version,
                last_seq,
                ts,
                snap,
            }) => {
                book_writer.commit_snapshot(snap);
                Some((db_version, last_seq, ts))
            }
        };

        Ok::<_, ChangeError>(committed)
    })?;

    let elapsed = start.elapsed();
    histogram!("corro.agent.changes.processing.time.seconds", "source" => "local").record(elapsed);

    match insert_info {
        None => Ok(SpeculativeChanges::Completed((ret, None, elapsed))),
        Some((db_version, last_seq, ts)) => {
            let agent = agent.clone();
            spawn_counted(async move { broadcast_changes(agent, db_version, last_seq, ts).await });

            Ok(SpeculativeChanges::Completed((
                ret,
                Some(db_version),
                elapsed,
            )))
        }
    }
}

async fn make_canonical_broadcastable_changes<F, T>(
    agent: &Agent,
    timeout: Option<u64>,
    f: F,
) -> Result<(T, Option<CrsqlDbVersion>, Duration), ChangeError>
where
    F: FnOnce(&InterruptibleTransaction<Transaction>) -> Result<T, ChangeError>,
{
    let actor_id = agent.actor_id();
    let start = Instant::now();
    let ts = Timestamp::from(agent.clock().new_timestamp());
    let timeout = timeout.map(Duration::from_secs);

    let mut conn = agent.pool().write_priority().await?;
    let mut book_writer = agent
        .booked()
        .write::<&str, _>("make_canonical_broadcastable_changes(booked writer)", None)
        .await;

    let (ret, insert_info) = block_in_place(move || {
        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            })?;

        let tx = InterruptibleTransaction::new(tx, timeout, "local_changes");

        if let Err(source) = tx
            .prepare_cached("SELECT crsql_set_ts(?)")
            .and_then(|mut stmt| stmt.query_row([&ts], |row| row.get::<_, String>(0)))
        {
            let _ = tx.execute_batch("ROLLBACK;");
            return Err(ChangeError::Rusqlite {
                source,
                actor_id: Some(actor_id),
                version: None,
            });
        }

        let ret = match f(&tx) {
            Ok(ret) => ret,
            Err(err) => {
                let _ = tx.execute_batch("ROLLBACK;");
                return Err(err);
            }
        };

        let insert_info = match insert_local_changes(agent, &tx, &mut book_writer) {
            Ok(insert_info) => insert_info,
            Err(err) => {
                let _ = tx.execute_batch("ROLLBACK;");
                return Err(err);
            }
        };

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: insert_info.as_ref().map(|info| info.db_version),
        })?;

        let committed = insert_info.map(
            |InsertChangesInfo {
                 db_version,
                 last_seq,
                 ts,
                 snap,
             }| {
                book_writer.commit_snapshot(snap);
                (db_version, last_seq, ts)
            },
        );

        Ok::<_, ChangeError>((ret, committed))
    })?;

    let elapsed = start.elapsed();
    histogram!("corro.agent.changes.processing.time.seconds", "source" => "local").record(elapsed);

    match insert_info {
        None => Ok((ret, None, elapsed)),
        Some((db_version, last_seq, ts)) => {
            let agent = agent.clone();
            spawn_counted(async move { broadcast_changes(agent, db_version, last_seq, ts).await });

            Ok((ret, Some(db_version), elapsed))
        }
    }
}

fn transaction_is_replayable(agent: &Agent, statements: &[Statement]) -> bool {
    let schema = agent.schema().read();

    statements.iter().all(|statement| {
        let sql = statement.query().trim().trim_end_matches(';').trim();
        if sql.is_empty() {
            return false;
        }

        let mut parser = sqlite3_parser::lexer::sql::Parser::new(sql.as_bytes());

        let cmd = match parser.next() {
            Ok(Some(cmd)) => cmd,
            _ => return false,
        };

        if !matches!(parser.next(), Ok(None)) {
            return false;
        }

        let table = match cmd {
            Cmd::Stmt(
                SqliteStmt::Insert { tbl_name, .. }
                | SqliteStmt::Update { tbl_name, .. }
                | SqliteStmt::Delete { tbl_name, .. },
            ) if tbl_name.db_name.is_none() => tbl_name.name.0,
            _ => return false,
        };

        schema.tables.contains_key(&table)
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

fn execute_transaction_statements<T>(
    tx: &InterruptibleTransaction<T>,
    statements: &[Statement],
) -> Result<Vec<ExecResult>, ChangeError>
where
    T: Deref<Target = rusqlite::Connection> + Committable,
{
    let mut total_rows_affected = 0;

    statements
        .iter()
        .map(|stmt| {
            let start = Instant::now();
            let res = execute_statement(tx, stmt).map_err(|source| ChangeError::Rusqlite {
                source,
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
                Err(err) => Err(err),
            }
        })
        .collect()
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
    let replayable = transaction_is_replayable(&agent, &statements);

    let res = if replayable {
        match make_broadcastable_changes(&agent, params.timeout, |tx| {
            execute_transaction_statements(tx, &statements)
        })
        .await
        {
            Ok(SpeculativeChanges::Completed(result)) => Ok(result),
            Ok(SpeculativeChanges::RequiresCanonical) => {
                make_canonical_broadcastable_changes(&agent, params.timeout, |tx| {
                    execute_transaction_statements(tx, &statements)
                })
                .await
            }
            Err(err) => Err(err),
        }
    } else {
        make_canonical_broadcastable_changes(&agent, params.timeout, |tx| {
            execute_transaction_statements(tx, &statements)
        })
        .await
    };

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
    use futures::StreamExt;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use super::*;

    use crate::agent::setup;

    async fn setup_api_test_agent() -> eyre::Result<(tempfile::TempDir, Agent)> {
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

        let (status_code, _) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        Ok((dir, agent))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_api_non_replayable_transaction_uses_canonical_path() -> eyre::Result<()> {
        let (_dir, agent) = setup_api_test_agent().await?;

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![
                Statement::Simple(
                    "INSERT INTO tests (id, text) VALUES ('canonical-http', 'preserved')".into(),
                ),
                Statement::Simple(
                    "CREATE TABLE http_canonical_marker (id INTEGER PRIMARY KEY)".into(),
                ),
            ]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK, "{body:?}");

        let conn = agent.pool().read().await?;

        let value: String = conn.query_row(
            "SELECT text FROM tests WHERE id = 'canonical-http'",
            (),
            |row| row.get(0),
        )?;

        assert_eq!(value, "preserved");

        let marker_exists: bool = conn.query_row(
            "
            SELECT EXISTS (
                SELECT 1
                FROM sqlite_schema
                WHERE type = 'table'
                  AND name = 'http_canonical_marker'
            )
            ",
            (),
            |row| row.get(0),
        )?;

        assert!(marker_exists);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_api_user_trigger_forces_canonical_path() -> eyre::Result<()> {
        let (_dir, agent) = setup_api_test_agent().await?;

        let trigger_schema = "
            CREATE TABLE http_trigger_target (
                id INTEGER NOT NULL PRIMARY KEY,
                first TEXT NOT NULL DEFAULT '',
                second TEXT NOT NULL DEFAULT ''
            );
        ";

        let (status_code, _) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![TEST_SCHEMA.to_owned(), trigger_schema.to_owned()]),
        )
        .await;
        assert_eq!(status_code, StatusCode::OK);

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![
                Statement::Simple(
                    "CREATE TABLE http_trigger_audit (
                        id INTEGER PRIMARY KEY,
                        seen TEXT NOT NULL
                    )"
                    .into(),
                ),
                Statement::Simple(
                    "CREATE TRIGGER http_trigger_target_audit
                     AFTER INSERT ON http_trigger_target
                     BEGIN
                         INSERT INTO http_trigger_audit (id, seen)
                         VALUES (NEW.id, NEW.first || ':' || NEW.second);
                     END"
                    .into(),
                ),
            ]),
        )
        .await;
        assert_eq!(status_code, StatusCode::OK, "{body:?}");

        {
            let conn = agent.pool().read().await?;
            assert!(database_has_user_triggers(&conn)?);
        }

        let statements = vec![Statement::Simple(
            "INSERT INTO http_trigger_target (id, first, second)
             VALUES (1, 'left', 'right')"
                .into(),
        )];

        let writer = agent.pool().write_priority().await?;
        let mut request = tokio::spawn({
            let agent = agent.clone();
            async move {
                api_v1_transactions(
                    Extension(agent),
                    axum::extract::Query(TimeoutParams { timeout: None }),
                    axum::Json(statements),
                )
                .await
            }
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut request)
                .await
                .is_err()
        );

        drop(writer);

        let (status_code, body) =
            tokio::time::timeout(Duration::from_secs(5), request).await??;
        assert_eq!(status_code, StatusCode::OK, "{body:?}");

        let conn = agent.pool().read().await?;
        let (count, seen): (i64, String) = conn.query_row(
            "SELECT COUNT(*), MIN(seen) FROM http_trigger_audit",
            (),
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        assert_eq!(count, 1);
        assert_eq!(seen, "left:right");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_api_speculative_failure_rolls_back_and_releases_writer() -> eyre::Result<()> {
        let (_dir, agent) = setup_api_test_agent().await?;
        let booked_before = agent
            .booked()
            .read::<&str, _>("test_api_speculative_failure", None)
            .await
            .last();

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![
                Statement::Simple(
                    "INSERT INTO tests (id, text) VALUES ('http-speculative-failure', 'first')"
                        .into(),
                ),
                Statement::Simple(
                    "INSERT INTO tests (id, text) VALUES ('http-speculative-failure', 'duplicate')"
                        .into(),
                ),
            ]),
        )
        .await;

        assert_eq!(status_code, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.0.version.is_none());

        {
            let conn = agent.pool().read().await?;
            let rows: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tests WHERE id = 'http-speculative-failure'",
                (),
                |row| row.get(0),
            )?;
            assert_eq!(rows, 0);
        }

        let booked_after = agent
            .booked()
            .read::<&str, _>("test_api_speculative_failure", None)
            .await
            .last();
        assert_eq!(booked_after, booked_before);

        let (status_code, body) = tokio::time::timeout(
            Duration::from_secs(5),
            api_v1_transactions(
                Extension(agent.clone()),
                axum::extract::Query(TimeoutParams { timeout: None }),
                axum::Json(vec![Statement::Simple(
                    "INSERT INTO tests (id, text) VALUES ('http-speculative-recovery', 'ok')"
                        .into(),
                )]),
            ),
        )
        .await
        .expect("writer remained blocked after speculative rollback");

        assert_eq!(status_code, StatusCode::OK, "{body:?}");
        assert!(body.0.version.is_some());

        let conn = agent.pool().read().await?;
        let recovery_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tests WHERE id = 'http-speculative-recovery'",
            (),
            |row| row.get(0),
        )?;
        assert_eq!(recovery_rows, 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_api_canonical_failure_rolls_back_and_releases_writer() -> eyre::Result<()> {
        let (_dir, agent) = setup_api_test_agent().await?;

        {
            let conn = agent.pool().write_priority().await?;
            block_in_place(|| {
                conn.execute_batch(
                    "CREATE TABLE http_canonical_side (
                        id INTEGER PRIMARY KEY
                    )",
                )
            })?;
        }

        let booked_before = agent
            .booked()
            .read::<&str, _>("test_api_canonical_failure", None)
            .await
            .last();

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
            axum::extract::Query(TimeoutParams { timeout: None }),
            axum::Json(vec![
                Statement::Simple(
                    "INSERT INTO tests (id, text) VALUES ('http-canonical-failure', 'first')"
                        .into(),
                ),
                Statement::Simple("INSERT INTO http_canonical_side (id) VALUES (1)".into()),
                Statement::Simple("INSERT INTO http_canonical_side (id) VALUES (1)".into()),
            ]),
        )
        .await;

        assert_eq!(status_code, StatusCode::INTERNAL_SERVER_ERROR);
        assert!(body.0.version.is_none());

        {
            let conn = agent.pool().read().await?;
            let tracked_rows: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tests WHERE id = 'http-canonical-failure'",
                (),
                |row| row.get(0),
            )?;
            let side_rows: i64 = conn.query_row(
                "SELECT COUNT(*) FROM http_canonical_side WHERE id = 1",
                (),
                |row| row.get(0),
            )?;
            assert_eq!(tracked_rows, 0);
            assert_eq!(side_rows, 0);
        }

        let booked_after = agent
            .booked()
            .read::<&str, _>("test_api_canonical_failure", None)
            .await
            .last();
        assert_eq!(booked_after, booked_before);

        let (status_code, body) = tokio::time::timeout(
            Duration::from_secs(5),
            api_v1_transactions(
                Extension(agent.clone()),
                axum::extract::Query(TimeoutParams { timeout: None }),
                axum::Json(vec![Statement::Simple(
                    "INSERT INTO tests (id, text) VALUES ('http-canonical-recovery', 'ok')".into(),
                )]),
            ),
        )
        .await
        .expect("writer remained blocked after canonical rollback");

        assert_eq!(status_code, StatusCode::OK, "{body:?}");
        assert!(body.0.version.is_some());

        let conn = agent.pool().read().await?;
        let recovery_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tests WHERE id = 'http-canonical-recovery'",
            (),
            |row| row.get(0),
        )?;
        assert_eq!(recovery_rows, 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_api_schema_change_aborts_speculative_replay() -> eyre::Result<()> {
        let (_dir, agent) = setup_api_test_agent().await?;

        {
            let conn = agent.pool().write_priority().await?;
            block_in_place(|| {
                conn.execute_batch(
                    "
                    CREATE TABLE http_schema_race_audit (
                        id INTEGER PRIMARY KEY,
                        seen TEXT NOT NULL
                    );
                    ",
                )
            })?;
        }

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let speculative = {
            let agent = agent.clone();

            tokio::spawn(async move {
                make_broadcastable_changes(&agent, None, move |tx| {
                    tx.execute(
                        "INSERT INTO tests (id, text) VALUES ('schema-race', 'old-schema')",
                        &[],
                    )
                    .map_err(|source| ChangeError::Rusqlite {
                        source,
                        actor_id: None,
                        version: None,
                    })?;

                    let _ = entered_tx.send(());
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("speculative write was not released");

                    Ok(())
                })
                .await
            })
        };

        entered_rx.await?;

        tokio::time::timeout(Duration::from_secs(5), async {
            let mut conn = agent.pool().write_normal().await?;

            block_in_place(|| -> eyre::Result<()> {
                let tx = conn.immediate_transaction()?;
                tx.execute_batch(
                    "
                    CREATE TRIGGER tests_schema_race_audit
                    AFTER INSERT ON tests
                    BEGIN
                        INSERT INTO http_schema_race_audit (id, seen)
                        VALUES (NEW.id, NEW.text);
                    END;
                    ",
                )?;
                tx.commit()?;
                Ok(())
            })
        })
        .await
        .expect("schema change was blocked by speculative transaction")??;

        release_tx.send(())?;

        let result = speculative.await?;
        assert!(matches!(result, Err(ChangeError::SchemaChanged { .. })));

        let conn = agent.pool().read().await?;
        let tracked_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tests WHERE id = 'schema-race'",
            (),
            |row| row.get(0),
        )?;
        let audit_rows: i64 =
            conn.query_row("SELECT COUNT(*) FROM http_schema_race_audit", (), |row| {
                row.get(0)
            })?;

        assert_eq!(tracked_rows, 0);
        assert_eq!(audit_rows, 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_api_speculative_write_does_not_block_normal_writer() -> eyre::Result<()> {
        let (_dir, agent) = setup_api_test_agent().await?;

        {
            let conn = agent.pool().write_priority().await?;
            block_in_place(|| {
                conn.execute_batch(
                    "
                    CREATE TABLE issue503_writer_probe (
                        id INTEGER PRIMARY KEY
                    );
                    ",
                )
            })?;
        }

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();

        let speculative = {
            let agent = agent.clone();

            tokio::spawn(async move {
                make_broadcastable_changes(&agent, None, move |tx| {
                    tx.execute(
                        "INSERT INTO tests (id, text) VALUES ('speculative-open', 'client')",
                        &[],
                    )
                    .map_err(|source| ChangeError::Rusqlite {
                        source,
                        actor_id: None,
                        version: None,
                    })?;

                    let _ = entered_tx.send(());
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("speculative write was not released");

                    Ok(())
                })
                .await
            })
        };

        entered_rx.await?;

        tokio::time::timeout(Duration::from_secs(5), async {
            let mut conn = agent.pool().write_normal().await?;

            block_in_place(|| -> eyre::Result<()> {
                let tx = conn.immediate_transaction()?;
                tx.execute("INSERT INTO issue503_writer_probe (id) VALUES (1)", ())?;
                tx.commit()?;
                Ok(())
            })
        })
        .await
        .expect("normal writer was blocked by speculative client")?;

        release_tx.send(())?;

        let (_, version, _) = speculative.await??.into_completed();
        assert!(version.is_some());

        let conn = agent.pool().read().await?;

        let client_row: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tests WHERE id = 'speculative-open'",
            (),
            |row| row.get(0),
        )?;

        let normal_row: i64 = conn.query_row(
            "SELECT COUNT(*) FROM issue503_writer_probe WHERE id = 1",
            (),
            |row| row.get(0),
        )?;

        assert_eq!(client_row, 1);
        assert_eq!(normal_row, 1);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_api_writes_execute_speculatively_in_parallel() -> eyre::Result<()> {
        use std::sync::{Arc, Condvar, Mutex};
        use std::time::Duration;

        fn enter(gate: &(Mutex<usize>, Condvar)) -> bool {
            let mut count = gate.0.lock().unwrap();
            *count += 1;
            gate.1.notify_all();

            let (count, timeout) = gate
                .1
                .wait_timeout_while(count, Duration::from_secs(5), |count| *count < 2)
                .unwrap();

            *count >= 2 && !timeout.timed_out()
        }

        let (_dir, agent) = setup_api_test_agent().await?;

        let gate = Arc::new((Mutex::new(0usize), Condvar::new()));

        let first = {
            let agent = agent.clone();
            let gate = gate.clone();

            tokio::spawn(async move {
                make_broadcastable_changes(&agent, None, move |tx| {
                    let concurrent = enter(&gate);

                    tx.execute(
                        "INSERT INTO tests (id, text) VALUES ('parallel-1', 'first')",
                        &[],
                    )
                    .map_err(|source| ChangeError::Rusqlite {
                        source,
                        actor_id: None,
                        version: None,
                    })?;

                    Ok(concurrent)
                })
                .await
            })
        };

        let second = {
            let agent = agent.clone();
            let gate = gate.clone();

            tokio::spawn(async move {
                make_broadcastable_changes(&agent, None, move |tx| {
                    let concurrent = enter(&gate);

                    tx.execute(
                        "INSERT INTO tests (id, text) VALUES ('parallel-2', 'second')",
                        &[],
                    )
                    .map_err(|source| ChangeError::Rusqlite {
                        source,
                        actor_id: None,
                        version: None,
                    })?;

                    Ok(concurrent)
                })
                .await
            })
        };

        let first = first.await??.into_completed();
        let second = second.await??.into_completed();

        assert!(first.0);
        assert!(second.0);
        assert!(first.1.is_some());
        assert!(second.1.is_some());
        assert_ne!(first.1, second.1);

        let conn = agent.pool().read().await?;
        let rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tests WHERE id IN ('parallel-1', 'parallel-2')",
            (),
            |row| row.get(0),
        )?;

        assert_eq!(rows, 2);

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
