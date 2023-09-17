use std::{
    iter::Peekable,
    ops::RangeInclusive,
    time::{Duration, Instant},
};

use axum::{response::IntoResponse, Extension};
use bytes::{BufMut, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    agent::{Agent, ChangeError, KnownDbVersion},
    api::{QueryEvent, RqliteResponse, RqliteResult, Statement},
    broadcast::{ChangeV1, Changeset, Timestamp},
    change::{row_to_change, SqliteValue},
    schema::{make_schema_inner, parse_sql},
    sqlite::SqlitePoolError,
};
use hyper::StatusCode;
use itertools::Itertools;
use metrics::counter;
use rusqlite::{params, params_from_iter, ToSql, Transaction};
use spawn::spawn_counted;
use tokio::{
    sync::{
        mpsc::{self, channel},
        oneshot,
    },
    task::block_in_place,
};
use tracing::{debug, error, info, trace};

use corro_types::{
    broadcast::{BroadcastInput, BroadcastV1},
    change::Change,
};

use crate::agent::process_subs;

pub const MAX_CHANGES_PER_MESSAGE: usize = 50;

pub struct ChunkedChanges<I: Iterator> {
    iter: Peekable<I>,
    changes: Vec<Change>,
    last_pushed_seq: i64,
    last_start_seq: i64,
    last_seq: i64,
    chunk_size: usize,
    done: bool,
}

impl<I> ChunkedChanges<I>
where
    I: Iterator,
{
    pub fn new(iter: I, start_seq: i64, last_seq: i64, chunk_size: usize) -> Self {
        Self {
            iter: iter.peekable(),
            changes: vec![],
            last_pushed_seq: 0,
            last_start_seq: start_seq,
            last_seq,
            chunk_size,
            done: false,
        }
    }
}

impl<I> Iterator for ChunkedChanges<I>
where
    I: Iterator<Item = rusqlite::Result<Change>>,
{
    type Item = Result<(Vec<Change>, RangeInclusive<i64>), rusqlite::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // previously marked as done because the Rows iterator returned None
        if self.done {
            return None;
        }

        loop {
            trace!("chunking through the rows iterator");
            match self.iter.next() {
                Some(Ok(change)) => {
                    trace!("got change: {change:?}");

                    self.last_pushed_seq = change.seq;

                    self.changes.push(change);

                    if self.last_pushed_seq == self.last_seq {
                        // this was the last seq! break early
                        break;
                    }

                    if self.changes.len() >= self.chunk_size {
                        // chunking it up
                        let start_seq = self.last_start_seq;

                        if self.iter.peek().is_none() {
                            // no more rows, break early
                            break;
                        }

                        // prepare for next round! we're not done...
                        self.last_start_seq = self.last_pushed_seq + 1;

                        return Some(Ok((
                            self.changes.drain(..).collect(),
                            start_seq..=self.last_pushed_seq,
                        )));
                    }
                }
                None => {
                    // probably not going to happen since we peek at the next and end early
                    // break out of the loop, don't return, there might be buffered changes
                    break;
                }
                Some(Err(e)) => return Some(Err(e)),
            }
        }

        self.done = true;

        // return buffered changes
        Some(Ok((
            self.changes.clone(),                // no need to drain here like before
            self.last_start_seq..=self.last_seq, // even if empty, this is all we have still applied
        )))
    }
}

pub async fn make_broadcastable_changes<F, T>(
    agent: &Agent,
    f: F,
) -> Result<(T, Duration), ChangeError>
where
    F: Fn(&Transaction) -> Result<T, ChangeError>,
{
    trace!("getting conn...");
    let mut conn = agent.pool().write_priority().await?;
    trace!("got conn");

    let actor_id = agent.actor_id();
    let booked = agent.bookie().for_actor(actor_id).await;
    // maybe we should do this earlier, but there can only ever be 1 write conn at a time,
    // so it probably doesn't matter too much, except for reads of internal state
    let mut book_writer = booked.write().await;

    let start = Instant::now();
    block_in_place(move || {
        let tx = conn.transaction()?;

        // Execute whatever might mutate state data
        let ret = f(&tx)?;

        let ts = Timestamp::from(agent.clock().new_timestamp());

        let db_version: i64 = tx
            .prepare_cached("SELECT crsql_next_db_version()")?
            .query_row((), |row| row.get(0))?;

        let has_changes: bool = tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM crsql_changes WHERE site_id IS NULL AND db_version = ?);",
        )?
        .query_row([db_version], |row| row.get(0))?;

        if !has_changes {
            tx.commit()?;
            return Ok((ret, start.elapsed()));
        }

        let last_seq: i64 = tx
            .prepare_cached(
                "SELECT MAX(seq) FROM crsql_changes WHERE site_id IS NULL AND db_version = ?",
            )?
            .query_row([db_version], |row| row.get(0))?;

        let last_version = book_writer.last().unwrap_or(0);
        trace!("last_version: {last_version}");
        let version = last_version + 1;
        trace!("version: {version}");

        let elapsed = {
            tx.prepare_cached(
                r#"
                INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                    VALUES (?, ?, ?, ?, ?);
            "#,
            )?
            .execute(params![actor_id, version, db_version, last_seq, ts])?;

            tx.commit()?;
            start.elapsed()
        };

        trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

        book_writer.insert(
            version,
            KnownDbVersion::Current {
                db_version,
                last_seq,
                ts,
            },
        );
        drop(book_writer);

        let agent = agent.clone();

        spawn_counted(async move {
            let conn = agent.pool().read().await?;

            block_in_place(|| {
                // TODO: make this more generic so both sync and local changes can use it.
                let mut prepped = conn.prepare_cached(r#"
                    SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl
                        FROM crsql_changes
                        WHERE site_id IS NULL
                          AND db_version = ?
                        ORDER BY seq ASC
                "#)?;
                let rows = prepped.query_map([db_version], row_to_change)?;
                let chunked = ChunkedChanges::new(rows, 0, last_seq, MAX_CHANGES_PER_MESSAGE);
                for changes_seqs in chunked {
                    match changes_seqs {
                        Ok((changes, seqs)) => {
                            for (table_name, count) in
                                changes.iter().counts_by(|change| &change.table)
                            {
                                counter!("corro.changes.committed", count as u64, "table" => table_name.to_string(), "source" => "local");
                            }
                            process_subs(&agent, &changes);

                            trace!("broadcasting changes: {changes:?} for seq: {seqs:?}");

                            let tx_bcast = agent.tx_bcast().clone();
                            tokio::spawn(async move {
                                if let Err(e) = tx_bcast
                                    .send(BroadcastInput::AddBroadcast(BroadcastV1::Change(
                                        ChangeV1 {
                                            actor_id,
                                            changeset: Changeset::Full {
                                                version,
                                                changes,
                                                seqs,
                                                last_seq,
                                                ts,
                                            },
                                        },
                                    )))
                                    .await
                                {
                                    error!("could not send change message for broadcast: {e}");
                                }
                            });
                        }
                        Err(e) => {
                            error!("could not process crsql change (db_version: {db_version}) for broadcast: {e}");
                            break;
                        }
                    }
                }
                Ok::<_, rusqlite::Error>(())
            })?;

            Ok::<_, eyre::Report>(())
        });

        Ok::<_, ChangeError>((ret, elapsed))
    })
}

fn execute_statement(tx: &Transaction, stmt: &Statement) -> rusqlite::Result<usize> {
    match stmt {
        Statement::Simple(q) => tx.execute(q, []),
        Statement::WithParams(q, params) => tx.execute(q, params_from_iter(params)),
        Statement::WithNamedParams(q, params) => tx.execute(
            q,
            params
                .iter()
                .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                .collect::<Vec<(&str, &dyn ToSql)>>()
                .as_slice(),
        ),
    }
}

pub async fn api_v1_transactions(
    // axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Extension(agent): Extension<Agent>,
    axum::extract::Json(statements): axum::extract::Json<Vec<Statement>>,
) -> (StatusCode, axum::Json<RqliteResponse>) {
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(RqliteResponse {
                results: vec![RqliteResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: None,
            }),
        );
    }

    let res = make_broadcastable_changes(&agent, move |tx| {
        let mut total_rows_affected = 0;

        let results = statements
            .iter()
            .map(|stmt| {
                let start = Instant::now();
                let res = execute_statement(tx, stmt);

                match res {
                    Ok(rows_affected) => {
                        total_rows_affected += rows_affected;
                        RqliteResult::Execute {
                            rows_affected,
                            time: Some(start.elapsed().as_secs_f64()),
                        }
                    }
                    Err(e) => RqliteResult::Error {
                        error: e.to_string(),
                    },
                }
            })
            .collect::<Vec<RqliteResult>>();

        Ok(results)
    })
    .await;

    let (results, elapsed) = match res {
        Ok(res) => res,
        Err(e) => {
            error!("could not execute statement(s): {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(RqliteResponse {
                    results: vec![RqliteResult::Error {
                        error: e.to_string(),
                    }],
                    time: None,
                }),
            );
        }
    };

    (
        StatusCode::OK,
        axum::Json(RqliteResponse {
            results,
            time: Some(elapsed.as_secs_f64()),
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
    data_tx: mpsc::Sender<QueryEvent>,
    stmt: Statement,
) -> Result<(), (StatusCode, RqliteResult)> {
    let (res_tx, res_rx) = oneshot::channel();

    let pool = agent.pool().clone();

    tokio::spawn(async move {
        let conn = match pool.read().await {
            Ok(conn) => conn,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    RqliteResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        let prepped_res = block_in_place(|| match &stmt {
            Statement::Simple(q) => conn.prepare(q),
            Statement::WithParams(q, _) => conn.prepare(q),
            Statement::WithNamedParams(q, _) => conn.prepare(q),
        });

        let mut prepped = match prepped_res {
            Ok(prepped) => prepped,
            Err(e) => {
                _ = res_tx.send(Err((
                    StatusCode::BAD_REQUEST,
                    RqliteResult::Error {
                        error: e.to_string(),
                    },
                )));
                return;
            }
        };

        block_in_place(|| {
            let col_count = prepped.column_count();
            trace!("inside block in place, col count: {col_count}");

            if let Err(e) = data_tx.blocking_send(QueryEvent::Columns(
                prepped
                    .columns()
                    .into_iter()
                    .map(|col| col.name().to_compact_string())
                    .collect(),
            )) {
                error!("could not send back columns: {e}");
                return;
            }

            let start = Instant::now();

            let query = match stmt {
                Statement::Simple(_) => prepped.query(()),
                Statement::WithParams(_, params) => prepped.query(params_from_iter(params)),
                Statement::WithNamedParams(_, params) => prepped.query(
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
                        RqliteResult::Error {
                            error: e.to_string(),
                        },
                    )));
                    return;
                }
            };
            let elapsed = start.elapsed();

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
                                if let Err(e) = data_tx.blocking_send(QueryEvent::Row(rowid, cells))
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
            });
        });
    });

    match res_rx.await {
        Ok(res) => res,
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            RqliteResult::Error {
                error: e.to_string(),
            },
        )),
    }
}

pub async fn api_v1_queries(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (mut tx, body) = hyper::Body::channel();

    // TODO: timeout on data send instead of infinitely waiting for channel space.
    let (data_tx, mut data_rx) = channel(512);

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

    match build_query_rows_response(&agent, data_tx, stmt).await {
        Ok(_) => {
            #[allow(clippy::needless_return)]
            return hyper::Response::builder()
                .status(StatusCode::OK)
                .body(body)
                .expect("could not build query response body");
        }
        Err((status, res)) => {
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

async fn execute_schema(agent: &Agent, statements: Vec<String>) -> eyre::Result<()> {
    let new_sql: String = statements.join(";");

    let partial_schema = parse_sql(&new_sql)?;

    let mut conn = agent.pool().write_priority().await?;

    // hold onto this lock so nothing else makes changes
    let mut schema_write = agent.schema().write();

    // clone the previous schema and apply
    let new_schema = {
        let mut schema = schema_write.clone();
        for (name, def) in partial_schema.tables.iter() {
            // overwrite table because users are expected to return a full table def
            schema.tables.insert(name.clone(), def.clone());
        }
        schema
    };

    block_in_place(|| {
        let tx = conn.transaction()?;

        make_schema_inner(&tx, &schema_write, &new_schema)?;

        for tbl_name in partial_schema.tables.keys() {
            tx.execute("DELETE FROM __corro_schema WHERE tbl_name = ?", [tbl_name])?;

            let n = tx.execute("INSERT INTO __corro_schema SELECT tbl_name, type, name, sql, 'api' AS source FROM sqlite_schema WHERE tbl_name = ? AND type IN ('table', 'index') AND name IS NOT NULL AND sql IS NOT NULL", [tbl_name])?;
            info!("updated {n} rows in __corro_schema for table {tbl_name}");
        }

        tx.commit()?;

        Ok::<_, eyre::Report>(())
    })?;

    *schema_write = new_schema;

    Ok(())
}

pub async fn api_v1_db_schema(
    Extension(agent): Extension<Agent>,
    axum::extract::Json(statements): axum::extract::Json<Vec<String>>,
) -> (StatusCode, axum::Json<RqliteResponse>) {
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(RqliteResponse {
                results: vec![RqliteResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: None,
            }),
        );
    }

    let start = Instant::now();

    if let Err(e) = execute_schema(&agent, statements).await {
        error!("could not merge schemas: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(RqliteResponse {
                results: vec![RqliteResult::Error {
                    error: e.to_string(),
                }],
                time: None,
            }),
        );
    }

    (
        StatusCode::OK,
        axum::Json(RqliteResponse {
            results: vec![],
            time: Some(start.elapsed().as_secs_f64()),
        }),
    )
}

#[cfg(test)]
mod tests {
    use arc_swap::ArcSwap;
    use bytes::Bytes;
    use corro_types::{actor::ActorId, agent::SplitPool, config::Config, schema::SqliteType};
    use futures::Stream;
    use http_body::{combinators::UnsyncBoxBody, Body};
    use tokio::sync::mpsc::{channel, error::TryRecvError};
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;
    use uuid::Uuid;

    use super::*;

    use crate::agent::migrate;

    struct UnsyncBodyStream(std::pin::Pin<Box<UnsyncBoxBody<Bytes, axum::Error>>>);

    impl Stream for UnsyncBodyStream {
        type Item = Result<Bytes, axum::Error>;

        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.0.as_mut().poll_data(cx)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_execute() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, _tripwire_worker, _tripwire_tx) = Tripwire::new_simple();

        let dir = tempfile::tempdir()?;

        let pool = SplitPool::create(dir.path().join("./test.sqlite"), tripwire.clone()).await?;

        {
            let mut conn = pool.write_priority().await?;
            migrate(&mut conn)?;
        }

        let (tx_bcast, mut rx_bcast) = channel(1);
        let (tx_apply, _rx_apply) = channel(1);

        let agent = Agent::new(corro_types::agent::AgentConfig {
            actor_id: ActorId(Uuid::new_v4()),
            pool,
            config: ArcSwap::from_pointee(
                Config::builder()
                    .db_path(dir.path().join("corrosion.db").display().to_string())
                    .gossip_addr("127.0.0.1:1234".parse()?)
                    .api_addr("127.0.0.1:8080".parse()?)
                    .build()?,
            ),
            gossip_addr: "127.0.0.1:0".parse().unwrap(),
            api_addr: "127.0.0.1:0".parse().unwrap(),
            members: Default::default(),
            clock: Default::default(),
            bookie: Default::default(),
            tx_bcast,
            tx_apply,
            schema: Default::default(),
            tripwire,
        });

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
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
                changeset: Changeset::Full { version: 1, .. },
                ..
            }))
        ));

        assert_eq!(agent.bookie().last(&agent.actor_id()).await, Some(1));

        println!("second req...");

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
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

        let pool = SplitPool::create(dir.path().join("./test.sqlite"), tripwire.clone()).await?;

        {
            let mut conn = pool.write_priority().await?;
            migrate(&mut conn)?;
        }

        let (tx_bcast, _rx_bcast) = channel(1);
        let (tx_apply, _rx_apply) = channel(1);

        let agent = Agent::new(corro_types::agent::AgentConfig {
            actor_id: ActorId(Uuid::new_v4()),
            pool,
            config: ArcSwap::from_pointee(
                Config::builder()
                    .db_path(dir.path().join("corrosion.db").display().to_string())
                    .gossip_addr("127.0.0.1:1234".parse()?)
                    .api_addr("127.0.0.1:8080".parse()?)
                    .build()?,
            ),
            gossip_addr: "127.0.0.1:0".parse().unwrap(),
            api_addr: "127.0.0.1:0".parse().unwrap(),
            members: Default::default(),
            clock: Default::default(),
            bookie: Default::default(),
            tx_bcast,
            tx_apply,
            schema: Default::default(),
            tripwire,
        });

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        let (status_code, body) = api_v1_transactions(
            Extension(agent.clone()),
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
            QueryEvent::Row(1, vec!["service-id".into(), "service-name".into()])
        );

        buf.extend_from_slice(&body.data().await.unwrap()?);

        let s = lines.decode(&mut buf).unwrap().unwrap();

        let row: QueryEvent = serde_json::from_str(&s).unwrap();

        assert_eq!(
            row,
            QueryEvent::Row(2, vec!["service-id-2".into(), "service-name-2".into()])
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

        let pool = SplitPool::create(dir.path().join("./test.sqlite"), tripwire.clone()).await?;

        {
            let mut conn = pool.write_priority().await?;
            migrate(&mut conn)?;
        }

        let (tx_bcast, _rx_bcast) = channel(1);
        let (tx_apply, _rx_apply) = channel(1);

        let agent = Agent::new(corro_types::agent::AgentConfig {
            actor_id: ActorId(Uuid::new_v4()),
            pool,
            config: ArcSwap::from_pointee(
                Config::builder()
                    .db_path(dir.path().join("corrosion.db").display().to_string())
                    .gossip_addr("127.0.0.1:1234".parse()?)
                    .api_addr("127.0.0.1:8080".parse()?)
                    .build()?,
            ),
            gossip_addr: "127.0.0.1:0".parse().unwrap(),
            api_addr: "127.0.0.1:0".parse().unwrap(),
            members: Default::default(),
            clock: Default::default(),
            bookie: Default::default(),
            tx_bcast,
            tx_apply,
            schema: Default::default(),
            tripwire,
        });

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![
                "CREATE TABLE tests (id BIGINT PRIMARY KEY, foo TEXT);".into(),
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
            assert_eq!(id_col.sql_type, SqliteType::Integer);
            assert!(id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type, SqliteType::Text);
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);
        }

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![
                "CREATE TABLE tests2 (id BIGINT PRIMARY KEY, foo TEXT);".into(),
                "CREATE TABLE tests (id BIGINT PRIMARY KEY, foo TEXT);".into(),
            ]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        let schema = agent.schema().read();
        let tests = schema
            .tables
            .get("tests")
            .expect("no tests table in schema");

        let id_col = tests.columns.get("id").unwrap();
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.sql_type, SqliteType::Integer);
        assert!(id_col.nullable);
        assert!(id_col.primary_key);

        let foo_col = tests.columns.get("foo").unwrap();
        assert_eq!(foo_col.name, "foo");
        assert_eq!(foo_col.sql_type, SqliteType::Text);
        assert!(foo_col.nullable);
        assert!(!foo_col.primary_key);

        let tests = schema
            .tables
            .get("tests2")
            .expect("no tests2 table in schema");

        let id_col = tests.columns.get("id").unwrap();
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.sql_type, SqliteType::Integer);
        assert!(id_col.nullable);
        assert!(id_col.primary_key);

        let foo_col = tests.columns.get("foo").unwrap();
        assert_eq!(foo_col.name, "foo");
        assert_eq!(foo_col.sql_type, SqliteType::Text);
        assert!(foo_col.nullable);
        assert!(!foo_col.primary_key);

        Ok(())
    }

    #[test]
    fn test_change_chunker() {
        // empty interator
        let mut chunker = ChunkedChanges::new(vec![].into_iter(), 0, 100, 50);

        assert_eq!(chunker.next(), Some(Ok((vec![], 0..=100))));
        assert_eq!(chunker.next(), None);

        let changes: Vec<Change> = (0..100)
            .map(|seq| Change {
                seq,
                ..Default::default()
            })
            .collect();

        // 2 iterations
        let mut chunker = ChunkedChanges::new(
            vec![
                Ok(changes[0].clone()),
                Ok(changes[1].clone()),
                Ok(changes[2].clone()),
            ]
            .into_iter(),
            0,
            100,
            2,
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[0].clone(), changes[1].clone()], 0..=1)))
        );
        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[2].clone()], 2..=100)))
        );
        assert_eq!(chunker.next(), None);

        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[1].clone())].into_iter(),
            0,
            0,
            1,
        );

        assert_eq!(chunker.next(), Some(Ok((vec![changes[0].clone()], 0..=0))));
        assert_eq!(chunker.next(), None);

        // gaps
        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[2].clone())].into_iter(),
            0,
            100,
            2,
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[0].clone(), changes[2].clone()], 0..=100)))
        );

        assert_eq!(chunker.next(), None);

        // gaps
        let mut chunker = ChunkedChanges::new(
            vec![
                Ok(changes[2].clone()),
                Ok(changes[4].clone()),
                Ok(changes[7].clone()),
                Ok(changes[8].clone()),
            ]
            .into_iter(),
            0,
            100,
            50,
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((
                vec![
                    changes[2].clone(),
                    changes[4].clone(),
                    changes[7].clone(),
                    changes[8].clone()
                ],
                0..=100
            )))
        );

        assert_eq!(chunker.next(), None);

        // gaps
        let mut chunker = ChunkedChanges::new(
            vec![
                Ok(changes[2].clone()),
                Ok(changes[4].clone()),
                Ok(changes[7].clone()),
                Ok(changes[8].clone()),
            ]
            .into_iter(),
            0,
            10,
            2,
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[2].clone(), changes[4].clone(),], 0..=4)))
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[7].clone(), changes[8].clone(),], 5..=10)))
        );

        assert_eq!(chunker.next(), None);
    }
}
