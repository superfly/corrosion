use std::{
    ops::RangeInclusive,
    time::{Duration, Instant},
};

use axum::Extension;
use bb8::RunError;
use compact_str::{CompactString, ToCompactString};
use corro_types::{
    agent::{Agent, KnownDbVersion},
    api::{QueryResultBuilder, RqliteResponse, RqliteResult, Statement},
    broadcast::{Changeset, Timestamp},
    schema::{make_schema_inner, parse_sql},
    sqlite::SqlitePool,
};
use hyper::StatusCode;
use rusqlite::{params, params_from_iter, Row, ToSql, Transaction};
use spawn::spawn_counted;
use tokio::task::block_in_place;
use tracing::{error, info, trace};

use corro_types::{
    broadcast::{BroadcastInput, Message, MessageV1},
    change::Change,
};

use crate::agent::process_subs;

pub const MAX_CHANGES_PER_MESSAGE: usize = 50;

// TODO: accept a few options
// #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
// #[serde(rename_all = "snake_case")]
// pub struct RqliteRequestOptions {
//     pretty: Option<bool>,
//     timings: Option<bool>,
//     transaction: Option<bool>,
//     q: Option<String>,
// }

#[derive(Debug, thiserror::Error)]
pub enum ChangeError {
    #[error("could not acquire pooled connection: {0}")]
    ConnAcquisition(#[from] RunError<bb8_rusqlite::Error>),
    #[error("rusqlite: {0}")]
    Rusqlite(#[from] rusqlite::Error),
    #[error("too many rows impacted")]
    TooManyRowsImpacted,
}

pub fn row_to_change(row: &Row) -> Result<Change, rusqlite::Error> {
    Ok(Change {
        table: row.get(0)?,
        pk: row.get(1)?,
        cid: row.get(2)?,
        val: row.get(3)?,
        col_version: row.get(4)?,
        db_version: row.get(5)?,
        seq: row.get(6)?,
        site_id: row.get(7)?,
    })
}

pub struct ChunkedChanges<I> {
    iter: I,
    changes: Vec<Change>,
    last_pushed_seq: i64,
    last_start_seq: i64,
    last_seq: i64,
    chunk_size: usize,
    done: bool,
}

impl<I> ChunkedChanges<I> {
    pub fn new(iter: I, start_seq: i64, last_seq: i64, chunk_size: usize) -> Self {
        Self {
            iter,
            changes: vec![],
            last_pushed_seq: 0,
            last_start_seq: start_seq,
            last_seq,
            chunk_size,
            done: false,
        }
    }
}

impl<'stmt, I> Iterator for ChunkedChanges<I>
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
                        self.done = true;
                        break;
                    }

                    if self.changes.len() >= self.chunk_size {
                        // chunking it up
                        let start_seq = self.last_start_seq;
                        self.last_start_seq = self.last_pushed_seq + 1;
                        return Some(Ok((
                            self.changes.drain(..).collect(),
                            start_seq..=self.last_pushed_seq,
                        )));
                    }
                }
                None => {
                    // marking as done for the next `next()` call
                    self.done = true;
                    // break out of the loop, don't return, there might be buffered changes
                    break;
                }
                Some(Err(e)) => return Some(Err(e)),
            }
        }

        // return buffered changes
        return Some(Ok((
            self.changes.clone(),                // no need to drain here like before
            self.last_start_seq..=self.last_seq, // even if empty, this is all we have still applied
        )));
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
    let mut conn = agent.read_write_pool().get().await?;
    trace!("got conn");

    let actor_id = agent.actor_id();

    let start = Instant::now();
    block_in_place(move || {
        let tx = conn.transaction()?;

        // Execute whatever might mutate state data
        let ret = f(&tx)?;

        let ts = Timestamp::from(agent.clock().new_timestamp());

        let db_version: i64 = tx
            .prepare_cached("SELECT crsql_nextdbversion()")?
            .query_row((), |row| row.get(0))?;

        let has_changes: bool = tx
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM crsql_changes WHERE site_id IS NULL AND db_version = ?);",
        )?
        .query_row([db_version], |row| row.get(0))?;

        let last_seq: Option<i64> = if has_changes {
            tx.prepare_cached(
                "SELECT MAX(seq) FROM crsql_changes WHERE site_id IS NULL AND db_version = ?",
            )?
            .query_row([db_version], |row| row.get(0))?
        } else {
            None
        };

        let booked = agent.bookie().for_actor(actor_id);
        let mut book_writer = booked.write();

        let last_version = book_writer.last().unwrap_or(0);
        trace!("last_version: {last_version}");
        let version = last_version + 1;
        trace!("version: {version}");

        let elapsed = {
            if let Some(last_seq) = last_seq {
                tx.prepare_cached(
                    r#"
                INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                    VALUES (?, ?, ?, ?, ?);
            "#,
                )?
                .execute(params![actor_id, version, db_version, last_seq, ts])?;
            }

            tx.commit()?;
            start.elapsed()
        };

        trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

        if let Some(last_seq) = last_seq {
            book_writer.insert(
                version,
                KnownDbVersion::Current {
                    db_version,
                    last_seq,
                    ts,
                },
            );

            let agent = agent.clone();

            spawn_counted(async move {
                let conn = agent.read_only_pool().get().await?;

                block_in_place(|| {
                    let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()) FROM crsql_changes WHERE site_id IS NULL AND db_version = ?"#)?;
                    let rows = prepped.query_map([db_version], row_to_change)?;
                    let mut chunked =
                        ChunkedChanges::new(rows, 0, last_seq, MAX_CHANGES_PER_MESSAGE);
                    while let Some(changes_seqs) = chunked.next() {
                        match changes_seqs {
                            Ok((changes, seqs)) => {
                                process_subs(&agent, &changes, db_version);

                                trace!("broadcasting changes: {changes:?} for seq: {seqs:?}");

                                let tx_bcast = agent.tx_bcast().clone();
                                tokio::spawn(async move {
                                    if let Err(e) = tx_bcast
                                        .send(BroadcastInput::AddBroadcast(Message::V1(
                                            MessageV1::Change {
                                                actor_id,
                                                version,
                                                changeset: Changeset::Full {
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
        }

        Ok::<_, ChangeError>((ret, elapsed))
    })
}

fn execute_statement(tx: &Transaction, stmt: &Statement) -> rusqlite::Result<usize> {
    match stmt {
        Statement::Simple(q) => tx.execute(&q, []),
        Statement::WithParams(params) => {
            let mut params = params.into_iter();

            let first = params.next();
            match first.as_ref().and_then(|q| q.as_str()) {
                Some(q) => tx.execute(&q, params_from_iter(params)),
                None => Ok(0),
            }
        }
        Statement::WithNamedParams(q, params) => tx.execute(
            &q,
            params
                .iter()
                .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                .collect::<Vec<(&str, &dyn ToSql)>>()
                .as_slice(),
        ),
    }
}

pub async fn api_v1_db_execute(
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
            .filter_map(|stmt| {
                let start = Instant::now();
                let res = execute_statement(&tx, stmt);

                Some(match res {
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
                })
            })
            .collect::<Vec<RqliteResult>>();

        Ok(results)
    })
    .await;

    let (results, elapsed) = match res {
        Ok(res) => res,
        Err(e) => match e {
            ChangeError::TooManyRowsImpacted => {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(RqliteResponse {
                        results: vec![RqliteResult::Error {
                            error: format!("too many changed columns, please restrict the number of statements per request to {}", agent.config().max_change_size),
                        }],
                        time: None,
                    }),
                );
            }
            e => {
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
        },
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
    Pool(#[from] bb8::RunError<bb8_rusqlite::Error>),
    #[error("sqlite error: {0}")]
    Rusqlite(#[from] rusqlite::Error),
}

async fn query_statements(
    pool: &SqlitePool,
    statements: &[Statement],
    associative: bool,
) -> Result<Vec<RqliteResult>, QueryError> {
    let conn = pool.get().await?;

    let mut results = vec![];

    block_in_place(|| {
        for stmt in statements.iter() {
            let start = Instant::now();
            let prepped_res = match stmt {
                Statement::Simple(q) => conn.prepare(q.as_str()),
                Statement::WithParams(params) => match params.first().and_then(|v| v.as_str()) {
                    Some(q) => conn.prepare(q),
                    None => {
                        let builder = QueryResultBuilder::new(
                            vec![],
                            vec![],
                            Some(start.elapsed().as_secs_f64()),
                        );
                        results.push(if associative {
                            builder.build_associative()
                        } else {
                            builder.build_associative()
                        });
                        continue;
                    }
                },
                Statement::WithNamedParams(q, _) => conn.prepare(q.as_str()),
            };

            let mut prepped = match prepped_res {
                Ok(prepped) => prepped,
                Err(e) => {
                    results.push(RqliteResult::Error {
                        error: e.to_string(),
                    });
                    continue;
                }
            };

            let col_names: Vec<CompactString> = prepped
                .column_names()
                .into_iter()
                .map(|s| s.to_compact_string())
                .collect();

            let col_types: Vec<Option<CompactString>> = prepped
                .columns()
                .into_iter()
                .map(|c| c.decl_type().map(|t| t.to_compact_string()))
                .collect();

            let rows_res = match stmt {
                Statement::Simple(_) => prepped.query(()),
                Statement::WithParams(params) => {
                    let mut iter = params.iter();
                    // skip 1
                    iter.next();
                    prepped.query(params_from_iter(iter))
                }
                Statement::WithNamedParams(_, params) => prepped.query(
                    params
                        .iter()
                        .map(|(k, v)| (k.as_str(), v as &dyn ToSql))
                        .collect::<Vec<(&str, &dyn ToSql)>>()
                        .as_slice(),
                ),
            };
            let elapsed = start.elapsed();

            let sqlite_rows = match rows_res {
                Ok(rows) => rows,
                Err(e) => {
                    results.push(RqliteResult::Error {
                        error: e.to_string(),
                    });
                    continue;
                }
            };

            results.push(rows_to_rqlite(
                sqlite_rows,
                col_names,
                col_types,
                elapsed,
                associative,
            ));
        }
    });

    Ok(results)
}

fn rows_to_rqlite<'stmt>(
    mut sqlite_rows: rusqlite::Rows<'stmt>,
    col_names: Vec<CompactString>,
    col_types: Vec<Option<CompactString>>,
    elapsed: Duration,
    associative: bool,
) -> RqliteResult {
    let mut builder = QueryResultBuilder::new(col_names, col_types, Some(elapsed.as_secs_f64()));

    loop {
        match sqlite_rows.next() {
            Ok(Some(row)) => {
                if let Err(e) = builder.add_row(row) {
                    return RqliteResult::Error {
                        error: e.to_string(),
                    };
                }
            }
            Ok(None) => {
                break;
            }
            Err(e) => {
                return RqliteResult::Error {
                    error: e.to_string(),
                };
            }
        }
    }

    if associative {
        builder.build_associative()
    } else {
        builder.build()
    }
}

pub async fn api_v1_db_query(
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

    let start = Instant::now();
    match query_statements(agent.read_only_pool(), &statements, false).await {
        Ok(results) => {
            let elapsed = start.elapsed();
            (
                StatusCode::OK,
                axum::Json(RqliteResponse {
                    results,
                    time: Some(elapsed.as_secs_f64()),
                }),
            )
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(RqliteResponse {
                results: vec![RqliteResult::Error {
                    error: e.to_string(),
                }],
                time: None,
            }),
        ),
    }
}

async fn execute_schema(agent: &Agent, statements: Vec<Statement>) -> eyre::Result<()> {
    let new_sql: String = statements
        .into_iter()
        .map(|stmt| match stmt {
            Statement::Simple(s) => Ok(s),
            _ => eyre::bail!("only simple statements are supported"),
        })
        .collect::<Result<Vec<_>, eyre::Report>>()?
        .join(";");

    let partial_schema = parse_sql(&new_sql)?;

    let mut conn = agent.read_write_pool().get().await?;

    // hold onto this lock so nothing else makes changes
    let mut schema_write = agent.0.schema.write();

    let mut new_schema = schema_write.clone();

    for (name, def) in partial_schema.tables.iter() {
        new_schema.tables.insert(name.clone(), def.clone());
    }

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
            time: None,
        }),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arc_swap::ArcSwap;
    use corro_types::{
        actor::ActorId,
        config::Config,
        schema::{apply_schema, NormalizedSchema, Type},
        sqlite::CrConnManager,
    };
    use tokio::sync::mpsc::{channel, error::TryRecvError};
    use uuid::Uuid;

    use super::*;

    use crate::agent::migrate;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn rqlite_db_execute() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir()?;
        let schema_path = dir.path().join("schema");
        tokio::fs::create_dir_all(&schema_path).await?;

        tokio::fs::write(schema_path.join("test.sql"), corro_tests::TEST_SCHEMA).await?;

        let rw_pool = bb8::Pool::builder()
            .max_size(1)
            .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite")));

        {
            let mut conn = rw_pool.get().await?;
            migrate(&mut conn)?;
            apply_schema(&mut conn, &[&schema_path], &NormalizedSchema::default())?;
        }

        let (tx, mut rx) = channel(1);

        let agent = Agent(Arc::new(corro_types::agent::AgentInner {
            actor_id: ActorId(Uuid::new_v4()),
            ro_pool: bb8::Pool::builder()
                .max_size(1)
                .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite"))),
            rw_pool,
            config: ArcSwap::from_pointee(
                Config::builder()
                    .db_path(dir.path().join("corrosion.db").display().to_string())
                    .add_schema_path(schema_path.display().to_string())
                    .gossip_addr("127.0.0.1:1234".parse()?)
                    .api_addr("127.0.0.1:8080".parse()?)
                    .build()?,
            ),
            gossip_addr: "127.0.0.1:0".parse().unwrap(),
            api_addr: "127.0.0.1:0".parse().unwrap(),
            members: Default::default(),
            clock: Default::default(),
            bookie: Default::default(),
            subscribers: Default::default(),
            tx_bcast: tx,
            schema: Default::default(),
        }));

        let (status_code, body) = api_v1_db_execute(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(vec![
                "insert into tests (id, text) values (?,?)".into(),
                "service-id".into(),
                "service-name".into(),
            ])]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        let msg = rx.recv().await.expect("not msg received on bcast channel");

        assert!(matches!(
            msg,
            BroadcastInput::AddBroadcast(Message::V1(MessageV1::Change { version: 1, .. }))
        ));

        assert_eq!(agent.bookie().last(&agent.actor_id()), Some(1));

        println!("second req...");

        let (status_code, body) = api_v1_db_execute(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(vec![
                "update tests SET text = ? where id = ?".into(),
                "service-name".into(),
                "service-id".into(),
            ])]),
        )
        .await;

        println!("{body:?}");

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 1);

        // no actual changes!
        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_db_schema() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let dir = tempfile::tempdir()?;

        let rw_pool = bb8::Pool::builder()
            .max_size(1)
            .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite")));

        {
            let mut conn = rw_pool.get().await?;
            migrate(&mut conn)?;
        };

        let (tx, _rx) = channel(1);

        let agent = Agent(Arc::new(corro_types::agent::AgentInner {
            actor_id: ActorId(Uuid::new_v4()),
            ro_pool: bb8::Pool::builder()
                .max_size(1)
                .build_unchecked(CrConnManager::new(dir.path().join("./test.sqlite"))),
            rw_pool,
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
            subscribers: Default::default(),
            tx_bcast: tx,
            schema: Default::default(),
        }));

        let (status_code, _body) = api_v1_db_schema(
            Extension(agent.clone()),
            axum::Json(vec![Statement::Simple(
                "CREATE TABLE tests (id BIGINT PRIMARY KEY, foo TEXT);".into(),
            )]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        let schema = agent.0.schema.read();
        let tests = schema
            .tables
            .get("tests")
            .expect("no tests table in schema");

        let id_col = tests.columns.get("id").unwrap();
        assert_eq!(id_col.name, "id");
        assert_eq!(id_col.sql_type, Type::Integer);
        assert_eq!(id_col.nullable, true);
        assert_eq!(id_col.primary_key, true);

        let foo_col = tests.columns.get("foo").unwrap();
        assert_eq!(foo_col.name, "foo");
        assert_eq!(foo_col.sql_type, Type::Text);
        assert_eq!(foo_col.nullable, true);
        assert_eq!(foo_col.primary_key, false);

        Ok(())
    }

    #[test]
    fn test_change_chunker() {
        // empty interator
        let mut chunker = ChunkedChanges::new(vec![].into_iter(), 0, 100, 50);

        assert_eq!(chunker.next(), Some(Ok((vec![], 0..=100))));
        assert_eq!(chunker.next(), None);

        let seq_0 = Change {
            seq: 0,
            ..Default::default()
        };
        let seq_1 = Change {
            seq: 1,
            ..Default::default()
        };
        let seq_2 = Change {
            seq: 2,
            ..Default::default()
        };

        // 2 iterations
        let mut chunker = ChunkedChanges::new(
            vec![Ok(seq_0.clone()), Ok(seq_1.clone()), Ok(seq_2.clone())].into_iter(),
            0,
            100,
            2,
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![seq_0.clone(), seq_1.clone()], 0..=1)))
        );
        assert_eq!(chunker.next(), Some(Ok((vec![seq_2.clone()], 2..=100))));
        assert_eq!(chunker.next(), None);

        let mut chunker = ChunkedChanges::new(
            vec![Ok(seq_0.clone()), Ok(seq_1.clone())].into_iter(),
            0,
            0,
            1,
        );

        assert_eq!(chunker.next(), Some(Ok((vec![seq_0.clone()], 0..=0))));
        assert_eq!(chunker.next(), None);
    }
}
