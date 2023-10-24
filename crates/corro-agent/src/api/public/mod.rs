use std::{
    collections::HashMap,
    iter::Peekable,
    mem::forget,
    ops::{Deref, DerefMut, RangeInclusive},
    time::{Duration, Instant},
};

use axum::{extract, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    agent::{Agent, ChangeError, KnownDbVersion},
    api::{row_to_change, ExecResponse, ExecResult, QueryEvent, Statement},
    broadcast::{ChangeV1, Changeset, Timestamp},
    change::SqliteValue,
    http::{IoBodyStream, LinesBytesCodec},
    schema::{apply_schema, parse_sql},
    sqlite::SqlitePoolError,
};
use futures::StreamExt;
use hyper::StatusCode;
use itertools::Itertools;
use metrics::counter;
use rusqlite::{named_params, params_from_iter, Connection, ToSql, Transaction};
use serde::{Deserialize, Serialize};
use spawn::spawn_counted;
use tokio::{
    sync::{
        mpsc::{self, channel, error::SendError, Receiver, Sender},
        oneshot,
    },
    task::block_in_place,
};
use tokio_util::{
    codec::{Encoder, FramedRead, LengthDelimitedCodec},
    io::StreamReader,
    sync::CancellationToken,
};
use tracing::{debug, error, info, trace, Instrument};

use corro_types::{
    broadcast::{BroadcastInput, BroadcastV1},
    change::Change,
};

use crate::agent::process_subs;

pub mod pubsub;

pub struct ChunkedChanges<I: Iterator> {
    iter: Peekable<I>,
    changes: Vec<Change>,
    last_pushed_seq: i64,
    last_start_seq: i64,
    last_seq: i64,
    max_buf_size: usize,
    buffered_size: usize,
    done: bool,
}

impl<I> ChunkedChanges<I>
where
    I: Iterator,
{
    pub fn new(iter: I, start_seq: i64, last_seq: i64, max_buf_size: usize) -> Self {
        Self {
            iter: iter.peekable(),
            changes: vec![],
            last_pushed_seq: 0,
            last_start_seq: start_seq,
            last_seq,
            max_buf_size,
            buffered_size: 0,
            done: false,
        }
    }

    pub fn max_buf_size(&self) -> usize {
        self.max_buf_size
    }

    pub fn set_max_buf_size(&mut self, size: usize) {
        self.max_buf_size = size;
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

        debug_assert!(self.changes.is_empty());

        // reset the buffered size
        self.buffered_size = 0;

        loop {
            trace!("chunking through the rows iterator");
            match self.iter.next() {
                Some(Ok(change)) => {
                    trace!("got change: {change:?}");

                    self.last_pushed_seq = change.seq;

                    self.buffered_size += change.estimated_byte_size();

                    self.changes.push(change);

                    if self.last_pushed_seq == self.last_seq {
                        // this was the last seq! break early
                        break;
                    }

                    if self.buffered_size >= self.max_buf_size {
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum Stmt {
    Prepare(String),
    Drop(u32),
    Reset(u32),
    Columns(u32),

    Execute(u32, Vec<SqliteValue>),
    Query(u32, Vec<SqliteValue>),

    Next(u32),

    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum SqliteResult {
    Ok,
    Error(String),

    Statement {
        id: u32,
        params_count: usize,
    },

    Execute {
        rows_affected: usize,
        last_insert_rowid: i64,
    },

    Columns(Vec<String>),

    // None represents the end of a statement's rows
    Row(Option<Vec<SqliteValue>>),
}

#[derive(Debug, thiserror::Error)]
enum HandleConnError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("events channel closed")]
    EventsChannelClosed,
}

impl<T> From<SendError<T>> for HandleConnError {
    fn from(value: SendError<T>) -> Self {
        HandleConnError::EventsChannelClosed
    }
}

#[derive(Clone, Debug)]
struct IncrMap<V> {
    map: HashMap<u32, V>,
    last: u32,
}

impl<V> IncrMap<V> {
    pub fn insert(&mut self, v: V) -> u32 {
        self.last += 1;
        self.map.insert(self.last, v);
        self.last
    }
}

impl<V> Default for IncrMap<V> {
    fn default() -> Self {
        Self {
            map: Default::default(),
            last: Default::default(),
        }
    }
}

impl<V> Deref for IncrMap<V> {
    type Target = HashMap<u32, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<V> DerefMut for IncrMap<V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

#[derive(Debug, Default)]
struct ConnCache<'conn> {
    prepared: IncrMap<rusqlite::Statement<'conn>>,
    cells: Vec<SqliteValue>,
}

#[derive(Debug, thiserror::Error)]
enum StmtError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("statement not found: {id}")]
    StatementNotFound { id: u32 },
}

fn handle_stmt<'conn>(
    agent: &Agent,
    conn: &'conn Connection,
    cache: &mut ConnCache<'conn>,
    stmt: Stmt,
) -> Result<SqliteResult, StmtError> {
    match stmt {
        Stmt::Prepare(sql) => {
            let prepped = conn.prepare(&sql)?;
            let params_count = prepped.parameter_count();
            let id = cache.prepared.insert(prepped);
            Ok(SqliteResult::Statement { id, params_count })
        }
        Stmt::Columns(id) => {
            let prepped = cache
                .prepared
                .get(&id)
                .ok_or(StmtError::StatementNotFound { id })?;
            Ok(SqliteResult::Columns(
                prepped
                    .column_names()
                    .into_iter()
                    .map(|name| name.to_string())
                    .collect(),
            ))
        }
        Stmt::Execute(id, params) => {
            let prepped = cache
                .prepared
                .get_mut(&id)
                .ok_or(StmtError::StatementNotFound { id })?;
            let rows_affected = prepped.execute(params_from_iter(params))?;
            Ok(SqliteResult::Execute {
                rows_affected,
                last_insert_rowid: conn.last_insert_rowid(),
            })
        }
        Stmt::Query(id, params) => {
            let prepped = cache
                .prepared
                .get_mut(&id)
                .ok_or(StmtError::StatementNotFound { id })?;

            for (i, param) in params.into_iter().enumerate() {
                prepped.raw_bind_parameter(i + 1, param)?;
            }

            Ok(SqliteResult::Ok)
        }
        Stmt::Next(id) => {
            let prepped = cache
                .prepared
                .get_mut(&id)
                .ok_or(StmtError::StatementNotFound { id })?;

            // creates an interator for already-bound statements
            let mut rows = prepped.raw_query();

            let res = match rows.next()? {
                Some(row) => {
                    let col_count = row.as_ref().column_count();
                    cache.cells.clear();
                    for idx in 0..col_count {
                        let v = row.get::<_, SqliteValue>(idx)?;
                        cache.cells.push(v);
                    }
                    Ok(SqliteResult::Row(Some(cache.cells.drain(..).collect_vec())))
                }
                None => Ok(SqliteResult::Row(None)),
            };

            // prevent running Drop so it doesn't reset everything...
            forget(rows);

            res
        }
        Stmt::Begin => {
            conn.execute_batch("BEGIN")?;
            Ok(SqliteResult::Ok)
        }
        Stmt::Commit => {
            handle_commit(agent, conn)?;
            Ok(SqliteResult::Ok)
        }
        Stmt::Rollback => {
            conn.execute_batch("ROLLBACK")?;
            Ok(SqliteResult::Ok)
        }
        Stmt::Drop(id) => {
            cache.prepared.remove(&id);
            Ok(SqliteResult::Ok)
        }
        Stmt::Reset(id) => {
            let prepped = cache
                .prepared
                .get_mut(&id)
                .ok_or(StmtError::StatementNotFound { id })?;

            // not sure how to reset a statement otherwise..
            let rows = prepped.raw_query();
            drop(rows);

            Ok(SqliteResult::Ok)
        }
    }
}

fn handle_interactive(
    agent: &Agent,
    mut queries: Receiver<Stmt>,
    events: Sender<SqliteResult>,
) -> Result<(), HandleConnError> {
    let conn = match agent.pool().client_dedicated() {
        Ok(conn) => conn,
        Err(e) => {
            return events
                .blocking_send(SqliteResult::Error(e.to_string()))
                .map_err(HandleConnError::from);
        }
    };

    let mut cache = ConnCache::default();

    while let Some(stmt) = queries.blocking_recv() {
        match handle_stmt(agent, &conn, &mut cache, stmt) {
            Ok(res) => {
                events.blocking_send(res)?;
            }
            Err(e) => {
                events.blocking_send(SqliteResult::Error(e.to_string()))?;
            }
        }
    }

    Ok(())
}

fn handle_commit(agent: &Agent, conn: &Connection) -> rusqlite::Result<()> {
    let actor_id = agent.actor_id();

    let ts = Timestamp::from(agent.clock().new_timestamp());

    let db_version: i64 = conn
        .prepare_cached("SELECT crsql_next_db_version()")?
        .query_row((), |row| row.get(0))?;

    let has_changes: bool = conn
        .prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM crsql_changes WHERE site_id IS NULL AND db_version = ?);",
        )?
        .query_row([db_version], |row| row.get(0))?;

    if !has_changes {
        conn.execute_batch("COMMIT")?;
        return Ok(());
    }

    let booked = {
        agent
            .bookie()
            .blocking_write("handle_write_tx(for_actor)")
            .for_actor(actor_id)
    };

    let last_seq: i64 = conn
        .prepare_cached(
            "SELECT MAX(seq) FROM crsql_changes WHERE site_id IS NULL AND db_version = ?",
        )?
        .query_row([db_version], |row| row.get(0))?;

    let mut book_writer = booked.blocking_write("handle_write_tx(book_writer)");

    let last_version = book_writer.last().unwrap_or_default();
    trace!("last_version: {last_version}");
    let version = last_version + 1;
    trace!("version: {version}");

    conn.prepare_cached(
        r#"
            INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                VALUES (:actor_id, :start_version, :db_version, :last_seq, :ts);
        "#,
    )?
    .execute(named_params! {
        ":actor_id": actor_id,
        ":start_version": version,
        ":db_version": db_version,
        ":last_seq": last_seq,
        ":ts": ts
    })?;

    debug!(%actor_id, %version, %db_version, "inserted local bookkeeping row!");

    conn.execute_batch("COMMIT")?;

    trace!("committed tx, db_version: {db_version}, last_seq: {last_seq:?}");

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
            let chunked = ChunkedChanges::new(rows, 0, last_seq, MAX_CHANGES_BYTE_SIZE);
            for changes_seqs in chunked {
                match changes_seqs {
                    Ok((changes, seqs)) => {
                        for (table_name, count) in changes.iter().counts_by(|change| &change.table)
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
    Ok::<_, rusqlite::Error>(())
}

#[tracing::instrument(skip_all)]
pub async fn api_v1_begins(
    // axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    Extension(agent): Extension<Agent>,
    req_body: extract::RawBody<hyper::Body>,
) -> impl IntoResponse {
    let (mut body_tx, body) = hyper::Body::channel();

    let req_body = IoBodyStream { body: req_body.0 };

    let (queries_tx, queries_rx) = channel(512);
    let (events_tx, mut events_rx) = channel(512);
    let cancel = CancellationToken::new();

    tokio::spawn({
        let cancel = cancel.clone();
        let events_tx = events_tx.clone();
        async move {
            let _drop_guard = cancel.drop_guard();

            let mut req_reader =
                FramedRead::new(StreamReader::new(req_body), LengthDelimitedCodec::default());

            while let Some(buf_res) = req_reader.next().await {
                match buf_res {
                    Ok(buf) => match rmp_serde::from_slice(&buf) {
                        Ok(req) => {
                            if let Err(e) = queries_tx.send(req).await {
                                error!("could not send request into channel: {e}");
                                if let Err(e) = events_tx
                                    .send(SqliteResult::Error("request channel closed".into()))
                                    .await
                                {
                                    error!("could not send error event: {e}");
                                }
                                return;
                            }
                        }
                        Err(e) => {
                            error!("could not parse message: {e}");
                            if let Err(e) = events_tx
                                .send(SqliteResult::Error("request channel closed".into()))
                                .await
                            {
                                error!("could not send error event: {e}");
                            }
                        }
                    },
                    Err(e) => {
                        error!("could not read buffer from request body: {e}");
                        break;
                    }
                }
            }
        }
        .in_current_span()
    });

    // probably a better way to do this...
    spawn_counted(
        async move { block_in_place(|| handle_interactive(&agent, queries_rx, events_tx)) }
            .in_current_span(),
    );

    tokio::spawn(async move {
        let mut ser_buf = BytesMut::new();
        let mut encode_buf = BytesMut::new();
        let mut codec = LengthDelimitedCodec::default();

        while let Some(event) = events_rx.recv().await {
            match rmp_serde::encode::write(&mut (&mut ser_buf).writer(), &event) {
                Ok(_) => match codec.encode(ser_buf.split().freeze(), &mut encode_buf) {
                    Ok(_) => {
                        if let Err(e) = body_tx.send_data(encode_buf.split().freeze()).await {
                            error!("could not send tx event to response body: {e}");
                            return;
                        }
                    }
                    Err(e) => {
                        error!("could not encode event: {e}");
                        if let Err(e) = body_tx
                            .send_data(Bytes::from(r#"{"error": "could not encode event"}"#))
                            .await
                        {
                            error!("could not send encoding error to body: {e}");
                            return;
                        }
                    }
                },
                Err(e) => {
                    error!("could not serialize event: {e}");
                    if let Err(e) = body_tx
                        .send_data(Bytes::from(r#"{"error": "could not serialize event"}"#))
                        .await
                    {
                        error!("could not send serialize error to body: {e}");
                        return;
                    }
                }
            }
        }
    });

    hyper::Response::builder()
        .status(hyper::StatusCode::SWITCHING_PROTOCOLS)
        .body(body)
        .unwrap()
}

const MAX_CHANGES_BYTE_SIZE: usize = 8 * 1024;

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
    let booked = {
        agent
            .bookie()
            .write("make_broadcastable_changes(for_actor)")
            .await
            .for_actor(actor_id)
    };
    // maybe we should do this earlier, but there can only ever be 1 write conn at a time,
    // so it probably doesn't matter too much, except for reads of internal state
    let mut book_writer = booked
        .write("make_broadcastable_changes(booked writer)")
        .await;

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

        let last_version = book_writer.last().unwrap_or_default();
        trace!("last_version: {last_version}");
        let version = last_version + 1;
        trace!("version: {version}");

        let last_seq: i64 = tx
            .prepare_cached(
                "SELECT MAX(seq) FROM crsql_changes WHERE site_id IS NULL AND db_version = ?",
            )?
            .query_row([db_version], |row| row.get(0))?;

        let elapsed = {
            tx.prepare_cached(
                r#"
                INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                    VALUES (:actor_id, :start_version, :db_version, :last_seq, :ts);
            "#,
            )?
            .execute(named_params! {
                ":actor_id": actor_id,
                ":start_version": version,
                ":db_version": db_version,
                ":last_seq": last_seq,
                ":ts": ts
            })?;

            debug!(%actor_id, %version, %db_version, "inserted local bookkeeping row!");

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
                let chunked = ChunkedChanges::new(rows, 0, last_seq, MAX_CHANGES_BYTE_SIZE);
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

#[tracing::instrument(skip_all, err)]
fn execute_statement(tx: &Transaction, stmt: &Statement) -> rusqlite::Result<usize> {
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
    axum::extract::Json(statements): axum::extract::Json<Vec<Statement>>,
) -> (StatusCode, axum::Json<ExecResponse>) {
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ExecResponse {
                results: vec![ExecResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: 0.0,
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
                        ExecResult::Execute {
                            rows_affected,
                            time: start.elapsed().as_secs_f64(),
                        }
                    }
                    Err(e) => ExecResult::Error {
                        error: e.to_string(),
                    },
                }
            })
            .collect::<Vec<ExecResult>>();

        Ok(results)
    })
    .await;

    let (results, elapsed) = match res {
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
                }),
            );
        }
    };

    (
        StatusCode::OK,
        axum::Json(ExecResponse {
            results,
            time: elapsed.as_secs_f64(),
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
                    .map(|col| col.name().to_compact_string())
                    .collect(),
            )) {
                error!("could not send back columns: {e}");
                return;
            }

            let start = Instant::now();

            let query = match stmt {
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
    let mut new_schema = {
        let mut schema = schema_write.clone();
        for (name, def) in partial_schema.tables.iter() {
            // overwrite table because users are expected to return a full table def
            schema.tables.insert(name.clone(), def.clone());
        }
        schema
    };

    block_in_place(|| {
        let tx = conn.transaction()?;

        apply_schema(&tx, &schema_write, &mut new_schema)?;

        for tbl_name in partial_schema.tables.keys() {
            tx.execute("DELETE FROM __corro_schema WHERE tbl_name = ?", [tbl_name])?;

            let n = tx.execute("INSERT INTO __corro_schema SELECT tbl_name, type, name, sql, 'api' AS source FROM sqlite_schema WHERE tbl_name = ? AND type IN ('table', 'index') AND name IS NOT NULL AND sql IS NOT NULL", [tbl_name])?;
            info!("Updated {n} rows in __corro_schema for table {tbl_name}");
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
) -> (StatusCode, axum::Json<ExecResponse>) {
    if statements.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(ExecResponse {
                results: vec![ExecResult::Error {
                    error: "at least 1 statement is required".into(),
                }],
                time: 0.0,
            }),
        );
    }

    let start = Instant::now();

    if let Err(e) = execute_schema(&agent, statements).await {
        error!("could not merge schemas: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(ExecResponse {
                results: vec![ExecResult::Error {
                    error: e.to_string(),
                }],
                time: 0.0,
            }),
        );
    }

    (
        StatusCode::OK,
        axum::Json(ExecResponse {
            results: vec![],
            time: start.elapsed().as_secs_f64(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use corro_tests::launch_test_agent;
    use corro_types::{api::RowId, config::Config, schema::SqliteType};
    use futures::Stream;
    use http_body::{combinators::UnsyncBoxBody, Body};
    use spawn::wait_for_all_pending_handles;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use super::*;

    use crate::agent::setup;

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

        assert_eq!(
            agent
                .bookie()
                .write("test")
                .await
                .for_actor(agent.actor_id())
                .read("test")
                .await
                .last(),
            Some(1)
        );

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
            assert_eq!(id_col.sql_type, SqliteType::Integer);
            assert!(!id_col.nullable);
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
            assert_eq!(id_col.sql_type, SqliteType::Integer);
            assert!(!id_col.nullable);
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
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type, SqliteType::Text);
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
            assert_eq!(id_col.sql_type, SqliteType::Integer);
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type, SqliteType::Text);
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let tests = schema
                .tables
                .get("tests3")
                .expect("no tests3 table in schema");

            let id_col = tests.columns.get("id").unwrap();
            assert_eq!(id_col.name, "id");
            assert_eq!(id_col.sql_type, SqliteType::Integer);
            assert!(!id_col.nullable);
            assert!(id_col.primary_key);

            let foo_col = tests.columns.get("foo").unwrap();
            assert_eq!(foo_col.name, "foo");
            assert_eq!(foo_col.sql_type, SqliteType::Text);
            assert!(foo_col.nullable);
            assert!(!foo_col.primary_key);

            let updated_at_col = tests.columns.get("updated_at").unwrap();
            assert_eq!(updated_at_col.name, "updated_at");
            assert_eq!(updated_at_col.sql_type, SqliteType::Integer);
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
            changes[0].estimated_byte_size() + changes[1].estimated_byte_size(),
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
            changes[0].estimated_byte_size(),
        );

        assert_eq!(chunker.next(), Some(Ok((vec![changes[0].clone()], 0..=0))));
        assert_eq!(chunker.next(), None);

        // gaps
        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[2].clone())].into_iter(),
            0,
            100,
            changes[0].estimated_byte_size() + changes[2].estimated_byte_size(),
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
            100000, // just send them all!
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
            changes[2].estimated_byte_size() + changes[4].estimated_byte_size(),
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_interactive() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let ta = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

        let (q_tx, q_rx) = channel(1);
        let (e_tx, mut e_rx) = channel(1);

        spawn_counted(async move { block_in_place(|| handle_interactive(&ta.agent, q_rx, e_tx)) });

        q_tx.send(Stmt::Prepare("SELECT 123".into())).await?;
        let e = e_rx.recv().await.unwrap();
        println!("e: {e:?}");

        q_tx.send(Stmt::Query(1, vec![])).await?;
        let e = e_rx.recv().await.unwrap();
        println!("e: {e:?}");

        q_tx.send(Stmt::Next(1)).await?;
        let e = e_rx.recv().await.unwrap();
        assert_eq!(e, SqliteResult::Row(Some(vec![SqliteValue::Integer(123)])));

        q_tx.send(Stmt::Next(1)).await?;
        let e = e_rx.recv().await.unwrap();
        assert_eq!(e, SqliteResult::Row(None));

        drop(q_tx);
        drop(e_rx);

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

        Ok(())
    }
}
