use std::{
    collections::HashMap,
    io::Write,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::{format_compact, ToCompactString};
use corro_types::{
    agent::Agent,
    api::{ChangeId, QueryEvent, QueryEventMeta, RowId, Statement},
    change::SqliteValue,
    pubsub::{Matcher, MatcherError, MatcherHandle, NormalizeStatementError},
    sqlite::SqlitePoolError,
};
use futures::{future::poll_fn, ready, Stream};
use rusqlite::{Connection, Transaction};
use serde::Deserialize;
use tokio::{
    sync::{broadcast, mpsc, oneshot, RwLock as TokioRwLock},
    task::block_in_place,
};
use tokio_stream::{wrappers::errors::BroadcastStreamRecvError, StreamExt};
use tokio_util::sync::PollSender;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

#[derive(Default, Deserialize)]
pub struct SubParams {
    #[serde(default)]
    from: Option<ChangeId>,
}

pub async fn api_v1_sub_by_id(
    Extension(agent): Extension<Agent>,
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<SubParams>,
) -> impl IntoResponse {
    sub_by_id(agent, id, params.from, &bcast_cache).await
}

async fn sub_by_id(
    agent: Agent,
    id: Uuid,
    from: Option<ChangeId>,
    bcast_cache: &SharedMatcherBroadcastCache,
) -> hyper::Response<hyper::Body> {
    let (matcher, rx) = match bcast_cache.read().await.get(&id).and_then(|tx| {
        agent.matchers().read().get(&id).cloned().map(|matcher| {
            debug!("found matcher by id {id}");
            (matcher, tx.subscribe())
        })
    }) {
        Some(matcher_rx) => matcher_rx,
        None => {
            // ensure this goes!
            bcast_cache.write().await.remove(&id);
            if let Some(handle) = agent.matchers().write().remove(&id) {
                handle.cleanup();
            }

            return hyper::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(
                    serde_json::to_vec(&QueryEvent::Error(format_compact!(
                        "could not find subscription with id {id}"
                    )))
                    .expect("could not serialize queries stream error")
                    .into(),
                )
                .expect("could not build error response");
        }
    };

    let (evt_tx, evt_rx) = mpsc::channel(512);

    tokio::spawn(catch_up_sub(agent, matcher, from, rx, evt_tx));

    let (tx, body) = hyper::Body::channel();

    tokio::spawn(forward_bytes_to_body_sender(evt_rx, tx));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", id.to_string())
        .body(body)
        .expect("could not build query response body")
}

fn make_query_event_bytes(
    buf: &mut BytesMut,
    query_evt: QueryEvent,
) -> serde_json::Result<(Bytes, QueryEventMeta)> {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, &query_evt)?;

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    Ok((buf.split().freeze(), query_evt.meta()))
}

const MAX_UNSUB_TIME: Duration = Duration::from_secs(300);
// this should be a fraction of the MAX_UNSUB_TIME
const RECEIVERS_CHECK_INTERVAL: Duration = Duration::from_secs(30);

pub async fn process_sub_channel(
    agent: Agent,
    id: Uuid,
    tx: broadcast::Sender<(Bytes, QueryEventMeta)>,
    mut evt_rx: mpsc::Receiver<QueryEvent>,
) {
    let mut buf = BytesMut::new();

    let mut deadline = if tx.receiver_count() == 0 {
        Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)))
    } else {
        None
    };

    // even if there are no more subscribers
    // useful for queries that don't change often so we can cleanup...
    let mut subs_check = tokio::time::interval(RECEIVERS_CHECK_INTERVAL);

    loop {
        let deadline_check = async {
            if let Some(sleep) = deadline.as_mut() {
                sleep.await
            } else {
                futures::future::pending().await
            }
        };

        let query_evt = tokio::select! {
            biased;
            Some(query_evt) = evt_rx.recv() => query_evt,
            _ = deadline_check => {
                info!("All subscribers for {id} are gone and didn't come back within {MAX_UNSUB_TIME:?}");
                break;
            },
            _ = subs_check.tick() => {
                if tx.receiver_count() == 0 {
                    if deadline.is_none() {
                        deadline = Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)));
                    }
                } else {
                    deadline = None;
                };
                continue;
            },
            else => {
                break;
            }
        };

        let is_still_active = match make_query_event_bytes(&mut buf, query_evt) {
            Ok(b) => tx.send(b).is_ok(),
            Err(e) => {
                _ = tx.send((
                    error_to_query_event_bytes(&mut buf, e),
                    QueryEventMeta::Error,
                ));
                break;
            }
        };

        if is_still_active {
            deadline = None;
        } else {
            debug!("no active subscribers");
            if deadline.is_none() {
                deadline = Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)));
            }
        }
    }

    debug!("subscription query channel done");

    // remove and get handle from the agent's "matchers"
    let handle = match agent.matchers().write().remove(&id) {
        Some(h) => h,
        None => {
            warn!("subscription handle was already gone. odd!");
            return;
        }
    };

    // clean up the subscription
    handle.cleanup();
}

fn expanded_statement(conn: &Connection, stmt: &Statement) -> rusqlite::Result<Option<String>> {
    Ok(match stmt {
        Statement::Simple(query)
        | Statement::Verbose {
            query,
            params: None,
            named_params: None,
        } => conn.prepare(query)?.expanded_sql(),
        Statement::WithParams(query, params)
        | Statement::Verbose {
            query,
            params: Some(params),
            ..
        } => {
            let mut prepped = conn.prepare(query)?;
            for (i, param) in params.iter().enumerate() {
                prepped.raw_bind_parameter(i + 1, param)?;
            }
            prepped.expanded_sql()
        }
        Statement::WithNamedParams(query, params)
        | Statement::Verbose {
            query,
            named_params: Some(params),
            ..
        } => {
            let mut prepped = conn.prepare(query)?;
            for (k, v) in params.iter() {
                let idx = match prepped.parameter_index(k)? {
                    Some(idx) => idx,
                    None => continue,
                };
                prepped.raw_bind_parameter(idx, v)?;
            }
            prepped.expanded_sql()
        }
    })
}

async fn expand_sql(agent: &Agent, stmt: &Statement) -> Result<String, MatcherUpsertError> {
    let conn = agent.pool().read().await?;
    expanded_statement(&conn, stmt)?.ok_or(MatcherUpsertError::CouldNotExpand)
}

#[derive(Debug, thiserror::Error)]
pub enum MatcherUpsertError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error("could not expand sql statement")]
    CouldNotExpand,
    #[error(transparent)]
    NormalizeStatement(#[from] NormalizeStatementError),
    #[error(transparent)]
    Matcher(#[from] MatcherError),
    #[error("a `from` query param was supplied, but no existing subscription found")]
    SubFromWithoutMatcher,
}

impl MatcherUpsertError {
    fn status_code(&self) -> StatusCode {
        match self {
            MatcherUpsertError::Pool(_) | MatcherUpsertError::CouldNotExpand => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            MatcherUpsertError::Sqlite(_)
            | MatcherUpsertError::NormalizeStatement(_)
            | MatcherUpsertError::Matcher(_)
            | MatcherUpsertError::SubFromWithoutMatcher => StatusCode::BAD_REQUEST,
        }
    }
}

impl From<MatcherUpsertError> for hyper::Response<hyper::Body> {
    fn from(value: MatcherUpsertError) -> Self {
        hyper::Response::builder()
            .status(value.status_code())
            .body(
                serde_json::to_vec(&QueryEvent::Error(value.to_compact_string()))
                    .expect("could not serialize queries stream error")
                    .into(),
            )
            .expect("could not build error response")
    }
}
pub type MatcherIdCache = HashMap<String, Uuid>;
pub type SharedMatcherIdCache = Arc<TokioRwLock<MatcherIdCache>>;
pub type MatcherBroadcastCache = HashMap<Uuid, broadcast::Sender<(Bytes, QueryEventMeta)>>;
pub type SharedMatcherBroadcastCache = Arc<TokioRwLock<MatcherBroadcastCache>>;

#[derive(Debug, thiserror::Error)]
pub enum CatchUpError {
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Send(#[from] mpsc::error::SendError<Bytes>),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
}

fn error_to_query_event_bytes<E: ToCompactString>(buf: &mut BytesMut, e: E) -> Bytes {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, &QueryEvent::Error(e.to_compact_string()))
            .expect("could not write QueryEvent::Error to buffer");

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    buf.split().freeze()
}

fn catch_up_sub_anew(
    tx: &Transaction,
    matcher: MatcherHandle,
    buf: &mut BytesMut,
    evt_tx: &mpsc::Sender<Bytes>,
) -> Result<(), CatchUpError> {
    let mut query_cols = vec![];
    for i in 0..(matcher.parsed_columns().len()) {
        query_cols.push(format!("col_{i}"));
    }
    let mut prepped = tx.prepare_cached(&format!(
        "SELECT __corro_rowid,{} FROM {}",
        query_cols.join(","),
        matcher.table_name()
    ))?;
    let col_count = prepped.column_count();

    evt_tx.blocking_send(
        make_query_event_bytes(buf, QueryEvent::Columns(matcher.col_names().to_vec()))?.0,
    )?;

    let start = Instant::now();
    let mut rows = prepped.query(())?;
    let elapsed = start.elapsed();

    loop {
        let row = match rows.next()? {
            Some(row) => row,
            None => break,
        };
        let rowid = row.get(0)?;

        let cells = (1..col_count)
            .map(|i| row.get::<_, SqliteValue>(i))
            .collect::<rusqlite::Result<Vec<_>>>()?;

        evt_tx.blocking_send(make_query_event_bytes(buf, QueryEvent::Row(rowid, cells))?.0)?;
    }

    evt_tx.blocking_send(
        make_query_event_bytes(
            buf,
            QueryEvent::EndOfQuery {
                time: elapsed.as_secs_f64(),
                change_id: Some(
                    tx.prepare(&format!(
                        "SELECT COALESCE(MAX(id),0) FROM {}",
                        matcher.changes_table_name()
                    ))?
                    .query_row([], |row| row.get(0))?,
                ),
            },
        )?
        .0,
    )?;

    Ok(())
}

fn catch_up_sub_from(
    tx: &Transaction, // read transaction
    matcher: MatcherHandle,
    from: ChangeId,
    buf: &mut BytesMut,
    evt_tx: &mpsc::Sender<Bytes>,
) -> Result<(), CatchUpError> {
    let mut query_cols = vec![];
    for i in 0..(matcher.parsed_columns().len()) {
        query_cols.push(format!("col_{i}"));
    }

    let mut prepped = tx.prepare_cached(&format!(
        "SELECT id, type, __corro_rowid, {} FROM {} WHERE id > ?",
        query_cols.join(","),
        matcher.changes_table_name()
    ))?;

    let col_count = prepped.column_count();

    let mut rows = prepped.query([from])?;

    loop {
        let row = match rows.next()? {
            Some(row) => row,
            None => break,
        };
        let id = row.get(0)?;
        let change_type = row.get(1)?;
        let rowid = row.get(2)?;

        let cells = (3..col_count)
            .map(|i| row.get::<_, SqliteValue>(i))
            .collect::<rusqlite::Result<Vec<_>>>()?;

        evt_tx.blocking_send(
            make_query_event_bytes(buf, QueryEvent::Change(change_type, rowid, cells, id))?.0,
        )?;
    }

    Ok(())
}

#[derive(Clone, Copy)]
enum LastQueryEvent {
    Row(RowId),
    Change(ChangeId),
}

pub async fn catch_up_sub(
    agent: Agent,
    matcher: MatcherHandle,
    from: Option<ChangeId>,
    sub_rx: broadcast::Receiver<(Bytes, QueryEventMeta)>,
    evt_tx: mpsc::Sender<Bytes>,
) -> eyre::Result<()> {
    debug!("catching up sub {} from: {from:?}", matcher.id());
    let (ready_tx, ready_rx) = oneshot::channel();

    let forward_task = tokio::spawn(forward_sub_to_sender(
        Some(ready_rx),
        sub_rx,
        evt_tx.clone(),
    ));

    let last_query_event = {
        let mut buf = BytesMut::new();

        let mut conn = match agent.pool().dedicated() {
            Ok(conn) => conn,
            Err(e) => {
                evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await?;
                return Ok(());
            }
        };

        let res = block_in_place(|| {
            let tx = conn.transaction()?; // read transaction
            let last_query_event = match from {
                Some(from) => {
                    let max_change_id: ChangeId = tx
                        .prepare(&format!(
                            "SELECT COALESCE(MAX(id), 0) FROM {}",
                            matcher.changes_table_name()
                        ))?
                        .query_row([], |row| row.get(0))?;
                    catch_up_sub_from(&tx, matcher, from, &mut buf, &evt_tx)?;
                    debug!("sub caught up to their 'from' of {from:?}");
                    LastQueryEvent::Change(max_change_id)
                }
                None => {
                    let max_row_id: RowId = tx
                        .prepare(&format!(
                            "SELECT COALESCE(MAX(__corro_rowid), 0) FROM {}",
                            matcher.table_name()
                        ))?
                        .query_row([], |row| row.get(0))?;
                    catch_up_sub_anew(&tx, matcher, &mut buf, &evt_tx)?;
                    debug!("sub caught up from scratch");
                    LastQueryEvent::Row(max_row_id)
                }
            };
            Ok(last_query_event)
        });

        match res {
            Ok(last_query_event) => last_query_event,
            Err(e) => {
                match e {
                    CatchUpError::Sqlite(e) => {
                        _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                    }
                    CatchUpError::SerdeJson(e) => {
                        _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                    }
                    CatchUpError::Send(_) => {
                        // can't send
                    }
                }
                return Ok(());
            }
        }
    };

    if let Err(_e) = ready_tx.send(last_query_event) {
        warn!("subscriber catch up readiness receiver was gone, aborting...");
        return Ok(());
    }

    // TODO: handle this spawn error?
    _ = forward_task.await;

    Ok(())
}

pub async fn upsert_sub(
    agent: &Agent,
    cache: &SharedMatcherIdCache,
    bcast_cache: &SharedMatcherBroadcastCache,
    stmt: Statement,
    from: Option<ChangeId>,
    tx: mpsc::Sender<Bytes>,
) -> Result<Uuid, MatcherUpsertError> {
    let stmt = expand_sql(agent, &stmt).await?;

    let mut cache_write = cache.write().await;
    let mut bcast_write = bcast_cache.write().await;

    let maybe_matcher = cache_write
        .get(&stmt)
        .and_then(|id| bcast_write.get(id).map(|sender| (*id, sender)));

    if let Some((matcher_id, sender)) = maybe_matcher {
        debug!("found matcher id {matcher_id} w/ sender");
        let maybe_matcher = (sender.receiver_count() > 0)
            .then(|| agent.matchers().read().get(&matcher_id).cloned())
            .flatten();
        if let Some(matcher) = maybe_matcher {
            debug!("found matcher handle");
            let rx = sender.subscribe();
            tokio::spawn(catch_up_sub(agent.clone(), matcher, from, rx, tx));
            return Ok(matcher_id);
        } else {
            cache_write.remove(&stmt);
            bcast_write.remove(&matcher_id);
        }
    }

    if from.is_some() {
        return Err(MatcherUpsertError::SubFromWithoutMatcher);
    }

    let conn = agent.pool().dedicated()?;

    let (evt_tx, evt_rx) = mpsc::channel(512);

    let matcher_id = Uuid::new_v4();

    let matcher = Matcher::create(matcher_id, &agent.schema().read(), conn, evt_tx, &stmt)?;

    let (sub_tx, sub_rx) = broadcast::channel(10240);

    cache_write.insert(stmt, matcher_id);
    bcast_write.insert(matcher_id, sub_tx.clone());

    {
        agent.matchers().write().insert(matcher_id, matcher);
    }

    tokio::spawn(forward_sub_to_sender(None, sub_rx, tx));

    tokio::spawn(process_sub_channel(
        agent.clone(),
        matcher_id,
        sub_tx,
        evt_rx,
    ));

    Ok(matcher_id)
}

pub async fn api_v1_subs(
    Extension(agent): Extension<Agent>,
    Extension(sub_cache): Extension<SharedMatcherIdCache>,
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    axum::extract::Query(params): axum::extract::Query<SubParams>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (tx, body) = hyper::Body::channel();
    let (forward_tx, forward_rx) = mpsc::channel(10240);

    let matcher_id = match upsert_sub(
        &agent,
        &sub_cache,
        &bcast_cache,
        stmt,
        params.from,
        forward_tx,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => return hyper::Response::<hyper::Body>::from(e),
    };

    tokio::spawn(forward_bytes_to_body_sender(forward_rx, tx));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", matcher_id.to_string())
        .body(body)
        .expect("could not generate ok http response for query request")
}

const MAX_EVENTS_BUFFER_SIZE: usize = 1024;

async fn forward_sub_to_sender(
    ready: Option<oneshot::Receiver<LastQueryEvent>>,
    mut sub_rx: broadcast::Receiver<(Bytes, QueryEventMeta)>,
    tx: mpsc::Sender<Bytes>,
) {
    let mut buf = BytesMut::new();
    if let Some(mut ready) = ready {
        let mut events_buf = vec![];

        let last_query_event = loop {
            tokio::select! {
                biased;
                ready_res = &mut ready => match ready_res {
                    Ok(last_query_event) => {
                        break last_query_event;
                    },
                    Err(_e) => {
                        _ = tx.send(error_to_query_event_bytes(&mut buf, "sub ready channel closed")).await;
                        return;
                    }
                },
                query_evt_res = sub_rx.recv() => match query_evt_res {
                    Ok(query_evt) => {
                        events_buf.push(query_evt);
                    },
                    Err(e) => {
                        _ = tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                        return;
                    }
                },
            }

            if events_buf.len() > MAX_EVENTS_BUFFER_SIZE {
                error!("subscriber could not catch up in time, buffered over {MAX_EVENTS_BUFFER_SIZE} events and bailed");
                _ = tx.send(error_to_query_event_bytes(&mut buf, format!("catching up too slowly, gave up after buffering {MAX_EVENTS_BUFFER_SIZE} events"))).await;
                return;
            }
        };

        let mut skipped = 0;
        let mut sent = 0;

        for (bytes, meta) in events_buf {
            // prevent sending duplicate query events by comparing the last sent event
            // w/ buffered events
            match (meta, last_query_event) {
                // already sent this change!
                (QueryEventMeta::Change(change_id), LastQueryEvent::Change(last_change_id))
                    if last_change_id >= change_id =>
                {
                    skipped += 1;
                    continue;
                }
                // already sent this row!
                (QueryEventMeta::Row(row_id), LastQueryEvent::Row(last_row_id))
                    if last_row_id >= row_id =>
                {
                    skipped += 1;
                    continue;
                }
                // buffered a row, which is slightly unexpected,
                // but we shouldn't send rows if we're expecting changes only
                (QueryEventMeta::Row(_), LastQueryEvent::Change(_)) => {
                    skipped += 1;
                    continue;
                }
                _ => {
                    // nothing to do.
                }
            }
            if let Err(_e) = tx.send(bytes).await {
                warn!("could not send buffered events to subscriber, receiver must be gone!");
                return;
            }
            sent += 0;
        }

        debug!("sent {sent} buffered events, skipped: {skipped}");
    }

    let chunker = tokio_stream::wrappers::BroadcastStream::new(sub_rx)
        .chunks_timeout(10, Duration::from_millis(10));

    tokio::pin!(chunker);

    let mut tx = PollSender::new(tx);

    loop {
        let res: Result<Option<Bytes>, BroadcastStreamRecvError> = poll_fn(|cx| {
            if let Err(_e) = ready!(tx.poll_reserve(cx)) {
                return Poll::Ready(Ok(None));
            }
            match ready!(chunker.as_mut().poll_next(cx)) {
                Some(chunks) => {
                    for chunk_res in chunks {
                        let (chunk, _) = chunk_res?;
                        buf.extend_from_slice(&chunk);
                    }
                    Poll::Ready(Ok(Some(buf.split().freeze())))
                }
                None => Poll::Ready(Ok(None)),
            }
        })
        .await;

        match res {
            Ok(Some(b)) => {
                if let Err(_e) = tx.send_item(b) {
                    error!("could not forward subscription query event to receiver, channel is closed!");
                    return;
                }
            }
            Ok(None) => {
                debug!("finished w/ broadcast");
                break;
            }
            Err(e) => {
                error!("could not receive subscription query event: {e}");
                buf.clear();
                // should be safe to send because the poll to reserve succeeded
                if let Err(_e) = tx.send_item(error_to_query_event_bytes(&mut buf, e)) {
                    debug!("could not send back subscription receive error! channel is closed");
                }
                return;
            }
        }
    }
}

async fn forward_bytes_to_body_sender(mut rx: mpsc::Receiver<Bytes>, mut tx: hyper::body::Sender) {
    loop {
        let res = {
            poll_fn(|cx| {
                ready!(tx.poll_ready(cx))?;
                Poll::Ready(Ok::<_, hyper::Error>(ready!(rx.poll_recv(cx))))
            })
            .await
        };
        match res {
            Ok(Some(b)) => {
                if let Err(e) = tx.send_data(b).await {
                    error!("could not send query event data through body: {e}");
                    break;
                }
            }
            Ok(None) => {
                // done...
                break;
            }
            Err(e) => {
                debug!("body was not ready anymore: {e}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use corro_types::{
        api::{ChangeId, RowId},
        config::Config,
        pubsub::ChangeType,
    };
    use http_body::Body;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use crate::{
        agent::setup,
        api::public::{api_v1_db_schema, api_v1_transactions},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_v1_subs() -> eyre::Result<()> {
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

        assert_eq!(status_code, StatusCode::OK);

        assert!(body.0.results.len() == 2);

        let cache: SharedMatcherIdCache = Default::default();
        let bcast_cache: SharedMatcherBroadcastCache = Default::default();

        {
            let mut res = api_v1_subs(
                Extension(agent.clone()),
                Extension(cache.clone()),
                Extension(bcast_cache.clone()),
                axum::extract::Query(SubParams::default()),
                axum::Json(Statement::Simple("select * from tests".into())),
            )
            .await
            .into_response();

            if !res.status().is_success() {
                let b = res.body_mut().data().await.unwrap().unwrap();
                println!("body: {}", String::from_utf8_lossy(&b));
            }

            assert_eq!(res.status(), StatusCode::OK);

            let (status_code, _) = api_v1_transactions(
                Extension(agent.clone()),
                axum::Json(vec![Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-3".into(), "service-name-3".into()],
                )]),
            )
            .await;

            assert_eq!(status_code, StatusCode::OK);

            let mut rows = RowsIter {
                body: res.into_body(),
                codec: LinesCodec::new(),
                buf: BytesMut::new(),
                done: false,
            };

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Columns(vec!["id".into(), "text".into()])
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(RowId(1), vec!["service-id".into(), "service-name".into()])
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(
                    RowId(2),
                    vec!["service-id-2".into(), "service-name-2".into()]
                )
            );

            assert!(matches!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::EndOfQuery { .. }
            ));

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Change(
                    ChangeType::Insert,
                    RowId(3),
                    vec!["service-id-3".into(), "service-name-3".into()],
                    ChangeId(1)
                )
            );

            let (status_code, _) = api_v1_transactions(
                Extension(agent.clone()),
                axum::Json(vec![Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-4".into(), "service-name-4".into()],
                )]),
            )
            .await;

            assert_eq!(status_code, StatusCode::OK);

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Change(
                    ChangeType::Insert,
                    RowId(4),
                    vec!["service-id-4".into(), "service-name-4".into()],
                    ChangeId(2)
                )
            );

            let mut res = api_v1_subs(
                Extension(agent.clone()),
                Extension(cache.clone()),
                Extension(bcast_cache.clone()),
                axum::extract::Query(SubParams {
                    from: Some(1.into()),
                }),
                axum::Json(Statement::Simple("select * from tests".into())),
            )
            .await
            .into_response();

            if !res.status().is_success() {
                let b = res.body_mut().data().await.unwrap().unwrap();
                println!("body: {}", String::from_utf8_lossy(&b));
            }

            assert_eq!(res.status(), StatusCode::OK);

            let mut rows_from = RowsIter {
                body: res.into_body(),
                codec: LinesCodec::new(),
                buf: BytesMut::new(),
                done: false,
            };

            assert_eq!(
                rows_from.recv().await.unwrap().unwrap(),
                QueryEvent::Change(
                    ChangeType::Insert,
                    RowId(4),
                    vec!["service-id-4".into(), "service-name-4".into()],
                    ChangeId(2)
                )
            );

            let (status_code, _) = api_v1_transactions(
                Extension(agent.clone()),
                axum::Json(vec![Statement::WithParams(
                    "insert into tests (id, text) values (?,?)".into(),
                    vec!["service-id-5".into(), "service-name-5".into()],
                )]),
            )
            .await;

            assert_eq!(status_code, StatusCode::OK);

            let query_evt = QueryEvent::Change(
                ChangeType::Insert,
                RowId(5),
                vec!["service-id-5".into(), "service-name-5".into()],
                ChangeId(3),
            );

            assert_eq!(rows.recv().await.unwrap().unwrap(), query_evt);

            assert_eq!(rows_from.recv().await.unwrap().unwrap(), query_evt);

            // subscriber who arrives later!

            let mut res = api_v1_subs(
                Extension(agent.clone()),
                Extension(cache.clone()),
                Extension(bcast_cache.clone()),
                axum::extract::Query(SubParams::default()),
                axum::Json(Statement::Simple("select * from tests".into())),
            )
            .await
            .into_response();

            if !res.status().is_success() {
                let b = res.body_mut().data().await.unwrap().unwrap();
                println!("body: {}", String::from_utf8_lossy(&b));
            }

            assert_eq!(res.status(), StatusCode::OK);

            let mut rows = RowsIter {
                body: res.into_body(),
                codec: LinesCodec::new(),
                buf: BytesMut::new(),
                done: false,
            };

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Columns(vec!["id".into(), "text".into()])
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(RowId(1), vec!["service-id".into(), "service-name".into()])
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(
                    RowId(2),
                    vec!["service-id-2".into(), "service-name-2".into()]
                )
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(
                    RowId(3),
                    vec!["service-id-3".into(), "service-name-3".into()],
                )
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(
                    RowId(4),
                    vec!["service-id-4".into(), "service-name-4".into()],
                )
            );

            assert_eq!(
                rows.recv().await.unwrap().unwrap(),
                QueryEvent::Row(
                    RowId(5),
                    vec!["service-id-5".into(), "service-name-5".into()]
                )
            );
        }

        // previous subs have been dropped.

        let mut res = api_v1_subs(
            Extension(agent.clone()),
            Extension(cache.clone()),
            Extension(bcast_cache.clone()),
            axum::extract::Query(SubParams {
                from: Some(1.into()),
            }),
            axum::Json(Statement::Simple("select * from tests".into())),
        )
        .await
        .into_response();

        if !res.status().is_success() {
            let b = res.body_mut().data().await.unwrap().unwrap();
            println!("body: {}", String::from_utf8_lossy(&b));
        }

        assert_eq!(res.status(), StatusCode::OK);

        let mut rows_from = RowsIter {
            body: res.into_body(),
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            done: false,
        };

        assert_eq!(
            rows_from.recv().await.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                RowId(4),
                vec!["service-id-4".into(), "service-name-4".into()],
                ChangeId(2)
            )
        );

        assert_eq!(
            rows_from.recv().await.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                RowId(5),
                vec!["service-id-5".into(), "service-name-5".into()],
                ChangeId(3),
            )
        );

        Ok(())
    }

    struct RowsIter {
        body: axum::body::BoxBody,
        codec: LinesCodec,
        buf: BytesMut,
        done: bool,
    }

    impl RowsIter {
        async fn recv(&mut self) -> Option<eyre::Result<QueryEvent>> {
            if self.done {
                return None;
            }
            loop {
                match self.codec.decode(&mut self.buf) {
                    Ok(Some(line)) => match serde_json::from_str(&line) {
                        Ok(res) => return Some(Ok(res)),
                        Err(e) => {
                            self.done = true;
                            return Some(Err(e.into()));
                        }
                    },
                    Ok(None) => {
                        // fall through
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e.into()));
                    }
                }

                let bytes_res = self.body.data().await;
                match bytes_res {
                    Some(Ok(b)) => {
                        // debug!("read {} bytes", b.len());
                        self.buf.extend_from_slice(&b)
                    }
                    Some(Err(e)) => {
                        self.done = true;
                        return Some(Err(e.into()));
                    }
                    None => {
                        self.done = true;
                        return None;
                    }
                }
            }
        }
    }
}
