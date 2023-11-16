use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::{format_compact, ToCompactString};
use corro_types::{
    agent::Agent,
    api::{ChangeId, QueryEvent, QueryEventMeta, Statement},
    pubsub::{MatcherError, MatcherHandle, NormalizeStatementError, SubsManager},
    sqlite::SqlitePoolError,
};
use futures::{future::poll_fn, ready, Stream};
use rusqlite::Connection;
use serde::Deserialize;
use tokio::{
    sync::{broadcast, mpsc, RwLock as TokioRwLock},
    task::block_in_place,
};
use tokio_stream::{wrappers::errors::BroadcastStreamRecvError, StreamExt};
use tokio_util::sync::PollSender;
use tracing::{debug, error, info, warn};
use tripwire::Tripwire;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, Deserialize)]
pub struct SubParams {
    #[serde(default)]
    from: Option<ChangeId>,
    #[serde(default)]
    skip_rows: bool,
}

pub async fn api_v1_sub_by_id(
    Extension(subs): Extension<SubsManager>,
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<SubParams>,
) -> impl IntoResponse {
    sub_by_id(&subs, id, params, &bcast_cache).await
}

async fn sub_by_id(
    subs: &SubsManager,
    id: Uuid,
    params: SubParams,
    bcast_cache: &SharedMatcherBroadcastCache,
) -> hyper::Response<hyper::Body> {
    let (matcher, rx) = match bcast_cache.read().await.get(&id).and_then(|tx| {
        subs.get(&id).map(|matcher| {
            debug!("found matcher by id {id}");
            (matcher, tx.subscribe())
        })
    }) {
        Some(matcher_rx) => matcher_rx,
        None => {
            // ensure this goes!
            bcast_cache.write().await.remove(&id);
            if let Some(handle) = subs.remove(&id) {
                info!(sub_id = %id, "Removed subscription from sub_by_id");
                tokio::spawn(handle.cleanup());
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

    tokio::spawn(catch_up_sub(matcher, params, rx, evt_tx));

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
    subs: SubsManager,
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
    let handle = match subs.remove(&id) {
        Some(h) => {
            info!(sub_id = %id, "Removed subscription from process_sub_channel");
            h
        }
        None => {
            warn!(sub_id = %id, "subscription handle was already gone. odd!");
            return;
        }
    };

    // clean up the subscription
    handle.cleanup().await;
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
    #[error("found a subscription, but missing broadcaster")]
    MissingBroadcaster,
}

impl MatcherUpsertError {
    fn status_code(&self) -> StatusCode {
        match self {
            MatcherUpsertError::Pool(_)
            | MatcherUpsertError::CouldNotExpand
            | MatcherUpsertError::MissingBroadcaster => StatusCode::INTERNAL_SERVER_ERROR,
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
pub type MatcherBroadcastCache = HashMap<Uuid, broadcast::Sender<(Bytes, QueryEventMeta)>>;
pub type SharedMatcherBroadcastCache = Arc<TokioRwLock<MatcherBroadcastCache>>;

#[derive(Debug, thiserror::Error)]
pub enum CatchUpError {
    #[error(transparent)]
    Pool(#[from] SqlitePoolError),
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),
    #[error(transparent)]
    Send(#[from] mpsc::error::SendError<Bytes>),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Matcher(#[from] MatcherError),
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
    conn: &Connection,
    matcher: &MatcherHandle,
    evt_tx: &mpsc::Sender<Bytes>,
) -> Result<ChangeId, CatchUpError> {
    let (q_tx, mut q_rx) = mpsc::channel(10240);

    tokio::spawn({
        let evt_tx = evt_tx.clone();
        async move {
            let mut buf = BytesMut::new();
            while let Some(event) = q_rx.recv().await {
                match make_query_event_bytes(&mut buf, event) {
                    Ok((b, _)) => {
                        if let Err(e) = evt_tx.send(b).await {
                            error!("could not send catching up event bytes: {e}");
                            return;
                        }
                    }
                    Err(e) => {
                        error!("could not serialize catching up event: {e}");
                        return;
                    }
                }
            }
        }
    });

    matcher.all_rows(conn, q_tx).map_err(CatchUpError::from)
}

fn catch_up_sub_from(
    conn: &Connection, // read transaction
    matcher: &MatcherHandle,
    from: ChangeId,
    evt_tx: &mpsc::Sender<Bytes>,
) -> Result<ChangeId, CatchUpError> {
    let (q_tx, mut q_rx) = mpsc::channel(10240);

    tokio::spawn({
        let evt_tx = evt_tx.clone();
        async move {
            let mut buf = BytesMut::new();
            while let Some(event) = q_rx.recv().await {
                match make_query_event_bytes(&mut buf, event) {
                    Ok((b, _)) => {
                        if let Err(e) = evt_tx.send(b).await {
                            error!("could not send catching up event bytes: {e}");
                            return;
                        }
                    }
                    Err(e) => {
                        error!("could not serialize catching up event: {e}");
                        return;
                    }
                }
            }
        }
    });

    matcher
        .changes_since(from, conn, q_tx)
        .map_err(CatchUpError::from)
}

pub async fn catch_up_sub(
    matcher: MatcherHandle,
    params: SubParams,
    mut sub_rx: broadcast::Receiver<(Bytes, QueryEventMeta)>,
    evt_tx: mpsc::Sender<Bytes>,
) {
    debug!("catching up sub {} params: {:?}", matcher.id(), params);

    let mut buf = BytesMut::new();
    let mut events_buf = VecDeque::new();

    let sub_fut = async {
        loop {
            let (buf, meta) = sub_rx.recv().await?;
            if let QueryEventMeta::Change(change_id) = meta {
                events_buf.push_back((buf, change_id));
            }
            if events_buf.len() > MAX_EVENTS_BUFFER_SIZE {
                return Err::<(), _>(eyre::eyre!("catching up too slowly, gave up after buffering {MAX_EVENTS_BUFFER_SIZE} events"));
            }
        }
    };

    let catch_up_fut = async {
        let mut conn = matcher.pool().get().await?;

        block_in_place(|| {
            let tx = conn.transaction()?; // read transaction
            match params.from {
                Some(from) => {
                    let change_id = catch_up_sub_from(&tx, &matcher, from, &evt_tx)?;
                    debug!("sub caught up to their 'from' of {from:?}");
                    Ok(change_id)
                }
                None => {
                    if params.skip_rows {
                        Ok(matcher.max_change_id(&tx)?)
                    } else {
                        let change_id = catch_up_sub_anew(&tx, &matcher, &evt_tx)?;
                        debug!("sub caught up from scratch");
                        Ok(change_id)
                    }
                }
            }
        })
    };

    let mut last_change_id = tokio::select! {
        res = sub_fut => {
            // if this finishes first, then we should return
            match res {
                Ok(_) => {
                    _ = evt_tx.send(error_to_query_event_bytes(&mut buf, "subscription channel returned early, aborting")).await;
                },
                Err(e) => {
                    _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                }
            }
            return;
        },
        res = catch_up_fut => {
            match res {
                Ok(change_id) => change_id,
                Err(e) => {
                    match e {
                        CatchUpError::Pool(e) => {
                            _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                        }
                        CatchUpError::Sqlite(e) => {
                            _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                        }
                        CatchUpError::SerdeJson(e) => {
                            _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                        }
                        CatchUpError::Send(_) => {
                            // can't send
                        }
                        CatchUpError::Matcher(_) => {
                            // upstream error
                            _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                        }
                    }
                    return;
                }
            }
        }
    };

    let last_received_change_id = match events_buf.get(0) {
        Some((_buf, change_id)) => {
            info!(sub_id = %matcher.id(), "last change id received by subscription: {change_id:?}");
            Some(*change_id)
        }
        None => {
            if let Ok((event_buf, meta)) = sub_rx.try_recv() {
                if let QueryEventMeta::Change(change_id) = meta {
                    info!(sub_id = %matcher.id(), "received a change event, using it as last change id to catch up to: {change_id:?}");
                    events_buf.push_back((event_buf, change_id));
                    Some(change_id)
                } else {
                    None
                }
            } else {
                let last_change_id_sent = matcher.last_change_id_sent();
                info!(sub_id = %matcher.id(), "last change id sent by subscription: {last_change_id_sent:?}");
                if last_change_id_sent.0 <= last_change_id.0 {
                    None
                } else {
                    Some(last_change_id_sent)
                }
            }
        }
    };

    if let Some(change_id) = last_received_change_id {
        debug!(sub_id = %matcher.id(), "got a change to check: {change_id:?}");
        for i in 0..5 {
            if change_id.0 > last_change_id.0 + 1 {
                // missed some updates!
                info!(sub_id = %matcher.id(), "attempt #{} to catch up subcription from change id: {change_id:?} (last: {last_change_id:?})", i+1);

                let mut conn = match matcher.pool().get().await {
                    Ok(conn) => conn,
                    Err(e) => {
                        _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                        return;
                    }
                };

                let res = block_in_place(|| {
                    let tx = conn.transaction()?; // read transaction
                    let new_last_change_id =
                        catch_up_sub_from(&tx, &matcher, last_change_id, &evt_tx)?;
                    debug!("sub caught up to the last max change id we got! {last_change_id:?} -> {new_last_change_id:?}");
                    Ok(new_last_change_id)
                });

                match res {
                    Ok(new_last_change_id) => last_change_id = new_last_change_id,
                    Err(e) => {
                        match e {
                            CatchUpError::Pool(e) => {
                                _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                            }
                            CatchUpError::Sqlite(e) => {
                                _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                            }
                            CatchUpError::SerdeJson(e) => {
                                _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                            }
                            CatchUpError::Send(_) => {
                                // can't send
                            }
                            CatchUpError::Matcher(_) => {
                                // upstream error
                                _ = evt_tx.send(error_to_query_event_bytes(&mut buf, e)).await;
                            }
                        }
                        return;
                    }
                }
            } else {
                break;
            }
            // sleep 100 millis
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if change_id.0 > last_change_id.0 + 1 {
            _ = evt_tx
                .send(error_to_query_event_bytes(
                    &mut buf,
                    "could not catch up to changes in 5 attempts",
                ))
                .await;
            return;
        }
    }

    info!(sub_id = %matcher.id(), "subscription is caught up, no gaps in change id. last change id: {last_change_id:?}, last_received_change_id: {last_received_change_id:?}");

    for (bytes, change_id) in events_buf {
        if change_id > last_change_id {
            if let Err(_e) = evt_tx.send(bytes).await {
                warn!("could not send buffered events to subscriber, receiver must be gone!");
                return;
            }
        }
    }

    forward_sub_to_sender(sub_rx, evt_tx).await
}

pub async fn upsert_sub(
    agent: &Agent,
    subs: &SubsManager,
    bcast_cache: &SharedMatcherBroadcastCache,
    stmt: Statement,
    params: SubParams,
    tx: mpsc::Sender<Bytes>,
    tripwire: Tripwire,
) -> Result<Uuid, MatcherUpsertError> {
    let stmt = expand_sql(agent, &stmt).await?;

    let mut bcast_write = bcast_cache.write().await;

    let (handle, maybe_created) = subs.get_or_insert(
        &stmt,
        &agent.config().db.subscriptions_path(),
        &agent.schema().read(),
        agent.pool(),
        agent.rx_db_version(),
        tripwire,
    )?;

    if let Some(created) = maybe_created {
        if params.from.is_some() {
            return Err(MatcherUpsertError::SubFromWithoutMatcher);
        }

        let (sub_tx, sub_rx) = broadcast::channel(10240);

        tokio::spawn(forward_sub_to_sender(sub_rx, tx));

        bcast_write.insert(handle.id(), sub_tx.clone());

        tokio::spawn(process_sub_channel(
            subs.clone(),
            handle.id(),
            sub_tx,
            created.evt_rx,
        ));

        Ok(handle.id())
    } else {
        let id = handle.id();
        let sub_tx = bcast_write
            .get(&id)
            .cloned()
            .ok_or(MatcherUpsertError::MissingBroadcaster)?;
        debug!("found matcher handle");

        tokio::spawn(catch_up_sub(handle, params, sub_tx.subscribe(), tx));

        Ok(id)
    }
}

pub async fn api_v1_subs(
    Extension(agent): Extension<Agent>,
    Extension(subs): Extension<SubsManager>,
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Query(params): axum::extract::Query<SubParams>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let (tx, body) = hyper::Body::channel();
    let (forward_tx, forward_rx) = mpsc::channel(10240);

    let matcher_id = match upsert_sub(
        &agent,
        &subs,
        &bcast_cache,
        stmt,
        params,
        forward_tx,
        tripwire,
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
    sub_rx: broadcast::Receiver<(Bytes, QueryEventMeta)>,
    tx: mpsc::Sender<Bytes>,
) {
    let mut buf = BytesMut::new();

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
            tripwire.clone(),
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

        let subs = SubsManager::default();
        let bcast_cache: SharedMatcherBroadcastCache = Default::default();

        {
            let mut res = api_v1_subs(
                Extension(agent.clone()),
                Extension(subs.clone()),
                Extension(bcast_cache.clone()),
                Extension(tripwire.clone()),
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
                Extension(subs.clone()),
                Extension(bcast_cache.clone()),
                Extension(tripwire.clone()),
                axum::extract::Query(SubParams {
                    from: Some(1.into()),
                    ..Default::default()
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
                Extension(subs.clone()),
                Extension(bcast_cache.clone()),
                Extension(tripwire.clone()),
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
            Extension(subs.clone()),
            Extension(bcast_cache.clone()),
            Extension(tripwire.clone()),
            axum::extract::Query(SubParams {
                from: Some(1.into()),
                ..Default::default()
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

        // skip rows!

        let mut res = api_v1_subs(
            Extension(agent.clone()),
            Extension(subs.clone()),
            Extension(bcast_cache.clone()),
            Extension(tripwire.clone()),
            axum::extract::Query(SubParams {
                skip_rows: true,
                ..Default::default()
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

        let (status_code, _) = api_v1_transactions(
            Extension(agent.clone()),
            axum::Json(vec![Statement::WithParams(
                "insert into tests (id, text) values (?,?)".into(),
                vec!["service-id-6".into(), "service-name-6".into()],
            )]),
        )
        .await;

        assert_eq!(status_code, StatusCode::OK);

        assert_eq!(
            rows_from.recv().await.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                RowId(6),
                vec!["service-id-6".into(), "service-name-6".into()],
                ChangeId(4),
            )
        );

        // skip rows AND from

        let mut res = api_v1_subs(
            Extension(agent.clone()),
            Extension(subs.clone()),
            Extension(bcast_cache.clone()),
            Extension(tripwire.clone()),
            axum::extract::Query(SubParams {
                skip_rows: true,
                from: Some(ChangeId(3)),
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
                RowId(6),
                vec!["service-id-6".into(), "service-name-6".into()],
                ChangeId(4),
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
