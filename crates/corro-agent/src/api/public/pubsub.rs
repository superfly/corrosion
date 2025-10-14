use std::{collections::HashMap, io::Write, sync::Arc, time::Duration};

use crate::api::utils::CountedBody;
use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::{format_compact, ToCompactString};
use corro_types::persistent_gauge;
use corro_types::updates::Handle;
use corro_types::{
    agent::Agent,
    api::{ChangeId, QueryEvent, QueryEventMeta, Statement},
    pubsub::{MatcherCreated, MatcherError, MatcherHandle, NormalizeStatementError, SubsManager},
    sqlite::SqlitePoolError,
};
use futures::future::poll_fn;
use rusqlite::Connection;
use serde::Deserialize;
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        mpsc::{self, error::TryRecvError},
        RwLock as TokioRwLock,
    },
    task::{block_in_place, JoinError},
};
use tokio_util::sync::CancellationToken;
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
    Extension(agent): Extension<Agent>,
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    axum::extract::Query(params): axum::extract::Query<SubParams>,
) -> impl IntoResponse {
    sub_by_id(agent.subs_manager(), id, params, &bcast_cache, tripwire).await
}

async fn sub_by_id(
    subs: &SubsManager,
    id: Uuid,
    params: SubParams,
    bcast_cache: &SharedMatcherBroadcastCache,
    tripwire: Tripwire,
) -> impl IntoResponse {
    let matcher_rx = bcast_cache.read().await.get(&id).and_then(|tx| {
        subs.get(&id).map(|matcher| {
            debug!("found matcher by id {id}");
            (matcher, tx.subscribe())
        })
    });

    let (matcher, rx) = match matcher_rx {
        Some(matcher_rx) => matcher_rx,
        None => {
            // ensure this goes!
            let mut bcast_cache_write = bcast_cache.write().await;

            if let Some(matcher_rx) = bcast_cache_write.get(&id).and_then(|tx| {
                subs.get(&id).map(|matcher| {
                    debug!("found matcher by id {id}");
                    (matcher, tx.subscribe())
                })
            }) {
                matcher_rx
            } else {
                bcast_cache_write.remove(&id);
                if let Some(handle) = subs.remove(&id) {
                    info!(sub_id = %id, "Removed subscription from sub_by_id");
                    tokio::spawn(async move {
                        handle.cleanup().await;
                    });
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
        }
    };

    let (evt_tx, evt_rx) = mpsc::channel(512);

    let query_hash = matcher.hash().to_owned();
    tokio::spawn(catch_up_sub(matcher, params, rx, evt_tx));

    let (tx, body) = CountedBody::channel(
        persistent_gauge!("corro.api.active.streams", "source" => "subscriptions", "protocol" => "http"),
    );

    tokio::spawn(forward_bytes_to_body_sender(id, evt_rx, tx, tripwire));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", id.to_string())
        .header("corro-query-hash", query_hash)
        .body(body)
        .expect("could not build query response body")
}

fn make_query_event_bytes(
    buf: &mut BytesMut,
    query_evt: &QueryEvent,
) -> serde_json::Result<(Bytes, QueryEventMeta)> {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, query_evt)?;

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    Ok((buf.split().freeze(), query_evt.meta()))
}

const MAX_UNSUB_TIME: Duration = Duration::from_secs(10 * 60);
// this should be a fraction of the MAX_UNSUB_TIME
const RECEIVERS_CHECK_INTERVAL: Duration = Duration::from_secs(60);

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
                if tx.receiver_count() == 0 {
                    info!(sub_id = %id, "All listeners for subscription are gone and didn't come back within {MAX_UNSUB_TIME:?}");
                    break;
                }

                // reset the deadline if there are receivers!
                deadline = None;
                continue;
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

        let is_still_active = match make_query_event_bytes(&mut buf, &query_evt) {
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
            debug!(sub_id = %id, "no active listeners to receive subscription event: {query_evt:?}");
            if deadline.is_none() {
                deadline = Some(Box::pin(tokio::time::sleep(MAX_UNSUB_TIME)));
            }
        }
    }

    warn!(sub_id = %id, "subscription query channel done");

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
    NormalizeStatement(#[from] Box<NormalizeStatementError>),
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

impl From<MatcherUpsertError> for hyper::Response<CountedBody<hyper::Body>> {
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
    Send(#[from] mpsc::error::SendError<(Bytes, QueryEventMeta)>),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Matcher(#[from] MatcherError),
    #[error(transparent)]
    Join(#[from] JoinError),
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

fn error_to_query_event_bytes_with_meta<E: ToCompactString>(
    buf: &mut BytesMut,
    e: E,
) -> (Bytes, QueryEventMeta) {
    (error_to_query_event_bytes(buf, e), QueryEventMeta::Error)
}

async fn catch_up_sub_anew(
    matcher: &MatcherHandle,
    evt_tx: &mpsc::Sender<(Bytes, QueryEventMeta)>,
) -> Result<ChangeId, CatchUpError> {
    let (q_tx, mut q_rx) = mpsc::channel(10240);

    let task = tokio::spawn({
        let evt_tx = evt_tx.clone();
        async move {
            let mut buf = BytesMut::new();
            while let Some(event) = q_rx.recv().await {
                evt_tx
                    .send(make_query_event_bytes(&mut buf, &event)?)
                    .await?;
            }
            Ok::<_, CatchUpError>(())
        }
    });

    let last_change_id = {
        let mut conn = matcher.pool().get().await?;
        block_in_place(|| {
            let conn_tx = conn.transaction()?;
            matcher.all_rows(&conn_tx, q_tx)
        })?
    };

    task.await??;

    Ok(last_change_id)
}

async fn catch_up_sub_from(
    matcher: &MatcherHandle,
    from: ChangeId,
    evt_tx: &mpsc::Sender<(Bytes, QueryEventMeta)>,
) -> Result<ChangeId, CatchUpError> {
    let (q_tx, mut q_rx) = mpsc::channel(10240);

    let task = tokio::spawn({
        let evt_tx = evt_tx.clone();
        async move {
            let mut buf = BytesMut::new();
            while let Some(event) = q_rx.recv().await {
                evt_tx
                    .send(make_query_event_bytes(&mut buf, &event)?)
                    .await?;
            }
            Ok::<_, CatchUpError>(())
        }
    });

    let last_change_id = {
        let conn = matcher.pool().get().await?;
        block_in_place(|| matcher.changes_since(from, &conn, q_tx))?
    };

    task.await??;

    Ok(last_change_id)
}

pub async fn catch_up_sub(
    matcher: MatcherHandle,
    params: SubParams,
    mut sub_rx: broadcast::Receiver<(Bytes, QueryEventMeta)>,
    evt_tx: mpsc::Sender<(Bytes, QueryEventMeta)>,
) {
    debug!("catching up sub {} params: {:?}", matcher.id(), params);

    let mut buf = BytesMut::new();

    // buffer events while we catch up...
    let (queue_tx, mut queue_rx) = mpsc::channel(10240);

    let cancel = CancellationToken::new();
    // just in case...
    let _drop_guard = cancel.clone().drop_guard();

    let queue_task = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            loop {
                let (buf, meta) = tokio::select! {
                    _ = cancel.cancelled() => {
                        break;
                    },
                    Ok(res) = sub_rx.recv() => res,
                    else => break
                };

                if let QueryEventMeta::Change(change_id) = meta {
                    if let Err(_e) = queue_tx.try_send((buf, change_id)) {
                        return Err(eyre::eyre!("catching up too slowly, gave up after buffering {MAX_EVENTS_BUFFER_SIZE} events"));
                    }
                }
            }
            Ok(sub_rx)
        }
    });

    let mut last_change_id = {
        let res = match params.from {
            Some(ChangeId(0)) | None => {
                if params.skip_rows {
                    let conn = match matcher.pool().get().await {
                        Ok(conn) => conn,
                        Err(e) => {
                            _ = evt_tx
                                .send(error_to_query_event_bytes_with_meta(&mut buf, e))
                                .await;
                            return;
                        }
                    };
                    block_in_place(|| matcher.max_change_id(&conn)).map_err(CatchUpError::from)
                } else {
                    catch_up_sub_anew(&matcher, &evt_tx).await
                }
            }
            Some(from) => catch_up_sub_from(&matcher, from, &evt_tx).await,
        };

        match res {
            Ok(change_id) => change_id,
            Err(e) => {
                if !matches!(e, CatchUpError::Send(_)) {
                    _ = evt_tx
                        .send(error_to_query_event_bytes_with_meta(&mut buf, e))
                        .await;
                }
                return;
            }
        }
    };

    let mut min_change_id = last_change_id + 1;
    info!(sub_id = %matcher.id(), "minimum expected change id: {min_change_id:?}");

    let mut pending_event = None;

    let last_sub_change_id = match queue_rx.try_recv() {
        Ok((event_buf, change_id)) => {
            info!(sub_id = %matcher.id(), "last change id received by subscription: {change_id:?}");
            pending_event = Some((event_buf, change_id));
            Some(change_id)
        }
        Err(e) => match e {
            TryRecvError::Empty => {
                let last_change_id_sent = matcher.last_change_id_sent();
                info!(sub_id = %matcher.id(), "last change id sent by subscription: {last_change_id_sent:?}");
                if last_change_id_sent <= last_change_id {
                    None
                } else {
                    Some(last_change_id_sent)
                }
            }
            TryRecvError::Disconnected => {
                // abnormal
                _ = evt_tx
                    .send(error_to_query_event_bytes_with_meta(
                        &mut buf,
                        "exceeded events buffer",
                    ))
                    .await;
                return;
            }
        },
    };

    if let Some(change_id) = last_sub_change_id {
        debug!(sub_id = %matcher.id(), "got a change to check: {change_id:?}");
        for i in 0..5 {
            min_change_id = last_change_id + 1;

            if change_id >= min_change_id {
                // missed some updates!
                info!(sub_id = %matcher.id(), "attempt #{} to catch up subcription from change id: {change_id:?} (last: {last_change_id:?})", i+1);

                let res = catch_up_sub_from(&matcher, last_change_id, &evt_tx).await;

                match res {
                    Ok(new_last_change_id) => last_change_id = new_last_change_id,
                    Err(e) => {
                        if !matches!(e, CatchUpError::Send(_)) {
                            _ = evt_tx
                                .send(error_to_query_event_bytes_with_meta(&mut buf, e))
                                .await;
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
        if change_id >= min_change_id {
            _ = evt_tx
                .send(error_to_query_event_bytes_with_meta(
                    &mut buf,
                    "could not catch up to changes in 5 attempts",
                ))
                .await;
            return;
        }
    }

    info!(sub_id = %matcher.id(), "subscription is caught up, no gaps in change id. last change id: {last_change_id:?}, last_sub_change_id: {last_sub_change_id:?}");

    if let Some((event_buf, change_id)) = pending_event {
        info!(sub_id = %matcher.id(), "had a pending event we popped from the queue, id: {change_id:?} (last change id: {last_change_id:?})");
        if change_id > last_change_id {
            info!(sub_id = %matcher.id(), "change was more recent, sending!");
            if let Err(_e) = evt_tx
                .send((event_buf, QueryEventMeta::Change(change_id)))
                .await
            {
                warn!(sub_id = %matcher.id(), "could not send buffered events to subscriber, receiver must be gone!");
                return;
            }

            last_change_id = change_id;
        }
    }

    // cancel queue task!
    cancel.cancel();

    while let Some((event_buf, change_id)) = queue_rx.recv().await {
        info!(sub_id = %matcher.id(), "processing buffered change, id: {change_id:?} (last change id: {last_change_id:?})");
        if change_id > last_change_id {
            info!(sub_id = %matcher.id(), "change was more recent, sending!");
            if let Err(_e) = evt_tx
                .send((event_buf, QueryEventMeta::Change(change_id)))
                .await
            {
                warn!(sub_id = %matcher.id(), "could not send buffered events to subscriber, receiver must be gone!");
                return;
            }

            last_change_id = change_id;
        }
    }

    let sub_rx = match queue_task.await {
        Ok(Ok(sub_rx)) => sub_rx,
        Ok(Err(e)) => {
            _ = evt_tx
                .send(error_to_query_event_bytes_with_meta(&mut buf, e))
                .await;
            return;
        }
        Err(e) => {
            _ = evt_tx
                .send(error_to_query_event_bytes_with_meta(&mut buf, e))
                .await;
            return;
        }
    };

    forward_sub_to_sender(matcher, sub_rx, evt_tx, params.skip_rows).await
}

pub async fn upsert_sub(
    handle: MatcherHandle,
    maybe_created: Option<MatcherCreated>,
    subs: &SubsManager,
    bcast_write: &mut MatcherBroadcastCache,
    params: SubParams,
    tx: mpsc::Sender<(Bytes, QueryEventMeta)>,
) -> Result<Uuid, MatcherUpsertError> {
    if let Some(created) = maybe_created {
        if params.from.is_some() {
            handle.cleanup().await;
            subs.remove(&handle.id());
            return Err(MatcherUpsertError::SubFromWithoutMatcher);
        }

        let (sub_tx, sub_rx) = broadcast::channel(10240);

        tokio::spawn(forward_sub_to_sender(
            handle.clone(),
            sub_rx,
            tx,
            params.skip_rows,
        ));

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
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Query(params): axum::extract::Query<SubParams>,
    axum::extract::Json(stmt): axum::extract::Json<Statement>,
) -> impl IntoResponse {
    let stmt = match expand_sql(&agent, &stmt).await {
        Ok(stmt) => stmt,
        Err(e) => return hyper::Response::<CountedBody<hyper::Body>>::from(e),
    };

    info!("Received subscription request for query: {stmt}");

    let mut bcast_write = bcast_cache.write().await;

    let subs = agent.subs_manager();

    let upsert_res = subs.get_or_insert(
        &stmt,
        &agent.config().db.subscriptions_path(),
        &agent.schema().read(),
        agent.pool(),
        tripwire.clone(),
    );

    let (handle, maybe_created) = match upsert_res {
        Ok(res) => res,
        Err(e) => {
            return hyper::Response::<CountedBody<hyper::Body>>::from(MatcherUpsertError::from(e))
        }
    };

    let (tx, body) = CountedBody::channel(
        persistent_gauge!("corro.api.active.streams", "source" => "subscriptions", "protocol" => "http"),
    );
    let (forward_tx, forward_rx) = mpsc::channel(10240);

    tokio::spawn(forward_bytes_to_body_sender(
        handle.id(),
        forward_rx,
        tx,
        tripwire,
    ));

    let query_hash = handle.hash().to_owned();
    let matcher_id = match upsert_sub(
        handle,
        maybe_created,
        subs,
        &mut bcast_write,
        params,
        forward_tx,
    )
    .await
    {
        Ok(id) => id,
        Err(e) => return hyper::Response::<CountedBody<hyper::Body>>::from(e),
    };

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", matcher_id.to_string())
        .header("corro-query-hash", query_hash)
        .body(body)
        .expect("could not generate ok http response for query request")
}

const MAX_EVENTS_BUFFER_SIZE: usize = 1024;

async fn forward_sub_to_sender(
    handle: MatcherHandle,
    mut sub_rx: broadcast::Receiver<(Bytes, QueryEventMeta)>,
    tx: mpsc::Sender<(Bytes, QueryEventMeta)>,
    skip_rows: bool,
) {
    info!(sub_id = %handle.id(), "forwarding subscription events to a sender");

    loop {
        let (event_buf, meta) = tokio::select! {
            res = sub_rx.recv() => {
                match res {
                    Ok((event_buf, meta)) => (event_buf, meta),
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(sub_id = %handle.id(), "subscription skipped {} events, aborting", skipped);
                        return;
                    },
                    Err(RecvError::Closed) => {
                        info!(sub_id = %handle.id(), "events subcription ran out");
                        return;
                    },
                }
            },
            _ = handle.cancelled() => {
                info!(sub_id = %handle.id(), "subscription cancelled, aborting forwarding bytes to subscriber");
                return;
            },
        };

        if skip_rows
            && matches!(
                meta,
                QueryEventMeta::Columns | QueryEventMeta::Row(_) | QueryEventMeta::EndOfQuery(_)
            )
        {
            continue;
        }
        if let Err(e) = tx.send((event_buf, meta)).await {
            warn!(sub_id = %handle.id(), "could not send subscription event to channel: {e}");
            return;
        }
    }
}

async fn handle_sub_event(
    sub_id: Uuid,
    buf: &mut BytesMut,
    event_buf: Bytes,
    meta: QueryEventMeta,
    tx: &mut hyper::body::Sender,
    last_change_id: &mut ChangeId,
) -> hyper::Result<()> {
    match meta {
        QueryEventMeta::EndOfQuery(Some(change_id)) | QueryEventMeta::Change(change_id) => {
            if !last_change_id.is_zero() && change_id > *last_change_id + 1 {
                warn!(%sub_id, "non-contiguous change id (> + 1) received: {change_id:?}, last seen: {last_change_id:?}");
            } else if !last_change_id.is_zero() && change_id == *last_change_id {
                warn!(%sub_id, "duplicate change id received: {change_id:?}, last seen: {last_change_id:?}");
            } else if change_id < *last_change_id {
                warn!(%sub_id, "smaller change id received: {change_id:?}, last seen: {last_change_id:?}");
            }
            *last_change_id = change_id;
        }
        _ => {
            // do nothing
        }
    }
    buf.extend_from_slice(&event_buf);
    let to_send = if buf.len() >= 64 * 1024 {
        buf.split().freeze()
    } else {
        return Ok(());
    };

    tx.send_data(to_send).await
}

async fn forward_bytes_to_body_sender(
    sub_id: Uuid,
    mut rx: mpsc::Receiver<(Bytes, QueryEventMeta)>,
    mut tx: hyper::body::Sender,
    mut tripwire: Tripwire,
) {
    let mut buf = BytesMut::new();

    let send_deadline = tokio::time::sleep(Duration::from_millis(10));
    tokio::pin!(send_deadline);

    let mut last_change_id = ChangeId(0);

    loop {
        tokio::select! {
            biased;
            res = rx.recv() => {
                match res {
                    Some((event_buf, meta)) => {
                        if let Err(e) = handle_sub_event(sub_id, &mut buf, event_buf, meta, &mut tx, &mut last_change_id).await {
                            warn!(%sub_id, "could not forward subscription query event to receiver: {e}");
                            return;
                        }
                    },
                    None => break,
                }
            },
            _ = &mut send_deadline => {
                if !buf.is_empty() {
                    if let Err(e) = tx.send_data(buf.split().freeze()).await {
                        warn!(%sub_id, "could not forward subscription query event to receiver: {e}");
                        return;
                    }
                } else {
                    if let Err(e) = poll_fn(|cx| tx.poll_ready(cx)).await {
                        warn!(%sub_id, error = %e, "body sender was closed or errored, stopping event broadcast sends");
                        return;
                    }
                    send_deadline.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(10));
                    continue;
                }
            },
            _ = &mut tripwire => {
                break;
            }
        }
    }

    while let Ok((event_buf, meta)) = rx.try_recv() {
        if let Err(e) = handle_sub_event(
            sub_id,
            &mut buf,
            event_buf,
            meta,
            &mut tx,
            &mut last_change_id,
        )
        .await
        {
            warn!(%sub_id, "could not forward subscription query event to receiver: {e}");
            return;
        }
    }

    if !buf.is_empty() {
        if let Err(e) = tx.send_data(buf.freeze()).await {
            warn!(%sub_id, "could not forward last subscription query event to receiver: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use corro_types::actor::ActorId;
    use corro_types::api::{ColumnName, TableName};
    use corro_types::api::{NotifyEvent, SqliteValue};
    use corro_types::base::{dbsr, CrsqlDbVersion, CrsqlSeq};
    use corro_types::broadcast::{ChangeSource, ChangeV1, Changeset};
    use corro_types::change::Change;
    use corro_types::pubsub::pack_columns;
    use corro_types::{
        api::{ChangeId, RowId},
        pubsub::ChangeType,
    };
    use eyre::{Result, WrapErr};
    use http_body::Body;
    use serde::de::DeserializeOwned;
    use spawn::wait_for_all_pending_handles;
    use std::time::Instant;
    use tokio::time::timeout;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tokio_util::either::Either;
    use tripwire::{Tripwire, TripwireWorker};

    use super::*;
    use crate::agent::process_multiple_changes;
    use crate::api::public::update::{api_v1_updates, SharedUpdateBroadcastCache};
    use crate::api::public::TimeoutParams;
    use crate::api::public::{api_v1_db_schema, api_v1_transactions};
    use corro_tests::launch_test_agent;
    use corro_types::api::SqliteValue::Integer;

    struct PubSubAgent {
        ta: corro_tests::TestAgent,
        ta_client: corro_tests::CorrosionClient,
        subs_bcast_cache: SharedMatcherBroadcastCache,
        updates_bcast_cache: SharedUpdateBroadcastCache,
        tripwire: Tripwire,
    }

    #[derive(Debug)]
    struct SubscriptionStream {
        iter: RowsIter,
        sub_id: Uuid,
    }

    impl SubscriptionStream {
        // If skip_rows is false and we're starting the subscription from the beginning, we should see
        // the initial query results before getting any updates.
        // We expect Columns, Rows x N, and then EndOfQuery.
        async fn assert_initial_query_results(
            &mut self,
            expected_column_names: Vec<ColumnName>,
            expected_rows: Vec<(RowId, &Vec<SqliteValue>)>,
            expected_change_id: ChangeId,
        ) -> () {
            assert_eq!(
                self.iter.recv::<QueryEvent>().await.unwrap().unwrap(),
                QueryEvent::Columns(expected_column_names)
            );

            for (row_id, row) in expected_rows {
                assert_eq!(
                    self.iter.recv::<QueryEvent>().await.unwrap().unwrap(),
                    QueryEvent::Row(row_id, row.to_vec())
                );
            }

            assert_eq!(
                self.iter
                    .recv::<QueryEvent>()
                    .await
                    .unwrap()
                    .unwrap()
                    .meta(),
                QueryEventMeta::EndOfQuery(Some(expected_change_id))
            );
        }

        async fn assert_updates(
            &mut self,
            expected_updates: Vec<(ChangeType, RowId, ChangeId, &Vec<SqliteValue>)>,
        ) {
            for (change_type, row_id, change_id, row) in expected_updates {
                assert_eq!(
                    self.iter.recv::<QueryEvent>().await.unwrap().unwrap(),
                    QueryEvent::Change(change_type, row_id, row.to_vec(), change_id)
                );
            }
        }

        async fn assert_no_more_events(&mut self) {
            // Assume no more events within 100ms. There is also a 2s timeout in recv.
            assert!(
                timeout(Duration::from_millis(100), self.iter.recv::<QueryEvent>())
                    .await
                    .is_err()
            );
        }

        async fn receive_all_events(&mut self) -> Vec<QueryEvent> {
            let mut events = Vec::new();
            // Receive events until reading times out
            while let Ok(data) =
                timeout(Duration::from_millis(100), self.iter.recv::<QueryEvent>()).await
            {
                if let Some(Ok(event)) = data {
                    events.push(event);
                } else if let Some(Err(err)) = data {
                    println!("SubscriptionStream receive_all_events: {:?}", err);
                }
                if self.iter.done {
                    break;
                }
            }
            events
        }
    }

    const TEST_QUERY1: &str = "select * from tests";
    const TEST_QUERY1_WITH_SPACES: &str = "select           *             from tests";
    const TEST_QUERY1_EXPLICIT: &str = "select id, text from tests";
    const TEST_QUERY2: &str =
        "select * from tests where substr(id, -1) IN ('0', '2', '4', '6', '8')";
    const TEST_QUERY3: &str =
        "select * from tests where substr(id, -1) IN ('1', '3', '5', '7', '9')";
    
    /// Helper struct for executing prepared statements
    struct PreparedStatement<'a> {
        agent: &'a PubSubAgent,
        sql: &'static str,
    }

    impl<'a> PreparedStatement<'a> {
        async fn execute<P, I, V>(&self, rows: I) -> eyre::Result<()>
        where
            I: IntoIterator<Item = P>,
            P: IntoIterator<Item = V>,
            V: Into<SqliteValue>,
        {
            let statements: Vec<Statement> = rows
                .into_iter()
                .map(|params| {
                    Statement::WithParams(
                        self.sql.into(),
                        params.into_iter().map(|v| v.into().into()).collect(),
                    )
                })
                .collect();
            
            let count = statements.len();
            let (status_code, body) = api_v1_transactions(
                Extension(self.agent.ta.agent.clone()),
                axum::extract::Query(TimeoutParams { timeout: None }),
                axum::Json(statements),
            )
            .await;

            assert_eq!(status_code, StatusCode::OK);
            assert_eq!(body.0.results.len(), count);
            Ok(())
        }
    }

    impl PubSubAgent {
        async fn subscribe_to_test_table(
            &self,
            sub_params: SubParams,
            query_or_sub_id: Either<corro_types::api::Statement, Uuid>,
        ) -> eyre::Result<SubscriptionStream> {
            let mut res = match &query_or_sub_id {
                Either::Right(sub_id) => api_v1_sub_by_id(
                    Extension(self.ta.agent.clone()),
                    Extension(self.subs_bcast_cache.clone()),
                    Extension(self.tripwire.clone()),
                    axum::extract::Path(*sub_id),
                    axum::extract::Query(sub_params),
                )
                .await
                .into_response(),
                Either::Left(statement) => api_v1_subs(
                    Extension(self.ta.agent.clone()),
                    Extension(self.subs_bcast_cache.clone()),
                    Extension(self.tripwire.clone()),
                    axum::extract::Query(sub_params),
                    axum::Json(statement.clone()),
                )
                .await
                .into_response(),
            };

            if res.status() != StatusCode::OK {
                let b = res.body_mut().data().await.unwrap().unwrap();
                return Err(eyre::eyre!(String::from_utf8_lossy(&b).to_string()));
            }

            // The api should always return a corro-query-id header
            let query_id_header = res
                .headers()
                .get("corro-query-id")
                .ok_or(eyre::eyre!("missing corro-query-id header"))?;
            let res_sub_id = Uuid::parse_str(query_id_header.to_str().unwrap())?;

            // If a sub_id was provided, it should match the response sub_id
            if let Either::Right(sub_id) = query_or_sub_id {
                assert_eq!(sub_id, res_sub_id);
            }

            Ok(SubscriptionStream {
                iter: RowsIter {
                    body: res.into_body(),
                    codec: LinesCodec::new(),
                    buf: BytesMut::new(),
                    done: false,
                },
                sub_id: res_sub_id,
            })
        }

        /// Prepares a statement that can be executed multiple times with different parameters
        fn prepare_statement(&self, sql: &'static str) -> PreparedStatement {
            PreparedStatement { agent: self, sql }
        }

        async fn insert_test_data<P, I, V>(&self, rows: I) -> eyre::Result<()>
        where
            I: IntoIterator<Item = P>,
            P: IntoIterator<Item = V>,
            V: Into<SqliteValue>,
        {
            const INSERT_TEST: &str = "insert into tests (id, text) values (?,?)";
            self.prepare_statement(INSERT_TEST).execute(rows).await
        }

        #[allow(dead_code)]
        async fn update_test_data<P, I, V>(&self, rows: I) -> eyre::Result<()>
        where
            I: IntoIterator<Item = P>,
            P: IntoIterator<Item = V>,
            V: Into<SqliteValue>,
        {
            const UPDATE_TEST: &str = "update tests set text = ?2 where id = ?1";
            self.prepare_statement(UPDATE_TEST).execute(rows).await
        }

        async fn delete_test_data<P, I, V>(&self, rows: I) -> eyre::Result<()>
        where
            I: IntoIterator<Item = P>,
            P: IntoIterator<Item = V>,
            V: Into<SqliteValue>,
        {
            const DELETE_TEST: &str = "delete from tests where id = ?1";
            self.prepare_statement(DELETE_TEST).execute(rows).await
        }
    }

    struct PubSubTest {
        agents: Vec<PubSubAgent>,
        #[allow(dead_code)]
        tripwire_worker: TripwireWorker<tokio_stream::wrappers::ReceiverStream<()>>,
    }

    impl PubSubTest {
        // Creates an cluster with N agents
        async fn with_n_agents(n: usize) -> eyre::Result<Self> {
            let (tripwire, tripwire_worker, _) = Tripwire::new_simple();
            let mut agents = Vec::with_capacity(n);
            for _ in 0..n {
                let prev_gossip = agents
                    .last()
                    .map(|a: &PubSubAgent| a.ta.agent.gossip_addr().to_string());
                let ta = launch_test_agent(
                    |conf| {
                        if let Some(gossip) = prev_gossip {
                            conf.bootstrap(vec![gossip]).build()
                        } else {
                            conf.build()
                        }
                    },
                    tripwire.clone(),
                )
                .await?;
                let ta_client = ta.client();
                agents.push(PubSubAgent {
                    ta,
                    ta_client,
                    subs_bcast_cache: Default::default(),
                    updates_bcast_cache: Default::default(),
                    tripwire: tripwire.clone(),
                });
            }
            Ok(Self {
                agents,
                tripwire_worker,
            })
        }

        const CHECK_RETRIES: usize = 10;
        const SLEEP_START_DELAY: Duration = Duration::from_millis(10);
        const SLEEP_MAX_DELAY: Duration = Duration::from_millis(1000);
        // Checks if the row count in the tests table is the same on all nodes
        async fn wait_for_nodes_to_sync(&self) -> eyre::Result<()> {
            if self.agents.len() < 2 {
                return Ok(());
            }

            let now = Instant::now();
            let mut delay = Self::SLEEP_START_DELAY;
            for _ in 0..Self::CHECK_RETRIES {
                let are_nodes_consistent =
                    futures::future::join_all(self.agents.iter().map(|a| async move {
                        let conn = a.ta_client.pool().get().await?;
                        let count: u64 =
                            conn.query_row("SELECT count(*) FROM tests", (), |row| row.get(0))?;
                        Ok(count) as eyre::Result<u64>
                    }))
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap()
                    .windows(2)
                    .all(|window| window[0] == window[1]);

                if are_nodes_consistent {
                    return Ok(());
                }

                if delay < Self::SLEEP_MAX_DELAY {
                    delay *= 2;
                    if delay > Self::SLEEP_MAX_DELAY {
                        delay = Self::SLEEP_MAX_DELAY;
                    }
                }
                tokio::time::sleep(delay).await;
            }
            eyre::bail!(
                "nodes did not reach consistent state within {} ms",
                now.elapsed().as_millis()
            );
        }
    }

    // Check subscription deduplication
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_same_sub_id_for_same_query() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let test = PubSubTest::with_n_agents(1).await?;
        let make_sub_fn = async |q: &str, sub_params: SubParams| {
            test.agents[0]
                .subscribe_to_test_table(sub_params, Either::Left(q.into()))
                .await
        };
        let mut s = Vec::new();
        for q in [
            TEST_QUERY1,
            TEST_QUERY1_WITH_SPACES,
            TEST_QUERY1_EXPLICIT,
            TEST_QUERY2,
            TEST_QUERY3,
        ]
        .into_iter()
        {
            // skip_rows should not affect the sub_id
            // resubscribing should not affect the sub_id
            let s0 = make_sub_fn(
                q,
                SubParams {
                    from: None,
                    skip_rows: false,
                },
            )
            .await?;
            let s1 = make_sub_fn(
                q,
                SubParams {
                    from: None,
                    skip_rows: false,
                },
            )
            .await?;
            let s2 = make_sub_fn(
                q,
                SubParams {
                    from: None,
                    skip_rows: true,
                },
            )
            .await?;
            let s3 = make_sub_fn(
                q,
                SubParams {
                    from: Some(0.into()),
                    skip_rows: false,
                },
            )
            .await?;
            let s4 = make_sub_fn(
                q,
                SubParams {
                    from: Some(0.into()),
                    skip_rows: true,
                },
            )
            .await?;
            assert_eq!(s0.sub_id, s1.sub_id);
            assert_eq!(s0.sub_id, s2.sub_id);
            assert_eq!(s0.sub_id, s3.sub_id);
            assert_eq!(s0.sub_id, s4.sub_id);
            s.push(s0);
        }

        // TODO: Semantically equivalent queries should get the same sub_id
        //       for now only byte-identical queries get the same sub_id
        for i in 1..s.len() {
            for j in i + 1..s.len() {
                assert_ne!(s[i].sub_id, s[j].sub_id);
            }
        }
        Ok(())
    }

    // Checks that rowIDs are different across different subscriptions
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_row_ids_are_unique_per_query() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let test = PubSubTest::with_n_agents(1).await?;
        let make_sub_fn = async |q: &str| {
            test.agents[0]
                .subscribe_to_test_table(
                    SubParams {
                        from: None,
                        skip_rows: false,
                    },
                    Either::Left(q.into()),
                )
                .await
        };

        // First insert some data to test rowIds for the initial query results
        let data = [
            vec!["service-id-0".into(), "service-name-0".into()],
            vec!["service-id-1".into(), "service-name-1".into()],
            vec!["service-id-2".into(), "service-name-2".into()],
            vec!["service-id-3".into(), "service-name-3".into()],
            vec!["service-id-4".into(), "service-name-4".into()],
            vec!["service-id-5".into(), "service-name-5".into()],
        ];
        test.agents[0].insert_test_data(&data).await?;

        let mut s_all = make_sub_fn(TEST_QUERY1).await?;
        let mut s_even = make_sub_fn(TEST_QUERY2).await?;
        let mut s_odd = make_sub_fn(TEST_QUERY3).await?;

        s_all
            .assert_initial_query_results(
                vec!["id".into(), "text".into()],
                vec![
                    (RowId(1), &data[0]),
                    (RowId(2), &data[1]),
                    (RowId(3), &data[2]),
                    (RowId(4), &data[3]),
                    (RowId(5), &data[4]),
                    (RowId(6), &data[5]),
                ],
                0.into(),
            )
            .await;

        s_even
            .assert_initial_query_results(
                vec!["id".into(), "text".into()],
                vec![
                    (RowId(1), &data[0]),
                    (RowId(2), &data[2]),
                    (RowId(3), &data[4]),
                ],
                0.into(),
            )
            .await;

        s_odd
            .assert_initial_query_results(
                vec!["id".into(), "text".into()],
                vec![
                    (RowId(1), &data[1]),
                    (RowId(2), &data[3]),
                    (RowId(3), &data[5]),
                ],
                0.into(),
            )
            .await;

        // Now check if updates work
        let data = [
            vec!["service-id-6".into(), "service-name-6".into()],
            vec!["service-id-7".into(), "service-name-7".into()],
            vec!["service-id-8".into(), "service-name-8".into()],
            vec!["service-id-9".into(), "service-name-9".into()],
            vec!["service-id-10".into(), "service-name-10".into()],
            vec!["service-id-11".into(), "service-name-11".into()],
        ];

        for i in 0..data.len() {
            test.agents[0].insert_test_data(&data[i..=i]).await?;
            let row = &data[i];
            let i = i as u64;

            s_all
                .assert_updates(vec![(
                    ChangeType::Insert,
                    RowId(7 + i),
                    ChangeId(1 + i),
                    row,
                )])
                .await;

            if i % 2 == 0 {
                s_even
                    .assert_updates(vec![(
                        ChangeType::Insert,
                        RowId(4 + i / 2),
                        ChangeId(1 + i / 2),
                        row,
                    )])
                    .await;
            } else {
                s_odd
                    .assert_updates(vec![(
                        ChangeType::Insert,
                        RowId(4 + i / 2),
                        ChangeId(1 + i / 2),
                        row,
                    )])
                    .await;
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_resubscription() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let test = PubSubTest::with_n_agents(1).await?;
        let make_sub_fn =
            async |sub_params: SubParams, query: &Either<corro_types::api::Statement, Uuid>| {
                test.agents[0]
                    .subscribe_to_test_table(sub_params, query.clone())
                    .await
            };
        let q = Either::Left(TEST_QUERY1.into());

        // From is only for existing subscriptions - trying to resubscribe to non-existing subscription should fail
        for (from, skip_rows) in [(0, false), (10, false), (0, true), (10, true)] {
            let _ = make_sub_fn(
                SubParams {
                    from: Some(from.into()),
                    skip_rows,
                },
                &q,
            )
            .await
            .expect_err("From is for resubscription - the subscription needs to first exist");
        }

        // Can't resubscribe to non-existing subscription
        let _ = make_sub_fn(
            SubParams {
                from: None,
                skip_rows: false,
            },
            &Either::Right(Uuid::new_v4()),
        )
        .await
        .expect_err("Can't subscribe to non-existing subscription");

        // Finally make a subscription
        let mut s_base = make_sub_fn(
            SubParams {
                from: None,
                skip_rows: false,
            },
            &q,
        )
        .await
        .wrap_err("Failed to create initial subscription")?;
        s_base
            .assert_initial_query_results(vec!["id".into(), "text".into()], vec![], 0.into())
            .await;
        let sub_id = Either::Right(s_base.sub_id);
        let mut active_subs = vec![&mut s_base];

        // Resubscribing from an future changeId should work
        let mut s_future = make_sub_fn(
            SubParams {
                from: Some(10.into()),
                skip_rows: false,
            },
            &q,
        )
        .await?;
        let mut s_future_by_id = make_sub_fn(
            SubParams {
                from: Some(100.into()),
                skip_rows: false,
            },
            &sub_id,
        )
        .await?;
        active_subs.push(&mut s_future);
        active_subs.push(&mut s_future_by_id);

        // Now check if updates work
        let data = [
            vec!["service-id-0".into(), "service-name-0".into()],
            vec!["service-id-1".into(), "service-name-1".into()],
            vec!["service-id-2".into(), "service-name-2".into()],
            vec!["service-id-3".into(), "service-name-3".into()],
            vec!["service-id-4".into(), "service-name-4".into()],
            vec!["service-id-5".into(), "service-name-5".into()],
        ];
        for i in 0..2 {
            test.agents[0].insert_test_data(&data[i..=i]).await?;
            let row = &data[i];
            let i = i as u64;

            for sub in &mut active_subs {
                sub.assert_updates(vec![(
                    ChangeType::Insert,
                    RowId(1 + i),
                    ChangeId(1 + i),
                    row,
                )])
                .await;
            }
        }

        // Resubscribing from changeId 0 should work as if from was not set at all
        let mut s_zero = make_sub_fn(
            SubParams {
                from: Some(0.into()),
                skip_rows: false,
            },
            &q,
        )
        .await?;
        let mut s_zero_by_id = make_sub_fn(
            SubParams {
                from: Some(0.into()),
                skip_rows: false,
            },
            &sub_id,
        )
        .await?;
        let mut s_zero_skip_rows = make_sub_fn(
            SubParams {
                from: Some(0.into()),
                skip_rows: true,
            },
            &q,
        )
        .await?;
        let mut s_zero_skip_rows_by_id = make_sub_fn(
            SubParams {
                from: Some(0.into()),
                skip_rows: true,
            },
            &sub_id,
        )
        .await?;

        // If skip_rows is true, the initial query should be skipped
        // from = 0 is essentially equivalent to not setting the changeId at all - it will only return new changes
        s_zero
            .assert_initial_query_results(
                vec!["id".into(), "text".into()],
                data[0..2]
                    .iter()
                    .enumerate()
                    .map(|(i, row)| (RowId(1 + i as u64), row))
                    .collect(),
                2.into(),
            )
            .await;
        s_zero_by_id
            .assert_initial_query_results(
                vec!["id".into(), "text".into()],
                data[0..2]
                    .iter()
                    .enumerate()
                    .map(|(i, row)| (RowId(1 + i as u64), row))
                    .collect(),
                2.into(),
            )
            .await;

        active_subs.push(&mut s_zero);
        active_subs.push(&mut s_zero_by_id);
        active_subs.push(&mut s_zero_skip_rows);
        active_subs.push(&mut s_zero_skip_rows_by_id);

        // Now check if updates work on all subscriptions
        for i in 2..4 {
            test.agents[0].insert_test_data(&data[i..=i]).await?;
            let row = &data[i];
            let i = i as u64;
            for sub in &mut active_subs {
                sub.assert_updates(vec![(
                    ChangeType::Insert,
                    RowId(1 + i),
                    ChangeId(1 + i),
                    row,
                )])
                .await;
            }
        }

        // Resubscribing from changeId 1 should send all events from changeId 1
        let mut s_one = make_sub_fn(
            SubParams {
                from: Some(1.into()),
                skip_rows: false,
            },
            &q,
        )
        .await?;
        let mut s_one_by_id = make_sub_fn(
            SubParams {
                from: Some(1.into()),
                skip_rows: false,
            },
            &sub_id,
        )
        .await?;
        let mut s_one_skip_rows = make_sub_fn(
            SubParams {
                from: Some(1.into()),
                skip_rows: true,
            },
            &q,
        )
        .await?;
        let mut s_one_skip_rows_by_id = make_sub_fn(
            SubParams {
                from: Some(1.into()),
                skip_rows: true,
            },
            &sub_id,
        )
        .await?;

        // We should get updates changeId 2 - 4
        for i in 1..4 {
            for sub in [
                &mut s_one,
                &mut s_one_by_id,
                &mut s_one_skip_rows,
                &mut s_one_skip_rows_by_id,
            ] {
                sub.assert_updates(vec![(
                    ChangeType::Insert,
                    RowId(1 + i),
                    ChangeId(1 + i),
                    &data[i as usize],
                )])
                .await;
            }
        }

        active_subs.push(&mut s_one);
        active_subs.push(&mut s_one_by_id);
        active_subs.push(&mut s_one_skip_rows);
        active_subs.push(&mut s_one_skip_rows_by_id);

        // Now check if updates work on all subscriptions
        for i in 4..6 {
            test.agents[0].insert_test_data(&data[i..=i]).await?;
            let row = &data[i];
            let i = i as u64;
            for sub in &mut active_subs {
                sub.assert_updates(vec![(
                    ChangeType::Insert,
                    RowId(1 + i),
                    ChangeId(1 + i),
                    row,
                )])
                .await;
            }
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_lagging_subscribers() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let test = PubSubTest::with_n_agents(1).await?;
        let agent = &test.agents[0];

        // Start off simple with 2 rows and a subscription
        let mut data: Vec<Vec<SqliteValue>> = vec![];
        for i in 0..2 {
            data.push(vec![
                format!("service-id-{}", i).into(),
                format!("service-name-{}", i).into(),
            ]);
            agent.insert_test_data(&data[i..=i]).await?;
        }

        let mut s = agent
            .subscribe_to_test_table(
                SubParams {
                    from: None,
                    skip_rows: false,
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;
        s.assert_initial_query_results(
            vec!["id".into(), "text".into()],
            data.iter()
                .enumerate()
                .map(|(i, row)| (RowId(1 + i as u64), row))
                .collect(),
            0.into(),
        )
        .await;

        let mut bulk_insert_fn = async |hundreds: usize| -> eyre::Result<()> {
            let chunk_size = 100;
            let start_idx = data.len();
            for i in 0..hundreds {
                // Insert 100 rows at a time in the same transaction
                for j in 0..chunk_size {
                    let idx = start_idx + i * chunk_size + j;
                    data.push(vec![
                        format!("service-id-{}", idx).into(),
                        format!("service-name-{}", idx).into(),
                    ]);
                }
                agent
                    .insert_test_data(
                        &data[start_idx + i * chunk_size
                            ..=start_idx + i * chunk_size + chunk_size - 1],
                    )
                    .await?;
            }
            Ok(())
        };

        // Now insert 10k rows and check that we can receive all of them
        bulk_insert_fn(100).await?;
        let events = s.receive_all_events().await;
        assert_eq!(events.len(), 10000);
        for i in 0..events.len() {
            let QueryEvent::Change(ChangeType::Insert, row_id, _, change_id) = &events[i] else {
                panic!("Expected QueryEvent::Change, got {:?}", events[i]);
            };
            assert_eq!(*row_id, RowId(i as u64 + 3));
            assert_eq!(*change_id, ChangeId(i as u64 + 1));
        }

        // If we can't keep up with the events, we should get disconnected
        bulk_insert_fn(410).await?;
        let events = s.receive_all_events().await;
        assert!(s.iter.done);
        assert!(s.iter.body.data().await.is_none());
        assert!(events.len() < 410 * 100);

        // No more events should be received on that subscription
        // Corrosion dropped the connection
        bulk_insert_fn(1).await?;
        // Calculate expected data length: 2 initial + 100*100 + 410*100 + 1*100
        let data_len = 2 + 10000 + 41000 + 100;
        let events = s.receive_all_events().await;
        assert!(events.is_empty());

        // Ensure the writes are flushed on the subscription
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Now check if we can subscribe and receive the full initial query result
        let mut s2 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: None,
                    skip_rows: false,
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;
        let events = s2.receive_all_events().await;
        assert_eq!(events.len(), data_len + 2); // data + columns + end of query

        // The same should work for from = 0
        let mut s3 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(0.into()),
                    skip_rows: false,
                },
                Either::Right(s2.sub_id),
            )
            .await?;
        let events = s3.receive_all_events().await;
        assert_eq!(events.len(), data_len + 2); // data + columns + end of query

        // The same should work for from = 100 - nothing was purged yet
        let mut s4 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(100.into()),
                    skip_rows: false,
                },
                Either::Right(s2.sub_id),
            )
            .await?;
        let events = s4.receive_all_events().await;
        assert_eq!(events.len(), data_len - 2 - 100); // data - from

        // Purge the subscriptions, s2, s3 and s4 should not get disconnected
        // We can't manipulate time in tokio runtime, so manually trigger the pruning
        if let Some(matcher) = agent.ta.agent.subs_manager().get(&s2.sub_id) {
            matcher
                .purge_old_changes()
                .await
                .expect("failed to purge old changes");
        }

        // Now subscribing to an small change id should fail
        let mut s5 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(100.into()),
                    skip_rows: false,
                },
                Either::Right(s2.sub_id),
            )
            .await?;
        let events = s5.receive_all_events().await;
        assert_eq!(events.len(), 1); // data + columns + end of query
        assert!(matches!(
            events[0],
            corro_types::api::TypedQueryEvent::Error(_)
        ));

        // Resubscribing from a large change id should work
        let mut s6 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(50600.into()),
                    skip_rows: false,
                },
                Either::Right(s2.sub_id),
            )
            .await?;
        let events = s6.receive_all_events().await;
        assert_eq!(events.len(), data_len - 2 - 50600); // data - from

        // Resubscribing from 0 should work
        let mut s7 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(0.into()),
                    skip_rows: false,
                },
                Either::Right(s2.sub_id),
            )
            .await?;
        let events = s7.receive_all_events().await;
        assert_eq!(events.len(), data_len + 2); // data + columns + end of query

        // Now check that events are sent to active subscriptions
        bulk_insert_fn(1).await?;
        // Ensure the writes are flushed on the subscription
        tokio::time::sleep(Duration::from_secs(1)).await;

        for sub in [&mut s2, &mut s3, &mut s4, &mut s6, &mut s7] {
            let events = sub.receive_all_events().await;
            assert_eq!(events.len(), 100);
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_on_2_nodes() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let test = PubSubTest::with_n_agents(2).await?;
        let data = [
            vec!["service-id-0".into(), "service-name-0".into()],
            vec!["service-id-1".into(), "service-name-1".into()],
            vec!["service-id-2".into(), "service-name-2".into()],
            vec!["service-id-3".into(), "service-name-3".into()],
        ];

        let insert_agent = &test.agents[0];
        let subscribe_agent = &test.agents[1];

        insert_agent.insert_test_data(&data[0..2]).await?;
        test.wait_for_nodes_to_sync().await?;

        let mut s1 = subscribe_agent
            .subscribe_to_test_table(
                SubParams {
                    from: None,
                    skip_rows: false,
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;

        s1.assert_initial_query_results(
            vec!["id".into(), "text".into()],
            data[0..2]
                .iter()
                .enumerate()
                .map(|(i, row)| (RowId(1 + i as u64), row))
                .collect(),
            0.into(),
        )
        .await;

        for i in 2..4 {
            insert_agent.insert_test_data(&data[i..=i]).await?;
            test.wait_for_nodes_to_sync().await?;

            s1.assert_updates(vec![(
                ChangeType::Insert,
                RowId(i as u64 + 1),
                ChangeId(i as u64 + 1 - 2),
                &data[i],
            )])
            .await;
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_api_v1_subs() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let test = PubSubTest::with_n_agents(1).await?;
        let agent = &test.agents[0];
        let data = [
            vec!["service-id-0".into(), "service-name-0".into()],
            vec!["service-id-1".into(), "service-name-1".into()],
            vec!["service-id-2".into(), "service-name-2".into()],
            vec!["service-id-3".into(), "service-name-3".into()],
            vec!["service-id-4".into(), "service-name-4".into()],
            vec!["service-id-5".into(), "service-name-5".into()],
        ];

        agent.insert_test_data(&data[0..2]).await?;
        {
            let mut s1 = agent
                .subscribe_to_test_table(
                    SubParams {
                        from: None,
                        skip_rows: false,
                    },
                    Either::Left(TEST_QUERY1.into()),
                )
                .await?;

            // only want notifications
            // small sleep here to make sure `broadcast_changes` has already run
            // for earlier transactions
            tokio::time::sleep(Duration::from_secs(1)).await;

            let mut notify_res = api_v1_updates(
                Extension(agent.ta.agent.clone()),
                Extension(agent.updates_bcast_cache.clone()),
                Extension(agent.tripwire.clone()),
                axum::extract::Path("tests".to_string()),
            )
            .await
            .into_response();

            if !notify_res.status().is_success() {
                let b = notify_res.body_mut().data().await.unwrap().unwrap();
                println!("body: {}", String::from_utf8_lossy(&b));
            }

            assert_eq!(notify_res.status(), StatusCode::OK);

            agent.insert_test_data(&data[2..3]).await?;

            let mut notify_rows = RowsIter {
                body: notify_res.into_body(),
                codec: LinesCodec::new(),
                buf: BytesMut::new(),
                done: false,
            };

            s1.assert_initial_query_results(
                vec!["id".into(), "text".into()],
                vec![(RowId(1), &data[0]), (RowId(2), &data[1])],
                0.into(),
            )
            .await;

            s1.assert_updates(vec![(ChangeType::Insert, RowId(3), ChangeId(1), &data[2])])
                .await;

            agent.insert_test_data(&data[3..4]).await?;

            s1.assert_updates(vec![(ChangeType::Insert, RowId(4), ChangeId(2), &data[3])])
                .await;

            assert_eq!(
                notify_rows.recv::<NotifyEvent>().await.unwrap().unwrap(),
                NotifyEvent::Notify(ChangeType::Update, vec!["service-id-2".into()],)
            );

            assert_eq!(
                notify_rows.recv::<NotifyEvent>().await.unwrap().unwrap(),
                NotifyEvent::Notify(ChangeType::Update, vec!["service-id-3".into()],)
            );

            // s2 subscribes from change id 1
            let mut s2 = agent
                .subscribe_to_test_table(
                    SubParams {
                        from: Some(1.into()),
                        skip_rows: false,
                    },
                    Either::Left(TEST_QUERY1.into()),
                )
                .await?;

            s2.assert_updates(vec![(ChangeType::Insert, RowId(4), ChangeId(2), &data[3])])
                .await;

            // new subscriber for updates
            let mut notify_res2 = api_v1_updates(
                Extension(agent.ta.agent.clone()),
                Extension(agent.updates_bcast_cache.clone()),
                Extension(agent.tripwire.clone()),
                axum::extract::Path("tests".to_string()),
            )
            .await
            .into_response();

            if !notify_res2.status().is_success() {
                let b = notify_res2.body_mut().data().await.unwrap().unwrap();
                println!("body: {}", String::from_utf8_lossy(&b));
            }

            assert_eq!(notify_res2.status(), StatusCode::OK);

            let mut notify_rows2 = RowsIter {
                body: notify_res2.into_body(),
                codec: LinesCodec::new(),
                buf: BytesMut::new(),
                done: false,
            };

            agent.insert_test_data(&data[4..5]).await?;

            // Both s1 and s2 should receive the insert
            let query_evt = (ChangeType::Insert, RowId(5), ChangeId(3), &data[4]);
            s1.assert_updates(vec![query_evt.clone()]).await;
            s2.assert_updates(vec![query_evt]).await;

            // And both notifications should be received
            let notify_evt = NotifyEvent::Notify(ChangeType::Update, vec!["service-id-4".into()]);
            assert_eq!(
                notify_rows.recv::<NotifyEvent>().await.unwrap().unwrap(),
                notify_evt
            );

            assert_eq!(
                notify_rows2.recv::<NotifyEvent>().await.unwrap().unwrap(),
                notify_evt
            );

            // s3 arrives late and subscribes from scratch
            let mut s3 = agent
                .subscribe_to_test_table(
                    SubParams {
                        from: None,
                        skip_rows: false,
                    },
                    Either::Left(TEST_QUERY1.into()),
                )
                .await?;

            s3.assert_initial_query_results(
                vec!["id".into(), "text".into()],
                vec![
                    (RowId(1), &data[0]),
                    (RowId(2), &data[1]),
                    (RowId(3), &data[2]),
                    (RowId(4), &data[3]),
                    (RowId(5), &data[4]),
                ],
                ChangeId(3),
            )
            .await;

            agent.insert_test_data(&data[5..6]).await?;
            agent.delete_test_data([vec![&data[5][0]]]).await?;

            // when we make changes to the same primary key in quick succession,
            // the newer event might get sent first (but in that case, the older one should be dropped)
            match notify_rows.recv::<NotifyEvent>().await.unwrap().unwrap() {
                NotifyEvent::Notify(ChangeType::Update, pk) => {
                    assert_eq!(pk, vec!["service-id-5".into()]);
                    assert_eq!(
                        notify_rows.recv::<NotifyEvent>().await.unwrap().unwrap(),
                        NotifyEvent::Notify(ChangeType::Delete, vec!["service-id-5".into()],)
                    );
                }
                NotifyEvent::Notify(ChangeType::Delete, pk) => {
                    assert_eq!(pk, vec!["service-id-5".into()]);
                    // check that we dont get an update after
                    assert!(tokio::time::timeout(
                        Duration::from_secs(2),
                        notify_rows.recv::<NotifyEvent>()
                    )
                    .await
                    .is_err());
                }
                _ => panic!("expected notify event"),
            }
        }

        // previous subs have been dropped.
        let mut s1 = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(1.into()),
                    ..Default::default()
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;

        s1.assert_updates(vec![
            (ChangeType::Insert, RowId(4), ChangeId(2), &data[3]),
            (ChangeType::Insert, RowId(5), ChangeId(3), &data[4]),
        ])
        .await;

        // change id 0 should start subs afresh
        let mut s_zero = agent
            .subscribe_to_test_table(
                SubParams {
                    from: Some(0.into()),
                    ..Default::default()
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;

        s_zero
            .assert_initial_query_results(
                vec!["id".into(), "text".into()],
                vec![
                    (RowId(1), &data[0]),
                    (RowId(2), &data[1]),
                    (RowId(3), &data[2]),
                    (RowId(4), &data[3]),
                    (RowId(5), &data[4]),
                ],
                ChangeId(3),
            )
            .await;

        // skip rows!
        let mut s_skip = agent
            .subscribe_to_test_table(
                SubParams {
                    skip_rows: true,
                    ..Default::default()
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;

        agent.insert_test_data(&data[5..6]).await?;

        let upd = (ChangeType::Insert, RowId(6), ChangeId(4), &data[5]);
        s_skip.assert_updates(vec![upd.clone()]).await;
        s_zero.assert_updates(vec![upd.clone()]).await;

        // skip rows AND from
        let mut s_skip_from = agent
            .subscribe_to_test_table(
                SubParams {
                    skip_rows: true,
                    from: Some(ChangeId(3)),
                },
                Either::Left(TEST_QUERY1.into()),
            )
            .await?;

        s_skip_from.assert_updates(vec![upd.clone()]).await;
        s_skip_from.assert_no_more_events().await;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn match_buffered_changes() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
        let tx_timeout = Duration::from_secs(60);

        let schema = "CREATE TABLE buftests (
            pk int NOT NULL PRIMARY KEY,
            col1 text,
            col2 text
         );";

        let (status_code, _body) = api_v1_db_schema(
            Extension(ta1.agent.clone()),
            axum::Json(vec![schema.into()]),
        )
        .await;
        assert_eq!(status_code, StatusCode::OK);

        let actor_id = ActorId(uuid::Uuid::new_v4());

        let change1 = Change {
            table: TableName("buftests".into()),
            pk: pack_columns(&vec![1i64.into()])?,
            cid: ColumnName("col1".into()),
            val: "one".into(),
            col_version: 1,
            db_version: CrsqlDbVersion(1),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let change2 = Change {
            table: TableName("buftests".into()),
            pk: pack_columns(&vec![1i64.into()])?,
            cid: ColumnName("col2".into()),
            val: "one line".into(),
            col_version: 1,
            db_version: CrsqlDbVersion(1),
            seq: CrsqlSeq(1),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let changes = ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: CrsqlDbVersion(1),
                changes: vec![change1, change2],
                seqs: dbsr!(0, 1),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        };

        process_multiple_changes(
            ta1.agent.clone(),
            ta1.bookie.clone(),
            vec![(changes, ChangeSource::Sync, Instant::now())],
            tx_timeout,
        )
        .await?;

        let bcast_cache: SharedMatcherBroadcastCache = Default::default();
        let update_bcast_cache: SharedUpdateBroadcastCache = Default::default();
        let mut res = api_v1_subs(
            Extension(ta1.agent.clone()),
            Extension(bcast_cache.clone()),
            Extension(tripwire.clone()),
            axum::extract::Query(SubParams::default()),
            axum::Json(Statement::Simple("select * from buftests".into())),
        )
        .await
        .into_response();

        if !res.status().is_success() {
            let b = res.body_mut().data().await.unwrap().unwrap();
            println!("body: {}", String::from_utf8_lossy(&b));
        }

        assert_eq!(res.status(), StatusCode::OK);

        // only notifications
        let mut notify_res = api_v1_updates(
            Extension(ta1.agent.clone()),
            Extension(update_bcast_cache.clone()),
            Extension(tripwire.clone()),
            axum::extract::Path("buftests".to_string()),
        )
        .await
        .into_response();

        if !notify_res.status().is_success() {
            let b = notify_res.body_mut().data().await.unwrap().unwrap();
            println!("body: {}", String::from_utf8_lossy(&b));
        }

        assert_eq!(notify_res.status(), StatusCode::OK);

        let mut notify_rows = RowsIter {
            body: notify_res.into_body(),
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            done: false,
        };

        let mut rows = RowsIter {
            body: res.into_body(),
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            done: false,
        };

        assert_eq!(
            rows.recv::<QueryEvent>().await.unwrap().unwrap(),
            QueryEvent::Columns(vec!["pk".into(), "col1".into(), "col2".into()])
        );

        assert_eq!(
            rows.recv::<QueryEvent>().await.unwrap().unwrap(),
            QueryEvent::Row(RowId(1), vec![Integer(1), "one".into(), "one line".into()])
        );

        assert!(matches!(
            rows.recv::<QueryEvent>().await.unwrap().unwrap(),
            QueryEvent::EndOfQuery { .. }
        ));

        // send partial change so it is buffered
        let change3 = Change {
            table: TableName("buftests".into()),
            pk: pack_columns(&vec![2i64.into()])?,
            cid: ColumnName("col1".into()),
            val: "two".into(),
            col_version: 1,
            db_version: CrsqlDbVersion(2),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let changes = ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: CrsqlDbVersion(2),
                changes: vec![change3],
                seqs: dbsr!(0, 0),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        };

        process_multiple_changes(
            ta1.agent.clone(),
            ta1.bookie.clone(),
            vec![(changes, ChangeSource::Sync, Instant::now())],
            tx_timeout,
        )
        .await?;

        // confirm that change is buffered in db
        {
            let conn = ta1.agent.pool().read().await?;
            let end = conn.query_row(
                "SELECT end_seq FROM __corro_seq_bookkeeping WHERE site_id = ? AND db_version = ?",
                (actor_id, 2),
                |row| row.get::<_, CrsqlSeq>(0),
            )?;
            assert_eq!(end, CrsqlSeq(0));
        }

        let change4 = Change {
            table: TableName("buftests".into()),
            pk: pack_columns(&vec![2i64.into()])?,
            cid: ColumnName("col2".into()),
            val: "two line".into(),
            col_version: 1,
            db_version: CrsqlDbVersion(2),
            seq: CrsqlSeq(1),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let changes = ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: CrsqlDbVersion(2),
                changes: vec![change4],
                seqs: dbsr!(1, 1),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        };

        process_multiple_changes(
            ta1.agent.clone(),
            ta1.bookie.clone(),
            vec![(changes, ChangeSource::Sync, Instant::now())],
            tx_timeout,
        )
        .await?;

        let res = timeout(Duration::from_secs(5), rows.recv::<QueryEvent>()).await?;

        assert_eq!(
            res.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                RowId(2),
                vec![Integer(2), "two".into(), "two line".into()],
                ChangeId(1)
            )
        );

        let notify_res = timeout(Duration::from_secs(5), notify_rows.recv::<NotifyEvent>()).await?;
        assert_eq!(
            notify_res.unwrap().unwrap(),
            NotifyEvent::Notify(ChangeType::Update, vec![Integer(2)],)
        );

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        ta1.agent.subs_manager().drop_handles().await;
        wait_for_all_pending_handles().await;

        Ok(())
    }

    #[derive(Debug)]
    struct RowsIter {
        body: axum::body::BoxBody,
        codec: LinesCodec,
        buf: BytesMut,
        done: bool,
    }

    const RECV_TIMEOUT: Duration = Duration::from_secs(2);
    impl RowsIter {
        async fn recv<T: DeserializeOwned>(&mut self) -> Option<eyre::Result<T>> {
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

                let bytes_res = timeout(RECV_TIMEOUT, self.body.data()).await;
                match bytes_res {
                    Ok(Some(Ok(b))) => {
                        // debug!("read {} bytes", b.len());
                        self.buf.extend_from_slice(&b)
                    }
                    Ok(Some(Err(e))) => {
                        self.done = true;
                        return Some(Err(e.into()));
                    }
                    Ok(None) => {
                        self.done = true;
                        return None;
                    }
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e.into()));
                    }
                }
            }
        }
    }
}
