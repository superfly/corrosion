use std::{collections::HashMap, io::Write, sync::Arc, time::Duration};

use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::{format_compact, ToCompactString};
use corro_types::updates::{Handle, Manager, UpdateHandle, UpdatesManager};
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
) -> hyper::Response<hyper::Body> {
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

    let (tx, body) = hyper::Body::channel();

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

const MAX_UNSUB_TIME: Duration = Duration::from_secs(120);
// this should be a fraction of the MAX_UNSUB_TIME
const RECEIVERS_CHECK_INTERVAL: Duration = Duration::from_secs(30);

pub async fn process_sub_channel(
    subs: impl Manager,
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
            Some(from) => catch_up_sub_from(&matcher, from, &evt_tx).await,
            None => {
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

pub async fn upsert_update(
    handle: UpdateHandle,
    maybe_created: Option<MatcherCreated>,
    updates: &UpdatesManager,
    bcast_write: &mut MatcherBroadcastCache,
    tx: mpsc::Sender<(Bytes, QueryEventMeta)>,
) -> Result<Uuid, MatcherUpsertError> {
    let sub_rx = if let Some(created) = maybe_created {
        let (sub_tx, sub_rx) = broadcast::channel(10240);
        bcast_write.insert(handle.id(), sub_tx.clone());
        tokio::spawn(process_sub_channel(
            updates.clone(),
            handle.id(),
            sub_tx,
            created.evt_rx,
        ));

        sub_rx
    } else {
        let id = handle.id();
        let sub_tx = bcast_write
            .get(&id)
            .cloned()
            .ok_or(MatcherUpsertError::MissingBroadcaster)?;
        debug!("found update handle");

        sub_tx.subscribe()
    };

    tokio::spawn(forward_sub_to_sender(handle.clone(), sub_rx, tx, false));

    Ok(handle.id())
}

pub async fn api_v1_updates(
    Extension(agent): Extension<Agent>,
    Extension(bcast_cache): Extension<SharedMatcherBroadcastCache>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Path(table): axum::extract::Path<String>,
) -> impl IntoResponse {
    info!("Received update request for table: {table}");

    let mut bcast_write = bcast_cache.write().await;
    let updates = agent.updates_manager();

    let upsert_res = updates.get_or_insert(
        &table,
        &agent.schema().read(),
        agent.pool(),
        tripwire.clone(),
    );

    let (handle, maybe_created) = match upsert_res {
        Ok(res) => res,
        Err(e) => return hyper::Response::<hyper::Body>::from(MatcherUpsertError::from(e)),
    };

    let (tx, body) = hyper::Body::channel();
    let (forward_tx, forward_rx) = mpsc::channel(10240);

    tokio::spawn(forward_bytes_to_body_sender(
        handle.id(),
        forward_rx,
        tx,
        tripwire,
    ));

    let update_id =
        match upsert_update(handle, maybe_created, updates, &mut bcast_write, forward_tx).await {
            Ok(id) => id,
            Err(e) => return hyper::Response::<hyper::Body>::from(e),
        };

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", update_id.to_string())
        .body(body)
        .expect("could not generate ok http response for update request")
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
        Err(e) => return hyper::Response::<hyper::Body>::from(e),
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
        Err(e) => return hyper::Response::<hyper::Body>::from(MatcherUpsertError::from(e)),
    };

    let (tx, body) = hyper::Body::channel();
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
        Err(e) => return hyper::Response::<hyper::Body>::from(e),
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
    handle: impl Handle,
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
    use corro_types::api::{Change, ColumnName, TableName};
    use corro_types::base::{CrsqlDbVersion, CrsqlSeq, Version};
    use corro_types::broadcast::{ChangeSource, ChangeV1, Changeset};
    use corro_types::pubsub::pack_columns;
    use corro_types::{
        api::{ChangeId, RowId},
        config::Config,
        pubsub::ChangeType,
    };
    use http_body::Body;
    use spawn::wait_for_all_pending_handles;
    use std::ops::RangeInclusive;
    use std::time::Instant;
    use tokio::time::timeout;
    use tokio_util::codec::{Decoder, LinesCodec};
    use tripwire::Tripwire;

    use super::*;
    use crate::agent::process_multiple_changes;
    use crate::{
        agent::setup,
        api::public::{api_v1_db_schema, api_v1_transactions},
    };
    use corro_tests::launch_test_agent;
    use corro_types::api::SqliteValue::Integer;

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

        let bcast_cache: SharedMatcherBroadcastCache = Default::default();

        {
            let mut res = api_v1_subs(
                Extension(agent.clone()),
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

            // only want notifications
            let mut notify_res = api_v1_updates(
                Extension(agent.clone()),
                Extension(bcast_cache.clone()),
                Extension(tripwire.clone()),
                axum::extract::Path("tests".to_string()),
            )
            .await
            .into_response();

            if !notify_res.status().is_success() {
                let b = notify_res.body_mut().data().await.unwrap().unwrap();
                println!("body: {}", String::from_utf8_lossy(&b));
            }

            assert_eq!(notify_res.status(), StatusCode::OK);

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

            let mut notify_rows = RowsIter {
                body: notify_res.into_body(),
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

            assert_eq!(
                notify_rows.recv().await.unwrap().unwrap(),
                QueryEvent::Notify(TableName("tests".into()), vec!["service-id-3".into()],)
            );

            assert_eq!(
                notify_rows.recv().await.unwrap().unwrap(),
                QueryEvent::Notify(TableName("tests".into()), vec!["service-id-4".into()],)
            );

            let mut res = api_v1_subs(
                Extension(agent.clone()),
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

            // new subscriber for updates
            let mut notify_res2 = api_v1_updates(
                Extension(agent.clone()),
                Extension(bcast_cache.clone()),
                Extension(tripwire.clone()),
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

            let notify_evt =
                QueryEvent::Notify(TableName("tests".into()), vec!["service-id-5".into()]);

            assert_eq!(notify_rows.recv().await.unwrap().unwrap(), notify_evt);

            assert_eq!(notify_rows2.recv().await.unwrap().unwrap(), notify_evt);

            // subscriber who arrives later!

            let mut res = api_v1_subs(
                Extension(agent.clone()),
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn match_buffered_changes() -> eyre::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let start = Instant::now();
        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();

        let ta1 = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;

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
                version: Version(1),
                changes: vec![change1, change2],
                seqs: RangeInclusive::new(CrsqlSeq(0), CrsqlSeq(1)),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        };

        process_multiple_changes(
            ta1.agent.clone(),
            ta1.bookie.clone(),
            vec![(changes, ChangeSource::Sync, Instant::now())],
        )
        .await?;

        let bcast_cache: SharedMatcherBroadcastCache = Default::default();
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
            Extension(bcast_cache.clone()),
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

        println!("done with update call in {:?}", start.elapsed());

        let mut rows = RowsIter {
            body: res.into_body(),
            codec: LinesCodec::new(),
            buf: BytesMut::new(),
            done: false,
        };

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Columns(vec!["pk".into(), "col1".into(), "col2".into()])
        );

        assert_eq!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::Row(RowId(1), vec![Integer(1), "one".into(), "one line".into()])
        );

        assert!(matches!(
            rows.recv().await.unwrap().unwrap(),
            QueryEvent::EndOfQuery { .. }
        ));

        // send partial change so it is buffered
        let change3 = Change {
            table: TableName("buftests".into()),
            pk: pack_columns(&vec![2i64.into()])?,
            cid: ColumnName("col1".into()),
            val: "two".into(),
            col_version: 1,
            db_version: CrsqlDbVersion(1),
            seq: CrsqlSeq(0),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let changes = ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: Version(2),
                changes: vec![change3],
                seqs: RangeInclusive::new(CrsqlSeq(0), CrsqlSeq(0)),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        };

        process_multiple_changes(
            ta1.agent.clone(),
            ta1.bookie.clone(),
            vec![(changes, ChangeSource::Sync, Instant::now())],
        )
        .await?;

        // confirm that change is buffered in db
        {
            let conn = ta1.agent.pool().read().await?;
            let end = conn.query_row(
                "SELECT end_seq FROM __corro_seq_bookkeeping WHERE site_id = ? AND version = ?",
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
            db_version: CrsqlDbVersion(1),
            seq: CrsqlSeq(1),
            site_id: actor_id.to_bytes(),
            cl: 1,
        };

        let changes = ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: Version(2),
                changes: vec![change4],
                seqs: RangeInclusive::new(CrsqlSeq(1), CrsqlSeq(1)),
                last_seq: CrsqlSeq(1),
                ts: Default::default(),
            },
        };

        process_multiple_changes(
            ta1.agent.clone(),
            ta1.bookie.clone(),
            vec![(changes, ChangeSource::Sync, Instant::now())],
        )
        .await?;

        let res = timeout(Duration::from_secs(5), rows.recv()).await?;

        assert_eq!(
            res.unwrap().unwrap(),
            QueryEvent::Change(
                ChangeType::Insert,
                RowId(2),
                vec![Integer(2), "two".into(), "two line".into()],
                ChangeId(1)
            )
        );

        let notify_res = timeout(Duration::from_secs(5), notify_rows.recv()).await?;
        assert_eq!(
            notify_res.unwrap().unwrap(),
            QueryEvent::Notify(TableName("buftests".into()), vec![Integer(2)],)
        );

        tripwire_tx.send(()).await.ok();
        tripwire_worker.await;
        wait_for_all_pending_handles().await;

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
