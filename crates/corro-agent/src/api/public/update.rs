use std::{collections::HashMap, io::Write, sync::Arc, time::Duration};

use antithesis_sdk::assert_sometimes;
use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    agent::Agent,
    api::NotifyEvent,
    persistent_gauge,
    updates::{Handle, UpdateCreated, UpdateHandle, UpdatesManager},
};
use tokio::sync::{
    broadcast::{self, error::RecvError},
    mpsc, RwLock as TokioRwLock,
};
use tracing::{debug, info, warn};
use tripwire::Tripwire;
use uuid::Uuid;

use crate::api::{
    public::pubsub::MatcherUpsertError,
    utils::{BodySender, CountedBody},
};

pub type UpdateBroadcastCache = HashMap<Uuid, broadcast::Sender<Bytes>>;
pub type SharedUpdateBroadcastCache = Arc<TokioRwLock<UpdateBroadcastCache>>;

// this should be a fraction of the MAX_UNSUB_TIME
const RECEIVERS_CHECK_INTERVAL: Duration = Duration::from_secs(30);

pub async fn api_v1_updates(
    Extension(agent): Extension<Agent>,
    Extension(bcast_cache): Extension<SharedUpdateBroadcastCache>,
    Extension(tripwire): Extension<Tripwire>,
    axum::extract::Path(table): axum::extract::Path<String>,
) -> impl IntoResponse {
    info!("Received update request for table: {table}");

    assert_sometimes!(true, "Corrosion receives requests for table updates");

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
        Err(e) => return hyper::Response::from(MatcherUpsertError::from(e)),
    };

    let (tx, body) = CountedBody::channel(
        persistent_gauge!("corro.api.active.streams", "source" => "updates", "protocol" => "http"),
    );

    let (update_id, sub_rx) =
        match upsert_update(handle.clone(), maybe_created, updates, &mut bcast_write).await {
            Ok(id) => id,
            Err(e) => return hyper::Response::from(e),
        };

    tokio::spawn(forward_update_bytes_to_body_sender(
        handle, sub_rx, tx, tripwire,
    ));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header("corro-query-id", update_id.to_string())
        .body(axum::body::Body::new(body))
        .expect("could not generate ok http response for update request")
}

pub async fn upsert_update(
    handle: UpdateHandle,
    maybe_created: Option<UpdateCreated>,
    updates: &UpdatesManager,
    bcast_write: &mut UpdateBroadcastCache,
) -> Result<(Uuid, broadcast::Receiver<Bytes>), MatcherUpsertError> {
    let sub_rx = if let Some(created) = maybe_created {
        let (sub_tx, sub_rx) = broadcast::channel(10240);
        bcast_write.insert(handle.id(), sub_tx.clone());
        tokio::spawn(process_update_channel(
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

    Ok((handle.id(), sub_rx))
}

pub async fn process_update_channel(
    updates: UpdatesManager,
    id: Uuid,
    tx: broadcast::Sender<Bytes>,
    mut evt_rx: mpsc::Receiver<NotifyEvent>,
) {
    let mut buf = BytesMut::new();

    // interval check for receivers
    // useful for queries that don't change often so we can cleanup...
    let mut subs_check = tokio::time::interval(RECEIVERS_CHECK_INTERVAL);

    loop {
        tokio::select! {
            biased;
            Some(query_evt) = evt_rx.recv() => {
                match make_query_event_bytes(&mut buf, &query_evt) {
                    Ok(b) => {
                        if tx.send(b).is_err() {
                            break;
                        }
                    },
                    Err(e) => {
                        match make_query_event_bytes(&mut buf, &NotifyEvent::Error(e.to_compact_string())) {
                            Ok(b) => {
                                let _ = tx.send(b);
                            }
                            Err(e) => {
                                warn!(update_id = %id, "failed to send error in update channel: {e}");
                            }
                        }
                        break;
                    }
                };
            },
            _ = subs_check.tick() => {
                if tx.receiver_count() == 0 {
                    break;
                };
            },
        };
    }

    warn!(sub_id = %id, "updates channel done");

    // remove and get handle from the agent's "matchers"
    let handle = match updates.remove(&id) {
        Some(h) => {
            info!(update_id = %id, "Removed update handle from process_update_channel");
            h
        }
        None => {
            warn!(update_id = %id, "update handle was already gone. odd!");
            return;
        }
    };

    // clean up the subscription
    handle.cleanup().await;
}

fn make_query_event_bytes(
    buf: &mut BytesMut,
    query_evt: &NotifyEvent,
) -> serde_json::Result<Bytes> {
    {
        let mut writer = buf.writer();
        serde_json::to_writer(&mut writer, query_evt)?;

        // NOTE: I think that's infaillible...
        writer
            .write_all(b"\n")
            .expect("could not write new line to BytesMut Writer");
    }

    Ok(buf.split().freeze())
}

async fn forward_update_bytes_to_body_sender(
    update: UpdateHandle,
    mut rx: broadcast::Receiver<Bytes>,
    mut tx: BodySender,
    mut tripwire: Tripwire,
) {
    let mut buf = BytesMut::new();

    let send_deadline = tokio::time::sleep(Duration::from_millis(10));
    tokio::pin!(send_deadline);

    loop {
        tokio::select! {
            biased;
            res = rx.recv() => {
                match res {
                    Ok(event_buf) => {
                        buf.extend_from_slice(&event_buf);
                        if buf.len() >= 64 * 1024 {
                            if let Err(e) = tx.send_data(buf.split().freeze()).await {
                                warn!(update_id = %update.id(), "could not forward update query event to receiver: {e}");
                                return;
                            }
                        };
                    },
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(update_id = %update.id(), "update skipped {} events, aborting", skipped);
                        return;
                    },
                    Err(RecvError::Closed) => {
                        info!(update_id = %update.id(), "events subscription ran out");
                        return;
                    },
                }
            },
            _ = &mut send_deadline => {
                if !buf.is_empty() {
                    if let Err(e) = tx.send_data(buf.split().freeze()).await {
                        warn!(update_id = %update.id(), "could not forward subscription query event to receiver: {e}");
                        return;
                    }
                } else {
                    if tx.is_closed() {
                        warn!(update_id = %update.id(), "body sender was closed, stopping event broadcast sends");
                        return;
                    }
                    send_deadline.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(10));
                    continue;
                }
            },
            _ = update.cancelled() => {
                info!(update_id = %update.id(), "update cancelled, aborting forwarding bytes to subscriber");
                return;
            },
            _ = &mut tripwire => {
                break;
            }
        }
    }

    while let Ok(event_buf) = rx.try_recv() {
        buf.extend_from_slice(&event_buf);
        if let Err(e) = tx.send_data(buf.split().freeze()).await {
            warn!(update_id = %update.id(), "could not forward subscription query event to receiver: {e}");
            return;
        }
    }
}
