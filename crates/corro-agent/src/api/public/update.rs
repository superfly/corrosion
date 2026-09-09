use std::{collections::HashMap, io::Write, sync::Arc, time::Duration};

use antithesis_sdk::assert_sometimes;
use axum::{http::StatusCode, response::IntoResponse, Extension};
use bytes::{BufMut, Bytes, BytesMut};
use compact_str::ToCompactString;
use corro_types::{
    agent::Agent,
    api::{NotifyEvent, QUERY_ID_HEADER},
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
        handle,
        sub_rx,
        tx,
        tripwire,
        Duration::from_millis(agent.config().perf.stream_flush_timeout),
    ));

    hyper::Response::builder()
        .status(StatusCode::OK)
        .header(QUERY_ID_HEADER, update_id.to_string())
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
    flush_timeout: Duration,
) {
    let mut buf = BytesMut::new();
    let mut send_deadline = None;

    loop {
        tokio::select! {
            biased;
            _ = tx.closed() => {
                warn!(update_id = %update.id(), "body sender was closed, stopping event broadcast sends");
                return;
            },
            // Preserve the first event's deadline even when more events arrive.
            _ = async { tokio::time::sleep_until(send_deadline.unwrap()).await }, if send_deadline.is_some() => {
                if let Err(e) = tx.send_data(buf.split().freeze()).await {
                    warn!(update_id = %update.id(), "could not forward subscription query event to receiver: {e}");
                    return;
                }
                send_deadline = None;
            },
            res = rx.recv() => {
                match res {
                    Ok(event_buf) => {
                        buf.extend_from_slice(&event_buf);
                        if buf.len() >= 64 * 1024 {
                            if let Err(e) = tx.send_data(buf.split().freeze()).await {
                                warn!(update_id = %update.id(), "could not forward update query event to receiver: {e}");
                                return;
                            }
                            send_deadline = None;
                        } else {
                            send_deadline.get_or_insert_with(|| tokio::time::Instant::now() + flush_timeout);
                        }
                    },
                    Err(RecvError::Lagged(skipped)) => {
                        warn!(update_id = %update.id(), "update skipped {} events, aborting", skipped);
                        return;
                    },
                    Err(RecvError::Closed) => {
                        info!(update_id = %update.id(), "events subscription ran out");
                        break;
                    },
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

    if !buf.is_empty() {
        if let Err(e) = tx.send_data(buf.freeze()).await {
            warn!(update_id = %update.id(), "could not forward last update query event to receiver: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn update_handle(tripwire: Tripwire) -> UpdateHandle {
        let schema =
            corro_types::schema::parse_sql("CREATE TABLE items (id TEXT NOT NULL PRIMARY KEY);")
                .unwrap();
        let (events, _rx) = mpsc::channel(16);
        UpdateHandle::create(Uuid::new_v4(), "items", &schema, events, tripwire).unwrap()
    }

    #[derive(Default)]
    struct StreamWakeCount(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for StreamWakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn update_forwarder_does_not_wake_while_idle() {
        use std::{future::Future, task::Context};

        let (_events, rx) = broadcast::channel(16);
        let (tx, _body) = CountedBody::channel(persistent_gauge!("test.streams"));
        let (tripwire, _worker, _trigger) = Tripwire::new_simple();
        let update = update_handle(tripwire.clone());
        let mut forward = Box::pin(forward_update_bytes_to_body_sender(
            update,
            rx,
            tx,
            tripwire,
            Duration::from_millis(10),
        ));
        let wakes = Arc::new(StreamWakeCount::default());
        let waker = std::task::Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);

        assert!(forward.as_mut().poll(&mut cx).is_pending());
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn update_forwarder_batches_from_first_event_after_idle() {
        use http_body_util::BodyExt;

        let (events, rx) = broadcast::channel(16);
        let (tx, mut body) = CountedBody::channel(persistent_gauge!("test.streams"));
        let (tripwire, _worker, _trigger) = Tripwire::new_simple();
        let update = update_handle(tripwire.clone());
        let mut forward = Box::pin(forward_update_bytes_to_body_sender(
            update,
            rx,
            tx,
            tripwire,
            Duration::from_millis(100),
        ));

        assert!(futures::poll!(&mut forward).is_pending());
        tokio::time::sleep(Duration::from_secs(1)).await;
        events.send(Bytes::from_static(b"first\n")).unwrap();
        assert!(futures::poll!(&mut forward).is_pending());
        assert!(futures::poll!(body.frame()).is_pending());
        tokio::time::sleep(Duration::from_millis(50)).await;
        events.send(Bytes::from_static(b"second\n")).unwrap();
        assert!(futures::poll!(&mut forward).is_pending());
        assert!(futures::poll!(body.frame()).is_pending());
        tokio::time::sleep(Duration::from_millis(51)).await;
        assert!(futures::poll!(&mut forward).is_pending());
        let frame = futures::poll!(body.frame())
            .map(Option::unwrap)
            .map(Result::unwrap);
        assert_eq!(
            frame.map(|f| f.into_data().unwrap()),
            std::task::Poll::Ready(Bytes::from_static(b"first\nsecond\n"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn update_forwarder_flushes_at_size_limit_without_an_idle_timer() {
        use http_body_util::BodyExt;
        use std::{future::Future, task::Context};

        let (events, rx) = broadcast::channel(16);
        let (tx, mut body) = CountedBody::channel(persistent_gauge!("test.streams"));
        let (tripwire, _worker, _trigger) = Tripwire::new_simple();
        let update = update_handle(tripwire.clone());
        let mut forward = Box::pin(forward_update_bytes_to_body_sender(
            update,
            rx,
            tx,
            tripwire,
            Duration::from_millis(10),
        ));
        let wakes = Arc::new(StreamWakeCount::default());
        let waker = std::task::Waker::from(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        let event = Bytes::from(vec![b'x'; 64 * 1024]);

        events.send(Bytes::from_static(b"prefix")).unwrap();
        assert!(forward.as_mut().poll(&mut cx).is_pending());
        events.send(event.clone()).unwrap();
        assert!(forward.as_mut().poll(&mut cx).is_pending());
        let frame = futures::poll!(body.frame())
            .map(Option::unwrap)
            .map(Result::unwrap);
        assert_eq!(
            frame.map(|f| f.into_data().unwrap().len()),
            std::task::Poll::Ready(6 + event.len())
        );
        wakes.0.store(0, std::sync::atomic::Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn update_forwarder_flushes_buffer_on_shutdown() {
        use http_body_util::BodyExt;

        let (events, rx) = broadcast::channel(16);
        let (tx, mut body) = CountedBody::channel(persistent_gauge!("test.streams"));
        let (tripwire, worker, _trigger) = Tripwire::new_simple();
        let update = update_handle(tripwire.clone());
        let mut forward = Box::pin(forward_update_bytes_to_body_sender(
            update,
            rx,
            tx,
            tripwire,
            Duration::from_millis(10),
        ));

        events.send(Bytes::from_static(b"last\n")).unwrap();
        assert!(futures::poll!(&mut forward).is_pending());
        drop(worker);
        assert!(futures::poll!(&mut forward).is_ready());
        let frame = futures::poll!(body.frame())
            .map(Option::unwrap)
            .map(Result::unwrap);
        assert_eq!(
            frame.map(|f| f.into_data().unwrap()),
            std::task::Poll::Ready(Bytes::from_static(b"last\n"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn update_forwarder_flushes_without_delay_when_timeout_is_zero() {
        use http_body_util::BodyExt;

        let (events, rx) = broadcast::channel(16);
        let (tx, mut body) = CountedBody::channel(persistent_gauge!("test.streams"));
        let (tripwire, _worker, _trigger) = Tripwire::new_simple();
        let update = update_handle(tripwire.clone());
        let mut forward = Box::pin(forward_update_bytes_to_body_sender(
            update,
            rx,
            tx,
            tripwire,
            Duration::ZERO,
        ));

        events.send(Bytes::from_static(b"now\n")).unwrap();
        assert!(futures::poll!(&mut forward).is_pending());
        let frame = futures::poll!(body.frame())
            .map(Option::unwrap)
            .map(Result::unwrap);
        assert_eq!(
            frame.map(|f| f.into_data().unwrap()),
            std::task::Poll::Ready(Bytes::from_static(b"now\n"))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn update_forwarder_stops_on_disconnect_without_a_timer() {
        let (_events, rx) = broadcast::channel(16);
        let (tx, body) = CountedBody::channel(persistent_gauge!("test.streams"));
        let (tripwire, _worker, _trigger) = Tripwire::new_simple();
        let update = update_handle(tripwire.clone());
        let mut forward = Box::pin(forward_update_bytes_to_body_sender(
            update,
            rx,
            tx,
            tripwire,
            Duration::from_millis(10),
        ));

        assert!(futures::poll!(&mut forward).is_pending());
        drop(body);
        assert!(futures::poll!(&mut forward).is_ready());
    }
}
