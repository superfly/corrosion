use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum::{
    extract::{
        ws::{self, WebSocket},
        ConnectInfo, WebSocketUpgrade,
    },
    http::StatusCode,
    response::IntoResponse,
    Extension, Json,
};
use corro_types::{
    agent::Agent,
    change::Change,
    filters::{match_expr, parse_expr, AggregateChange},
    pubsub::{
        Subscriber, SubscriberId, Subscription, SubscriptionEvent, SubscriptionFilter,
        SubscriptionId, SubscriptionInfo, SubscriptionMessage,
    },
};
use metrics::increment_counter;
use parking_lot::RwLock;
use rusqlite::params;
use serde_json::json;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedSender},
    task::block_in_place,
};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tracing::{debug, error, info, trace, warn};
use tripwire::Tripwire;

#[derive(Debug, thiserror::Error)]
pub enum SubscribeError {
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Broadcast(#[from] BroadcastStreamRecvError),
}

#[allow(clippy::too_many_arguments)]
fn add_subscriber(
    agent: &Agent,
    conn_id: SubscriberId,
    sub: Subscriber,
    id: SubscriptionId,
    filter: Option<SubscriptionFilter>,
) {
    trace!("add subscription");
    let conn_subs = {
        let map = agent.subscribers().read();
        if let Some(subs) = map.get(&conn_id) {
            subs.clone()
        } else {
            drop(map);
            let mut map = agent.subscribers().write();
            map.entry(conn_id)
                .or_insert_with(|| Arc::new(RwLock::new(sub)))
                .clone()
        }
    };
    increment_counter!("corro.subscriptions.update.count", "id" => id.to_string());
    let updated_at = agent.clock().new_timestamp();

    {
        let mut sub = conn_subs.write();
        sub.insert(id.clone(), SubscriptionInfo { filter, updated_at });
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn api_v1_subscribe_post(
    Extension(agent): Extension<Agent>,
    Json(sub): Json<Subscription>,
) -> impl IntoResponse {
    match sub {
        Subscription::Add {
            id,
            where_clause: filter,
            ..
        } => {
            let filter = match filter {
                None => {
                    return (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(json!({"error": "filter is required for global subscription"})),
                    )
                }
                Some(f) => {
                    match parse_expr(f.as_str()).map(|expr| SubscriptionFilter::new(f, expr)) {
                        Ok(filter) => filter,
                        Err(e) => {
                            return (
                                StatusCode::UNPROCESSABLE_ENTITY,
                                Json(json!({"error": e.to_string()})),
                            )
                        }
                    }
                }
            };
            add_subscriber(
                &agent,
                SubscriberId::Global,
                Subscriber::Global {
                    subscriptions: HashMap::new(),
                },
                id,
                Some(filter),
            );
            (StatusCode::OK, Json(json!({"ok": true})))
        }
        Subscription::Remove { id } => {
            agent
                .subscribers()
                .read()
                .get(&SubscriberId::Global)
                .map(|subs| subs.write().remove(&id));
            (StatusCode::OK, Json(json!({"ok": true})))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn api_v1_subscribe_ws(
    ws: WebSocketUpgrade,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Extension(agent): Extension<Agent>,
    Extension(tripwire): Extension<Tripwire>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, addr, agent, tripwire))
}

/// Actual websocket statemachine (one will be spawned per connection)
async fn handle_socket(
    mut socket: WebSocket,
    addr: SocketAddr,
    agent: Agent,
    mut tripwire: Tripwire,
) {
    trace!("subscribe post!");

    let sub_id = SubscriberId::Local { addr };

    let (tx, mut rx) = unbounded_channel();

    enum Event {
        Recv(Option<Result<ws::Message, axum::Error>>),
        Send(Option<SubscriptionMessage>),
        Tripwire,
    }

    loop {
        let evt = tokio::select! {
            biased;
            send = rx.recv() => Event::Send(send),
            recv = socket.recv() => Event::Recv(recv),
            _ = &mut tripwire => Event::Tripwire,
        };

        match evt {
            Event::Recv(recv) => match recv {
                None => {
                    info!("{sub_id} disconnected");
                    break;
                }
                Some(Err(e)) => {
                    error!("got a ws error: {e}");
                }
                Some(Ok(message)) => {
                    let sub: Subscription = match message {
                        ws::Message::Text(s) => match serde_json::from_str(s.as_str()) {
                            Ok(sub) => sub,
                            Err(e) => {
                                warn!("could not parse sub message from client: {e}");
                                continue;
                            }
                        },
                        ws::Message::Binary(v) => match serde_json::from_slice(v.as_slice()) {
                            Ok(sub) => sub,
                            Err(e) => {
                                warn!("could not parse sub message from client: {e}");
                                continue;
                            }
                        },
                        ws::Message::Close(_) => {
                            info!("client {sub_id} gracefully closing connection");
                            break;
                        }
                        ws::Message::Ping(_) | ws::Message::Pong(_) => {
                            trace!("ping or pong, nothing to do");
                            continue;
                        }
                    };
                    match sub {
                        Subscription::Add {
                            id,
                            where_clause: filter,
                            from_db_version,
                        } => {
                            let filter = match filter
                                .map(|f| {
                                    parse_expr(f.as_str())
                                        .map(|expr| SubscriptionFilter::new(f, expr))
                                })
                                .transpose()
                            {
                                Ok(res) => res,
                                Err(e) => {
                                    error!("error parsing expr: {e}");
                                    let msg = SubscriptionMessage::Event {
                                        id: id.clone(),
                                        event: SubscriptionEvent::Error {
                                            error: e.to_string(),
                                        },
                                    };
                                    match serde_json::to_vec(&msg) {
                                        Ok(msg_bytes) => {
                                            if let Err(e) =
                                                socket.send(ws::Message::Binary(msg_bytes)).await
                                            {
                                                warn!("could not send op sub msg bytes: {e}");
                                            }
                                        }
                                        Err(e) => {
                                            error!(
                                                "could not serialize op sub message: {e} ({msg:?})"
                                            );
                                            break;
                                        }
                                    }
                                    continue;
                                }
                            };
                            trace!("parsed subscription filter: {filter:?}");

                            if let Some(from_db_version) = from_db_version {
                                let tx = tx.clone();
                                let agent = agent.clone();

                                tokio::spawn(async move {
                                    if let Err(e) = catch_up_subscriber(
                                        CatchUp {
                                            id,
                                            filter,
                                            from_db_version,
                                            tx,
                                        },
                                        &agent,
                                        sub_id,
                                    )
                                    .await
                                    {
                                        error!("could not catch up subscriber: {e}");
                                    }
                                });
                            } else {
                                add_subscriber(
                                    &agent,
                                    sub_id,
                                    Subscriber::Local {
                                        subscriptions: HashMap::new(),
                                        sender: tx.clone(),
                                    },
                                    id,
                                    filter,
                                );
                            }
                        }
                        Subscription::Remove { id } => {
                            let map = agent.subscribers().read();
                            if let Some(subs) = map.get(&sub_id) {
                                let mut subs = subs.write();
                                _ = subs.remove(&id);
                            }
                        }
                    }
                }
            },
            Event::Send(send) => match send {
                Some(msg) => match serde_json::to_vec(&msg) {
                    Ok(msg_bytes) => {
                        if let Err(e) = socket.send(ws::Message::Binary(msg_bytes)).await {
                            error!("could not send sub message: {e}");
                        }
                    }
                    Err(e) => {
                        error!("could not serialize op sub message: {e} ({msg:?})");
                    }
                },
                None => {
                    break;
                }
            },
            Event::Tripwire => {
                if let Err(e) = socket
                    .send(ws::Message::Close(Some(ws::CloseFrame {
                        code: ws::close_code::AWAY,
                        reason: "".into(),
                    })))
                    .await
                {
                    error!("could not send 'going away' close message: {e}");
                }
                break;
            }
        }
    }

    debug!("subscriber with sub id: {sub_id} is done");

    {
        let mut subscribers = agent.subscribers().write();
        subscribers.remove(&sub_id);
    }
}

struct CatchUp {
    id: SubscriptionId,
    filter: Option<SubscriptionFilter>,
    from_db_version: i64,
    tx: UnboundedSender<SubscriptionMessage>,
}

async fn catch_up_subscriber(
    catch_up: CatchUp,
    agent: &Agent,
    sub_id: SubscriberId,
) -> eyre::Result<()> {
    let CatchUp {
        id,
        filter,
        from_db_version,
        tx,
    } = catch_up;

    let conn = agent.pool().read().await?;

    block_in_place(|| {
        let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id FROM crsql_changes WHERE db_version >= ? ORDER BY db_version, seq ASC"#)?;

        let mut rows = prepped.query(params![from_db_version])?;

        let mut changeset = vec![];
        let mut last_db_version = from_db_version;

        while let Some(row) = rows.next()? {
            let change = Change {
                table: row.get(0)?,
                pk: row.get(1)?,
                cid: row.get(2)?,
                val: row.get(3)?,
                col_version: row.get(4)?,
                db_version: row.get(5)?,
                seq: row.get(6)?,
                site_id: row.get(7)?,
            };

            if change.db_version != last_db_version {
                let schema = agent.schema().read();
                if !changeset.is_empty() {
                    let aggs = AggregateChange::from_changes(
                        changeset.drain(..),
                        &schema,
                        last_db_version,
                    );
                    for agg in aggs {
                        let matches = filter
                            .as_ref()
                            .map(|filter| match_expr(filter.expr(), &agg))
                            .unwrap_or(true);
                        if matches {
                            _ = tx.send(SubscriptionMessage::Event {
                                id: id.clone(),
                                event: SubscriptionEvent::Change(agg.to_owned()),
                            });
                        }
                    }
                }
            }

            last_db_version = change.db_version;
        }

        if !changeset.is_empty() {
            let schema = agent.schema().read();
            let aggs = AggregateChange::from_changes(changeset.drain(..), &schema, last_db_version);
            for agg in aggs {
                let matches = filter
                    .as_ref()
                    .map(|filter| match_expr(filter.expr(), &agg))
                    .unwrap_or(true);
                if matches {
                    _ = tx.send(SubscriptionMessage::Event {
                        id: id.clone(),
                        event: SubscriptionEvent::Change(agg.to_owned()),
                    });
                }
            }
        }

        Ok::<_, rusqlite::Error>(())
    })?;

    add_subscriber(
        agent,
        sub_id,
        Subscriber::Local {
            subscriptions: HashMap::new(),
            sender: tx,
        },
        id,
        filter,
    );

    Ok(())
}
