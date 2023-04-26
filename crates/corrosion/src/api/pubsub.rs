// use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

// use axum::{
//     extract::{ws::WebSocket, ConnectInfo, WebSocketUpgrade},
//     response::IntoResponse,
//     Extension,
// };
// use corro_types::{
//     actor::ActorId,
//     broadcast::{BroadcastInput, Message, MessageV1},
//     pubsub::{
//         Subscriber, SubscriberId, Subscribers, Subscription, SubscriptionFilter, SubscriptionId,
//         SubscriptionInfo, SubscriptionMessage,
//     },
// };
// use futures::Stream;
// use metrics::increment_counter;
// use parking_lot::RwLock;
// use tokio::sync::mpsc::{channel, unbounded_channel, Sender, UnboundedSender};
// use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
// use tokio_util::{codec::LengthDelimitedCodec, io::StreamReader};
// use tracing::{debug, error, trace};
// use tripwire::Tripwire;
// use uhlc::HLC;

// #[derive(Debug, thiserror::Error)]
// pub enum SubscribeError {
//     #[error(transparent)]
//     Serde(#[from] serde_json::Error),
//     #[error(transparent)]
//     Broadcast(#[from] BroadcastStreamRecvError),
// }

// #[allow(clippy::too_many_arguments)]
// fn add_subscriber(
//     subscribers: &Subscribers,
//     conn_id: SubscriberId,
//     clock: &HLC,
//     tx: UnboundedSender<SubscriptionMessage>,
//     id: SubscriptionId,
//     filter: Option<SubscriptionFilter>,
//     broadcast: bool,
//     update_tx: &Sender<Vec<SubscriptionFilter>>,
// ) {
//     trace!("add subscription");
//     let conn_subs = {
//         let map = subscribers.read();
//         if let Some(subs) = map.get(&conn_id) {
//             subs.clone()
//         } else {
//             drop(map);
//             let mut map = subscribers.write();
//             map.entry(conn_id)
//                 .or_insert_with(|| {
//                     Arc::new(RwLock::new(Subscriber {
//                         subscriptions: HashMap::new(),
//                         sender: tx.clone(),
//                     }))
//                 })
//                 .clone()
//         }
//     };
//     increment_counter!("corrosion.subscriptions.update.count", "id" => id.to_string());
//     {
//         let mut sub = conn_subs.write();
//         sub.subscriptions.insert(
//             id,
//             SubscriptionInfo {
//                 filter,
//                 broadcast,
//                 updated_at: clock.new_timestamp(),
//             },
//         );
//     }
//     if broadcast {
//         update_actor_subscriptions(subscribers, update_tx);
//     }
// }

// fn update_actor_subscriptions(
//     subscribers: &Subscribers,
//     update_tx: &Sender<Vec<SubscriptionFilter>>,
// ) {
//     debug!("updating actor subscriptions from API");

//     if let Err(e) = update_tx.try_send(generate_broadcasted_subscriptions(subscribers)) {
//         increment_counter!("corrosion.channel.error", "type" => "full", "name" => "update_actor_subscription.update");
//         error!("error dispatching actor subscriptions update: {e}");
//     }
// }

// fn generate_broadcasted_subscriptions(subscribers: &Subscribers) -> Vec<SubscriptionFilter> {
//     subscribers
//         .read()
//         .values()
//         .flat_map(|map| {
//             map.read()
//                 .subscriptions
//                 .values()
//                 // only set filters if actually filtering, or else this is a firehose between all nodes
//                 .filter_map(|info| {
//                     if info.broadcast {
//                         info.filter.as_ref()
//                     } else {
//                         None
//                     }
//                 })
//                 .cloned()
//                 .collect::<Vec<SubscriptionFilter>>()
//         })
//         .collect()
// }

// fn generate_upsert_sub(
//     actor_id: ActorId,
//     id: SubscriptionId,
//     filter: SubscriptionFilter,
//     clock: &HLC,
// ) -> BroadcastInput {
//     BroadcastInput::AddBroadcast(Message::V1(MessageV1::UpsertSubscription {
//         actor_id,
//         id,
//         filter: filter.input().to_owned(),
//         ts: clock.new_timestamp().into(),
//     }))
// }

// #[allow(clippy::too_many_arguments)]
// pub async fn api_v1_subscribe_post(
//     ws: WebSocketUpgrade,
//     ConnectInfo(addr): ConnectInfo<SocketAddr>,
//     Extension(actor_id): Extension<ActorId>,
//     Extension(clock): Extension<Arc<HLC>>,
//     Extension(subscribers): Extension<Subscribers>,
//     Extension(bcast_tx): Extension<Sender<BroadcastInput>>,
//     Extension(mut tripwire): Extension<Tripwire>,
// ) -> impl IntoResponse {
//     ws.on_upgrade(move |socket| handle_socket(socket, addr))
// }

// /// Actual websocket statemachine (one will be spawned per connection)
// async fn handle_socket(mut socket: WebSocket, addr: SocketAddr) {
//     let (mut sender, res_body) = hyper::body::Body::channel();

//     trace!("subscribe post!");

//     let sub_id = SubscriberId(addr);

//     // enum Event {
//     //     Recv(io::Result<Option<Subscription>>),
//     //     Send(Option<SubscriptionMessage>),
//     //     SetUpdatedSubs(Vec<SubscriptionFilter>),
//     //     CheckUpdate,
//     //     Tripwire,
//     // }

//     // loop {
//     //     let evt = tokio::select! {
//     //         biased;
//     //         send = rx.recv() => Event::Send(send),
//     //         recv = framed.try_next() => Event::Recv(recv),
//     //         updated = update_rx.recv() => match updated {
//     //             Some(subs) => Event::SetUpdatedSubs(subs),
//     //             None => continue,
//     //         },
//     //         _ = interval.tick() => Event::CheckUpdate,
//     //         _ = &mut tripwire => Event::Tripwire,
//     //     };

//     //     match evt {
//     //         Event::SetUpdatedSubs(new_subs) => {
//     //             trace!("setting updated subs: {:?}", new_subs);
//     //             last_received_subs = new_subs;
//     //         }
//     //         Event::CheckUpdate => {
//     //             trace!(
//     //                 "check updated subscriptions to send, last len: {}, recv len: {}",
//     //                 last_updated_subs.len(),
//     //                 last_received_subs.len()
//     //             );
//     //             if last_updated_subs != last_received_subs {
//     //                 debug!("yes, they didn't match, send them along");
//     //                 _ = foca_tx
//     //                     .send(generate_upsert_sub(&actor, last_received_subs.as_slice()))
//     //                     .await;
//     //                 last_updated_subs = last_received_subs.clone();
//     //             }
//     //         }
//     //         Event::Recv(recv) => match recv {
//     //             Ok(Some(sub)) => match sub {
//     //                 Subscription::Add {
//     //                     id,
//     //                     filter,
//     //                     apply_index,
//     //                     broadcast_interest,
//     //                 } => {
//     //                     if filter.is_none() && broadcast_interest {
//     //                         _ = tx.send(SubscriptionMessage::Event {
//     //                             id,
//     //                             event: SubscriptionEvent::Error {
//     //                                 error: "can't broadcast interest for 'everything' filter"
//     //                                     .into(),
//     //                             },
//     //                         });
//     //                         continue;
//     //                     }
//     //                     let filter = match filter
//     //                         .map(|f| {
//     //                             parse_expr(f.as_str()).map(|expr| SubscriptionFilter::new(f, expr))
//     //                         })
//     //                         .transpose()
//     //                     {
//     //                         Ok(res) => res,
//     //                         Err(e) => {
//     //                             error!("error parsing expr: {e}");
//     //                             let msg = SubscriptionMessage::Event {
//     //                                 id: id.clone(),
//     //                                 event: SubscriptionEvent::Error {
//     //                                     error: e.to_string(),
//     //                                 },
//     //                             };
//     //                             match serde_json::to_vec(&msg) {
//     //                                 Ok(msg_bytes) => {
//     //                                     let mut buf = BytesMut::with_capacity(
//     //                                         size_of::<u32>() + msg_bytes.len(),
//     //                                     );
//     //                                     buf.put_uint(msg_bytes.len() as u64, size_of::<u32>());
//     //                                     buf.put(msg_bytes.as_slice());
//     //                                     if let Err(e) = sender.send_data(buf.freeze()).await {
//     //                                         warn!("could not send op sub msg bytes: {e}");
//     //                                     }
//     //                                 }
//     //                                 Err(e) => {
//     //                                     error!("could not serialize op sub message: {e} ({msg:?})");
//     //                                     break;
//     //                                 }
//     //                             }
//     //                             continue;
//     //                         }
//     //                     };
//     //                     trace!("parsed subscription filter: {filter:?}");

//     //                     if let Some(last_op_id) = apply_index {
//     //                         let ops_db = ops_db.clone();
//     //                         let tx = tx.clone();
//     //                         let subscribers = subscribers.clone();
//     //                         let update_tx = update_tx.clone();

//     //                         let clock = clock.clone();
//     //                         tokio::runtime::Handle::current().spawn_blocking(move || {
//     //                             if let Err(e) = catch_up_subscriber(
//     //                                 ops_db,
//     //                                 CatchUp {
//     //                                     id,
//     //                                     filter,
//     //                                     broadcast_interest,
//     //                                     last_op_id,
//     //                                     tx,
//     //                                 },
//     //                                 &clock,
//     //                                 sub_id,
//     //                                 subscribers,
//     //                                 &update_tx,
//     //                             ) {
//     //                                 error!("could not catch up subscriber: {e}");
//     //                             }
//     //                         });
//     //                     } else {
//     //                         add_subscriber(
//     //                             &subscribers,
//     //                             sub_id,
//     //                             &clock,
//     //                             tx.clone(),
//     //                             id,
//     //                             filter,
//     //                             broadcast_interest,
//     //                             &update_tx,
//     //                         );
//     //                     }
//     //                 }
//     //                 Subscription::Remove { id } => {
//     //                     let map = subscribers.read();
//     //                     let should_update = if let Some(subs) = map.get(&sub_id) {
//     //                         let mut subs = subs.write();
//     //                         if let Some(info) = subs.subscriptions.remove(&id) {
//     //                             info.broadcast
//     //                         } else {
//     //                             false
//     //                         }
//     //                     } else {
//     //                         false
//     //                     };
//     //                     if should_update {
//     //                         update_actor_subscriptions(&subscribers, &update_tx);
//     //                     }
//     //                 }
//     //             },
//     //             Ok(None) => {
//     //                 debug!("subscriber body is done");
//     //                 break;
//     //             }
//     //             Err(e) => {
//     //                 warn!("op subscriber req body error: {e}");
//     //                 break;
//     //             }
//     //         },
//     //         Event::Send(send) => match send {
//     //             Some(msg) => match serde_json::to_vec(&msg) {
//     //                 Ok(msg_bytes) => {
//     //                     let mut buf = BytesMut::with_capacity(size_of::<u32>() + msg_bytes.len());
//     //                     buf.put_uint(msg_bytes.len() as u64, size_of::<u32>());
//     //                     buf.put(msg_bytes.as_slice());
//     //                     if let Err(e) = sender.send_data(buf.freeze()).await {
//     //                         warn!("could not send op sub msg bytes: {e}");
//     //                         break;
//     //                     }
//     //                 }
//     //                 Err(e) => {
//     //                     error!("could not serialize op sub message: {e} ({msg:?})");
//     //                 }
//     //             },
//     //             None => {
//     //                 break;
//     //             }
//     //         },
//     //         Event::Tripwire => {
//     //             match serde_json::to_vec(&SubscriptionMessage::GoingAway) {
//     //                 Ok(msg_bytes) => {
//     //                     let mut buf = BytesMut::with_capacity(size_of::<u32>() + msg_bytes.len());
//     //                     buf.put_uint(msg_bytes.len() as u64, size_of::<u32>());
//     //                     buf.put(msg_bytes.as_slice());
//     //                     if let Err(e) = sender.send_data(buf.freeze()).await {
//     //                         warn!("could not send op sub msg bytes: {e}");
//     //                         break;
//     //                     }
//     //                 }
//     //                 Err(e) => {
//     //                     error!("could not serialize 'going away' sub message: {e}");
//     //                 }
//     //             }
//     //             break;
//     //         }
//     //     }
//     // }

//     // debug!("subscriber with sub id: {sub_id} is done");

//     // let should_update = {
//     //     // drop all subscription for this conn
//     //     let mut subscribers = subscribers.write();
//     //     if let Some(subs) = subscribers.remove(&sub_id) {
//     //         subs.read()
//     //             .subscriptions
//     //             .iter()
//     //             .any(|(_, info)| info.broadcast)
//     //     } else {
//     //         false
//     //     }
//     // };

//     // if should_update {
//     //     let sub_filters = generate_broadcasted_subscriptions(&subscribers);
//     //     _ = foca_tx
//     //         .send(generate_upsert_sub(&actor, sub_filters.as_slice()))
//     //         .await;
//     // }
// }

// // struct CatchUp {
// //     id: SubscriptionId,
// //     filter: Option<SubscriptionFilter>,
// //     broadcast_interest: bool,
// //     last_op_id: u128,
// //     tx: UnboundedSender<SubscriptionMessage>,
// // }

// // fn catch_up_subscriber(
// //     ops_db: OpsDb,
// //     catch_up: CatchUp,
// //     clock: &HLC,
// //     sub_id: SubscriberId,
// //     subs: Subscribers,
// //     update_tx: &Sender<Vec<SubscriptionFilter>>,
// // ) -> eyre::Result<()> {
// //     let CatchUp {
// //         id,
// //         filter,
// //         broadcast_interest,
// //         last_op_id,
// //         tx,
// //     } = catch_up;
// //     if let Some(actual_last_op_id) = ops_db.last_op_id()? {
// //         if actual_last_op_id < (last_op_id + 1) {
// //             add_subscriber(
// //                 &subs,
// //                 sub_id,
// //                 clock,
// //                 tx,
// //                 id,
// //                 filter,
// //                 broadcast_interest,
// //                 update_tx,
// //             );
// //             return Ok(());
// //         }
// //     } else {
// //         add_subscriber(
// //             &subs,
// //             sub_id,
// //             clock,
// //             tx,
// //             id,
// //             filter,
// //             broadcast_interest,
// //             update_tx,
// //         );
// //         return Ok(());
// //     }
// //     let mut op_id = last_op_id;

// //     loop {
// //         let mut processed = 0;
// //         for (cur_op_id, mut change_ids) in ops_db.range_op_ids((op_id + 1)..)? {
// //             op_id = cur_op_id;
// //             let change_id_bytes = change_ids.next().unwrap();
// //             let mut v = change_id_bytes.as_ref();
// //             let actor_id = ActorId::from(v.get_u128());
// //             let version = v.get_u64();

// //             if let Some(Ok(persy_id)) = ops_db.actor_op(actor_id, version)?.map(|s| s.parse()) {
// //                 if let Some(op_bytes) = ops_db.read("ops", &persy_id)? {
// //                     let op = Operation::from_bytes(&op_bytes)?;

// //                     if let Some(filter) = &filter {
// //                         let op_info = op.info();
// //                         if let Some(applied_vec) =
// //                             ops_db.one_applied(op_info.record_type, &op_info.key)?
// //                         {
// //                             let mut v = applied_vec.as_ref();
// //                             v.advance(
// //                                 size_of::<i64>()
// //                                     + size_of::<u8>()
// //                                     + size_of::<u128>()
// //                                     + size_of::<u64>(),
// //                             );
// //                             let applied = if v.remaining() > 0 {
// //                                 postcard::from_bytes(v).ok()
// //                             } else {
// //                                 None
// //                             };
// //                             let ctx = build_filter_context(&op, applied.as_ref())?;
// //                             if !match_expr(filter.input(), filter.expr(), &ctx) {
// //                                 continue;
// //                             }
// //                         } else {
// //                             continue;
// //                         }
// //                     }

// //                     tx.send(SubscriptionMessage::Event {
// //                         id: id.clone(),
// //                         event: SubscriptionEvent::Change {
// //                             op: op.clone(),
// //                             id: op_id,
// //                         },
// //                     })?;
// //                 }
// //             }
// //             processed += 1;
// //         }
// //         // we're up to date
// //         if processed == 0 {
// //             break;
// //         }
// //     }

// //     add_subscriber(
// //         &subs,
// //         sub_id,
// //         clock,
// //         tx,
// //         id,
// //         filter,
// //         broadcast_interest,
// //         update_tx,
// //     );

// //     Ok(())
// // }
