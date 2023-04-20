use std::{net::SocketAddr, sync::Arc};

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{
    extract::{ConnectInfo, RawBody},
    Extension,
};
use bytes::BytesMut;
use futures::StreamExt;
use tokio::sync::mpsc::Sender;
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, error, warn};

use crate::Bookie;
use crate::{
    actor::ActorId,
    broadcast::{BroadcastSrc, FocaInput, Message},
    handle_payload,
};

#[allow(clippy::too_many_arguments)]
pub async fn peer_api_v1_broadcast(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(actor_id): Extension<ActorId>,
    Extension(gossip_msg_tx): Extension<Sender<Message>>,
    Extension(foca_tx): Extension<Sender<FocaInput>>,
    Extension(bookie): Extension<Bookie>,
    Extension(clock): Extension<Arc<uhlc::HLC>>,
    RawBody(mut body): RawBody,
) -> impl IntoResponse {
    match headers.get("corro-clock") {
        Some(value) => match serde_json::from_slice::<'_, uhlc::Timestamp>(value.as_bytes()) {
            Ok(ts) => {
                if let Err(e) = clock.update_with_timestamp(&ts) {
                    warn!("clock drifted too much! {e}");
                }
            }
            Err(e) => {
                error!("could not parse timestamp: {e}");
            }
        },
        None => {
            debug!("old corroded geezer");
        }
    }

    let mut codec = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .new_codec();
    let mut buf = BytesMut::new();

    let mut kind = None;

    loop {
        match body.next().await {
            Some(Ok(bytes)) => {
                kind = Some(
                    match handle_payload(
                        kind,
                        BroadcastSrc::Http(addr),
                        &mut buf,
                        &mut codec,
                        bytes,
                        &foca_tx,
                        actor_id,
                        &bookie,
                        &gossip_msg_tx,
                    )
                    .await
                    {
                        Ok(kind) => kind,
                        Err(e) => {
                            error!("error handling payload: {e}");
                            return StatusCode::UNPROCESSABLE_ENTITY;
                        }
                    },
                )
            }
            Some(Err(e)) => {
                error!("error reading bytes from gossip body: {e}");
                return StatusCode::UNPROCESSABLE_ENTITY;
            }
            None => {
                break;
            }
        }
    }

    StatusCode::ACCEPTED
}

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),

    #[error("expected a column, there were none")]
    NoMoreColumns,

    #[error("expected column '{0}', got none")]
    MissingColumn(&'static str),

    #[error("expected column '{0}', got '{1}'")]
    WrongColumn(&'static str, String),

    #[error("unexpected column '{0}'")]
    UnexpectedColumn(String),

    #[error("unknown resource state")]
    UnknownResourceState,

    #[error("unknown resource type")]
    UnknownResourceType,

    #[error("wrong ip byte len: {0}")]
    WrongIpByteLength(usize),
}

#[derive(Debug, thiserror::Error)]
#[error("unknown accept header value '{0}'")]
pub struct UnknownAcceptHeaderValue(String);

// fn process_one(one: OneVersion) -> eyre::Result<bool> {
//     let op = match one.got {
//         Some(true) => fetch_one(&mut *one.tx, one.actor_index, &one.version)?,
//         Some(false) => None,
//         None => return Ok(false),
//     };
//     one.sender.send((
//         ChangeId {
//             actor_id: one.actor_id,
//             version: Version(one.version),
//         },
//         op,
//     ))?;
//     Ok(true)
// }

// fn fetch_one<'a>(
//     tx: &'a mut corro_types::persy::Transaction,
//     actor_index: &'a ActorIndex,
//     version: &'a u64,
// ) -> eyre::Result<Option<Vec<u8>>> {
//     Ok(match actor_index.one_tx(&mut *tx, version)? {
//         Some(persy_id) if !persy_id.is_empty() => tx.read("ops", &persy_id.parse()?)?,
//         Some(_) | None => None,
//     })
// }

// fn process_one_local<'a>(
//     tx: &'a mut corro_types::persy::Transaction,
//     actor_id: ActorId,
//     actor_index: &'a ActorIndex,
//     version: u64,
//     sender: &'a UnboundedSender<(ChangeId, Option<Vec<u8>>)>,
// ) -> eyre::Result<bool> {
//     let op = fetch_one(&mut *tx, actor_index, &version)?;

//     sender.send((
//         ChangeId {
//             actor_id,
//             version: Version(version),
//         },
//         op,
//     ))?;
//     Ok(true)
// }

// fn process_sync(
//     ops_db: &OpsDb,
//     sync: SyncMessage,
//     local_actor_id: ActorId,
//     sender: UnboundedSender<(ChangeId, Option<Vec<u8>>)>,
// ) -> eyre::Result<()> {
//     let heads: HashMap<ActorId, Version> = sync
//         .heads
//         .into_iter()
//         .map(|change_id| (change_id.actor_id, change_id.version))
//         .collect();

//     let actor_ids: Vec<ActorId> = ops_db.actor_ids();

//     let mut ops_tx = ops_db.begin("sync-server")?;

//     for actor_id in actor_ids {
//         let mut processed = 0;
//         let known_index = match ops_tx.known_index_cached(&actor_id) {
//             None => continue,
//             Some(known) => known,
//         };

//         if !known_index.read().synced() {
//             continue;
//         }

//         let (index, tx, is_local) = if actor_id == local_actor_id {
//             (ops_tx.local_ops_index(), ops_tx.local_tx(), true)
//         } else {
//             (ops_tx.actor_index(actor_id)?, ops_tx.tx(), false)
//         };

//         if let Some(versions) = sync.need.get(&actor_id).cloned() {
//             trace!(
//                 "sync needs for {}, count: {}",
//                 actor_id.0.as_simple(),
//                 versions.len()
//             );

//             for vr in versions {
//                 match vr {
//                     VersionRange::Single(version) => {
//                         let success = if is_local {
//                             process_one_local(tx, actor_id, &index, version, &sender)?
//                         } else {
//                             process_one(OneVersion {
//                                 got: known_index.read().map.get(&version).copied(),
//                                 tx,
//                                 actor_index: &index,
//                                 actor_id,
//                                 version,

//                                 sender: &sender,
//                             })?
//                         };
//                         if success {
//                             processed += 1;
//                         }
//                     }
//                     // TODO: process with a persy range instead of one by one
//                     VersionRange::Range(range) => {
//                         for version in range[0]..range[1] {
//                             let success = if is_local {
//                                 process_one_local(tx, actor_id, &index, version, &sender)?
//                             } else {
//                                 process_one(OneVersion {
//                                     actor_index: &index,
//                                     actor_id,
//                                     version,
//                                     got: known_index.read().map.get(&version).copied(),
//                                     sender: &sender,
//                                     tx,
//                                 })?
//                             };
//                             if success {
//                                 processed += 1;
//                             }
//                         }
//                     }
//                 }
//             }
//         }

//         let start_version = heads.get(&actor_id).map(|v| v.0).unwrap_or_default() + 1;

//         if let Some(last_version) = index.last_key_tx(tx)? {
//             for version in start_version..=last_version {
//                 let success = if is_local {
//                     process_one_local(tx, actor_id, &index, version, &sender)?
//                 } else {
//                     process_one(OneVersion {
//                         actor_index: &index,
//                         actor_id,
//                         version,
//                         got: known_index.read().map.get(&version).copied(),
//                         sender: &sender,
//                         tx,
//                     })?
//                 };
//                 if success {
//                     processed += 1;
//                 }
//             }
//         }

//         counter!("corrosion.sync.server.changes.processed", processed as u64, "actor_id" => actor_id.0.to_string());
//     }

//     debug!("done with sync process");

//     Ok(())
// }

// pub async fn peer_api_v1_sync_post(
//     headers: HeaderMap,
//     RawBody(req_body): RawBody,
//     Extension(ops_db): Extension<OpsDb>,
//     Extension(clock): Extension<Arc<uhlc::HLC>>,
//     Extension(actor): Extension<Actor>,
// ) -> impl IntoResponse {
//     match headers.get("corro-clock") {
//         Some(value) => match serde_json::from_slice::<'_, uhlc::Timestamp>(value.as_bytes()) {
//             Ok(ts) => {
//                 if let Err(e) = clock.update_with_timestamp(&ts) {
//                     error!("clock drifted too much! {e}");
//                 }
//             }
//             Err(e) => {
//                 error!("could not parse timestamp: {e}");
//             }
//         },
//         None => {
//             debug!("old corroded geezer");
//         }
//     }

//     let (mut sender, body) = hyper::body::Body::channel();
//     let (tx, rx) = unbounded_channel::<(ChangeId, Option<Vec<u8>>)>();

//     tokio::spawn(async move {
//         // let start = Instant::now();
//         let mut rx = tokio_stream::wrappers::UnboundedReceiverStream::new(rx)
//             .map(|(change_id, data)| {
//                 let framed_len = size_of::<u128>()
//                     + size_of::<u64>()
//                     + data.as_ref().map(|data| data.len()).unwrap_or_default();
//                 let mut buf = BytesMut::with_capacity(size_of::<u32>() + framed_len);
//                 buf.put_uint(framed_len as u64, size_of::<u32>());
//                 buf.put_u128(change_id.actor_id.0.as_u128());
//                 buf.put_u64(change_id.version.0);
//                 if let Some(ref data) = data {
//                     buf.put(&data[..]);
//                 }
//                 buf.freeze()
//             })
//             .chunks(512);
//         while let Some(bytes) = rx.next().await {
//             let buf_len = bytes.iter().map(|b| b.len()).sum();
//             let mut buf = BytesMut::with_capacity(buf_len);
//             for b in bytes {
//                 buf.extend(b);
//             }

//             match timeout(Duration::from_secs(5), sender.send_data(buf.freeze())).await {
//                 Err(_) => {
//                     error!("sending data timed out");
//                     break;
//                 }
//                 // this only happens if the channel closes...
//                 Ok(Err(e)) => {
//                     debug!("error sending data: {e}");
//                     break;
//                 }
//                 Ok(_) => {}
//             }

//             counter!("corrosion.sync.server.chunk.sent.bytes", buf_len as u64);
//         }
//     });

//     tokio::spawn(async move {
//         let bytes = hyper::body::to_bytes(req_body).await?;

//         tokio::runtime::Handle::current()
//             .spawn_blocking(move || {
//                 let sync = serde_json::from_slice(bytes.as_ref())?;

//                 let now = Instant::now();
//                 match process_sync(&ops_db, sync, actor.id(), tx) {
//                     Ok(_) => {
//                         histogram!(
//                             "corrosion.sync.server.process.time.seconds",
//                             now.elapsed().as_secs_f64()
//                         );
//                     }
//                     Err(e) => {
//                         error!("could not process sync request: {e}");
//                     }
//                 }

//                 Ok::<_, eyre::Report>(())
//             })
//             .await??;

//         Ok::<_, eyre::Report>(())
//     });

//     hyper::Response::builder()
//         .header(hyper::header::CONTENT_TYPE, "application/json")
//         .body(body)
//         .unwrap()
// }
