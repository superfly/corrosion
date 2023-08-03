use std::collections::HashMap;
use std::error::Error;
use std::ops::RangeInclusive;
use std::time::Duration;
use std::{cmp, io};

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{extract::RawBody, Extension};
use bytes::{Buf, BytesMut};
use corro_types::agent::{Agent, KnownDbVersion, SplitPool};
use corro_types::broadcast::{ChangeV1, Changeset, Message};
use corro_types::change::row_to_change;
use corro_types::sync::{
    generate_sync, SyncMessage, SyncMessageEncodeError, SyncMessageV1, SyncStateV1,
};
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use metrics::{counter, increment_counter};
use rusqlite::{params, Connection};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_util::codec::LengthDelimitedCodec;
use tokio_util::io::StreamReader;
use tracing::{debug, error, trace, warn};

use crate::agent::{handle_broadcast, process_single_version, PayloadKind, SyncRecvError};
use crate::api::http::{ChunkedChanges, MAX_CHANGES_PER_MESSAGE};

use corro_types::{
    actor::ActorId,
    agent::{Booked, Bookie},
};

#[allow(clippy::too_many_arguments)]
pub async fn peer_api_v1_broadcast(
    headers: HeaderMap,
    Extension(agent): Extension<Agent>,
    Extension(bcast_msg_tx): Extension<Sender<Message>>,
    RawBody(mut body): RawBody,
) -> impl IntoResponse {
    match headers.get("corro-clock") {
        Some(value) => match serde_json::from_slice::<'_, uhlc::Timestamp>(value.as_bytes()) {
            Ok(ts) => {
                if let Err(e) = agent.clock().update_with_timestamp(&ts) {
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
            Some(Ok(mut bytes)) => {
                match kind.get_or_insert_with(|| {
                    PayloadKind::from_repr(bytes.get_u8()).map(|kind| {
                        increment_counter!("corro.payload.recv.count", "kind" => kind.to_string());
                        kind
                    })
                }) {
                    None => {
                        return StatusCode::UNPROCESSABLE_ENTITY;
                    }
                    // I'm not sure this can ever happen, I doubt it...
                    Some(PayloadKind::Swim) => {
                        return StatusCode::BAD_REQUEST;
                    }
                    Some(PayloadKind::Broadcast | PayloadKind::PriorityBroadcast) => {
                        buf.extend_from_slice(&bytes);

                        if let Err(e) = handle_broadcast(
                            &mut codec,
                            &mut buf,
                            agent.actor_id(),
                            agent.bookie(),
                            &bcast_msg_tx,
                        )
                        .await
                        {
                            error!("could not handle broadcast from HTTP: {e}");
                        }
                    }
                }
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
    Send(#[from] SyncSendError),
    #[error(transparent)]
    Recv(#[from] SyncRecvError),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncSendError {
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

    #[error("could not encode message: {0}")]
    Encode(#[from] SyncMessageEncodeError),

    #[error("sync send channel is closed")]
    ChannelClosed,
}

#[derive(Debug, thiserror::Error)]
#[error("unknown accept header value '{0}'")]
pub struct UnknownAcceptHeaderValue(String);

fn process_range(
    booked: &Booked,
    conn: &Connection,
    range: &RangeInclusive<i64>,
    actor_id: ActorId,
    is_local: bool,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    let (start, end) = (range.start(), range.end());
    trace!("processing range {start}..={end} for {}", actor_id);

    let overlapping: Vec<(_, KnownDbVersion)> = {
        booked
            .read()
            .overlapping(range)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    };

    for (versions, known_version) in overlapping {
        block_in_place(|| {
            if let KnownDbVersion::Cleared = &known_version {
                sender.blocking_send(SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    changeset: Changeset::Empty { versions },
                })))?;
                return Ok(());
            }

            for version in versions {
                process_version(
                    &conn,
                    actor_id,
                    is_local,
                    version,
                    &known_version,
                    vec![],
                    &sender,
                )?;
            }
            Ok::<_, eyre::Report>(())
        })?;
    }

    Ok(())
}

fn process_version(
    conn: &Connection,
    actor_id: ActorId,
    is_local: bool,
    version: i64,
    known_version: &KnownDbVersion,
    mut seqs_needed: Vec<RangeInclusive<i64>>,
    sender: &Sender<SyncMessage>,
) -> eyre::Result<()> {
    match known_version {
        KnownDbVersion::Current {
            db_version,
            last_seq,
            ts,
        } => {
            let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()), cl FROM crsql_changes WHERE site_id IS ? AND db_version = ? ORDER BY seq ASC"#)?;
            let site_id: Option<[u8; 16]> = (!is_local)
                .then_some(actor_id)
                .map(|actor_id| actor_id.to_bytes());

            let rows = prepped.query_map(params![site_id, db_version], row_to_change)?;

            let mut chunked = ChunkedChanges::new(rows, 0, *last_seq, MAX_CHANGES_PER_MESSAGE);
            while let Some(changes_seqs) = chunked.next() {
                match changes_seqs {
                    Ok((changes, seqs)) => {
                        if let Err(_) = sender.blocking_send(SyncMessage::V1(
                            SyncMessageV1::Changeset(ChangeV1 {
                                actor_id,
                                changeset: Changeset::Full {
                                    version,
                                    changes,
                                    seqs,
                                    last_seq: *last_seq,
                                    ts: *ts,
                                },
                            }),
                        )) {
                            eyre::bail!("sync message sender channel is closed");
                        }
                    }
                    Err(e) => {
                        error!("could not process crsql change (db_version: {db_version}) for broadcast: {e}");
                        break;
                    }
                }
            }
        }
        KnownDbVersion::Partial { seqs, last_seq, ts } => {
            debug!("seqs needed: {seqs_needed:?}");
            debug!("seqs we got: {seqs:?}");
            if seqs_needed.is_empty() {
                seqs_needed = vec![(0..=*last_seq)];
            }

            for range_needed in seqs_needed {
                for range in seqs.overlapping(&range_needed) {
                    let start_seq = cmp::max(range.start(), range_needed.start());
                    debug!("start seq: {start_seq}");
                    let end_seq = cmp::min(range.end(), range_needed.end());
                    debug!("end seq: {end_seq}");

                    let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id, cl FROM __corro_buffered_changes WHERE site_id = ? AND version = ? AND seq >= ? AND seq <= ?"#)?;

                    let site_id: [u8; 16] = actor_id.to_bytes();

                    let rows = prepped
                        .query_map(params![site_id, version, start_seq, end_seq], row_to_change)?;

                    let mut chunked =
                        ChunkedChanges::new(rows, *start_seq, *end_seq, MAX_CHANGES_PER_MESSAGE);
                    while let Some(changes_seqs) = chunked.next() {
                        match changes_seqs {
                            Ok((changes, seqs)) => {
                                if let Err(_e) = sender.blocking_send(SyncMessage::V1(
                                    SyncMessageV1::Changeset(ChangeV1 {
                                        actor_id,
                                        changeset: Changeset::Full {
                                            version,
                                            changes,
                                            seqs,
                                            last_seq: *last_seq,
                                            ts: *ts,
                                        },
                                    }),
                                )) {
                                    eyre::bail!("sync message sender channel is closed");
                                }
                            }
                            Err(e) => {
                                error!("could not process buffered crsql change (version: {version}) for broadcast: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        }
        _ => {
            warn!("not supposed to happen");
        }
    }

    trace!("done processing version: {version} for actor_id: {actor_id}");

    Ok(())
}

async fn process_sync(
    local_actor_id: ActorId,
    pool: SplitPool,
    bookie: Bookie,
    sync_state: SyncStateV1,
    sender: Sender<SyncMessage>,
) -> eyre::Result<()> {
    let conn = pool.read().await?;

    let bookie: HashMap<ActorId, Booked> =
        { bookie.read().iter().map(|(k, v)| (*k, v.clone())).collect() };

    for (actor_id, booked) in bookie {
        if actor_id == sync_state.actor_id {
            trace!("skipping itself!");
            // don't send the requester's data
            continue;
        }

        let is_local = actor_id == local_actor_id;

        // 1. process needed versions
        if let Some(needed) = sync_state.need.get(&actor_id) {
            for range in needed {
                process_range(&booked, &conn, range, actor_id, is_local, &sender)?;
            }
        }

        // 2. process partial needs
        if let Some(partially_needed) = sync_state.partial_need.get(&actor_id) {
            for (version, seqs_needed) in partially_needed.iter() {
                let known = { booked.read().get(version).cloned() };
                if let Some(known) = known {
                    block_in_place(|| {
                        process_version(
                            &conn,
                            actor_id,
                            is_local,
                            *version,
                            &known,
                            seqs_needed.clone(),
                            &sender,
                        )
                    })?;
                }
            }
        }

        // 3. process newer-than-heads
        let their_last_version = sync_state.heads.get(&actor_id).copied().unwrap_or(0);
        let our_last_version = booked.last().unwrap_or(0);

        trace!("their last version: {their_last_version} vs ours: {our_last_version}");

        if their_last_version >= our_last_version {
            // nothing to teach the other node!
            continue;
        }

        process_range(
            &booked,
            &conn,
            &((their_last_version + 1)..=our_last_version),
            actor_id,
            is_local,
            &sender,
        )?;
    }

    debug!("done with sync process");

    Ok(())
}

pub async fn peer_api_v1_sync_post(
    headers: HeaderMap,
    Extension(agent): Extension<Agent>,
    RawBody(req_body): RawBody,
) -> impl IntoResponse {
    match headers.get("corro-clock") {
        Some(value) => match serde_json::from_slice::<'_, uhlc::Timestamp>(value.as_bytes()) {
            Ok(ts) => {
                if let Err(e) = agent.clock().update_with_timestamp(&ts) {
                    error!("clock drifted too much! {e}");
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

    let (sender, body) = hyper::body::Body::channel();

    let sync_state = generate_sync(agent.bookie(), agent.actor_id());

    tokio::spawn(async move {
        if let Err(e) = bidirectional_sync(&agent, sync_state, req_body, sender).await {
            error!("error bidirectionally syncing: {e}");
        }
    });

    hyper::Response::builder().body(body).unwrap()
}

// #[derive(Debug, Copy, Clone, Default)]
// pub struct SyncSummary {
//     send_count: usize,
//     recv_count: usize,
// }

pub async fn bidirectional_sync(
    agent: &Agent,
    sync_state: SyncStateV1,
    body_in: hyper::Body,
    mut body_out: hyper::body::Sender,
) -> Result<usize, SyncError> {
    let (tx, mut rx) = channel::<SyncMessage>(256);

    tokio::spawn(async move {
        let mut codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_codec();
        let mut buf = BytesMut::new();

        while let Some(msg) = rx.recv().await {
            if let Err(e) = msg.encode_w_codec(&mut codec, &mut buf) {
                error!("could not encode message, skipping: {e}");
                continue;
            }

            let buf_len = buf.len();

            match timeout(
                Duration::from_secs(5),
                body_out.send_data(buf.split().freeze()),
            )
            .await
            {
                Err(_) => {
                    error!("sending data timed out");
                    break;
                }
                // this only happens if the channel closes...
                Ok(Err(e)) => {
                    debug!("error sending data: {e}");
                    break;
                }
                Ok(_) => {}
            }

            counter!("corro.sync.chunk.sent.bytes", buf_len as u64);
        }
    });

    // send our sync state

    tx.send(SyncMessage::V1(SyncMessageV1::State(sync_state)))
        .await
        .map_err(|_| SyncSendError::ChannelClosed)?;

    let body = StreamReader::new(body_in.map_err(|e| {
        if let Some(io_error) = e
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
        {
            return io::Error::from(io_error.kind());
        }
        io::Error::new(io::ErrorKind::Other, e)
    }));

    let mut framed = LengthDelimitedCodec::builder()
        .length_field_type::<u32>()
        .new_read(body)
        .inspect_ok(|b| counter!("corro.sync.chunk.recv.bytes", b.len() as u64));

    if let Some(buf_res) = framed.next().await {
        let mut buf = buf_res.map_err(SyncRecvError::from)?;
        let msg = SyncMessage::from_buf(&mut buf).map_err(SyncRecvError::from)?;
        if let SyncMessage::V1(SyncMessageV1::State(sync)) = msg {
            tokio::spawn(
                process_sync(
                    agent.actor_id(),
                    agent.pool().clone(),
                    agent.bookie().clone(),
                    sync,
                    tx,
                )
                .inspect_err(|e| error!("could not process sync request: {e}")),
            );
        } else {
            return Err(SyncRecvError::UnexpectedSyncMessage.into());
        }
    }

    let mut count = 0;

    while let Some(buf_res) = framed.next().await {
        let mut buf = buf_res.map_err(SyncRecvError::from)?;
        match SyncMessage::from_buf(&mut buf) {
            Ok(msg) => {
                let len = match msg {
                    SyncMessage::V1(SyncMessageV1::Changeset(change)) => {
                        let len = change.len();
                        process_single_version(agent, change)
                            .await
                            .map_err(SyncRecvError::from)?;
                        len
                    }
                    SyncMessage::V1(SyncMessageV1::State(_)) => {
                        warn!("received sync state message more than once, ignoring");
                        continue;
                    }
                };
                count += len;
            }
            Err(e) => return Err(SyncRecvError::from(e).into()),
        }
    }

    Ok(count)
}
