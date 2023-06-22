use std::cmp;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{
    extract::{ConnectInfo, RawBody},
    Extension,
};
use bytes::BytesMut;
use corro_types::agent::{Agent, KnownDbVersion};
use corro_types::broadcast::{ChangeV1, Changeset};
use corro_types::sync::{SyncMessage, SyncMessageV1};
use futures::StreamExt;
use metrics::{counter, histogram};
use rusqlite::{params, Connection};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, error, trace, warn};

use crate::agent::handle_payload;
use crate::api::http::{row_to_change, ChunkedChanges, MAX_CHANGES_PER_MESSAGE};

use corro_types::{
    actor::ActorId,
    agent::{Booked, Bookie},
    broadcast::{BroadcastSrc, FocaInput, Message},
    sqlite::SqlitePool,
};

#[allow(clippy::too_many_arguments)]
pub async fn peer_api_v1_broadcast(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Extension(agent): Extension<Agent>,
    Extension(gossip_msg_tx): Extension<Sender<Message>>,
    Extension(foca_tx): Extension<Sender<FocaInput>>,
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
            Some(Ok(bytes)) => {
                kind = Some(
                    match handle_payload(
                        kind,
                        BroadcastSrc::Http(addr),
                        &mut buf,
                        &mut codec,
                        bytes,
                        &foca_tx,
                        agent.actor_id(),
                        agent.bookie(),
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

fn send_msg(msg: SyncMessage, sender: &Sender<SyncMessage>) {
    trace!("sending sync msg: {msg:?}");

    // async send the data... not ideal but it'll have to do
    // TODO: figure out a better way to backpressure here, maybe drop this range?
    tokio::spawn({
        let sender = sender.clone();
        async move {
            if let Err(e) = sender.send(msg).await {
                error!("could not send sync message: {e}");
            }
        }
    });
}

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
            let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_siteid()) FROM crsql_changes WHERE site_id IS ? AND db_version = ? ORDER BY seq ASC"#)?;
            let site_id: Option<[u8; 16]> = (!is_local)
                .then_some(actor_id)
                .map(|actor_id| actor_id.to_bytes());

            let rows = prepped.query_map(params![site_id, db_version], row_to_change)?;

            let mut chunked = ChunkedChanges::new(rows, 0, *last_seq, MAX_CHANGES_PER_MESSAGE);
            while let Some(changes_seqs) = chunked.next() {
                match changes_seqs {
                    Ok((changes, seqs)) => {
                        // if changes.len() < 50 {
                        //     debug!(
                        //         "CURRENT CHANGE SENT SMALLER 50 changes batch ({}) for seqs: {seqs:?}!",
                        //         changes.len()
                        //     );
                        // }

                        send_msg(
                            SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                                actor_id,
                                version,
                                changeset: Changeset::Full {
                                    changes,
                                    seqs,
                                    last_seq: *last_seq,
                                    ts: *ts,
                                },
                            })),
                            &sender,
                        );
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

                    let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, site_id FROM __corro_buffered_changes WHERE site_id = ? AND version = ? AND seq >= ? AND seq <= ?"#)?;

                    let site_id: [u8; 16] = actor_id.to_bytes();

                    let rows = prepped
                        .query_map(params![site_id, version, start_seq, end_seq], row_to_change)?;

                    let mut chunked =
                        ChunkedChanges::new(rows, *start_seq, *end_seq, MAX_CHANGES_PER_MESSAGE);
                    while let Some(changes_seqs) = chunked.next() {
                        match changes_seqs {
                            Ok((changes, seqs)) => {
                                // if changes.len() < 50 {
                                //     debug!(
                                //         "PARTIAL SENT SMALLER THAN 50 changes batch ({}) for seqs: {seqs:?}!", changes.len()
                                //     );
                                // }
                                send_msg(
                                    SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                                        actor_id,
                                        version,
                                        changeset: Changeset::Full {
                                            changes,
                                            seqs,
                                            last_seq: *last_seq,
                                            ts: *ts,
                                        },
                                    })),
                                    &sender,
                                );
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
        KnownDbVersion::Cleared => {
            send_msg(
                SyncMessage::V1(SyncMessageV1::Changeset(ChangeV1 {
                    actor_id,
                    version,
                    changeset: Changeset::Empty,
                })),
                sender,
            );
        }
    }

    trace!("done processing version: {version} for actor_id: {actor_id}");

    Ok(())
}

async fn process_sync(
    local_actor_id: ActorId,
    pool: &SqlitePool,
    bookie: &Bookie,
    sync: SyncMessage,
    sender: Sender<SyncMessage>,
) -> eyre::Result<()> {
    let sync_state = match sync.state() {
        Some(state) => state,
        None => eyre::bail!("unexpected sync message: {sync:?}"),
    };

    let conn = pool.get().await?;

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

    let (mut sender, body) = hyper::body::Body::channel();
    let (tx, mut rx) = channel::<SyncMessage>(256);

    tokio::spawn(async move {
        let mut buf = BytesMut::new();

        while let Some(msg) = rx.recv().await {
            if let Err(e) = msg.encode(&mut buf) {
                error!("could not encode message, skipping: {e}");
                continue;
            }

            let buf_len = buf.len();

            match timeout(
                Duration::from_secs(5),
                sender.send_data(buf.split().freeze()),
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

            counter!("corro.sync.server.chunk.sent.bytes", buf_len as u64);
        }
    });

    tokio::spawn(async move {
        let bytes = hyper::body::to_bytes(req_body).await?;

        let mut buf = BytesMut::with_capacity(bytes.len());
        buf.extend_from_slice(&bytes);

        let mut codec = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_codec();
        let sync = SyncMessage::decode(&mut codec, &mut buf)?;

        let sync = match sync {
            Some(msg) => msg,
            None => {
                error!("no sync message to decode");
                return Ok(());
            }
        };

        let now = Instant::now();
        match process_sync(
            agent.actor_id(),
            agent.read_only_pool(),
            agent.bookie(),
            sync,
            tx,
        )
        .await
        {
            Ok(_) => {
                histogram!(
                    "corro.sync.server.process.time.seconds",
                    now.elapsed().as_secs_f64()
                );
            }
            Err(e) => {
                error!("could not process sync request: {e}");
            }
        }

        Ok::<_, eyre::Report>(())
    });

    hyper::Response::builder()
        .header(hyper::header::CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}
