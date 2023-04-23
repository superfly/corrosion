use std::cmp;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::time::{Duration, Instant};
use std::{net::SocketAddr, sync::Arc};

use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::{
    extract::{ConnectInfo, RawBody},
    Extension,
};
use bytes::BytesMut;
use futures::StreamExt;
use metrics::{counter, histogram};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, error, trace, warn};

use crate::broadcast::MessageV1;
use crate::sqlite::SqlitePool;
use crate::types::change::Change;
use crate::{
    actor::ActorId,
    broadcast::{BroadcastSrc, FocaInput, Message},
    handle_payload,
};
use crate::{Booked, Bookie};

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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SyncMessage {
    pub actor_id: ActorId,
    pub heads: HashMap<ActorId, i64>,
    pub need: HashMap<ActorId, Vec<RangeInclusive<i64>>>,
}

impl SyncMessage {
    pub fn need_len(&self) -> i64 {
        self.need
            .values()
            .flat_map(|v| v.iter().map(|range| (range.end() - range.start()) + 1))
            .sum()
    }

    pub fn need_len_for_actor(&self, actor_id: &ActorId) -> i64 {
        self.need
            .get(actor_id)
            .map(|v| {
                v.iter()
                    .map(|range| (range.end() - range.start()) + 1)
                    .sum()
            })
            .unwrap_or(0)
    }
}

// generates a `SyncMessage` to tell another node what versions we're missing
pub fn generate_sync(bookie: &Bookie, actor_id: ActorId) -> SyncMessage {
    let mut sync = SyncMessage {
        actor_id,
        heads: Default::default(),
        need: Default::default(),
    };

    let actors: Vec<(ActorId, Booked)> = {
        bookie
            .0
            .read()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    };

    for (actor_id, booked) in actors {
        let last_version = match booked.last() {
            Some(v) => v,
            None => continue,
        };

        sync.need.insert(
            actor_id,
            booked.0.read().gaps(&(0..=last_version)).collect(),
        );

        sync.heads.insert(actor_id, last_version);
    }

    sync
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

const MAX_CHANGES: i32 = 20;

fn send_msg(
    actor_id: ActorId,
    start: i64,
    end: i64,
    changeset: &mut Vec<Change>,
    sender: &Sender<Message>,
) {
    let changeset_len = changeset.len();

    // build the Message
    let msg = Message::V1(MessageV1::Change {
        actor_id,
        start_version: start,
        end_version: end,
        changeset: changeset.drain(..).collect(),
    });

    trace!(
        "sending msg: actor: {}, start: {start}, end: {end}, changes len: {changeset_len}",
        actor_id.hyphenated(),
    );

    // async send the data... not ideal but it'll have to do
    // TODO: figure out a better way to backpressure here, maybe drop this range?
    tokio::spawn({
        let sender = sender.clone();
        async move {
            if let Err(e) = sender.send(msg).await {
                println!("could not send change: {e}");
            }
        }
    });
}

fn process_range(
    conn: &Connection,
    actor_id: ActorId,
    mut start: i64,
    end: i64,
    sender: &Sender<Message>,
) -> eyre::Result<()> {
    trace!(
        "processing range {start}..={end} for {}",
        actor_id.hyphenated()
    );

    let original_start = start;

    let mut prepped = conn.prepare_cached(
        r#"
            SELECT tbl_name, pk, cid, val, col_version, db_version, site_id
                FROM __corro_changes
                WHERE site_id = ? AND db_version >= ? AND db_version <= ?
                ORDER BY db_version ASC
        "#,
    )?;

    trace!(
        "prepped query successfully, querying with {}, {}",
        actor_id.hyphenated(),
        original_start
    );

    let mut rows = prepped.query_map(params![actor_id.0, original_start, end], |row| {
        Ok(Change {
            table: row.get(0)?,
            pk: row.get(1)?,
            cid: row.get(2)?,
            val: row.get(3)?,
            col_version: row.get(4)?,
            db_version: row.get(5)?,
            site_id: row.get(6)?,
        })
    })?;

    let mut last_version = start;
    let mut count = MAX_CHANGES;

    let mut changeset = vec![];

    while let Some(row) = rows.next() {
        // trace!("processing a row {row:?}");

        let change = row?;

        if change.db_version > end {
            warn!("went overboard, that shouldn't happen!");
            break;
        }

        // we don't want to send much more than this!
        if count <= 0 {
            trace!(
                "count is under 0: {count}, last_version: {last_version}, change.db_version: {}",
                change.db_version
            );
            // ... but we wanna send all the data between versions, that's for sure.
            if change.db_version > last_version {
                send_msg(actor_id, start, last_version, &mut changeset, &sender);

                // this is the new start value
                start = last_version + 1;
                // reset the count
                count = MAX_CHANGES;
            }
        }

        // current last version we've seen
        last_version = change.db_version;

        // put that change in the changeset for the next msg send
        changeset.push(change);

        // decrement count
        count -= 1;
    }

    // no more messages!

    trace!("(after loop) sending pending changes");

    send_msg(actor_id, start, end, &mut changeset, &sender);

    trace!("done processing original range: {original_start}..={end}");

    Ok(())
}

async fn process_sync(
    pool: SqlitePool,
    bookie: Bookie,
    sync: SyncMessage,
    sender: Sender<Message>,
) -> eyre::Result<()> {
    let conn = pool.get().await?;

    let bookie: HashMap<ActorId, Booked> =
        { bookie.read().iter().map(|(k, v)| (*k, v.clone())).collect() };

    for (actor_id, booked) in bookie {
        if let Some(needed) = sync.need.get(&actor_id) {
            for range in needed {
                let overlapping: Vec<RangeInclusive<i64>> =
                    { booked.read().overlapping(&range).cloned().collect() };

                for overlap in overlapping {
                    block_in_place(|| {
                        process_range(
                            &conn,
                            actor_id,
                            *overlap.start(),
                            *cmp::max(overlap.end(), range.end()),
                            &sender,
                        )
                    })?;
                }
            }
        }

        if actor_id == sync.actor_id {
            trace!("skipping itself!");
            // don't send the requester's data
            continue;
        }

        let their_last_version = sync.heads.get(&actor_id).copied().unwrap_or(0);
        let our_last_version = booked.last().unwrap_or(0);

        trace!("their last version: {their_last_version} vs ours: {our_last_version}");

        if their_last_version >= our_last_version {
            // nothing to teach the other node!
            continue;
        }
        block_in_place(|| {
            process_range(
                &conn,
                actor_id,
                their_last_version,
                our_last_version,
                &sender,
            )
        })?;
    }

    debug!("done with sync process");

    Ok(())
}

pub async fn peer_api_v1_sync_post(
    headers: HeaderMap,
    Extension(pool): Extension<SqlitePool>,
    Extension(bookie): Extension<Bookie>,
    Extension(clock): Extension<Arc<uhlc::HLC>>,
    RawBody(req_body): RawBody,
) -> impl IntoResponse {
    match headers.get("corro-clock") {
        Some(value) => match serde_json::from_slice::<'_, uhlc::Timestamp>(value.as_bytes()) {
            Ok(ts) => {
                if let Err(e) = clock.update_with_timestamp(&ts) {
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
    let (tx, mut rx) = channel::<Message>(256);

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

            counter!("corrosion.sync.server.chunk.sent.bytes", buf_len as u64);
        }
    });

    tokio::spawn(async move {
        let bytes = hyper::body::to_bytes(req_body).await?;

        let sync = serde_json::from_slice(bytes.as_ref())?;

        let now = Instant::now();
        match process_sync(pool, bookie, sync, tx).await {
            Ok(_) => {
                histogram!(
                    "corrosion.sync.server.process.time.seconds",
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
