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
use corro_types::agent::Agent;
use futures::StreamExt;
use metrics::{counter, histogram};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio::time::timeout;
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, error, trace, warn};

use crate::agent::handle_payload;

use corro_types::{
    actor::ActorId,
    agent::{Booked, Bookie},
    broadcast::MessageV1,
    broadcast::{BroadcastSrc, FocaInput, Message},
    change::Change,
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

    let actors: Vec<(ActorId, Booked)> =
        { bookie.read().iter().map(|(k, v)| (*k, v.clone())).collect() };

    for (actor_id, booked) in actors {
        let last_version = match booked.last() {
            Some(v) => v,
            None => continue,
        };

        sync.need
            .insert(actor_id, booked.read().gaps(&(1..=last_version)).collect());

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

fn send_msg(
    actor_id: ActorId,
    version: i64,
    changeset: Vec<Change>,
    sender: &Sender<Message>,
    ts: u64,
) {
    let changeset_len = changeset.len();

    // build the Message
    let msg = Message::V1(MessageV1::Change {
        actor_id,
        version,
        changeset,
        ts,
    });

    trace!(
        "sending msg: actor: {}, version: {version}, changes len: {changeset_len}",
        actor_id,
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
    booked: &Booked,
    conn: &Connection,
    range: &RangeInclusive<i64>,
    actor_id: ActorId,
    is_local: bool,
    sender: &Sender<Message>,
) -> eyre::Result<()> {
    let (start, end) = (range.start(), range.end());
    trace!("processing range {start}..={end} for {}", actor_id);

    let overlapping: Vec<(i64, (Option<i64>, u64))> = {
        booked
            .read()
            .overlapping(range)
            .map(|(k, v)| (*k.end(), v.clone()))
            .collect()
    };

    for (version, (db_version, ts)) in overlapping {
        match db_version {
            Some(db_version) => block_in_place(|| {
                process_version(&conn, version, db_version, ts, actor_id, is_local, &sender)
            })?,
            None => {
                send_msg(actor_id, version, vec![], sender, ts);
            }
        }
    }

    Ok(())
}

fn process_version(
    conn: &Connection,
    version: i64,
    db_version: i64,
    ts: u64,
    actor_id: ActorId,
    is_local: bool,
    sender: &Sender<Message>,
) -> eyre::Result<()> {
    let mut prepped = conn.prepare_cached(
        r#"
            SELECT "table", pk, cid, val, col_version, db_version, COALESCE(site_id, crsql_siteid())
                FROM crsql_changes
                WHERE site_id IS ? AND db_version = ?
        "#,
    )?;

    trace!(
        "prepped query successfully, querying with {}, {}",
        actor_id,
        version
    );

    let site_id: Option<[u8; 16]> = if is_local {
        None
    } else {
        Some(actor_id.to_bytes())
    };

    let changeset: Vec<Change> = prepped
        .query_map(params![site_id, db_version], |row| {
            Ok(Change {
                table: row.get(0)?,
                pk: row.get(1)?,
                cid: row.get(2)?,
                val: row.get(3)?,
                col_version: row.get(4)?,
                db_version: row.get(5)?,
                site_id: row.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<Change>>>()?;

    send_msg(actor_id, version, changeset, &sender, ts);

    trace!("done processing db version: {db_version}");

    Ok(())
}

async fn process_sync(
    local_actor_id: ActorId,
    pool: &SqlitePool,
    bookie: &Bookie,
    sync: SyncMessage,
    sender: Sender<Message>,
) -> eyre::Result<()> {
    let conn = pool.get().await?;

    let bookie: HashMap<ActorId, Booked> =
        { bookie.read().iter().map(|(k, v)| (*k, v.clone())).collect() };

    for (actor_id, booked) in bookie {
        if actor_id == sync.actor_id {
            trace!("skipping itself!");
            // don't send the requester's data
            continue;
        }

        let is_local = actor_id == local_actor_id;

        // 1. process needed versions
        if let Some(needed) = sync.need.get(&actor_id) {
            for range in needed {
                process_range(&booked, &conn, range, actor_id, is_local, &sender)?;
            }
        }

        // 2. process newer-than-heads
        let their_last_version = sync.heads.get(&actor_id).copied().unwrap_or(0);
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

            counter!("corro.sync.server.chunk.sent.bytes", buf_len as u64);
        }
    });

    tokio::spawn(async move {
        let bytes = hyper::body::to_bytes(req_body).await?;

        let sync = serde_json::from_slice(bytes.as_ref())?;

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
