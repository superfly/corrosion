use std::cmp;
use std::collections::HashMap;
use std::ops::RangeInclusive;

use bytes::{BufMut, BytesMut};
use corro_types::agent::{Agent, KnownDbVersion, SplitPool};
use corro_types::broadcast::{ChangeV1, Changeset};
use corro_types::change::row_to_change;
use corro_types::sync::{SyncMessage, SyncMessageEncodeError, SyncMessageV1, SyncStateV1};
use futures::{SinkExt, StreamExt, TryFutureExt};
use metrics::counter;
use quinn::{RecvStream, SendStream};
use rusqlite::{params, Connection};
use speedy::Writable;
use tokio::sync::mpsc::{channel, Sender};
use tokio::task::block_in_place;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, trace, warn};

use crate::agent::{process_single_version, SyncRecvError};
use crate::api::http::{ChunkedChanges, MAX_CHANGES_PER_MESSAGE};

use corro_types::{
    actor::ActorId,
    agent::{Booked, Bookie},
};

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
    Io(#[from] std::io::Error),

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
            let mut prepped = conn.prepare_cached(r#"SELECT "table", pk, cid, val, col_version, db_version, seq, COALESCE(site_id, crsql_site_id()), cl FROM crsql_changes WHERE site_id IS ? AND db_version = ? ORDER BY seq ASC"#)?;
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
        trace!(actor_id = %local_actor_id, "processing sync for {actor_id}, last: {:?}", booked.last());

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

        debug!(actor_id = %local_actor_id, "their last version: {their_last_version} vs ours: {our_last_version}");

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

    debug!("done processing sync state");

    Ok(())
}

pub async fn bidirectional_sync(
    agent: &Agent,
    our_sync_state: SyncStateV1,
    their_sync_state: Option<SyncStateV1>,
    read: RecvStream,
    write: SendStream,
) -> Result<usize, SyncError> {
    let (tx, mut rx) = channel::<SyncMessage>(256);

    let mut read = FramedRead::new(read, LengthDelimitedCodec::new());
    let mut write = FramedWrite::new(write, LengthDelimitedCodec::new());

    tx.send(SyncMessage::V1(SyncMessageV1::State(our_sync_state)))
        .await
        .map_err(|_| SyncSendError::ChannelClosed)?;

    let (_sent_count, recv_count) = tokio::try_join!(
        async move {
            let mut count = 0;
            let mut buf = BytesMut::new();
            while let Some(msg) = rx.recv().await {
                msg.write_to_stream((&mut buf).writer())
                    .map_err(SyncMessageEncodeError::from)
                    .map_err(SyncSendError::from)?;

                let buf_len = buf.len();
                write
                    .send(buf.split().freeze())
                    .await
                    .map_err(SyncSendError::from)?;

                count += 1;

                counter!("corro.sync.chunk.sent.bytes", buf_len as u64);
            }
            debug!(actor_id = %agent.actor_id(), "done writing sync messages (count: {count})");

            Ok::<_, SyncError>(count)
        },
        async move {
            let their_sync_state = match their_sync_state {
                Some(state) => state,
                None => {
                    if let Some(buf_res) = read.next().await {
                        let mut buf = buf_res.map_err(SyncRecvError::from)?;
                        let msg = SyncMessage::from_buf(&mut buf).map_err(SyncRecvError::from)?;
                        if let SyncMessage::V1(SyncMessageV1::State(state)) = msg {
                            state
                        } else {
                            return Err(SyncRecvError::UnexpectedSyncMessage.into());
                        }
                    } else {
                        return Err(SyncRecvError::UnexpectedEndOfStream.into());
                    }
                }
            };

            tokio::spawn(
                process_sync(
                    agent.actor_id(),
                    agent.pool().clone(),
                    agent.bookie().clone(),
                    their_sync_state,
                    tx,
                )
                .inspect_err(|e| error!("could not process sync request: {e}")),
            );

            let mut count = 0;

            while let Some(buf_res) = read.next().await {
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
            debug!(actor_id = %agent.actor_id(), "done reading sync messages");

            Ok(count)
        }
    )?;

    Ok(recv_count)
}
