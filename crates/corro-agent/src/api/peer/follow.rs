use std::{collections::HashMap, io, time::Duration};

use bytes::{BufMut, BytesMut};
use corro_types::{
    actor::ActorId,
    agent::Agent,
    api::row_to_change,
    base::{CrsqlDbVersion, CrsqlSeq, Version},
    broadcast::{BiPayload, ChangeSource, ChangeV1, Changeset, Timestamp},
    change::ChunkedChanges,
    config::FollowBroadcast,
    sqlite::SqlitePoolError,
};
use futures::{Stream, StreamExt};
use metrics::counter;
use quinn::{RecvStream, SendStream};
use rand::{rngs::OsRng, Rng};
use rusqlite::{params_from_iter, OptionalExtension, Row, ToSql};
use speedy::{Readable, Writable};
use tokio::{sync::mpsc, task::block_in_place};
use tokio_util::codec::{Encoder, FramedRead, LengthDelimitedCodec};
use tracing::{debug, error, trace};

use super::{encode_write_bipayload_msg, BiPayloadSendError};

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum FollowMessage {
    V1(FollowMessageV1),
}

impl FollowMessage {
    pub fn from_slice<S: AsRef<[u8]>>(slice: S) -> Result<Self, speedy::Error> {
        Self::read_from_buffer(slice.as_ref())
    }

    pub fn from_buf(buf: &mut BytesMut) -> Result<Self, FollowMessageDecodeError> {
        Ok(Self::from_slice(buf)?)
    }
}

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum FollowMessageV1 {
    Change(ChangeV1),
}

#[derive(Debug, thiserror::Error)]
pub enum FollowError {
    #[error(transparent)]
    SqlitePool(#[from] SqlitePoolError),
    #[error(transparent)]
    Encode(#[from] FollowMessageEncodeError),
    #[error(transparent)]
    Decode(#[from] FollowMessageDecodeError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Write(#[from] quinn::WriteError),
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
    #[error("follow send channel is closed")]
    ChannelClosed,
    #[error(transparent)]
    BiPayloadSend(#[from] BiPayloadSendError),
}

#[derive(Debug, thiserror::Error)]
pub enum FollowMessageEncodeError {
    #[error(transparent)]
    Encode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum FollowMessageDecodeError {
    #[error(transparent)]
    Decode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

async fn encode_write_follow_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: FollowMessage,
    write: &mut SendStream,
) -> Result<(), FollowError> {
    encode_follow_msg(codec, encode_buf, send_buf, msg)?;

    write_buf(send_buf, write).await
}

fn encode_follow_msg(
    codec: &mut LengthDelimitedCodec,
    encode_buf: &mut BytesMut,
    send_buf: &mut BytesMut,
    msg: FollowMessage,
) -> Result<(), FollowError> {
    msg.write_to_stream(encode_buf.writer())
        .map_err(FollowMessageEncodeError::from)?;

    let data = encode_buf.split().freeze();
    trace!("encoded sync message, len: {}", data.len());
    codec.encode(data, send_buf)?;
    Ok(())
}

async fn write_buf(send_buf: &mut BytesMut, write: &mut SendStream) -> Result<(), FollowError> {
    let len = send_buf.len();
    write.write_chunk(send_buf.split().freeze()).await?;
    counter!("corro.follow.chunk.sent.bytes").increment(len as u64);

    Ok(())
}

pub async fn serve_follow(
    agent: &Agent,
    from: Option<CrsqlDbVersion>,
    local_only: bool,
    mut write: SendStream,
) -> Result<(), FollowError> {
    let mut last_db_version = {
        if let Some(db_version) = from {
            db_version
        } else {
            let conn = agent.pool().read().await?;
            conn.query_row("SELECT crsql_db_version()", [], |row| row.get(0))?
        }
    };

    // channel provides backpressure
    let (tx, mut rx) = mpsc::channel(128);

    tokio::spawn(async move {
        let mut codec = LengthDelimitedCodec::builder()
            .max_frame_length(100 * 1_024 * 1_024)
            .new_codec();
        let mut send_buf = BytesMut::new();
        let mut encode_buf = BytesMut::new();

        while let Some(msg) = rx.recv().await {
            encode_write_follow_msg(&mut codec, &mut encode_buf, &mut send_buf, msg, &mut write)
                .await?;
        }

        Ok::<_, FollowError>(())
    });

    let actor_id = agent.actor_id();
    let from_ts: Timestamp = {
        let conn = agent.pool().read().await?;
        conn.query_row(
            "SELECT MIN(ts) FROM __corro_bookkeeping WHERE db_version >= ? and (? or actor_id = ?)",
            (last_db_version, !local_only, actor_id),
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(Timestamp::from(agent.clock().new_timestamp()))
    };

    debug!("sending cleared version since from - {from_ts}");
    let mut last_empty_ts: HashMap<ActorId, Timestamp> = HashMap::new();
    loop {
        let conn = agent.pool().read().await?;

        block_in_place(|| {
            let (extra_where_clause, query_params): (_, Vec<&dyn ToSql>) = if local_only {
                ("AND actor_id = ?", vec![&last_db_version, &actor_id])
            } else {
                ("", vec![&last_db_version])
            };

            let mut bk_prepped = conn.prepare_cached(&format!("SELECT actor_id, start_version, db_version, last_seq, ts FROM __corro_bookkeeping WHERE db_version IS NOT NULL AND db_version > ? {extra_where_clause} ORDER BY db_version ASC"))?;

            let map = |row: &Row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            };

            // implicit read transaction
            let bk_rows = bk_prepped.query_map(params_from_iter(query_params), map)?;

            for bk_res in bk_rows {
                let (actor_id, version, db_version, last_seq, ts): (
                    ActorId,
                    Version,
                    CrsqlDbVersion,
                    CrsqlSeq,
                    Timestamp,
                ) = bk_res?;

                debug!("sending changes for: {actor_id} v{version} (db_version: {db_version})");

                let mut prepped = conn.prepare_cached(
                    "SELECT \"table\", pk, cid, val, col_version, db_version, seq, site_id, cl FROM crsql_changes WHERE db_version = ? ORDER BY db_version ASC, seq ASC",
                )?;
                // implicit read transaction
                let rows = prepped.query_map([db_version], row_to_change)?;

                let chunked = ChunkedChanges::new(rows, CrsqlSeq(0), last_seq, 8192);

                for changes_seqs in chunked {
                    let (changes, seqs) = changes_seqs?;
                    tx.blocking_send(FollowMessage::V1(FollowMessageV1::Change(ChangeV1 {
                        actor_id,
                        changeset: Changeset::Full {
                            version,
                            changes,
                            seqs,
                            last_seq,
                            ts,
                        },
                    })))
                    .map_err(|_| FollowError::ChannelClosed)?;
                }

                last_db_version = db_version; // record last db version processed for next go around
            }

            // we do this everytime so we can pick up new actor_ids
            let actor_ids = {
                if local_only {
                    vec![actor_id]
                } else {
                    conn.prepare_cached("SELECT DISTINCT actor_id FROM __corro_bookkeeping")?
                        .query_map([], |row| Ok(row.get(0)?))
                        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())?
                }
            };

            info!("sending cleared version since from - {from_ts} for {} actors", last_empty_ts.len());
            for id in actor_ids {
                if !last_empty_ts.contains_key(&id) {
                    last_empty_ts.insert(actor_id, from_ts);
                }
            }

            for (actor_id, empty_ts) in last_empty_ts.clone() {
                let mut empty_prepped = conn.prepare_cached(
                    "SELECT start_version, end_version, ts FROM __corro_bookkeeping WHERE db_version IS NULL AND ts > ? AND actor_id = ?  ORDER BY ts ASC",
                )?;

                let empty_rows = empty_prepped.query_map((empty_ts, actor_id), |row| {
                    Ok(Changeset::Empty {
                        versions: row.get(0)?..=row.get(1)?,
                        ts: row.get(2)?,
                    })
                })?;

                let mut last_ts: Option<Timestamp> = None;
                for row in empty_rows {
                    let changeset = row?;
                    last_ts = changeset.ts();
                    debug!("sending cleared versions for {actor_id}, versions - {:?}", changeset.versions());
                    tx.blocking_send(FollowMessage::V1(FollowMessageV1::Change(ChangeV1 {
                        actor_id,
                        changeset,
                    })))
                    .map_err(|_| FollowError::ChannelClosed)?;
                }

                if let Some(ts) = last_ts {
                    last_empty_ts.insert(actor_id, ts);
                }
            }
            Ok::<_, FollowError>(())
        })?;

        // prevents hot-looping
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

pub async fn read_follow_msg<R: Stream<Item = std::io::Result<BytesMut>> + Unpin>(
    read: &mut R,
) -> Result<Option<FollowMessage>, FollowError> {
    match read.next().await {
        Some(buf_res) => match buf_res {
            Ok(mut buf) => {
                counter!("corro.follow.chunk.recv.bytes").increment(buf.len() as u64);
                match FollowMessage::from_buf(&mut buf) {
                    Ok(msg) => Ok(Some(msg)),
                    Err(e) => Err(FollowError::from(e)),
                }
            }
            Err(e) => Err(FollowError::from(e)),
        },
        None => Ok(None),
    }
}

pub async fn recv_follow(
    agent: &Agent,
    mut read: FramedRead<RecvStream, LengthDelimitedCodec>,
    local_only: bool,
    broadcast: Option<&FollowBroadcast>,
) -> Result<Option<CrsqlDbVersion>, FollowError> {
    let mut last_db_version = None;
    let tx_changes = agent.tx_changes();
    loop {
        match read_follow_msg(&mut read).await {
            Ok(None) => break,
            Err(e) => {
                error!("could not receive follow message: {e}");
                break;
            }
            Ok(Some(msg)) => match msg {
                FollowMessage::V1(FollowMessageV1::Change(changeset)) => {
                    let db_version = changeset.changes().first().map(|change| change.db_version);
                    debug!(
                        "received changeset for version(s) {:?} and db_version {db_version:?}",
                        changeset.versions()
                    );
                    let change_src = if local_only
                        || broadcast
                            .map(|bcast| should_broadcast(&changeset.actor_id, bcast))
                            .unwrap_or(false)
                    {
                        ChangeSource::Broadcast
                    } else {
                        ChangeSource::Follow
                    };
                    tx_changes
                        .send((changeset, change_src))
                        .await
                        .map_err(|_| FollowError::ChannelClosed)?;
                    if let Some(db_version) = db_version {
                        last_db_version = Some(db_version);
                    }
                }
            },
        }
    }

    Ok(last_db_version)
}

fn should_broadcast(actor_id: &ActorId, broadcast: &FollowBroadcast) -> bool {
    match broadcast {
        FollowBroadcast::ActorIds(set) => set.contains(actor_id),
        FollowBroadcast::Percent(percent) => OsRng.gen_range(0..100) < *percent,
    }
}

pub async fn follow(
    agent: &Agent,
    mut tx: SendStream,
    recv: RecvStream,
    from: Option<CrsqlDbVersion>,
    local_only: bool,
    broadcast: Option<&FollowBroadcast>,
) -> Result<Option<CrsqlDbVersion>, FollowError> {
    let mut codec = LengthDelimitedCodec::builder()
        .max_frame_length(100 * 1_024 * 1_024)
        .new_codec();
    let mut encoding_buf = BytesMut::new();
    let mut buf = BytesMut::new();

    encode_write_bipayload_msg(
        &mut codec,
        &mut encoding_buf,
        &mut buf,
        BiPayload::V1 {
            data: corro_types::broadcast::BiPayloadV1::Follow { from, local_only },
            cluster_id: agent.cluster_id(),
        },
        &mut tx,
    )
    .await?;

    let framed = FramedRead::new(
        recv,
        LengthDelimitedCodec::builder()
            .max_frame_length(100 * 1_024 * 1_024)
            .new_codec(),
    );

    recv_follow(agent, framed, local_only, broadcast).await
}

#[cfg(test)]
mod tests {
    use corro_types::config::FollowFrom;
    use corro_tests::launch_test_agent;
    use axum::Extension;
    use hyper::StatusCode;
    use tripwire::Tripwire;
    
    use crate::{
        api::{
            public::{api_v1_db_schema, api_v1_transactions},
        },
    };
    use corro_tests::*;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_follow() -> eyre::Result<()> {

        _ = tracing_subscriber::fmt::try_init();

        let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
        let main = launch_test_agent(|conf| conf.build(), tripwire.clone()).await?;
        let follower = launch_test_agent(|conf| conf.follow(main.agent.gossip_addr(), FollowFrom::Latest, None).build(), tripwire.clone()).await?;
    
        // setup the schema, for both nodes
        let (status_code, _body) = api_v1_db_schema(
            Extension(main.agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;
    
        assert_eq!(status_code, StatusCode::OK);
    
        let (status_code, _body) = api_v1_db_schema(
            Extension(follower.agent.clone()),
            axum::Json(vec![corro_tests::TEST_SCHEMA.into()]),
        )
        .await;
        assert_eq!(status_code, StatusCode::OK);
    
        // make about 50 transactions to ta1
        insert_rows(follower.agent.clone(), 1, 50).await;

        Ok(())
    }
}
