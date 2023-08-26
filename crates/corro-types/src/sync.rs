use std::{collections::HashMap, io, ops::RangeInclusive};

use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use tokio_util::codec::{Decoder, LengthDelimitedCodec};

use crate::{
    actor::ActorId,
    agent::{Booked, Bookie, KnownDbVersion},
    broadcast::{ChangeV1, Timestamp},
};

#[derive(Debug, Clone, Readable, Writable)]
pub enum SyncMessage {
    V1(SyncMessageV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum SyncMessageV1 {
    State(SyncStateV1),
    Changeset(ChangeV1),
    Clock(Timestamp),
}

#[derive(Debug, Default, Clone, Readable, Writable, Serialize, Deserialize)]
pub struct SyncStateV1 {
    pub actor_id: ActorId,
    pub heads: HashMap<ActorId, i64>,
    pub need: HashMap<ActorId, Vec<RangeInclusive<i64>>>,
    pub partial_need: HashMap<ActorId, HashMap<i64, Vec<RangeInclusive<i64>>>>,
}

impl SyncStateV1 {
    pub fn need_len(&self) -> i64 {
        self.need
            .values()
            .flat_map(|v| v.iter().map(|range| (range.end() - range.start()) + 1))
            .sum::<i64>()
            + self
                .partial_need
                .values()
                .map(|partials| partials.len() as i64)
                .sum::<i64>()
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
            + self
                .partial_need
                .get(actor_id)
                .map(|partials| partials.len() as i64)
                .unwrap_or(0)
    }
}

impl From<SyncStateV1> for SyncMessage {
    fn from(value: SyncStateV1) -> Self {
        SyncMessage::V1(SyncMessageV1::State(value))
    }
}

// generates a `SyncMessage` to tell another node what versions we're missing
pub async fn generate_sync(bookie: &Bookie, actor_id: ActorId) -> SyncStateV1 {
    let mut state = SyncStateV1 {
        actor_id,
        ..Default::default()
    };

    let actors: Vec<(ActorId, Booked)> = {
        bookie
            .read()
            .await
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    };

    for (actor_id, booked) in actors {
        let last_version = match booked.last().await {
            Some(v) => v,
            None => continue,
        };

        let need: Vec<_> = { booked.read().await.gaps(&(1..=last_version)).collect() };

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            let read = booked.read().await;
            for (range, known) in read.iter() {
                if let KnownDbVersion::Partial { seqs, last_seq, .. } = known {
                    if seqs.gaps(&(0..=*last_seq)).count() == 0 {
                        // soon to be processed, but we got it all
                        continue;
                    }

                    state
                        .partial_need
                        .entry(actor_id)
                        .or_default()
                        .insert(*range.start(), seqs.gaps(&(0..=*last_seq)).collect());
                }
            }
        }

        state.heads.insert(actor_id, last_version);
    }

    state
}

#[derive(Debug, thiserror::Error)]
pub enum SyncMessageEncodeError {
    #[error(transparent)]
    Encode(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum SyncMessageDecodeError {
    #[error(transparent)]
    Decode(#[from] speedy::Error),
    #[error("corrupted message, crc mismatch (got: {0}, expected {1})")]
    Corrupted(u32, u32),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl SyncMessage {
    pub fn state(&self) -> Option<&SyncStateV1> {
        match self {
            SyncMessage::V1(SyncMessageV1::State(state)) => Some(state),
            _ => None,
        }
    }

    pub fn from_slice<S: AsRef<[u8]>>(slice: S) -> Result<Self, speedy::Error> {
        Self::read_from_buffer(slice.as_ref())
    }

    pub fn from_buf(buf: &mut BytesMut) -> Result<Self, SyncMessageDecodeError> {
        Ok(Self::from_slice(&buf)?)
    }

    pub fn decode(
        codec: &mut LengthDelimitedCodec,
        buf: &mut BytesMut,
    ) -> Result<Option<Self>, SyncMessageDecodeError> {
        Ok(match codec.decode(buf)? {
            Some(mut buf) => Some(Self::from_buf(&mut buf)?),
            None => None,
        })
    }
}
