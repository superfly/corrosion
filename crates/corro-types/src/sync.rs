use std::{collections::HashMap, io, ops::RangeInclusive};

use bytes::{BufMut, BytesMut};
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::{
    actor::ActorId,
    agent::{Booked, Bookie, KnownDbVersion},
    broadcast::ChangeV1,
};

#[derive(Debug, Clone, Readable, Writable)]
pub enum SyncMessage {
    V1(SyncMessageV1),
}

#[derive(Debug, Clone, Readable, Writable)]
pub enum SyncMessageV1 {
    State(SyncStateV1),
    Changeset(ChangeV1),
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
pub fn generate_sync(bookie: &Bookie, actor_id: ActorId) -> SyncStateV1 {
    let mut state = SyncStateV1 {
        actor_id,
        ..Default::default()
    };

    let actors: Vec<(ActorId, Booked)> =
        { bookie.read().iter().map(|(k, v)| (*k, v.clone())).collect() };

    for (actor_id, booked) in actors {
        let last_version = match booked.last() {
            Some(v) => v,
            None => continue,
        };

        let need: Vec<_> = booked.read().gaps(&(1..=last_version)).collect();

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            let read = booked.read();
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

    pub fn encode_w_codec(
        &self,
        codec: &mut LengthDelimitedCodec,
        buf: &mut BytesMut,
    ) -> Result<(), SyncMessageEncodeError> {
        self.write_to_stream(buf.writer())?;
        // let mut bytes = buf.split();
        // let hash = crc32fast::hash(&bytes);
        // bytes.put_u32(hash);

        codec.encode(buf.split().freeze(), buf)?;

        Ok(())
    }

    // pub fn encode(&self, buf: &mut BytesMut) -> Result<(), SyncMessageEncodeError> {
    //     let mut codec = LengthDelimitedCodec::builder()
    //         .length_field_type::<u32>()
    //         .new_codec();
    //     self.encode_w_codec(&mut codec, buf)?;

    //     Ok(())
    // }

    pub fn from_buf(buf: &mut BytesMut) -> Result<Self, SyncMessageDecodeError> {
        // let mut crc_bytes = buf.split_off(len - 4);

        // let crc = crc_bytes.get_u32();
        // let new_crc = crc32fast::hash(&buf);
        // if crc != new_crc {
        //     return Err(SyncMessageDecodeError::Corrupted(crc, new_crc));
        // }

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
