use std::{cmp, collections::HashMap, io, ops::RangeInclusive};

use bytes::BytesMut;
use opentelemetry::propagation::{Extractor, Injector};
use rangemap::RangeInclusiveSet;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use tokio_util::codec::{Decoder, LengthDelimitedCodec};
use tracing::warn;

use crate::{
    actor::ActorId,
    agent::{Booked, Bookie},
    broadcast::{ChangeV1, Timestamp},
};

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum SyncMessage {
    V1(SyncMessageV1),
}

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum SyncMessageV1 {
    State(SyncStateV1),
    Changeset(ChangeV1),
    Clock(Timestamp),
    Rejection(SyncRejectionV1),
    Request(SyncRequestV1),
}

#[derive(Debug, Default, Clone, PartialEq, Readable, Writable)]
pub struct SyncTraceContextV1 {
    pub traceparent: Option<String>,
    pub tracestate: Option<String>,
}

impl Injector for SyncTraceContextV1 {
    fn set(&mut self, key: &str, value: String) {
        match key {
            "traceparent" if !value.is_empty() => self.traceparent = Some(value),
            "tracestate" if !value.is_empty() => self.tracestate = Some(value),
            _ => {}
        }
    }
}

impl Extractor for SyncTraceContextV1 {
    fn get(&self, key: &str) -> Option<&str> {
        match key {
            "traceparent" => self.traceparent.as_deref(),
            "tracestate" => self.tracestate.as_deref(),
            _ => None,
        }
    }

    fn keys(&self) -> Vec<&str> {
        let mut v = Vec::with_capacity(2);
        if self.traceparent.is_some() {
            v.push("traceparent");
        }
        if self.tracestate.is_some() {
            v.push("tracestate");
        }
        v
    }
}

pub type SyncRequestV1 = Vec<(ActorId, Vec<SyncNeedV1>)>;

#[derive(Debug, thiserror::Error, Clone, PartialEq, Readable, Writable)]
pub enum SyncRejectionV1 {
    #[error("max concurrency reached")]
    MaxConcurrencyReached,
}

#[derive(Debug, Default, Clone, PartialEq, Readable, Writable, Serialize, Deserialize)]
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
            + (
                self.partial_need
                    .values()
                    .flat_map(|partials| {
                        partials.values().flat_map(|ranges| {
                            ranges.iter().map(|range| (range.end() - range.start()) + 1)
                        })
                    })
                    .sum::<i64>()
                    / 50
                // this is how many chunks we're looking at, kind of random...
            )
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

    pub fn compute_available_needs(
        &self,
        other: &SyncStateV1,
    ) -> HashMap<ActorId, Vec<SyncNeedV1>> {
        let mut needs: HashMap<ActorId, Vec<SyncNeedV1>> = HashMap::new();

        for (actor_id, head) in other.heads.iter() {
            if *actor_id == self.actor_id {
                continue;
            }
            if *head == 0 {
                warn!(actor_id = %other.actor_id, "sent a 0 head version for actor id {}", actor_id);
                continue;
            }
            let other_haves = {
                let mut haves = RangeInclusiveSet::from_iter([(1..=*head)].into_iter());

                // remove needs
                if let Some(other_need) = other.need.get(actor_id) {
                    for need in other_need.iter() {
                        // create gaps
                        haves.remove(need.clone());
                    }
                }

                // remove partials
                if let Some(other_partials) = other.partial_need.get(actor_id) {
                    for (v, _) in other_partials.iter() {
                        haves.remove(*v..=*v);
                    }
                }

                // we are left with all the versions they fully have!

                haves
            };

            if let Some(our_need) = self.need.get(actor_id) {
                for range in our_need.iter() {
                    for overlap in other_haves.overlapping(range) {
                        let start = cmp::max(range.start(), overlap.start());
                        let end = cmp::min(range.end(), overlap.end());
                        needs.entry(*actor_id).or_default().push(SyncNeedV1::Full {
                            versions: *start..=*end,
                        })
                    }
                }
            }

            if let Some(our_partials) = self.partial_need.get(actor_id) {
                for (v, seqs) in our_partials.iter() {
                    if other_haves.contains(v) {
                        needs
                            .entry(*actor_id)
                            .or_default()
                            .push(SyncNeedV1::Partial {
                                version: *v,
                                seqs: seqs.clone(),
                            });
                    } else if let Some(other_seqs) = other
                        .partial_need
                        .get(actor_id)
                        .and_then(|versions| versions.get(v))
                    {
                        let max_other_seq = other_seqs.iter().map(|range| *range.end()).max();
                        let max_our_seq = seqs.iter().map(|range| *range.end()).max();

                        let end_seq = cmp::max(max_other_seq, max_our_seq);

                        if let Some(end) = end_seq {
                            let mut other_seqs_haves = RangeInclusiveSet::from_iter([0..=end]);

                            for seqs in other_seqs.iter() {
                                other_seqs_haves.remove(seqs.clone());
                            }

                            let seqs = seqs
                                .iter()
                                .flat_map(|range| {
                                    other_seqs_haves
                                        .overlapping(range)
                                        .map(|overlap| {
                                            let start = cmp::max(range.start(), overlap.start());
                                            let end = cmp::min(range.end(), overlap.end());
                                            *start..=*end
                                        })
                                        .collect::<Vec<RangeInclusive<i64>>>()
                                })
                                .collect::<Vec<RangeInclusive<i64>>>();

                            if !seqs.is_empty() {
                                needs
                                    .entry(*actor_id)
                                    .or_default()
                                    .push(SyncNeedV1::Partial { version: *v, seqs });
                            }
                        }
                    }
                }
            }

            let missing = match self.heads.get(actor_id) {
                Some(our_head) => {
                    if head > our_head {
                        Some((*our_head + 1)..=*head)
                    } else {
                        None
                    }
                }
                None => Some(1..=*head),
            };

            if let Some(missing) = missing {
                needs
                    .entry(*actor_id)
                    .or_default()
                    .push(SyncNeedV1::Full { versions: missing });
            }
        }

        needs
    }
}

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum SyncNeedV1 {
    Full {
        versions: RangeInclusive<i64>,
    },
    Partial {
        version: i64,
        seqs: Vec<RangeInclusive<i64>>,
    },
}

impl SyncNeedV1 {
    pub fn count(&self) -> usize {
        match self {
            SyncNeedV1::Full { versions } => (versions.end() - versions.start()) as usize + 1,
            SyncNeedV1::Partial { .. } => 1,
        }
    }
}

impl From<SyncStateV1> for SyncMessage {
    fn from(value: SyncStateV1) -> Self {
        SyncMessage::V1(SyncMessageV1::State(value))
    }
}

// generates a `SyncMessage` to tell another node what versions we're missing
#[tracing::instrument(skip_all, level = "debug")]
pub async fn generate_sync(bookie: &Bookie, actor_id: ActorId) -> SyncStateV1 {
    let mut state = SyncStateV1 {
        actor_id,
        ..Default::default()
    };

    let actors: Vec<(ActorId, Booked)> = {
        bookie
            .read("generate_sync")
            .await
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    };

    for (actor_id, booked) in actors {
        let bookedr = booked
            .read(format!("generate_sync:{}", actor_id.as_simple()))
            .await;

        let last_version = match { bookedr.last() } {
            None => continue,
            Some(v) => v,
        };

        let need: Vec<_> = bookedr.sync_need().iter().cloned().collect();

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            for (v, partial) in bookedr.partials.iter() {
                state
                    .partial_need
                    .entry(actor_id)
                    .or_default()
                    .insert(*v, partial.seqs.gaps(&(0..=partial.last_seq)).collect());
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
        Ok(Self::from_slice(buf)?)
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

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_compute_available_needs() {
        let actor1 = ActorId(Uuid::new_v4());

        let mut our_state = SyncStateV1::default();
        our_state.heads.insert(actor1, 10);

        let mut other_state = SyncStateV1::default();
        other_state.heads.insert(actor1, 13);

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(actor1, vec![SyncNeedV1::Full { versions: 11..=13 }])].into()
        );

        our_state.need.entry(actor1).or_default().push(2..=5);
        our_state.need.entry(actor1).or_default().push(7..=7);

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full { versions: 2..=5 },
                    SyncNeedV1::Full { versions: 7..=7 },
                    SyncNeedV1::Full { versions: 11..=13 }
                ]
            )]
            .into()
        );

        our_state
            .partial_need
            .insert(actor1, [(9i64, vec![100..=120, 130..=132])].into());

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full { versions: 2..=5 },
                    SyncNeedV1::Full { versions: 7..=7 },
                    SyncNeedV1::Partial {
                        version: 9,
                        seqs: vec![100..=120, 130..=132]
                    },
                    SyncNeedV1::Full { versions: 11..=13 }
                ]
            )]
            .into()
        );

        other_state
            .partial_need
            .insert(actor1, [(9i64, vec![100..=110, 130..=130])].into());

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full { versions: 2..=5 },
                    SyncNeedV1::Full { versions: 7..=7 },
                    SyncNeedV1::Partial {
                        version: 9,
                        seqs: vec![111..=120, 131..=132]
                    },
                    SyncNeedV1::Full { versions: 11..=13 }
                ]
            )]
            .into()
        );
    }
}
