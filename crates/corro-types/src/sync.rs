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
    base::{CrsqlSeq, Version},
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
    #[error("different cluster")]
    DifferentCluster,
}

#[derive(Debug, Default, Clone, PartialEq, Readable, Writable, Serialize, Deserialize)]
pub struct SyncStateV1 {
    pub actor_id: ActorId,
    pub heads: HashMap<ActorId, Version>,
    pub need: HashMap<ActorId, Vec<RangeInclusive<Version>>>,
    pub partial_need: HashMap<ActorId, HashMap<Version, Vec<RangeInclusive<CrsqlSeq>>>>,
    #[speedy(default_on_eof)]
    pub last_cleared_ts: Option<Timestamp>,
}
impl SyncStateV1 {
    pub fn contains(&self, actor_id: &ActorId, versions: &RangeInclusive<Version>) -> bool {
        !(self.heads.get(actor_id).cloned().unwrap_or_default() < *versions.end()
            || self
                .need
                .get(actor_id)
                .cloned()
                .unwrap_or_default()
                .iter()
                .any(|x| x.start() < versions.end() && versions.start() < x.end()))
    }

    pub fn merge_needs(&mut self, needs: &HashMap<ActorId, Vec<SyncNeedV1>>) {
        for (actor, need) in needs {
            for n in need {
                match n {
                    SyncNeedV1::Full { versions } => {
                        self.merge_full_version(*actor, versions);
                    }
                    SyncNeedV1::Partial { version, seqs } => {
                        let mut delete = false;
                        self.partial_need.entry(*actor).and_modify(|e| {
                            if let Some(need_seqs) = e.get_mut(version) {
                                let mut seqs_set =
                                    RangeInclusiveSet::from_iter(need_seqs.clone().into_iter());
                                for seq in seqs {
                                    seqs_set.remove(seq.clone())
                                }
                                *need_seqs = Vec::from_iter(seqs_set.into_iter());
                                if need_seqs.is_empty() {
                                    delete = true
                                }
                            };
                        });

                        // we have gotten all sequence numbers, delete need
                        if delete {
                            if let Some(partials) = self.partial_need.get_mut(actor) {
                                partials.remove(version);
                            };
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    pub fn merge_full_version(&mut self, actor_id: ActorId, version: &RangeInclusive<Version>) {
        let head = self.heads.entry(actor_id).or_default();
        if version.end() > head {
            // check for gaps
            if *head + 1 < *version.start() {
                let range = *head + 1..=*version.start() - 1;
                self.need.entry(actor_id).or_default().push(range);
            }
            *head = *version.end();
        }

        self.need.entry(actor_id).and_modify(|e| {
            let mut set = RangeInclusiveSet::from_iter(e.clone().into_iter());
            set.remove(version.clone());
            *e = Vec::from_iter(set.into_iter());
        });
    }

    pub fn need_len(&self) -> u64 {
        self.need
            .values()
            .flat_map(|v| v.iter().map(|range| (range.end().0 - range.start().0) + 1))
            .sum::<u64>()
            + (
                self.partial_need
                    .values()
                    .flat_map(|partials| {
                        partials.values().flat_map(|ranges| {
                            ranges
                                .iter()
                                .map(|range| (range.end().0 - range.start().0) + 1)
                        })
                    })
                    .sum::<u64>()
                    / 50
                // this is how many chunks we're looking at, kind of random...
            )
    }

    pub fn need_len_for_actor(&self, actor_id: &ActorId) -> u64 {
        self.need
            .get(actor_id)
            .map(|v| {
                v.iter()
                    .map(|range| (range.end().0 - range.start().0) + 1)
                    .sum()
            })
            .unwrap_or(0)
            + self
                .partial_need
                .get(actor_id)
                .map(|partials| partials.len() as u64)
                .unwrap_or(0)
    }

    pub fn get_n_needs(&self, other: &SyncStateV1, n: u64) -> HashMap<ActorId, Vec<SyncNeedV1>> {
        let mut needs: HashMap<ActorId, Vec<SyncNeedV1>> = HashMap::new();
        let mut total = 0;

        for (actor_id, head) in other.heads.iter() {
            if *actor_id == self.actor_id {
                continue;
            }
            if *head == Version(0) {
                warn!(actor_id = %other.actor_id, "sent a 0 head version for actor id {}", actor_id);
                continue;
            }
            let other_haves = {
                let mut haves = RangeInclusiveSet::from_iter([(Version(1)..=*head)].into_iter());

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
                        let left = n - total;
                        let new_end = cmp::min(*end, *start + left);
                        needs.entry(*actor_id).or_default().push(SyncNeedV1::Full {
                            versions: *start..=new_end,
                        });
                        total += new_end.0 - start.0 + 1;
                        if total >= n {
                            return needs;
                        }
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
                        total += 1;
                        if total >= n {
                            return needs;
                        }
                    } else if let Some(other_seqs) = other
                        .partial_need
                        .get(actor_id)
                        .and_then(|versions| versions.get(v))
                    {
                        let max_other_seq = other_seqs.iter().map(|range| *range.end()).max();
                        let max_our_seq = seqs.iter().map(|range| *range.end()).max();

                        let end_seq = cmp::max(max_other_seq, max_our_seq);

                        if let Some(end) = end_seq {
                            let mut other_seqs_haves =
                                RangeInclusiveSet::from_iter([CrsqlSeq(0)..=end]);

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
                                        .collect::<Vec<RangeInclusive<CrsqlSeq>>>()
                                })
                                .collect::<Vec<RangeInclusive<CrsqlSeq>>>();

                            if !seqs.is_empty() {
                                needs
                                    .entry(*actor_id)
                                    .or_default()
                                    .push(SyncNeedV1::Partial { version: *v, seqs });
                            }
                            total += 1;
                            if total >= n {
                                return needs;
                            }
                        }
                    }
                }
            }

            let left = n - total;
            let missing = match self.heads.get(actor_id) {
                Some(our_head) => {
                    if head > our_head {
                        let new_head = cmp::min(*our_head + left, *head);
                        Some((*our_head + 1)..=new_head)
                    } else {
                        None
                    }
                }
                None => {
                    let new_head = Version(cmp::min(head.0, left));
                    Some(Version(1)..=new_head)
                }
            };

            if let Some(missing) = missing {
                let mut missing = RangeInclusiveSet::from_iter([missing].into_iter());
                if let Some(other_needs) = other.need.get(actor_id) {
                    // remove needs
                    for need in other_needs.iter() {
                        missing.remove(need.clone());
                    }
                }

                // remove partial needs
                if let Some(partials) = other.partial_need.get(actor_id) {
                    for (v, _) in partials {
                        missing.remove(*v..=*v);
                    }
                }

                missing.into_iter().for_each(|v| {
                    total += v.end().0 - v.start().0 + 1;
                    needs
                        .entry(*actor_id)
                        .or_default()
                        .push(SyncNeedV1::Full { versions: v });
                });
            }

            if total >= n {
                return needs;
            }
        }

        needs
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
            if *head == Version(0) {
                warn!(actor_id = %other.actor_id, "sent a 0 head version for actor id {}", actor_id);
                continue;
            }
            let other_haves = {
                let mut haves = RangeInclusiveSet::from_iter([(Version(1)..=*head)].into_iter());

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
                            let mut other_seqs_haves =
                                RangeInclusiveSet::from_iter([CrsqlSeq(0)..=end]);

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
                                        .collect::<Vec<RangeInclusive<CrsqlSeq>>>()
                                })
                                .collect::<Vec<RangeInclusive<CrsqlSeq>>>();

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
                None => Some(Version(1)..=*head),
            };

            if let Some(missing) = missing {
                let mut missing = RangeInclusiveSet::from_iter([missing].into_iter());
                if let Some(other_needs) = other.need.get(actor_id) {
                    // remove needs
                    for need in other_needs.iter() {
                        missing.remove(need.clone());
                    }
                }

                // remove partial needs
                if let Some(partials) = other.partial_need.get(actor_id) {
                    for (v, _) in partials {
                        missing.remove(*v..=*v);
                    }
                }

                missing.into_iter().for_each(|v| {
                    needs
                        .entry(*actor_id)
                        .or_default()
                        .push(SyncNeedV1::Full { versions: v });
                });
            }
        }

        needs
    }
}

#[derive(Debug, Clone, PartialEq, Readable, Writable)]
pub enum SyncNeedV1 {
    Full {
        versions: RangeInclusive<Version>,
    },
    Partial {
        version: Version,
        seqs: Vec<RangeInclusive<CrsqlSeq>>,
    },
    Empty {
        ts: Option<Timestamp>,
    },
}

impl SyncNeedV1 {
    pub fn count(&self) -> usize {
        match self {
            SyncNeedV1::Full { versions } => (versions.end().0 - versions.start().0) as usize + 1,
            SyncNeedV1::Partial { .. } => 1,
            SyncNeedV1::Empty { .. } => 1,
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
pub async fn generate_sync(bookie: &Bookie, self_actor_id: ActorId) -> SyncStateV1 {
    let mut state = SyncStateV1 {
        actor_id: self_actor_id,
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

    let mut last_ts = None;

    for (actor_id, booked) in actors {
        let (last_version, needs, partials, last_cleared_ts) = {
            let bookedr = booked
                .read(format!("generate_sync:{}", actor_id.as_simple()))
                .await;
            (
                bookedr.last(),
                bookedr.needed().clone(),
                bookedr.partials.clone(),
                bookedr.last_cleared_ts(),
            )
        };

        let last_version = match last_version {
            None => continue,
            Some(v) => v,
        };

        let need: Vec<_> = needs.iter().cloned().collect();

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            for (v, partial) in partials
                .iter()
                // don't set partial if it is effectively complete
                .filter(|(_, partial)| !partial.is_complete())
            {
                state.partial_need.entry(actor_id).or_default().insert(
                    *v,
                    partial
                        .seqs
                        .gaps(&(CrsqlSeq(0)..=partial.last_seq))
                        .collect(),
                );
            }
        }

        if actor_id == self_actor_id {
            last_ts = last_cleared_ts;
        }

        state.heads.insert(actor_id, last_version);
    }

    state.last_cleared_ts = last_ts;

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
    use itertools::Itertools;
    use uuid::Uuid;

    use super::*;

    // TODO: this test occasionally fails because get_n_needs loops over a HashMap which is
    // unordered. This could probably be fixed by using a BTreeMap instead
    // #[test]
    // fn test_get_n_needs() -> eyre::Result<()> {
    //     let mut original_state: SyncStateV1 = SyncStateV1::default();
    //
    //     // static strings so the order is predictible
    //     let actor1 = ActorId(Uuid::parse_str("adea794c-8bb8-4ca6-b04b-87ec22348326").unwrap());
    //     let actor2 = ActorId(Uuid::parse_str("0dea794c-8bb8-4ca6-b04b-87ec22348326").unwrap());
    //
    //     let mut actor1_state = SyncStateV1::default();
    //     actor1_state.heads.insert(actor1, Version(20));
    //     let expected = HashMap::from([(
    //         actor1,
    //         vec![SyncNeedV1::Full {
    //             versions: Version(1)..=Version(10),
    //         }],
    //     )]);
    //     assert_eq!(original_state.get_n_needs(&actor1_state, 10), expected);
    //
    //     let mut actor1_state = SyncStateV1::default();
    //     actor1_state.heads.insert(actor1, Version(8));
    //     actor1_state.heads.insert(actor2, Version(20));
    //     let got = original_state.get_n_needs(&actor1_state, 10);
    //     let expected = HashMap::from([
    //         (
    //             actor1,
    //             vec![SyncNeedV1::Full {
    //                 versions: Version(1)..=Version(8),
    //             }],
    //         ),
    //         (
    //             actor2,
    //             vec![SyncNeedV1::Full {
    //                 versions: Version(1)..=Version(2),
    //             }],
    //         ),
    //     ]);
    //     assert_eq!(got, expected);
    //
    //     let mut actor1_state = SyncStateV1::default();
    //     actor1_state.heads.insert(actor1, Version(30));
    //     actor1_state.partial_need.insert(
    //         actor1,
    //         HashMap::from([(Version(13), vec![CrsqlSeq(20)..=CrsqlSeq(25)])]),
    //     );
    //     actor1_state
    //         .need
    //         .insert(actor1, vec![Version(21)..=Version(24)]);
    //
    //     original_state.heads.insert(actor1, Version(10));
    //     original_state
    //         .need
    //         .insert(actor1, vec![Version(4)..=Version(5)]);
    //     original_state.partial_need.insert(
    //         actor1,
    //         HashMap::from([(Version(9), vec![CrsqlSeq(1)..=CrsqlSeq(10)])]),
    //     );
    //
    //     let got = original_state.get_n_needs(&actor1_state, 10);
    //     let expected = HashMap::from([(
    //         actor1,
    //         vec![
    //             SyncNeedV1::Full {
    //                 versions: Version(4)..=Version(5),
    //             },
    //             SyncNeedV1::Partial {
    //                 version: Version(9),
    //                 seqs: vec![CrsqlSeq(1)..=CrsqlSeq(10)],
    //             },
    //             SyncNeedV1::Full {
    //                 versions: Version(11)..=Version(12),
    //             },
    //             SyncNeedV1::Full {
    //                 versions: Version(14)..=Version(17),
    //             },
    //         ],
    //     )]);
    //     assert_eq!(got, expected);
    //
    //     Ok(())
    // }

    #[test]
    fn test_merge_need() {
        let actor1 = ActorId(Uuid::new_v4());

        let mut state = SyncStateV1::default();

        let mut needs = HashMap::new();
        needs.insert(
            actor1,
            vec![SyncNeedV1::Full {
                versions: Version(1)..=Version(50),
            }],
        );
        state.merge_needs(&needs);
        assert_eq!(state.heads.get(&actor1).unwrap(), &Version(50));
        assert!(state.need.get(&actor1).is_none());
        assert!(state.partial_need.get(&actor1).is_none());

        needs.get_mut(&actor1).unwrap().push(SyncNeedV1::Full {
            versions: Version(70)..=Version(90),
        });
        state.merge_needs(&needs);
        assert_eq!(state.heads.get(&actor1).unwrap(), &Version(90));
        assert!(state
            .need
            .get(&actor1)
            .unwrap()
            .iter()
            .contains(&(Version(51)..=Version(69))));
        assert!(state.partial_need.get(&actor1).is_none());

        needs.get_mut(&actor1).unwrap().push(SyncNeedV1::Full {
            versions: Version(60)..=Version(65),
        });
        state.merge_needs(&needs);
        assert_eq!(state.heads.get(&actor1).unwrap(), &Version(90));
        assert!(state
            .need
            .get(&actor1)
            .unwrap()
            .iter()
            .contains(&(Version(51)..=Version(59))));
        assert!(state
            .need
            .get(&actor1)
            .unwrap()
            .iter()
            .contains(&(Version(66)..=Version(69))));
        assert!(state.partial_need.get(&actor1).is_none());

        needs.get_mut(&actor1).unwrap().push(SyncNeedV1::Partial {
            version: Version(40),
            seqs: vec![CrsqlSeq(22)..=CrsqlSeq(25)],
        });
        state
            .partial_need
            .entry(actor1)
            .or_default()
            .entry(Version(40))
            .or_default()
            .extend_from_slice(&vec![
                CrsqlSeq(1)..=CrsqlSeq(10),
                CrsqlSeq(20)..=CrsqlSeq(25),
            ]);
        state.merge_needs(&needs);
        assert!(state
            .partial_need
            .get(&actor1)
            .unwrap()
            .get(&Version(40))
            .unwrap()
            .contains(&(CrsqlSeq(1)..=CrsqlSeq(10))));
        assert!(state
            .partial_need
            .get(&actor1)
            .unwrap()
            .get(&Version(40))
            .unwrap()
            .contains(&(CrsqlSeq(20)..=CrsqlSeq(21))));

        let mut needs = HashMap::new();
        needs.insert(
            actor1,
            vec![
                SyncNeedV1::Partial {
                    version: Version(40),
                    seqs: vec![CrsqlSeq(1)..=CrsqlSeq(10)],
                },
                SyncNeedV1::Partial {
                    version: Version(40),
                    seqs: vec![CrsqlSeq(20)..=CrsqlSeq(21)],
                },
            ],
        );
        state.merge_needs(&needs);
        assert!(state.partial_need.get(&actor1).unwrap().is_empty());
    }

    #[test]
    fn test_compute_available_needs() {
        let actor1 = ActorId(Uuid::new_v4());

        let mut our_state = SyncStateV1::default();
        our_state.heads.insert(actor1, Version(10));

        let mut other_state = SyncStateV1::default();
        other_state.heads.insert(actor1, Version(13));

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![SyncNeedV1::Full {
                    versions: Version(11)..=Version(13)
                }]
            )]
            .into()
        );

        our_state
            .need
            .entry(actor1)
            .or_default()
            .push(Version(2)..=Version(5));
        our_state
            .need
            .entry(actor1)
            .or_default()
            .push(Version(7)..=Version(7));

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: Version(2)..=Version(5)
                    },
                    SyncNeedV1::Full {
                        versions: Version(7)..=Version(7)
                    },
                    SyncNeedV1::Full {
                        versions: Version(11)..=Version(13)
                    }
                ]
            )]
            .into()
        );

        our_state.partial_need.insert(
            actor1,
            [(
                Version(9),
                vec![CrsqlSeq(100)..=CrsqlSeq(120), CrsqlSeq(130)..=CrsqlSeq(132)],
            )]
            .into(),
        );

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: Version(2)..=Version(5)
                    },
                    SyncNeedV1::Full {
                        versions: Version(7)..=Version(7)
                    },
                    SyncNeedV1::Partial {
                        version: Version(9),
                        seqs: vec![CrsqlSeq(100)..=CrsqlSeq(120), CrsqlSeq(130)..=CrsqlSeq(132)]
                    },
                    SyncNeedV1::Full {
                        versions: Version(11)..=Version(13)
                    }
                ]
            )]
            .into()
        );

        other_state.partial_need.insert(
            actor1,
            [(
                Version(9),
                vec![CrsqlSeq(100)..=CrsqlSeq(110), CrsqlSeq(130)..=CrsqlSeq(130)],
            )]
            .into(),
        );

        assert_eq!(
            our_state.compute_available_needs(&other_state),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: Version(2)..=Version(5)
                    },
                    SyncNeedV1::Full {
                        versions: Version(7)..=Version(7)
                    },
                    SyncNeedV1::Partial {
                        version: Version(9),
                        seqs: vec![CrsqlSeq(111)..=CrsqlSeq(120), CrsqlSeq(131)..=CrsqlSeq(132)]
                    },
                    SyncNeedV1::Full {
                        versions: Version(11)..=Version(13)
                    }
                ]
            )]
            .into()
        );
    }
}
