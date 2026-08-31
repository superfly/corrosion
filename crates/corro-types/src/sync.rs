use std::{cmp, collections::HashMap, io, ops::RangeInclusive};

use bytes::BytesMut;
use metrics::counter;
use opentelemetry::propagation::{Extractor, Injector};
use rangemap::RangeInclusiveSet;
use serde::{Deserialize, Serialize};
use speedy::{Readable, Writable};
use tokio_util::codec::{Decoder, LengthDelimitedCodec};
use tracing::{debug, warn};

use crate::{
    actor::ActorId,
    agent::Bookie,
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq, CrsqlSeqRange},
    broadcast::{ChangeV1, Timestamp},
    compress::{decompress_change, try_compress_change_for_wire, CompressError, WireCompression},
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
    // zstd-compressed, speedy-encoded ChangeV1 -- only ever sent to peers that
    // advertised support for it via BiPayload::V1::supports_compression
    CompressedChangeset(Vec<u8>),
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

#[derive(Debug, Default, Clone, PartialEq, Writable, Serialize, Deserialize)]
pub struct SyncStateV1 {
    pub actor_id: ActorId,
    pub heads: HashMap<ActorId, CrsqlDbVersion>,
    pub need: HashMap<ActorId, Vec<RangeInclusive<CrsqlDbVersion>>>,
    pub partial_need: HashMap<ActorId, HashMap<CrsqlDbVersion, Vec<RangeInclusive<CrsqlSeq>>>>,
    #[speedy(default_on_eof)]
    pub last_cleared_ts: Option<Timestamp>,
}

// Keep the generated wire decoder separate from the validated public type. The
// wire layout is identical to `SyncStateV1`, but conversion into the public
// type only succeeds after every inclusive range has been checked.
#[derive(Readable)]
struct UnvalidatedSyncStateV1 {
    actor_id: ActorId,
    heads: HashMap<ActorId, CrsqlDbVersion>,
    need: HashMap<ActorId, Vec<RangeInclusive<CrsqlDbVersion>>>,
    partial_need: HashMap<ActorId, HashMap<CrsqlDbVersion, Vec<RangeInclusive<CrsqlSeq>>>>,
    #[speedy(default_on_eof)]
    last_cleared_ts: Option<Timestamp>,
}

impl<'a, C> Readable<'a, C> for SyncStateV1
where
    C: speedy::Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let state = UnvalidatedSyncStateV1::read_from(reader)?;
        let state = Self {
            actor_id: state.actor_id,
            heads: state.heads,
            need: state.need,
            partial_need: state.partial_need,
            last_cleared_ts: state.last_cleared_ts,
        };

        state
            .validate()
            .map_err(|error| speedy::Error::custom(error.to_string()))?;

        Ok(state)
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        <UnvalidatedSyncStateV1 as Readable<'a, C>>::minimum_bytes_needed()
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SyncStateValidationError {
    #[error("invalid need range for actor {actor_id}: start {start} is greater than end {end}")]
    InvertedNeed {
        actor_id: ActorId,
        start: CrsqlDbVersion,
        end: CrsqlDbVersion,
    },
    #[error(
        "invalid partial need range for actor {actor_id} at version {version}: start {start} is greater than end {end}"
    )]
    InvertedPartialNeed {
        actor_id: ActorId,
        version: CrsqlDbVersion,
        start: CrsqlSeq,
        end: CrsqlSeq,
    },
}

impl SyncStateV1 {
    pub fn validate(&self) -> Result<(), SyncStateValidationError> {
        for (actor_id, ranges) in &self.need {
            for range in ranges {
                if range.start() > range.end() {
                    return Err(SyncStateValidationError::InvertedNeed {
                        actor_id: *actor_id,
                        start: *range.start(),
                        end: *range.end(),
                    });
                }
            }
        }

        for (actor_id, partials) in &self.partial_need {
            for (version, ranges) in partials {
                for range in ranges {
                    if range.start() > range.end() {
                        return Err(SyncStateValidationError::InvertedPartialNeed {
                            actor_id: *actor_id,
                            version: *version,
                            start: *range.start(),
                            end: *range.end(),
                        });
                    }
                }
            }
        }

        Ok(())
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

    pub fn compute_available_needs(
        &self,
        other: &SyncStateV1,
    ) -> Result<HashMap<ActorId, Vec<SyncNeedV1>>, SyncStateValidationError> {
        self.validate()?;
        other.validate()?;

        let mut needs: HashMap<ActorId, Vec<SyncNeedV1>> = HashMap::new();

        for (actor_id, head) in other.heads.iter() {
            if *actor_id == self.actor_id {
                continue;
            }
            if *head == CrsqlDbVersion(0) {
                warn!(actor_id = %other.actor_id, "sent a 0 head version for actor id {}", actor_id);
                continue;
            }
            let other_haves = {
                let mut haves =
                    RangeInclusiveSet::from_iter([(CrsqlDbVersion(1)..=*head)].into_iter());

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
                            versions: CrsqlDbVersionRange::new(*start, *end),
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
                                seqs: seqs.iter().map(CrsqlSeqRange::from).collect(),
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
                                    other_seqs_haves.overlapping(range).map(|overlap| {
                                        let start = cmp::max(range.start(), overlap.start());
                                        let end = cmp::min(range.end(), overlap.end());
                                        CrsqlSeqRange::new(*start, *end)
                                    })
                                })
                                .collect::<Vec<CrsqlSeqRange>>();

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
                None => Some(CrsqlDbVersion(1)..=*head),
            };

            if let Some(missing) = missing {
                needs.entry(*actor_id).or_default().push(SyncNeedV1::Full {
                    versions: missing.into(),
                });
            }
        }

        Ok(needs)
    }
}

#[derive(Debug, Clone, PartialEq, Writable)]
pub enum SyncNeedV1 {
    Full {
        versions: CrsqlDbVersionRange,
    },
    Partial {
        version: CrsqlDbVersion,
        seqs: Vec<CrsqlSeqRange>,
    },
    Empty {
        ts: Option<Timestamp>,
    },
}

#[derive(Readable)]
enum UnvalidatedSyncNeedV1 {
    Full {
        versions: CrsqlDbVersionRange,
    },
    Partial {
        version: CrsqlDbVersion,
        seqs: Vec<CrsqlSeqRange>,
    },
    Empty {
        ts: Option<Timestamp>,
    },
}

impl<'a, C> Readable<'a, C> for SyncNeedV1
where
    C: speedy::Context,
{
    fn read_from<R: speedy::Reader<'a, C>>(reader: &mut R) -> Result<Self, C::Error> {
        let need = match UnvalidatedSyncNeedV1::read_from(reader)? {
            UnvalidatedSyncNeedV1::Full { versions } => Self::Full { versions },
            UnvalidatedSyncNeedV1::Partial { version, seqs } => Self::Partial { version, seqs },
            UnvalidatedSyncNeedV1::Empty { ts } => Self::Empty { ts },
        };

        need.validate()
            .map_err(|error| speedy::Error::custom(error.to_string()))?;

        Ok(need)
    }

    #[inline]
    fn minimum_bytes_needed() -> usize {
        <UnvalidatedSyncNeedV1 as Readable<'a, C>>::minimum_bytes_needed()
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum SyncNeedValidationError {
    #[error("invalid full sync range: start {start} is greater than end {end}")]
    InvertedFull {
        start: CrsqlDbVersion,
        end: CrsqlDbVersion,
    },
    #[error(
        "invalid partial sync range at version {version}: start {start} is greater than end {end}"
    )]
    InvertedPartial {
        version: CrsqlDbVersion,
        start: CrsqlSeq,
        end: CrsqlSeq,
    },
}

impl SyncNeedV1 {
    pub fn validate(&self) -> Result<(), SyncNeedValidationError> {
        match self {
            Self::Full { versions } if versions.start() > versions.end() => {
                Err(SyncNeedValidationError::InvertedFull {
                    start: versions.start(),
                    end: versions.end(),
                })
            }
            Self::Partial { version, seqs } => {
                for range in seqs {
                    if range.start() > range.end() {
                        return Err(SyncNeedValidationError::InvertedPartial {
                            version: *version,
                            start: range.start(),
                            end: range.end(),
                        });
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    #[inline]
    pub fn count(&self) -> usize {
        match self {
            SyncNeedV1::Full { versions } => versions.len(),
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

    let guard = bookie.owned_guard();

    for (&actor_id, booked) in bookie.iter(&guard) {
        let bookedr = booked.read();

        let last_version = match bookedr.last() {
            None => continue,
            Some(v) => v,
        };

        let need: Vec<_> = bookedr.needed().iter().cloned().collect();

        if !need.is_empty() {
            state.need.insert(actor_id, need);
        }

        {
            for (v, partial) in bookedr
                .partials
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
    #[error(transparent)]
    Compress(#[from] CompressError),
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
        let msg = Self::from_slice(buf)?;
        Ok(match msg {
            SyncMessage::V1(SyncMessageV1::CompressedChangeset(data)) => SyncMessage::V1(
                SyncMessageV1::Changeset(decompress_change(&data, "sync", None)?),
            ),
            other => other,
        })
    }

    /// Try to compress a `Changeset` message for the wire. Falls back to the
    /// original, uncompressed message if compression fails or doesn't
    /// actually shrink the payload. Should only be sent to peers that have
    /// advertised support for decoding `CompressedChangeset`.
    pub fn compress_changeset(self, level: i32) -> Self {
        let SyncMessage::V1(SyncMessageV1::Changeset(change)) = self else {
            return self;
        };

        match try_compress_change_for_wire(&change, "sync", level, None) {
            Ok(WireCompression::Compressed(compressed)) => {
                SyncMessage::V1(SyncMessageV1::CompressedChangeset(compressed))
            }
            Ok(WireCompression::Uncompressed) => SyncMessage::V1(SyncMessageV1::Changeset(change)),
            Err(e) => {
                counter!("corro.compression.errors.total", "traffic" => "sync").increment(1);
                debug!("could not compress sync changeset, sending uncompressed: {e}");
                SyncMessage::V1(SyncMessageV1::Changeset(change))
            }
        }
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
    use crate::base::{dbsr, dbsri, dbvr, dbvri};
    use uuid::Uuid;

    use super::*;

    #[test]
    fn test_compute_available_needs() {
        let actor1 = ActorId(Uuid::new_v4());

        let mut our_state = SyncStateV1::default();
        our_state.heads.insert(actor1, CrsqlDbVersion(10));

        let mut other_state = SyncStateV1::default();
        other_state.heads.insert(actor1, CrsqlDbVersion(13));

        assert_eq!(
            our_state.compute_available_needs(&other_state).unwrap(),
            [(
                actor1,
                vec![SyncNeedV1::Full {
                    versions: dbvr!(11, 13)
                }]
            )]
            .into()
        );

        our_state.need.entry(actor1).or_default().push(dbvri!(2, 5));
        our_state.need.entry(actor1).or_default().push(dbvri!(7, 7));

        assert_eq!(
            our_state.compute_available_needs(&other_state).unwrap(),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: dbvr!(2, 5)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(7, 7)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(11, 13)
                    }
                ]
            )]
            .into()
        );

        our_state.partial_need.insert(
            actor1,
            [(CrsqlDbVersion(9), vec![dbsri!(100, 120), dbsri!(130, 132)])].into(),
        );

        assert_eq!(
            our_state.compute_available_needs(&other_state).unwrap(),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: dbvr!(2, 5)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(7, 7)
                    },
                    SyncNeedV1::Partial {
                        version: CrsqlDbVersion(9),
                        seqs: vec![dbsr!(100, 120), dbsr!(130, 132)]
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(11, 13)
                    }
                ]
            )]
            .into()
        );

        other_state.partial_need.insert(
            actor1,
            [(CrsqlDbVersion(9), vec![dbsri!(100, 110), dbsri!(130, 130)])].into(),
        );

        assert_eq!(
            our_state.compute_available_needs(&other_state).unwrap(),
            [(
                actor1,
                vec![
                    SyncNeedV1::Full {
                        versions: dbvr!(2, 5)
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(7, 7)
                    },
                    SyncNeedV1::Partial {
                        version: CrsqlDbVersion(9),
                        seqs: vec![dbsr!(111, 120), dbsr!(131, 132)]
                    },
                    SyncNeedV1::Full {
                        versions: dbvr!(11, 13)
                    }
                ]
            )]
            .into()
        );
    }

    fn change_with_rows(n: usize) -> ChangeV1 {
        use crate::{actor::ActorId, base::CrsqlDbVersion, change::Change};
        use corro_api_types::SqliteValue;

        let actor_id = ActorId(Uuid::new_v4());
        let changes = (0..n)
            .map(|i| Change {
                table: "test_table".into(),
                pk: format!("pk-{i}").into_bytes(),
                cid: "some_column".into(),
                val: SqliteValue::Text("a fairly repetitive value".into()),
                col_version: 1,
                db_version: CrsqlDbVersion(1),
                seq: CrsqlSeq(i as u64),
                site_id: actor_id.to_bytes(),
                cl: 1,
            })
            .collect::<Vec<_>>();

        ChangeV1 {
            actor_id,
            changeset: crate::broadcast::Changeset::Full {
                version: CrsqlDbVersion(1),
                changes,
                seqs: CrsqlSeqRange::new(CrsqlSeq(0), CrsqlSeq(n.max(1) as u64 - 1)),
                last_seq: CrsqlSeq(n.max(1) as u64 - 1),
                ts: Timestamp::zero(),
            },
        }
    }

    #[test]
    fn test_compress_changeset_roundtrips_through_wire() {
        let change = change_with_rows(200);
        let msg = SyncMessage::V1(SyncMessageV1::Changeset(change.clone())).compress_changeset(3);

        assert!(
            matches!(msg, SyncMessage::V1(SyncMessageV1::CompressedChangeset(_))),
            "expected a large, repetitive changeset to compress"
        );

        let mut buf = BytesMut::from(msg.write_to_vec().unwrap().as_slice());
        let decoded = SyncMessage::from_buf(&mut buf).unwrap();

        assert_eq!(decoded, SyncMessage::V1(SyncMessageV1::Changeset(change)));
    }
}

#[cfg(test)]
mod wire_range_validation_tests {
    use super::*;
    use crate::{
        base::{dbsr, dbsri, dbvr, dbvri, CrsqlSeqRange},
        broadcast::{BroadcastV1, ChangeValidationError, Changeset, ChangesetPerTable},
    };
    use rangemap::RangeInclusiveSet;
    use speedy::{Readable, Writable};
    use uuid::Uuid;

    fn actor() -> ActorId {
        ActorId(Uuid::from_u128(1))
    }

    fn assert_wire_rejects(message: SyncMessage, expected: &str) {
        let bytes = message.write_to_vec().unwrap();
        let error = SyncMessage::read_from_buffer(&bytes).unwrap_err();

        assert!(
            error.to_string().contains(expected),
            "unexpected decode error: {error}"
        );
    }

    fn assert_wire_roundtrip(message: SyncMessage) {
        let bytes = message.write_to_vec().unwrap();
        assert_eq!(SyncMessage::read_from_buffer(&bytes).unwrap(), message);
    }

    fn request(need: SyncNeedV1) -> SyncMessage {
        SyncMessage::V1(SyncMessageV1::Request(vec![(actor(), vec![need])]))
    }

    #[test]
    fn wire_decode_rejects_inverted_need_range() {
        let target = actor();
        let mut state = SyncStateV1::default();
        state.heads.insert(target, CrsqlDbVersion(20));
        state.need.insert(target, vec![dbvri!(10, 5)]);

        assert_wire_rejects(
            SyncMessage::V1(SyncMessageV1::State(state)),
            "invalid need range for actor",
        );
    }

    #[test]
    fn wire_decode_rejects_inverted_partial_need_range() {
        let target = actor();
        let mut state = SyncStateV1::default();
        state.heads.insert(target, CrsqlDbVersion(20));
        state
            .partial_need
            .insert(target, [(CrsqlDbVersion(11), vec![dbsri!(60, 50)])].into());

        assert_wire_rejects(
            SyncMessage::V1(SyncMessageV1::State(state)),
            "invalid partial need range for actor",
        );
    }

    #[test]
    fn wire_decode_accepts_valid_need_and_partial_ranges() {
        let target = actor();
        let mut state = SyncStateV1::default();
        state.heads.insert(target, CrsqlDbVersion(20));
        state.need.insert(target, vec![dbvri!(5, 10)]);
        state
            .partial_need
            .insert(target, [(CrsqlDbVersion(11), vec![dbsri!(50, 60)])].into());

        assert_wire_roundtrip(SyncMessage::V1(SyncMessageV1::State(state)));
    }

    #[test]
    fn compute_available_needs_returns_error_on_inverted_need() {
        let target = actor();
        let our = SyncStateV1::default();
        let mut theirs = SyncStateV1::default();
        theirs.heads.insert(target, CrsqlDbVersion(20));
        theirs.need.insert(target, vec![dbvri!(10, 5)]);

        assert_eq!(
            our.compute_available_needs(&theirs).unwrap_err(),
            SyncStateValidationError::InvertedNeed {
                actor_id: target,
                start: CrsqlDbVersion(10),
                end: CrsqlDbVersion(5),
            }
        );
    }

    #[test]
    fn compute_available_needs_returns_error_on_inverted_partial_need() {
        let target = actor();
        let mut our = SyncStateV1::default();
        our.partial_need
            .insert(target, [(CrsqlDbVersion(9), vec![dbsri!(100, 120)])].into());

        let mut theirs = SyncStateV1::default();
        theirs.heads.insert(target, CrsqlDbVersion(20));
        theirs
            .partial_need
            .insert(target, [(CrsqlDbVersion(9), vec![dbsri!(50, 10)])].into());

        assert_eq!(
            our.compute_available_needs(&theirs).unwrap_err(),
            SyncStateValidationError::InvertedPartialNeed {
                actor_id: target,
                version: CrsqlDbVersion(9),
                start: CrsqlSeq(50),
                end: CrsqlSeq(10),
            }
        );
    }

    #[test]
    fn compute_available_needs_accepts_valid_ranges() {
        let target = actor();
        let mut our = SyncStateV1::default();
        our.need.insert(target, vec![dbvri!(5, 10)]);
        let mut theirs = SyncStateV1::default();
        theirs.heads.insert(target, CrsqlDbVersion(20));

        assert!(our.compute_available_needs(&theirs).is_ok());
    }

    #[test]
    fn wire_decode_rejects_inverted_full_sync_request() {
        assert_wire_rejects(
            request(SyncNeedV1::Full {
                versions: dbvr!(10, 5),
            }),
            "invalid full sync range",
        );
    }

    #[test]
    fn wire_decode_rejects_inverted_partial_sync_request() {
        assert_wire_rejects(
            request(SyncNeedV1::Partial {
                version: CrsqlDbVersion(10),
                seqs: vec![dbsr!(10, 5)],
            }),
            "invalid partial sync range",
        );
    }

    #[test]
    fn wire_decode_accepts_valid_sync_request_ranges() {
        let message = SyncMessage::V1(SyncMessageV1::Request(vec![(
            actor(),
            vec![
                SyncNeedV1::Full {
                    versions: dbvr!(5, 10),
                },
                SyncNeedV1::Partial {
                    version: CrsqlDbVersion(11),
                    seqs: vec![dbsr!(50, 60)],
                },
            ],
        )]));

        assert_wire_roundtrip(message);
    }

    fn full_v2_change(seqs: CrsqlSeqRange) -> ChangeV1 {
        let actor_id = actor();
        ChangeV1 {
            actor_id,
            changeset: Changeset::FullV2 {
                actor_id,
                version: CrsqlDbVersion(7),
                changes: ChangesetPerTable::default(),
                seqs,
                last_seq: CrsqlSeq(20),
                ts: Timestamp::zero(),
            },
        }
    }

    fn decode_sync_change(change: ChangeV1) -> Result<ChangeV1, String> {
        let bytes = SyncMessage::V1(SyncMessageV1::Changeset(change))
            .write_to_vec()
            .unwrap();
        match SyncMessage::read_from_buffer(&bytes).map_err(|error| error.to_string())? {
            SyncMessage::V1(SyncMessageV1::Changeset(change)) => Ok(change),
            message => Err(format!("expected a changeset, got {message:?}")),
        }
    }

    fn decode_broadcast_change(change: ChangeV1) -> Result<ChangeV1, String> {
        let bytes = BroadcastV1::Change(change).write_to_vec().unwrap();
        BroadcastV1::read_from_buffer(&bytes)
            .map_err(|error| error.to_string())?
            .into_change(None)
            .map_err(|error| error.to_string())
    }

    fn assert_decode_error(result: Result<ChangeV1, String>, expected: &str) {
        let error = result.unwrap_err();
        assert!(error.contains(expected), "unexpected decode error: {error}");
    }

    fn assert_invalid_versions(changeset: Changeset) {
        let change = ChangeV1 {
            actor_id: actor(),
            changeset,
        };

        assert_eq!(
            change.validate().unwrap_err(),
            ChangeValidationError::InvertedVersions {
                start: CrsqlDbVersion(10),
                end: CrsqlDbVersion(5),
            }
        );
        assert_decode_error(
            decode_broadcast_change(change),
            "invalid changeset version range",
        );
    }

    #[test]
    fn wire_decode_rejects_inverted_empty_changeset_versions() {
        assert_invalid_versions(Changeset::Empty {
            versions: dbvr!(10, 5),
            ts: None,
        });
    }

    #[test]
    fn wire_decode_rejects_inverted_empty_set_changeset_versions() {
        assert_invalid_versions(Changeset::EmptySet {
            versions: vec![dbvr!(10, 5)],
            ts: Timestamp::zero(),
        });
    }

    #[test]
    fn wire_decode_rejects_inverted_changeset_seqs() {
        let change = full_v2_change(dbsr!(10, 5));

        assert_eq!(
            change.validate().unwrap_err(),
            ChangeValidationError::InvertedSeqs {
                start: CrsqlSeq(10),
                end: CrsqlSeq(5),
            }
        );

        assert_decode_error(
            decode_sync_change(change.clone()),
            "invalid changeset sequence range",
        );
        assert_decode_error(
            decode_broadcast_change(change),
            "invalid changeset sequence range",
        );
    }

    #[test]
    fn wire_decode_accepts_valid_changeset_seqs_for_seen_cache() {
        let change = full_v2_change(dbsr!(5, 10));
        let decoded = decode_sync_change(change.clone()).unwrap();

        let seqs = decoded.seqs().unwrap();
        let mut entry: RangeInclusiveSet<CrsqlSeq> = RangeInclusiveSet::new();
        entry.extend([seqs.into()]);
        assert!(entry.contains(&CrsqlSeq(5)));
        assert!(entry.contains(&CrsqlSeq(10)));

        let decoded = decode_broadcast_change(change).unwrap();
        assert_eq!(decoded.seqs(), Some(dbsr!(5, 10)));
    }
}
