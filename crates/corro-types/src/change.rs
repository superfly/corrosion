use std::{
    iter::Peekable,
    ops::{DerefMut, RangeInclusive},
};

use antithesis_sdk::{assert_always, assert_unreachable};
pub use corro_api_types::SqliteValue;
use corro_api_types::{ColumnName, TableName};
use corro_base_types::{varint, CrsqlDbVersion, CrsqlSeqRange};
use rangemap::RangeInclusiveSet;
use rusqlite::{Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use serde_json::json;
use speedy::{Readable, Writable};
use tracing::{debug, trace, warn};

use crate::{
    agent::{Agent, BookedVersions, ChangeError},
    base::CrsqlSeq,
    broadcast::{ChangesetPerTable, PackedChangesetPerTable, Timestamp},
};

#[derive(Debug, Default, Clone, Serialize, Deserialize, Readable, Writable, PartialEq)]
pub struct Change {
    pub table: TableName,
    pub pk: Vec<u8>,
    pub cid: ColumnName,
    pub val: SqliteValue,
    pub col_version: i64,
    pub db_version: CrsqlDbVersion,
    pub seq: CrsqlSeq,
    pub site_id: [u8; 16],
    pub cl: i64,
}

impl Change {
    // this is an ESTIMATE, it should give a rough idea of how many bytes will
    // be required on the wire
    pub fn estimated_byte_size(&self) -> usize {
        self.table.len() + self.pk.len() + self.cid.len() + self.val.estimated_byte_size() +
        // db_version
        8 +
        self.estimated_column_byte_size() +
        // site_id
        16
    }

    pub fn estimated_column_byte_size(&self) -> usize {
        self.cid.len() + self.val.estimated_byte_size() +
        // col_version
        8 +
        // seq
        8 +
        // cl
        8
    }
}

pub fn row_to_change(row: &Row) -> Result<Change, rusqlite::Error> {
    Ok(Change {
        table: row.get(0)?,
        pk: row.get(1)?,
        cid: row.get(2)?,
        val: row.get(3)?,
        col_version: row.get(4)?,
        db_version: row.get(5)?,
        seq: row.get(6)?,
        site_id: row.get(7)?,
        cl: row.get(8)?,
    })
}

/// A change row from `crsql_changes` in V2 packed mode.
///
/// Preserves packed BLOB values for `cid`, `val`, `col_version`, `seq`
/// using `SqliteValue` (which handles all SQLite runtime types).
/// Tombstone/sentinel rows have scalar values (TEXT/INT) in these fields
/// and are also preserved correctly.
///
/// `max_seq` is the maximum scalar seq extracted from the packed seq blob,
/// used for `CrsqlSeqRange` chunking metadata. For scalar (non-packed) rows,
/// it's just the seq value directly.
#[derive(Debug, Default, Clone, Serialize, Deserialize, Readable, Writable, PartialEq)]
pub struct PackedChange {
    pub table: TableName,
    pub pk: Vec<u8>,
    pub cid: SqliteValue,
    pub val: SqliteValue,
    pub col_version: SqliteValue,
    pub db_version: CrsqlDbVersion,
    pub seq: SqliteValue,
    pub site_id: [u8; 16],
    pub cl: i64,
    /// Max scalar seq extracted from packed seq blob, for chunking.
    /// Not sent over the wire — recomputed on receive from `seq`.
    #[speedy(skip)]
    pub max_seq: CrsqlSeq,
    /// Min scalar seq extracted from packed seq blob, for buffered-changes
    /// overlap filtering. For V1 scalar seqs, equals max_seq.
    /// Not sent over the wire — recomputed on receive from `seq`.
    #[speedy(skip)]
    pub min_seq: CrsqlSeq,
}

impl PackedChange {
    /// Estimated wire byte size for chunking.
    pub fn estimated_byte_size(&self) -> usize {
        self.table.len()
            + self.pk.len()
            + self.cid.estimated_byte_size()
            + self.val.estimated_byte_size()
            + self.col_version.estimated_byte_size()
            + self.seq.estimated_byte_size()
            + 8 // db_version
            + 16 // site_id
            + 8 // cl
            + 8 // max_seq
    }

    /// Estimated wire byte size for the column-level data only (excluding
    /// table name and pk, which are stored as map keys in
    /// `PackedChangesetPerTable`).  Used by the chunking iterator to match
    /// the V1 `ChunkedChanges` cost model so that chunks are comparable in
    /// size across wire modes.
    pub fn estimated_column_byte_size(&self) -> usize {
        self.cid.estimated_byte_size()
            + self.val.estimated_byte_size()
            + self.col_version.estimated_byte_size()
            + self.seq.estimated_byte_size()
            + 8 // cl
    }

    /// Extract the min and max scalar seq from the packed seq value.
    /// For scalar seq (tombstone/sentinel rows), both equal the value directly.
    pub fn compute_min_max_seq(seq: &SqliteValue) -> (CrsqlSeq, CrsqlSeq) {
        match seq {
            SqliteValue::Integer(s) => {
                let s = CrsqlSeq(*s as u64);
                (s, s)
            }
            SqliteValue::Blob(data) => match varint::unpack_i64_vec(data) {
                Ok(seqs) => {
                    let (min, max) = seqs
                        .into_iter()
                        .map(|s| s as u64)
                        .fold((u64::MAX, 0u64), |(min, max), s| (min.min(s), max.max(s)));
                    (CrsqlSeq(min), CrsqlSeq(max))
                }
                Err(e) => {
                    // This should never happen with valid cr-sqlite packed data.
                    // A malformed seq blob indicates a cr-sqlite packing bug or
                    // corruption — fail loudly in debug, fall back in release.
                    debug_assert!(false, "failed to unpack seq varints: {e}");
                    tracing::error!("failed to unpack seq varints: {e}, falling back to 0");
                    (CrsqlSeq(0), CrsqlSeq(0))
                }
            },
            SqliteValue::Null => (CrsqlSeq(0), CrsqlSeq(0)),
            _ => (CrsqlSeq(0), CrsqlSeq(0)),
        }
    }

    /// Extract the max scalar seq from the packed seq value.
    /// For scalar seq (tombstone/sentinel rows), returns the value directly.
    pub fn compute_max_seq(seq: &SqliteValue) -> CrsqlSeq {
        Self::compute_min_max_seq(seq).1
    }

    /// Extract the min scalar seq from the packed seq value.
    /// For scalar seq (tombstone/sentinel rows), returns the value directly.
    pub fn compute_min_seq(seq: &SqliteValue) -> CrsqlSeq {
        Self::compute_min_max_seq(seq).0
    }

    pub fn sequence_values(seq: &SqliteValue) -> Result<Vec<CrsqlSeq>, String> {
        match seq {
            SqliteValue::Integer(value) => {
                Ok(vec![CrsqlSeq((*value).try_into().map_err(|_| {
                    format!("negative sequence value: {value}")
                })?)])
            }
            SqliteValue::Blob(data) => varint::unpack_i64_vec(data)
                .map_err(|error| format!("failed to unpack sequence values: {error}"))?
                .into_iter()
                .map(|value| {
                    value
                        .try_into()
                        .map(CrsqlSeq)
                        .map_err(|_| format!("negative sequence value: {value}"))
                })
                .collect(),
            SqliteValue::Null => Ok(vec![CrsqlSeq(0)]),
            value => Err(format!("unsupported sequence SQLite value: {value:?}")),
        }
    }
}

/// Read a row from `crsql_changes` preserving native SQLite types.
/// Works in both V1 (scalar) and V2 packed (packed) modes.
pub fn row_to_packed_change(row: &Row) -> Result<PackedChange, rusqlite::Error> {
    let table: TableName = row.get(0)?;
    let pk: Vec<u8> = row.get(1)?;
    let cid: SqliteValue = row.get(2)?;
    let val: SqliteValue = row.get(3)?;
    let col_version: SqliteValue = row.get(4)?;
    let db_version: CrsqlDbVersion = row.get(5)?;
    let seq: SqliteValue = row.get(6)?;
    let site_id: [u8; 16] = row.get(7)?;
    let cl: i64 = row.get(8)?;

    let (min_seq, max_seq) = PackedChange::compute_min_max_seq(&seq);

    Ok(PackedChange {
        table,
        pk,
        cid,
        val,
        col_version,
        db_version,
        seq,
        site_id,
        cl,
        max_seq,
        min_seq,
    })
}

pub struct ChunkedChanges<I: Iterator> {
    iter: Peekable<I>,
    changes: ChangesetPerTable,
    last_pushed_seq: CrsqlSeq,
    last_start_seq: CrsqlSeq,
    last_seq: CrsqlSeq,
    max_buf_size: usize,
    buffered_size: usize,
    seen_seqs: RangeInclusiveSet<CrsqlSeq>,
    saw_change: bool,
    done: bool,
}

impl<I> ChunkedChanges<I>
where
    I: Iterator,
{
    pub fn new(iter: I, start_seq: CrsqlSeq, last_seq: CrsqlSeq, max_buf_size: usize) -> Self {
        Self {
            iter: iter.peekable(),
            changes: Default::default(),
            last_pushed_seq: CrsqlSeq(0),
            last_start_seq: start_seq,
            last_seq,
            max_buf_size,
            buffered_size: 0,
            seen_seqs: RangeInclusiveSet::new(),
            saw_change: false,
            done: false,
        }
    }

    pub fn max_buf_size(&self) -> usize {
        self.max_buf_size
    }

    pub fn set_max_buf_size(&mut self, size: usize) {
        self.max_buf_size = size;
    }
}

impl<I> Iterator for ChunkedChanges<I>
where
    I: Iterator<Item = rusqlite::Result<Change>>,
{
    type Item = Result<(ChangesetPerTable, CrsqlSeqRange), rusqlite::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // previously marked as done because the Rows iterator returned None
        if self.done {
            return None;
        }

        let details = json!({});
        assert_always!(
            self.changes.is_empty(),
            "iterator for ChunkedChanges still has changes when next() is called",
            &details
        );

        // reset the buffered size
        self.buffered_size = 0;

        loop {
            trace!("chunking through the rows iterator");
            match self.iter.next() {
                Some(Ok(change)) => {
                    trace!("got change: {change:?}");
                    let seq = change.seq;
                    let seq_range = seq..=seq;

                    if self.seen_seqs.overlapping(&seq_range).next().is_some() {
                        assert_unreachable!(
                            "V1 changes must not contain duplicate sequence values",
                            &json!({
                                "sequence": seq.0,
                                "last_pushed_seq": self.last_pushed_seq.0,
                            })
                        );
                        self.changes = ChangesetPerTable::default();
                        self.done = true;
                        return Some(Err(sequence_error(format!(
                            "V1 change contains duplicate sequence value: {}",
                            seq.0
                        ))));
                    }
                    if self.saw_change && seq < self.last_pushed_seq {
                        assert_unreachable!(
                            "V1 changes must arrive in sequence order",
                            &json!({
                                "previous_seq": self.last_pushed_seq.0,
                                "current_seq": seq.0,
                            })
                        );
                        self.changes = ChangesetPerTable::default();
                        self.done = true;
                        return Some(Err(sequence_error(format!(
                            "V1 changes are out of order: previous seq={}, current seq={}",
                            self.last_pushed_seq.0, seq.0
                        ))));
                    }
                    self.seen_seqs.insert(seq_range);
                    self.saw_change = true;
                    self.last_pushed_seq = seq;

                    let size = self.changes.insert(change);
                    self.buffered_size += size;

                    let can_split = self.buffered_size >= self.max_buf_size
                        && self.last_pushed_seq < self.last_seq
                        && self.iter.peek().is_some_and(|next| {
                            next.as_ref()
                                .map(|next| next.seq > self.last_pushed_seq)
                                .unwrap_or(false)
                        });
                    if can_split {
                        let start_seq = self.last_start_seq;
                        self.last_start_seq = self.last_pushed_seq + 1;

                        return Some(Ok((
                            self.changes.drain(),
                            CrsqlSeqRange::new(start_seq, self.last_pushed_seq),
                        )));
                    }
                }
                None => {
                    // probably not going to happen since we peek at the next and end early
                    // break out of the loop, don't return, there might be buffered changes
                    trace!("no more changes to iterate on");
                    break;
                }
                Some(Err(e)) => {
                    self.changes = ChangesetPerTable::default();
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }

        self.done = true;

        // return buffered changes
        Some(Ok((
            self.changes.clone(), // no need to drain here like before
            CrsqlSeqRange::new(self.last_start_seq, self.last_seq), // even if empty, this is all we have still applied
        )))
    }
}

pub const MAX_CHANGES_BYTE_SIZE: usize = 8 * 1024;

/// Chunked iterator for packed V2 packed changes.
///
/// Like `ChunkedChanges` but tracks the exact sequence values in each packed
/// row. It also advances the queried range as a checked watermark once the
/// current row's minimum proves no unread row can contain an earlier sequence.
/// Does not sort or filter input rows.
/// Requires reading changes ordered by the min_seq in the change
pub struct ChunkedPackedChanges<I: Iterator> {
    iter: I,
    changes: PackedChangesetPerTable,
    queried_range: CrsqlSeqRange,
    // Which seq ranges we can still possibly read
    remaining_ranges: RangeInclusiveSet<CrsqlSeq>,
    // Previous packed row's min_seq, to verify ORDER BY min_seq.
    last_row_min_seq: Option<CrsqlSeq>,
    max_buf_size: usize,
    buffered_size: usize,
    // Exact sequence values seen across the iterator, for duplicate detection.
    all_seen_seqs: RangeInclusiveSet<CrsqlSeq>,
    // Payload sequences plus checked watermark coverage for the current chunk.
    chunk_seqs: RangeInclusiveSet<CrsqlSeq>,
    done: bool,
}

impl<I> ChunkedPackedChanges<I>
where
    I: Iterator,
{
    /// `checked_range` is the source-authoritative sequence interval queried.
    /// The iterator attaches its checked prefix to each chunk as min-seq order
    /// proves that no unread packed row can contain an earlier sequence.
    pub fn new(iter: I, checked_range: CrsqlSeqRange, max_buf_size: usize) -> Self {
        let remaining_ranges = RangeInclusiveSet::from_iter([checked_range.into()]);
        Self {
            iter,
            changes: Default::default(),
            queried_range: checked_range,
            remaining_ranges,
            last_row_min_seq: None,
            max_buf_size,
            buffered_size: 0,
            all_seen_seqs: RangeInclusiveSet::new(),
            chunk_seqs: RangeInclusiveSet::new(),
            done: false,
        }
    }

    pub fn max_buf_size(&self) -> usize {
        self.max_buf_size
    }

    pub fn set_max_buf_size(&mut self, max_buf_size: usize) {
        self.max_buf_size = max_buf_size;
    }
}

fn sequence_error(message: impl Into<String>) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    )))
}

impl<I> Iterator for ChunkedPackedChanges<I>
where
    I: Iterator<Item = rusqlite::Result<PackedChange>>,
{
    type Item = Result<(PackedChangesetPerTable, Vec<CrsqlSeqRange>), rusqlite::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let details = json!({});
        assert_always!(
            self.changes.is_empty(),
            "iterator for ChunkedPackedChanges still has changes when next() is called",
            &details
        );

        self.buffered_size = 0;

        loop {
            trace!("chunking through packed rows iterator");
            match self.iter.next() {
                Some(Ok(change)) => {
                    trace!("got packed change: {change:?}");

                    if let Some(last_row_min_seq) = self.last_row_min_seq {
                        assert_always!(
                            change.min_seq > last_row_min_seq,
                            "packed changes must be ordered by strictly increasing min_seq",
                            &json!({
                                "last_row_min_seq": last_row_min_seq.0,
                                "change_min_seq": change.min_seq.0,
                            })
                        );
                        if change.min_seq <= last_row_min_seq {
                            self.changes = PackedChangesetPerTable::default();
                            self.done = true;
                            return Some(Err(sequence_error(format!(
                                "packed changes are not ordered by min_seq: previous={}, current={}",
                                last_row_min_seq.0, change.min_seq.0
                            ))));
                        }
                    }
                    self.last_row_min_seq = Some(change.min_seq);

                    if change.min_seq > change.max_seq {
                        assert_unreachable!(
                            "packed change sequence range must not be inverted",
                            &json!({
                                "min_seq": change.min_seq.0,
                                "max_seq": change.max_seq.0,
                            })
                        );
                        self.changes = PackedChangesetPerTable::default();
                        self.done = true;
                        return Some(Err(sequence_error(format!(
                            "packed change has inverted sequence range: min_seq={}, max_seq={}",
                            change.min_seq.0, change.max_seq.0
                        ))));
                    }

                    let sequence_values = match PackedChange::sequence_values(&change.seq) {
                        Ok(values) => values,
                        Err(error) => {
                            assert_unreachable!(
                                "packed sequence values must decode successfully",
                                &json!({ "error": error })
                            );
                            self.changes = PackedChangesetPerTable::default();
                            self.done = true;
                            return Some(Err(sequence_error(format!(
                                "invalid packed sequence values: {error}"
                            ))));
                        }
                    };
                    let mut change_seqs = RangeInclusiveSet::new();
                    for seq in sequence_values {
                        let seq_range = seq..=seq;
                        if change_seqs.overlapping(&seq_range).next().is_some() {
                            assert_unreachable!(
                                "packed changes must not contain duplicate sequence values",
                                &json!({
                                    "sequence": seq.0,
                                })
                            );
                            self.changes = PackedChangesetPerTable::default();
                            self.done = true;
                            return Some(Err(sequence_error(format!(
                                "packed change contains duplicate sequence value: {}",
                                seq.0
                            ))));
                        }
                        change_seqs.insert(seq_range);
                    }

                    let queried_range = RangeInclusive::from(self.queried_range);
                    if change_seqs.overlapping(&queried_range).next().is_none() {
                        // The min/max SQL predicate can return rows whose packed
                        // seq values do not actually intersect the requested range.
                        self.add_checked_through(change.min_seq);
                        if self.remaining_ranges.is_empty() {
                            self.done = true;
                            if self.changes.is_empty() && self.chunk_seqs.is_empty() {
                                return None;
                            }
                            return Some(Ok(self.take_chunk()));
                        }
                        continue;
                    }

                    for range in change_seqs.iter() {
                        if self.all_seen_seqs.overlapping(range).next().is_some() {
                            assert_unreachable!(
                                "packed changes must not contain duplicate sequence values",
                                &json!({
                                    "duplicate_range": format!("{range:?}"),
                                    "seen_sequences": self.all_seen_seqs.len(),
                                })
                            );
                            self.changes = PackedChangesetPerTable::default();
                            self.done = true;
                            return Some(Err(sequence_error(format!(
                                "packed change contains duplicate sequence values: {range:?}"
                            ))));
                        }
                    }

                    for range in change_seqs.iter() {
                        self.remaining_ranges.remove(range.clone());
                    }
                    self.all_seen_seqs.extend(change_seqs.iter().cloned());
                    self.chunk_seqs.extend(change_seqs);
                    self.add_checked_through(change.min_seq);

                    let size = self.changes.insert(change);
                    self.buffered_size += size;

                    if self.remaining_ranges.is_empty() {
                        self.done = true;
                        return Some(Ok(self.take_chunk()));
                    }

                    if self.buffered_size >= self.max_buf_size {
                        return Some(Ok(self.take_chunk()));
                    }
                }
                None => {
                    trace!("no more packed changes to iterate on");
                    break;
                }
                Some(Err(e)) => {
                    self.changes = PackedChangesetPerTable::default();
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }

        self.done = true;
        self.chunk_seqs
            .extend(self.remaining_ranges.iter().cloned());

        if self.changes.is_empty() && self.chunk_seqs.is_empty() {
            return None;
        }

        Some(Ok((
            self.changes.clone(),
            self.chunk_seqs.iter().map(CrsqlSeqRange::from).collect(),
        )))
    }
}

impl<I: Iterator> ChunkedPackedChanges<I> {
    fn take_chunk(&mut self) -> (PackedChangesetPerTable, Vec<CrsqlSeqRange>) {
        let seqs = self.chunk_seqs.iter().map(CrsqlSeqRange::from).collect();
        self.chunk_seqs = RangeInclusiveSet::new();
        (self.changes.drain(), seqs)
    }

    /// Mark the still-unreported portion of the query through this watermark.
    fn add_checked_through(&mut self, watermark: CrsqlSeq) {
        let checked: Vec<_> = self
            .remaining_ranges
            .iter()
            .filter_map(|range| {
                let start = *range.start();
                if start > watermark {
                    return None;
                }
                Some(start..=(*range.end()).min(watermark))
            })
            .collect();

        for range in checked {
            self.chunk_seqs.insert(range.clone());
            self.remaining_ranges.remove(range);
        }
    }
}

pub struct InsertChangesInfo {
    pub db_version: CrsqlDbVersion,
    pub last_seq: CrsqlSeq,
    pub ts: Timestamp,
}

pub fn insert_local_changes(
    agent: &Agent,
    tx: &Connection,
    book_writer: &mut impl DerefMut<Target = BookedVersions>,
) -> Result<Option<InsertChangesInfo>, ChangeError> {
    let actor_id = agent.actor_id();

    let db_version: CrsqlDbVersion = tx
        .prepare_cached("SELECT crsql_peek_next_db_version()")
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_row((), |row| row.get(0))
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    // GROUP BY db_version enables cr-sqlite's scalar seq pushdown in V2 mode,
    // while retaining the ordinary integer MAX(seq) behavior in V1 mode.
    let version_info: (Option<CrsqlSeq>, Option<Timestamp>) = tx
        .prepare_cached(
            "SELECT MAX(seq), MAX(ts) FROM main.crsql_changes \
             WHERE site_id = ? AND db_version = ? \
             GROUP BY db_version",
        )
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_row((agent.actor_id(), db_version), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .unwrap_or((None, None));

    match version_info {
        (None, None) => Ok(None),
        (None, Some(ts)) => {
            warn!("found db_version {db_version} without seq, last ts: {ts:?}");
            Ok(None)
        }
        (Some(last_seq), ts) => {
            let ts = ts.unwrap_or_else(|| {
                warn!("found db_version {db_version} without ts");
                Timestamp::from(agent.clock().new_timestamp())
            });

            debug!("found db_version {db_version} (last seq: {last_seq}, last ts: {ts})");

            let db_versions = db_version..=db_version;

            book_writer
                .insert_db(tx, [db_versions].into())
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(db_version),
                })?;

            Ok(Some(InsertChangesInfo {
                db_version,
                last_seq,
                ts,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::dbsr;
    use crate::broadcast::PackedChangesetPerTable;

    #[test]
    fn test_change_chunker() {
        // empty interator
        let mut chunker = ChunkedChanges::new(vec![].into_iter(), CrsqlSeq(0), CrsqlSeq(100), 50);

        assert_eq!(
            chunker.next(),
            Some(Ok((ChangesetPerTable::default(), dbsr!(0, 100))))
        );
        assert_eq!(chunker.next(), None);

        let changes: Vec<Change> = (0..100)
            .map(|seq| Change {
                seq: CrsqlSeq(seq),
                ..Default::default()
            })
            .collect();

        let (changeset, size) =
            mapped_changeset_from_changes(vec![changes[0].clone(), changes[1].clone()]);
        // 2 iterations
        let mut chunker = ChunkedChanges::new(
            vec![
                Ok(changes[0].clone()),
                Ok(changes[1].clone()),
                Ok(changes[2].clone()),
            ]
            .into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(100),
            size,
        );

        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(0, 1)))));

        let (changeset, _) = mapped_changeset_from_changes(vec![changes[2].clone()]);
        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(2, 100)))));
        assert_eq!(chunker.next(), None);

        let (changeset, size) =
            mapped_changeset_from_changes(vec![changes[0].clone(), changes[1].clone()]);
        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[1].clone())].into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(0),
            size,
        );

        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(0, 0)))));
        assert_eq!(chunker.next(), None);

        let (changeset, size) =
            mapped_changeset_from_changes(vec![changes[0].clone(), changes[2].clone()]);
        // gaps
        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[2].clone())].into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(100),
            size,
        );

        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(0, 100)))));

        assert_eq!(chunker.next(), None);

        // gaps
        let (changeset, _) = mapped_changeset_from_changes(vec![
            changes[2].clone(),
            changes[4].clone(),
            changes[7].clone(),
            changes[8].clone(),
        ]);
        let mut chunker = ChunkedChanges::new(
            vec![
                Ok(changes[2].clone()),
                Ok(changes[4].clone()),
                Ok(changes[7].clone()),
                Ok(changes[8].clone()),
            ]
            .into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(100),
            100000, // just send them all!
        );

        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(0, 100)))));

        assert_eq!(chunker.next(), None);

        // gaps
        let (changeset, size) =
            mapped_changeset_from_changes(vec![changes[2].clone(), changes[4].clone()]);
        let mut chunker = ChunkedChanges::new(
            vec![
                Ok(changes[2].clone()),
                Ok(changes[4].clone()),
                Ok(changes[7].clone()),
                Ok(changes[8].clone()),
            ]
            .into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(10),
            size,
        );

        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(0, 4)))));

        let (changeset, _) =
            mapped_changeset_from_changes(vec![changes[7].clone(), changes[8].clone()]);
        assert_eq!(chunker.next(), Some(Ok((changeset, dbsr!(5, 10)))));

        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_v1_chunker_rejects_duplicate_and_out_of_order_sequences() {
        let make_change = |seq| Change {
            seq: CrsqlSeq(seq),
            ..Default::default()
        };

        for (changes, message) in [
            (
                vec![make_change(0), make_change(0)],
                "duplicate sequence value",
            ),
            (vec![make_change(1), make_change(0)], "out of order"),
        ] {
            let mut chunker = ChunkedChanges::new(
                changes.into_iter().map(Ok).collect::<Vec<_>>().into_iter(),
                CrsqlSeq(0),
                CrsqlSeq(1),
                100_000,
            );
            let error = chunker.next().unwrap().unwrap_err().to_string();
            assert!(error.contains(message), "unexpected error: {error}");
            assert_eq!(chunker.next(), None);
        }
    }

    fn mapped_changeset_from_changes(changes: Vec<Change>) -> (ChangesetPerTable, usize) {
        let mut changeset = ChangesetPerTable::default();
        let mut size = 0;
        for change in changes {
            size += changeset.insert(change);
        }
        (changeset, size)
    }

    #[test]
    fn test_packed_change_max_seq_scalar() {
        // Scalar seq (tombstone/sentinel) — max_seq = seq directly
        let packed = PackedChange {
            seq: SqliteValue::Integer(42),
            ..Default::default()
        };
        assert_eq!(packed.max_seq, CrsqlSeq(0)); // default before compute
        let max = PackedChange::compute_max_seq(&SqliteValue::Integer(42));
        assert_eq!(max, CrsqlSeq(42));
    }

    #[test]
    fn test_packed_change_max_seq_blob() {
        // Packed seq blob: [count=3, val=5, val=10, val=3]
        // Encode using varint format
        let blob: Vec<u8> = vec![3, 5, 10, 3]; // count=3, val=5, val=10, val=3
        let packed = PackedChange {
            seq: SqliteValue::Blob(blob.into()),
            ..Default::default()
        };
        let max = PackedChange::compute_max_seq(&packed.seq);
        assert_eq!(max, CrsqlSeq(10));
    }

    #[test]
    fn test_packed_change_max_seq_null() {
        let max = PackedChange::compute_max_seq(&SqliteValue::Null);
        assert_eq!(max, CrsqlSeq(0));
    }

    #[test]
    fn test_packed_chunker_empty() {
        let mut chunker = ChunkedPackedChanges::new(vec![].into_iter(), dbsr!(0, 100), 50);
        assert_eq!(
            chunker.next(),
            Some(Ok((
                PackedChangesetPerTable::default(),
                vec![dbsr!(0, 100)]
            )))
        );
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_emits_checked_tail_without_lookahead() {
        let change = PackedChange {
            table: TableName("tests".into()),
            pk: vec![1],
            seq: SqliteValue::Integer(0),
            min_seq: CrsqlSeq(0),
            max_seq: CrsqlSeq(0),
            ..Default::default()
        };
        let mut chunker = ChunkedPackedChanges::new(vec![Ok(change)].into_iter(), dbsr!(0, 2), 1);

        let data = chunker.next().unwrap().unwrap();
        assert_eq!(data.1, vec![dbsr!(0, 0)]);
        let checked_tail = chunker.next().unwrap().unwrap();
        assert!(checked_tail.0.is_empty());
        assert_eq!(checked_tail.1, vec![dbsr!(1, 2)]);
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_basic() {
        // Create packed changes with scalar seqs
        let changes: Vec<PackedChange> = (0..5)
            .map(|seq| PackedChange {
                table: TableName("tests".into()),
                pk: vec![seq as u8],
                seq: SqliteValue::Integer(seq as i64),
                max_seq: CrsqlSeq(seq),
                min_seq: CrsqlSeq(seq),
                ..Default::default()
            })
            .collect();

        let mut chunker = ChunkedPackedChanges::new(
            changes
                .iter()
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>()
                .into_iter(),
            dbsr!(0, 4),
            100000, // large enough to send all
        );

        let chunk = chunker.next().unwrap().unwrap();
        assert_eq!(chunk.1, vec![dbsr!(0, 4)]);
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_preserves_out_of_order_packed_sequence_ranges() {
        let changes = [
            (vec![2, 10, 13], 10, 13),
            (vec![1, 11], 11, 11),
            (vec![1, 12], 12, 12),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (seq, min_seq, max_seq))| PackedChange {
            table: TableName("tests".into()),
            pk: vec![index as u8],
            seq: SqliteValue::Blob(seq.into()),
            max_seq: CrsqlSeq(max_seq as u64),
            min_seq: CrsqlSeq(min_seq as u64),
            ..Default::default()
        });
        let mut chunker = ChunkedPackedChanges::new(
            changes.map(Ok).collect::<Vec<_>>().into_iter(),
            dbsr!(10, 12),
            1,
        );

        let first = chunker.next().unwrap().unwrap();
        assert_eq!(first.1, vec![dbsr!(10, 10), dbsr!(13, 13)]);
        let second = chunker.next().unwrap().unwrap();
        assert_eq!(second.1, vec![dbsr!(11, 11)]);
        let third = chunker.next().unwrap().unwrap();
        assert_eq!(third.1, vec![dbsr!(12, 12)]);
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_emits_checked_query_prefix_and_packed_extras() {
        let changes = [(vec![3, 0, 4, 5], 0, 5), (vec![1, 3], 3, 3)]
            .into_iter()
            .enumerate()
            .map(|(index, (seq, min_seq, max_seq))| PackedChange {
                table: TableName("tests".into()),
                pk: vec![index as u8],
                seq: SqliteValue::Blob(seq.into()),
                val: SqliteValue::Text(if index == 0 {
                    "large packed value".repeat(20).into()
                } else {
                    "x".into()
                }),
                max_seq: CrsqlSeq(max_seq),
                min_seq: CrsqlSeq(min_seq),
                ..Default::default()
            });
        let mut chunker = ChunkedPackedChanges::new(
            changes.map(Ok).collect::<Vec<_>>().into_iter(),
            dbsr!(0, 10),
            100,
        );

        let first = chunker.next().unwrap().unwrap();
        assert_eq!(first.1, vec![dbsr!(0, 0), dbsr!(4, 5)]);
        let second = chunker.next().unwrap().unwrap();
        assert_eq!(second.1, vec![dbsr!(1, 3), dbsr!(6, 10)]);
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_drops_min_max_false_positives() {
        let changes = [
            (vec![2, 10, 60], 10, 60), // Envelope overlaps, exact seqs do not.
            (vec![2, 50, 61], 50, 61),
            (vec![1, 51], 51, 51),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (seq, min_seq, max_seq))| PackedChange {
            table: TableName("tests".into()),
            pk: vec![index as u8],
            seq: SqliteValue::Blob(seq.into()),
            max_seq: CrsqlSeq(max_seq),
            min_seq: CrsqlSeq(min_seq),
            ..Default::default()
        });
        let mut chunker = ChunkedPackedChanges::new(
            changes.map(Ok).collect::<Vec<_>>().into_iter(),
            dbsr!(50, 51),
            usize::MAX,
        );

        let (changes, seqs) = chunker.next().unwrap().unwrap();
        assert_eq!(changes.count().get("tests"), Some(&2));
        assert_eq!(seqs, vec![dbsr!(50, 51), dbsr!(61, 61)]);
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_rejects_nonmonotonic_min_seq_rows() {
        let make_change = |seq| PackedChange {
            table: TableName("tests".into()),
            pk: vec![seq as u8],
            seq: SqliteValue::Integer(seq as i64),
            min_seq: CrsqlSeq(seq),
            max_seq: CrsqlSeq(seq),
            ..Default::default()
        };
        let mut chunker = ChunkedPackedChanges::new(
            vec![Ok(make_change(2)), Ok(make_change(1))].into_iter(),
            dbsr!(0, 2),
            1,
        );

        let first = chunker.next().unwrap().unwrap();
        assert_eq!(first.1, vec![dbsr!(0, 2)]);
        let error = chunker.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("not ordered by min_seq"));
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_stops_when_checked_range_is_exhausted() {
        let change = PackedChange {
            table: TableName("tests".into()),
            pk: vec![1],
            seq: SqliteValue::Integer(0),
            min_seq: CrsqlSeq(0),
            max_seq: CrsqlSeq(0),
            ..Default::default()
        };
        let mut chunker = ChunkedPackedChanges::new(
            vec![Ok(change), Err(rusqlite::Error::InvalidQuery)].into_iter(),
            dbsr!(0, 0),
            usize::MAX,
        );

        let chunk = chunker.next().unwrap().unwrap();
        assert_eq!(chunk.1, vec![dbsr!(0, 0)]);
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_rejects_duplicate_terminal_sequences() {
        let changes = [0, 1, 2, 2].into_iter().map(|max_seq| PackedChange {
            table: TableName("tests".into()),
            pk: vec![max_seq as u8],
            seq: SqliteValue::Integer(max_seq),
            max_seq: CrsqlSeq(max_seq as u64),
            min_seq: CrsqlSeq(max_seq as u64),
            ..Default::default()
        });
        let mut chunker = ChunkedPackedChanges::new(
            changes.map(Ok).collect::<Vec<_>>().into_iter(),
            dbsr!(0, 2),
            100_000,
        );

        let error = chunker.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("duplicate sequence value"));
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_chunker_rejects_inverted_sequence_range() {
        let change = PackedChange {
            table: TableName("tests".into()),
            pk: vec![1],
            seq: SqliteValue::Integer(2),
            max_seq: CrsqlSeq(2),
            min_seq: CrsqlSeq(3),
            ..Default::default()
        };
        let mut chunker =
            ChunkedPackedChanges::new(vec![Ok(change)].into_iter(), dbsr!(0, 2), 100_000);

        let error = chunker.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("inverted sequence range"));
    }

    #[test]
    fn test_packed_chunker_stops_after_error() {
        let change = PackedChange {
            table: TableName("tests".into()),
            pk: vec![1],
            max_seq: CrsqlSeq(0),
            min_seq: CrsqlSeq(0),
            ..Default::default()
        };
        let mut chunker = ChunkedPackedChanges::new(
            vec![Ok(change), Err(rusqlite::Error::InvalidQuery)].into_iter(),
            dbsr!(0, 1),
            100_000,
        );

        assert!(chunker.next().unwrap().is_err());
        assert_eq!(chunker.next(), None);
    }

    #[test]
    fn test_packed_change_speedy_roundtrip() {
        use speedy::Readable;

        let change = PackedChange {
            table: TableName("tests".into()),
            pk: vec![1, 2, 3],
            cid: SqliteValue::Text("col1".into()),
            val: SqliteValue::Integer(42),
            col_version: SqliteValue::Integer(1),
            db_version: CrsqlDbVersion(5),
            seq: SqliteValue::Integer(3),
            site_id: [0xAA; 16],
            cl: 1,
            max_seq: CrsqlSeq(3),
            min_seq: CrsqlSeq(3),
        };

        let bytes = change.write_to_vec().unwrap();
        let mut back = PackedChange::read_from_buffer(&bytes).unwrap();
        // min_seq/max_seq are #[speedy(skip)] — recompute after deserialization
        let (min_seq, max_seq) = PackedChange::compute_min_max_seq(&back.seq);
        back.min_seq = min_seq;
        back.max_seq = max_seq;
        assert_eq!(change, back);
    }

    #[test]
    fn test_fullv2packed_changeset_speedy_roundtrip() {
        use crate::actor::ActorId;
        use crate::broadcast::{Changeset, Timestamp};
        use speedy::Readable;
        use uuid::Uuid;

        let mut packed = PackedChangesetPerTable::default();
        packed.insert(PackedChange {
            table: TableName("tests".into()),
            pk: vec![1],
            cid: SqliteValue::Text("text".into()),
            val: SqliteValue::Text("hello".into()),
            col_version: SqliteValue::Integer(1),
            db_version: CrsqlDbVersion(1),
            seq: SqliteValue::Blob(vec![2, 0, 2].into()),
            site_id: [0xBB; 16],
            cl: 1,
            max_seq: CrsqlSeq(2),
            min_seq: CrsqlSeq(0),
        });

        let changeset = Changeset::FullV2Packed {
            actor_id: ActorId(Uuid::nil()),
            version: CrsqlDbVersion(1),
            changes: packed,
            last_seq: CrsqlSeq(2),
            seqs: vec![dbsr!(0, 0), dbsr!(2, 2)],
            ts: Timestamp::from(12345u64),
        };

        let bytes = changeset.write_to_vec().unwrap();
        let back = Changeset::read_from_buffer(&bytes).unwrap();
        assert_eq!(changeset, back);
        assert!(!changeset.is_complete());

        let id = changeset.id();
        let bytes = id.write_to_vec().unwrap();
        let back = crate::broadcast::ChangesetId::read_from_buffer(&bytes).unwrap();
        assert_eq!(id, back);
    }
}
