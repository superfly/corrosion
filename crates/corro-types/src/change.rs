use std::{iter::Peekable, ops::RangeInclusive, time::Instant};

pub use corro_api_types::{row_to_change, Change, SqliteValue};
use corro_base_types::{CrsqlDbVersion, Version};
use rangemap::RangeInclusiveSet;
use rusqlite::{named_params, params, Connection};
use tracing::{debug, trace, warn};

use crate::{
    actor::ActorId,
    agent::{find_overwritten_versions, Agent, BookedVersions, ChangeError, VersionsSnapshot},
    base::CrsqlSeq,
    broadcast::Timestamp,
};

pub struct ChunkedChanges<I: Iterator> {
    iter: Peekable<I>,
    changes: Vec<Change>,
    last_pushed_seq: CrsqlSeq,
    last_start_seq: CrsqlSeq,
    last_seq: CrsqlSeq,
    max_buf_size: usize,
    buffered_size: usize,
    done: bool,
}

impl<I> ChunkedChanges<I>
where
    I: Iterator,
{
    pub fn new(iter: I, start_seq: CrsqlSeq, last_seq: CrsqlSeq, max_buf_size: usize) -> Self {
        Self {
            iter: iter.peekable(),
            changes: vec![],
            last_pushed_seq: CrsqlSeq(0),
            last_start_seq: start_seq,
            last_seq,
            max_buf_size,
            buffered_size: 0,
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
    type Item = Result<(Vec<Change>, RangeInclusive<CrsqlSeq>), rusqlite::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        // previously marked as done because the Rows iterator returned None
        if self.done {
            return None;
        }

        debug_assert!(self.changes.is_empty());

        // reset the buffered size
        self.buffered_size = 0;

        loop {
            trace!("chunking through the rows iterator");
            match self.iter.next() {
                Some(Ok(change)) => {
                    trace!("got change: {change:?}");

                    self.last_pushed_seq = change.seq;

                    self.buffered_size += change.estimated_byte_size();

                    self.changes.push(change);

                    if self.last_pushed_seq == self.last_seq {
                        // this was the last seq! break early
                        break;
                    }

                    if self.buffered_size >= self.max_buf_size {
                        // chunking it up
                        let start_seq = self.last_start_seq;

                        if self.iter.peek().is_none() {
                            // no more rows, break early
                            break;
                        }

                        // prepare for next round! we're not done...
                        self.last_start_seq = self.last_pushed_seq + 1;

                        return Some(Ok((
                            self.changes.drain(..).collect(),
                            start_seq..=self.last_pushed_seq,
                        )));
                    }
                }
                None => {
                    // probably not going to happen since we peek at the next and end early
                    // break out of the loop, don't return, there might be buffered changes
                    break;
                }
                Some(Err(e)) => return Some(Err(e)),
            }
        }

        self.done = true;

        // return buffered changes
        Some(Ok((
            self.changes.clone(),                // no need to drain here like before
            self.last_start_seq..=self.last_seq, // even if empty, this is all we have still applied
        )))
    }
}

pub const MAX_CHANGES_BYTE_SIZE: usize = 8 * 1024;

pub struct InsertChangesInfo {
    pub version: Version,
    pub db_version: CrsqlDbVersion,
    pub last_seq: CrsqlSeq,
    pub ts: Timestamp,
    pub snap: VersionsSnapshot,
}

pub fn insert_local_changes(
    agent: &Agent,
    tx: &Connection,
    book_writer: &mut tokio::sync::RwLockWriteGuard<'_, BookedVersions>,
) -> Result<Option<InsertChangesInfo>, ChangeError> {
    let actor_id = agent.actor_id();
    let ts = Timestamp::from(agent.clock().new_timestamp());

    let db_version: CrsqlDbVersion = tx
        .prepare_cached("SELECT crsql_next_db_version()")
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

    let has_changes: bool = tx
        .prepare_cached("SELECT EXISTS(SELECT 1 FROM crsql_changes WHERE db_version = ?);")
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_row([db_version], |row| row.get(0))
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    if !has_changes {
        return Ok(None);
    }

    let last_version = book_writer.last().unwrap_or_default();
    trace!("last_version: {last_version}");
    let version = last_version + 1;
    trace!("version: {version}");

    let last_seq: CrsqlSeq = tx
        .prepare_cached("SELECT MAX(seq) FROM crsql_changes WHERE db_version = ?")
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: Some(version),
        })?
        .query_row([db_version], |row| row.get(0))
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: Some(version),
        })?;

    let versions = version..=version;

    tx.prepare_cached(
        r#"
                INSERT INTO __corro_bookkeeping (actor_id, start_version, db_version, last_seq, ts)
                    VALUES (:actor_id, :start_version, :db_version, :last_seq, :ts);
            "#,
    )
    .map_err(|source| ChangeError::Rusqlite {
        source,
        actor_id: Some(actor_id),
        version: Some(version),
    })?
    .execute(named_params! {
        ":actor_id": actor_id,
        ":start_version": version,
        ":db_version": db_version,
        ":last_seq": last_seq,
        ":ts": ts
    })
    .map_err(|source| ChangeError::Rusqlite {
        source,
        actor_id: Some(actor_id),
        version: Some(version),
    })?;

    debug!(%actor_id, %version, %db_version, "inserted local bookkeeping row!");

    let mut snap = book_writer.snapshot();
    snap.insert_db(tx, [versions].into())
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: Some(version),
        })?;

    let overwritten = find_overwritten_versions(tx).map_err(|source| ChangeError::Rusqlite {
        source,
        actor_id: Some(actor_id),
        version: Some(version),
    })?;

    for (actor_id, versions_set) in overwritten {
        for versions in versions_set {
            store_empty_changeset(tx, actor_id, versions)?;
        }
    }

    Ok(Some(InsertChangesInfo {
        version,
        db_version,
        last_seq,
        ts,
        snap,
    }))
}

pub fn store_empty_changeset(
    conn: &Connection,
    actor_id: ActorId,
    versions: RangeInclusive<Version>,
) -> Result<usize, ChangeError> {
    trace!(%actor_id, "attempting to delete versions range {versions:?}");
    let start = Instant::now();
    // first, delete "current" versions, they're now gone!
    let deleted: Vec<RangeInclusive<Version>> = conn
        .prepare_cached(
            "
        DELETE FROM __corro_bookkeeping
            WHERE
                actor_id = :actor_id AND
                start_version >= COALESCE((
                    -- try to find the previous range
                    SELECT start_version
                        FROM __corro_bookkeeping
                        WHERE
                            actor_id = :actor_id AND
                            start_version < :start -- AND end_version IS NOT NULL
                        ORDER BY start_version DESC
                        LIMIT 1
                ), 1)
                AND
                start_version <= COALESCE((
                    -- try to find the next range
                    SELECT start_version
                        FROM __corro_bookkeeping
                        WHERE
                            actor_id = :actor_id AND
                            start_version > :end -- AND end_version IS NOT NULL
                        ORDER BY start_version ASC
                        LIMIT 1
                ), :end + 1)
                AND
                (
                    -- [:start]---[start_version]---[:end]
                    ( start_version BETWEEN :start AND :end ) OR

                    -- [start_version]---[:start]---[:end]---[end_version]
                    ( start_version <= :start AND end_version >= :end ) OR

                    -- [:start]---[start_version]---[:end]---[end_version]
                    ( start_version <= :end AND end_version >= :end ) OR

                    -- [:start]---[end_version]---[:end]
                    ( end_version BETWEEN :start AND :end ) OR

                    -- ---[:end][start_version]---[end_version]
                    ( start_version = :end + 1 AND end_version IS NOT NULL ) OR

                    -- [end_version][:start]---
                    ( end_version = :start - 1 )
                )
            RETURNING start_version, end_version",
        )
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_map(
            named_params![
                ":actor_id": actor_id,
                ":start": versions.start(),
                ":end": versions.end(),
            ],
            |row| {
                let start = row.get(0)?;
                Ok(start..=row.get::<_, Option<Version>>(1)?.unwrap_or(start))
            },
        )
        .and_then(|rows| rows.collect::<rusqlite::Result<Vec<_>>>())
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    debug!(%actor_id, "deleted: {deleted:?} in {:?}", start.elapsed());

    // re-compute the ranges
    let mut new_ranges = RangeInclusiveSet::from_iter(deleted);
    new_ranges.insert(versions);

    debug!(%actor_id, "new ranges: {new_ranges:?}");

    // we should never have deleted non-contiguous ranges, abort!
    if new_ranges.len() > 1 {
        warn!("deleted non-contiguous ranges! {new_ranges:?}");
        return Err(ChangeError::NonContiguousDelete);
    }

    let mut inserted = 0;

    // println!("inserting: {new_ranges:?}");

    for range in new_ranges {
        // insert cleared versions
        inserted += conn
        .prepare_cached(
            "
                INSERT INTO __corro_bookkeeping (actor_id, start_version, end_version, db_version, last_seq, ts)
                    VALUES (?, ?, ?, NULL, NULL, NULL);
            ",
        ).map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .execute(params![actor_id, range.start(), range.end()]).map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;
    }

    debug!(%actor_id, "stored empty changesets in {:?} (total)", start.elapsed());

    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_change_chunker() {
        // empty interator
        let mut chunker = ChunkedChanges::new(vec![].into_iter(), CrsqlSeq(0), CrsqlSeq(100), 50);

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![], CrsqlSeq(0)..=CrsqlSeq(100))))
        );
        assert_eq!(chunker.next(), None);

        let changes: Vec<Change> = (CrsqlSeq(0)..CrsqlSeq(100))
            .map(|seq| Change {
                seq,
                ..Default::default()
            })
            .collect();

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
            changes[0].estimated_byte_size() + changes[1].estimated_byte_size(),
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((
                vec![changes[0].clone(), changes[1].clone()],
                CrsqlSeq(0)..=CrsqlSeq(1)
            )))
        );
        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[2].clone()], CrsqlSeq(2)..=CrsqlSeq(100))))
        );
        assert_eq!(chunker.next(), None);

        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[1].clone())].into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(0),
            changes[0].estimated_byte_size(),
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((vec![changes[0].clone()], CrsqlSeq(0)..=CrsqlSeq(0))))
        );
        assert_eq!(chunker.next(), None);

        // gaps
        let mut chunker = ChunkedChanges::new(
            vec![Ok(changes[0].clone()), Ok(changes[2].clone())].into_iter(),
            CrsqlSeq(0),
            CrsqlSeq(100),
            changes[0].estimated_byte_size() + changes[2].estimated_byte_size(),
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((
                vec![changes[0].clone(), changes[2].clone()],
                CrsqlSeq(0)..=CrsqlSeq(100)
            )))
        );

        assert_eq!(chunker.next(), None);

        // gaps
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

        assert_eq!(
            chunker.next(),
            Some(Ok((
                vec![
                    changes[2].clone(),
                    changes[4].clone(),
                    changes[7].clone(),
                    changes[8].clone()
                ],
                CrsqlSeq(0)..=CrsqlSeq(100)
            )))
        );

        assert_eq!(chunker.next(), None);

        // gaps
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
            changes[2].estimated_byte_size() + changes[4].estimated_byte_size(),
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((
                vec![changes[2].clone(), changes[4].clone(),],
                CrsqlSeq(0)..=CrsqlSeq(4)
            )))
        );

        assert_eq!(
            chunker.next(),
            Some(Ok((
                vec![changes[7].clone(), changes[8].clone(),],
                CrsqlSeq(5)..=CrsqlSeq(10)
            )))
        );

        assert_eq!(chunker.next(), None);
    }
}
