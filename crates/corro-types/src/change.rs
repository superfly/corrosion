use std::{iter::Peekable, ops::RangeInclusive};

pub use corro_api_types::{row_to_change, Change, SqliteValue};
use corro_base_types::{CrsqlDbVersion, Version};
use rusqlite::{named_params, Connection};
use tracing::{debug, trace};

use crate::{
    agent::{Agent, BookedVersions, ChangeError, VersionsSnapshot},
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

    Ok(Some(InsertChangesInfo {
        version,
        db_version,
        last_seq,
        ts,
        snap,
    }))
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
