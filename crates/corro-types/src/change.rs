use std::iter::Peekable;

use antithesis_sdk::assert_always;
pub use corro_api_types::SqliteValue;
use corro_api_types::{ColumnName, TableName};
use corro_base_types::{CrsqlDbVersion, CrsqlSeqRange};
use rusqlite::{params, Connection, Row};
use serde::{Deserialize, Serialize};
use serde_json::json;
use speedy::{Readable, Writable};
use tracing::{debug, trace, warn};

use crate::{
    actor::ActorId,
    agent::{Agent, BookedVersions, ChangeError, VersionsSnapshot},
    base::CrsqlSeq,
    broadcast::{ChangesetPerTable, Timestamp},
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

pub struct ChunkedChanges<I: Iterator> {
    iter: Peekable<I>,
    changes: ChangesetPerTable,
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
            changes: Default::default(),
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

                    self.last_pushed_seq = change.seq;

                    let size = self.changes.insert(change);
                    self.buffered_size += size;

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
                Some(Err(e)) => return Some(Err(e)),
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

pub struct InsertChangesInfo {
    pub db_version: CrsqlDbVersion,
    pub last_seq: CrsqlSeq,
    pub ts: Timestamp,
    pub snap: VersionsSnapshot,
}

pub fn database_schema_version(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("PRAGMA main.schema_version", (), |row| row.get(0))
}

pub fn database_table_is_crr(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        r#"
        SELECT
            EXISTS (
                SELECT 1
                FROM main.sqlite_schema
                WHERE type = 'table'
                  AND name = ?1 COLLATE NOCASE
            )
            AND EXISTS (
                SELECT 1
                FROM main.sqlite_schema
                WHERE type = 'table'
                  AND name = (?1 || '__crsql_clock') COLLATE NOCASE
            )
            AND 3 = (
                SELECT COUNT(*)
                FROM main.sqlite_schema AS trg
                WHERE trg.type = 'trigger'
                  AND trg.tbl_name = ?1 COLLATE NOCASE
                  AND instr(
                      lower(coalesce(trg.sql, '')),
                      'crsql_internal_sync_bit()'
                  ) > 0
                  AND (
                      (
                          trg.name = (?1 || '__crsql_itrig') COLLATE NOCASE
                          AND instr(lower(trg.sql), 'crsql_after_insert(') > 0
                      )
                      OR (
                          trg.name = (?1 || '__crsql_utrig') COLLATE NOCASE
                          AND instr(lower(trg.sql), 'crsql_after_update(') > 0
                      )
                      OR (
                          trg.name = (?1 || '__crsql_dtrig') COLLATE NOCASE
                          AND instr(lower(trg.sql), 'crsql_after_delete(') > 0
                      )
                  )
            )
        "#,
        [table],
        |row| row.get(0),
    )
}

pub fn database_has_user_triggers(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM main.sqlite_schema AS trg
            WHERE trg.type = 'trigger'
              AND NOT (
                  (
                      EXISTS (
                          SELECT 1
                          FROM main.sqlite_schema AS clock
                          WHERE clock.type = 'table'
                            AND clock.name = trg.tbl_name || '__crsql_clock'
                      )
                      AND instr(
                          lower(coalesce(trg.sql, '')),
                          'crsql_internal_sync_bit()'
                      ) > 0
                      AND (
                          (
                              trg.name = trg.tbl_name || '__crsql_itrig'
                              AND instr(lower(trg.sql), 'crsql_after_insert(') > 0
                          )
                          OR (
                              trg.name = trg.tbl_name || '__crsql_utrig'
                              AND instr(lower(trg.sql), 'crsql_after_update(') > 0
                          )
                          OR (
                              trg.name = trg.tbl_name || '__crsql_dtrig'
                              AND instr(lower(trg.sql), 'crsql_after_delete(') > 0
                          )
                      )
                  )
                  OR (
                      trg.tbl_name = 'crsql_site_id'
                      AND trg.name IN (
                          'crsql_site_id_insert_trig',
                          'crsql_site_id_update_trig',
                          'crsql_site_id_delete_trig'
                      )
                      AND instr(
                          lower(coalesce(trg.sql, '')),
                          'crsql_update_site_id('
                      ) > 0
                  )
              )
        )
        "#,
        (),
        |row| row.get(0),
    )
}

pub fn database_has_foreign_keys(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM main.sqlite_schema
            WHERE type = 'table'
              AND instr(lower(coalesce(sql, '')), 'references') > 0
        )
        "#,
        (),
        |row| row.get(0),
    )
}

#[derive(Debug, Clone)]
struct PendingLocalChange {
    change: Change,
    ts: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct PendingLocalChanges {
    changes: Vec<PendingLocalChange>,
}

impl PendingLocalChanges {
    pub fn capture(conn: &Connection, actor_id: ActorId) -> rusqlite::Result<Self> {
        let db_version: CrsqlDbVersion =
            conn.query_row("SELECT crsql_peek_next_db_version()", (), |row| row.get(0))?;

        let mut stmt = conn.prepare_cached(
            r#"
            SELECT
                "table",
                pk,
                cid,
                val,
                col_version,
                db_version,
                seq,
                site_id,
                cl,
                ts
            FROM main.crsql_changes
            WHERE site_id = ?
              AND db_version = ?
            ORDER BY seq
            "#,
        )?;

        let changes = stmt
            .query_map((actor_id, db_version), |row| {
                Ok(PendingLocalChange {
                    change: row_to_change(row)?,
                    ts: row.get(9)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(Self { changes })
    }

    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    pub fn replay(&self, conn: &Connection) -> rusqlite::Result<Option<CrsqlDbVersion>> {
        if self.changes.is_empty() {
            return Ok(None);
        }

        let db_version: CrsqlDbVersion =
            conn.query_row("SELECT crsql_next_db_version()", (), |row| row.get(0))?;

        let mut stmt = conn.prepare_cached(
            r#"
            INSERT INTO main.crsql_changes
                ("table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )?;

        for pending in &self.changes {
            let change = &pending.change;

            stmt.execute(params![
                &change.table,
                &change.pk,
                &change.cid,
                &change.val,
                change.col_version,
                db_version,
                &change.site_id,
                change.cl,
                change.seq,
                &pending.ts,
            ])?;
        }

        Ok(Some(db_version))
    }
}

pub fn insert_local_changes(
    agent: &Agent,
    tx: &Connection,
    book_writer: &mut tokio::sync::RwLockWriteGuard<'_, BookedVersions>,
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

    let version_info: (Option<CrsqlSeq>, Option<Timestamp>) = tx
        .prepare_cached(
            "SELECT MAX(seq), MAX(ts) FROM main.crsql_changes WHERE site_id = ? AND db_version = ?;",
        )
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?
        .query_row((agent.actor_id(), db_version), |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: Some(actor_id),
            version: None,
        })?;

    match version_info {
        (None, None) => Ok(None),
        (None, Some(ts)) => {
            warn!("found db_version {db_version} without seq, last ts: {ts:?})");
            Ok(None)
        }
        (Some(last_seq), ts) => {
            let ts = ts.unwrap_or_else(|| {
                warn!("found db_version {db_version} without seq, last ts: {ts:?}");
                Timestamp::from(agent.clock().new_timestamp())
            });

            debug!("found db_version {db_version} (last seq: {last_seq}, last ts: {ts})");

            let db_versions = db_version..=db_version;

            let mut snap = book_writer.snapshot();
            snap.insert_db(tx, [db_versions].into())
                .map_err(|source| ChangeError::Rusqlite {
                    source,
                    actor_id: Some(actor_id),
                    version: Some(db_version),
                })?;

            Ok(Some(InsertChangesInfo {
                db_version,
                last_seq,
                ts,
                snap,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::base::dbsr;

    #[test]
    fn test_database_table_is_crr_uses_live_schema() -> Result<(), Box<dyn std::error::Error>> {
        let conn = crate::sqlite::rusqlite_to_crsqlite(Connection::open_in_memory()?)?;

        conn.execute_batch(
            "
            CREATE TABLE tracked (
                id INTEGER NOT NULL PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            SELECT crsql_as_crr('tracked');
            ",
        )?;

        assert!(database_table_is_crr(&conn, "tracked")?);

        conn.execute_batch(
            "
            DROP TABLE tracked;
            CREATE TABLE tracked (
                id INTEGER PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            ",
        )?;

        assert!(!database_table_is_crr(&conn, "tracked")?);

        Ok(())
    }

    #[test]
    fn test_replay_side_effect_detection() -> Result<(), Box<dyn std::error::Error>> {
        let conn = crate::sqlite::rusqlite_to_crsqlite(Connection::open_in_memory()?)?;

        conn.execute_batch(
            "
            CREATE TABLE tracked (
                id INTEGER NOT NULL PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            SELECT crsql_as_crr('tracked');
            ",
        )?;

        assert!(!database_has_user_triggers(&conn)?);
        assert!(!database_has_foreign_keys(&conn)?);

        conn.execute_batch(
            "
            CREATE TABLE exact_name_shadow (id INTEGER PRIMARY KEY);
            CREATE TRIGGER exact_name_shadow__crsql_itrig
            AFTER INSERT ON exact_name_shadow
            BEGIN
                SELECT 1;
            END;
            ",
        )?;

        assert!(database_has_user_triggers(&conn)?);

        conn.execute_batch(
            "
            DROP TRIGGER exact_name_shadow__crsql_itrig;
            CREATE TABLE fk_parent (id INTEGER PRIMARY KEY);
            CREATE TABLE fk_child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER REFERENCES fk_parent(id)
            );
            ",
        )?;

        assert!(!database_has_user_triggers(&conn)?);
        assert!(database_has_foreign_keys(&conn)?);

        Ok(())
    }

    #[test]
    fn test_pending_local_changes_capture_and_replay() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("pending-local-changes.db");

        let speculative = crate::sqlite::rusqlite_to_crsqlite(Connection::open(&path)?)?;

        speculative.execute_batch(
            "
            CREATE TABLE foo (
                id INTEGER NOT NULL PRIMARY KEY,
                value TEXT NOT NULL DEFAULT ''
            );
            SELECT crsql_as_crr('foo');
            BEGIN CONCURRENT;
            ",
        )?;

        let actor_id: ActorId =
            speculative.query_row("SELECT crsql_site_id()", (), |row| row.get(0))?;

        let _: String =
            speculative.query_row("SELECT crsql_set_ts('401')", (), |row| row.get(0))?;

        speculative.execute("INSERT INTO foo (id, value) VALUES (1, 'speculative')", ())?;

        let pending = PendingLocalChanges::capture(&speculative, actor_id)?;
        assert!(!pending.is_empty());

        speculative.execute_batch("ROLLBACK;")?;

        let canonical = crate::sqlite::rusqlite_to_crsqlite(Connection::open(&path)?)?;

        canonical.execute_batch("BEGIN IMMEDIATE;")?;

        let db_version = pending
            .replay(&canonical)?
            .expect("expected a reserved db version");

        let rows: i64 = canonical.query_row(
            "SELECT COUNT(*) FROM crsql_changes WHERE site_id = ? AND db_version = ?",
            (actor_id, db_version),
            |row| row.get(0),
        )?;

        assert_eq!(rows, 1);

        canonical.execute_batch("COMMIT;")?;

        let value: String =
            canonical.query_row("SELECT value FROM foo WHERE id = 1", (), |row| row.get(0))?;

        assert_eq!(value, "speculative");

        Ok(())
    }

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

        let (changeset, size) = mapped_changeset_from_changes(vec![changes[0].clone()]);
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

    fn mapped_changeset_from_changes(changes: Vec<Change>) -> (ChangesetPerTable, usize) {
        let mut changeset = ChangesetPerTable::default();
        let mut size = 0;
        for change in changes {
            size += changeset.insert(change);
        }
        (changeset, size)
    }
}
