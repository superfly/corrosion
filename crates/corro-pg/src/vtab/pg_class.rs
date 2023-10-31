use std::{marker::PhantomData, os::raw::c_int};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, IndexInfo, VTab, VTabConnection, VTabCursor, Values,
};

#[repr(C)]
pub struct PgClassTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for PgClassTable {
    type Aux = ();
    type Cursor = PgClassTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgClassTable)> {
        let vtab = PgClassTable {
            base: sqlite3_vtab::default(),
        };

        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                oid                 INTEGER,
                relname             TEXT,
                relnamespace        INTEGER,
                reltype             INTEGER,
                reloftype           INTEGER,
                relowner            INTEGER,
                relam               INTEGER,
                relfilenode         INTEGER,
                reltablespace       INTEGER,
                relpages            INTEGER,
                reltuples           REAL,
                relallvisible       INTEGER,
                reltoastrelid       INTEGER,
                relhasindex         INTEGER,
                relisshared         INTEGER,
                relpersistence      TEXT,
                relkind             TEXT,
                relnatts            INTEGER,
                relchecks           INTEGER,
                relhasrules         INTEGER,
                relhastriggers      INTEGER,
                relhassubclass      INTEGER,
                relrowsecurity      INTEGER,
                relforcerowsecurity INTEGER,
                relispopulated      INTEGER,
                relreplident        TEXT,
                relispartition      INTEGER,
                relrewrite          INTEGER,
                relfrozenxid        INTEGER,
                relminmxid          INTEGER,
                relacl              TEXT,
                reloptions          TEXT,
                relpartbound        TEXT
		    )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgClassTableCursor<'vtab>> {
        Ok(PgClassTableCursor::default())
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgClassTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    /// The rowid
    row_id: i64,
    phantom: PhantomData<&'vtab PgClassTable>,
}

unsafe impl VTabCursor for PgClassTableCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Values<'_>,
    ) -> rusqlite::Result<()> {
        self.row_id = 1;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        true // no rows...
    }

    fn column(&self, _ctx: &mut rusqlite::vtab::Context, _col: c_int) -> rusqlite::Result<()> {
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
