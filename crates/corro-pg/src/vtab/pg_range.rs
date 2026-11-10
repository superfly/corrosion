use std::{marker::PhantomData, os::raw::c_int};

use rusqlite::vtab::{sqlite3_vtab, sqlite3_vtab_cursor, Filters, VTabCursor};

#[repr(C)]
pub struct PgRangeTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
}

crate::pg_mod!(
    PgRangeTable,
    PgRangeTableCursor,
    "rngtypid 	INTEGER,
rngsubtype	INTEGER,
rngmultitypid	INTEGER,
rngcollation	INTEGER,
rngsubopc	INTEGER,
rngcanonical	TEXT,
rngsubdiff	TEXT"
);

#[derive(Default)]
#[repr(C)]
pub struct PgRangeTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    phantom: PhantomData<&'vtab PgRangeTable>,
}

unsafe impl VTabCursor for PgRangeTableCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        Ok(())
    }

    fn eof(&self) -> bool {
        true // no rows...
    }

    fn column(&self, _ctx: &mut rusqlite::vtab::Context, _col: c_int) -> rusqlite::Result<()> {
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(1)
    }
}
