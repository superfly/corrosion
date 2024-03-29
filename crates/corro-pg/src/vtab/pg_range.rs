use std::{marker::PhantomData, os::raw::c_int};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, IndexInfo, VTab, VTabConnection, VTabCursor, Values,
};

#[repr(C)]
pub struct PgRangeTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for PgRangeTable {
    type Aux = ();
    type Cursor = PgRangeTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgRangeTable)> {
        let vtab = PgRangeTable {
            base: sqlite3_vtab::default(),
        };

        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                rngtypid 	INTEGER,
                rngsubtype	INTEGER,
                rngmultitypid	INTEGER,
                rngcollation	INTEGER,
                rngsubopc	INTEGER,
                rngcanonical	TEXT,
                rngsubdiff	TEXT
		    )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgRangeTableCursor<'vtab>> {
        Ok(PgRangeTableCursor::default())
    }
}

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
        _args: &Values<'_>,
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
