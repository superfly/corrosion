use std::{marker::PhantomData, os::raw::c_int};

use rusqlite::vtab::{sqlite3_vtab, sqlite3_vtab_cursor, Filters, VTabCursor};

/// PostgreSQL access method entries.  SQLite only uses btree, so we expose
/// a single row matching PostgreSQL's built-in btree AM (OID 403).
const AM_ENTRIES: [(i64, &str); 1] = [(403, "btree")];

#[repr(C)]
pub struct PgAmTable {
    base: sqlite3_vtab,
}

crate::pg_mod!(
    PgAmTable,
    PgAmTableCursor,
    "oid        INTEGER,
amname     TEXT,
amhandler  INTEGER,
amtype     TEXT"
);

#[derive(Default)]
#[repr(C)]
pub struct PgAmTableCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    _phantom: PhantomData<&'vtab PgAmTable>,
}

unsafe impl VTabCursor for PgAmTableCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        self.row_id = 0;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_id >= AM_ENTRIES.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some((oid, amname)) = AM_ENTRIES.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(oid),
                1 => ctx.set_result(amname),
                2 => ctx.set_result(Option::<i64>::None), // amhandler
                3 => ctx.set_result("i"),                 // amtype: index
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_am out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
