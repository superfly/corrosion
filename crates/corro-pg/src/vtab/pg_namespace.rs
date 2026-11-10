use std::{marker::PhantomData, os::raw::c_int};

use rusqlite::vtab::{sqlite3_vtab, sqlite3_vtab_cursor, Filters, VTabCursor};

const PG_NAMESPACES: &[(i64, &str, i64, &str)] = &[
    (99, "pg_toast", 10, ""),
    (11, "pg_catalog", 10, ""),
    (2200, "main", 10, ""),
    (13427, "information_schema", 10, ""),
];

#[repr(C)]
pub struct PgNamespaceTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
}

crate::pg_mod!(
    PgNamespaceTable,
    PgNamespaceTableCursor,
    "oid 	INTEGER,
nspname  TEXT,
nspowner INTEGER,
nspacl   TEXT"
);

#[derive(Default)]
#[repr(C)]
pub struct PgNamespaceTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    /// The rowid
    row_id: i64,
    phantom: PhantomData<&'vtab PgNamespaceTable>,
}

unsafe impl VTabCursor for PgNamespaceTableCursor<'_> {
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
        self.row_id >= PG_NAMESPACES.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(pg_ns) = PG_NAMESPACES.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(pg_ns.0),
                1 => ctx.set_result(pg_ns.1),
                2 => ctx.set_result(pg_ns.2),
                3 => ctx.set_result(pg_ns.3),
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg namespace out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
