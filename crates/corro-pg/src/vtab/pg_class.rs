use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

/// Starting OID for user-created objects (matches PostgreSQL's
/// `FirstNormalObjectId`).
const FIRST_USER_OID: i64 = 16384;

/// Entry in the `pg_class` catalog representing a user table.
pub struct PgClassEntry {
    pub oid: i64,
    pub relname: String,
    pub relnatts: i64,
}

pub fn load_pg_class_entries(
    conn: &rusqlite::Connection,
    table_names: &[String],
) -> rusqlite::Result<Vec<PgClassEntry>> {
    let mut entries = Vec::with_capacity(table_names.len());
    for (i, table_name) in table_names.iter().enumerate() {
        let oid = FIRST_USER_OID + i as i64;
        // Count user-visible columns (cid >= 0, not hidden) via pragma_table_xinfo.
        let natts: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_xinfo(?1) WHERE cid >= 0 AND hidden = 0",
                [table_name],
                |row| row.get(0),
            )
            .unwrap_or(0);
        entries.push(PgClassEntry {
            oid,
            relname: table_name.clone(),
            relnatts: natts,
        });
    }
    Ok(entries)
}

#[repr(C)]
pub struct PgClassTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    entries: Arc<Vec<PgClassEntry>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgClassTable {
    type Aux = Arc<Vec<PgClassEntry>>;
    type Cursor = PgClassTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgClassEntry>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgClassTable)> {
        let vtab = PgClassTable {
            base: sqlite3_vtab::default(),
            entries: aux.unwrap().clone(),
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
        Ok(PgClassTableCursor {
            entries: self.entries.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgClassTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    row_id: i64,
    entries: &'vtab [PgClassEntry],
    phantom: PhantomData<&'vtab PgClassTable>,
}

unsafe impl VTabCursor for PgClassTableCursor<'_> {
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
        self.row_id >= self.entries.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(entry) = self.entries.get(self.row_id as usize) {
            // 2200 is the OID of the "main" namespace (see pg_namespace.rs).
            match col {
                0 => ctx.set_result(&entry.oid),               // oid
                1 => ctx.set_result(&entry.relname),           // relname
                2 => ctx.set_result(&2200i64),                 // relnamespace -> "main"
                3 => ctx.set_result(&Option::<i64>::None),     // reltype
                4 => ctx.set_result(&Option::<i64>::None),     // reloftype
                5 => ctx.set_result(&10i64),                   // relowner
                6 => ctx.set_result(&Option::<i64>::None),     // relam
                7 => ctx.set_result(&Option::<i64>::None),     // relfilenode
                8 => ctx.set_result(&0i64),                    // reltablespace
                9 => ctx.set_result(&0i64),                    // relpages
                10 => ctx.set_result(&0.0f64),                 // reltuples
                11 => ctx.set_result(&0i64),                   // relallvisible
                12 => ctx.set_result(&Option::<i64>::None),    // reltoastrelid
                13 => ctx.set_result(&0i64),                   // relhasindex
                14 => ctx.set_result(&0i64),                   // relisshared
                15 => ctx.set_result(&"p"),                    // relpersistence: permanent
                16 => ctx.set_result(&"r"),                    // relkind: ordinary table
                17 => ctx.set_result(&entry.relnatts),         // relnatts
                18 => ctx.set_result(&0i64),                   // relchecks
                19 => ctx.set_result(&0i64),                   // relhasrules
                20 => ctx.set_result(&0i64),                   // relhastriggers
                21 => ctx.set_result(&0i64),                   // relhassubclass
                22 => ctx.set_result(&0i64),                   // relrowsecurity
                23 => ctx.set_result(&0i64),                   // relforcerowsecurity
                24 => ctx.set_result(&1i64),                   // relispopulated
                25 => ctx.set_result(&"d"),                    // relreplident: default
                26 => ctx.set_result(&0i64),                   // relispartition
                27 => ctx.set_result(&Option::<i64>::None),    // relrewrite
                28 => ctx.set_result(&Option::<i64>::None),    // relfrozenxid
                29 => ctx.set_result(&Option::<i64>::None),    // relminmxid
                30 => ctx.set_result(&Option::<String>::None), // relacl
                31 => ctx.set_result(&Option::<String>::None), // reloptions
                32 => ctx.set_result(&Option::<String>::None), // relpartbound
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_class out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
