use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

/// Entry in the `pg_proc` catalog representing a function.
pub struct PgProcEntry {
    pub oid: i64,
    pub proname: String,
    pub pronamespace: i64,
    pub proargtypes: String,
    pub prorettype: i64,
    /// 'f' = function, 'a' = aggregate, 'w' = window, 'p' = procedure
    pub prokind: char,
    pub pronargs: i64,
    pub proretset: bool,
    pub provolatile: char,
    pub proisagg: bool,
    pub proiswindow: bool,
    pub procost: f64,
    pub prolang: i64,
    pub prosrc: String,
    pub proallargtypes: Option<String>,
    pub proargmodes: Option<String>,
    pub proargnames: Option<String>,
}

/// Loads function entries from `pragma_function_list()`.
/// Only exposes scalar (s) and aggregate (a/w) functions, skipping
/// builtin operators like `->`.
pub fn load_pg_proc_entries(conn: &rusqlite::Connection) -> rusqlite::Result<Vec<PgProcEntry>> {
    let mut stmt =
        conn.prepare("SELECT name, narg, type FROM pragma_function_list() ORDER BY name")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?, // name
            row.get::<_, i64>(1)?,    // narg
            row.get::<_, String>(2)?, // type: 's'=scalar, 'a'=aggregate, 'w'=window
        ))
    })?;

    let mut entries = Vec::new();
    let mut oid: i64 = 1000; // Functions start at OID 1000 in PG
    for row in rows {
        let (name, narg, ftype) = row?;
        // Skip operators (non-identifier names like ->, ||, etc.)
        if !name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false)
        {
            continue;
        }
        let (prokind, proisagg, proiswindow) = match ftype.as_str() {
            "a" => ('a', true, false),
            "w" => ('w', false, true),
            _ => ('f', false, false),
        };
        entries.push(PgProcEntry {
            oid,
            proname: name.clone(),
            pronamespace: 2200, // "main" namespace
            proargtypes: if narg > 0 {
                (0..narg).map(|_| "0").collect::<Vec<_>>().join(" ")
            } else {
                String::new()
            },
            prorettype: 25, // text OID as a default return type
            prokind,
            pronargs: narg,
            proretset: false,
            provolatile: 'v', // volatile
            proisagg,
            proiswindow,
            procost: 100.0,
            prolang: 12, // internal language OID
            prosrc: String::new(),
            proallargtypes: None,
            proargmodes: None,
            proargnames: None,
        });
        oid += 1;
    }
    Ok(entries)
}

#[repr(C)]
pub struct PgProcTable {
    base: sqlite3_vtab,
    entries: Arc<Vec<PgProcEntry>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgProcTable {
    type Aux = Arc<Vec<PgProcEntry>>;
    type Cursor = PgProcCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgProcEntry>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgProcTable)> {
        let vtab = PgProcTable {
            base: sqlite3_vtab::default(),
            entries: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    oid              INTEGER,
                    proname          TEXT,
                    pronamespace     INTEGER,
                    proowner         INTEGER,
                    prolang          INTEGER,
                    procost          REAL,
                    prorows          REAL,
                    provariadic      INTEGER,
                    prosupport       INTEGER,
                    prokind          TEXT,
                    prosecdef        INTEGER,
                    proleakproof     INTEGER,
                    proisstrict      INTEGER,
                    proretset        INTEGER,
                    provolatile      TEXT,
                    proparallel      TEXT,
                    proisagg         INTEGER,
                    proiswindow      INTEGER,
                    prosecdef        INTEGER,
                    pronargs         INTEGER,
                    prorettype       INTEGER,
                    proargtypes      TEXT,
                    proallargtypes   TEXT,
                    proargmodes      TEXT,
                    proargnames      TEXT,
                    prosrc           TEXT,
                    proconfig        TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgProcCursor<'vtab>> {
        Ok(PgProcCursor {
            entries: self.entries.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgProcCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    entries: &'vtab [PgProcEntry],
    phantom: PhantomData<&'vtab PgProcTable>,
}

unsafe impl VTabCursor for PgProcCursor<'_> {
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
            match col {
                0 => ctx.set_result(&entry.oid),                 // oid
                1 => ctx.set_result(&entry.proname),             // proname
                2 => ctx.set_result(&entry.pronamespace),        // pronamespace
                3 => ctx.set_result(&10i64),                     // proowner (postgres)
                4 => ctx.set_result(&entry.prolang),             // prolang
                5 => ctx.set_result(&entry.procost),             // procost
                6 => ctx.set_result(&0i64),                      // prorows
                7 => ctx.set_result(&0i64),                      // provariadic
                8 => ctx.set_result(&0i64),                      // prosupport
                9 => ctx.set_result(&entry.prokind.to_string()), // prokind
                10 => ctx.set_result(&0i64),                     // prosecdef
                11 => ctx.set_result(&0i64),                     // proleakproof
                12 => ctx.set_result(&1i64),                     // proisstrict
                13 => ctx.set_result(&if entry.proretset { 1i64 } else { 0i64 }), // proretset
                14 => ctx.set_result(&entry.provolatile.to_string()), // provolatile
                15 => ctx.set_result(&"s"),                      // proparallel: safe
                16 => ctx.set_result(&if entry.proisagg { 1i64 } else { 0i64 }), // proisagg
                17 => ctx.set_result(&if entry.proiswindow { 1i64 } else { 0i64 }), // proiswindow
                18 => ctx.set_result(&0i64), // prosecdef (duplicate, kept for column alignment)
                19 => ctx.set_result(&entry.pronargs), // pronargs
                20 => ctx.set_result(&entry.prorettype), // prorettype
                21 => ctx.set_result(&entry.proargtypes), // proargtypes
                22 => ctx.set_result(&entry.proallargtypes), // proallargtypes
                23 => ctx.set_result(&entry.proargmodes), // proargmodes
                24 => ctx.set_result(&entry.proargnames), // proargnames
                25 => ctx.set_result(&entry.prosrc), // prosrc
                26 => ctx.set_result(&Option::<String>::None), // proconfig
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_proc out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
