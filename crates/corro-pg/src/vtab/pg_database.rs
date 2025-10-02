use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, IndexInfo, VTab, VTabConnection, VTabCursor, Values,
};

pub struct PgDatabase {
    oid: i64,
    datname: String,
    datdba: i64,
    encoding: i64,
    datcollate: String,
    datctype: String,
    datistemplate: bool,
    datallowconn: bool,
    datconnlimit: i64,
    datlastsysoid: i64,
    datfrozenxid: i64,
    datminmxid: i64,
    dattablespace: i64,
    datacl: Option<String>,
}

impl PgDatabase {
    pub fn new(name: String) -> Self {
        PgDatabase {
            oid: 1,
            datname: name,
            datdba: 1,
            encoding: 6,
            datcollate: "en_US.UTF-8".into(),
            datctype: "en_US.UTF-8".into(),
            datistemplate: false,
            datallowconn: true,
            datconnlimit: -1,
            datlastsysoid: 14041,
            datfrozenxid: 726,
            datminmxid: 1,
            dattablespace: 1663,
            datacl: None,
        }
    }
}

#[repr(C)]
pub struct PgDatabaseTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    databases: Arc<Vec<PgDatabase>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgDatabaseTable {
    type Aux = Arc<Vec<PgDatabase>>;
    type Cursor = PgDatabaseTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgDatabase>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgDatabaseTable)> {
        let vtab = PgDatabaseTable {
            base: sqlite3_vtab::default(),
            databases: aux.unwrap().clone(),
        };

        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                oid           INTEGER,
                datname       TEXT,
                datdba        INTEGER,
                encoding      INTEGER,
                datcollate    TEXT,
                datctype      TEXT,
                datistemplate INTEGER,
                datallowconn  INTEGER,
                datconnlimit  INTEGER,
                datlastsysoid INTEGER,
                datfrozenxid  INTEGER,
                datminmxid    INTEGER,
                dattablespace INTEGER,
                datacl        TEXT
		    )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgDatabaseTableCursor<'vtab>> {
        Ok(PgDatabaseTableCursor {
            databases: self.databases.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgDatabaseTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    /// The rowid
    row_id: i64,
    databases: &'vtab [PgDatabase],
    phantom: PhantomData<&'vtab PgDatabaseTable>,
}

unsafe impl VTabCursor for PgDatabaseTableCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Values<'_>,
    ) -> rusqlite::Result<()> {
        self.row_id = 0;
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        self.row_id += 1;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.row_id >= self.databases.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(pg_db) = self.databases.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(&pg_db.oid),
                1 => ctx.set_result(&pg_db.datname),
                2 => ctx.set_result(&pg_db.datdba),
                3 => ctx.set_result(&pg_db.encoding),
                4 => ctx.set_result(&pg_db.datcollate),
                5 => ctx.set_result(&pg_db.datctype),
                6 => ctx.set_result(&pg_db.datistemplate),
                7 => ctx.set_result(&pg_db.datallowconn),
                8 => ctx.set_result(&pg_db.datconnlimit),
                9 => ctx.set_result(&pg_db.datlastsysoid),
                10 => ctx.set_result(&pg_db.datfrozenxid),
                11 => ctx.set_result(&pg_db.datminmxid),
                12 => ctx.set_result(&pg_db.dattablespace),
                13 => ctx.set_result(&pg_db.datacl),

                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg database out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
