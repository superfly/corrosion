use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

pub struct PgLanguageEntry {
    pub oid: i64,
    pub lanname: String,
}

pub fn load_pg_language_entries() -> Vec<PgLanguageEntry> {
    vec![
        PgLanguageEntry {
            oid: 10,
            lanname: "internal".into(),
        },
        PgLanguageEntry {
            oid: 11,
            lanname: "c".into(),
        },
        PgLanguageEntry {
            oid: 12,
            lanname: "sql".into(),
        },
    ]
}

#[repr(C)]
pub struct PgLanguageTable {
    base: sqlite3_vtab,
    entries: Arc<Vec<PgLanguageEntry>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgLanguageTable {
    type Aux = Arc<Vec<PgLanguageEntry>>;
    type Cursor = PgLanguageCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgLanguageEntry>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgLanguageTable)> {
        let vtab = PgLanguageTable {
            base: sqlite3_vtab::default(),
            entries: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    oid       INTEGER,
                    lanname   TEXT,
                    lanowner  INTEGER,
                    lanispl   INTEGER,
                    lanpltrusted INTEGER,
                    lanplcallfoid INTEGER,
                    laninline INTEGER,
                    lanvalidator INTEGER,
                    lanacl    TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgLanguageCursor<'vtab>> {
        Ok(PgLanguageCursor {
            entries: self.entries.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgLanguageCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    entries: &'vtab [PgLanguageEntry],
    phantom: PhantomData<&'vtab PgLanguageTable>,
}

unsafe impl VTabCursor for PgLanguageCursor<'_> {
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
                0 => ctx.set_result(&entry.oid),              // oid
                1 => ctx.set_result(&entry.lanname),          // lanname
                2 => ctx.set_result(&10i64),                  // lanowner
                3 => ctx.set_result(&0i64),                   // lanispl
                4 => ctx.set_result(&0i64),                   // lanpltrusted
                5 => ctx.set_result(&0i64),                   // lanplcallfoid
                6 => ctx.set_result(&0i64),                   // laninline
                7 => ctx.set_result(&0i64),                   // lanvalidator
                8 => ctx.set_result(&Option::<String>::None), // lanacl
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_language out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
