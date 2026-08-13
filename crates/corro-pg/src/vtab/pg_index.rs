use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

/// Entry in the `pg_index` catalog representing a single index.
pub struct PgIndexEntry {
    /// OID of the index entry in pg_class.
    pub indexrelid: i64,
    /// OID of the table the index is on (in pg_class).
    pub indrelid: i64,
    /// Whether the index is unique.
    pub indisunique: bool,
    /// Whether the index is a primary key.
    pub indisprimary: bool,
    /// Total number of columns in the index (key + included).
    pub indnatts: i64,
    /// Number of key columns (not included).
    pub indnkeyatts: i64,
    /// Column numbers (1-based attnum from pg_attribute), space-separated.
    pub indkey: String,
    /// Partial index predicate (NULL if not partial).
    pub indpred: Option<String>,
}

/// Loads index entries from `sqlite_master`, mapping index and table names to
/// their OIDs in `pg_class`.
///
/// `table_oid_map` maps table name → pg_class OID.
/// `index_oid_map` maps index name → pg_class OID.
pub fn load_pg_index_entries(
    conn: &rusqlite::Connection,
    table_oid_map: &std::collections::HashMap<String, i64>,
    index_oid_map: &std::collections::HashMap<String, i64>,
) -> rusqlite::Result<Vec<PgIndexEntry>> {
    let mut stmt = conn.prepare(
        "SELECT m.name, m.tbl_name, p.\"unique\", p.origin \
         FROM sqlite_master m \
         LEFT JOIN pragma_index_list(m.tbl_name) p ON p.name = m.name \
         WHERE m.type = 'index' \
         ORDER BY m.name",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,         // index name
            row.get::<_, String>(1)?,         // table name
            row.get::<_, Option<i64>>(2)?,    // unique (from pragma_index_list)
            row.get::<_, Option<String>>(3)?, // origin: 'c'=create_index, 'u'=unique_constraint, 'pk'=primary_key
        ))
    })?;

    let mut entries = Vec::new();
    for row in rows {
        let (index_name, tbl_name, unique, origin) = row?;
        let indexrelid = *index_oid_map.get(&index_name).unwrap_or(&0);
        let indrelid = *table_oid_map.get(&tbl_name).unwrap_or(&0);
        let is_unique = unique.unwrap_or(0) != 0;
        // 'pk' origin means this index was auto-created for a PRIMARY KEY.
        let is_primary = origin.as_deref() == Some("pk");

        // Get column count and column names from pragma_index_info.
        let mut col_stmt = conn.prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")?;
        let col_names: Vec<String> = col_stmt
            .query_map([&index_name], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        let nkeyatts = col_names.len() as i64;

        // Map column names to 1-based attnum from pragma_table_xinfo.
        let mut indkey_parts: Vec<String> = Vec::new();
        for col_name in &col_names {
            let attnum = conn
                .query_row(
                    "SELECT cid + 1 FROM pragma_table_xinfo(?1) WHERE name = ?2",
                    [&tbl_name, col_name],
                    |r| r.get::<_, i64>(0),
                )
                .unwrap_or(0);
            indkey_parts.push(attnum.to_string());
        }

        entries.push(PgIndexEntry {
            indexrelid,
            indrelid,
            indisunique: is_unique,
            indisprimary: is_primary,
            indnatts: nkeyatts,
            indnkeyatts: nkeyatts,
            indkey: indkey_parts.join(" "),
            indpred: None,
        });
    }

    Ok(entries)
}

#[repr(C)]
pub struct PgIndexTable {
    base: sqlite3_vtab,
    entries: Arc<Vec<PgIndexEntry>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgIndexTable {
    type Aux = Arc<Vec<PgIndexEntry>>;
    type Cursor = PgIndexTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgIndexEntry>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgIndexTable)> {
        let vtab = PgIndexTable {
            base: sqlite3_vtab::default(),
            entries: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    indexrelid      INTEGER,
                    indrelid        INTEGER,
                    indnatts        INTEGER,
                    indnkeyatts     INTEGER,
                    indisunique     INTEGER,
                    indisprimary    INTEGER,
                    indisexclusion  INTEGER,
                    indimmediate    INTEGER,
                    indisclustered  INTEGER,
                    indisvalid      INTEGER,
                    indcheckxmin    INTEGER,
                    indisready      INTEGER,
                    indislive       INTEGER,
                    indisreplident  INTEGER,
                    indkey          TEXT,
                    indcollation    TEXT,
                    indclass        TEXT,
                    indoption       TEXT,
                    indexprs        TEXT,
                    indpred         TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgIndexTableCursor<'vtab>> {
        Ok(PgIndexTableCursor {
            entries: self.entries.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgIndexTableCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    entries: &'vtab [PgIndexEntry],
    phantom: PhantomData<&'vtab PgIndexTable>,
}

unsafe impl VTabCursor for PgIndexTableCursor<'_> {
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
                0 => ctx.set_result(&entry.indexrelid),  // indexrelid
                1 => ctx.set_result(&entry.indrelid),    // indrelid
                2 => ctx.set_result(&entry.indnatts),    // indnatts
                3 => ctx.set_result(&entry.indnkeyatts), // indnkeyatts
                4 => ctx.set_result(&if entry.indisunique { 1i64 } else { 0i64 }), // indisunique
                5 => ctx.set_result(&if entry.indisprimary { 1i64 } else { 0i64 }), // indisprimary
                6 => ctx.set_result(&0i64),              // indisexclusion
                7 => ctx.set_result(&1i64),              // indimmediate
                8 => ctx.set_result(&0i64),              // indisclustered
                9 => ctx.set_result(&1i64),              // indisvalid
                10 => ctx.set_result(&0i64),             // indcheckxmin
                11 => ctx.set_result(&1i64),             // indisready
                12 => ctx.set_result(&1i64),             // indislive
                13 => ctx.set_result(&0i64),             // indisreplident
                14 => ctx.set_result(&entry.indkey),     // indkey
                15 => ctx.set_result(&Option::<String>::None), // indcollation
                16 => ctx.set_result(&Option::<String>::None), // indclass
                17 => ctx.set_result(&Option::<String>::None), // indoption
                18 => ctx.set_result(&Option::<String>::None), // indexprs
                19 => ctx.set_result(&entry.indpred),    // indpred
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_index out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
