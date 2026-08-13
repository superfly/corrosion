use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

/// Entry in the `pg_constraint` catalog.
pub struct PgConstraintEntry {
    pub oid: i64,
    pub conname: String,
    /// 'p' = primary key, 'u' = unique, 'f' = foreign key, 'c' = check
    pub contype: &'static str,
    /// OID of the table this constraint is on (pg_class.oid)
    pub conrelid: i64,
    /// OID of the namespace (pg_namespace.oid)
    pub connamespace: i64,
    /// OID of the index supporting this constraint (pg_class.oid), if any
    pub conindid: i64,
    /// 1-based column positions, formatted as a PG array literal e.g. "{1,2}"
    pub conkey: String,
    /// Definition text e.g. "PRIMARY KEY (id)"
    pub consrc: String,
}

/// Loads constraint entries from SQLite metadata.
///
/// `table_oid_map` maps table name → pg_class OID.
/// `index_oid_map` maps index name → pg_class OID.
pub fn load_pg_constraint_entries(
    conn: &rusqlite::Connection,
    table_names: &[String],
    table_oid_map: &std::collections::HashMap<String, i64>,
    index_oid_map: &std::collections::HashMap<String, i64>,
) -> rusqlite::Result<Vec<PgConstraintEntry>> {
    let mut entries = Vec::new();
    let mut next_oid: i64 = 50000; // Constraints get their own OID range

    for table_name in table_names {
        let table_oid = *table_oid_map.get(table_name).unwrap_or(&0);

        // --- Primary key constraints ---
        // Gather PK columns from pragma_table_xinfo.
        let mut pk_cols: Vec<(i64, String)> = Vec::new();
        let mut stmt = conn
            .prepare("SELECT cid, name, pk FROM pragma_table_xinfo(?1) WHERE pk > 0 ORDER BY pk")?;
        let pk_rows = stmt.query_map([table_name], |row| {
            Ok((
                row.get::<_, i64>(0)?,    // cid (0-based)
                row.get::<_, String>(1)?, // column name
            ))
        })?;
        for row in pk_rows {
            let (cid, col_name) = row?;
            pk_cols.push((cid, col_name));
        }

        if !pk_cols.is_empty() {
            // Find the synthetic <table>_pkey index OID
            let pk_index_name = format!("{table_name}_pkey");
            let conindid = *index_oid_map.get(&pk_index_name).unwrap_or(&0);
            let conkey = format!(
                "{{{}}}",
                pk_cols
                    .iter()
                    .map(|(cid, _)| (cid + 1).to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let col_list = pk_cols
                .iter()
                .map(|(_, name)| name.clone())
                .collect::<Vec<_>>()
                .join(", ");
            entries.push(PgConstraintEntry {
                oid: next_oid,
                conname: format!("{table_name}_pkey"),
                contype: "p",
                conrelid: table_oid,
                connamespace: 2200, // "main" namespace
                conindid,
                conkey,
                consrc: format!("PRIMARY KEY ({col_list})"),
            });
            next_oid += 1;
        }

        // --- Unique constraints ---
        // From pragma_index_list where origin = 'u' (unique constraint).
        let mut uq_stmt =
            conn.prepare("SELECT name FROM pragma_index_list(?1) WHERE origin = 'u'")?;
        let uq_rows = uq_stmt.query_map([table_name], |row| row.get::<_, String>(0))?;
        for row in uq_rows {
            let index_name = row?;
            let conindid = *index_oid_map.get(&index_name).unwrap_or(&0);
            // Get the columns in the unique index
            let mut col_stmt =
                conn.prepare("SELECT name FROM pragma_index_info(?1) ORDER BY seqno")?;
            let col_rows = col_stmt.query_map([&index_name], |row| row.get::<_, String>(0))?;
            let mut uq_cols: Vec<String> = Vec::new();
            let mut uq_positions: Vec<i64> = Vec::new();
            for col_row in col_rows {
                let col_name = col_row?;
                uq_cols.push(col_name.clone());
                // Find the column position (1-based) from pragma_table_xinfo
                let pos = conn
                    .query_row(
                        "SELECT cid + 1 FROM pragma_table_xinfo(?1) WHERE name = ?2",
                        [table_name, &col_name],
                        |r| r.get::<_, i64>(0),
                    )
                    .unwrap_or(0);
                uq_positions.push(pos);
            }
            let conkey = format!(
                "{{{}}}",
                uq_positions
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let col_list = uq_cols.join(", ");
            entries.push(PgConstraintEntry {
                oid: next_oid,
                conname: format!("{table_name}_{index_name}_key"),
                contype: "u",
                conrelid: table_oid,
                connamespace: 2200,
                conindid,
                conkey,
                consrc: format!("UNIQUE ({col_list})"),
            });
            next_oid += 1;
        }
    }

    Ok(entries)
}

#[repr(C)]
pub struct PgConstraintTable {
    base: sqlite3_vtab,
    entries: Arc<Vec<PgConstraintEntry>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgConstraintTable {
    type Aux = Arc<Vec<PgConstraintEntry>>;
    type Cursor = PgConstraintTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgConstraintEntry>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgConstraintTable)> {
        let vtab = PgConstraintTable {
            base: sqlite3_vtab::default(),
            entries: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    oid            INTEGER,
                    conname        TEXT,
                    connamespace   INTEGER,
                    conrelid       INTEGER,
                    contype        TEXT,
                    conindid       INTEGER,
                    confrelid      INTEGER,
                    confupdtype    TEXT,
                    confdeltype    TEXT,
                    confmatchtype  TEXT,
                    conislocal     INTEGER,
                    coninhcount    INTEGER,
                    connoinherit   INTEGER,
                    conkey         TEXT,
                    confkey        TEXT,
                    conpfeqop      TEXT,
                    conppeqop      TEXT,
                    conffeqop      TEXT,
                    conexclop      TEXT,
                    conbin         TEXT,
                    consrc         TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgConstraintTableCursor<'vtab>> {
        Ok(PgConstraintTableCursor {
            entries: self.entries.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgConstraintTableCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    entries: &'vtab [PgConstraintEntry],
    phantom: PhantomData<&'vtab PgConstraintTable>,
}

unsafe impl VTabCursor for PgConstraintTableCursor<'_> {
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
                0 => ctx.set_result(&entry.oid),               // oid
                1 => ctx.set_result(&entry.conname),           // conname
                2 => ctx.set_result(&entry.connamespace),      // connamespace
                3 => ctx.set_result(&entry.conrelid),          // conrelid
                4 => ctx.set_result(&entry.contype),           // contype
                5 => ctx.set_result(&entry.conindid),          // conindid
                6 => ctx.set_result(&0i64),                    // confrelid (no FKs)
                7 => ctx.set_result(&Option::<String>::None),  // confupdtype
                8 => ctx.set_result(&Option::<String>::None),  // confdeltype
                9 => ctx.set_result(&Option::<String>::None),  // confmatchtype
                10 => ctx.set_result(&1i64),                   // conislocal
                11 => ctx.set_result(&0i64),                   // coninhcount
                12 => ctx.set_result(&1i64),                   // connoinherit
                13 => ctx.set_result(&entry.conkey),           // conkey
                14 => ctx.set_result(&Option::<String>::None), // confkey
                15 => ctx.set_result(&Option::<String>::None), // conpfeqop
                16 => ctx.set_result(&Option::<String>::None), // conppeqop
                17 => ctx.set_result(&Option::<String>::None), // conffeqop
                18 => ctx.set_result(&Option::<String>::None), // conexclop
                19 => ctx.set_result(&Option::<String>::None), // conbin
                20 => ctx.set_result(&entry.consrc),           // consrc
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_constraint out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
