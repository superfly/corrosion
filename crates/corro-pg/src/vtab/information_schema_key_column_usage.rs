use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

use super::information_schema_columns::InformationSchemaColumn;

pub struct InformationSchemaKeyColumnUsageEntry {
    pub constraint_name: String,
    pub table_name: String,
    pub column_name: String,
    pub ordinal_position: i64,
}

pub fn load_information_schema_key_column_usage(
    columns: &[InformationSchemaColumn],
) -> Vec<InformationSchemaKeyColumnUsageEntry> {
    let mut entries = Vec::new();
    for column in columns {
        if let Some(pk_ordinal) = column.primary_key_ordinal {
            entries.push(InformationSchemaKeyColumnUsageEntry {
                constraint_name: format!("{}_pkey", column.table_name),
                table_name: column.table_name.clone(),
                column_name: column.column_name.clone(),
                ordinal_position: pk_ordinal,
            });
        }
    }
    entries
}

#[repr(C)]
pub struct InformationSchemaKeyColumnUsageTable {
    base: sqlite3_vtab,
    entries: Arc<Vec<InformationSchemaKeyColumnUsageEntry>>,
}

unsafe impl<'vtab> VTab<'vtab> for InformationSchemaKeyColumnUsageTable {
    type Aux = Arc<Vec<InformationSchemaKeyColumnUsageEntry>>;
    type Cursor = InformationSchemaKeyColumnUsageCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<InformationSchemaKeyColumnUsageEntry>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, InformationSchemaKeyColumnUsageTable)> {
        let vtab = InformationSchemaKeyColumnUsageTable {
            base: sqlite3_vtab::default(),
            entries: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    constraint_catalog              TEXT,
                    constraint_schema               TEXT,
                    constraint_name                 TEXT,
                    table_catalog                   TEXT,
                    table_schema                    TEXT,
                    table_name                      TEXT,
                    column_name                     TEXT,
                    ordinal_position                INTEGER,
                    position_in_unique_constraint   INTEGER
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<InformationSchemaKeyColumnUsageCursor<'vtab>> {
        Ok(InformationSchemaKeyColumnUsageCursor {
            entries: self.entries.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct InformationSchemaKeyColumnUsageCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    entries: &'vtab [InformationSchemaKeyColumnUsageEntry],
    phantom: PhantomData<&'vtab InformationSchemaKeyColumnUsageTable>,
}

unsafe impl VTabCursor for InformationSchemaKeyColumnUsageCursor<'_> {
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
                0 | 3 => ctx.set_result(&"state"),
                1 | 4 => ctx.set_result(&"main"),
                2 => ctx.set_result(&entry.constraint_name),
                5 => ctx.set_result(&entry.table_name),
                6 => ctx.set_result(&entry.column_name),
                7 => ctx.set_result(&entry.ordinal_position),
                8 => ctx.set_result(&Option::<i64>::None), // position_in_unique_constraint (null for PK)
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "information_schema.key_column_usage out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
