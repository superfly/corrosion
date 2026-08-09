use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

#[repr(C)]
pub struct InformationSchemaTablesTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    table_names: Arc<Vec<String>>,
}

unsafe impl<'vtab> VTab<'vtab> for InformationSchemaTablesTable {
    type Aux = Arc<Vec<String>>;
    type Cursor = InformationSchemaTablesCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<String>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, InformationSchemaTablesTable)> {
        let vtab = InformationSchemaTablesTable {
            base: sqlite3_vtab::default(),
            table_names: aux.unwrap().clone(),
        };

        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    table_catalog                TEXT,
                    table_schema                 TEXT,
                    table_name                   TEXT,
                    table_type                   TEXT,
                    self_referencing_column_name TEXT,
                    reference_generation         TEXT,
                    user_defined_type_catalog    TEXT,
                    user_defined_type_schema     TEXT,
                    user_defined_type_name       TEXT,
                    is_insertable_into            TEXT,
                    is_typed                      TEXT,
                    commit_action                 TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<InformationSchemaTablesCursor<'vtab>> {
        Ok(InformationSchemaTablesCursor {
            table_names: self.table_names.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct InformationSchemaTablesCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    row_id: i64,
    table_names: &'vtab [String],
    phantom: PhantomData<&'vtab InformationSchemaTablesTable>,
}

unsafe impl VTabCursor for InformationSchemaTablesCursor<'_> {
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
        self.row_id >= self.table_names.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(table_name) = self.table_names.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(&"state"),
                1 => ctx.set_result(&"public"),
                2 => ctx.set_result(table_name),
                3 => ctx.set_result(&"BASE TABLE"),
                4..=8 | 11 => ctx.set_result(&Option::<String>::None),
                9 => ctx.set_result(&"YES"),
                10 => ctx.set_result(&"NO"),
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "information schema table out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
