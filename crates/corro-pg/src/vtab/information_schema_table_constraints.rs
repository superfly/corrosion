use std::{collections::BTreeSet, marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

use super::information_schema_columns::InformationSchemaColumn;

pub struct InformationSchemaTableConstraint {
    table_name: String,
    constraint_name: String,
}

pub fn load_information_schema_table_constraints(
    columns: &[InformationSchemaColumn],
) -> Vec<InformationSchemaTableConstraint> {
    let mut primary_key_tables = BTreeSet::new();
    for column in columns {
        if column.primary_key_ordinal.is_some() {
            primary_key_tables.insert(column.table_name.clone());
        }
    }

    primary_key_tables
        .into_iter()
        .map(|table_name| InformationSchemaTableConstraint {
            constraint_name: format!("{table_name}_pkey"),
            table_name,
        })
        .collect()
}

#[repr(C)]
pub struct InformationSchemaTableConstraintsTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    constraints: Arc<Vec<InformationSchemaTableConstraint>>,
}

unsafe impl<'vtab> VTab<'vtab> for InformationSchemaTableConstraintsTable {
    type Aux = Arc<Vec<InformationSchemaTableConstraint>>;
    type Cursor = InformationSchemaTableConstraintsCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<InformationSchemaTableConstraint>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, InformationSchemaTableConstraintsTable)> {
        let vtab = InformationSchemaTableConstraintsTable {
            base: sqlite3_vtab::default(),
            constraints: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    constraint_catalog TEXT,
                    constraint_schema  TEXT,
                    constraint_name    TEXT,
                    table_catalog      TEXT,
                    table_schema       TEXT,
                    table_name         TEXT,
                    constraint_type    TEXT,
                    is_deferrable      TEXT,
                    initially_deferred TEXT,
                    enforced           TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<InformationSchemaTableConstraintsCursor<'vtab>> {
        Ok(InformationSchemaTableConstraintsCursor {
            constraints: self.constraints.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct InformationSchemaTableConstraintsCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    row_id: i64,
    constraints: &'vtab [InformationSchemaTableConstraint],
    phantom: PhantomData<&'vtab InformationSchemaTableConstraintsTable>,
}

unsafe impl VTabCursor for InformationSchemaTableConstraintsCursor<'_> {
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
        self.row_id >= self.constraints.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(constraint) = self.constraints.get(self.row_id as usize) {
            match col {
                0 | 3 => ctx.set_result(&"state"),
                1 | 4 => ctx.set_result(&"main"),
                2 => ctx.set_result(&constraint.constraint_name),
                5 => ctx.set_result(&constraint.table_name),
                6 => ctx.set_result(&"PRIMARY KEY"),
                7 | 8 => ctx.set_result(&"NO"),
                9 => ctx.set_result(&"YES"),
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "information schema table constraint out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
