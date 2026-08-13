use std::{marker::PhantomData, os::raw::c_int};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

/// A virtual table that always returns zero rows.  Used for PostgreSQL system
/// catalogs that we don't populate yet (e.g. `pg_index`, `pg_am`, `pg_proc`)
/// but that external tools reference in JOINs.  Returning an empty result set
/// is safer than erroring because the tool can degrade gracefully.
#[repr(C)]
pub struct EmptyCatalogTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    ddl: &'static str,
}

unsafe impl<'vtab> VTab<'vtab> for EmptyCatalogTable {
    type Aux = &'static str;
    type Cursor = EmptyCatalogCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&&'static str>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, EmptyCatalogTable)> {
        let ddl = aux.unwrap();
        let vtab = EmptyCatalogTable {
            base: sqlite3_vtab::default(),
            ddl,
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;
        Ok((format!("CREATE TABLE {table_name} {ddl}"), vtab))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<EmptyCatalogCursor<'vtab>> {
        Ok(EmptyCatalogCursor {
            base: sqlite3_vtab_cursor::default(),
            _phantom: PhantomData,
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct EmptyCatalogCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    _phantom: PhantomData<&'vtab EmptyCatalogTable>,
}

unsafe impl VTabCursor for EmptyCatalogCursor<'_> {
    fn filter(
        &mut self,
        _idx_num: c_int,
        _idx_str: Option<&str>,
        _args: &Filters<'_>,
    ) -> rusqlite::Result<()> {
        Ok(())
    }

    fn next(&mut self) -> rusqlite::Result<()> {
        Ok(())
    }

    fn eof(&self) -> bool {
        true // always empty
    }

    fn column(&self, _ctx: &mut rusqlite::vtab::Context, _col: c_int) -> rusqlite::Result<()> {
        Ok(())
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(0)
    }
}

/// DDL for `pg_extension` – matches the PostgreSQL 14 column set.
pub const PG_EXTENSION_DDL: &str = "(oid INTEGER, extname TEXT, extowner INTEGER, extnamespace INTEGER, extrelocatable INTEGER, extversion TEXT, extconfig TEXT, extcondition TEXT)";

/// DDL for `pg_statio_user_tables` – a PostgreSQL statistics view.
pub const PG_STATIO_USER_TABLES_DDL: &str = "(relid INTEGER, schemaname TEXT, relname TEXT, heap_blks_read INTEGER, heap_blks_hit INTEGER, idx_blks_read INTEGER, idx_blks_hit INTEGER, toast_blks_read INTEGER, toast_blks_hit INTEGER, tidx_blks_read INTEGER, tidx_blks_hit INTEGER)";

/// DDL for `pg_description` – stores comments on database objects.
pub const PG_DESCRIPTION_DDL: &str =
    "(objoid INTEGER, classoid INTEGER, objsubid INTEGER, description TEXT)";

/// DDL for `pg_shdescription` – stores comments on shared database objects.
pub const PG_SHDESCRIPTION_DDL: &str = "(objoid INTEGER, classoid INTEGER, description TEXT)";

/// DDL for `pg_attrdef` – stores default values for columns.
pub const PG_ATTRDEF_DDL: &str =
    "(oid INTEGER, adrelid INTEGER, adnum INTEGER, adbin TEXT, adsrc TEXT)";

/// DDL for `pg_trigger` – stores trigger definitions.
pub const PG_TRIGGER_DDL: &str = "(oid INTEGER, tgrelid INTEGER, tgname TEXT, tgfoid INTEGER, tgtype INTEGER, tgenabled TEXT, tgisinternal INTEGER, tgconstrrelid INTEGER, tgconstrindid INTEGER, tgconstraint INTEGER, tgdeferrable INTEGER, tginitdeferred INTEGER, tgnargs INTEGER, tgattr TEXT, tgargs BLOB, tgqual TEXT, tgoldtable TEXT, tgnewtable TEXT)";
