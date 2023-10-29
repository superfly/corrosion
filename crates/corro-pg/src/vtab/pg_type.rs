use std::{marker::PhantomData, os::raw::c_int};

use postgres_types::Type;
use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, IndexInfo, VTab, VTabConnection, VTabCursor, Values,
};

#[repr(C)]
pub struct PgTypeTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
}

unsafe impl<'vtab> VTab<'vtab> for PgTypeTable {
    type Aux = ();
    type Cursor = PgTypeTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        _aux: Option<&()>,
        _args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgTypeTable)> {
        let vtab = PgTypeTable {
            base: sqlite3_vtab::default(),
        };

        Ok((
            "CREATE TABLE pg_type (
			oid            INTEGER,
			typname        TEXT,
			typnamespace   INTEGER,
			typowner       INTEGER,
			typlen         INTEGER,
			typbyval       INTEGER,
			typtype        TEXT,
			typcategory    TEXT,
			typispreferred INTEGER,
			typisdefined   INTEGER,
			typdelim       TEXT,
			typrelid       INTEGER,
			typelem        INTEGER,
			typarray       INTEGER,
			typinput       TEXT,
			typoutput      TEXT,
			typreceive     TEXT,
			typsend        TEXT,
			typmodin       TEXT,
			typmodout      TEXT,
			typanalyze     TEXT,
			typalign       TEXT,
			typstorage     TEXT,
			typnotnull     INTEGER,
			typbasetype    INTEGER,
			typtypmod      INTEGER,
			typndims       INTEGER,
			typcollation   INTEGER,
			typdefaultbin  TEXT,
			typdefault     TEXT,
			typacl         TEXT
		)"
            .into(),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgTypeTableCursor<'vtab>> {
        Ok(PgTypeTableCursor::default())
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgTypeTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    /// The rowid
    row_id: i64,
    phantom: PhantomData<&'vtab PgTypeTable>,
}

struct PgType(Type);

impl PgType {
    fn oid(&self) -> u32 {
        self.0.oid()
    }

    fn typname(&self) -> &str {
        self.0.name()
    }

    fn typnamespace(&self) -> &'static str {
        "11"
    }
    fn typowner(&self) -> &'static str {
        "10"
    }
    fn typlen(&self) -> i16 {
        match self.0 {
            Type::BOOL => 1,
            Type::BYTEA => -1,
            Type::INT2 => 2,
            Type::INT4 => 4,
            Type::INT8 => 8,
            Type::TEXT => -1,
            Type::VARCHAR => -1,
            Type::FLOAT4 => 4,
            Type::FLOAT8 => 8,
            Type::TIMESTAMP => 8,
            Type::JSON => -1,
            Type::JSONB => -1,
            _ => {
                // TODO: not default...
                Default::default()
            }
        }
    }
    fn typbyval(&self) -> bool {
        match self.0 {
            Type::BOOL => true,
            Type::BYTEA => false,
            Type::INT2 => true,
            Type::INT4 => true,
            Type::INT8 => true,
            Type::TEXT => false,
            Type::VARCHAR => false,
            Type::FLOAT4 => true,
            Type::FLOAT8 => true,
            Type::TIMESTAMP => true,
            Type::JSON => false,
            Type::JSONB => false,
            _ => {
                // TODO: not default...
                Default::default()
            }
        }
    }
    fn typtype(&self) -> &'static str {
        "b"
    }
    fn typcategory(&self) -> &'static str {
        match self.0 {
            Type::BOOL => "B",
            Type::BYTEA => "U",
            Type::INT2 => "N",
            Type::INT4 => "N",
            Type::INT8 => "N",
            Type::TEXT => "S",
            Type::VARCHAR => "S",
            Type::FLOAT4 => "N",
            Type::FLOAT8 => "N",
            Type::TIMESTAMP => "D",
            Type::JSON => "U",
            Type::JSONB => "U",
            _ => {
                // TODO: not default...
                Default::default()
            }
        }
    }
    fn typispreferred(&self) -> bool {
        // TODO: not default...
        Default::default()
    }
    fn typisdefined(&self) -> bool {
        true
    }
    fn typdelim(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typrelid(&self) -> i64 {
        0
    }
    fn typelem(&self) -> &'static str {
        "0"
    }
    fn typarray(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typinput(&self) -> String {
        format!("{}in", self.0.name())
    }
    fn typoutput(&self) -> String {
        format!("{}out", self.0.name())
    }
    fn typreceive(&self) -> String {
        format!("{}recv", self.0.name())
    }
    fn typsend(&self) -> String {
        format!("{}send", self.0.name())
    }
    fn typmodin(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typmodout(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typanalyze(&self) -> &'static str {
        "-"
    }
    fn typalign(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typstorage(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typnotnull(&self) -> bool {
        false
    }
    fn typbasetype(&self) -> &'static str {
        "0"
    }
    fn typtypmod(&self) -> i32 {
        -1
    }
    fn typndims(&self) -> i32 {
        0
    }
    fn typcollation(&self) -> &'static str {
        // TODO: not default...
        Default::default()
    }
    fn typdefaultbin(&self) -> rusqlite::types::Null {
        rusqlite::types::Null
    }
    fn typdefault(&self) -> Option<&'static str> {
        None
    }
    fn typacl(&self) -> rusqlite::types::Null {
        rusqlite::types::Null
    }
}

const PG_TYPES: &[PgType] = &[
    // TINY INT
    PgType(Type::BOOL),
    // BLOB
    PgType(Type::BYTEA),
    // INTS
    PgType(Type::INT2),
    PgType(Type::INT4),
    PgType(Type::INT8),
    // TEXT
    PgType(Type::TEXT),
    PgType(Type::VARCHAR),
    // REAL
    PgType(Type::FLOAT4),
    PgType(Type::FLOAT8),
    PgType(Type::TIMESTAMP),
    PgType(Type::JSON),
    PgType(Type::JSONB),
];

unsafe impl VTabCursor for PgTypeTableCursor<'_> {
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
        self.row_id >= PG_TYPES.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(pg_type) = PG_TYPES.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(&pg_type.oid()),
                1 => ctx.set_result(&pg_type.typname()), // pg_type.typname
                2 => ctx.set_result(&pg_type.typnamespace()), // pg_type.typnamespace
                3 => ctx.set_result(&pg_type.typowner()), // pg_type.typowner
                4 => ctx.set_result(&pg_type.typlen()),  // pg_type.typlen
                5 => ctx.set_result(&pg_type.typbyval()), // pg_type.typbyval
                6 => ctx.set_result(&pg_type.typtype()), // pg_type.typtype
                7 => ctx.set_result(&pg_type.typcategory()), // pg_type.typcategory
                8 => ctx.set_result(&pg_type.typispreferred()), // pg_type.typispreferred
                9 => ctx.set_result(&pg_type.typisdefined()), // pg_type.typisdefined
                10 => ctx.set_result(&pg_type.typdelim()), // pg_type.typdelim
                11 => ctx.set_result(&pg_type.typrelid()), // pg_type.typrelid
                12 => ctx.set_result(&pg_type.typelem()), // pg_type.typelem
                13 => ctx.set_result(&pg_type.typarray()), // pg_type.typarray
                14 => ctx.set_result(&pg_type.typinput()), // pg_type.typinput
                15 => ctx.set_result(&pg_type.typoutput()), // pg_type.typoutput
                16 => ctx.set_result(&pg_type.typreceive()), // pg_type.typreceive
                17 => ctx.set_result(&pg_type.typsend()), // pg_type.typsend
                18 => ctx.set_result(&pg_type.typmodin()), // pg_type.typmodin
                19 => ctx.set_result(&pg_type.typmodout()), // pg_type.typmodout
                20 => ctx.set_result(&pg_type.typanalyze()), // pg_type.typanalyze
                21 => ctx.set_result(&pg_type.typalign()), // pg_type.typalign
                22 => ctx.set_result(&pg_type.typstorage()), // pg_type.typstorage
                23 => ctx.set_result(&pg_type.typnotnull()), // pg_type.typnotnull
                24 => ctx.set_result(&pg_type.typbasetype()), // pg_type.typbasetype
                25 => ctx.set_result(&pg_type.typtypmod()), // pg_type.typtypmod
                26 => ctx.set_result(&pg_type.typndims()), // pg_type.typndims
                27 => ctx.set_result(&pg_type.typcollation()), // pg_type.typcollation
                28 => ctx.set_result(&pg_type.typdefaultbin()), // pg_type.typdefaultbin
                29 => ctx.set_result(&pg_type.typdefault()), // pg_type.typdefault
                30 => ctx.set_result(&pg_type.typacl()), // pg_type.typacl
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg type out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}

#[cfg(test)]
mod tests {
    use rusqlite::vtab::eponymous_only_module;

    use super::*;

    #[test]
    fn query_pg_type() -> rusqlite::Result<()> {
        let conn = rusqlite::Connection::open_in_memory()?;
        conn.create_module("pg_type", eponymous_only_module::<PgTypeTable>(), None)?;

        let mut prepped = conn.prepare("SELECT * FROM pg_type;")?;
        let mut rows = prepped.query([])?;

        let mut idx = 0;
        loop {
            let row = match rows.next()? {
                Some(row) => row,
                None => break,
            };

            let pg_type = &PG_TYPES[idx];

            assert_eq!(row.get::<_, u32>(0)?, pg_type.oid());
            assert_eq!(row.get::<_, String>(1)?, pg_type.typname());
            assert_eq!(row.get::<_, String>(2)?, pg_type.typnamespace());
            assert_eq!(row.get::<_, String>(3)?, pg_type.typowner());
            assert_eq!(row.get::<_, i16>(4)?, pg_type.typlen());
            assert_eq!(row.get::<_, bool>(5)?, pg_type.typbyval());
            assert_eq!(row.get::<_, String>(6)?, pg_type.typtype());
            assert_eq!(row.get::<_, String>(7)?, pg_type.typcategory());
            assert_eq!(row.get::<_, bool>(8)?, pg_type.typispreferred());
            assert_eq!(row.get::<_, bool>(9)?, pg_type.typisdefined());
            assert_eq!(row.get::<_, String>(10)?, pg_type.typdelim());
            assert_eq!(row.get::<_, i64>(11)?, pg_type.typrelid());
            assert_eq!(row.get::<_, String>(12)?, pg_type.typelem());
            assert_eq!(row.get::<_, String>(13)?, pg_type.typarray());
            assert_eq!(row.get::<_, String>(14)?, pg_type.typinput());
            assert_eq!(row.get::<_, String>(15)?, pg_type.typoutput());
            assert_eq!(row.get::<_, String>(16)?, pg_type.typreceive());
            assert_eq!(row.get::<_, String>(17)?, pg_type.typsend());
            assert_eq!(row.get::<_, String>(18)?, pg_type.typmodin());
            assert_eq!(row.get::<_, String>(19)?, pg_type.typmodout());
            assert_eq!(row.get::<_, String>(20)?, pg_type.typanalyze());
            assert_eq!(row.get::<_, String>(21)?, pg_type.typalign());
            assert_eq!(row.get::<_, String>(22)?, pg_type.typstorage());
            assert_eq!(row.get::<_, bool>(23)?, pg_type.typnotnull());
            assert_eq!(row.get::<_, String>(24)?, pg_type.typbasetype());
            assert_eq!(row.get::<_, i32>(25)?, pg_type.typtypmod());
            assert_eq!(row.get::<_, i32>(26)?, pg_type.typndims());
            assert_eq!(row.get::<_, String>(27)?, pg_type.typcollation());
            // assert_eq!(row.get::<_, String>(28)?, pg_type.typdefaultbin(), );
            assert_eq!(
                row.get::<_, Option<String>>(29)?.as_deref(),
                pg_type.typdefault()
            );
            // assert_eq!(row.get::<_, String>(30)?, pg_type.typacl(), );

            idx += 1;
        }

        assert_eq!(idx, PG_TYPES.len());

        Ok(())
    }
}
