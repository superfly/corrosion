use std::{collections::HashMap, marker::PhantomData, os::raw::c_int, sync::Arc};

use postgres_types::Type;
use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

use super::information_schema_columns::InformationSchemaColumn;
use super::pg_class::PgClassEntry;

/// Maps a PostgreSQL `udt_name` (as exposed by `information_schema.columns`)
/// to the corresponding `postgres_types::Type` so we can report the correct
/// `atttypid` OID in `pg_attribute`.
fn udt_name_to_type(udt_name: &str) -> Type {
    match udt_name {
        "int8" => Type::INT8,
        "int4" => Type::INT4,
        "int2" => Type::INT2,
        "varchar" => Type::VARCHAR,
        "text" => Type::TEXT,
        "bpchar" => Type::BPCHAR,
        "bytea" => Type::BYTEA,
        "float8" => Type::FLOAT8,
        "float4" => Type::FLOAT4,
        "bool" => Type::BOOL,
        "timestamp" => Type::TIMESTAMP,
        "date" => Type::DATE,
        "time" => Type::TIME,
        "numeric" => Type::NUMERIC,
        "json" => Type::JSON,
        "jsonb" => Type::JSONB,
        _ => Type::TEXT,
    }
}

pub struct PgAttribute {
    pub attrelid: i64,
    pub attname: String,
    pub atttypid: u32,
    pub attlen: i16,
    pub attnum: i64,
    pub attnotnull: bool,
    pub atthasdef: bool,
    pub attgenerated: char,
}

pub fn load_pg_attributes(
    columns: &[InformationSchemaColumn],
    class_entries: &[PgClassEntry],
) -> Vec<PgAttribute> {
    let table_oids: HashMap<&str, i64> = class_entries
        .iter()
        .map(|e| (e.relname.as_str(), e.oid))
        .collect();

    let mut attributes = Vec::with_capacity(columns.len());
    for col in columns {
        let attrelid = table_oids
            .get(col.table_name.as_str())
            .copied()
            .unwrap_or(0);
        let pg_type = udt_name_to_type(&col.udt_name);
        let attlen = match pg_type {
            Type::BOOL => 1,
            Type::INT2 => 2,
            Type::INT4 => 4,
            Type::INT8 => 8,
            Type::FLOAT4 => 4,
            Type::FLOAT8 => 8,
            Type::TIMESTAMP => 8,
            Type::DATE => 4,
            Type::TIME => 8,
            _ => -1, // variable-length
        };
        attributes.push(PgAttribute {
            attrelid,
            attname: col.column_name.clone(),
            atttypid: pg_type.oid(),
            attlen,
            attnum: col.ordinal_position,
            attnotnull: !col.nullable,
            atthasdef: col.column_default.is_some(),
            // 's' = stored generated column, empty otherwise
            attgenerated: if col.generated { 's' } else { '\0' },
        });
    }
    attributes
}

#[repr(C)]
pub struct PgAttributeTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    attributes: Arc<Vec<PgAttribute>>,
}

unsafe impl<'vtab> VTab<'vtab> for PgAttributeTable {
    type Aux = Arc<Vec<PgAttribute>>;
    type Cursor = PgAttributeTableCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<PgAttribute>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, PgAttributeTable)> {
        let vtab = PgAttributeTable {
            base: sqlite3_vtab::default(),
            attributes: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    attrelid        INTEGER,
                    attname         TEXT,
                    atttypid        INTEGER,
                    attlen          INTEGER,
                    attnum          INTEGER,
                    attndims        INTEGER,
                    attcacheoff     INTEGER,
                    atttypmod       INTEGER,
                    attbyval        INTEGER,
                    attstorage      TEXT,
                    attalign        TEXT,
                    attnotnull      INTEGER,
                    atthasdef       INTEGER,
                    atthasmissing   INTEGER,
                    attidentity     TEXT,
                    attgenerated    TEXT,
                    attisdropped    INTEGER,
                    attislocal      INTEGER,
                    attinhcount     INTEGER,
                    attcollation    INTEGER,
                    attacl          TEXT,
                    attoptions      TEXT,
                    attfdwoptions   TEXT,
                    attmissingval   TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<PgAttributeTableCursor<'vtab>> {
        Ok(PgAttributeTableCursor {
            attributes: self.attributes.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct PgAttributeTableCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    row_id: i64,
    attributes: &'vtab [PgAttribute],
    phantom: PhantomData<&'vtab PgAttributeTable>,
}

unsafe impl VTabCursor for PgAttributeTableCursor<'_> {
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
        self.row_id >= self.attributes.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(attr) = self.attributes.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(&attr.attrelid),          // attrelid
                1 => ctx.set_result(&attr.attname),           // attname
                2 => ctx.set_result(&(attr.atttypid as i64)), // atttypid
                3 => ctx.set_result(&(attr.attlen as i64)),   // attlen
                4 => ctx.set_result(&attr.attnum),            // attnum
                5 => ctx.set_result(&0i64),                   // attndims
                6 => ctx.set_result(&(-1i64)),                // attcacheoff
                7 => ctx.set_result(&(-1i64)),                // atttypmod
                8 => ctx.set_result(&0i64),                   // attbyval
                9 => ctx.set_result(&"x"),                    // attstorage: EXTENDED
                10 => ctx.set_result(&"d"),                   // attalign: double
                11 => ctx.set_result(&attr.attnotnull),       // attnotnull
                12 => ctx.set_result(&attr.atthasdef),        // atthasdef
                13 => ctx.set_result(&false),                 // atthasmissing
                14 => ctx.set_result(&""),                    // attidentity: not identity
                15 => {
                    // attgenerated: 's' for stored, empty string otherwise
                    if attr.attgenerated == '\0' {
                        ctx.set_result(&"")
                    } else {
                        let s = attr.attgenerated.to_string();
                        ctx.set_result(&s)
                    }
                }
                16 => ctx.set_result(&false), // attisdropped
                17 => ctx.set_result(&true),  // attislocal
                18 => ctx.set_result(&0i64),  // attinhcount
                19 => ctx.set_result(&0i64),  // attcollation
                20..=23 => ctx.set_result(&Option::<String>::None),
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "pg_attribute out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
