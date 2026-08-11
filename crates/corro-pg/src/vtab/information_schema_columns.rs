use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

pub struct InformationSchemaColumn {
    pub(crate) table_name: String,
    column_name: String,
    ordinal_position: i64,
    column_default: Option<String>,
    nullable: bool,
    data_type: String,
    character_maximum_length: Option<i64>,
    character_octet_length: Option<i64>,
    numeric_precision: Option<i64>,
    numeric_precision_radix: Option<i64>,
    numeric_scale: Option<i64>,
    datetime_precision: Option<i64>,
    udt_name: String,
    dtd_identifier: String,
    generated: bool,
    pub(crate) primary_key_ordinal: Option<i64>,
}

struct TypeMetadata {
    data_type: String,
    character_maximum_length: Option<i64>,
    character_octet_length: Option<i64>,
    numeric_precision: Option<i64>,
    numeric_precision_radix: Option<i64>,
    numeric_scale: Option<i64>,
    datetime_precision: Option<i64>,
    udt_name: String,
}

impl TypeMetadata {
    fn new(data_type: &str, udt_name: &str) -> Self {
        Self {
            data_type: data_type.into(),
            character_maximum_length: None,
            character_octet_length: None,
            numeric_precision: None,
            numeric_precision_radix: None,
            numeric_scale: None,
            datetime_precision: None,
            udt_name: udt_name.into(),
        }
    }
}

fn type_modifiers(declared_type: &str) -> (Option<i64>, Option<i64>) {
    let Some(open) = declared_type.find('(') else {
        return (None, None);
    };
    let Some(close) = declared_type.rfind(')') else {
        return (None, None);
    };
    if close <= open {
        return (None, None);
    }

    let mut values = declared_type[open + 1..close]
        .split(',')
        .map(str::trim)
        .map(str::parse::<i64>);
    (
        values.next().and_then(Result::ok),
        values.next().and_then(Result::ok),
    )
}

fn postgres_type_metadata(declared_type: &str) -> TypeMetadata {
    let normalized = declared_type.trim().to_ascii_uppercase();
    let (first_modifier, second_modifier) = type_modifiers(&normalized);

    if normalized == "JSONB" {
        TypeMetadata::new("jsonb", "jsonb")
    } else if normalized == "JSON" {
        TypeMetadata::new("json", "json")
    } else if normalized.contains("INT") {
        let mut metadata = TypeMetadata::new("bigint", "int8");
        metadata.numeric_precision = Some(64);
        metadata.numeric_precision_radix = Some(2);
        metadata.numeric_scale = Some(0);
        metadata
    } else if normalized.starts_with("DATETIME") || normalized.starts_with("TIMESTAMP") {
        let mut metadata = TypeMetadata::new("timestamp without time zone", "timestamp");
        metadata.datetime_precision = Some(first_modifier.unwrap_or(6));
        metadata
    } else if normalized.starts_with("VARCHAR")
        || normalized.starts_with("CHARACTER VARYING")
        || normalized.starts_with("CHAR VARYING")
    {
        let mut metadata = TypeMetadata::new("character varying", "varchar");
        metadata.character_maximum_length = first_modifier;
        metadata.character_octet_length = first_modifier.and_then(|length| length.checked_mul(4));
        metadata
    } else if normalized.contains("CLOB") || normalized.contains("TEXT") {
        TypeMetadata::new("text", "text")
    } else if normalized.contains("CHAR") {
        let mut metadata = TypeMetadata::new("character", "bpchar");
        let length = first_modifier.or(Some(1));
        metadata.character_maximum_length = length;
        metadata.character_octet_length = length.and_then(|length| length.checked_mul(4));
        metadata
    } else if normalized.is_empty() || normalized.contains("BLOB") || normalized.contains("BINARY")
    {
        TypeMetadata::new("bytea", "bytea")
    } else if normalized.contains("REAL")
        || normalized.contains("FLOA")
        || normalized.contains("DOUB")
    {
        let mut metadata = TypeMetadata::new("double precision", "float8");
        metadata.numeric_precision = Some(53);
        metadata.numeric_precision_radix = Some(2);
        metadata
    } else if normalized.starts_with("BOOL") {
        TypeMetadata::new("boolean", "bool")
    } else if normalized.starts_with("DECIMAL") || normalized.starts_with("NUMERIC") {
        let mut metadata = TypeMetadata::new("numeric", "numeric");
        metadata.numeric_precision = first_modifier;
        metadata.numeric_precision_radix = Some(10);
        metadata.numeric_scale = second_modifier;
        metadata
    } else if normalized.starts_with("DATE") {
        TypeMetadata::new("date", "date")
    } else if normalized.starts_with("TIME") {
        let mut metadata = TypeMetadata::new("time without time zone", "time");
        metadata.datetime_precision = Some(first_modifier.unwrap_or(6));
        metadata
    } else {
        let udt_name = normalized
            .split(|character: char| character == '(' || character.is_ascii_whitespace())
            .next()
            .filter(|name| !name.is_empty())
            .unwrap_or("unknown")
            .to_ascii_lowercase();
        TypeMetadata::new("USER-DEFINED", &udt_name)
    }
}

pub fn load_information_schema_columns(
    conn: &rusqlite::Connection,
    table_names: &[String],
) -> rusqlite::Result<Vec<InformationSchemaColumn>> {
    let mut statement = conn.prepare(
        "SELECT cid, name, type, \"notnull\", dflt_value, pk, hidden
         FROM pragma_table_xinfo(?1)
         ORDER BY cid",
    )?;
    let mut columns = Vec::new();

    for table_name in table_names {
        let table_columns = statement
            .query_map([table_name], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, i64>(5)?,
                    row.get::<_, i64>(6)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        for (cid, column_name, declared_type, not_null, column_default, primary_key, hidden) in
            table_columns
        {
            // Negative column IDs and hidden virtual-table arguments are SQLite implementation
            // details rather than user-visible table columns. Generated columns use 2 or 3.
            if cid < 0 || hidden == 1 {
                continue;
            }

            let metadata = postgres_type_metadata(declared_type.as_deref().unwrap_or_default());
            let ordinal_position = cid + 1;
            columns.push(InformationSchemaColumn {
                table_name: table_name.clone(),
                column_name,
                ordinal_position,
                column_default,
                nullable: not_null == 0 && primary_key == 0,
                data_type: metadata.data_type,
                character_maximum_length: metadata.character_maximum_length,
                character_octet_length: metadata.character_octet_length,
                numeric_precision: metadata.numeric_precision,
                numeric_precision_radix: metadata.numeric_precision_radix,
                numeric_scale: metadata.numeric_scale,
                datetime_precision: metadata.datetime_precision,
                udt_name: metadata.udt_name,
                dtd_identifier: ordinal_position.to_string(),
                generated: matches!(hidden, 2 | 3),
                primary_key_ordinal: (primary_key > 0).then_some(primary_key),
            });
        }
    }

    Ok(columns)
}

#[repr(C)]
pub struct InformationSchemaColumnsTable {
    /// Base class. Must be first
    base: sqlite3_vtab,
    columns: Arc<Vec<InformationSchemaColumn>>,
}

unsafe impl<'vtab> VTab<'vtab> for InformationSchemaColumnsTable {
    type Aux = Arc<Vec<InformationSchemaColumn>>;
    type Cursor = InformationSchemaColumnsCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<InformationSchemaColumn>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, InformationSchemaColumnsTable)> {
        let vtab = InformationSchemaColumnsTable {
            base: sqlite3_vtab::default(),
            columns: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    table_catalog            TEXT,
                    table_schema             TEXT,
                    table_name               TEXT,
                    column_name              TEXT,
                    ordinal_position         INTEGER,
                    column_default           TEXT,
                    is_nullable              TEXT,
                    data_type                TEXT,
                    character_maximum_length INTEGER,
                    character_octet_length   INTEGER,
                    numeric_precision        INTEGER,
                    numeric_precision_radix  INTEGER,
                    numeric_scale            INTEGER,
                    datetime_precision       INTEGER,
                    interval_type            TEXT,
                    interval_precision       INTEGER,
                    character_set_catalog    TEXT,
                    character_set_schema     TEXT,
                    character_set_name       TEXT,
                    collation_catalog        TEXT,
                    collation_schema         TEXT,
                    collation_name           TEXT,
                    domain_catalog           TEXT,
                    domain_schema            TEXT,
                    domain_name              TEXT,
                    udt_catalog              TEXT,
                    udt_schema               TEXT,
                    udt_name                 TEXT,
                    scope_catalog            TEXT,
                    scope_schema             TEXT,
                    scope_name               TEXT,
                    maximum_cardinality      INTEGER,
                    dtd_identifier           TEXT,
                    is_self_referencing      TEXT,
                    is_identity              TEXT,
                    identity_generation      TEXT,
                    identity_start           TEXT,
                    identity_increment       TEXT,
                    identity_maximum         TEXT,
                    identity_minimum         TEXT,
                    identity_cycle           TEXT,
                    is_generated             TEXT,
                    generation_expression    TEXT,
                    is_updatable             TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<InformationSchemaColumnsCursor<'vtab>> {
        Ok(InformationSchemaColumnsCursor {
            columns: self.columns.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct InformationSchemaColumnsCursor<'vtab> {
    /// Base class. Must be first
    base: sqlite3_vtab_cursor,
    row_id: i64,
    columns: &'vtab [InformationSchemaColumn],
    phantom: PhantomData<&'vtab InformationSchemaColumnsTable>,
}

unsafe impl VTabCursor for InformationSchemaColumnsCursor<'_> {
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
        self.row_id >= self.columns.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(column) = self.columns.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(&"state"),
                1 => ctx.set_result(&"main"),
                2 => ctx.set_result(&column.table_name),
                3 => ctx.set_result(&column.column_name),
                4 => ctx.set_result(&column.ordinal_position),
                5 => ctx.set_result(&column.column_default),
                6 => ctx.set_result(&if column.nullable { "YES" } else { "NO" }),
                7 => ctx.set_result(&column.data_type),
                8 => ctx.set_result(&column.character_maximum_length),
                9 => ctx.set_result(&column.character_octet_length),
                10 => ctx.set_result(&column.numeric_precision),
                11 => ctx.set_result(&column.numeric_precision_radix),
                12 => ctx.set_result(&column.numeric_scale),
                13 => ctx.set_result(&column.datetime_precision),
                14 => ctx.set_result(&Option::<String>::None),
                15 => ctx.set_result(&Option::<i64>::None),
                16..=24 | 28..=30 => ctx.set_result(&Option::<String>::None),
                25 => ctx.set_result(&"state"),
                26 => ctx.set_result(&"pg_catalog"),
                27 => ctx.set_result(&column.udt_name),
                31 => ctx.set_result(&Option::<i64>::None),
                32 => ctx.set_result(&column.dtd_identifier),
                33 | 34 => ctx.set_result(&"NO"),
                35..=40 => ctx.set_result(&Option::<String>::None),
                41 => ctx.set_result(&if column.generated { "ALWAYS" } else { "NEVER" }),
                42 => ctx.set_result(&Option::<String>::None),
                43 => ctx.set_result(&"YES"),
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "information schema column out of bound (row id: {})",
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
    use super::postgres_type_metadata;

    #[test]
    fn maps_sqlite_declared_types_to_postgres_catalog_types() {
        let bigint = postgres_type_metadata("INTEGER");
        assert_eq!(bigint.data_type, "bigint");
        assert_eq!(bigint.udt_name, "int8");
        assert_eq!(bigint.numeric_precision, Some(64));

        let varchar = postgres_type_metadata("VARCHAR(255)");
        assert_eq!(varchar.data_type, "character varying");
        assert_eq!(varchar.character_maximum_length, Some(255));

        let timestamp = postgres_type_metadata("DATETIME");
        assert_eq!(timestamp.data_type, "timestamp without time zone");
        assert_eq!(timestamp.datetime_precision, Some(6));

        let float = postgres_type_metadata("REAL");
        assert_eq!(float.data_type, "double precision");
        assert_eq!(float.udt_name, "float8");
    }
}
