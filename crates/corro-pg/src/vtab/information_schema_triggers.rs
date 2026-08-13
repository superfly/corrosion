use std::{marker::PhantomData, os::raw::c_int, sync::Arc};

use rusqlite::vtab::{
    sqlite3_vtab, sqlite3_vtab_cursor, Filters, IndexInfo, VTab, VTabConnection, VTabCursor,
};

/// A row in `information_schema.triggers`, matching the columns that
/// TablePlus and similar tools query.
pub struct InformationSchemaTrigger {
    pub trigger_name: String,
    pub event_object_schema: String,
    pub event_object_table: String,
    pub event_manipulation: String,
    pub action_timing: String,
    pub action_statement: String,
}

/// Loads triggers from `sqlite_master` and parses the `CREATE TRIGGER` SQL to
/// extract the timing, event, and body.
pub fn load_information_schema_triggers(
    conn: &rusqlite::Connection,
) -> rusqlite::Result<Vec<InformationSchemaTrigger>> {
    let mut stmt = conn.prepare(
        "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger' AND sql IS NOT NULL ORDER BY name",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?, // name
            row.get::<_, String>(1)?, // tbl_name
            row.get::<_, String>(2)?, // sql
        ))
    })?;

    let mut triggers = Vec::new();
    for row in rows {
        let (name, tbl_name, sql) = row?;
        let (timing, event, body) = parse_trigger_sql(&sql);
        triggers.push(InformationSchemaTrigger {
            trigger_name: name,
            event_object_schema: "main".into(),
            event_object_table: tbl_name,
            event_manipulation: event,
            action_timing: timing,
            action_statement: body,
        });
    }

    Ok(triggers)
}

/// Parses a `CREATE TRIGGER` SQL statement and returns
/// `(action_timing, event_manipulation, action_statement)`.
///
/// SQLite trigger syntax:
///   `CREATE TRIGGER <name> [BEFORE|AFTER|INSTEAD OF] [INSERT|UPDATE|DELETE] ON <table> BEGIN <body> END`
fn parse_trigger_sql(sql: &str) -> (String, String, String) {
    // Normalize: remove "CREATE TRIGGER" prefix (case-insensitive)
    let lower = sql.to_ascii_lowercase();
    let after_trigger = if let Some(idx) = lower.find("trigger") {
        &sql[idx + 7..]
    } else {
        sql
    };

    // Extract timing: BEFORE, AFTER, or INSTEAD OF
    let trimmed = after_trigger.trim_start();
    let (timing, rest) = if trimmed.to_ascii_lowercase().starts_with("before") {
        ("BEFORE".to_string(), &trimmed[6..])
    } else if trimmed.to_ascii_lowercase().starts_with("after") {
        ("AFTER".to_string(), &trimmed[5..])
    } else if trimmed.to_ascii_lowercase().starts_with("instead of") {
        ("INSTEAD OF".to_string(), &trimmed[10..])
    } else {
        ("AFTER".to_string(), trimmed) // default
    };

    // Extract event: INSERT, UPDATE, or DELETE
    let rest = rest.trim_start();
    let (event, _rest) = if rest.to_ascii_lowercase().starts_with("insert") {
        ("INSERT".to_string(), &rest[6..])
    } else if rest.to_ascii_lowercase().starts_with("update") {
        ("UPDATE".to_string(), &rest[6..])
    } else if rest.to_ascii_lowercase().starts_with("delete") {
        ("DELETE".to_string(), &rest[6..])
    } else {
        ("INSERT".to_string(), rest)
    };

    // Extract body: everything between BEGIN and END (case-insensitive)
    let body = extract_trigger_body(sql);

    (timing, event, body)
}

/// Extracts the trigger body — the statements between `BEGIN` and `END`.
fn extract_trigger_body(sql: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let begin_idx = lower.find("begin").map(|i| i + 5);
    let end_idx = lower.rfind("end");

    match (begin_idx, end_idx) {
        (Some(begin), Some(end)) if begin < end => {
            let body = sql[begin..end].trim();
            if body.is_empty() {
                "EXECUTE PROCEDURE \"\"()".to_string()
            } else {
                format!("EXECUTE PROCEDURE \"\"({})", body)
            }
        }
        _ => "EXECUTE PROCEDURE \"\"()".to_string(),
    }
}

#[repr(C)]
pub struct InformationSchemaTriggersTable {
    base: sqlite3_vtab,
    triggers: Arc<Vec<InformationSchemaTrigger>>,
}

unsafe impl<'vtab> VTab<'vtab> for InformationSchemaTriggersTable {
    type Aux = Arc<Vec<InformationSchemaTrigger>>;
    type Cursor = InformationSchemaTriggersCursor<'vtab>;

    fn connect(
        _: &mut VTabConnection,
        aux: Option<&Arc<Vec<InformationSchemaTrigger>>>,
        args: &[&[u8]],
    ) -> rusqlite::Result<(String, InformationSchemaTriggersTable)> {
        let vtab = InformationSchemaTriggersTable {
            base: sqlite3_vtab::default(),
            triggers: aux.unwrap().clone(),
        };
        let table_name = std::str::from_utf8(args[0]).map_err(rusqlite::Error::Utf8Error)?;

        Ok((
            format!(
                "CREATE TABLE {table_name} (
                    trigger_catalog            TEXT,
                    trigger_schema             TEXT,
                    trigger_name               TEXT,
                    event_manipulation         TEXT,
                    event_object_catalog       TEXT,
                    event_object_schema        TEXT,
                    event_object_table         TEXT,
                    action_ordering            INTEGER,
                    action_condition           TEXT,
                    action_statement           TEXT,
                    action_orientation         TEXT,
                    action_timing              TEXT,
                    action_reference_old_table TEXT,
                    action_reference_new_table TEXT,
                    action_reference_old_row   TEXT,
                    action_reference_new_row   TEXT,
                    created                    TEXT
                )"
            ),
            vtab,
        ))
    }

    fn best_index(&self, info: &mut IndexInfo) -> rusqlite::Result<()> {
        info.set_estimated_cost(1.);
        Ok(())
    }

    fn open(&'vtab mut self) -> rusqlite::Result<InformationSchemaTriggersCursor<'vtab>> {
        Ok(InformationSchemaTriggersCursor {
            triggers: self.triggers.as_slice(),
            ..Default::default()
        })
    }
}

#[derive(Default)]
#[repr(C)]
pub struct InformationSchemaTriggersCursor<'vtab> {
    base: sqlite3_vtab_cursor,
    row_id: i64,
    triggers: &'vtab [InformationSchemaTrigger],
    phantom: PhantomData<&'vtab InformationSchemaTriggersTable>,
}

unsafe impl VTabCursor for InformationSchemaTriggersCursor<'_> {
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
        self.row_id >= self.triggers.len() as i64
    }

    fn column(&self, ctx: &mut rusqlite::vtab::Context, col: c_int) -> rusqlite::Result<()> {
        if let Some(trigger) = self.triggers.get(self.row_id as usize) {
            match col {
                0 => ctx.set_result(&"state"), // trigger_catalog
                1 => ctx.set_result(&"main"),  // trigger_schema
                2 => ctx.set_result(&trigger.trigger_name),
                3 => ctx.set_result(&trigger.event_manipulation),
                4 => ctx.set_result(&"state"), // event_object_catalog
                5 => ctx.set_result(&trigger.event_object_schema),
                6 => ctx.set_result(&trigger.event_object_table),
                7 => ctx.set_result(&0i64), // action_ordering
                8 => ctx.set_result(&Option::<String>::None), // action_condition
                9 => ctx.set_result(&trigger.action_statement),
                10 => ctx.set_result(&"ROW"), // action_orientation
                11 => ctx.set_result(&trigger.action_timing),
                12 => ctx.set_result(&Option::<String>::None), // action_reference_old_table
                13 => ctx.set_result(&Option::<String>::None), // action_reference_new_table
                14 => ctx.set_result(&Option::<String>::None), // action_reference_old_row
                15 => ctx.set_result(&Option::<String>::None), // action_reference_new_row
                16 => ctx.set_result(&Option::<String>::None), // created
                _ => Err(rusqlite::Error::InvalidColumnIndex(col as usize)),
            }
        } else {
            Err(rusqlite::Error::ModuleError(format!(
                "information schema triggers out of bound (row id: {})",
                self.row_id
            )))
        }
    }

    fn rowid(&self) -> rusqlite::Result<i64> {
        Ok(self.row_id)
    }
}
