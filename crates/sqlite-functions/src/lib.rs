use rusqlite::{
    functions::FunctionFlags,
    params_from_iter,
    types::{Value as SqliteValue, ValueRef},
    Connection, Error, Result,
};
use serde_json::Value;

/// Add custom Corrosion functions into SQLite connection.
pub fn add_to_connection(db: &Connection) -> Result<()> {
    add_corro_json_contains(db)?;
    add_corro_get_value(db)?;

    Ok(())
}

fn add_corro_get_value(db: &Connection) -> Result<()> {
    db.create_scalar_function(
        "corro_get_value",
        -1,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            assert!(
                ctx.len() >= 4,
                "at least 4 arguments are requred: table, column, primary key names and primary key values"
            );

            let tbl_name: ValueRef = ctx.get_raw(0);
            let col_name: ValueRef = ctx.get_raw(1);

            let pk_len = (ctx.len() - 2) / 2;

            let mut conditions = vec![];
            let mut values: Vec<SqliteValue> = vec![];

            let mut curr = 2;

            for idx in 0..pk_len {
                let pk_name: ValueRef = ctx.get_raw(curr + idx);
                conditions.push(format!("{} IS ?", pk_name.as_str()?));
            }
            curr += pk_len;

            for idx in 0..pk_len {
                let pk_val: SqliteValue = ctx.get(curr + idx)?;
                values.push(pk_val);
            }

            let conn = unsafe { ctx.get_connection() }?;

            let q = format!(
                "SELECT {} FROM {} WHERE {}",
                col_name.as_str()?,
                tbl_name.as_str()?,
                conditions.join(" AND ")
            );

            let mut prepped = conn.prepare_cached(&q)?;
            let val: SqliteValue = prepped.query_row(params_from_iter(values), |row| row.get(0))?;

            Ok(val)
        },
    )
}

// corro_json_contains returns true if the first argument of the function
// (JSON object) is fully contained within the second argument of the
// function (also JSON object)
fn add_corro_json_contains(db: &Connection) -> Result<()> {
    db.create_scalar_function(
        "corro_json_contains",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        move |ctx| {
            assert_eq!(ctx.len(), 2, "called with unexpected number of arguments");

            let selector: Value = serde_json::from_str(ctx.get_raw(0).as_str()?)
                .map_err(|e| Error::UserFunctionError(e.into()))?;
            let object: Value = serde_json::from_str(ctx.get_raw(1).as_str()?)
                .map_err(|e| Error::UserFunctionError(e.into()))?;

            Ok(corro_json_contains(selector, object))
        },
    )
}

// Helper function that returns true if the first JSON object is full
// contained within the second JSON object.
fn corro_json_contains(selector: Value, object: Value) -> bool {
    match (selector, object) {
        (Value::Object(s_map), Value::Object(mut o_map)) => {
            for (s_k, s_v) in s_map {
                if let Some(o_v) = o_map.remove(&s_k) {
                    if !corro_json_contains(s_v, o_v) {
                        return false;
                    }
                } else {
                    return false;
                }
            }

            true
        }
        (s, o) => s == o,
    }
}

#[cfg(test)]
mod test {
    use rusqlite::{Connection, Result};

    fn get_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("cannot open in-memory connection");
        super::add_corro_json_contains(&conn)
            .expect("cannot add corrosion functions to connection");

        conn
    }

    fn query_corro_json_contains(conn: &Connection, obj1: &str, obj2: &str) -> Result<bool> {
        conn.query_row("SELECT corro_json_contains(?1, ?2)", [obj1, obj2], |row| {
            row.get(0)
        })
    }

    #[test]
    fn test_corro_json_contains() {
        let conn = get_conn();

        // Empty object is contained in other empty objects
        assert_eq!(Ok(true), query_corro_json_contains(&conn, "{}", "{}"));
        // Empty object is contained in non-empty objects
        assert_eq!(
            Ok(true),
            query_corro_json_contains(&conn, "{}", r#"{"key": "value"}"#)
        );
        // An object with keys is not contained in an empty object
        assert_ne!(
            Ok(true),
            query_corro_json_contains(&conn, r#"{"key": "value"}"#, "{}")
        );

        // Equal key/values match
        assert_eq!(
            Ok(true),
            query_corro_json_contains(&conn, r#"{"key": "value"}"#, r#"{"key": "value"}"#)
        );
        // Smaller objects is also Ok
        assert_eq!(
            Ok(true),
            query_corro_json_contains(
                &conn,
                r#"{"key": "value"}"#,
                r#"{"key": "value", "key2": "value2"}"#
            )
        );
        // Not equal key/values do not match
        assert_ne!(
            Ok(true),
            query_corro_json_contains(&conn, r#"{"key": "value"}"#, r#"{"key": "wrong value"}"#)
        );

        // Nested objects work
        assert_eq!(
            Ok(true),
            query_corro_json_contains(
                &conn,
                r#"{"metadata": { "key": "value"} }"#,
                r#"{"metadata": { "key": "value"} }"#
            )
        );
        // And do not match with different key/values
        assert_ne!(
            Ok(true),
            query_corro_json_contains(
                &conn,
                r#"{"metadata": { "key": "value"} }"#,
                r#"{"metadata": { "key": "wrong value"} }"#
            )
        );
    }
}
