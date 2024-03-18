use rusqlite::{functions::FunctionFlags, Connection, Error, Result};
use serde_json::Value;

/// Add custom Corrosion functions into SQLite connection.
pub fn add_to_connection(db: &Connection) -> Result<()> {
    add_corro_json_contains(db)?;

    Ok(())
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
