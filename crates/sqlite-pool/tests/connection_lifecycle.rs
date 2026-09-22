use rusqlite::Connection;
use sqlite_pool::Config;

fn run_to_completion(conn: &Connection, sql: &str) -> rusqlite::Result<(bool, usize)> {
    let mut statement = conn.prepare(sql)?;
    let readonly = statement.readonly();
    let column_count = statement.column_count();
    let mut rows = statement.query(())?;
    while rows.next()?.is_some() {}
    Ok((readonly, column_count))
}

#[test]
fn readonly_pragmas_change_connection_state_during_prepare() -> eyre::Result<()> {
    let conn = Connection::open_in_memory()?;

    for sql in ["PRAGMA query_only = ON", "EXPLAIN PRAGMA query_only = ON"] {
        let statement = conn.prepare(sql)?;
        assert!(statement.readonly());
        assert_eq!(
            conn.query_row("PRAGMA query_only", (), |row| row.get::<_, i64>(0))?,
            1,
            "{sql:?} did not take effect during prepare"
        );
        eprintln!("prepare_applied={sql:?} readonly=true query_only=1");
        drop(statement);
        conn.execute_batch("PRAGMA query_only = OFF")?;
    }

    Ok(())
}

#[test]
fn classify_readonly_statements_that_change_connection_state() -> eyre::Result<()> {
    let conn = Connection::open_in_memory()?;
    eprintln!("sqlite_version={}", rusqlite::version());

    for sql in ["BEGIN", "SAVEPOINT leaked"] {
        let (readonly, column_count) = run_to_completion(&conn, sql)?;
        eprintln!(
            "sql={sql:?} readonly={readonly} column_count={column_count} autocommit={}",
            conn.is_autocommit()
        );
        assert!(readonly, "SQLite classifies {sql:?} as readonly");
        assert_eq!(column_count, 0);
        assert!(!conn.is_autocommit(), "{sql:?} opens a transaction");
        conn.execute_batch("ROLLBACK")?;
    }

    for sql in [
        "PRAGMA query_only = ON",
        "PRAGMA read_uncommitted = ON",
        "ATTACH DATABASE ':memory:' AS leaked",
    ] {
        let (readonly, column_count) = run_to_completion(&conn, sql)?;
        eprintln!(
            "sql={sql:?} readonly={readonly} column_count={column_count} autocommit={}",
            conn.is_autocommit()
        );
        assert!(readonly, "SQLite classifies {sql:?} as readonly");
        assert_eq!(column_count, 0);

        match sql {
            "PRAGMA query_only = ON" => {
                assert_eq!(
                    conn.query_row("PRAGMA query_only", (), |row| row.get::<_, i64>(0))?,
                    1
                );
                conn.execute_batch("PRAGMA query_only = OFF")?;
            }
            "PRAGMA read_uncommitted = ON" => {
                assert_eq!(
                    conn.query_row("PRAGMA read_uncommitted", (), |row| row.get::<_, i64>(0))?,
                    1
                );
                conn.execute_batch("PRAGMA read_uncommitted = OFF")?;
            }
            "ATTACH DATABASE ':memory:' AS leaked" => {
                let attached: i64 = conn.query_row(
                    "SELECT count(*) FROM pragma_database_list WHERE name = 'leaked'",
                    (),
                    |row| row.get(0),
                )?;
                assert_eq!(attached, 1);
                conn.execute_batch("DETACH DATABASE leaked")?;
            }
            _ => unreachable!(),
        }
    }

    let sql = "PRAGMA locking_mode = EXCLUSIVE";
    let (readonly, column_count) = run_to_completion(&conn, sql)?;
    eprintln!(
        "sql={sql:?} readonly={readonly} column_count={column_count} autocommit={}",
        conn.is_autocommit()
    );
    assert!(readonly, "SQLite classifies {sql:?} as readonly");
    assert_eq!(column_count, 1, "stateful pragmas can return rows");
    assert_eq!(
        conn.query_row("PRAGMA locking_mode", (), |row| row.get::<_, String>(0))?,
        "exclusive"
    );
    conn.execute_batch("PRAGMA locking_mode = NORMAL")?;

    Ok(())
}

async fn freshness_trial(transaction_start: Option<&str>, trial: usize) -> eyre::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("fresh.db");
    let writer = Connection::open(&path)?;
    writer.execute_batch(
        "PRAGMA journal_mode = WAL;
         CREATE TABLE freshness (value INTEGER NOT NULL);
         INSERT INTO freshness VALUES (1);",
    )?;

    let pool = Config::new(&path).read_only().max_size(1).create_pool()?;

    if let Some(sql) = transaction_start {
        let conn = pool.get().await?;
        let (readonly, column_count) = run_to_completion(&conn, sql)?;
        assert!(readonly);
        assert_eq!(column_count, 0);
        assert!(!conn.is_autocommit());
    }

    {
        let conn = pool.get().await?;
        let value: i64 = conn.query_row("SELECT value FROM freshness", (), |row| row.get(0))?;
        assert_eq!(value, 1);
    }

    writer.execute("UPDATE freshness SET value = 2", ())?;

    let conn = pool.get().await?;
    let value: i64 = conn.query_row("SELECT value FROM freshness", (), |row| row.get(0))?;
    assert_eq!(
        value, 2,
        "trial {trial} recycled a stale snapshot after {transaction_start:?}"
    );

    Ok(())
}

#[tokio::test]
async fn recycled_connections_do_not_keep_transaction_snapshots() -> eyre::Result<()> {
    for transaction_start in ["BEGIN", "SAVEPOINT leaked"] {
        for trial in 0..5 {
            freshness_trial(Some(transaction_start), trial).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn autocommit_connections_observe_fresh_writes() -> eyre::Result<()> {
    for trial in 0..5 {
        freshness_trial(None, trial).await?;
    }
    Ok(())
}
