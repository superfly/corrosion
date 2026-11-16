use rusqlite::Connection;
use sqlite_pool::{Config, SqliteConn};
use tempfile::TempDir;

#[tokio::test]
async fn test_pool_discards_non_autocommit_connection() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal_test.db");

    // Initialize the DB in WAL mode and create a table with value 1
    {
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 CREATE TABLE test (val INTEGER);
                 INSERT INTO test VALUES (1);",
            )
            .unwrap();
    }

    // Create a read-only pool with max_size = 1
    let pool = Config::new(&path)
        .max_size(1)
        .read_only()
        .create_pool()
        .unwrap();

    // Borrow the sole connection and execute BEGIN to simulate dirty state
    {
        let conn = pool.get().await.unwrap();
        conn.conn().execute_batch("BEGIN;").unwrap();
        assert!(!conn.conn().is_autocommit());
        // conn is returned to pool here on drop
    }

    // Separate writer updates value to 2 and commits
    {
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch("UPDATE test SET val = 2;")
            .unwrap();
    }

    // Borrow from the pool again.
    // The pool's recycle validation must reject the dirty connection (autocommit = false),
    // discard it, and provide a fresh connection.
    let conn = pool.get().await.unwrap();
    assert!(conn.conn().is_autocommit());
    let val: i64 = conn
        .conn()
        .query_row("SELECT val FROM test;", [], |row| row.get(0))
        .unwrap();
    // Fresh connection sees 2, NOT the stale pinned snapshot 1!
    assert_eq!(val, 2);
}
