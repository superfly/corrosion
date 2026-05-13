//! Mirrors the custom-connection example in `README.md` (see “Configuration” section).

use rusqlite::Connection;
use sqlite_pool::{Config, SqliteConn};
use tempfile::TempDir;

struct MyConn(Connection);

impl SqliteConn for MyConn {
    fn conn(&self) -> &Connection {
        &self.0
    }
}

#[tokio::test]
async fn create_pool_transform_from_readme() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("app.db");

    let pool = Config::new(&path)
        .create_pool_transform(|c| Ok(MyConn(c)))
        .unwrap();

    let conn = pool.get().await.unwrap();
    let _: i64 = conn
        .conn()
        .query_row("SELECT 1", [], |row| row.get(0))
        .unwrap();
}
