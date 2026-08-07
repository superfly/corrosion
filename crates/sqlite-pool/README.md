# sqlite-pool

Async connection pooling built on [deadpool](https://docs.rs/deadpool/)’s managed pool and interrupt handling for [rusqlite](https://github.com/rusqlite/rusqlite).


## Example (Connection pool)

```rust
use sqlite_pool::Config;

let pool = Config::new("/path/to/app.db")
    .max_size(10)
    .create_pool()?;

// Within a Tokio runtime, e.g. async fn main:
let mut conn = pool.get().await?;
conn.execute_batch("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY);")?;
```

## Configuration

| Piece | Role |
|--------|------|
| `path` | Database file path. |
| `open_flags` | Flags used when creating the rusqlite connection. Use [`Config::read_only`](crate::Config::read_only) for a common read-only preset. |
| `pool` | [`deadpool::managed::PoolConfig`](https://docs.rs/deadpool/latest/deadpool/managed/struct.PoolConfig.html): `max_size`, `timeouts`, `queue_mode`. |


```rust,ignore
use sqlite_pool::rusqlite::Connection;
use sqlite_pool::{Config, SqliteConn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let pool = Config::new("app.db")
        .max_size(10)
        .create_pool()?;

    let conn = pool.get().await?;
    let n: i64 = conn.conn().query_row("SELECT 1", [], |row| row.get(0))?;
    assert_eq!(n, 1);

    Ok(())
}
```

## InterruptibleTransaction

[`InterruptibleTransaction`](crate::InterruptibleTransaction) wraps something that dereferences to `rusqlite::Connection` (often a [`Transaction`](https://docs.rs/rusqlite/latest/rusqlite/struct.Transaction.html)) and wraps `execute`, `prepare`, `prepare_cached`, `execute_batch`, and iterator-style reads so operations can get interrupted after a configured timeout.
