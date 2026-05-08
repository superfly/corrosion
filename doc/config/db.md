# The `[db]` configuration

The `[db]` block configures the Corrosion's SQLite database.

### Required fields

#### `db.path`

Path of the sqlite3 database.

```toml
[db]
path = "/var/lib/corrosion/db.sqlite"
```

### Optional fields

#### `db.schema_paths`

Array of paths where [schema](../schema.md) `.sql` files are present. If a directory is specified, all `.sql` files within it are loaded.

```toml
[db]
schema_paths = ["/etc/corrosion/schema", "/path/to/table_name.sql"]
```

#### `db.subscriptions_path`

Directory where the per-subscription SQLite databases used by [`/v1/subscriptions`](../api/subscriptions.md) live. Each active subscription gets its own database file under this directory.

If unset, defaults to a `subscriptions/` directory next to `db.path` (so `/var/lib/corrosion/db.sqlite` produces `/var/lib/corrosion/subscriptions/`).

```toml
[db]
subscriptions_path = "/var/lib/corrosion/subscriptions"
```

#### `db.cache_size_kib`

SQLite page cache size for write connections, expressed using SQLite's [`PRAGMA cache_size`](https://www.sqlite.org/pragma.html#pragma_cache_size) convention: a **negative** value is a number of kibibytes, a positive value is a number of pages.

Defaults to `-1048576` (1 GiB).

Larger values can improve write performance noticeably under heavy ingest, at the cost of resident memory. Setting this too low (under ~100 MiB) can severely degrade performance, so prefer leaving it at the default unless you have a measured reason to change it.

```toml
[db]
cache_size_kib = -1048576  # 1 GiB
```
