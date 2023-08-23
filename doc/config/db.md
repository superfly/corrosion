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

Array of paths where [schema]() .sql files are present.

```toml
[db]
schema_paths = ["/etc/corrosion/schema", "/path/to/table_name.sql"]
```

If a directory is specified, all .sql files will be loaded.