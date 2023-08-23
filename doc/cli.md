# Command-line Interface

## `backup`

Creates a backup of the current database by running `VACUUM INTO` and cleaning up node-specific data. This includes removing `crsql_site_id` as well as rewriting `__crsql_clock` tables to make the backup generic, ready for a `corrosion restore`.

```
$ corrosion backup --help
Backup the Corrosion DB

Usage: corrosion backup [OPTIONS] <PATH>

Arguments:
  <PATH>

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>
      --db-path <DB_PATH>
      --admin-path <ADMIN_PATH>
  -h, --help                     Print help
```

## `restore`

Restores a database from a backup produced by `corrosion backup`. This is an "online restore", it acquires all the appropriate locks on the sqlite3 database so as to not disrupt database readers. It then replaces the database in-place and releases the locks.

```
$ corrosion restore --help
Restore the Corrosion DB from a backup

Usage: corrosion restore [OPTIONS] <PATH>

Arguments:
  <PATH>

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>
      --db-path <DB_PATH>
      --admin-path <ADMIN_PATH>
  -h, --help                     Print help
```