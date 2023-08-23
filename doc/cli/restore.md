# The `corrosion restore` command

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