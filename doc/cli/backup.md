# The `backup` command

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