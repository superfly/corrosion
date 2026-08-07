# The `corrosion reload-dicts` command

Rescans `gossip.compression.dict_dir` on the running agent and swaps in the
dictionaries found there. Use this after dropping a new decoder dictionary into
the directory so peers using that dictionary id can be understood without
restarting Corrosion.

```
$ corrosion reload-dicts --help
Rescan gossip.compression.dict_dir and reload zstd dictionaries without restarting the agent

Usage: corrosion reload-dicts [OPTIONS]

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: /etc/corrosion/config.toml]
      --api-addr <API_ADDR>
      --db-path <DB_PATH>
      --admin-path <ADMIN_PATH>
  -h, --help
```

Keep older dictionary files in `dict_dir` for as long as peers may still encode
with them — reload replaces the in-memory set with whatever is currently on disk.
