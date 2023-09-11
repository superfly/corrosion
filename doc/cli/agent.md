# The `corrosion agent` command

Starts Corrosion on the host. The `--api-addr`, `--db-path`, and `--admin-path` options override corresponding settings in the configuration file.

```cmd
$ corrosion agent --help
Launches the agent

Usage: corrosion agent [OPTIONS]

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: /etc/corrosion/config.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```

