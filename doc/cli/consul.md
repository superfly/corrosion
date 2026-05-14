# The `corrosion consul` command

Utilities for interacting with [Consul](https://www.consul.io/).

## Subcommands

### `corrosion consul sync`

Starts a long-lived process that synchronizes local consul data to corrosion. It connects to consul using the `[consul]` 
block in your config.yaml, and inserts checks and services into the `consul_services` and `consul_checks` tables in 
corrosion. The process expects that these tables already exist on the database with the right schema. 

You should add the following sql to your schema:

```
CREATE TABLE consul_services (
    node TEXT NOT NULL,
    id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]',
    meta TEXT NOT NULL DEFAULT '{}',
    port INTEGER NOT NULL DEFAULT 0,
    address TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL DEFAULT 0,
    source TEXT, -- corro-consul ignores any row with non-NULL source
    app_id INTEGER AS (CAST(JSON_EXTRACT(meta, '$.app_id') AS INTEGER)),
    network_id INTEGER AS (
        CAST(JSON_EXTRACT(meta, '$.network_id') AS INTEGER)
    ),
    app_name TEXT AS (JSON_EXTRACT(meta, '$.app_name')),
    PRIMARY KEY (node, id)
) WITHOUT ROWID;

CREATE TABLE consul_checks (
    node TEXT NOT NULL,
    id TEXT NOT NULL,
    service_id TEXT NOT NULL DEFAULT '',
    service_name TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    output TEXT NOT NULL DEFAULT '',
    updated_at INTEGER NOT NULL DEFAULT 0,
    source TEXT, -- corro-consul ignores any row with non-NULL source
    PRIMARY KEY (node, id)
) WITHOUT ROWID;
```

## Command output
```
Synchronizes the local consul agent with Corrosion

Usage: corrosion consul sync [OPTIONS]

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: /etc/corrosion/config.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help         
```