# The `corrosion exec` command

Writes to Corrosion's database, via the [`/v1/transactions/`](../api/transactions.md) endpoint hosted by the local Corrosion agent. 

Use `corrosion exec` to mutate data within existing CR-SQLite-enabled Corrosion database tables, for propagation throughout the cluster.

Corrosion does not sync schema changes made using this command. Use Corrosion's [schema files](../schema.md) to create and update the cluster's database schema.

```
$ corrosion exec --help
Execute a SQL statement that mutates the state of Corrosion

Usage: corrosion exec [OPTIONS] <QUERY>

Arguments:
  <QUERY>  

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: /etc/corrosion/config.toml]
      --timer                    
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```