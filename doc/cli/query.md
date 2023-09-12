# The `corrosion query` command

Reads from Corrosion's database, via the [`/v1/queries/`](../api/queries.md) endpoint hosted by the local Corrosion agent. 

Use the `--columns` option to see column headings in the output.

```
$ corrosion query --help
Query data from Corrosion w/ a SQL statement

Usage: corrosion query [OPTIONS] <QUERY>

Arguments:
  <QUERY>  

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: /etc/corrosion/config.toml]
      --columns                  
      --api-addr <API_ADDR>      
      --timer                    
      --db-path <DB_PATH>        
      --param <PARAM>            
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```
