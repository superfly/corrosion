# The `corrosion template` command

Uses Corrosion's template engine to generate and update a local file based on a [Rhai](https://rhai.rs/) script and the latest data from Corrosion.

Specify the name of the `.rhai` file and the desired name for the output file. 

```
$ corrosion template --help
Usage: corrosion template [OPTIONS] [TEMPLATE]...

Arguments:
  [TEMPLATE]...  

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: /etc/corrosion/config.toml]
  -o, --once                     
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```