# Corrosion CLI

Corrosion has a CLI for running and managing cluster instances. It includes commands for reading from and writing to the database.

Some commands (e.g. `query` and `exec`) call the API on a running Corrosion agent at the configured address and port (`api_addr` in the configuration file, or as specified by the `--api-addr` command-line option). 

```
Usage: corrosion [OPTIONS] <COMMAND>

Commands:
  agent     Launches the agent
  consul    Consul interactions
  query     Query data from Corrosion w/ a SQL statement
  exec      Execute a SQL statement that mutates the state of Corrosion
  reload    Reload the config
  sync      Sync-related commands
  template  
  help      Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
  -V, --version                  Print version
```

`corrosion` is the base command. 

## `agent`

Start the local Corrosion agent. Pass it a config file name with the `--config` option, if there's not a `corrosion.toml` that you want it to use, in the working dir.

```
Usage: corrosion agent [OPTIONS]

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```

### Notes
* If there's no `.sql` schema file in the location specified for `schema_paths` in the Corrosion config file, this message is shown in the logs:

  ```
  corrosion::command::agent: could not apply schema: received unexpected response code: 400 Bad Request
  ```

  An empty `.sql` file is enough to prevent this log line. No need, though, because once the agent is running, you can use the API or `corrosion exec` to establish the schema for this Corrosion instance. You probably do want to use a schema file to establish a common schema for all nodes in the cluster, since Corrosion does not propagate the local schema.

## `consul`

"Consul interactions"

(Doc TK)

Needs a setting in config, probably to say where to connect to a consul instance. Has a single subcommand `sync`.

```
Usage: corrosion consul [OPTIONS] <COMMAND>

Commands:
  sync  Synchronizes the local consul agent with Corrosion
  help  Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```

## `query` 

Query data from Corrosion w/ a SQL statement

This is a command that calls the API on a running Corrosion agent.

**To note: I think there's a bug; it crashes, but it does first get the result:**
```
ham
thread 'main' panicked at 'called `Option::unwrap()` on a `None` value', crates/corrosion/src/main.rs:59:57
stack backtrace:
(etc.)
```

```
Usage: corrosion query [OPTIONS] <QUERY>

Arguments:
  <QUERY>  

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --columns                  
      --api-addr <API_ADDR>      
      --timer                    
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```


### `corrosion query` example

Use the `--columns` option to see column headings in the output.

```sh
./corrosion query "SELECT sandwich FROM sw WHERE machine_id=1" --columns
```

```
ham
```

## `exec`

Execute a SQL statement that mutates the state of Corrosion

**To note: does this include schema changes?**

This is a command that calls the API on a running Corrosion agent.

```
Usage: corrosion exec [OPTIONS] <QUERY>

Arguments:
  <QUERY>  

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --timer                    
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```
### `corrosion exec` example

```
./corrosion exec --timer "INSERT OR IGNORE INTO sw (machine_id, sandwich) VALUES (1, 'nonsense')"
```
```
Rows affected: 1
Run Time: real 0.000028875
```

## `reload`

Reload Corrosion's configuration

```
Usage: corrosion reload [OPTIONS]

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```

**To note: Gives a 400 error if it doesn't find a schema file**


## `sync`

"Sync-related commands"
```
Usage: corrosion sync [OPTIONS] <COMMAND>

Commands:
  generate  Generate a sync message from the current agent
  help      Print this message or the help of the given subcommand(s)

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```

### `corrosion sync generate` example

```sh
$ ./corrosion sync generate
```
```json
{
  "actor_id": "7404bdec-7df2-41ff-b07a-e7647891edd3",
  "heads": {
    "7404bdec-7df2-41ff-b07a-e7647891edd3": 111974,
    "7a894059-ddee-49d9-bebd-fa83b0ed26a0": 495692,
    "d668410a-ff7a-47bd-b577-292e3e88694f": 672546
  },
  "need": {
    "7404bdec-7df2-41ff-b07a-e7647891edd3": [],
    "7a894059-ddee-49d9-bebd-fa83b0ed26a0": [],
    "d668410a-ff7a-47bd-b577-292e3e88694f": []
  },
  "partial_need": {}
}
```

## `template`

Use Corrosion's template engine to generate and start updating a local file based on a `.rhai` template file and the latest data from Corrosion.

Specify the name of the `.rhai` template and the desired name for the output file. 

```
Usage: corrosion template [OPTIONS] [TEMPLATE]...

Arguments:
  [TEMPLATE]...  

Options:
  -c, --config <CONFIG_PATH>     Set the config file path [default: corrosion.toml]
      --api-addr <API_ADDR>      
      --db-path <DB_PATH>        
      --admin-path <ADMIN_PATH>  
  -h, --help                     Print help
```

### `corrosion template` example
Contents of template file `./sw_template.rhai`:
```
<%= sql("SELECT * FROM sw;").to_json(#{pretty: true}) %>
```

Generate output file `output.json` using the above template, keeping it up to date as the relevant data updates in the connected Corrosion instance:

```cmd
./corrosion --api-addr "127.0.0.1:8080" template "./sw_template.rhai:output.json"
```
```out
tracing-filter directives: info
```

The command doesn't return, but if I `^C` it and repeatedly `cat` the contents of the output file, the result does change.

**To note: How to stop it?**


## Corrosion command examples

### Add data with `corrosion exec`

```
./corrosion exec "CREATE TABLE sw (machine_id INTEGER PRIMARY KEY, sandwich TEXT);"
```

Add to a table!

```
./corrosion exec "INSERT INTO tests (id, foo) VALUES (15, \"Woot\")"
```

Delete a row!

```
./corrosion exec "DELETE FROM tests WHERE id = 12;" 
```

Check it's gone:

```
$ ./corrosion query --columns --config ./config.example.toml 'SELECT * FROM tests;' 
id|foo
123|
13|HOW
15|Woot
Run Time: real 0.00001071
```



### Query Corrosion's database with `corrosion query`

Look at the contents of the `tests` table:

```
$  ./corrosion query --timer --columns --config ./config.example.toml 'SELECT * FROM tests;'
```

Result:

```
id|foo
123|
12|
13|HOW
15|Woot
Run Time: real 0.00001162
```



### Change the value of `foo` to `"boono"` in the row with `id = 1035` in table `tests`

```
./corrosion exec --timer --config ./config.example.toml "UPDATE tests SET foo = \"boono\" WHERE id = 1035"
```





### More examples

Check out the internal properties of the table:

```
$  ./corrosion query --timer --columns --config ./config.example.toml 'PRAGMA table_info(tests);' 
cid|name|type|notnull|dflt_value|pk
0|id|BIGINT|0||1
1|foo|TEXT|0||0
Run Time: real 0.0000108
```
