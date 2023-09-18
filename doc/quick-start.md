# Quick start

This is a simple of starting a 2-node Corrosion cluster running on the same host and replicating data.

## Node A

### 1. Create a schema file

```sql
-- /etc/corrosion/schema/todo.sql

CREATE TABLE todos (
    id BLOB PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    completed_at INTEGER
);
```

### 2. Configure Corrosion

```toml
# /etc/corrosion/config.toml

[db]
path = "/var/lib/corrosion/state.db"
schema_paths = ["/etc/corrosion/schema"]

[gossip]
addr = "[::]:8787"
plaintext = true

[api]
addr = "127.0.0.1:8080"

[admin]
path = "/var/run/corrosion/admin.sock"
```

### 3. Start a Corrosion agent

```bash
$ corrosion agent
2023-09-18T13:13:26.526907Z  INFO corrosion::command::agent: Starting Corrosion Agent v0.0.1
2023-09-18T13:13:26.623782Z  INFO corro_agent::agent: Actor ID: a3f72d6d38a24d0daee8258e10071f13
2023-09-18T13:13:26.655779Z  INFO corro_admin: Starting Corrosion admin socket at /var/run/corrosion/admin.sock
2023-09-18T13:13:26.658476Z  INFO corro_agent::agent: Starting peer API on udp/[::]:8787 (QUIC)
2023-09-18T13:13:26.661713Z  INFO corro_agent::agent: Starting public API server on tcp/127.0.0.1:8080
2023-09-18T13:13:27.022947Z  INFO corro_types::schema: creating table 'todos'
2023-09-18T13:13:27.023884Z  INFO corro_agent::api::public: updated 1 rows in __corro_schema for table todos
2023-09-18T13:13:27.025223Z  INFO corrosion::command::agent: Applied schema in 0.35491575s
```

### 4. Insert some data

```bash
$ corrosion exec --param 'some-id' --param 'Write some Corrosion docs!' 'INSERT INTO todos (id, title) VALUES (?, ?)'
INFO corrosion: Rows affected: 1
```

### 5. Query some data

Either via SQLite directly if you have access to the database directly:

```bash
$ sqlite3 /var/lib/corrosion/state.db 'SELECT * FROM todos;'
some-id|Write some Corrosion docs!|
```

or via the API if you don't:

```bash
$ corrosion query 'SELECT * FROM todos;'
some-id|Write some Corrosion docs!|
```

## Node B

### 1. Copy the schema file

```bash
$ mkdir -p /etc/corrosion-b/schema
$ cp /etc/corrosion/schema/todo.sql /etc/corrosion-b/schema/todo.sql
```

### 2. Configure Corrosion

```toml
# /etc/corrosion-b/config.toml

[db]
path = "/var/lib/corrosion-b/state.db"
schema_paths = ["/etc/corrosion-b/schema"]

[gossip]
addr = "[::]:8788"
bootstrap = ["[::1]:8787"] # bootstrap the node's cluster discovery w/ node A
plaintext = true

[api]
addr = "127.0.0.1:8081"

[admin]
path = "/var/run/corrosion-b/admin.sock"
```

### 3. Start a Corrosion agent

```bash
$ corrosion -c /etc/corrosion-b/config.toml agent
2023-09-18T13:37:00.696728Z  INFO corrosion::command::agent: Starting Corrosion Agent v0.0.1
2023-09-18T13:37:00.768080Z  INFO corro_agent::agent: Actor ID: 4e3e57d1faee47449c1f238559284bc2
2023-09-18T13:37:00.772773Z  INFO corro_admin: Starting Corrosion admin socket at /var/run/corrosion-b/admin.sock
2023-09-18T13:37:00.773162Z  INFO corro_agent::agent: Starting peer API on udp/[::]:8788 (QUIC)
2023-09-18T13:37:00.773559Z  INFO corro_agent::agent: Starting public API server on tcp/127.0.0.1:8081
2023-09-18T13:37:00.775504Z  INFO corro_types::schema: creating table 'todos'
2023-09-18T13:37:00.775964Z  INFO corro_agent::api::public: updated 1 rows in __corro_schema for table todos
2023-09-18T13:37:00.776398Z  INFO corro_agent::agent: Current node is considered ACTIVE
2023-09-18T13:37:00.776731Z  INFO corrosion::command::agent: Applied schema in 0.001515042s
2023-09-18T13:37:01.954585Z  INFO corro_agent::agent: synced 2 changes w/ b4fcbb65501f44f0802aba631508be9d in 0.012817167s @ 156.04072257153237 changes/s
```

This last log shows that node B synchronized changes w/ node A.

#### Why were `2 changes` synchronized? There's only 1 row!

cr-sqlite creates 1 change per column "changed" in a row. It's possible to inspect this directly for troubleshooting:

```bash
$ sqlite3 data-b/corrosion.db
sqlite> .mode column
sqlite> select * from todos__crsql_clock;
id         __crsql_col_name  __crsql_col_version  __crsql_db_version  __crsql_site_id  __crsql_seq
---------  ----------------  -------------------  ------------------  ---------------  -----------
some-id    title             1                    1                   1                0
some-id    completed_at      1                    1                   1                1
```

### 4. Query for the just-synchronized data

Either via SQLite directly if you have access to the database directly:

```bash
$ sqlite3 /var/lib/corrosion-b/state.db 'SELECT * FROM todos;'
some-id|Write some Corrosion docs!|
```

### 5. Insert some data of our own

```bash
$ corrosion -c /etc/corrosion-b/config.toml exec --param 'some-id-2' --param 'Show how broadcasts work' 'INSERT INTO todos (id, title) VALUES (?, ?)'
INFO corrosion: Rows affected: 1
```

### 6. Query data from Node A

The second row has been propagated.

```bash
# here we're pointing at node A's config explicitly for clarity, the default is /etc/corrosion/config.toml
$ corrosion -c /etc/corrosion/config.toml query 'SELECT * FROM todos;'
some-id|Write some Corrosion docs!|
some-id-2|Show how broadcasts work|
```