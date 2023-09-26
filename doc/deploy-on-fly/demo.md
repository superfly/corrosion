# Work with cluster data

With your Corrosion cluster deployed, you can work with the example database.

To get started, shell into each Corrosion node, in separate terminals. 
```
fly ssh console --pty --app <your-app-name> --select`
```

We'll call one Machine "Node A" and the other "Node B". Every node is read-write, so it doesn't matter which is which.

The example schema, `todo.sql`, specifies a single table called `todos`, with `id`, `title`, and `completed_at` columns.

```sql
-- /etc/corrosion/schemas/todo.sql

CREATE TABLE todos (
    id BLOB PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    completed_at INTEGER
);
```

## Inserting and querying data

### Insert some data on Node A

```bash
# corrosion exec --param 'some-id' --param 'Write some Corrosion docs!' 'INSERT INTO todos (id, title) VALUES (?, ?)'
INFO corrosion: Rows affected: 1
```
### Query data on Node A

Via SQLite directly:

```bash
# sqlite3 /var/lib/corrosion/state.db 'SELECT * FROM todos;'
some-id|Write some Corrosion docs!|
```

Using the API, via the CLI:

```bash
# corrosion query 'SELECT * FROM todos;' --columns
id|title|completed_at
some-id|Write some Corrosion docs!|
```

### Query data on Node B

```bash
# corrosion query 'SELECT * FROM todos;' --columns
id|title|completed_at
some-id|Write some Corrosion docs!|
```
Node A's contribution is already present in Node B's database.

### Insert data on Node B

```bash
# corrosion exec --param 'some-id-2' --param 'Show how broadcasts work' 'INSERT INTO todos (id, title) VALUES (?, ?)'
INFO corrosion: Rows affected: 1
```

### Check the data on Node A
```bash
# corrosion query 'SELECT * FROM todos;' --columns
id|title|completed_at
some-id|Write some Corrosion docs!|
some-id-2|Show how broadcasts work|
``` 

The second update has propagated back to Node A.

## Updating a file using a Corrosion template

The example template `todos.rhai` makes a checklist out of the rows in our `todos` table.

```js
/* /etc/corrosion/templates/todos.rhai */

<% for todo in sql("SELECT title, completed_at FROM todos") { %>
[<% if todo.completed_at.is_null() { %> <% } else { %>X<% } %>] <%= todo.title %>
<% } %>
```

### Start `corrosion template` and watch the output file

On Node A, start processing the template. 

```bash
# corrosion template "/etc/corrosion/templates/todos.rhai:todos.txt" &
[1] 354
root@4d8964eb9d9487:/#  INFO corrosion::command::tpl: Watching and rendering /etc/corrosion/templates/todos.rhai to todos.txt
```
Whenever there's an update to the results of the template's query (or queries), `corrosion template` re-renders the output file.

Start watching the output file.

```bash
# watch -n 0.5 cat todos.txt
Every 0.5s: cat todos.txt

[ ] Write some Corrosion docs!
[ ] Show how broadcasts work
```

### Add a todo item

On the other Machine (Node B), insert some data.

```bash
# corrosion exec --param 'some-id-3' --param 'Hello from a template!' 'INSERT INTO todos (id, title) VALUES (?, ?)'
INFO corrosion: Rows affected: 1
```

The new todo item gets propagated back to Node A, and your watch should look like this:

```
Every 0.5s: cat todos.txt

[ ] Write some Corrosion docs!
[ ] Show how broadcasts work
[ ] Hello from a template!
```

### Mark all items as done

Mark all tasks as completed, on either node:

```bash
# corrosion exec 'UPDATE todos SET completed_at = 1234567890'
INFO corrosion: Rows affected: 3
```

```bash
$ watch -n 0.5 cat todos.txt
Every 0.5s: cat todos.txt

[X] Write some Corrosion docs!
[X] Show how broadcasts work
[X] Hello from a template!
```


