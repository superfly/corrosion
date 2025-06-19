# Conflict-free Replicated Data Types

## What's a CRDT?

[About CRDTs](https://crdt.tech/#:~:text=Conflict%2Dfree%20Replicated%20Data%20Types%20(CRDTs)%20are%20used%20in,merged%20into%20a%20consistent%20state.)

> Conflict-free Replicated Data Types (CRDTs) are used in systems with optimistic replication, where they take care of conflict resolution. CRDTs ensure that, no matter what data modifications are made on different replicas, the data can always be merged into a consistent state. This merge is performed automatically by the CRDT, without requiring any special conflict resolution code or user intervention.

## cr-sqlite

Corrosion uses the [`cr-sqlite` SQLite extension](https://github.com/vlcn-io/cr-sqlite) to accomplish its multi-writer and eventual consistency promise. Here's a [short intro](https://vlcn.io/docs/cr-sqlite/intro) about how it generally works. The rest of this section assumes some knowledge of cr-sqlite.

`cr-sqlite` provides functions to mark tables, in a SQLite database, as backed by CRDTs. These include Causal-Length ([pdf paper](https://dl.acm.org/doi/pdf/10.1145/3380787.3393678)) and Last-Write-Wins (LWW).

As of cr-sqlite 0.15, the CRDT for an existing row being update is this:
1. Biggest `col_version` wins
2. In case of a tie, the "biggest" value is used.

### Basics

With the extension loaded, writes to CRDT-backed tables will trigger insertions in internal tables for each column in a row.

An aggregate view table is available as `crsql_changes`. By `SELECT`-ing from this table and `INSERT`-ing into it, it's possible to distribute changes to a cluster. Each set of changes (a transaction produces a single set of changes) gets a new `db_version`, unique to the database.

## Corrosion and cr-sqlite

Corrosion executes transactions by processing requests made to its client HTTP API. Each transaction triggers 1+ broadcast (big changesets are chunked). Each change is serialized in an efficient format and sent to ~random members of the cluster.

The main caveat of this approach is: **writes to the database all have to go through Corrosion**. If a sqlite client were to issue writes w/ or w/o the proper extension loaded, then data would become inconsistent for CRDT-backed tables.

## CRsqlite tables

Crsqlite adds several virtual tables, but the main one I wanna look at is `crsql_changes`, since this is what we mainly interact with from corrosion.  Each "real" data table also gets its own `<table name>__crsql_clock` table, which largely keeps track of the same information, but specific to that one table.  These refer to (and keep track of) the "logical clock" of certain changes.  A logical clock is a mechanism to establish causality of changes, without needing an actual, synchronous global clock between different participants in a system.  Crsqlite specifically uses a ["lamport timestamp"](https://en.wikipedia.org/wiki/Lamport_timestamp) which, if you squint at from a distance, could be most concisely boiled down to a monotonically increasing counter.

Fun fact, crsqlite has another special table, the `crsql_site_id`, which we use to get a unique actor ID in our corrosion cluster (and which keeps track of other known actors IDs from across the network).

```sqlite
sqlite> select hex(site_id) from crsql_site_id;
D5F143E7BA65421C938C850CE78FC9F2
```

(This also means that by deleting the `corrosion.db` when re-instantiating a node, that node's actor ID will change automatically).

Let's start by setting up a new database with crsqlite enabled, and create a new table `my_machines` which simply keeps track of a machine ID, its name, and its status (if you wanna follow along, you'll have to download the library from the [github releases](https://github.com/vlcn-io/cr-sqlite/releases)).  Afterwards we call the special `crsql_as_crr` ("as conflict-free replicated relation") function to instantiate crsql for this particular table.  This means that we can opt-into crsql on a table-by-table basis!

```sqlite
 ❤ (tempest) ~/P/_/flyio> sqlite3 test1.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> .load extra-lib/crsqlite.so
sqlite> create table my_machines (id primary key not null, name text not null default '', status text not null default 'broken');
sqlite> select crsql_as_crr('my_machines');
OK
```

Importantly: to apply `crsql_as_crr` any fields that are `not null` must have a default value to allow for forwards (and backwards) compatible schema changes.  In a distributed system this is much more important than for a traditional database server.

## A simple example: generating some changes

Ok let's actually insert some data.  Let's say we create two new machines `meow` and `woof`:

```sqlite
sqlite> .mode qbox
sqlite> insert into my_machines (id, name, status) values (1, 'meow', 'created');
sqlite> insert into my_machines (id, name, status) values (2, 'woof', 'created');
qlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010901' │ 'status' │ 'created' │ 1           │ 1          │ 1  │ 1   │
│ 'my_machines' │ x'010902' │ 'name'   │ 'woof'    │ 1           │ 2          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'status' │ 'created' │ 1           │ 2          │ 1  │ 1   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```

So what happened here?  Well, let's see what these values mean:

- `table` :: which table was the change made on.
- `pk` :: the respective primary key for each of the changes, encoded in a funky way to send and validate between different writers/ nodes.
- `cid` and `val` :: the column id of the change, along with its new value
- `col_version` :: an incremented counter for each column.  Whenever a change is applied to a column, this counter is incremented.
- `db_version` :: an overall database version.  With each _transaction_ this counter is incremented.  So you can see that the two column changes for `meow` are both on `db_version = 1`, and it gets incremented to `db_version = 2` when we insert `woof`.
- `cl` :: this is the "causal length", which indicates whether the row is still present or was deleted.  I had to do some digging to learn about this, but essentially it indicates the number of operations that causally (i.e. in relationship to) preceeded a given change.  De-facto I don't think this value is currently used (at least not in our use-cases, but please correct me on that).
- `seq` :: the order of changes in a larger transaction, making sure to apply a big insertion in the same order on each node.  When merging multiple changes we make sure to remove redundant sequences.

Ok so far so good.  But let's setup a second node/ database and insert the crsql changes there:

```sqlite
 ❤ (tempest) ~/P/_/flyio> sqlite3 test1.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> .mode qbox
sqlite> .load extra-lib/crsqlite.so
sqlite> create table my_machines (id primary key not null default '', name text not null default '', status text not null default 'broken');
sqlite> select crsql_as_crr('my_machines');
┌─────────────────────────────┐
│ crsql_as_crr('my_machines') │
├─────────────────────────────┤
│ 'OK'                        │
└─────────────────────────────┘
```

Now, something I ignored in the previous insert is the `site_id` (in corrosion parlour the "actor ID").  Each change has a source after all, which is represented by the actor ID.  In our case it is `D5F143E7BA65421C938C850CE78FC9F2` for node 1 and `75D983BA38A644E987735592FB89CA70` for node 2.  And so, when inserting into `test2.db`'s crsql_changes we need to consider it.

```sqlite
sqlite> insert into crsql_changes values ('my_machines', X'010901', 'name', 'meow', 1, 1, X'D5F143E7BA65421C938C850CE78FC9F2', 1, 0);
sqlite> insert into crsql_changes values ('my_machines', X'010901', 'status', 'created', 1, 1, X'D5F143E7BA65421C938C850CE78FC9F2', 1, 1);
sqlite> insert into crsql_changes values ('my_machines', X'010902', 'name', 'woof', 1, 2, X'D5F143E7BA65421C938C850CE78FC9F2', 1, 0);
sqlite> insert into crsql_changes values ('my_machines', X'010902', 'status', 'created', 1, 2, X'D5F143E7BA65421C938C850CE78FC9F2', 1, 1);
```

So what does `test2.db` look like now?

```sqlite
sqlite> select "name", "status" from my_machines;
┌────────┬───────────┐
│  name  │  status   │
├────────┼───────────┤
│ 'meow' │ 'created' │
│ 'woof' │ 'created' │
└────────┴───────────┘
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010901' │ 'status' │ 'created' │ 1           │ 2          │ 1  │ 1   │
│ 'my_machines' │ x'010902' │ 'name'   │ 'woof'    │ 1           │ 3          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'status' │ 'created' │ 1           │ 4          │ 1  │ 1   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```

Neat!  So you see, by sending each of the changes from `crsql_changes` to other nodes we can apply things as they were applied in the original database.  Note how the `db_version` is incremented with every individual change, while on the original node each impacted row shares a version.  This is not what happens in corrosion, because we batch multiple changes into the same transaction, which isn't the case for me in the `sqlite3` repl:

### Corrosion node A

```shell
 ❤  (tempest) ~/P/_/f/corrosion> sqlite3 devel-state/A/corrosion.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> .mode qbox
sqlite> .load ../extra-lib/crsqlite.so
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010901' │ 'status' │ 'created' │ 1           │ 1          │ 1  │ 1   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```

### Corrosion node B

```shell
 ❤  (tempest) ~/P/_/f/corrosion> sqlite3 devel-state/B/corrosion.db
SQLite version 3.45.1 2024-01-30 16:01:20
Enter ".help" for usage hints.
sqlite> .mode qbox
sqlite> .load ../extra-lib/crsqlite.so
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010901' │ 'status' │ 'created' │ 1           │ 1          │ 1  │ 1   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```


## Handling conflicting changes

Now, what if, god forbid, we create some kind of conflict.  Let's say we set the machine `meow` to `started` in test1.db and to `destroyed` in test2.db.

On node 1:

```sqlite
sqlite> update my_machines set status = 'started' where name = 'meow';
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'name'   │ 'woof'    │ 1           │ 2          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'status' │ 'created' │ 1           │ 2          │ 1  │ 1   │
│ 'my_machines' │ x'010901' │ 'status' │ 'started' │ 2           │ 3          │ 1  │ 0   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```

On node 2:

```sqlite
sqlite> update my_machines set status = 'destroyed' where name = 'meow';
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬─────────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │     val     │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼─────────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'      │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'name'   │ 'woof'      │ 1           │ 3          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'status' │ 'created'   │ 1           │ 4          │ 1  │ 1   │
│ 'my_machines' │ x'010901' │ 'status' │ 'destroyed' │ 2           │ 5          │ 1  │ 0   │
└───────────────┴───────────┴──────────┴─────────────┴─────────────┴────────────┴────┴─────┘
```

Let's apply the change from `1` on `2`:

```sqlite
sqlite> insert into crsql_changes values ('my_machines', X'010901', 'status', 'started', 2, 3, X'D5F143E7BA65421C938C850CE78FC9F2', 1, 0);
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'name'   │ 'woof'    │ 1           │ 3          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'status' │ 'created' │ 1           │ 4          │ 1  │ 1   │
│ 'my_machines' │ x'010901' │ 'status' │ 'started' │ 2           │ 6          │ 1  │ 0   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```

Huh, so our `status = destroyed` was overwritten by the `status = started` change.  Let's apply the change in the reverse direction (i.e. let test1.db know that we destroyed the machine).

```sqlite
sqlite> insert into crsql_changes values ('my_machines', X'010901', 'status', 'destroyed', 2, 5, X'75D983BA38A644E987735592FB89CA70', 1, 0);
sqlite> select "table", "pk", "cid", "val", "col_version", "db_version", "cl", "seq" from crsql_changes;
┌───────────────┬───────────┬──────────┬───────────┬─────────────┬────────────┬────┬─────┐
│     table     │    pk     │   cid    │    val    │ col_version │ db_version │ cl │ seq │
├───────────────┼───────────┼──────────┼───────────┼─────────────┼────────────┼────┼─────┤
│ 'my_machines' │ x'010901' │ 'name'   │ 'meow'    │ 1           │ 1          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'name'   │ 'woof'    │ 1           │ 2          │ 1  │ 0   │
│ 'my_machines' │ x'010902' │ 'status' │ 'created' │ 1           │ 2          │ 1  │ 1   │
│ 'my_machines' │ x'010901' │ 'status' │ 'started' │ 2           │ 3          │ 1  │ 0   │
└───────────────┴───────────┴──────────┴───────────┴─────────────┴────────────┴────┴─────┘
```

The machine remains started!  Which is good.  We don't want nodes disagreeing with each other about what the state of a machine (or any data, really) is.  This is what "eventually consistent" means: eventually all the nodes are going to agree on what the state should be, even if there's some other funky writes in the system that may temporarily disagree.  Why did it pick started?  Well, because crsql uses a "largest write wins" strategy.

The order in which crsql checks for which value is "larger" is: `col_version`, followed by the `value` of the change, and finally the `site_id` (so essentially randomly picking one because the `site_id` is a random data).

In our example you can see that both the `status = started` and `status = destroyed` changes had a `col_version = 2`, so that comparison is out.  What about the value?  Sqlite provides a `max` function which uses lexographic ordering to determine which string is "bigger".  `destroyed` comes before `started` and so `started` is "bigger":

```
sqlite> select max('started', 'destroyed');
┌─────────────────────────────┐
│ max('started', 'destroyed') │
├─────────────────────────────┤
│ 'started'                   │
└─────────────────────────────┘
```
