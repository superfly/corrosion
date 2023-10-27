# Conflict-free Replicated Data Types

## What's a CRDT?

[About CRDTs](https://crdt.tech/#:~:text=Conflict%2Dfree%20Replicated%20Data%20Types%20(CRDTs)%20are%20used%20in,merged%20into%20a%20consistent%20state.)

```admonish
Conflict-free Replicated Data Types (CRDTs) are used in systems with optimistic replication, where they take care of conflict resolution. CRDTs ensure that, no matter what data modifications are made on different replicas, the data can always be merged into a consistent state. This merge is performed automatically by the CRDT, without requiring any special conflict resolution code or user intervention.
```

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