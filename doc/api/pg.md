# PostgreSQL Wire Protocol v3 API (experimental)

It's possible to configure a PostgreSQL wire protocol compatible API listener via the `api.pg.addr` setting.

This is currently experimental, but it does work for most queries that are SQLite-flavored SQL.

## What works

- Read and write queries, parsable as SQLite-flavored SQL
- Most parameter bindings, but not all (work in progress)

## Does not work

- Any PostgreSQL-only SQL syntax
- Some placement of variable parameters (when binding)