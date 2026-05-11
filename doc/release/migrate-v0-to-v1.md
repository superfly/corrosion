# Migrating from Corrosion v0.1.0 to v1.0.0

The schema of cr-sqlite's internal tables changed between Corrosion v0 and v1. The most significant changes are:

- The clock tables (`<table>__crsql_clock`) have a new schema.
- The `__corro_bookkeeping` table has been removed.

Because of these schema differences, a running v0 cluster cannot be upgraded
in-place. Instead, the entire cluster must be re-bootstrapped from a single
snapshot that contains no cr-sqlite internal tables and no corrosion
bookkeeping table. When the v1 nodes start against that snapshot, they
recreate the internal state from scratch using the v1 schema.

```admonish warning
Re-bootstrapping the cluster from a snapshot means that any writes accepted
after the snapshot was taken will be lost. Either stop all writes before
taking the snapshot, or be prepared to replay the missing writes after the
migration (see [Re-insert any missing data](#re-insert-any-missing-data)).
```

## Overview

The migration is performed in three phases:

1. On a single v0 node, produce a clean snapshot suitable for v1.
2. Distribute the snapshot and restore it on every node, running v1.
3. Re-apply any writes that happened after the snapshot was taken.

## 1. Prepare a clean snapshot

Pick one node in the old cluster and use it as the source of truth for the
snapshot. All other nodes will be restored from the backup created from this node.

### 1.1 Create a backup

While the corrosion agent is still running on the source node:

```bash
corrosion backup /path/to/v0-snapshot.db
```

`corrosion backup` runs `VACUUM INTO` and strips per-node state
(`crsql_site_id`, `__corro_members`, `__corro_subs`, consul hash tables),
so the backup is often smaller than the running database.

### 1.2 Bump the cluster id

A new `cluster_id` ensures nodes that come up with the new snapshot don't
accidentally talk to any nodes still on the old version. Run this against
the backup you just created:

```bash
sqlite3 /path/to/v0-snapshot.db <<'SQL'
INSERT INTO __corro_state(key, value)
VALUES ('cluster_id', 1)
ON CONFLICT(key)
DO UPDATE SET value = value + 1;
SQL
```

If no `cluster_id` is set (it defaults to `0` when unset) this inserts `1`;
otherwise it increments the existing value by one.

### 1.3 Drop cr-sqlite internal tables

The backup still contains cr-sqlite internal tables. Drop them so that
v1 can recreate them under the new schema. Run the following against the
snapshot file (not the live database):

```bash
# Drop every internal cr-sqlite table.
sqlite3 v0-snapshot.db ".mode list" "SELECT 'DROP TABLE ' || name || ';' \
FROM sqlite_schema \
WHERE type = 'table' \
  AND (name LIKE '%__crsql_clock' \
    OR name LIKE '%__crsql_pks');" | sqlite3 v0-snapshot.db
```

This first queries SQLite to generate the `DROP TABLE` statements, then
pipes them back into `sqlite3` to execute them.

Delete the `__corro_bookkeeping` table and vacuum the backup:

```bash
sqlite3 /path/to/v0-snapshot.db <<'SQL'
DROP TABLE IF EXISTS __corro_bookkeeping;

VACUUM;
SQL
```

The final `VACUUM` reclaims the space freed by the dropped tables and keeps
the snapshot small for transfer.

### 1.4 Publish the snapshot

Compress the snapshot and upload it to a location every node can reach (S3, an internal artifact store, etc.):

```bash
pigz /path/to/v0-snapshot.db
# upload /path/to/v0-snapshot.db.gz somewhere
```

## 2. Restore every node from the snapshot

For each node in the cluster:

1. Install the `corrosion` v1 binary.
2. Download and decompress the snapshot to a local path.
3. Stop the Corrosion agent. `corrosion restore` refuses to run while the agent is up.
4. Restore it:

   ```bash
   corrosion restore /path/to/v0-snapshot.db
   ```

5. Start the v1 agent.

Once a v1 node starts, it will create the `<table>__crsql_clock` and
`<table>__crsql_pks` tables with the new schema and start gossiping
changes.

## 3. Re-insert any missing data

Any rows written between the snapshot being taken (step 1.1) and the cluster
coming back up on v1 are not present in the restored database. Re-apply them
normally (via `POST /v1/transactions` or the PostgreSQL endpoint) 
so they get re-inserted and gossiped to the rest of the cluster.

If you took the snapshot during a write freeze there is nothing to do; the
cluster is fully migrated.
