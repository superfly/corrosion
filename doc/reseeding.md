# Reseeding Corrosion

You may need to restart every Corrosion node from an earlier snapshot. Common reasons include:

- **Major corrosion updates** — a version upgrade might have changed the database structure of internal tables in an incompatible way.
- **Breaking schema change** — you need to make a breaking change to your database schema that cr-sqlite forbids like for ex. removing a column.
- **Bad data spreading** — unwanted data is propagating through the cluster, and restoring a known-good snapshot is faster than deleting or repairing it.
We call this restore-from-snapshot workflow a **reseed**. The rest of this document explains how to do it safely.

> **Warning — data loss is expected.**
> Reseeding from a snapshot means any writes accepted *after* the snapshot was taken will be lost. Either stop all writes before taking the snapshot, or be prepared to replay the missing writes afterward (see [Re-insert any missing data](#3-re-insert-any-missing-data)).

## Overview

The migration is performed in three phases:

1. On a single node, create a clean snapshot suitable (skip if you already have one).
2. Distribute the snapshot, stop corrosion and restore it on every node.
3. Start corrosion and re-apply any writes that happened after the snapshot was taken.

## 1. Prepare a clean snapshot

Pick one node in the old cluster and use it as the source of truth for the
snapshot. All other nodes will be restored from the backup created from this node.

### 1.1 Create a backup

While the corrosion agent is still running on the source node:

```bash
corrosion backup /path/to/snapshot.db
```

`corrosion backup` runs `VACUUM INTO` and strips per-node state
(`crsql_site_id`, `__corro_members`, `__corro_subs`, consul hash tables),
so the backup is often smaller than the running database.

### 1.2 Bump the cluster id

A new `cluster_id` ensures nodes that come up with the new snapshot don't
accidentally talk to any nodes still on the old snapshot. Run this against
the backup you just created:

```bash
sqlite3 /path/to/snapshot.db <<'SQL'
INSERT INTO __corro_state(key, value)
VALUES ('cluster_id', 1)
ON CONFLICT(key)
DO UPDATE SET value = value + 1;
SQL
```

If no `cluster_id` is set (it defaults to `0` when unset) this inserts `1`;
otherwise it increments the existing value by one.

### 1.3 Drop cr-sqlite internal tables (optional)

Do this if either of the following applies:

- You're upgrading versions and the cr-sqlite internal table schema has changed.
- Your database has grown large and you want a clean reset.

Dropping these tables lets them be recreated under the new schema (if schema changed) on startup. Run everything below against the **snapshot file**, not the live database.

**Drop the clock and primary-key tables:**

```bash
# Drop every internal cr-sqlite table.
sqlite3 /path/to/snapshot.db ".mode list" "SELECT 'DROP TABLE ' || name || ';' \
FROM sqlite_schema \
WHERE type = 'table' \
  AND (name LIKE '%__crsql_clock' \
    OR name LIKE '%__crsql_pks');" | sqlite3 /path/to/snapshot.db
```

This generates DROP TABLE statements from the schema, then pipes them back into sqlite3 to execute.

**Drop bookkeeping and reclaim space:**

```bash
sqlite3 /path/to/snapshot.db <<'SQL'
DROP TABLE IF EXISTS __corro_bookkeeping;

VACUUM;
SQL
```

The VACUUM reclaims space freed by the dropped tables and keeps the snapshot small for transfer.

### 1.4 Publish the snapshot

Compress the snapshot and upload it to a location every node can reach (S3, an internal artifact store, etc.):

```bash
pigz /path/to/snapshot.db
# upload /path/to/snapshot.db.gz somewhere
```

## 2. Restore every node from the snapshot

Repeat these steps on **every node** in the cluster:

1. **(Version upgrade only)** Install the new `corrosion` binary .
2. Download and decompress the snapshot to a local path.
3. Stop the Corrosion agent. `corrosion restore` refuses to run while the agent is up.
4. Restore:

   ```bash
   corrosion restore /path/to/snapshot.db
   ```

5. Start the Corrosion agent.

Once a node starts, it will recreate the `<table>__crsql_clock` and
`<table>__crsql_pks` tables with the new schema if needed and start gossiping
changes.

## 3. Re-insert any missing data

Any rows written between the snapshot being taken (step 1.1) and the cluster
coming back up on v1 are not present in the restored database. Re-apply them
normally (via `POST /v1/transactions` or the PostgreSQL endpoint) 
so they get re-inserted and gossiped to the rest of the cluster.

If you took the snapshot during a write freeze there is nothing to do; the
cluster is fully migrated.
