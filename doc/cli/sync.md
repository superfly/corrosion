# The `corrosion sync` command

Debugging commands used to inspect or fix Corrosion's internal bookkeeping state.

## Subcommands

### `corrosion sync generate`

Output in-memory bookkeeping state as JSON. This is mostly used for when debugging replication, inspecting what the node sends
during sync with other nodes.

```bash
corrosion sync generate
```

### `corrosion sync reconcile-gaps`

This command collapses overlapping gaps (missing versions) in the database and reconciles the in-memory bookie with the data on the database.

```bash
corrosion sync reconcile-gaps
```

### `corrosion sync check-bookie-consistency`

Compares in-memory bookie state with database bookie state for all actors and prints a JSON report to stdout. If the command finds mismatches (`ok: false`), the command fails with an error summarizing counts of value mismatches and keys only in memory vs only in DB.

```bash
corrosion sync check-bookie-consistency
```

### `corrosion sync process-buffered-changes`

Applies a single fully-buffered (gap-free) version's changes from `__corro_buffered_changes` into `crsql_changes`, in batches of `--chunk-size` rows instead of one large insert. Useful for a version whose buffered changeset is too large to comfortably apply in one transaction. Versions are normally applied automatically as soon as they become gap-free; use this command to nudge a specific `actor-id`/`version` along in smaller steps. Fails if the given version isn't known or still has gaps.

```bash
corrosion sync process-buffered-changes <actor-id> <version> --chunk-size 1000
```
