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

Applies one fully buffered (gap-free) version from `__corro_buffered_changes` into `crsql_changes`, in batches of `--chunk-size` seqs instead of a single insert. The agent normally applies these versions automatically; use this only when a changeset is too large for that path to finish. Prefer smaller transactions at write time over relying on this command.

```bash
corrosion sync process-buffered-changes <actor-id> <version> --chunk-size 1000
```
