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
