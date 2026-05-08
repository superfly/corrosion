# The `[reaper]` configuration

Corrosion keeps data around in its CR-SQLite bookkeeping tables even when associated rows have been deleted. For tables with many deleted keys that are never re-inserted, this can take up unnecessary space.

The reaper config allows you specify tables so Corrosion will periodically delete cr-sqlite bookkeeping rows (the `<table>__crsql_clock` and `<table>__crsql_pks` tables) for primary keys that were deleted long enough ago that you no longer expect them to come back.

When unset, no reaping is performed and bookkeeping rows for deleted primary keys are kept forever.

```admonish warning
Reaping a table is an irreversible cluster-wide operation. If a deleted primary key reappears (e.g. a row is recreated using the same id) **after** its retention has elapsed, You may end up with inconsistent state across the cluster if there are nodes that are yet to reap that primary key. Only configure the reaper for tables whose primary keys are guaranteed not to be reused, and prefer generous retention windows.
```

### Optional fields

#### `reaper.check_interval`

How often (in seconds) the reaper wakes up and scans configured tables. Defaults to `3600` (one hour).

```toml
[reaper]
check_interval = 3600
```

#### `reaper.tables`


##### `reaper.tables.<table>.retention` (required)

How long after a primary key has been deleted Corrosion should wait before garbage-collecting its cr-sqlite bookkeeping rows. Expressed as a number followed by a unit:

| Unit | Meaning |
| ---- | ------- |
| `s`  | seconds |
| `m`  | minutes |
| `h`  | hours   |
| `d`  | days    |
| `w`  | weeks   |
| `y`  | years (365 days) |

Examples: `"30s"`, `"15m"`, `"24h"`, `"14d"`, `"4w"`, `"1y"`.

##### `reaper.tables.<table>.match_filter` (optional)

SQL `WHERE`-clause fragment, **without the leading `WHERE`**, restricting which deleted primary keys this rule applies to. The fragment is referenced as `AND (<match_filter>)` in the generated query, so it must be valid in that position and is constrained to columns of the bookkeeping table.

Use this to scope reaping to keys you've explicitly marked as throwaway:

```toml
[reaper.tables.sessions]
retention    = "7d"
match_filter = "id LIKE 'tmp-%'"
```

## Example config

```toml
[reaper]
check_interval = 3600  # optional, seconds, defaults to 3600

# Reap primary keys of `sessions` 7 days after deletion, but only those whose id was prefixed with `tmp-`.
[reaper.tables.sessions]
retention    = "7d"
match_filter = "id LIKE 'tmp-%'"

# Reap primary key of `audit_log` tables once 90 days after deletion.
[reaper.tables.audit_log]
retention = "90d"
```
