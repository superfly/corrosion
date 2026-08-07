# POST /v1/updates/{table}

Stream primary key update notifications for a single table. Unlike [`/v1/subscriptions`](subscriptions.md), this endpoint does **not** evaluate a SQL query — it only tells you *which primary keys* changed in the requested table. This makes it cheap to run and a good fit when you only need to invalidate caches or trigger downstream work, and you'll fetch fresh rows yourself.

The agent responds with a Newline Delimited JSON (NDJSON) stream that stays open for as long as the connection is alive.

## Request

### URL parameters

#### `table` (required, path)

Name of an existing CR-SQLite-enabled table to watch.

The table must already be defined in the cluster's [schema](../schema.md). Requests for unknown tables, or tables without primary keys, return an error.

### Body

The endpoint takes no body. Send an empty `POST`.

### Example

```bash
curl -X POST http://localhost:8080/v1/updates/sandwiches
```

## Response

### Body

The response body is an NDJSON stream of events. There is no initial snapshot — events only describe changes that happen *after* the request is received.

Example:

```json
{ "notify": ["update", ["sandwich-1"]] }
{ "notify": ["update", ["sandwich-2"]] }
{ "notify": ["delete", ["sandwich-1"]] }
```

#### Event type: `notify`

A change occurred for a row in the table. The value is a tuple of two elements:

1. Type of change: `"update"` or `"delete"`.
2. The primary key of the affected row, as an array of column values (in the order the columns appear in the table's primary key).

```json
{ "notify": ["update", ["pk_col_1_value", "pk_col_2_value"]] }
```

New inserts are reported as `"update"`. The endpoint deliberately surfaces only "the row at this primary key changed" or "the row at this primary key was deleted" — figuring out whether it is a new row is left to the client (typically by checking whether you already had a copy).

#### Event type: `error`

A fatal error occurred. The connection will be closed shortly after.

```json
{ "error": "some error message" }
```

## Behavior and caveats

### No SQL, no filtering

`/v1/updates/{table}` only filters by table and doesn't accept sql queries. If you need to receive updates for a particular query, use [`/v1/subscriptions`](subscriptions.md) instead.


## Client usage

The `corro-client` Rust crate exposes this endpoint via [`CorrosionClient::updates`](https://github.com/superfly/corrosion/blob/main/crates/corro-client/src/lib.rs). The same guidance from the [subscriptions client guide](subscriptions.md#client-implementation-guide) applies: treat `error` events as fatal, buffer events client-side if you can't keep up, and retry with backoff on disconnect.
