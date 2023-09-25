# POST /v1/subscriptions

Start receiving updates for a desired SQL query. The `/v1/subscriptions` endpoint accepts a single SQL statement in JSON format.
The Corrosion agent responds with a Newline Delimited JSON (`NDJSON`) stream that notifies of any changes to the response to this query.

## Request

### URL query params

#### `from={change_id}` (optional)

If you are re-subscribing, this will start returning events from that point on.

### Body

Query statement to subscribe to as a JSON string.

```json
"SELECT sandwich FROM sandwiches"
```

Accepts SQL params by using an array:

```json
["SELECT sandwich FROM sandwiches WHERE name = ?", ["my-sandwich-name"]]
```

### Example

```bash
curl http://localhost:8080/v1/subscriptions \
 -H "content-type: application/json" \
 -d "\"SELECT sandwich FROM sandwiches\""
```

## Response

### Headers

Returns a Query ID (UUID) that can be referenced later to re-subscribe.

Example:

```
corro-query-id: ba247cbc-2a7f-486b-873c-8a9620e72182
```

### Body

Response bodies will contain Newline Delimited JSON (NDJSON) stream of events.

Example:

```json
{ "columns": ["sandwich"] }
{ "row":     [1, ["shiitake"]] }
{ "row":     [2, ["ham"]] }
{ "row":     [3, ["grilled cheese"]] }
{ "row":     [4, ["brie and cranberry"]] }
{ "eoq":     { "time": 8e-8, "change_id": 0 } }
{ "change":  ["update", 2, ["smoked meat"], 1] }
{ "change":  ["update", 1, ["smoked meat"], 2] }
{ "change":  ["update", 2, ["ham"], 3] }
{ "change":  ["update", 1, ["burger"], 4] }
{ "change":  ["update", 2, ["smoked meat"], 5] }
// ...
```

#### Event type: `columns`

Name of all columns returned by the query

```json
{ "columns": ["col_1", "col_2"] }
```

#### Event type: `row`

A tuple as an array of 2 elements containing the query result rowid and all column values as an array.

```json
{ "row": [1, ["cell_1", "cell_2"]] }
```

#### Event type: `eoq`

End Of Query (EOQ) marks the end of the initial query results. Useful for determining when to perform an initial render of a template, for example.

It also includes:
- Query execution time (not counting iterating all rows, just the actual query)
- Last change ID recorded for the rows it sent

The latter is useful to resume a subscription stream when you received all rows but never got a change and you don't want to start from `0`.

```json
{ "eoq": { "time": 8e-8, "change_id": 0 } }
```

#### Event type: `change`

A wild, new, result for your query appears!

Represented by a tupled as an array of 4 elements:

1. Type of change (`insert`, `update`, `delete`)
2. Row ID for the modified record (unique per query)
3. **All** values of the columns, even on deletion
4. Change ID (unique and contiguously increasing per query)

It has been designed this way to make it easy to change single records out of a map of `rowid -> record`. Allowing users to create memory-efficient reactive interfaces.

With the Change ID, it is possible to pick back up a subscription from an existing point. Useful in disconnection events or restarts of either Corrosion or a client.

```json
{ "change": ["update", 1, ["cell_1", "cell_2"], 1] }
{ "change": ["insert", 2, ["cell_a", "cell_b"], 2] }
{ "change": ["delete", 2, ["cell_a", "cell_b"], 3] }
```

# GET /v1/subscriptions/:id

Subscribe to an already existing query, without prior knowledge of the SQL, knowing the Query ID (UUID).

## Request

### URL query params

Passing no query parameters will return all previous rows for the query and all future changes.

#### `from={change_id}` (optional)

If you are re-subscribing, this will start returning events from that point on.

### Examples

```bash
curl http://localhost:8080/v1/subscriptions/ba247cbc-2a7f-486b-873c-8a9620e72182
{ "columns": ["sandwich"] }
{ "row":     [1, ["shiitake"]] }
{ "row":     [2, ["ham"]] }
{ "eoq":     { "time": 8e-8, "change_id": 2 } }
```

```bash
curl http://localhost:8080/v1/subscriptions/ba247cbc-2a7f-486b-873c-8a9620e72182?from=1
{ "change": [2, "insert", ["shiitake"], 2] }
{ "change": [3, "insert", ["grilled cheese"], 3] }
```

## Response

Exact same as `POST /v1/subscriptions`

# Client implementation guide

If you can digest Rust, the `corro-client` crate in Corrosion's repository provides a decent implementation.

## Handling errors

Any error-type message received should be considered "fatal" for the client. Some errors cannot be recovered from server-side, in which case it won't be possible to re-subscribe to a subscription.

## Buffering data

If your client cannot process rows / changes fast enough, it should buffer them to avoid receiving an error. If any client lags too much, Corrosion will send an error and terminate the request. Sometimes that only leaves the clients a few milliseconds to process a row / change. There's only so much buffering Corrosion will do server-side.

## Reconnections and retries

It is encouraged to provide a seamless experience in the event of network errors. By storing the subscription ID and the last obversed change ID, it should be possible to resume subscriptions.

Retrying in a loop w/ a backoff is encouraged, as long as the client gives up after a while and return an error actionable by programs or users.

# Usage guide

## Reactivity

Mapping data by row ID (often referred to as `rowid`) is ideal. When receiving changes, they refer to the affected rowid so a consumer can proceed with modifying data with minimal memory usage.

In many cases, it may not be necessary to store each row's cells and instead just a reference to their position in a document or a cheap-to-clone type.

## Caveats

### Row ordering is not preserved

Root-level ORDER BY won't be honored for changes. Meaning new rows will be out of order relative to previously returned rows. Ordering is only kept for a full set of changes (equivalent to creating a transaction).

"Inner" ordering should work just fine as each query result is re-computed when there are changes. That means if you have a a subquery in your query, its ordering will be honored.