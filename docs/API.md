
## Corrosion API

**To note: Still rqlite-compatible?**

See: https://rqlite.io/docs/api/api/#writing-data

Corrosion-specific limitations:
- All requests are treated as if the `transaction` query param had been passed (everything runs in a transaction)
- There's a limit to how many statements can be input at once to reduce changeset sizes (this shouldn't be the case for long)

API Endpoints:
- [Corrosion API](#corrosion-api)
- [`POST v1/queries`](#post-v1queries)
  - [`v1/queries` example](#v1queries-example)
- [`POST v1/transactions`](#post-v1transactions)
  - [`v1/transactions` example](#v1transactions-example)
- [`POST v1/watches`](#post-v1watches)
  - [`v1/watches` example](#v1watches-example)
- [`GET v1/watches/:id`](#get-v1watchesid)
- [`POST v1/migrations`](#post-v1migrations)
- [`POST v1/sync`](#post-v1sync)
- [`POST v1/broadcast`](#post-v1broadcast)


## `POST v1/queries`

Read from the Corrosion database

### `v1/queries` example 

Look for the value of the `sandwich` column corresponding to `pk` string value `'mad'`, in table `sw` in the local database.

Request:
```
$ curl http://localhost:8080/v1/queries \
 -H "content-type: application/json" \
 -d "\"SELECT sandwich FROM sw WHERE pk='mad'\"" 
```
Response:
```
{"event":"columns","data":["sandwich"]}
{"event":"row","data":{"rowid":1,"change_type":"upsert","cells":["halloumi"]}}
```

## `POST v1/transactions`

Write to the Corrosion database 

### `v1/transactions` example

Add a value in the `sandwich` column, for `machine_id` integer value `3`, in table `sw` in the local database.

Request:
```
curl http://localhost:8080/v1/transactions \
 -H "content-type: application/json" \
 -d "[\"INSERT OR IGNORE INTO sw (machine_id, sandwich) VALUES (3, 'nonsense')\"]"
```

Response:
```
{"results":[{"rows_affected":1,"time":0.000027208}],"time":0.000300708}% 
```

## `POST v1/watches`

Watch for changes to the results of a query

Corrosion responds with an HTML stream that notifies of any changes to the response to your SQL query.

### `v1/watches` example

See changes to the value of `sandwich` for `pk='mad'` in table `sw` in the local database.

Request:
```
curl http://top1.nearest.of.ccorrosion.internal:8080/v1/watches \
 -H "content-type: application/json" \
 -d "\"SELECT sandwich FROM sw WHERE pk='mad'\""
```
Response:
```
curl http://top1.nearest.of.ccorrosion.internal:8080/v1/watches \
 -H "content-type: application/json" \
 -d "\"SELECT sandwich FROM sw WHERE pk='mad'\""
{"event":"columns","data":["sandwich"]}
{"event":"row","data":{"rowid":1,"change_type":"upsert","cells":["burger"]}}
{"event":"end_of_query"}
{"event":"row","data":{"rowid":1,"change_type":"upsert","cells":["portobello"]}}
{"event":"row","data":{"rowid":1,"change_type":"upsert","cells":["burger"]}}
...
```

## `GET v1/watches/:id`

TK

## `POST v1/migrations`

TK


## `POST v1/sync`
**Internal?**

Send a sync message to a single cluster member? 


## `POST v1/broadcast`
**Internal?**




