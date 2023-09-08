## POST /v1/transactions

Write changes to the Corrosion database for propagation through the cluster. The `v1/transactions` endpoint accepts a JSON list of SQL statements.

### Sample request
```
curl http://localhost:8080/v1/transactions \
 -H "content-type: application/json" \
 -d "[\"INSERT OR IGNORE INTO sandwiches (pk, sandwich) VALUES (3, 'brie and cranberry')\"]"
```

### Sample response
```json
{"results":[{"rows_affected":1,"time":0.000027208}],"time":0.000300708}% 
```