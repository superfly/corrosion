## POST /v1/queries

Read from the Corrosion database. The `v1/queries` endpoint accepts a single SQL statement in JSON format.

### Sample request
```
curl http://top1.nearest.of.ccorrosion.internal:8080/v1/queries \ 
 -H "content-type: application/json" \
 -d "\"SELECT sandwich FROM sandwiches\""
```

### Sample response
```json
{"columns":["sandwich"]}
{"row":[1,["burger"]]}
{"row":[2,["ham"]]}
{"row":[3,["grilled cheese"]]}
{"row":[4,["brie and cranberry"]]}
{"eoq":{"time":5e-8}}
```