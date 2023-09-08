# POST /v1/subscriptions

Start receiving updates for a desired SQL query. The `v1/subscriptions` endpoint accepts a single SQL statement in JSON format.
The Corrosion agent responds with an HTML stream that notifies of any changes to the response to this query.

### Sample request
```
curl http://localhost:8080/v1/subscriptions \
 -H "content-type: application/json" \
 -d "\"SELECT sandwich FROM sw\""
```

### Sample response

```json
{"columns":["sandwich"]}
{"row":[1,["shiitake"]]}
{"row":[2,["ham"]]}
{"row":[3,["grilled cheese"]]}
{"row":[4,["brie and cranberry"]]}
{"eoq":{"time":8e-8}}
{"change":["update",2,["smoked meat"]]}
{"change":["update",1,["smoked meat"]]}
{"change":["update",2,["ham"]]}
{"change":["update",1,["burger"]]}
{"change":["update",2,["smoked meat"]]}
...
```