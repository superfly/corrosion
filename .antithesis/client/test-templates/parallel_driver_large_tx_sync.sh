#! /bin/bash

addrs=("corrosion1:8080" "corrosion2:8080" "corrosion3:8080")

counts=(10000 1000 900 800 700 600 500 400 300 200 100 1000 900 800 700 600 500
    400 300 200 100 1000 900 800 700 600 500 400 300 200 100 1000 900 800 700
    600 500 400 300 200 100 1000 900 800 700 600 500 400 300 200 100 1000 900
    800 700 600 500 400 300 200 100 1000 900 800 700 600 500 400 300 200 100
    1000 900 800 700 600 500 400 300 200 100 1000 900 800 700 600 500 400 300
    200 100 1000 900 800 700 600 500 400 300 200 100
);
    
for count in ${counts[@]}; do
    addr=${addrs[$((RANDOM % 3))]}
    echo "Count: $count, addr: $addr"
    response=$(curl -s -w "\n%{http_code}" -X POST http://${addr}/v1/transactions -H "Content-Type: application/json" -d "[\"INSERT INTO testsbool (id) WITH RECURSIVE    cte(id) AS (       SELECT random()       UNION ALL       SELECT random()         FROM cte        LIMIT ${count}  ) SELECT id FROM cte;\"]")
    http_code=$(echo "$response" | tail -n1)
    body=$(echo "$response" | sed '$d')
    # Check if status code is 200
    if [ "$http_code" -ne 200 ]; then
        echo "Error! Response code: $http_code"
        echo "Response body: $body"
    fi
done
