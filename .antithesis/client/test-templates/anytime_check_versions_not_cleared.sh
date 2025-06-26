#!/bin/bash

query=$(cat <<"EOF"
SELECT count(*) from 
    (select actor_id, db_version, start_version FROM __corro_bookkeeping WHERE end_version IS NULL) as n 
    INNER JOIN 
    __corro_bookkeeping bk ON 
            bk.end_version IS NOT NULL 
            AND n.start_version between bk.start_version and bk.end_version 
            AND n.actor_id = bk.actor_id
EOF
)


corrosion_nodes=("corrosion1" "corrosion2" "corrosion3")
echo "Query: $query"
for node in ${corrosion_nodes[@]}; do
    count=$(sqlite3 /var/lib/${node}/state.db "$query")
    echo "Count: $count"
    if [[ -z $count ]]; then
        echo "failed to run query"
        continue
    fi
    if [[ $count -ne 0 ]]; then
        echo "$count versions are not cleared on node $node"
        exit 1
    fi
done

echo "All versions correctly cleared"
exit 0
