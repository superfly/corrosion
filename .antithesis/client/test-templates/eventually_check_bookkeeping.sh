#! /bin/bash

set +e

arr=("/tmp/corrosion1.toml" "/tmp/corrosion2.toml" "/tmp/corrosion3.toml")

echo ${arr[@]}

max_attempts=100
for i in {1..100}; do
    python3 /opt/antithesis/test/v1/basic_test/check_bookkeeping.py ${arr[@]}
    if [ $? -eq 0 ]; then
        break
    fi
    sleep 30
done
