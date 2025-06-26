#! /bin/bash

set +e

arr=("/tmp/corrosion1.toml" "/tmp/corrosion2.toml" "/tmp/corrosion3.toml")

echo ${arr[@]}

max_attempts=100
for i in {1..100}; do
    python3 /opt/antithesis/py-resources/check_bookkeeping.py ${arr[@]}
    if [ $? -eq 0 ]; then
        echo "Bookkeeping is consistent"
        exit 0
    fi
    sleep 30
done

echo "Bookkeeping is inconsistent, check output for more details"
exit 1
