#!/bin/bash

set +e
set -o pipefail

verify_subs() {
    exit_code=0
    for i in {1..3}; do
        subs_dir="/var/lib/corrosion${i}/$1"
        if [ ! -d "$subs_dir" ]; then
            echo "No subscription directories found in $subs_dir"
            continue
        fi
        ls -1 $subs_dir \
        | xargs -I {} python3 /opt/antithesis/py-resources/verify_subscription.py /var/lib/corrosion${i}/state.db $subs_dir/{}/sub.sqlite
        if [ $? -ne 0 ]; then
            exit_code=1
        fi
    done

    return $exit_code
}

passed_subs=false
for i in {1..30}; do
    if [ "$passed_subs" == false ]; then
        verify_subs "subscriptions"
        if [ $? -eq 0 ]; then
            echo "Subscription database is consistent"
            passed_subs=true
        fi
    fi

    verify_subs "load-gen"
    if [ $? -eq 0 ]; then
        echo "Load generator is consistent"
        exit 0
    fi

    sleep 30
done


exit 1