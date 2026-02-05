#!/bin/bash

set +e
set -o pipefail

verify_subs() {
    exit_code=0
    for i in {1..3}; do
        ls -1 /var/lib/corrosion${i}/subscriptions | xargs -I {} python3 /opt/antithesis/py-resources/verify_subscription.py /var/lib/corrosion${i}/state.db {}
        if [ $? -ne 0 ]; then
            exit_code=1
        fi
    done

    return $exit_code
}

for i in {1..40}; do
    verify_subs
    if [ $? -eq 0 ]; then
        echo "Subscription database is consistent"
        exit 0
    fi

    sleep 30
done


exit 1