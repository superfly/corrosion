#!/bin/bash

set +e

for i in {1..40}; do
    ls -1 /var/lib/corrosion${i}/subscriptions | xargs -I {} python3 /opt/antithesis/py-resources/verify_subscription.py /var/lib/corrosion${i}/state.db {}
    if [ $? -eq 0 ]; then
        echo "Subscription for corrosion${i} is consistent"
        exit 0
    fi

    sleep 30
done


exit 1