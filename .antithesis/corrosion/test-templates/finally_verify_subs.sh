#!/bin/bash

for i in {1..30}; do
    ls -1 /var/lib/corrosion/subscriptions | xargs -I {} python3 /opt/antithesis/py-resources/verify_subscription.py /var/lib/corrosion/state.db {}
    if [ $? -eq 0 ]; then
        echo "All subscription databases are consistent"
        exit 0
    fi

    echo "Inconsistent data in subscriptions, retrying"
    sleep 10
done

echo "Inconsistent data in subscriptions"
exit 1