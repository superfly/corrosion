#!/bin/bash

set +e

for i in {1..40}; do
    ls -1 /var/lib/corrosion/subscriptions | xargs -I {} python3 /opt/antithesis/py-resources/verify_subscription.py /var/lib/corrosion/state.db {}
    if [ $? -eq 0 ]; then
        echo "Subscription database is consistent"
        exit 0
    fi

    sleep 30
done


exit 1