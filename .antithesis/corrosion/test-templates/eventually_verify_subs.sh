#!/bin/bash
ls -1 /var/lib/corrosion/subscriptions | xargs -I {} python3 /opt/antithesis/test/v1/basic_test/verify_subscription.py /var/lib/corrosion/state.db {}
