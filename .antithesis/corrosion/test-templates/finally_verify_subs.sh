#!/bin/bash
ls -1 /var/lib/corrosion/subscriptions | xargs -I {} python3 /opt/antithesis/py-resources/verify_subscription.py /var/lib/corrosion/state.db {}
