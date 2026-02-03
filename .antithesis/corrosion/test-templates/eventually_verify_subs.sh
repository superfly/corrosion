#!/bin/bash
ls -1 /var/lib/corrosion/subscriptions | xargs -I {} python3 scripts/verify_subscription.py /var/lib/corrosion/state.db {}
