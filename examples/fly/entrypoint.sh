#!/bin/bash

# export GOSSIP_ADDR="[$(getent hosts fly-local-6pn | cut -d ' ' -f1)]:8787"
sed -i 's/\[gossip\]/&\naddr = "['${FLY_PRIVATE_IP}']:8787"\nbootstrap = ["'${FLY_APP_NAME}'.internal:8787"]/' corrosion.toml
# sed -i 's/\[gossip\]/&\nbootstrap = ["'${FLY_APP_NAME}'.internal:8787"]/' corrosion.toml

# set -m # to make job control work
# sleep inf &
# /app/corrosion agent
#fg %1 # gross

exec "$@"