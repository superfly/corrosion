#!/bin/bash

export GOSSIP_ADDR="[$(getent hosts fly-local-6pn | cut -d ' ' -f1)]:8787"

echo "Set GOSSIP_ADDR=$GOSSIP_ADDR"

exec "$@"