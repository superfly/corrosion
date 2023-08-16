#!/bin/bash

export GOSSIP__ADDR="[$(getent hosts fly-local-6pn | cut -d ' ' -f1)]:8787"

echo "Set GOSSIP__ADDR=$GOSSIP__ADDR"

exec "$@"