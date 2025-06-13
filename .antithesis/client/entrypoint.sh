#!/bin/bash
set -e

# hack to wait a bit for corrosion to start
# this would eventually be handled by antithesis sdk
sleep 10

# hack to use corrosion cli to send commands to corrosion nodes
for i in {1..3}; do
    mkdir -p /var/lib/corrosion${i}/backups
    export ID=$i
    socat UNIX-LISTEN:/tmp/corrosion${i}_admin.sock,fork,reuseaddr,unlink-early,mode=770 TCP:corrosion${i}:6644 &
    envsubst < /opt/antithesis/config.toml > /tmp/corrosion${i}.toml
done

echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl

sleep infinity
