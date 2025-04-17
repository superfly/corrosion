#!/bin/bash
set -e


# hack to use corrosion cli to send commands to corrosion nodes
for i in {1..3}; do
    socat UNIX-LISTEN:/tmp/corrosion${i}_admin.sock,fork,reuseaddr,unlink-early,mode=770 TCP:corrosion${i}:6644 &
    sed "s/\[admin\]/&\npath = \"\/tmp\/corrosion${i}_admin.sock\"/" /opt/antithesis/config.toml > /tmp/corrosion${i}.toml
done

echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl
 
sleep infinity
