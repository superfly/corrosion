#!/bin/bash
set -e

# hack to wait a bit for corrosion to start 
# this would eventually be handled by antithesis sdk
sleep 10

echo '{"antithesis_setup": { "status": "complete", "details": null }}' > $ANTITHESIS_OUTPUT_DIR/sdk.jsonl
 
sleep infinity
