#! /bin/bash

set -e

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

echo "Launching Antithesis debugging with: ${SESSION_ID}, ${INPUT_HASH}, ${VTIME}"
response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" \
-X POST https://flyio.antithesis.com/api/v1/launch/debugging \
-d "{\"params\": {
        \"antithesis.debugging.session_id\":\"${SESSION_ID}\",
        \"antithesis.debugging.input_hash\":\"${INPUT_HASH}\",
        \"antithesis.debugging.vtime\":\"${VTIME}\",
        \"antithesis.report.recipients\":\"networking@fly.io\"
    }}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis debug session $response"
    exit 1
fi

echo "Launched Antithesis debug session $response"
