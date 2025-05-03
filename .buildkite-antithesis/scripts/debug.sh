#! /bin/bash

set -e

CUSTOM_DURATION=${CUSTOM_DURATION:-0.1}

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

echo "Launching Antithesis test with params: ${SESSION_ID}, ${INPUT_HASH}, ${VTIME}, ${EMAIL}, ${CUSTOM_DURATION}"
response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" \
-X POST https://flyio.antithesis.com/api/v1/launch/debugging \
-d "{\"params\": { 
        \"antithesis.debugging.session_id\":\"${SESSION_ID}\",  
        \"antithesis.debugging.input_hash\":\"${INPUT_HASH}\", 
        \"antithesis.debugging.vtime\":\"${VTIME}\", 
        \"antithesis.report.recipients\":\"${EMAIL}\" 
    }}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis test $response"
    exit 1
fi

echo "Launched Antithesis test $response"
