#! /bin/bash

set -e

CUSTOM_DURATION=${CUSTOM_DURATION:-0.1}

response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" -X POST https://flyio.antithesis.com/api/v1/launch/flyio -d "{\"params\": { \"antithesis.description\":\"basic_test on main\",
    \"custom.duration\":\"${CUSTOM_DURATION}\",
    \"antithesis.config_image\":\"antithesis-config:${BUILDKITE_COMMIT}\",
    \"antithesis.images\":\"corrosion:${BUILDKITE_COMMIT},corro-client:${BUILDKITE_COMMIT}\", 
    \"antithesis.report.recipients\":\"somtochi@fly.io\"
}}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis test $response"
    exit 1
fi

echo "Launched Antithesis test $response"
