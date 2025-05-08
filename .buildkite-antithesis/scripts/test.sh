#! /bin/bash

set -e

set -eo pipefail

CUSTOM_DURATION="0.1"
if buildkite-agent meta-data exists "test-duration"; then
    CUSTOM_DURATION=$(buildkite-agent meta-data get test-duration)
fi

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" -X POST https://flyio.antithesis.com/api/v1/launch/flyio -d "{\"params\": { \"antithesis.description\":\"basic_test on main\",
    \"antithesis.duration\":\"${CUSTOM_DURATION}\",
    \"antithesis.config_image\":\"antithesis-config:${COMMIT_HASH}\",
    \"antithesis.images\":\"corrosion:${COMMIT_HASH},corro-client:${COMMIT_HASH}\", 
    \"antithesis.report.recipients\":\"somtochi@fly.io\"
}}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis test $response"
    exit 1
fi

echo "Launched Antithesis test $response"
