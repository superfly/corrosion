
#! /bin/bash

set -e

set -eo pipefail

CUSTOM_DURATION=${CUSTOM_DURATION:-1}

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

echo "Launching Antithesis test ${BUILDKITE_BRANCH} ${COMMIT_HASH}"

response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" -X POST https://flyio.antithesis.com/api/v1/launch/corrosion -d "{\"params\": {
    \"antithesis.description\":\"Run tests on ${BUILDKITE_BRANCH} for ${CUSTOM_DURATION}h\",
    \"antithesis.duration\":\"${CUSTOM_DURATION}\",
    \"antithesis.config_image\":\"antithesis-config:${COMMIT_HASH}\",
    \"antithesis.images\":\"corrosion:${COMMIT_HASH},corro-client:${COMMIT_HASH}\",
    \"antithesis.report.recipients\":\"networking@fly.io\",
    \"antithesis.source\":\"${BUILDKITE_BRANCH}\"
}}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis test $response"
    exit 1
fi

echo "Launched Antithesis test $response"
