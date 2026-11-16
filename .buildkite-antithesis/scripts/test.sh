
#! /bin/bash

set -e

set -eo pipefail

CUSTOM_DURATION=${CUSTOM_DURATION:-1}
CORRO_METADATA_WRITE_VERSION=${CORRO_METADATA_WRITE_VERSION:-}
CORRO_METADATA_USE_VERSION=${CORRO_METADATA_USE_VERSION:-}
CORRO_SYNC_LOG_VERSION=${CORRO_SYNC_LOG_VERSION:-}

if [[ -n "$CORRO_METADATA_WRITE_VERSION" && ! "$CORRO_METADATA_WRITE_VERSION" =~ ^[123]$ ]]; then
    echo "Invalid CORRO_METADATA_WRITE_VERSION: $CORRO_METADATA_WRITE_VERSION" >&2
    exit 1
fi
if [[ -n "$CORRO_METADATA_USE_VERSION" && ! "$CORRO_METADATA_USE_VERSION" =~ ^[12]$ ]]; then
    echo "Invalid CORRO_METADATA_USE_VERSION: $CORRO_METADATA_USE_VERSION" >&2
    exit 1
fi
if [[ -n "$CORRO_SYNC_LOG_VERSION" && ! "$CORRO_SYNC_LOG_VERSION" =~ ^[12]$ ]]; then
    echo "Invalid CORRO_SYNC_LOG_VERSION: $CORRO_SYNC_LOG_VERSION" >&2
    exit 1
fi

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

echo "Launching Antithesis test ${BUILDKITE_BRANCH} ${COMMIT_HASH}"

response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" -X POST https://flyio.antithesis.com/api/v1/launch/corrosion -d "{\"params\": {
    \"antithesis.description\":\"Run tests on ${BUILDKITE_BRANCH} for ${CUSTOM_DURATION}h (metadata_write_version=${CORRO_METADATA_WRITE_VERSION:-default}, metadata_use_version=${CORRO_METADATA_USE_VERSION:-default}, sync_log_version=${CORRO_SYNC_LOG_VERSION:-default})\",
    \"antithesis.duration\":\"${CUSTOM_DURATION}\",
    \"antithesis.config_image\":\"antithesis-config:${COMMIT_HASH}\",
    \"antithesis.images\":\"corrosion:${COMMIT_HASH},corro-client:${COMMIT_HASH}\",
    \"antithesis.report.recipients\":\"networking@fly.io\",
    \"antithesis.source\":\"${BUILDKITE_BRANCH}\"
}}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" -ge 200 ] && [ "$status" -lt 300 ]; then
    echo "Launched Antithesis test $response"
else
    echo "Failed to launch Antithesis test $response"
    exit 1
fi
