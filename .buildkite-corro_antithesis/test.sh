#! /bin/bash

set -e

response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" -X POST https://flyio.antithesis.com/api/v1/launch/basic_test -d "{\"params\": { \"antithesis.description\":\"basic_test on main\",
    \"antithesis.duration\":\"3\",
    \"antithesis.config_image\":\"antithesis-config:${BUILDKITE_COMMIT}\",
    \"antithesis.images\":\"corrosion:${BUILDKITE_COMMIT},corro-client:pre-v1.0\", 
    \"antithesis.report.recipients\":\"somtochi@fly.io\"
}}")

status=$(echo $response | jq -r '.statusCode')
    
if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis test $response"
    exit 1
fi

echo "Launched Antithesis test $response"
