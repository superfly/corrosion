#! /bin/bash

set -e

set -eo pipefail

CUSTOM_DURATION=${CUSTOM_DURATION:-1}

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

response=$(curl --fail -u "flyio:${ANTITHESIS_PASSWORD}" -X POST https://flyio.antithesis.com/api/v1/launch/flyio -d "{\"params\": { \"antithesis.description\":\"basic_test on main\",
    \"antithesis.duration\":\"${CUSTOM_DURATION}\",
    \"antithesis.config_image\":\"antithesis-config:${COMMIT_HASH}\",
    \"antithesis.images\":\"corrosion:${COMMIT_HASH},corro-client:${COMMIT_HASH},consul:1.15.4\",
    \"antithesis.report.recipients\":\"somtochi@fly.io\",
    \"antithesis.source\":\"${BUILDKITE_BRANCH}\"
}}")

status=$(echo $response | jq -r '.statusCode')

if [ "$status" != "200" ]; then
    echo "Failed to launch Antithesis test $response"
    exit 1
fi

echo "Launched Antithesis test $response"

// URL: https://flyio.antithesis.com/report/lovLmGTDXKkFr15BnBvJRaGe/7tiGwkybGUkkCBY7C1wkNdUIkXKfPKRE_bCmsD5-Xpc.html?auth=v2.public.eyJzY29wZSI6eyJSZXBvcnRTY29wZVYxIjp7ImFzc2V0IjoiN3RpR3dreWJHVWtrQ0JZN0Mxd2tOZFVJa1hLZlBLUkVfYkNtc0Q1LVhwYy5odG1sIiwicmVwb3J0X2lkIjoibG92TG1HVERYS2tGcjE1Qm5CdkpSYUdlIn19LCJuYmYiOiIyMDI1LTA1LTEzVDEwOjU0OjA3LjU1MjU0ODcwNloifeC2OBiOjBV8KRCyP_UWhl-rkpmDJUsWJ6WgODwxSD-LZqLfQ25vlHsud3_tO3bLOdZkUH_Jiqur7x-2oz5VlwQ#/run/af771c4e0d6d99c682805b7a7c6d8791-31-6/finding/eebbc27b70f668bdf61a3a4232674f3697a3c1a2 
// Notebook: /notebook/webhook/flyio.nb2
// Params: antithesis.config_image=antithesis-config%3A4f7aefc6&antithesis.description=basic_test+on+main&antithesis.duration=1&antithesis.images=corrosion%3A4f7aefc6%2Ccorro-client%3A4f7aefc6%2Cconsul%3A1.15.4&antithesis.integrations.type=none&antithesis.is_ephemeral=false&antithesis.report.recipients=somtochi%40fly.io
// Property: Always: Missed Always grouped by: bookie lock held for too long
// Failed on 2025-05-13T10:29Z
replay_moment = Moment._exact(
    Session.from({id: "ba896da1c219743dd519fe2288b98530-31-6"}),
    '1039582008731450858',
    205.15466672251932
)
