#! /bin/bash

set -e

parse_moment() {
    local input="$1"

    if [ -z "$input" ]; then
        echo "Error: No input provided to parse_moment function" >&2
        exit 1
    fi

    # Convert multi-line input to single line for easier parsing
    input=$(echo "$input" | tr -d '\n' | sed 's/[[:space:]]\+/ /g')

    # Extract session ID from Session.from({id: "..."})
    SESSION_ID=$(echo "$input" | perl -pe 's/.*Session\.from\(\{id:\s*"([^"]+)".*/\1/')

    # Extract input hash from the second parameter (can be single or double quoted)
    INPUT_HASH=$(echo "$input" | perl -pe 's/.*,\s*["\x27]([^"\x27]+)["\x27].*/\1/')

    # Extract vtime from the third parameter (decimal number)
    VTIME=$(echo "$input" | perl -pe 's/.*,\s*([0-9]+\.[0-9]+)\).*/\1/')

    echo "SESSION_ID: $SESSION_ID"
    echo "INPUT_HASH: $INPUT_HASH"
    echo "VTIME: $VTIME"

    export SESSION_ID=$SESSION_ID
    export INPUT_HASH=$INPUT_HASH
    export VTIME=$VTIME
}

COMMIT_HASH=${BUILDKITE_COMMIT:0:8}

echo "Parsing $MOMENT"

if [ -n "$MOMENT" ]; then
    parse_moment "$MOMENT"
fi

if [ -z "$SESSION_ID" ] || [ -z "$INPUT_HASH" ] || [ -z "$VTIME" ]; then
    echo "Error: SESSION_ID, INPUT_HASH, or VTIME not set. Set env variable or export MOMENT"
    exit 1
fi

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
