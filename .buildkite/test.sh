#!/bin/bash

export RUST_LOG=info,corro=debug

# Tests (with coverage + generating JUnit XML report)
export NEXTEST_PROFILE=ci
set +e
cargo nextest run --all-targets --workspace
test_status=$?
set -e

# Upload JUnit XML report for test analytics
curl -X POST \
  -H "Authorization: Token token=\"$BUILDKITE_ANALYTICS_TOKEN\"" \
  -F "format=junit" \
  -F "data=@target/nextest/ci/junit.xml" \
  -F "run_env[CI]=buildkite" \
  -F "run_env[key]=$BUILDKITE_BUILD_ID" \
  -F "run_env[number]=$BUILDKITE_BUILD_NUMBER" \
  -F "run_env[job_id]=$BUILDKITE_JOB_ID" \
  -F "run_env[branch]=$BUILDKITE_BRANCH" \
  -F "run_env[commit_sha]=$BUILDKITE_COMMIT" \
  -F "run_env[message]=$BUILDKITE_MESSAGE" \
  -F "run_env[url]=$BUILDKITE_BUILD_URL" \
  https://analytics-api.buildkite.com/v1/uploads


exit $test_status
