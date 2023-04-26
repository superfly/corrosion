#!/bin/bash

set -e

# download artifacts from previous step
mkdir -p builds
buildkite-agent artifact download 'builds/*.tar.gz' ./builds

# sanitize branch name (e.g. "foo/bar-baz" => "foo--bar-baz")
BUILD_NAME="${BUILDKITE_BRANCH//[^a-zA-Z0-9-]/--}"

# upload
aws s3 cp builds/corrosion.tar.gz s3://flyio-builds/corrosion2/${BUILD_NAME}-${BUILDKITE_COMMIT:0:8}.tar.gz --region us-east-2 &
aws s3 cp builds/corro-consul.tar.gz s3://flyio-builds/corro-consul/${BUILD_NAME}-${BUILDKITE_COMMIT:0:8}.tar.gz --region us-east-2 &
wait

aws s3 cp s3://flyio-builds/corrosion2/${BUILD_NAME}-${BUILDKITE_COMMIT:0:8}.tar.gz s3://flyio-builds/corrosion2/${BUILD_NAME}.tar.gz --region us-east-2 &
aws s3 cp s3://flyio-builds/corro-consul/${BUILD_NAME}-${BUILDKITE_COMMIT:0:8}.tar.gz s3://flyio-builds/corro-consul/${BUILD_NAME}.tar.gz --region us-east-2 &
wait