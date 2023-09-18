#!/bin/bash

set -e

# download artifacts from previous step
mkdir -p builds
buildkite-agent artifact download 'builds/*.tar.gz' ./builds

# sanitize branch name (e.g. "foo/bar-baz" => "foo--bar-baz")
BUILD_NAME="${BUILDKITE_BRANCH//[^a-zA-Z0-9-]/--}"

# upload
aws s3 cp builds/corrosion.tar.gz ${UPLOAD_BUCKET_PATH}/${BUILD_NAME}-${BUILDKITE_COMMIT:0:8}.tar.gz --region us-east-2 &
wait

aws s3 cp ${UPLOAD_BUCKET_PATH}/${BUILD_NAME}-${BUILDKITE_COMMIT:0:8}.tar.gz ${UPLOAD_BUCKET_PATH}/${BUILD_NAME}.tar.gz --region us-east-2 &
wait