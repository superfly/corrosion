#!/bin/bash

set -e

# ensure latest rust image
docker pull flyio/docker-rust-cargo:main

docker run --rm -t \
    -e RUST_BACKTRACE=1 \
    -v rustup-home:/usr/local/rustup \
    -v cargo-registry-cache:/usr/local/cargo/registry \
    -v cargo-target-dir:/var/cache/cargo-corrosion \
    -e CARGO_TARGET_DIR=/var/cache/cargo-corrosion \
    -e BUILDKITE_BUILD_ID="${BUILDKITE_BUILD_ID}" \
    -e BUILDKITE_BUILD_NUMBER="${BUILDKITE_BUILD_NUMBER}" \
    -e BUILDKITE_JOB_ID="${BUILDKITE_JOB_ID}" \
    -e BUILDKITE_BRANCH="${BUILDKITE_BRANCH}" \
    -e BUILDKITE_COMMIT="${BUILDKITE_COMMIT}" \
    -e BUILDKITE_MESSAGE="${BUILDKITE_MESSAGE}" \
    -e BUILDKITE_BUILD_URL="${BUILDKITE_BUILD_URL}" \
    -e BUILDKITE_ANALYTICS_TOKEN="${BUILDKITE_ANALYTICS_TOKEN}" \
    -e SSH_AUTH_SOCK=/ssh-agent \
    -v "${SSH_AUTH_SOCK}:/ssh-agent" \
		-v "${PWD}:/build_dir" \
		-w /build_dir \
		flyio/docker-rust-cargo:main /bin/bash -c "$@"
