#!/bin/bash

set -eux

# build all binaries, optimized
cargo build --verbose --release \
	--bin corrosion

ls -lhA ${CARGO_TARGET_DIR}/release

mkdir -p ./builds/
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/release/corrosion ./builds/corrosion &
wait

# archive. tarballs preserve the executable bit, buildkite artifacts don't.
pushd builds
tar -cf corrosion.tar corrosion
popd

pigz -f builds/corrosion.tar
