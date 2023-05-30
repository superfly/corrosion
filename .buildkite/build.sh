#!/bin/bash

set -eux

# build all binaries, optimized
cargo build --verbose --release \
	--bin corrosion \
	--bin corro-consul

ls -lhA ${CARGO_TARGET_DIR}/release

mkdir -p ./builds/
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/release/corrosion ./builds/corrosion &
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/release/corro-consul ./builds/corro-consul &
wait

# archive. tarballs preserve the executable bit, buildkite artifacts don't.
pushd builds
tar -cf corrosion.tar corrosion corro-consul
popd

pigz -f builds/corrosion.tar
