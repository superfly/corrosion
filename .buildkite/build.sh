#!/bin/bash

set -eux

# build all binaries, optimized
cargo build --verbose --release \
	--bin corrosion \
	--bin corroctl \
	--bin corro-consul

ls -lhA ${CARGO_TARGET_DIR}/release

mkdir -p ./builds/
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/release/corrosion ./builds/corrosion &
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/release/corro-consul ./builds/corro-consul &
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/release/corroctl ./builds/corroctl &
wait

# archive. tarballs preserve the executable bit, buildkite artifacts don't.
pushd builds
tar -cf corrosion.tar corrosion corroctl
tar -cf corro-consul.tar corro-consul
popd

pigz -f builds/corrosion.tar
pigz -f builds/corro-consul.tar
