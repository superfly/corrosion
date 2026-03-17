#!/bin/bash

set -eux

# Ring wants the standard cross toolchain naming
ln -s /usr/bin/musl-gcc /usr/bin/x86_64-linux-musl-gcc

# build all binaries, optimized
cargo build --verbose --release \
	--target x86_64-unknown-linux-musl \
	--bin corrosion

ls -lhA ${CARGO_TARGET_DIR}/x86_64-unknown-linux-musl/release

mkdir -p ./builds/
time objcopy --compress-debug-sections ${CARGO_TARGET_DIR}/x86_64-unknown-linux-musl/release/corrosion ./builds/corrosion &
wait

# archive. tarballs preserve the executable bit, buildkite artifacts don't.
pushd builds
tar -cf corrosion.tar corrosion
popd

pigz -f builds/corrosion.tar
