#!/bin/bash

set -eux

# build all binaries, optimized with Ubuntu Bionic (18.04) sysroot:
CFLAGS="--sysroot=/ubuntu-bionic-sysroot" cargo build --verbose --release \
	--config='target.x86_64-unknown-linux-gnu.rustflags = ["-C", "link-arg=--sysroot=/ubuntu-bionic-sysroot"]' \
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
