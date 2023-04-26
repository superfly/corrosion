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
tar -cf corrosion.tar corrosion
tar -cf corro-consul.tar corro-consul
popd

# # corrosion deploy stuff
# pushd crates/corrosion/deploy
# tar -rf ../../../builds/corrosion.tar deploy-corro share/*
# popd
# pigz -f builds/corrosion.tar

# # corro-dns deploy stuff
# pushd crates/corro-dns/deploy
# tar -rf ../../../builds/corro-dns.tar deploy-dns share/*
# popd
# pigz -f builds/corro-dns.tar

# # corrolog deploy stuff
# pushd crates/corrolog/deploy
# tar -rf ../../../builds/corrolog.tar deploy-corrolog share/*
# popd
# pigz -f builds/corrolog.tar