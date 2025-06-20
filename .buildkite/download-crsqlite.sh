#!/bin/bash

if [[ -z "$REPO" ]]; then
    echo "Set crsqlite repository owner with REPO env var"
    exit 1
fi

if [[ -z "$TAG" ]]; then
    echo "Set crsqlite tag with TAG env var"
    exit 1
fi

oses=("darwin-aarch64" "linux-aarch64" "linux-x86_64")

for os in "${oses[@]}"; do
    if [[ $os == *"darwin"* ]]; then
        EXT="dylib"
    else
        EXT="so"
    fi
    curl -L -o crsqlite-${os}.zip https://github.com/${REPO}/cr-sqlite/releases/download/${TAG}/crsqlite-${os}.zip
    unzip -p crsqlite-${os}.zip > crates/corro-types/crsqlite-${os}.${EXT}
    rm crsqlite-${os}.zip
done
