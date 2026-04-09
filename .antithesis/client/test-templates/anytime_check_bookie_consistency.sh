#!/usr/bin/env bash

set -euo pipefail

IFS=',' read -ra configs <<< "${CORROSION_CONFIGS:-/tmp/corrosion1.toml,/tmp/corrosion2.toml,/tmp/corrosion3.toml}"
echo "${configs[@]}"

if [ -n "${BOOKIE_CHECK_PRIMARY_BIN:-}" ]; then
    primary_bin="${BOOKIE_CHECK_PRIMARY_BIN}"
elif command -v corrosion2 >/dev/null 2>&1; then
    primary_bin="corrosion2"
elif command -v corrosion >/dev/null 2>&1; then
    primary_bin="corrosion"
else
    primary_bin="corrosion2"
fi

check_once() {
    local bin="$1"
    local config="$2"

    if ! command -v "${bin}" >/dev/null 2>&1; then
        return 127
    fi

    if [ ! -f "${config}" ]; then
        return 127
    fi

    local max_retries=6
    local attempt=0
    local output ok

    while [ "${attempt}" -lt "${max_retries}" ]; do
        attempt=$((attempt + 1))
        echo "[bookie-check] attempt ${attempt}/${max_retries}: ${bin} -c ${config} sync check-bookie-consistency"
        output=$("${bin}" -c "${config}" sync check-bookie-consistency 2>&1) || true
        echo "${output}"
        ok=$(echo "${output}" | grep -o '"ok":[[:space:]]*true' 2>/dev/null) || ok=""
        if [ -n "${ok}" ]; then
            return 0
        fi
        echo "[bookie-check] attempt ${attempt}/${max_retries} failed (ok=${ok})"
        if [ "${attempt}" -lt "${max_retries}" ]; then
            sleep 10
        fi
    done

    return 1
}

for config in "${configs[@]}"; do
    if ! check_once "${primary_bin}" "${config}"; then
        echo "[bookie-check] consistency check failed: ${primary_bin} -c ${config}"
        exit 1
    fi
done

echo "[bookie-check] consistency check passed"
