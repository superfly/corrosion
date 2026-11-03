#!/usr/bin/env bash

set -euo pipefail

IFS=',' read -ra configs <<< "${CORROSION_CONFIGS:-/tmp/corrosion1.toml,/tmp/corrosion2.toml,/tmp/corrosion3.toml}"

check_once() {
    local config="$1"
    local config_name node db_path

    config_name="$(basename "${config}" .toml)"
    node="${config_name#/tmp/}"

    if [ ! -f "${config}" ]; then
        echo "[rolled-back-writes] config unavailable, skipping: ${config}"
        return 0
    fi

    db_path="/var/lib/${node}/state.db"
    if [ ! -f "${db_path}" ]; then
        echo "[rolled-back-writes] database unavailable, skipping: ${node}"
        return 0
    fi

    echo "[rolled-back-writes] checking ${node}"
    timeout --kill-after=5s "${ROLLED_BACK_WRITES_CHECK_TIMEOUT_SECONDS:-60}s" \
        python3 /opt/antithesis/py-resources/check_rolled_back_writes.py "${db_path}"
}

max_attempts="${ROLLED_BACK_WRITES_MAX_ATTEMPTS:-40}"
interval_seconds="${ROLLED_BACK_WRITES_INTERVAL_SECONDS:-30}"

echo "[rolled-back-writes] starting (${max_attempts} attempts, ${interval_seconds}s interval)"
for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    echo "[rolled-back-writes] attempt ${attempt}/${max_attempts}"
    for config in "${configs[@]}"; do
        check_once "${config}"
    done
    if [ "${attempt}" -lt "${max_attempts}" ]; then
        sleep "${interval_seconds}"
    fi
done

echo "[rolled-back-writes] completed without finding a persisted rollback"
