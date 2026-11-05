#!/usr/bin/env bash

set -euo pipefail

IFS=',' read -ra configs <<< "${CORROSION_CONFIGS:-/tmp/corrosion1.toml,/tmp/corrosion2.toml,/tmp/corrosion3.toml}"

check_once() {
    local config="$1"
    local config_name node db_path

    config_name="$(basename "${config}" .toml)"
    node="${config_name#/tmp/}"

    if [ ! -f "${config}" ]; then
        echo "[db-version-reuse] config unavailable, skipping: ${config}"
        return 0
    fi

    db_path="/var/lib/${node}/state.db"
    if [ ! -f "${db_path}" ]; then
        echo "[db-version-reuse] database unavailable, skipping: ${node}"
        return 0
    fi

    echo "[db-version-reuse] checking ${node}"
    timeout --kill-after=5s "${DB_VERSION_REUSE_CHECK_TIMEOUT_SECONDS:-120}s" \
        python3 /opt/antithesis/py-resources/check_db_version_reuse.py "${db_path}"
}

max_attempts="${DB_VERSION_REUSE_MAX_ATTEMPTS:-40}"
interval_seconds="${DB_VERSION_REUSE_INTERVAL_SECONDS:-30}"

echo "[db-version-reuse] starting (${max_attempts} attempts, ${interval_seconds}s interval)"
for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    echo "[db-version-reuse] attempt ${attempt}/${max_attempts}"
    for config in "${configs[@]}"; do
        check_once "${config}"
    done
    if [ "${attempt}" -lt "${max_attempts}" ]; then
        sleep "${interval_seconds}"
    fi
done

echo "[db-version-reuse] completed without finding a violation"
