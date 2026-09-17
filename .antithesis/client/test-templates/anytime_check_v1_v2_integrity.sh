#!/usr/bin/env bash

set -euo pipefail

IFS=',' read -ra configs <<< "${CORROSION_CONFIGS:-/tmp/corrosion1.toml,/tmp/corrosion2.toml,/tmp/corrosion3.toml}"

check_once() {
    local config="$1"
    local node config_name status write_version use_version sync_log_version migration_complete

    config_name="$(basename "${config}" .toml)"
    node="${config_name#/tmp/}"

    if [ ! -f "${config}" ]; then
        echo "[v1-v2-integrity] config unavailable, skipping: ${config}"
        return 0
    fi

    if ! status=$(curl --connect-timeout 2 --max-time 5 -fsS \
        "http://${node}:8080/v1/migrate/status" 2>/dev/null); then
        echo "[v1-v2-integrity] node unavailable, skipping: ${node}"
        return 0
    fi

    read -r write_version use_version sync_log_version migration_complete < <(
        STATUS_JSON="${status}" python3 - <<'PY'
import json
import os

status = json.loads(os.environ["STATUS_JSON"])
print(
    status.get("write_version", 0),
    status.get("use_version", 0),
    status.get("sync_log_version", 0),
    str(status.get("migration_complete", False)).lower(),
)
PY
    )

    if [ "${write_version}" != "2" ] || [ "${migration_complete}" != "true" ]; then
        echo "[v1-v2-integrity] not applicable for ${node}: write=${write_version} use=${use_version} sync=${sync_log_version} migration_complete=${migration_complete}"
        return 0
    fi

    local db_path="/var/lib/${node}/state.db"
    echo "[v1-v2-integrity] checking ${node}"
    if ! python3 /opt/antithesis/py-resources/check_v1_v2_integrity.py "${db_path}"; then
        echo "[v1-v2-integrity] integrity check failed: ${node}"
        return 1
    fi
}

while true; do
    for config in "${configs[@]}"; do
        check_once "${config}"
    done
    sleep "${V1_V2_INTEGRITY_INTERVAL_SECONDS:-5}"
done
