#!/bin/bash
set -e
# Add gossip and bootstrap addresses to Corrosion config file before
# starting agent with Dockerfile CMD
IP_ADDR=$(ip -4 a s eth0 | grep inet | awk '{print $2}' | cut -d '/' -f 1)
export IP_ADDR=${IP_ADDR}
envsubst < /etc/corrosion/config.toml.tpl > /etc/corrosion/config.toml

crsqlite_settings=()
if [[ -n "${CORRO_METADATA_WRITE_VERSION:-}" ]]; then
    case "$CORRO_METADATA_WRITE_VERSION" in
        1|2|3) crsqlite_settings+=("metadata_write_version = $CORRO_METADATA_WRITE_VERSION") ;;
        *) echo "Invalid CORRO_METADATA_WRITE_VERSION: $CORRO_METADATA_WRITE_VERSION" >&2; exit 1 ;;
    esac
fi
if [[ -n "${CORRO_METADATA_USE_VERSION:-}" ]]; then
    case "$CORRO_METADATA_USE_VERSION" in
        1|2) crsqlite_settings+=("metadata_use_version = $CORRO_METADATA_USE_VERSION") ;;
        *) echo "Invalid CORRO_METADATA_USE_VERSION: $CORRO_METADATA_USE_VERSION" >&2; exit 1 ;;
    esac
fi
if [[ -n "${CORRO_SYNC_LOG_VERSION:-}" ]]; then
    case "$CORRO_SYNC_LOG_VERSION" in
        1|2) crsqlite_settings+=("sync_log_version = $CORRO_SYNC_LOG_VERSION") ;;
        *) echo "Invalid CORRO_SYNC_LOG_VERSION: $CORRO_SYNC_LOG_VERSION" >&2; exit 1 ;;
    esac
fi
if ((${#crsqlite_settings[@]})); then
    {
        printf '\n[crsqlite]\n'
        printf '%s\n' "${crsqlite_settings[@]}"
    } >> /etc/corrosion/config.toml
fi

cat /etc/corrosion/config.toml

# access admin socket over network
socat TCP-LISTEN:6644,reuseaddr,fork UNIX-CONNECT:/app/admin.sock &

exec "$@"
