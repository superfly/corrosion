#!/bin/bash

addrs=("corrosion1" "corrosion2" "corrosion3")


addr=${addrs[$((RANDOM % 3))]}

backup_candidates=()
for a in "${addrs[@]}"; do
    if [ "$a" != "$addr" ]; then
        backup_candidates+=("$a")
    fi
done
backup_addr=${backup_candidates[$((RANDOM % ${#backup_candidates[@]}))]}

echo "Backup $addr to $backup_addr dir"

exec {lockfd}<>"/var/lib/${backup_addr}/backup.lock"

flock "$lockfd"

if [ -f "/var/lib/${backup_addr}/backups/state.db" ]; then
    rm /var/lib/${backup_addr}/backups/state.db
fi

corrosion -c /tmp/${addr}.toml backup  /var/lib/${backup_addr}/backups/state.db
