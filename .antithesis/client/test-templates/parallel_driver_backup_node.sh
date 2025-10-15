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

exec {lockfd}<>"/var/lib/${backup_addr}/backups/backup.lock"

flock "$lockfd"

echo "Backup $addr to $backup_addr dir"

rm -f /var/lib/${backup_addr}/backups/state.db

corrosion -c /tmp/${addr}.toml backup  /var/lib/${backup_addr}/backups/state.db

if [ $? -ne 0 ]; then
    echo "Backup failed"
    rm -f /var/lib/${backup_addr}/backups/state.db
    exit 1
fi
