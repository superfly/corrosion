#!/bin/bash

addrs=("corrosion1" "corrosion2" "corrosion3")


addr=${addrs[$((RANDOM % 3))]}
backup_to=${addrs[$((RANDOM % 3))]}

echo "Backup $addr to $backup_to dir"

if [ -f "/var/lib/${backup_to}/backups/state.db" ]; then
    rm /var/lib/${backup_to}/backups/state.db
fi

corrosion -c /tmp/${addr}.toml backup  /var/lib/${addr}/backups/state.db
