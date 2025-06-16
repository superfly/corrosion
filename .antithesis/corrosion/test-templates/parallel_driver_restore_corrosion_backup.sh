#! /bin/bash

if [ -f "/var/lib/corrosion/backups/state.db"]; then
    echo "Restoring backup for corrosion"

    echo "Stopping corrosion first"
    supervisorctl stop corrosion

    corrosion restore /var/lib/corrosion${ID}/backups/state.db
    supervisorctl start corrosion
    exit 0
fi

echo "Backup for corrosion${ID} not found"
