#! /bin/bash

if [ -f "/var/lib/corrosion/backups/state.db" ]; then
    echo "Restoring backup for corrosion"

    echo "Stopping corrosion first"
    supervisorctl stop corrosion

    corrosion restore /var/lib/corrosion${ID}/backups/state.db
    supervisorctl start corrosion
    supervisorctl restart corro-consul
    exit 0
fi

echo "No current backup for corrosion"
