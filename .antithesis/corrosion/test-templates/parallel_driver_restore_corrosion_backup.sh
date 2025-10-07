#! /bin/bash

set -e

if [ -f "/var/lib/corrosion/backups/state.db" ]; then
    exec {lockfd}<>"/var/lib/corrosion/backups/backup.lock"
    flock "$lockfd"

    echo "Restoring backup for corrosion"

    echo "Stopping corrosion first"
    supervisorctl stop corrosion

    corrosion restore /var/lib/corrosion/backups/state.db
    echo "Restore done. Starting corrosion"
    supervisorctl start corrosion
    supervisorctl restart corro-consul

    rm -f /var/lib/corrosion/backups/state.db
    exit 0
fi

echo "No current backup for corrosion"
