#! /bin/bash

set -e

check_shutdown() {
    if [ $? -ne 0 ]; then
        supervisorctl status corrosion | grep "SHUTDOWN_STATE"
        if [ $? -eq 0 ]; then
            echo "Corrosion is shutting down (container might be stopping)"
            exit 137
        fi
    fi
}

if [ ! -f "/var/lib/corrosion/backups/state.db" ]; then
    echo "No backup for corrosion"
    exit 0
fi

exec {lockfd}<>"/var/lib/corrosion/backups/backup.lock"
flock "$lockfd"

# a concurrent script might have applied and deleted the state file
# while we waited for the lock
if [ ! -f "/var/lib/corrosion/backups/state.db" ]; then
    echo "Backup already applied and deleted."
    exit 0
fi

trap check_shutdown EXIT
echo "Restoring backup for corrosion"

echo "Stopping corrosion first"
supervisorctl stop corrosion

corrosion restore /var/lib/corrosion/backups/state.db
echo "Restore done. Starting corrosion"
supervisorctl start corrosion
supervisorctl restart corro-consul

rm -f /var/lib/corrosion/backups/state.db
exit 0
