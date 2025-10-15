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

# TODO: actually this doesn't work, we can still have the backup script executing concurrently
# because it runs in a different container, use a consul lock or something else instead
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

set +e

exit_code=0
if ! corrosion restore /var/lib/corrosion/backups/state.db; then
    echo "Restore failed"
    exit_code=1
    # if there's no site_id with 0, means there was a partial restore
    ordinal=$(sqlite3 /var/lib/corrosion/state.db "SELECT ordinal FROM crsql_site_id WHERE ordinal = 0;")
    if [ -z "$ordinal" ]; then
        echo "Partial restore detected, removing state.db and retrying"
        rm -f /var/lib/corrosion/state.db
        corrosion restore /var/lib/corrosion/backups/state.db
    fi
else
    echo "Restore successful, restarting corrosion and consul"
fi

supervisorctl start corrosion
supervisorctl restart corro-consul

rm -f /var/lib/corrosion/backups/state.db
exit $exit_code
