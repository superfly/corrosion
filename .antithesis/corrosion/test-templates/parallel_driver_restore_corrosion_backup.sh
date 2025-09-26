#! /bin/bash

set -e

function start_corrosion {
  STATUS=$(supervisorctl status corrosion | awk '{print $2}')

  if [ "$STATUS" != "RUNNING" ]; then
      echo "Restarting corrosion, current status: $STATUS"
      supervisorctl start corrosion
  fi
}

if [ -f "/var/lib/corrosion/backups/state.db" ]; then
    exec {lockfd}<>"/var/lib/corrosion/backups/backup.lock"
    flock "$lockfd"

    echo "Restoring backup for corrosion"

    echo "Stopping corrosion first"
    trap start_corrosion EXIT
    supervisorctl stop corrosion

    corrosion restore /var/lib/corrosion/backups/state.db
    supervisorctl start corrosion
    supervisorctl restart corro-consul
    exit 0
fi

echo "No current backup for corrosion"
