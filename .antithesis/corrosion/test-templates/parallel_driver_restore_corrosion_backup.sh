#! /bin/bash

set -e

function start_corrosion {
  STATUS=$(supervisorctl status corrosion | awk '{print $2}')

  if [ "$STATUS" != "RUNNING" ]; then
      echo "Restarting corrosion"
      supervisorctl start corrosion
      supervisorctl start corro-consul
  fi
}

if [ -f "/var/lib/corrosion/backups/state.db" ]; then
    exec {lockfd}<>"/var/lib/corrosion/backups/backup.lock"
    flock "$lockfd"

    echo "Restoring backup for corrosion"

    echo "Stopping corrosion first"
    trap start_corrosion EXIT
    supervisorctl stop corrosion

    corrosion restore /var/lib/corrosion${ID}/backups/state.db
    supervisorctl start corrosion
    supervisorctl start corro-consul
    exit 0
fi

echo "No current backup for corrosion"
