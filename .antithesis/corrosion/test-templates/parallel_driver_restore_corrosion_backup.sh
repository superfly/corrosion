#! /bin/bash

set -e

function start_corrosion {
  echo "Restarting corrosion"
  supervisorctl start corrosion
  supervisorctl start corro-consul
}

if [ -f "/var/lib/corrosion/backups/state.db" ]; then
    echo "Restoring backup for corrosion"

    echo "Stopping corrosion first"
    trap start_corrosion EXIT
    supervisorctl stop corrosion

    corrosion restore /var/lib/corrosion${ID}/backups/state.db
    supervisorctl start corrosion
    supervisorctl restart corro-consul
    exit 0
fi

echo "No current backup for corrosion"
