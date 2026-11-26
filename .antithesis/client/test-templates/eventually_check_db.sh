#!/bin/bash

default_dir="/var/lib/corrosion1/state.db"
default_corrosion_dirs=("/var/lib/corrosion2/state.db" "/var/lib/corrosion3/state.db")


# Compare all other databases against the first one
against_dir="${1:-$default_dir}"
if [ $# -gt 1 ]; then
    corrosion_dir=("${@:2}")
else
    corrosion_dir=("${default_corrosion_dirs[@]}")
fi

diffDb () {
    python3 - "$against_dir" "${corrosion_dir[@]}" <<'PY'
import re
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path


def tracked_tables(path):
    uri = Path(path).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        names = {name for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
    return {
        name.rsplit("__crsql_", 1)[0]
        for name in names
        if "__crsql_" in name
        and name.rsplit("__crsql_", 1)[1]
        and name.rsplit("__crsql_", 1)[0] in names
    }


def compare(reference, other):
    print(f"Checking {other} against {reference}", flush=True)
    try:
        first, second = tracked_tables(reference), tracked_tables(other)
    except (OSError, sqlite3.Error) as error:
        print(f"Failed to discover tracked tables: {error}")
        return False
    if not first or not second:
        print("No CR-SQLite tracked tables found")
        return False
    if first != second:
        print(f"Tracked tables differ: only in {reference}: {sorted(first - second)}; "
              f"only in {other}: {sorted(second - first)}")
        return False

    consistent = True
    for table in sorted(first | second):
        print(f"Checking table {table}", flush=True)
        try:
            result = subprocess.run(
                ["sqldiff", "--primarykey", "--summary", "--table", table, reference, other],
                capture_output=True, text=True, check=True,
            )
        except (OSError, subprocess.CalledProcessError) as error:
            print(f"Failed to run sqldiff on {table}: {error}")
            consistent = False
            continue
        summary = re.search(
            r": (\d+) changes, (\d+) inserts, (\d+) deletes(?:, \d+ unchanged)?\s*$",
            result.stdout,
        )
        if summary is None:
            print(f"Unexpected sqldiff output for {table}: {result.stdout!r}")
            consistent = False
        elif any(int(count) for count in summary.groups()):
            print(f"Table {table} is not consistent: {result.stdout.strip()}")
            consistent = False
    return consistent


reference, *others = sys.argv[1:]
results = [compare(reference, other) for other in others]
sys.exit(0 if all(results) else 1)
PY
}

for i in {1..40}; do
    diffDb
    if [ $? -eq 0 ]; then
        echo "All tables are consistent"
        exit 0
    fi
    sleep 30
done

echo "Inconsistent data in db"
exit 1
