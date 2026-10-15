#!/usr/bin/env python3
"""Check that CR-SQLite never reuses a db_version with multiple timestamps."""

import sqlite3
import sys


def qi(name):
    return '"' + name.replace('"', '""') + '"'


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} DATABASE", file=sys.stderr)
        return 2

    db_path = sys.argv[1]
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=300)
    try:
        tables = [
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table' AND (
                    name LIKE '%__crsql_clock' OR
                    name LIKE '%__crsql_v2_clock' OR
                    name LIKE '%__crsql_v2_tombstones'
                )
                """
            )
        ]

        errors = []
        for table in tables:
            rows = conn.execute(
                f"""
                SELECT site_id, db_version, COUNT(*)
                FROM (
                    SELECT DISTINCT site_id, db_version, ts
                    FROM {qi(table)}
                )
                GROUP BY site_id, db_version
                HAVING COUNT(*) > 1
                """
            ).fetchall()
            errors.extend(
                f"{table}: site_id {site_id} db_version {db_version} has {timestamp_count} timestamps"
                for site_id, db_version, timestamp_count in rows
            )
    finally:
        conn.close()

    if errors:
        print("db_version reuse check FAILED")
        for error in errors:
            print(f"  - {error}")
        return 1

    print(f"db_version reuse check OK ({len(tables)} metadata tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
