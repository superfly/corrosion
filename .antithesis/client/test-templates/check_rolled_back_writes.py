#!/usr/bin/env python3
"""Fail if a statement that the workload rolled back was persisted."""

import sqlite3
import sys

PREFIX = "corro_rb_"


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} DATABASE", file=sys.stderr)
        return 2

    conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=300)
    try:
        present = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='teams'"
        ).fetchone()
        if present is None:
            print("rolled-back write check skipped: teams table missing")
            return 0

        rows = conn.execute(
            "SELECT id, name FROM teams WHERE name LIKE ? LIMIT 20",
            (PREFIX + "%",),
        ).fetchall()
    finally:
        conn.close()

    if rows:
        print(f"rolled-back write check FAILED: {len(rows)} sentinel rows persisted")
        for row_id, name in rows:
            print(f"  - id={row_id} name={name}")
        return 1

    print("rolled-back write check OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
