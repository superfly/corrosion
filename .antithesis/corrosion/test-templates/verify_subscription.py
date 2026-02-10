#!/usr/bin/env python3
"""
Verify that subscription query table matches the executed query results.

Usage:
    python3 scripts/verify_subscription.py <state_db_path> <subscription_id_or_path>

Examples:
    # Using subscription ID (UUID)
    python3 scripts/verify_subscription.py ./state.db ba247cbc-2a7f-486b-873c-8a9620e72182
    
    # Using full path to subscription database
    python3 scripts/verify_subscription.py ./state.db ./subscriptions/ba247cbc-2a7f-486b-873c-8a9620e72182/sub.sqlite
"""

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import List


def get_subscription_db_path(state_db: Path, subscription: str) -> Path:
    if '/' in subscription:
        return Path(subscription)
    else:
        # Assume it's a UUID, construct path
        state_parent = state_db.parent
        if not state_parent:
            raise ValueError("State DB path has no parent directory")
        return state_parent / "subscriptions" / subscription / "sub.sqlite"


def get_sql_from_meta(conn: sqlite3.Connection):
    cursor = conn.execute("SELECT value FROM meta WHERE key = 'sql'")
    row = cursor.fetchone()
    if not row:
        raise ValueError("SQL not found in meta table")
    return row[0]


def get_query_columns(conn: sqlite3.Connection) -> List[str]:
    cursor = conn.execute("PRAGMA table_info(query)")
    columns = [row[1] for row in cursor.fetchall() if "__corro_" not in row[1]]
    return columns


def compare_with_except(
    sub_db_path: Path,
    state_db_path: Path,
    columns: List[str],
    sql: str,
):
    columns_str = ", ".join(columns)
    
    sub_conn = sqlite3.connect(str(sub_db_path))
    sub_conn.execute(f"ATTACH DATABASE '{state_db_path}' AS corro_main")

    # TODO: use count to do a quick chec
    # sub_count = sub_conn.execute("SELECT COUNT(*) FROM query").fetchone()[0]
    # sub_conn.close()


    try:
        # Find rows in subscription that are NOT in executed query
        except_sub_query = f"""
            SELECT {columns_str} FROM query
            EXCEPT
            {sql} LIMIT 2
        """
        
        # Find rows in executed query that are NOT in subscription
        except_state_query = f"""
            {sql}
            EXCEPT
            SELECT {columns_str} FROM query
            LIMIT 2
        """

        cursor = sub_conn.execute(except_sub_query)
        rows_not_in_state = cursor.fetchall()
        cursor = sub_conn.execute(except_state_query)
        rows_not_in_sub = cursor.fetchall()
    except sqlite3.OperationalError as e:
        print(f"error comparing queries: {e}")
        return [], []

    return rows_not_in_sub, rows_not_in_state

def main():
    parser = argparse.ArgumentParser(
        description="Verify subscription query table matches executed query results"
    )
    parser.add_argument("state_db", default="./state.db", type=Path, help="Path to the corrosion state database")
    parser.add_argument(
        "subscription",
        type=str,
        help="Subscription ID (UUID) or path to subscription database",
    )
    
    args = parser.parse_args()
    
    # Determine subscription database path
    try:
        sub_db_path = get_subscription_db_path(args.state_db, args.subscription)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    
    if not sub_db_path.exists():
        print(f"Error: Subscription database not found: {sub_db_path}", file=sys.stderr)
        sys.exit(1)
    
    if not args.state_db.exists():
        print(f"Error: State database not found: {args.state_db}", file=sys.stderr)
        sys.exit(1)
    
    print(f"State DB: {args.state_db}")
    print(f"Subscription DB: {sub_db_path}")
    print()
    
    # Open subscription database
    sub_conn = sqlite3.connect(str(sub_db_path))
    sub_conn.row_factory = sqlite3.Row

    sql = get_sql_from_meta(sub_conn)
    print(f"Original SQL: {sql}")  
    query_columns = get_query_columns(sub_conn)
    print(f"Query columns ({len(query_columns)}): {', '.join(query_columns)}")
        
    sub_conn.close()
    print("Comparing results using SQLite EXCEPT...")
        
    try:
        rows_not_in_sub, rows_not_in_state = compare_with_except(sub_db_path, args.state_db, query_columns, sql)
        if len(rows_not_in_sub) > 0 or len(rows_not_in_state) > 0:
            print("found mismatch between subscription and state db")
            for row in rows_not_in_sub:
                print(f"Row: {row} in main db but not in subs")
            for row in rows_not_in_state:
                print(f"Row: {row} in subs but not in main db")
            sys.exit(1)
        else:
            print("no mismatch found")
    except Exception as e:
        print(f"error comparing queries: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

