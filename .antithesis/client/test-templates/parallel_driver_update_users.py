#!/usr/bin/env -S python3 -u

import argparse
import json
from antithesis.random import AntithesisRandom
import string
import sys
import time
sys.path.append("/opt/antithesis/py-resources")
import helper

from antithesis.assertions import (
    reachable,
    always,
    sometimes
)

random = AntithesisRandom()

def random_name(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def update_in_corrosion(address, op, id, encoded_id, status):
    """Attempts to update a user in the Corrosion node."""

    try:
        if op == "update":
            sql_command = f"UPDATE users SET encoded_id = '{encoded_id}', status = '{status}', updated_at = {time.time()} WHERE id = {id}"
        elif op == "delete":
            sql_command = f"DELETE FROM users WHERE id = {id}"

        helper.execute_sql(address, sql_command)
        print(f"Successfully {op}d user {id} in Corrosion node.")
    except Exception as e:
        print(f"Error executing {op} for user {id}: {e}")
        return

def get_user_ids(address, http_port):
    data = helper.query_sql(address, http_port, "SELECT id FROM users ORDER BY RANDOM() LIMIT 100")
    print(f"Found {len(data)} users in Corrosion.")
    numbers = []
    for line in data:
        numbers.append(line[0])
    return numbers

def do_updates(corro_addrs, pg_port, http_port):
    address = random.choice(corro_addrs)

    try:
        user_ids = get_user_ids(address, http_port)
        if len(user_ids) < 2:
            print("Not enough users in Corrosion.")
            exit(0)

        update_ids = random.choices(user_ids, k=15)
        # todo: use psql here
        for id in update_ids:
            encoded_id = random_name()
            status = random.choice(["active", "inactive", "blocked", "suspended", "admin"])
            op = random.choice(["update", "delete"])
            update_in_corrosion(f"{address}:{http_port}", op, id, encoded_id, status)
    except Exception as e:
        print(f"Error updating users: {e}")

def main():
    parser = argparse.ArgumentParser(description='Insert teams and users into corrosion databases')
    parser.add_argument('--addrs', nargs='+', help='List of corrosion hostnames (e.g., --addresses corrosion1 corrosion2)')
    parser.add_argument('--pg-port', nargs='?', type=int, default=5470, help='postgres port')
    parser.add_argument('--http-port', nargs='?', type=int, default=8080, help='http port')

    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1", "corrosion2", "corrosion3"]

    do_updates(args.addrs, args.pg_port, args.http_port)

if __name__ == "__main__":
    main()
