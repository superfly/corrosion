#!/usr/bin/env -S python3 -u

import json
import random
import requests
import string
import time
sys.path.append("/opt/antithesis/py-resources")
import helper

from antithesis.assertions import (
    reachable,
    always,
    sometimes
)

def random_name(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def update_in_corrosion(address, op, id, encoded_id, status):
    """Attempts to update a user in the Corrosion node."""

    if op == "update":
        sql_command = f"UPDATE users SET encoded_id = '{encoded_id}', status = '{status}', updated_at = {time.time()} WHERE id = {id}"
    elif op == "delete":
        sql_command = f"DELETE FROM users WHERE id = {id}"

    success, err = helper.execute_sql(address, sql_command)
    if not success:
        print(f"Error executing {op} for user {id}: {err}")
        return
    print(f"Successfully {op}d user {id} in Corrosion node.")

def get_user_ids(address):
    data = helper.query_sql(address, "SELECT id FROM users ORDER BY RANDOM() LIMIT 100")
    print(f"Found {len(data)} users in Corrosion.")
    numbers = []
    for line in data:
        numbers.append(line[0])
    return numbers

def do_updates():
    corro_addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]
    address = random.choice(corro_addrs)

    try:
        user_ids = get_user_ids(address)
        if len(user_ids) < 2:
            print("Not enough users in Corrosion.")
            exit(0)

        update_ids = random.choices(user_ids, k=15)
        # todo: use psql here
        for id in update_ids:
            encoded_id = random_name()
            status = random.choice(["active", "inactive", "blocked", "suspended", "admin"])
            op = random.choice(["update", "delete"])
            update_in_corrosion(address, op, id, encoded_id, status)
    except Exception as e:
        print(f"Error updating users: {e}")

def main():
    do_updates()

if __name__ == "__main__":
    main()
