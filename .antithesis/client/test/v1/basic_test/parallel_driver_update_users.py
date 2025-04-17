#!/usr/bin/env -S python3 -u

import json
import random
import requests
import string
import time

from antithesis.assertions import (
    reachable,
    always,
    sometimes
)

def execute_sql(address, sql_command):
    headers = {'Content-Type': 'application/json'}
    try:
        result = requests.post(f"http://{address}/v1/transactions", headers=headers, json=[sql_command])
        sometimes(result.status_code == 200, "Clients can make successful transactions to corrosion", {"status_code": result.status_code})
        if result.status_code != 200:
            return False, f"Unexpected status code: {result.status_code} - {result.text}"
        else:
            return True, None
    except requests.exceptions.ConnectionError as e:
        return False, e

def query_sql(address, sql_command):
    headers = {'Content-Type': 'application/json'}
    try:
        result = requests.post(f"http://{address}/v1/queries", headers=headers, json=f"{sql_command}")
        sometimes(result.status_code == 200, "Clients can make successful queries to corrosion", {"status_code": result.status_code})
        if result.status_code != 200:
            return False, f"Unexpected status code: {result.status_code} - {result.text}"
        else:
            return result.text, None
    except requests.exceptions.ConnectionError as e:
        return False, e

def random_name(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def update_in_corrosion(address, op, id, encoded_id, status):
    """Attempts to update a user in the Corrosion node."""

    if op == "update":
        sql_command = f"UPDATE users SET encoded_id = '{encoded_id}', status = '{status}', updated_at = {time.time()} WHERE id = {id}"
    elif op == "delete":
        sql_command = f"DELETE FROM users WHERE id = {id}"

    success, err = execute_sql(address, sql_command)
    if not success:
        print(f"Error executing {op} for user {id}: {err}")
        return
    print(f"Successfully {op}d user {id} in Corrosion node.")

def get_user_ids(address):
    data, err = query_sql(address, "SELECT id FROM users ORDER BY RANDOM() LIMIT 100")
    if err:
        print(f"Error getting user ids: {err}")
        return []
    print(f"Found {data} users in Corrosion.")
    numbers = []
    lines = data.splitlines()
    for line in lines:
        parsed = json.loads(line)
        if 'row' in parsed:
            inner_number = parsed["row"][1][0]
            numbers.append(inner_number)
    return numbers

def do_updates():
    corro_addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]
    address = random.choice(corro_addrs)

    user_ids = get_user_ids(address)
    if len(user_ids) < 15:
        print("Not enough users in Corrosion.")
        exit(0)

    update_ids = random.choices(user_ids, k=15)
    # todo: use psql here
    for id in update_ids:
        encoded_id = random_name()
        status = random.choice(["active", "inactive", "blocked", "suspended", "admin"])
        op = random.choice(["update", "delete"])
        update_in_corrosion(address, op, id, encoded_id, status)

def main():
    do_updates()

if __name__ == "__main__":
    main()
