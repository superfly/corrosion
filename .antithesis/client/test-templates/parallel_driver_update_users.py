#!/usr/bin/env -S python3 -u

import argparse
import json
import random
import requests
import string
import threading
import time

from antithesis.assertions import (
    reachable,
    always,
    sometimes
)

from antithesis.random import (
    random_choice
)

random = random.SystemRandom()

import sys
sys.path.append("/opt/antithesis/py-resources")
import helper

def update_in_corrosion(address, port, table, id, new_data):
    """Attempts to update a user in the Corrosion node."""

    columns = list(new_data.keys())
    random_columns = random.choices(columns, k=random.randint(1, len(columns)))

    sql_clauses = []
    for column in random_columns:
        sql_clauses.append(f"{column} = '{new_data[column]}'")
    sql_command = f"UPDATE {table} SET {', '.join(sql_clauses)} WHERE id = {id}"
    conn = helper.get_db_connection(address, port)
    success, err = helper.execute_psql(conn, sql_command)

    if not success:
        print(f"Error executing updating {table} {id}: {err}")
        return
    # print(f"Successfully updated {table} {id} in Corrosion node.")

def get_ids(address, port, table):
    data, err = helper.query_sql(address, port, f"SELECT id FROM {table} ORDER BY RANDOM() LIMIT 100")
    if err:
        print(f"Error getting user ids: {err}")
        return []
    print(f"Found {data} users in Corrosion.")
    ids = []
    lines = data.splitlines()
    for line in lines:
        parsed = json.loads(line)
        if 'row' in parsed:
            id = parsed["row"][1][0]
            ids.append(id)

    k = 1000
    if len(ids) < k:
        k = len(ids)

    return random.choices(ids, k=k)

def do_updates(corro_addrs, pg_port, http_port):
    for i in range(random.randint(1, 100)):
        address = random.choice(corro_addrs)
        threads = []
        for table in ["users", "teams", "deployments"]:
            table_ids = get_ids(address, http_port, table)
            if len(table_ids) == 0:
                print("Not enough users in Corrosion.")
                exit(0)
            for id in table_ids:
                new_data = helper.random_data(table)
                thread = threading.Thread(target=update_in_corrosion, args=(address, pg_port, table, id, new_data))
                threads.append(thread)
                thread.start()

        for thread in threads:
            thread.join()
 
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
