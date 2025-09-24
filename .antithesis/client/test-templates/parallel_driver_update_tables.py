#!/usr/bin/env -S python3 -u

import argparse
import json
import random
import requests
import string
import threading
import time


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
    helper.execute_psql(conn, sql_command)


def get_ids(address, port, table):
    ids = helper.query_sql(address, port, f"SELECT id FROM {table} ORDER BY RANDOM() LIMIT 100")
    print(f"Found {len(ids)} {table} ids in Corrosion.")
    return ids
 

def do_updates(address, pg_port, http_port):
    try:
        for table in ["users", "teams", "deployments"]:
            table_ids = get_ids(address, http_port, table)
            if len(table_ids) == 0:
                print(f"Not enough {table} in Corrosion.")
                continue
            for id in table_ids:
                new_data = helper.random_data(table)
                update_in_corrosion(address, pg_port, table, id[0], new_data)
    except Exception as e:
        print(f"Error updating tables: {e}")
        return

def main():
    parser = argparse.ArgumentParser(description='Insert teams and users into corrosion databases')
    parser.add_argument('--addrs', nargs='+', help='List of corrosion hostnames (e.g., --addresses corrosion1 corrosion2)')
    parser.add_argument('--pg-port', nargs='?', type=int, default=5470, help='postgres port')
    parser.add_argument('--http-port', nargs='?', type=int, default=8080, help='http port')

    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1", "corrosion2", "corrosion3"]

    threads = []
    for i in range(random.randint(1, 100)):
        address = random.choice(args.addrs)
        thread = threading.Thread(target=do_updates, args=(address, args.pg_port, args.http_port))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
