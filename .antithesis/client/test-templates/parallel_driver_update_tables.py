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
    try:
        columns = list(new_data.keys())
        random_columns = random.choices(columns, k=random.randint(1, len(columns)))

        sql_clauses = []
        for column in random_columns:
            sql_clauses.append(f"{column} = '{new_data[column]}'")
        sql_command = f"UPDATE {table} SET {', '.join(sql_clauses)} WHERE id = {id}"
        conn = helper.get_db_connection(address, port)
        helper.execute_psql(conn, sql_command)
    except Exception as e:
        print(f"Error executing updating {table} {id}: {e}")
        return


def get_ids(address, port, table):
    ids = helper.query_sql(address, port, f"SELECT id FROM {table} ORDER BY RANDOM() LIMIT 100")
    print(f"Found {len(ids)} {table} ids in Corrosion.")
    return ids

def do_updates(corro_addrs, pg_port, http_port):
    for i in range(random.randint(1, 1000)):
        address = random.choice(corro_addrs)
        threads = []
        for table in ["users", "teams", "deployments"]:
            table_ids = get_ids(address, http_port, table)
            if len(table_ids) == 0:
                print("Not enough users in Corrosion.")
                exit(0)
            for id in table_ids:
                new_data = helper.random_data(table)
                thread = threading.Thread(target=update_in_corrosion, args=(address, pg_port, table, id[0], new_data))
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
