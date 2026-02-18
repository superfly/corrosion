#!/usr/bin/env -S python3 -u
from antithesis.random import AntithesisRandom
import argparse
import time
import threading

import sys
sys.path.append("/opt/antithesis/py-resources")
import helper


random = AntithesisRandom()

def insert_user(address, team_id):
    try:
        name = helper.random_name()
        status = random.choice(["active", "inactive", "blocked", "suspended", "admin"])
        sql_command = f"INSERT INTO users (name, email, team_id, status, created_at) VALUES ('{name}', '{name}@email.com', {team_id}, '{status}', {time.time()}) ON CONFLICT DO NOTHING"
        helper.execute_sql(address, sql_command)
    except Exception as e:
        print(f"Error inserting user for {address}: {e}")


def do_inserts(address):
    try:
        id = helper.get_random_cols(address, "teams", ["id"])
        print(f"inserting user for team {id} into {address}")
        if id is not None:
            for j in range(helper.random_int(1, 2)):
                insert_user(address, id[0])
    except Exception as e:
        print(f"Error inserting user: {e}")
        return

def main():
    parser = argparse.ArgumentParser(description='Insert teams and users into corrosion databases')
    parser.add_argument('--addrs', nargs='+', help='List of corrosion addresses (e.g., --addresses corrosion1:8080 corrosion2:8080)')
    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]
    threads = []
    for i in range(helper.random_int(1, 100)):
        address = random.choice(args.addrs)
        thread = threading.Thread(target=do_inserts, args=(address,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
