#!/usr/bin/env -S python3 -u
from antithesis.random import AntithesisRandom
import argparse
import time
import threading


import sys
sys.path.append("/opt/antithesis/py-resources")
import helper


random = AntithesisRandom()

def insert_team(address):
    try:
        data = {
            "id": random.randint(1, 10000000),
            "name": helper.random_name(),
            "net_id": random.randint(1, 1000),
            "role": random.choice(["admin", "dev", "user", "viewer"]),
            "settings": {},
            "state": random.choice(["creating", "deliquent", "active", "inactive"]),
            "balancer_ip": "127.0.0.1",
            "created_at": time.time(),
            "updated_at": time.time()
        }

        sql_command = f"""INSERT INTO teams
            (name, net_id, role, settings, state, balancer_ip, created_at, updated_at)
        VALUES
            ('{data['name']}', {data['net_id']}, '{data['role']}', '{data['settings']}', '{data['state']}', '{data['balancer_ip']}', {data['created_at']}, {data['updated_at']})
        ON CONFLICT DO NOTHING"""
        helper.execute_sql(address, sql_command)
    except Exception as e:
        print(f"Error inserting team for {address}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Insert teams and users into corrosion databases')
    parser.add_argument('--addrs', nargs='+', help='List of corrosion addresses (e.g., --addresses corrosion1:8080 corrosion2:8080)')
    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]
    threads = []
    for i in range(helper.random_int(1, 100)):
        address = random.choice(args.addrs)
        thread = threading.Thread(target=insert_team, args=(address,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
