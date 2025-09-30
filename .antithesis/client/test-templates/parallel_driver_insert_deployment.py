#!/usr/bin/env -S python3 -u
import argparse
import random
import time
import threading

import sys
sys.path.append("/opt/antithesis/py-resources")
import helper


random = random.SystemRandom()

def insert_deployment(conn, team_id, net_id):
    try:
        data = {
            "id": random.randint(1, 10000000),
            "node": f"node-{random.randint(1, 5)}",
            "name": f"deployment-{helper.random_name()}",
            "version": helper.random_version(),
            "status": random.choice(["creating", "running", "failed", "stopped"]),
            "settings": "{}",
            "egress_ip": f"10.0.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "ingress_ip": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
            "team_id": team_id,
            "net_id": net_id,
            "created_at": int(time.time()),
            "updated_at": int(time.time())
        }

        sql_command = """
            INSERT INTO deployments (
                id, node, name, version, status, settings,
                egress_ip, ingress_ip, team_id, net_id,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        params = (
            data['id'], data['node'], data['name'], data['version'],
            data['status'], data['settings'], data['egress_ip'],
            data['ingress_ip'], data['team_id'], data['net_id'],
            data['created_at'], data['updated_at']
        )

        helper.execute_psql(conn, sql_command, params)
    except Exception as e:
        print(f"Error inserting deployment: {e}")

def do_inserts(address):
    id = helper.get_random_cols(address, "teams", ["id"])

    host, port = address.split(':')
    conn = helper.get_db_connection(host, "5470")
    if conn is not None:
        for j in range(helper.random_int(1, 10)):
            insert_deployment(conn, id[0], helper.random_int(1, 1000))

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
