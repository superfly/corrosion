#!/usr/bin/env -S python3 -u
import argparse
import random
import subprocess
import string
import time
import requests
import threading

from antithesis.assertions import (
    reachable,
    always,
    sometimes
)

import sys
sys.path.append("/opt/antithesis/py-resources")
import helper


random = random.SystemRandom()

def insert_deployment(conn, team_id, net_id):
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

    success, err = helper.execute_psql(conn, sql_command, params)
    if not success:
        print(f"Error inserting deployment: {err}")
    # else:
    #     print(f"Inserted deployment {data['id']} for team {team_id}")

def insert_user(address, team_id):
    name = helper.random_name()
    status = random.choice(["active", "inactive", "blocked", "suspended", "admin"])
    sql_command = f"INSERT INTO users (name, email, team_id, status, created_at) VALUES ('{name}', '{name}@email.com', {team_id}, '{status}', {time.time()}) ON CONFLICT DO NOTHING"
    success, err = helper.execute_sql(address, sql_command)
    if not success:
        print(f"Error inserting user: {err}")
    # else:
    #     print(f"Inserted user {name} for team {team_id}")

def insert_team(address):
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
    success, err = helper.execute_sql(address, sql_command)
    if not success:
        print(f"Error inserting team: {err}")
        return None
    
    # print(f"Inserted team {data['id']}")
    return data['id']
 
def do_inserts(address):
    host, port = address.split(':')
    id = insert_team(address)
    if id is not None:
        for j in range(helper.random_int(1, 100)):
            insert_user(address, id)
            conn = helper.get_db_connection(host, "5470")
            if conn is not None:
                for j in range(helper.random_int(1, 1000)):
                    insert_deployment(conn, id, helper.random_int(1, 1000))
            
 
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
