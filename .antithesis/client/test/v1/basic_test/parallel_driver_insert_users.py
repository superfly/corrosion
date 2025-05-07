#!/usr/bin/env -S python3 -u

import random
import subprocess
import string
import time
import requests
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


def random_name(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))
 
def insert_user(address, team_id):
    name = random_name()
    status = random.choice(["active", "inactive", "blocked", "suspended", "admin"])
    sql_command = f"INSERT INTO users (name, email, team_id, status, created_at) VALUES ('{name}', '{name}@email.com', {team_id}, '{status}', {time.time()})"
    success, err = execute_sql(address, sql_command)
    if not success:
        print(f"Error inserting user: {err}")
    #else:
        # print(f"Inserted user {name} for team {team_id}")

def insert_team(address):
    data = {
        "id": random.randint(1, 10000000),
        "name": random_name(),
        "net_id": random.randint(1, 1000),
        "role": random.choice(["admin", "dev", "user", "viewer"]),
        "settings": {},
        "state": random.choice(["creating", "deliquent", "active", "inactive"]),
        "balancer_ip": "127.0.0.1",
        "created_at": time.time(),
        "updated_at": time.time()
    }

    sql_command = f"INSERT INTO teams (name, net_id, role, settings, state, balancer_ip, created_at, updated_at) VALUES ('{data['name']}', {data['net_id']}, '{data['role']}', '{data['settings']}', '{data['state']}', '{data['balancer_ip']}', {data['created_at']}, {data['updated_at']})"
    success, err = execute_sql(address, sql_command)
    if not success:
        print(f"Error inserting team: {err}")
        return None
    
    print(f"Inserted team {data['id']}")
    return data['id']
 
def do_inserts():
    corro_addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]
    # create 20 teams
    for i in range(20):
        address = random.choice(corro_addrs)
        id = insert_team(address)
        # create ten users for each team
        if id is not None:
            for j in range(10):
                insert_user(address, id)
 
def main():
    do_inserts()
 
if __name__ == "__main__":
    main()
