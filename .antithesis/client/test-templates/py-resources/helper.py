import json
import pg8000.dbapi
import requests
import string
import random
import time
from antithesis.assertions import (
    reachable,
    always,
    sometimes
)

from antithesis.random import (
    random_choice
)

def get_db_connection(host, port):
    try:
        return pg8000.dbapi.Connection(
            host=host,
            port=int(port),
            database="corrosion",
            user="corrosion",
            password="corrosion"
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def execute_psql(conn, sql_command, params=None):
    cursor = conn.cursor()
    if params:
        cursor.execute(sql_command, params)
    else:
        cursor.execute(sql_command)
    conn.commit()
    cursor.close()

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

def query_sql(address, port, sql_command):
    headers = {'Content-Type': 'application/json'}
    result = requests.post(f"http://{address}:{port}/v1/queries", headers=headers, json=f"{sql_command}")
    sometimes(result.status_code == 200, "Clients can make successful queries to corrosion", {"status_code": result.status_code})
    if result.status_code != 200:
        raise Exception(f"Unexpected status code: {result.status_code} - {result.text} {result.request.body}")
    else:
        res = []
        lines = result.text.splitlines()
        for line in lines:
            parsed = json.loads(line)
            if 'row' in parsed:
                res.append(parsed["row"][1])
        return res

def random_name(length=6):
    return ''.join(random.choices(string.ascii_letters, k=length))

def random_int(min, max):
    return random_choice(range(min, max))

def random_version():
    return f"{random.randint(1, 10)}.{random.randint(0, 9)}.{random.randint(0, 9)}"

def random_deploy():
    data = {
        "node": f"node-{random_int(1, 5)}",
        "name": f"deployment-{random_name()}",
        "version": random_version(),
        "status": random_choice(["creating", "running", "failed", "stopped"]),
        "settings": "{}",
        "egress_ip": f"10.0.{random_int(1, 255)}.{random_int(1, 255)}",
        "ingress_ip": f"192.168.{random_int(1, 255)}.{random_int(1, 255)}",
        "updated_at": int(time.time())
    }
    return data

def random_team():
    data = {
        "name": random_name(),
        "net_id": random.randint(1, 1000),
        "role": random_choice(["admin", "dev", "user", "viewer"]),
        "settings": {},
        "state": random_choice(["creating", "deliquent", "active", "inactive"]),
        "balancer_ip": "127.0.0.1"
    }
    return data

def random_user():
    data = {
        "name": random_name(),
        "encoded_id": random_name(),
        "email": random_name() + "@example.com",
        "status": random_choice(["active", "inactive", "blocked", "suspended", "admin"]),
        "team_id": random.randint(1, 1000),
    }
    return data

def random_data(table):
    if table == "users":
        return random_user()
    elif table == "teams":
        return random_team()
    elif table == "deployments":
        return random_deploy()

def get_random_cols(address, table, cols=["id"]):
    host, port = address.split(':')
    sql_command = f"SELECT {', '.join(cols)} FROM {table} ORDER BY RANDOM() LIMIT 1"
    res = query_sql(host, port, sql_command)
    if len(res) == 0:
        return None
    else:
        return res[0]
