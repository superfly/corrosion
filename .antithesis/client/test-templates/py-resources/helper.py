import json
import pg8000.dbapi
import requests
import socket
import string
import struct
import random
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

def get_db_connection(host, port):
    return pg8000.dbapi.Connection(
        host=host,
        port=int(port),
        database="corrosion",
        user="corrosion",
        password="corrosion"
    )


# Rows with this name prefix are written only to be rolled back. A persisted
# row means crsqlite kept a change that the transaction discarded.
ROLLED_BACK_PREFIX = "corro_rb_"

# Long enough that a 1s server timeout or a cancel hits it, short enough that
# a missed interrupt does not hold the write lock for long.
SLOW_READ = (
    "WITH RECURSIVE c(i) AS ("
    "SELECT 1 UNION ALL SELECT i + 1 FROM c LIMIT 10000000"
    ") SELECT max(i) FROM c"
)

def pick_tx_mode():
    # Commit stays the common path so the workload still makes progress.
    return random_choice(
        ["commit"] * 8
        + ["partial_rollback", "partial_rollback"]
        + ["rollback", "rollback"]
        + ["interrupt"]
    )

def note_tx_mode(mode, protocol):
    details = {"mode": mode, "protocol": protocol}
    sometimes(mode == "commit", "Clients commit pg and http transactions", details)
    sometimes(
        mode == "partial_rollback",
        "Clients roll back some statements and commit others",
        details,
    )
    sometimes(mode == "rollback", "Clients roll back pg and http transactions", details)
    sometimes(mode == "interrupt", "Clients interrupt pg and http transactions", details)

def rolled_back_insert():
    return f"INSERT INTO teams (name) VALUES ('{ROLLED_BACK_PREFIX}{random_name()}')"

def _exec(cursor, sql_command, params=None):
    if params:
        cursor.execute(sql_command, params)
    else:
        cursor.execute(sql_command)

def _cancel_pg(conn):
    key = getattr(conn, "_backend_key_data", None)
    sock = getattr(conn, "_usock", None)
    if not key or len(key) < 8 or sock is None:
        return
    pid, secret = struct.unpack("!ii", key[:8])
    try:
        peer = sock.getpeername()
    except OSError as e:
        print(f"pg cancel skipped: {e}")
        return
    try:
        with socket.create_connection(peer, timeout=5) as cancel_sock:
            # Postgres CancelRequest: length, cancel code, backend pid, secret.
            cancel_sock.sendall(struct.pack("!iiii", 16, 80877102, pid, secret))
    except OSError as e:
        print(f"pg cancel failed: {e}")

def _interrupt_pg(conn, cursor):
    def run():
        try:
            cursor.execute(SLOW_READ)
        except Exception as e:
            print(f"interrupted pg statement: {e}")

    worker = threading.Thread(target=run)
    worker.start()
    time.sleep(0.2)
    _cancel_pg(conn)
    worker.join(timeout=10)
    if worker.is_alive():
        try:
            conn._usock.close()
        except Exception:
            pass
        worker.join(timeout=5)

def execute_psql(conn, sql_command, params=None):
    mode = pick_tx_mode()
    note_tx_mode(mode, "pg")
    cursor = conn.cursor()
    try:
        _exec(cursor, sql_command, params)
        if mode == "commit":
            conn.commit()
        elif mode == "rollback":
            _exec(cursor, rolled_back_insert())
            conn.rollback()
        elif mode == "partial_rollback":
            # The caller's statement stays. The sentinel insert is discarded.
            cursor.execute("SAVEPOINT corro_sp")
            _exec(cursor, rolled_back_insert())
            cursor.execute("ROLLBACK TO SAVEPOINT corro_sp")
            cursor.execute("RELEASE SAVEPOINT corro_sp")
            conn.commit()
        else:
            _interrupt_pg(conn, cursor)
            try:
                conn.rollback()
            except Exception as e:
                print(f"pg rollback after interrupt failed: {e}")
                try:
                    conn.close()
                except Exception:
                    pass
    finally:
        cursor.close()

def execute_sql(address, sql_command):
    mode = pick_tx_mode()
    note_tx_mode(mode, "http")
    headers = {'Content-Type': 'application/json'}
    url = f"http://{address}/v1/transactions"
    expect_ok = True
    if mode == "partial_rollback":
        statements = [
            sql_command,
            "SAVEPOINT corro_sp",
            rolled_back_insert(),
            "ROLLBACK TO SAVEPOINT corro_sp",
            "RELEASE SAVEPOINT corro_sp",
        ]
    elif mode == "rollback":
        # A later statement error aborts the whole HTTP transaction.
        statements = [sql_command, rolled_back_insert(), "SELECT * FROM __corro_force_rollback"]
        expect_ok = False
    elif mode == "interrupt":
        url += "?timeout=1"
        statements = [sql_command, SLOW_READ]
        expect_ok = False
    else:
        statements = [sql_command]

    try:
        result = requests.post(url, headers=headers, json=statements, timeout=20)
    except requests.RequestException as e:
        print(f"{mode} transaction request failed for {address}: {e}")
        if expect_ok:
            raise
        return

    if not expect_ok:
        print(f"{mode} transaction finished with status {result.status_code} for {address}")
        return

    sometimes(result.status_code == 200, "Clients can make successful transactions to corrosion", {"status_code": result.status_code})
    if result.status_code != 200:
        raise Exception(f"Unexpected status code: {result.status_code} - {result.text} {result.request.body} for {address}")

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
