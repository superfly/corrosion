#!/usr/bin/env -S python3 -u
import argparse
import random
import threading

import sys
sys.path.append("/opt/antithesis/py-resources")
import helper


random = random.SystemRandom()

def do_deletes(address):
    try:
        id = helper.get_random_cols(address, "users", ["id"]) 
        host, port = address.split(':')
        print(f"deleting user {id} from {address}")
        conn = helper.get_db_connection(host, "5470")
        if id is not None:
            id = id[0]
            helper.execute_psql(conn, f"DELETE FROM users WHERE id = {id}")
    except Exception as e:
        print(f"Error deleting team {id}: {e}")
        return


def main():
    parser = argparse.ArgumentParser(description='Insert teams and users into corrosion databases')
    parser.add_argument('--addrs', nargs='+', help='List of corrosion addresses (e.g., --addresses corrosion1:8080 corrosion2:8080)')
    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]
    threads = []
    for i in range(helper.random_int(1, 20)):
        address = random.choice(args.addrs)
        thread = threading.Thread(target=do_deletes, args=(address,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()