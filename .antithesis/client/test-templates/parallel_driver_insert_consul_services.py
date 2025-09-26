#! /usr/bin/env python3

import argparse
import requests
import random
import string
import json
import threading
random = random.SystemRandom()

import sys
sys.path.append("/opt/antithesis/py-resources")
import helper

def random_string(length=6):
    return ''.join(random.choices(string.ascii_lowercase, k=length))

def generate_service():
    service_name = f"service-{random_string()}"
    service_id = f"{service_name}-{random.randint(1000, 9999)}"
    port = random.randint(1000, 9999)
    check_type = random.choice(['tcp', 'http'])

    if check_type == 'tcp':
        check = {
            "Check": {
                "TCP": f"localhost:{port}",
                "Interval": "10s",
                "Timeout": "1s"
            }
        }
    else:
        check = {
            "Check": {
                "HTTP": f"http://localhost:{port}/health",
                "Interval": "10s",
                "Timeout": "1s"
            }
        }

    service_def = {
        "Name": service_name,
        "ID": service_id,
        "Address": "localhost",
        "Port": port,
        **check
    }

    return service_def

def register_service(address, service_def):
    try:
        response = requests.put(f"http://{address}/v1/agent/service/register", data=json.dumps(service_def))
        if response.status_code == 200:
            print(f"Registered: {service_def['Name']} on {service_def['Address']}:{service_def['Port']}")
        else:
            print(f"Failed to register service {service_def['Name']}: {response.text}")
    except requests.exceptions.ConnectionError as e:
        print(f"Failed to register service {service_def['Name']}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Insert services into consul')
    parser.add_argument('--addrs', nargs='+', help='List of consul addresses (e.g., --addresses consul1:8080 consul2:8080)')
    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1:8500", "corrosion2:8500", "corrosion3:8500"]
    threads = []
    for i in range(helper.random_int(1, 500)):
        address = random.choice(args.addrs)
        service_def = generate_service()
        thread = threading.Thread(target=register_service, args=(address, service_def))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
        
if __name__ == "__main__":
    main()
