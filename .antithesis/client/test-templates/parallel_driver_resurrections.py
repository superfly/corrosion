#!/usr/bin/env python3
"""Repeatedly delete and reinsert a bounded set of composite primary keys."""

import argparse
import json
import os
import time

import requests

try:
    from antithesis.assertions import sometimes
    from antithesis.random import AntithesisRandom
except ModuleNotFoundError:
    if os.environ.get("ANTITHESIS_OUTPUT_DIR"):
        raise
    from random import Random as AntithesisRandom

    def sometimes(condition, message, details):
        print(f"{message}: {condition} {details}")


DEFAULT_ADDRESSES = ("corrosion1:8080", "corrosion2:8080", "corrosion3:8080")
BUCKETS = 8
ROWS_PER_BATCH = 16
DEFAULT_ROUNDS = 32
MAX_SECONDS = 60
REQUEST_TIMEOUT = (2, 5)


def statements(bucket, value):
    # Reserve a namespace within wide so the driver never deletes other rows.
    blob_key = (b"antithesis-resurrection-" + bytes([bucket])).hex()
    keys = [f"resurrection-{i:02d}" for i in range(ROWS_PER_BATCH)]
    values = ", ".join(
        f"(X'{blob_key}', '{key}', {value}, 0.5, X'00ff')" for key in keys
    )
    insert = (
        'INSERT INTO wide (id1, id2, "int", "float", "blob") '
        f"VALUES {values} ON CONFLICT(id1, id2) DO NOTHING"
    )
    key_list = ", ".join(f"'{key}'" for key in keys)
    predicate = f"id1=X'{blob_key}' AND id2 IN ({key_list})"
    delete = f"DELETE FROM wide WHERE {predicate}"
    update = f'UPDATE wide SET "int" = "int" + 1 WHERE {predicate}'
    return insert, delete, update


def execute(session, address, sql):
    try:
        response = session.post(
            f"http://{address}/v1/transactions",
            json=[sql],
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        body = response.json()
        results = body.get("results") if isinstance(body, dict) else None
        if not isinstance(results, list) or len(results) != 1:
            raise ValueError("unexpected transaction response")
        result = results[0]
        count = result.get("rows_affected") if isinstance(result, dict) else None
        if type(count) is not int or not 0 <= count <= ROWS_PER_BATCH:
            raise ValueError("missing or invalid rows_affected")
        return count
    except (requests.RequestException, ValueError) as error:
        print(f"Resurrection transaction failed on {address}: {error}")
        return None


def drive(session, rng, addresses, rounds=DEFAULT_ROUNDS, max_seconds=MAX_SECONDS):
    stats = {
        "rounds_attempted": 0,
        "transactions_attempted": 0,
        "transactions_acknowledged": 0,
        "confirmed_resurrections": 0,
        "updates_acknowledged": 0,
        "updated_rows": 0,
        "cross_node_updates_acknowledged": 0,
        "confirmed_updates_after_resurrection": 0,
        "final_deletes": 0,
    }
    deadline = time.monotonic() + max_seconds

    for _ in range(rounds):
        if time.monotonic() >= deadline:
            break
        stats["rounds_attempted"] += 1
        address = rng.choice(addresses)
        bucket = rng.choice(range(BUCKETS))
        value = rng.choice((0, 1, -1, 127, 128, 32767, 32768))
        insert, delete, update = statements(bucket, value)

        def transaction(sql, target=address):
            stats["transactions_attempted"] += 1
            count = execute(session, target, sql)
            if count is not None:
                stats["transactions_acknowledged"] += 1
            return count

        # Establish the lifecycle with separate commits on the same node.
        if transaction(insert) is None:
            continue
        deleted = transaction(delete)
        if deleted is None:
            continue
        inserted = transaction(insert)
        if inserted is None:
            continue

        # Other driver instances may touch the same batch between requests.
        stats["confirmed_resurrections"] += max(0, deleted + inserted - ROWS_PER_BATCH)
        update_address = rng.choice((address, address, rng.choice(addresses)))
        updated = transaction(update, update_address)
        if updated is None:
            continue
        stats["updates_acknowledged"] += 1
        stats["updated_rows"] += updated
        if update_address != address:
            stats["cross_node_updates_acknowledged"] += 1
        stats["confirmed_updates_after_resurrection"] += max(
            0, deleted + inserted + updated - 2 * ROWS_PER_BATCH
        )
        if rng.choice((True, False)):
            final_deleted = transaction(delete)
            if final_deleted is not None:
                stats["final_deletes"] += final_deleted

    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--addrs", nargs="+", default=DEFAULT_ADDRESSES)
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("--rounds must be positive")

    with requests.Session() as session:
        stats = drive(session, AntithesisRandom(), args.addrs, args.rounds)
    print(f"Resurrection workload: {json.dumps(stats, sort_keys=True)}")
    sometimes(
        stats["confirmed_resurrections"] > 0,
        "Resurrection workload commits delete and reinsert",
        stats,
    )
    sometimes(
        stats["confirmed_updates_after_resurrection"] > 0,
        "Resurrection workload updates reinserted keys",
        stats,
    )


if __name__ == "__main__":
    main()
