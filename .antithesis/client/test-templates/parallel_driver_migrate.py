#!/usr/bin/env -S python3 -u
"""
Parallel driver: incrementally migrate corrosion nodes from V1 to V2 metadata format.

This driver implements the migration sequence described in cr-sqlite PR #26.
The three config knobs can be advanced independently, creating several valid
intermediate states that we want antithesis to explore:

  metadata-write-version:  1 (V1) → 2 (dual-write) → 3 (V2-only)
  metadata-use-version:    1 (V1) → 2 (V2)
  sync-log-version:        1 (V1) → 2 (packed V2 wire)

Full state diagram (write, use, sync):

  (1,1,1) ──v2_db_format──▶ (2,1,1)  [dual write, migration starts]
                                │
                          migration completes (background migrator)
                                │
                          (2,1,1, mig_done)
                                ├── use_v2_metadata ──▶ (2,2,1)  [dual write, use V2, V1 wire]
                                │                          ├── v2_wire_format ──▶ (2,2,2)  [dual write, V2 wire]
                                │                          │                          └── v2_db_only ──▶ (3,2,2) [done]
                                │                          └── v2_db_only ──▶ (3,2,1)  [V2 db, V1 wire]
                                │                                                  └── v2_wire_format ──▶ (3,2,2) [done]
                                └── v2_db_only ──▶ (3,2,1)  [forces use→2]
                                                        └── v2_wire_format ──▶ (3,2,2) [done]

The driver randomly chooses which path to take, so different nodes may be in
different states simultaneously. The existing test properties (convergence,
bookkeeping, etc.) will find bugs while this driver slowly transitions nodes.

Rollback: a node in dual-write (2,*,*) can be rolled back to (1,1,1) via
rollback_v1. This is randomly attempted to test the rollback path.
"""
import argparse
import json
import sys

import requests
from antithesis.assertions import sometimes, always
from antithesis.random import AntithesisRandom

sys.path.append("/opt/antithesis/py-resources")
import helper

random = AntithesisRandom()

# Migration step values sent as JSON body to POST /v1/migrate
STEP_V2_DB_FORMAT = "v2_db_format"
STEP_USE_V2_METADATA = "use_v2_metadata"
STEP_V2_DB_ONLY = "v2_db_only"
STEP_V2_WIRE_FORMAT = "v2_wire_format"
STEP_ROLLBACK_V1 = "rollback_v1"


def get_migrate_status(address):
    """GET /v1/migrate/status for a node. Returns the JSON status dict or None."""
    try:
        resp = requests.get(f"http://{address}/v1/migrate/status", timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"Status check failed for {address}: {resp.status_code} {resp.text}")
            return None
    except Exception as e:
        print(f"Status check error for {address}: {e}")
        return None


def post_migrate_step(address, step):
    """POST /v1/migrate with a step string. Returns (success, response_json)."""
    try:
        resp = requests.post(
            f"http://{address}/v1/migrate",
            headers={"Content-Type": "application/json"},
            data=json.dumps(step),
            timeout=60,
        )
        body = {}
        try:
            body = resp.json()
        except Exception:
            pass
        if resp.status_code == 200:
            sometimes(
                True,
                "Migration step succeeds",
                {"address": address, "step": step, "response": body},
            )
            return True, body
        else:
            # Migration step can fail legitimately (e.g. trying to advance
            # before migration is complete, or invalid transition). We don't
            # want to crash, just log and let the driver retry next tick.
            sometimes(
                True,
                "Migration step can fail during incremental migration",
                {
                    "address": address,
                    "step": step,
                    "status_code": resp.status_code,
                    "response": body,
                },
            )
            return False, body
    except Exception as e:
        print(f"Migration step error for {address} step={step}: {e}")
        return False, {"error": str(e)}


def classify_node(status):
    """Classify a node's migration state based on its status.

    The state is a tuple (write_version, use_version, sync_log_version)
    plus whether migration/cleanup is complete.

    Returns one of:
      "v1"                  — (1,1,1) not started
      "dual_write_migrating" — (2,1,1) dual-write, migration in progress
      "dual_write_done"     — (2,1,1) dual-write, migration+cleanup complete, still use V1
      "dual_write_use_v2"   — (2,2,1) dual-write, using V2 metadata, V1 wire
      "dual_write_v2_wire"  — (2,2,2) dual-write, V2 metadata, V2 wire
      "v2_db_v1_wire"       — (3,2,1) V2-only DB, V2 metadata, V1 wire
      "v2_done"             — (3,2,2) fully migrated
      "unknown"             — unexpected state
    """
    wv = status.get("write_version", 1)
    uv = status.get("use_version", 1)
    slv = status.get("sync_log_version", 1)
    mig_done = status.get("migration_complete", False)
    cleanup_done = status.get("cleanup_complete", False)

    if wv == 1 and uv == 1 and slv == 1:
        return "v1"
    if wv == 2 and uv == 1 and slv == 1:
        if mig_done and cleanup_done:
            return "dual_write_done"
        return "dual_write_migrating"
    if wv == 2 and uv == 2 and slv == 1:
        return "dual_write_use_v2"
    if wv == 2 and uv == 2 and slv == 2:
        return "dual_write_v2_wire"
    if wv == 3 and uv == 2 and slv == 1:
        return "v2_db_v1_wire"
    if wv == 3 and uv == 2 and slv == 2:
        return "v2_done"
    return "unknown"


def main():
    parser = argparse.ArgumentParser(
        description="Incrementally migrate corrosion nodes to V2 metadata format"
    )
    parser.add_argument(
        "--addrs",
        nargs="+",
        help="List of corrosion addresses (e.g., --addrs corrosion1:8080 corrosion2:8080)",
    )
    args = parser.parse_args()
    if args.addrs is None:
        args.addrs = ["corrosion1:8080", "corrosion2:8080", "corrosion3:8080"]

    # Gather status of all nodes
    nodes = {}  # addr -> (status_dict, classification)
    for addr in args.addrs:
        status = get_migrate_status(addr)
        if status is not None:
            nodes[addr] = (status, classify_node(status))

    if not nodes:
        print("No nodes reachable for migration, exiting")
        return

    # Categorize nodes by migration state
    categories = {
        "v1": [],
        "dual_write_migrating": [],
        "dual_write_done": [],
        "dual_write_use_v2": [],
        "dual_write_v2_wire": [],
        "v2_db_v1_wire": [],
        "v2_done": [],
        "unknown": [],
    }
    for addr, (_, cls) in nodes.items():
        categories[cls].append(addr)

    state_summary = {k: len(v) for k, v in categories.items() if v}
    print(f"Migration state: {state_summary}")

    fully_migrated = len(categories["v2_done"])
    total = len(args.addrs)

    always(
        fully_migrated <= total,
        "Number of fully migrated nodes never exceeds total",
    )

    # Priority 1: If any node is still V1, pick a random one and start dual-write
    if categories["v1"]:
        addr = random.choice(categories["v1"])
        print(f"Step: Starting dual-write migration on {addr}")
        success, resp = post_migrate_step(addr, STEP_V2_DB_FORMAT)
        if success:
            print(f"  {addr} now in dual-write mode: {resp}")
        return

    # Priority 2: If any node is in dual-write with migration complete,
    # randomly choose between:
    #   A) Switch to use V2 metadata (while still dual-writing)
    #   B) Go straight to V2-only DB (drops V1 tables)
    #   C) Occasionally roll back to V1 (test rollback path)
    if categories["dual_write_done"]:
        addr = random.choice(categories["dual_write_done"])

        # 20% chance to roll back (test the rollback path)
        roll = random.randint(1, 100)
        if roll <= 20:
            print(f"Step: Rolling back dual-write node {addr} to V1")
            success, resp = post_migrate_step(addr, STEP_ROLLBACK_V1)
            if success:
                print(f"  {addr} rolled back to V1: {resp}")
            return

        # 50% chance: switch to use V2 metadata while still dual-writing
        # 30% chance: go straight to V2-only DB
        if roll <= 70:
            print(f"Step: Switching {addr} to use V2 metadata (still dual-writing)")
            success, resp = post_migrate_step(addr, STEP_USE_V2_METADATA)
            if success:
                print(f"  {addr} now using V2 metadata in dual-write mode: {resp}")
        else:
            print(f"Step: Completing V2-only DB migration on {addr}")
            success, resp = post_migrate_step(addr, STEP_V2_DB_ONLY)
            if success:
                print(f"  {addr} now in V2-only DB mode: {resp}")
        return

    # Priority 3: If any node is in (2,2,1) — dual write, use V2, V1 wire —
    # randomly choose between:
    #   A) Switch to V2 wire format (while still dual-writing)
    #   B) Go to V2-only DB (keeps V1 wire, becomes (3,2,1))
    if categories["dual_write_use_v2"]:
        addr = random.choice(categories["dual_write_use_v2"])
        roll = random.randint(1, 100)

        # 15% chance to roll back
        if roll <= 15:
            print(f"Step: Rolling back dual-write+useV2 node {addr} to V1")
            success, resp = post_migrate_step(addr, STEP_ROLLBACK_V1)
            if success:
                print(f"  {addr} rolled back to V1: {resp}")
            return

        # 55% chance: switch to V2 wire format while still dual-writing
        # 30% chance: go to V2-only DB
        if roll <= 70:
            print(f"Step: Switching {addr} to V2 wire format (still dual-writing)")
            success, resp = post_migrate_step(addr, STEP_V2_WIRE_FORMAT)
            if success:
                print(f"  {addr} now using V2 wire in dual-write mode: {resp}")
        else:
            print(f"Step: Completing V2-only DB migration on {addr} (from useV2)")
            success, resp = post_migrate_step(addr, STEP_V2_DB_ONLY)
            if success:
                print(f"  {addr} now in V2-only DB mode: {resp}")
        return

    # Priority 4: If any node is in (2,2,2) — dual write, V2 wire —
    # only thing left is to go to V2-only DB
    if categories["dual_write_v2_wire"]:
        addr = random.choice(categories["dual_write_v2_wire"])

        # 10% chance to roll back
        roll = random.randint(1, 100)
        if roll <= 10:
            print(f"Step: Rolling back dual-write+V2wire node {addr} to V1")
            success, resp = post_migrate_step(addr, STEP_ROLLBACK_V1)
            if success:
                print(f"  {addr} rolled back to V1: {resp}")
            return

        print(f"Step: Completing V2-only DB migration on {addr} (from V2 wire)")
        success, resp = post_migrate_step(addr, STEP_V2_DB_ONLY)
        if success:
            print(f"  {addr} now fully V2-only: {resp}")
        return

    # Priority 5: If any node is in (3,2,1) — V2-only DB, V1 wire —
    # switch to V2 wire format
    if categories["v2_db_v1_wire"]:
        addr = random.choice(categories["v2_db_v1_wire"])
        print(f"Step: Switching wire format to V2 on {addr}")
        success, resp = post_migrate_step(addr, STEP_V2_WIRE_FORMAT)
        if success:
            print(f"  {addr} now using V2 wire format: {resp}")
        return

    # If we get here, either all nodes are fully migrated, or all are still
    # migrating (dual_write_migrating) and we need to wait for the background
    # migrator to finish.
    if categories["dual_write_migrating"]:
        print(
            f"Waiting for migration to complete on {len(categories['dual_write_migrating'])} node(s)"
        )
        sometimes(
            True,
            "Nodes can be in dual-write migration in progress state",
            {"migrating_count": len(categories["dual_write_migrating"])},
        )
        return

    # All nodes fully migrated
    sometimes(
        fully_migrated == total,
        "All nodes eventually reach V2 wire format",
        {"v2_done_count": fully_migrated, "total": total},
    )
    print(f"All nodes fully migrated to V2! ({fully_migrated}/{total})")


if __name__ == "__main__":
    main()
