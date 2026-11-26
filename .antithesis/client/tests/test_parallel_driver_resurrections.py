import importlib.util
import os
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import requests


DRIVER = (
    Path(__file__).resolve().parents[1]
    / "test-templates"
    / "parallel_driver_resurrections.py"
)
with patch.dict(os.environ, {"ANTITHESIS_OUTPUT_DIR": ""}):
    spec = importlib.util.spec_from_file_location("parallel_driver_resurrections", DRIVER)
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)


class Response:
    def __init__(self, body):
        self.body = body

    def raise_for_status(self):
        pass

    def json(self):
        return self.body


class SqliteSession:
    def __init__(self, conn, lost_ack=None):
        self.conn = conn
        self.lost_ack = lost_ack
        self.calls = []

    def post(self, url, json, timeout):
        self.calls.append((url, json, timeout))
        cursor = self.conn.execute(json[0])
        count = cursor.rowcount
        self.conn.commit()
        if len(self.calls) == self.lost_ack:
            raise requests.Timeout("response lost after commit")
        return Response({"results": [{"rows_affected": count}]})


class FixedRandom:
    def __init__(self, leave_deleted=False):
        self.leave_deleted = leave_deleted

    def choice(self, values):
        if values == (True, False):
            return self.leave_deleted
        return values[0]


class ResurrectionDriverTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.execute(
            """CREATE TABLE wide (
                id1 BLOB NOT NULL, id2 TEXT NOT NULL,
                "int" INTEGER NOT NULL DEFAULT 1, "float" REAL NOT NULL DEFAULT 1.0,
                "blob" BLOB, PRIMARY KEY (id1, id2)
            ) WITHOUT ROWID"""
        )

    def test_repeated_cycles_are_separate_commits_and_preserve_other_rows(self):
        self.conn.execute(
            "INSERT INTO wide (id1, id2) VALUES (X'0102', 'resurrection-00')"
        )
        self.conn.commit()
        session = SqliteSession(self.conn)
        stats = driver.drive(session, FixedRandom(), ["node1:8080"], rounds=3)
        self.assertEqual(stats["confirmed_resurrections"], 3 * driver.ROWS_PER_BATCH)
        self.assertEqual(stats["transactions_attempted"], 12)
        self.assertEqual(stats["transactions_acknowledged"], 12)
        self.assertEqual(stats["confirmed_updates_after_resurrection"], 48)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM wide").fetchone()[0], 17)
        self.assertEqual(
            self.conn.execute("SELECT count(*) FROM wide WHERE id1=X'0102'").fetchone()[0],
            1,
        )
        for start in range(0, len(session.calls), 4):
            urls = [call[0] for call in session.calls[start:start + 4]]
            self.assertEqual(len(set(urls)), 1)
            self.assertEqual(session.calls[start][2], driver.REQUEST_TIMEOUT)
            self.assertTrue(session.calls[start][1][0].startswith("INSERT"))
            self.assertTrue(session.calls[start + 1][1][0].startswith("DELETE"))
            self.assertTrue(session.calls[start + 2][1][0].startswith("INSERT"))
            self.assertTrue(session.calls[start + 3][1][0].startswith("UPDATE"))
        self.assertEqual(
            self.conn.execute('SELECT count(*) FROM wide WHERE "int"=1 AND id1 != X\'0102\'').fetchone()[0],
            16,
        )

    def test_some_cycles_leave_tombstones(self):
        session = SqliteSession(self.conn)
        stats = driver.drive(session, FixedRandom(leave_deleted=True), ["node"], rounds=2)
        self.assertEqual(stats["confirmed_resurrections"], 2 * driver.ROWS_PER_BATCH)
        self.assertEqual(stats["confirmed_updates_after_resurrection"], 2 * driver.ROWS_PER_BATCH)
        self.assertEqual(stats["final_deletes"], 2 * driver.ROWS_PER_BATCH)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM wide").fetchone()[0], 0)

    def test_lost_delete_ack_is_not_counted_and_next_cycle_progresses(self):
        session = SqliteSession(self.conn, lost_ack=2)
        stats = driver.drive(session, FixedRandom(), ["node"], rounds=2)
        self.assertEqual(stats["transactions_attempted"], 6)
        self.assertEqual(stats["transactions_acknowledged"], 5)
        self.assertEqual(stats["confirmed_resurrections"], driver.ROWS_PER_BATCH)
        self.assertEqual(stats["confirmed_updates_after_resurrection"], driver.ROWS_PER_BATCH)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM wide").fetchone()[0], 16)

    def test_partial_counts_do_not_overstate_resurrections(self):
        class PartialSession:
            def __init__(self):
                self.counts = iter((16, 3, 2, 1))

            def post(self, url, json, timeout):
                return Response({"results": [{"rows_affected": next(self.counts)}]})

        stats = driver.drive(PartialSession(), FixedRandom(), ["node"], rounds=1)
        self.assertEqual(stats["confirmed_resurrections"], 0)
        self.assertEqual(stats["confirmed_updates_after_resurrection"], 0)

    def test_reach_claim_reports_acknowledged_cycles(self):
        session = SqliteSession(self.conn)
        context = Mock()
        context.__enter__ = Mock(return_value=session)
        context.__exit__ = Mock(return_value=False)
        with patch.object(driver.requests, "Session", return_value=context), \
             patch.object(driver, "AntithesisRandom", return_value=FixedRandom()), \
             patch.object(driver, "sometimes") as sometimes, \
             patch.object(sys, "argv", ["driver", "--addrs", "node", "--rounds", "1"]):
            driver.main()
        self.assertEqual(sometimes.call_count, 2)
        self.assertTrue(sometimes.call_args_list[0].args[0])
        self.assertTrue(sometimes.call_args_list[1].args[0])
        self.assertEqual(
            sometimes.call_args_list[1].args[2]["confirmed_updates_after_resurrection"], 16
        )

    def test_update_can_use_another_node(self):
        class CrossNodeRandom(FixedRandom):
            def __init__(self):
                super().__init__()
                self.address_picks = 0

            def choice(self, values):
                if values == ["node1", "node2"]:
                    self.address_picks += 1
                    return "node1" if self.address_picks == 1 else "node2"
                if values == ("node1", "node1", "node2"):
                    return "node2"
                return super().choice(values)

        session = SqliteSession(self.conn)
        stats = driver.drive(session, CrossNodeRandom(), ["node1", "node2"], rounds=1)
        self.assertTrue(session.calls[2][0].startswith("http://node1/"))
        self.assertTrue(session.calls[3][0].startswith("http://node2/"))
        self.assertEqual(stats["cross_node_updates_acknowledged"], 1)
        self.assertEqual(stats["confirmed_updates_after_resurrection"], 16)

    def test_lost_update_ack_is_not_counted(self):
        session = SqliteSession(self.conn, lost_ack=4)
        stats = driver.drive(session, FixedRandom(leave_deleted=True), ["node"], rounds=1)
        self.assertEqual(stats["confirmed_resurrections"], 16)
        self.assertEqual(stats["confirmed_updates_after_resurrection"], 0)
        self.assertEqual(stats["updates_acknowledged"], 0)
        self.assertEqual(stats["final_deletes"], 0)

    def test_invalid_responses_are_not_acknowledged(self):
        class InvalidSession:
            def __init__(self, body):
                self.body = body

            def post(self, url, json, timeout):
                return Response(self.body)

        for body in (
            [],
            {"results": []},
            {"results": [{"error": "failed"}]},
            {"results": [{"rows_affected": True}]},
            {"results": [{"rows_affected": driver.ROWS_PER_BATCH + 1}]},
        ):
            with self.subTest(body=body):
                self.assertIsNone(driver.execute(InvalidSession(body), "node", "INSERT"))

    def test_deadline_prevents_unbounded_work(self):
        session = SqliteSession(self.conn)
        stats = driver.drive(session, FixedRandom(), ["node"], rounds=32, max_seconds=0)
        self.assertEqual(stats["rounds_attempted"], 0)
        self.assertEqual(session.calls, [])

    def test_driver_is_executable(self):
        self.assertTrue(os.access(DRIVER, os.X_OK))


if __name__ == "__main__":
    unittest.main()
