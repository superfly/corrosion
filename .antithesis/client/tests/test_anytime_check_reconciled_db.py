import argparse
import fcntl
import importlib.util
import json
import os
import platform
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / ".antithesis/client/test-templates/anytime_check_reconciled_db.py"
spec = importlib.util.spec_from_file_location("anytime_check_reconciled_db", SCRIPT)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)
ARCH = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(
    platform.machine()
)
OS = "darwin" if sys.platform == "darwin" else "linux"
SUFFIX = "dylib" if OS == "darwin" else "so"
EXTENSION = ROOT / f"crates/corro-types/crsqlite-{OS}-{ARCH}.{SUFFIX}"
COLS = '"table",pk,cid,val,col_version,db_version,site_id,cl,seq,ts'


@unittest.skipUnless(ARCH and EXTENSION.exists() and shutil.which("sqldiff"),
                     "bundled CR-SQLite and sqldiff are required")
class SnapshotCheckTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db_paths = [self.root / "node-a.db", self.root / "node-b.db"]
        self.conns = [self.new_db(path) for path in self.db_paths]
        self.addCleanup(self.close_connections)
        self.sample = self.root / "sample"
        self.sample.mkdir()

    def close_connections(self):
        for conn in self.conns:
            conn.close()

    def new_db(self, path):
        conn = sqlite3.connect(path)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension(?, ?)", (str(EXTENSION), "sqlite3_crsqlite_init"))
        try:
            conn.execute("SELECT crsql_config_set('default-ts', 1)")
        except sqlite3.OperationalError as error:
            if "Unknown setting name" not in str(error):
                raise
            conn.execute("SELECT crsql_set_ts(1)")
        conn.execute("CREATE TABLE item (id TEXT PRIMARY KEY NOT NULL, val TEXT NOT NULL DEFAULT '')")
        conn.execute("SELECT crsql_as_crr('item')")
        conn.executescript("""
            CREATE TABLE __corro_bookkeeping_gaps (
                actor_id BLOB NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL,
                PRIMARY KEY (actor_id, start)
            ) WITHOUT ROWID;
            CREATE TABLE __corro_seq_bookkeeping (
                site_id BLOB NOT NULL, db_version INTEGER NOT NULL,
                start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL,
                ts TEXT NOT NULL, PRIMARY KEY (site_id, db_version, start_seq)
            ) WITHOUT ROWID;
            CREATE TABLE __corro_buffered_changes (
                "table",pk,cid,val,col_version,db_version,site_id,cl,seq,ts,
                PRIMARY KEY (site_id, db_version, seq)
            ) WITHOUT ROWID;
        """)
        conn.commit()
        return conn

    def insert_first(self):
        self.conns[0].execute("INSERT INTO item VALUES ('key', 'original')")
        self.conns[0].commit()

    def replicate(self):
        rows = self.conns[0].execute(f"SELECT {COLS} FROM crsql_changes").fetchall()
        self.conns[1].executemany(
            f"INSERT INTO crsql_changes ({COLS}) VALUES (?,?,?,?,?,?,?,?,?,?)", rows
        )
        self.conns[1].commit()

    def diverge_without_new_version(self):
        conn = self.conns[1]
        triggers = conn.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='item'"
        ).fetchall()
        for name, _ in triggers:
            conn.execute('DROP TRIGGER "' + name.replace('"', '""') + '"')
        conn.execute("UPDATE item SET val='diverged' WHERE id='key'")
        for _, sql in triggers:
            conn.execute(sql)
        conn.commit()

    def check(self):
        return checker.check_pair(self.db_paths, self.sample, str(EXTENSION))

    def test_reconciles_missing_version_without_modifying_sources(self):
        self.insert_first()
        result = self.check()
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(result["reconciliation"]["versions_replayed"], 1)
        self.assertEqual(result["differences"], {})
        self.assertEqual(self.conns[0].execute("SELECT val FROM item").fetchall(), [("original",)])
        self.assertEqual(self.conns[1].execute("SELECT val FROM item").fetchall(), [])
        self.assertFalse(list(self.root.glob("*.tar")))

    def test_divergence_keeps_originals_reconciled_copies_and_manifest(self):
        self.insert_first()
        self.replicate()
        self.diverge_without_new_version()
        result = self.check()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reconciliation"]["versions_replayed"], 0)
        self.assertEqual(result["differences"]["item"]["changes"], 1)
        self.assertEqual(result["original_snapshots"], [
            str(self.sample / "node-0-original.db"),
            str(self.sample / "node-1-original.db"),
        ])
        self.assertEqual(result["reconciled_snapshots"], [
            str(self.sample / "node-0-reconciled.db"),
            str(self.sample / "node-1-reconciled.db"),
        ])
        self.assertEqual(result["nodes"], [str(path) for path in self.db_paths])
        archive = Path(result["archive"])
        self.assertTrue(archive.is_file())
        with tarfile.open(archive) as tar:
            names = set(tar.getnames())
            self.assertTrue({
                "node-0-original.db", "node-1-original.db",
                "node-0-reconciled.db", "node-1-reconciled.db",
                "manifest.json", "diff-0000.sql",
            }.issubset(names))
            manifest = json.load(tar.extractfile("manifest.json"))
            self.assertEqual(manifest["differences"], result["differences"])
            self.assertEqual(manifest["original_snapshots"], result["original_snapshots"])
        for path in self.sample.glob("*.db"):
            self.assertEqual(result["sha256"][path.name], checker.sha256(path))
        self.assertEqual(self.conns[1].execute("SELECT val FROM item").fetchone()[0], "diverged")

    def test_archive_failure_does_not_suppress_divergence(self):
        self.insert_first()
        self.replicate()
        self.diverge_without_new_version()
        with patch.object(checker.tarfile, "open", side_effect=OSError("disk full")):
            result = self.check()
        self.assertEqual(result["status"], "divergent", result)
        self.assertIsNone(result["archive"])
        self.assertIn("disk full", result["artifact_error"])
        self.assertTrue((self.sample / "node-0-original.db").exists())
        self.assertTrue((self.sample / "node-1-reconciled.db").exists())

    def test_diff_artifact_failure_does_not_suppress_divergence(self):
        self.insert_first()
        self.replicate()
        self.diverge_without_new_version()
        real_run = subprocess.run

        def run(command, **kwargs):
            if "--summary" not in command:
                raise OSError("could not generate diff")
            return real_run(command, **kwargs)

        with patch.object(checker.subprocess, "run", side_effect=run):
            result = self.check()
        self.assertEqual(result["status"], "divergent", result)
        self.assertIsNone(result["differences"]["item"]["diff_file"])
        self.assertIn("could not generate diff", result["differences"]["item"]["diff_error"])

    def test_differing_schemas_are_inconclusive(self):
        self.conns[1].execute("ALTER TABLE item ADD COLUMN other TEXT")
        self.conns[1].commit()
        result = self.check()
        self.assertEqual(result["status"], "inconclusive", result)
        self.assertIn("schemas differ", result["reason"])
        self.assertFalse(list(self.root.glob("*.tar")))

    def test_missing_database_is_operational_error(self):
        result = checker.check_pair(
            [self.db_paths[0], self.root / "missing.db"], self.sample, str(EXTENSION)
        )
        self.assertEqual(result["status"], "error", result)
        self.assertFalse((self.root / "missing.db").exists())

    def test_shared_partial_is_not_treated_as_divergence(self):
        third = self.new_db(self.root / "node-c.db")
        self.conns.append(third)
        third.executemany("INSERT INTO item VALUES (?,?)", [("one", "1"), ("two", "2")])
        third.commit()
        site = third.execute("SELECT crsql_site_id()").fetchone()[0]
        feed = third.execute(f"SELECT {COLS} FROM crsql_changes ORDER BY seq").fetchall()
        self.assertGreater(len(feed), 1)
        for conn in self.conns[:2]:
            conn.execute(
                "INSERT INTO __corro_seq_bookkeeping VALUES (?,?,?,?,?,?)",
                (site, 1, 0, 0, len(feed) - 1, "1"),
            )
            conn.execute(
                f"INSERT INTO __corro_buffered_changes ({COLS}) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)", feed[0],
            )
            conn.commit()
        result = self.check()
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(result["reconciliation"]["shared_missing_versions"], 1)
        self.assertEqual(result["reconciliation"]["versions_replayed"], 0)
        self.assertTrue(result["before"][0]["actors"][site.hex()]["partials"])

    def test_reidentified_snapshot_syncs_newer_version_from_its_original_actor(self):
        self.insert_first()
        self.replicate()
        earlier = self.root / "earlier-a.db"
        checker.backup(self.db_paths[0], earlier)
        probe = sqlite3.connect(earlier)
        try:
            original_actor = probe.execute(
                "SELECT hex(site_id) FROM crsql_site_id WHERE ordinal=0"
            ).fetchone()[0].lower()
        finally:
            probe.close()
        self.conns[0].execute("UPDATE item SET val='new' WHERE id='key'")
        self.conns[0].commit()
        self.replicate()

        result = checker.check_pair([earlier, self.db_paths[1]], self.sample, str(EXTENSION))
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(result["reconciliation"]["versions_replayed"], 1)
        self.assertEqual(result["reidentification"][0]["moved_actor"], original_actor)
        self.assertNotEqual(result["before"][0]["own_site"], original_actor.lower())

    def test_backup_without_ordinal_zero_does_not_move_the_fresh_actor(self):
        self.insert_first()
        backup_inputs = [self.root / "backup-a.db", self.root / "backup-b.db"]
        for source, target in zip(self.db_paths, backup_inputs):
            checker.backup(source, target)
        for path in backup_inputs:
            conn = checker.open_copy(path, str(EXTENSION))
            try:
                checker.load_reconciler().reidentify_local_actor(conn)
            finally:
                conn.close()
            probe = sqlite3.connect(path)
            try:
                self.assertIsNone(probe.execute(
                    "SELECT ordinal FROM crsql_site_id WHERE ordinal=0"
                ).fetchone())
            finally:
                probe.close()

        result = checker.check_pair(backup_inputs, self.sample, str(EXTENSION))
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(
            result["reidentification"],
            [{"ordinal_zero_present": False}, {"ordinal_zero_present": False}],
        )

    def test_backup_includes_committed_wal_without_modifying_source(self):
        conn = self.conns[0]
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=0")
        self.insert_first()
        self.assertGreater(Path(str(self.db_paths[0]) + "-wal").stat().st_size, 0)
        result = self.check()
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(result["reconciliation"]["versions_replayed"], 1)
        self.assertEqual(conn.execute("SELECT val FROM item").fetchall(), [("original",)])

    def test_worker_subprocess_runs_end_to_end(self):
        self.insert_first()
        result = checker.run_worker(self.db_paths, self.sample, str(EXTENSION), 30)
        self.assertEqual(result["status"], "equal", result)
        self.assertEqual(result["reconciliation"]["versions_replayed"], 1)


class ControllerTests(unittest.TestCase):
    def test_single_flight_skips_overlapping_check(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                artifact_dir=directory, db=["a", "b"], extension="ext",
                sample_timeout=1, interval=0,
            )
            with (Path(directory) / "check.lock").open("a") as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                with patch.object(checker, "run_worker") as worker:
                    checker.run(args)
                    worker.assert_not_called()

    def test_all_three_pairs_are_available_to_random_sampling(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                artifact_dir=directory, db=["a", "b", "c"], extension="ext",
                sample_timeout=1, interval=0,
            )
            rng = Mock()
            rng.choice.side_effect = lambda pairs: pairs[rng.choice.call_count - 1]
            with patch.object(checker, "AntithesisRandom", return_value=rng), \
                 patch.object(checker, "run_worker", return_value={"status": "equal"}) as worker, \
                 patch.object(checker, "always"), patch.object(checker, "sometimes"):
                for _ in range(3):
                    checker.run(args)
            self.assertEqual(
                [call.args[0] for call in worker.call_args_list],
                [("a", "b"), ("a", "c"), ("b", "c")],
            )

    def test_inconclusive_sample_does_not_assert_success(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                artifact_dir=directory, db=["a", "b", "c"], extension="ext",
                sample_timeout=1, interval=0,
            )
            with patch.object(checker, "run_worker", return_value={"status": "inconclusive"}), \
                 patch.object(checker, "always") as always, \
                 patch.object(checker, "sometimes") as sometimes:
                checker.run(args)
            always.assert_not_called()
            self.assertFalse(sometimes.call_args.args[0])
            self.assertEqual(sorted(p.name for p in Path(directory).iterdir()), ["check.lock"])

    def test_cooldown_applies_across_invocations_even_on_divergence(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                artifact_dir=directory, db=["a", "b"], extension="ext",
                sample_timeout=1, interval=300,
            )
            with patch.object(checker, "run_worker", return_value={"status": "divergent"}) as worker, \
                 patch.object(checker, "always"), patch.object(checker, "sometimes"), \
                 patch.object(checker.time, "monotonic", return_value=1000) as clock:
                checker.run(args)
                clock.return_value = 1100
                checker.run(args)
                self.assertEqual(worker.call_count, 1)
                clock.return_value = 1300
                checker.run(args)
                self.assertEqual(worker.call_count, 2)

    def test_failure_is_asserted_after_artifacts_exist(self):
        with tempfile.TemporaryDirectory() as directory:
            args = argparse.Namespace(
                artifact_dir=directory, db=["a", "b", "c"], extension="ext",
                sample_timeout=1, interval=0,
            )

            def worker(_databases, sample, _extension, _timeout):
                archive = sample.with_suffix(".tar")
                archive.write_text("artifact")
                return {"status": "divergent", "archive": str(archive)}

            def assertion(condition, _message, details):
                self.assertFalse(condition)
                self.assertTrue(Path(details["archive"]).exists())

            with patch.object(checker, "run_worker", side_effect=worker), \
                 patch.object(checker, "always", side_effect=assertion) as always, \
                 patch.object(checker, "sometimes"):
                checker.run(args)
            always.assert_called_once()

    def test_timeout_after_divergence_preserves_failed_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            sample = Path(directory)
            (sample / "outcome.json").write_text(json.dumps({"status": "divergent"}))
            process = Mock(pid=123, returncode=None)
            process.communicate.side_effect = [
                subprocess.TimeoutExpired("worker", 1), ("", ""),
            ]
            with patch.object(checker.subprocess, "Popen", return_value=process), \
                 patch.object(checker.os, "killpg") as killpg:
                result = checker.run_worker(["a", "b"], sample, "ext", 1)
            self.assertEqual(result["status"], "divergent")
            self.assertIsNone(result["archive"])
            self.assertIn("exceeded", result["artifact_error"])
            killpg.assert_called_once()

    def test_worker_stdout_does_not_corrupt_result(self):
        with tempfile.TemporaryDirectory() as directory:
            sample = Path(directory)
            (sample / "result.json").write_text(json.dumps({"status": "equal"}))
            process = Mock(pid=123, returncode=0)
            process.communicate.return_value = ("SDK startup output\n", "")
            with patch.object(checker.subprocess, "Popen", return_value=process):
                result = checker.run_worker(["a", "b"], sample, "ext", 1)
            self.assertEqual(result["status"], "equal")


if __name__ == "__main__":
    unittest.main()
