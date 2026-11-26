import importlib.util
import os
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path


CHECKER = Path(__file__).resolve().parents[1] / "test-templates" / "check_v1_v2_integrity.py"
spec = importlib.util.spec_from_file_location("check_v1_v2_integrity", CHECKER)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


class IntegrityCheckTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)
        self.conn.executescript(
            """
            CREATE TABLE crsql_master (key TEXT PRIMARY KEY, value);
            INSERT INTO crsql_master VALUES
                ('config.metadata-write-version', 2),
                ('v2_pks_sample', 'nh:a:TEXT,b:INTEGER');
            CREATE TABLE sample (a TEXT, b INTEGER, val TEXT, PRIMARY KEY (a, b)) WITHOUT ROWID;
            CREATE TABLE sample__crsql_pks (
                __crsql_key INTEGER PRIMARY KEY, a, b, UNIQUE (a, b)
            );
            CREATE TABLE sample__crsql_clock (
                key INTEGER, col_name TEXT, col_version INTEGER, db_version INTEGER,
                site_id INTEGER, seq INTEGER, ts INTEGER, PRIMARY KEY (key, col_name)
            );
            CREATE TABLE sample__crsql_v2_pks (
                __crsql_key INTEGER PRIMARY KEY, a TEXT, b INTEGER, hashed_pk BLOB,
                cl INTEGER
            );
            CREATE TABLE sample__crsql_v2_tombstones (
                hashed_pk BLOB PRIMARY KEY, cl INTEGER, site_id INTEGER,
                db_version INTEGER, seq INTEGER
            );
            CREATE TABLE sample__crsql_v2_tombstone_pks (
                hashed_pk BLOB PRIMARY KEY, a TEXT, b INTEGER
            );
            CREATE TABLE sample__crsql_v2_col_map (col_id INTEGER, col_name TEXT);
            CREATE TABLE sample__crsql_v2_clock (
                cell_key INTEGER PRIMARY KEY, col_version INTEGER,
                site_id INTEGER, db_version INTEGER, seq INTEGER, ts INTEGER
            );
            INSERT INTO sample VALUES ('first', 1, 'a'), ('revived', 2, 'b');
            INSERT INTO sample__crsql_pks VALUES
                (10, 'first', 1), (20, 'revived', 2), (30, 'deleted', 3);
            INSERT INTO sample__crsql_v2_pks VALUES
                (101, 'first', 1, X'01', 1), (102, 'revived', 2, X'02', 3);
            INSERT INTO sample__crsql_clock VALUES
                (10, 'val', 1, 1, 0, 1, 10),
                (20, '-1', 3, 2, 0, 2, 10),
                (20, 'val', 1, 3, 0, 3, 10),
                (30, '-1', 4, 4, 0, 4, 10);
            INSERT INTO sample__crsql_v2_col_map VALUES (1, 'val');
            INSERT INTO sample__crsql_v2_clock VALUES
                ((101 << 12) | 1, 1, 0, 1, 1, 10),
                ((102 << 12) | 1, 1, 0, 3, 3, 10);
            INSERT INTO sample__crsql_v2_tombstones VALUES (X'03', 4, 9, 99, 99);
            INSERT INTO sample__crsql_v2_tombstone_pks VALUES (X'03', 'deleted', 3);
            """
        )

    def errors(self):
        return checker.check_table(self.conn, "sample")

    def test_matching_composite_keys_and_resurrection(self):
        # Different internal keys and event IDs must not affect PK/CL matching.
        self.assertEqual(self.errors(), [])

    def test_live_cl_with_and_without_sentinel(self):
        self.conn.execute("UPDATE sample__crsql_v2_pks SET cl = cl + 2")
        self.assertIn("sample: live CL mismatches=2", self.errors())

    def test_tombstone_cl_mismatch_with_different_event_ids(self):
        self.conn.execute("UPDATE sample__crsql_v2_tombstones SET cl = 6")
        self.assertIn("sample: tombstone CL mismatches=1", self.errors())

    def test_missing_hashed_tombstone_mapping(self):
        self.conn.execute("DELETE FROM sample__crsql_v2_tombstone_pks")
        errors = self.errors()
        self.assertIn("sample: tombstones missing PK rows=1", errors)
        self.assertNotIn("sample: V2 tombstones missing V1 PK rows=1", errors)

    def test_missing_v1_pk_and_delete_marker(self):
        self.conn.execute("DELETE FROM sample__crsql_pks WHERE __crsql_key=30")
        self.assertIn("sample: V2 tombstones missing V1 PK rows=1", self.errors())
        self.conn.execute("INSERT INTO sample__crsql_pks VALUES (30, 'deleted', 3)")
        self.conn.execute("DELETE FROM sample__crsql_clock WHERE key=30")
        self.assertIn("sample: V2 tombstones missing V1 delete markers=1", self.errors())
        self.conn.execute(
            "INSERT INTO sample__crsql_clock VALUES (30, '-1', 3, 4, 0, 4, 10)"
        )
        self.assertIn("sample: V2 tombstones missing V1 delete markers=1", self.errors())

    def test_missing_tombstone_mapping_table(self):
        self.conn.execute("DROP TABLE sample__crsql_v2_tombstone_pks")
        self.assertIn("sample: hash mode missing v2_tombstone_pks table", self.errors())

    def test_missing_v1_live_pk(self):
        self.conn.execute("DELETE FROM sample__crsql_pks WHERE __crsql_key=10")
        self.assertIn("sample: live V2 PK rows missing V1 PK rows=1", self.errors())

    def test_failure_backup_uses_the_check_read_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.db"
            setup = sqlite3.connect(source)
            setup.execute("PRAGMA journal_mode=WAL")
            setup.execute("CREATE TABLE item (id TEXT PRIMARY KEY)")
            setup.execute("INSERT INTO item VALUES ('before')")
            setup.commit()
            setup.close()

            reader = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
            try:
                reader.execute("BEGIN")
                self.assertEqual(reader.execute("SELECT count(*) FROM item").fetchone()[0], 1)
                writer = sqlite3.connect(source)
                writer.execute("INSERT INTO item VALUES ('after')")
                writer.commit()
                writer.close()

                destination = root / "evidence.db"
                checker.backup_snapshot(reader, destination)
            finally:
                reader.close()
            evidence = sqlite3.connect(destination)
            try:
                self.assertEqual(
                    evidence.execute("SELECT id FROM item ORDER BY id").fetchall(),
                    [("before",)],
                )
            finally:
                evidence.close()

    def run_checker(self):
        self.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.db"
            target = sqlite3.connect(path)
            try:
                self.conn.backup(target)
            finally:
                target.close()
            env = dict(
                os.environ,
                V1_V2_INTEGRITY_ARTIFACT_DIR=str(Path(directory) / "artifacts"),
            )
            result = subprocess.run(
                [sys.executable, str(CHECKER), str(path)],
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )
        return result

    def test_checker_exits_nonzero_for_cl_mismatch(self):
        self.conn.execute("UPDATE sample__crsql_v2_tombstones SET cl=6")
        result = self.run_checker()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("sample: tombstone CL mismatches=1", result.stdout)

    def test_checker_passes_for_consistent_dual_write(self):
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("V1/V2 integrity check OK", result.stdout)

    def test_checker_skips_non_dual_write_versions(self):
        self.conn.execute("UPDATE sample__crsql_v2_tombstones SET cl=6")
        for version in (1, 3):
            with self.subTest(version=version):
                self.conn.execute(
                    "UPDATE crsql_master SET value=? WHERE key='config.metadata-write-version'",
                    (version,),
                )
                result = self.run_checker()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("V1/V2 integrity check NOT APPLICABLE", result.stdout)

    def test_checker_skips_pending_maintenance_in_same_snapshot(self):
        self.conn.execute("UPDATE sample__crsql_v2_tombstones SET cl=6")
        for key in (
            "migration_v1_to_v2_migration_sample",
            "cleanup_v1_tables_sample",
            "cleanup_v2_tables_sample",
        ):
            with self.subTest(key=key):
                self.conn.execute("INSERT INTO crsql_master VALUES (?, 1)", (key,))
                result = self.run_checker()
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertIn("NOT APPLICABLE (maintenance pending)", result.stdout)
                self.conn.execute("DELETE FROM crsql_master WHERE key=?", (key,))

    def test_skip_hash_integer_keys(self):
        self.conn.executescript(
            """
            CREATE TABLE plain (id INTEGER PRIMARY KEY);
            CREATE TABLE plain__crsql_pks (__crsql_key INTEGER PRIMARY KEY, id);
            CREATE UNIQUE INDEX plain_pk_idx ON plain__crsql_pks(id);
            CREATE TABLE plain__crsql_clock (
                key INTEGER, col_name TEXT, col_version INTEGER,
                PRIMARY KEY (key, col_name)
            );
            CREATE TABLE plain__crsql_v2_pks (__crsql_key INTEGER PRIMARY KEY, id INTEGER, cl INTEGER);
            CREATE TABLE plain__crsql_v2_tombstones (id INTEGER, cl INTEGER);
            INSERT INTO plain VALUES (1);
            INSERT INTO plain__crsql_pks VALUES (10, 1), (20, 2);
            INSERT INTO plain__crsql_v2_pks VALUES (100, 1, 1);
            INSERT INTO plain__crsql_v2_tombstones VALUES (2, 2);
            INSERT INTO plain__crsql_clock VALUES (20, '-1', 2);
            """
        )
        statements = []
        self.conn.set_trace_callback(statements.append)
        self.assertEqual(checker.check_causal_lengths(self.conn, "plain", "ns", ["id"], True), [])
        self.conn.set_trace_callback(None)
        queries = [sql for sql in statements if 'FROM "plain__crsql_v2_' in sql]
        self.assertEqual(len(queries), 2)
        for sql in queries:
            plan = self.conn.execute(f"EXPLAIN QUERY PLAN {sql}").fetchall()
            self.assertTrue(
                any("SEARCH p USING" in row[3] and "plain_pk_idx" in row[3] for row in plan),
                plan,
            )
        self.conn.execute("UPDATE plain__crsql_v2_tombstones SET cl=4")
        self.assertIn(
            "tombstone CL mismatches=1",
            checker.check_causal_lengths(self.conn, "plain", "ns", ["id"], True),
        )

    def test_rowid_backed_v2_pk_fallback(self):
        self.conn.executescript(
            """
            CREATE TABLE rowid_table (id INTEGER PRIMARY KEY);
            CREATE TABLE rowid_table__crsql_pks (__crsql_key INTEGER PRIMARY KEY, id);
            CREATE UNIQUE INDEX rowid_table_pk_idx ON rowid_table__crsql_pks(id);
            CREATE TABLE rowid_table__crsql_clock (
                key INTEGER, col_name TEXT, col_version INTEGER,
                PRIMARY KEY (key, col_name)
            );
            CREATE TABLE rowid_table__crsql_v2_pks (__crsql_key INTEGER PRIMARY KEY, cl INTEGER);
            CREATE TABLE rowid_table__crsql_v2_tombstones (id INTEGER, cl INTEGER);
            INSERT INTO rowid_table VALUES (7);
            INSERT INTO rowid_table__crsql_pks VALUES (99, 7);
            INSERT INTO rowid_table__crsql_v2_pks VALUES (7, 1);
            """
        )
        self.assertEqual(
            checker.check_causal_lengths(self.conn, "rowid_table", "rs", ["id"], False),
            [],
        )
        self.conn.execute("UPDATE rowid_table__crsql_v2_pks SET cl=3")
        self.assertIn(
            "live CL mismatches=1",
            checker.check_causal_lengths(self.conn, "rowid_table", "rs", ["id"], False),
        )

    def test_checker_preserves_exact_failure_evidence(self):
        self.conn.execute("UPDATE sample__crsql_v2_tombstones SET cl=6")
        self.conn.commit()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "state.db"
            target = sqlite3.connect(path)
            try:
                self.conn.backup(target)
            finally:
                target.close()
            artifact_root = root / "artifacts"
            env = dict(os.environ, V1_V2_INTEGRITY_ARTIFACT_DIR=str(artifact_root))
            result = subprocess.run(
                [sys.executable, str(CHECKER), str(path)],
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("evidence snapshot:", result.stdout)
            evidence_dirs = [path for path in artifact_root.glob("evidence-*") if path.is_dir()]
            self.assertEqual(len(evidence_dirs), 1)
            archive = evidence_dirs[0].with_suffix(".tar")
            self.assertTrue(archive.is_file())
            with tarfile.open(archive) as tar:
                self.assertEqual(
                    sorted(tar.getnames()),
                    ["manifest.json", "state.db"],
                )

    def test_checker_reports_operational_errors_as_inconclusive(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "empty.db"
            path.write_bytes(b"")
            result = subprocess.run(
                [sys.executable, str(CHECKER), str(path)],
                capture_output=True,
                text=True,
                check=False,
            )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn("V1/V2 integrity check INCONCLUSIVE", result.stdout)


class IntegrityCommandTests(unittest.TestCase):
    SCRIPT = Path(__file__).resolve().parents[1] / "test-templates" / "anytime_check_v1_v2_integrity.sh"

    def run_command(self, checker_exit):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            status = (
                '{"write_version":2,"use_version":1,"sync_log_version":1,'
                '"migration_complete":true,"cleanup_complete":true}'
            )
            curl = bin_dir / "curl"
            curl.write_text("#!/bin/sh\nprintf '%s\n' '" + status + "'\n")
            timeout = bin_dir / "timeout"
            timeout.write_text(f"#!/bin/sh\nexit {checker_exit}\n")
            curl.chmod(0o755)
            timeout.chmod(0o755)
            config = root / "corrosion1.toml"
            config.write_text("")
            env = dict(
                os.environ,
                PATH=f"{bin_dir}:{os.environ['PATH']}",
                CORROSION_CONFIGS=str(config),
                V1_V2_INTEGRITY_MAX_ATTEMPTS="1",
            )
            return subprocess.run(
                ["bash", str(self.SCRIPT)],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )

    def test_timeout_is_an_inconclusive_attempt(self):
        result = self.run_command(124)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("inconclusive attempt", result.stdout)
        self.assertNotIn("integrity check failed:", result.stdout)

    def test_checker_inconclusive_is_an_inconclusive_attempt(self):
        result = self.run_command(2)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("inconclusive attempt", result.stdout)

    def test_mismatch_still_fails_the_command(self):
        result = self.run_command(1)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("integrity check failed:", result.stdout)


if __name__ == "__main__":
    unittest.main()
