import os
import shutil
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "test-templates" / "eventually_check_db.sh"
SCHEMAS = SCRIPT.parents[2] / "corrosion" / "schemas"


class EventualConvergenceTests(unittest.TestCase):
    def make_db(self, path, tables):
        with sqlite3.connect(path) as conn:
            for table, metadata in tables:
                quoted = '"' + table.replace('"', '""') + '"'
                clock = '"' + (table + "__crsql_" + metadata).replace('"', '""') + '"'
                conn.execute(f"CREATE TABLE {quoted} (id INTEGER PRIMARY KEY)")
                conn.execute(f"CREATE TABLE {clock} (id INTEGER PRIMARY KEY)")

    def test_wide_divergence_is_detected_and_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first, second = root / "node1.db", root / "node2.db"
            for db in (first, second):
                self.make_db(db, [("wide", "clock")])
            sqldiff = root / "sqldiff"
            sqldiff.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" != --primarykey ] || [ \"$2\" != --summary ] || "
                "[ \"$3\" != --table ]; then exit 2; fi\n"
                "if [ \"$4\" = wide ] && [ ! -f \"$MARKER\" ]; then\n"
                "  touch \"$MARKER\"\n"
                "  echo 'wide: 1 changes, 0 inserts, 0 deletes'\n"
                "else\n"
                "  echo \"$4: 0 changes, 0 inserts, 0 deletes\"\n"
                "fi\n"
            )
            sleep = root / "sleep"
            sleep.write_text("#!/bin/sh\nexit 0\n")
            sqldiff.chmod(0o755)
            sleep.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", MARKER=str(root / "seen"))
            result = subprocess.run(
                ["bash", str(SCRIPT), str(first), str(second)],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Table wide is not consistent", result.stdout)
        self.assertIn("All tables are consistent", result.stdout)

    def test_discovers_v1_and_v2_tables_on_all_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            databases = [root / f"node{i}.db" for i in range(3)]
            for db in databases:
                self.make_db(db, [("new table", "clock"), ("new_v2", "v2_pks")])
                with sqlite3.connect(db) as conn:
                    conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
            sqldiff = root / "sqldiff"
            sqldiff.write_text(
                "#!/bin/sh\n"
                "printf '%s\\n' \"$4\" >> \"$MARKER\"\n"
                "echo \"$4: 0 changes, 0 inserts, 0 deletes\"\n"
            )
            sqldiff.chmod(0o755)
            marker = root / "seen"
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}", MARKER=str(marker))
            result = subprocess.run(
                ["bash", str(SCRIPT), *(str(db) for db in databases)],
                env=env, capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertCountEqual(marker.read_text().splitlines(),
                                  ["new table", "new_v2", "new table", "new_v2"])

    def test_missing_tracked_table_is_not_treated_as_consistent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first, second = root / "node1.db", root / "node2.db"
            self.make_db(first, [("shared", "clock"), ("missing", "v2_pks")])
            self.make_db(second, [("shared", "clock")])
            sleep = root / "sleep"
            sleep.write_text("#!/bin/sh\nexit 0\n")
            sleep.chmod(0o755)
            env = dict(os.environ, PATH=f"{root}:{os.environ['PATH']}")
            result = subprocess.run(
                ["bash", str(SCRIPT), str(first), str(second)],
                env=env, capture_output=True, text=True, timeout=20,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Tracked tables differ", result.stdout)
        self.assertIn("missing", result.stdout)

    @unittest.skipUnless(shutil.which("sqldiff"), "sqldiff is not installed")
    def test_same_composite_keys_with_different_rowids_converge(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = root / "first.db"
            second = root / "second.db"
            rows = [(b"a", "one", 1), (b"b", "two", 2)]
            for db, data in ((first, rows), (second, list(reversed(rows)))):
                with sqlite3.connect(db) as conn:
                    for schema in ("todo.sql", "team.sql", "deploy.sql", "consul.sql"):
                        conn.executescript((SCHEMAS / schema).read_text())
                    conn.execute("CREATE TABLE wide__crsql_clock (id INTEGER PRIMARY KEY)")
                    conn.executemany('INSERT INTO wide (id1, id2, "int") VALUES (?, ?, ?)', data)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            sleep = fake_bin / "sleep"
            sleep.write_text("#!/bin/sh\nexit 0\n")
            sleep.chmod(0o755)
            env = dict(os.environ, PATH=f"{fake_bin}:{os.environ['PATH']}")
            result = subprocess.run(
                ["bash", str(SCRIPT), str(first), str(second)],
                env=env,
                capture_output=True,
                text=True,
                timeout=20,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("All tables are consistent", result.stdout)


if __name__ == "__main__":
    unittest.main()
