import importlib.util
import platform
import random
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / ".antithesis/client/test-templates/py-resources/snapshot_reconcile.py"
spec = importlib.util.spec_from_file_location("snapshot_reconcile", MODULE)
reconcile = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = reconcile
spec.loader.exec_module(reconcile)

ARCH = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(
    platform.machine()
)
OS = "darwin" if sys.platform == "darwin" else "linux"
SUFFIX = "dylib" if OS == "darwin" else "so"
EXTENSION = ROOT / f"crates/corro-types/crsqlite-{OS}-{ARCH}.{SUFFIX}"
COLS = reconcile.CHANGE_COLUMNS


class RangeTests(unittest.TestCase):
    def test_range_subtraction_matches_set_difference(self):
        def expand(pairs):
            return {n for start, end in pairs for n in range(start, end + 1)}

        rng = random.Random(42)
        for _ in range(500):
            ranges = reconcile.merge_ranges(
                [(start, start + rng.randrange(8)) for start in rng.sample(range(50), 10)]
            )
            removed = reconcile.merge_ranges(
                [(start, start + rng.randrange(8)) for start in rng.sample(range(50), 10)]
            )
            self.assertEqual(
                expand(reconcile.subtract_ranges(ranges, removed)),
                expand(ranges) - expand(removed),
            )


@unittest.skipUnless(ARCH and EXTENSION.exists(), "bundled CR-SQLite is unavailable")
class SnapshotReconcileTests(unittest.TestCase):
    dual_write = True
    legacy_buffer = False

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.connections = []
        self.addCleanup(self.close_connections)
        self.a = self.new_db("a")
        self.b = self.new_db("b")

    def close_connections(self):
        for conn in self.connections:
            conn.close()

    def new_db(self, name):
        conn = sqlite3.connect(str(Path(self.tmp.name) / f"{name}.db"))
        self.connections.append(conn)
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension(?, ?)", (str(EXTENSION), "sqlite3_crsqlite_init"))
        try:
            conn.execute("SELECT crsql_config_set('default-ts', 1)")
        except sqlite3.OperationalError as error:
            if "Unknown setting name" not in str(error):
                raise
            conn.execute("SELECT crsql_set_ts(1)")
        conn.execute(
            "CREATE TABLE wide (id BLOB NOT NULL, k TEXT NOT NULL, "
            "val TEXT NOT NULL DEFAULT '', PRIMARY KEY(id,k))"
        )
        conn.execute("SELECT crsql_as_crr('wide')")
        if self.dual_write:
            try:
                conn.execute("SELECT crsql_config_set('metadata-write-version', 2)")
            except sqlite3.OperationalError as error:
                if "Unknown setting name" not in str(error):
                    raise
                self.skipTest("extension does not support dual-write metadata")
            conn.execute("SELECT crsql_incremental_maintenance(1000)")
        conn.executescript("""
            CREATE TABLE __corro_bookkeeping_gaps (
                actor_id BLOB NOT NULL, start INTEGER NOT NULL, end INTEGER NOT NULL,
                PRIMARY KEY (actor_id, start)
            ) WITHOUT ROWID;
            CREATE TABLE __corro_seq_bookkeeping (
                site_id BLOB NOT NULL, db_version INTEGER NOT NULL,
                start_seq INTEGER NOT NULL, end_seq INTEGER NOT NULL, last_seq INTEGER NOT NULL,
                ts TEXT NOT NULL, PRIMARY KEY (site_id,db_version,start_seq)
            ) WITHOUT ROWID;
        """)
        seq_bounds = "" if self.legacy_buffer else ",min_seq,max_seq"
        conn.execute(
            'CREATE TABLE __corro_buffered_changes ('
            '"table",pk,cid,val,col_version,db_version,site_id,cl,seq,ts'
            f'{seq_bounds},PRIMARY KEY (site_id,db_version,seq)) WITHOUT ROWID'
        )
        return conn

    def actor(self, conn):
        return conn.execute("SELECT crsql_site_id()").fetchone()[0]

    def insert(self, conn, key, value):
        conn.execute("INSERT INTO wide VALUES (?, ?, ?)", (key.encode(), key, value))
        conn.commit()

    def feed(self, conn, site, version):
        return conn.execute(
            f"SELECT {COLS} FROM crsql_changes WHERE site_id=? AND db_version=? ORDER BY seq",
            (site, version),
        ).fetchall()

    def replay(self, conn, rows):
        conn.executemany(
            f"INSERT INTO crsql_changes ({COLS}) VALUES (?,?,?,?,?,?,?,?,?,?)", rows
        )
        conn.commit()

    def partial(self, conn, site, version, last, seq_ranges, rows):
        for start, end in seq_ranges:
            conn.execute(
                "INSERT INTO __corro_seq_bookkeeping VALUES (?,?,?,?,?,?)",
                (site, version, start, end, last, "1"),
            )
        for row in rows:
            conn.execute(
                f"INSERT INTO __corro_buffered_changes ({COLS}) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                row,
            )
        conn.commit()

    def rows(self, conn):
        return conn.execute("SELECT id,k,val FROM wide ORDER BY id,k").fetchall()

    def test_tracked_tables_include_v1_and_v2(self):
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.executescript("""
            CREATE TABLE sample (id INTEGER PRIMARY KEY);
            CREATE TABLE sample__crsql_clock (key INTEGER);
            CREATE TABLE v2_only (id INTEGER PRIMARY KEY);
            CREATE TABLE v2_only__crsql_v2_pks (id INTEGER);
            CREATE TABLE unrelated (id INTEGER);
            CREATE TABLE orphan__crsql_clock (key INTEGER);
        """)
        self.assertEqual(reconcile.tracked_tables(conn), {"sample", "v2_only"})

    def test_returns_union_of_tracked_tables_for_later_diff(self):
        self.a.execute("CREATE TABLE extra (id INTEGER PRIMARY KEY)")
        self.a.execute("CREATE TABLE extra__crsql_clock (id INTEGER)")
        self.a.commit()
        self.assertEqual(reconcile.reconcile(self.a, self.b)["tables"], ["extra", "wide"])

    def test_v2_read_and_wire_mode(self):
        if not self.dual_write:
            self.skipTest("V2 configuration requires the dual-write extension")
        for conn in (self.a, self.b):
            conn.execute("SELECT crsql_config_set('metadata-use-version', 2)")
            conn.execute("SELECT crsql_config_set('sync-log-version', 2)")
            conn.commit()
        self.insert(self.a, "key", "00042")
        result = reconcile.reconcile(self.a, self.b)
        self.assertGreater(result["rows_replayed"], 0)
        self.assertEqual(self.rows(self.a), self.rows(self.b))

    def test_v2_only_mode(self):
        if not self.dual_write:
            self.skipTest("V2 configuration requires the dual-write extension")
        for conn in (self.a, self.b):
            conn.execute("SELECT crsql_config_set('metadata-use-version', 2)")
            conn.execute("SELECT crsql_config_set('metadata-write-version', 3)")
            conn.execute("SELECT crsql_incremental_maintenance(1000)")
            conn.commit()
            self.assertIsNone(conn.execute(
                "SELECT name FROM sqlite_master WHERE name='wide__crsql_clock'"
            ).fetchone())
        self.insert(self.a, "key", "value")
        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["tables"], ["wide"])
        self.assertEqual(self.rows(self.a), self.rows(self.b))

    def test_reidentify_moves_original_actor_and_clock_ordinals(self):
        self.insert(self.a, "key", "value")
        original = self.actor(self.a)
        original_zero = self.a.execute(
            "SELECT ordinal FROM crsql_site_id WHERE site_id=?", (original,)
        ).fetchone()[0]
        self.assertEqual(original_zero, 0)

        result = reconcile.reidentify_local_actor(self.a)

        self.assertEqual(result["moved_actor"], original.hex())
        moved_ordinal = result["moved_actor_ordinal"]
        self.assertGreater(moved_ordinal, 0)
        self.assertIsNone(self.a.execute(
            "SELECT ordinal FROM crsql_site_id WHERE ordinal=0"
        ).fetchone())
        self.assertEqual(
            self.a.execute("SELECT DISTINCT site_id FROM wide__crsql_clock").fetchall(),
            [(moved_ordinal,)],
        )

    def test_same_actor_snapshots_are_inconclusive(self):
        clone = sqlite3.connect(str(Path(self.tmp.name) / "clone.db"))
        self.connections.append(clone)
        self.a.backup(clone)
        clone.enable_load_extension(True)
        clone.execute("SELECT load_extension(?, ?)", (str(EXTENSION), "sqlite3_crsqlite_init"))
        with self.assertRaisesRegex(reconcile.Inconclusive, "same local actor ID"):
            reconcile.reconcile(self.a, clone)

    def test_gap_replays_missing_complete_versions_and_clears_gap(self):
        site = self.actor(self.a)
        for key in ("one", "two", "three"):
            self.insert(self.a, key, key)
        self.replay(self.b, self.feed(self.a, site, 3))
        self.b.execute("INSERT INTO __corro_bookkeeping_gaps VALUES (?,1,2)", (site,))
        self.b.commit()

        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 2)
        self.assertEqual(self.rows(self.a), self.rows(self.b))
        self.assertEqual(self.b.execute("SELECT count(*) FROM __corro_bookkeeping_gaps").fetchone()[0], 0)

    def test_replays_changes_in_both_directions(self):
        self.insert(self.a, "from-a", "00042")
        self.insert(self.b, "from-b", b"\x00\xff")
        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 2)
        self.assertEqual(self.rows(self.a), self.rows(self.b))

    def test_fully_known_donor_replaces_partial_buffer(self):
        site = self.actor(self.a)
        self.a.executemany("INSERT INTO wide VALUES (?,?,?)", [
            (b"a", "a", "00042"), (b"b", "b", b"\x00\xff"), (b"c", "c", "c")
        ])
        self.a.commit()
        feed = self.feed(self.a, site, 1)
        self.assertEqual([row[8] for row in feed], [0, 1, 2])
        self.partial(self.b, site, 1, 2, [(0, 0)], [feed[0]])

        reconcile.reconcile(self.a, self.b)
        self.assertEqual(self.rows(self.a), self.rows(self.b))
        self.assertEqual(self.b.execute("SELECT val,typeof(val) FROM wide WHERE k='a'").fetchone(), ("00042", "text"))
        self.assertEqual(self.b.execute("SELECT val,typeof(val) FROM wide WHERE k='b'").fetchone(), (b"\x00\xff", "blob"))
        self.assertEqual(self.b.execute("SELECT count(*) FROM __corro_seq_bookkeeping").fetchone()[0], 0)
        self.assertEqual(self.b.execute("SELECT count(*) FROM __corro_buffered_changes").fetchone()[0], 0)

    def test_empty_surviving_version_discards_obsolete_partial(self):
        site = self.actor(self.a)
        self.insert(self.a, "key", "old")
        old_feed = self.feed(self.a, site, 1)
        self.a.execute("UPDATE wide SET val='new' WHERE k='key'")
        self.a.commit()
        self.assertEqual(self.feed(self.a, site, 1), [])
        self.replay(self.b, self.feed(self.a, site, 2))
        self.partial(self.b, site, 1, 0, [(0, 0)], old_feed)

        reconcile.reconcile(self.a, self.b)
        self.assertEqual(self.rows(self.a), self.rows(self.b))
        self.assertEqual(self.b.execute("SELECT val FROM wide").fetchone()[0], "new")
        self.assertEqual(self.b.execute("SELECT count(*) FROM __corro_buffered_changes").fetchone()[0], 0)

    def test_complete_donor_replays_version_after_partial_was_applied(self):
        site = self.actor(self.a)
        self.a.executemany("INSERT INTO wide VALUES (?,?,?)", [
            (b"a", "a", "a"), (b"b", "b", "b"), (b"c", "c", "c")
        ])
        self.a.commit()
        feed = self.feed(self.a, site, 1)
        self.replay(self.b, feed[:1])
        self.partial(self.b, site, 1, 2, [(0, 0)], feed[:1])

        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["rows_replayed"], len(feed))
        self.assertEqual(self.rows(self.a), self.rows(self.b))
        self.assertEqual(self.b.execute("SELECT count(*) FROM __corro_seq_bookkeeping").fetchone()[0], 0)
        self.assertEqual(self.b.execute("SELECT count(*) FROM __corro_buffered_changes").fetchone()[0], 0)

    def test_already_known_versions_are_not_replayed(self):
        site = self.actor(self.a)
        self.insert(self.a, "key", "original")
        self.replay(self.b, self.feed(self.a, site, 1))
        triggers = self.b.execute(
            "SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='wide'"
        ).fetchall()
        self.assertTrue(triggers)
        for name, _ in triggers:
            self.b.execute(f'DROP TRIGGER "{name.replace(chr(34), chr(34) * 2)}"')
        self.b.execute("UPDATE wide SET val='diverged' WHERE k='key'")
        for _, sql in triggers:
            self.b.execute(sql)
        self.b.commit()

        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 0)
        self.assertNotEqual(self.rows(self.a), self.rows(self.b))

    def test_complementary_partial_buffers_are_both_treated_as_missing(self):
        c = self.new_db("c")
        site = self.actor(c)
        c.executemany("INSERT INTO wide VALUES (?,?,?)", [
            (b"a", "a", "a"), (b"b", "b", "b"), (b"c", "c", "c")
        ])
        c.commit()
        feed = self.feed(c, site, 1)
        self.partial(self.a, site, 1, 2, [(0, 0)], feed[:1])
        self.partial(self.b, site, 1, 2, [(1, 2)], feed[1:])

        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 0)
        self.assertEqual(result["shared_missing_versions"], 1)
        self.assertEqual(self.rows(self.a), [])
        self.assertEqual(self.rows(self.b), [])
        for conn in (self.a, self.b):
            self.assertEqual(conn.execute("SELECT count(*) FROM __corro_seq_bookkeeping").fetchone()[0], 1)
            self.assertGreater(conn.execute("SELECT count(*) FROM __corro_buffered_changes").fetchone()[0], 0)

    def test_both_missing_a_sequence_remain_comparable(self):
        c = self.new_db("c")
        site = self.actor(c)
        c.executemany("INSERT INTO wide VALUES (?,?,?)", [
            (b"a", "a", "a"), (b"b", "b", "b"), (b"c", "c", "c")
        ])
        c.commit()
        feed = self.feed(c, site, 1)
        self.partial(self.a, site, 1, 2, [(0, 0)], feed[:1])
        self.partial(self.b, site, 1, 2, [(1, 1)], feed[1:2])
        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 0)
        self.assertEqual(result["shared_missing_versions"], 1)
        self.assertEqual(self.rows(self.a), [])
        self.assertEqual(self.rows(self.b), [])
        for conn in (self.a, self.b):
            self.assertEqual(conn.execute("SELECT count(*) FROM __corro_seq_bookkeeping").fetchone()[0], 1)

    def test_shared_gap_remains_missing_while_other_versions_are_replayed(self):
        source = self.new_db("source")
        site = self.actor(source)
        for key in ("one", "two", "three", "four"):
            self.insert(source, key, key)
        for conn in (self.a, self.b):
            self.replay(conn, self.feed(source, site, 1))
        self.replay(self.a, self.feed(source, site, 3))
        self.replay(self.b, self.feed(source, site, 4))
        self.a.execute("INSERT INTO __corro_bookkeeping_gaps VALUES (?,2,2)", (site,))
        self.b.execute("INSERT INTO __corro_bookkeeping_gaps VALUES (?,2,3)", (site,))
        self.a.commit()
        self.b.commit()

        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 2)
        self.assertEqual(result["shared_missing_versions"], 1)
        self.assertEqual(self.rows(self.a), self.rows(self.b))
        for conn in (self.a, self.b):
            self.assertEqual(
                conn.execute("SELECT start,end FROM __corro_bookkeeping_gaps "
                             "WHERE actor_id=?", (site,)).fetchall(),
                [(2, 2)],
            )

    def test_partial_newer_than_its_own_actor_snapshot_needs_no_replay(self):
        site = self.actor(self.a)
        self.insert(self.a, "one", "one")
        self.replay(self.b, self.feed(self.a, site, 1))
        source = self.new_db("source")
        source_site = self.actor(source)
        self.insert(source, "two", "two")
        row = list(self.feed(source, source_site, 1)[0])
        row[5] = 2
        row[6] = site
        self.partial(self.b, site, 2, 0, [(0, 0)], [row])

        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 0)
        self.assertEqual(result["shared_missing_versions"], 1)
        self.assertEqual(self.rows(self.a), self.rows(self.b))

    def test_reconciles_more_than_2048_empty_versions(self):
        c = self.new_db("c")
        site = self.actor(c)
        self.a.execute("SELECT crsql_set_db_version(?,?)", (site, 2050))
        self.a.commit()
        result = reconcile.reconcile(self.a, self.b)
        self.assertEqual(result["versions_replayed"], 2050)
        self.assertEqual(result["rows_replayed"], 0)
        self.assertEqual(
            self.b.execute("SELECT db_version FROM crsql_db_versions WHERE site_id=?", (site,)).fetchone()[0],
            2050,
        )

    def test_replays_more_than_100000_changes(self):
        self.a.executemany(
            "INSERT INTO wide VALUES (?,?,?)",
            ((str(i).encode(), str(i), "value") for i in range(100001)),
        )
        self.a.commit()
        result = reconcile.reconcile(self.a, self.b)
        self.assertGreater(result["rows_replayed"], 100000)
        self.assertEqual(self.b.execute("SELECT count(*) FROM wide").fetchone()[0], 100001)


class V1SnapshotReconcileTests(SnapshotReconcileTests):
    dual_write = False
    legacy_buffer = True


if __name__ == "__main__":
    unittest.main()
