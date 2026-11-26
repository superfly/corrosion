"""Conservatively reconcile two disposable Corrosion database snapshots."""

import sqlite3
from dataclasses import dataclass, field


CHANGE_COLUMNS = '"table",pk,cid,val,col_version,db_version,site_id,cl,seq,ts'


class Inconclusive(Exception):
    """A snapshot cannot safely be brought to a common coverage point."""


def merge_ranges(ranges):
    merged = []
    for start, end in sorted(ranges):
        if start > end:
            raise Inconclusive("inverted coverage range")
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return merged


def contains(ranges, value):
    return any(start <= value <= end for start, end in ranges)


def subtract_ranges(ranges, removed):
    result = []
    index = 0
    for start, end in ranges:
        cursor = start
        while index < len(removed) and removed[index][1] < cursor:
            index += 1
        pos = index
        while pos < len(removed) and removed[pos][0] <= end:
            first, last = removed[pos]
            if first > cursor:
                result.append((cursor, first - 1))
            cursor = max(cursor, last + 1)
            if cursor > end:
                break
            pos += 1
        if cursor <= end:
            result.append((cursor, end))
        index = pos
    return result


@dataclass
class Actor:
    head: int = 0
    gaps: list = field(default_factory=list)
    partials: set = field(default_factory=set)

    def status(self, version):
        if version in self.partials:
            return "partial"
        if version <= self.head and not contains(self.gaps, version):
            return "full"
        return "missing"

    def needed(self, target_head):
        ranges = list(self.gaps)
        ranges.extend((v, v) for v in self.partials)
        if self.head < target_head:
            ranges.append((self.head + 1, target_head))
        return merge_ranges(ranges)

    def coverage_head(self):
        return max(self.head, max((end for _, end in self.gaps), default=0))


@dataclass
class State:
    own_site: bytes
    actors: dict

    def actor(self, site):
        return self.actors.get(site, Actor())


def read_state(conn):
    own_site = conn.execute("SELECT crsql_site_id()").fetchone()[0]
    if not isinstance(own_site, bytes) or len(own_site) != 16:
        raise Inconclusive("invalid local actor ID")
    actors = {}

    def actor(site):
        if not isinstance(site, bytes) or len(site) != 16:
            raise Inconclusive("invalid actor ID in bookkeeping")
        return actors.setdefault(site, Actor())

    for site, head in conn.execute("SELECT site_id,db_version FROM crsql_db_versions"):
        if not isinstance(head, int) or head < 0:
            raise Inconclusive("invalid version head")
        entry = actor(site)
        entry.head = head
    for site, version in conn.execute(
        "SELECT DISTINCT site_id,db_version FROM __corro_seq_bookkeeping"
    ):
        if not isinstance(version, int) or version < 1:
            raise Inconclusive("invalid partial version")
        entry = actor(site)
        entry.head = max(entry.head, version)
        entry.partials.add(version)
    for site, start, end in conn.execute(
        "SELECT actor_id,start,end FROM __corro_bookkeeping_gaps"
    ):
        if start < 1 or start > end:
            raise Inconclusive("invalid version gap")
        actor(site).gaps.append((start, end))

    for entry in actors.values():
        entry.gaps = merge_ranges(entry.gaps)

    for site, version in conn.execute(
        "SELECT DISTINCT site_id,db_version FROM __corro_buffered_changes"
    ):
        entry = actor(site)
        if entry.status(version) == "missing":
            raise Inconclusive("buffered changes have no coverage metadata")

    return State(own_site, actors)


def tracked_tables(conn):
    tables = {name for (name,) in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )}
    names = {
        name.rsplit("__crsql_", 1)[0]
        for name in tables
        if "__crsql_" in name and name.rsplit("__crsql_", 1)[1]
    }
    tracked = names & tables
    if not tracked:
        raise Inconclusive("no tracked tables found")
    return tracked


def reidentify_local_actor(conn):
    """Move the copied node's actor away from ordinal zero.

    This mirrors Corrosion's backup preparation. Reopening the copy then gives
    it a fresh local actor, allowing versions authored by the original node to
    be replayed as remote versions. Only call this on a disposable copy.
    """
    conn.execute("SELECT crsql_set_ts(1)")
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "DELETE FROM crsql_site_id WHERE ordinal = 0 RETURNING site_id"
        ).fetchone()
        if row is None or not isinstance(row[0], bytes) or len(row[0]) != 16:
            raise Inconclusive("snapshot has no valid local actor ID")
        original_actor = row[0]
        original_ordinal = conn.execute(
            "INSERT INTO crsql_site_id (site_id) VALUES (?) RETURNING ordinal",
            (original_actor,),
        ).fetchone()[0]
        clock_tables = conn.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND ("
            "name LIKE '%__crsql_clock' OR "
            "name LIKE '%__crsql_v2_clock' OR "
            "name LIKE '%__crsql_v2_tombstones')"
        ).fetchall()
        for (table,) in clock_tables:
            quoted = '"' + table.replace('"', '""') + '"'
            conn.execute(
                f"UPDATE {quoted} SET site_id = ? WHERE site_id = 0",
                (original_ordinal,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {
        "moved_actor": original_actor.hex(),
        "moved_actor_ordinal": original_ordinal,
    }


def plan(states):
    sites = set(states[0].actors) | set(states[1].actors)
    shared_missing = {}
    needs = [[], []]
    for site in sorted(sites):
        entries = [state.actor(site) for state in states]
        head = max(entry.coverage_head() for entry in entries)
        missing = [entry.needed(head) for entry in entries]
        # Only replay the difference; the intersection is missing on both.
        shared_missing[site] = subtract_ranges(
            missing[0], subtract_ranges(missing[0], missing[1])
        )
        for target in (0, 1):
            for start, end in subtract_ranges(missing[target], missing[1 - target]):
                if site == states[target].own_site:
                    raise Inconclusive("snapshot needs changes from its own actor")
                needs[target].append((site, start, end))
    return shared_missing, needs


def prepare_replay(conns, states, needs, stage):
    stage.executescript("""
        CREATE TABLE actions (
            target INTEGER, site BLOB, first_version INTEGER, last_version INTEGER,
            PRIMARY KEY (target, site, first_version)
        ) WITHOUT ROWID;
        CREATE TABLE changes (
            target INTEGER, site BLOB, version INTEGER,
            "table", pk, cid, val, col_version, db_version, site_id, cl, seq, ts
        );
        CREATE INDEX changes_version ON changes (target, site, version);
    """)
    versions = 0
    rows = 0
    for target in (0, 1):
        source = 1 - target
        for site, start, end in needs[target]:
            donor = states[source].actor(site)
            if (donor.head < end or
                any(gap_start <= end and gap_end >= start
                    for gap_start, gap_end in donor.gaps) or
                any(start <= version <= end for version in donor.partials)):
                raise Inconclusive("complete donor version is unavailable")
            stage.execute(
                "INSERT INTO actions VALUES (?,?,?,?)", (target, site, start, end)
            )
            feed = conns[source].execute(
                f"SELECT {CHANGE_COLUMNS} FROM crsql_changes "
                "WHERE site_id=? AND db_version BETWEEN ? AND ? ORDER BY db_version,seq",
                (site, start, end),
            )
            inserted = stage.executemany(
                "INSERT INTO changes VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                ((target, site, row[5], *row) for row in feed),
            )
            versions += end - start + 1
            rows += inserted.rowcount
    return versions, rows


def apply_replay(conn, state, shared_missing, stage, target):
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("SELECT crsql_set_ts(1)")
        replay_heads = {}
        actions = stage.execute(
            "SELECT site,first_version,last_version FROM actions WHERE target=? "
            "ORDER BY site,first_version", (target,)
        )
        for site, start, end in actions:
            replay_heads[site] = max(replay_heads.get(site, 0), end)
            rows = stage.execute(
                f"SELECT {CHANGE_COLUMNS} FROM changes "
                "WHERE target=? AND site=? AND version BETWEEN ? AND ? "
                "ORDER BY version,rowid",
                (target, site, start, end),
            )
            conn.executemany(
                f"INSERT INTO crsql_changes ({CHANGE_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?,?)",
                rows,
            )
            conn.execute(
                "DELETE FROM __corro_seq_bookkeeping "
                "WHERE site_id=? AND db_version BETWEEN ? AND ?", (site, start, end),
            )
            conn.execute(
                "DELETE FROM __corro_buffered_changes "
                "WHERE site_id=? AND db_version BETWEEN ? AND ?", (site, start, end),
            )
        for site, missing in shared_missing.items():
            current = conn.execute(
                "SELECT db_version FROM crsql_db_versions WHERE site_id=?", (site,)
            ).fetchone()
            replay_head = replay_heads.get(site, 0)
            if replay_head > (current[0] if current else 0):
                conn.execute("SELECT crsql_set_db_version(?,?)", (site, replay_head))
            coverage_head = max(state.actor(site).coverage_head(), replay_head)
            conn.execute(
                "DELETE FROM __corro_bookkeeping_gaps WHERE actor_id=?", (site,)
            )
            conn.executemany(
                "INSERT INTO __corro_bookkeeping_gaps (actor_id,start,end) VALUES (?,?,?)",
                ((site, start, min(end, coverage_head))
                 for start, end in missing if start <= coverage_head),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def reconcile(conn_a, conn_b):
    """Reconcile two disposable snapshots with CR-SQLite loaded.

    Both connections must be isolated from other writers. A failure can occur
    after one snapshot commits, so discard both copies if reconciliation fails.

    Shared partials are treated as unapplied. The manual admin command for
    processing buffered changes can commit a version in chunks; snapshots taken
    during or after an interrupted admin operation may therefore be incomparable.
    """
    conns = (conn_a, conn_b)
    states = [read_state(conn) for conn in conns]
    if states[0].own_site == states[1].own_site:
        raise Inconclusive("snapshots have the same local actor ID")
    tables = tracked_tables(conn_a) | tracked_tables(conn_b)
    shared_missing, needs = plan(states)
    stage = sqlite3.connect("")
    try:
        versions, rows = prepare_replay(conns, states, needs, stage)
        for target, (conn, state) in enumerate(zip(conns, states)):
            apply_replay(conn, state, shared_missing, stage, target)
    finally:
        stage.close()
    after = [read_state(conn) for conn in conns]
    for site in shared_missing:
        actors = [state.actor(site) for state in after]
        head = max(actor.coverage_head() for actor in actors)
        if actors[0].needed(head) != actors[1].needed(head):
            raise Inconclusive("version coverage does not match after replay")
    return {
        "actors": len(shared_missing),
        "versions_replayed": versions,
        "rows_replayed": rows,
        "shared_missing_versions": sum(
            end - start + 1 for ranges in shared_missing.values() for start, end in ranges
        ),
        "tables": sorted(tables),
    }
