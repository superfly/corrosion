#!/usr/bin/env python3
"""Validate V1/V2 metadata compatibility while dual-write is active."""

import sqlite3
import sys

COL_ID_BITS = 12
COL_ID_MASK = (1 << COL_ID_BITS) - 1


def qi(name):
    return '"' + name.replace('"', '""') + '"'


def exists(conn, name):
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


def pk_info(conn, table):
    row = conn.execute(
        "SELECT value FROM crsql_master WHERE key=?", (f"v2_pks_{table}",)
    ).fetchone()
    if not row:
        return None, []
    mode, columns = row[0].split(":", 1)
    return mode, [part.split(":", 1)[0] for part in columns.split(",")]


def check_table(conn, table):
    v1_clock = f"{table}__crsql_clock"
    v1_pks = f"{table}__crsql_pks"
    v2_clock = f"{table}__crsql_v2_clock"
    v2_pks = f"{table}__crsql_v2_pks"
    v2_tomb = f"{table}__crsql_v2_tombstones"
    v2_tomb_pks = f"{table}__crsql_v2_tombstone_pks"
    v2_map = f"{table}__crsql_v2_col_map"
    required = (v1_clock, v1_pks, v2_clock, v2_pks, v2_tomb)
    missing = [name for name in required if not exists(conn, name)]
    if missing:
        return [f"{table}: missing metadata tables: {', '.join(missing)}"]

    errors = []
    mode, pk_cols = pk_info(conn, table)
    if not mode:
        return [f"{table}: missing crsql_master v2_pks metadata"]

    v1_cols = [row[1] for row in conn.execute(f"PRAGMA table_info({qi(v1_pks)})")]
    v2_cols = [row[1] for row in conn.execute(f"PRAGMA table_info({qi(v2_pks)})")]
    v2_has_pk_cols = all(col in v2_cols for col in pk_cols)
    base_join = " AND ".join(f"b.{qi(col)} = p.{qi(col)}" for col in pk_cols)
    if v2_has_pk_cols:
        v2_join = " AND ".join(f"vp.{qi(col)} = p.{qi(col)}" for col in pk_cols)
        base_v2_join = " AND ".join(f"b.{qi(col)} = vp.{qi(col)}" for col in pk_cols)
    else:
        v2_join = "vp.__crsql_key = b.rowid"
        base_v2_join = "vp.__crsql_key = b.rowid"

    base_count = conn.execute(f"SELECT count(*) FROM {qi(table)}").fetchone()[0]
    v2_pks_count = conn.execute(f"SELECT count(*) FROM {qi(v2_pks)}").fetchone()[0]
    if base_count != v2_pks_count:
        errors.append(f"alive count mismatch: base={base_count}, v2_pks={v2_pks_count}")

    orphan_alive = conn.execute(
        f"SELECT count(*) FROM {qi(v2_pks)} vp LEFT JOIN {qi(table)} b "
        f"ON {base_v2_join} WHERE b.rowid IS NULL"
    ).fetchone()[0]
    if orphan_alive:
        errors.append(f"orphan alive V2 PK rows={orphan_alive}")

    v1_dead = conn.execute(
        f"SELECT count(*) FROM {qi(v1_clock)} WHERE col_name='-1' AND col_version % 2=0"
    ).fetchone()[0]
    v2_dead = conn.execute(f"SELECT count(*) FROM {qi(v2_tomb)}").fetchone()[0]
    if v1_dead != v2_dead:
        errors.append(f"dead count mismatch: v1={v1_dead}, v2={v2_dead}")

    if mode.endswith("h"):
        if not exists(conn, v2_tomb_pks):
            errors.append("hash mode missing v2_tombstone_pks table")
        else:
            tomb_pks = conn.execute(f"SELECT count(*) FROM {qi(v2_tomb_pks)}").fetchone()[0]
            if tomb_pks != v2_dead:
                errors.append(f"tombstone PK count mismatch: v2={tomb_pks}, tombstones={v2_dead}")
            missing_tomb_pks = conn.execute(
                f"SELECT count(*) FROM {qi(v2_tomb)} t LEFT JOIN {qi(v2_tomb_pks)} p "
                "ON t.hashed_pk=p.hashed_pk WHERE p.hashed_pk IS NULL"
            ).fetchone()[0]
            if missing_tomb_pks:
                errors.append(f"tombstones missing PK rows={missing_tomb_pks}")

    v2_clock_count = conn.execute(f"SELECT count(*) FROM {qi(v2_clock)}").fetchone()[0]
    pk_only = not exists(conn, v2_map) or conn.execute(
        f"SELECT count(*) FROM {qi(v2_map)} WHERE col_name != ''"
    ).fetchone()[0] == 0
    v1_filter = "c.col_name='-1'" if pk_only else "c.col_name!='-1'"
    v1_alive_clock_count = conn.execute(
        f"SELECT count(*) FROM {qi(v1_clock)} c JOIN {qi(v1_pks)} p ON c.key=p.__crsql_key "
        f"JOIN {qi(table)} b ON {base_join} WHERE {v1_filter}"
    ).fetchone()[0]
    if v2_clock_count != v1_alive_clock_count:
        errors.append(f"clock count mismatch: v2={v2_clock_count}, v1_alive={v1_alive_clock_count}")

    orphan_clocks = conn.execute(
        f"SELECT count(*) FROM {qi(v2_clock)} c LEFT JOIN {qi(v2_pks)} p "
        f"ON p.__crsql_key=(c.cell_key >> {COL_ID_BITS}) WHERE p.__crsql_key IS NULL"
    ).fetchone()[0]
    if orphan_clocks:
        errors.append(f"orphan V2 clock rows={orphan_clocks}")

    if pk_only:
        mismatch = conn.execute(
            f"""SELECT count(*) FROM {qi(v1_clock)} c
                JOIN {qi(v1_pks)} p ON c.key=p.__crsql_key
                JOIN {qi(table)} b ON {base_join}
                JOIN {qi(v2_pks)} vp ON {v2_join}
                LEFT JOIN {qi(v2_clock)} v
                  ON v.cell_key=((vp.__crsql_key << {COL_ID_BITS}) | 0)
                WHERE c.col_name='-1'
                  AND (v.cell_key IS NULL OR c.col_version != v.col_version
                    OR c.site_id != v.site_id OR c.db_version != v.db_version
                    OR c.seq != v.seq
                    OR CAST(c.ts AS INTEGER) != v.ts)"""
        ).fetchone()[0]
    else:
        mismatch = conn.execute(
            f"""SELECT count(*) FROM {qi(v1_clock)} c
                JOIN {qi(v1_pks)} p ON c.key=p.__crsql_key
                JOIN {qi(table)} b ON {base_join}
                JOIN {qi(v2_map)} m ON c.col_name=m.col_name
                JOIN {qi(v2_pks)} vp ON {v2_join}
                LEFT JOIN {qi(v2_clock)} v
                  ON v.cell_key=((vp.__crsql_key << {COL_ID_BITS}) | m.col_id)
                WHERE c.col_name!='-1'
                  AND (v.cell_key IS NULL OR c.col_version != v.col_version
                    OR c.site_id != v.site_id OR c.db_version != v.db_version
                    OR c.seq != v.seq
                    OR CAST(c.ts AS INTEGER) != v.ts)"""
        ).fetchone()[0]
    if mismatch:
        errors.append(f"clock value mismatches={mismatch}")

    return [f"{table}: {error}" for error in errors]


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} DATABASE", file=sys.stderr)
        return 2
    conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=300)
    try:
        clocks = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%__crsql_clock'"
        ).fetchall()
        tables = [name.removesuffix('__crsql_clock') for (name,) in clocks]
        errors = [error for table in tables for error in check_table(conn, table)]
    finally:
        conn.close()

    if not tables:
        print("V1/V2 integrity check FAILED: no V1 CRR tables found")
        return 1
    if errors:
        print("V1/V2 integrity check FAILED")
        for error in errors:
            print(f"  - {error}")
        return 1
    print(f"V1/V2 integrity check OK ({len(tables)} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
