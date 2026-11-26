#!/usr/bin/env python3
"""Validate V1/V2 metadata compatibility while dual-write is active."""

import fcntl
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import time
from pathlib import Path

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


def check_causal_lengths(conn, table, mode, pk_cols, v2_has_pk_cols):
    v1_clock = qi(f"{table}__crsql_clock")
    v1_pks = qi(f"{table}__crsql_pks")
    v2_pks = qi(f"{table}__crsql_v2_pks")
    v2_tomb = qi(f"{table}__crsql_v2_tombstones")
    tomb_pks_name = f"{table}__crsql_v2_tombstone_pks"
    errors = []

    # V1 and V2 internal keys are unrelated. Unary + removes the V2 column's
    # affinity so SQLite can use the V1 PK index for integer PKs as well.
    source = "vp" if v2_has_pk_cols else "b"
    live_base = "" if v2_has_pk_cols else (
        f"LEFT JOIN {qi(table)} b ON b.rowid = vp.__crsql_key"
    )
    live_join = " AND ".join(f"p.{qi(col)} = +{source}.{qi(col)}" for col in pk_cols)
    missing_live_pks, live_cl_mismatches = conn.execute(
        f"""SELECT
              count(CASE WHEN p.__crsql_key IS NULL THEN 1 END),
              count(CASE WHEN p.__crsql_key IS NOT NULL
                         AND vp.cl IS NOT COALESCE(c.col_version, 1) THEN 1 END)
            FROM {v2_pks} vp
            {live_base}
            LEFT JOIN {v1_pks} p ON {live_join}
            LEFT JOIN {v1_clock} c
              ON c.key = p.__crsql_key AND c.col_name = '-1'"""
    ).fetchone()
    if missing_live_pks:
        errors.append(f"live V2 PK rows missing V1 PK rows={missing_live_pks}")
    if live_cl_mismatches:
        errors.append(f"live CL mismatches={live_cl_mismatches}")

    if mode.endswith("h"):
        if not exists(conn, tomb_pks_name):
            # The missing table is reported by the existing structural check.
            return errors
        tomb_mapping = (
            f"LEFT JOIN {qi(tomb_pks_name)} tp ON tp.hashed_pk = t.hashed_pk"
        )
        source, has_mapping = "tp", "tp.hashed_pk IS NOT NULL"
    else:
        tomb_mapping = ""
        source, has_mapping = "t", "1"
    tomb_join = " AND ".join(f"p.{qi(col)} = +{source}.{qi(col)}" for col in pk_cols)
    missing_tomb_pks, missing_markers, tomb_cl_mismatches = conn.execute(
        f"""SELECT
              count(CASE WHEN {has_mapping} AND p.__crsql_key IS NULL THEN 1 END),
              count(CASE WHEN p.__crsql_key IS NOT NULL
                         AND (c.key IS NULL OR c.col_version % 2 != 0) THEN 1 END),
              count(CASE WHEN c.key IS NOT NULL AND c.col_version % 2 = 0
                         AND t.cl IS NOT c.col_version THEN 1 END)
            FROM {v2_tomb} t
            {tomb_mapping}
            LEFT JOIN {v1_pks} p ON {tomb_join}
            LEFT JOIN {v1_clock} c
              ON c.key = p.__crsql_key AND c.col_name = '-1'"""
    ).fetchone()
    if missing_tomb_pks:
        errors.append(f"V2 tombstones missing V1 PK rows={missing_tomb_pks}")
    if missing_markers:
        errors.append(f"V2 tombstones missing V1 delete markers={missing_markers}")
    if tomb_cl_mismatches:
        errors.append(f"tombstone CL mismatches={tomb_cl_mismatches}")
    return errors


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
    if not pk_cols:
        return [f"{table}: empty crsql_master v2_pks metadata"]

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
        f"ON {base_v2_join} WHERE b.{qi(pk_cols[0])} IS NULL"
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

    errors.extend(check_causal_lengths(conn, table, mode, pk_cols, v2_has_pk_cols))

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


def backup_snapshot(conn, destination):
    target = sqlite3.connect(str(destination), timeout=300)
    try:
        # In WAL mode, backing up through this same connection preserves the
        # read transaction's snapshot rather than newer committed writes.
        conn.backup(target)
        checkpoint = target.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint and checkpoint[0]:
            raise RuntimeError("could not checkpoint evidence snapshot")
    finally:
        target.close()


def save_evidence(conn, database, errors):
    root = Path(os.environ.get("V1_V2_INTEGRITY_ARTIFACT_DIR", "/tmp/corrosion-v1v2-integrity"))
    root.mkdir(parents=True, exist_ok=True)
    node = re.sub(r"[^A-Za-z0-9_.-]+", "-", Path(database).parent.name or "database")
    evidence_dir = root / f"evidence-{node}"
    archive = evidence_dir.with_suffix(".tar")

    with (root / "evidence.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        backup_path = evidence_dir / "state.db"
        if not backup_path.exists():
            staging = Path(tempfile.mkdtemp(prefix=f".{node}-", dir=root))
            try:
                backup_snapshot(conn, staging / "state.db")
                manifest = {
                    "source_database": database,
                    "created_at_unix": time.time(),
                    "errors": errors,
                }
                (staging / "manifest.json").write_text(
                    json.dumps(manifest, indent=2, sort_keys=True) + "\n"
                )
                os.rename(staging, evidence_dir)
            except Exception:
                shutil.rmtree(staging, ignore_errors=True)
                raise
        if not archive.exists():
            with archive.open("wb") as file:
                with tarfile.open(fileobj=file, mode="w") as tar:
                    for path in sorted(evidence_dir.iterdir()):
                        if path.is_file():
                            tar.add(path, arcname=path.name)
                file.flush()
                os.fsync(file.fileno())
    return evidence_dir, archive


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} DATABASE", file=sys.stderr)
        return 2
    try:
        conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True, timeout=300)
    except (OSError, sqlite3.Error) as error:
        print(
            f"V1/V2 integrity check INCONCLUSIVE ({sys.argv[1]}): "
            f"could not open database: {error}"
        )
        return 2

    errors = []
    inconclusive = []
    evidence = None
    evidence_error = None
    try:
        try:
            conn.execute("BEGIN")
            config = dict(
                conn.execute(
                    "SELECT key, value FROM crsql_master "
                    "WHERE key IN ('config.metadata-write-version', "
                    "'config.metadata-use-version', 'config.sync-log-version')"
                ).fetchall()
            )
            write_version = int(config.get("config.metadata-write-version", 1))
            use_version = int(config.get("config.metadata-use-version", 1))
            sync_log_version = int(config.get("config.sync-log-version", 1))
            if write_version != 2:
                print(
                    "V1/V2 integrity check NOT APPLICABLE "
                    f"(persisted config write={write_version}, use={use_version}, "
                    f"sync={sync_log_version})"
                )
                return 0

            pending = conn.execute(
                "SELECT count(*) FROM crsql_master "
                "WHERE key LIKE 'migration_v1_to_v2_migration_%' "
                "OR key LIKE 'cleanup_v1_tables_%' OR key LIKE 'cleanup_v2_tables_%'"
            ).fetchone()[0]
            if pending:
                print("V1/V2 integrity check NOT APPLICABLE (maintenance pending)")
                return 0

            clocks = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%__crsql_clock'"
            ).fetchall()
            tables = [name.removesuffix('__crsql_clock') for (name,) in clocks]
            for table in tables:
                try:
                    errors.extend(check_table(conn, table))
                except sqlite3.Error as error:
                    inconclusive.append(f"{table}: checker query failed: {error}")
            if errors:
                try:
                    evidence = save_evidence(conn, sys.argv[1], errors)
                except (OSError, RuntimeError, sqlite3.Error, tarfile.TarError) as error:
                    evidence_error = str(error)
        except (OSError, sqlite3.Error, TypeError, ValueError) as error:
            print(
                f"V1/V2 integrity check INCONCLUSIVE ({sys.argv[1]}): "
                f"checker could not inspect the snapshot: {error}"
            )
            return 2
    finally:
        conn.close()

    if not tables:
        print("V1/V2 integrity check FAILED: no V1 CRR tables found")
        return 1
    if errors:
        print(f"V1/V2 integrity check FAILED ({sys.argv[1]}, {len(tables)} tables)")
        for error in errors:
            print(f"  - {error}")
        if evidence:
            print(f"  evidence snapshot: {evidence[0]}")
            print(f"  evidence archive: {evidence[1]}")
        elif evidence_error:
            print(f"  evidence snapshot error: {evidence_error}")
        if inconclusive:
            print("Some tables could not be inspected; mismatch evidence above is still valid:")
            for error in inconclusive:
                print(f"  - {error}")
        return 1
    if inconclusive:
        print(f"V1/V2 integrity check INCONCLUSIVE ({sys.argv[1]}, {len(tables)} tables)")
        for error in inconclusive:
            print(f"  - {error}")
        return 2
    print(f"V1/V2 integrity check OK ({sys.argv[1]}, {len(tables)} tables)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
