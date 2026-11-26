#!/usr/bin/env python3
"""Sample pairs of Corrosion snapshots and compare their reconciled base tables."""

import argparse
import fcntl
import hashlib
import importlib.util
import itertools
import json
import os
import platform
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time
from contextlib import closing
from pathlib import Path

try:
    from antithesis.assertions import always, sometimes
    from antithesis.random import AntithesisRandom
except ModuleNotFoundError:
    if os.environ.get("ANTITHESIS_OUTPUT_DIR"):
        raise
    from random import Random as AntithesisRandom

    def always(condition, message, details):
        print(f"{message}: {condition} {json.dumps(details, sort_keys=True)}", flush=True)

    def sometimes(condition, message, details):
        print(f"{message}: {condition} {json.dumps(details, sort_keys=True)}", flush=True)


DEFAULT_DATABASES = (
    "/var/lib/corrosion1/state.db",
    "/var/lib/corrosion2/state.db",
    "/var/lib/corrosion3/state.db",
)
DEFAULT_ROOT = "/tmp/corrosion-reconciled-snapshots"
SUMMARY = re.compile(
    r": (\d+) changes, (\d+) inserts, (\d+) deletes(?:, \d+ unchanged)?\s*$"
)


def load_reconciler():
    candidates = [
        Path("/opt/antithesis/py-resources/snapshot_reconcile.py"),
        Path(__file__).resolve().parent / "py-resources/snapshot_reconcile.py",
    ]
    path = next((candidate for candidate in candidates if candidate.is_file()), None)
    if path is None:
        raise RuntimeError("snapshot_reconcile.py is not installed")
    spec = importlib.util.spec_from_file_location("snapshot_reconcile", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def default_extension():
    arch = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x86_64"}.get(
        platform.machine()
    )
    if arch is None:
        raise RuntimeError(f"unsupported architecture: {platform.machine()}")
    return f"/opt/antithesis/crsqlite/crsqlite-linux-{arch}.so"


def backup(source, destination):
    uri = Path(source).resolve().as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True, timeout=2)) as src:
        with closing(sqlite3.connect(str(destination), timeout=2)) as dst:
            src.backup(dst, pages=256, sleep=0.05)
            checkpoint = dst.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            if checkpoint and checkpoint[0]:
                raise RuntimeError(f"could not checkpoint snapshot {destination}")


def has_source_local_actor(path):
    with closing(sqlite3.connect(str(path), timeout=2)) as conn:
        return conn.execute(
            "SELECT 1 FROM crsql_site_id WHERE ordinal = 0"
        ).fetchone() is not None


def open_copy(path, extension):
    conn = sqlite3.connect(str(path), timeout=2)
    try:
        conn.enable_load_extension(True)
        conn.execute("SELECT load_extension(?, ?)", (extension, "sqlite3_crsqlite_init"))
        conn.enable_load_extension(False)
        return conn
    except Exception:
        conn.close()
        raise


def schema(conn, tables):
    result = {}
    for table in sorted(tables):
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if row is None or row[0] is None:
            raise ValueError(f"missing table schema: {table}")
        result[table] = row[0]
    return result


def state_details(state):
    return {
        "own_site": state.own_site.hex(),
        "actors": {
            site.hex(): {
                "head": actor.head,
                "gaps": actor.gaps,
                "partials": sorted(actor.partials),
            }
            for site, actor in sorted(state.actors.items())
        },
    }


def stored_config(conn):
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='crsql_master'"
    ).fetchone()
    if not exists:
        return {}
    return {
        key: {"hex": value.hex()} if isinstance(value, bytes) else value
        for key, value in conn.execute(
            "SELECT key,value FROM crsql_master WHERE key LIKE 'config.%' ORDER BY key"
        )
    }


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def diff_tables(first, second, tables):
    differences = {}
    for table in sorted(tables):
        result = subprocess.run(
            ["sqldiff", "--primarykey", "--summary", "--table", table, str(first), str(second)],
            capture_output=True, text=True, check=True,
        )
        match = SUMMARY.search(result.stdout)
        if match is None:
            raise RuntimeError(f"unexpected sqldiff summary for {table}: {result.stdout!r}")
        counts = dict(zip(("changes", "inserts", "deletes"), map(int, match.groups())))
        if any(counts.values()):
            differences[table] = counts
    return differences


def save_artifacts(sample_dir, result, paths, copies):
    archive = sample_dir.with_suffix(".tar")
    try:
        for index, (table, counts) in enumerate(sorted(result["differences"].items())):
            diff_name = f"diff-{index:04d}.sql"
            try:
                with (sample_dir / diff_name).open("w") as output:
                    diff = subprocess.run(
                        ["sqldiff", "--primarykey", "--table", table, *(str(p) for p in copies)],
                        stdout=output, stderr=subprocess.PIPE, text=True,
                    )
                counts["diff_file"] = diff_name if diff.returncode == 0 else None
                if diff.returncode:
                    counts["diff_error"] = diff.stderr
            except OSError as error:
                counts["diff_file"] = None
                counts["diff_error"] = str(error)
        result["sha256"] = {path.name: sha256(path) for path in paths}
        result["archive"] = str(archive)
        manifest = sample_dir / "manifest.json"
        manifest.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        with archive.open("wb") as file:
            with tarfile.open(fileobj=file, mode="w") as tar:
                for path in sorted(sample_dir.iterdir()):
                    if path.is_file():
                        tar.add(path, arcname=path.name)
            file.flush()
            os.fsync(file.fileno())
    except (OSError, tarfile.TarError) as error:
        result["archive"] = None
        result["artifact_error"] = str(error)
    return result


def check_pair(databases, sample_dir, extension):
    started = time.monotonic()
    result = {
        "nodes": [str(path) for path in databases],
        "sample_dir": str(sample_dir),
    }
    originals = [sample_dir / f"node-{i}-original.db" for i in range(2)]
    copies = [sample_dir / f"node-{i}-reconciled.db" for i in range(2)]
    reconciler = load_reconciler()
    try:
        for source, target in zip(databases, originals):
            backup(source, target)
        for source, target in zip(originals, copies):
            backup(source, target)
        reidentified = []
        for path in copies:
            if not has_source_local_actor(path):
                # Corrosion backups already have no ordinal-zero local actor.
                # Loading CR-SQLite initializes a fresh one when the copy opens.
                reidentified.append({"ordinal_zero_present": False})
                continue
            conn = open_copy(path, extension)
            try:
                entry = reconciler.reidentify_local_actor(conn)
                entry["ordinal_zero_present"] = True
                reidentified.append(entry)
            finally:
                conn.close()
        result["reidentification"] = reidentified
        conns = []
        try:
            for path in copies:
                conns.append(open_copy(path, extension))
            tables = [reconciler.tracked_tables(conn) for conn in conns]
            if tables[0] != tables[1] or schema(conns[0], tables[0]) != schema(conns[1], tables[1]):
                raise reconciler.Inconclusive("tracked table schemas differ")
            result["stored_config"] = [stored_config(conn) for conn in conns]
            result["before"] = [state_details(reconciler.read_state(conn)) for conn in conns]
            result["reconciliation"] = reconciler.reconcile(*conns)
            result["after"] = [state_details(reconciler.read_state(conn)) for conn in conns]
            for conn in conns:
                checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if checkpoint and checkpoint[0]:
                    raise RuntimeError("could not checkpoint reconciled snapshot")
        finally:
            for conn in conns:
                conn.close()
        differences = diff_tables(*copies, tables[0])
        result["differences"] = differences
        result["status"] = "divergent" if differences else "equal"
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        if differences:
            result["original_snapshots"] = [str(path) for path in originals]
            result["reconciled_snapshots"] = [str(path) for path in copies]
            try:
                with (sample_dir / "outcome.json").open("w") as file:
                    json.dump(result, file, sort_keys=True)
                    file.flush()
                    os.fsync(file.fileno())
            except OSError as error:
                result["artifact_error"] = f"could not save outcome checkpoint: {error}"
            save_artifacts(sample_dir, result, originals + copies, copies)
    except reconciler.Inconclusive as error:
        result.update(status="inconclusive", reason=str(error))
    except (OSError, sqlite3.Error, subprocess.SubprocessError, RuntimeError, ValueError) as error:
        result.update(status="error", reason=str(error))
    result.setdefault("elapsed_seconds", round(time.monotonic() - started, 3))
    return result


def assertion_details(result):
    details = {
        "nodes": result.get("nodes"),
        "status": result.get("status"),
        "sample_dir": result.get("sample_dir"),
        "archive": result.get("archive"),
        "original_snapshots": result.get("original_snapshots"),
        "reconciled_snapshots": result.get("reconciled_snapshots"),
        "differences": {
            table: {
                key: value for key, value in summary.items()
                if key in ("changes", "inserts", "deletes", "diff_file")
            }
            for table, summary in result.get("differences", {}).items()
        },
    }
    if result.get("artifact_error"):
        details["artifact_error"] = result["artifact_error"]
    return details


def run_worker(databases, sample_dir, extension, timeout):
    result_file = sample_dir / "result.json"
    command = [
        sys.executable, str(Path(__file__).resolve()), "--worker",
        "--sample-dir", str(sample_dir), "--result-file", str(result_file),
        "--extension", extension,
        "--db", str(databases[0]), "--db", str(databases[1]),
    ]
    worker = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )

    def stop_worker(signum, _frame):
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.wait()
        raise SystemExit(0)

    previous = {sig: signal.signal(sig, stop_worker) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        def failed_worker(reason):
            try:
                result = json.loads((sample_dir / "outcome.json").read_text())
                if result.get("status") == "divergent":
                    result["archive"] = None
                    result["artifact_error"] = reason
                    return result
            except (OSError, ValueError):
                pass
            return {"status": "error", "reason": reason}

        try:
            stdout, stderr = worker.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid, signal.SIGKILL)
            worker.communicate()
            return failed_worker(f"sample exceeded {timeout}s")
        if worker.returncode:
            return failed_worker(f"worker exited {worker.returncode}: {stderr}")
        try:
            return json.loads(result_file.read_text())
        except (OSError, ValueError):
            return failed_worker(f"invalid worker result: {stdout!r} {stderr}")
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def run(args):
    root = Path(args.artifact_dir)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "check.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("A snapshot comparison is already running", flush=True)
            return
        lock.seek(0)
        try:
            last_sample = float(lock.read())
        except ValueError:
            last_sample = None
        now = time.monotonic()
        if last_sample is not None and 0 <= now - last_sample < args.interval:
            print("Snapshot comparison is cooling down", flush=True)
            return
        rng = AntithesisRandom()
        pairs = tuple(itertools.combinations(args.db, 2))
        databases = rng.choice(pairs)
        try:
            sample_dir = Path(tempfile.mkdtemp(prefix="sample-", dir=root))
        except OSError as error:
            print(f"Could not create snapshot sample: {error}", flush=True)
            return
        result = None
        try:
            started = time.monotonic()
            result = run_worker(databases, sample_dir, args.extension, args.sample_timeout)
            result["total_elapsed_seconds"] = round(time.monotonic() - started, 3)
            result.setdefault("nodes", list(databases))
            result.setdefault("sample_dir", str(sample_dir))
            # Keep the cooldown even after a divergence, so repeated scheduling
            # cannot generate unbounded failure archives.
            try:
                lock.seek(0)
                lock.truncate()
                lock.write(str(time.monotonic()))
                lock.flush()
            except OSError as error:
                print(f"Could not record snapshot cooldown: {error}", flush=True)
            print(json.dumps(result, sort_keys=True), flush=True)
            comparable = result["status"] in ("equal", "divergent")
            sometimes(
                comparable,
                "Reconciled snapshot comparison completes",
                {"nodes": result["nodes"], "status": result["status"]},
            )
            observed_missing = any(
                actor["gaps"] or actor["partials"]
                for state in result.get("before", [])
                for actor in state["actors"].values()
            )
            sometimes(
                observed_missing,
                "Snapshot comparison observes gaps or partial versions",
                {"nodes": result["nodes"], "status": result["status"]},
            )
            if comparable:
                always(
                    result["status"] == "equal",
                    "Reconciled snapshots have consistent tracked tables",
                    assertion_details(result),
                )
        finally:
            if result is None or result.get("status") != "divergent":
                shutil.rmtree(sample_dir)
                sample_dir.with_suffix(".tar").unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", action="append")
    parser.add_argument("--extension", default=os.environ.get("CRSQLITE_EXTENSION"))
    parser.add_argument("--artifact-dir", default=os.environ.get("SNAPSHOT_ARTIFACT_DIR", DEFAULT_ROOT))
    parser.add_argument("--interval", type=float, default=float(os.environ.get("SNAPSHOT_INTERVAL_SECONDS", "300")))
    parser.add_argument("--sample-timeout", type=float, default=float(os.environ.get("SNAPSHOT_TIMEOUT_SECONDS", "300")))
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--sample-dir")
    parser.add_argument("--result-file")
    args = parser.parse_args()
    args.db = args.db or list(DEFAULT_DATABASES)
    if len(args.db) < 2 or len(set(args.db)) != len(args.db):
        parser.error("at least two distinct database paths are required")
    if args.interval < 0 or args.sample_timeout <= 0:
        parser.error("interval must be nonnegative and sample timeout must be positive")
    if not args.extension:
        args.extension = default_extension()
    if args.worker:
        if len(args.db) != 2 or not args.sample_dir:
            parser.error("worker requires two databases and a sample directory")
        result = check_pair(args.db, Path(args.sample_dir), args.extension)
        if args.result_file:
            Path(args.result_file).write_text(json.dumps(result, sort_keys=True))
        else:
            print(json.dumps(result, sort_keys=True))
    else:
        run(args)


if __name__ == "__main__":
    main()
