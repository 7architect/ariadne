"""Reproducible R3-A gate for the PostgreSQL checkpoint coordinator."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import asyncpg


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
DEFAULT_DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
SOURCES = (
    "postgres_stream_v2.py",
    "postgres_barrier_coordinator.py",
    "postgres_barrier_oracle.py",
    "postgres_checkpoint_runtime.py",
    "postgres_barrier_crash_worker.py",
    "postgres_barrier_gate_harness.py",
    "test_postgres_barrier_coordinator.py",
)
CRASH_BOUNDARIES = (
    "intent_before_commit",
    "intent_after_commit",
    "barrier_after_partition_lock",
    "barrier_before_commit",
    "barrier_after_commit",
    "participant_before_commit",
    "participant_after_commit",
    "complete_after_validation",
    "complete_before_commit",
    "complete_after_commit",
    "recovery_plan_before_commit",
    "recovery_plan_after_commit",
    "recovery_ready_before_commit",
    "recovery_ready_after_commit",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def database_profile(dsn: str) -> dict[str, object]:
    conn = await asyncpg.connect(dsn)
    try:
        version = await conn.fetchval("SHOW server_version")
        synchronous_commit = await conn.fetchval("SHOW synchronous_commit")
        synchronous_standby_names = await conn.fetchval(
            "SHOW synchronous_standby_names"
        )
        replicas = [
            dict(row)
            for row in await conn.fetch(
                """SELECT application_name,state,sync_state
                   FROM pg_stat_replication ORDER BY application_name"""
            )
        ]
    finally:
        await conn.close()
    return {
        "server_version": version,
        "synchronous_commit": synchronous_commit,
        "synchronous_standby_names": synchronous_standby_names,
        "replicas": replicas,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    env = dict(os.environ, ARIADNE_STREAM_DSN=args.dsn)
    command = [
        sys.executable,
        "-m",
        "unittest",
        "-v",
        "test_postgres_barrier_coordinator.py",
    ]
    run = await asyncio.to_thread(
        subprocess.run,
        command,
        cwd=HERE,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    test_output = run.stdout + run.stderr
    count_match = re.search(r"Ran (\d+) tests?", test_output)
    test_count = int(count_match.group(1)) if count_match else 0
    profile = await database_profile(args.dsn)
    git_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout.strip()
    passed = (
        run.returncode == 0
        and test_count == 10
        and profile["synchronous_commit"] == "remote_apply"
        and any(row["sync_state"] == "sync" for row in profile["replicas"])
    )
    report = {
        "gate": "R3-A coordinator barriers",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_revision": git_revision,
        "command": command,
        "test_count": test_count,
        "test_returncode": run.returncode,
        "test_cases": [
            line.split(" ... ", 1)[0]
            for line in test_output.splitlines()
            if line.startswith("test_")
        ],
        "real_process_crash_boundaries": list(CRASH_BOUNDARIES),
        "real_process_crash_boundary_count": len(CRASH_BOUNDARIES),
        "independent_oracle": "postgres_barrier_oracle.py imports no runtime module",
        "database": profile,
        "source_sha256": {name: sha256(HERE / name) for name in SOURCES},
        "passed": passed,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
