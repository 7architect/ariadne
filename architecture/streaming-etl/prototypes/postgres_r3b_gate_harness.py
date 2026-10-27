"""Archive executable R3-B candidate evidence without awarding readiness."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import subprocess
import sys
import unittest
from datetime import UTC, datetime
from pathlib import Path

import asyncpg

HERE = Path(__file__).resolve().parent
DSN = os.environ.get(
    "ARIADNE_STREAM_DSN", "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming"
)
CRASH_POINTS = [
    "step_before_commit",
    "step_after_commit",
    "terminal_observed",
    "terminal_before_commit",
    "terminal_after_commit",
    "alignment_after_snapshot",
    "restore_begin_before_commit",
    "restore_begin_after_commit",
    "replay_turn_before_commit",
    "replay_turn_after_commit",
    "restore_ready_before_commit",
    "restore_ready_after_commit",
]


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--expected-tests", type=int)
    a = p.parse_args()
    modules = sorted(p.name for p in HERE.glob("test_postgres_*.py"))

    def source_hashes():
        sources = {*HERE.glob("postgres_*.py"), *(HERE / m for m in modules)}
        return {
            p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(sources)
        }

    initial_hashes = source_hashes()
    expected_tests = (
        a.expected_tests
        if a.expected_tests is not None
        else sum(
            unittest.defaultTestLoader.loadTestsFromName(
                Path(name).stem
            ).countTestCases()
            for name in modules
        )
    )
    cmd = [sys.executable, "-m", "unittest", "-v", *modules]
    run = await asyncio.to_thread(
        subprocess.run, cmd, cwd=HERE, capture_output=True, text=True, check=False
    )
    output = run.stdout + run.stderr
    measurements = [
        json.loads(match[1])
        for match in re.finditer(r"R3B_MEASUREMENT_JSON=(\{[^\n]+\})", output)
    ]
    count = re.search(r"Ran (\d+) tests?", output)
    count = int(count[1]) if count else 0
    conn = await asyncpg.connect(DSN)
    try:
        profile = {
            "postgres_version": await conn.fetchval("SHOW server_version"),
            "synchronous_commit": await conn.fetchval("SHOW synchronous_commit"),
            "replicas": [
                dict(x)
                for x in await conn.fetch(
                    "SELECT application_name,state,sync_state FROM pg_stat_replication"
                )
            ],
        }
    finally:
        await conn.close()
    final_hashes = source_hashes()
    source_drift = sorted(
        name
        for name in initial_hashes.keys() | final_hashes.keys()
        if initial_hashes.get(name) != final_hashes.get(name)
    )
    durability_profile_passed = profile["synchronous_commit"] == "remote_apply" and any(
        r["state"] == "streaming" and r["sync_state"] == "sync"
        for r in profile["replicas"]
    )
    passed = (
        run.returncode == 0
        and count == expected_tests
        and "skipped=" not in output
        and not source_drift
        and durability_profile_passed
    )
    report = {
        "phase": "R3-B candidate evidence; Principal gate and ADR criteria govern readiness",
        "generated_at": datetime.now(UTC).isoformat(),
        "command": cmd,
        "test_count": count,
        "expected_tests": expected_tests,
        "test_returncode": run.returncode,
        "passed": passed,
        "join_real_process_crash_boundaries": CRASH_POINTS,
        "regional_real_process_crash_boundaries": [
            "regional_replay_turn_before_commit",
            "regional_replay_turn_after_commit",
            "regional_worker_ready_before_commit",
            "regional_worker_ready_after_commit",
        ],
        "checkpoint_retirement_real_process_crash_boundaries": [
            "checkpoint_archive_planned",
            "checkpoint_metadata_deleted",
            "ordered_log_prefix_deleted",
            "checkpoint_archive_before_commit",
        ],
        "stream_gc_real_process_crash_boundaries": [
            "stream_gc_data_compacted",
            "stream_gc_metadata_compacted",
            "stream_gc_completed",
        ],
        "stream_gc_exception_boundary": "stream_gc_planned",
        "database": profile,
        "durability_profile_passed": durability_profile_passed,
        "source_sha256": initial_hashes,
        "source_drift": source_drift,
        "test_summary": output[
            output.rfind(
                "----------------------------------------------------------------------"
            ) :
        ].strip(),
        "readiness_awarded": False,
        "measurements": measurements,
    }
    if not passed:
        report["failure_output"] = output[-12000:]
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(
        json.dumps(
            {
                k: v
                for k, v in report.items()
                if k
                not in ("command", "source_sha256", "failure_output", "measurements")
            },
            indent=2,
        )
    )
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
