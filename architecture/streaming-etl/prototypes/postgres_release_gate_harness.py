"""Freeze sources and archive a complete PostgreSQL release-gate candidate."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
import unittest
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
from validate_adr_graph import evaluate as evaluate_documentation

HERE = Path(__file__).resolve().parent
DSN = os.environ.get(
    "ARIADNE_STREAM_DSN", "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming"
)


def source_hashes() -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted([*HERE.glob("*.py"), *HERE.glob("*.html")])
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase", choices=("R4-A", "R4-B", "R5-A", "R5-B"), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1200)
    args = parser.parse_args()
    modules = sorted(path.stem for path in HERE.glob("test_postgres_*.py"))
    expected = sum(
        unittest.defaultTestLoader.loadTestsFromName(name).countTestCases()
        for name in modules
    )
    initial = source_hashes()
    command = [sys.executable, "-m", "unittest", "-v", *modules]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    log_path = args.output.with_suffix(".log")
    started = time.monotonic()
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=HERE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    lines = []

    async def collect() -> None:
        with log_path.open("w") as log:
            async for line in process.stdout:
                value = line.decode("utf-8", errors="replace")
                lines.append(value)
                log.write(value)
                log.flush()
        await process.wait()

    timed_out = False
    try:
        await asyncio.wait_for(collect(), timeout=args.timeout)
    except TimeoutError:
        timed_out = True
        process.kill()
        await process.wait()
    output = "".join(lines)
    count_match = re.search(r"Ran (\d+) tests?", output)
    count = 0 if count_match is None else int(count_match[1])
    final = source_hashes()
    documentation = evaluate_documentation()
    drift = sorted(
        name
        for name in initial.keys() | final.keys()
        if initial.get(name) != final.get(name)
    )
    conn = await asyncpg.connect(DSN)
    try:
        profile = {
            "postgres_version": await conn.fetchval("SHOW server_version"),
            "synchronous_commit": await conn.fetchval("SHOW synchronous_commit"),
            "replicas": [
                dict(row)
                for row in await conn.fetch(
                    "SELECT application_name,state,sync_state FROM pg_stat_replication"
                )
            ],
        }
    finally:
        await conn.close()
    durability = profile["synchronous_commit"] == "remote_apply" and any(
        row["state"] == "streaming" and row["sync_state"] == "sync"
        for row in profile["replicas"]
    )
    measurements = [
        json.loads(match[1])
        for match in re.finditer(r"(?:R3B|R4|R5)_MEASUREMENT_JSON=(\{[^\n]+\})", output)
    ]
    passed = (
        process.returncode == 0
        and count == expected
        and not timed_out
        and not drift
        and durability
        and "skipped=" not in output
        and documentation["passed"]
    )
    report = {
        "phase": args.phase,
        "generated_at": datetime.now(UTC).isoformat(),
        "passed": passed,
        "test_returncode": process.returncode,
        "timed_out": timed_out,
        "command": command,
        "test_count": count,
        "expected_tests": expected,
        "duration_seconds": round(time.monotonic() - started, 3),
        "python": sys.version,
        "database": profile,
        "durability_profile_passed": durability,
        "source_sha256": initial,
        "source_drift": drift,
        "measurements": measurements,
        "raw_log": log_path.name,
        "raw_log_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
        "readiness_awarded": False,
        "documentation": documentation,
    }
    if not passed:
        report["failure_output"] = output[-16000:]
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: value
                for key, value in report.items()
                if key
                not in {"command", "source_sha256", "measurements", "failure_output"}
            },
            indent=2,
        )
    )
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
