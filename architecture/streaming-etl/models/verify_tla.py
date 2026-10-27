#!/usr/bin/env python3
"""Run the safe TLA+ model and assert both mutation controls fail as expected."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).parent
CASES = (
    ("BarrierAlignment.cfg", 0, "Model checking completed. No error has been found."),
    (
        "BarrierAlignmentUnsafeMixed.cfg",
        12,
        "Invariant SameCheckpointCut is violated.",
    ),
    (
        "BarrierAlignmentUnsafe.cfg",
        12,
        "Invariant FencingSafety is violated.",
    ),
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    reports = []
    jar = os.environ.get("TLA2TOOLS_JAR")
    if jar is None:
        raise SystemExit("set TLA2TOOLS_JAR to the TLC 1.8.0 tla2tools.jar path")
    for config, expected_code, expected_text in CASES:
        result = subprocess.run(
            [
                "java",
                "-cp",
                jar,
                "tlc2.TLC",
                "-cleanup",
                "-config",
                config,
                "BarrierAlignment.tla",
            ],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        if result.returncode != expected_code or expected_text not in result.stdout:
            sys.stderr.write(result.stdout)
            return 1
        reports.append(
            {
                "configuration": config,
                "actual_returncode": result.returncode,
                "expected_returncode": expected_code,
                "expected_result": expected_text,
                "output": result.stdout,
                "passed": True,
            }
        )
        print(f"{config}: expected result observed")
    if args.output:
        sources = [
            ROOT / "BarrierAlignment.tla",
            ROOT / "verify_tla.py",
            *(ROOT / c[0] for c in CASES),
        ]
        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "tool_sha256": hashlib.sha256(Path(jar).read_bytes()).hexdigest(),
            "source_sha256": {
                s.name: hashlib.sha256(s.read_bytes()).hexdigest() for s in sources
            },
            "cases": reports,
            "passed": True,
            "scope": "Bounded safety abstraction; excludes storage, leases, aborts, GC and regional liveness",
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
