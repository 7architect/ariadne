"""Observe one immutable segment while reporting bounded local progress."""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import signal
import threading
from pathlib import Path
from types import SimpleNamespace

import cli


def preflight(args) -> dict:
    volume = "ariadne-soak-capacity-" + secrets.token_hex(8)
    cli.docker("volume", "create", volume)
    try:
        result = cli.docker(
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--memory",
            "64m",
            "--pids-limit",
            "32",
            "--mount",
            f"type=volume,src={volume},dst=/probe",
            cli.PYTHON_IMAGE,
            "python",
            "-c",
            "import os; s=os.statvfs('/probe'); print(s.f_bavail*s.f_frsize)",
        )
        report = {
            "state_free_bytes": shutil.disk_usage(args.root).free,
            "docker_volume_free_bytes": int(result.stdout.strip()),
            "required_free_bytes": args.required_free_bytes,
        }
        report["passed"] = (
            min(report["state_free_bytes"], report["docker_volume_free_bytes"])
            >= args.required_free_bytes
        )
        print("ARIADNE_SOAK_CAPACITY_JSON=" + json.dumps(report), flush=True)
        if not report["passed"]:
            raise RuntimeError("insufficient free disk for the declared long profile")
        return report
    finally:
        cli.docker("volume", "rm", volume, check=False)


def progress(directory: Path, segment: int) -> dict:
    value = json.loads((directory / "service-status.json").read_text())
    return {
        "segment": segment,
        "elapsed_seconds": float(value["elapsed_seconds"]),
        "source_completed": int(value["run"]["source_completed"]),
        "process_boots": int(value["run"]["process_boots"]),
        "owner_faults": int(value["run"]["owner_faults"]),
    }


def run_segment(args) -> dict:
    directory = cli.state_dir(args.root, args.run_id)
    manifest = json.loads((directory / "manifest.json").read_text())
    args.duration = manifest["segment_seconds"]
    args.poll_seconds = 3 if manifest["smoke"] else 60
    args.inject_process_loss = False
    stopped = threading.Event()

    def heartbeat():
        while not stopped.is_set():
            try:
                receipt = progress(directory, args.segment)
                cli.atomic_json(directory / "arc-progress.json", receipt)
                print("ARIADNE_SOAK_PROGRESS_JSON=" + json.dumps(receipt), flush=True)
            except (OSError, ValueError, KeyError, TypeError):
                print("ARIADNE_SOAK_PROGRESS_UNAVAILABLE", flush=True)
            stopped.wait(60)

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        report = cli.watch(args)
        if not report["passed"]:
            raise RuntimeError("segment independent oracle failed")
        return {
            "segment": args.segment,
            "digest": cli.digest(report),
            "previous_segment_digest": report["previous_segment_digest"],
            "duration_seconds": report["duration_seconds"],
            "process_loss_injected": report["process_loss_injected"],
            "passed": True,
        }
    except BaseException:
        # Preserve only the public whitelist. CI also attempts final export and
        # cleanup, but a hard pod loss can prevent both and fails the campaign.
        try:
            cli.export_public(
                SimpleNamespace(
                    root=args.root, run_id=args.run_id, output=args.public_output
                )
            )
        finally:
            cli.stop(SimpleNamespace(root=args.root, run_id=args.run_id, cleanup=False))
        raise
    finally:
        stopped.set()
        thread.join(timeout=1)


def interrupted(signum, _frame):
    raise SystemExit(128 + signum)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=cli.DEFAULT_ROOT)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--segment", type=int)
    parser.add_argument("--public-output", type=Path)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--required-free-bytes", type=int, default=20 * 1024**3)
    args = parser.parse_args()
    if args.preflight:
        preflight(args)
        return
    if args.segment is None or args.public_output is None:
        parser.error("segment observation requires --segment and --public-output")
    for name in (signal.SIGTERM, signal.SIGINT):
        signal.signal(name, interrupted)
    print(json.dumps(run_segment(args), sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
