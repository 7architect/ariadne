"""Deterministic, read-only reviewer for segmented long-soak evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
PROTOTYPES = HERE.parent
MIN_LONG_SECONDS = 72 * 60 * 60
SOURCE_FILES = ("Dockerfile", "pyproject.toml", "uv.lock", "README.md")
PYTHON_IMAGE = (
    "python@sha256:a0f57064cbdcd6285fa45f51237ab0b35ea8ebaed06a1f5973e89e19dfb86704"
)
POSTGRES_IMAGE = (
    "postgres@sha256:eee6c6e1b0894fa4fe0075eb054af9fd1cf39a35321c2d4110626cfe506411fb"
)
UV_IMAGE = (
    "ghcr.io/astral-sh/uv@sha256:"
    "61d393e44e249f2e4b526b6c7ddcecce245946826e608e11c93ad4f5bba55b21"
)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def rolling(prior: str, payload: bytes) -> str:
    return hashlib.sha256(bytes.fromhex(prior) + payload).hexdigest()


def source_hashes() -> dict[str, str]:
    paths = sorted(HERE.glob("*.py")) + [HERE / name for name in SOURCE_FILES]
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def core_hashes() -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(PROTOTYPES.glob("postgres_*.py"))
    }


def current_runner_identity() -> dict[str, Any]:
    machine = Path("/etc/machine-id")
    return {
        "runner_name": os.environ.get("RUNNER_NAME"),
        "runner_environment": os.environ.get("RUNNER_ENVIRONMENT"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "hostname": socket.gethostname(),
        "machine_id_sha256": (
            hashlib.sha256(machine.read_bytes()).hexdigest()
            if machine.exists()
            else None
        ),
    }


def expected_states(source_completed: int) -> dict[str, dict[str, Any]]:
    once = "0" * 64
    twice = "0" * 64
    for number in range(1, source_completed + 1):
        payload = f"document-{number}".encode()
        once = rolling(once, payload)
        twice = rolling(rolling(twice, payload), payload)
    return {
        "A": {"count": source_completed, "rolling_digest": once},
        "B": {"count": source_completed, "rolling_digest": once},
        "C": {"count": source_completed, "rolling_digest": once},
        "J": {"count": source_completed * 2, "rolling_digest": twice},
        "D": {"count": source_completed * 2, "rolling_digest": twice},
    }


def slope_per_hour(points: list[tuple[float, int]]) -> float:
    if len(points) < 2 or points[-1][0] <= points[0][0]:
        return 0.0
    return (points[-1][1] - points[0][1]) / ((points[-1][0] - points[0][0]) / 3600)


def review(root: Path, run_id: str) -> dict[str, Any]:
    directory = root.resolve() / run_id
    manifest = json.loads((directory / "manifest.json").read_text())
    status = json.loads((directory / "service-status.json").read_text())
    violations: list[str] = []
    if (
        manifest.get("protocol") != "ariadne-long-soak/v1"
        or manifest.get("run_id") != run_id
    ):
        violations.append("manifest_identity")
    if not manifest["smoke"] and (
        manifest.get("target_seconds") != MIN_LONG_SECONDS
        or manifest.get("segment_seconds") != 4 * 60 * 60
        or manifest.get("segment_count") != 18
    ):
        violations.append("long_campaign_shape")
    if digest(manifest["source_sha256"]) != manifest["source_manifest_digest"]:
        violations.append("manifest_source_digest")
    if not manifest["smoke"] and not manifest.get("git_provenance", {}).get(
        "worktree_clean"
    ):
        violations.append("campaign_checkout_was_dirty")
    if manifest.get("dependencies") != {
        "python": "3.12",
        "uv": "0.12.23",
        "asyncpg": "0.30.0",
        "cryptography": "45.0.7",
        "python_image": PYTHON_IMAGE,
        "uv_image": UV_IMAGE,
        "postgres_image": POSTGRES_IMAGE,
    }:
        violations.append("dependency_identity")
    current_hashes = source_hashes()
    if current_hashes != manifest["source_sha256"]:
        violations.append("review_checkout_source_drift")
    if core_hashes() != manifest["core_source_sha256"]:
        violations.append("review_core_source_drift")
    current_identity = current_runner_identity()
    for key, value in current_identity.items():
        if value is not None and manifest["runner_identity"].get(key) != value:
            violations.append("review_runner_identity_drift:" + key)
    if status.get("run_id") != run_id or status.get("phase") == "FAILED":
        violations.append("service_status")

    segment_paths = sorted((directory / "segments").glob("segment-*.json"))
    segments = [json.loads(path.read_text()) for path in segment_paths]
    expected_segment_count = manifest["segment_count"]
    if not manifest["smoke"] and len(segments) != expected_segment_count:
        violations.append("segment_count")
    if manifest["smoke"] and not segments:
        violations.append("smoke_segment_missing")
    previous_digest = None
    previous_end = None
    previous_elapsed = -1.0
    previous_source = -1
    all_samples = []
    injected_process_losses = 0
    observed_loss_segments: list[int] = []
    for index, segment in enumerate(segments):
        if segment.get("segment") != index:
            violations.append(f"segment_index:{index}")
        if segment.get("run_id") != run_id:
            violations.append(f"segment_run:{index}")
        if segment.get("manifest_digest") != digest(manifest):
            violations.append(f"segment_manifest:{index}")
        if segment.get("previous_segment_digest") != previous_digest:
            violations.append(f"segment_chain:{index}")
        if not segment.get("passed"):
            violations.append(f"segment_failed:{index}")
        if segment.get("process_loss_injected"):
            injected_process_losses += 1
            observed_loss_segments.append(index)
        if not manifest["smoke"] and segment.get("duration_seconds", 0) < (
            manifest["segment_seconds"] * 0.98
        ):
            violations.append(f"segment_duration:{index}")
        started = datetime.fromisoformat(segment["started_at"])
        ended = datetime.fromisoformat(segment["ended_at"])
        if previous_end is not None:
            gap = (started - previous_end).total_seconds()
            if gap < 0 or gap > 20 * 60:
                violations.append(f"segment_observation_gap:{index}")
        previous_end = ended
        previous_digest = digest(segment)
        for name, oracle in segment.get("oracles", {}).items():
            if not oracle.get("passed") or oracle.get("returncode") != 0:
                violations.append(f"oracle:{index}:{name}")
        if set(segment.get("oracles", {})) != {"stream", "barrier", "join", "release"}:
            violations.append(f"oracle_set:{index}")
        for sample in segment.get("samples", []):
            elapsed = float(sample["elapsed_seconds"])
            source = int(sample["source_completed"])
            if elapsed < previous_elapsed:
                violations.append(f"elapsed_regression:{index}")
            if source < previous_source:
                violations.append(f"source_cursor_regression:{index}")
            previous_elapsed, previous_source = elapsed, source
            replicas = sample["database"].get("replicas", [])
            if not any(
                row.get("state") == "streaming" and row.get("sync_state") == "sync"
                for row in replicas
            ):
                violations.append(f"sync_replica:{index}")
            if sample["database"].get("synchronous_commit") != "remote_apply":
                violations.append(f"remote_apply:{index}")
            if sample["database"].get("release_session_remaining_seconds", 0) <= 0:
                violations.append(f"release_session_expired:{index}")
            for pressure in sample["pressure"]:
                if (
                    pressure["used_records"] > pressure["max_records"]
                    or pressure["used_bytes"] > pressure["max_bytes"]
                ):
                    violations.append(
                        f"capacity:{index}:{pressure['channel']}/{pressure['partition_id']}"
                    )
            all_samples.append(sample)

    run = status.get("run", {})
    if (
        not manifest["smoke"]
        and observed_loss_segments
        != manifest["fault_schedule"]["process_loss_segments"]
    ):
        violations.append("process_loss_schedule")
    source_completed = int(run.get("source_completed", -1))
    states = status.get("states", {})
    expected = expected_states(max(0, source_completed))
    if states != expected:
        violations.append("managed_state_or_checksum")
    if run.get("expected_d_digest") != expected["D"]["rolling_digest"]:
        violations.append("independent_expected_digest")
    if run.get("last_checkpoint_source", 0) > source_completed:
        violations.append("checkpoint_ahead_of_source")
    if source_completed <= 0:
        violations.append("no_source_progress")
    expected_owner_faults = source_completed // int(
        manifest["fault_schedule"]["owner_loss_every_records"]
    )
    if run.get("owner_faults") != expected_owner_faults:
        violations.append("owner_loss_schedule")
    expected_process_boots = 1 + injected_process_losses
    if run.get("process_boots") != expected_process_boots:
        violations.append("unexpected_service_process_boots")
    max_restart_count = max(
        (int(sample.get("container_restart_count", 0)) for sample in all_samples),
        default=0,
    )
    if max_restart_count != 0:
        violations.append("unexpected_container_restarts")
    elapsed = float(status.get("elapsed_seconds", 0))
    if status.get("phase") == "TARGET_REACHED" and run.get("completed_at") is None:
        violations.append("target_completion_witness")

    physical_points = [
        (float(sample["elapsed_seconds"]), int(sample["metrics"]["primary_data_bytes"]))
        for sample in all_samples
    ]
    schema_points = [
        (
            float(sample["elapsed_seconds"]),
            int(sample["database"]["schema_relation_bytes"]),
        )
        for sample in all_samples
    ]
    rss_points = [
        (float(sample["elapsed_seconds"]), int(sample["metrics"]["service_rss_bytes"]))
        for sample in all_samples
    ]
    latter_physical = physical_points[len(physical_points) // 2 :]
    latter_schema = schema_points[len(schema_points) // 2 :]
    latter_rss = rss_points[len(rss_points) // 2 :]
    physical_slope = slope_per_hour(latter_physical)
    schema_slope = slope_per_hour(latter_schema)
    rss_slope = slope_per_hour(latter_rss)
    max_primary = max((point[1] for point in physical_points), default=0)
    max_schema = max((point[1] for point in schema_points), default=0)
    max_rss = max((point[1] for point in rss_points), default=0)
    # PostgreSQL keeps bounded WAL and relation allocation. These are declared
    # test-profile limits, not general production sizing guarantees.
    if max_primary > 8 * 1024**3:
        violations.append("primary_storage_budget")
    if max_schema > 512 * 1024**2:
        violations.append("schema_storage_budget")
    if max_rss > 768 * 1024**2:
        violations.append("service_rss_budget")
    # A cold smoke observes finite allocations over seconds. Preserve their
    # measured rates, while the long profile enforces sustained-growth limits.
    if not manifest["smoke"]:
        if len(latter_physical) >= 4 and physical_slope > 32 * 1024**2:
            violations.append("unexplained_primary_growth")
        if len(latter_schema) >= 4 and schema_slope > 8 * 1024**2:
            violations.append("unexplained_schema_growth")
        if len(latter_rss) >= 4 and rss_slope > 16 * 1024**2:
            violations.append("unexplained_service_rss_growth")

    smoke_passed = manifest["smoke"] and not violations
    long_soak_passed = (
        not manifest["smoke"]
        and manifest["target_seconds"] >= MIN_LONG_SECONDS
        and elapsed >= MIN_LONG_SECONDS
        and len(segments) == expected_segment_count
        and not violations
    )
    if not manifest["smoke"] and elapsed < MIN_LONG_SECONDS:
        violations.append("elapsed_below_72_hours")
        long_soak_passed = False
    return {
        "protocol": "ariadne-long-soak-review/v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "manifest_digest": digest(manifest),
        "segment_count": len(segments),
        "elapsed_seconds": elapsed,
        "source_completed": source_completed,
        "owner_faults": run.get("owner_faults"),
        "process_boots": run.get("process_boots"),
        "resource_measurements": {
            "max_primary_data_bytes": max_primary,
            "max_schema_relation_bytes": max_schema,
            "latter_half_primary_bytes_per_hour": physical_slope,
            "latter_half_schema_bytes_per_hour": schema_slope,
            "max_service_rss_bytes": max_rss,
            "latter_half_service_rss_bytes_per_hour": rss_slope,
        },
        "resource_policy": {
            "profile": (
                "smoke_absolute_caps"
                if manifest["smoke"]
                else "long_absolute_and_rate_caps"
            ),
            "absolute_caps_enforced": True,
            "growth_rate_caps_enforced": not manifest["smoke"],
            "growth_rates_descriptive": bool(manifest["smoke"]),
            "absolute_caps_bytes": {
                "primary_data": 8 * 1024**3,
                "schema_relation": 512 * 1024**2,
                "service_rss": 768 * 1024**2,
            },
            "growth_rate_caps_bytes_per_hour": {
                "primary_data": 32 * 1024**2,
                "schema_relation": 8 * 1024**2,
                "service_rss": 16 * 1024**2,
            },
        },
        "violations": sorted(set(violations)),
        "smoke_passed": smoke_passed,
        "long_soak_passed": long_soak_passed,
        "actual_prefect_in_72h": False,
        "scope": [
            "real PostgreSQL 17 synchronous remote_apply pair",
            "ordinary Python yield DAG source->A->{B,C}->J->D",
            "periodic barriers and bounded retirement/GC",
            "release-session storage guard",
            "owner loss and externally scheduled service-process loss",
            "actual Prefect integration remains separate short R4-B evidence",
        ],
    }


def markdown(report: dict[str, Any]) -> str:
    verdict = (
        "LONG SOAK PASS"
        if report["long_soak_passed"]
        else "SMOKE PASS"
        if report["smoke_passed"]
        else "FAIL"
    )
    lines = [
        f"# Long-soak review: {verdict}",
        "",
        f"- Run: `{report['run_id']}`",
        f"- Measured elapsed: `{report['elapsed_seconds']:.1f}` seconds",
        f"- Segments: `{report['segment_count']}`",
        f"- Source records: `{report['source_completed']}`",
        f"- Owner faults: `{report['owner_faults']}`",
        f"- Process boots: `{report['process_boots']}`",
        f"- `long_soak_passed`: `{str(report['long_soak_passed']).lower()}`",
        f"- `smoke_passed`: `{str(report['smoke_passed']).lower()}`",
        f"- Resource policy: `{report.get('resource_policy', {}).get('profile', 'unspecified')}`",
        "",
        "## Violations",
        "",
    ]
    lines.extend([f"- `{item}`" for item in report["violations"]] or ["- None"])
    lines.extend(
        [
            "",
            "## Scope",
            "",
            *[f"- {item}" for item in report["scope"]],
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = review(args.root, args.run_id)
    output = args.output or (args.root.resolve() / args.run_id / "review.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    output.with_suffix(".md").write_text(markdown(report))
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0 if report["long_soak_passed"] or report["smoke_passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
