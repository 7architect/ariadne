"""Lifecycle CLI for the persistent, segmented long-soak service.

The detached Docker deployment survives individual CI jobs.  Eighteen
sequential four-hour ``watch`` invocations refresh CI credentials and publish
small evidence segments without resetting PostgreSQL or the ETL service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
PROTOTYPES = HERE.parent
DEFAULT_ROOT = Path(os.environ.get("ARIADNE_SOAK_ROOT", "/var/tmp/ariadne-soak"))
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


class DockerCommandError(RuntimeError):
    """A Docker boundary failure whose message never contains raw credentials."""


def redact_sensitive(value: str) -> str:
    value = re.sub(
        r"(?i)\b([A-Z0-9_]*(?:PASSWORD|TOKEN|SECRET|DSN)[A-Z0-9_]*)=[^\s'\";]+",
        r"\1=<redacted>",
        value,
    )
    value = re.sub(r"(?i)(\bPASSWORD\s+)'[^']*'", r"\1 '<redacted>'", value)
    return re.sub(
        r"(?i)(postgres(?:ql)?://[^:\s]+:)[^@\s]+(@)",
        r"\1<redacted>\2",
        value,
    )


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def atomic_json(path: Path, value: Any, *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    os.chmod(temporary, mode)
    temporary.replace(path)


def checked_run_id(value: str) -> str:
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{2,39}", value):
        raise ValueError("run ID must be 3-40 lowercase alphanumeric/dash characters")
    return value


def docker(*args: str, check: bool = True, timeout: float = 120):
    env = os.environ.copy()
    for key in (
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_TLS",
        "DOCKER_TLS_VERIFY",
        "DOCKER_CERT_PATH",
        "TESTCONTAINERS_HOST_OVERRIDE",
        "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
    ):
        env.pop(key, None)
    command = ["docker", "--host=unix:///var/run/docker.sock", *args]
    try:
        result = subprocess.run(
            command,
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        operation = args[0] if args else "unknown"
        raise DockerCommandError(
            f"Docker operation timed out after {timeout}s: {operation}"
        ) from None
    if check and result.returncode:
        operation = args[0] if args else "unknown"
        detail = redact_sensitive(result.stderr[-500:].strip())
        suffix = f"; stderr={detail}" if detail else ""
        raise DockerCommandError(
            f"Docker operation failed with exit {result.returncode}: {operation}{suffix}"
        )
    return result


def names(run_id: str) -> dict[str, str]:
    suffix = hashlib.sha256(run_id.encode()).hexdigest()[:12]
    prefix = "ariadne-soak-" + suffix
    return {
        "network": prefix + "-net",
        "primary": prefix + "-primary",
        "standby": prefix + "-standby",
        "service": prefix + "-service",
        "primary_volume": prefix + "-primary-data",
        "standby_volume": prefix + "-standby-data",
        "image": "ariadne-long-soak:" + suffix,
    }


def state_dir(root: Path, run_id: str) -> Path:
    return root.resolve() / checked_run_id(run_id)


def source_hashes() -> dict[str, str]:
    paths = sorted(HERE.glob("*.py")) + [HERE / name for name in SOURCE_FILES]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise RuntimeError(
            "long-soak source manifest is incomplete: " + ",".join(missing)
        )
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def core_hashes() -> dict[str, str]:
    paths = sorted(PROTOTYPES.glob("postgres_*.py"))
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def git_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=HERE,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else "unavailable"


def git_provenance() -> dict[str, Any]:
    result = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all"],
        cwd=HERE,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        return {"worktree_clean": False, "status_sha256": None}
    status = result.stdout
    return {
        "worktree_clean": not bool(status),
        "status_sha256": hashlib.sha256(status.encode()).hexdigest(),
    }


def parse_segments(value: str, count: int) -> list[int]:
    try:
        result = sorted({int(item) for item in value.split(",") if item != ""})
    except ValueError as exc:
        raise ValueError(
            "process-loss segments must be comma-separated integers"
        ) from exc
    if any(item < 0 or item >= count for item in result):
        raise ValueError("process-loss segment outside declared campaign")
    return result


def runner_identity() -> dict[str, Any]:
    docker_info = json.loads(docker("info", "--format", "{{json .}}").stdout)
    machine = Path("/etc/machine-id")
    machine_digest = (
        hashlib.sha256(machine.read_bytes()).hexdigest() if machine.exists() else None
    )
    return {
        "runner_name": os.environ.get("RUNNER_NAME"),
        "runner_environment": os.environ.get("RUNNER_ENVIRONMENT"),
        "runner_arch": os.environ.get("RUNNER_ARCH"),
        "runner_os": os.environ.get("RUNNER_OS"),
        "hostname": socket.gethostname(),
        "machine_id_sha256": machine_digest,
        "docker_id": docker_info.get("ID"),
        "docker_server_version": docker_info.get("ServerVersion"),
        "docker_root_dir": docker_info.get("DockerRootDir"),
    }


def _wait_pg(container: str, *, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    consecutive = 0
    while time.monotonic() < deadline:
        result = docker(
            "exec", container, "pg_isready", "-U", "ariadne", check=False, timeout=5
        )
        if result.returncode == 0:
            consecutive += 1
            if consecutive >= 3:
                return
        else:
            consecutive = 0
        time.sleep(0.5)
    raise RuntimeError(f"PostgreSQL container {container} did not become ready")


def _psql(container: str, sql: str) -> str:
    return docker(
        "exec",
        container,
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-At",
        "-U",
        "ariadne",
        "-d",
        "streaming",
        "-c",
        sql,
    ).stdout.strip()


def _manifest(directory: Path) -> dict[str, Any]:
    path = directory / "manifest.json"
    if not path.exists():
        raise RuntimeError("long-soak run is not bootstrapped")
    return json.loads(path.read_text())


def _assert_campaign_identity(manifest: dict[str, Any]) -> None:
    if (
        git_commit() != manifest["git_commit"]
        or source_hashes() != manifest["source_sha256"]
        or core_hashes() != manifest["core_source_sha256"]
        or runner_identity() != manifest["runner_identity"]
    ):
        raise RuntimeError("monitor checkout or dedicated runner identity drift")


def bootstrap(args) -> dict[str, Any]:
    run_id = checked_run_id(args.run_id)
    directory = state_dir(args.root, run_id)
    directory.mkdir(parents=True, exist_ok=True)
    os.chmod(directory, 0o700)
    manifest_path = directory / "manifest.json"
    hashes = source_hashes()
    runtime_hashes = core_hashes()
    identity = runner_identity()
    commit = git_commit()
    provenance = git_provenance()
    if args.require_clean and not provenance["worktree_clean"]:
        raise RuntimeError("CI long-soak bootstrap requires a clean pinned checkout")
    process_loss_segments = parse_segments(
        args.process_loss_segments, args.segment_count
    )
    resources = names(run_id)
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())
        if (
            existing["run_id"],
            existing["git_commit"],
            existing["source_sha256"],
            existing["core_source_sha256"],
            existing["runner_identity"],
            existing["fault_schedule"]["process_loss_segments"],
        ) != (
            run_id,
            commit,
            hashes,
            runtime_hashes,
            identity,
            process_loss_segments,
        ):
            raise RuntimeError(
                "existing long-soak identity differs from this runner/source"
            )
        return existing
    # A failed bootstrap has no manifest authority and cannot be resumed as a
    # campaign. Remove only the deterministic resources owned by this run ID.
    for container in (resources["service"], resources["standby"], resources["primary"]):
        docker("rm", "-f", container, check=False)
    docker("network", "rm", resources["network"], check=False)
    for volume in (resources["standby_volume"], resources["primary_volume"]):
        docker("volume", "rm", volume, check=False)
    docker("image", "rm", resources["image"], check=False)
    password = secrets.token_hex(24)
    replication_password = secrets.token_hex(24)
    secret_path = directory / "credentials.json"
    atomic_json(
        secret_path,
        {"postgres_password": password, "replication_password": replication_password},
    )
    for resource in (resources["network"],):
        docker("network", "create", resource)
    for resource in (resources["primary_volume"], resources["standby_volume"]):
        docker("volume", "create", resource)
    docker(
        "run",
        "-d",
        "--name",
        resources["primary"],
        "--network",
        resources["network"],
        "--restart",
        "unless-stopped",
        "--memory",
        "2g",
        "--cpus",
        "2",
        "--log-opt",
        "max-size=20m",
        "--log-opt",
        "max-file=3",
        "-e",
        "POSTGRES_USER=ariadne",
        "-e",
        f"POSTGRES_PASSWORD={password}",
        "-e",
        "POSTGRES_DB=streaming",
        "-v",
        f"{resources['primary_volume']}:/var/lib/postgresql/data",
        POSTGRES_IMAGE,
        "-c",
        "wal_level=replica",
        "-c",
        "max_wal_senders=10",
        "-c",
        "hot_standby=on",
        "-c",
        "max_wal_size=4GB",
        "-c",
        "min_wal_size=1GB",
        "-c",
        "checkpoint_timeout=15min",
    )
    _wait_pg(resources["primary"])
    quoted = replication_password.replace("'", "''")
    _psql(
        resources["primary"],
        f"CREATE ROLE ariadne_repl WITH REPLICATION LOGIN PASSWORD '{quoted}'",
    )
    docker(
        "exec",
        resources["primary"],
        "sh",
        "-ec",
        "echo 'host replication ariadne_repl 0.0.0.0/0 scram-sha-256' "
        ">> /var/lib/postgresql/data/pg_hba.conf",
    )
    _psql(resources["primary"], "SELECT pg_reload_conf()")
    docker(
        "run",
        "--rm",
        "--network",
        resources["network"],
        "-e",
        f"PGPASSWORD={replication_password}",
        "-v",
        f"{resources['standby_volume']}:/var/lib/postgresql/data",
        POSTGRES_IMAGE,
        "sh",
        "-ec",
        "rm -rf /var/lib/postgresql/data/*; "
        "chown -R postgres:postgres /var/lib/postgresql/data; "
        f"gosu postgres pg_basebackup -h {resources['primary']} -U ariadne_repl "
        "-D /var/lib/postgresql/data -Fp -Xs -R -P",
        timeout=300,
    )
    docker(
        "run",
        "-d",
        "--name",
        resources["standby"],
        "--network",
        resources["network"],
        "--restart",
        "unless-stopped",
        "--memory",
        "2g",
        "--cpus",
        "2",
        "--log-opt",
        "max-size=20m",
        "--log-opt",
        "max-file=3",
        "-v",
        f"{resources['standby_volume']}:/var/lib/postgresql/data",
        POSTGRES_IMAGE,
    )
    _wait_pg(resources["standby"])
    _psql(resources["primary"], "ALTER SYSTEM SET synchronous_standby_names='*'")
    _psql(resources["primary"], "ALTER SYSTEM SET synchronous_commit='remote_apply'")
    _psql(resources["primary"], "SELECT pg_reload_conf()")
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        sync = _psql(
            resources["primary"],
            "SELECT count(*) FROM pg_stat_replication "
            "WHERE state='streaming' AND sync_state='sync'",
        )
        if sync == "1":
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("synchronous remote_apply standby did not become ready")
    dockerfile = HERE / "Dockerfile"
    proxy_ca = Path(
        os.environ.get(
            "CODEX_PROXY_CERT",
            "/usr/local/share/ca-certificates/environment-proxy-ca.crt",
        )
    )
    before = source_hashes()
    core_before = core_hashes()
    build_args = [
        "build",
        "--pull=false",
        "--file",
        str(dockerfile),
        "--build-arg",
        f"SOAK_SOURCE_DIGEST={digest(hashes)}",
        "--build-arg",
        f"SOAK_CORE_DIGEST={digest(runtime_hashes)}",
    ]
    if proxy_ca.exists():
        build_args.extend(("--secret", f"id=proxy_ca,src={proxy_ca}"))
    build_args.extend(("--tag", resources["image"], str(PROTOTYPES)))
    docker(*build_args, timeout=600)
    after = source_hashes()
    core_after = core_hashes()
    if (
        before != after
        or before != hashes
        or core_before != core_after
        or core_before != runtime_hashes
    ):
        raise RuntimeError("source drifted while building long-soak image")
    image = json.loads(
        docker("image", "inspect", "--format", "{{json .}}", resources["image"]).stdout
    )
    manifest = {
        "protocol": "ariadne-long-soak/v1",
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(),
        "git_commit": commit,
        "git_provenance": provenance,
        "source_sha256": hashes,
        "source_manifest_digest": digest(hashes),
        "core_source_sha256": runtime_hashes,
        "core_source_manifest_digest": digest(runtime_hashes),
        "runner_identity": identity,
        "runner_identity_digest": digest(identity),
        "resources": resources,
        "image_id": image["Id"],
        "image_repo_digests": image.get("RepoDigests") or [],
        "postgres_image_id": docker(
            "image", "inspect", "--format", "{{.Id}}", POSTGRES_IMAGE
        ).stdout.strip(),
        "dependencies": {
            "python": "3.12",
            "uv": "0.12.23",
            "asyncpg": "0.30.0",
            "cryptography": "45.0.7",
            "python_image": PYTHON_IMAGE,
            "uv_image": UV_IMAGE,
            "postgres_image": POSTGRES_IMAGE,
        },
        "target_seconds": args.target_seconds,
        "segment_seconds": args.segment_seconds,
        "segment_count": args.segment_count,
        "smoke": args.smoke,
        "fault_schedule": {
            "owner_loss_every_records": args.owner_fault_every,
            "process_loss_segments": process_loss_segments,
        },
        "profile": {
            "dag": "source->A->{B,C}->J->D",
            "records_per_second": args.records_per_second,
            "checkpoint_every": args.checkpoint_every,
            "owner_fault_every": args.owner_fault_every,
            "release_session_guard": True,
            "actual_prefect_in_72h": False,
            "actual_prefect_evidence": "separate short R4-B gate",
        },
    }
    atomic_json(manifest_path, manifest)
    return manifest


def start(args) -> dict[str, Any]:
    manifest = (
        bootstrap(args)
        if args.bootstrap
        else _manifest(state_dir(args.root, args.run_id))
    )
    directory = state_dir(args.root, args.run_id)
    credentials = json.loads((directory / "credentials.json").read_text())
    resources = manifest["resources"]
    existing = docker("container", "inspect", resources["service"], check=False)
    if existing.returncode == 0:
        docker("start", resources["service"])
    else:
        dsn = (
            "postgresql://ariadne:"
            + credentials["postgres_password"]
            + "@"
            + resources["primary"]
            + "/streaming"
        )
        docker(
            "run",
            "-d",
            "--name",
            resources["service"],
            "--network",
            resources["network"],
            "--restart",
            "unless-stopped",
            "--memory",
            "1g",
            "--cpus",
            "2",
            "--pids-limit",
            "256",
            "--log-opt",
            "max-size=20m",
            "--log-opt",
            "max-file=3",
            "-v",
            f"{directory}:/state",
            "-e",
            f"SOAK_RUN_ID={manifest['run_id']}",
            "-e",
            f"SOAK_DSN={dsn}",
            "-e",
            "SOAK_SCHEMA=long_soak",
            "-e",
            f"SOAK_TARGET_SECONDS={manifest['target_seconds']}",
            "-e",
            f"SOAK_SMOKE={'true' if manifest['smoke'] else 'false'}",
            "-e",
            f"SOAK_RECORDS_PER_SECOND={manifest['profile']['records_per_second']}",
            "-e",
            f"SOAK_CHECKPOINT_EVERY={manifest['profile']['checkpoint_every']}",
            "-e",
            f"SOAK_OWNER_FAULT_EVERY={manifest['profile']['owner_fault_every']}",
            manifest["image_id"],
        )
    deadline = time.monotonic() + 120
    status_path = directory / "service-status.json"
    while time.monotonic() < deadline:
        if status_path.exists():
            status = json.loads(status_path.read_text())
            if status.get("phase") in {
                "IDLE",
                "PROCESSING",
                "CHECKPOINTING",
                "TARGET_REACHED",
            }:
                return status
            if status.get("phase") == "FAILED":
                raise RuntimeError(
                    "long-soak service failed: " + status.get("error", "")
                )
        time.sleep(0.5)
    raise RuntimeError("long-soak service did not publish readiness")


def _container_metrics(resources: dict[str, str]) -> dict[str, Any]:
    stats = json.loads(
        docker(
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
            resources["service"],
        ).stdout
    )
    primary_bytes = int(
        docker(
            "exec",
            resources["primary"],
            "du",
            "-sb",
            "/var/lib/postgresql/data",
        ).stdout.split()[0]
    )
    standby_bytes = int(
        docker(
            "exec",
            resources["standby"],
            "du",
            "-sb",
            "/var/lib/postgresql/data",
        ).stdout.split()[0]
    )
    usage, limit = (part.strip() for part in stats["MemUsage"].split("/", 1))
    return {
        "service": stats,
        "service_rss_bytes": parse_human_bytes(usage),
        "service_memory_limit_bytes": parse_human_bytes(limit),
        "service_cpu_percent": float(stats["CPUPerc"].rstrip("%")),
        "primary_data_bytes": primary_bytes,
        "standby_data_bytes": standby_bytes,
    }


def parse_human_bytes(value: str) -> int:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([KMGT]?i?B)", value)
    if match is None:
        raise RuntimeError("unrecognized Docker memory measurement")
    number, suffix = match.groups()
    multipliers = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
    }
    return int(float(number) * multipliers[suffix])


def _oracles(resources: dict[str, str]) -> dict[str, Any]:
    reports = {}
    for name in ("stream", "barrier", "join", "release"):
        script = f"/app/postgres_{name}_oracle.py"
        result = docker(
            "exec",
            resources["service"],
            "sh",
            "-ec",
            f'exec python {script} "$SOAK_DSN" "$SOAK_SCHEMA"',
            check=False,
            timeout=120,
        )
        try:
            report = json.loads(result.stdout.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError):
            report = {
                "passed": False,
                "violations": ["oracle_output_invalid"],
                "stderr": result.stderr[-500:],
            }
        report["returncode"] = result.returncode
        reports[name] = report
    return reports


def status(args) -> dict[str, Any]:
    directory = state_dir(args.root, args.run_id)
    manifest = _manifest(directory)
    service_status = json.loads((directory / "service-status.json").read_text())
    resources = manifest["resources"]
    inspect = json.loads(
        docker(
            "container", "inspect", "--format", "{{json .}}", resources["service"]
        ).stdout
    )
    return {
        "manifest_digest": digest(manifest),
        "service_status": service_status,
        "container_state": inspect["State"],
        "container_restart_count": inspect["RestartCount"],
        "metrics": _container_metrics(resources),
    }


def watch(args) -> dict[str, Any]:
    directory = state_dir(args.root, args.run_id)
    manifest = _manifest(directory)
    _assert_campaign_identity(manifest)
    if not 0 <= args.segment < manifest["segment_count"]:
        raise ValueError("segment index outside declared campaign")
    segment_path = directory / "segments" / f"segment-{args.segment:02d}.json"
    if segment_path.exists():
        return json.loads(segment_path.read_text())
    if args.segment:
        previous_path = directory / "segments" / f"segment-{args.segment - 1:02d}.json"
        if not previous_path.exists():
            raise RuntimeError("previous monitor segment is missing")
        previous = json.loads(previous_path.read_text())
        previous_digest = digest(previous)
    else:
        previous_digest = None
    started = datetime.now(UTC)
    deadline = time.monotonic() + args.duration
    samples = []
    injected = False
    while True:
        current = status(args)
        observed = datetime.now(UTC)
        service_status = current["service_status"]
        if (
            service_status["phase"] == "FAILED"
            or not current["container_state"]["Running"]
        ):
            raise RuntimeError("long-soak service is not healthy")
        age = (
            observed - datetime.fromisoformat(service_status["observed_at"])
        ).total_seconds()
        if age > max(120, args.poll_seconds * 3):
            raise RuntimeError("long-soak heartbeat is stale")
        samples.append(
            {
                "observed_at": observed.isoformat(),
                "boot_id": service_status["boot_id"],
                "phase": service_status["phase"],
                "elapsed_seconds": service_status["elapsed_seconds"],
                "source_completed": service_status["run"]["source_completed"],
                "last_checkpoint_source": service_status["run"][
                    "last_checkpoint_source"
                ],
                "owner_faults": service_status["run"]["owner_faults"],
                "process_boots": service_status["run"]["process_boots"],
                "container_restart_count": current["container_restart_count"],
                "database": service_status["database"],
                "pressure": service_status["pressure"],
                "metrics": current["metrics"],
            }
        )
        scheduled_loss = (
            args.segment in manifest["fault_schedule"]["process_loss_segments"]
        )
        if args.inject_process_loss and not manifest["smoke"] and not scheduled_loss:
            raise RuntimeError("ad-hoc process loss is forbidden outside smoke mode")
        wants_loss = scheduled_loss or args.inject_process_loss
        if wants_loss and not injected and service_status["phase"] == "TARGET_REACHED":
            raise RuntimeError("declared process-loss boundary missed before target")
        if wants_loss and not injected and samples:
            before = service_status["boot_id"]
            # Kill only the workload child. The persistent container supervisor
            # retains resource-accounting continuity and starts a fresh process
            # against the same durable PostgreSQL state.
            docker(
                "exec",
                manifest["resources"]["service"],
                "sh",
                "-ec",
                "kill -KILL $(cat /state/service-child.pid)",
                check=False,
            )
            restart_deadline = time.monotonic() + 120
            while time.monotonic() < restart_deadline:
                time.sleep(0.5)
                candidate_path = directory / "service-status.json"
                if candidate_path.exists():
                    candidate = json.loads(candidate_path.read_text())
                    if (
                        candidate.get("boot_id") != before
                        and candidate.get("phase") != "FAILED"
                    ):
                        injected = True
                        deadline = max(deadline, time.monotonic() + args.poll_seconds)
                        break
            if not injected:
                raise RuntimeError(
                    "service did not resume after process-loss injection"
                )
        if time.monotonic() >= deadline:
            break
        time.sleep(min(args.poll_seconds, max(0.1, deadline - time.monotonic())))
    oracle_reports = _oracles(manifest["resources"])
    ended = datetime.now(UTC)
    report = {
        "protocol": "ariadne-long-soak-segment/v1",
        "run_id": manifest["run_id"],
        "segment": args.segment,
        "started_at": started.isoformat(),
        "ended_at": ended.isoformat(),
        "duration_seconds": (ended - started).total_seconds(),
        "manifest_digest": digest(manifest),
        "previous_segment_digest": previous_digest,
        "runner_identity_digest": manifest["runner_identity_digest"],
        "source_manifest_digest": manifest["source_manifest_digest"],
        "process_loss_injected": injected,
        "samples": samples,
        "oracles": oracle_reports,
        "passed": all(item.get("passed") for item in oracle_reports.values()),
    }
    atomic_json(segment_path, report)
    return report


def stop(args) -> dict[str, Any]:
    directory = state_dir(args.root, args.run_id)
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    resources = manifest["resources"] if manifest is not None else names(args.run_id)
    docker("stop", "--time", "30", resources["service"], check=False)
    result = {
        "run_id": args.run_id,
        "service_stopped": True,
        "cleanup": args.cleanup,
        "manifest_present": manifest is not None,
    }
    if args.cleanup:
        for container in (
            resources["service"],
            resources["standby"],
            resources["primary"],
        ):
            docker("rm", "-f", container, check=False)
        docker("network", "rm", resources["network"], check=False)
        for volume in (resources["standby_volume"], resources["primary_volume"]):
            docker("volume", "rm", volume, check=False)
        docker("image", "rm", resources["image"], check=False)
        (directory / "credentials.json").unlink(missing_ok=True)
    return result


def export_public(args) -> dict[str, Any]:
    """Copy the explicit credential-free CI artifact whitelist."""
    directory = state_dir(args.root, args.run_id)
    manifest = _manifest(directory)
    destination = args.output.resolve()
    if destination.exists():
        shutil.rmtree(destination)
    (destination / "segments").mkdir(parents=True)
    copied = []
    for name in ("manifest.json", "review.json", "review.md"):
        source = directory / name
        if source.exists():
            shutil.copy2(source, destination / name)
            copied.append(name)
    for source in sorted((directory / "segments").glob("segment-*.json")):
        target = destination / "segments" / source.name
        shutil.copy2(source, target)
        copied.append("segments/" + source.name)
    segment_count = sum(item.startswith("segments/") for item in copied)
    complete = (
        segment_count == manifest["segment_count"]
        and "review.json" in copied
        and "review.md" in copied
    )
    return {
        "run_id": manifest["run_id"],
        "output": str(destination),
        "files": copied,
        "complete": complete,
        "observed_segments": segment_count,
        "expected_segments": manifest["segment_count"],
        "credentials_exported": False,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = value.add_subparsers(dest="command", required=True)
    for command in ("bootstrap", "start"):
        item = sub.add_parser(command)
        item.add_argument("--run-id", required=True)
        item.add_argument("--target-seconds", type=int, default=72 * 60 * 60)
        item.add_argument("--segment-seconds", type=int, default=4 * 60 * 60)
        item.add_argument("--segment-count", type=int, default=18)
        item.add_argument("--records-per-second", type=float, default=1.0)
        item.add_argument("--checkpoint-every", type=int, default=20)
        item.add_argument("--owner-fault-every", type=int, default=101)
        item.add_argument("--process-loss-segments", default="5,11")
        item.add_argument("--require-clean", action="store_true")
        item.add_argument("--smoke", action="store_true")
        if command == "start":
            item.add_argument("--bootstrap", action="store_true")
    for command in ("status", "watch"):
        item = sub.add_parser(command)
        item.add_argument("--run-id", required=True)
        if command == "watch":
            item.add_argument("--segment", type=int, required=True)
            item.add_argument("--duration", type=float, default=4 * 60 * 60)
            item.add_argument("--poll-seconds", type=float, default=60)
            item.add_argument("--inject-process-loss", action="store_true")
    item = sub.add_parser("stop")
    item.add_argument("--run-id", required=True)
    item.add_argument("--cleanup", action="store_true")
    item = sub.add_parser("review")
    item.add_argument("--run-id", required=True)
    item.add_argument("--output", type=Path)
    item = sub.add_parser("export-public")
    item.add_argument("--run-id", required=True)
    item.add_argument("--output", type=Path, required=True)
    return value


def main() -> int:
    args = parser().parse_args()
    if args.command == "bootstrap":
        result = bootstrap(args)
    elif args.command == "start":
        result = start(args)
    elif args.command == "status":
        result = status(args)
    elif args.command == "watch":
        result = watch(args)
    elif args.command == "stop":
        result = stop(args)
    elif args.command == "export-public":
        result = export_public(args)
    else:
        reviewer = HERE / "review.py"
        command = [
            sys.executable,
            str(reviewer),
            "--root",
            str(args.root),
            "--run-id",
            args.run_id,
        ]
        if args.output:
            command.extend(("--output", str(args.output)))
        return subprocess.run(command, check=False).returncode
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
