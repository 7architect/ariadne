"""Credential-free worker for the selected closed-network operator profile.

The platform starts this module in a container with no network, mounts, or
platform credentials.  It speaks one JSON object per line over stdio.  It is
deliberately small: the prototype supports the pinned uppercase artifact only.
"""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import socket
import sys
from pathlib import Path

_ARTIFACT_SPEC = importlib.util.spec_from_file_location(
    "isolated_operator_artifact",
    Path(__file__).resolve().with_name("isolated_operator_artifact.py"),
)
if _ARTIFACT_SPEC is None or _ARTIFACT_SPEC.loader is None:
    raise RuntimeError("isolated operator artifact module is unavailable")
artifact = importlib.util.module_from_spec(_ARTIFACT_SPEC)
sys.modules[_ARTIFACT_SPEC.name] = artifact
_ARTIFACT_SPEC.loader.exec_module(artifact)

PROTOCOL = "ariadne-isolated-operator/v1"


def _emit(value: dict) -> None:
    sys.stdout.write(json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _probe() -> dict:
    secret_markers = ("DSN", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL", "AWS_")
    exposed = sorted(
        key
        for key in os.environ
        if any(marker in key.upper() for marker in secret_markers)
    )
    connected = False
    try:
        with socket.create_connection(("127.0.0.1", 55432), timeout=0.05):
            connected = True
    except OSError:
        pass
    return {
        "secret_environment_keys": exposed,
        "docker_socket": Path("/var/run/docker.sock").exists(),
        "host_proc_mounted": Path("/host/proc").exists(),
        "pid1": Path("/proc/1/cmdline")
        .read_bytes()
        .replace(b"\0", b" ")
        .decode(errors="replace"),
        "platform_pg_reachable": connected,
    }


def _execute(request: dict, artifact_id: str, artifact_digest: str) -> dict:
    required = {
        "type",
        "request_id",
        "artifact_id",
        "artifact_digest",
        "participant_id",
        "input_channel",
        "input_partition",
        "input_sequence",
        "payload_b64",
        "managed_state",
    }
    if set(request) != required or request["type"] != "execute":
        raise ValueError("invalid RPC request shape")
    if (request["artifact_id"], request["artifact_digest"]) != (
        artifact_id,
        artifact_digest,
    ):
        raise ValueError("operator artifact identity mismatch")
    if not isinstance(request["managed_state"], dict):
        raise TypeError("managed state must be an object")
    if (
        not isinstance(request["input_sequence"], int)
        or isinstance(request["input_sequence"], bool)
        or request["input_sequence"] <= 0
    ):
        raise ValueError("invalid input sequence")
    payload = base64.b64decode(request["payload_b64"], validate=True)
    result = artifact.execute(
        request["participant_id"],
        request["input_channel"],
        request["input_partition"],
        request["input_sequence"],
        payload,
        request["managed_state"],
    )
    return {
        "type": "result",
        "request_id": request["request_id"],
        "artifact_id": artifact_id,
        "artifact_digest": artifact_digest,
        "terminal": result["terminal"],
        "payload_b64": (
            None
            if result["payload"] is None
            else base64.b64encode(result["payload"]).decode()
        ),
        "schema_id": result["schema_id"],
        "managed_state": result["managed_state"],
    }


def main() -> int:
    artifact_id = os.environ.get("ARIADNE_ARTIFACT_ID", "")
    artifact_digest = os.environ.get("ARIADNE_ARTIFACT_DIGEST", "")
    if (
        not artifact_id
        or len(artifact_digest) != 64
        or artifact.artifact_digest() != artifact_digest
    ):
        return 64
    worker_digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    _emit(
        {
            "type": "ready",
            "protocol": PROTOCOL,
            "artifact_id": artifact_id,
            "artifact_digest": artifact_digest,
            "worker_digest": worker_digest,
            "pid": os.getpid(),
        }
    )
    for line in sys.stdin:
        try:
            request = json.loads(line)
            if request == {"type": "shutdown"}:
                _emit({"type": "stopped"})
                return 0
            if request == {"type": "probe"}:
                _emit({"type": "probe", **_probe()})
                continue
            _emit(_execute(request, artifact_id, artifact_digest))
        except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
            _emit({"type": "error", "error": type(exc).__name__, "message": str(exc)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
