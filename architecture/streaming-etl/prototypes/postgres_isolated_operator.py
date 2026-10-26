"""Selected closed-network operator boundary for protocol epoch 2.

The trusted platform retains PostgreSQL credentials, claims the durable input,
validates the worker result, and commits the cursor/state/output.  The worker
receives payload bytes and managed state only over a framed stdio RPC.

This is an executable container profile, not a claim of general container or
external-connector isolation.  The Docker daemon and platform process remain
trusted, and the selected profile supports one pinned built-in artifact.
"""

from __future__ import annotations

import base64
import hashlib
import inspect
import json
import os
import selectors
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from postgres_generator_runtime import OutputSpec
from prefect_stream_adapter import resolve_operator_artifact


class IsolatedOperatorError(RuntimeError):
    pass


class IsolatedOperatorProtocolError(IsolatedOperatorError):
    pass


@dataclass(frozen=True)
class IsolatedOperatorResult:
    terminal: str
    payload: bytes | None
    schema_id: str | None
    managed_state: dict[str, Any]


class IsolatedOperatorSupervisor:
    """Long-lived, credential-free Docker worker controlled by the platform."""

    PROTOCOL = "ariadne-isolated-operator/v1"

    def __init__(
        self,
        *,
        artifact_id: str,
        artifact_digest: str,
        image: str,
        worker_digest: str,
        container_name: str,
        max_payload_bytes: int = 1_000_000,
        max_state_bytes: int = 256_000,
        rpc_timeout_seconds: float = 5.0,
    ):
        registry_digest, _ = resolve_operator_artifact(artifact_id)
        if registry_digest != artifact_digest:
            raise IsolatedOperatorError("operator registry artifact digest mismatch")
        if not all((image, worker_digest, container_name)):
            raise ValueError("complete isolated worker identity required")
        if min(max_payload_bytes, max_state_bytes) <= 0 or rpc_timeout_seconds <= 0:
            raise ValueError("positive isolated worker limits required")
        self.artifact_id = artifact_id
        self.artifact_digest = artifact_digest
        self.image = image
        self.worker_digest = worker_digest
        self.container_name = container_name
        self.max_payload_bytes = max_payload_bytes
        self.max_state_bytes = max_state_bytes
        self.max_rpc_bytes = 2 * (max_payload_bytes + max_state_bytes) + 4096
        self.rpc_timeout_seconds = rpc_timeout_seconds
        self._lock = threading.Lock()
        self._process: subprocess.Popen[str] | None = None
        self.worker_pid: int | None = None
        self.image_id: str | None = None
        self.bound_participant_id: str | None = None
        self.bound_attempt_id: str | None = None

    async def bind_authority(
        self,
        attempts,
        token: str,
        *,
        control_run_id: str,
        attempt_id: str,
        participant_id: str,
        revision,
    ) -> None:
        """Bind execution to the current durable attempt and revision manifest."""
        participant = next(
            (
                item
                for item in revision.participant_manifest
                if item["participant_id"] == participant_id
            ),
            None,
        )
        if participant is None or participant["code_digest"] != self.artifact_digest:
            raise IsolatedOperatorError("revision operator artifact identity drift")
        projection = await attempts.project_attempt(
            token, control_run_id=control_run_id
        )
        current = projection.get("current")
        if current is None or current.get("attempt_id") != attempt_id:
            raise IsolatedOperatorError("isolated operator attempt is not current")
        binding = next(
            (
                item
                for item in projection.get("operators", ())
                if item.get("participant_id") == participant_id
            ),
            None,
        )
        if binding is None or (
            binding.get("artifact_id"),
            binding.get("artifact_digest"),
        ) != (self.artifact_id, self.artifact_digest):
            raise IsolatedOperatorError("attempt operator artifact identity drift")
        self.bound_participant_id = participant_id
        self.bound_attempt_id = attempt_id

    @staticmethod
    def build_image(
        worker_path: Path,
        *,
        artifact_id: str,
        artifact_digest: str,
        base_image: str = "python@sha256:b658b05ce9c4eb97ec6fc19424818a1f31993659c2b505a532f470e83807815d",
    ) -> tuple[str, str]:
        registry_digest, function = resolve_operator_artifact(artifact_id)
        if registry_digest != artifact_digest:
            raise IsolatedOperatorError("operator registry artifact digest mismatch")
        if not inspect.isgeneratorfunction(function):
            raise IsolatedOperatorError(
                "selected isolated profile requires a synchronous generator"
            )
        if function.__qualname__ != function.__name__:
            raise IsolatedOperatorError(
                "selected isolated artifact must be module-level"
            )
        source = inspect.getsource(function)
        artifact_module = "\n".join(
            (
                "from __future__ import annotations",
                "import hashlib",
                "import inspect",
                "import json",
                "from dataclasses import dataclass, field",
                "from types import SimpleNamespace",
                "from typing import Any",
                "",
                "@dataclass(frozen=True)",
                "class OutputSpec:",
                "    payload: bytes",
                "    schema_id: str = 'bytes/v1'",
                "    managed_state: dict[str, Any] = field(default_factory=dict)",
                "",
                "def stream_task(function):",
                "    function.__ariadne_stream_protocol__ = 'yield-v2'",
                "    function.__ariadne_stream_mode__ = 'sync'",
                "    return function",
                "",
                f"SOURCE = {source!r}",
                f"MODULE = {function.__module__!r}",
                f"QUALNAME = {function.__qualname__!r}",
                source.rstrip(),
                f"FUNCTION = {function.__name__}",
                "",
                "def artifact_digest():",
                "    if inspect.getsource(FUNCTION) != SOURCE:",
                "        raise RuntimeError('executed artifact source differs from manifest')",
                "    if getattr(FUNCTION, '__ariadne_stream_protocol__', None) != 'yield-v2':",
                "        raise RuntimeError('executed artifact protocol differs from manifest')",
                "    material = '\\n'.join((MODULE, QUALNAME, SOURCE, 'yield-v2'))",
                "    return hashlib.sha256(material.encode()).hexdigest()",
                "",
                "def execute(participant, channel, partition, sequence, payload, state):",
                "    managed = json.loads(json.dumps(state, sort_keys=True, separators=(',', ':')))",
                "    context = SimpleNamespace(participant_id=participant, input_key=channel + '/' + partition, input_sequence=sequence, managed_state=managed)",
                "    generator = FUNCTION(context, payload)",
                "    try:",
                "        spec = next(generator)",
                "    except StopIteration as stop:",
                "        after = context.managed_state if stop.value is None else stop.value",
                "        return {'terminal': 'RETURN', 'payload': None, 'schema_id': None, 'managed_state': after}",
                "    finally:",
                "        generator.close()",
                "    if not isinstance(spec, OutputSpec):",
                "        raise TypeError('artifact must yield OutputSpec')",
                "    return {'terminal': 'YIELD', 'payload': bytes(spec.payload), 'schema_id': str(spec.schema_id), 'managed_state': spec.managed_state}",
                "",
            )
        )
        worker = worker_path.read_bytes()
        worker_digest = hashlib.sha256(worker).hexdigest()
        tag = (
            "ariadne-isolated-operator:"
            + hashlib.sha256((worker_digest + artifact_digest).encode()).hexdigest()[
                :20
            ]
        )
        dockerfile = "\n".join(
            (
                f"FROM {base_image}",
                "WORKDIR /app",
                "COPY --chown=65532:65532 worker.py /app/worker.py",
                "COPY --chown=65532:65532 artifact.py /app/isolated_operator_artifact.py",
                f'LABEL ariadne.worker.digest="{worker_digest}"',
                f'LABEL ariadne.artifact.digest="{artifact_digest}"',
                "USER 65532:65532",
                'ENTRYPOINT ["python","-I","/app/worker.py"]',
                "",
            )
        )
        env = os.environ.copy()
        for key in (
            "DOCKER_HOST",
            "TESTCONTAINERS_HOST_OVERRIDE",
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
        ):
            env.pop(key, None)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "worker.py").write_bytes(worker)
            (root / "artifact.py").write_text(artifact_module)
            (root / "Dockerfile").write_text(dockerfile)
            subprocess.run(
                [
                    "docker",
                    "build",
                    "--network",
                    "none",
                    "--quiet",
                    "--tag",
                    tag,
                    ".",
                ],
                cwd=root,
                env=env,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
        labels = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{json .Config.Labels}}",
                tag,
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        actual = json.loads(labels.stdout)
        if actual != {
            "ariadne.artifact.digest": artifact_digest,
            "ariadne.worker.digest": worker_digest,
        }:
            raise IsolatedOperatorError("isolated worker image labels drift")
        return tag, worker_digest

    def start(self) -> None:
        if self._process is not None:
            raise IsolatedOperatorError("isolated worker is already started")
        env = os.environ.copy()
        for key in (
            "DOCKER_HOST",
            "TESTCONTAINERS_HOST_OVERRIDE",
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
        ):
            env.pop(key, None)
        image = subprocess.run(
            [
                "docker",
                "image",
                "inspect",
                "--format",
                "{{json .}}",
                self.image,
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        inspected = json.loads(image.stdout)
        labels = inspected.get("Config", {}).get("Labels")
        if labels != {
            "ariadne.artifact.digest": self.artifact_digest,
            "ariadne.worker.digest": self.worker_digest,
        } or not str(inspected.get("Id", "")).startswith("sha256:"):
            raise IsolatedOperatorError("isolated worker image authority drift")
        self.image_id = inspected["Id"]
        command = [
            "docker",
            "run",
            "--rm",
            "--interactive",
            "--name",
            self.container_name,
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--pids-limit",
            "32",
            "--memory",
            "64m",
            "--cpus",
            "0.5",
            "--tmpfs",
            "/tmp:rw,noexec,nosuid,nodev,size=1m",
            "--env",
            f"ARIADNE_ARTIFACT_ID={self.artifact_id}",
            "--env",
            f"ARIADNE_ARTIFACT_DIGEST={self.artifact_digest}",
            self.image_id,
        ]
        self._process = subprocess.Popen(
            command,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        ready = self._read()
        if ready != {
            "type": "ready",
            "protocol": self.PROTOCOL,
            "artifact_id": self.artifact_id,
            "artifact_digest": self.artifact_digest,
            "worker_digest": self.worker_digest,
            "pid": ready.get("pid"),
        } or not isinstance(ready.get("pid"), int):
            self.close(force=True)
            raise IsolatedOperatorProtocolError("isolated worker ready identity drift")
        self.worker_pid = ready["pid"]

    def _read(self) -> dict[str, Any]:
        if self._process is None or self._process.stdout is None:
            raise IsolatedOperatorError("isolated worker is not running")
        selector = selectors.DefaultSelector()
        try:
            selector.register(self._process.stdout, selectors.EVENT_READ)
            if not selector.select(self.rpc_timeout_seconds):
                raise IsolatedOperatorProtocolError("isolated worker RPC timeout")
            line = self._process.stdout.readline(self.max_rpc_bytes + 1)
        finally:
            selector.close()
        if not line:
            stderr = ""
            if self._process.stderr is not None and self._process.poll() is not None:
                stderr = self._process.stderr.read()[-500:]
            raise IsolatedOperatorProtocolError(
                f"isolated worker exited without response: {stderr}"
            )
        if len(line.encode()) > self.max_rpc_bytes or not line.endswith("\n"):
            raise IsolatedOperatorProtocolError(
                "isolated worker response exceeds RPC capacity"
            )
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise IsolatedOperatorProtocolError(
                "isolated worker returned invalid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise IsolatedOperatorProtocolError(
                "isolated worker response must be an object"
            )
        return value

    def _rpc(self, request: dict[str, Any]) -> dict[str, Any]:
        if self._process is None or self._process.stdin is None:
            raise IsolatedOperatorError("isolated worker is not running")
        with self._lock:
            if self._process.poll() is not None:
                raise IsolatedOperatorProtocolError("isolated worker process is dead")
            try:
                self._process.stdin.write(
                    json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n"
                )
                self._process.stdin.flush()
                response = self._read()
            except BaseException:
                # A late response after timeout must never be mistaken for the
                # next input. The platform discards this worker generation and
                # retries the still-uncommitted durable input with a new one.
                self.close(force=True)
                raise
        if response.get("type") == "error":
            raise IsolatedOperatorProtocolError(
                "isolated worker rejected request: " + str(response.get("message", ""))
            )
        return response

    def probe(self) -> dict[str, Any]:
        response = self._rpc({"type": "probe"})
        if response.get("type") != "probe":
            raise IsolatedOperatorProtocolError("isolated worker probe shape drift")
        return response

    def inspect_boundary(self) -> dict[str, Any]:
        env = os.environ.copy()
        for key in (
            "DOCKER_HOST",
            "TESTCONTAINERS_HOST_OVERRIDE",
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
        ):
            env.pop(key, None)
        inspected = subprocess.run(
            ["docker", "container", "inspect", self.container_name],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        values = json.loads(inspected.stdout)
        if not isinstance(values, list) or len(values) != 1:
            raise IsolatedOperatorError("isolated worker inspection failed")
        item = values[0]
        host = item["HostConfig"]
        return {
            "network_mode": host["NetworkMode"],
            "readonly_rootfs": host["ReadonlyRootfs"],
            "cap_drop": host["CapDrop"],
            "security_opt": host["SecurityOpt"],
            "binds": host["Binds"],
            "pid_mode": host["PidMode"],
            "pids_limit": host["PidsLimit"],
            "memory": host["Memory"],
            "nano_cpus": host["NanoCpus"],
            "tmpfs": host["Tmpfs"],
        }

    def execute(
        self,
        *,
        participant_id: str,
        input_key: str,
        input_sequence: int,
        payload: bytes,
        managed_state: dict[str, Any],
    ) -> IsolatedOperatorResult:
        if self.bound_participant_id != participant_id:
            raise IsolatedOperatorProtocolError(
                "isolated operator is not bound to this participant"
            )
        if "/" not in input_key or len(payload) > self.max_payload_bytes:
            raise IsolatedOperatorProtocolError(
                "isolated operator input exceeds contract"
            )
        channel, partition = input_key.split("/", 1)
        state_json = json.dumps(managed_state, sort_keys=True, separators=(",", ":"))
        if len(state_json.encode()) > self.max_state_bytes:
            raise IsolatedOperatorProtocolError(
                "isolated operator managed state exceeds contract"
            )
        request_id = hashlib.sha256(
            json.dumps(
                [
                    self.artifact_digest,
                    participant_id,
                    channel,
                    partition,
                    input_sequence,
                    hashlib.sha256(payload).hexdigest(),
                    state_json,
                ],
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        request = {
            "type": "execute",
            "request_id": request_id,
            "artifact_id": self.artifact_id,
            "artifact_digest": self.artifact_digest,
            "participant_id": participant_id,
            "input_channel": channel,
            "input_partition": partition,
            "input_sequence": input_sequence,
            "payload_b64": base64.b64encode(payload).decode(),
            "managed_state": json.loads(state_json),
        }
        return self._validate_result(self._rpc(request), request_id)

    def _validate_result(
        self, response: dict[str, Any], request_id: str
    ) -> IsolatedOperatorResult:
        expected = {
            "type",
            "request_id",
            "artifact_id",
            "artifact_digest",
            "terminal",
            "payload_b64",
            "schema_id",
            "managed_state",
        }
        if set(response) != expected or (
            response.get("type"),
            response.get("request_id"),
            response.get("artifact_id"),
            response.get("artifact_digest"),
        ) != ("result", request_id, self.artifact_id, self.artifact_digest):
            raise IsolatedOperatorProtocolError("isolated worker result identity drift")
        terminal = response.get("terminal")
        if terminal not in {"YIELD", "RETURN"}:
            raise IsolatedOperatorProtocolError("isolated worker terminal is invalid")
        state = response.get("managed_state")
        if not isinstance(state, dict):
            raise IsolatedOperatorProtocolError("isolated worker state is invalid")
        try:
            state = json.loads(json.dumps(state, sort_keys=True, separators=(",", ":")))
        except (TypeError, ValueError) as exc:
            raise IsolatedOperatorProtocolError(
                "isolated worker state is not canonical JSON"
            ) from exc
        if (
            len(json.dumps(state, sort_keys=True, separators=(",", ":")).encode())
            > self.max_state_bytes
        ):
            raise IsolatedOperatorProtocolError(
                "isolated worker state exceeds contract"
            )
        if terminal == "RETURN":
            if (
                response.get("payload_b64") is not None
                or response.get("schema_id") is not None
            ):
                raise IsolatedOperatorProtocolError("RETURN cannot carry output")
            return IsolatedOperatorResult(terminal, None, None, state)
        if not isinstance(response.get("schema_id"), str) or not response["schema_id"]:
            raise IsolatedOperatorProtocolError("isolated worker schema is invalid")
        try:
            payload = base64.b64decode(response.get("payload_b64"), validate=True)
        except (TypeError, ValueError) as exc:
            raise IsolatedOperatorProtocolError(
                "isolated worker payload is invalid"
            ) from exc
        if len(payload) > self.max_payload_bytes:
            raise IsolatedOperatorProtocolError(
                "isolated worker output exceeds contract"
            )
        return IsolatedOperatorResult(terminal, payload, response["schema_id"], state)

    def task(self):
        supervisor = self

        def isolated_task(context, payload):
            result = supervisor.execute(
                participant_id=context.participant_id,
                input_key=context.input_key,
                input_sequence=context.input_sequence,
                payload=payload,
                managed_state=context.managed_state,
            )
            if result.terminal == "YIELD":
                yield OutputSpec(result.payload, result.schema_id, result.managed_state)
            else:
                context.managed_state.clear()
                context.managed_state.update(result.managed_state)
                return result.managed_state

        isolated_task.__ariadne_stream_protocol__ = "yield-v2"
        isolated_task.__ariadne_stream_mode__ = "sync"
        return isolated_task

    def close(self, *, force: bool = False) -> None:
        process = self._process
        self._process = None
        if process is None:
            return
        if process.poll() is None and not force:
            try:
                if process.stdin is not None:
                    process.stdin.write('{"type":"shutdown"}\n')
                    process.stdin.flush()
                process.wait(timeout=2)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                process.kill()
                process.wait(timeout=2)
        elif process.poll() is None:
            process.kill()
            process.wait(timeout=2)
        for pipe in (process.stdin, process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()


def selected_profile_limitations() -> tuple[str, ...]:
    return (
        "Docker daemon and platform process are trusted",
        "closed network; external connectors are unsupported",
        "one pinned uppercase artifact is implemented",
        "container escape resistance and hostile kernel isolation are not claimed",
    )
