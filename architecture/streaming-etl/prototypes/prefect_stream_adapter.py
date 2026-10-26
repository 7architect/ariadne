"""Experimental Prefect 3 adapter for the durable streaming prototype."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import os
from dataclasses import asdict, dataclass
from typing import Any

from postgres_barrier_coordinator import PostgresBarrierCoordinator, Revision, decoded
from postgres_control_plane import (
    ControlCapacityBlocked,
    ControlConflict,
    PostgresControlPlane,
)
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_runtime import PostgresJoinRuntime
from postgres_prefect_attempts import (
    FeatureDisabled,
    OperatorBinding,
    PostgresPrefectAttempts,
    SourceBinding,
)
from postgres_prefect_maintenance import PostgresPrefectMaintenance
from postgres_stream_telemetry import StreamTelemetry
from postgres_stream_v2 import CapacityBlocked, PostgresStream, content_object_key

from prefect import flow, task
from prefect.context import get_run_context
from prefect.states import Cancelled

ADAPTER_TELEMETRY = StreamTelemetry(capacity=128)
OPERATOR_REGISTRY: dict[str, tuple[str, Any]] = {}


def operator_artifact_digest(function: Any) -> str:
    material = "\n".join(
        (
            function.__module__,
            function.__qualname__,
            inspect.getsource(function),
            str(getattr(function, "__ariadne_stream_protocol__", "")),
        )
    )
    return hashlib.sha256(material.encode()).hexdigest()


def register_operator_artifact(artifact_id: str, function: Any) -> str:
    if (
        not artifact_id
        or not (
            inspect.isgeneratorfunction(function)
            or inspect.isasyncgenfunction(function)
        )
        or getattr(function, "__ariadne_stream_protocol__", None) != "yield-v2"
    ):
        raise ValueError("operator artifact requires a declared stream generator")
    artifact_digest = operator_artifact_digest(function)
    existing = OPERATOR_REGISTRY.get(artifact_id)
    if existing is not None and existing != (artifact_digest, function):
        raise ValueError("operator artifact identity already registered")
    OPERATOR_REGISTRY[artifact_id] = (artifact_digest, function)
    return artifact_digest


def resolve_operator_artifact(artifact_id: str) -> tuple[str, Any]:
    try:
        return OPERATOR_REGISTRY[artifact_id]
    except KeyError as exc:
        raise FeatureDisabled(
            f"operator artifact {artifact_id!r} is unavailable"
        ) from exc


@dataclass(frozen=True)
class PrefectStreamAdapterConfig:
    enabled: bool
    dsn_env: str
    schema: str
    admin_token_env: str
    source_token_env: str
    control_run_id: str
    revision_id: str
    deployment_id: str
    graph_version: str
    graph_digest: str
    code_version: str
    leader_id: str
    leader_epoch: int
    coordinator_owner: str
    coordinator_epoch: int
    source_producer_id: str
    operator_artifacts: dict[str, str]
    leader_lease_seconds: float = 60.0
    replace_attempt_id: str | None = None
    source_delay_seconds: float = 0.0
    source_keep_open: bool = False
    operator_start_delay_seconds: float = 0.0
    operator_lease_seconds: float = 30.0
    maintenance_enabled: bool = False
    checkpoint_interval_records: int = 2
    max_checkpoint_delay_seconds: float = 5.0
    maintenance_poll_seconds: float = 0.02
    coordinator_lease_seconds: float = 60.0
    fault_maintenance_retries: bool = False
    fault_participant_once: str | None = None
    terminal_mode: str = "DRAIN"

    def validate(self) -> None:
        if not self.enabled:
            raise FeatureDisabled("Prefect streaming adapter feature is disabled")
        values = (
            self.dsn_env,
            self.schema,
            self.admin_token_env,
            self.source_token_env,
            self.control_run_id,
            self.revision_id,
            self.deployment_id,
            self.graph_version,
            self.graph_digest,
            self.code_version,
            self.leader_id,
            self.coordinator_owner,
            self.source_producer_id,
        )
        if (
            any(not value for value in values)
            or min(self.leader_epoch, self.coordinator_epoch) <= 0
        ):
            raise ValueError("complete Prefect adapter identity required")
        if self.terminal_mode not in {"DRAIN", "CANCEL"}:
            raise ValueError("terminal mode must be DRAIN or CANCEL")
        if min(self.source_delay_seconds, self.operator_start_delay_seconds) < 0:
            raise ValueError("adapter delays cannot be negative")
        if not isinstance(self.source_keep_open, bool):
            raise TypeError("source_keep_open must be boolean")
        if (
            not isinstance(self.operator_artifacts, dict)
            or not self.operator_artifacts
            or any(
                not isinstance(key, str)
                or not key
                or not isinstance(value, str)
                or not value
                for key, value in self.operator_artifacts.items()
            )
        ):
            raise TypeError("operator_artifacts requires a nonempty string mapping")
        if self.leader_lease_seconds <= 3:
            raise ValueError("leader lease must exceed three seconds")
        if self.operator_lease_seconds <= 0.3:
            raise ValueError("operator lease must exceed 0.3 seconds")
        if not isinstance(self.maintenance_enabled, bool):
            raise TypeError("maintenance_enabled must be boolean")
        if not isinstance(self.fault_maintenance_retries, bool):
            raise TypeError("maintenance fault control must be boolean")
        if (
            isinstance(self.checkpoint_interval_records, bool)
            or not isinstance(self.checkpoint_interval_records, int)
            or self.checkpoint_interval_records <= 0
            or self.max_checkpoint_delay_seconds <= 0
            or self.maintenance_poll_seconds <= 0
            or self.coordinator_lease_seconds <= 0.3
        ):
            raise ValueError("positive maintenance interval and leases required")


def _secret(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise FeatureDisabled(f"required adapter secret environment {name} missing")
    return value


async def _open(config: PrefectStreamAdapterConfig):
    stream = await PostgresStream.connect_existing(
        _secret(config.dsn_env), config.schema
    )
    coordinator = PostgresBarrierCoordinator(stream)
    control = PostgresControlPlane.connect_existing(stream)
    attempts = PostgresPrefectAttempts.connect_existing(control)
    async with stream.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM deployment_revisions WHERE revision_id=$1",
            config.revision_id,
        )
    if row is None or (
        row["deployment_id"],
        row["graph_version"],
        row["graph_digest"],
    ) != (config.deployment_id, config.graph_version, config.graph_digest):
        await stream.close()
        raise FeatureDisabled("Prefect adapter revision identity drift")
    revision = Revision(
        row["revision_id"],
        row["deployment_id"],
        row["graph_version"],
        row["graph_digest"],
        decoded(row["source_manifest"]),
        decoded(row["participant_manifest"]),
    )
    return stream, coordinator, control, attempts, revision


async def _validate_capacity_profile(
    stream: PostgresStream, revision: Revision
) -> None:
    locations = {
        (item["channel"], item["partition"])
        for participant in revision.participant_manifest
        for field in ("inputs", "outputs")
        for item in participant[field]
    }
    async with stream.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT channel,partition_id,max_records FROM partitions"
        )
    capacities = {
        (row["channel"], row["partition_id"]): row["max_records"]
        for row in rows
        if (row["channel"], row["partition_id"]) in locations
    }
    if set(capacities) != locations:
        raise FeatureDisabled("operator capacity profile has missing partitions")
    for participant in revision.participant_manifest:
        inputs = {
            (item["channel"], item["partition"]) for item in participant["inputs"]
        }
        required = sum(capacities[location] for location in inputs)
        for output in participant["outputs"]:
            location = (output["channel"], output["partition"])
            if capacities[location] < required:
                raise FeatureDisabled(
                    "operator output capacity is below its bounded input window"
                )


async def _validate_maintenance_profile(
    stream: PostgresStream,
    revision: Revision,
    config: PrefectStreamAdapterConfig,
    documents: list[dict[str, Any]],
) -> None:
    source = revision.source_manifest[0]
    async with stream.pool.acquire() as conn:
        partition = await conn.fetchrow(
            "SELECT max_records,max_bytes FROM partitions "
            "WHERE channel=$1 AND partition_id=$2",
            source["channel"],
            source["partition"],
        )
        run = await conn.fetchrow(
            "SELECT max_admission_records,max_admission_bytes "
            "FROM control_runs_v1 WHERE run_id=$1",
            config.control_run_id,
        )
    if partition is None or run is None:
        raise FeatureDisabled("maintenance capacity authority is missing")
    payload_sizes = []
    for document in documents:
        payload = document.get("payload")
        if not isinstance(payload, bytes):
            raise TypeError("source document payload must be bytes")
        payload_sizes.append(len(payload))
    byte_capacity = min(partition["max_bytes"], run["max_admission_bytes"])
    if any(size > byte_capacity for size in payload_sizes):
        raise FeatureDisabled("source document exceeds hard byte capacity")
    if not config.maintenance_enabled:
        return
    interval = config.checkpoint_interval_records
    if interval > min(partition["max_records"], run["max_admission_records"]):
        raise FeatureDisabled(
            "checkpoint interval exceeds source or admission record capacity"
        )
    max_window_bytes = max(
        (
            sum(payload_sizes[index : index + interval])
            for index in range(len(payload_sizes))
        ),
        default=0,
    )
    if max_window_bytes > byte_capacity:
        raise FeatureDisabled(
            "checkpoint interval exceeds source or admission byte capacity"
        )


@stream_task
def prefect_passthrough(ctx, payload):
    state = {
        "completed": ctx.managed_state.get("completed", 0) + 1,
        "last_sequence": ctx.input_sequence,
    }
    yield OutputSpec(payload, "bytes/v1", state)


@stream_task
def prefect_uppercase(ctx, payload):
    state = {
        "completed": ctx.managed_state.get("completed", 0) + 1,
        "last_sequence": ctx.input_sequence,
    }
    yield OutputSpec(payload.upper(), "bytes/v1", state)


@stream_task
def prefect_return_only(ctx, payload):
    del payload
    ctx.managed_state["completed"] = ctx.managed_state.get("completed", 0) + 1
    ctx.managed_state["last_sequence"] = ctx.input_sequence
    if False:  # keep this a generator while intentionally emitting no output
        yield


PASSTHROUGH_ARTIFACT = "ariadne.demo/passthrough-v1"
UPPERCASE_ARTIFACT = "ariadne.demo/uppercase-v1"
RETURN_ONLY_ARTIFACT = "ariadne.demo/return-only-v1"
PASSTHROUGH_DIGEST = register_operator_artifact(
    PASSTHROUGH_ARTIFACT, prefect_passthrough
)
UPPERCASE_DIGEST = register_operator_artifact(UPPERCASE_ARTIFACT, prefect_uppercase)
RETURN_ONLY_DIGEST = register_operator_artifact(
    RETURN_ONLY_ARTIFACT, prefect_return_only
)


@task(name="durable-stream-source", persist_result=False)
async def prefect_source_actor(
    config_data: dict[str, Any],
    attempt_id: str,
    documents: list[dict[str, Any]],
) -> dict[str, Any]:
    config = PrefectStreamAdapterConfig(**config_data)
    config.validate()
    stream, _, control, attempts, revision = await _open(config)
    try:
        await _validate_capacity_profile(stream, revision)
        await _validate_maintenance_profile(stream, revision, config, documents)
        admin_token = _secret(config.admin_token_env)
        source_token = _secret(config.source_token_env)
        task_run_id = str(get_run_context().task_run.id)
        ADAPTER_TELEMETRY.event(
            "source_actor_started",
            {
                "event.type": "source_actor_started",
                "stream.run_id": config.control_run_id,
                "stream.attempt_id": attempt_id,
                "prefect.flow_run_id": str(get_run_context().task_run.flow_run_id),
                "prefect.task_run_id": task_run_id,
            },
        )
        await attempts.reconnect_attempt(
            admin_token,
            control_run_id=config.control_run_id,
            attempt_id=attempt_id,
            ordinal=(
                await attempts.project_attempt(
                    admin_token, control_run_id=config.control_run_id
                )
            )["current"]["ordinal"],
            prefect_flow_run_id=str(get_run_context().task_run.flow_run_id),
            graph_digest=config.graph_digest,
            code_version=config.code_version,
        )
        source = revision.source_manifest[0]
        producer_id = config.source_producer_id
        published = 0
        for document in documents:
            sequence = document.get("producer_sequence")
            record_id = document.get("record_id")
            payload = document.get("payload")
            if (
                isinstance(sequence, bool)
                or not isinstance(sequence, int)
                or sequence <= 0
                or not isinstance(record_id, str)
                or not record_id
                or not isinstance(payload, bytes)
            ):
                raise ValueError(
                    "source document requires stable sequence, ID and bytes"
                )
            while True:
                try:
                    admission = await control.admit_source(
                        source_token,
                        run_id=config.control_run_id,
                        request_id=f"prefect-admit:{config.control_run_id}:{sequence}",
                        channel=source["channel"],
                        partition=source["partition"],
                        record_id=record_id,
                        attempt_id=f"prefect-append:{config.control_run_id}:{sequence}",
                        object_key=content_object_key(payload, "bytes/v1"),
                        payload_digest=hashlib.sha256(payload).hexdigest(),
                        payload_size=len(payload),
                        schema_id="bytes/v1",
                        lineage_digest=document.get("lineage_digest", record_id),
                        producer_id=producer_id,
                        producer_sequence=sequence,
                    )
                    await control.publish_admission(
                        source_token,
                        run_id=config.control_run_id,
                        admission_id=admission["admission_id"],
                        payload=payload,
                    )
                    break
                except (
                    CapacityBlocked,
                    ControlCapacityBlocked,
                    ControlConflict,
                ) as exc:
                    projection = await control.project_run(
                        admin_token, run_id=config.control_run_id
                    )
                    lifecycle = projection["control"]["lifecycle"]
                    if lifecycle == "CANCELLED":
                        return Cancelled(
                            message="durable stream source cancelled",
                            data={"attempt_id": attempt_id, "published": published},
                        )
                    if lifecycle in {"DRAINING", "DRAINED"}:
                        break
                    if not isinstance(exc, (CapacityBlocked, ControlCapacityBlocked)):
                        raise
                    await asyncio.sleep(0.02)
                continue
            else:  # pragma: no cover - loop exits through break or task return
                raise AssertionError("unreachable source admission loop")
            projection = await control.project_run(
                admin_token, run_id=config.control_run_id
            )
            if projection["control"]["lifecycle"] in {
                "DRAINING",
                "DRAINED",
                "CANCELLED",
            }:
                break
            published += 1
            if config.source_delay_seconds:
                await asyncio.sleep(config.source_delay_seconds)
        while config.source_keep_open:
            projection = await control.project_run(
                admin_token, run_id=config.control_run_id
            )
            lifecycle = projection["control"]["lifecycle"]
            if lifecycle == "CANCELLED":
                return Cancelled(
                    message="durable stream source cancelled",
                    data={"attempt_id": attempt_id, "published": published},
                )
            if lifecycle in {"DRAINING", "DRAINED"}:
                break
            await asyncio.sleep(0.05)
        ADAPTER_TELEMETRY.event(
            "source_actor_completed",
            {
                "event.type": "source_actor_completed",
                "stream.run_id": config.control_run_id,
                "stream.attempt_id": attempt_id,
                "prefect.task_run_id": task_run_id,
            },
        )
        return {"attempt_id": attempt_id, "published": published}
    finally:
        await stream.close()


@task(
    name="durable-stream-operator",
    retries=1,
    retry_delay_seconds=0,
    persist_result=False,
)
async def prefect_operator_actor(
    config_data: dict[str, Any],
    attempt_id: str,
    participant_id: str,
) -> dict[str, Any] | Cancelled:
    config = PrefectStreamAdapterConfig(**config_data)
    config.validate()
    stream, coordinator, control, attempts, revision = await _open(config)
    lease_stopped = asyncio.Event()
    lease_heartbeat: asyncio.Task[None] | None = None
    lease_errors: list[BaseException] = []
    try:
        admin_token = _secret(config.admin_token_env)
        task_run = get_run_context().task_run
        artifact_id = config.operator_artifacts.get(participant_id)
        if artifact_id is None:
            raise FeatureDisabled("operator artifact manifest is incomplete")
        artifact_digest, operator_function = resolve_operator_artifact(artifact_id)
        participant = next(
            item
            for item in revision.participant_manifest
            if item["participant_id"] == participant_id
        )
        if participant["code_digest"] != artifact_digest:
            raise FeatureDisabled("operator artifact code identity drift")
        task_run_id = str(task_run.id)
        actor_attributes = {
            "stream.run_id": config.control_run_id,
            "stream.revision_id": config.revision_id,
            "stream.attempt_id": attempt_id,
            "stream.participant_id": participant_id,
            "prefect.flow_run_id": str(task_run.flow_run_id),
            "prefect.task_run_id": task_run_id,
        }
        initial_control = await control.project_run(
            admin_token, run_id=config.control_run_id
        )
        if initial_control["control"]["lifecycle"] == "CANCELLED":
            return Cancelled(
                message="durable stream run cancelled",
                data={"attempt_id": attempt_id, "processed": 0},
            )
        if config.fault_participant_once == participant_id and task_run.run_count == 1:
            ADAPTER_TELEMETRY.event(
                "operator_fault_injected",
                {**actor_attributes, "event.type": "operator_fault_injected"},
            )
            raise RuntimeError("deterministic operator process-loss control")
        projection = await attempts.project_attempt(
            admin_token, control_run_id=config.control_run_id
        )
        binding = next(
            item
            for item in projection["operators"]
            if item["participant_id"] == participant_id
        )
        if task_run.run_count > 1:
            with ADAPTER_TELEMETRY.span("stream.operator.rebind", actor_attributes):
                binding = await attempts.replace_operator(
                    admin_token,
                    control_run_id=config.control_run_id,
                    attempt_id=attempt_id,
                    participant_id=participant_id,
                    prefect_task_run_id=task_run_id,
                    owner_id=f"prefect:{task_run_id}:retry:{task_run.run_count}",
                    reason="PREFECT_TASK_RETRY",
                )
            ADAPTER_TELEMETRY.event(
                "operator_rebound",
                {**actor_attributes, "event.type": "operator_rebound"},
            )
        else:
            binding = await attempts.bind_operator_task(
                admin_token,
                control_run_id=config.control_run_id,
                attempt_id=attempt_id,
                participant_id=participant_id,
                prefect_task_run_id=task_run_id,
            )
        runtime = PostgresJoinRuntime(coordinator, revision, participant_id)
        binding = await attempts.renew_operator_lease(
            admin_token,
            control_run_id=config.control_run_id,
            attempt_id=attempt_id,
            participant_id=participant_id,
            prefect_task_run_id=task_run_id,
            owner_id=binding["owner_id"],
            lease_seconds=config.operator_lease_seconds,
        )

        async def heartbeat_operator() -> None:
            nonlocal binding
            try:
                while not lease_stopped.is_set():
                    try:
                        await asyncio.wait_for(
                            lease_stopped.wait(), config.operator_lease_seconds / 3
                        )
                    except TimeoutError:
                        binding = await attempts.renew_operator_lease(
                            admin_token,
                            control_run_id=config.control_run_id,
                            attempt_id=attempt_id,
                            participant_id=participant_id,
                            prefect_task_run_id=task_run_id,
                            owner_id=binding["owner_id"],
                            lease_seconds=config.operator_lease_seconds,
                        )
            except Exception as exc:  # noqa: BLE001 - actor observes exact failure
                lease_errors.append(exc)
                lease_stopped.set()

        lease_heartbeat = asyncio.create_task(heartbeat_operator())
        if config.operator_start_delay_seconds:
            await asyncio.sleep(config.operator_start_delay_seconds)
        ADAPTER_TELEMETRY.event(
            "operator_actor_started",
            {**actor_attributes, "event.type": "operator_actor_started"},
        )
        inputs = next(
            item["inputs"]
            for item in revision.participant_manifest
            if item["participant_id"] == participant_id
        )
        idle = 0
        processed = 0
        while True:
            if lease_errors:
                failed_control = await control.project_run(
                    admin_token, run_id=config.control_run_id
                )
                if failed_control["control"]["lifecycle"] == "CANCELLED":
                    return Cancelled(
                        message="durable stream run cancelled",
                        data={"attempt_id": attempt_id, "processed": processed},
                    )
                raise lease_errors[0]
            result_seen = False
            for source in inputs:
                async with stream.pool.acquire() as conn:
                    coordinator_lease = await conn.fetchrow(
                        "SELECT owner_id,owner_epoch FROM coordinator_leases "
                        "WHERE revision_id=$1",
                        config.revision_id,
                    )
                if (
                    coordinator_lease is None
                    or coordinator_lease["owner_id"] != config.coordinator_owner
                ):
                    raise FeatureDisabled("checkpoint coordinator identity drift")
                try:
                    result = await runtime.step(
                        f"{source['channel']}/{source['partition']}",
                        operator_function,
                        binding["owner_id"],
                        binding["owner_epoch"],
                        coordinator_owner=config.coordinator_owner,
                        coordinator_epoch=coordinator_lease["owner_epoch"],
                    )
                except Exception:
                    failed_control = await control.project_run(
                        admin_token, run_id=config.control_run_id
                    )
                    if failed_control["control"]["lifecycle"] == "CANCELLED":
                        return Cancelled(
                            message="durable stream run cancelled",
                            data={
                                "attempt_id": attempt_id,
                                "processed": processed,
                            },
                        )
                    raise
                if result not in {"EMPTY", "WAITING"}:
                    result_seen = True
                if result in {"APPLIED", "DRAINED"}:
                    processed += 1
                    async with stream.pool.acquire() as conn:
                        terminal = await conn.fetchrow(
                            "SELECT t.terminal,r.channel,r.partition_id,"
                            "r.data_sequence,t.writer_epoch "
                            "FROM join_turns_v3 t JOIN join_receipts_v3 r "
                            "USING(revision_id,participant_id,arrival_ordinal) "
                            "WHERE t.revision_id=$1 AND t.participant_id=$2 "
                            "ORDER BY t.apply_ordinal DESC LIMIT 1",
                            config.revision_id,
                            participant_id,
                        )
                    if terminal is None:
                        # Telemetry is a projection of durable execution, never
                        # an execution authority.  A missing post-commit read is
                        # observable but cannot roll back or fail the turn.
                        ADAPTER_TELEMETRY.event(
                            "terminal_observation_unavailable",
                            {
                                **actor_attributes,
                                "event.type": "terminal_observation_unavailable",
                                "error.type": "TerminalObservationUnavailable",
                            },
                        )
                        continue
                    terminal_attributes = {
                        **actor_attributes,
                        "stream.owner_id": binding["owner_id"],
                        "stream.owner_epoch": terminal["writer_epoch"],
                        "stream.input.channel": terminal["channel"],
                        "stream.input.partition": terminal["partition_id"],
                        "stream.input.sequence": terminal["data_sequence"],
                        "event.type": "terminal_" + terminal["terminal"].lower(),
                    }
                    with ADAPTER_TELEMETRY.span(
                        "stream.operator.terminal", terminal_attributes
                    ):
                        ADAPTER_TELEMETRY.event(
                            "terminal_" + terminal["terminal"].lower(),
                            terminal_attributes,
                        )
            control_projection = await control.project_run(
                admin_token, run_id=config.control_run_id
            )
            lifecycle = control_projection["control"]["lifecycle"]
            if lifecycle == "CANCELLED":
                ADAPTER_TELEMETRY.event(
                    "operator_actor_cancelled",
                    {**actor_attributes, "event.type": "operator_actor_cancelled"},
                )
                return Cancelled(
                    message="durable stream run cancelled",
                    data={"attempt_id": attempt_id, "processed": processed},
                )
            if lifecycle == "DRAINED":
                ADAPTER_TELEMETRY.event(
                    "operator_actor_drained",
                    {**actor_attributes, "event.type": "operator_actor_drained"},
                )
                return {"attempt_id": attempt_id, "processed": processed}
            if result_seen:
                idle = 0
            else:
                idle += 1
                await asyncio.sleep(min(0.05, 0.005 * idle))
    finally:
        lease_stopped.set()
        if lease_heartbeat is not None:
            await lease_heartbeat
        await stream.close()


@task(
    name="durable-stream-maintenance",
    retries=2,
    retry_delay_seconds=0,
    persist_result=False,
)
async def prefect_maintenance_actor(
    config_data: dict[str, Any], attempt_id: str
) -> dict[str, Any]:
    config = PrefectStreamAdapterConfig(**config_data)
    config.validate()
    stream, coordinator, control, _, revision = await _open(config)
    try:
        admin_token = _secret(config.admin_token_env)
        task_run_count = get_run_context().task_run.run_count

        def fault(stage: str) -> None:
            if os.environ.get("ARIADNE_TEST_MAINTENANCE_EXIT_STAGE") == stage:
                os._exit(86)
            expected = {1: "after_barrier", 2: "after_checkpoint"}
            if (
                config.fault_maintenance_retries
                and expected.get(task_run_count) == stage
            ):
                raise RuntimeError("deterministic maintenance process-loss control")

        maintenance = PostgresPrefectMaintenance(stream, coordinator, control, revision)
        result = await maintenance.run(
            admin_token=admin_token,
            control_run_id=config.control_run_id,
            source_producer_id=config.source_producer_id,
            coordinator_owner=config.coordinator_owner,
            coordinator_epoch=config.coordinator_epoch,
            coordinator_lease_seconds=config.coordinator_lease_seconds,
            leader_id=config.leader_id,
            leader_epoch=config.leader_epoch,
            checkpoint_interval_records=config.checkpoint_interval_records,
            max_checkpoint_delay_seconds=config.max_checkpoint_delay_seconds,
            poll_seconds=config.maintenance_poll_seconds,
            fault=fault,
        )
        return {
            "attempt_id": attempt_id,
            "checkpoints_completed": result.checkpoints_completed,
            "last_source_cut": result.last_source_cut,
        }
    finally:
        await stream.close()


@flow(name="durable-stream-flow", persist_result=False)
async def prefect_stream_flow(
    config_data: dict[str, Any], documents: list[dict[str, Any]]
) -> dict[str, Any] | Cancelled:
    config = PrefectStreamAdapterConfig(**config_data)
    config.validate()
    stream, _, control, attempts, revision = await _open(config)
    try:
        await _validate_capacity_profile(stream, revision)
        await _validate_maintenance_profile(stream, revision, config, documents)
        admin_token = _secret(config.admin_token_env)
        source_token = _secret(config.source_token_env)
        flow_run_id = str(get_run_context().flow_run.id)
        attempt_attributes = {
            "stream.run_id": config.control_run_id,
            "stream.revision_id": config.revision_id,
            "prefect.flow_run_id": flow_run_id,
        }
        source = revision.source_manifest[0]
        source_bindings = [
            SourceBinding(
                source["channel"],
                source["partition"],
                config.source_producer_id,
            )
        ]
        operator_bindings = []
        for item in revision.participant_manifest:
            participant_id = item["participant_id"]
            artifact_id = config.operator_artifacts.get(participant_id)
            if artifact_id is None:
                raise FeatureDisabled("operator artifact manifest is incomplete")
            artifact_digest, _ = resolve_operator_artifact(artifact_id)
            if artifact_digest != item["code_digest"]:
                raise FeatureDisabled("operator artifact code identity drift")
            operator_bindings.append(
                OperatorBinding(
                    participant_id,
                    f"prefect:{flow_run_id}:{participant_id}:run:1",
                    artifact_id=artifact_id,
                    artifact_digest=artifact_digest,
                )
            )
    except BaseException:
        await stream.close()
        raise
    heartbeat_stopped = asyncio.Event()

    async def heartbeat_control_leader() -> None:
        while not heartbeat_stopped.is_set():
            lease = await control.claim_leader(
                admin_token,
                deployment_id=config.deployment_id,
                leader_id=config.leader_id,
                lease_seconds=config.leader_lease_seconds,
            )
            if lease.leader_epoch != config.leader_epoch:
                raise RuntimeError("control leader epoch changed")
            try:
                await asyncio.wait_for(
                    heartbeat_stopped.wait(), config.leader_lease_seconds / 3
                )
            except TimeoutError:
                continue

    heartbeat = asyncio.create_task(heartbeat_control_leader())
    try:
        with ADAPTER_TELEMETRY.span("stream.attempt.activate", attempt_attributes):
            if config.replace_attempt_id is None:
                attempt = await attempts.activate_attempt(
                    admin_token,
                    control_run_id=config.control_run_id,
                    prefect_flow_run_id=flow_run_id,
                    graph_digest=config.graph_digest,
                    code_version=config.code_version,
                    source_token=source_token,
                    source_bindings=source_bindings,
                    operator_bindings=operator_bindings,
                )
            else:
                attempt = await attempts.replace_execution(
                    admin_token,
                    control_run_id=config.control_run_id,
                    expected_attempt_id=config.replace_attempt_id,
                    prefect_flow_run_id=flow_run_id,
                    graph_digest=config.graph_digest,
                    code_version=config.code_version,
                    source_token=source_token,
                    source_bindings=source_bindings,
                    operator_bindings=operator_bindings,
                    reason="PREFECT_INFRASTRUCTURE_REPLACED",
                )
        ADAPTER_TELEMETRY.event(
            "attempt_activated",
            {
                **attempt_attributes,
                "event.type": "attempt_activated",
                "stream.attempt_id": attempt.attempt_id,
                "stream.attempt_epoch": attempt.attempt_epoch,
            },
        )
        source_future = prefect_source_actor.submit(
            asdict(config), attempt.attempt_id, documents
        )
        operator_futures = [
            prefect_operator_actor.submit(
                asdict(config), attempt.attempt_id, item["participant_id"]
            )
            for item in revision.participant_manifest
        ]
        maintenance_future = (
            prefect_maintenance_actor.submit(asdict(config), attempt.attempt_id)
            if config.maintenance_enabled
            else None
        )

        def raise_actor_failure() -> None:
            if heartbeat.done():
                heartbeat.result()
            for actor_future in operator_futures:
                actor_state = actor_future.state
                if actor_state is not None and (
                    actor_state.is_failed() or actor_state.is_crashed()
                ):
                    actor_future.result()
            if maintenance_future is not None:
                maintenance_state = maintenance_future.state
                if maintenance_state is not None and (
                    maintenance_state.is_failed() or maintenance_state.is_crashed()
                ):
                    maintenance_future.result()

        while source_future.state is None or not source_future.state.is_final():
            raise_actor_failure()
            await asyncio.sleep(0.02)
        if heartbeat.done():
            heartbeat.result()
        control_after_source = await control.project_run(
            admin_token, run_id=config.control_run_id
        )
        source_cancelled = source_future.state is not None and (
            source_future.state.is_cancelled()
        )
        if source_cancelled or (
            control_after_source["control"]["lifecycle"] == "CANCELLED"
        ):
            terminal = "CANCELLED"
        elif source_future.state is not None and (
            source_future.state.is_failed() or source_future.state.is_crashed()
        ):
            source_future.result()
            raise AssertionError("unreachable source failure")
        elif config.terminal_mode == "DRAIN":
            if control_after_source["control"]["lifecycle"] == "ACTIVE":
                if maintenance_future is not None:
                    while True:
                        async with stream.pool.acquire() as conn:
                            progress = await conn.fetchval(
                                "SELECT next_sequence-1 FROM partitions "
                                "WHERE channel=$1 AND partition_id=$2",
                                source["channel"],
                                source["partition"],
                            )
                            checkpoint_cut = await conn.fetchval(
                                "SELECT COALESCE(max(o.cut_data_sequence),0) "
                                "FROM source_barrier_observations o "
                                "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                                "JOIN checkpoint_intents_v2 i USING(checkpoint_id) "
                                "WHERE i.revision_id=$1 AND o.channel=$2 "
                                "AND o.partition_id=$3 AND l.status='COMPLETED'",
                                config.revision_id,
                                source["channel"],
                                source["partition"],
                            )
                        if (
                            progress - checkpoint_cut
                            < config.checkpoint_interval_records
                        ):
                            break
                        raise_actor_failure()
                        await asyncio.sleep(config.maintenance_poll_seconds)
                await control.drain_run(
                    admin_token,
                    run_id=config.control_run_id,
                    request_id=f"prefect-drain:{attempt.attempt_id}",
                    leader_id=config.leader_id,
                    leader_epoch=config.leader_epoch,
                )
            while True:
                raise_actor_failure()
                state = await control.reconcile_run(
                    admin_token,
                    run_id=config.control_run_id,
                    leader_id=config.leader_id,
                    leader_epoch=config.leader_epoch,
                )
                if state["lifecycle"] == "DRAINED":
                    break
                await asyncio.sleep(0.01)
            terminal = "DRAINED"
        else:
            await control.cancel_run(
                admin_token,
                run_id=config.control_run_id,
                request_id=f"prefect-cancel:{attempt.attempt_id}",
                leader_id=config.leader_id,
                leader_epoch=config.leader_epoch,
            )
            terminal = "CANCELLED"
        for future in operator_futures:
            await asyncio.to_thread(future.wait)
        if maintenance_future is not None:
            await asyncio.to_thread(maintenance_future.wait)
            maintenance_future.result()
        await attempts.finish_attempt(
            admin_token,
            control_run_id=config.control_run_id,
            attempt_id=attempt.attempt_id,
            terminal_status=terminal,
        )
        if terminal == "CANCELLED":
            return Cancelled(
                message="durable stream flow cancelled",
                data={"control_run_id": config.control_run_id},
            )
        return {
            "control_run_id": config.control_run_id,
            "attempt_id": attempt.attempt_id,
            "source_task_run_id": str(source_future.task_run_id),
            "operator_task_run_ids": [
                str(future.task_run_id) for future in operator_futures
            ],
            "maintenance_task_run_id": (
                str(maintenance_future.task_run_id)
                if maintenance_future is not None
                else None
            ),
        }
    except BaseException:
        projection = await attempts.project_attempt(
            admin_token, control_run_id=config.control_run_id
        )
        if projection["current"]["status"] == "ACTIVE":
            control_projection = await control.project_run(
                admin_token, run_id=config.control_run_id
            )
            if control_projection["control"]["lifecycle"] not in {
                "CANCELLED",
                "DRAINED",
            }:
                await control.cancel_run(
                    admin_token,
                    run_id=config.control_run_id,
                    request_id=f"prefect-failure:{projection['current']['attempt_id']}",
                    leader_id=config.leader_id,
                    leader_epoch=config.leader_epoch,
                )
            await attempts.finish_attempt(
                admin_token,
                control_run_id=config.control_run_id,
                attempt_id=projection["current"]["attempt_id"],
                terminal_status="FAILED",
            )
        raise
    finally:
        heartbeat_stopped.set()
        if not heartbeat.done():
            await heartbeat
        await stream.close()
