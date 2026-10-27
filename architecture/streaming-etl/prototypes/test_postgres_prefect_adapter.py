from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
import os
import socket
import subprocess
import tempfile
import unittest
import uuid
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path

import asyncpg
import httpx
from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_checkpoint_retention import PostgresCheckpointRetention
from postgres_control_plane import AuthenticationFailed, PostgresControlPlane
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_retention import PostgresJoinRetention
from postgres_join_runtime import PostgresJoinRuntime
from postgres_prefect_attempts import (
    AttemptHorizonExpired,
    FeatureDisabled,
    OperatorBinding,
    PostgresPrefectAttempts,
    RestoringSwapUnsupported,
    ResumeIdentityMismatch,
    SourceBinding,
)
from postgres_prefect_maintenance_oracle import evaluate as evaluate_maintenance
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_stream_observability import PrefectStateReader
from postgres_stream_v2 import PostgresStream, StaleOwner, content_object_key
from postgres_streaming_retention import PostgresStreamingRetention
from prefect_stream_adapter import (
    ADAPTER_TELEMETRY,
    RETURN_ONLY_ARTIFACT,
    RETURN_ONLY_DIGEST,
    UPPERCASE_ARTIFACT,
    UPPERCASE_DIGEST,
    PrefectStreamAdapterConfig,
    _validate_capacity_profile,
    _validate_maintenance_profile,
    prefect_passthrough,
    prefect_return_only,
    prefect_stream_flow,
    prefect_uppercase,
)

from prefect.client.orchestration import get_client
from prefect.events.worker import EventsWorker
from prefect.exceptions import ObjectNotFound
from prefect.filesystems import LocalFileSystem
from prefect.logging.handlers import APILogWorker
from prefect.settings import temporary_settings
from prefect.states import Crashed, StateType

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
HERE = Path(__file__).resolve().parent
RUNNER = HERE / "prefect_stream_adapter_runner.py"

STARTED: asyncio.Event | None = None
RELEASE: asyncio.Event | None = None


@stream_task
async def blocking_document(ctx, payload):
    assert STARTED is not None and RELEASE is not None
    STARTED.set()
    await RELEASE.wait()
    yield OutputSpec(
        payload,
        "bytes/v1",
        {"completed": ctx.managed_state.get("completed", 0) + 1},
    )


class PostgresPrefectAttemptTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds041_" + uuid.uuid4().hex[:12]
        self.stream = await PostgresStream.open(DSN, self.schema)
        for channel in ("input", "output-b", "output-c"):
            await self.stream.create_partition(
                channel, "p0", max_records=4, max_bytes=1_000_000
            )
        await self.stream.register_producer("input", "p0", "stable-source")
        self.coordinator = await PostgresBarrierCoordinator.install(self.stream)
        for channel in ("input", "output-b", "output-c"):
            await self.coordinator.configure_ordered_log_capacity(channel, "p0", 1000)
        participants = [
            {
                "participant_id": participant,
                "inputs": [{"channel": "input", "partition": "p0"}],
                "outputs": [{"channel": f"output-{participant}", "partition": "p0"}],
                "code_digest": (
                    RETURN_ONLY_DIGEST if participant == "b" else UPPERCASE_DIGEST
                ),
                "state_schema": "count/v1",
                "state_version": 1,
            }
            for participant in ("b", "c")
        ]
        self.revision = await self.coordinator.register_revision(
            revision_id="prefect-revision",
            deployment_id="prefect-deployment",
            graph_version="prefect-v1",
            sources=[("input", "p0")],
            participants=participants,
            code_manifest_digest="prefect-code-v1",
            state_contract_digest="prefect-state-v1",
        )
        self.runtimes = {
            participant: PostgresJoinRuntime(
                self.coordinator, self.revision, participant
            )
            for participant in ("b", "c")
        }
        for runtime in self.runtimes.values():
            await runtime.install(initial_state={"completed": 0})
        planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.coordinator, planner, self.revision
        )
        await regional.bind_revision_topology()
        self.regional = regional
        self.join_retention = await PostgresJoinRetention.install(self.coordinator)
        self.checkpoint_retention = await PostgresCheckpointRetention.install(
            self.coordinator
        )
        self.streaming_retention = await PostgresStreamingRetention.install(
            self.stream, self.coordinator
        )
        self.control = await PostgresControlPlane.install(self.stream)
        self.admin_token = "prefect-admin-token"
        await self.control.provision_tenant(
            "tenant-prefect",
            self.admin_token,
            {"deploy", "run", "read", "control"},
        )
        await self.control.register_deployment(
            self.admin_token,
            deployment_id="prefect-deployment",
            revision_id="prefect-revision",
            graph_version="prefect-v1",
            graph_digest=self.revision.graph_digest,
        )
        self.leader = await self.control.claim_leader(
            self.admin_token,
            deployment_id="prefect-deployment",
            leader_id="prefect-controller",
            lease_seconds=300,
        )
        self.run = await self.control.create_run(
            self.admin_token,
            deployment_id="prefect-deployment",
            request_id="prefect-run",
            consumer_ids=[],
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
            max_admission_records=4,
            max_admission_bytes=1_000_000,
        )
        self.run_id = self.run["run_id"]
        self.attempts = await PostgresPrefectAttempts.install(self.control)
        self.coordinator_epoch = await self.coordinator.claim_coordinator(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            "checkpoint-controller",
            lease_seconds=300,
        )

    async def asyncTearDown(self):
        if not hasattr(self, "stream"):
            return
        await self.stream.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        finally:
            await conn.close()

    def sources(self):
        return [SourceBinding("input", "p0", "stable-source")]

    @staticmethod
    def operators(prefix: str):
        return [
            OperatorBinding(
                participant,
                f"{prefix}-{participant}",
                artifact_id=(
                    RETURN_ONLY_ARTIFACT if participant == "b" else UPPERCASE_ARTIFACT
                ),
                artifact_digest=(
                    RETURN_ONLY_DIGEST if participant == "b" else UPPERCASE_DIGEST
                ),
            )
            for participant in ("b", "c")
        ]

    async def activate(
        self,
        number: int = 1,
        *,
        max_hot_attempts: int = 2,
    ):
        return await self.attempts.activate_attempt(
            self.admin_token,
            control_run_id=self.run_id,
            prefect_flow_run_id=f"flow-{number}",
            graph_digest=self.revision.graph_digest,
            code_version="adapter-v1",
            source_token=f"source-cap-{number}",
            source_bindings=self.sources(),
            operator_bindings=self.operators(f"attempt-{number}"),
            max_hot_attempts=max_hot_attempts,
        )

    async def replace(self, old, number: int):
        return await self.attempts.replace_execution(
            self.admin_token,
            control_run_id=self.run_id,
            expected_attempt_id=old.attempt_id,
            prefect_flow_run_id=f"flow-{number}",
            graph_digest=self.revision.graph_digest,
            code_version="adapter-v1",
            source_token=f"source-cap-{number}",
            source_bindings=self.sources(),
            operator_bindings=self.operators(f"attempt-{number}"),
            reason="INFRASTRUCTURE_REPLACED",
        )

    async def append(self, token: str, number: int):
        payload = f"document-{number}".encode()
        admission = await self.control.admit_source(
            token,
            run_id=self.run_id,
            request_id=f"admit-{number}",
            channel="input",
            partition="p0",
            record_id=f"document-{number}",
            attempt_id=f"append-{number}",
            object_key=content_object_key(payload, "bytes/v1"),
            payload_digest=hashlib.sha256(payload).hexdigest(),
            payload_size=len(payload),
            schema_id="bytes/v1",
            lineage_digest=f"lineage-{number}",
            producer_id="stable-source",
            producer_sequence=number,
        )
        return await self.control.publish_admission(
            token,
            run_id=self.run_id,
            admission_id=admission["admission_id"],
            payload=payload,
        )

    async def start_prefect_server(self):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()
        home = tempfile.TemporaryDirectory()
        env = os.environ.copy()
        env.update(
            {
                "PREFECT_HOME": home.name,
                "PREFECT_API_URL": f"http://127.0.0.1:{port}/api",
                "PREFECT_SERVER_ANALYTICS_ENABLED": "false",
            }
        )
        process = await asyncio.create_subprocess_exec(
            str(HERE.parents[2] / ".venv" / "bin" / "prefect"),
            "server",
            "start",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--no-ui",
            "--analytics-off",
            cwd=HERE,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.STDOUT,
        )
        health = f"http://127.0.0.1:{port}/api/health"

        healthy = False
        async with httpx.AsyncClient(timeout=0.2, trust_env=False) as client:
            for _ in range(600):
                if process.returncode is not None:
                    break
                try:
                    response = await client.get(health)
                    response.raise_for_status()
                    healthy = True
                    break
                except httpx.HTTPError:
                    await asyncio.sleep(0.05)
        if not healthy and process.returncode is None:
            process.terminate()
            await process.wait()
            home.cleanup()
            raise AssertionError("Prefect API did not start")
        if process.returncode is not None:
            home.cleanup()
            raise AssertionError("Prefect API exited before health check")
        previous = {
            key: os.environ.get(key)
            for key in (
                "PREFECT_HOME",
                "PREFECT_API_URL",
                "PREFECT_SERVER_ANALYTICS_ENABLED",
                "DS041_DSN",
                "DS041_ADMIN",
                "DS041_SOURCE",
            )
        }
        os.environ.update(
            {
                "PREFECT_HOME": home.name,
                "PREFECT_API_URL": env["PREFECT_API_URL"],
                "PREFECT_SERVER_ANALYTICS_ENABLED": "false",
                "DS041_DSN": DSN,
                "DS041_ADMIN": self.admin_token,
                "DS041_SOURCE": "source-cap-prefect",
            }
        )
        settings = ExitStack()
        settings.enter_context(
            temporary_settings(updates={"PREFECT_API_URL": env["PREFECT_API_URL"]})
        )
        try:
            storage_name = f"stream-acceptance-{port}"
            await LocalFileSystem(basepath=home.name + "/results").asave(
                storage_name, overwrite=True
            )
            # Terminal stream outputs live in PostgreSQL. An explicit public
            # SDK storage setting also avoids repeated automatic discovery
            # through a sync HTTP client that the selected SDK leaves open.
            settings.enter_context(
                temporary_settings(
                    updates={
                        "PREFECT_RESULTS_DEFAULT_STORAGE_BLOCK": "local-file-system/"
                        + storage_name
                    }
                )
            )
        except BaseException:
            await self.stop_prefect_server((process, home, previous, settings))
            raise
        return process, home, previous, settings

    @staticmethod
    async def stop_prefect_server(resources):
        process, home, previous, settings = resources
        try:
            drained_logs = APILogWorker.instance().drain()
            if inspect.isawaitable(drained_logs):
                await drained_logs
        except RuntimeError:
            pass
        drained_events = EventsWorker.drain_all()
        if inspect.isawaitable(drained_events):
            await drained_events
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 5)
        except TimeoutError:
            process.kill()
            await process.wait()
        home.cleanup()
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        settings.__exit__(None, None, None)

    def adapter_config(
        self,
        terminal_mode="DRAIN",
        delay=0.0,
        keep_source_open=False,
        operator_delay=0.0,
        operator_lease=30.0,
        maintenance=False,
        checkpoint_interval=2,
        max_checkpoint_delay=5.0,
        coordinator_lease=60.0,
        fault_maintenance_retries=False,
        fault_participant_once=None,
    ):
        return PrefectStreamAdapterConfig(
            enabled=True,
            dsn_env="DS041_DSN",
            schema=self.schema,
            admin_token_env="DS041_ADMIN",
            source_token_env="DS041_SOURCE",
            control_run_id=self.run_id,
            revision_id=self.revision.revision_id,
            deployment_id=self.revision.deployment_id,
            graph_version=self.revision.graph_version,
            graph_digest=self.revision.graph_digest,
            code_version="adapter-v1",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
            coordinator_owner="checkpoint-controller",
            coordinator_epoch=self.coordinator_epoch,
            source_producer_id="stable-source",
            operator_artifacts={
                "b": RETURN_ONLY_ARTIFACT,
                "c": UPPERCASE_ARTIFACT,
            },
            source_delay_seconds=delay,
            source_keep_open=keep_source_open,
            operator_start_delay_seconds=operator_delay,
            operator_lease_seconds=operator_lease,
            maintenance_enabled=maintenance,
            checkpoint_interval_records=checkpoint_interval,
            max_checkpoint_delay_seconds=max_checkpoint_delay,
            coordinator_lease_seconds=coordinator_lease,
            fault_maintenance_retries=fault_maintenance_retries,
            fault_participant_once=fault_participant_once,
            terminal_mode=terminal_mode,
        )

    @staticmethod
    async def read_prefect_tasks(client, task_ids):
        for _ in range(100):
            try:
                return [
                    await client.read_task_run(uuid.UUID(task_id))
                    for task_id in task_ids
                ]
            except ObjectNotFound:
                await asyncio.sleep(0.1)
        raise AssertionError("Prefect task runs were not published to the API")

    @staticmethod
    def prefect_documents(*payloads: bytes, start: int = 1):
        return [
            {
                "producer_sequence": sequence,
                "record_id": f"prefect-document-{sequence}",
                "lineage_digest": f"prefect-lineage-{sequence}",
                "payload": payload,
            }
            for sequence, payload in enumerate(payloads, start=start)
        ]

    async def test_attempt_takeover_fences_source_and_inflight_terminal(self):
        first = await self.activate()
        await self.append("source-cap-1", 1)
        projection = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        b = next(x for x in projection["operators"] if x["participant_id"] == "b")
        global STARTED, RELEASE
        STARTED = asyncio.Event()
        RELEASE = asyncio.Event()
        old_step = asyncio.create_task(
            self.runtimes["b"].step(
                "input/p0",
                blocking_document,
                b["owner_id"],
                b["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.coordinator_epoch,
            )
        )
        await asyncio.wait_for(STARTED.wait(), 5)
        second = await self.replace(first, 2)
        with self.assertRaises(AuthenticationFailed):
            await self.control.admit_source(
                "source-cap-1",
                run_id=self.run_id,
                request_id="old-source",
                channel="input",
                partition="p0",
                record_id="old-source",
                attempt_id="old-source",
                object_key=content_object_key(b"old", "bytes/v1"),
                payload_digest=hashlib.sha256(b"old").hexdigest(),
                payload_size=3,
                schema_id="bytes/v1",
                lineage_digest="old-source",
                producer_id="stable-source",
                producer_sequence=2,
            )
        RELEASE.set()
        with self.assertRaises(StaleOwner):
            await old_step
        retried = await self.append("source-cap-2", 1)
        self.assertEqual(retried["sequence"], 1)
        await self.append("source-cap-2", 2)
        self.assertEqual(second.ordinal, 2)

    async def test_feature_flag_graph_and_resume_identity_fail_before_data(self):
        disabled = self.adapter_config()
        disabled = PrefectStreamAdapterConfig(**{**asdict(disabled), "enabled": False})
        with self.assertRaises(FeatureDisabled):
            disabled.validate()
        with self.assertRaises(ResumeIdentityMismatch):
            await self.attempts.activate_attempt(
                self.admin_token,
                control_run_id=self.run_id,
                prefect_flow_run_id="bad-graph-flow",
                graph_digest="bad-graph",
                code_version="adapter-v1",
                source_token="bad-graph-source",
                source_bindings=self.sources(),
                operator_bindings=self.operators("bad-graph"),
            )
        drifted_artifacts = self.operators("bad-artifact")
        drifted_artifacts[0] = OperatorBinding(
            "b",
            "bad-artifact-b",
            artifact_id=RETURN_ONLY_ARTIFACT,
            artifact_digest="f" * 64,
        )
        with self.assertRaises(ResumeIdentityMismatch):
            await self.attempts.activate_attempt(
                self.admin_token,
                control_run_id=self.run_id,
                prefect_flow_run_id="bad-artifact-flow",
                graph_digest=self.revision.graph_digest,
                code_version="adapter-v1",
                source_token="bad-artifact-source",
                source_bindings=self.sources(),
                operator_bindings=drifted_artifacts,
            )
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM records"), 0)
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM prefect_attempt_heads_v1"),
                0,
            )
        first = await self.activate()
        with self.assertRaises(ResumeIdentityMismatch):
            await self.attempts.reconnect_attempt(
                self.admin_token,
                control_run_id=self.run_id,
                attempt_id=first.attempt_id,
                ordinal=first.ordinal,
                prefect_flow_run_id=first.prefect_flow_run_id,
                graph_digest=first.graph_digest,
                code_version="adapter-v2",
            )

    async def test_task_retry_rebinds_one_operator_without_rotating_attempt(self):
        attempt = await self.activate()
        before = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        c_before = next(x for x in before["operators"] if x["participant_id"] == "c")
        b_after = await self.attempts.replace_operator(
            self.admin_token,
            control_run_id=self.run_id,
            attempt_id=attempt.attempt_id,
            participant_id="b",
            prefect_task_run_id="task-b-retry",
            owner_id="attempt-1-b-retry",
            reason="TASK_RETRY",
        )
        after = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        c_after = next(x for x in after["operators"] if x["participant_id"] == "c")
        self.assertEqual(after["current"]["attempt_id"], attempt.attempt_id)
        self.assertEqual(c_before, c_after)
        self.assertEqual(b_after["replacement_count"], 1)

    async def test_controller_reconnect_keeps_attempt_and_prefect_id(self):
        first = await self.activate()
        replacement_stream = await PostgresStream.connect_existing(DSN, self.schema)
        try:
            replacement_control = PostgresControlPlane.connect_existing(
                replacement_stream
            )
            replacement_attempts = PostgresPrefectAttempts.connect_existing(
                replacement_control
            )
            renewed = await replacement_control.claim_leader(
                self.admin_token,
                deployment_id=self.revision.deployment_id,
                leader_id=self.leader.leader_id,
                lease_seconds=300,
            )
            self.assertEqual(renewed.leader_epoch, self.leader.leader_epoch)
            resumed = await replacement_attempts.reconnect_attempt(
                self.admin_token,
                control_run_id=self.run_id,
                attempt_id=first.attempt_id,
                ordinal=first.ordinal,
                prefect_flow_run_id=first.prefect_flow_run_id,
                graph_digest=first.graph_digest,
                code_version=first.code_version,
            )
            self.assertEqual(resumed, first)
            async with replacement_stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM prefect_execution_attempts_v1"
                    ),
                    1,
                )
        finally:
            await replacement_stream.close()

    async def test_expired_operator_lease_reacquires_with_new_fence(self):
        first = await self.activate()
        bound = await self.attempts.bind_operator_task(
            self.admin_token,
            control_run_id=self.run_id,
            attempt_id=first.attempt_id,
            participant_id="b",
            prefect_task_run_id="task-b",
        )
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "UPDATE join_workers SET lease_expires_at="
                "clock_timestamp()-interval '1 second' "
                "WHERE revision_id=$1 AND participant_id='b'",
                self.revision.revision_id,
            )
        reacquired = await self.attempts.renew_operator_lease(
            self.admin_token,
            control_run_id=self.run_id,
            attempt_id=first.attempt_id,
            participant_id="b",
            prefect_task_run_id="task-b",
            owner_id=bound["owner_id"],
            lease_seconds=1,
        )
        self.assertEqual(reacquired["owner_epoch"], bound["owner_epoch"] + 1)
        self.assertEqual(reacquired["replacement_count"], 1)
        with self.assertRaises(StaleOwner):
            await self.runtimes["b"].step(
                "input/p0",
                prefect_passthrough,
                bound["owner_id"],
                bound["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.coordinator_epoch,
            )

    async def test_attempt_history_is_bounded_and_old_resume_expires(self):
        attempts = [await self.activate(max_hot_attempts=2)]
        for number in range(2, 102):
            attempts.append(await self.replace(attempts[-1], number))
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM prefect_execution_attempts_v1 WHERE run_id=$1",
                    self.run_id,
                ),
                2,
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM control_capabilities_v1"),
                3,
            )
        projection = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        self.assertEqual(projection["retired_through"], 99)
        self.assertEqual(projection["retired_count"], 99)
        with self.assertRaises(AttemptHorizonExpired):
            await self.attempts.reconnect_attempt(
                self.admin_token,
                control_run_id=self.run_id,
                attempt_id=attempts[0].attempt_id,
                ordinal=1,
                prefect_flow_run_id="flow-1",
                graph_digest=self.revision.graph_digest,
                code_version="adapter-v1",
            )
        self.assertEqual(len({x.attempt_id for x in attempts}), len(attempts))

    async def test_attempt_swap_rejects_restoring_worker_before_fence(self):
        first = await self.activate()
        async with self.stream.pool.acquire() as conn, conn.transaction():
            await conn.execute("ALTER TABLE join_workers DISABLE TRIGGER USER")
            await conn.execute(
                "UPDATE join_workers SET phase='RESTORING' "
                "WHERE revision_id=$1 AND participant_id='b'",
                self.revision.revision_id,
            )
            await conn.execute("ALTER TABLE join_workers ENABLE TRIGGER USER")
        before = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        with self.assertRaises(RestoringSwapUnsupported):
            await self.replace(first, 2)
        after = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        self.assertEqual(before, after)

    async def test_active_attempt_rejects_regional_recovery_atomically(self):
        await self.activate()
        await self.append("source-cap-1", 1)
        projection = await self.attempts.project_attempt(
            self.admin_token, control_run_id=self.run_id
        )
        operators = {row["participant_id"]: row for row in projection["operators"]}
        self.assertEqual(
            await self.runtimes["b"].step(
                "input/p0",
                prefect_return_only,
                operators["b"]["owner_id"],
                operators["b"]["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.coordinator_epoch,
            ),
            "APPLIED",
        )
        self.assertEqual(
            await self.runtimes["c"].step(
                "input/p0",
                prefect_uppercase,
                operators["c"]["owner_id"],
                operators["c"]["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.coordinator_epoch,
            ),
            "APPLIED",
        )
        checkpoint = await self.coordinator.start_checkpoint(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            "checkpoint-controller",
            self.coordinator_epoch,
            "prefect-regional-guard-checkpoint",
        )
        await self.coordinator.inject_source_barrier(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            "checkpoint-controller",
            self.coordinator_epoch,
            checkpoint.checkpoint_id,
            "input",
            "p0",
        )
        for participant, task_function in (
            ("b", prefect_return_only),
            ("c", prefect_uppercase),
        ):
            self.assertEqual(
                await self.runtimes[participant].step(
                    "input/p0",
                    task_function,
                    operators[participant]["owner_id"],
                    operators[participant]["owner_epoch"],
                    coordinator_owner="checkpoint-controller",
                    coordinator_epoch=self.coordinator_epoch,
                ),
                "ALIGNED",
            )
        await self.coordinator.complete_checkpoint(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            "checkpoint-controller",
            self.coordinator_epoch,
            checkpoint.checkpoint_id,
        )
        await self.regional.bind_checkpoint_cuts(checkpoint.checkpoint_id)
        async with self.stream.pool.acquire() as conn:
            before = [
                tuple(row)
                for row in await conn.fetch(
                    "SELECT participant_id,owner_id,owner_epoch,phase,state "
                    "FROM join_workers WHERE revision_id=$1 ORDER BY participant_id",
                    self.revision.revision_id,
                )
            ]
        with self.assertRaisesRegex(
            asyncpg.RaiseError, "active Prefect attempt rejects regional recovery"
        ):
            await self.regional.start_recovery(
                "checkpoint-controller",
                self.coordinator_epoch,
                "prefect-active-attempt-regional-recovery",
                "b",
                checkpoint.checkpoint_id,
                "regional-owner",
            )
        async with self.stream.pool.acquire() as conn:
            after = [
                tuple(row)
                for row in await conn.fetch(
                    "SELECT participant_id,owner_id,owner_epoch,phase,state "
                    "FROM join_workers WHERE revision_id=$1 ORDER BY participant_id",
                    self.revision.revision_id,
                )
            ]
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM region_recovery_plans_v1 WHERE revision_id=$1",
                    self.revision.revision_id,
                ),
                0,
            )
        self.assertEqual(after, before)

    async def test_unsafe_fanin_capacity_rejected_before_data(self):
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "UPDATE partitions SET max_records=3 "
                "WHERE channel='output-b' AND partition_id='p0'"
            )
        with self.assertRaisesRegex(FeatureDisabled, "bounded input window"):
            await _validate_capacity_profile(self.stream, self.revision)
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM records"), 0)
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM prefect_attempt_heads_v1"),
                0,
            )

    async def test_actual_prefect_flow_streams_before_source_finishes_and_drains(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(delay=0.3)
            running = asyncio.create_task(
                prefect_stream_flow(
                    asdict(config),
                    self.prefect_documents(b"one", b"two", b"three"),
                    return_state=True,
                )
            )
            for _ in range(200):
                async with self.stream.pool.acquire() as conn:
                    outputs = await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel IN('output-b','output-c')"
                    )
                if outputs:
                    break
                await asyncio.sleep(0.02)
            self.assertGreater(outputs, 0)
            self.assertFalse(
                running.done(), "output must arrive while source stays open"
            )
            state = await asyncio.wait_for(running, 30)
            self.assertEqual(state.type, StateType.COMPLETED)
            flow_run_id = state.state_details.flow_run_id
            result = await asyncio.to_thread(state.result)
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            task_ids = [
                result["source_task_run_id"],
                *[row["prefect_task_run_id"] for row in projection["operators"]],
            ]
            async with get_client() as client:
                flow_run = await client.read_flow_run(flow_run_id)
                task_runs = await self.read_prefect_tasks(client, task_ids)
            self.assertEqual(flow_run.state_type, StateType.COMPLETED)
            self.assertEqual(len(task_runs), 3)
            self.assertTrue(
                all(row.state_type == StateType.COMPLETED for row in task_runs)
            )
            reader = PrefectStateReader(os.environ["PREFECT_API_URL"])
            try:
                native = await reader.read(str(flow_run_id))
            finally:
                await reader.close()
            self.assertTrue(native["available"], native)
            self.assertEqual(native["flow_state_type"], "COMPLETED")
            self.assertEqual(
                {row["task_run_id"] for row in native["tasks"]}, set(task_ids)
            )
            self.assertTrue(
                all(row["state_type"] == "COMPLETED" for row in native["tasks"])
            )
            self.assertEqual(
                projection["current"]["prefect_flow_run_id"], str(flow_run_id)
            )
            self.assertEqual(projection["current"]["status"], "DRAINED")
            self.assertEqual(
                {row["prefect_task_run_id"] for row in projection["operators"]},
                set(result["operator_task_run_ids"]),
            )
            self.assertEqual(
                {
                    row["participant_id"]: row["artifact_id"]
                    for row in projection["operators"]
                },
                {"b": RETURN_ONLY_ARTIFACT, "c": UPPERCASE_ARTIFACT},
            )
            async with self.stream.pool.acquire() as conn:
                transformed = {
                    row["channel"]: bytes(row["payload"])
                    for row in await conn.fetch(
                        "SELECT r.channel,o.payload FROM records r JOIN objects o "
                        "USING(object_key) WHERE r.sequence=1 AND "
                        "r.channel IN('output-b','output-c')"
                    )
                }
            self.assertEqual(transformed, {"output-c": b"ONE"})
            serialized_parameters = json.dumps(flow_run.parameters, sort_keys=True)
            for secret in (DSN, self.admin_token, "source-cap-prefect"):
                self.assertNotIn(secret, serialized_parameters)
            events = ADAPTER_TELEMETRY.events_for_run(self.run_id)
            spans = ADAPTER_TELEMETRY.exporter.for_run(self.run_id)
            self.assertTrue(any(x["event_type"] == "attempt_activated" for x in events))
            self.assertTrue(any(x["name"] == "stream.attempt.activate" for x in spans))
            self.assertTrue(
                any(
                    event["event_type"] == "terminal_yield"
                    and event["stream.participant_id"] == "c"
                    and event["stream.input.channel"] == "input"
                    and event["stream.input.partition"] == "p0"
                    and event["stream.input.sequence"] == 1
                    and event["prefect.flow_run_id"] == str(flow_run_id)
                    and event["stream.owner_epoch"] > 0
                    for event in events
                ),
                events,
            )
            safe_observation = json.dumps([events, spans], sort_keys=True)
            for secret in (DSN, self.admin_token, "source-cap-prefect"):
                self.assertNotIn(secret, safe_observation)
        finally:
            await self.stop_prefect_server(resources)

    async def test_actual_prefect_maintenance_crosses_hard_capacity(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(maintenance=True, checkpoint_interval=2)
            documents = self.prefect_documents(
                *(f"bounded-{number}".encode() for number in range(1, 21))
            )
            state = await prefect_stream_flow(
                asdict(config), documents, return_state=True
            )
            self.assertEqual(state.type, StateType.COMPLETED)
            result = await asyncio.to_thread(state.result)
            self.assertIsNotNone(result["maintenance_task_run_id"])
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            self.assertEqual(projection["current"]["status"], "DRAINED")
            async with self.stream.pool.acquire() as conn:
                run = await conn.fetchrow(
                    "SELECT * FROM control_runs_v1 WHERE run_id=$1", self.run_id
                )
                source_progress = await conn.fetchrow(
                    "SELECT * FROM producer_progress WHERE channel='input' "
                    "AND partition_id='p0' AND producer_id='stable-source'"
                )
                states = {
                    row["participant_id"]: json.loads(row["state"])
                    for row in await conn.fetch(
                        "SELECT participant_id,state FROM join_workers "
                        "WHERE revision_id=$1",
                        self.revision.revision_id,
                    )
                }
                live_records = await conn.fetchval("SELECT count(*) FROM records")
            self.assertLessEqual(run["used_admission_records"], 1)
            self.assertLessEqual(run["used_admission_bytes"], 128)
            self.assertEqual(source_progress["accepted_through"], 20)
            self.assertEqual(
                {key: value["completed"] for key, value in states.items()},
                {"b": 20, "c": 20},
            )
            self.assertLessEqual(live_records, 12)
            maintenance_report = await evaluate_maintenance(
                DSN, self.schema, self.revision.revision_id
            )
            self.assertTrue(maintenance_report["passed"], maintenance_report)
            async with get_client() as client:
                maintenance_run = await client.read_task_run(
                    uuid.UUID(result["maintenance_task_run_id"])
                )
            self.assertEqual(maintenance_run.state_type, StateType.COMPLETED)
        finally:
            await self.stop_prefect_server(resources)

    async def test_maintenance_profile_rejects_deadlocking_interval_before_data(self):
        config = self.adapter_config(maintenance=True, checkpoint_interval=5)
        with self.assertRaisesRegex(
            FeatureDisabled, "checkpoint interval exceeds.*record capacity"
        ):
            await _validate_maintenance_profile(
                self.stream,
                self.revision,
                config,
                self.prefect_documents(b"one", b"two", b"three", b"four", b"five"),
            )
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(await conn.fetchval("SELECT count(*) FROM records"), 0)
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM prefect_attempt_heads_v1"),
                0,
            )
        oversized = self.adapter_config(maintenance=False)
        with self.assertRaisesRegex(
            FeatureDisabled, "source document exceeds hard byte capacity"
        ):
            await _validate_maintenance_profile(
                self.stream,
                self.revision,
                oversized,
                self.prefect_documents(b"x" * 1_000_001),
            )

    async def test_actual_prefect_maintenance_resumes_pending_and_completed_gap(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(
                maintenance=True,
                checkpoint_interval=2,
                coordinator_lease=0.45,
                operator_delay=0.6,
                fault_maintenance_retries=True,
            )
            state = await prefect_stream_flow(
                asdict(config),
                self.prefect_documents(
                    *(f"resume-{number}".encode() for number in range(1, 9))
                ),
                return_state=True,
            )
            self.assertEqual(state.type, StateType.COMPLETED)
            result = await asyncio.to_thread(state.result)
            async with self.stream.pool.acquire() as conn:
                statuses = await conn.fetch(
                    "SELECT l.status,count(*) n FROM checkpoint_lifecycle_v2 l "
                    "JOIN checkpoint_intents_v2 i USING(checkpoint_id) "
                    "WHERE i.revision_id=$1 GROUP BY l.status",
                    self.revision.revision_id,
                )
                attempts = await conn.fetchval(
                    "SELECT count(*) FROM streaming_retention_operations_v1 "
                    "WHERE revision_id=$1 AND status='COMPLETED'",
                    self.revision.revision_id,
                )
            self.assertTrue(statuses)
            self.assertLessEqual(
                {row["status"] for row in statuses}, {"COMPLETED", "RETIRED"}
            )
            self.assertGreater(attempts, 0)
            async with get_client() as client:
                maintenance_run = await client.read_task_run(
                    uuid.UUID(result["maintenance_task_run_id"])
                )
            self.assertEqual(maintenance_run.run_count, 3)
            self.assertEqual(maintenance_run.state_type, StateType.COMPLETED)
            maintenance_report = await evaluate_maintenance(
                DSN, self.schema, self.revision.revision_id
            )
            self.assertTrue(maintenance_report["passed"], maintenance_report)
        finally:
            await self.stop_prefect_server(resources)

    async def test_actual_prefect_cancel_uses_public_terminal_states(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(terminal_mode="CANCEL")
            state = await prefect_stream_flow(asdict(config), [], return_state=True)
            self.assertEqual(state.type, StateType.CANCELLED)
            flow_run_id = state.state_details.flow_run_id
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            async with get_client() as client:
                flow_run = await client.read_flow_run(flow_run_id)
                task_runs = await self.read_prefect_tasks(
                    client,
                    [row["prefect_task_run_id"] for row in projection["operators"]],
                )
            self.assertEqual(flow_run.state_type, StateType.CANCELLED)
            operator_states = [
                row.state_type
                for row in task_runs
                if row.name.startswith("durable-stream-operator")
            ]
            self.assertEqual(operator_states, [StateType.CANCELLED] * 2)
            self.assertEqual(projection["current"]["status"], "CANCELLED")
        finally:
            await self.stop_prefect_server(resources)

    async def test_external_cancel_stops_open_source_and_returns_cancelled(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(delay=1)
            running = asyncio.create_task(
                prefect_stream_flow(
                    asdict(config),
                    self.prefect_documents(b"one", b"two", b"three"),
                    return_state=True,
                )
            )
            for _ in range(300):
                async with self.stream.pool.acquire() as conn:
                    accepted = await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel='input'"
                    )
                if accepted:
                    break
                await asyncio.sleep(0.02)
            self.assertEqual(accepted, 1)
            await self.control.cancel_run(
                self.admin_token,
                run_id=self.run_id,
                request_id="external-prefect-cancel",
                leader_id=self.leader.leader_id,
                leader_epoch=self.leader.leader_epoch,
            )
            state = await asyncio.wait_for(running, 30)
            self.assertEqual(state.type, StateType.CANCELLED)
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            self.assertEqual(projection["current"]["status"], "CANCELLED")
            async with get_client() as client:
                flow_run = await client.read_flow_run(state.state_details.flow_run_id)
                task_runs = await self.read_prefect_tasks(
                    client,
                    [row["prefect_task_run_id"] for row in projection["operators"]],
                )
            self.assertEqual(flow_run.state_type, StateType.CANCELLED)
            self.assertEqual(
                [row.state_type for row in task_runs],
                [StateType.CANCELLED] * 2,
            )
            async with self.stream.pool.acquire() as conn:
                final_accepted = await conn.fetchval(
                    "SELECT count(*) FROM records WHERE channel='input'"
                )
            self.assertLess(final_accepted, 3)
        finally:
            await self.stop_prefect_server(resources)

    async def test_external_drain_stops_open_source_and_completes_admitted_data(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(
                keep_source_open=True,
                operator_delay=1.2,
                operator_lease=0.45,
            )
            running = asyncio.create_task(
                prefect_stream_flow(
                    asdict(config),
                    self.prefect_documents(b"admitted-before-drain"),
                    return_state=True,
                )
            )
            for _ in range(300):
                async with self.stream.pool.acquire() as conn:
                    accepted = await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel='input'"
                    )
                if accepted:
                    break
                await asyncio.sleep(0.02)
            self.assertEqual(accepted, 1)
            self.assertFalse(running.done(), "open source must await durable control")
            await self.control.drain_run(
                self.admin_token,
                run_id=self.run_id,
                request_id="external-prefect-drain",
                leader_id=self.leader.leader_id,
                leader_epoch=self.leader.leader_epoch,
            )
            state = await asyncio.wait_for(running, 30)
            self.assertEqual(state.type, StateType.COMPLETED)
            result = await asyncio.to_thread(state.result)
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            self.assertEqual(projection["current"]["status"], "DRAINED")
            async with self.stream.pool.acquire() as conn:
                outputs = await conn.fetchval(
                    "SELECT count(*) FROM records "
                    "WHERE channel IN('output-b','output-c')"
                )
            self.assertEqual(outputs, 1)
            task_ids = [
                result["source_task_run_id"],
                *[row["prefect_task_run_id"] for row in projection["operators"]],
            ]
            async with get_client() as client:
                flow_run = await client.read_flow_run(state.state_details.flow_run_id)
                task_runs = await self.read_prefect_tasks(client, task_ids)
            self.assertEqual(flow_run.state_type, StateType.COMPLETED)
            self.assertEqual(len(task_runs), 3)
            self.assertTrue(
                all(row.state_type == StateType.COMPLETED for row in task_runs)
            )
        finally:
            await self.stop_prefect_server(resources)

    async def test_actual_prefect_task_retry_rebinds_only_failed_operator(self):
        resources = await self.start_prefect_server()
        try:
            config = self.adapter_config(fault_participant_once="b")
            state = await prefect_stream_flow(
                asdict(config),
                self.prefect_documents(b"retry-me"),
                return_state=True,
            )
            self.assertEqual(state.type, StateType.COMPLETED)
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            self.assertEqual(projection["current"]["ordinal"], 1)
            operators = {row["participant_id"]: row for row in projection["operators"]}
            self.assertEqual(operators["b"]["replacement_count"], 1)
            self.assertEqual(operators["c"]["replacement_count"], 0)
            async with get_client() as client:
                task_runs = await self.read_prefect_tasks(
                    client,
                    [row["prefect_task_run_id"] for row in operators.values()],
                )
            by_id = {str(row.id): row for row in task_runs}
            self.assertEqual(by_id[operators["b"]["prefect_task_run_id"]].run_count, 2)
            self.assertEqual(by_id[operators["c"]["prefect_task_run_id"]].run_count, 1)
            self.assertTrue(
                all(row.state_type == StateType.COMPLETED for row in task_runs)
            )
        finally:
            await self.stop_prefect_server(resources)

    async def test_killed_execution_replaces_attempt_and_resumes_first_incomplete(self):
        resources = await self.start_prefect_server()
        try:
            first_config = self.adapter_config(delay=30, operator_delay=30)
            first_env = os.environ.copy()
            first_env["DS041_SOURCE"] = "source-cap-process-1"
            first = await asyncio.create_subprocess_exec(
                str(HERE.parents[2] / ".venv" / "bin" / "python"),
                str(RUNNER),
                json.dumps(asdict(first_config), sort_keys=True),
                json.dumps(
                    [
                        {
                            "producer_sequence": 1,
                            "record_id": "prefect-document-1",
                            "lineage_digest": "prefect-lineage-1",
                            "payload_b64": base64.b64encode(
                                b"first-incomplete"
                            ).decode(),
                        }
                    ],
                    sort_keys=True,
                ),
                cwd=HERE,
                env=first_env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            first_projection = None
            for _ in range(400):
                async with self.stream.pool.acquire() as conn:
                    accepted = await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel='input'"
                    )
                    outputs = await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel IN('output-b','output-c')"
                    )
                if accepted == 1:
                    first_projection = await self.attempts.project_attempt(
                        self.admin_token, control_run_id=self.run_id
                    )
                    break
                if first.returncode is not None:
                    break
                await asyncio.sleep(0.025)
            self.assertIsNotNone(first_projection)
            self.assertEqual(outputs, 0)
            first.kill()
            await first.wait()
            self.assertLess(first.returncode, 0)
            old_flow_run_id = uuid.UUID(
                first_projection["current"]["prefect_flow_run_id"]
            )
            async with get_client() as client:
                old_before = await client.read_flow_run(old_flow_run_id)
                self.assertEqual(old_before.state_type, StateType.RUNNING)
                orchestration = await client.set_flow_run_state(
                    old_flow_run_id,
                    Crashed(message="verified adapter execution process loss"),
                )
                old_after = await client.read_flow_run(old_flow_run_id)
            self.assertEqual(orchestration.state.type, StateType.CRASHED)
            self.assertEqual(old_after.state_type, StateType.CRASHED)

            replacement_config = self.adapter_config()
            replacement_config = PrefectStreamAdapterConfig(
                **{
                    **asdict(replacement_config),
                    "replace_attempt_id": first_projection["current"]["attempt_id"],
                }
            )
            replacement_env = os.environ.copy()
            replacement_env["DS041_SOURCE"] = "source-cap-process-2"
            replacement = await asyncio.create_subprocess_exec(
                str(HERE.parents[2] / ".venv" / "bin" / "python"),
                str(RUNNER),
                json.dumps(asdict(replacement_config), sort_keys=True),
                "[]",
                cwd=HERE,
                env=replacement_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            output, _ = await asyncio.wait_for(replacement.communicate(), 30)
            self.assertEqual(replacement.returncode, 0, output.decode(errors="replace"))
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            self.assertEqual(projection["current"]["ordinal"], 2)
            self.assertEqual(projection["current"]["status"], "DRAINED")
            async with self.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel IN('output-b','output-c')"
                    ),
                    1,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT min(completed) FROM join_inputs_v3 WHERE revision_id=$1",
                        self.revision.revision_id,
                    ),
                    1,
                )
            async with get_client() as client:
                flow_run = await client.read_flow_run(
                    uuid.UUID(projection["current"]["prefect_flow_run_id"])
                )
                task_runs = await self.read_prefect_tasks(
                    client,
                    [row["prefect_task_run_id"] for row in projection["operators"]],
                )
            self.assertEqual(flow_run.state_type, StateType.COMPLETED)
            self.assertTrue(
                all(row.state_type == StateType.COMPLETED for row in task_runs)
            )
        finally:
            await self.stop_prefect_server(resources)

    async def test_killed_maintenance_process_resumes_pending_and_retention_gap(self):
        resources = await self.start_prefect_server()

        async def run_process(config, documents, token, exit_stage=None):
            environment = os.environ.copy()
            environment["DS041_SOURCE"] = token
            if exit_stage is None:
                environment.pop("ARIADNE_TEST_MAINTENANCE_EXIT_STAGE", None)
            else:
                environment["ARIADNE_TEST_MAINTENANCE_EXIT_STAGE"] = exit_stage
            encoded = [
                {
                    **{key: value for key, value in item.items() if key != "payload"},
                    "payload_b64": base64.b64encode(item["payload"]).decode(),
                }
                for item in documents
            ]
            process = await asyncio.create_subprocess_exec(
                str(HERE.parents[2] / ".venv" / "bin" / "python"),
                str(RUNNER),
                json.dumps(asdict(config), sort_keys=True),
                json.dumps(encoded, sort_keys=True),
                cwd=HERE,
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            output, _ = await asyncio.wait_for(process.communicate(), 40)
            return process.returncode, output

        async def mark_current_crashed():
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            async with get_client() as client:
                response = await client.set_flow_run_state(
                    uuid.UUID(projection["current"]["prefect_flow_run_id"]),
                    Crashed(message="verified maintenance process loss"),
                )
            self.assertEqual(response.state.type, StateType.CRASHED)
            return projection

        try:
            first_config = self.adapter_config(maintenance=True, checkpoint_interval=2)
            code, output = await run_process(
                first_config,
                self.prefect_documents(b"one", b"two"),
                "source-cap-maintenance-process-1",
                "after_barrier",
            )
            self.assertEqual(code, 86, output.decode(errors="replace"))
            first = await mark_current_crashed()
            async with self.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM checkpoint_lifecycle_v2 l "
                        "JOIN checkpoint_intents_v2 i USING(checkpoint_id) "
                        "WHERE i.revision_id=$1 AND l.status "
                        "NOT IN('COMPLETED','ABORTED','RETIRED')",
                        self.revision.revision_id,
                    ),
                    1,
                )

            second_config = self.adapter_config(maintenance=True, checkpoint_interval=2)
            second_config = PrefectStreamAdapterConfig(
                **{
                    **asdict(second_config),
                    "replace_attempt_id": first["current"]["attempt_id"],
                }
            )
            code, output = await run_process(
                second_config,
                [],
                "source-cap-maintenance-process-2",
                "after_checkpoint",
            )
            self.assertEqual(code, 86, output.decode(errors="replace"))
            second = await mark_current_crashed()
            async with self.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM checkpoint_lifecycle_v2 l "
                        "JOIN checkpoint_intents_v2 i USING(checkpoint_id) "
                        "WHERE i.revision_id=$1 AND l.status='COMPLETED'",
                        self.revision.revision_id,
                    ),
                    1,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM streaming_retention_operations_v1"
                    ),
                    0,
                )

            final_config = self.adapter_config(maintenance=True, checkpoint_interval=2)
            final_config = PrefectStreamAdapterConfig(
                **{
                    **asdict(final_config),
                    "replace_attempt_id": second["current"]["attempt_id"],
                }
            )
            code, output = await run_process(
                final_config,
                self.prefect_documents(
                    b"three", b"four", b"five", b"six", b"seven", b"eight", start=3
                ),
                "source-cap-maintenance-process-3",
            )
            self.assertEqual(code, 0, output.decode(errors="replace"))
            projection = await self.attempts.project_attempt(
                self.admin_token, control_run_id=self.run_id
            )
            self.assertEqual(projection["current"]["ordinal"], 3)
            self.assertEqual(projection["current"]["status"], "DRAINED")
            async with self.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT accepted_through FROM producer_progress "
                        "WHERE channel='input' AND partition_id='p0' "
                        "AND producer_id='stable-source'"
                    ),
                    8,
                )
                self.assertGreater(
                    await conn.fetchval(
                        "SELECT count(*) FROM streaming_retention_operations_v1 "
                        "WHERE status='COMPLETED'"
                    ),
                    0,
                )
            report = await evaluate_maintenance(
                DSN, self.schema, self.revision.revision_id
            )
            self.assertTrue(report["passed"], report)
        finally:
            await self.stop_prefect_server(resources)

    async def test_delayed_checkpoint_releases_retained_byte_tail_without_churn(self):
        resources = await self.start_prefect_server()
        process = None
        try:
            async with self.stream.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE partitions SET max_bytes=10 "
                    "WHERE channel IN('input','output-c') AND partition_id='p0'"
                )
            first_config = self.adapter_config(
                maintenance=True,
                checkpoint_interval=2,
                max_checkpoint_delay=30,
                delay=0.4,
                keep_source_open=True,
            )
            environment = os.environ.copy()
            environment["DS041_SOURCE"] = "source-cap-byte-tail-1"
            encoded = [
                {
                    **{key: value for key, value in item.items() if key != "payload"},
                    "payload_b64": base64.b64encode(item["payload"]).decode(),
                }
                for item in self.prefect_documents(b"a", b"b", b"x" * 9)
            ]
            process = await asyncio.create_subprocess_exec(
                str(HERE.parents[2] / ".venv" / "bin" / "python"),
                str(RUNNER),
                json.dumps(asdict(first_config), sort_keys=True),
                json.dumps(encoded, sort_keys=True),
                cwd=HERE,
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            first_projection = None
            for _ in range(600):
                async with self.stream.pool.acquire() as conn:
                    accepted = await conn.fetchval(
                        "SELECT accepted_through FROM producer_progress "
                        "WHERE channel='input' AND partition_id='p0' "
                        "AND producer_id='stable-source'"
                    )
                    anchors = await conn.fetchval(
                        "SELECT count(*) FROM join_retention_anchors_v1 "
                        "WHERE revision_id=$1",
                        self.revision.revision_id,
                    )
                    pending_retention = await conn.fetchval(
                        "SELECT count(*) FROM streaming_retention_operations_v1 "
                        "WHERE revision_id=$1 AND status<>'COMPLETED'",
                        self.revision.revision_id,
                    )
                    completed_retention = await conn.fetchval(
                        "SELECT count(*) FROM streaming_retention_operations_v1 "
                        "WHERE revision_id=$1 AND status='COMPLETED'",
                        self.revision.revision_id,
                    )
                if (
                    accepted == 3
                    and anchors == 2
                    and pending_retention == 0
                    and completed_retention > 0
                ):
                    first_projection = await self.attempts.project_attempt(
                        self.admin_token, control_run_id=self.run_id
                    )
                    break
                if process.returncode is not None:
                    break
                await asyncio.sleep(0.02)
            self.assertIsNotNone(first_projection)
            async with self.stream.pool.acquire() as conn:
                revision = await conn.fetchrow(
                    "SELECT next_checkpoint_ordinal FROM deployment_revisions "
                    "WHERE revision_id=$1",
                    self.revision.revision_id,
                )
                latest_cut = await conn.fetchval(
                    "SELECT max(o.cut_data_sequence) FROM source_barrier_observations o "
                    "JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                    "JOIN checkpoint_intents_v2 i USING(checkpoint_id) "
                    "WHERE i.revision_id=$1 AND l.status='COMPLETED'",
                    self.revision.revision_id,
                )
            self.assertEqual(revision["next_checkpoint_ordinal"], 3)
            self.assertEqual(latest_cut, 2)
            # A healthy open source may idle for many maintenance polls.  The
            # persisted exact participant anchors, rather than a process-local
            # completed-checkpoint count, prevent zero-delta anchor churn.
            await asyncio.sleep(0.25)
            async with self.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT next_checkpoint_ordinal FROM deployment_revisions "
                        "WHERE revision_id=$1",
                        self.revision.revision_id,
                    ),
                    3,
                )
            process.kill()
            await process.wait()
            process = None
            async with get_client() as client:
                await client.set_flow_run_state(
                    uuid.UUID(first_projection["current"]["prefect_flow_run_id"]),
                    Crashed(message="verified retained-byte-tail process loss"),
                )

            replacement = self.adapter_config(
                maintenance=True,
                checkpoint_interval=2,
                max_checkpoint_delay=0.2,
            )
            replacement = PrefectStreamAdapterConfig(
                **{
                    **asdict(replacement),
                    "replace_attempt_id": first_projection["current"]["attempt_id"],
                }
            )
            os.environ["DS041_SOURCE"] = "source-cap-byte-tail-2"
            state = await asyncio.wait_for(
                prefect_stream_flow(
                    asdict(replacement),
                    self.prefect_documents(b"zz", start=4),
                    return_state=True,
                ),
                30,
            )
            self.assertEqual(state.type, StateType.COMPLETED)
            async with self.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT accepted_through FROM producer_progress "
                        "WHERE channel='input' AND partition_id='p0' "
                        "AND producer_id='stable-source'"
                    ),
                    4,
                )
                self.assertGreaterEqual(
                    await conn.fetchval(
                        "SELECT retired_through FROM metadata_summaries "
                        "WHERE channel='input' AND partition_id='p0'"
                    ),
                    3,
                )
            report = await evaluate_maintenance(
                DSN, self.schema, self.revision.revision_id
            )
            self.assertTrue(report["passed"], report)
        finally:
            if process is not None and process.returncode is None:
                process.kill()
                await process.wait()
            await self.stop_prefect_server(resources)


if __name__ == "__main__":
    unittest.main()
