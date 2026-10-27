from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import http.client
import json
import os
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import asyncpg
from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_barrier_oracle import evaluate as evaluate_barrier
from postgres_control_plane import (
    AuthenticationFailed,
    AuthorizationFailed,
    ControlCapacityBlocked,
    ControlConflict,
    ControlIdempotencyHorizonExpired,
    ControlNotFound,
    PostgresControlPlane,
    StaleLeader,
)
from postgres_control_plane_oracle import evaluate as evaluate_control
from postgres_control_plane_server import JsonHttpServer
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_runtime import PostgresJoinRuntime
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_stream_v2 import (
    PostgresProtocolError,
    PostgresStream,
    StaleOwner,
    content_object_key,
    metadata_operation_id,
)

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
HERE = Path(__file__).resolve().parent
SERVER = HERE / "postgres_control_plane_server.py"


@stream_task
def control_region_task(ctx, payload):
    yield OutputSpec(
        payload,
        "bytes/v1",
        {"count": ctx.managed_state.get("count", 0) + 1},
    )


class PostgresControlPlaneTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds040_" + uuid.uuid4().hex[:12]
        self.stream = await PostgresStream.open(DSN, self.schema)
        await self.stream.create_partition(
            "input", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.stream.create_partition(
            "output", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.stream.create_consumer("anna", "input", "p0")
        await self.stream.register_producer("input", "p0", "source-producer")
        self.coordinator = await PostgresBarrierCoordinator.install(self.stream)
        self.revision = await self.coordinator.register_revision(
            revision_id="revision-1",
            deployment_id="deployment-1",
            graph_version="graph-v1",
            sources=[("input", "p0")],
            participants=[
                {
                    "participant_id": "task",
                    "inputs": [{"channel": "input", "partition": "p0"}],
                    "outputs": [{"channel": "output", "partition": "p0"}],
                    "code_digest": "task-code-v1",
                    "state_schema": "state/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="code-v1",
            state_contract_digest="state-v1",
        )
        self.control = await PostgresControlPlane.install(self.stream)
        self.token = "tenant-a-token"
        self.other_token = "tenant-b-token"
        self.read_token = "tenant-a-read"
        capabilities = {"deploy", "run", "read", "control", "source"}
        await self.control.provision_tenant("tenant-a", self.token, capabilities)
        await self.control.provision_tenant("tenant-b", self.other_token, capabilities)
        await self.control.provision_tenant("tenant-a", self.read_token, {"read"})
        await self.control.provision_tenant("tenant-a", "expiring-token", {"read"})
        await self.control.register_deployment(
            self.token,
            deployment_id="deployment-1",
            revision_id="revision-1",
            graph_version="graph-v1",
            graph_digest=self.revision.graph_digest,
        )
        self.leader = await self.control.claim_leader(
            self.token,
            deployment_id="deployment-1",
            leader_id="server-a",
            lease_seconds=300,
        )
        self.run = await self.control.create_run(
            self.token,
            deployment_id="deployment-1",
            request_id="run-request-1",
            consumer_ids=["anna"],
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
            max_admission_records=2,
            max_admission_bytes=1024,
        )
        self.run_id = self.run["run_id"]

    async def asyncTearDown(self) -> None:
        if not hasattr(self, "stream"):
            return
        await self.stream.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        finally:
            await conn.close()

    async def _admit_append(self, number: int):
        payload = f"payload-{number}".encode()
        schema_id = "bytes/v1"
        admission = await self.control.admit_source(
            self.token,
            run_id=self.run_id,
            request_id=f"admit-{number}",
            channel="input",
            partition="p0",
            record_id=f"record-{number}",
            attempt_id=f"append-{number}",
            object_key=content_object_key(payload, schema_id),
            payload_digest=hashlib.sha256(payload).hexdigest(),
            payload_size=len(payload),
            schema_id=schema_id,
            lineage_digest=f"lineage-{number}",
            producer_id="source-producer",
            producer_sequence=number,
        )
        published = await self.control.publish_admission(
            self.token,
            run_id=self.run_id,
            admission_id=admission["admission_id"],
            payload=payload,
        )
        return admission, published

    async def _assert_oracle(self, projection=None):
        report = await evaluate_control(DSN, self.schema, projection)
        self.assertTrue(report["passed"], report["violations"])

    async def _complete_one(self, owner: str = "anna-worker"):
        epoch = await self.stream.claim_consumer("anna", owner, lease_seconds=300)
        delivered = await self.stream.deliver_next("anna", owner, epoch)
        self.assertIsNotNone(delivered)
        token, _ = delivered
        holder = f"holder-{token.sequence}-{owner}"
        await self.stream.acquire_invocation(token, holder, lease_seconds=300)
        await self.stream.complete(
            token,
            "RETURN",
            managed_state={"completed": token.sequence},
            invocation_holder_id=holder,
        )
        return token

    async def _http(
        self,
        port: int,
        method: str,
        path: str,
        *,
        token: str | None = None,
        body: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict]:
        encoded = None if body is None else json.dumps(body).encode()
        request_headers = {} if headers is None else dict(headers)
        if token is not None:
            request_headers["Authorization"] = f"Bearer {token}"
        if encoded is not None:
            request_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}",
            data=encoded,
            headers=request_headers,
            method=method,
        )

        def send():
            try:
                with urllib.request.urlopen(request, timeout=5) as response:
                    return response.status, json.loads(response.read())
            except urllib.error.HTTPError as exc:
                return exc.code, json.loads(exc.read())

        return await asyncio.to_thread(send)

    async def test_projection_is_exact_and_auth_rejects_before_mutation(self):
        await self._admit_append(1)
        token = await self._complete_one()
        projection = await self.control.project_run(self.token, run_id=self.run_id)
        self.assertEqual(
            projection["consumers"][0]["completed_through"], token.sequence
        )
        self.assertEqual(projection["partitions"][0]["next_sequence"], 2)
        self.assertEqual(projection["consumers"][0]["owner_epoch"], token.owner_epoch)
        self.assertIsNone(projection["barrier"])
        self.assertIsNone(projection["recovery"])
        await self._assert_oracle(projection)
        forged_leader = copy.deepcopy(projection)
        forged_leader["leader"]["leader_epoch"] += 1
        report = await evaluate_control(DSN, self.schema, forged_leader)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_leader" in item for item in report["violations"]),
            report["violations"],
        )
        forged_pressure = copy.deepcopy(projection)
        forged_pressure["admission_pressure"]["used_records"] += 1
        report = await evaluate_control(DSN, self.schema, forged_pressure)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any(
                "projection_admission_pressure" in item for item in report["violations"]
            ),
            report["violations"],
        )
        forged_consumer = copy.deepcopy(projection)
        forged_consumer["consumers"][0]["in_flight_end"] = 999
        report = await evaluate_control(DSN, self.schema, forged_consumer)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_consumers" in item for item in report["violations"]),
            report["violations"],
        )
        forged_control = copy.deepcopy(projection)
        forged_control["control"]["drain_admission_cut"] = 999
        report = await evaluate_control(DSN, self.schema, forged_control)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_control" in item for item in report["violations"]),
            report["violations"],
        )
        async with self.stream.pool.acquire() as conn:
            before = await conn.fetchval("SELECT count(*) FROM control_commands_v1")
        with self.assertRaises(ControlNotFound):
            await self.control.cancel_run(
                self.other_token,
                run_id=self.run_id,
                request_id="foreign-cancel",
                leader_id="server-a",
                leader_epoch=1,
            )
        with self.assertRaises(AuthorizationFailed):
            await self.control.cancel_run(
                self.read_token,
                run_id=self.run_id,
                request_id="weak-cancel",
                leader_id="server-a",
                leader_epoch=1,
            )
        with self.assertRaises(AuthenticationFailed):
            await self.control.project_run("forged", run_id=self.run_id)
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM control_commands_v1"), before
            )
        await self.control.revoke_token(self.read_token)
        with self.assertRaises(AuthenticationFailed):
            await self.control.project_run(self.read_token, run_id=self.run_id)
        await self.control.expire_token("expiring-token")
        with self.assertRaises(AuthenticationFailed):
            await self.control.project_run("expiring-token", run_id=self.run_id)

    async def test_join_projection_and_oracle_reject_invented_owner_and_cursor(self):
        await self.stream.create_partition(
            "dag-input", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.stream.create_partition(
            "dag-output", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.coordinator.configure_ordered_log_capacity("dag-input", "p0", 100)
        await self.coordinator.configure_ordered_log_capacity("dag-output", "p0", 100)
        revision = await self.coordinator.register_revision(
            revision_id="revision-dag",
            deployment_id="deployment-dag",
            graph_version="graph-dag-v1",
            sources=[("dag-input", "p0")],
            participants=[
                {
                    "participant_id": "join",
                    "inputs": [{"channel": "dag-input", "partition": "p0"}],
                    "outputs": [{"channel": "dag-output", "partition": "p0"}],
                    "code_digest": "join-code-v1",
                    "state_schema": "join-state/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="dag-code-v1",
            state_contract_digest="dag-state-v1",
        )
        runtime = PostgresJoinRuntime(self.coordinator, revision, "join")
        await runtime.install(initial_state={"count": 0})
        planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.coordinator, planner, revision
        )
        await regional.bind_revision_topology()
        await self.control._initialize()
        await self.control.register_deployment(
            self.token,
            deployment_id="deployment-dag",
            revision_id="revision-dag",
            graph_version="graph-dag-v1",
            graph_digest=revision.graph_digest,
        )
        leader = await self.control.claim_leader(
            self.token,
            deployment_id="deployment-dag",
            leader_id="server-dag",
            lease_seconds=300,
        )
        run = await self.control.create_run(
            self.token,
            deployment_id="deployment-dag",
            request_id="run-dag",
            consumer_ids=[],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        projection = await self.control.project_run(self.token, run_id=run["run_id"])
        self.assertEqual(
            [row["participant_id"] for row in projection["operators"]], ["join"]
        )
        self.assertEqual(projection["inputs"][0]["completed"], 0)
        self.assertEqual(
            {(row["channel"], row["partition_id"]) for row in projection["partitions"]},
            {("dag-input", "p0"), ("dag-output", "p0")},
        )
        await self._assert_oracle(projection)
        forged_owner = copy.deepcopy(projection)
        forged_owner["operators"][0]["owner_epoch"] += 100
        report = await evaluate_control(DSN, self.schema, forged_owner)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_operator" in item for item in report["violations"]),
            report["violations"],
        )
        forged_lease = copy.deepcopy(projection)
        forged_lease["operators"][0]["lease_expires_at"] = "2099-01-01T00:00:00+00:00"
        report = await evaluate_control(DSN, self.schema, forged_lease)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_operators" in item for item in report["violations"]),
            report["violations"],
        )
        forged_input = copy.deepcopy(projection)
        forged_input["inputs"][0]["completed"] += 1
        report = await evaluate_control(DSN, self.schema, forged_input)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_input" in item for item in report["violations"]),
            report["violations"],
        )

        coordinator_epoch = await self.coordinator.claim_coordinator(
            "revision-dag",
            "graph-dag-v1",
            revision.graph_digest,
            "dag-coordinator",
            lease_seconds=300,
        )
        worker_epoch = await runtime.claim("dag-worker", lease_seconds=300)
        checkpoint = await self.coordinator.start_checkpoint(
            "revision-dag",
            "graph-dag-v1",
            revision.graph_digest,
            "dag-coordinator",
            coordinator_epoch,
            "drain-barrier",
        )
        await self.coordinator.inject_source_barrier(
            "revision-dag",
            "graph-dag-v1",
            revision.graph_digest,
            "dag-coordinator",
            coordinator_epoch,
            checkpoint.checkpoint_id,
            "dag-input",
            "p0",
        )
        await self.control.drain_run(
            self.token,
            run_id=run["run_id"],
            request_id="drain-with-pending-barrier",
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        still_draining = await self.control.reconcile_run(
            self.token,
            run_id=run["run_id"],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        self.assertEqual(still_draining["lifecycle"], "DRAINING")
        self.assertEqual(
            await runtime.step(
                "dag-input/p0",
                control_region_task,
                "dag-worker",
                worker_epoch,
                coordinator_owner="dag-coordinator",
                coordinator_epoch=coordinator_epoch,
            ),
            "ALIGNED",
        )
        aligned_not_complete = await self.control.reconcile_run(
            self.token,
            run_id=run["run_id"],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        self.assertEqual(aligned_not_complete["lifecycle"], "DRAINING")
        await self.coordinator.complete_checkpoint(
            "revision-dag",
            "graph-dag-v1",
            revision.graph_digest,
            "dag-coordinator",
            coordinator_epoch,
            checkpoint.checkpoint_id,
        )
        with self.assertRaisesRegex(asyncpg.RaiseError, "lifecycle rejects"):
            await self.coordinator.start_checkpoint(
                "revision-dag",
                "graph-dag-v1",
                revision.graph_digest,
                "dag-coordinator",
                coordinator_epoch,
                "checkpoint-after-drain",
            )
        recovery = await self.coordinator.begin_recovery(
            "revision-dag",
            "graph-dag-v1",
            revision.graph_digest,
            "dag-coordinator",
            coordinator_epoch,
            "drain-global-recovery",
        )
        restoring = await self.control.reconcile_run(
            self.token,
            run_id=run["run_id"],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        self.assertEqual(restoring["lifecycle"], "DRAINING")
        await self.coordinator.finish_recovery(
            "revision-dag",
            "graph-dag-v1",
            revision.graph_digest,
            "dag-coordinator",
            coordinator_epoch,
            recovery.recovery_id,
        )
        drained = await self.control.reconcile_run(
            self.token,
            run_id=run["run_id"],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        self.assertEqual(drained["lifecycle"], "DRAINED")
        await self._assert_oracle(
            await self.control.project_run(self.token, run_id=run["run_id"])
        )

    async def test_concurrent_tenants_cannot_share_output_partition(self):
        for channel in ("tenant-a-input", "tenant-b-input", "shared-output"):
            await self.stream.create_partition(
                channel, "p0", max_records=100, max_bytes=1_000_000
            )

        async def revision(revision_id, deployment_id, source, participant):
            return await self.coordinator.register_revision(
                revision_id=revision_id,
                deployment_id=deployment_id,
                graph_version="shared-v1",
                sources=[(source, "p0")],
                participants=[
                    {
                        "participant_id": participant,
                        "inputs": [{"channel": source, "partition": "p0"}],
                        "outputs": [{"channel": "shared-output", "partition": "p0"}],
                        "code_digest": participant + "-code",
                        "state_schema": "shared-state/v1",
                        "state_version": 1,
                    }
                ],
                code_manifest_digest=participant + "-manifest",
                state_contract_digest=participant + "-state",
            )

        first, second = await asyncio.gather(
            revision("tenant-revision-a", "tenant-deployment-a", "tenant-a-input", "a"),
            revision("tenant-revision-b", "tenant-deployment-b", "tenant-b-input", "b"),
        )
        results = await asyncio.gather(
            self.control.register_deployment(
                self.token,
                deployment_id="tenant-deployment-a",
                revision_id="tenant-revision-a",
                graph_version="shared-v1",
                graph_digest=first.graph_digest,
            ),
            self.control.register_deployment(
                self.other_token,
                deployment_id="tenant-deployment-b",
                revision_id="tenant-revision-b",
                graph_version="shared-v1",
                graph_digest=second.graph_digest,
            ),
            return_exceptions=True,
        )
        self.assertEqual(sum(isinstance(item, dict) for item in results), 1)
        self.assertEqual(sum(isinstance(item, ControlConflict) for item in results), 1)
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM control_partition_ownership_v1 "
                    "WHERE channel='shared-output' AND partition_id='p0'"
                ),
                1,
            )

    async def test_native_run_rejects_partial_graph_and_missing_control_hooks(self):
        for partition in ("partial-input", "partial-mid", "partial-output"):
            await self.stream.create_partition(
                partition, "p0", max_records=100, max_bytes=1_000_000
            )
            await self.coordinator.configure_ordered_log_capacity(partition, "p0", 100)
        participants = [
            {
                "participant_id": "partial-a",
                "inputs": [{"channel": "partial-input", "partition": "p0"}],
                "outputs": [{"channel": "partial-mid", "partition": "p0"}],
                "code_digest": "partial-a-v1",
                "state_schema": "partial-state/v1",
                "state_version": 1,
            },
            {
                "participant_id": "partial-b",
                "inputs": [{"channel": "partial-mid", "partition": "p0"}],
                "outputs": [{"channel": "partial-output", "partition": "p0"}],
                "code_digest": "partial-b-v1",
                "state_schema": "partial-state/v1",
                "state_version": 1,
            },
        ]
        revision = await self.coordinator.register_revision(
            revision_id="partial-revision",
            deployment_id="partial-deployment",
            graph_version="partial-v1",
            sources=[("partial-input", "p0")],
            participants=participants,
            code_manifest_digest="partial-code-v1",
            state_contract_digest="partial-state-v1",
        )
        runtime_a = PostgresJoinRuntime(self.coordinator, revision, "partial-a")
        await runtime_a.install(initial_state={"count": 0})
        planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.coordinator, planner, revision
        )
        await regional.bind_revision_topology()
        await self.control._initialize()
        await self.control.register_deployment(
            self.token,
            deployment_id="partial-deployment",
            revision_id="partial-revision",
            graph_version="partial-v1",
            graph_digest=revision.graph_digest,
        )
        leader = await self.control.claim_leader(
            self.token,
            deployment_id="partial-deployment",
            leader_id="partial-server",
            lease_seconds=300,
        )
        with self.assertRaisesRegex(ControlConflict, "participant manifest"):
            await self.control.create_run(
                self.token,
                deployment_id="partial-deployment",
                request_id="partial-run",
                consumer_ids=[],
                leader_id=leader.leader_id,
                leader_epoch=leader.leader_epoch,
            )
        runtime_b = PostgresJoinRuntime(self.coordinator, revision, "partial-b")
        await runtime_b.install(initial_state={"count": 0})
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "DROP TRIGGER control_region_plan_guard ON region_recovery_plans_v1"
            )
        with self.assertRaisesRegex(ControlConflict, "storage guards"):
            await self.control.create_run(
                self.token,
                deployment_id="partial-deployment",
                request_id="partial-run",
                consumer_ids=[],
                leader_id=leader.leader_id,
                leader_epoch=leader.leader_epoch,
            )
        await self.control._initialize()
        run = await self.control.create_run(
            self.token,
            deployment_id="partial-deployment",
            request_id="partial-run",
            consumer_ids=[],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        self.assertEqual(run["execution_mode"], "NATIVE_JOIN")
        late = PostgresJoinRuntime(self.coordinator, self.revision, "task")
        with self.assertRaisesRegex(asyncpg.RaiseError, "freezes native graph"):
            await late.install(initial_state={"count": 0})
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM control_deployments_v1 "
                    "WHERE deployment_id IN('tenant-deployment-a','tenant-deployment-b')"
                ),
                1,
            )

    async def test_run_binds_exact_legacy_consumer_manifest(self):
        await self.stream.create_partition(
            "legacy-input", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.stream.create_partition(
            "legacy-output", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.stream.create_consumer("legacy-anna", "legacy-input", "p0")
        await self.stream.create_consumer("legacy-boris", "legacy-input", "p0")
        await self.stream.create_consumer("legacy-leaf", "legacy-output", "p0")
        revision = await self.coordinator.register_revision(
            revision_id="legacy-revision",
            deployment_id="legacy-deployment",
            graph_version="legacy-v1",
            sources=[("legacy-input", "p0")],
            participants=[
                {
                    "participant_id": "legacy-task",
                    "inputs": [{"channel": "legacy-input", "partition": "p0"}],
                    "outputs": [{"channel": "legacy-output", "partition": "p0"}],
                    "code_digest": "legacy-code-v1",
                    "state_schema": "legacy-state/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="legacy-code-v1",
            state_contract_digest="legacy-state-v1",
        )
        await self.control.register_deployment(
            self.token,
            deployment_id="legacy-deployment",
            revision_id="legacy-revision",
            graph_version="legacy-v1",
            graph_digest=revision.graph_digest,
        )
        leader = await self.control.claim_leader(
            self.token,
            deployment_id="legacy-deployment",
            leader_id="legacy-server",
            lease_seconds=300,
        )
        with self.assertRaisesRegex(ControlConflict, "exact installed"):
            await self.control.create_run(
                self.token,
                deployment_id="legacy-deployment",
                request_id="legacy-run",
                consumer_ids=["legacy-anna"],
                leader_id=leader.leader_id,
                leader_epoch=leader.leader_epoch,
            )
        with self.assertRaisesRegex(ControlConflict, "outputs are unsupported"):
            await self.control.create_run(
                self.token,
                deployment_id="legacy-deployment",
                request_id="legacy-run",
                consumer_ids=["legacy-anna", "legacy-boris"],
                leader_id=leader.leader_id,
                leader_epoch=leader.leader_epoch,
            )
        async with self.stream.pool.acquire() as conn:
            self.assertFalse(
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM control_runs_v1 "
                    "WHERE revision_id='legacy-revision')"
                )
            )
            await conn.execute("DELETE FROM consumers WHERE consumer_id='legacy-leaf'")
        run = await self.control.create_run(
            self.token,
            deployment_id="legacy-deployment",
            request_id="legacy-run",
            consumer_ids=["legacy-anna", "legacy-boris"],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        self.assertEqual(run["revision_id"], "legacy-revision")
        with self.assertRaisesRegex(asyncpg.RaiseError, "manifest is frozen"):
            await self.stream.create_consumer("legacy-tanya", "legacy-input", "p0")
        with self.assertRaisesRegex(asyncpg.RaiseError, "manifest is frozen"):
            await self.stream.create_consumer("legacy-leaf-late", "legacy-output", "p0")

        for channel in ("deep-input", "deep-mid", "deep-output"):
            await self.stream.create_partition(
                channel, "p0", max_records=100, max_bytes=1_000_000
            )
        await self.stream.create_consumer("deep-anna", "deep-input", "p0")
        deep = await self.coordinator.register_revision(
            revision_id="deep-legacy-revision",
            deployment_id="deep-legacy-deployment",
            graph_version="deep-legacy-v1",
            sources=[("deep-input", "p0")],
            participants=[
                {
                    "participant_id": "deep-a",
                    "inputs": [{"channel": "deep-input", "partition": "p0"}],
                    "outputs": [{"channel": "deep-mid", "partition": "p0"}],
                    "code_digest": "deep-a-v1",
                    "state_schema": "deep/v1",
                    "state_version": 1,
                },
                {
                    "participant_id": "deep-b",
                    "inputs": [{"channel": "deep-mid", "partition": "p0"}],
                    "outputs": [{"channel": "deep-output", "partition": "p0"}],
                    "code_digest": "deep-b-v1",
                    "state_schema": "deep/v1",
                    "state_version": 1,
                },
            ],
            code_manifest_digest="deep-code-v1",
            state_contract_digest="deep-state-v1",
        )
        await self.control.register_deployment(
            self.token,
            deployment_id="deep-legacy-deployment",
            revision_id="deep-legacy-revision",
            graph_version="deep-legacy-v1",
            graph_digest=deep.graph_digest,
        )
        deep_leader = await self.control.claim_leader(
            self.token,
            deployment_id="deep-legacy-deployment",
            leader_id="deep-legacy-server",
            lease_seconds=300,
        )
        with self.assertRaisesRegex(ControlConflict, "routed participant inputs"):
            await self.control.create_run(
                self.token,
                deployment_id="deep-legacy-deployment",
                request_id="deep-legacy-run",
                consumer_ids=["deep-anna"],
                leader_id=deep_leader.leader_id,
                leader_epoch=deep_leader.leader_epoch,
            )
        await self._assert_oracle()

    async def test_leader_takeover_fences_stale_control_writer(self):
        short = await self.control.claim_leader(
            self.token,
            deployment_id="deployment-1",
            leader_id="server-a",
            lease_seconds=0.01,
        )
        async with self.stream.pool.acquire() as conn, conn.transaction():
            await self.control._assert_leader(
                conn, "deployment-1", short.leader_id, short.leader_epoch
            )
            takeover = asyncio.create_task(
                self.control.claim_leader(
                    self.token,
                    deployment_id="deployment-1",
                    leader_id="server-b",
                    lease_seconds=300,
                )
            )
            await conn.execute("SELECT pg_sleep(0.03)")
            self.assertFalse(takeover.done())
        replacement = await takeover
        self.assertGreater(replacement.leader_epoch, short.leader_epoch)
        with self.assertRaises(StaleLeader):
            await self.control.drain_run(
                self.token,
                run_id=self.run_id,
                request_id="stale-drain",
                leader_id=short.leader_id,
                leader_epoch=short.leader_epoch,
            )
        outcome = await self.control.drain_run(
            self.token,
            run_id=self.run_id,
            request_id="new-drain",
            leader_id=replacement.leader_id,
            leader_epoch=replacement.leader_epoch,
        )
        self.assertEqual(outcome["desired_state"], "DRAINING")
        with self.assertRaises(StaleLeader):
            await self.control.reconcile_run(
                self.token,
                run_id=self.run_id,
                leader_id=short.leader_id,
                leader_epoch=short.leader_epoch,
            )
        with self.assertRaises(ControlConflict):
            await self.control.drain_run(
                self.token,
                run_id=self.run_id,
                request_id="another-drain",
                leader_id=replacement.leader_id,
                leader_epoch=replacement.leader_epoch,
            )

    async def test_drain_closes_admission_but_finishes_pre_admitted_record(self):
        payload = b"payload-1"
        admission = await self.control.admit_source(
            self.token,
            run_id=self.run_id,
            request_id="admit-1",
            channel="input",
            partition="p0",
            record_id="record-1",
            attempt_id="append-1",
            object_key=content_object_key(payload, "bytes/v1"),
            payload_digest=hashlib.sha256(payload).hexdigest(),
            payload_size=len(payload),
            schema_id="bytes/v1",
            lineage_digest="lineage-1",
            producer_id="source-producer",
            producer_sequence=1,
        )
        drain = await self.control.drain_run(
            self.token,
            run_id=self.run_id,
            request_id="drain-1",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        self.assertEqual(drain["drain_admission_cut"], 1)
        with self.assertRaises(ControlConflict):
            await self.control.admit_source(
                self.token,
                run_id=self.run_id,
                request_id="admit-2",
                channel="input",
                partition="p0",
                record_id="record-2",
                attempt_id="append-2",
                object_key=content_object_key(b"payload-2", "bytes/v1"),
                payload_digest=hashlib.sha256(b"payload-2").hexdigest(),
                payload_size=len(b"payload-2"),
                schema_id="bytes/v1",
                lineage_digest="lineage-2",
                producer_id="source-producer",
                producer_sequence=2,
            )
        await self.stream.append(
            "append-1",
            "input",
            "p0",
            "record-1",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            "lineage-1",
            producer_id="source-producer",
            producer_sequence=1,
        )
        await self.control.publish_admission(
            self.token,
            run_id=self.run_id,
            admission_id=admission["admission_id"],
            payload=payload,
        )
        self.assertEqual(
            (
                await self.control.reconcile_run(
                    self.token,
                    run_id=self.run_id,
                    leader_id=self.leader.leader_id,
                    leader_epoch=self.leader.leader_epoch,
                )
            )["lifecycle"],
            "DRAINING",
        )
        async with self.stream.pool.acquire() as conn:
            with self.assertRaisesRegex(asyncpg.RaiseError, "pending source"):
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO control_worker_fences_v1 "
                        "SELECT run_id,control_version+1,'DRAINED',clock_timestamp() "
                        "FROM control_runs_v1 WHERE run_id=$1",
                        self.run_id,
                    )
        await self._complete_one()
        reconciled = await self.control.reconcile_run(
            self.token,
            run_id=self.run_id,
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        self.assertEqual(reconciled["lifecycle"], "DRAINED")
        await self._assert_oracle()
        with self.assertRaises(asyncpg.RaiseError):
            await self.stream.claim_consumer("anna", "late-worker", lease_seconds=30)

    async def test_publication_linearizes_with_drain_without_deadlock(self):
        payload = b"publication-drain-race"
        admission = await self.control.admit_source(
            self.token,
            run_id=self.run_id,
            request_id="race-admit-1",
            channel="input",
            partition="p0",
            record_id="race-record-1",
            attempt_id="race-append-1",
            object_key=content_object_key(payload, "bytes/v1"),
            payload_digest=hashlib.sha256(payload).hexdigest(),
            payload_size=len(payload),
            schema_id="bytes/v1",
            lineage_digest="race-lineage-1",
            producer_id="source-producer",
            producer_sequence=1,
        )
        published, drained = await asyncio.wait_for(
            asyncio.gather(
                self.control.publish_admission(
                    self.token,
                    run_id=self.run_id,
                    admission_id=admission["admission_id"],
                    payload=payload,
                ),
                self.control.drain_run(
                    self.token,
                    run_id=self.run_id,
                    request_id="race-drain-1",
                    leader_id=self.leader.leader_id,
                    leader_epoch=self.leader.leader_epoch,
                ),
            ),
            5,
        )
        self.assertEqual(published["status"], "PUBLISHED")
        self.assertEqual(drained["drain_admission_cut"], 1)
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM records WHERE channel='input' "
                    "AND partition_id='p0'"
                ),
                1,
            )
        await self._assert_oracle()

    async def test_published_retry_linearizes_with_retirement_without_deadlock(self):
        _, published = await self._admit_append(1)
        await self._complete_one()
        await self.stream.compact("input", "p0", 1)
        cuts = [("input", "p0", 1)]
        await self.stream.compact_metadata(
            cuts, operation_id=metadata_operation_id(cuts)
        )

        async def retry_publication():
            try:
                return await self.control.publish_admission(
                    self.token,
                    run_id=self.run_id,
                    admission_id=published["admission_id"],
                    payload=b"payload-1",
                )
            except ControlIdempotencyHorizonExpired:
                return "EXPIRED"

        retried, retired = await asyncio.wait_for(
            asyncio.gather(
                retry_publication(),
                self.control.retire_admissions(
                    self.token,
                    run_id=self.run_id,
                    leader_id=self.leader.leader_id,
                    leader_epoch=self.leader.leader_epoch,
                ),
            ),
            5,
        )
        self.assertTrue(retried == "EXPIRED" or retried["status"] == "PUBLISHED")
        self.assertEqual(retired["retired_records"], 1)
        with self.assertRaises(ControlIdempotencyHorizonExpired):
            await self.control.publish_admission(
                self.token,
                run_id=self.run_id,
                admission_id=published["admission_id"],
                payload=b"payload-1",
            )
        await self._assert_oracle()

    async def test_cancel_fences_current_and_future_worker(self):
        await self._admit_append(1)
        epoch = await self.stream.claim_consumer(
            "anna", "old-worker", lease_seconds=300
        )
        delivered = await self.stream.deliver_next("anna", "old-worker", epoch)
        self.assertIsNotNone(delivered)
        token, _ = delivered
        await self.stream.acquire_invocation(token, "old-holder", lease_seconds=300)
        await self.control.cancel_run(
            self.token,
            run_id=self.run_id,
            request_id="cancel-1",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        with self.assertRaises((StaleOwner, PostgresProtocolError)):
            await self.stream.complete(
                token,
                "RETURN",
                managed_state={},
                invocation_holder_id="old-holder",
            )
        with self.assertRaises(asyncpg.RaiseError):
            await self.stream.claim_consumer("anna", "replacement", lease_seconds=30)
        await self._assert_oracle()

    async def test_cancel_fences_live_regional_recovery(self):
        await self.stream.create_partition(
            "region-input", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.stream.create_partition(
            "region-output", "p0", max_records=100, max_bytes=1_000_000
        )
        revision = await self.coordinator.register_revision(
            revision_id="region-revision",
            deployment_id="region-deployment",
            graph_version="region-v1",
            sources=[("region-input", "p0")],
            participants=[
                {
                    "participant_id": "region-task",
                    "inputs": [{"channel": "region-input", "partition": "p0"}],
                    "outputs": [{"channel": "region-output", "partition": "p0"}],
                    "code_digest": "region-task-v1",
                    "state_schema": "count/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="region-code-v1",
            state_contract_digest="region-state-v1",
        )
        coordinator_epoch = await self.coordinator.claim_coordinator(
            "region-revision",
            "region-v1",
            revision.graph_digest,
            "region-coordinator",
            lease_seconds=300,
        )
        runtime = PostgresJoinRuntime(self.coordinator, revision, "region-task")
        await runtime.install(initial_state={"count": 0})
        worker_epoch = await runtime.claim("region-worker", lease_seconds=300)
        planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.coordinator, planner, revision
        )
        await regional.bind_revision_topology()

        async def append_and_step(number: int):
            payload = f"region-{number}".encode()
            await self.stream.append(
                f"region-append-{number}",
                "region-input",
                "p0",
                f"region-record-{number}",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                f"region-lineage-{number}",
            )
            return await runtime.step(
                "region-input/p0",
                control_region_task,
                "region-worker",
                worker_epoch,
                coordinator_owner="region-coordinator",
                coordinator_epoch=coordinator_epoch,
            )

        self.assertEqual(await append_and_step(1), "APPLIED")
        checkpoint = await self.coordinator.start_checkpoint(
            "region-revision",
            "region-v1",
            revision.graph_digest,
            "region-coordinator",
            coordinator_epoch,
            "region-checkpoint",
        )
        await self.coordinator.inject_source_barrier(
            "region-revision",
            "region-v1",
            revision.graph_digest,
            "region-coordinator",
            coordinator_epoch,
            checkpoint.checkpoint_id,
            "region-input",
            "p0",
        )
        self.assertEqual(
            await runtime.step(
                "region-input/p0",
                control_region_task,
                "region-worker",
                worker_epoch,
                coordinator_owner="region-coordinator",
                coordinator_epoch=coordinator_epoch,
            ),
            "ALIGNED",
        )
        await self.coordinator.complete_checkpoint(
            "region-revision",
            "region-v1",
            revision.graph_digest,
            "region-coordinator",
            coordinator_epoch,
            checkpoint.checkpoint_id,
        )
        await regional.bind_checkpoint_cuts(checkpoint.checkpoint_id)
        self.assertEqual(await append_and_step(2), "APPLIED")

        # Regional tables are installed after the shared control schema in this test;
        # a restarted server installs the same storage fences before serving commands.
        await self.control._initialize()
        await self.control.register_deployment(
            self.token,
            deployment_id="region-deployment",
            revision_id="region-revision",
            graph_version="region-v1",
            graph_digest=revision.graph_digest,
        )
        leader = await self.control.claim_leader(
            self.token,
            deployment_id="region-deployment",
            leader_id="region-server",
            lease_seconds=300,
        )
        run = await self.control.create_run(
            self.token,
            deployment_id="region-deployment",
            request_id="region-run",
            consumer_ids=[],
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        recovery = await regional.start_recovery(
            "region-coordinator",
            coordinator_epoch,
            "region-recovery",
            "region-task",
            checkpoint.checkpoint_id,
            "regional-owner",
            lease_seconds=0.2,
        )
        await self.control.cancel_run(
            self.token,
            run_id=run["run_id"],
            request_id="cancel-region",
            leader_id=leader.leader_id,
            leader_epoch=leader.leader_epoch,
        )
        generation = recovery.workers[0]
        with self.assertRaises(asyncpg.RaiseError):
            await regional.replay_worker(
                recovery.plan.plan_id,
                runtime,
                control_region_task,
                "regional-owner",
                generation.recovery_owner_epoch,
            )
        async with self.stream.pool.acquire() as conn:
            await conn.execute("SELECT pg_sleep(0.25)")
        with self.assertRaises(asyncpg.RaiseError):
            await regional.takeover_recovery(
                recovery.plan.plan_id,
                "regional-replacement",
                request_id="region-takeover-after-cancel",
            )
        with self.assertRaises((asyncpg.RaiseError, PostgresProtocolError)):
            await regional.activate_region(recovery.plan.plan_id)
        with self.assertRaises(asyncpg.RaiseError):
            await self.coordinator.begin_recovery(
                "region-revision",
                "region-v1",
                revision.graph_digest,
                "region-coordinator",
                coordinator_epoch,
                "global-recovery-after-cancel",
            )
        with self.assertRaises(asyncpg.RaiseError):
            await self.coordinator.start_checkpoint(
                "region-revision",
                "region-v1",
                revision.graph_digest,
                "region-coordinator",
                coordinator_epoch,
                "checkpoint-after-cancel",
            )
        barrier_report = await evaluate_barrier(DSN, self.schema)
        self.assertTrue(barrier_report["passed"], barrier_report["violations"])
        projection = await self.control.project_run(self.token, run_id=run["run_id"])
        await self._assert_oracle(projection)
        forged_recovery = copy.deepcopy(projection)
        forged_recovery["recovery"]["checkpoint_id"] = "forged-checkpoint"
        report = await evaluate_control(DSN, self.schema, forged_recovery)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("projection_recovery" in item for item in report["violations"]),
            report["violations"],
        )

    async def test_cancel_after_drain_rejects_reserved_source_publication(self):
        payload = b"reserved-before-drain"
        admission = await self.control.admit_source(
            self.token,
            run_id=self.run_id,
            request_id="reserved-1",
            channel="input",
            partition="p0",
            record_id="reserved-record-1",
            attempt_id="reserved-append-1",
            object_key=content_object_key(payload, "bytes/v1"),
            payload_digest=hashlib.sha256(payload).hexdigest(),
            payload_size=len(payload),
            schema_id="bytes/v1",
            lineage_digest="reserved-lineage-1",
            producer_id="source-producer",
            producer_sequence=1,
        )
        await self.control.drain_run(
            self.token,
            run_id=self.run_id,
            request_id="drain-before-cancel",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        await self.control.cancel_run(
            self.token,
            run_id=self.run_id,
            request_id="cancel-after-drain",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        async with self.stream.pool.acquire() as conn:
            before = (
                await conn.fetchval("SELECT count(*) FROM records"),
                await conn.fetchval("SELECT count(*) FROM partition_log_entries"),
            )
        with self.assertRaises(ControlConflict):
            await self.control.publish_admission(
                self.token,
                run_id=self.run_id,
                admission_id=admission["admission_id"],
                payload=payload,
            )
        async with self.stream.pool.acquire() as conn:
            after = (
                await conn.fetchval("SELECT count(*) FROM records"),
                await conn.fetchval("SELECT count(*) FROM partition_log_entries"),
            )
        self.assertEqual(after, before)
        await self._assert_oracle()

    async def test_drained_run_allows_authorized_retention_but_not_cancel(self):
        await self._admit_append(1)
        await self._complete_one()
        await self._admit_append(2)
        await self._complete_one()
        await self.control.drain_run(
            self.token,
            run_id=self.run_id,
            request_id="drain-clean",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        reconciled = await self.control.reconcile_run(
            self.token,
            run_id=self.run_id,
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        self.assertEqual(reconciled["lifecycle"], "DRAINED")
        compacted = await self.stream.compact("input", "p0", 1)
        self.assertEqual(compacted["safe_through"], 1)
        cuts = [("input", "p0", 1)]
        await self.stream.compact_metadata(
            cuts, operation_id=metadata_operation_id(cuts)
        )
        retired = await self.control.retire_admissions(
            self.token,
            run_id=self.run_id,
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        self.assertEqual(retired["retired_records"], 1)
        with self.assertRaises(ControlConflict):
            await self.control.cancel_run(
                self.token,
                run_id=self.run_id,
                request_id="cancel-after-drained",
                leader_id=self.leader.leader_id,
                leader_epoch=self.leader.leader_epoch,
            )
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM control_worker_fences_v1 WHERE run_id=$1",
                    self.run_id,
                ),
                1,
            )
        await self._assert_oracle()

    async def test_cancelled_run_retires_completed_prefix_and_pins_pending(self):
        await self._admit_append(1)
        await self._complete_one()
        await self._admit_append(2)
        await self.control.cancel_run(
            self.token,
            run_id=self.run_id,
            request_id="cancel-with-pending",
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        compacted = await self.stream.compact("input", "p0", 2)
        self.assertEqual(compacted["safe_through"], 1)
        cuts = [("input", "p0", 1)]
        await self.stream.compact_metadata(
            cuts, operation_id=metadata_operation_id(cuts)
        )
        retired = await self.control.retire_admissions(
            self.token,
            run_id=self.run_id,
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        self.assertEqual(retired["retired_records"], 1)
        async with self.stream.pool.acquire() as conn:
            remaining = await conn.fetchrow(
                "SELECT producer_sequence,status FROM control_source_admissions_v1"
            )
            self.assertEqual(tuple(remaining), (2, "PUBLISHED"))
            self.assertTrue(
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM records WHERE channel='input' "
                    "AND partition_id='p0' AND sequence=2)"
                )
            )
        await self._assert_oracle()

    async def test_admission_capacity_retires_at_existing_metadata_horizon(self):
        await self._admit_append(1)
        await self.stream.register_producer("input", "p0", "other-source-producer")
        with self.assertRaisesRegex(ControlConflict, "producer binding drift"):
            await self.control.admit_source(
                self.token,
                run_id=self.run_id,
                request_id="other-producer",
                channel="input",
                partition="p0",
                record_id="other-record",
                attempt_id="other-append",
                object_key=content_object_key(b"other", "bytes/v1"),
                payload_digest=hashlib.sha256(b"other").hexdigest(),
                payload_size=len(b"other"),
                schema_id="bytes/v1",
                lineage_digest="other-lineage",
                producer_id="other-source-producer",
                producer_sequence=1,
            )
        payload_two = b"payload-2"
        second = await self.control.admit_source(
            self.token,
            run_id=self.run_id,
            request_id="admit-2",
            channel="input",
            partition="p0",
            record_id="record-2",
            attempt_id="append-2",
            object_key=content_object_key(payload_two, "bytes/v1"),
            payload_digest=hashlib.sha256(payload_two).hexdigest(),
            payload_size=len(payload_two),
            schema_id="bytes/v1",
            lineage_digest="lineage-2",
            producer_id="source-producer",
            producer_sequence=2,
        )
        with self.assertRaises(ControlCapacityBlocked):
            await self.control.admit_source(
                self.token,
                run_id=self.run_id,
                request_id="admit-3",
                channel="input",
                partition="p0",
                record_id="record-3",
                attempt_id="append-3",
                object_key=content_object_key(b"payload-3", "bytes/v1"),
                payload_digest=hashlib.sha256(b"payload-3").hexdigest(),
                payload_size=len(b"payload-3"),
                schema_id="bytes/v1",
                lineage_digest="lineage-3",
                producer_id="source-producer",
                producer_sequence=3,
            )
        await self._complete_one()
        compacted = await self.stream.compact("input", "p0", 1)
        self.assertEqual(compacted["safe_through"], 1)
        cuts = [("input", "p0", 1)]
        await self.stream.compact_metadata(
            cuts, operation_id=metadata_operation_id(cuts)
        )
        retired = await self.control.retire_admissions(
            self.token,
            run_id=self.run_id,
            leader_id=self.leader.leader_id,
            leader_epoch=self.leader.leader_epoch,
        )
        self.assertEqual(retired["retired_records"], 1)
        self.assertEqual(retired["pressure"]["used_admission_records"], 1)
        with self.assertRaises(ControlIdempotencyHorizonExpired):
            await self.control.admit_source(
                self.token,
                run_id=self.run_id,
                request_id="admit-1",
                channel="input",
                partition="p0",
                record_id="record-1",
                attempt_id="append-1",
                object_key=content_object_key(b"payload-1", "bytes/v1"),
                payload_digest=hashlib.sha256(b"payload-1").hexdigest(),
                payload_size=len(b"payload-1"),
                schema_id="bytes/v1",
                lineage_digest="lineage-1",
                producer_id="source-producer",
                producer_sequence=1,
            )
        self.assertEqual(
            (
                await self.control.admit_source(
                    self.token,
                    run_id=self.run_id,
                    request_id="admit-2",
                    channel="input",
                    partition="p0",
                    record_id="record-2",
                    attempt_id="append-2",
                    object_key=content_object_key(payload_two, "bytes/v1"),
                    payload_digest=hashlib.sha256(payload_two).hexdigest(),
                    payload_size=len(payload_two),
                    schema_id="bytes/v1",
                    lineage_digest="lineage-2",
                    producer_id="source-producer",
                    producer_sequence=2,
                )
            )["admission_id"],
            second["admission_id"],
        )
        admitted = await self.control.admit_source(
            self.token,
            run_id=self.run_id,
            request_id="admit-3",
            channel="input",
            partition="p0",
            record_id="record-3",
            attempt_id="append-3",
            object_key=content_object_key(b"payload-3", "bytes/v1"),
            payload_digest=hashlib.sha256(b"payload-3").hexdigest(),
            payload_size=len(b"payload-3"),
            schema_id="bytes/v1",
            lineage_digest="lineage-3",
            producer_id="source-producer",
            producer_sequence=3,
        )
        self.assertEqual(admitted["producer_sequence"], 3)
        await self._assert_oracle()

    async def test_actual_http_and_process_loss_resume_durable_drain(self):
        server = JsonHttpServer(self.control)
        port = await server.start()
        try:
            status, projection = await self._http(
                port, "GET", f"/v1/runs/{self.run_id}", token=self.token
            )
            self.assertEqual(status, 200)
            self.assertEqual(projection["control"]["run_id"], self.run_id)
            status, _ = await self._http(
                port, "GET", f"/v1/runs/{self.run_id}", token=self.other_token
            )
            self.assertEqual(status, 404)
            payload = b"http-payload"
            status, admission = await self._http(
                port,
                "POST",
                f"/v1/runs/{self.run_id}/admissions",
                token=self.token,
                body={
                    "request_id": "http-admit-1",
                    "channel": "input",
                    "partition": "p0",
                    "record_id": "http-record-1",
                    "attempt_id": "http-append-1",
                    "object_key": content_object_key(payload, "bytes/v1"),
                    "payload_digest": hashlib.sha256(payload).hexdigest(),
                    "payload_size": len(payload),
                    "schema_id": "bytes/v1",
                    "lineage_digest": "http-lineage-1",
                    "producer_id": "source-producer",
                    "producer_sequence": 1,
                },
            )
            self.assertEqual(status, 201)
            publish_path = (
                f"/v1/runs/{self.run_id}/admissions/{admission['admission_id']}/publish"
            )
            status, _ = await self._http(
                port,
                "POST",
                publish_path,
                token=self.token,
                body={"payload_b64": "%%%"},
            )
            self.assertEqual(status, 400)
            status, published = await self._http(
                port,
                "POST",
                publish_path,
                token=self.token,
                body={"payload_b64": base64.b64encode(payload).decode()},
            )
            self.assertEqual(status, 200)
            self.assertEqual(published["status"], "PUBLISHED")
            status, _ = await self._http(
                port,
                "GET",
                f"/v1/runs/{self.run_id}",
                token=self.token,
                headers={"X-Crash-After-Commit": "86"},
            )
            self.assertEqual(status, 403)
            status, _ = await self._http(
                port, "GET", f"/v1/runs/{self.run_id}", token=self.token
            )
            self.assertEqual(status, 200)
        finally:
            await server.close()
        with tempfile.TemporaryDirectory() as tmp:
            port_file = Path(tmp) / "port"
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(SERVER),
                "--dsn",
                DSN,
                "--schema",
                self.schema,
                "--port-file",
                str(port_file),
                "--allow-crash-fault",
                cwd=HERE,
            )
            try:
                for _ in range(500):
                    if port_file.exists():
                        break
                    if process.returncode is not None:
                        self.fail(f"server exited early: {process.returncode}")
                    await asyncio.sleep(0.01)
                crash_port = int(port_file.read_text())
                self.leader = await self.control.claim_leader(
                    self.token,
                    deployment_id="deployment-1",
                    leader_id="server-a",
                    lease_seconds=0.2,
                )
                connection = http.client.HTTPConnection(
                    "127.0.0.1", crash_port, timeout=5
                )
                body = json.dumps({"request_id": "crash-drain"})
                connection.request(
                    "POST",
                    f"/v1/runs/{self.run_id}/drain",
                    body=body,
                    headers={
                        "Authorization": f"Bearer {self.token}",
                        "Content-Type": "application/json",
                        "X-Leader-ID": self.leader.leader_id,
                        "X-Leader-Epoch": str(self.leader.leader_epoch),
                        "X-Crash-After-Commit": "86",
                    },
                )
                with self.assertRaises(
                    (http.client.RemoteDisconnected, ConnectionResetError)
                ):
                    connection.getresponse()
                await asyncio.wait_for(process.wait(), 5)
                self.assertEqual(process.returncode, 86)
                async with self.stream.pool.acquire() as conn:
                    await conn.execute("SELECT pg_sleep(0.25)")
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
            port_file.unlink(missing_ok=True)
            restarted = await asyncio.create_subprocess_exec(
                sys.executable,
                str(SERVER),
                "--dsn",
                DSN,
                "--schema",
                self.schema,
                "--port-file",
                str(port_file),
                cwd=HERE,
            )
            try:
                for _ in range(500):
                    if port_file.exists():
                        break
                    if restarted.returncode is not None:
                        self.fail(f"restart exited early: {restarted.returncode}")
                    await asyncio.sleep(0.01)
                restart_port = int(port_file.read_text())
                status, replacement = await self._http(
                    restart_port,
                    "POST",
                    "/internal/v1/leaders/claim",
                    token=self.token,
                    body={
                        "deployment_id": "deployment-1",
                        "leader_id": "server-b",
                        "lease_seconds": 300,
                    },
                )
                self.assertEqual(status, 200)
                self.assertGreater(
                    replacement["leader_epoch"], self.leader.leader_epoch
                )
                status, retry = await self._http(
                    restart_port,
                    "POST",
                    f"/v1/runs/{self.run_id}/drain",
                    token=self.token,
                    body={"request_id": "crash-drain"},
                    headers={
                        "X-Leader-ID": replacement["leader_id"],
                        "X-Leader-Epoch": str(replacement["leader_epoch"]),
                    },
                )
                self.assertEqual(status, 200)
                self.assertEqual(retry["drain_admission_cut"], 1)
                status, projection = await self._http(
                    restart_port,
                    "GET",
                    f"/v1/runs/{self.run_id}",
                    token=self.token,
                )
                self.assertEqual(status, 200)
                self.assertEqual(projection["control"]["desired_state"], "DRAINING")
                await self._assert_oracle(projection)
                async with self.stream.pool.acquire() as conn:
                    self.assertEqual(
                        await conn.fetchval(
                            "SELECT count(*) FROM control_commands_v1 "
                            "WHERE run_id=$1 AND request_id='crash-drain'",
                            self.run_id,
                        ),
                        1,
                    )
                status, cancelled = await self._http(
                    restart_port,
                    "POST",
                    f"/v1/runs/{self.run_id}/cancel",
                    token=self.token,
                    body={"request_id": "cancel-after-server-restart"},
                    headers={
                        "X-Leader-ID": replacement["leader_id"],
                        "X-Leader-Epoch": str(replacement["leader_epoch"]),
                    },
                )
                self.assertEqual(status, 200)
                self.assertEqual(cancelled["desired_state"], "CANCELLED")
                await self._assert_oracle(
                    (
                        await self._http(
                            restart_port,
                            "GET",
                            f"/v1/runs/{self.run_id}",
                            token=self.token,
                        )
                    )[1]
                )
            finally:
                restarted.terminate()
                await asyncio.wait_for(restarted.wait(), 5)


if __name__ == "__main__":
    unittest.main()
