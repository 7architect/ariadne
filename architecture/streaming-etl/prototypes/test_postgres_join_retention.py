from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import threading
import unittest
import uuid
from pathlib import Path

import asyncpg
import test_postgres_join_runtime as join_fixtures
from postgres_barrier_coordinator import PostgresBarrierCoordinator, decoded
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_retention import JoinReplayHorizonExpired, PostgresJoinRetention
from postgres_join_runtime import PostgresJoinRuntime
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_stream_v2 import PostgresProtocolError, PostgresStream, content_object_key

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN", "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming"
)


@stream_task
def retain_document(ctx, payload):
    state = {"documents": ctx.managed_state.get("documents", []) + [payload.decode()]}
    yield OutputSpec(payload, "bytes/v1", state)


class PostgresJoinRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "ds031r_" + uuid.uuid4().hex[:12]
        self.store = await PostgresStream.open(DSN, self.schema)
        await self.store.create_partition(
            "input", "p0", max_records=1000, max_bytes=10_000_000
        )
        await self.store.create_partition(
            "output", "p0", max_records=1000, max_bytes=10_000_000
        )
        self.coordinator = await PostgresBarrierCoordinator.install(self.store)
        self.revision = await self.coordinator.register_revision(
            revision_id="r1",
            deployment_id="d1",
            graph_version="v1",
            sources=[("input", "p0")],
            participants=[
                {
                    "participant_id": "join",
                    "inputs": [{"channel": "input", "partition": "p0"}],
                    "outputs": [{"channel": "output", "partition": "p0"}],
                    "code_digest": "retain-document-v1",
                    "state_schema": "documents/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="code-v1",
            state_contract_digest="state-v1",
            max_retained_checkpoints=200,
            max_checkpoint_metadata_bytes=getattr(
                self, "checkpoint_metadata_budget", 20_000_000
            ),
        )
        self.coordinator_epoch = await self.coordinator.claim_coordinator(
            "r1", "v1", self.revision.graph_digest, "coordinator", lease_seconds=300
        )
        self.runtime = PostgresJoinRuntime(self.coordinator, self.revision, "join")
        await self.runtime.install(
            initial_state={"documents": []},
            max_mailbox_records=10_000,
            max_metadata_bytes=20_000_000,
        )
        self.worker_epoch = await self.runtime.claim("worker", lease_seconds=300)
        self.retention = await PostgresJoinRetention.install(self.coordinator)
        self.checkpoints = []

    async def asyncTearDown(self):
        await self.store.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        finally:
            await conn.close()

    def _coordinator_args(self):
        return (
            "r1",
            "v1",
            self.revision.graph_digest,
            "coordinator",
            self.coordinator_epoch,
        )

    async def _append(self, number: int) -> None:
        payload = f"document-{number}".encode()
        await self.store.append(
            f"source-{number}",
            "input",
            "p0",
            f"record-{number}",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            f"lineage-{number}",
        )

    async def _step(self, task=retain_document):
        return await self.runtime.step(
            "input/p0",
            task,
            "worker",
            self.worker_epoch,
            coordinator_owner="coordinator",
            coordinator_epoch=self.coordinator_epoch,
        )

    async def _cycle(self, number: int):
        await self._append(number)
        self.assertEqual(await self._step(), "APPLIED")
        checkpoint = await self.coordinator.start_checkpoint(
            *self._coordinator_args(), f"checkpoint-{number}"
        )
        await self.coordinator.inject_source_barrier(
            *self._coordinator_args(), checkpoint.checkpoint_id, "input", "p0"
        )
        self.assertEqual(await self._step(), "ALIGNED")
        await self.coordinator.complete_checkpoint(
            *self._coordinator_args(), checkpoint.checkpoint_id
        )
        self.checkpoints.append(checkpoint.checkpoint_id)
        return checkpoint

    async def test_retirement_rolls_back_when_archive_exceeds_metadata_budget(self):
        limited = PostgresJoinRetentionTests("runTest")
        limited.checkpoint_metadata_budget = 7_000
        await limited.asyncSetUp()
        try:
            await limited._cycle(1)
            async with limited.store.pool.acquire() as conn:
                before = await limited.coordinator._metadata_bytes(conn, "r1")
                roots = await conn.fetch(
                    "SELECT * FROM durable_refs ORDER BY ref_kind,ref_id"
                )
            self.assertLess(before, 7_000)
            with self.assertRaisesRegex(PostgresProtocolError, "metadata-byte budget"):
                await limited.retention.retire_latest("r1", "join", "over-budget")
            async with limited.store.pool.acquire() as conn:
                self.assertEqual(
                    await limited.coordinator._metadata_bytes(conn, "r1"), before
                )
                self.assertEqual(
                    await conn.fetch(
                        "SELECT * FROM durable_refs ORDER BY ref_kind,ref_id"
                    ),
                    roots,
                )
                for table in ("join_turns_v3", "join_receipts_v3"):
                    self.assertEqual(
                        await conn.fetchval(f"SELECT count(*) FROM {table}"), 1
                    )
                for table in (
                    "join_retention_anchors_v1",
                    "join_retirement_manifests_v1",
                    "join_retirement_rows_v1",
                ):
                    self.assertEqual(
                        await conn.fetchval(f"SELECT count(*) FROM {table}"), 0
                    )
            await limited._assert_oracles()
        finally:
            await limited.asyncTearDown()

    async def _assert_oracles(self, schema=None):
        for name in (
            "postgres_join_oracle.py",
            "postgres_barrier_oracle.py",
            "postgres_stream_oracle.py",
        ):
            result = await asyncio.to_thread(
                subprocess.run,
                [
                    sys.executable,
                    str(Path(__file__).parent / name),
                    DSN,
                    self.schema if schema is None else schema,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, name + result.stdout + result.stderr)

    async def test_atomic_manifest_root_advance_and_named_horizon(self):
        first = await self._cycle(1)
        old_plan = await self.coordinator.begin_recovery(
            *self._coordinator_args(), "old-finished-recovery"
        )
        await self.coordinator.finish_recovery(
            *self._coordinator_args(), old_plan.recovery_id
        )

        def fail(point):
            if point == "join_retirement_rows_deleted":
                raise RuntimeError("retirement crash control")

        with self.assertRaisesRegex(RuntimeError, "crash control"):
            await self.retention.retire_latest("r1", "join", "retire-1", fault=fail)
        pressure = await self.retention.pressure("r1", "join")
        self.assertEqual(
            pressure["hot_rows"], {"receipts": 1, "turns": 1, "invocations": 1}
        )
        self.assertIsNone(pressure["anchor"])
        outcome = await self.retention.retire_latest("r1", "join", "retire-1")
        self.assertEqual(
            (outcome.retired_apply_through, outcome.retired_arrival_through), (1, 1)
        )
        retry = await self.retention.retire_latest("r1", "join", "retire-1")
        self.assertTrue(retry.idempotent_retry)
        pressure = await self.retention.pressure("r1", "join")
        self.assertEqual(
            pressure["hot_rows"], {"receipts": 0, "turns": 0, "invocations": 0}
        )
        async with self.store.pool.acquire() as conn:
            root = await conn.fetchval(
                """SELECT d.sequence FROM join_inputs_v3 i JOIN durable_refs d
                   ON d.ref_kind='JOIN_INPUT' AND d.ref_id=i.retention_ref
                   WHERE i.revision_id='r1' AND i.participant_id='join'"""
            )
            self.assertEqual(root, 2)
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "DELETE FROM join_retirement_manifests_v1 WHERE operation_id='retire-1'"
                )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "UPDATE join_retention_anchors_v1 SET state_json='{}'::jsonb"
                )
            with self.assertRaises(asyncpg.PostgresError):
                async with conn.transaction():
                    await conn.execute(
                        """INSERT INTO join_retirement_manifests_v1
                           SELECT 'forged-incomplete',revision_id,participant_id,
                             prior_summary_digest,checkpoint_id,snapshot_object_key,
                             snapshot_payload_digest,mailbox_highwater,
                             retired_apply_from,retired_apply_through,
                             retired_arrival_from,retired_arrival_through,state_json,
                             state_digest,input_cuts,row_counts,repeat('a',64),
                             repeat('b',64),'PLANNED',clock_timestamp(),NULL
                           FROM join_retirement_manifests_v1
                           WHERE operation_id='retire-1'"""
                    )
        await self._cycle(2)
        await self.retention.retire_latest("r1", "join", "retire-2")
        with self.assertRaises(JoinReplayHorizonExpired):
            await self.retention.assert_checkpoint_replayable(
                "r1", "join", first.checkpoint_id
            )
        await self.retention.assert_checkpoint_replayable(
            "r1", "join", self.checkpoints[-1]
        )
        with self.assertRaises(JoinReplayHorizonExpired):
            await self.runtime.restore(
                old_plan.recovery_id, retain_document, "worker", self.worker_epoch
            )
        await self._assert_oracles()

    async def test_active_invocation_and_global_recovery_pin_retirement(self):
        await self._cycle(1)
        entered, release = threading.Event(), threading.Event()
        await self._append(2)

        @stream_task
        def waiting(ctx, payload):
            entered.set()
            if not release.wait(10):
                raise RuntimeError("wait timeout")
            yield OutputSpec(
                payload,
                "bytes/v1",
                {"documents": ctx.managed_state["documents"] + [payload.decode()]},
            )

        running = asyncio.create_task(self._step(waiting))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 5))
            with self.assertRaisesRegex(PostgresProtocolError, "active invocation"):
                await self.retention.retire_latest("r1", "join", "pinned-invocation")
        finally:
            release.set()
            await running
        await self._cycle(3)
        plan = await self.coordinator.begin_recovery(
            *self._coordinator_args(), "active-global-recovery"
        )
        with self.assertRaisesRegex(PostgresProtocolError, "global recovery"):
            await self.retention.retire_latest("r1", "join", "pinned-recovery")
        await self.coordinator.finish_recovery(
            *self._coordinator_args(), plan.recovery_id
        )
        planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.coordinator, planner, self.revision
        )
        await regional.bind_revision_topology()
        await regional.bind_checkpoint_cuts(self.checkpoints[-1])
        regional_recovery = await regional.start_recovery(
            "coordinator",
            self.coordinator_epoch,
            "active-region",
            "join",
            self.checkpoints[-1],
            "regional-worker",
        )
        with self.assertRaisesRegex(PostgresProtocolError, "regional recovery"):
            await self.retention.retire_latest("r1", "join", "pinned-region")
        generation = regional_recovery.workers[0]
        await regional.replay_worker(
            regional_recovery.plan.plan_id,
            self.runtime,
            retain_document,
            "regional-worker",
            generation.recovery_owner_epoch,
        )
        await regional.activate_region(regional_recovery.plan.plan_id)
        await self.retention.retire_latest("r1", "join", "after-active-region")

    async def test_one_hundred_cycles_keep_hot_join_mailbox_bounded(self):
        for number in range(1, 101):
            checkpoint = await self._cycle(number)
            outcome = await self.retention.retire_latest(
                "r1", "join", f"retire-{number}"
            )
            self.assertEqual(outcome.checkpoint_id, checkpoint.checkpoint_id)
            pressure = await self.retention.pressure("r1", "join")
            self.assertEqual(
                pressure["hot_rows"],
                {"receipts": 0, "turns": 0, "invocations": 0},
            )
        anchor = (await self.retention.pressure("r1", "join"))["anchor"]
        self.assertEqual(anchor["retired_apply_through"], 100)
        self.assertEqual(anchor["retired_arrival_through"], 100)
        self.assertEqual(anchor["retired_turns"], 100)
        self.assertEqual(len(decoded(anchor["state_json"])["documents"]), 100)
        plan = await self.coordinator.begin_recovery(
            *self._coordinator_args(), "latest-after-retirement"
        )
        restored = await self.runtime.restore(
            plan.recovery_id, retain_document, "worker", self.worker_epoch
        )
        self.assertEqual(len(restored["documents"]), 100)
        await self._assert_oracles()

    async def test_skew_retires_only_a_contiguous_arrival_and_apply_prefix(self):
        fixture = join_fixtures.PostgresJoinTests("runTest")
        await fixture.asyncSetUp()
        try:
            retention = await PostgresJoinRetention.install(fixture.coordinator)
            checkpoint = await fixture.skew()
            self.assertEqual(await fixture.step("slow"), "ALIGNED")
            await fixture.coordinator.complete_checkpoint(
                *fixture.common(), checkpoint.checkpoint_id
            )
            first = await retention.retire_latest("r1", "join", "skew-first")
            self.assertEqual(
                (first.retired_apply_through, first.retired_arrival_through),
                (2, 2),
            )
            pressure = await retention.pressure("r1", "join")
            self.assertEqual(
                pressure["hot_rows"],
                {"receipts": 4, "turns": 2, "invocations": 2},
            )
            async with fixture.store.pool.acquire() as conn:
                roots = {
                    row["partition_id"]: row["sequence"]
                    for row in await conn.fetch(
                        """SELECT i.partition_id,d.sequence FROM join_inputs_v3 i
                           JOIN durable_refs d ON d.ref_kind='JOIN_INPUT'
                             AND d.ref_id=i.retention_ref
                           WHERE i.revision_id='r1' AND i.participant_id='join'"""
                    )
                }
            self.assertEqual(roots, {"fast": 3, "slow": 1})
            await self._assert_oracles(fixture.schema)
        finally:
            await fixture.asyncTearDown()


if __name__ == "__main__":
    unittest.main()
