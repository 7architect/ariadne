from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import unittest
import uuid
from pathlib import Path

import asyncpg
from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_checkpoint_runtime import (
    PostgresCheckpointRuntime,
    SharedStateContract,
)
from postgres_stream_v2 import PostgresProtocolError, PostgresStream, StaleOwner

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
HERE = Path(__file__).resolve().parent
ORACLE = HERE / "postgres_barrier_oracle.py"
DATA_ORACLE = HERE / "postgres_stream_oracle.py"
CRASH_WORKER = HERE / "postgres_barrier_crash_worker.py"


class PostgresBarrierCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds030_" + uuid.uuid4().hex[:12]
        self.stream = await PostgresStream.open(DSN, self.schema)
        for partition in ("left", "right"):
            await self.stream.create_partition(
                "input", partition, max_records=500, max_bytes=1_000_000
            )
        await self.stream.create_partition(
            "output", "p0", max_records=500, max_bytes=1_000_000
        )
        self.coordinator = await PostgresBarrierCoordinator.install(self.stream)
        max_retained = (
            1
            if self._testMethodName == "test_checkpoint_budget_is_public_and_hard"
            else 100
        )
        self.revision = await self.coordinator.register_revision(
            revision_id="revision-1",
            deployment_id="deployment-1",
            graph_version="graph-v1",
            sources=[("input", "left"), ("input", "right")],
            participants=[
                {
                    "participant_id": "join",
                    "inputs": [
                        {"channel": "input", "partition": "left"},
                        {"channel": "input", "partition": "right"},
                    ],
                    "outputs": [{"channel": "output", "partition": "p0"}],
                    "code_digest": "join-code-v1",
                    "state_schema": "join-state/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="code-manifest-v1",
            state_contract_digest="state-contract-v1",
            max_inflight_checkpoints=4,
            max_retained_checkpoints=max_retained,
        )
        self.owner = "coordinator-a"
        self.epoch = await self.coordinator.claim_coordinator(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            self.owner,
            lease_seconds=300,
        )
        self.checkpoint_runtime = PostgresCheckpointRuntime(
            self.coordinator,
            revision_id=self.revision.revision_id,
            graph_version=self.revision.graph_version,
            graph_digest=self.revision.graph_digest,
            owner_id=self.owner,
            owner_epoch=self.epoch,
            contract=SharedStateContract("join", "join-state/v1", 1, "join-code-v1"),
        )
        await self.checkpoint_runtime.bind()
        for partition in ("left", "right"):
            await self._append(partition, 1)

    async def asyncTearDown(self) -> None:
        if not hasattr(self, "stream"):
            return
        await self.stream.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        finally:
            await conn.close()

    async def _append(self, partition: str, number: int) -> None:
        await self.stream.append(
            f"attempt-{partition}-{number}",
            "input",
            partition,
            f"record-{partition}-{number}",
            f"object-{partition}-{number}",
            f"payload-{partition}-{number}".encode(),
            "bytes/v1",
            f"lineage-{partition}-{number}",
        )

    def _common(self) -> tuple[str, str, str, str, int]:
        return (
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            self.owner,
            self.epoch,
        )

    async def _budget_revision(self, label: str, **budgets):
        channel = f"budget-{label}"
        await self.stream.create_partition(
            channel, "p0", max_records=10, max_bytes=10_000
        )
        revision = await self.coordinator.register_revision(
            revision_id=f"revision-{label}",
            deployment_id=f"deployment-{label}",
            graph_version="graph-v1",
            sources=[(channel, "p0")],
            participants=[
                {
                    "participant_id": "worker",
                    "inputs": [{"channel": channel, "partition": "p0"}],
                    "outputs": [],
                    "code_digest": "worker-code-v1",
                    "state_schema": "worker-state/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="code-manifest-v1",
            state_contract_digest="state-contract-v1",
            **budgets,
        )
        owner = f"owner-{label}"
        epoch = await self.coordinator.claim_coordinator(
            revision.revision_id,
            revision.graph_version,
            revision.graph_digest,
            owner,
            lease_seconds=300,
        )
        return revision, (
            revision.revision_id,
            revision.graph_version,
            revision.graph_digest,
            owner,
            epoch,
        )

    async def _aligned(self, request_id: str):
        checkpoint = await self.coordinator.start_checkpoint(
            *self._common(), request_id
        )
        for partition in ("left", "right"):
            await self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, "input", partition
            )
        return checkpoint

    async def _completed(self, request_id: str):
        checkpoint = await self._aligned(request_id)
        await self.checkpoint_runtime.snapshot_aligned_checkpoint(
            checkpoint.checkpoint_id,
            {"count": 2},
            mailbox_highwater=2,
        )
        await self.coordinator.complete_checkpoint(
            *self._common(), checkpoint.checkpoint_id
        )
        return checkpoint

    async def _oracle(self) -> subprocess.CompletedProcess[str]:
        return await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )

    async def test_data_and_barrier_share_one_serial_order(self) -> None:
        checkpoint = await self.coordinator.start_checkpoint(
            *self._common(), "ordered-race"
        )
        await asyncio.gather(
            self._append("left", 2),
            self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, "input", "left"
            ),
        )
        await self._append("right", 2)
        await self.coordinator.inject_source_barrier(
            *self._common(), checkpoint.checkpoint_id, "input", "right"
        )
        for partition in ("left", "right"):
            log = await self.coordinator.ordered_log("input", partition)
            self.assertEqual(
                [row["log_sequence"] for row in log],
                list(range(1, len(log) + 1)),
            )
            barrier = next(row for row in log if row["entry_kind"] == "BARRIER")
            preceding = [
                row["data_sequence"]
                for row in log
                if row["entry_kind"] == "DATA"
                and row["log_sequence"] < barrier["log_sequence"]
            ]
            self.assertEqual(barrier["cut_data_sequence"], max(preceding, default=0))
        report = await self._oracle()
        self.assertEqual(report.returncode, 0, report.stdout + report.stderr)
        self.assertTrue((await self.stream.invariant_report())["passed"])
        data_oracle = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(DATA_ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(
            data_oracle.returncode, 0, data_oracle.stdout + data_oracle.stderr
        )

    async def test_compaction_race_and_output_barrier_do_not_lose_tail(self) -> None:
        """The partition lock serializes retention cuts with DATA/BARRIER order."""
        await self._append("left", 2)
        checkpoint = await self.coordinator.start_checkpoint(
            *self._common(), "retention-output-order"
        )
        left_barrier, _ = await asyncio.gather(
            self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, "input", "left"
            ),
            self.stream.compact("input", "left", 2),
        )
        self.assertEqual(left_barrier.cut_data_sequence, 2)
        await self.coordinator.inject_source_barrier(
            *self._common(), checkpoint.checkpoint_id, "input", "right"
        )

        # A post-cut record is the replay tail. The sealed retention root must
        # keep it live even when compaction asks to pass it.
        await self._append("left", 3)
        compacted = await self.stream.compact("input", "left", 3)
        self.assertEqual(compacted["safe_through"], 2)
        async with self.stream.pool.acquire() as conn:
            live = await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM records
                   WHERE channel='input' AND partition_id='left' AND sequence=3)"""
            )
        self.assertTrue(live)

        # The participant's output barrier is inserted through the same
        # ordered namespace after all output DATA already made durable.
        await self.stream.append(
            "attempt-output-1",
            "output",
            "p0",
            "record-output-1",
            "object-output-1",
            b"payload-output-1",
            "bytes/v1",
            "lineage-output-1",
        )
        await self.checkpoint_runtime.snapshot_aligned_checkpoint(
            checkpoint.checkpoint_id,
            {"count": 2},
            mailbox_highwater=2,
        )
        await self.stream.append(
            "attempt-output-2",
            "output",
            "p0",
            "record-output-2",
            "object-output-2",
            b"payload-output-2",
            "bytes/v1",
            "lineage-output-2",
        )
        output_log = await self.coordinator.ordered_log("output", "p0")
        self.assertEqual(
            [row["entry_kind"] for row in output_log],
            ["DATA", "BARRIER", "DATA"],
        )
        self.assertEqual(output_log[1]["barrier_role"], "OUTPUT")
        self.assertEqual(output_log[1]["cut_data_sequence"], 1)
        await self.coordinator.complete_checkpoint(
            *self._common(), checkpoint.checkpoint_id
        )

        async with self.stream.pool.acquire() as conn:
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """UPDATE partition_log_entries SET entry_kind='BARRIER'
                       WHERE channel='output' AND partition_id='p0'
                         AND log_sequence=1"""
                )
        report = await self._oracle()
        self.assertEqual(report.returncode, 0, report.stdout + report.stderr)

    async def test_takeover_resumes_intent_and_fences_old_owner(self) -> None:
        checkpoint = await self.coordinator.start_checkpoint(
            *self._common(), "takeover"
        )
        await self.coordinator.inject_source_barrier(
            *self._common(), checkpoint.checkpoint_id, "input", "left"
        )
        with self.assertRaises(StaleOwner):
            await self.coordinator.claim_coordinator(
                self.revision.revision_id,
                self.revision.graph_version,
                self.revision.graph_digest,
                "coordinator-b",
                lease_seconds=30,
            )
        await self.coordinator.renew_coordinator(
            self.revision.revision_id, self.owner, self.epoch, lease_seconds=0.05
        )
        async with self.stream.pool.acquire() as conn:
            await conn.execute("SELECT pg_sleep(0.08)")
        new_epoch = await self.coordinator.claim_coordinator(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            "coordinator-b",
            lease_seconds=30,
        )
        with self.assertRaises(StaleOwner):
            await self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, "input", "right"
            )
        new_common = (
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            "coordinator-b",
            new_epoch,
        )
        await self.coordinator.inject_source_barrier(
            *new_common, checkpoint.checkpoint_id, "input", "right"
        )
        await self.coordinator.acknowledge_participant(
            *new_common,
            checkpoint.checkpoint_id,
            "join",
            {"count": 2},
            mailbox_highwater=2,
            state_version=1,
        )
        await self.coordinator.complete_checkpoint(
            *new_common, checkpoint.checkpoint_id
        )
        self.owner, self.epoch = "coordinator-b", new_epoch
        self.assertEqual(
            (
                await self.coordinator.latest_completed(
                    self.revision.revision_id,
                    self.revision.graph_version,
                    self.revision.graph_digest,
                )
            ).checkpoint_id,
            checkpoint.checkpoint_id,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "graph identity drift"):
            await self.coordinator.latest_completed(
                self.revision.revision_id, "graph-v2", self.revision.graph_digest
            )
        report = await self._oracle()
        self.assertEqual(report.returncode, 0, report.stdout + report.stderr)

    async def test_recovery_pin_cannot_be_released_or_compacted(self) -> None:
        checkpoint = await self._completed("retention-authority")
        await self._append("left", 2)
        plan = await self.coordinator.begin_recovery(
            *self._common(), "retention-recovery"
        )
        async with self.stream.pool.acquire() as conn:
            ref_id = await conn.fetchval(
                """SELECT retention_ref_id FROM source_barrier_observations
                   WHERE checkpoint_id=$1 AND channel='input'
                     AND partition_id='left'""",
                checkpoint.checkpoint_id,
            )
        with self.assertRaises(asyncpg.PostgresError):
            await self.stream.release_checkpoint(ref_id)
        compacted = await self.stream.compact("input", "left", 2)
        self.assertEqual(compacted["safe_through"], 1)
        async with self.stream.pool.acquire() as conn:
            self.assertTrue(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM records
                   WHERE channel='input' AND partition_id='left' AND sequence=2)"""
                )
            )
        ready = await self.coordinator.finish_recovery(
            *self._common(), plan.recovery_id
        )
        self.assertEqual(ready.status, "READY")
        report = await self._oracle()
        self.assertEqual(report.returncode, 0, report.stdout + report.stderr)

    async def test_ordered_log_has_a_hard_accounted_capacity(self) -> None:
        await self.stream.create_partition(
            "bounded-log",
            "p0",
            max_records=10,
            max_bytes=10_000,
            max_metadata_records=100,
        )
        await self.coordinator.configure_ordered_log_capacity("bounded-log", "p0", 1)
        await self.stream.append(
            "bounded-log-1",
            "bounded-log",
            "p0",
            "bounded-record-1",
            "bounded-object-1",
            b"one",
            "bytes/v1",
            "bounded-lineage-1",
        )
        with self.assertRaisesRegex(asyncpg.PostgresError, "metadata budget"):
            await self.stream.append(
                "bounded-log-2",
                "bounded-log",
                "p0",
                "bounded-record-2",
                "bounded-object-2",
                b"two",
                "bytes/v1",
                "bounded-lineage-2",
            )
        async with self.stream.pool.acquire() as conn:
            head = await conn.fetchrow(
                """SELECT * FROM partition_log_heads
                   WHERE channel='bounded-log' AND partition_id='p0'"""
            )
        self.assertEqual(head["used_log_entries"], head["max_log_entries"])

    async def test_oracle_reports_missing_barrier_without_crashing(self) -> None:
        checkpoint = await self._completed("missing-barrier-control")
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                """ALTER TABLE source_barrier_observations DISABLE TRIGGER
                   source_barrier_immutable"""
            )
            await conn.execute(
                """ALTER TABLE source_barrier_observations DISABLE TRIGGER
                   barrier_authority_link"""
            )
            await conn.execute(
                """DELETE FROM source_barrier_observations
                   WHERE checkpoint_id=$1 AND channel='input'
                     AND partition_id='right'""",
                checkpoint.checkpoint_id,
            )
            await conn.execute(
                """ALTER TABLE source_barrier_observations ENABLE TRIGGER
                   barrier_authority_link"""
            )
            await conn.execute(
                """ALTER TABLE source_barrier_observations ENABLE TRIGGER
                   source_barrier_immutable"""
            )
        report = await self._oracle()
        self.assertEqual(report.returncode, 2, report.stdout + report.stderr)
        parsed = json.loads(report.stdout)
        self.assertFalse(parsed["passed"])
        self.assertTrue(
            any(
                item.startswith("participant_missing_input:")
                for item in parsed["violations"]
            )
        )

    async def test_complete_only_selection_and_frozen_recovery_plan(self) -> None:
        first = await self._completed("first-complete")
        second = await self._aligned("second-incomplete")
        latest = await self.coordinator.latest_completed(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
        )
        self.assertEqual(latest.checkpoint_id, first.checkpoint_id)
        plan = await self.coordinator.begin_recovery(*self._common(), "recovery-one")
        self.assertEqual(plan.checkpoint_id, first.checkpoint_id)
        restored = await self.checkpoint_runtime.restore_frozen_plan(plan)
        self.assertEqual(restored.state, {"count": 2})
        self.assertEqual(restored.checkpoint_id, first.checkpoint_id)
        incompatible_code = PostgresCheckpointRuntime(
            self.coordinator,
            revision_id=self.revision.revision_id,
            graph_version=self.revision.graph_version,
            graph_digest=self.revision.graph_digest,
            owner_id=self.owner,
            owner_epoch=self.epoch,
            contract=SharedStateContract(
                "join", "join-state/v1", 1, "join-code-forged"
            ),
        )
        with self.assertRaisesRegex(PostgresProtocolError, "contract is incompatible"):
            await incompatible_code.bind()
        incompatible_version = PostgresCheckpointRuntime(
            self.coordinator,
            revision_id=self.revision.revision_id,
            graph_version=self.revision.graph_version,
            graph_digest=self.revision.graph_digest,
            owner_id=self.owner,
            owner_epoch=self.epoch,
            contract=SharedStateContract("join", "join-state/v1", 2, "join-code-v1"),
        )
        with self.assertRaisesRegex(PostgresProtocolError, "contract is incompatible"):
            await incompatible_version.bind()
        forged_plan = type(plan)(
            recovery_id="recovery:forged-without-row",
            checkpoint_id=plan.checkpoint_id,
            revision_id=plan.revision_id,
            target_data_sequences=plan.target_data_sequences,
            participant_snapshots=plan.participant_snapshots,
            plan_digest=plan.plan_digest,
            status=plan.status,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "no durable authority"):
            await self.checkpoint_runtime.restore_frozen_plan(forged_plan)
        await self.coordinator.acknowledge_participant(
            *self._common(),
            second.checkpoint_id,
            "join",
            {"count": 2},
            mailbox_highwater=2,
            state_version=1,
        )
        await self.coordinator.complete_checkpoint(
            *self._common(), second.checkpoint_id
        )
        retry = await self.coordinator.begin_recovery(*self._common(), "recovery-one")
        self.assertEqual(retry.plan_digest, plan.plan_digest)
        self.assertEqual(retry.checkpoint_id, first.checkpoint_id)
        ready = await self.coordinator.finish_recovery(
            *self._common(), plan.recovery_id
        )
        self.assertEqual(ready.status, "READY")
        next_plan = await self.coordinator.begin_recovery(
            *self._common(), "recovery-two"
        )
        self.assertEqual(next_plan.checkpoint_id, second.checkpoint_id)
        report = await self._oracle()
        self.assertEqual(report.returncode, 0, report.stdout + report.stderr)

    async def test_identity_drift_storage_guards_and_unsafe_control(self) -> None:
        checkpoint = await self._completed("immutable")
        with self.assertRaisesRegex(PostgresProtocolError, "state version"):
            await self.coordinator.acknowledge_participant(
                *self._common(),
                checkpoint.checkpoint_id,
                "join",
                {"count": 2},
                mailbox_highwater=2,
                state_version=999,
            )
        with self.assertRaisesRegex(PostgresProtocolError, "acknowledgement drift"):
            await self.coordinator.acknowledge_participant(
                *self._common(),
                checkpoint.checkpoint_id,
                "join",
                {"count": 999},
                mailbox_highwater=2,
                state_version=1,
            )
        async with self.stream.pool.acquire() as conn:
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "UPDATE checkpoint_intents_v2 SET graph_digest='forged' "
                    "WHERE checkpoint_id=$1",
                    checkpoint.checkpoint_id,
                )
            await conn.execute(
                "ALTER TABLE checkpoint_lifecycle_v2 DISABLE TRIGGER "
                "checkpoint_lifecycle_v2_guard"
            )
            await conn.execute(
                "ALTER TABLE checkpoint_completions_v2 DISABLE TRIGGER "
                "completion_v2_immutable"
            )
            original = await conn.fetchrow(
                """SELECT completion_authority_ordinal,writer_epoch
                   FROM checkpoint_completions_v2 WHERE checkpoint_id=$1""",
                checkpoint.checkpoint_id,
            )
            intent_ordinal = await conn.fetchval(
                """SELECT intent_authority_ordinal FROM checkpoint_intents_v2
                   WHERE checkpoint_id=$1""",
                checkpoint.checkpoint_id,
            )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """UPDATE checkpoint_completions_v2
                       SET completion_authority_ordinal=$2,writer_epoch=999
                       WHERE checkpoint_id=$1""",
                    checkpoint.checkpoint_id,
                    intent_ordinal,
                )
            await conn.execute(
                """ALTER TABLE checkpoint_completions_v2 DISABLE TRIGGER
                   completion_authority_link"""
            )
            await conn.execute(
                """UPDATE checkpoint_completions_v2
                   SET completion_authority_ordinal=$2,writer_epoch=999
                   WHERE checkpoint_id=$1""",
                checkpoint.checkpoint_id,
                intent_ordinal,
            )
        authority_report = await self._oracle()
        self.assertEqual(
            authority_report.returncode,
            2,
            authority_report.stdout + authority_report.stderr,
        )
        self.assertIn(
            f"completion_authority:{checkpoint.checkpoint_id}",
            json.loads(authority_report.stdout)["violations"],
        )
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                """UPDATE checkpoint_completions_v2
                   SET completion_authority_ordinal=$2,writer_epoch=$3
                   WHERE checkpoint_id=$1""",
                checkpoint.checkpoint_id,
                original["completion_authority_ordinal"],
                original["writer_epoch"],
            )
            await conn.execute(
                """ALTER TABLE checkpoint_completions_v2 ENABLE TRIGGER
                   completion_authority_link"""
            )
            await conn.execute(
                "DELETE FROM checkpoint_completions_v2 WHERE checkpoint_id=$1",
                checkpoint.checkpoint_id,
            )
            await conn.execute(
                "ALTER TABLE checkpoint_completions_v2 ENABLE TRIGGER "
                "completion_v2_immutable"
            )
            await conn.execute(
                "ALTER TABLE checkpoint_lifecycle_v2 ENABLE TRIGGER "
                "checkpoint_lifecycle_v2_guard"
            )
        report = await self._oracle()
        self.assertEqual(report.returncode, 2, report.stdout + report.stderr)
        violations = json.loads(report.stdout)["violations"]
        self.assertIn(f"completion_projection:{checkpoint.checkpoint_id}", violations)

    async def test_checkpoint_budget_is_public_and_hard(self) -> None:
        await self._completed("only-retained-slot")
        pressure = await self.coordinator.checkpoint_pressure(self.revision.revision_id)
        self.assertIn("retained_checkpoints", pressure["binding_dimensions"])
        with self.assertRaisesRegex(PostgresProtocolError, "retention budget"):
            await self.coordinator.start_checkpoint(
                *self._common(), "blocked-by-budget"
            )

        _, inflight_common = await self._budget_revision(
            "inflight",
            max_inflight_checkpoints=1,
        )
        await self.coordinator.start_checkpoint(*inflight_common, "inflight-1")
        with self.assertRaisesRegex(PostgresProtocolError, "inflight budget"):
            await self.coordinator.start_checkpoint(*inflight_common, "inflight-2")

        metadata_revision, metadata_common = await self._budget_revision(
            "metadata",
            max_checkpoint_metadata_bytes=1000,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "metadata-byte budget"):
            await self.coordinator.start_checkpoint(*metadata_common, "metadata-1")
        metadata_pressure = await self.coordinator.checkpoint_pressure(
            metadata_revision.revision_id
        )
        self.assertLessEqual(
            metadata_pressure["metadata_bytes"],
            metadata_pressure["max_checkpoint_metadata_bytes"],
        )

        age_revision, age_common = await self._budget_revision(
            "age",
            max_checkpoint_age_seconds=0.01,
        )
        await self.coordinator.start_checkpoint(*age_common, "age-1")
        async with self.stream.pool.acquire() as conn:
            await conn.execute("SELECT pg_sleep(0.02)")
        age_pressure = await self.coordinator.checkpoint_pressure(
            age_revision.revision_id
        )
        self.assertIn("metadata_age", age_pressure["binding_dimensions"])
        with self.assertRaisesRegex(PostgresProtocolError, "metadata-age budget"):
            await self.coordinator.start_checkpoint(*age_common, "age-2")

        _, event_common = await self._budget_revision(
            "events",
            max_authority_events=1,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "authority-event budget"):
            await self.coordinator.start_checkpoint(*event_common, "event-1")

    async def test_real_process_crash_matrix_exact_retry(self) -> None:
        def crash(
            operation: str, identity: str, point: str
        ) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [
                    sys.executable,
                    str(CRASH_WORKER),
                    DSN,
                    self.schema,
                    self.revision.revision_id,
                    self.revision.graph_version,
                    self.revision.graph_digest,
                    self.owner,
                    str(self.epoch),
                    operation,
                    identity,
                    point,
                ],
                cwd=HERE,
                text=True,
                capture_output=True,
                check=False,
            )

        for point in ("intent_before_commit", "intent_after_commit"):
            request_id = f"crash-{point}"
            result = await asyncio.to_thread(crash, "intent", request_id, point)
            self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
            checkpoint = await self.coordinator.start_checkpoint(
                *self._common(), request_id
            )
            await self.coordinator.abort_checkpoint(
                *self._common(), checkpoint.checkpoint_id, "matrix cleanup"
            )

        for point in (
            "barrier_after_partition_lock",
            "barrier_before_commit",
            "barrier_after_commit",
        ):
            checkpoint = await self.coordinator.start_checkpoint(
                *self._common(), f"crash-{point}"
            )
            await self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, "input", "right"
            )
            result = await asyncio.to_thread(
                crash, "barrier", checkpoint.checkpoint_id, point
            )
            self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
            await self.coordinator.inject_source_barrier(
                *self._common(), checkpoint.checkpoint_id, "input", "left"
            )
            await self.coordinator.abort_checkpoint(
                *self._common(), checkpoint.checkpoint_id, "matrix cleanup"
            )

        for point in ("participant_before_commit", "participant_after_commit"):
            checkpoint = await self._aligned(f"crash-{point}")
            result = await asyncio.to_thread(
                crash, "participant", checkpoint.checkpoint_id, point
            )
            self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
            await self.coordinator.acknowledge_participant(
                *self._common(),
                checkpoint.checkpoint_id,
                "join",
                {"count": 2},
                mailbox_highwater=2,
                state_version=1,
            )
            await self.coordinator.abort_checkpoint(
                *self._common(), checkpoint.checkpoint_id, "matrix cleanup"
            )

        last_completed = None
        for point in (
            "complete_after_validation",
            "complete_before_commit",
            "complete_after_commit",
        ):
            checkpoint = await self._aligned(f"crash-{point}")
            await self.coordinator.acknowledge_participant(
                *self._common(),
                checkpoint.checkpoint_id,
                "join",
                {"count": 2},
                mailbox_highwater=2,
                state_version=1,
            )
            result = await asyncio.to_thread(
                crash, "complete", checkpoint.checkpoint_id, point
            )
            self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
            await self.coordinator.complete_checkpoint(
                *self._common(), checkpoint.checkpoint_id
            )
            last_completed = checkpoint

        self.assertIsNotNone(last_completed)
        for point in ("recovery_plan_before_commit", "recovery_plan_after_commit"):
            request_id = f"crash-{point}"
            result = await asyncio.to_thread(crash, "recovery_plan", request_id, point)
            self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
            plan = await self.coordinator.begin_recovery(*self._common(), request_id)
            await self.coordinator.finish_recovery(*self._common(), plan.recovery_id)

        plan = await self.coordinator.begin_recovery(
            *self._common(), "crash-ready-plan"
        )
        for point in ("recovery_ready_before_commit", "recovery_ready_after_commit"):
            if point.endswith("after_commit"):
                plan = await self.coordinator.begin_recovery(
                    *self._common(), "crash-ready-plan-2"
                )
            result = await asyncio.to_thread(
                crash, "recovery_ready", plan.recovery_id, point
            )
            self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
            ready = await self.coordinator.finish_recovery(
                *self._common(), plan.recovery_id
            )
            self.assertEqual(ready.status, "READY")

        report = await self._oracle()
        self.assertEqual(report.returncode, 0, report.stdout + report.stderr)


if __name__ == "__main__":
    unittest.main()
