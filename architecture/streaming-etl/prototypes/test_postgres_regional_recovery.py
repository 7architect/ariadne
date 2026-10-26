from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import unittest
import uuid
from pathlib import Path

import asyncpg
from postgres_barrier_coordinator import PostgresBarrierCoordinator, decoded
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_runtime import PostgresJoinRuntime
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_stream_v2 import (
    PostgresProtocolError,
    PostgresStream,
    StaleOwner,
    content_object_key,
)

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN", "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming"
)
HERE = Path(__file__).resolve().parent


@stream_task
def pass_document(ctx, payload):
    state = {"documents": ctx.managed_state.get("documents", []) + [payload.decode()]}
    yield OutputSpec(payload, "bytes/v1", state)


@stream_task
def divergent_document(ctx, payload):
    state = {
        "documents": ctx.managed_state.get("documents", [])
        + ["divergent:" + payload.decode()]
    }
    yield OutputSpec(b"divergent:" + payload, "bytes/v1", state)


class PostgresRegionalRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "ds031b_" + uuid.uuid4().hex[:12]
        self.store = await PostgresStream.open(DSN, self.schema)
        for partition in ("source", "a", "b", "c", "j", "d"):
            await self.store.create_partition(
                "dag", partition, max_records=1000, max_bytes=1_000_000
            )
        self.coordinator = await PostgresBarrierCoordinator.install(self.store)
        self.topology = {
            "A": (["source"], "a"),
            "B": (["a"], "b"),
            "C": (["a"], "c"),
            "J": (["b", "c"], "j"),
            "D": (["j"], "d"),
        }
        participants = [
            {
                "participant_id": name,
                "inputs": [
                    {"channel": "dag", "partition": partition} for partition in inputs
                ],
                "outputs": [{"channel": "dag", "partition": output}],
                "code_digest": "pass-document-v1",
                "state_schema": "documents/v1",
                "state_version": 1,
            }
            for name, (inputs, output) in self.topology.items()
        ]
        self.revision = await self.coordinator.register_revision(
            revision_id="dag-r1",
            deployment_id="dag-d1",
            graph_version="v1",
            sources=[("dag", "source")],
            participants=participants,
            code_manifest_digest="dag-code-v1",
            state_contract_digest="dag-state-v1",
        )
        self.coordinator_epoch = await self.coordinator.claim_coordinator(
            "dag-r1",
            "v1",
            self.revision.graph_digest,
            "coordinator",
            lease_seconds=300,
        )
        self.runtimes = {
            name: PostgresJoinRuntime(self.coordinator, self.revision, name)
            for name in self.topology
        }
        self.epochs = {}
        for name, runtime in self.runtimes.items():
            await runtime.install(initial_state={"documents": []})
            self.epochs[name] = await runtime.claim(name, lease_seconds=300)
        self.planner = await PostgresRecoveryRegionPlanner.install(self.coordinator)
        self.regional = await PostgresRegionalRecovery.install(
            self.coordinator, self.planner, self.revision
        )
        await self.regional.bind_revision_topology(
            nonreplayable_edges={
                self.regional.edge_id("dag-r1", "dag", partition, target)
                for partition, target in getattr(self, "nonreplayable_inputs", ())
            }
        )
        await self._process_document(1)
        await self._process_document(2)
        self.checkpoint = await self._checkpoint()
        await self.regional.bind_checkpoint_cuts(self.checkpoint.checkpoint_id)
        await self._process_document(3)
        await self._process_document(4)

    async def asyncTearDown(self):
        await self.store.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        finally:
            await conn.close()

    def _coordinator_args(self):
        return (
            "dag-r1",
            "v1",
            self.revision.graph_digest,
            "coordinator",
            self.coordinator_epoch,
        )

    async def _step(self, participant: str, partition: str) -> str:
        return await self.runtimes[participant].step(
            "dag/" + partition,
            pass_document,
            participant,
            self.epochs[participant],
            coordinator_owner="coordinator",
            coordinator_epoch=self.coordinator_epoch,
        )

    async def _append_source(self, number: int) -> None:
        payload = f"document-{number}".encode()
        await self.store.append(
            f"source-attempt-{number}",
            "dag",
            "source",
            f"document-{number}",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            f"source-lineage-{number}",
        )

    async def _process_document(self, number: int) -> None:
        await self._append_source(number)
        for participant, partition in (
            ("A", "source"),
            ("B", "a"),
            ("C", "a"),
            ("J", "b"),
            ("J", "c"),
            ("D", "j"),
            ("D", "j"),
        ):
            self.assertEqual(await self._step(participant, partition), "APPLIED")

    async def _checkpoint(self):
        checkpoint = await self.coordinator.start_checkpoint(
            *self._coordinator_args(), "regional-checkpoint"
        )
        await self.coordinator.inject_source_barrier(
            *self._coordinator_args(), checkpoint.checkpoint_id, "dag", "source"
        )
        for participant, partition, expected in (
            ("A", "source", "ALIGNED"),
            ("B", "a", "ALIGNED"),
            ("C", "a", "ALIGNED"),
            ("J", "b", "WAITING"),
            ("J", "c", "ALIGNED"),
            ("D", "j", "ALIGNED"),
        ):
            self.assertEqual(await self._step(participant, partition), expected)
        await self.coordinator.complete_checkpoint(
            *self._coordinator_args(), checkpoint.checkpoint_id
        )
        return checkpoint

    async def _worker_rows(self):
        async with self.store.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM join_workers WHERE revision_id='dag-r1'"
            )
        return {row["participant_id"]: row for row in rows}

    async def _fresh_runtime_handles(self) -> None:
        await self.store.close()
        self.store = await PostgresStream.connect_existing(DSN, self.schema)
        self.coordinator = PostgresBarrierCoordinator(self.store)
        self.planner = PostgresRecoveryRegionPlanner(self.coordinator)
        self.regional = PostgresRegionalRecovery(
            self.coordinator, self.planner, self.revision
        )
        self.runtimes = {
            name: PostgresJoinRuntime(self.coordinator, self.revision, name)
            for name in self.topology
        }

    async def _start(self, request_id: str = "recover-b"):
        return await self.regional.start_recovery(
            "coordinator",
            self.coordinator_epoch,
            request_id,
            "B",
            self.checkpoint.checkpoint_id,
            "regional-worker",
        )

    async def _coordinator_oracle(self) -> None:
        result = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                str(HERE / "postgres_barrier_oracle.py"),
                DSN,
                self.schema,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    async def _replay_all(self, recovery) -> None:
        epochs = {
            generation.participant_id: generation.recovery_owner_epoch
            for generation in recovery.workers
        }
        for participant in ("B", "J", "D"):
            await self.regional.replay_worker(
                recovery.plan.plan_id,
                self.runtimes[participant],
                pass_document,
                "regional-worker",
                epochs[participant],
            )

    async def _replay_current(self, plan_id: str, owner_id: str) -> None:
        generations = {
            generation.participant_id: generation
            for generation in await self.regional.generations(plan_id)
        }
        for participant in ("B", "J", "D"):
            await self.regional.replay_worker(
                plan_id,
                self.runtimes[participant],
                pass_document,
                owner_id,
                generations[participant].recovery_owner_epoch,
            )

    async def test_failed_branch_and_tail_restore_without_stopping_healthy_sibling(
        self,
    ):
        before = await self._worker_rows()
        output_counts = {}
        async with self.store.pool.acquire() as conn:
            for partition in ("b", "j", "d"):
                output_counts[partition] = await conn.fetchval(
                    "SELECT count(*) FROM records WHERE channel='dag' AND partition_id=$1",
                    partition,
                )

        recovery = await self._start()
        await self._coordinator_oracle()
        self.assertEqual(recovery.plan.region, ("B", "D", "J"))
        self.assertEqual(
            {generation.participant_id for generation in recovery.workers},
            {"B", "J", "D"},
        )
        fenced = await self._worker_rows()
        for participant in ("B", "J", "D"):
            self.assertEqual(fenced[participant]["phase"], "RESTORING")
            self.assertEqual(
                fenced[participant]["owner_epoch"],
                before[participant]["owner_epoch"] + 1,
            )
        self.assertEqual(
            (
                fenced["C"]["owner_id"],
                fenced["C"]["owner_epoch"],
                decoded(fenced["C"]["state"]),
            ),
            (
                before["C"]["owner_id"],
                before["C"]["owner_epoch"],
                decoded(before["C"]["state"]),
            ),
        )

        # Healthy A/C continue while the failed B->J->D region stays fenced.
        await self._append_source(5)
        self.assertEqual(await self._step("A", "source"), "APPLIED")
        self.assertEqual(await self._step("C", "a"), "APPLIED")
        with self.assertRaises((StaleOwner, PostgresProtocolError)):
            await self._step("B", "a")
        healthy = await self._worker_rows()
        self.assertEqual(healthy["C"]["owner_epoch"], before["C"]["owner_epoch"])
        self.assertEqual(len(decoded(healthy["C"]["state"])["documents"]), 5)

        await self._replay_all(recovery)
        await self._coordinator_oracle()
        ready = await self.regional.generations(recovery.plan.plan_id)
        self.assertTrue(all(generation.status == "READY" for generation in ready))
        with self.assertRaisesRegex(PostgresProtocolError, "restoring"):
            await self.runtimes["B"].step(
                "dag/a",
                pass_document,
                "regional-worker",
                next(
                    generation.recovery_owner_epoch
                    for generation in ready
                    if generation.participant_id == "B"
                ),
                coordinator_owner="coordinator",
                coordinator_epoch=self.coordinator_epoch,
            )
        await self.regional.activate_region(recovery.plan.plan_id)
        await self._coordinator_oracle()
        active = await self.regional.generations(recovery.plan.plan_id)
        self.assertTrue(all(generation.status == "ACTIVE" for generation in active))

        after = await self._worker_rows()
        for participant in ("B", "J", "D"):
            self.assertEqual(after[participant]["phase"], "LIVE")
            self.assertEqual(
                decoded(after[participant]["state"]),
                decoded(before[participant]["state"]),
            )
        self.assertEqual(decoded(after["C"]["state"]), decoded(healthy["C"]["state"]))
        async with self.store.pool.acquire() as conn:
            for partition, count in output_counts.items():
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel='dag' AND partition_id=$1",
                        partition,
                    ),
                    count,
                )

    async def test_region_fence_is_atomic_and_fault_rolls_back_plan(self):
        before = await self._worker_rows()

        def fail(point):
            if point == "regional_workers_fenced":
                raise RuntimeError("process-loss control")

        with self.assertRaisesRegex(RuntimeError, "process-loss control"):
            await self.regional.start_recovery(
                "coordinator",
                self.coordinator_epoch,
                "rollback-region",
                "B",
                self.checkpoint.checkpoint_id,
                "regional-worker",
                fault=fail,
            )
        after = await self._worker_rows()
        for participant in self.topology:
            self.assertEqual(
                (
                    after[participant]["owner_id"],
                    after[participant]["owner_epoch"],
                    after[participant]["phase"],
                ),
                (
                    before[participant]["owner_id"],
                    before[participant]["owner_epoch"],
                    before[participant]["phase"],
                ),
            )
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM region_recovery_plans_v1"), 0
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM regional_recovery_workers_v1"
                ),
                0,
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM recovery_plans_v2"), 0
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM coordinator_authority_events WHERE event_type='RECOVERY_FENCE'"
                ),
                0,
            )

    async def test_divergent_replay_cannot_be_ready_or_active(self):
        recovery = await self._start("divergent-region")
        generation = next(
            item for item in recovery.workers if item.participant_id == "B"
        )
        with self.assertRaisesRegex(PostgresProtocolError, "diverged"):
            await self.regional.replay_worker(
                recovery.plan.plan_id,
                self.runtimes["B"],
                divergent_document,
                "regional-worker",
                generation.recovery_owner_epoch,
            )
        statuses = {
            item.participant_id: item.status
            for item in await self.regional.generations(recovery.plan.plan_id)
        }
        self.assertEqual(statuses["B"], "RESTORING")
        with self.assertRaisesRegex(PostgresProtocolError, "every worker READY"):
            await self.regional.activate_region(recovery.plan.plan_id)

    async def test_partial_replay_cannot_publish_a_partial_live_region(self):
        recovery = await self._start("partial-region")
        generation = next(
            item for item in recovery.workers if item.participant_id == "B"
        )
        await self.regional.replay_worker(
            recovery.plan.plan_id,
            self.runtimes["B"],
            pass_document,
            "regional-worker",
            generation.recovery_owner_epoch,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "every worker READY"):
            await self.regional.activate_region(recovery.plan.plan_id)
        # A generic per-worker restore has enough local READY evidence to pass
        # join_worker_guard.  The deferred region constraint must still reject
        # publishing B without J and D in the same commit.
        async with self.store.pool.acquire() as conn:
            with self.assertRaisesRegex(asyncpg.PostgresError, "activation is partial"):
                await conn.execute(
                    """UPDATE join_workers SET phase='LIVE'
                       WHERE revision_id='dag-r1' AND participant_id='B'"""
                )
        workers = await self._worker_rows()
        self.assertTrue(
            all(
                workers[participant]["phase"] == "RESTORING"
                for participant in ("B", "J", "D")
            )
        )

    async def test_expired_recovery_generation_has_no_replay_authority(self):
        recovery = await self._start("expired-region")
        generation = next(
            item for item in recovery.workers if item.participant_id == "B"
        )
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                """UPDATE join_workers
                   SET lease_expires_at=clock_timestamp()-interval '1 second'
                   WHERE revision_id='dag-r1' AND participant_id IN('B','J','D')"""
            )
        with self.assertRaisesRegex(StaleOwner, "lease expired"):
            await self.regional.replay_worker(
                recovery.plan.plan_id,
                self.runtimes["B"],
                pass_document,
                "regional-worker",
                generation.recovery_owner_epoch,
            )
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                """ALTER TABLE regional_recovery_attempt_workers_v1
                   DISABLE TRIGGER regional_attempt_worker_immutable"""
            )
            await conn.execute(
                """ALTER TABLE regional_recovery_workers_v1
                   DISABLE TRIGGER regional_generation_no_delete"""
            )
            await conn.execute(
                """DELETE FROM regional_recovery_attempt_workers_v1
                   WHERE plan_id=$1 AND participant_id='B'""",
                recovery.plan.plan_id,
            )
            await conn.execute(
                """DELETE FROM regional_recovery_workers_v1
                   WHERE plan_id=$1 AND participant_id='B'""",
                recovery.plan.plan_id,
            )
            await conn.execute(
                """ALTER TABLE regional_recovery_workers_v1
                   ENABLE TRIGGER regional_generation_no_delete"""
            )
            await conn.execute(
                """ALTER TABLE regional_recovery_attempt_workers_v1
                   ENABLE TRIGGER regional_attempt_worker_immutable"""
            )
        oracle = await asyncio.to_thread(
            subprocess.run,
            [
                sys.executable,
                str(HERE / "postgres_barrier_oracle.py"),
                DSN,
                self.schema,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(oracle.returncode, 2, oracle.stdout + oracle.stderr)
        self.assertIn("regional_generation_manifest", oracle.stdout)

    async def test_recovery_owner_takeover_is_append_only_and_fences_old_attempt(self):
        recovery = await self._start("takeover-region")
        async with self.store.pool.acquire() as conn:
            frozen = {
                row["participant_id"]: (
                    row["generation_digest"],
                    row["snapshot_object_key"],
                    row["mailbox_highwater"],
                    row["target_apply"],
                )
                for row in await conn.fetch(
                    "SELECT * FROM regional_recovery_workers_v1 WHERE plan_id=$1",
                    recovery.plan.plan_id,
                )
            }
        with self.assertRaisesRegex(StaleOwner, "live lease"):
            await self.regional.takeover_recovery(recovery.plan.plan_id, "replacement")
        await self.regional.renew_recovery(
            recovery.plan.plan_id, 1, "regional-worker", lease_seconds=300
        )
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                """UPDATE join_workers
                   SET lease_expires_at=clock_timestamp()-interval '1 second'
                   WHERE revision_id='dag-r1' AND participant_id IN('B','J','D')"""
            )

        def fail_takeover(point):
            if point == "regional_takeover_workers_fenced":
                raise RuntimeError("takeover process-loss control")

        with self.assertRaisesRegex(RuntimeError, "process-loss control"):
            await self.regional.takeover_recovery(
                recovery.plan.plan_id,
                "lost-replacement",
                fault=fail_takeover,
            )
        self.assertTrue(
            all(
                generation.attempt_ordinal == 1
                for generation in await self.regional.generations(recovery.plan.plan_id)
            )
        )

        def lose_takeover_ack(point):
            if point == "regional_takeover_committed":
                raise RuntimeError("lost takeover ACK")

        with self.assertRaisesRegex(RuntimeError, "lost takeover ACK"):
            await self.regional.takeover_recovery(
                recovery.plan.plan_id,
                "replacement",
                request_id="takeover-two",
                fault=lose_takeover_ack,
            )
        takeover = await self.regional.takeover_recovery(
            recovery.plan.plan_id,
            "replacement",
            request_id="takeover-two",
        )
        self.assertEqual(takeover.attempt_ordinal, 2)
        self.assertEqual({item[0] for item in takeover.workers}, {"B", "J", "D"})
        current = await self.regional.generations(recovery.plan.plan_id)
        self.assertTrue(
            all(
                generation.recovery_owner_id == "replacement"
                and generation.attempt_ordinal == 2
                for generation in current
            )
        )
        old_b = next(item for item in recovery.workers if item.participant_id == "B")
        with self.assertRaisesRegex(StaleOwner, "fenced"):
            await self.regional.replay_worker(
                recovery.plan.plan_id,
                self.runtimes["B"],
                pass_document,
                "regional-worker",
                old_b.recovery_owner_epoch,
            )
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM regional_recovery_attempts_v1 WHERE plan_id=$1",
                    recovery.plan.plan_id,
                ),
                2,
            )
            after = {
                row["participant_id"]: (
                    row["generation_digest"],
                    row["snapshot_object_key"],
                    row["mailbox_highwater"],
                    row["target_apply"],
                )
                for row in await conn.fetch(
                    "SELECT * FROM regional_recovery_workers_v1 WHERE plan_id=$1",
                    recovery.plan.plan_id,
                )
            }
            self.assertEqual(after, frozen)
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    """UPDATE regional_recovery_attempts_v1 SET owner_id='forged'
                       WHERE plan_id=$1 AND attempt_ordinal=1""",
                    recovery.plan.plan_id,
                )
        await self._replay_current(recovery.plan.plan_id, "replacement")
        await self.regional.activate_region(recovery.plan.plan_id)
        self.assertTrue(
            all(
                generation.status == "ACTIVE"
                for generation in await self.regional.generations(recovery.plan.plan_id)
            )
        )

    async def test_real_process_loss_replays_under_fresh_takeover_attempts(self):
        recovery = await self._start("process-loss-region")
        async with self.store.pool.acquire() as conn:
            output_count = await conn.fetchval(
                """SELECT count(*) FROM records
                   WHERE channel='dag' AND partition_id IN('b','j','d')"""
            )
        owner = "regional-worker"
        cases = (
            ("B", "regional_replay_turn_before_commit"),
            ("J", "regional_replay_turn_after_commit"),
            ("D", "regional_worker_ready_before_commit"),
            ("D", "regional_worker_ready_after_commit"),
        )
        for index, (participant, point) in enumerate(cases, start=2):
            generations = {
                item.participant_id: item
                for item in await self.regional.generations(recovery.plan.plan_id)
            }
            result = await asyncio.to_thread(
                subprocess.run,
                [
                    sys.executable,
                    str(HERE / "postgres_regional_recovery_crash_worker.py"),
                    DSN,
                    self.schema,
                    self.revision.revision_id,
                    participant,
                    recovery.plan.plan_id,
                    owner,
                    str(generations[participant].recovery_owner_epoch),
                    point,
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                result.returncode, 86, point + result.stdout + result.stderr
            )
            await self._fresh_runtime_handles()
            async with self.store.pool.acquire() as conn:
                await conn.execute(
                    """UPDATE join_workers
                       SET lease_expires_at=clock_timestamp()-interval '1 second'
                       WHERE revision_id='dag-r1' AND participant_id IN('B','J','D')"""
                )
            owner = f"replacement-{index}"
            attempt = await self.regional.takeover_recovery(
                recovery.plan.plan_id, owner
            )
            self.assertEqual(attempt.attempt_ordinal, index)
        await self._replay_current(recovery.plan.plan_id, owner)
        await self.regional.activate_region(recovery.plan.plan_id)
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    """SELECT count(*) FROM records
                       WHERE channel='dag' AND partition_id IN('b','j','d')"""
                ),
                output_count,
            )
            self.assertEqual(
                await conn.fetchval(
                    """SELECT count(*) FROM regional_recovery_attempts_v1
                       WHERE plan_id=$1""",
                    recovery.plan.plan_id,
                ),
                5,
            )

    async def test_topology_is_derived_from_real_revision_edges_and_immutable(self):
        async with self.store.pool.acquire() as conn:
            real = await conn.fetchval(
                "SELECT count(*) FROM revision_edges WHERE revision_id='dag-r1'"
            )
            bound = await conn.fetchval(
                "SELECT count(*) FROM regional_topology_bindings_v1 WHERE revision_id='dag-r1'"
            )
            replay_root = await conn.fetchval(
                "SELECT retention_ref_id FROM region_edge_cuts_v1 LIMIT 1"
            )
            self.assertEqual(bound, real)
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "UPDATE regional_topology_bindings_v1 SET target_id='C' WHERE target_id='B'"
                )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "DELETE FROM regional_topology_bindings_v1 WHERE target_id='B'"
                )
        with self.assertRaises(asyncpg.PostgresError):
            await self.store.release_checkpoint(replay_root)


if __name__ == "__main__":
    unittest.main()
