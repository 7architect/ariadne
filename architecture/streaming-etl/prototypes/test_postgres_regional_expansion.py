from __future__ import annotations

import unittest

import test_postgres_regional_recovery as fixture
from postgres_barrier_coordinator import decoded
from postgres_join_oracle import evaluate as evaluate_join
from postgres_recovery_regions import UnrecoverableBoundary
from postgres_stream_oracle import evaluate as evaluate_stream


class PostgresRegionalExpansionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.base = fixture.PostgresRegionalRecoveryTests("runTest")

    async def asyncTearDown(self):
        if hasattr(self.base, "store"):
            await self.base.asyncTearDown()

    async def _oracles(self):
        await self.base._coordinator_oracle()
        for oracle in (evaluate_join, evaluate_stream):
            report = await oracle(fixture.DSN, self.base.schema)
            self.assertTrue(report["passed"], report)

    async def test_unavailable_sibling_boundary_expands_and_executes_region(self):
        self.base.nonreplayable_inputs = (("c", "J"),)
        await self.base.asyncSetUp()
        before = await self.base._worker_rows()
        async with self.base.store.pool.acquire() as conn:
            output_count = await conn.fetchval("SELECT count(*) FROM records")
        recovery = await self.base._start("expanded-c-boundary")
        self.assertEqual(set(recovery.plan.region), {"B", "C", "J", "D"})
        self.assertIn("unavailable", recovery.plan.reasons["C"])
        self.assertEqual(recovery.plan.source_rewinds, ())
        self.assertEqual(
            {worker.participant_id for worker in recovery.workers},
            {"B", "C", "J", "D"},
        )
        await self.base._fresh_runtime_handles()
        for generation in recovery.workers:
            await self.base.regional.replay_worker(
                recovery.plan.plan_id,
                self.base.runtimes[generation.participant_id],
                fixture.pass_document,
                "regional-worker",
                generation.recovery_owner_epoch,
            )
        await self.base.regional.activate_region(recovery.plan.plan_id)
        after = await self.base._worker_rows()
        self.assertEqual(dict(after["A"]), dict(before["A"]))
        for participant in ("B", "C", "J", "D"):
            self.assertEqual(
                decoded(after[participant]["state"]),
                decoded(before[participant]["state"]),
            )
            self.assertEqual(after[participant]["phase"], "LIVE")
            self.base.epochs[participant] = after[participant]["owner_epoch"]
        async with self.base.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM records"), output_count
            )
        await self.base._append_source(5)
        for participant, partition in (
            ("A", "source"),
            ("B", "a"),
            ("C", "a"),
            ("J", "b"),
            ("J", "c"),
            ("D", "j"),
            ("D", "j"),
        ):
            self.assertEqual(
                await self.base.runtimes[participant].step(
                    "dag/" + partition,
                    fixture.pass_document,
                    after[participant]["owner_id"],
                    after[participant]["owner_epoch"],
                    coordinator_owner="coordinator",
                    coordinator_epoch=self.base.coordinator_epoch,
                ),
                "APPLIED",
            )
        live = await self.base._worker_rows()
        self.assertEqual(decoded(live["A"]["state"])["documents"][-1], "document-5")
        for participant in ("B", "C"):
            self.assertEqual(
                decoded(live[participant]["state"])["documents"][-1], "document-5"
            )
        for participant in ("J", "D"):
            self.assertEqual(
                decoded(live[participant]["state"])["documents"][-2:],
                ["document-5", "document-5"],
            )
        await self._oracles()

    async def test_recursive_expansion_to_nonrewindable_source_has_no_fence(self):
        self.base.nonreplayable_inputs = (("a", "B"), ("source", "A"))
        await self.base.asyncSetUp()
        before = await self.base._worker_rows()
        async with self.base.store.pool.acquire() as conn:
            ordinal = await conn.fetchval(
                "SELECT next_authority_ordinal FROM deployment_revisions WHERE revision_id='dag-r1'"
            )
        with self.assertRaisesRegex(UnrecoverableBoundary, "SOURCE.*no rewind"):
            await self.base._start("unsupported-source-rewind")
        after = await self.base._worker_rows()
        self.assertEqual(
            {key: dict(row) for key, row in after.items()},
            {key: dict(row) for key, row in before.items()},
        )
        async with self.base.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT next_authority_ordinal FROM deployment_revisions WHERE revision_id='dag-r1'"
                ),
                ordinal,
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM region_recovery_plans_v1"), 0
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM regional_recovery_workers_v1"
                ),
                0,
            )
        await self._oracles()

    async def test_corrupt_internal_replay_output_refuses_before_region_fence(self):
        self.base.nonreplayable_inputs = (("c", "J"),)
        await self.base.asyncSetUp()
        before = await self.base._worker_rows()
        async with self.base.store.pool.acquire() as conn:
            ordinal = await conn.fetchval(
                """SELECT next_authority_ordinal FROM deployment_revisions
                   WHERE revision_id='dag-r1'"""
            )
            await conn.execute(
                """UPDATE records SET payload_digest=repeat('0',64)
                   WHERE channel='dag' AND partition_id='b'
                     AND sequence=(SELECT max(sequence) FROM records
                       WHERE channel='dag' AND partition_id='b')"""
            )
        with self.assertRaisesRegex(
            UnrecoverableBoundary,
            "participant B replay output is corrupt outside retained durable horizon",
        ):
            await self.base._start("corrupt-internal-output")
        after = await self.base._worker_rows()
        self.assertEqual(
            {key: dict(row) for key, row in after.items()},
            {key: dict(row) for key, row in before.items()},
        )
        async with self.base.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    """SELECT next_authority_ordinal FROM deployment_revisions
                       WHERE revision_id='dag-r1'"""
                ),
                ordinal,
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM region_recovery_plans_v1"),
                0,
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM regional_recovery_workers_v1"
                ),
                0,
            )
