from __future__ import annotations

import asyncio
import json
import sys
import unittest
from pathlib import Path

import asyncpg
import test_postgres_join_retention as join_fixture
from postgres_checkpoint_retention import (
    CheckpointHorizonExpired,
    PostgresCheckpointRetention,
    RegionalArchiveRequired,
)
from postgres_checkpoint_retention_oracle import evaluate
from postgres_join_retention import PostgresJoinRetention
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_stream_v2 import PostgresProtocolError


class PostgresCheckpointRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = join_fixture.PostgresJoinRetentionTests("runTest")
        await self.fixture.asyncSetUp()
        self.join_retention = await PostgresJoinRetention.install(
            self.fixture.coordinator
        )
        self.retention = await PostgresCheckpointRetention.install(
            self.fixture.coordinator
        )

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def test_old_checkpoint_and_log_prefix_roll_into_bounded_heads(self):
        await self.fixture.store.create_partition(
            "other", "p0", max_records=10, max_bytes=100_000
        )
        await self.fixture.store.create_partition(
            "other-out", "p0", max_records=10, max_bytes=100_000
        )
        await self.fixture.coordinator.register_revision(
            revision_id="r2",
            deployment_id="d2",
            graph_version="v1",
            sources=[("other", "p0")],
            participants=[
                {
                    "participant_id": "other-task",
                    "inputs": [{"channel": "other", "partition": "p0"}],
                    "outputs": [{"channel": "other-out", "partition": "p0"}],
                    "code_digest": "other-task-v1",
                    "state_schema": "other/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="other-code",
            state_contract_digest="other-state",
        )
        await self.fixture.store.append(
            "other-attempt",
            "other",
            "p0",
            "other-record",
            "other-object",
            b"other",
            "bytes/v1",
            "other-lineage",
        )
        first = await self.fixture._cycle(1)
        await self.join_retention.retire_latest("r1", "join", "join-retire-1")
        second = await self.fixture._cycle(2)
        await self.join_retention.retire_latest("r1", "join", "join-retire-2")

        def fail_after_delete(point):
            if point == "checkpoint_metadata_deleted":
                raise RuntimeError("checkpoint retirement crash control")

        with self.assertRaisesRegex(RuntimeError, "crash control"):
            await self.retention.retire_prefix(
                *self.fixture._coordinator_args(),
                "checkpoint-retire-rollback",
                fault=fail_after_delete,
            )
        async with self.fixture.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM checkpoint_intents_v2 WHERE revision_id='r1'"
                ),
                2,
            )
            self.assertFalse(
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM checkpoint_archive_heads_v1)"
                )
            )
        outcome = await self.retention.retire_prefix(
            *self.fixture._coordinator_args(), "checkpoint-retire-1"
        )
        self.assertEqual(outcome.retired_checkpoint_through, 1)
        self.assertGreater(outcome.retired_log_entries, 0)
        retry = await self.retention.retire_prefix(
            *self.fixture._coordinator_args(), "checkpoint-retire-1"
        )
        self.assertTrue(retry.idempotent_retry)
        with self.assertRaises(CheckpointHorizonExpired):
            await self.retention.assert_available("r1", first.checkpoint_id)
        await self.retention.assert_available("r1", second.checkpoint_id)
        await self.fixture._assert_oracles()
        self.assertEqual(
            (await evaluate(join_fixture.DSN, self.fixture.schema, "r1")).violations,
            (),
        )
        async with self.fixture.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM checkpoint_intents_v2 WHERE revision_id='r1'"
                ),
                1,
            )
            head = await conn.fetchrow(
                "SELECT * FROM checkpoint_archive_heads_v1 WHERE revision_id='r1'"
            )
            self.assertEqual(head["retired_checkpoints"], 1)
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "UPDATE checkpoint_archive_heads_v1 SET rolling_digest=repeat('f',64)"
                )
            with self.assertRaises(asyncpg.PostgresError):
                await conn.execute(
                    "DELETE FROM partition_log_entries WHERE log_sequence=(SELECT min(log_sequence) FROM partition_log_entries)"
                )
            log_heads = await conn.fetch("SELECT * FROM partition_log_heads")
            for log_head in log_heads:
                live = await conn.fetchval(
                    """SELECT count(*) FROM partition_log_entries
                       WHERE channel=$1 AND partition_id=$2""",
                    log_head["channel"],
                    log_head["partition_id"],
                )
                self.assertEqual(log_head["used_log_entries"], live)
            self.assertEqual(
                await conn.fetchval(
                    """SELECT used_log_entries FROM partition_log_heads
                       WHERE channel='other' AND partition_id='p0'"""
                ),
                1,
            )
            await conn.execute(
                "DROP TRIGGER partition_log_head_guard ON partition_log_heads"
            )
            await conn.execute(
                """UPDATE partition_log_heads SET used_log_entries=used_log_entries+1
                   WHERE (channel,partition_id)=(SELECT channel,partition_id
                     FROM partition_log_heads ORDER BY channel,partition_id LIMIT 1)"""
            )
        report = await evaluate(join_fixture.DSN, self.fixture.schema, "r1")
        self.assertTrue(
            any(item.startswith("ordered_log_credit:") for item in report.violations)
        )

    async def test_one_hundred_cycles_keep_coordinator_and_log_hot_rows_bounded(self):
        for number in range(1, 101):
            await self.fixture._cycle(number)
            await self.join_retention.retire_latest(
                "r1", "join", f"join-retire-{number}"
            )
            if number > 1:
                await self.retention.retire_prefix(
                    *self.fixture._coordinator_args(),
                    f"checkpoint-retire-{number - 1}",
                )
            async with self.fixture.store.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM checkpoint_intents_v2 WHERE revision_id='r1'"
                    ),
                    1,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM checkpoint_retirement_operations_v1"
                    ),
                    0 if number == 1 else 1,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM checkpoint_retirement_rows_v1"
                    ),
                    0,
                )
                self.assertLessEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM join_retirement_manifests_v1"
                    ),
                    1,
                )
                self.assertLessEqual(
                    await conn.fetchval(
                        "SELECT max(used_log_entries) FROM partition_log_heads"
                    ),
                    4,
                )
        async with self.fixture.store.pool.acquire() as conn:
            head = await conn.fetchrow(
                "SELECT * FROM checkpoint_archive_heads_v1 WHERE revision_id='r1'"
            )
            self.assertEqual(head["retired_checkpoint_through"], 99)
            self.assertEqual(head["retired_checkpoints"], 99)
        plan = await self.fixture.coordinator.begin_recovery(
            *self.fixture._coordinator_args(), "latest-after-checkpoint-archive"
        )
        restored = await self.fixture.runtime.restore(
            plan.recovery_id,
            join_fixture.retain_document,
            "worker",
            self.fixture.worker_epoch,
        )
        self.assertEqual(len(restored["documents"]), 100)
        await self.fixture._assert_oracles()
        self.assertEqual(
            (await evaluate(join_fixture.DSN, self.fixture.schema, "r1")).violations,
            (),
        )

    async def test_active_global_recovery_pins_its_old_checkpoint(self):
        await self.fixture._cycle(1)
        await self.join_retention.retire_latest("r1", "join", "join-retire-1")
        await self.fixture.coordinator.begin_recovery(
            *self.fixture._coordinator_args(), "old-active-recovery"
        )
        await self.fixture._cycle(2)
        with self.assertRaisesRegex(PostgresProtocolError, "no safe prefix"):
            await self.retention.retire_prefix(
                *self.fixture._coordinator_args(), "must-remain-pinned"
            )

    async def test_raw_guc_operation_and_head_cannot_forge_retirement(self):
        await self.fixture._cycle(1)
        await self.fixture._cycle(2)
        plan = {
            "checkpoint_ids": [self.fixture.checkpoints[0]],
            "retired_checkpoint_from": 1,
            "retired_checkpoint_through": 1,
            "latest_retained_checkpoint_id": self.fixture.checkpoints[1],
            "latest_retained_checkpoint_ordinal": 2,
            "log_cuts": {},
            "prior_log_cuts": {},
            "log_counts": {},
            "authority_cut": 0,
        }
        async with self.fixture.store.pool.acquire() as conn:
            with self.assertRaises(asyncpg.PostgresError):
                async with conn.transaction():
                    await conn.execute(
                        "SELECT set_config('ariadne.checkpoint_retirement_operation','forged',true)"
                    )
                    await conn.execute(
                        """INSERT INTO checkpoint_retirement_operations_v1
                           VALUES('r1','forged','forged',$1::jsonb,NULL,'PLANNED',
                                  clock_timestamp(),NULL)""",
                        json.dumps(plan),
                    )
                    await conn.execute(
                        """INSERT INTO checkpoint_archive_heads_v1 VALUES(
                           'r1',1,0,1,0,0,repeat('0',64),repeat('a',64),
                           'forged',clock_timestamp())"""
                    )
                    await conn.execute(
                        """UPDATE checkpoint_retirement_operations_v1
                           SET status='COMPLETED',outcome_json=jsonb_build_object(
                             'rolling_digest',repeat('a',64)) WHERE revision_id='r1'"""
                    )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM checkpoint_intents_v2 WHERE revision_id='r1'"
                ),
                2,
            )

    async def test_storage_rejects_plan_that_deletes_latest_output_barrier(self):
        await self.fixture._cycle(1)
        await self.join_retention.retire_latest("r1", "join", "join-retire-1")
        await self.fixture._cycle(2)
        await self.join_retention.retire_latest("r1", "join", "join-retire-2")
        safe_selector = self.retention._select_log_prefix

        async def forged_selector(conn, revision_id, retiring):
            rows, cuts = await safe_selector(conn, revision_id, retiring)
            output_rows = list(
                await conn.fetch(
                    """SELECT * FROM partition_log_entries
                       WHERE channel='output' AND partition_id='p0'
                       ORDER BY log_sequence"""
                )
            )
            rows["output/p0"] = output_rows
            cuts["output/p0"] = output_rows[-1]["log_sequence"]
            return rows, cuts

        self.retention._select_log_prefix = forged_selector
        try:
            with self.assertRaises(asyncpg.PostgresError):
                await self.retention.retire_prefix(
                    *self.fixture._coordinator_args(), "forged-output-cut"
                )
        finally:
            self.retention._select_log_prefix = safe_selector
        async with self.fixture.store.pool.acquire() as conn:
            latest = self.fixture.checkpoints[-1]
            self.assertTrue(
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM partition_log_entries
                       WHERE channel='output' AND checkpoint_id=$1
                         AND barrier_role='OUTPUT')""",
                    latest,
                )
            )

    async def test_real_process_loss_rolls_back_every_retirement_phase(self):
        await self.fixture._cycle(1)
        await self.join_retention.retire_latest("r1", "join", "join-retire-1")
        await self.fixture._cycle(2)
        await self.join_retention.retire_latest("r1", "join", "join-retire-2")
        worker = Path(__file__).with_name(
            "postgres_checkpoint_retention_crash_worker.py"
        )
        points = (
            "checkpoint_archive_planned",
            "checkpoint_metadata_deleted",
            "ordered_log_prefix_deleted",
            "checkpoint_archive_before_commit",
        )
        for index, point in enumerate(points):
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(worker),
                "--dsn",
                join_fixture.DSN,
                "--schema",
                self.fixture.schema,
                "--revision",
                "r1",
                "--operation",
                f"crash-{index}",
                "--point",
                point,
            )
            self.assertEqual(await process.wait(), 86)
            async with self.fixture.store.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM checkpoint_intents_v2 WHERE revision_id='r1'"
                    ),
                    2,
                )
                self.assertFalse(
                    await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM checkpoint_archive_heads_v1)"
                    )
                )
        await self.retention.retire_prefix(
            *self.fixture._coordinator_args(), "converged-after-process-loss"
        )
        self.assertEqual(
            (await evaluate(join_fixture.DSN, self.fixture.schema, "r1")).violations,
            (),
        )

    async def test_historical_regional_authority_is_explicitly_deferred_to_r3r3(self):
        first = await self.fixture._cycle(1)
        await self.join_retention.retire_latest("r1", "join", "join-retire-1")
        planner = await PostgresRecoveryRegionPlanner.install(self.fixture.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.fixture.coordinator, planner, self.fixture.revision
        )
        await regional.bind_revision_topology()
        await regional.bind_checkpoint_cuts(first.checkpoint_id)
        recovery = await regional.start_recovery(
            "coordinator",
            self.fixture.coordinator_epoch,
            "region-history",
            "join",
            first.checkpoint_id,
            "worker",
        )
        generation = recovery.workers[0]
        await regional.replay_worker(
            recovery.plan.plan_id,
            self.fixture.runtime,
            join_fixture.retain_document,
            "worker",
            generation.recovery_owner_epoch,
        )
        await regional.activate_region(recovery.plan.plan_id)
        self.fixture.worker_epoch = generation.recovery_owner_epoch
        await self.fixture._cycle(2)
        await self.join_retention.retire_latest("r1", "join", "join-retire-2")
        with self.assertRaises(RegionalArchiveRequired):
            await self.retention.retire_prefix(
                *self.fixture._coordinator_args(), "regional-history-refused"
            )


if __name__ == "__main__":
    unittest.main()
