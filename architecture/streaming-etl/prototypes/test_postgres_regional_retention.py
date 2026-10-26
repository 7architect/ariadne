from __future__ import annotations

import hashlib
import unittest

import asyncpg
import test_postgres_checkpoint_retention as checkpoint_fixture
import test_postgres_join_retention as join_fixture
from postgres_barrier_coordinator import canonical
from postgres_checkpoint_retention import RegionalArchiveRequired
from postgres_checkpoint_retention_oracle import evaluate
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_regional_retention import (
    PostgresRegionalRetention,
    RegionalRecoveryHorizonExpired,
)
from postgres_regional_retention_oracle import evaluate as evaluate_regional
from postgres_stream_v2 import PostgresProtocolError


class PostgresRegionalRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.base = checkpoint_fixture.PostgresCheckpointRetentionTests("runTest")
        await self.base.asyncSetUp()

    async def asyncTearDown(self):
        await self.base.asyncTearDown()

    async def _historical_region(self):
        first = await self.base.fixture._cycle(1)
        await self.base.join_retention.retire_latest("r1", "join", "join-retire-1")
        planner = await PostgresRecoveryRegionPlanner.install(
            self.base.fixture.coordinator
        )
        regional = await PostgresRegionalRecovery.install(
            self.base.fixture.coordinator, planner, self.base.fixture.revision
        )
        await regional.bind_revision_topology()
        await regional.bind_checkpoint_cuts(first.checkpoint_id)
        recovery = await regional.start_recovery(
            "coordinator",
            self.base.fixture.coordinator_epoch,
            "region-history",
            "join",
            first.checkpoint_id,
            "worker",
        )
        generation = recovery.workers[0]
        await regional.replay_worker(
            recovery.plan.plan_id,
            self.base.fixture.runtime,
            join_fixture.retain_document,
            "worker",
            generation.recovery_owner_epoch,
        )
        await regional.activate_region(recovery.plan.plan_id)
        self.base.fixture.worker_epoch = generation.recovery_owner_epoch
        await self.base.fixture._cycle(2)
        await self.base.join_retention.retire_latest("r1", "join", "join-retire-2")
        return recovery.plan.plan_id, planner, regional

    async def test_active_history_archives_then_checkpoint_prefix_can_retire(self):
        plan_id, planner, regional = await self._historical_region()
        with self.assertRaises(RegionalArchiveRequired):
            await self.base.retention.retire_prefix(
                *self.base.fixture._coordinator_args(), "blocked-before-r3r3"
            )
        regional_retention = await PostgresRegionalRetention.install(
            self.base.fixture.coordinator
        )
        async with self.base.fixture.store.pool.acquire() as conn:
            with self.assertRaises(asyncpg.PostgresError):
                async with conn.transaction():
                    await conn.execute(
                        "SELECT set_config('ariadne.regional_retirement_operation','forged-region',true)"
                    )
                    await conn.execute(
                        """INSERT INTO regional_retirement_operations_v1 VALUES(
                           'r1','forged-region','forged',
                           jsonb_build_object(
                             'plan_ids','[]'::jsonb,'checkpoint_ids','[]'::jsonb,
                             'root_ids','[]'::jsonb,'plan_digests','[]'::jsonb,
                             'prior_totals',jsonb_build_object(
                               'retired_plans',0,'retired_attempts',0,
                               'retired_generations',0,'retired_cuts',0)),
                           NULL,'PLANNED',NULL)""",
                    )
                    await conn.execute(
                        """INSERT INTO regional_archive_heads_v1(
                           revision_id,retired_plans,retired_attempts,
                           retired_generations,retired_cuts,last_plan_digest,
                           prior_digest,material_digest,rolling_digest,
                           operation_id) VALUES(
                           'r1',1,1,1,1,'forged',repeat('0',64),
                           repeat('b',64),repeat('a',64),'forged-region')"""
                    )
                    await conn.execute(
                        """UPDATE regional_retirement_operations_v1
                           SET status='COMPLETED',outcome_json=jsonb_build_object(
                             'rolling_digest',repeat('a',64)) WHERE revision_id='r1'"""
                    )
            latest_roots = await conn.fetch(
                """SELECT retention_ref_id FROM source_barrier_observations
                   WHERE checkpoint_id=$1 UNION ALL
                   SELECT retention_ref_id FROM participant_output_roots
                   WHERE checkpoint_id=$1""",
                self.base.fixture.checkpoints[-1],
            )
            for index, root in enumerate(latest_roots):
                with self.assertRaises(asyncpg.PostgresError):
                    async with conn.transaction():
                        operation = f"forged-root-{index}"
                        await conn.execute(
                            "SELECT set_config('ariadne.regional_retirement_operation',$1,true)",
                            operation,
                        )
                        await conn.execute(
                            """INSERT INTO regional_retirement_operations_v1 VALUES(
                               'r1',$1,'forged',jsonb_build_object(
                                 'plan_ids',jsonb_build_array($2::text),
                                 'checkpoint_ids','[]'::jsonb,
                                 'root_ids',jsonb_build_array($3::text)),
                               NULL,'PLANNED',NULL)""",
                            operation,
                            plan_id,
                            root["retention_ref_id"],
                        )
                        await conn.execute(
                            """INSERT INTO regional_retirement_rows_v1
                               SELECT $1,'checkpoints','forged',to_jsonb(c)
                               FROM checkpoints c WHERE checkpoint_id=$2""",
                            operation,
                            root["retention_ref_id"],
                        )
        outcome = await regional_retention.retire_historical(
            *self.base.fixture._coordinator_args(), "region-retire-1"
        )
        self.assertEqual(outcome.retired_plans, 1)
        self.assertGreaterEqual(outcome.retired_attempts, 1)
        retry = await regional_retention.retire_historical(
            *self.base.fixture._coordinator_args(), "region-retire-1"
        )
        self.assertTrue(retry.idempotent_retry)
        with self.assertRaises(RegionalRecoveryHorizonExpired):
            await regional_retention.assert_plan_available("r1", plan_id)
        async with self.base.fixture.store.pool.acquire() as conn:
            for table in (
                "region_recovery_plans_v1",
                "regional_recovery_workers_v1",
                "regional_recovery_attempts_v1",
                "regional_recovery_attempt_workers_v1",
            ):
                self.assertEqual(
                    await conn.fetchval(f"SELECT count(*) FROM {table}"), 0
                )
            self.assertGreater(
                await conn.fetchval("SELECT count(*) FROM region_graph_nodes_v1"), 0
            )
        latest_checkpoint = self.base.fixture.checkpoints[-1]
        await regional.bind_checkpoint_cuts(latest_checkpoint)
        replacement = await planner.create_plan(
            "r1",
            "v1",
            self.base.fixture.revision.graph_digest,
            "coordinator",
            self.base.fixture.coordinator_epoch,
            "region-history",
            "join",
            latest_checkpoint,
        )
        self.assertNotEqual(replacement.plan_id, plan_id)
        await self.base.retention.retire_prefix(
            *self.base.fixture._coordinator_args(), "checkpoint-after-r3r3"
        )
        await self.base.fixture._assert_oracles()
        new_recovery = await regional.start_recovery(
            "coordinator",
            self.base.fixture.coordinator_epoch,
            "region-history",
            "join",
            latest_checkpoint,
            "worker",
        )
        self.assertEqual(new_recovery.plan.plan_id, replacement.plan_id)
        self.assertEqual(
            (
                await evaluate(join_fixture.DSN, self.base.fixture.schema, "r1")
            ).violations,
            (),
        )
        self.assertEqual(
            (
                await evaluate_regional(
                    join_fixture.DSN, self.base.fixture.schema, "r1"
                )
            ).violations,
            (),
        )

    async def test_restoring_generation_is_never_archive_eligible(self):
        checkpoint = await self.base.fixture._cycle(1)
        planner = await PostgresRecoveryRegionPlanner.install(
            self.base.fixture.coordinator
        )
        regional = await PostgresRegionalRecovery.install(
            self.base.fixture.coordinator, planner, self.base.fixture.revision
        )
        await regional.bind_revision_topology()
        await regional.bind_checkpoint_cuts(checkpoint.checkpoint_id)
        await regional.start_recovery(
            "coordinator",
            self.base.fixture.coordinator_epoch,
            "active-region",
            "join",
            checkpoint.checkpoint_id,
            "worker",
        )
        retention = await PostgresRegionalRetention.install(
            self.base.fixture.coordinator
        )
        with self.assertRaisesRegex(PostgresProtocolError, "active regional"):
            await retention.retire_historical(
                *self.base.fixture._coordinator_args(), "must-not-retire"
            )
        self.assertEqual(
            (
                await evaluate_regional(
                    join_fixture.DSN, self.base.fixture.schema, "r1"
                )
            ).violations,
            (),
        )

    async def test_archive_head_rejects_bogus_prior_totals_with_exact_live_witnesses(
        self,
    ):
        plan_id, _, _ = await self._historical_region()
        retention = await PostgresRegionalRetention.install(
            self.base.fixture.coordinator
        )
        async with self.base.fixture.store.pool.acquire() as conn:
            plan = await conn.fetchrow(
                "SELECT * FROM region_recovery_plans_v1 WHERE plan_id=$1", plan_id
            )
            cut = await conn.fetchrow(
                """SELECT * FROM region_edge_cuts_v1
                   WHERE checkpoint_id=$1 ORDER BY edge_id LIMIT 1""",
                plan["checkpoint_id"],
            )
            operation_id = "forged-prior-total"
            identity = {
                "revision_id": "r1",
                "plan_ids": [plan_id],
                "checkpoint_ids": [plan["checkpoint_id"]],
                "root_ids": [cut["retention_ref_id"]],
                "latest_completed_ordinal": 2,
                "plan_digests": [plan["plan_digest"]],
                "prior_digest": "0" * 64,
                "prior_totals": {
                    "retired_plans": 1_000_000,
                    "retired_attempts": 0,
                    "retired_generations": 0,
                    "retired_cuts": 0,
                },
            }
            transaction = conn.transaction()
            await transaction.start()
            try:
                await conn.execute(
                    "SELECT set_config('ariadne.regional_retirement_operation',$1,true)",
                    operation_id,
                )
                await conn.execute(
                    """INSERT INTO regional_retirement_operations_v1 VALUES(
                       'r1',$1,$2,$3::jsonb,NULL,'PLANNED',NULL)""",
                    operation_id,
                    hashlib.sha256(canonical(identity).encode()).hexdigest(),
                    canonical(identity),
                )
                await retention._witness(
                    conn, operation_id, "region_recovery_plans_v1", [plan]
                )
                await retention._witness(
                    conn, operation_id, "region_edge_cuts_v1", [cut]
                )
                material = await conn.fetchval(
                    """SELECT encode(sha256(convert_to(jsonb_agg(
                         jsonb_build_object('table',table_name,'key',row_key,'row',row_json)
                         ORDER BY table_name,row_key)::text,'UTF8')),'hex')
                       FROM regional_retirement_rows_v1 WHERE operation_id=$1""",
                    operation_id,
                )
                rolling = hashlib.sha256((("0" * 64) + material).encode()).hexdigest()
                with self.assertRaises(asyncpg.PostgresError):
                    await conn.execute(
                        """INSERT INTO regional_archive_heads_v1(
                           revision_id,retired_plans,retired_attempts,
                           retired_generations,retired_cuts,last_plan_digest,
                           prior_digest,material_digest,rolling_digest,operation_id)
                           VALUES('r1',1000001,0,0,1,$1,repeat('0',64),$2,$3,$4)""",
                        plan["plan_digest"],
                        material,
                        rolling,
                        operation_id,
                    )
            finally:
                await transaction.rollback()

    async def test_ten_regional_generations_keep_history_tables_bounded(self):
        await self.base.fixture._cycle(1)
        await self.base.join_retention.retire_latest("r1", "join", "join-retire-1")
        planner = await PostgresRecoveryRegionPlanner.install(
            self.base.fixture.coordinator
        )
        regional = await PostgresRegionalRecovery.install(
            self.base.fixture.coordinator, planner, self.base.fixture.revision
        )
        await regional.bind_revision_topology()
        retirement = await PostgresRegionalRetention.install(
            self.base.fixture.coordinator
        )
        for number in range(1, 11):
            checkpoint_id = self.base.fixture.checkpoints[-1]
            await regional.bind_checkpoint_cuts(checkpoint_id)
            recovery = await regional.start_recovery(
                "coordinator",
                self.base.fixture.coordinator_epoch,
                f"regional-cycle-{number}",
                "join",
                checkpoint_id,
                "worker",
            )
            generation = recovery.workers[0]
            await regional.replay_worker(
                recovery.plan.plan_id,
                self.base.fixture.runtime,
                join_fixture.retain_document,
                "worker",
                generation.recovery_owner_epoch,
            )
            await regional.activate_region(recovery.plan.plan_id)
            self.base.fixture.worker_epoch = generation.recovery_owner_epoch
            await self.base.fixture._cycle(number + 1)
            await self.base.join_retention.retire_latest(
                "r1", "join", f"join-retire-{number + 1}"
            )
            await retirement.retire_historical(
                *self.base.fixture._coordinator_args(), f"region-retire-{number}"
            )
            await self.base.retention.retire_prefix(
                *self.base.fixture._coordinator_args(), f"checkpoint-retire-{number}"
            )
            async with self.base.fixture.store.pool.acquire() as conn:
                for table in (
                    "region_recovery_plans_v1",
                    "regional_recovery_workers_v1",
                    "regional_recovery_attempts_v1",
                    "regional_recovery_attempt_workers_v1",
                    "regional_retirement_rows_v1",
                ):
                    self.assertEqual(
                        await conn.fetchval(f"SELECT count(*) FROM {table}"), 0
                    )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM regional_retirement_operations_v1"
                    ),
                    1,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM regional_archive_heads_v1"
                    ),
                    1,
                )
        async with self.base.fixture.store.pool.acquire() as conn:
            head = await conn.fetchrow(
                "SELECT * FROM regional_archive_heads_v1 WHERE revision_id='r1'"
            )
            self.assertEqual(head["retired_plans"], 10)
            self.assertEqual(head["retired_attempts"], 10)
        self.assertEqual(
            (
                await evaluate_regional(
                    join_fixture.DSN, self.base.fixture.schema, "r1"
                )
            ).violations,
            (),
        )


if __name__ == "__main__":
    unittest.main()
