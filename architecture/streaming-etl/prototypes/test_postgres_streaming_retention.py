from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
import unittest
from pathlib import Path

import asyncpg
import test_postgres_checkpoint_retention as checkpoint_fixture
import test_postgres_join_retention as join_fixture
import test_postgres_regional_recovery as regional_fixture
from postgres_barrier_coordinator import (
    CheckpointControlPressure,
    PostgresBarrierCoordinator,
    decoded,
    digest,
)
from postgres_checkpoint_retention_oracle import evaluate as evaluate_checkpoint
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_regional_retention import PostgresRegionalRetention
from postgres_storage_footprint import evaluate as evaluate_footprint
from postgres_stream_v2 import (
    IdempotencyHorizonExpired,
    PostgresStream,
    content_object_key,
)
from postgres_streaming_retention import PostgresStreamingRetention

HERE = Path(__file__).resolve().parent


@stream_task
def aggregate_document(ctx, payload):
    prior = ctx.managed_state
    rolling = hashlib.sha256(
        bytes.fromhex(prior.get("rolling_digest", "0" * 64)) + payload
    ).hexdigest()
    yield OutputSpec(
        payload,
        "bytes/v1",
        {"count": prior.get("count", 0) + 1, "rolling_digest": rolling},
    )


class PostgresStreamingRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.base = checkpoint_fixture.PostgresCheckpointRetentionTests("runTest")
        await self.base.asyncSetUp()
        await self.base.fixture.store.register_producer(
            "input", "p0", "source-producer:r1"
        )
        self.retention = await PostgresStreamingRetention.install(
            self.base.fixture.store, self.base.fixture.coordinator
        )
        self.worker_owner = "worker"

    async def asyncTearDown(self):
        await self.base.asyncTearDown()

    async def _cycle(self, number: int):
        payload = f"document-{number}".encode()
        await self.base.fixture.store.append(
            f"source-attempt-{number}",
            "input",
            "p0",
            f"source-record-{number}",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            f"source-lineage-{number}",
            producer_id="source-producer:r1",
            producer_sequence=number,
        )
        self.assertEqual(
            await self.base.fixture.runtime.step(
                "input/p0",
                aggregate_document,
                self.worker_owner,
                self.base.fixture.worker_epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.base.fixture.coordinator_epoch,
            ),
            "APPLIED",
        )
        checkpoint = await self.base.fixture.coordinator.start_checkpoint(
            *self.base.fixture._coordinator_args(), f"checkpoint-{number}"
        )
        await self.base.fixture.coordinator.inject_source_barrier(
            *self.base.fixture._coordinator_args(),
            checkpoint.checkpoint_id,
            "input",
            "p0",
        )
        self.assertEqual(
            await self.base.fixture.runtime.step(
                "input/p0",
                aggregate_document,
                self.worker_owner,
                self.base.fixture.worker_epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.base.fixture.coordinator_epoch,
            ),
            "ALIGNED",
        )
        await self.base.fixture.coordinator.complete_checkpoint(
            *self.base.fixture._coordinator_args(), checkpoint.checkpoint_id
        )
        self.base.fixture.checkpoints.append(checkpoint.checkpoint_id)
        return checkpoint

    async def test_one_hundred_cycles_bound_data_objects_and_idempotency_metadata(self):
        planner = await PostgresRecoveryRegionPlanner.install(
            self.base.fixture.coordinator
        )
        regional = await PostgresRegionalRecovery.install(
            self.base.fixture.coordinator, planner, self.base.fixture.revision
        )
        await regional.bind_revision_topology()
        regional_retention = await PostgresRegionalRetention.install(
            self.base.fixture.coordinator
        )
        prior_region = None
        last_regional_state = None
        footprint_samples = []
        for number in range(1, 101):
            await self._cycle(number)
            await self.base.join_retention.retire_latest(
                "r1", "join", f"join-retire-{number}"
            )
            if prior_region is not None:
                await regional_retention.retire_historical(
                    *self.base.fixture._coordinator_args(),
                    f"region-retire-{number - 1}",
                )
            if number == 1:
                outcome = None
            else:
                await self.base.retention.retire_prefix(
                    *self.base.fixture._coordinator_args(),
                    f"checkpoint-retire-{number - 1}",
                )
                outcome = await self.retention.compact_latest(
                    *self.base.fixture._coordinator_args(), f"stream-retire-{number}"
                )
                self.assertEqual(set(outcome.cuts), {"input/p0", "output/p0"})

            checkpoint_id = self.base.fixture.checkpoints[-1]
            await regional.bind_checkpoint_cuts(checkpoint_id)
            recovery = await regional.start_recovery(
                "coordinator",
                self.base.fixture.coordinator_epoch,
                f"stream-region-{number}",
                "join",
                checkpoint_id,
                "regional-worker",
            )
            generation = recovery.workers[0]
            last_regional_state = await regional.replay_worker(
                recovery.plan.plan_id,
                self.base.fixture.runtime,
                aggregate_document,
                "regional-worker",
                generation.recovery_owner_epoch,
            )
            await regional.activate_region(recovery.plan.plan_id)
            self.worker_owner = "regional-worker"
            self.base.fixture.worker_epoch = generation.recovery_owner_epoch
            prior_region = recovery.plan.plan_id

            if number == 1:
                continue
            async with self.base.fixture.store.pool.acquire() as conn:
                pressure = await conn.fetch(
                    """SELECT channel,used_records,used_bytes,used_metadata_records
                       FROM partitions WHERE channel IN('input','output')
                       ORDER BY channel"""
                )
                self.assertTrue(
                    all(
                        (
                            row["used_records"],
                            row["used_bytes"],
                            row["used_metadata_records"],
                        )
                        == (0, 0, 0)
                        for row in pressure
                    )
                )
                self.assertEqual(await conn.fetchval("SELECT count(*) FROM records"), 0)
                self.assertEqual(await conn.fetchval("SELECT count(*) FROM objects"), 0)
                self.assertEqual(
                    await conn.fetchval("SELECT count(*) FROM compaction_tombstones"),
                    0,
                )
                self.assertLessEqual(
                    await conn.fetchval("SELECT count(*) FROM metadata_operations"),
                    1,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM streaming_retention_operations_v1"
                    ),
                    1,
                )
            if number in (25, 50, 75, 100):
                footprint = await evaluate_footprint(
                    join_fixture.DSN, self.base.fixture.schema
                )
                sample = {
                    "cycle": number,
                    "total_rows": footprint.total_rows,
                    "total_json_bytes": footprint.total_json_bytes,
                    "table_rows": {
                        table.table: table.rows for table in footprint.tables
                    },
                }
                footprint_samples.append(sample)
                self.assertLessEqual(footprint.total_rows, 512)
                self.assertLessEqual(footprint.total_json_bytes, 256 * 1024)
        self.assertEqual(
            [sample["table_rows"] for sample in footprint_samples],
            [footprint_samples[0]["table_rows"]] * 4,
            "logical table row census grew after the 25-cycle warmup",
        )
        print(
            "R3B_MEASUREMENT_JSON="
            + json.dumps(
                {
                    "name": "streaming_retention_100_cycles",
                    "samples": footprint_samples,
                    "logical_row_budget": 512,
                    "logical_byte_budget": 256 * 1024,
                    "scope": "logical row/JSON bytes; excludes heap/index/WAL",
                },
                sort_keys=True,
            )
        )
        with self.assertRaises(IdempotencyHorizonExpired):
            payload = b"document-1"
            await self.base.fixture.store.append(
                "retry-retired-source",
                "input",
                "p0",
                "source-record-1",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                "source-lineage-1",
                producer_id="source-producer:r1",
                producer_sequence=1,
            )
        self.assertEqual(last_regional_state["count"], 100)
        self.assertEqual(len(last_regional_state["rolling_digest"]), 64)
        await self.base.fixture._assert_oracles()
        self.assertEqual(
            (
                await evaluate_checkpoint(
                    join_fixture.DSN, self.base.fixture.schema, "r1"
                )
            ).violations,
            (),
        )

    async def test_each_composition_boundary_reopens_and_converges_exactly(self):
        await self._cycle(1)
        await self.base.join_retention.retire_latest("r1", "join", "join-retire-1")
        for number, point in enumerate(
            (
                "stream_gc_planned",
                "stream_gc_data_compacted",
                "stream_gc_metadata_compacted",
                "stream_gc_completed",
            ),
            start=2,
        ):
            await self._cycle(number)
            await self.base.join_retention.retire_latest(
                "r1", "join", f"join-retire-{number}"
            )
            await self.base.retention.retire_prefix(
                *self.base.fixture._coordinator_args(),
                f"checkpoint-retire-{number - 1}",
            )

            operation_id = f"stream-retire-{number}"
            if point == "stream_gc_planned":

                def fail(actual):
                    if actual == "stream_gc_planned":
                        raise RuntimeError(actual)

                with self.assertRaisesRegex(RuntimeError, point):
                    await self.retention.compact_latest(
                        *self.base.fixture._coordinator_args(),
                        operation_id,
                        fault=fail,
                    )
            else:
                result = await asyncio.to_thread(
                    subprocess.run,
                    [
                        sys.executable,
                        str(HERE / "postgres_streaming_retention_crash_worker.py"),
                        join_fixture.DSN,
                        self.base.fixture.schema,
                        "r1",
                        "v1",
                        self.base.fixture.revision.graph_digest,
                        "coordinator",
                        str(self.base.fixture.coordinator_epoch),
                        operation_id,
                        point,
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 86, result.stdout + result.stderr)
                if point == "stream_gc_metadata_compacted":
                    async with self.base.fixture.store.pool.acquire() as conn:
                        with self.assertRaises(asyncpg.PostgresError):
                            async with conn.transaction():
                                await conn.execute(
                                    """SELECT set_config(
                                       'ariadne.streaming_retention_operation',$1,true)""",
                                    operation_id,
                                )
                                await conn.execute(
                                    """UPDATE streaming_retention_operations_v1
                                       SET status='COMPLETED',outcome_json='{}'::jsonb
                                       WHERE revision_id='r1'"""
                                )
            fresh_stream = await PostgresStream.connect_existing(
                join_fixture.DSN, self.base.fixture.schema
            )
            try:
                retry = await PostgresStreamingRetention(
                    fresh_stream, PostgresBarrierCoordinator(fresh_stream)
                ).compact_latest(*self.base.fixture._coordinator_args(), operation_id)
            finally:
                await fresh_stream.close()
            async with self.base.fixture.store.pool.acquire() as conn:
                operation = await conn.fetchrow(
                    "SELECT * FROM streaming_retention_operations_v1 WHERE revision_id='r1'"
                )
                self.assertEqual(operation["status"], "COMPLETED")
                self.assertEqual(
                    await conn.fetchval(
                        """SELECT count(*) FROM compaction_manifests
                           WHERE channel IN('input','output')"""
                    ),
                    0,
                )
                self.assertEqual(
                    await conn.fetchval(
                        """SELECT count(*) FROM metadata_operations
                           WHERE operation_id=$1""",
                        f"metadata:{operation['cuts_digest']}",
                    ),
                    1,
                )
            if point == "stream_gc_completed":
                self.assertTrue(retry.idempotent_retry)
            self.assertEqual(retry.cuts, {"input/p0": number, "output/p0": number})
        old = await self.retention.compact_latest(
            *self.base.fixture._coordinator_args(), "stream-retire-5"
        )
        await self._cycle(6)
        old_after_new_checkpoint = await self.retention.compact_latest(
            *self.base.fixture._coordinator_args(), "stream-retire-5"
        )
        self.assertEqual(old_after_new_checkpoint.checkpoint_id, old.checkpoint_id)
        self.assertTrue(old_after_new_checkpoint.idempotent_retry)
        async with self.base.fixture.store.pool.acquire() as conn:
            with self.assertRaises(asyncpg.PostgresError):
                async with conn.transaction():
                    await conn.execute(
                        "SELECT set_config('ariadne.streaming_retention_operation',$1,true)",
                        "stream-retire-5",
                    )
                    await conn.execute(
                        """UPDATE streaming_retention_operations_v1
                           SET cuts_json='{"input/p0":999,"output/p0":999}'::jsonb
                           WHERE revision_id='r1'"""
                    )
        await self.base.fixture._assert_oracles()
        self.assertEqual(
            (
                await evaluate_checkpoint(
                    join_fixture.DSN, self.base.fixture.schema, "r1"
                )
            ).violations,
            (),
        )

    async def test_incomplete_gc_freezes_checkpoint_control_but_not_data_turns(self):
        await self._cycle(1)
        await self.base.join_retention.retire_latest("r1", "join", "join-retire-1")
        await self._cycle(2)
        await self.base.join_retention.retire_latest("r1", "join", "join-retire-2")
        await self.base.retention.retire_prefix(
            *self.base.fixture._coordinator_args(), "checkpoint-retire-1"
        )

        def pause(point):
            if point == "stream_gc_data_compacted":
                raise RuntimeError(point)

        with self.assertRaisesRegex(RuntimeError, "stream_gc_data_compacted"):
            await self.retention.compact_latest(
                *self.base.fixture._coordinator_args(), "paused-stream-gc", fault=pause
            )

        payload = b"document-3"
        await self.base.fixture.store.append(
            "source-attempt-3",
            "input",
            "p0",
            "source-record-3",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            "source-lineage-3",
            producer_id="source-producer:r1",
            producer_sequence=3,
        )
        self.assertEqual(
            await self.base.fixture.runtime.step(
                "input/p0",
                aggregate_document,
                "worker",
                self.base.fixture.worker_epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.base.fixture.coordinator_epoch,
            ),
            "APPLIED",
        )
        with self.assertRaisesRegex(
            CheckpointControlPressure, "incomplete streaming retention"
        ):
            await self.base.fixture.coordinator.start_checkpoint(
                *self.base.fixture._coordinator_args(), "must-wait-for-stream-gc"
            )
        with self.assertRaisesRegex(
            CheckpointControlPressure, "incomplete streaming retention"
        ):
            await self.base.retention.retire_prefix(
                *self.base.fixture._coordinator_args(), "must-not-retire-frozen-cut"
            )

        resumed = await self.retention.compact_latest(
            *self.base.fixture._coordinator_args(), "paused-stream-gc"
        )
        self.assertEqual(resumed.cuts, {"input/p0": 2, "output/p0": 2})
        checkpoint = await self.base.fixture.coordinator.start_checkpoint(
            *self.base.fixture._coordinator_args(), "after-stream-gc"
        )
        await self.base.fixture.coordinator.inject_source_barrier(
            *self.base.fixture._coordinator_args(),
            checkpoint.checkpoint_id,
            "input",
            "p0",
        )
        self.assertEqual(
            await self.base.fixture.runtime.step(
                "input/p0",
                aggregate_document,
                "worker",
                self.base.fixture.worker_epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.base.fixture.coordinator_epoch,
            ),
            "ALIGNED",
        )
        await self.base.fixture.coordinator.complete_checkpoint(
            *self.base.fixture._coordinator_args(), checkpoint.checkpoint_id
        )


class RegionalCatchupCompositionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.base = regional_fixture.PostgresRegionalRecoveryTests("runTest")
        await self.base.asyncSetUp()

    async def asyncTearDown(self):
        await self.base.asyncTearDown()

    async def test_healthy_sibling_continues_and_region_catches_up_stable_document_five(
        self,
    ):
        recovery = await self.base._start("r4-live-catchup")
        await self.base._append_source(5)
        self.assertEqual(await self.base._step("A", "source"), "APPLIED")
        self.assertEqual(await self.base._step("C", "a"), "APPLIED")
        healthy = await self.base._worker_rows()
        healthy_c_epoch = healthy["C"]["owner_epoch"]

        await self.base._replay_all(recovery)
        await self.base.regional.activate_region(recovery.plan.plan_id)
        generations = {
            generation.participant_id: generation.recovery_owner_epoch
            for generation in await self.base.regional.generations(
                recovery.plan.plan_id
            )
        }
        for participant in ("B", "J", "D"):
            self.base.epochs[participant] = generations[participant]
        for participant, partition in (
            ("B", "a"),
            ("J", "b"),
            ("J", "c"),
            ("D", "j"),
            ("D", "j"),
        ):
            self.assertEqual(
                await self.base.runtimes[participant].step(
                    "dag/" + partition,
                    regional_fixture.pass_document,
                    "regional-worker",
                    generations[participant],
                    coordinator_owner="coordinator",
                    coordinator_epoch=self.base.coordinator_epoch,
                ),
                "APPLIED",
            )

        rows = await self.base._worker_rows()
        self.assertEqual(rows["C"]["owner_epoch"], healthy_c_epoch)
        self.assertEqual(len(decoded(rows["B"]["state"])["documents"]), 5)
        self.assertEqual(len(decoded(rows["C"]["state"])["documents"]), 5)
        self.assertEqual(len(decoded(rows["J"]["state"])["documents"]), 10)
        self.assertEqual(len(decoded(rows["D"]["state"])["documents"]), 10)

        expected_a = "join-output:" + digest(
            ["dag-r1", "A", "dag", "source", "document-5"]
        )
        expected_b = "join-output:" + digest(["dag-r1", "B", "dag", "a", expected_a])
        expected_c = "join-output:" + digest(["dag-r1", "C", "dag", "a", expected_a])
        expected_j = {
            "join-output:" + digest(["dag-r1", "J", "dag", "b", expected_b]),
            "join-output:" + digest(["dag-r1", "J", "dag", "c", expected_c]),
        }
        expected_d = {
            "join-output:" + digest(["dag-r1", "D", "dag", "j", record_id])
            for record_id in expected_j
        }
        async with self.base.store.pool.acquire() as conn:
            actual = {
                row["partition_id"]: set(row["record_ids"])
                for row in await conn.fetch(
                    """SELECT partition_id,array_agg(record_id ORDER BY sequence) record_ids
                       FROM records WHERE channel='dag' AND partition_id=ANY($1::text[])
                       GROUP BY partition_id""",
                    ["a", "b", "c", "j", "d"],
                )
            }
            self.assertIn(expected_a, actual["a"])
            self.assertIn(expected_b, actual["b"])
            self.assertIn(expected_c, actual["c"])
            self.assertTrue(expected_j <= actual["j"])
            self.assertTrue(expected_d <= actual["d"])
            pressure = await conn.fetch(
                """SELECT used_records,max_records,used_bytes,max_bytes
                   FROM partitions WHERE channel='dag'"""
            )
            self.assertTrue(
                all(
                    row["used_records"] <= row["max_records"]
                    and row["used_bytes"] <= row["max_bytes"]
                    for row in pressure
                )
            )
        await self.base._coordinator_oracle()


if __name__ == "__main__":
    unittest.main()
