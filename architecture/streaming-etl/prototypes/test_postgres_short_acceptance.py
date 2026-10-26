"""Measured short acceptance of the real Prefect adapter, separately from soak."""

from __future__ import annotations

import asyncio
import json
import resource
import time
import unittest
from dataclasses import asdict

import test_postgres_prefect_adapter as fixtures
from postgres_prefect_attempt_oracle import evaluate as evaluate_attempt
from postgres_storage_footprint import evaluate as evaluate_footprint
from postgres_stream_oracle import evaluate as evaluate_stream
from prefect_stream_adapter import prefect_stream_flow

from prefect.client.orchestration import get_client
from prefect.states import StateType


class PostgresShortAcceptanceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.PostgresPrefectAttemptTests("runTest")
        await self.fixture.asyncSetUp()

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def test_real_prefect_hundred_documents_stay_bounded_and_keep_four_tasks(
        self,
    ):
        f = self.fixture
        resources = await f.start_prefect_server()
        task = None
        try:
            config = f.adapter_config(
                maintenance=True, checkpoint_interval=2, operator_delay=0.6
            )
            documents = f.prefect_documents(
                *(f"acceptance-{n}".encode() for n in range(1, 101))
            )
            samples, next_sample, peak_admissions = [], 25, 0
            started = time.monotonic()
            task = asyncio.create_task(
                prefect_stream_flow(asdict(config), documents, return_state=True)
            )
            while not task.done():
                async with f.stream.pool.acquire() as conn:
                    accepted = await conn.fetchval(
                        "SELECT accepted_through FROM producer_progress WHERE channel='input' AND partition_id='p0' AND producer_id='stable-source'"
                    )
                    pressure = await conn.fetchval(
                        "SELECT used_admission_records FROM control_runs_v1 WHERE run_id=$1",
                        f.run_id,
                    )
                peak_admissions = max(peak_admissions, pressure)
                if accepted >= next_sample:
                    census = await evaluate_footprint(fixtures.DSN, f.schema)
                    async with f.stream.pool.acquire() as conn:
                        physical = await conn.fetchval(
                            "SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN('r','p')",
                            f.schema,
                        )
                    samples.append(
                        {
                            "accepted": accepted,
                            "elapsed_seconds": round(time.monotonic() - started, 3),
                            "logical_rows": census.total_rows,
                            "logical_json_bytes": census.total_json_bytes,
                            "physical_relation_bytes": physical,
                            "process_peak_rss_bytes": resource.getrusage(
                                resource.RUSAGE_SELF
                            ).ru_maxrss
                            * 1024,
                        }
                    )
                    self.assertLessEqual(census.total_rows, 512)
                    self.assertLessEqual(census.total_json_bytes, 256 * 1024)
                    next_sample += 25
                if time.monotonic() - started > 180:
                    self.fail("short native Prefect profile exceeded its deadline")
                await asyncio.sleep(0.02)
            state = await task
            elapsed = time.monotonic() - started
            self.assertEqual(state.type, StateType.COMPLETED)
            self.assertEqual(peak_admissions, 4)
            result = await state.result()
            task_ids = [
                result["source_task_run_id"],
                *result["operator_task_run_ids"],
                result["maintenance_task_run_id"],
            ]
            self.assertEqual(len(task_ids), 4)
            async with get_client() as client:
                actual = await f.read_prefect_tasks(client, task_ids)
            self.assertTrue(
                all(row.state_type == StateType.COMPLETED for row in actual)
            )
            async with f.stream.pool.acquire() as conn:
                states = await conn.fetch(
                    "SELECT state FROM join_workers ORDER BY participant_id"
                )
                self.assertEqual(
                    [json.loads(row["state"])["completed"] for row in states],
                    [100, 100],
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT accepted_through FROM producer_progress WHERE channel='input' AND partition_id='p0' AND producer_id='stable-source'"
                    ),
                    100,
                )
            for oracle in (evaluate_stream, evaluate_attempt):
                report = await oracle(fixtures.DSN, f.schema)
                self.assertTrue(report["passed"], report)
            print(
                "R5_MEASUREMENT_JSON="
                + json.dumps(
                    {
                        "name": "native_prefect_short_100_documents",
                        "documents": 100,
                        "duration_seconds": round(elapsed, 3),
                        "throughput_documents_per_second": round(100 / elapsed, 3),
                        "native_tasks": 4,
                        "physical_partition_records": 4,
                        "peak_admission_records": peak_admissions,
                        "samples": samples,
                        "long_soak_passed": False,
                        "scope": "single-source two-branch RETURN/uppercase profile; measured run includes startup and deliberate 0.6s operator delay; physical sizes are observations, no long-term vacuum/RSS stability claim",
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await f.stop_prefect_server(resources)


if __name__ == "__main__":
    unittest.main()
