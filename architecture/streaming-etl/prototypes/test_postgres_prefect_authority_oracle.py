from __future__ import annotations

import asyncio
import copy
import unittest

import test_postgres_prefect_adapter as fixtures
from postgres_prefect_attempt_oracle import evaluate
from postgres_stream_observability import StreamObservability


class PostgresPrefectAuthorityOracleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.PostgresPrefectAttemptTests("runTest")
        await self.fixture.asyncSetUp()

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def test_independent_attempt_horizon_and_projection_controls(self):
        f = self.fixture
        current = await f.activate()
        for number in range(2, 7):
            current = await f.replace(current, number)
        projection = await f.attempts.project_attempt(
            f.admin_token, control_run_id=f.run_id
        )
        report = await evaluate(fixtures.DSN, f.schema, projection)
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["observations"][f.run_id]["hot_attempts"], 2)
        self.assertEqual(report["observations"][f.run_id]["retired_through"], 4)
        for field in ("owner_epoch", "artifact_digest", "prefect_task_run_id"):
            corrupted = copy.deepcopy(projection)
            corrupted["operators"][0][field] = (
                999 if field == "owner_epoch" else "invented"
            )
            report = await evaluate(fixtures.DSN, f.schema, corrupted)
            self.assertIn("projection_mismatch:" + f.run_id, report["violations"])
        corrupted = copy.deepcopy(projection)
        corrupted["sources"][0]["enabled"] = False
        self.assertFalse((await evaluate(fixtures.DSN, f.schema, corrupted))["passed"])
        self.assertTrue((await evaluate(fixtures.DSN, f.schema, projection))["passed"])
        await f.control.expire_token("source-cap-6")
        report = await evaluate(fixtures.DSN, f.schema)
        self.assertIn("source_capability:" + current.attempt_id, report["violations"])

    async def test_oracle_detects_forbidden_old_source_reactivation(self):
        f = self.fixture
        old = await f.activate()
        await f.replace(old, 2)
        async with f.stream.pool.acquire() as conn, conn.transaction():
            # Explicitly corrupt a disposable schema to prove independent detection.
            await conn.execute(
                "ALTER TABLE prefect_attempt_source_caps_v1 DISABLE TRIGGER prefect_source_cap_guard"
            )
            await conn.execute("DROP INDEX prefect_one_live_source_actor")
            await conn.execute(
                "UPDATE prefect_attempt_source_caps_v1 SET enabled=true WHERE attempt_id=$1",
                old.attempt_id,
            )
            # The independent index also blocks reactivation. Removing it is part
            # of the deliberately unsafe negative control, never a recovery API.
        report = await evaluate(fixtures.DSN, f.schema)
        self.assertIn("source_capability:" + old.attempt_id, report["violations"])

    async def test_oracle_detects_terminal_actor_and_digest_drift(self):
        f = self.fixture
        current = await f.activate()
        await f.attempts.finish_attempt(
            f.admin_token,
            control_run_id=f.run_id,
            attempt_id=current.attempt_id,
            terminal_status="FAILED",
        )
        self.assertTrue((await evaluate(fixtures.DSN, f.schema))["passed"])
        async with f.stream.pool.acquire() as conn:
            await conn.execute(
                "UPDATE prefect_attempt_operators_v1 SET status='ACTIVE',replacement_digest='invalid' WHERE attempt_id=$1 AND participant_id='b'",
                current.attempt_id,
            )
        report = await evaluate(fixtures.DSN, f.schema)
        self.assertIn("current_operator_status:b", report["violations"])
        self.assertIn("operator_identity:b", report["violations"])

    async def test_native_observation_reports_only_actual_inflight_coordinate(self):
        f = self.fixture
        await f.activate()
        await f.append("source-cap-1", 1)
        projection = await f.attempts.project_attempt(
            f.admin_token, control_run_id=f.run_id
        )
        binding = next(
            row for row in projection["operators"] if row["participant_id"] == "b"
        )
        fixtures.STARTED, fixtures.RELEASE = asyncio.Event(), asyncio.Event()
        step = asyncio.create_task(
            f.runtimes["b"].step(
                "input/p0",
                fixtures.blocking_document,
                binding["owner_id"],
                binding["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=f.coordinator_epoch,
            )
        )
        service = StreamObservability(f.control, attempts=f.attempts)
        try:
            await asyncio.wait_for(fixtures.STARTED.wait(), 5)
            sample = await service.sample(f.admin_token, f.run_id)
            inputs = {row["node"]: row for row in sample["metrics"]["inputs"]}
            self.assertEqual(inputs["b"]["in_flight_sequence"], 1)
            self.assertTrue(inputs["b"]["in_flight"])
            self.assertIsNone(inputs["c"]["in_flight_sequence"])
            self.assertFalse(inputs["c"]["in_flight"])
        finally:
            fixtures.RELEASE.set()
            await asyncio.wait_for(step, 5)
            await service.close()


if __name__ == "__main__":
    unittest.main()
