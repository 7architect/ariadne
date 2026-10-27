from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import time
import unittest
import uuid
from pathlib import Path

import asyncpg
from postgres_generator_runtime import (
    OutputSpec,
    PostgresGeneratorRuntime,
    RuntimeProtocolError,
    stream_task,
)
from postgres_stream_v2 import PostgresProtocolError, PostgresStream

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
HERE = Path(__file__).resolve().parent
CRASH_WORKER = HERE / "postgres_runtime_crash_worker.py"
ORACLE = HERE / "postgres_stream_oracle.py"


class PostgresGeneratorRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def assert_independent_oracle(self, schema: str) -> None:
        result = subprocess.run(
            [sys.executable, str(ORACLE), DSN, schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    async def asyncSetUp(self) -> None:
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds020_" + uuid.uuid4().hex[:12]
        self.store = await PostgresStream.open(DSN, self.schema)
        await self.store.create_partition(
            "input", "p0", max_records=100, max_bytes=1_000_000
        )
        await self.store.create_partition(
            "output", "p0", max_records=100, max_bytes=1_000_000
        )

    async def asyncTearDown(self) -> None:
        if not hasattr(self, "store"):
            return
        await self.store.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        finally:
            await conn.close()

    async def seed(self, count: int) -> None:
        await self.store.append_batch(
            [
                (
                    f"input-attempt-{n}",
                    "input",
                    "p0",
                    f"input-record-{n}",
                    f"input-object-{n}",
                    f"document-{n}".encode(),
                    "bytes/v1",
                    f"source-{n}",
                )
                for n in range(1, count + 1)
            ]
        )
        await self.store.create_consumer("reviewer", "input", "p0")

    async def test_sync_async_exception_cancel_and_timeout(self) -> None:
        await self.seed(6)
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=60)
        runtime = PostgresGeneratorRuntime(self.store, output_channel="output")

        def undecorated(ctx, envelope):
            yield OutputSpec(b"forbidden")

        @stream_task
        async def wrong_async_mode(ctx, envelope):
            yield OutputSpec(b"forbidden")

        with self.assertRaisesRegex(RuntimeProtocolError, "not declared"):
            await runtime.run_sync("reviewer", "anna", epoch, undecorated)
        with self.assertRaisesRegex(RuntimeProtocolError, "cannot run as sync"):
            await runtime.run_sync("reviewer", "anna", epoch, wrong_async_mode)
        admission_position = await self.store.position("reviewer")
        self.assertEqual(admission_position["completed_through"], 0)
        self.assertIsNone(admission_position["in_flight_sequence"])

        @stream_task
        def sync_task(ctx, envelope):
            yield OutputSpec(
                b"signed-1",
                managed_state={"count": ctx.managed_state.get("count", 0) + 1},
            )

        self.assertEqual(
            await runtime.run_sync("reviewer", "anna", epoch, sync_task), "YIELD"
        )

        @stream_task
        async def async_task(ctx, envelope):
            yield OutputSpec(
                b"signed-2", managed_state={"count": ctx.managed_state["count"] + 1}
            )

        with self.assertRaisesRegex(RuntimeProtocolError, "cannot run as async"):
            await runtime.run_async("reviewer", "anna", epoch, sync_task)
        cross_mode_position = await self.store.position("reviewer")
        self.assertEqual(cross_mode_position["completed_through"], 1)
        self.assertIsNone(cross_mode_position["in_flight_sequence"])

        self.assertEqual(
            await runtime.run_async("reviewer", "anna", epoch, async_task), "YIELD"
        )
        self.assertEqual(sync_task.__ariadne_stream_mode__, "sync")
        self.assertEqual(async_task.__ariadne_stream_mode__, "async")
        with self.assertRaisesRegex(RuntimeProtocolError, "requires a generator"):
            stream_task(lambda: None)

        @stream_task
        def failed(ctx, envelope):
            raise ValueError("client failed")
            yield OutputSpec(b"never")

        with self.assertRaisesRegex(ValueError, "client failed"):
            await runtime.run_sync("reviewer", "anna", epoch, failed)

        started = asyncio.Event()

        @stream_task
        async def blocked(ctx, envelope):
            started.set()
            await asyncio.Event().wait()
            yield OutputSpec(b"never")

        with self.assertRaises(asyncio.TimeoutError):
            await runtime.run_async(
                "reviewer", "anna", epoch, blocked, timeout_seconds=0.02
            )
        with self.assertRaisesRegex(RuntimeProtocolError, "worker-process supervision"):
            await runtime.run_sync(
                "reviewer", "anna", epoch, sync_task, timeout_seconds=0.02
            )
        self.assertEqual(
            (await self.store.position("reviewer"))["in_flight_sequence"], 3
        )

        @stream_task
        async def recovered(ctx, envelope):
            yield OutputSpec(b"signed-3", managed_state={"count": 3})

        await runtime.run_async("reviewer", "anna", epoch, recovered)
        started.clear()
        task = asyncio.create_task(
            runtime.run_async("reviewer", "anna", epoch, blocked)
        )
        await started.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(
            (await self.store.position("reviewer"))["in_flight_sequence"], 4
        )

        @stream_task
        def sync_return(ctx, envelope):
            if False:
                yield OutputSpec(b"never")
            return {"count": ctx.managed_state["count"] + 1}

        self.assertEqual(
            await runtime.run_sync("reviewer", "anna", epoch, sync_return), "RETURN"
        )

        @stream_task
        async def async_return(ctx, envelope):
            if False:
                yield OutputSpec(b"never")

        self.assertEqual(
            await runtime.run_async("reviewer", "anna", epoch, async_return), "RETURN"
        )
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 5
        )
        self.assertTrue((await self.store.invariant_report())["passed"])
        self.assert_independent_oracle(self.schema)

    async def test_heartbeat_and_same_owner_recovery(self) -> None:
        await self.seed(2)
        epoch = await self.store.claim_consumer(
            "reviewer", "stable-worker", lease_seconds=0.05
        )
        runtime = PostgresGeneratorRuntime(
            self.store,
            output_channel="output",
            invocation_lease_seconds=0.12,
            heartbeat_interval_seconds=0.02,
        )

        @stream_task
        def slow_sync(ctx, envelope):
            time.sleep(0.16)
            yield OutputSpec(b"slow-sync", managed_state={"count": 1})

        await runtime.run_sync("reviewer", "stable-worker", epoch, slow_sync)

        @stream_task
        async def slow_async(ctx, envelope):
            await asyncio.sleep(0.16)
            yield OutputSpec(b"slow-async", managed_state={"count": 2})

        await runtime.run_async("reviewer", "stable-worker", epoch, slow_async)
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 2
        )

        await self.store.create_partition(
            "same-owner", "p0", max_records=1, max_bytes=100
        )
        await self.store.append(
            "same-a",
            "same-owner",
            "p0",
            "same-r",
            "same-o",
            b"x",
            "bytes/v1",
            "same-l",
        )
        await self.store.create_consumer("same-consumer", "same-owner", "p0")
        first_epoch = await self.store.claim_consumer(
            "same-consumer", "stable-worker", lease_seconds=0.05
        )
        first_token, _ = await self.store.deliver_next(
            "same-consumer", "stable-worker", first_epoch
        )
        await self.store.acquire_invocation(
            first_token, "dead-process", lease_seconds=0.05
        )
        async with self.store.pool.acquire() as conn:
            await conn.execute("SELECT pg_sleep(0.08)")
        second_epoch = await self.store.claim_consumer(
            "same-consumer", "stable-worker", lease_seconds=1
        )
        second_token, _ = await self.store.deliver_next(
            "same-consumer", "stable-worker", second_epoch
        )
        self.assertGreater(second_epoch, first_epoch)
        self.assertGreater(second_token.attempt, first_token.attempt)
        await self.store.acquire_invocation(
            second_token, "replacement", lease_seconds=1
        )
        await self.store.complete(
            second_token,
            "RETURN",
            managed_state={},
            invocation_holder_id="replacement",
        )
        self.assert_independent_oracle(self.schema)

    async def test_sync_cancellation_holds_invocation_until_thread_exits(self) -> None:
        await self.seed(1)
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=1)
        runtime = PostgresGeneratorRuntime(
            self.store,
            output_channel="output",
            invocation_lease_seconds=0.15,
            heartbeat_interval_seconds=0.02,
        )
        entered = threading.Event()
        release = threading.Event()

        @stream_task
        def slow(ctx, envelope):
            entered.set()
            release.wait(timeout=2)
            yield OutputSpec(b"cancelled-result", managed_state={"count": 1})

        running = asyncio.create_task(runtime.run_sync("reviewer", "anna", epoch, slow))
        self.assertTrue(await asyncio.to_thread(entered.wait, 1))
        running.cancel()
        await asyncio.sleep(0.06)
        self.assertFalse(running.done())
        running.cancel()
        await asyncio.sleep(0.03)
        running.cancel()
        await asyncio.sleep(0.03)
        self.assertFalse(running.done())
        with self.assertRaisesRegex(PostgresProtocolError, "lease held"):
            await self.store.claim_consumer("reviewer", "tanya", lease_seconds=1)
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM runtime_invocations"), 1
            )
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await running
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 0
        )
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM runtime_invocations"), 0
            )

    async def test_async_cancel_and_timeout_cannot_be_suppressed(self) -> None:
        await self.seed(1)
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=60)
        runtime = PostgresGeneratorRuntime(self.store, output_channel="output")
        started = asyncio.Event()

        @stream_task
        async def suppress_cancel(ctx, envelope):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                yield OutputSpec(b"must-not-commit", managed_state={"count": 999})

        running = asyncio.create_task(
            runtime.run_async("reviewer", "anna", epoch, suppress_cancel)
        )
        await started.wait()
        running.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await running
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 0
        )

        started.clear()
        with self.assertRaises(asyncio.TimeoutError):
            await runtime.run_async(
                "reviewer", "anna", epoch, suppress_cancel, timeout_seconds=0.02
            )
        position = await self.store.position("reviewer")
        self.assertEqual(position["completed_through"], 0)
        self.assertEqual(position["in_flight_sequence"], 1)

    async def test_state_migration_is_deterministic_or_rejected(self) -> None:
        await self.seed(3)
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=60)
        v1 = PostgresGeneratorRuntime(
            self.store, output_channel="output", state_schema="counter", state_version=1
        )

        @stream_task
        def first(ctx, envelope):
            yield OutputSpec(b"one", managed_state={"count": 1})

        await v1.run_sync("reviewer", "anna", epoch, first)
        v2 = PostgresGeneratorRuntime(
            self.store,
            output_channel="output",
            state_schema="counter",
            state_version=2,
            migrations={1: lambda state: {**state, "migrated": True}},
        )

        @stream_task
        def second(ctx, envelope):
            self.assertEqual(ctx.managed_state, {"count": 1, "migrated": True})
            yield OutputSpec(b"two", managed_state={"count": 2, "migrated": True})

        await v2.run_sync("reviewer", "anna", epoch, second)
        calls = 0

        def nondeterministic(state):
            nonlocal calls
            calls += 1
            return {**state, "nonce": calls}

        v3 = PostgresGeneratorRuntime(
            self.store,
            output_channel="output",
            state_schema="counter",
            state_version=3,
            migrations={2: nondeterministic},
        )
        with self.assertRaisesRegex(RuntimeProtocolError, "nondeterministic"):
            await v3.run_sync("reviewer", "anna", epoch, second)
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 2
        )
        incompatible = PostgresGeneratorRuntime(
            self.store, output_channel="output", state_schema="other", state_version=2
        )
        with self.assertRaisesRegex(RuntimeProtocolError, "schema is incompatible"):
            await incompatible.run_sync("reviewer", "anna", epoch, second)
        missing = PostgresGeneratorRuntime(
            self.store, output_channel="output", state_schema="counter", state_version=4
        )
        with self.assertRaisesRegex(
            RuntimeProtocolError, "missing state migration 2->3"
        ):
            await missing.run_sync("reviewer", "anna", epoch, second)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE consumers SET state_version=99 WHERE consumer_id='reviewer'"
            )
            await conn.execute(
                "UPDATE turn_completions SET state_version=99 "
                "WHERE consumer_id='reviewer' AND input_sequence=2"
            )
        future = PostgresGeneratorRuntime(
            self.store, output_channel="output", state_schema="counter", state_version=3
        )
        with self.assertRaisesRegex(RuntimeProtocolError, "newer than runtime"):
            await future.run_sync("reviewer", "anna", epoch, second)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE turn_completions SET state_version=2 "
                "WHERE consumer_id='reviewer' AND input_sequence=2"
            )
            await conn.execute(
                "UPDATE consumers SET state_version=2,managed_state='{\"count\":999}'::jsonb "
                "WHERE consumer_id='reviewer'"
            )
        with self.assertRaisesRegex(
            PostgresProtocolError, "managed state projection corrupted"
        ):
            await v2.run_sync("reviewer", "anna", epoch, second)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                """UPDATE consumers c SET managed_state=t.managed_state,
                   state_schema=t.state_schema,state_version=t.state_version
                   FROM turn_completions t WHERE c.consumer_id='reviewer'
                   AND t.consumer_id=c.consumer_id
                   AND t.input_sequence=c.completed_through"""
            )
        self.assertTrue((await self.store.invariant_report())["passed"])
        self.assert_independent_oracle(self.schema)

    async def test_repeated_anna_tanya_replacement_matrix(self) -> None:
        await self.seed(20)
        runtime = PostgresGeneratorRuntime(
            self.store,
            output_channel="output",
            state_schema="counter",
            state_version=1,
            invocation_lease_seconds=30,
        )
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=60)
        owner = "anna"

        @stream_task
        def task(ctx, envelope):
            yield OutputSpec(
                f"signed-{envelope.sequence}".encode(),
                managed_state={"count": ctx.managed_state.get("count", 0) + 1},
            )

        for sequence in range(1, 21):
            if sequence % 4 == 0:
                with self.assertRaisesRegex(RuntimeError, "worker lost"):
                    await runtime.run_sync(
                        "reviewer",
                        owner,
                        epoch,
                        task,
                        fault=lambda point: (
                            (_ for _ in ()).throw(RuntimeError("worker lost"))
                            if point == "runtime:after_delivery"
                            else None
                        ),
                    )
                async with self.store.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE consumers SET lease_expires_at=clock_timestamp()-interval '1 second' "
                        "WHERE consumer_id='reviewer'"
                    )
                    await conn.execute(
                        "UPDATE runtime_invocations SET expires_at=clock_timestamp()-interval '1 second' "
                        "WHERE consumer_id='reviewer'"
                    )
                owner = "tanya" if owner == "anna" else "anna"
                epoch = await self.store.claim_consumer(
                    "reviewer", owner, lease_seconds=60
                )
            await runtime.run_sync("reviewer", owner, epoch, task)
        position = await self.store.position("reviewer")
        state = (
            json.loads(position["managed_state"])
            if isinstance(position["managed_state"], str)
            else position["managed_state"]
        )
        self.assertEqual(position["completed_through"], 20)
        self.assertEqual(state, {"count": 20})
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM turn_completions"), 20
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM runtime_invocations"), 0
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM records WHERE channel='output'"
                ),
                20,
            )
        self.assertTrue((await self.store.invariant_report())["passed"])
        self.assert_independent_oracle(self.schema)

    async def test_real_process_loss_matrix_and_fresh_recovery(self) -> None:
        yield_points = (
            "runtime:after_delivery",
            "runtime:after_terminal",
            "completion:after_output_before_marker",
            "completion:after_marker_before_progress",
            "completion:after_progress_before_commit",
            "completion:after_commit_before_ack",
        )
        return_points = tuple(
            point
            for point in yield_points
            if point != "completion:after_output_before_marker"
        )
        for mode in ("sync", "async"):
            for kind, points in (("yield", yield_points), ("return", return_points)):
                for point in points:
                    with self.subTest(mode=mode, kind=kind, point=point):
                        schema = "ds020_crash_" + uuid.uuid4().hex[:10]
                        store = await PostgresStream.open(DSN, schema)
                        await store.create_partition(
                            "input", "p0", max_records=10, max_bytes=10_000
                        )
                        await store.create_partition(
                            "output", "p0", max_records=10, max_bytes=10_000
                        )
                        await store.append_batch(
                            [
                                (
                                    f"a-{n}",
                                    "input",
                                    "p0",
                                    f"r-{n}",
                                    f"o-{n}",
                                    f"document-{n}".encode(),
                                    "bytes/v1",
                                    f"l-{n}",
                                )
                                for n in (1, 2)
                            ]
                        )
                        await store.create_consumer("reviewer", "input", "p0")
                        await store.claim_consumer("reviewer", "anna", lease_seconds=60)
                        await store.close()
                        crashed = subprocess.run(
                            [
                                sys.executable,
                                str(CRASH_WORKER),
                                DSN,
                                schema,
                                mode,
                                point,
                                "--kind",
                                kind,
                            ],
                            cwd=HERE,
                            check=False,
                        )
                        self.assertEqual(crashed.returncode, 86)
                        recovered = subprocess.run(
                            [
                                sys.executable,
                                str(CRASH_WORKER),
                                DSN,
                                schema,
                                mode,
                                point,
                                "--kind",
                                kind,
                                "--recover",
                            ],
                            cwd=HERE,
                            check=False,
                        )
                        self.assertEqual(recovered.returncode, 0)
                        check = await PostgresStream.open(DSN, schema)
                        expected = (
                            2 if point == "completion:after_commit_before_ack" else 1
                        )
                        self.assertEqual(
                            (await check.position("reviewer"))["completed_through"],
                            expected,
                        )
                        async with check.pool.acquire() as conn:
                            self.assertEqual(
                                await conn.fetchval(
                                    "SELECT count(*) FROM turn_completions"
                                ),
                                expected,
                            )
                            self.assertEqual(
                                await conn.fetchval(
                                    "SELECT count(*) FROM runtime_invocations"
                                ),
                                0,
                            )
                        self.assertTrue((await check.invariant_report())["passed"])
                        await check.close()
                        self.assert_independent_oracle(schema)
                        conn = await asyncpg.connect(DSN)
                        await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
                        await conn.close()


if __name__ == "__main__":
    unittest.main()
