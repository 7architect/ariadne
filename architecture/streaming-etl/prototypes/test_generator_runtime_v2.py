from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from durable_stream_v2 import DurableStream, ObjectRef
from generator_runtime_v2 import GeneratorProtocolError, GeneratorRuntime, OutputSpec

HERE = Path(__file__).resolve().parent
ORACLE = HERE / "generator_runtime_oracle.py"
WORKER = HERE / "generator_runtime_crash_worker.py"


class GeneratorRuntimeTests(unittest.TestCase):
    def test_sync_yield_return_and_no_n_plus_one_before_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sync.sqlite"
            store, runtime = self.seed(path)

            def yielded(ctx, envelope):
                self.assertEqual(store.position("consumer")["in_flight"], 1)
                yield OutputSpec(b"one", managed_state={"sum": 1})

            self.assertEqual(runtime.run_sync("consumer", "anna", 1, yielded), "YIELD")
            self.assertEqual(store.position("consumer")["completed_through"], 1)

            def returned(ctx, envelope):
                if False:
                    yield OutputSpec(b"never")
                return {"sum": 3}

            self.assertEqual(
                runtime.run_sync("consumer", "anna", 1, returned), "RETURN"
            )
            self.assertEqual(store.position("consumer")["completed_through"], 2)
            store.close()
            self.oracle(path)

    def test_sync_exception_and_illegal_generator_redeliver_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sync-errors.sqlite"
            store, runtime = self.seed(path)

            def failed(ctx, envelope):
                raise ValueError("client failed")
                yield OutputSpec(b"never")

            with self.assertRaisesRegex(ValueError, "client failed"):
                runtime.run_sync("consumer", "anna", 1, failed)
            self.assertEqual(store.position("consumer")["in_flight"], 1)

            def emits_on_close(ctx, envelope):
                try:
                    yield OutputSpec(b"first")
                finally:
                    yield OutputSpec(b"illegal-second")

            with self.assertRaisesRegex(GeneratorProtocolError, "after terminal"):
                runtime.run_sync("consumer", "anna", 1, emits_on_close)
            self.assertEqual(store.position("consumer")["completed_through"], 0)
            store.close()
            self.oracle(path)

    def test_process_loss_matrix_converges_in_fresh_runtime(self) -> None:
        points = (
            "adapter:after_delivery_before_client",
            "adapter:after_terminal_observed_before_close",
            "adapter:after_close_before_object",
            "adapter:after_object_before_completion",
            "completion:after_output_insert_before_marker",
            "completion:after_marker_before_progress",
            "progress:after_cursor_update_before_commit",
            "progress:after_commit_before_ack",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for mode in ("sync", "async"):
                for point in points:
                    with self.subTest(mode=mode, point=point):
                        path = root / f"{mode}-{point.replace(':', '-')}.sqlite"
                        store, _ = self.seed(path)
                        store.close()
                        crashed = subprocess.run(
                            [sys.executable, str(WORKER), str(path), mode, point],
                            cwd=HERE,
                            check=False,
                        )
                        self.assertEqual(crashed.returncode, 86)
                        recovered = subprocess.run(
                            [
                                sys.executable,
                                str(WORKER),
                                str(path),
                                mode,
                                point,
                                "--recover",
                            ],
                            cwd=HERE,
                            check=False,
                        )
                        self.assertEqual(recovered.returncode, 0)
                        db = sqlite3.connect(path)
                        expected_completed = (
                            2 if point == "progress:after_commit_before_ack" else 1
                        )
                        self.assertEqual(
                            db.execute(
                                "SELECT completed_through FROM consumers WHERE consumer_id = 'consumer'"
                            ).fetchone()[0],
                            expected_completed,
                        )
                        self.assertEqual(
                            db.execute(
                                "SELECT COUNT(*) FROM turn_completions"
                            ).fetchone()[0],
                            expected_completed,
                        )
                        self.assertEqual(
                            db.execute(
                                "SELECT COUNT(*) FROM runtime_invocations"
                            ).fetchone()[0],
                            0,
                        )
                        db.close()
                        self.oracle(path)

    def test_terminal_state_is_frozen_before_generator_close(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state-snapshot.sqlite"
            store, runtime = self.seed(path)
            state = {"value": 1}

            def task(ctx, envelope):
                try:
                    yield OutputSpec(b"one", managed_state=state)
                finally:
                    state["value"] = 999

            self.assertEqual(runtime.run_sync("consumer", "anna", 1, task), "YIELD")
            self.assertEqual(state, {"value": 999})
            self.assertEqual(store.position("consumer")["managed_state"], {"value": 1})
            store.close()
            self.oracle(path)

    def test_false_non_dict_return_is_rejected(self) -> None:
        for returned in ([], 0, ""):
            with (
                self.subTest(returned=returned),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "bad-return.sqlite"
                store, runtime = self.seed(path)

                def task(ctx, envelope):
                    if False:
                        yield OutputSpec(b"never")
                    return returned

                with self.assertRaisesRegex(GeneratorProtocolError, "RETURN state"):
                    runtime.run_sync("consumer", "anna", 1, task)
                self.assertEqual(store.position("consumer")["completed_through"], 0)
                store.close()

    def test_oracle_rejects_consumer_state_projection_corruption(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state-corruption.sqlite"
            store, runtime = self.seed(path)

            def task(ctx, envelope):
                yield OutputSpec(b"one", managed_state={"value": 1})

            runtime.run_sync("consumer", "anna", 1, task)
            store.close()
            db = sqlite3.connect(path)
            with db:
                db.execute("DROP TRIGGER consumer_progress_guard")
                db.execute("UPDATE consumers SET state_json=?", ('{"value":999}',))
                db.execute(
                    "INSERT INTO runtime_invocations VALUES "
                    "('consumer',1,1,'anna',1,'forged-holder',10)"
                )
            db.close()
            result = subprocess.run(
                [sys.executable, str(ORACLE), str(path)],
                cwd=HERE,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 2)
            self.assertIn(
                "consumer_state_projection:consumer",
                json.loads(result.stdout)["violations"],
            )
            self.assertIn(
                "terminal_invocation:consumer", json.loads(result.stdout)["violations"]
            )

    @staticmethod
    def seed(path: Path) -> tuple[DurableStream, GeneratorRuntime]:
        store = DurableStream(path)
        store.claim_producer("input", "p0", "petya", 1)
        store.claim_producer("output", "p0", "runtime", 1)
        for sequence in (1, 2):
            payload = f"input-{sequence}".encode()
            ref = ObjectRef.for_payload(f"input-{sequence}", payload, "bytes/v1")
            store.register_object(ref)
            store.append(
                "input",
                "p0",
                "petya",
                1,
                f"record-{sequence}",
                ref,
                f"lineage-{sequence}",
            )
        store.create_consumer("consumer", "input", "p0")
        store.claim_consumer("consumer", "anna", 1, lease_ttl=100)
        return store, GeneratorRuntime(store, output_channel="output")

    @staticmethod
    def oracle(path: Path) -> dict[str, object]:
        result = subprocess.run(
            [sys.executable, str(ORACLE), str(path)],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise AssertionError((result.returncode, result.stdout, result.stderr))
        return json.loads(result.stdout)


class AsyncGeneratorRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_before_commit_redelivers_and_after_commit_advances(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "async-cancel.sqlite"
            store, runtime = GeneratorRuntimeTests.seed(path)
            started = asyncio.Event()
            release = asyncio.Event()

            async def waiting(ctx, envelope):
                started.set()
                await release.wait()
                yield OutputSpec(b"one")

            running = asyncio.create_task(
                runtime.run_async("consumer", "anna", 1, waiting)
            )
            await started.wait()
            running.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await running
            self.assertEqual(store.position("consumer")["in_flight"], 1)

            async def completed(ctx, envelope):
                yield OutputSpec(b"one", managed_state={"seen": 1})

            self.assertEqual(
                await runtime.run_async("consumer", "anna", 1, completed), "YIELD"
            )
            self.assertEqual(store.position("consumer")["completed_through"], 1)

            def cancel_after_commit(point: str) -> None:
                if point == "progress:after_commit_before_ack":
                    raise asyncio.CancelledError

            with self.assertRaises(asyncio.CancelledError):
                await runtime.run_async(
                    "consumer",
                    "anna",
                    1,
                    completed,
                    completion_fault=cancel_after_commit,
                )
            self.assertEqual(store.position("consumer")["completed_through"], 2)
            store.close()
            GeneratorRuntimeTests.oracle(path)

    async def test_concurrent_async_invocation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "async-concurrent.sqlite"
            store, runtime = GeneratorRuntimeTests.seed(path)
            started = asyncio.Event()
            release = asyncio.Event()

            async def waiting(ctx, envelope):
                started.set()
                await release.wait()
                yield OutputSpec(b"one")

            first = asyncio.create_task(
                runtime.run_async("consumer", "anna", 1, waiting)
            )
            await started.wait()
            with self.assertRaisesRegex(GeneratorProtocolError, "already active"):
                await runtime.run_async("consumer", "anna", 1, waiting)
            first.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await first
            store.close()

    async def test_concurrent_runtime_instances_share_durable_guard(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "async-multi-runtime.sqlite"
            store, first_runtime = GeneratorRuntimeTests.seed(path)
            second_runtime = GeneratorRuntime(store, output_channel="output")
            started = asyncio.Event()
            release = asyncio.Event()
            effects: list[int] = []

            async def waiting(ctx, envelope):
                effects.append(envelope.sequence)
                started.set()
                await release.wait()
                yield OutputSpec(b"one")

            first = asyncio.create_task(
                first_runtime.run_async("consumer", "anna", 1, waiting)
            )
            await started.wait()
            with self.assertRaisesRegex(GeneratorProtocolError, "already active"):
                await second_runtime.run_async("consumer", "anna", 1, waiting)
            self.assertEqual(effects, [1])
            release.set()
            self.assertEqual(await first, "YIELD")
            store.close()
            GeneratorRuntimeTests.oracle(path)

    async def test_expired_invocation_cannot_be_stolen_with_same_owner_epoch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "same-owner-expiry.sqlite"
            store, first_runtime = GeneratorRuntimeTests.seed(path)
            second_runtime = GeneratorRuntime(
                store, output_channel="output", invocation_ttl=10
            )
            started = asyncio.Event()
            release = asyncio.Event()
            effects: list[int] = []

            async def waiting(ctx, envelope):
                effects.append(envelope.sequence)
                started.set()
                await release.wait()
                yield OutputSpec(b"one")

            first = asyncio.create_task(
                first_runtime.run_async("consumer", "anna", 1, waiting)
            )
            await started.wait()
            store.advance_time(10)
            with self.assertRaisesRegex(GeneratorProtocolError, "already active"):
                await second_runtime.run_async("consumer", "anna", 1, waiting)
            self.assertEqual(effects, [1])
            release.set()
            self.assertEqual(await first, "YIELD")
            store.close()
            GeneratorRuntimeTests.oracle(path)

    async def test_timeout_releases_invocation_and_redelivers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "async-timeout.sqlite"
            store, runtime = GeneratorRuntimeTests.seed(path)

            async def blocked(ctx, envelope):
                await asyncio.Event().wait()
                yield OutputSpec(b"never")

            with self.assertRaises(asyncio.TimeoutError):
                await runtime.run_async(
                    "consumer", "anna", 1, blocked, timeout_seconds=0.01
                )
            self.assertEqual(store.position("consumer")["in_flight"], 1)

            async def recovered(ctx, envelope):
                yield OutputSpec(b"one")

            self.assertEqual(
                await runtime.run_async("consumer", "anna", 1, recovered), "YIELD"
            )
            store.close()
            GeneratorRuntimeTests.oracle(path)


if __name__ == "__main__":
    unittest.main()
