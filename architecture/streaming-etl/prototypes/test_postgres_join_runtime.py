from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
import unittest
import uuid
from pathlib import Path

import asyncpg
from postgres_barrier_coordinator import PostgresBarrierCoordinator, decoded, digest
from postgres_dag_pressure import PostgresDagPressure
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_runtime import JoinPressure, PostgresJoinRuntime
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
def ordered_documents(ctx, payload):
    state = {"documents": ctx.managed_state.get("documents", []) + [payload.decode()]}
    yield OutputSpec(json.dumps(state, sort_keys=True).encode(), "documents/v1", state)


@stream_task
def pass_document(ctx, payload):
    state = {"documents": ctx.managed_state.get("documents", []) + [payload.decode()]}
    yield OutputSpec(payload, "bytes/v1", state)


class PostgresJoinTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.schema = "ds031_" + uuid.uuid4().hex[:12]
        self.store = await PostgresStream.open(DSN, self.schema)
        for partition in ("fast", "slow"):
            await self.store.create_partition(
                "input", partition, max_records=100, max_bytes=100000
            )
        await self.store.create_partition(
            "output", "p0", max_records=100, max_bytes=100000
        )
        self.coordinator = await PostgresBarrierCoordinator.install(self.store)
        self.revision = await self.coordinator.register_revision(
            revision_id="r1",
            deployment_id="d1",
            graph_version="v1",
            sources=[("input", "fast"), ("input", "slow")],
            participants=[
                {
                    "participant_id": "join",
                    "inputs": [
                        {"channel": "input", "partition": "fast"},
                        {"channel": "input", "partition": "slow"},
                    ],
                    "outputs": [{"channel": "output", "partition": "p0"}],
                    "code_digest": "ordered-documents-v1",
                    "state_schema": "documents/v1",
                    "state_version": 1,
                }
            ],
            code_manifest_digest="code-v1",
            state_contract_digest="state-v1",
            max_retained_checkpoints=100,
        )
        self.ce = await self.coordinator.claim_coordinator(
            "r1", "v1", self.revision.graph_digest, "coordinator", lease_seconds=300
        )
        self.runtime = PostgresJoinRuntime(self.coordinator, self.revision, "join")
        await self.runtime.install(
            initial_state={"documents": []},
            max_buffer_records=2,
            max_buffer_bytes=getattr(self, "buffer_byte_limit", 1000000),
        )
        self.epoch = await self.runtime.claim("anna", lease_seconds=300)

    async def asyncTearDown(self):
        await self.store.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        finally:
            await conn.close()

    def common(self):
        return ("r1", "v1", self.revision.graph_digest, "coordinator", self.ce)

    async def append(self, partition, number):
        payload = f"{partition}-{number}".encode()
        await self.store.append(
            f"append:{partition}:{number}",
            "input",
            partition,
            f"record:{partition}:{number}",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            f"lineage:{partition}:{number}",
        )

    async def step(self, partition, **kw):
        return await self.runtime.step(
            "input/" + partition,
            ordered_documents,
            "anna",
            self.epoch,
            coordinator_owner="coordinator",
            coordinator_epoch=self.ce,
            **kw,
        )

    async def skew(self):
        for p in ("fast", "slow"):
            for n in (1, 2):
                await self.append(p, n)
        cp = await self.coordinator.start_checkpoint(*self.common(), "first")
        for p in ("fast", "slow"):
            await self.coordinator.inject_source_barrier(
                *self.common(), cp.checkpoint_id, "input", p
            )
        for n in (3, 4):
            await self.append("fast", n)
        for _ in range(2):
            self.assertEqual(await self.step("fast"), "APPLIED")
        self.assertEqual(await self.step("fast"), "WAITING")
        self.assertEqual(await self.step("fast"), "BUFFERED")
        self.assertEqual(await self.step("fast"), "BUFFERED")
        self.assertEqual(await self.step("slow"), "APPLIED")
        self.assertEqual(await self.step("slow"), "APPLIED")
        return cp

    async def oracle(self):
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(HERE / "postgres_join_oracle.py"), DSN, self.schema],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        coordinator_report = await asyncio.to_thread(
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
        self.assertEqual(
            coordinator_report.returncode,
            0,
            coordinator_report.stdout + coordinator_report.stderr,
        )
        data_report = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(HERE / "postgres_stream_oracle.py"), DSN, self.schema],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            data_report.returncode, 0, data_report.stdout + data_report.stderr
        )
        return json.loads(result.stdout)

    async def test_skewed_alignment_snapshot_drain_and_replay(self):
        cp = await self.skew()
        self.assertEqual(
            decoded((await self.runtime.position())["state"])["documents"],
            ["fast-1", "fast-2", "slow-1", "slow-2"],
        )
        self.assertEqual(await self.step("slow"), "ALIGNED")
        self.assertEqual(
            await self.coordinator.checkpoint_status(cp.checkpoint_id), "ALIGNED"
        )
        self.assertEqual(await self.step("slow"), "DRAINED")
        self.assertEqual(await self.step("slow"), "DRAINED")
        log = await self.coordinator.ordered_log("output", "p0")
        self.assertEqual(
            [x["entry_kind"] for x in log], ["DATA"] * 4 + ["BARRIER"] + ["DATA"] * 2
        )
        # Local aligned state flows before global completion.
        await self.coordinator.complete_checkpoint(*self.common(), cp.checkpoint_id)
        plan = await self.coordinator.begin_recovery(*self.common(), "replace-join")
        before = await self.runtime.position()
        await self.store.close()
        self.store = await PostgresStream.connect_existing(DSN, self.schema)
        self.coordinator = PostgresBarrierCoordinator(self.store)
        self.runtime = PostgresJoinRuntime(self.coordinator, self.revision, "join")
        after = await self.runtime.restore(
            plan.recovery_id, ordered_documents, "anna", self.epoch
        )
        self.assertEqual(after, decoded(before["state"]))
        self.assertEqual(len(await self.coordinator.ordered_log("output", "p0")), 7)
        await self.append("slow", 3)
        self.assertEqual(await self.step("slow"), "APPLIED")
        await self.oracle()

    async def test_buffer_bound_does_not_advance_scan_and_slow_input_continues(self):
        await self.skew()
        await self.append("fast", 5)
        with self.assertRaises(JoinPressure):
            await self.step("fast")
        async with self.store.pool.acquire() as conn:
            scanned = await conn.fetchval(
                "SELECT scanned FROM join_inputs_v3 WHERE partition_id='fast'"
            )
        self.assertEqual(scanned, 5)
        self.assertEqual(await self.step("slow"), "ALIGNED")
        self.assertEqual(await self.step("slow"), "DRAINED")
        self.assertEqual(await self.step("slow"), "DRAINED")
        self.assertEqual(await self.step("fast"), "APPLIED")
        await self.oracle()

    async def test_abort_does_not_publish_a_mixed_or_partial_snapshot(self):
        cp = await self.skew()
        await self.coordinator.abort_checkpoint(
            *self.common(), cp.checkpoint_id, "test abort"
        )
        self.assertEqual(await self.step("slow"), "DRAINED")
        self.assertEqual(await self.step("slow"), "DRAINED")
        self.assertEqual(await self.step("slow"), "ABORTED")
        await self.append("slow", 3)
        self.assertEqual(await self.step("slow"), "APPLIED")
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM join_alignments_v3"), 0
            )

    async def test_loss_at_terminal_and_alignment_is_atomic(self):
        await self.skew()

        def fail(point):
            if point == "alignment_after_snapshot":
                raise RuntimeError("crash control")

        with self.assertRaises(RuntimeError):
            await self.step("slow", fault=fail)
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM participant_checkpoint_acks"),
                0,
            )
        self.assertEqual(await self.step("slow"), "ALIGNED")
        await self.oracle()

    async def test_client_execution_releases_db_locks_and_stale_terminal_is_fenced(
        self,
    ):
        await self.append("fast", 1)
        entered, release = threading.Event(), threading.Event()

        @stream_task
        def waiting(ctx, payload):
            entered.set()
            if not release.wait(10):
                raise RuntimeError("test client wait timeout")
            yield OutputSpec(payload, "bytes/v1", {"documents": ["fast-1"]})

        running = asyncio.create_task(
            self.runtime.step(
                "input/fast",
                waiting,
                "anna",
                self.epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.ce,
            )
        )
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 5))
            await self.runtime.claim("anna", lease_seconds=0.05)
            async with self.store.pool.acquire() as conn, conn.transaction():
                await conn.execute("SET LOCAL lock_timeout='200ms'")
                await conn.fetchrow("SELECT * FROM join_workers FOR UPDATE")
                await conn.execute("SELECT pg_sleep(0.08)")
            new_epoch = await self.runtime.claim("tanya", lease_seconds=300)
            release.set()
            with self.assertRaises(StaleOwner):
                await running
            self.assertEqual(
                await self.runtime.step(
                    "input/fast",
                    ordered_documents,
                    "tanya",
                    new_epoch,
                    coordinator_owner="coordinator",
                    coordinator_epoch=self.ce,
                ),
                "DRAINED",
            )
        finally:
            release.set()
            if not running.done():
                await running
        await self.oracle()

    async def test_multistage_routed_barriers_and_output_retention_roots(self):
        # A fans out to B/C; both feed J; D is the dependent tail.
        for partition in ("source", "a", "b", "c", "j", "d"):
            await self.store.create_partition(
                "dag", partition, max_records=100, max_bytes=100000
            )
        topology = {
            "A": (["source"], "a"),
            "B": (["a"], "b"),
            "C": (["a"], "c"),
            "J": (["b", "c"], "j"),
            "D": (["j"], "d"),
        }
        participants = [
            {
                "participant_id": name,
                "inputs": [{"channel": "dag", "partition": p} for p in ins],
                "outputs": [{"channel": "dag", "partition": out}],
                "code_digest": "pass-document-v1",
                "state_schema": "documents/v1",
                "state_version": 1,
            }
            for name, (ins, out) in topology.items()
        ]
        rev = await self.coordinator.register_revision(
            revision_id="dag-r1",
            deployment_id="dag-d1",
            graph_version="v1",
            sources=[("dag", "source")],
            participants=participants,
            code_manifest_digest="dag-code",
            state_contract_digest="dag-state",
        )
        ce = await self.coordinator.claim_coordinator(
            rev.revision_id, "v1", rev.graph_digest, "coordinator", lease_seconds=300
        )
        runtimes = {
            name: PostgresJoinRuntime(self.coordinator, rev, name) for name in topology
        }
        epochs = {}
        for name, rt in runtimes.items():
            await rt.install(initial_state={"documents": []})
            epochs[name] = await rt.claim(name, lease_seconds=300)

        async def step(name, part):
            return await runtimes[name].step(
                "dag/" + part,
                pass_document,
                name,
                epochs[name],
                coordinator_owner="coordinator",
                coordinator_epoch=ce,
            )

        for n in (1, 2):
            payload = f"document-{n}".encode()
            await self.store.append(
                f"dag-source-{n}",
                "dag",
                "source",
                f"dag-document-{n}",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                f"dag-lineage-{n}",
            )
            for name, part in (
                ("A", "source"),
                ("B", "a"),
                ("C", "a"),
                ("J", "b"),
                ("J", "c"),
                ("D", "j"),
                ("D", "j"),
            ):
                self.assertEqual(await step(name, part), "APPLIED")
        cp = await self.coordinator.start_checkpoint(
            "dag-r1", "v1", rev.graph_digest, "coordinator", ce, "dag-checkpoint"
        )
        await self.coordinator.inject_source_barrier(
            "dag-r1",
            "v1",
            rev.graph_digest,
            "coordinator",
            ce,
            cp.checkpoint_id,
            "dag",
            "source",
        )
        for name, part, expected in (
            ("A", "source", "ALIGNED"),
            ("B", "a", "ALIGNED"),
            ("C", "a", "ALIGNED"),
            ("J", "b", "WAITING"),
            ("J", "c", "ALIGNED"),
            ("D", "j", "ALIGNED"),
        ):
            self.assertEqual(await step(name, part), expected)
        await self.coordinator.complete_checkpoint(
            "dag-r1", "v1", rev.graph_digest, "coordinator", ce, cp.checkpoint_id
        )
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM participant_output_roots WHERE checkpoint_id=$1",
                    cp.checkpoint_id,
                ),
                5,
            )
            ref = await conn.fetchval(
                "SELECT retention_ref_id FROM participant_output_roots WHERE checkpoint_id=$1 AND participant_id='C'",
                cp.checkpoint_id,
            )
        with self.assertRaises(asyncpg.PostgresError):
            await self.store.release_checkpoint(ref)
        await self.oracle()

    async def test_real_process_loss_matrix_and_replacement(self):
        points = (
            "step_before_commit",
            "step_after_commit",
            "terminal_observed",
            "terminal_before_commit",
            "terminal_after_commit",
            "alignment_after_snapshot",
            "restore_begin_before_commit",
            "restore_begin_after_commit",
            "replay_turn_before_commit",
            "replay_turn_after_commit",
            "restore_ready_before_commit",
            "restore_ready_after_commit",
        )
        for point in points:
            fixture = PostgresJoinTests("runTest")
            await fixture.asyncSetUp()
            try:
                recovery = None
                if point.startswith(("alignment", "restore", "replay")):
                    cp = await fixture.skew()
                    if point.startswith(("restore", "replay")):
                        await fixture.step("slow")
                        await fixture.step("slow")
                        await fixture.step("slow")
                        await fixture.coordinator.complete_checkpoint(
                            *fixture.common(), cp.checkpoint_id
                        )
                        recovery = await fixture.coordinator.begin_recovery(
                            *fixture.common(), "crash-recovery"
                        )
                else:
                    await fixture.append("fast", 1)
                operation = "restore" if recovery else "step"
                identity = (
                    recovery.recovery_id
                    if recovery
                    else (
                        "input/slow" if point.startswith("alignment") else "input/fast"
                    )
                )
                result = await asyncio.to_thread(
                    subprocess.run,
                    [
                        sys.executable,
                        str(HERE / "postgres_join_crash_worker.py"),
                        DSN,
                        fixture.schema,
                        "r1",
                        "join",
                        "anna",
                        str(fixture.epoch),
                        operation,
                        identity,
                        point,
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    result.returncode, 86, point + result.stdout + result.stderr
                )
                # Explicit test clock control after confirmed process death.
                async with fixture.store.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE join_workers SET lease_expires_at=clock_timestamp()-interval '1 second'"
                    )
                epoch = await fixture.runtime.claim("tanya", lease_seconds=300)
                if recovery:
                    restored = await fixture.runtime.restore(
                        recovery.recovery_id, ordered_documents, "tanya", epoch
                    )
                    self.assertEqual(len(restored["documents"]), 6)
                else:
                    await fixture.runtime.step(
                        identity,
                        ordered_documents,
                        "tanya",
                        epoch,
                        coordinator_owner="coordinator",
                        coordinator_epoch=fixture.ce,
                    )
                await fixture.oracle()
                if not point.startswith(("alignment", "restore", "replay")):
                    self.assertEqual(
                        decoded((await fixture.runtime.position())["state"])[
                            "documents"
                        ],
                        ["fast-1"],
                    )
            finally:
                await fixture.asyncTearDown()

    async def test_async_yield_return_and_client_failure_retry_under_alignment(self):
        await self.skew()
        with self.assertRaisesRegex(PostgresProtocolError, "one active checkpoint"):
            await self.coordinator.start_checkpoint(*self.common(), "overlapping")
        self.assertEqual(await self.step("slow"), "ALIGNED")
        await self.step("slow")
        await self.step("slow")
        await self.append("slow", 3)

        @stream_task
        async def async_sign(ctx, payload):
            await asyncio.sleep(0)
            state = {"documents": ctx.managed_state["documents"] + [payload.decode()]}
            yield OutputSpec(payload, "bytes/v1", state)

        self.assertEqual(
            await self.runtime.step(
                "input/slow",
                async_sign,
                "anna",
                self.epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.ce,
            ),
            "APPLIED",
        )
        await self.append("slow", 4)

        @stream_task
        def return_only(ctx, payload):
            if False:
                yield
            return {"documents": ctx.managed_state["documents"] + [payload.decode()]}

        self.assertEqual(
            await self.runtime.step(
                "input/slow",
                return_only,
                "anna",
                self.epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.ce,
            ),
            "APPLIED",
        )
        await self.oracle()

    async def test_storage_guards_and_independent_corruption_control(self):
        await self.append("fast", 1)
        await self.step("fast")
        async with self.store.pool.acquire() as conn:
            for sql in (
                "UPDATE join_workers SET state='{}'::jsonb",
                "UPDATE join_inputs_v3 SET completed=completed+1 WHERE partition_id='fast'",
                "UPDATE join_inputs_v3 SET scanned=scanned+1 WHERE partition_id='fast'",
                "UPDATE join_receipts_v3 SET data_sequence=99",
                "UPDATE join_turns_v3 SET after_state='{}'::jsonb",
            ):
                with self.assertRaises(asyncpg.PostgresError):
                    await conn.execute(sql)
            await conn.execute("ALTER TABLE join_turns_v3 DISABLE TRIGGER immutable")
            await conn.execute("UPDATE join_turns_v3 SET before_state='{}'::jsonb")
            await conn.execute("ALTER TABLE join_turns_v3 ENABLE TRIGGER immutable")
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(HERE / "postgres_join_oracle.py"), DSN, self.schema],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        violations = json.loads(result.stdout)["violations"]
        self.assertIn("state_chain:r1:join", violations)

    async def test_failed_slow_turn_retries_before_earlier_blocked_fast_receipt(self):
        for part in ("fast", "slow"):
            await self.append(part, 1)
        cp = await self.coordinator.start_checkpoint(
            *self.common(), "retry-under-alignment"
        )
        for part in ("fast", "slow"):
            await self.coordinator.inject_source_barrier(
                *self.common(), cp.checkpoint_id, "input", part
            )
        await self.append("fast", 2)
        await self.step("fast")
        await self.step("fast")
        await self.step("fast")

        @stream_task
        def fail_before_terminal(ctx, payload):
            if False:
                yield
            raise RuntimeError("client failed")

        with self.assertRaisesRegex(RuntimeError, "client failed"):
            await self.runtime.step(
                "input/slow",
                fail_before_terminal,
                "anna",
                self.epoch,
                coordinator_owner="coordinator",
                coordinator_epoch=self.ce,
            )
        self.assertEqual(await self.step("slow"), "DRAINED")
        self.assertEqual(await self.step("slow"), "ALIGNED")
        self.assertEqual(await self.step("fast"), "DRAINED")
        await self.oracle()

    async def test_forged_terminal_insert_cannot_authorize_state_or_cursor(self):
        await self.append("fast", 1)

        def fail(point):
            if point == "terminal_observed":
                raise RuntimeError("client observed; platform not completed")

        with self.assertRaises(RuntimeError):
            await self.step("fast", fault=fail)
        async with self.store.pool.acquire() as conn:
            claim = await conn.fetchrow(
                "SELECT * FROM join_invocation_history_v3 LIMIT 1"
            )
            with self.assertRaisesRegex(
                asyncpg.PostgresError, "active fenced invocation"
            ):
                await conn.execute(
                    """INSERT INTO join_turns_v3 VALUES(
                    'r1','join',1,1,'{"documents":[]}'::jsonb,'{"documents":["FORGED"]}'::jsonb,
                    'RETURN',NULL,NULL,$1,'forged-digest',$2)""",
                    self.epoch,
                    claim["token"],
                )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM join_turns_v3"), 0
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT completed FROM join_inputs_v3 WHERE partition_id='fast'"
                ),
                0,
            )
        self.assertEqual(await self.step("fast"), "DRAINED")
        await self.oracle()

    async def test_oracle_rejects_coherently_rehashed_receipt_object_forgery(self):
        await self.append("fast", 1)
        await self.step("fast")
        async with self.store.pool.acquire() as conn:
            receipt = dict(
                await conn.fetchrow("SELECT * FROM join_receipts_v3 LIMIT 1")
            )
            receipt.pop("receipt_digest")
            receipt.update(
                object_key="forged-object", payload_digest="0" * 64, payload_size=999
            )
            await conn.execute("ALTER TABLE join_receipts_v3 DISABLE TRIGGER immutable")
            await conn.execute(
                """UPDATE join_receipts_v3 SET object_key=$1,payload_digest=$2,
                payload_size=$3,receipt_digest=$4""",
                receipt["object_key"],
                receipt["payload_digest"],
                receipt["payload_size"],
                digest(receipt),
            )
            await conn.execute("ALTER TABLE join_receipts_v3 ENABLE TRIGGER immutable")
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(HERE / "postgres_join_oracle.py"), DSN, self.schema],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
        self.assertIn(
            "receipt_object_identity:r1:join", json.loads(result.stdout)["violations"]
        )

    async def test_last_buffer_slot_race_pressure_and_credit_return(self):
        for part in ("fast", "slow"):
            await self.append(part, 1)
        cp = await self.coordinator.start_checkpoint(*self.common(), "last-slot")
        for part in ("fast", "slow"):
            await self.coordinator.inject_source_barrier(
                *self.common(), cp.checkpoint_id, "input", part
            )
        for number in (2, 3, 4):
            await self.append("fast", number)
        await self.step("fast")
        await self.step("fast")
        await self.step("fast")
        results = await asyncio.gather(
            self.step("fast"), self.step("fast"), return_exceptions=True
        )
        self.assertEqual(sum(x == "BUFFERED" for x in results), 1)
        self.assertEqual(sum(isinstance(x, JoinPressure) for x in results), 1)
        report = await PostgresDagPressure(self.coordinator).snapshot("r1")
        self.assertEqual(report["workers"][0]["buffered_records"], 2)
        self.assertIn("buffer_records", report["workers"][0]["binding_dimensions"])
        self.assertFalse(report["snapshot_reserves_credit"])
        self.assertEqual(await self.step("slow"), "APPLIED")
        self.assertEqual(await self.step("slow"), "ALIGNED")
        await self.step("slow")
        await self.step("slow")
        report = await PostgresDagPressure(self.coordinator).snapshot("r1")
        self.assertEqual(report["workers"][0]["buffered_records"], 0)
        self.assertEqual(await self.step("fast"), "APPLIED")
        await self.oracle()

    async def test_byte_only_buffer_saturation_does_not_advance_scan(self):
        fixture = PostgresJoinTests("runTest")
        fixture.buffer_byte_limit = 1
        await fixture.asyncSetUp()
        try:
            for part in ("fast", "slow"):
                await fixture.append(part, 1)
            cp = await fixture.coordinator.start_checkpoint(
                *fixture.common(), "byte-only"
            )
            for part in ("fast", "slow"):
                await fixture.coordinator.inject_source_barrier(
                    *fixture.common(), cp.checkpoint_id, "input", part
                )
            await fixture.append("fast", 2)
            await fixture.step("fast")
            await fixture.step("fast")
            with self.assertRaises(JoinPressure):
                await fixture.step("fast")
            async with fixture.store.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT scanned FROM join_inputs_v3 WHERE partition_id='fast'"
                    ),
                    2,
                )
            await fixture.step("slow")
            await fixture.step("slow")
            self.assertEqual(await fixture.step("fast"), "APPLIED")
            await fixture.oracle()
        finally:
            await fixture.asyncTearDown()


if __name__ == "__main__":
    unittest.main()
