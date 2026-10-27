from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import unittest
import uuid
from dataclasses import replace
from pathlib import Path

import asyncpg
from postgres_stream_v2 import (
    AppendResult,
    CapacityBlocked,
    DeliveryToken,
    IdempotencyHorizonExpired,
    Output,
    PostgresProtocolError,
    PostgresStream,
    StaleOwner,
    content_object_key,
    metadata_operation_id,
)

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
HERE = Path(__file__).resolve().parent
ORACLE = HERE / "postgres_stream_oracle.py"


class PostgresStreamTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds010_" + uuid.uuid4().hex[:12]
        self.store = await PostgresStream.open(DSN, self.schema)

    async def asyncTearDown(self) -> None:
        if not hasattr(self, "store"):
            return
        await self.store.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        finally:
            await conn.close()

    async def test_two_coordinators_allocate_one_contiguous_order(self) -> None:
        await self.store.create_partition(
            "input", "p0", max_records=200, max_bytes=200_000
        )
        peer = await PostgresStream.open(DSN, self.schema)
        try:

            async def append(number: int):
                store = self.store if number % 2 else peer
                return await store.append(
                    f"attempt-{number}",
                    "input",
                    "p0",
                    f"record-{number}",
                    f"object-{number}",
                    f"payload-{number}".encode(),
                    "bytes/v1",
                    f"lineage-{number}",
                )

            results = await asyncio.gather(*(append(n) for n in range(1, 101)))
            self.assertEqual(
                sorted(result.sequence for result in results), list(range(1, 101))
            )
            retried = await peer.append(
                "attempt-37",
                "input",
                "p0",
                "record-37",
                "object-37",
                b"payload-37",
                "bytes/v1",
                "lineage-37",
            )
            original = next(item for item in results if item.record_id == "record-37")
            self.assertEqual(retried, original)
            with self.assertRaisesRegex(PostgresProtocolError, "identity drift"):
                await peer.append(
                    "attempt-37",
                    "input",
                    "p0",
                    "record-other",
                    "object-37",
                    b"payload-37",
                    "bytes/v1",
                    "lineage-37",
                )
            self.assertTrue((await self.store.invariant_report())["passed"])
        finally:
            await peer.close()

    async def test_capacity_is_atomic_under_concurrent_near_limit_append(self) -> None:
        await self.store.create_partition(
            "bounded", "p0", max_records=20, max_bytes=200
        )

        async def append(number: int) -> str:
            try:
                await self.store.append(
                    f"capacity-{number}",
                    "bounded",
                    "p0",
                    f"capacity-record-{number}",
                    f"capacity-object-{number}",
                    b"x" * 10,
                    "bytes/v1",
                    f"l-{number}",
                )
            except CapacityBlocked:
                return "BLOCKED"
            return "APPENDED"

        outcomes = await asyncio.gather(*(append(n) for n in range(50)))
        self.assertEqual(outcomes.count("APPENDED"), 20)
        self.assertEqual(outcomes.count("BLOCKED"), 30)
        async with self.store.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel='bounded' AND partition_id='p0'"
            )
            self.assertEqual((row["used_records"], row["used_bytes"]), (20, 200))
        self.assertTrue((await self.store.invariant_report())["passed"])

    async def test_pressure_api_names_record_and_byte_dimensions(self) -> None:
        await self.store.create_partition("records", "p0", max_records=1, max_bytes=100)
        await self.store.append(
            "records-a1",
            "records",
            "p0",
            "records-r1",
            "records-o1",
            b"x",
            "bytes/v1",
            "records-l1",
        )
        with self.assertRaises(CapacityBlocked) as record_block:
            await self.store.append(
                "records-a2",
                "records",
                "p0",
                "records-r2",
                "records-o2",
                b"y",
                "bytes/v1",
                "records-l2",
            )
        self.assertEqual(record_block.exception.attempt_ids, ("records-a2",))
        self.assertEqual(
            record_block.exception.pressure["binding_dimensions"], ["records"]
        )
        public = await self.store.pressure_snapshot(
            "records", "p0", proposed_records=1, proposed_bytes=1
        )
        self.assertEqual(public["binding_dimensions"], ["records"])
        async with self.store.pool.acquire() as conn:
            durable = await conn.fetchrow(
                "SELECT block_reason,pressure_snapshot FROM append_attempts "
                "WHERE attempt_id='records-a2'"
            )
        self.assertEqual(durable["block_reason"], "records")
        self.assertEqual(json.loads(durable["pressure_snapshot"])["used_records"], 1)
        await self.store.compact("records", "p0", 1)
        resumed = await self.store.append(
            "records-a2",
            "records",
            "p0",
            "records-r2",
            "records-o2",
            b"y",
            "bytes/v1",
            "records-l2",
        )
        self.assertEqual(resumed.sequence, 2)

        await self.store.create_partition("bytes", "p0", max_records=10, max_bytes=3)
        await self.store.append(
            "bytes-a1",
            "bytes",
            "p0",
            "bytes-r1",
            "bytes-o1",
            b"xx",
            "bytes/v1",
            "bytes-l1",
        )
        with self.assertRaises(CapacityBlocked) as byte_block:
            await self.store.append(
                "bytes-a2",
                "bytes",
                "p0",
                "bytes-r2",
                "bytes-o2",
                b"yy",
                "bytes/v1",
                "bytes-l2",
            )
        self.assertEqual(byte_block.exception.pressure["binding_dimensions"], ["bytes"])
        self.assertEqual(byte_block.exception.pressure["projected_bytes"], 4)

    async def test_time_budget_blocks_until_compaction_returns_credit(self) -> None:
        await self.store.create_partition(
            "aged", "p0", max_records=10, max_bytes=1000, max_age_seconds=60
        )
        await self.store.append(
            "aged-1",
            "aged",
            "p0",
            "aged-r1",
            "aged-o1",
            b"one",
            "bytes/v1",
            "aged-l1",
        )
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE records SET created_at=clock_timestamp()-interval '61 seconds' "
                "WHERE channel='aged' AND partition_id='p0' AND sequence=1"
            )
        with self.assertRaises(CapacityBlocked) as age_block:
            await self.store.append(
                "aged-2",
                "aged",
                "p0",
                "aged-r2",
                "aged-o2",
                b"two",
                "bytes/v1",
                "aged-l2",
            )
        self.assertEqual(age_block.exception.pressure["binding_dimensions"], ["age"])
        age_public = await self.store.pressure_snapshot("aged", "p0")
        self.assertEqual(age_public["binding_dimensions"], ["age"])
        reclaimed = await self.store.compact("aged", "p0", 1)
        self.assertEqual(reclaimed["reclaimed_records"], 1)
        retried = await self.store.append(
            "aged-2",
            "aged",
            "p0",
            "aged-r2",
            "aged-o2",
            b"two",
            "bytes/v1",
            "aged-l2",
        )
        self.assertEqual(retried.sequence, 2)

    async def test_consumer_capacity_is_atomic_and_public(self) -> None:
        await self.store.create_partition(
            "consumer-cap",
            "p0",
            max_records=1,
            max_bytes=100,
            max_consumers=1,
        )

        async def register(name: str):
            try:
                await self.store.create_consumer(name, "consumer-cap", "p0")
                return name
            except CapacityBlocked as exc:
                return exc

        results = await asyncio.gather(register("consumer-a"), register("consumer-b"))
        admitted = [result for result in results if isinstance(result, str)]
        blocked = [result for result in results if isinstance(result, CapacityBlocked)]
        self.assertEqual(len(admitted), 1)
        self.assertEqual(len(blocked), 1)
        self.assertEqual(blocked[0].pressure["binding_dimensions"], ["consumers"])
        pressure = await self.store.pressure_snapshot(
            "consumer-cap", "p0", proposed_consumers=1
        )
        self.assertEqual(pressure["used_consumers"], 1)
        self.assertEqual(pressure["max_consumers"], 1)
        self.assertEqual(pressure["binding_dimensions"], ["consumers"])
        oracle = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(oracle.returncode, 0, oracle.stdout + oracle.stderr)

    async def test_server_time_lease_takeover_fences_stale_completion(self) -> None:
        await self.store.create_partition("input", "p0", max_records=10, max_bytes=1000)
        await self.store.create_partition(
            "output", "p0", max_records=10, max_bytes=1000
        )
        await self.store.append(
            "input-1",
            "input",
            "p0",
            "record-1",
            "object-1",
            b"document-1",
            "bytes/v1",
            "source-1",
        )
        await self.store.create_consumer("reviewer", "input", "p0")
        anna_epoch = await self.store.claim_consumer(
            "reviewer", "anna", lease_seconds=0.05
        )
        anna_token, _ = await self.store.deliver_next("reviewer", "anna", anna_epoch)
        async with self.store.pool.acquire() as conn:
            await conn.execute("SELECT pg_sleep(0.08)")
        tanya_epoch = await self.store.claim_consumer(
            "reviewer", "tanya", lease_seconds=10
        )
        self.assertGreater(tanya_epoch, anna_epoch)
        with self.assertRaises(StaleOwner):
            await self.store.complete(
                anna_token, "RETURN", managed_state={"seen": "anna"}
            )
        tanya_token, envelope = await self.store.deliver_next(
            "reviewer", "tanya", tanya_epoch
        )
        self.assertEqual(envelope.sequence, 1)
        await self.store.complete(
            tanya_token,
            "YIELD",
            output=Output(
                "output",
                "p0",
                "signed-1",
                "signed-object-1",
                b"signed",
                "bytes/v1",
                "reviewer-1",
            ),
            managed_state={"seen": 1},
        )
        input_compaction = await self.store.compact("input", "p0", 1)
        output_compaction = await self.store.compact("output", "p0", 1)
        self.assertEqual(input_compaction["reclaimed_records"], 1)
        self.assertEqual(output_compaction["reclaimed_records"], 1)
        self.assertEqual(
            await self.store.complete(
                tanya_token,
                "YIELD",
                output=Output(
                    "output",
                    "p0",
                    "signed-1",
                    "signed-object-1",
                    b"signed",
                    "bytes/v1",
                    "reviewer-1",
                ),
                managed_state={"seen": 1},
            ),
            1,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "completion identity drift"):
            await self.store.complete(
                tanya_token,
                "YIELD",
                output=Output(
                    "output",
                    "p0",
                    "different",
                    "different-object",
                    b"different",
                    "bytes/v2",
                    "different-lineage",
                ),
                managed_state={"seen": 999},
                state_schema="different/v2",
                state_version=2,
            )
        position = await self.store.position("reviewer")
        self.assertEqual(position["completed_through"], 1)
        self.assertEqual(position["owner_id"], "tanya")
        self.assertTrue((await self.store.invariant_report())["passed"])

    async def test_forged_delivery_attempt_cannot_advance_progress(self) -> None:
        await self.store.create_partition("input", "p0", max_records=2, max_bytes=100)
        await self.store.append(
            "a-1", "input", "p0", "r-1", "o-1", b"one", "bytes/v1", "l-1"
        )
        await self.store.create_consumer("reviewer", "input", "p0")
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=60)
        token, _ = await self.store.deliver_next("reviewer", "anna", epoch)
        forged = DeliveryToken(
            token.consumer_id,
            token.sequence,
            token.owner_id,
            token.owner_epoch,
            token.attempt + 998,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "exact in-flight"):
            await self.store.complete(forged, "RETURN", managed_state={})
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 0
        )

    async def test_bounded_batch_requires_delivery_and_replays_whole_range(
        self,
    ) -> None:
        await self.store.create_partition("input", "p0", max_records=10, max_bytes=1000)
        for number in range(1, 11):
            await self.store.append(
                f"a-{number}",
                "input",
                "p0",
                f"r-{number}",
                f"o-{number}",
                f"document-{number}".encode(),
                "bytes/v1",
                f"l-{number}",
            )
        await self.store.create_consumer("reviewer", "input", "p0")
        epoch = await self.store.claim_consumer("reviewer", "anna", lease_seconds=10)
        first = await self.store.deliver_batch("reviewer", "anna", epoch, max_records=4)
        self.assertEqual([token.sequence for token, _ in first], [1, 2, 3, 4])
        forged = list(first)
        forged[-1] = (replace(forged[-1][0], attempt=999), forged[-1][1])
        with self.assertRaisesRegex(PostgresProtocolError, "token identity drift"):
            await self.store.complete_return_batch(forged)
        forged_envelope = list(first)
        forged_envelope[-1] = (
            forged_envelope[-1][0],
            replace(forged_envelope[-1][1], payload=b"forged"),
        )
        with self.assertRaisesRegex(PostgresProtocolError, "envelope identity drift"):
            await self.store.complete_return_batch(forged_envelope)
        states = [{"signed": number} for number in range(1, 5)]
        self.assertEqual(
            await self.store.complete_return_batch(first, managed_states=states), 4
        )
        compacted_first = await self.store.compact("input", "p0", 4)
        self.assertEqual(compacted_first["sequences"], [1, 2, 3, 4])
        self.assertEqual(
            await self.store.complete_return_batch(first, managed_states=states), 4
        )
        forged_after_compaction = list(first)
        forged_after_compaction[-1] = (
            forged_after_compaction[-1][0],
            replace(forged_after_compaction[-1][1], payload=b"forged"),
        )
        with self.assertRaisesRegex(PostgresProtocolError, "envelope identity drift"):
            await self.store.complete_return_batch(
                forged_after_compaction, managed_states=states
            )
        with self.assertRaisesRegex(PostgresProtocolError, "identity drift"):
            await self.store.complete_return_batch(
                first, managed_states=[{"signed": 999}] * 4
            )
        second = await self.store.deliver_batch(
            "reviewer", "anna", epoch, max_records=4
        )
        self.assertEqual([token.sequence for token, _ in second], [5, 6, 7, 8])
        with self.assertRaisesRegex(PostgresProtocolError, "bounded batch"):
            await self.store.deliver_next("reviewer", "anna", epoch)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE consumers SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE consumer_id='reviewer'"
            )
        new_epoch = await self.store.claim_consumer(
            "reviewer", "tanya", lease_seconds=10
        )
        with self.assertRaises(StaleOwner):
            await self.store.complete_return_batch(second)
        replayed = await self.store.deliver_batch(
            "reviewer", "tanya", new_epoch, max_records=8
        )
        self.assertEqual([token.sequence for token, _ in replayed], [5, 6, 7, 8])
        self.assertGreater(replayed[0][0].attempt, second[0][0].attempt)
        await self.store.complete_return_batch(replayed)
        self.assertEqual(
            (await self.store.position("reviewer"))["completed_through"], 8
        )
        self.assertTrue((await self.store.invariant_report())["passed"])

    async def test_staged_orphan_is_invisible_until_atomic_append(self) -> None:
        await self.store.create_partition("input", "p0", max_records=2, max_bytes=100)
        await self.store.stage_object("orphan", b"payload", "bytes/v1")
        self.assertEqual(await self.store.orphan_objects(), ["orphan"])
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM records WHERE object_key='orphan'"
                ),
                0,
            )
        result = await self.store.append(
            "publish-orphan",
            "input",
            "p0",
            "record-1",
            "orphan",
            b"payload",
            "bytes/v1",
            "lineage-1",
        )
        self.assertEqual(result.sequence, 1)
        self.assertEqual(await self.store.orphan_objects(), [])
        self.assertTrue((await self.store.invariant_report())["passed"])

    async def test_completion_batch_straddling_retired_horizon_expires(self) -> None:
        await self.store.create_partition(
            "batch-horizon",
            "p0",
            max_records=4,
            max_bytes=1000,
            max_metadata_records=4,
            max_producers=1,
        )
        await self.store.register_producer("batch-horizon", "p0", "batch-producer")
        items = []
        for number in range(1, 5):
            payload = f"batch-{number}".encode()
            items.append(
                (
                    f"batch-a-{number}",
                    "batch-horizon",
                    "p0",
                    f"batch-r-{number}",
                    content_object_key(payload, "bytes/v1"),
                    payload,
                    "bytes/v1",
                    f"batch-l-{number}",
                    "batch-producer",
                    number,
                )
            )
        await self.store.append_batch(items)
        await self.store.create_consumer("batch-consumer", "batch-horizon", "p0")
        epoch = await self.store.claim_consumer(
            "batch-consumer", "worker", lease_seconds=60
        )
        delivered = await self.store.deliver_batch(
            "batch-consumer", "worker", epoch, max_records=4
        )
        states = [{"last": number} for number in range(1, 5)]
        await self.store.complete_return_batch(delivered, managed_states=states)
        await self.store.compact("batch-horizon", "p0", 2)
        cuts = [("batch-horizon", "p0", 2)]
        await self.store.compact_metadata(
            cuts, operation_id=metadata_operation_id(cuts)
        )
        with self.assertRaisesRegex(
            IdempotencyHorizonExpired, "intersects retired horizon"
        ):
            await self.store.complete_return_batch(delivered, managed_states=states)
        drifted = list(delivered)
        drifted[-1] = (drifted[-1][0], replace(drifted[-1][1], payload=b"drifted"))
        with self.assertRaisesRegex(
            IdempotencyHorizonExpired, "intersects retired horizon"
        ):
            await self.store.complete_return_batch(drifted, managed_states=states)
        self.assertEqual(
            await self.store.complete_return_batch(
                delivered[2:], managed_states=states[2:]
            ),
            2,
        )

    async def test_object_identity_survives_payload_compaction(self) -> None:
        await self.store.create_partition("input", "p0", max_records=3, max_bytes=100)
        await self.store.append(
            "shared-a1",
            "input",
            "p0",
            "shared-r1",
            "shared",
            b"stable",
            "bytes/v1",
            "shared-l1",
        )
        await self.store.compact("input", "p0", 1)
        same = await self.store.append(
            "shared-a2",
            "input",
            "p0",
            "shared-r2",
            "shared",
            b"stable",
            "bytes/v1",
            "shared-l2",
        )
        self.assertEqual(same.sequence, 2)
        await self.store.compact("input", "p0", 2)
        with self.assertRaisesRegex(PostgresProtocolError, "object identity drift"):
            await self.store.append(
                "shared-a3",
                "input",
                "p0",
                "shared-r3",
                "shared",
                b"changed",
                "bytes/v2",
                "shared-l3",
            )
        oracle = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(oracle.returncode, 0, oracle.stdout + oracle.stderr)

    async def test_metadata_horizon_stays_bounded_for_100_cycles(self) -> None:
        await self.store.create_partition(
            "cycle",
            "p0",
            max_records=1,
            max_bytes=100,
            max_metadata_records=1,
            max_producers=1,
        )
        await self.store.register_producer("cycle", "p0", "cycle-producer")
        await self.store.create_consumer("cycle-consumer", "cycle", "p0")
        epoch = await self.store.claim_consumer(
            "cycle-consumer", "worker", lease_seconds=300
        )
        for number in range(1, 101):
            payload = f"cycle-{number}".encode()
            result = await self.store.append(
                f"cycle-attempt-{number}",
                "cycle",
                "p0",
                f"cycle-record-{number}",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                f"cycle-lineage-{number}",
                producer_id="cycle-producer",
                producer_sequence=number,
            )
            self.assertEqual(result.sequence, number)
            token, _ = await self.store.deliver_next("cycle-consumer", "worker", epoch)
            await self.store.complete(
                token,
                "RETURN",
                managed_state={"last": number},
                state_schema="cycle/v1",
                state_version=1,
            )
            await self.store.compact("cycle", "p0", number)
            cuts = [("cycle", "p0", number)]
            retired = await self.store.compact_metadata(
                cuts, operation_id=metadata_operation_id(cuts)
            )
            self.assertEqual(
                retired["locations"]["cycle/p0"]["retired_through"], number
            )
            with self.assertRaises(IdempotencyHorizonExpired):
                await self.store.append(
                    f"cycle-attempt-{number}",
                    "cycle",
                    "p0",
                    f"cycle-record-{number}",
                    content_object_key(payload, "bytes/v1"),
                    payload,
                    "bytes/v1",
                    f"cycle-lineage-{number}",
                    producer_id="cycle-producer",
                    producer_sequence=number,
                )
            pressure = await self.store.pressure_snapshot("cycle", "p0")
            self.assertEqual(pressure["used_metadata_records"], 0)
            self.assertLessEqual(pressure["hot_metadata_rows_total"], 4)
        position = await self.store.position("cycle-consumer")
        self.assertEqual(position["completion_retired_through"], 99)
        async with self.store.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM append_attempts"), 0
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM turn_completions"), 1
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM metadata_summaries"), 1
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM metadata_operations"), 1
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM object_identities"), 0
            )
        oracle = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(oracle.returncode, 0, oracle.stdout + oracle.stderr)

    async def test_metadata_operation_retry_rejects_cut_and_partition_drift(
        self,
    ) -> None:
        for partition in ("p0", "p1"):
            await self.store.create_partition(
                "ops",
                partition,
                max_records=3,
                max_bytes=1000,
                max_metadata_records=3,
                max_producers=1,
            )
            await self.store.register_producer(
                "ops", partition, f"producer-{partition}"
            )
            payload = f"{partition}-one".encode()
            await self.store.append(
                f"attempt-{partition}-1",
                "ops",
                partition,
                f"record-{partition}-1",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                f"lineage-{partition}-1",
                producer_id=f"producer-{partition}",
                producer_sequence=1,
            )
            await self.store.compact("ops", partition, 1)
        first_cuts = [("ops", "p0", 1)]
        operation_id = metadata_operation_id(first_cuts)
        first = await self.store.compact_metadata(first_cuts, operation_id=operation_id)
        retried = await self.store.compact_metadata(
            first_cuts, operation_id=operation_id
        )
        self.assertFalse(first["idempotent_retry"])
        self.assertTrue(retried["idempotent_retry"])
        self.assertEqual(first["locations"], retried["locations"])

        payload = b"p0-two"
        await self.store.append(
            "attempt-p0-2",
            "ops",
            "p0",
            "record-p0-2",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            "lineage-p0-2",
            producer_id="producer-p0",
            producer_sequence=2,
        )
        await self.store.compact("ops", "p0", 2)
        with self.assertRaisesRegex(PostgresProtocolError, "operation identity drift"):
            await self.store.compact_metadata(
                [("ops", "p0", 2)], operation_id=operation_id
            )
        with self.assertRaisesRegex(PostgresProtocolError, "operation identity drift"):
            await self.store.compact_metadata(
                [("ops", "p0", 1), ("ops", "p1", 1)],
                operation_id=operation_id,
            )
        async with self.store.pool.acquire() as conn:
            summary = await conn.fetchrow(
                "SELECT retired_through FROM metadata_summaries "
                "WHERE channel='ops' AND partition_id='p0'"
            )
            self.assertEqual(summary["retired_through"], 1)
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM compaction_tombstones "
                    "WHERE channel='ops' AND partition_id='p0' AND sequence=2"
                ),
                1,
            )
        oracle = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(oracle.returncode, 0, oracle.stdout + oracle.stderr)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                """UPDATE metadata_operations
                   SET outcome=jsonb_set(outcome,'{locations,ops/p0,retired_through}',
                                         '999'::jsonb)
                   WHERE operation_id=$1""",
                operation_id,
            )
        corrupted = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(corrupted.returncode, 0, corrupted.stdout)
        self.assertIn("metadata_operation_outcome_cut", corrupted.stdout)

    async def test_structured_producer_retry_gap_race_and_expiry(self) -> None:
        await self.store.create_partition(
            "structured",
            "p0",
            max_records=4,
            max_bytes=1000,
            max_metadata_records=4,
            max_producers=1,
        )
        await self.store.register_producer(
            "structured", "p0", "deployment-v1/producer-1"
        )
        with self.assertRaises(CapacityBlocked):
            await self.store.register_producer(
                "structured", "p0", "deployment-v1/producer-2"
            )
        gap_payload = b"gap"
        with self.assertRaisesRegex(PostgresProtocolError, "sequence gap"):
            await self.store.append(
                "structured-gap",
                "structured",
                "p0",
                "structured-gap-r",
                content_object_key(gap_payload, "bytes/v1"),
                gap_payload,
                "bytes/v1",
                "structured-gap-l",
                producer_id="deployment-v1/producer-1",
                producer_sequence=2,
            )
        first_payload = b"first"
        first_args = (
            "structured-a1",
            "structured",
            "p0",
            "structured-r1",
            content_object_key(first_payload, "bytes/v1"),
            first_payload,
            "bytes/v1",
            "structured-l1",
        )
        first = await self.store.append(
            *first_args, producer_id="deployment-v1/producer-1", producer_sequence=1
        )
        self.assertEqual(
            await self.store.append(
                *first_args, producer_id="deployment-v1/producer-1", producer_sequence=1
            ),
            first,
        )
        with self.assertRaisesRegex(PostgresProtocolError, "identity drift"):
            await self.store.append(
                "structured-a1",
                "structured",
                "p0",
                "structured-r1",
                content_object_key(b"drift", "bytes/v1"),
                b"drift",
                "bytes/v1",
                "structured-l1",
                producer_id="deployment-v1/producer-1",
                producer_sequence=1,
            )

        async def contender(label: str, payload: bytes):
            try:
                return await self.store.append(
                    f"structured-{label}",
                    "structured",
                    "p0",
                    f"structured-r-{label}",
                    content_object_key(payload, "bytes/v1"),
                    payload,
                    "bytes/v1",
                    f"structured-l-{label}",
                    producer_id="deployment-v1/producer-1",
                    producer_sequence=2,
                )
            except PostgresProtocolError as exc:
                return exc

        contenders = await asyncio.gather(
            contender("left", b"left"), contender("right", b"right")
        )
        winners = [value for value in contenders if not isinstance(value, Exception)]
        self.assertEqual(len(winners), 1)
        winner = winners[0]
        await self.store.compact("structured", "p0", 2)
        winner_attempt = (
            "structured-left"
            if winner.record_id.endswith("left")
            else "structured-right"
        )
        winner_payload = b"left" if winner_attempt.endswith("left") else b"right"
        winner_args = (
            winner_attempt,
            "structured",
            "p0",
            winner.record_id,
            content_object_key(winner_payload, "bytes/v1"),
            winner_payload,
            "bytes/v1",
            "structured-l-left"
            if winner_attempt.endswith("left")
            else "structured-l-right",
        )

        async def retry_winner():
            try:
                return await self.store.append(
                    *winner_args,
                    producer_id="deployment-v1/producer-1",
                    producer_sequence=2,
                )
            except IdempotencyHorizonExpired as exc:
                return exc

        retirement_cuts = [("structured", "p0", 2)]
        retry_result, retirement = await asyncio.gather(
            retry_winner(),
            self.store.compact_metadata(
                retirement_cuts,
                operation_id=metadata_operation_id(retirement_cuts),
            ),
        )
        self.assertTrue(
            isinstance(retry_result, (AppendResult, IdempotencyHorizonExpired))
        )
        self.assertEqual(retirement["locations"]["structured/p0"]["retired_through"], 2)
        with self.assertRaises(IdempotencyHorizonExpired):
            await self.store.append(
                *winner_args,
                producer_id="deployment-v1/producer-1",
                producer_sequence=2,
            )
        with self.assertRaises(IdempotencyHorizonExpired):
            await self.store.append(
                winner_attempt,
                "structured",
                "p0",
                winner.record_id,
                content_object_key(b"expired-drift", "bytes/v1"),
                b"expired-drift",
                "bytes/v1",
                winner_args[7],
                producer_id="deployment-v1/producer-1",
                producer_sequence=2,
            )

    async def test_oracle_binds_retired_watermarks_to_operation_anchors(self) -> None:
        await self.store.create_partition(
            "no-summary",
            "p0",
            max_records=1,
            max_bytes=100,
            max_metadata_records=1,
            max_producers=1,
        )
        await self.store.register_producer("no-summary", "p0", "no-summary-producer")
        await self.store.create_consumer("no-summary-consumer", "no-summary", "p0")
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE producer_progress SET retired_through=1 "
                "WHERE producer_id='no-summary-producer'"
            )
            await conn.execute(
                "UPDATE consumers SET completion_retired_through=1 "
                "WHERE consumer_id='no-summary-consumer'"
            )
        missing_summary = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(missing_summary.returncode, 0, missing_summary.stdout)
        self.assertIn("retired_without_summary", missing_summary.stdout)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE producer_progress SET retired_through=0 "
                "WHERE producer_id='no-summary-producer'"
            )
            await conn.execute(
                "UPDATE consumers SET completion_retired_through=0 "
                "WHERE consumer_id='no-summary-consumer'"
            )

        await self.store.create_partition(
            "anchors",
            "p0",
            max_records=3,
            max_bytes=1000,
            max_metadata_records=3,
            max_producers=1,
        )
        await self.store.register_producer("anchors", "p0", "anchor-producer")
        for number in range(1, 4):
            payload = f"anchor-{number}".encode()
            await self.store.append(
                f"anchor-a-{number}",
                "anchors",
                "p0",
                f"anchor-r-{number}",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                f"anchor-l-{number}",
                producer_id="anchor-producer",
                producer_sequence=number,
            )
        for consumer_id in ("anchor-left", "anchor-right"):
            await self.store.create_consumer(consumer_id, "anchors", "p0")
            epoch = await self.store.claim_consumer(
                consumer_id, consumer_id, lease_seconds=60
            )
            delivered = await self.store.deliver_batch(
                consumer_id, consumer_id, epoch, max_records=3
            )
            await self.store.complete_return_batch(delivered)
        await self.store.compact("anchors", "p0", 2)
        cuts = [("anchors", "p0", 2)]
        await self.store.compact_metadata(
            cuts, operation_id=metadata_operation_id(cuts)
        )
        valid = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stdout + valid.stderr)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE consumers SET completion_retired_through=1 "
                "WHERE consumer_id='anchor-left'"
            )
        corrupted = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(corrupted.returncode, 0, corrupted.stdout)
        self.assertIn("metadata_operation_anchors", corrupted.stdout)

    async def test_oracle_rejects_missing_hot_append_authority(self) -> None:
        await self.store.create_partition(
            "hot-authority",
            "p0",
            max_records=1,
            max_bytes=100,
            max_metadata_records=1,
            max_producers=1,
        )
        await self.store.register_producer("hot-authority", "p0", "hot-producer")
        payload = b"hot"
        await self.store.append(
            "hot-attempt",
            "hot-authority",
            "p0",
            "hot-record",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            "hot-lineage",
            producer_id="hot-producer",
            producer_sequence=1,
        )
        valid = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(valid.returncode, 0, valid.stdout + valid.stderr)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM append_attempts WHERE attempt_id='hot-attempt'"
            )
            await conn.execute(
                "UPDATE partitions SET used_metadata_records=0 "
                "WHERE channel='hot-authority' AND partition_id='p0'"
            )
        corrupted = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(corrupted.returncode, 0, corrupted.stdout)
        self.assertIn("producer_hot_attempts", corrupted.stdout)
        self.assertIn("record_append_binding", corrupted.stdout)

    async def test_oracle_recomputes_live_content_address(self) -> None:
        await self.store.create_partition(
            "content",
            "p0",
            max_records=1,
            max_bytes=100,
            max_metadata_records=1,
            max_producers=1,
        )
        await self.store.register_producer("content", "p0", "content-producer")
        original = b"x"
        key = content_object_key(original, "bytes/v1")
        await self.store.append(
            "content-attempt",
            "content",
            "p0",
            "content-record",
            key,
            original,
            "bytes/v1",
            "content-lineage",
            producer_id="content-producer",
            producer_sequence=1,
        )
        replacement = b"y"
        digest = hashlib.sha256(replacement).hexdigest()
        identity = {
            "attempt_id": "content-attempt",
            "channel": "content",
            "partition": "p0",
            "record_id": "content-record",
            "object_key": key,
            "payload_digest": digest,
            "payload_size": len(replacement),
            "schema_id": "bytes/v1",
            "lineage_digest": "content-lineage",
            "producer_id": "content-producer",
            "producer_sequence": 1,
        }
        identity_hash = hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        async with self.store.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "UPDATE objects SET payload=$2,payload_digest=$3,payload_size=$4 "
                "WHERE object_key=$1",
                key,
                replacement,
                digest,
                len(replacement),
            )
            await conn.execute(
                "UPDATE object_identities SET payload_digest=$2,payload_size=$3 "
                "WHERE object_key=$1",
                key,
                digest,
                len(replacement),
            )
            await conn.execute(
                "UPDATE records SET payload_digest=$2,payload_size=$3 "
                "WHERE object_key=$1",
                key,
                digest,
                len(replacement),
            )
            await conn.execute(
                """UPDATE append_attempts
                   SET payload_digest=$1,payload_size=$2,identity_digest=$3
                   WHERE attempt_id='content-attempt'""",
                digest,
                len(replacement),
                identity_hash,
            )
        corrupted = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertNotEqual(corrupted.returncode, 0, corrupted.stdout)
        self.assertIn("content_address", corrupted.stdout)

    async def test_reference_safe_compaction_returns_credits_and_retry_witness(
        self,
    ) -> None:
        await self.store.create_partition("input", "p0", max_records=5, max_bytes=50)
        originals = []
        for number in range(1, 6):
            originals.append(
                await self.store.append(
                    f"a-{number}",
                    "input",
                    "p0",
                    f"r-{number}",
                    f"o-{number}",
                    str(number).encode().ljust(10, b"x"),
                    "bytes/v1",
                    f"l-{number}",
                )
            )
        await self.store.create_consumer("late", "input", "p0", start_sequence=3)
        await self.store.seal_checkpoint("cp-4", "input", "p0", 3)
        await self.store.add_ref("RECOVERY_PIN", "pin-2", "input", "p0", 2)
        first = await self.store.compact("input", "p0", 5)
        self.assertEqual(first["safe_through"], 1)
        self.assertEqual(first["sequences"], [1])
        retried = await self.store.append(
            "a-1",
            "input",
            "p0",
            "r-1",
            "o-1",
            b"1xxxxxxxxx",
            "bytes/v1",
            "l-1",
        )
        self.assertEqual(retried, originals[0])
        self.assertEqual(await self.store.retained_floor("input", "p0"), 2)
        with self.assertRaisesRegex(PostgresProtocolError, "outside retained suffix"):
            await self.store.create_consumer(
                "too-late", "input", "p0", start_sequence=1
            )
        with self.assertRaisesRegex(PostgresProtocolError, "outside retained suffix"):
            await self.store.add_ref("RECOVERY_PIN", "late-pin", "input", "p0", 1)
        await self.store.release_ref("RECOVERY_PIN", "pin-2")
        await self.store.release_checkpoint("cp-4")
        second = await self.store.compact("input", "p0", 5)
        self.assertEqual(second["safe_through"], 2)
        self.assertEqual(second["sequences"], [2])
        appended = await self.store.append(
            "a-6",
            "input",
            "p0",
            "r-6",
            "o-6",
            b"6xxxxxxxxx",
            "bytes/v1",
            "l-6",
        )
        self.assertEqual(appended.sequence, 6)
        self.assertTrue((await self.store.invariant_report())["passed"])

    async def test_root_creation_serializes_with_compaction(self) -> None:
        await self.store.create_partition("input", "p0", max_records=2, max_bytes=100)
        for number in (1, 2):
            await self.store.append(
                f"a-{number}",
                "input",
                "p0",
                f"r-{number}",
                f"o-{number}",
                str(number).encode(),
                "bytes/v1",
                f"l-{number}",
            )
        roots_read = asyncio.Event()
        allow_delete = asyncio.Event()

        async def hook() -> None:
            roots_read.set()
            await allow_delete.wait()

        compaction = asyncio.create_task(
            self.store.compact("input", "p0", 2, after_root_snapshot=hook)
        )
        await roots_read.wait()
        late_root = asyncio.create_task(
            self.store.create_consumer("late", "input", "p0", start_sequence=1)
        )
        await asyncio.sleep(0)
        self.assertFalse(late_root.done())
        allow_delete.set()
        result = await compaction
        self.assertEqual(result["sequences"], [1, 2])
        with self.assertRaisesRegex(PostgresProtocolError, "outside retained suffix"):
            await late_root
        self.assertTrue((await self.store.invariant_report())["passed"])

    async def test_independent_oracle_rejects_late_consumer_projection(self) -> None:
        await self.store.create_partition("input", "p0", max_records=2, max_bytes=100)
        for number in (1, 2):
            await self.store.append(
                f"a-{number}",
                "input",
                "p0",
                f"r-{number}",
                f"o-{number}",
                str(number).encode(),
                "bytes/v1",
                f"l-{number}",
            )
        await self.store.compact("input", "p0", 2)
        good = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(good.returncode, 0, good.stdout + good.stderr)
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO consumers(
                   consumer_id,channel,partition_id,start_sequence,completed_through)
                   VALUES('forged-late','input','p0',1,0)"""
            )
        bad = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(bad.returncode, 2)
        self.assertIn(
            "consumer_retained_floor:forged-late",
            json.loads(bad.stdout)["violations"],
        )

    async def test_independent_oracle_rejects_lost_active_roots(self) -> None:
        await self.store.create_partition("input", "p0", max_records=2, max_bytes=100)
        await self.store.append(
            "root-a1",
            "input",
            "p0",
            "root-r1",
            "root-o1",
            b"one",
            "bytes/v1",
            "root-l1",
        )
        await self.store.add_ref("RECOVERY_PIN", "root-pin", "input", "p0", 1)
        await self.store.seal_checkpoint("root-checkpoint", "input", "p0", 0)
        async with self.store.pool.acquire() as conn, conn.transaction():
            manifest = await conn.fetchval(
                """INSERT INTO compaction_manifests(
                   channel,partition_id,requested_through,safe_through,roots,sequences,status)
                   VALUES('input','p0',1,1,'[]'::jsonb,'[1]'::jsonb,'COMPLETED')
                   RETURNING manifest_id"""
            )
            await conn.execute(
                """INSERT INTO compaction_tombstones
                   SELECT channel,partition_id,sequence,$1,record_id,object_key,
                          payload_digest,payload_size,schema_id,lineage_digest
                   FROM records WHERE channel='input' AND partition_id='p0' AND sequence=1""",
                manifest,
            )
            await conn.execute(
                "DELETE FROM records WHERE channel='input' AND partition_id='p0' AND sequence=1"
            )
            await conn.execute("DELETE FROM objects WHERE object_key='root-o1'")
            await conn.execute(
                "UPDATE partitions SET used_records=0,used_bytes=0 "
                "WHERE channel='input' AND partition_id='p0'"
            )
        bad = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(bad.returncode, 2)
        violations = json.loads(bad.stdout)["violations"]
        self.assertIn("durable_ref_not_live:RECOVERY_PIN:root-pin", violations)
        self.assertIn("checkpoint_not_restorable:root-checkpoint", violations)
        self.assertIn("retained_root_loss:input:p0:1", violations)

    async def test_independent_oracle_rejects_cross_linked_yield_output(self) -> None:
        await self.store.create_partition("input", "p0", max_records=2, max_bytes=100)
        await self.store.create_partition("output", "p0", max_records=2, max_bytes=100)
        for number in (1, 2):
            await self.store.append(
                f"cross-a{number}",
                "input",
                "p0",
                f"cross-r{number}",
                f"cross-o{number}",
                str(number).encode(),
                "bytes/v1",
                f"cross-l{number}",
            )
        await self.store.create_consumer("cross-consumer", "input", "p0")
        epoch = await self.store.claim_consumer(
            "cross-consumer", "anna", lease_seconds=60
        )
        for number in (1, 2):
            token, _ = await self.store.deliver_next("cross-consumer", "anna", epoch)
            await self.store.complete(
                token,
                "YIELD",
                output=Output(
                    "output",
                    "p0",
                    f"cross-out-r{number}",
                    f"cross-out-o{number}",
                    f"out-{number}".encode(),
                    "bytes/v1",
                    f"cross-out-l{number}",
                ),
                managed_state={"last": number},
            )
        async with self.store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE turn_completions SET output_sequence=2 "
                "WHERE consumer_id='cross-consumer' AND input_sequence=1"
            )
        bad = subprocess.run(
            [sys.executable, str(ORACLE), DSN, self.schema],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(bad.returncode, 2)
        self.assertIn(
            "completion_output_binding:cross-consumer:1",
            json.loads(bad.stdout)["violations"],
        )


if __name__ == "__main__":
    unittest.main()
