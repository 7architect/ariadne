from __future__ import annotations

import hashlib
import json
import secrets
import unittest

import test_postgres_streaming_retention as streaming_fixture
from postgres_control_plane import PostgresControlPlane
from postgres_control_plane_client import ControlHttpClient, ControlHttpError
from postgres_control_plane_oracle import evaluate
from postgres_control_plane_server import JsonHttpServer
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_storage_footprint import evaluate as evaluate_footprint
from postgres_stream_v2 import content_object_key


class PostgresControlStreamingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = streaming_fixture.PostgresStreamingRetentionTests("runTest")
        await self.fixture.asyncSetUp()
        self.addAsyncCleanup(self.fixture.asyncTearDown)
        self.protocol = self.fixture.base.fixture
        planner = await PostgresRecoveryRegionPlanner.install(self.protocol.coordinator)
        regional = await PostgresRegionalRecovery.install(
            self.protocol.coordinator, planner, self.protocol.revision
        )
        await regional.bind_revision_topology()
        self.control = await PostgresControlPlane.install(self.protocol.store)
        self.token = secrets.token_urlsafe(32)
        await self.control.provision_tenant(
            "streaming-tenant",
            self.token,
            {"deploy", "run", "read", "control", "source"},
        )
        self.server = JsonHttpServer(self.control)
        port = await self.server.start()
        self.addAsyncCleanup(self.server.close)
        self.client = ControlHttpClient(f"http://127.0.0.1:{port}", self.token)
        await self.client.__aenter__()
        self.addAsyncCleanup(self.client.__aexit__, None, None, None)
        await self.client.deploy(
            deployment_id="d1",
            revision_id="r1",
            graph_version="v1",
            graph_digest=self.protocol.revision.graph_digest,
        )
        self.leader = await self.client.claim_leader("d1", "http-controller", 300)
        self.lease = {key: self.leader[key] for key in ("leader_id", "leader_epoch")}
        run = await self.client.create_run(
            deployment_id="d1",
            request_id="http-streaming-run",
            consumer_ids=[],
            max_admission_records=4,
            max_admission_bytes=128,
            **self.lease,
        )
        self.run_id = run["run_id"]

    @staticmethod
    def _source(number: int):
        payload = f"document-{number}".encode()
        return payload, {
            "request_id": f"admission-{number}",
            "channel": "input",
            "partition": "p0",
            "record_id": f"source-record-{number}",
            "attempt_id": f"source-attempt-{number}",
            "object_key": content_object_key(payload, "bytes/v1"),
            "payload_digest": hashlib.sha256(payload).hexdigest(),
            "payload_size": len(payload),
            "schema_id": "bytes/v1",
            "lineage_digest": f"source-lineage-{number}",
            "producer_id": "source-producer:r1",
            "producer_sequence": number,
        }

    async def test_one_hundred_http_cycles_return_control_and_data_credits(self):
        samples = []
        for number in range(1, 101):
            payload, identity = self._source(number)
            admission = await self.client.admit_source(self.run_id, **identity)
            publication = await self.client.publish_source(
                self.run_id, admission["admission_id"], payload
            )
            self.assertEqual(
                (publication["status"], publication["sequence"]), ("PUBLISHED", number)
            )
            # The fixture repeats the exact source append to exercise a lost-ACK
            # retry, then processes the input and creates the graph checkpoint.
            await self.fixture._cycle(number)
            await self.fixture.base.join_retention.retire_latest(
                "r1", "join", f"join-retire-{number}"
            )
            if number == 1:
                continue
            await self.fixture.base.retention.retire_prefix(
                *self.protocol._coordinator_args(), f"checkpoint-retire-{number - 1}"
            )
            await self.fixture.retention.compact_latest(
                *self.protocol._coordinator_args(), f"stream-retire-{number}"
            )
            await self.client.retire_admissions(self.run_id, **self.lease)
            async with self.protocol.store.pool.acquire() as conn:
                run = await conn.fetchrow(
                    "SELECT * FROM control_runs_v1 WHERE run_id=$1", self.run_id
                )
                self.assertEqual(
                    (run["used_admission_records"], run["used_admission_bytes"]), (0, 0)
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM control_source_admissions_v1"
                    ),
                    0,
                )
            if number in (25, 50, 75, 100):
                census = await evaluate_footprint(
                    streaming_fixture.join_fixture.DSN, self.protocol.schema
                )
                self.assertLessEqual(census.total_rows, 512)
                self.assertLessEqual(census.total_json_bytes, 256 * 1024)
                samples.append(
                    {
                        "cycle": number,
                        "rows": census.total_rows,
                        "json_bytes": census.total_json_bytes,
                        "table_rows": {item.table: item.rows for item in census.tables},
                    }
                )
        self.assertEqual(
            [sample["table_rows"] for sample in samples], [samples[0]["table_rows"]] * 4
        )
        payload, old = self._source(1)
        old["request_id"] = "expired-admission-retry"
        with self.assertRaises(ControlHttpError) as rejected:
            await self.client.admit_source(self.run_id, **old)
        self.assertEqual(rejected.exception.status_code, 409)
        projected = await self.client.project(self.run_id)
        report = await evaluate(
            streaming_fixture.join_fixture.DSN, self.protocol.schema, projected
        )
        self.assertTrue(report["passed"], report)
        await self.protocol._assert_oracles()
        self.assertEqual(projected["admission_pressure"]["used_records"], 0)
        print(
            "R4_MEASUREMENT_JSON="
            + json.dumps(
                {
                    "name": "control_http_100_cycles",
                    "samples": samples,
                    "logical_row_budget": 512,
                    "logical_byte_budget": 256 * 1024,
                    "scope": "logical rows/JSON bytes; excludes heap/index/WAL",
                },
                sort_keys=True,
            )
        )
