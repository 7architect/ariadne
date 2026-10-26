"""Actual socket reply loss and PostgreSQL connection termination controls."""

from __future__ import annotations

import asyncio
import hashlib
import subprocess
import unittest

import asyncpg
import httpx
import test_postgres_control_plane as fixtures
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from postgres_control_plane_client import ControlHttpClient
from postgres_control_plane_server import JsonHttpServer
from postgres_release_authority import PostgresReleaseAuthority, RuntimeContract
from postgres_release_oracle import evaluate as evaluate_release
from postgres_restore_contracts import public_key_b64
from postgres_restore_registry import database_scope
from postgres_schema_backup import DOCKER, assert_container_system
from postgres_stream_oracle import evaluate as evaluate_stream
from postgres_stream_v2 import Output, PostgresStream, content_object_key


class ReplyCutProxy:
    def __init__(self, port, mode):
        self.upstream_port, self.mode = port, mode
        self.cut_count = 0
        self.observed_response = False

    async def start(self):
        self.server = await asyncio.start_server(self.handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def close(self):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, reader, writer):
        upstream = None
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            fields = {}
            for line in headers.split(b"\r\n")[1:]:
                if b":" in line:
                    key, value = line.split(b":", 1)
                    fields[key.lower()] = value.strip()
            body = await reader.readexactly(int(fields.get(b"content-length", b"0")))
            publish = headers.split(b"\r\n", 1)[0].split()[1].endswith(b"/publish")
            cut = publish and self.cut_count == 0
            if cut and self.mode == "before_forward":
                self.cut_count += 1
                writer.transport.abort()
                return
            reply, upstream = await asyncio.open_connection(
                "127.0.0.1", self.upstream_port
            )
            upstream.write(headers + body)
            await upstream.drain()
            response = await reply.read()
            if cut:
                self.cut_count += 1
                self.observed_response = b"HTTP/1.1 200" in response
                writer.transport.abort()
                return
            writer.write(response)
            await writer.drain()
        finally:
            if upstream is not None:
                upstream.close()
                await upstream.wait_closed()
            writer.close()
            await writer.wait_closed()


class PostgresNetworkFaultTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.PostgresControlPlaneTests("runTest")
        await self.fixture.asyncSetUp()

    async def asyncTearDown(self):
        await self.fixture.asyncTearDown()

    async def test_socket_loss_before_and_after_publication_retries_exactly(self):
        f = self.fixture
        server = JsonHttpServer(f.control)
        port = await server.start()
        try:
            for number, mode in enumerate(("before_forward", "after_reply"), start=1):
                proxy = ReplyCutProxy(port, mode)
                proxy_port = await proxy.start()
                try:
                    async with ControlHttpClient(
                        f"http://127.0.0.1:{proxy_port}", f.token, timeout=3
                    ) as client:
                        payload = f"wire-document-{number}".encode()
                        admission = await client.admit_source(
                            f.run_id,
                            request_id=f"wire-{number}",
                            channel="input",
                            partition="p0",
                            record_id=f"wire-record-{number}",
                            attempt_id=f"wire-append-{number}",
                            object_key=content_object_key(payload, "bytes/v1"),
                            payload_digest=hashlib.sha256(payload).hexdigest(),
                            payload_size=len(payload),
                            schema_id="bytes/v1",
                            lineage_digest=f"wire-lineage-{number}",
                            producer_id="source-producer",
                            producer_sequence=number,
                        )
                        with self.assertRaises(httpx.RemoteProtocolError):
                            await client.publish_source(
                                f.run_id, admission["admission_id"], payload
                            )
                        async with f.stream.pool.acquire() as conn:
                            count = await conn.fetchval(
                                "SELECT count(*) FROM records WHERE channel='input'"
                            )
                        self.assertEqual(
                            count, number - 1 if mode == "before_forward" else number
                        )
                        await client.publish_source(
                            f.run_id, admission["admission_id"], payload
                        )
                        await client.publish_source(
                            f.run_id, admission["admission_id"], payload
                        )
                        self.assertEqual(proxy.cut_count, 1)
                        self.assertEqual(proxy.observed_response, mode == "after_reply")
                        await f._complete_one()
                finally:
                    await proxy.close()
            async with f.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel='input'"
                    ),
                    2,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT completed_through FROM consumers WHERE consumer_id='anna'"
                    ),
                    2,
                )
            await f._assert_oracle()
            report = await evaluate_stream(fixtures.DSN, f.schema)
            self.assertTrue(report["passed"], report)
        finally:
            await server.close()

    async def test_terminated_postgres_socket_rolls_back_whole_terminal_transaction(
        self,
    ):
        f = self.fixture
        await f._admit_append(1)
        scope = await database_scope(f.stream.pool, f.schema)
        await assert_container_system("ariadne-stream-postgres", scope)
        epoch = await f.stream.claim_consumer(
            "anna", "network-worker", lease_seconds=300
        )
        token, _ = await f.stream.deliver_next("anna", "network-worker", epoch)
        holder = "network-invocation"
        await f.stream.acquire_invocation(token, holder, lease_seconds=300)
        release = await PostgresReleaseAuthority.install(
            f.stream.pool,
            f.schema,
            schema_version="r4a",
            runtime_contracts=(RuntimeContract("network-r4a", "1" * 64, "2" * 64),),
            restore_public_key_b64=public_key_b64(Ed25519PrivateKey.generate()),
        )
        session = await release.open_session(
            runtime_version="network-r4a",
            actor_id="network-worker",
            role="EXECUTION",
            lease_seconds=300,
        )
        pool = await release.open_bound_pool(
            fixtures.DSN, session.session_id, min_size=1, max_size=1
        )
        try:
            async with pool.acquire() as conn:
                backend_pid = conn.get_server_pid()
            worker = PostgresStream(pool, f.schema)
            payload = b"signed-document-1"
            output = Output(
                "output",
                "p0",
                "signed-1",
                content_object_key(payload, "bytes/v1"),
                payload,
                "bytes/v1",
                "signed-lineage-1",
            )
            terminated = []

            def cut_socket(stage):
                if stage == "completion:after_progress_before_commit":
                    result = subprocess.run(
                        [
                            *DOCKER,
                            "exec",
                            "ariadne-stream-postgres",
                            "psql",
                            "-At",
                            "-U",
                            "ariadne",
                            "-d",
                            scope["database"],
                            "-c",
                            f"SELECT pg_terminate_backend({backend_pid}) FROM pg_stat_activity WHERE pid={backend_pid} AND state='idle in transaction'",
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    self.assertEqual(result.stdout.strip(), "t")
                    terminated.append(backend_pid)

            with self.assertRaises(
                (
                    asyncpg.PostgresConnectionError,
                    asyncpg.InterfaceError,
                    ConnectionError,
                )
            ):
                await worker.complete(
                    token,
                    "YIELD",
                    output=output,
                    managed_state={"completed": 1},
                    invocation_holder_id=holder,
                    fault=cut_socket,
                )
            self.assertEqual(terminated, [backend_pid])
            async with f.stream.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval("SELECT count(*) FROM turn_completions"), 0
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM records WHERE channel='output'"
                    ),
                    0,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT completed_through FROM consumers WHERE consumer_id='anna'"
                    ),
                    0,
                )
            first = await worker.complete(
                token,
                "YIELD",
                output=output,
                managed_state={"completed": 1},
                invocation_holder_id=holder,
            )
            replay = await worker.complete(
                token,
                "YIELD",
                output=output,
                managed_state={"completed": 1},
                invocation_holder_id=holder,
            )
            self.assertEqual((first, replay), (1, 1))
            for oracle in (evaluate_stream,):
                report = await oracle(fixtures.DSN, f.schema)
                self.assertTrue(report["passed"], report)
            report = await evaluate_release(fixtures.DSN, f.schema)
            self.assertTrue(report.passed, report.to_dict())
        finally:
            await pool.close()


if __name__ == "__main__":
    unittest.main()
