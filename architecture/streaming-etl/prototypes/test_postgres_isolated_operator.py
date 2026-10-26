from __future__ import annotations

import asyncio
import base64
import os
import subprocess
import unittest
import uuid
from pathlib import Path

import test_postgres_prefect_adapter as adapter_fixtures
from postgres_isolated_operator import (
    IsolatedOperatorProtocolError,
    IsolatedOperatorSupervisor,
    selected_profile_limitations,
)
from postgres_stream_v2 import StaleOwner
from prefect_stream_adapter import UPPERCASE_ARTIFACT, UPPERCASE_DIGEST

HERE = Path(__file__).resolve().parent
WORKER = HERE / "isolated_operator_worker.py"


class PostgresIsolatedOperatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = adapter_fixtures.PostgresPrefectAttemptTests(
            methodName="test_attempt_takeover_fences_source_and_inflight_terminal"
        )
        await self.fixture.asyncSetUp()
        self.image = None
        self.supervisor = None

    async def asyncTearDown(self):
        if self.supervisor is not None:
            await asyncio.to_thread(self.supervisor.close, force=True)
        if self.image is not None:
            env = os.environ.copy()
            for key in (
                "DOCKER_HOST",
                "TESTCONTAINERS_HOST_OVERRIDE",
                "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE",
            ):
                env.pop(key, None)
            await asyncio.to_thread(
                subprocess.run,
                ["docker", "image", "rm", "--force", self.image],
                env=env,
                check=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        await self.fixture.asyncTearDown()

    async def _start(self, attempt):
        try:
            self.image, worker_digest = await asyncio.to_thread(
                IsolatedOperatorSupervisor.build_image,
                WORKER,
                artifact_id=UPPERCASE_ARTIFACT,
                artifact_digest=UPPERCASE_DIGEST,
            )
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            self.skipTest(f"Docker isolated-worker profile unavailable: {exc}")
        self.supervisor = IsolatedOperatorSupervisor(
            artifact_id=UPPERCASE_ARTIFACT,
            artifact_digest=UPPERCASE_DIGEST,
            image=self.image,
            worker_digest=worker_digest,
            container_name="ariadne-isolated-" + uuid.uuid4().hex[:12],
            max_payload_bytes=1024,
        )
        await self.supervisor.bind_authority(
            self.fixture.attempts,
            self.fixture.admin_token,
            control_run_id=self.fixture.run_id,
            attempt_id=attempt.attempt_id,
            participant_id="c",
            revision=self.fixture.revision,
        )
        await asyncio.to_thread(self.supervisor.start)

    async def test_closed_network_worker_cannot_observe_credentials_or_write_platform_db(
        self,
    ):
        attempt = await self.fixture.activate()
        await self._start(attempt)
        probe = await asyncio.to_thread(self.supervisor.probe)
        self.assertEqual(probe["secret_environment_keys"], [])
        self.assertFalse(probe["docker_socket"])
        self.assertFalse(probe["host_proc_mounted"])
        self.assertFalse(probe["platform_pg_reachable"])
        self.assertEqual(probe["pid1"], "python -I /app/worker.py ")
        boundary = await asyncio.to_thread(self.supervisor.inspect_boundary)
        self.assertEqual(boundary["network_mode"], "none")
        self.assertTrue(boundary["readonly_rootfs"])
        self.assertEqual(boundary["cap_drop"], ["ALL"])
        self.assertIn("no-new-privileges", boundary["security_opt"])
        self.assertIsNone(boundary["binds"])
        self.assertEqual(boundary["pid_mode"], "")
        self.assertEqual(boundary["pids_limit"], 32)
        self.assertEqual(boundary["memory"], 64 * 1024 * 1024)
        self.assertEqual(boundary["nano_cpus"], 500_000_000)
        self.assertEqual(boundary["tmpfs"], {"/tmp": "rw,noexec,nosuid,nodev,size=1m"})
        self.assertIn("closed network", " ".join(selected_profile_limitations()))

        async with self.fixture.stream.pool.acquire() as conn:
            before = await conn.fetchval(
                "SELECT count(*) FROM join_turns_v3 WHERE revision_id=$1",
                self.fixture.revision.revision_id,
            )
        # The worker has no database protocol or credentials.  A probe and an
        # arbitrary execution RPC cannot advance authority; only runtime.step can.
        result = await asyncio.to_thread(
            self.supervisor.execute,
            participant_id="c",
            input_key="input/p0",
            input_sequence=999,
            payload=b"probe",
            managed_state={},
        )
        self.assertEqual(result.payload, b"PROBE")
        async with self.fixture.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM join_turns_v3 WHERE revision_id=$1",
                    self.fixture.revision.revision_id,
                ),
                before,
            )

    async def test_platform_validates_result_and_owns_atomic_cursor(self):
        attempt = await self.fixture.activate()
        await self.fixture.append("source-cap-1", 1)
        await self._start(attempt)
        projection = await self.fixture.attempts.project_attempt(
            self.fixture.admin_token, control_run_id=self.fixture.run_id
        )
        binding = next(
            item for item in projection["operators"] if item["participant_id"] == "c"
        )
        task = self.supervisor.task()
        worker_pid = self.supervisor.worker_pid

        def lose_platform_before_commit(point):
            if point == "terminal_before_commit":
                raise RuntimeError("platform process loss before terminal commit")

        with self.assertRaisesRegex(RuntimeError, "platform process loss"):
            await self.fixture.runtimes["c"].step(
                "input/p0",
                task,
                binding["owner_id"],
                binding["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.fixture.coordinator_epoch,
                fault=lose_platform_before_commit,
            )
        async with self.fixture.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT completed FROM join_inputs_v3 WHERE revision_id=$1 "
                    "AND participant_id='c'",
                    self.fixture.revision.revision_id,
                ),
                0,
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM records WHERE channel='output-c'"
                ),
                0,
            )

        self.assertEqual(
            await self.fixture.runtimes["c"].step(
                "input/p0",
                task,
                binding["owner_id"],
                binding["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.fixture.coordinator_epoch,
            ),
            "DRAINED",
        )
        self.assertEqual(self.supervisor.worker_pid, worker_pid)
        async with self.fixture.stream.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT r.sequence,o.payload FROM records r JOIN objects o USING(object_key) "
                "WHERE r.channel='output-c' AND r.partition_id='p0'"
            )
            self.assertEqual(
                (row["sequence"], bytes(row["payload"])), (1, b"DOCUMENT-1")
            )
            self.assertEqual(
                await conn.fetchval(
                    "SELECT completed FROM join_inputs_v3 WHERE revision_id=$1 "
                    "AND participant_id='c'",
                    self.fixture.revision.revision_id,
                ),
                1,
            )

        request_id = "a" * 64
        forged = {
            "type": "result",
            "request_id": request_id,
            "artifact_id": UPPERCASE_ARTIFACT,
            "artifact_digest": "0" * 64,
            "terminal": "YIELD",
            "payload_b64": base64.b64encode(b"forged").decode(),
            "schema_id": "bytes/v1",
            "managed_state": {},
        }
        with self.assertRaisesRegex(
            IsolatedOperatorProtocolError, "result identity drift"
        ):
            self.supervisor._validate_result(forged, request_id)
        forged["artifact_digest"] = UPPERCASE_DIGEST
        forged["managed_state"] = {"oversized": "x" * 256_001}
        with self.assertRaisesRegex(
            IsolatedOperatorProtocolError, "state exceeds contract"
        ):
            self.supervisor._validate_result(forged, request_id)

        second = await self.fixture.replace(attempt, 2)
        self.assertEqual(second.ordinal, 2)
        await self.fixture.append("source-cap-2", 2)
        with self.assertRaises(StaleOwner):
            await self.fixture.runtimes["c"].step(
                "input/p0",
                task,
                binding["owner_id"],
                binding["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=self.fixture.coordinator_epoch,
            )
        async with self.fixture.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM join_turns_v3 WHERE revision_id=$1 "
                    "AND participant_id='c'",
                    self.fixture.revision.revision_id,
                ),
                1,
            )


if __name__ == "__main__":
    unittest.main()
