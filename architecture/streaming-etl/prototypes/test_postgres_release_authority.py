from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import unittest
from pathlib import Path

import asyncpg
import test_postgres_prefect_adapter as adapter_fixtures
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_control_plane import PostgresControlPlane
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_runtime import PostgresJoinRuntime
from postgres_prefect_attempts import PostgresPrefectAttempts
from postgres_release_authority import (
    PostgresReleaseAuthority,
    ReleaseCompatibilityError,
    ReleaseMigrationConflict,
    ReleaseProtocolError,
    ReleaseQuiesceBusy,
    RuntimeContract,
)
from postgres_release_oracle import evaluate as evaluate_release
from postgres_restore_contracts import key_id, public_key_b64
from postgres_stream_v2 import PostgresStream, content_object_key

DSN = adapter_fixtures.DSN
HERE = Path(__file__).resolve().parent


class PostgresReleaseAuthorityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.fixture = adapter_fixtures.PostgresPrefectAttemptTests("runTest")
        await self.fixture.asyncSetUp()
        if not hasattr(self.fixture, "stream"):
            self.skipTest("PostgreSQL streaming profile is not running")
        self.stream = self.fixture.stream
        self.schema = self.fixture.schema
        self.control = self.fixture.control
        self.old_conn = None
        self.bound_pools = []
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                """
                DROP TRIGGER IF EXISTS prefect_join_worker_guard ON join_workers;
                DROP TRIGGER IF EXISTS prefect_join_invocation_guard ON join_invocations_v3;
                DROP TRIGGER IF EXISTS prefect_join_turn_guard ON join_turns_v3;
                DROP TRIGGER IF EXISTS prefect_regional_start_guard
                  ON regional_recovery_workers_v1;
                DROP TABLE IF EXISTS prefect_attempt_heads_v1 CASCADE;
                DROP TABLE IF EXISTS prefect_attempt_source_caps_v1 CASCADE;
                DROP TABLE IF EXISTS prefect_attempt_operators_v1 CASCADE;
                DROP TABLE IF EXISTS prefect_execution_attempts_v1 CASCADE;
                DROP FUNCTION IF EXISTS prefect_attempt_identity_guard() CASCADE;
                DROP FUNCTION IF EXISTS prefect_attempt_delete_guard() CASCADE;
                DROP FUNCTION IF EXISTS prefect_source_cap_guard() CASCADE;
                DROP FUNCTION IF EXISTS prefect_join_worker_guard() CASCADE;
                DROP FUNCTION IF EXISTS prefect_join_terminal_guard() CASCADE;
                DROP FUNCTION IF EXISTS prefect_regional_start_guard() CASCADE;
                """
            )
        self.old_contract = RuntimeContract("control-r4a", "1" * 64, "a" * 64)
        self.new_contract = RuntimeContract("prefect-r4b", "2" * 64, "b" * 64)
        self.key = Ed25519PrivateKey.generate()
        self.release = await PostgresReleaseAuthority.install(
            self.stream.pool,
            self.schema,
            schema_version="r4a",
            runtime_contracts=(self.old_contract,),
            restore_public_key_b64=public_key_b64(self.key),
            max_hot_sessions=8,
        )
        self.old_session = await self.release.open_session(
            runtime_version="control-r4a",
            actor_id="old-controller",
            role="CONTROL",
            lease_seconds=300,
        )
        self.artifact_digest = hashlib.sha256(
            (HERE / "postgres_prefect_attempts.py").read_bytes()
        ).hexdigest()
        self.assertEqual(
            await self.release.open_session(
                runtime_version="control-r4a",
                actor_id="old-controller",
                role="CONTROL",
                lease_seconds=1,
            ),
            self.old_session,
        )

    async def asyncTearDown(self) -> None:
        if self.old_conn is not None:
            await self.stream.pool.release(self.old_conn)
            self.old_conn = None
        for pool in self.bound_pools:
            await pool.close()
        if hasattr(self, "fixture"):
            await self.fixture.asyncTearDown()

    async def _expand_and_cutover(self):
        attempts = PostgresPrefectAttempts.connect_existing(self.control)
        await self.release.expand_prefect_attempts(
            self.old_session.session_id,
            attempts,
            migration_id="r4a-to-r4b-attempt-authority",
            from_version="r4a",
            to_version="r4b",
            artifact_digest=self.artifact_digest,
        )
        cutover_fence = await self.release.cutover(
            self.old_session.session_id,
            migration_id="r4a-to-r4b-attempt-authority",
            target_contracts=(self.new_contract,),
        )
        # A lost response after the durable cutover must be readable by the same
        # now-fenced caller; it may not execute the cutover a second time.
        self.assertEqual(
            await self.release.cutover(
                self.old_session.session_id,
                migration_id="r4a-to-r4b-attempt-authority",
                target_contracts=(self.new_contract,),
            ),
            cutover_fence,
        )
        with self.assertRaisesRegex(ReleaseMigrationConflict, "drift"):
            await self.release.cutover(
                self.old_session.session_id,
                migration_id="r4a-to-r4b-attempt-authority",
                target_contracts=(RuntimeContract("other-r4b", "2" * 64, "b" * 64),),
            )
        session = await self.release.open_session(
            runtime_version="prefect-r4b",
            actor_id="prefect-controller",
            role="EXECUTION",
            lease_seconds=300,
        )
        pool = await self.release.open_bound_pool(DSN, session.session_id)
        self.bound_pools.append(pool)
        stream = PostgresStream(pool, self.schema)
        control = PostgresControlPlane.connect_existing(stream)
        return session, pool, stream, control

    async def test_real_attempt_migration_is_atomic_and_stale_connection_fenced(self):
        with self.assertRaisesRegex(ValueError, "compatibility capacity"):
            await PostgresReleaseAuthority.install(
                self.stream.pool,
                self.schema,
                schema_version="r4a",
                runtime_contracts=(
                    self.old_contract,
                    RuntimeContract("second-runtime", "1" * 64, "a" * 64),
                ),
                restore_public_key_b64=public_key_b64(self.key),
                max_compatibility_rows=1,
            )
        self.assertEqual(
            (
                await PostgresReleaseAuthority.install(
                    self.stream.pool,
                    self.schema,
                    schema_version="r4a",
                    runtime_contracts=(self.old_contract,),
                    restore_public_key_b64=public_key_b64(self.key),
                    max_hot_sessions=8,
                )
            ).schema,
            self.schema,
        )
        with self.assertRaisesRegex(ReleaseMigrationConflict, "install identity drift"):
            await PostgresReleaseAuthority.install(
                self.stream.pool,
                self.schema,
                schema_version="r4a",
                runtime_contracts=(
                    RuntimeContract("forged-runtime", "1" * 64, "a" * 64),
                ),
                restore_public_key_b64=public_key_b64(self.key),
                max_hot_sessions=8,
            )
        old_conn = await self.stream.pool.acquire()
        self.old_conn = old_conn
        await self.release.bind_connection(old_conn, self.old_session.session_id)
        await old_conn.execute(
            "UPDATE control_leaders_v1 SET lease_expires_at=lease_expires_at+interval '1 second'"
        )
        attempts = PostgresPrefectAttempts.connect_existing(self.control)

        def fail_after_ddl(stage: str) -> None:
            self.assertEqual(stage, "after_ddl_before_marker")
            raise RuntimeError("process lost during migration")

        with self.assertRaisesRegex(RuntimeError, "process lost"):
            await self.release.expand_prefect_attempts(
                self.old_session.session_id,
                attempts,
                migration_id="r4a-to-r4b-attempt-authority",
                from_version="r4a",
                to_version="r4b",
                artifact_digest=self.artifact_digest,
                fault=fail_after_ddl,
            )
        async with self.stream.pool.acquire() as conn:
            self.assertFalse(
                await conn.fetchval(
                    "SELECT to_regclass('prefect_execution_attempts_v1') IS NOT NULL"
                )
            )
            head = await conn.fetchrow("SELECT * FROM release_heads_v1")
            self.assertIsNone(head["pending_schema_version"])
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM release_migrations_v1"), 0
            )

        await self.release.expand_prefect_attempts(
            self.old_session.session_id,
            attempts,
            migration_id="r4a-to-r4b-attempt-authority",
            from_version="r4a",
            to_version="r4b",
            artifact_digest=self.artifact_digest,
        )
        # EXPAND preserves the old writer only while its semantics remain active.
        await old_conn.execute(
            "UPDATE control_leaders_v1 SET lease_expires_at=lease_expires_at+interval '1 second'"
        )
        with self.assertRaises(ReleaseMigrationConflict):
            await self.release.expand_prefect_attempts(
                self.old_session.session_id,
                attempts,
                migration_id="r4a-to-r4b-attempt-authority",
                from_version="r4a",
                to_version="r4b",
                artifact_digest="f" * 64,
            )

        await self.release.cutover(
            self.old_session.session_id,
            migration_id="r4a-to-r4b-attempt-authority",
            target_contracts=(self.new_contract,),
        )
        before = await old_conn.fetchval(
            "SELECT lease_expires_at FROM control_leaders_v1"
        )
        with self.assertRaisesRegex(asyncpg.RaiseError, "release session fenced"):
            await old_conn.execute(
                "UPDATE control_leaders_v1 SET lease_expires_at=lease_expires_at+interval '1 second'"
            )
        with self.assertRaisesRegex(asyncpg.RaiseError, "release"):
            await old_conn.execute(
                "UPDATE release_heads_v1 SET fence_epoch=fence_epoch+1"
            )
        with self.assertRaisesRegex(asyncpg.RaiseError, "migration"):
            await old_conn.execute(
                "UPDATE release_migrations_v1 SET artifact_digest=repeat('0',64)"
            )
        with self.assertRaisesRegex(asyncpg.RaiseError, "restore grant"):
            await old_conn.execute(
                "INSERT INTO release_grant_witnesses_v1("
                "grant_id,grant_digest,target_system_id,target_database,target_schema,"
                "cuts_digest,source_release_generation,target_release_generation,"
                "source_finalize_digest,activated_fence,session_id) VALUES("
                "'forged','forged','same','same','same','forged',1,2,repeat('f',64),2,$1)",
                self.old_session.session_id,
            )
        self.assertEqual(
            await old_conn.fetchval("SELECT lease_expires_at FROM control_leaders_v1"),
            before,
        )
        with self.assertRaises(ReleaseCompatibilityError):
            await self.release.open_session(
                runtime_version="control-r4a",
                actor_id="late-old-controller",
                role="CONTROL",
                lease_seconds=300,
            )
        new_session = await self.release.open_session(
            runtime_version="prefect-r4b",
            actor_id="new-controller",
            role="CONTROL",
            lease_seconds=300,
        )
        async with self.stream.pool.acquire() as conn, conn.transaction():
            await self.release.bind_transaction(conn, new_session.session_id)
            await conn.execute(
                "UPDATE control_leaders_v1 SET lease_expires_at=lease_expires_at+interval '1 second'"
            )
        head = await self.release.project_head()
        self.assertEqual(
            (head["schema_version"], head["release_generation"], head["fence_epoch"]),
            ("r4b", 2, 2),
        )
        self.assertTrue((await evaluate_release(DSN, self.schema)).passed)
        oracle = await asyncio.create_subprocess_exec(
            sys.executable,
            str(HERE / "postgres_release_oracle.py"),
            DSN,
            self.schema,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(oracle.communicate(), 10)
        self.assertEqual(oracle.returncode, 0, stderr.decode())
        self.assertTrue(json.loads(stdout)["passed"])

    async def test_real_process_loss_rolls_back_or_recovers_migration(self):
        worker = HERE / "postgres_release_crash_worker.py"

        async def crash(operation: str) -> None:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                str(worker),
                "--dsn",
                DSN,
                "--schema",
                self.schema,
                "--session-id",
                self.old_session.session_id,
                "--migration-id",
                "r4a-to-r4b-attempt-authority",
                "--artifact-digest",
                self.artifact_digest,
                "--operation",
                operation,
            )
            self.assertEqual(await asyncio.wait_for(process.wait(), 10), 86)

        await crash("expand")
        async with self.stream.pool.acquire() as conn:
            self.assertFalse(
                await conn.fetchval(
                    "SELECT to_regclass('prefect_execution_attempts_v1') IS NOT NULL"
                )
            )
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM release_migrations_v1"), 0
            )

        attempts = PostgresPrefectAttempts.connect_existing(self.control)
        await self.release.expand_prefect_attempts(
            self.old_session.session_id,
            attempts,
            migration_id="r4a-to-r4b-attempt-authority",
            from_version="r4a",
            to_version="r4b",
            artifact_digest=self.artifact_digest,
        )
        await crash("cutover-before-commit")
        head = await self.release.project_head()
        self.assertEqual(
            (head["schema_version"], head["pending_schema_version"]),
            ("r4a", "r4b"),
        )
        await crash("cutover-after-commit")
        self.assertEqual(
            await self.release.cutover(
                self.old_session.session_id,
                migration_id="r4a-to-r4b-attempt-authority",
                target_contracts=(self.new_contract,),
            ),
            2,
        )
        self.assertTrue((await evaluate_release(DSN, self.schema)).passed)

    async def test_declared_compatibility_capacity_refuses_cutover_atomically(self):
        attempts = PostgresPrefectAttempts.connect_existing(self.control)
        await self.release.expand_prefect_attempts(
            self.old_session.session_id,
            attempts,
            migration_id="r4a-to-r4b-attempt-authority",
            from_version="r4a",
            to_version="r4b",
            artifact_digest=self.artifact_digest,
        )
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE release_heads_v1 DISABLE TRIGGER release_head_guard"
            )
            await conn.execute("UPDATE release_heads_v1 SET max_compatibility_rows=1")
            await conn.execute(
                "ALTER TABLE release_heads_v1 ENABLE TRIGGER release_head_guard"
            )
        before = await self.release.project_head()
        with self.assertRaisesRegex(
            ReleaseProtocolError, "compatibility history capacity"
        ):
            await self.release.cutover(
                self.old_session.session_id,
                migration_id="r4a-to-r4b-attempt-authority",
                target_contracts=(self.new_contract,),
            )
        self.assertEqual(await self.release.project_head(), before)
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    "SELECT count(*) FROM release_runtime_compatibility_v1 "
                    "WHERE schema_version='r4b'"
                ),
                0,
            )

    async def test_declared_migration_capacity_refuses_expand_before_ddl(self):
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE release_heads_v1 DISABLE TRIGGER release_head_guard"
            )
            await conn.execute("UPDATE release_heads_v1 SET max_migrations=1")
            await conn.execute(
                "ALTER TABLE release_heads_v1 ENABLE TRIGGER release_head_guard"
            )
            await conn.execute(
                "ALTER TABLE release_migrations_v1 DISABLE TRIGGER release_migration_guard"
            )
            await conn.execute(
                "INSERT INTO release_migrations_v1 VALUES("
                "'retained-migration','legacy','r4a',$1,$2,'CUTOVER',$3,"
                "clock_timestamp(),clock_timestamp())",
                "9" * 64,
                "8" * 64,
                "7" * 64,
            )
            await conn.execute(
                "ALTER TABLE release_migrations_v1 ENABLE TRIGGER release_migration_guard"
            )
        before = await self.release.project_head()
        attempts = PostgresPrefectAttempts.connect_existing(self.control)
        with self.assertRaisesRegex(ReleaseProtocolError, "migration history capacity"):
            await self.release.expand_prefect_attempts(
                self.old_session.session_id,
                attempts,
                migration_id="r4a-to-r4b-attempt-authority",
                from_version="r4a",
                to_version="r4b",
                artifact_digest=self.artifact_digest,
            )
        self.assertEqual(await self.release.project_head(), before)
        async with self.stream.pool.acquire() as conn:
            self.assertIsNone(
                await conn.fetchval(
                    "SELECT to_regclass(current_schema() || '.prefect_attempt_heads_v1')"
                )
            )

    async def test_busy_quiesce_is_atomic_then_snapshot_is_stable(self):
        session, pool, stream, control = await self._expand_and_cutover()
        attempts = PostgresPrefectAttempts.connect_existing(control)
        await attempts.activate_attempt(
            self.fixture.admin_token,
            control_run_id=self.fixture.run_id,
            prefect_flow_run_id="release-flow",
            graph_digest=self.fixture.revision.graph_digest,
            code_version="adapter-v1",
            source_token="release-source-cap",
            source_bindings=self.fixture.sources(),
            operator_bindings=self.fixture.operators("release"),
            max_hot_attempts=2,
        )
        admission = await control.admit_source(
            "release-source-cap",
            run_id=self.fixture.run_id,
            request_id="release-admit-1",
            channel="input",
            partition="p0",
            record_id="release-document-1",
            attempt_id="release-append-1",
            object_key=content_object_key(b"release-one", "bytes/v1"),
            payload_digest=hashlib.sha256(b"release-one").hexdigest(),
            payload_size=len(b"release-one"),
            schema_id="bytes/v1",
            lineage_digest="release-lineage-1",
            producer_id="stable-source",
            producer_sequence=1,
        )
        await control.publish_admission(
            "release-source-cap",
            run_id=self.fixture.run_id,
            admission_id=admission["admission_id"],
            payload=b"release-one",
        )
        coordinator = PostgresBarrierCoordinator(stream)
        coordinator_epoch = await coordinator.claim_coordinator(
            self.fixture.revision.revision_id,
            self.fixture.revision.graph_version,
            self.fixture.revision.graph_digest,
            "checkpoint-controller",
            lease_seconds=300,
        )
        runtime = PostgresJoinRuntime(coordinator, self.fixture.revision, "c")
        projection = await attempts.project_attempt(
            self.fixture.admin_token, control_run_id=self.fixture.run_id
        )
        operator = next(
            row for row in projection["operators"] if row["participant_id"] == "c"
        )
        started = asyncio.Event()
        release = asyncio.Event()

        @stream_task
        async def blocked_task(ctx, payload):
            started.set()
            await release.wait()
            yield OutputSpec(payload.upper(), "bytes/v1", {"completed": 1})

        step = asyncio.create_task(
            runtime.step(
                "input/p0",
                blocked_task,
                operator["owner_id"],
                operator["owner_epoch"],
                coordinator_owner="checkpoint-controller",
                coordinator_epoch=coordinator_epoch,
            )
        )
        await asyncio.wait_for(started.wait(), 5)
        before = await self.release.project_head()
        with self.assertRaisesRegex(ReleaseQuiesceBusy, "join_invocations_v3"):
            await self.release.quiesce(session.session_id, run_id=self.fixture.run_id)
        self.assertEqual(await self.release.project_head(), before)
        release.set()
        self.assertEqual(await asyncio.wait_for(step, 5), "APPLIED")

        manifest = await self.release.quiesce(
            session.session_id, run_id=self.fixture.run_id
        )
        self.assertEqual(
            await self.release.quiesce(session.session_id, run_id=self.fixture.run_id),
            manifest,
        )
        self.assertEqual(manifest.run_id, self.fixture.run_id)
        self.assertEqual(manifest.release_generation, 2)
        self.assertEqual(manifest.fence_epoch, 3)
        self.assertEqual(manifest.restore_key_id, self.release_key_id())
        async with (
            self.stream.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            exported = await self.release.snapshot_cuts(self.fixture.run_id, conn)
        self.assertEqual(exported, manifest.cuts)
        head = await self.release.project_head()
        self.assertEqual(head["state"], "QUIESCED")
        self.assertEqual(head["quiesced_cuts_digest"], manifest.cuts.digest)
        async with pool.acquire() as conn:
            with self.assertRaisesRegex(asyncpg.RaiseError, "release is not writable"):
                await conn.execute(
                    "UPDATE control_leaders_v1 SET lease_expires_at=lease_expires_at+interval '1 second'"
                )
        finalized = await self.release.finalize_source(
            quiesce_manifest_digest=manifest.manifest_digest,
            expected_cuts=manifest.cuts,
        )
        self.assertEqual(
            await self.release.mark_cutover_retired(
                quiesce_manifest_digest=manifest.manifest_digest,
                expected_cuts=manifest.cuts,
            ),
            finalized,
        )
        self.assertEqual(
            await self.release.assert_cutover_retired(
                manifest.manifest_digest, manifest.cuts
            ),
            finalized,
        )
        self.assertEqual(
            await self.release.finalize_source(
                quiesce_manifest_digest=manifest.manifest_digest,
                expected_cuts=manifest.cuts,
            ),
            finalized,
        )
        async with self.stream.pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT desired_state,lifecycle FROM control_runs_v1 WHERE run_id=$1",
                self.fixture.run_id,
            )
            self.assertEqual(tuple(run), ("CANCELLED", "CANCELLED"))
            self.assertEqual(
                await conn.fetchval(
                    "SELECT status FROM prefect_execution_attempts_v1 "
                    "WHERE attempt_id=(SELECT current_attempt_id FROM "
                    "prefect_attempt_heads_v1 WHERE run_id=$1)",
                    self.fixture.run_id,
                ),
                "CANCELLED",
            )
        self.assertEqual(
            (await self.release.project_head())["state"], "CUTOVER_RETIRED"
        )
        self.assertTrue((await evaluate_release(DSN, self.schema)).passed)

    async def test_session_history_is_bounded_by_one_rolling_archive(self):
        for ordinal in range(100):
            session = await self.release.open_session(
                runtime_version="control-r4a",
                actor_id=f"short-controller-{ordinal}",
                role="CONTROL",
                lease_seconds=300,
            )
            await self.release.close_session(session.session_id)

        async with self.stream.pool.acquire() as conn:
            live = await conn.fetch(
                "SELECT actor_id,status FROM release_sessions_v1 ORDER BY actor_id"
            )
            archive = await conn.fetchrow("SELECT * FROM release_session_archive_v1")
        self.assertEqual(
            [(row["actor_id"], row["status"]) for row in live],
            [("old-controller", "ACTIVE")],
        )
        self.assertEqual(archive["retired_count"], 100)
        self.assertEqual(archive["retired_through"], 101)
        self.assertEqual(len(archive["rolling_digest"]), 64)
        await self.release.close_session(self.old_session.session_id)
        async with self.stream.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval("SELECT count(*) FROM release_sessions_v1"), 0
            )
            archive = await conn.fetchrow("SELECT * FROM release_session_archive_v1")
        self.assertEqual(archive["retired_count"], 101)
        self.assertEqual(archive["retired_through"], 101)
        self.assertTrue((await evaluate_release(DSN, self.schema)).passed)

    async def test_oracle_rejects_authority_corruption_without_throwing(self):
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "ALTER TABLE release_heads_v1 DISABLE TRIGGER release_head_guard"
            )
            await conn.execute("UPDATE release_heads_v1 SET fence_epoch=fence_epoch+1")
            await conn.execute(
                "ALTER TABLE release_heads_v1 ENABLE TRIGGER release_head_guard"
            )
        report = await evaluate_release(DSN, self.schema)
        self.assertFalse(report.passed)
        self.assertIn("active_release_session_authority", report.violations)

    def release_key_id(self) -> str:
        return key_id(public_key_b64(self.key))


if __name__ == "__main__":
    unittest.main()
