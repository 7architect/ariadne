from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sys
import tempfile
import unittest
from contextlib import AsyncExitStack
from dataclasses import replace
from pathlib import Path

import test_postgres_prefect_adapter as fixtures
from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_control_plane import AuthenticationFailed, PostgresControlPlane
from postgres_control_plane_oracle import evaluate as evaluate_control
from postgres_join_oracle import evaluate as evaluate_join
from postgres_join_runtime import PostgresJoinRuntime
from postgres_prefect_attempt_oracle import evaluate as evaluate_attempt
from postgres_prefect_attempts import PostgresPrefectAttempts
from postgres_release_authority import (
    PostgresReleaseAuthority,
    ReleaseCompatibilityError,
    ReleaseProtocolError,
    RuntimeContract,
)
from postgres_release_oracle import evaluate as evaluate_release
from postgres_restore_contracts import RestoreContractError
from postgres_restore_registry import PostgresRestoreRegistry, RestoreGrantConflict
from postgres_schema_backup import (
    IsolatedRestorePostgres,
    create_schema_backup,
    docker_command,
    file_digest,
    restore_schema_backup,
)
from postgres_stream_oracle import evaluate as evaluate_stream
from postgres_stream_v2 import PostgresStream, content_object_key
from prefect_stream_adapter import prefect_return_only, prefect_uppercase


class PostgresSchemaBackupTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.PostgresPrefectAttemptTests("runTest")
        await self.fixture.asyncSetUp()
        f = self.fixture
        self.schema = f.schema
        self.registry_schema = "restore_reg_" + self.schema
        self.temp = tempfile.TemporaryDirectory(prefix="ariadne-schema-backup-")
        self.artifact = Path(self.temp.name) / "stream.dump"
        self.old_attempt = await f.activate()
        current = await f.attempts.project_attempt(
            f.admin_token, control_run_id=f.run_id
        )
        self.old_bindings = {row["participant_id"]: row for row in current["operators"]}
        for number in (1, 2):
            await f.append("source-cap-1", number)
            await self.process_one(f.runtimes, self.old_bindings, f.coordinator_epoch)
        await f.append("source-cap-1", 3)
        self.registry = await PostgresRestoreRegistry.install(
            f.stream.pool, self.registry_schema
        )
        self.contract = RuntimeContract("backup-r4b", "1" * 64, "2" * 64)
        self.release = await PostgresReleaseAuthority.install(
            f.stream.pool,
            self.schema,
            schema_version="r4b",
            runtime_contracts=(self.contract,),
            restore_public_key_b64=await self.registry.verification_key(),
        )
        session = await self.release.open_session(
            runtime_version=self.contract.runtime_version,
            actor_id="source-controller",
            role="EXECUTION",
            lease_seconds=300,
        )
        self.quiesced = await self.release.quiesce(session.session_id, run_id=f.run_id)

    async def asyncTearDown(self):
        if hasattr(self, "registry_schema") and hasattr(self.fixture, "stream"):
            async with self.fixture.stream.pool.acquire() as conn:
                await conn.execute(
                    'DROP SCHEMA IF EXISTS "' + self.registry_schema + '" CASCADE'
                )
        if hasattr(self, "temp"):
            self.temp.cleanup()
        await self.fixture.asyncTearDown()

    async def process_one(self, runtimes, bindings, coordinator_epoch):
        for participant, function in (
            ("b", prefect_return_only),
            ("c", prefect_uppercase),
        ):
            binding = bindings[participant]
            self.assertEqual(
                await runtimes[participant].step(
                    "input/p0",
                    function,
                    binding["owner_id"],
                    binding["owner_epoch"],
                    coordinator_owner="checkpoint-controller",
                    coordinator_epoch=coordinator_epoch,
                ),
                "APPLIED",
            )

    async def backup(self):
        return await create_schema_backup(
            self.registry,
            self.release,
            self.quiesced,
            self.artifact,
            source_container="ariadne-stream-postgres",
        )

    async def target(self, stack):
        image = await docker_command(
            "inspect", "--format", "{{.Image}}", "ariadne-stream-postgres"
        )
        target = await stack.enter_async_context(
            IsolatedRestorePostgres(self.schema, image=image)
        )
        return target

    async def test_actual_export_restore_one_grant_and_first_incomplete_resume(self):
        f = self.fixture
        envelope = await self.backup()
        manifest = envelope["manifest"]
        self.assertGreater(manifest["artifact_bytes"], 0)
        self.assertEqual(manifest["cuts"]["run"]["run_id"], f.run_id)
        self.assertEqual(manifest["cuts"]["inputs"][0]["completed"], 2)
        async with AsyncExitStack() as stack:
            target = await self.target(stack)
            await restore_schema_backup(
                self.registry,
                manifest["backup_id"],
                self.artifact,
                target.pool,
                target_container=target.name,
            )
            authority = PostgresReleaseAuthority.connect_existing(
                target.pool, self.schema
            )
            with self.assertRaises(ReleaseProtocolError) as not_fenced:
                await self.registry.mint_grant(
                    manifest["backup_id"], self.release, authority
                )
            self.assertIn("retired", str(not_fenced.exception))
            await self.release.finalize_source(
                quiesce_manifest_digest=self.quiesced.manifest_digest,
                expected_cuts=self.quiesced.cuts,
            )
            await self.release.mark_cutover_retired(
                quiesce_manifest_digest=self.quiesced.manifest_digest,
                expected_cuts=self.quiesced.cuts,
            )
            grant = await self.registry.mint_grant(
                manifest["backup_id"], self.release, authority
            )
            with self.assertRaises(ReleaseCompatibilityError):
                await authority.activate_restored(
                    replace(
                        grant, claims=replace(grant.claims, target_database="forged")
                    ),
                    runtime_version=self.contract.runtime_version,
                    actor_id="restored-controller",
                    role="EXECUTION",
                    lease_seconds=300,
                )
            session = await authority.activate_restored(
                grant,
                runtime_version=self.contract.runtime_version,
                actor_id="restored-controller",
                role="EXECUTION",
                lease_seconds=300,
            )
            self.assertEqual(
                await authority.activate_restored(
                    grant,
                    runtime_version=self.contract.runtime_version,
                    actor_id="restored-controller",
                    role="EXECUTION",
                    lease_seconds=300,
                ),
                session,
            )
            self.assertEqual(
                await self.registry.mint_grant(
                    manifest["backup_id"], self.release, authority
                ),
                grant,
            )
            pool = await authority.open_bound_pool(target.dsn, session.session_id)
            stack.push_async_callback(pool.close)
            stream = PostgresStream(pool, self.schema)
            control = PostgresControlPlane.connect_existing(stream)
            attempts = PostgresPrefectAttempts.connect_existing(control)
            await attempts.replace_execution(
                f.admin_token,
                control_run_id=f.run_id,
                expected_attempt_id=self.old_attempt.attempt_id,
                prefect_flow_run_id="restored-flow",
                graph_digest=f.revision.graph_digest,
                code_version="adapter-v1",
                source_token="restored-source-cap",
                source_bindings=f.sources(),
                operator_bindings=f.operators("restored"),
                reason="SCHEMA_BACKUP_RESTORE",
            )
            binding = await attempts.project_attempt(
                f.admin_token, control_run_id=f.run_id
            )
            bindings = {row["participant_id"]: row for row in binding["operators"]}
            coordinator = PostgresBarrierCoordinator(stream)
            epoch = await coordinator.claim_coordinator(
                f.revision.revision_id,
                f.revision.graph_version,
                f.revision.graph_digest,
                "checkpoint-controller",
                lease_seconds=300,
            )
            runtimes = {
                p: PostgresJoinRuntime(coordinator, f.revision, p) for p in ("b", "c")
            }
            await self.process_one(runtimes, bindings, epoch)
            async with pool.acquire() as conn:
                states = await conn.fetch(
                    "SELECT state FROM join_workers ORDER BY participant_id"
                )
                self.assertEqual(
                    [json.loads(row["state"])["completed"] for row in states], [3, 3]
                )
                self.assertFalse(
                    await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)",
                        self.registry_schema,
                    )
                )
            for oracle in (
                evaluate_stream,
                evaluate_join,
                evaluate_control,
                evaluate_attempt,
            ):
                report = await oracle(target.dsn, self.schema)
                self.assertTrue(report["passed"], report)
            release_report = await evaluate_release(target.dsn, self.schema)
            self.assertTrue(release_report.passed, release_report)
            with self.assertRaises(AuthenticationFailed):
                await f.append("source-cap-1", 4)
            self.assertEqual(
                (await self.release.project_head())["state"], "CUTOVER_RETIRED"
            )
            source_release = await evaluate_release(fixtures.DSN, self.schema)
            self.assertTrue(source_release.passed, source_release)

    async def test_concurrent_isolated_targets_get_one_nonreassignable_grant(self):
        envelope = await self.backup()
        manifest = envelope["manifest"]
        await self.release.finalize_source(
            quiesce_manifest_digest=self.quiesced.manifest_digest,
            expected_cuts=self.quiesced.cuts,
        )
        await self.release.mark_cutover_retired(
            quiesce_manifest_digest=self.quiesced.manifest_digest,
            expected_cuts=self.quiesced.cuts,
        )
        async with AsyncExitStack() as stack:
            targets = [await self.target(stack), await self.target(stack)]
            authorities = []
            for target in targets:
                await restore_schema_backup(
                    self.registry,
                    manifest["backup_id"],
                    self.artifact,
                    target.pool,
                    target_container=target.name,
                )
                authorities.append(
                    PostgresReleaseAuthority.connect_existing(target.pool, self.schema)
                )
            outcomes = await asyncio.gather(
                *(
                    self.registry.mint_grant(
                        manifest["backup_id"], self.release, authority
                    )
                    for authority in authorities
                ),
                return_exceptions=True,
            )
            self.assertEqual(
                sum(not isinstance(value, BaseException) for value in outcomes),
                1,
                outcomes,
            )
            self.assertEqual(
                sum(isinstance(value, RestoreGrantConflict) for value in outcomes),
                1,
                outcomes,
            )
            loser = next(
                index
                for index, value in enumerate(outcomes)
                if isinstance(value, RestoreGrantConflict)
            )
            async with targets[loser].pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_grant_witnesses_v1"
                    ),
                    0,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_sessions_v1 WHERE status='ACTIVE'"
                    ),
                    0,
                )

    async def test_real_backup_process_loss_reconciles_artifact_and_registry(self):
        manifest_file = Path(self.temp.name) / "quiesced.json"
        manifest_file.write_text(json.dumps(self.quiesced.to_dict()))
        environment = os.environ.copy()
        environment["DS050_BACKUP_DSN"] = fixtures.DSN
        for stage in (
            "after_pending_manifest",
            "after_dump_before_receipt",
            "after_receipt_before_publish",
            "after_artifact_before_registry",
            "after_registry_before_ack",
        ):
            artifact = Path(self.temp.name) / (stage + ".dump")
            command = [
                sys.executable,
                str(Path(__file__).with_name("postgres_schema_backup_worker.py")),
                "--dsn-env",
                "DS050_BACKUP_DSN",
                "--schema",
                self.schema,
                "--registry-schema",
                self.registry_schema,
                "--quiesce-manifest",
                str(manifest_file),
                "--artifact",
                str(artifact),
            ]
            lost = await asyncio.create_subprocess_exec(
                *command,
                "--fault-stage",
                stage,
                env=environment,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await asyncio.wait_for(lost.communicate(), 40)
            self.assertEqual(lost.returncode, 86, (out.decode(), err.decode()))
            stale_receipt = artifact.with_name(
                artifact.name + ".json.writing.interrupted"
            )
            stale_receipt.write_text("unpublished receipt")
            replacement = await asyncio.create_subprocess_exec(
                *command,
                env=environment,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await asyncio.wait_for(replacement.communicate(), 40)
            self.assertEqual(replacement.returncode, 0, err.decode())
            result = json.loads(out)
            envelope = json.loads(artifact.with_suffix(".dump.json").read_text())
            self.assertEqual(file_digest(artifact), result["artifact_sha256"])
            self.assertEqual(envelope["manifest_sha256"], result["manifest_sha256"])
            row = await self.registry.backup_record(result["backup_id"])
            self.assertEqual(row["status"], "COMPLETED")
            self.assertFalse(stale_receipt.exists())
            self.assertEqual(
                list(artifact.parent.glob(artifact.name + ".partial.*")), []
            )
            self.assertEqual(
                (await self.release.snapshot_cuts(self.fixture.run_id)),
                self.quiesced.cuts,
            )

    async def test_two_restore_generations_archive_prior_witness_and_resume_tail(self):
        f = self.fixture

        async def activate(target, authority, grant, prior_attempt, label, stack):
            session = await authority.activate_restored(
                grant,
                runtime_version=self.contract.runtime_version,
                actor_id=label,
                role="EXECUTION",
                lease_seconds=300,
            )
            pool = await authority.open_bound_pool(target.dsn, session.session_id)
            stack.push_async_callback(pool.close)
            stream = PostgresStream(pool, self.schema)
            control = PostgresControlPlane.connect_existing(stream)
            attempts = PostgresPrefectAttempts.connect_existing(control)
            attempt = await attempts.replace_execution(
                f.admin_token,
                control_run_id=f.run_id,
                expected_attempt_id=prior_attempt,
                prefect_flow_run_id=label,
                graph_digest=f.revision.graph_digest,
                code_version="adapter-v1",
                source_token=label + "-source",
                source_bindings=f.sources(),
                operator_bindings=f.operators(label),
                reason="SCHEMA_BACKUP_RESTORE",
            )
            projection = await attempts.project_attempt(
                f.admin_token, control_run_id=f.run_id
            )
            bindings = {row["participant_id"]: row for row in projection["operators"]}
            coordinator = PostgresBarrierCoordinator(stream)
            epoch = await coordinator.claim_coordinator(
                f.revision.revision_id,
                f.revision.graph_version,
                f.revision.graph_digest,
                "checkpoint-controller",
                lease_seconds=300,
            )
            runtimes = {
                p: PostgresJoinRuntime(coordinator, f.revision, p) for p in ("b", "c")
            }
            return session, pool, control, attempt, bindings, runtimes, epoch

        first = (await self.backup())["manifest"]
        await self.release.finalize_source(
            quiesce_manifest_digest=self.quiesced.manifest_digest,
            expected_cuts=self.quiesced.cuts,
        )
        await self.release.mark_cutover_retired(
            quiesce_manifest_digest=self.quiesced.manifest_digest,
            expected_cuts=self.quiesced.cuts,
        )
        async with AsyncExitStack() as stack:
            target1 = await self.target(stack)
            await restore_schema_backup(
                self.registry,
                first["backup_id"],
                self.artifact,
                target1.pool,
                target_container=target1.name,
            )
            authority1 = PostgresReleaseAuthority.connect_existing(
                target1.pool, self.schema
            )
            grant1 = await self.registry.mint_grant(
                first["backup_id"], self.release, authority1
            )
            (
                session1,
                pool1,
                control1,
                attempt1,
                bindings1,
                runtimes1,
                epoch1,
            ) = await activate(
                target1,
                authority1,
                grant1,
                self.old_attempt.attempt_id,
                "generation-2",
                stack,
            )
            await self.process_one(runtimes1, bindings1, epoch1)
            payload = b"document-4"
            admission = await control1.admit_source(
                "generation-2-source",
                run_id=f.run_id,
                request_id="admit-4",
                channel="input",
                partition="p0",
                record_id="document-4",
                attempt_id="append-4",
                object_key=content_object_key(payload, "bytes/v1"),
                payload_digest=hashlib.sha256(payload).hexdigest(),
                payload_size=len(payload),
                schema_id="bytes/v1",
                lineage_digest="lineage-4",
                producer_id="stable-source",
                producer_sequence=4,
            )
            await control1.publish_admission(
                "generation-2-source",
                run_id=f.run_id,
                admission_id=admission["admission_id"],
                payload=payload,
            )
            quiesced2 = await authority1.quiesce(session1.session_id, run_id=f.run_id)
            async with pool1.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_grant_witnesses_v1"
                    ),
                    0,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_grant_archive_v1"
                    ),
                    1,
                )
            second_artifact = Path(self.temp.name) / "generation-2.dump"
            second = (
                await create_schema_backup(
                    self.registry,
                    authority1,
                    quiesced2,
                    second_artifact,
                    source_container=target1.name,
                )
            )["manifest"]
            await authority1.finalize_source(
                quiesce_manifest_digest=quiesced2.manifest_digest,
                expected_cuts=quiesced2.cuts,
            )
            await authority1.mark_cutover_retired(
                quiesce_manifest_digest=quiesced2.manifest_digest,
                expected_cuts=quiesced2.cuts,
            )
            target2 = await self.target(stack)
            await restore_schema_backup(
                self.registry,
                second["backup_id"],
                second_artifact,
                target2.pool,
                target_container=target2.name,
            )
            authority2 = PostgresReleaseAuthority.connect_existing(
                target2.pool, self.schema
            )
            grant2 = await self.registry.mint_grant(
                second["backup_id"], authority1, authority2
            )
            _, pool2, _, _, bindings2, runtimes2, epoch2 = await activate(
                target2, authority2, grant2, attempt1.attempt_id, "generation-3", stack
            )
            await self.process_one(runtimes2, bindings2, epoch2)
            self.assertEqual((await authority2.project_head())["release_generation"], 3)
            async with pool2.acquire() as conn:
                states = await conn.fetch(
                    "SELECT state FROM join_workers ORDER BY participant_id"
                )
                self.assertEqual(
                    [json.loads(row["state"])["completed"] for row in states], [4, 4]
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_grant_witnesses_v1"
                    ),
                    1,
                )
            report = await evaluate_release(target2.dsn, self.schema)
            self.assertTrue(report.passed, report.to_dict())
            for oracle in (
                evaluate_stream,
                evaluate_join,
                evaluate_control,
                evaluate_attempt,
            ):
                report = await oracle(target2.dsn, self.schema)
                self.assertTrue(report["passed"], report)

    async def test_corrupt_target_payload_rejects_signed_activation_without_mutation(
        self,
    ):
        manifest = (await self.backup())["manifest"]
        await self.release.finalize_source(
            quiesce_manifest_digest=self.quiesced.manifest_digest,
            expected_cuts=self.quiesced.cuts,
        )
        await self.release.mark_cutover_retired(
            quiesce_manifest_digest=self.quiesced.manifest_digest,
            expected_cuts=self.quiesced.cuts,
        )
        async with AsyncExitStack() as stack:
            target = await self.target(stack)
            await restore_schema_backup(
                self.registry,
                manifest["backup_id"],
                self.artifact,
                target.pool,
                target_container=target.name,
            )
            authority = PostgresReleaseAuthority.connect_existing(
                target.pool, self.schema
            )
            grant = await self.registry.mint_grant(
                manifest["backup_id"], self.release, authority
            )
            before = await authority.snapshot_cuts(self.fixture.run_id)
            async with target.pool.acquire() as conn, conn.transaction():
                # Deliberate corruption in an isolated disposable target. This
                # bypass is not an application or recovery operation.
                await conn.execute("ALTER TABLE objects DISABLE TRIGGER USER")
                await conn.execute(
                    "UPDATE objects SET payload=decode(repeat('00',octet_length(payload)),'hex') WHERE object_key=(SELECT object_key FROM records WHERE channel='input' AND sequence=3)"
                )
                await conn.execute("ALTER TABLE objects ENABLE TRIGGER USER")
            self.assertEqual(await authority.snapshot_cuts(self.fixture.run_id), before)
            with self.assertRaisesRegex(RestoreContractError, "corrupt"):
                await authority.activate_restored(
                    grant,
                    runtime_version=self.contract.runtime_version,
                    actor_id="corrupt-target",
                    role="EXECUTION",
                    lease_seconds=300,
                )
            async with target.pool.acquire() as conn:
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_grant_witnesses_v1"
                    ),
                    0,
                )
                self.assertEqual(
                    await conn.fetchval(
                        "SELECT count(*) FROM release_sessions_v1 WHERE status='ACTIVE'"
                    ),
                    0,
                )
            report = await evaluate_stream(target.dsn, self.schema)
            self.assertFalse(report["passed"])

    async def test_committed_artifact_corruption_is_named_without_silent_redump(self):
        envelope = await self.backup()
        original = self.artifact.read_bytes()
        self.artifact.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        with self.assertRaisesRegex(RestoreContractError, "corrupt"):
            await self.backup()
        row = await self.registry.backup_record(envelope["manifest"]["backup_id"])
        self.assertEqual(row["status"], "COMPLETED")


if __name__ == "__main__":
    unittest.main()
