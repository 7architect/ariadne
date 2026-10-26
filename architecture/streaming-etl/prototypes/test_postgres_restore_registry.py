from __future__ import annotations

import os
import unittest
import uuid
from dataclasses import replace

import asyncpg
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from postgres_restore_contracts import (
    BoundGrantWitness,
    RestoreContractError,
    RestoreGrantClaims,
    canonical,
    digest,
    key_id,
)
from postgres_restore_registry import BackupRegistryFull, PostgresRestoreRegistry

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN", "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming"
)


class PostgresRestoreRegistryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        try:
            probe = await asyncpg.connect(DSN, timeout=2)
        except (OSError, asyncpg.PostgresError):
            self.skipTest("PostgreSQL streaming profile is not running")
        else:
            await probe.close()
        self.schema = "ds050_registry_" + uuid.uuid4().hex[:12]
        self.pool = await asyncpg.create_pool(DSN, min_size=1, max_size=4)
        self.registry = await PostgresRestoreRegistry.install(
            self.pool, self.schema, max_backups=2
        )

    async def asyncTearDown(self):
        if not hasattr(self, "pool"):
            return
        await self.pool.close()
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute('DROP SCHEMA "' + self.schema + '" CASCADE')
        finally:
            await conn.close()

    def pending(self, number=1):
        return {
            "source_scope": {
                "system_id": "source",
                "database": "streaming",
                "schema": "source_schema",
            },
            "snapshot_id": f"snapshot-{number}",
            "quiesce_manifest_digest": "a" * 64,
            "run_id": "run-1",
        }

    async def test_pending_capacity_and_immutable_finalization_metadata(self):
        # This isolates registry transitions; actual dump/restore is a separate
        # integration criterion and is not inferred from a metadata-only test.
        pending = self.pending()
        backup = await self.registry.begin_backup(pending)
        self.assertEqual(await self.registry.begin_backup(pending), backup)
        with self.assertRaises(RestoreContractError):
            await self.registry.completed_backup(backup)
        manifest = {**pending, "backup_id": backup, "artifact_sha256": "b" * 64}
        completed = await self.registry.finish_backup(backup, manifest)
        self.assertEqual(await self.registry.finish_backup(backup, manifest), completed)
        with self.assertRaises(RestoreContractError):
            await self.registry.finish_backup(
                backup, {**manifest, "artifact_sha256": "c" * 64}
            )
        with self.assertRaises(asyncpg.RaiseError):
            async with self.pool.acquire() as conn:
                await conn.execute(
                    f"UPDATE \"{self.schema}\".schema_backup_manifests_v1 SET snapshot_id='forged' WHERE backup_id=$1",
                    backup,
                )
        second = await self.registry.begin_backup(self.pending(2))
        await self.registry.reject_backup(second, "ControlledDumpFailure")
        with self.assertRaises(RestoreContractError):
            await self.registry.completed_backup(second)
        with self.assertRaises(BackupRegistryFull):
            await self.registry.begin_backup(self.pending(3))
        async with self.pool.acquire() as conn:
            self.assertEqual(
                await conn.fetchval(
                    f'SELECT count(*) FROM "{self.schema}".schema_backup_manifests_v1'
                ),
                2,
            )

    async def test_signed_target_and_unicode_cuts_cannot_be_rebound(self):
        public = await self.registry.verification_key()
        async with self.pool.acquire() as conn:
            private = await conn.fetchval(
                f'SELECT private_key FROM "{self.schema}".restore_issuer_v1'
            )
        claims = RestoreGrantClaims(
            grant_id="grant-1",
            registry_ordinal=1,
            issuer_key_id=key_id(public),
            source_system_id="source-system",
            source_database="streaming",
            source_schema="source_schema",
            target_system_id="target-system",
            target_database="streaming",
            target_schema="source_schema",
            deployment_id="deployment",
            revision_id="revision",
            run_id="run-1",
            source_release_generation=1,
            source_release_fence=2,
            target_release_generation=2,
            backup_artifact_sha256="a" * 64,
            backup_manifest_sha256="b" * 64,
            schema_digest="c" * 64,
            cuts_json=canonical({"document": "документ", "sequence": 200}),
            cuts_digest=digest({"document": "документ", "sequence": 200}),
            quiesce_manifest_digest="d" * 64,
            source_finalize_digest="e" * 64,
        )
        witness = BoundGrantWitness.sign(
            claims, Ed25519PrivateKey.from_private_bytes(bytes(private))
        )
        witness.verify(public)
        BoundGrantWitness.from_dict(witness.to_dict()).verify(public)
        for changed in (
            replace(claims, target_system_id="another-system"),
            replace(claims, registry_ordinal=2),
            replace(
                claims,
                cuts_json=canonical({"sequence": 201}),
                cuts_digest=digest({"sequence": 201}),
            ),
        ):
            with self.assertRaises(RestoreContractError):
                replace(witness, claims=changed).verify(public)
        self.assertEqual(claims.cuts["sequence"], 200)
        modified = claims.cuts
        modified["sequence"] = 999
        self.assertEqual(claims.cuts["sequence"], 200)


if __name__ == "__main__":
    unittest.main()
