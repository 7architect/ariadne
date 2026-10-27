"""External single-use signed restore grants; excluded from tenant backups."""

from __future__ import annotations

import json
import re
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)
from postgres_release_authority import CanonicalReleaseCuts
from postgres_restore_contracts import (
    BoundGrantWitness,
    RestoreContractError,
    RestoreGrantClaims,
    canonical,
    digest,
    key_id,
    public_key_b64,
)
from postgres_restore_integrity import assert_retained_payload_integrity
from postgres_schema_identity import schema_identity


class RestoreGrantConflict(RestoreContractError):
    pass


class BackupRegistryFull(RestoreContractError):
    pass


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


async def database_scope(pool: asyncpg.Pool, schema: str) -> dict[str, str]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT system_identifier::text AS system_id,current_database() AS database "
            "FROM pg_control_system()"
        )
    return {**dict(row), "schema": schema}


def cuts_document(cuts: Any) -> dict[str, Any]:
    if isinstance(cuts, dict):
        return cuts
    if hasattr(cuts, "material") and hasattr(cuts, "digest"):
        if digest(cuts.material) != cuts.digest:
            raise RestoreContractError("release cuts were mutated after capture")
        return json.loads(canonical(cuts.material))
    if hasattr(cuts, "to_dict"):
        return cuts.to_dict()
    raise RestoreContractError("canonical release cuts must expose to_dict")


class PostgresRestoreRegistry:
    """Trusted bootstrap service with no grant timeout or reassignment.

    Its private key lives outside the dumped source schema. Direct bootstrap
    database credentials remain a separate later security boundary.
    """

    def __init__(self, pool: asyncpg.Pool, schema: str, max_backups: int = 16):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
            raise ValueError("unsafe registry schema")
        if not 1 <= max_backups <= 128:
            raise ValueError("backup registry capacity must be 1..128")
        self.pool, self.schema, self.max_backups = pool, schema, max_backups

    @classmethod
    async def install(cls, pool, schema, *, max_backups=16):
        store = cls(pool, schema, max_backups)
        namespace = '"' + schema + '"'
        async with pool.acquire() as conn, conn.transaction():
            await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {namespace}")
            await conn.execute(f"SET LOCAL search_path={namespace},pg_catalog")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS restore_issuer_v1(
                  singleton boolean PRIMARY KEY CHECK(singleton),
                  key_id text NOT NULL UNIQUE,public_key_b64 text NOT NULL,
                  private_key bytea NOT NULL,max_backups int NOT NULL);
                CREATE TABLE IF NOT EXISTS schema_backup_manifests_v1(
                  backup_id text PRIMARY KEY,source_scope jsonb NOT NULL,
                  snapshot_id text NOT NULL,quiesce_digest text NOT NULL,
                  status text NOT NULL CHECK(status IN('PENDING','COMPLETED','REJECTED')),
                  pending_json jsonb NOT NULL,manifest_json jsonb,
                  manifest_digest text,artifact_sha256 text,error_type text);
                CREATE TABLE IF NOT EXISTS schema_restore_grants_v1(
                  registry_ordinal bigserial UNIQUE,
                  source_system_id text NOT NULL,source_database text NOT NULL,
                  source_schema text NOT NULL,run_id text NOT NULL,
                  backup_id text NOT NULL REFERENCES schema_backup_manifests_v1,
                  witness_json jsonb NOT NULL,witness_digest text NOT NULL,
                  PRIMARY KEY(source_system_id,source_database,source_schema,run_id));
                CREATE OR REPLACE FUNCTION immutable_restore_registry() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                  RAISE EXCEPTION 'restore registry authority immutable';
                END $$;
                DROP TRIGGER IF EXISTS immutable_restore_issuer ON restore_issuer_v1;
                CREATE TRIGGER immutable_restore_issuer BEFORE UPDATE OR DELETE
                  ON restore_issuer_v1 FOR EACH ROW EXECUTE FUNCTION immutable_restore_registry();
                DROP TRIGGER IF EXISTS immutable_restore_grant ON schema_restore_grants_v1;
                CREATE TRIGGER immutable_restore_grant BEFORE UPDATE OR DELETE
                  ON schema_restore_grants_v1 FOR EACH ROW EXECUTE FUNCTION immutable_restore_registry();
                CREATE OR REPLACE FUNCTION backup_finalization_guard() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN
                  IF TG_OP='DELETE' OR OLD.status<>'PENDING' OR
                     (NEW.backup_id,NEW.source_scope,NEW.snapshot_id,NEW.quiesce_digest,NEW.pending_json)
                     IS DISTINCT FROM
                     (OLD.backup_id,OLD.source_scope,OLD.snapshot_id,OLD.quiesce_digest,OLD.pending_json)
                     OR NEW.status NOT IN('COMPLETED','REJECTED')
                  THEN RAISE EXCEPTION 'backup manifest immutable'; END IF;
                  IF NEW.status='COMPLETED' AND (NEW.manifest_json IS NULL OR
                     NEW.manifest_digest IS NULL OR NEW.artifact_sha256 IS NULL)
                  THEN RAISE EXCEPTION 'backup completion requires artifact'; END IF;
                  RETURN NEW;
                END $$;
                DROP TRIGGER IF EXISTS backup_finalization_guard ON schema_backup_manifests_v1;
                CREATE TRIGGER backup_finalization_guard BEFORE UPDATE OR DELETE
                  ON schema_backup_manifests_v1 FOR EACH ROW EXECUTE FUNCTION backup_finalization_guard();
            """)
            existing = await conn.fetchrow("SELECT * FROM restore_issuer_v1")
            if existing is None:
                key = Ed25519PrivateKey.generate()
                public = public_key_b64(key)
                await conn.execute(
                    "INSERT INTO restore_issuer_v1 VALUES(true,$1,$2,$3,$4)",
                    key_id(public),
                    public,
                    key.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption()),
                    max_backups,
                )
            elif existing["max_backups"] != max_backups:
                raise RestoreContractError("registry capacity identity drift")
        return store

    async def verification_key(self) -> str:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                f'SELECT public_key_b64 FROM "{self.schema}".restore_issuer_v1'
            )

    @asynccontextmanager
    async def backup_request_lock(self, request_id: str):
        if self.pool.get_max_size() < 3:
            raise RestoreContractError(
                "schema backup requires at least three pool connections"
            )
        async with self.pool.acquire() as conn:
            await conn.execute(
                "SELECT pg_advisory_lock(hashtextextended($1,0))",
                "ariadne.schema-backup:" + request_id,
            )
            try:
                yield
            finally:
                await conn.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1,0))",
                    "ariadne.schema-backup:" + request_id,
                )

    async def backup_records_for_request(self, request_id: str):
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                f"SELECT * FROM \"{self.schema}\".schema_backup_manifests_v1 WHERE pending_json->>'artifact_request_id'=$1 ORDER BY backup_id",
                request_id,
            )

    async def backup_record(self, backup_id: str):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT * FROM "{self.schema}".schema_backup_manifests_v1 WHERE backup_id=$1',
                backup_id,
            )
        if row is None:
            raise RestoreContractError("backup record not found")
        return row

    async def begin_backup(self, pending: dict[str, Any]) -> str:
        backup_id = "schema-backup:" + digest(pending)
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(f'SET LOCAL search_path="{self.schema}",pg_catalog')
            await conn.fetchrow("SELECT * FROM restore_issuer_v1 FOR UPDATE")
            prior = await conn.fetchrow(
                "SELECT * FROM schema_backup_manifests_v1 WHERE backup_id=$1", backup_id
            )
            if prior is not None:
                if decoded(prior["pending_json"]) != pending:
                    raise RestoreContractError("backup request identity drift")
                return backup_id
            if (
                await conn.fetchval("SELECT count(*) FROM schema_backup_manifests_v1")
                >= self.max_backups
            ):
                raise BackupRegistryFull("declared backup registry capacity reached")
            await conn.execute(
                "INSERT INTO schema_backup_manifests_v1(backup_id,source_scope,snapshot_id,quiesce_digest,status,pending_json) VALUES($1,$2::jsonb,$3,$4,'PENDING',$5::jsonb)",
                backup_id,
                canonical(pending["source_scope"]),
                pending["snapshot_id"],
                pending["quiesce_manifest_digest"],
                canonical(pending),
            )
        return backup_id

    async def finish_backup(self, backup_id, manifest):
        manifest_digest = digest(manifest)
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(f'SET LOCAL search_path="{self.schema}",pg_catalog')
            row = await conn.fetchrow(
                "SELECT * FROM schema_backup_manifests_v1 WHERE backup_id=$1 FOR UPDATE",
                backup_id,
            )
            if row is None:
                raise RestoreContractError("pending backup not found")
            if row["status"] == "COMPLETED":
                if row["manifest_digest"] != manifest_digest:
                    raise RestoreContractError("backup finalization drift")
                return manifest_digest
            if (
                row["status"] != "PENDING"
                or manifest["backup_id"] != backup_id
                or any(
                    manifest.get(key) != value
                    for key, value in decoded(row["pending_json"]).items()
                )
            ):
                raise RestoreContractError("backup finalization identity drift")
            await conn.execute(
                "UPDATE schema_backup_manifests_v1 SET status='COMPLETED',manifest_json=$2::jsonb,manifest_digest=$3,artifact_sha256=$4 WHERE backup_id=$1",
                backup_id,
                canonical(manifest),
                manifest_digest,
                manifest["artifact_sha256"],
            )
        return manifest_digest

    async def completed_backup(self, backup_id):
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT * FROM "{self.schema}".schema_backup_manifests_v1 WHERE backup_id=$1',
                backup_id,
            )
        if row is None or row["status"] != "COMPLETED":
            raise RestoreContractError("completed backup required")
        manifest = decoded(row["manifest_json"])
        if digest(manifest) != row["manifest_digest"]:
            raise RestoreContractError("backup manifest digest drift")
        return manifest, row["manifest_digest"]

    async def reject_backup(self, backup_id: str, error_type: str) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(f'SET LOCAL search_path="{self.schema}",pg_catalog')
            row = await conn.fetchrow(
                "SELECT status,error_type FROM schema_backup_manifests_v1 WHERE backup_id=$1 FOR UPDATE",
                backup_id,
            )
            if row is None or row["status"] == "COMPLETED":
                raise RestoreContractError("pending backup required for rejection")
            if row["status"] == "REJECTED":
                if row["error_type"] != error_type:
                    raise RestoreContractError("backup rejection identity drift")
                return
            await conn.execute(
                "UPDATE schema_backup_manifests_v1 SET status='REJECTED',error_type=$2 WHERE backup_id=$1",
                backup_id,
                error_type,
            )

    async def mint_grant(self, backup_id, source_authority, target_authority):
        manifest, manifest_digest = await self.completed_backup(backup_id)
        source_scope = await database_scope(
            source_authority.pool, source_authority.schema
        )
        if source_scope != manifest["source_scope"]:
            raise RestoreContractError("source backup scope drift")
        finalize_digest = await source_authority.assert_cutover_retired(
            manifest["quiesce_manifest_digest"],
            CanonicalReleaseCuts(manifest["cuts"], digest(manifest["cuts"])),
        )
        target_scope = await database_scope(
            target_authority.pool, target_authority.schema
        )
        if target_scope["system_id"] == source_scope["system_id"]:
            raise RestoreContractError(
                "restore target must be a separate PostgreSQL system"
            )
        if target_scope["schema"] != source_scope["schema"]:
            raise RestoreContractError("selected restore profile preserves schema name")
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(f'SET LOCAL search_path="{self.schema}",pg_catalog')
            issuer = await conn.fetchrow("SELECT * FROM restore_issuer_v1 FOR UPDATE")
            prior = await conn.fetchrow(
                "SELECT * FROM schema_restore_grants_v1 WHERE source_system_id=$1 AND source_database=$2 AND source_schema=$3 AND run_id=$4",
                source_scope["system_id"],
                source_scope["database"],
                source_scope["schema"],
                manifest["run_id"],
            )
            if prior is not None:
                witness = BoundGrantWitness.from_dict(decoded(prior["witness_json"]))
                claims = witness.claims
                if (
                    claims.backup_manifest_sha256,
                    claims.target_system_id,
                    claims.target_database,
                    claims.target_schema,
                ) != (
                    manifest_digest,
                    target_scope["system_id"],
                    target_scope["database"],
                    target_scope["schema"],
                ):
                    raise RestoreGrantConflict(
                        "logical run already granted to another restore target"
                    )
                witness.verify(issuer["public_key_b64"])
                if witness.witness_digest != prior["witness_digest"]:
                    raise RestoreContractError("stored restore grant digest drift")
                return witness
            async with (
                target_authority.pool.acquire() as target_conn,
                target_conn.transaction(isolation="repeatable_read", readonly=True),
            ):
                fingerprint = await schema_identity(target_conn, target_scope["schema"])
                await assert_retained_payload_integrity(target_conn)
            if fingerprint["sha256"] != manifest["schema_digest"]:
                raise RestoreContractError(
                    "restored schema definition differs from backup"
                )
            if (
                cuts_document(await target_authority.snapshot_cuts(manifest["run_id"]))
                != manifest["cuts"]
            ):
                raise RestoreContractError("restored authority differs from snapshot")
            head = await target_authority.project_head()
            if (
                head["state"] != "QUIESCED"
                or head["restore_public_key_b64"] != issuer["public_key_b64"]
            ):
                raise RestoreContractError(
                    "target must retain quiesced pinned authority"
                )
            ordinal = await conn.fetchval(
                "SELECT nextval(pg_get_serial_sequence('schema_restore_grants_v1','registry_ordinal'))"
            )
            claims = RestoreGrantClaims(
                grant_id="restore-grant:" + digest([backup_id, target_scope, ordinal]),
                registry_ordinal=ordinal,
                issuer_key_id=issuer["key_id"],
                source_system_id=source_scope["system_id"],
                source_database=source_scope["database"],
                source_schema=source_scope["schema"],
                target_system_id=target_scope["system_id"],
                target_database=target_scope["database"],
                target_schema=target_scope["schema"],
                deployment_id=manifest["deployment_id"],
                revision_id=manifest["revision_id"],
                run_id=manifest["run_id"],
                source_release_generation=manifest["release_generation"],
                source_release_fence=manifest["release_fence"],
                target_release_generation=manifest["release_generation"] + 1,
                backup_artifact_sha256=manifest["artifact_sha256"],
                backup_manifest_sha256=manifest_digest,
                schema_digest=manifest["schema_digest"],
                cuts_json=canonical(manifest["cuts"]),
                cuts_digest=digest(manifest["cuts"]),
                quiesce_manifest_digest=manifest["quiesce_manifest_digest"],
                source_finalize_digest=finalize_digest,
            )
            witness = BoundGrantWitness.sign(
                claims,
                Ed25519PrivateKey.from_private_bytes(bytes(issuer["private_key"])),
            )
            await conn.execute(
                "INSERT INTO schema_restore_grants_v1(registry_ordinal,source_system_id,source_database,source_schema,run_id,backup_id,witness_json,witness_digest) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)",
                ordinal,
                source_scope["system_id"],
                source_scope["database"],
                source_scope["schema"],
                manifest["run_id"],
                backup_id,
                canonical(witness.to_dict()),
                witness.witness_digest,
            )
        return witness
