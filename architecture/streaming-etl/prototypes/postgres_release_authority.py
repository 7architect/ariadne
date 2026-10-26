"""Release/session fencing and transactional R4-A -> R4-B migration prototype."""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

import asyncpg
from postgres_prefect_attempts import PostgresPrefectAttempts
from postgres_restore_contracts import (
    BoundGrantWitness,
    RestoreContractError,
    canonical,
    digest,
    key_id,
)
from postgres_restore_integrity import assert_retained_payload_integrity
from postgres_schema_identity import schema_identity


class ReleaseProtocolError(RuntimeError):
    pass


class ReleaseCompatibilityError(ReleaseProtocolError):
    pass


class ReleaseSessionFenced(ReleaseProtocolError):
    pass


class ReleaseQuiesceBusy(ReleaseProtocolError):
    pass


class ReleaseMigrationConflict(ReleaseProtocolError):
    pass


@dataclass(frozen=True)
class RuntimeContract:
    runtime_version: str
    writer_semantics_digest: str
    feature_digest: str


@dataclass(frozen=True)
class ReleaseSession:
    session_id: str
    ordinal: int
    fence_epoch: int
    runtime_version: str
    actor_id: str
    role: str


@dataclass(frozen=True)
class CanonicalReleaseCuts:
    material: dict[str, Any]
    digest: str


@dataclass(frozen=True)
class QuiescedManifest:
    release_id: str
    run_id: str
    release_generation: int
    fence_epoch: int
    schema_version: str
    restore_public_key_b64: str
    restore_key_id: str
    cuts: CanonicalReleaseCuts

    @property
    def manifest_digest(self) -> str:
        return digest(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "release_id": self.release_id,
            "run_id": self.run_id,
            "release_generation": self.release_generation,
            "fence_epoch": self.fence_epoch,
            "schema_version": self.schema_version,
            "restore_public_key_b64": self.restore_public_key_b64,
            "restore_key_id": self.restore_key_id,
            "cuts": self.cuts.material,
            "cuts_digest": self.cuts.digest,
        }


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("unsafe PostgreSQL identifier")
    return value


def _contract(value: RuntimeContract) -> RuntimeContract:
    if not value.runtime_version or not value.runtime_version.strip():
        raise ValueError("runtime version required")
    for item in (value.writer_semantics_digest, value.feature_digest):
        if not re.fullmatch(r"[0-9a-f]{64}", item):
            raise ValueError("contract digests must be lowercase sha256")
    return value


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, asyncpg.Record):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


class PostgresReleaseAuthority:
    RELEASE_ID = "schema-release"
    _OWN_TABLES: ClassVar[set[str]] = {
        "release_heads_v1",
        "release_runtime_compatibility_v1",
        "release_sessions_v1",
        "release_session_archive_v1",
        "release_migrations_v1",
        "release_grant_witnesses_v1",
        "release_grant_archive_v1",
        "release_source_finalizations_v1",
    }

    def __init__(self, pool: asyncpg.Pool, schema: str):
        self.pool = pool
        self.schema = _identifier(schema)

    @classmethod
    async def install(
        cls,
        pool: asyncpg.Pool,
        schema: str,
        *,
        schema_version: str,
        runtime_contracts: tuple[RuntimeContract, ...],
        restore_public_key_b64: str,
        max_hot_sessions: int = 16,
        max_migrations: int = 16,
        max_compatibility_rows: int = 64,
    ) -> PostgresReleaseAuthority:
        authority = cls(pool, schema)
        if not schema_version or not restore_public_key_b64:
            raise ValueError("schema version and restore issuer key required")
        restore_key_id = key_id(restore_public_key_b64)
        if max_hot_sessions < 2 or max_hot_sessions > 128:
            raise ValueError("max_hot_sessions must be between 2 and 128")
        if not 1 <= max_migrations <= 128:
            raise ValueError("max_migrations must be between 1 and 128")
        if not 1 <= max_compatibility_rows <= 512:
            raise ValueError("max_compatibility_rows must be between 1 and 512")
        contracts = tuple(_contract(item) for item in runtime_contracts)
        if not contracts or len({item.runtime_version for item in contracts}) != len(
            contracts
        ):
            raise ValueError("unique runtime contracts required")
        if len(contracts) > max_compatibility_rows:
            raise ValueError("initial runtime compatibility capacity exceeded")
        semantics = {item.writer_semantics_digest for item in contracts}
        if len(semantics) != 1:
            raise ValueError("initial mixed writers require identical semantics")
        await authority._initialize(
            schema_version,
            contracts,
            restore_public_key_b64,
            restore_key_id,
            max_hot_sessions,
            max_migrations,
            max_compatibility_rows,
        )
        return authority

    @classmethod
    def connect_existing(
        cls, pool: asyncpg.Pool, schema: str
    ) -> PostgresReleaseAuthority:
        return cls(pool, schema)

    async def open_bound_pool(
        self,
        dsn: str,
        session_id: str,
        *,
        min_size: int = 1,
        max_size: int = 12,
    ) -> asyncpg.Pool:
        """Return a pool that rebinds the durable session on every checkout."""

        async def setup(conn: asyncpg.Connection) -> None:
            await conn.execute(
                "SELECT set_config('ariadne.release_session',$1,false)", session_id
            )

        return await asyncpg.create_pool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            server_settings={"search_path": f'"{self.schema}",public'},
            setup=setup,
        )

    async def project_head(self) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT release_id,state,schema_version,pending_schema_version,"
                "release_generation,fence_epoch,writer_semantics_digest,feature_digest,"
                "restore_public_key_b64,restore_key_id,source_system_id,source_database,"
                "source_schema,guarded_tables,quiesced_run_id,quiesced_cuts_digest,"
                "quiesce_manifest_digest,source_finalize_digest,max_migrations,"
                "max_compatibility_rows "
                "FROM release_heads_v1 WHERE release_id=$1",
                self.RELEASE_ID,
            )
        if row is None:
            raise ReleaseProtocolError("release authority missing")
        return _json_value(row)

    async def _initialize(
        self,
        schema_version: str,
        contracts: tuple[RuntimeContract, ...],
        restore_public_key_b64: str,
        restore_key_id: str,
        max_hot_sessions: int,
        max_migrations: int,
        max_compatibility_rows: int,
    ) -> None:
        schema = self.schema
        ddl = f"""
        CREATE TABLE IF NOT EXISTS release_heads_v1(
          release_id text PRIMARY KEY,
          state text NOT NULL CHECK(state IN('ACTIVE','QUIESCED','CUTOVER_RETIRED')),
          schema_version text NOT NULL,pending_schema_version text,
          release_generation bigint NOT NULL CHECK(release_generation>0),
          fence_epoch bigint NOT NULL CHECK(fence_epoch>0),
          next_session_ordinal bigint NOT NULL CHECK(next_session_ordinal>0),
          writer_semantics_digest text NOT NULL,feature_digest text NOT NULL,
          restore_public_key_b64 text NOT NULL,restore_key_id text NOT NULL,
          source_system_id text NOT NULL,source_database text NOT NULL,
          source_schema text NOT NULL,
          guarded_tables jsonb NOT NULL,max_hot_sessions bigint NOT NULL
            CHECK(max_hot_sessions BETWEEN 2 AND 128),
          max_migrations bigint NOT NULL CHECK(max_migrations BETWEEN 1 AND 128),
          max_compatibility_rows bigint NOT NULL
            CHECK(max_compatibility_rows BETWEEN 1 AND 512),
          quiesced_cuts jsonb,quiesced_cuts_digest text,
          quiesced_run_id text,quiesce_manifest_digest text,
          source_finalize_digest text,
          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          updated_at timestamptz NOT NULL DEFAULT clock_timestamp());
        CREATE TABLE IF NOT EXISTS release_runtime_compatibility_v1(
          schema_version text NOT NULL,runtime_version text NOT NULL,
          writer_semantics_digest text NOT NULL,feature_digest text NOT NULL,
          enabled boolean NOT NULL DEFAULT true,
          PRIMARY KEY(schema_version,runtime_version));
        CREATE TABLE IF NOT EXISTS release_sessions_v1(
          session_id text PRIMARY KEY,ordinal bigint NOT NULL UNIQUE CHECK(ordinal>0),
          fence_epoch bigint NOT NULL CHECK(fence_epoch>0),
          runtime_version text NOT NULL,actor_id text NOT NULL,role text NOT NULL,
          status text NOT NULL CHECK(status IN('ACTIVE','CLOSED','REVOKED')),
          lease_expires_at timestamptz NOT NULL,
          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),closed_at timestamptz);
        CREATE TABLE IF NOT EXISTS release_session_archive_v1(
          release_id text PRIMARY KEY REFERENCES release_heads_v1,
          retired_through bigint NOT NULL DEFAULT 0,retired_count bigint NOT NULL DEFAULT 0,
          rolling_digest text NOT NULL DEFAULT repeat('0',64));
        CREATE TABLE IF NOT EXISTS release_migrations_v1(
          migration_id text PRIMARY KEY,from_version text NOT NULL,to_version text NOT NULL,
          artifact_digest text NOT NULL,guarded_tables_digest text NOT NULL,
          status text NOT NULL CHECK(status IN('EXPANDED','CUTOVER')),
          identity_digest text NOT NULL UNIQUE,
          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),cutover_at timestamptz);
        CREATE TABLE IF NOT EXISTS release_grant_witnesses_v1(
          grant_id text PRIMARY KEY,grant_digest text NOT NULL UNIQUE,
          target_system_id text NOT NULL,target_database text NOT NULL,
          target_schema text NOT NULL,cuts_digest text NOT NULL,
          source_release_generation bigint NOT NULL CHECK(source_release_generation>0),
          target_release_generation bigint NOT NULL CHECK(target_release_generation>1),
          source_finalize_digest text NOT NULL,
          activated_fence bigint NOT NULL,session_id text NOT NULL UNIQUE,
          created_at timestamptz NOT NULL DEFAULT clock_timestamp());
        CREATE TABLE IF NOT EXISTS release_grant_archive_v1(
          release_id text PRIMARY KEY REFERENCES release_heads_v1,
          retired_through_generation bigint NOT NULL DEFAULT 0,
          retired_count bigint NOT NULL DEFAULT 0,
          latest_source_finalize_digest text,
          rolling_digest text NOT NULL DEFAULT repeat('0',64));
        CREATE TABLE IF NOT EXISTS release_source_finalizations_v1(
          finalize_id text PRIMARY KEY,run_id text NOT NULL UNIQUE,
          quiesce_manifest_digest text NOT NULL,cuts_digest text NOT NULL,
          status text NOT NULL CHECK(status IN('STARTED','COMPLETED')),
          outcome_digest text,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          completed_at timestamptz);

        CREATE OR REPLACE FUNCTION release_write_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        DECLARE h release_heads_v1%ROWTYPE;
        DECLARE s release_sessions_v1%ROWTYPE;
        DECLARE sid text;
        BEGIN
          SELECT * INTO h FROM release_heads_v1
            WHERE release_id='schema-release' FOR KEY SHARE;
          IF h.release_id IS NULL THEN RAISE EXCEPTION 'release authority missing'; END IF;
          IF h.state='QUIESCED'
             AND current_setting('ariadne.release_finalize',true) IS NOT NULL
             AND TG_TABLE_NAME=ANY(ARRAY[
               'control_worker_fences_v1','control_runs_v1','control_commands_v1',
               'control_capabilities_v1','consumers','runtime_invocations',
               'prefect_attempt_source_caps_v1','prefect_attempt_operators_v1',
               'prefect_execution_attempts_v1'])
             AND EXISTS(
               SELECT 1 FROM release_source_finalizations_v1 f
               WHERE f.finalize_id=current_setting('ariadne.release_finalize',true)
                 AND f.status='STARTED'
                 AND f.quiesce_manifest_digest=h.quiesce_manifest_digest)
          THEN RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END; END IF;
          IF h.state<>'ACTIVE' THEN RAISE EXCEPTION 'release is not writable'; END IF;
          sid=current_setting('ariadne.release_session',true);
          IF sid IS NULL OR sid='' THEN RAISE EXCEPTION 'release session missing'; END IF;
          SELECT * INTO s FROM release_sessions_v1 WHERE session_id=sid;
          IF s.session_id IS NULL OR s.status<>'ACTIVE'
             OR s.fence_epoch<>h.fence_epoch OR s.lease_expires_at<=clock_timestamp()
          THEN RAISE EXCEPTION 'release session fenced'; END IF;
          IF NOT EXISTS(
            SELECT 1 FROM release_runtime_compatibility_v1 c
            WHERE c.schema_version=h.schema_version AND c.runtime_version=s.runtime_version
              AND c.enabled AND c.writer_semantics_digest=h.writer_semantics_digest
              AND c.feature_digest=h.feature_digest)
          THEN RAISE EXCEPTION 'release session incompatible'; END IF;
          RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
        END $$;

        CREATE OR REPLACE FUNCTION release_head_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        BEGIN
          IF TG_OP='INSERT' THEN
            IF EXISTS(SELECT 1 FROM release_heads_v1 WHERE release_id=NEW.release_id)
            THEN RAISE EXCEPTION 'release head is create-once'; END IF;
            RETURN NEW;
          END IF;
          IF TG_OP='DELETE' THEN RAISE EXCEPTION 'release head is immutable'; END IF;
          IF NEW.release_id IS DISTINCT FROM OLD.release_id
             OR NEW.restore_public_key_b64 IS DISTINCT FROM OLD.restore_public_key_b64
             OR NEW.restore_key_id IS DISTINCT FROM OLD.restore_key_id
             OR NEW.max_hot_sessions IS DISTINCT FROM OLD.max_hot_sessions
             OR NEW.max_migrations IS DISTINCT FROM OLD.max_migrations
             OR NEW.max_compatibility_rows IS DISTINCT FROM OLD.max_compatibility_rows
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR NEW.release_generation<OLD.release_generation
             OR NEW.fence_epoch<OLD.fence_epoch
             OR NEW.next_session_ordinal<OLD.next_session_ordinal
             OR NEW.next_session_ordinal>OLD.next_session_ordinal+1
          THEN RAISE EXCEPTION 'release head identity or monotonicity violation'; END IF;

          IF OLD.state='ACTIVE' AND NEW.state='ACTIVE'
             AND NEW.schema_version=OLD.schema_version THEN
            IF NEW.release_generation<>OLD.release_generation
               OR NEW.fence_epoch<>OLD.fence_epoch
               OR NEW.writer_semantics_digest<>OLD.writer_semantics_digest
               OR NEW.feature_digest<>OLD.feature_digest
               OR NEW.quiesced_cuts IS DISTINCT FROM OLD.quiesced_cuts
               OR NEW.quiesced_cuts_digest IS DISTINCT FROM OLD.quiesced_cuts_digest
               OR NEW.quiesced_run_id IS DISTINCT FROM OLD.quiesced_run_id
               OR NEW.quiesce_manifest_digest IS DISTINCT FROM OLD.quiesce_manifest_digest
               OR NEW.source_finalize_digest IS DISTINCT FROM OLD.source_finalize_digest
               OR (NEW.source_system_id,NEW.source_database,NEW.source_schema)
                  IS DISTINCT FROM
                  (OLD.source_system_id,OLD.source_database,OLD.source_schema)
               OR (OLD.pending_schema_version IS NOT NULL
                   AND NEW.pending_schema_version IS DISTINCT FROM OLD.pending_schema_version)
               OR (NEW.guarded_tables IS DISTINCT FROM OLD.guarded_tables
                   AND NOT (OLD.pending_schema_version IS NULL
                            AND NEW.pending_schema_version IS NOT NULL))
               OR (OLD.pending_schema_version IS NULL
                   AND NEW.pending_schema_version IS NOT NULL
                   AND NOT EXISTS(
                     SELECT 1 FROM release_migrations_v1 m
                     WHERE m.from_version=OLD.schema_version
                       AND m.to_version=NEW.pending_schema_version
                       AND m.status='EXPANDED'))
            THEN RAISE EXCEPTION 'illegal active release mutation'; END IF;
            RETURN NEW;
          END IF;

          IF OLD.state='ACTIVE' AND NEW.state='ACTIVE'
             AND NEW.schema_version<>OLD.schema_version THEN
            IF OLD.pending_schema_version IS DISTINCT FROM NEW.schema_version
               OR NEW.pending_schema_version IS NOT NULL
               OR NEW.release_generation<>OLD.release_generation+1
               OR NEW.fence_epoch<>OLD.fence_epoch+1
               OR NEW.next_session_ordinal<>OLD.next_session_ordinal
               OR NEW.guarded_tables IS DISTINCT FROM OLD.guarded_tables
               OR NEW.quiesced_cuts IS DISTINCT FROM OLD.quiesced_cuts
               OR NEW.quiesced_cuts_digest IS DISTINCT FROM OLD.quiesced_cuts_digest
               OR NEW.quiesced_run_id IS DISTINCT FROM OLD.quiesced_run_id
               OR NEW.quiesce_manifest_digest IS DISTINCT FROM OLD.quiesce_manifest_digest
               OR NEW.source_finalize_digest IS DISTINCT FROM OLD.source_finalize_digest
               OR (NEW.source_system_id,NEW.source_database,NEW.source_schema)
                  IS DISTINCT FROM
                  (OLD.source_system_id,OLD.source_database,OLD.source_schema)
               OR NOT EXISTS(
                 SELECT 1 FROM release_migrations_v1 m
                 WHERE m.from_version=OLD.schema_version
                   AND m.to_version=NEW.schema_version AND m.status='EXPANDED')
            THEN RAISE EXCEPTION 'illegal release cutover'; END IF;
            RETURN NEW;
          END IF;

          IF OLD.state='ACTIVE' AND NEW.state='QUIESCED' THEN
            IF NEW.schema_version<>OLD.schema_version
               OR NEW.pending_schema_version IS DISTINCT FROM OLD.pending_schema_version
               OR NEW.release_generation<>OLD.release_generation
               OR NEW.fence_epoch<>OLD.fence_epoch+1
               OR NEW.next_session_ordinal<>OLD.next_session_ordinal
               OR NEW.writer_semantics_digest<>OLD.writer_semantics_digest
               OR NEW.feature_digest<>OLD.feature_digest
               OR NEW.guarded_tables IS DISTINCT FROM OLD.guarded_tables
               OR NEW.quiesced_cuts IS NULL OR NEW.quiesced_cuts_digest IS NULL
               OR NEW.quiesced_run_id IS NULL OR NEW.quiesce_manifest_digest IS NULL
               OR NEW.source_finalize_digest IS DISTINCT FROM OLD.source_finalize_digest
               OR (NEW.source_system_id,NEW.source_database,NEW.source_schema)
                  IS DISTINCT FROM
                  (OLD.source_system_id,OLD.source_database,OLD.source_schema)
            THEN RAISE EXCEPTION 'illegal release quiesce'; END IF;
            RETURN NEW;
          END IF;

          IF OLD.state='QUIESCED' AND NEW.state='QUIESCED' THEN
            IF NEW.schema_version<>OLD.schema_version
               OR NEW.pending_schema_version IS DISTINCT FROM OLD.pending_schema_version
               OR NEW.release_generation<>OLD.release_generation
               OR NEW.fence_epoch<>OLD.fence_epoch
               OR NEW.next_session_ordinal<>OLD.next_session_ordinal
               OR NEW.writer_semantics_digest<>OLD.writer_semantics_digest
               OR NEW.feature_digest<>OLD.feature_digest
               OR NEW.guarded_tables IS DISTINCT FROM OLD.guarded_tables
               OR NEW.quiesced_cuts IS DISTINCT FROM OLD.quiesced_cuts
               OR NEW.quiesced_cuts_digest IS DISTINCT FROM OLD.quiesced_cuts_digest
               OR NEW.quiesced_run_id IS DISTINCT FROM OLD.quiesced_run_id
               OR NEW.quiesce_manifest_digest IS DISTINCT FROM OLD.quiesce_manifest_digest
               OR (NEW.source_system_id,NEW.source_database,NEW.source_schema)
                  IS DISTINCT FROM
                  (OLD.source_system_id,OLD.source_database,OLD.source_schema)
               OR (NEW.source_finalize_digest IS DISTINCT FROM OLD.source_finalize_digest
                   AND NOT EXISTS(
                     SELECT 1 FROM release_source_finalizations_v1 f
                     WHERE f.outcome_digest=NEW.source_finalize_digest
                       AND f.status='COMPLETED'
                       AND f.quiesce_manifest_digest=NEW.quiesce_manifest_digest))
            THEN RAISE EXCEPTION 'illegal quiesced release mutation'; END IF;
            RETURN NEW;
          END IF;

          IF OLD.state='QUIESCED' AND NEW.state='CUTOVER_RETIRED' THEN
            IF NEW.release_generation<>OLD.release_generation
               OR NEW.fence_epoch<>OLD.fence_epoch
               OR NEW.schema_version<>OLD.schema_version
               OR NEW.pending_schema_version IS DISTINCT FROM OLD.pending_schema_version
               OR NEW.next_session_ordinal<>OLD.next_session_ordinal
               OR NEW.writer_semantics_digest<>OLD.writer_semantics_digest
               OR NEW.feature_digest<>OLD.feature_digest
               OR NEW.guarded_tables IS DISTINCT FROM OLD.guarded_tables
               OR NEW.quiesced_cuts IS DISTINCT FROM OLD.quiesced_cuts
               OR NEW.quiesced_cuts_digest IS DISTINCT FROM OLD.quiesced_cuts_digest
               OR NEW.quiesced_run_id IS DISTINCT FROM OLD.quiesced_run_id
               OR NEW.quiesce_manifest_digest IS DISTINCT FROM OLD.quiesce_manifest_digest
               OR NEW.source_finalize_digest IS NULL
               OR (NEW.source_system_id,NEW.source_database,NEW.source_schema)
                  IS DISTINCT FROM
                  (OLD.source_system_id,OLD.source_database,OLD.source_schema)
               OR NOT EXISTS(
                 SELECT 1 FROM release_source_finalizations_v1 f
                 WHERE f.outcome_digest=NEW.source_finalize_digest
                   AND f.status='COMPLETED')
            THEN RAISE EXCEPTION 'illegal source retirement'; END IF;
            RETURN NEW;
          END IF;

          IF OLD.state='QUIESCED' AND NEW.state='ACTIVE' THEN
            IF NEW.release_generation<>OLD.release_generation+1
               OR NEW.fence_epoch<>OLD.fence_epoch+1
               OR NEW.schema_version<>OLD.schema_version
               OR NEW.pending_schema_version IS DISTINCT FROM OLD.pending_schema_version
               OR NEW.next_session_ordinal<>OLD.next_session_ordinal+1
               OR NEW.writer_semantics_digest<>OLD.writer_semantics_digest
               OR NEW.feature_digest<>OLD.feature_digest
               OR NEW.guarded_tables IS DISTINCT FROM OLD.guarded_tables
               OR NEW.quiesced_cuts IS DISTINCT FROM OLD.quiesced_cuts
               OR NEW.quiesced_cuts_digest IS DISTINCT FROM OLD.quiesced_cuts_digest
               OR NEW.quiesced_run_id IS DISTINCT FROM OLD.quiesced_run_id
               OR NEW.quiesce_manifest_digest IS DISTINCT FROM OLD.quiesce_manifest_digest
               OR NEW.source_finalize_digest IS NULL
               OR NEW.source_system_id IS DISTINCT FROM
                  (SELECT system_identifier::text FROM pg_control_system())
               OR NEW.source_database IS DISTINCT FROM current_database()
               OR NEW.source_schema IS DISTINCT FROM OLD.source_schema
            THEN RAISE EXCEPTION 'illegal restore activation'; END IF;
            RETURN NEW;
          END IF;

          IF OLD.state='CUTOVER_RETIRED' AND NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NEW;
          END IF;
          RAISE EXCEPTION 'illegal release lifecycle transition';
        END $$;

        CREATE OR REPLACE FUNCTION release_session_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        DECLARE h release_heads_v1%ROWTYPE;
        BEGIN
          IF TG_OP='INSERT' THEN
            IF EXISTS(SELECT 1 FROM release_sessions_v1 WHERE session_id=NEW.session_id)
            THEN RAISE EXCEPTION 'release session is create-once'; END IF;
            SELECT * INTO h FROM release_heads_v1 WHERE release_id='schema-release';
            IF NEW.status<>'ACTIVE' OR h.state<>'ACTIVE' OR NEW.fence_epoch<>h.fence_epoch
               OR NEW.ordinal NOT IN(h.next_session_ordinal,h.next_session_ordinal-1)
               OR NOT EXISTS(
                 SELECT 1 FROM release_runtime_compatibility_v1 c
                 WHERE c.schema_version=h.schema_version
                   AND c.runtime_version=NEW.runtime_version AND c.enabled
                   AND c.writer_semantics_digest=h.writer_semantics_digest
                   AND c.feature_digest=h.feature_digest)
            THEN RAISE EXCEPTION 'release session admission rejected'; END IF;
            RETURN NEW;
          END IF;
          IF TG_OP='DELETE' THEN
            IF OLD.status='ACTIVE' THEN RAISE EXCEPTION 'active release session retained'; END IF;
            RETURN OLD;
          END IF;
          IF NEW.session_id<>OLD.session_id OR NEW.ordinal<>OLD.ordinal
             OR NEW.fence_epoch<>OLD.fence_epoch
             OR NEW.runtime_version<>OLD.runtime_version OR NEW.actor_id<>OLD.actor_id
             OR NEW.role<>OLD.role OR NEW.created_at<>OLD.created_at
          THEN RAISE EXCEPTION 'release session identity immutable'; END IF;
          IF OLD.status='ACTIVE' AND NEW.status='ACTIVE' THEN
            SELECT * INTO h FROM release_heads_v1 WHERE release_id='schema-release';
            IF NEW.lease_expires_at<OLD.lease_expires_at OR h.state<>'ACTIVE'
               OR h.fence_epoch<>OLD.fence_epoch
            THEN RAISE EXCEPTION 'release session renewal rejected'; END IF;
          ELSIF OLD.status='ACTIVE' AND NEW.status IN('CLOSED','REVOKED') THEN
            IF NEW.lease_expires_at<>OLD.lease_expires_at OR NEW.closed_at IS NULL
            THEN RAISE EXCEPTION 'release session terminal transition invalid'; END IF;
          ELSE
            RAISE EXCEPTION 'release session terminal state immutable';
          END IF;
          RETURN NEW;
        END $$;

        CREATE OR REPLACE FUNCTION release_migration_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        BEGIN
          IF TG_OP='INSERT' THEN
            IF EXISTS(SELECT 1 FROM release_migrations_v1 WHERE migration_id=NEW.migration_id)
               OR NEW.status<>'EXPANDED' OR NEW.cutover_at IS NOT NULL
            THEN RAISE EXCEPTION 'migration create-only admission rejected'; END IF;
            RETURN NEW;
          END IF;
          IF TG_OP='DELETE' THEN RAISE EXCEPTION 'migration history immutable'; END IF;
          IF NEW.migration_id<>OLD.migration_id OR NEW.from_version<>OLD.from_version
             OR NEW.to_version<>OLD.to_version OR NEW.artifact_digest<>OLD.artifact_digest
             OR NEW.guarded_tables_digest<>OLD.guarded_tables_digest
             OR NEW.identity_digest<>OLD.identity_digest OR NEW.created_at<>OLD.created_at
             OR OLD.status<>'EXPANDED' OR NEW.status<>'CUTOVER'
             OR NEW.cutover_at IS NULL
          THEN RAISE EXCEPTION 'illegal migration transition'; END IF;
          RETURN NEW;
        END $$;

        CREATE OR REPLACE FUNCTION release_compatibility_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        DECLARE h release_heads_v1%ROWTYPE;
        BEGIN
          IF TG_OP='INSERT' THEN
            IF EXISTS(SELECT 1 FROM release_runtime_compatibility_v1 c
                      WHERE c.schema_version=NEW.schema_version
                        AND c.runtime_version=NEW.runtime_version)
            THEN RAISE EXCEPTION 'runtime compatibility is create-once'; END IF;
            SELECT * INTO h FROM release_heads_v1 WHERE release_id='schema-release';
            IF NOT NEW.enabled OR NOT (
                 (NEW.schema_version=h.schema_version
                  AND h.release_generation=1 AND h.next_session_ordinal=1
                  AND h.pending_schema_version IS NULL)
                 OR (NEW.schema_version=h.pending_schema_version AND EXISTS(
                   SELECT 1 FROM release_migrations_v1 m
                   WHERE m.from_version=h.schema_version
                     AND m.to_version=NEW.schema_version AND m.status='EXPANDED')))
            THEN RAISE EXCEPTION 'runtime compatibility admission rejected'; END IF;
            RETURN NEW;
          END IF;
          RAISE EXCEPTION 'runtime compatibility immutable';
        END $$;

        CREATE OR REPLACE FUNCTION release_finalization_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        DECLARE h release_heads_v1%ROWTYPE;
        BEGIN
          IF TG_OP='INSERT' THEN
            SELECT * INTO h FROM release_heads_v1 WHERE release_id='schema-release';
            IF EXISTS(SELECT 1 FROM release_source_finalizations_v1
                      WHERE finalize_id=NEW.finalize_id OR run_id=NEW.run_id)
               OR NEW.status<>'STARTED' OR NEW.outcome_digest IS NOT NULL
               OR h.state<>'QUIESCED' OR NEW.run_id<>h.quiesced_run_id
               OR NEW.quiesce_manifest_digest<>h.quiesce_manifest_digest
               OR NEW.cuts_digest<>h.quiesced_cuts_digest
            THEN RAISE EXCEPTION 'source finalization admission rejected'; END IF;
            RETURN NEW;
          END IF;
          IF TG_OP='DELETE' THEN RAISE EXCEPTION 'source finalization immutable'; END IF;
          IF NEW.finalize_id<>OLD.finalize_id OR NEW.run_id<>OLD.run_id
             OR NEW.quiesce_manifest_digest<>OLD.quiesce_manifest_digest
             OR NEW.cuts_digest<>OLD.cuts_digest OR NEW.created_at<>OLD.created_at
             OR OLD.status<>'STARTED' OR NEW.status<>'COMPLETED'
             OR NEW.outcome_digest IS NULL OR NEW.completed_at IS NULL
          THEN RAISE EXCEPTION 'source finalization transition invalid'; END IF;
          RETURN NEW;
        END $$;

        CREATE OR REPLACE FUNCTION release_grant_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        DECLARE h release_heads_v1%ROWTYPE;
        BEGIN
          IF TG_OP='DELETE' THEN
            SELECT * INTO h FROM release_heads_v1 WHERE release_id='schema-release';
            IF h.state<>'QUIESCED' OR NOT EXISTS(
              SELECT 1 FROM release_grant_archive_v1 a
              WHERE a.release_id=h.release_id
                AND a.retired_through_generation>=OLD.target_release_generation)
            THEN RAISE EXCEPTION 'restore grant witness retained'; END IF;
            RETURN OLD;
          END IF;
          IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'restore grant witness immutable'; END IF;
          IF EXISTS(SELECT 1 FROM release_grant_witnesses_v1
                    WHERE grant_id=NEW.grant_id OR grant_digest=NEW.grant_digest)
          THEN RAISE EXCEPTION 'restore grant witness create-once'; END IF;
          SELECT * INTO h FROM release_heads_v1 WHERE release_id='schema-release';
          IF h.state<>'ACTIVE' OR h.quiesced_cuts_digest IS NULL
             OR h.quiesce_manifest_digest IS NULL OR h.source_finalize_digest IS NULL
             OR NEW.activated_fence IS DISTINCT FROM h.fence_epoch
             OR NEW.target_release_generation IS DISTINCT FROM h.release_generation
             OR NEW.source_release_generation<>NEW.target_release_generation-1
             OR NEW.source_finalize_digest IS DISTINCT FROM h.source_finalize_digest
             OR NEW.target_system_id IS DISTINCT FROM
                (SELECT system_identifier::text FROM pg_control_system())
             OR NEW.target_database IS DISTINCT FROM current_database()
             OR NEW.target_schema IS DISTINCT FROM h.source_schema
             OR NEW.cuts_digest IS DISTINCT FROM h.quiesced_cuts_digest
             OR NOT EXISTS(
               SELECT 1 FROM release_sessions_v1 s WHERE s.session_id=NEW.session_id
                 AND s.status='ACTIVE' AND s.fence_epoch=NEW.activated_fence)
          THEN RAISE EXCEPTION 'restore grant witness admission rejected'; END IF;
          RETURN NEW;
        END $$;

        CREATE OR REPLACE FUNCTION release_grant_archive_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        BEGIN
          IF TG_OP='INSERT' THEN
            IF EXISTS(SELECT 1 FROM release_grant_archive_v1
                      WHERE release_id=NEW.release_id)
               OR NEW.retired_through_generation<>0 OR NEW.retired_count<>0
               OR NEW.latest_source_finalize_digest IS NOT NULL
               OR NEW.rolling_digest<>repeat('0',64)
            THEN RAISE EXCEPTION 'grant archive create-once admission rejected'; END IF;
            RETURN NEW;
          END IF;
          IF TG_OP='DELETE' THEN RAISE EXCEPTION 'grant archive immutable'; END IF;
          IF NEW.release_id<>OLD.release_id
             OR NEW.retired_through_generation<=OLD.retired_through_generation
             OR NEW.retired_count<=OLD.retired_count
             OR NEW.latest_source_finalize_digest IS NULL
             OR NEW.rolling_digest=OLD.rolling_digest
             OR NEW.rolling_digest !~ '^[0-9a-f]{{64}}$'
          THEN RAISE EXCEPTION 'grant archive transition invalid'; END IF;
          RETURN NEW;
        END $$;

        CREATE OR REPLACE FUNCTION release_archive_guard() RETURNS trigger
        LANGUAGE plpgsql
        SET search_path TO "{schema}",pg_catalog AS $$
        BEGIN
          IF TG_OP='INSERT' THEN
            IF EXISTS(SELECT 1 FROM release_session_archive_v1
                      WHERE release_id=NEW.release_id)
               OR NEW.retired_through<>0 OR NEW.retired_count<>0
               OR NEW.rolling_digest<>repeat('0',64)
            THEN RAISE EXCEPTION 'release archive create-once admission rejected'; END IF;
            RETURN NEW;
          END IF;
          IF TG_OP='DELETE' THEN RAISE EXCEPTION 'release archive immutable'; END IF;
          IF NEW.release_id<>OLD.release_id
             OR NEW.retired_through<OLD.retired_through
             OR NEW.retired_count<=OLD.retired_count
             OR NEW.rolling_digest=OLD.rolling_digest
             OR NEW.rolling_digest !~ '^[0-9a-f]{{64}}$'
          THEN RAISE EXCEPTION 'release archive transition invalid'; END IF;
          RETURN NEW;
        END $$;

        DROP TRIGGER IF EXISTS release_head_guard ON release_heads_v1;
        CREATE TRIGGER release_head_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_heads_v1 FOR EACH ROW EXECUTE FUNCTION release_head_guard();
        DROP TRIGGER IF EXISTS release_session_guard ON release_sessions_v1;
        CREATE TRIGGER release_session_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_sessions_v1 FOR EACH ROW EXECUTE FUNCTION release_session_guard();
        DROP TRIGGER IF EXISTS release_migration_guard ON release_migrations_v1;
        CREATE TRIGGER release_migration_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_migrations_v1 FOR EACH ROW EXECUTE FUNCTION release_migration_guard();
        DROP TRIGGER IF EXISTS release_compatibility_guard
          ON release_runtime_compatibility_v1;
        CREATE TRIGGER release_compatibility_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_runtime_compatibility_v1 FOR EACH ROW
          EXECUTE FUNCTION release_compatibility_guard();
        DROP TRIGGER IF EXISTS release_finalization_guard
          ON release_source_finalizations_v1;
        CREATE TRIGGER release_finalization_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_source_finalizations_v1 FOR EACH ROW
          EXECUTE FUNCTION release_finalization_guard();
        DROP TRIGGER IF EXISTS release_grant_guard ON release_grant_witnesses_v1;
        CREATE TRIGGER release_grant_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_grant_witnesses_v1 FOR EACH ROW EXECUTE FUNCTION release_grant_guard();
        DROP TRIGGER IF EXISTS release_grant_archive_guard ON release_grant_archive_v1;
        CREATE TRIGGER release_grant_archive_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_grant_archive_v1 FOR EACH ROW
          EXECUTE FUNCTION release_grant_archive_guard();
        DROP TRIGGER IF EXISTS release_archive_guard ON release_session_archive_v1;
        CREATE TRIGGER release_archive_guard BEFORE INSERT OR UPDATE OR DELETE
          ON release_session_archive_v1 FOR EACH ROW EXECUTE FUNCTION release_archive_guard();
        """
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(ddl)
            existing = await conn.fetchrow(
                "SELECT * FROM release_heads_v1 WHERE release_id=$1 FOR UPDATE",
                self.RELEASE_ID,
            )
            if existing is not None:
                migration_count = await conn.fetchval(
                    "SELECT count(*) FROM release_migrations_v1"
                )
                compatibility_count = await conn.fetchval(
                    "SELECT count(*) FROM release_runtime_compatibility_v1"
                )
                if (
                    migration_count > max_migrations
                    or compatibility_count > max_compatibility_rows
                ):
                    raise ReleaseProtocolError(
                        "stored release history exceeds configured capacity"
                    )
                stored_contracts = {
                    (
                        row["runtime_version"],
                        row["writer_semantics_digest"],
                        row["feature_digest"],
                    )
                    for row in await conn.fetch(
                        "SELECT runtime_version,writer_semantics_digest,feature_digest "
                        "FROM release_runtime_compatibility_v1 "
                        "WHERE schema_version=$1 AND enabled",
                        schema_version,
                    )
                }
                expected_contracts = {
                    (
                        item.runtime_version,
                        item.writer_semantics_digest,
                        item.feature_digest,
                    )
                    for item in contracts
                }
                discovered = await self._discover_tables(conn)
                expected = (
                    schema_version,
                    restore_public_key_b64,
                    restore_key_id,
                    max_hot_sessions,
                    max_migrations,
                    max_compatibility_rows,
                    contracts[0].writer_semantics_digest,
                    contracts[0].feature_digest,
                    canonical(discovered),
                    expected_contracts,
                )
                actual = (
                    existing["schema_version"],
                    existing["restore_public_key_b64"],
                    existing["restore_key_id"],
                    existing["max_hot_sessions"],
                    existing["max_migrations"],
                    existing["max_compatibility_rows"],
                    existing["writer_semantics_digest"],
                    existing["feature_digest"],
                    canonical(json.loads(existing["guarded_tables"])),
                    stored_contracts,
                )
                if actual != expected:
                    raise ReleaseMigrationConflict("release install identity drift")
                await self._attach_guards(conn, discovered)
                return
            if await conn.fetchval(
                "SELECT count(*) FROM release_migrations_v1"
            ) or await conn.fetchval(
                "SELECT count(*) FROM release_runtime_compatibility_v1"
            ):
                raise ReleaseProtocolError(
                    "release history exists without configured authority"
                )
            tables = await self._discover_tables(conn)
            system_id = str(
                await conn.fetchval("SELECT system_identifier FROM pg_control_system()")
            )
            database = await conn.fetchval("SELECT current_database()")
            common_semantics = contracts[0].writer_semantics_digest
            common_features = contracts[0].feature_digest
            if {item.feature_digest for item in contracts} != {common_features}:
                raise ValueError("initial mixed writers require identical features")
            await conn.execute(
                "INSERT INTO release_heads_v1 VALUES($1,'ACTIVE',$2,NULL,1,1,1,$3,$4,"
                "$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,NULL,NULL,NULL,NULL,NULL,"
                "clock_timestamp(),clock_timestamp())",
                self.RELEASE_ID,
                schema_version,
                common_semantics,
                common_features,
                restore_public_key_b64,
                restore_key_id,
                system_id,
                database,
                self.schema,
                canonical(tables),
                max_hot_sessions,
                max_migrations,
                max_compatibility_rows,
            )
            await conn.execute(
                "INSERT INTO release_session_archive_v1(release_id) VALUES($1)",
                self.RELEASE_ID,
            )
            await conn.execute(
                "INSERT INTO release_grant_archive_v1(release_id) VALUES($1)",
                self.RELEASE_ID,
            )
            for item in contracts:
                await conn.execute(
                    "INSERT INTO release_runtime_compatibility_v1 VALUES($1,$2,$3,$4,true)",
                    schema_version,
                    item.runtime_version,
                    item.writer_semantics_digest,
                    item.feature_digest,
                )
            await self._attach_guards(conn, tables)

    async def _discover_tables(self, conn: asyncpg.Connection) -> list[str]:
        rows = await conn.fetch(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema=$1 AND table_type='BASE TABLE' ORDER BY table_name",
            self.schema,
        )
        return [
            row["table_name"]
            for row in rows
            if row["table_name"] not in self._OWN_TABLES
        ]

    async def _attach_guards(self, conn: asyncpg.Connection, tables: list[str]) -> None:
        for table in tables:
            table = _identifier(table)
            await conn.execute(
                f'DROP TRIGGER IF EXISTS release_write_guard ON "{table}";'
                f"CREATE TRIGGER release_write_guard BEFORE INSERT OR UPDATE OR DELETE "
                f'ON "{table}" FOR EACH ROW EXECUTE FUNCTION release_write_guard()'
            )

    async def _locked_head(self, conn: asyncpg.Connection) -> asyncpg.Record:
        head = await conn.fetchrow(
            "SELECT * FROM release_heads_v1 WHERE release_id=$1 FOR UPDATE",
            self.RELEASE_ID,
        )
        if head is None:
            raise ReleaseProtocolError("release authority missing")
        return head

    async def _validate_session(
        self, conn: asyncpg.Connection, session_id: str, *, writable: bool = True
    ) -> tuple[asyncpg.Record, asyncpg.Record]:
        head = await self._locked_head(conn)
        session = await conn.fetchrow(
            "SELECT * FROM release_sessions_v1 WHERE session_id=$1 FOR UPDATE",
            session_id,
        )
        if session is None:
            raise ReleaseSessionFenced("release session is unknown or retired")
        if (
            session["status"] != "ACTIVE"
            or session["fence_epoch"] != head["fence_epoch"]
            or not await conn.fetchval(
                "SELECT lease_expires_at>clock_timestamp() FROM release_sessions_v1 "
                "WHERE session_id=$1",
                session_id,
            )
        ):
            raise ReleaseSessionFenced("release session is stale, expired or closed")
        if writable and head["state"] != "ACTIVE":
            raise ReleaseSessionFenced("release is not writable")
        compatible = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM release_runtime_compatibility_v1 "
            "WHERE schema_version=$1 AND runtime_version=$2 AND enabled "
            "AND writer_semantics_digest=$3 AND feature_digest=$4)",
            head["schema_version"],
            session["runtime_version"],
            head["writer_semantics_digest"],
            head["feature_digest"],
        )
        if not compatible:
            raise ReleaseCompatibilityError(
                "runtime is incompatible with current release"
            )
        return head, session

    async def open_session(
        self,
        *,
        runtime_version: str,
        actor_id: str,
        role: str,
        lease_seconds: float,
    ) -> ReleaseSession:
        if not runtime_version or not actor_id or not role or lease_seconds <= 0:
            raise ValueError("valid runtime, actor, role and positive lease required")
        async with self.pool.acquire() as conn, conn.transaction():
            head = await self._locked_head(conn)
            if head["state"] != "ACTIVE":
                raise ReleaseSessionFenced("release is not accepting sessions")
            await conn.execute(
                "UPDATE release_sessions_v1 SET status='REVOKED',"
                "closed_at=clock_timestamp() WHERE status='ACTIVE' "
                "AND lease_expires_at<=clock_timestamp()"
            )
            await self._retire_sessions(conn)
            compatible = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM release_runtime_compatibility_v1 "
                "WHERE schema_version=$1 AND runtime_version=$2 AND enabled "
                "AND writer_semantics_digest=$3 AND feature_digest=$4)",
                head["schema_version"],
                runtime_version,
                head["writer_semantics_digest"],
                head["feature_digest"],
            )
            if not compatible:
                raise ReleaseCompatibilityError("runtime is incompatible before DATA")
            existing = await conn.fetchrow(
                "SELECT * FROM release_sessions_v1 WHERE status='ACTIVE' "
                "AND fence_epoch=$1 AND runtime_version=$2 AND actor_id=$3 AND role=$4",
                head["fence_epoch"],
                runtime_version,
                actor_id,
                role,
            )
            if existing is not None:
                return ReleaseSession(
                    existing["session_id"],
                    existing["ordinal"],
                    existing["fence_epoch"],
                    existing["runtime_version"],
                    existing["actor_id"],
                    existing["role"],
                )
            active = await conn.fetchval(
                "SELECT count(*) FROM release_sessions_v1 WHERE status='ACTIVE' "
                "AND lease_expires_at>clock_timestamp()"
            )
            if active >= head["max_hot_sessions"]:
                raise ReleaseProtocolError("release session capacity exceeded")
            ordinal = head["next_session_ordinal"]
            session_id = (
                "release-session:"
                + str(ordinal)
                + ":"
                + digest(
                    [
                        self.schema,
                        head["fence_epoch"],
                        ordinal,
                        runtime_version,
                        actor_id,
                        role,
                    ]
                )
            )
            await conn.execute(
                "INSERT INTO release_sessions_v1(session_id,ordinal,fence_epoch,"
                "runtime_version,actor_id,role,status,lease_expires_at) "
                "VALUES($1,$2,$3,$4,$5,$6,'ACTIVE',"
                "clock_timestamp()+$7*interval '1 second')",
                session_id,
                ordinal,
                head["fence_epoch"],
                runtime_version,
                actor_id,
                role,
                lease_seconds,
            )
            await conn.execute(
                "UPDATE release_heads_v1 SET next_session_ordinal=next_session_ordinal+1,"
                "updated_at=clock_timestamp() WHERE release_id=$1",
                self.RELEASE_ID,
            )
            return ReleaseSession(
                session_id,
                ordinal,
                head["fence_epoch"],
                runtime_version,
                actor_id,
                role,
            )

    async def bind_transaction(self, conn: asyncpg.Connection, session_id: str) -> None:
        await conn.execute(
            "SELECT set_config('ariadne.release_session',$1,true)", session_id
        )

    async def bind_connection(self, conn: asyncpg.Connection, session_id: str) -> None:
        await conn.execute(
            "SELECT set_config('ariadne.release_session',$1,false)", session_id
        )

    async def renew_session(self, session_id: str, *, lease_seconds: float) -> None:
        if lease_seconds <= 0:
            raise ValueError("positive release lease required")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._validate_session(conn, session_id)
            await conn.execute(
                "UPDATE release_sessions_v1 SET lease_expires_at=GREATEST("
                "lease_expires_at,clock_timestamp()+$2*interval '1 second') "
                "WHERE session_id=$1",
                session_id,
                lease_seconds,
            )

    async def close_session(self, session_id: str) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM release_sessions_v1 WHERE session_id=$1 FOR UPDATE",
                session_id,
            )
            if row is None:
                raise ReleaseSessionFenced("release session is unknown or retired")
            if row["status"] == "ACTIVE":
                await conn.execute(
                    "UPDATE release_sessions_v1 SET status='CLOSED',"
                    "closed_at=clock_timestamp() WHERE session_id=$1",
                    session_id,
                )
            await self._retire_sessions(conn)

    async def _retire_sessions(self, conn: asyncpg.Connection) -> None:
        rows = await conn.fetch(
            "SELECT * FROM release_sessions_v1 WHERE status<>'ACTIVE' "
            "ORDER BY ordinal FOR UPDATE"
        )
        if not rows:
            return
        archive = await conn.fetchrow(
            "SELECT * FROM release_session_archive_v1 WHERE release_id=$1 FOR UPDATE",
            self.RELEASE_ID,
        )
        material = [
            [row["session_id"], row["ordinal"], row["fence_epoch"], row["status"]]
            for row in rows
        ]
        rolling = digest([archive["rolling_digest"], material])
        await conn.execute(
            "UPDATE release_session_archive_v1 SET retired_through=GREATEST(retired_through,$2),"
            "retired_count=retired_count+$3,rolling_digest=$4 WHERE release_id=$1",
            self.RELEASE_ID,
            rows[-1]["ordinal"],
            len(rows),
            rolling,
        )
        await conn.execute(
            "DELETE FROM release_sessions_v1 WHERE session_id=ANY($1::text[])",
            [row["session_id"] for row in rows],
        )

    async def _retire_grants(self, conn: asyncpg.Connection) -> None:
        rows = await conn.fetch(
            "SELECT * FROM release_grant_witnesses_v1 "
            "ORDER BY target_release_generation FOR UPDATE"
        )
        if not rows:
            return
        archive = await conn.fetchrow(
            "SELECT * FROM release_grant_archive_v1 WHERE release_id=$1 FOR UPDATE",
            self.RELEASE_ID,
        )
        material = [
            [
                row["grant_id"],
                row["grant_digest"],
                row["target_system_id"],
                row["target_database"],
                row["target_schema"],
                row["cuts_digest"],
                row["source_release_generation"],
                row["target_release_generation"],
                row["source_finalize_digest"],
                row["activated_fence"],
                row["session_id"],
            ]
            for row in rows
        ]
        await conn.execute(
            "UPDATE release_grant_archive_v1 SET "
            "retired_through_generation=$2,retired_count=retired_count+$3,"
            "rolling_digest=$4,latest_source_finalize_digest=$5 WHERE release_id=$1",
            self.RELEASE_ID,
            rows[-1]["target_release_generation"],
            len(rows),
            digest([archive["rolling_digest"], material]),
            rows[-1]["source_finalize_digest"],
        )
        await conn.execute(
            "DELETE FROM release_grant_witnesses_v1 WHERE grant_id=ANY($1::text[])",
            [row["grant_id"] for row in rows],
        )

    async def expand_prefect_attempts(
        self,
        session_id: str,
        attempts: PostgresPrefectAttempts,
        *,
        migration_id: str,
        from_version: str,
        to_version: str,
        artifact_digest: str,
        fault: Callable[[str], None] | None = None,
    ) -> None:
        if not all((migration_id, from_version, to_version)) or not re.fullmatch(
            r"[0-9a-f]{64}", artifact_digest
        ):
            raise ValueError("stable migration identity and artifact digest required")
        async with self.pool.acquire() as conn, conn.transaction():
            head, _ = await self._validate_session(conn, session_id)
            existing = await conn.fetchrow(
                "SELECT * FROM release_migrations_v1 WHERE migration_id=$1",
                migration_id,
            )
            identity = digest([migration_id, from_version, to_version, artifact_digest])
            if existing is not None:
                if existing["identity_digest"] != identity:
                    raise ReleaseMigrationConflict("migration retry identity drift")
                return
            if (
                head["schema_version"] != from_version
                or head["pending_schema_version"] is not None
            ):
                raise ReleaseMigrationConflict("migration source release drift")
            if (
                await conn.fetchval("SELECT count(*) FROM release_migrations_v1")
                >= head["max_migrations"]
            ):
                raise ReleaseProtocolError(
                    "release migration history capacity exceeded"
                )
            await self.bind_transaction(conn, session_id)
            await attempts.install_in_transaction(conn)
            if fault is not None:
                fault("after_ddl_before_marker")
            tables = await self._discover_tables(conn)
            await self._attach_guards(conn, tables)
            tables_digest = digest(tables)
            await conn.execute(
                "INSERT INTO release_migrations_v1 VALUES($1,$2,$3,$4,$5,'EXPANDED',"
                "$6,clock_timestamp(),NULL)",
                migration_id,
                from_version,
                to_version,
                artifact_digest,
                tables_digest,
                identity,
            )
            await conn.execute(
                "UPDATE release_heads_v1 SET pending_schema_version=$2,"
                "guarded_tables=$3::jsonb,updated_at=clock_timestamp() "
                "WHERE release_id=$1",
                self.RELEASE_ID,
                to_version,
                canonical(tables),
            )

    async def cutover(
        self,
        session_id: str,
        *,
        migration_id: str,
        target_contracts: tuple[RuntimeContract, ...],
        fault: Callable[[str], None] | None = None,
    ) -> int:
        contracts = tuple(_contract(item) for item in target_contracts)
        if not contracts:
            raise ValueError("target runtime contract required")
        semantics = {item.writer_semantics_digest for item in contracts}
        features = {item.feature_digest for item in contracts}
        if len(semantics) != 1 or len(features) != 1:
            raise ValueError(
                "mixed target writers require identical semantics and features"
            )
        async with self.pool.acquire() as conn, conn.transaction():
            head = await self._locked_head(conn)
            migration = await conn.fetchrow(
                "SELECT * FROM release_migrations_v1 WHERE migration_id=$1 FOR UPDATE",
                migration_id,
            )
            if migration is None:
                raise ReleaseMigrationConflict("expanded migration is missing")
            if migration["status"] == "CUTOVER":
                installed_contracts = {
                    (
                        row["runtime_version"],
                        row["writer_semantics_digest"],
                        row["feature_digest"],
                    )
                    for row in await conn.fetch(
                        "SELECT runtime_version,writer_semantics_digest,feature_digest "
                        "FROM release_runtime_compatibility_v1 WHERE schema_version=$1 "
                        "AND enabled",
                        migration["to_version"],
                    )
                }
                expected_contracts = {
                    (
                        item.runtime_version,
                        item.writer_semantics_digest,
                        item.feature_digest,
                    )
                    for item in contracts
                }
                if (
                    head["schema_version"] != migration["to_version"]
                    or head["pending_schema_version"] is not None
                    or installed_contracts != expected_contracts
                    or head["writer_semantics_digest"]
                    != contracts[0].writer_semantics_digest
                    or head["feature_digest"] != contracts[0].feature_digest
                ):
                    raise ReleaseMigrationConflict("completed cutover projection drift")
                return head["fence_epoch"]
            head, _ = await self._validate_session(conn, session_id)
            if head["pending_schema_version"] != migration["to_version"]:
                raise ReleaseMigrationConflict(
                    "pending release does not match migration"
                )
            compatibility_count = await conn.fetchval(
                "SELECT count(*) FROM release_runtime_compatibility_v1"
            )
            if compatibility_count + len(contracts) > head["max_compatibility_rows"]:
                raise ReleaseProtocolError(
                    "release compatibility history capacity exceeded"
                )
            for item in contracts:
                await conn.execute(
                    "INSERT INTO release_runtime_compatibility_v1 VALUES($1,$2,$3,$4,true)",
                    migration["to_version"],
                    item.runtime_version,
                    item.writer_semantics_digest,
                    item.feature_digest,
                )
            installed_contracts = {
                (
                    row["runtime_version"],
                    row["writer_semantics_digest"],
                    row["feature_digest"],
                )
                for row in await conn.fetch(
                    "SELECT runtime_version,writer_semantics_digest,feature_digest "
                    "FROM release_runtime_compatibility_v1 WHERE schema_version=$1 "
                    "AND enabled",
                    migration["to_version"],
                )
            }
            expected_contracts = {
                (
                    item.runtime_version,
                    item.writer_semantics_digest,
                    item.feature_digest,
                )
                for item in contracts
            }
            if installed_contracts != expected_contracts:
                raise ReleaseMigrationConflict(
                    "target runtime contract manifest is not exact"
                )
            fence = head["fence_epoch"] + 1
            await conn.execute(
                "UPDATE release_sessions_v1 SET status='REVOKED',"
                "closed_at=clock_timestamp() WHERE status='ACTIVE'"
            )
            await conn.execute(
                "UPDATE release_heads_v1 SET schema_version=$2,pending_schema_version=NULL,"
                "release_generation=release_generation+1,fence_epoch=$3,"
                "writer_semantics_digest=$4,feature_digest=$5,"
                "updated_at=clock_timestamp() WHERE release_id=$1",
                self.RELEASE_ID,
                migration["to_version"],
                fence,
                contracts[0].writer_semantics_digest,
                contracts[0].feature_digest,
            )
            await conn.execute(
                "UPDATE release_migrations_v1 SET status='CUTOVER',"
                "cutover_at=clock_timestamp() WHERE migration_id=$1",
                migration_id,
            )
            await self._retire_sessions(conn)
            if fault is not None:
                fault("after_cutover_before_commit")
            return fence

    async def _table_exists(self, conn: asyncpg.Connection, table: str) -> bool:
        return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table))

    async def _busy_reason(self, conn: asyncpg.Connection) -> str | None:
        checks = (
            ("runtime_invocations", "SELECT EXISTS(SELECT 1 FROM runtime_invocations)"),
            (
                "consumers",
                "SELECT EXISTS(SELECT 1 FROM consumers WHERE in_flight_sequence IS NOT NULL)",
            ),
            ("join_invocations_v3", "SELECT EXISTS(SELECT 1 FROM join_invocations_v3)"),
            (
                "join_workers",
                "SELECT EXISTS(SELECT 1 FROM join_workers WHERE phase='RESTORING')",
            ),
            (
                "recovery_plans_v2",
                (
                    "SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 r "
                    "WHERE r.status='RESTORING' AND NOT EXISTS("
                    "SELECT 1 FROM region_recovery_plans_v1 p "
                    "WHERE p.plan_id=r.recovery_id))"
                ),
            ),
            (
                "regional_recovery_workers_v1",
                (
                    "SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 "
                    "WHERE status<>'ACTIVE')"
                ),
            ),
            (
                "streaming_retention_operations_v1",
                (
                    "SELECT EXISTS(SELECT 1 FROM streaming_retention_operations_v1 "
                    "WHERE status<>'COMPLETED')"
                ),
            ),
            (
                "checkpoint_retirement_operations_v1",
                (
                    "SELECT EXISTS(SELECT 1 FROM checkpoint_retirement_operations_v1 "
                    "WHERE status<>'COMPLETED')"
                ),
            ),
            (
                "checkpoint_lifecycle_v2",
                (
                    "SELECT EXISTS(SELECT 1 FROM checkpoint_lifecycle_v2 "
                    "WHERE status NOT IN('COMPLETED','ABORTED','RETIRED'))"
                ),
            ),
            (
                "control_source_admissions_v1",
                (
                    "SELECT EXISTS(SELECT 1 FROM control_source_admissions_v1 "
                    "WHERE status='ADMITTED')"
                ),
            ),
        )
        for table, query in checks:
            if await self._table_exists(conn, table) and await conn.fetchval(query):
                return table
        return None

    async def _snapshot_cuts(
        self, conn: asyncpg.Connection, run_id: str
    ) -> CanonicalReleaseCuts:
        runs = await conn.fetch(
            "SELECT run_id,deployment_id,revision_id FROM control_runs_v1 "
            "ORDER BY run_id"
        )
        if len(runs) != 1 or runs[0]["run_id"] != run_id:
            raise ReleaseProtocolError(
                "selected schema backup requires exactly one logical control run"
            )
        run = runs[0]
        revision_id = run["revision_id"]
        queries: dict[str, tuple[str, tuple[Any, ...]]] = {
            "partitions": (
                (
                    "SELECT channel,partition_id,next_sequence,used_records,used_bytes "
                    "FROM partitions ORDER BY channel,partition_id"
                ),
                (),
            ),
            "producers": (
                (
                    "SELECT channel,partition_id,producer_id,accepted_through,retired_through "
                    "FROM producer_progress ORDER BY channel,partition_id,producer_id"
                ),
                (),
            ),
            "consumers": (
                (
                    "SELECT consumer_id,channel,partition_id,completed_through,"
                    "in_flight_sequence FROM consumers ORDER BY consumer_id"
                ),
                (),
            ),
            "workers": (
                (
                    "SELECT participant_id,state,next_arrival,next_apply,owner_id,owner_epoch,phase "
                    "FROM join_workers WHERE revision_id=$1 ORDER BY participant_id"
                ),
                (revision_id,),
            ),
            "inputs": (
                (
                    "SELECT participant_id,channel,partition_id,scanned,completed,blocked_checkpoint "
                    "FROM join_inputs_v3 WHERE revision_id=$1 "
                    "ORDER BY participant_id,channel,partition_id"
                ),
                (revision_id,),
            ),
            "checkpoints": (
                (
                    "SELECT i.checkpoint_id,i.checkpoint_ordinal,l.status "
                    "FROM checkpoint_intents_v2 i JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) "
                    "WHERE i.revision_id=$1 ORDER BY i.checkpoint_ordinal"
                ),
                (revision_id,),
            ),
        }
        material: dict[str, Any] = {"run": _json_value(run)}
        for name, (query, args) in queries.items():
            if await self._table_exists(conn, query.split("FROM ", 1)[1].split()[0]):
                material[name] = _json_value(await conn.fetch(query, *args))
            else:
                material[name] = []
        owned_locations = {
            (row["channel"], row["partition_id"])
            for row in await conn.fetch(
                "SELECT channel,partition_id FROM control_partition_ownership_v1 "
                "WHERE deployment_id=$1",
                run["deployment_id"],
            )
        }
        partition_locations = {
            (row["channel"], row["partition_id"]) for row in material["partitions"]
        }
        if not owned_locations or partition_locations != owned_locations:
            raise ReleaseProtocolError(
                "selected schema contains unowned or missing stream partitions"
            )
        declared_consumers = {
            row["consumer_id"]
            for row in await conn.fetch(
                "SELECT consumer_id FROM control_run_consumers_v1 WHERE run_id=$1",
                run_id,
            )
        }
        if {row["consumer_id"] for row in material["consumers"]} != declared_consumers:
            raise ReleaseProtocolError(
                "selected schema contains consumers outside the logical run"
            )
        if await self._table_exists(conn, "prefect_attempt_heads_v1"):
            material["attempt"] = _json_value(
                await conn.fetchrow(
                    "SELECT h.current_attempt_id,h.current_ordinal,h.current_epoch,"
                    "h.retired_through,a.prefect_flow_run_id "
                    "FROM prefect_attempt_heads_v1 h JOIN prefect_execution_attempts_v1 a "
                    "ON a.attempt_id=h.current_attempt_id WHERE h.run_id=$1",
                    run_id,
                )
            )
        else:
            material["attempt"] = None
        return CanonicalReleaseCuts(material, digest(material))

    async def snapshot_cuts(
        self, run_id: str, conn: asyncpg.Connection | None = None
    ) -> CanonicalReleaseCuts:
        if conn is not None:
            head = await conn.fetchrow(
                "SELECT * FROM release_heads_v1 WHERE release_id=$1",
                self.RELEASE_ID,
            )
            if head is None or head["state"] != "QUIESCED":
                raise ReleaseProtocolError(
                    "stable release cuts require QUIESCED authority"
                )
            cuts = await self._snapshot_cuts(conn, run_id)
            if cuts.digest != head["quiesced_cuts_digest"]:
                raise ReleaseProtocolError("quiesced release cuts drift")
            return cuts
        async with (
            self.pool.acquire() as acquired,
            acquired.transaction(isolation="repeatable_read", readonly=True),
        ):
            return await self.snapshot_cuts(run_id, acquired)

    async def quiesce(self, session_id: str, *, run_id: str) -> QuiescedManifest:
        async with self.pool.acquire() as conn, conn.transaction():
            projected = await self._locked_head(conn)
            if projected["state"] in ("QUIESCED", "CUTOVER_RETIRED"):
                if projected["quiesced_run_id"] != run_id:
                    raise ReleaseProtocolError("quiesce retry run identity drift")
                cuts = CanonicalReleaseCuts(
                    json.loads(projected["quiesced_cuts"]),
                    projected["quiesced_cuts_digest"],
                )
                manifest = QuiescedManifest(
                    self.RELEASE_ID,
                    run_id,
                    projected["release_generation"],
                    projected["fence_epoch"],
                    projected["schema_version"],
                    projected["restore_public_key_b64"],
                    projected["restore_key_id"],
                    cuts,
                )
                if manifest.manifest_digest != projected["quiesce_manifest_digest"]:
                    raise ReleaseProtocolError("quiesce retry manifest drift")
                return manifest
            head, _ = await self._validate_session(conn, session_id)
            busy = await self._busy_reason(conn)
            if busy is not None:
                raise ReleaseQuiesceBusy(f"release quiesce blocked by {busy}")
            cuts = await self._snapshot_cuts(conn, run_id)
            fence = head["fence_epoch"] + 1
            manifest = QuiescedManifest(
                self.RELEASE_ID,
                run_id,
                head["release_generation"],
                fence,
                head["schema_version"],
                head["restore_public_key_b64"],
                head["restore_key_id"],
                cuts,
            )
            await conn.execute(
                "UPDATE release_sessions_v1 SET status='REVOKED',"
                "closed_at=clock_timestamp() WHERE status='ACTIVE'"
            )
            await conn.execute(
                "UPDATE release_heads_v1 SET state='QUIESCED',fence_epoch=$2,"
                "quiesced_cuts=$3::jsonb,quiesced_cuts_digest=$4,quiesced_run_id=$5,"
                "quiesce_manifest_digest=$6,"
                "updated_at=clock_timestamp() WHERE release_id=$1",
                self.RELEASE_ID,
                fence,
                canonical(cuts.material),
                cuts.digest,
                run_id,
                manifest.manifest_digest,
            )
            await self._retire_grants(conn)
            await self._retire_sessions(conn)
            return manifest

    async def finalize_source(
        self,
        *,
        quiesce_manifest_digest: str,
        expected_cuts: CanonicalReleaseCuts,
    ) -> str:
        """Cancel the original run through the only QUIESCED mutation path."""
        async with self.pool.acquire() as conn, conn.transaction():
            head = await self._locked_head(conn)
            if head["state"] == "CUTOVER_RETIRED":
                if (
                    head["quiesce_manifest_digest"] != quiesce_manifest_digest
                    or head["quiesced_cuts_digest"] != expected_cuts.digest
                    or json.loads(head["quiesced_cuts"]) != expected_cuts.material
                    or head["source_finalize_digest"] is None
                ):
                    raise ReleaseProtocolError(
                        "source finalization retry authority drift"
                    )
                existing = await conn.fetchrow(
                    "SELECT * FROM release_source_finalizations_v1 WHERE run_id=$1",
                    head["quiesced_run_id"],
                )
                if (
                    existing is None
                    or existing["status"] != "COMPLETED"
                    or existing["outcome_digest"] != head["source_finalize_digest"]
                ):
                    raise ReleaseProtocolError(
                        "source finalization retry projection drift"
                    )
                return existing["outcome_digest"]
            if (
                head["state"] != "QUIESCED"
                or head["quiesce_manifest_digest"] != quiesce_manifest_digest
                or head["quiesced_cuts_digest"] != expected_cuts.digest
                or json.loads(head["quiesced_cuts"]) != expected_cuts.material
            ):
                raise ReleaseProtocolError("source finalization authority drift")
            run_id = head["quiesced_run_id"]
            observed = await self._snapshot_cuts(conn, run_id)
            if observed != expected_cuts:
                raise ReleaseProtocolError("source finalization cuts advanced")
            finalize_id = "release-finalize:" + digest(
                [quiesce_manifest_digest, expected_cuts.digest]
            )
            existing = await conn.fetchrow(
                "SELECT * FROM release_source_finalizations_v1 WHERE run_id=$1 FOR UPDATE",
                run_id,
            )
            if existing is not None:
                if (
                    existing["finalize_id"] != finalize_id
                    or existing["quiesce_manifest_digest"] != quiesce_manifest_digest
                    or existing["cuts_digest"] != expected_cuts.digest
                    or existing["status"] != "COMPLETED"
                ):
                    raise ReleaseProtocolError("source finalization retry drift")
                return existing["outcome_digest"]
            await conn.execute(
                "INSERT INTO release_source_finalizations_v1 VALUES("
                "$1,$2,$3,$4,'STARTED',NULL,clock_timestamp(),NULL)",
                finalize_id,
                run_id,
                quiesce_manifest_digest,
                expected_cuts.digest,
            )
            await conn.execute(
                "SELECT set_config('ariadne.release_finalize',$1,true)", finalize_id
            )
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
            )
            if run is None or run["lifecycle"] not in ("ACTIVE", "DRAINING"):
                raise ReleaseProtocolError("source run is not finalizable")
            consumer_ids = [
                row["consumer_id"]
                for row in await conn.fetch(
                    "SELECT consumer_id FROM control_run_consumers_v1 WHERE run_id=$1",
                    run_id,
                )
            ]
            await conn.execute(
                "DELETE FROM runtime_invocations WHERE consumer_id=ANY($1::text[])",
                consumer_ids,
            )
            await conn.execute(
                "UPDATE consumers SET owner_id=NULL,owner_epoch=owner_epoch+1,"
                "lease_expires_at=NULL,in_flight_sequence=NULL,in_flight_end=NULL "
                "WHERE consumer_id=ANY($1::text[])",
                consumer_ids,
            )
            await conn.execute(
                "INSERT INTO control_worker_fences_v1(run_id,fence_epoch,reason) "
                "VALUES($1,$2,'CANCELLED')",
                run_id,
                run["control_version"] + 1,
            )
            await conn.execute(
                "UPDATE control_runs_v1 SET desired_state='CANCELLED',"
                "lifecycle='CANCELLED',control_version=control_version+1,"
                "updated_at=clock_timestamp() WHERE run_id=$1",
                run_id,
            )
            attempt_id = None
            if await self._table_exists(conn, "prefect_attempt_heads_v1"):
                attempt_id = await conn.fetchval(
                    "SELECT current_attempt_id FROM prefect_attempt_heads_v1 WHERE run_id=$1",
                    run_id,
                )
            if attempt_id is not None:
                token_digests = [
                    row["token_digest"]
                    for row in await conn.fetch(
                        "SELECT token_digest FROM prefect_attempt_source_caps_v1 "
                        "WHERE attempt_id=$1",
                        attempt_id,
                    )
                ]
                await conn.execute(
                    "UPDATE prefect_attempt_source_caps_v1 SET enabled=false "
                    "WHERE attempt_id=$1 AND enabled",
                    attempt_id,
                )
                await conn.execute(
                    "UPDATE prefect_attempt_operators_v1 SET status='CANCELLED' "
                    "WHERE attempt_id=$1 AND status='ACTIVE'",
                    attempt_id,
                )
                await conn.execute(
                    "UPDATE control_capabilities_v1 SET disabled=true "
                    "WHERE token_digest=ANY($1::text[])",
                    token_digests,
                )
                await conn.execute(
                    "UPDATE prefect_execution_attempts_v1 SET status='CANCELLED',"
                    "terminal_at=clock_timestamp() WHERE attempt_id=$1 AND status='ACTIVE'",
                    attempt_id,
                )
            leader = await conn.fetchrow(
                "SELECT leader_id,leader_epoch FROM control_leaders_v1 "
                "WHERE deployment_id=$1",
                run["deployment_id"],
            )
            outcome = {"run_id": run_id, "desired_state": "CANCELLED"}
            await conn.execute(
                "INSERT INTO control_commands_v1 VALUES($1,$2,'CANCEL',$3,$4,$5::jsonb,"
                "clock_timestamp())",
                run_id,
                finalize_id,
                leader["leader_id"],
                leader["leader_epoch"],
                canonical(outcome),
            )
            after = await self._snapshot_cuts(conn, run_id)
            if after != expected_cuts:
                raise ReleaseProtocolError("source finalization changed durable cuts")
            outcome_digest = digest(
                [finalize_id, expected_cuts.digest, run_id, attempt_id, "CANCELLED"]
            )
            await conn.execute(
                "UPDATE release_source_finalizations_v1 SET status='COMPLETED',"
                "outcome_digest=$2,completed_at=clock_timestamp() WHERE finalize_id=$1",
                finalize_id,
                outcome_digest,
            )
            await conn.execute(
                "UPDATE release_heads_v1 SET source_finalize_digest=$2,"
                "updated_at=clock_timestamp() WHERE release_id=$1",
                self.RELEASE_ID,
                outcome_digest,
            )
            return outcome_digest

    async def mark_cutover_retired(
        self,
        *,
        quiesce_manifest_digest: str,
        expected_cuts: CanonicalReleaseCuts,
    ) -> str:
        async with self.pool.acquire() as conn, conn.transaction():
            head = await self._locked_head(conn)
            if head["state"] == "CUTOVER_RETIRED":
                return await self.assert_cutover_retired(
                    quiesce_manifest_digest, expected_cuts, conn
                )
            if (
                head["state"] != "QUIESCED"
                or head["quiesce_manifest_digest"] != quiesce_manifest_digest
                or head["source_finalize_digest"] is None
                or await self._snapshot_cuts(conn, head["quiesced_run_id"])
                != expected_cuts
            ):
                raise ReleaseProtocolError("cutover retirement authority drift")
            await conn.execute(
                "UPDATE release_heads_v1 SET state='CUTOVER_RETIRED',"
                "updated_at=clock_timestamp() WHERE release_id=$1",
                self.RELEASE_ID,
            )
            return head["source_finalize_digest"]

    async def assert_cutover_retired(
        self,
        quiesce_manifest_digest: str,
        expected_cuts: CanonicalReleaseCuts,
        conn: asyncpg.Connection | None = None,
    ) -> str:
        if conn is None:
            async with self.pool.acquire() as acquired:
                return await self.assert_cutover_retired(
                    quiesce_manifest_digest, expected_cuts, acquired
                )
        head = await conn.fetchrow(
            "SELECT * FROM release_heads_v1 WHERE release_id=$1", self.RELEASE_ID
        )
        if (
            head is None
            or head["state"] != "CUTOVER_RETIRED"
            or head["quiesce_manifest_digest"] != quiesce_manifest_digest
            or head["quiesced_cuts_digest"] != expected_cuts.digest
            or json.loads(head["quiesced_cuts"]) != expected_cuts.material
            or head["source_finalize_digest"] is None
        ):
            raise ReleaseProtocolError("source cutover is not durably retired")
        return head["source_finalize_digest"]

    async def activate_restored(
        self,
        witness: BoundGrantWitness,
        *,
        runtime_version: str,
        actor_id: str,
        role: str,
        lease_seconds: float,
    ) -> ReleaseSession:
        if not runtime_version or not actor_id or not role or lease_seconds <= 0:
            raise ValueError("valid restored session identity required")
        async with self.pool.acquire() as conn, conn.transaction():
            head = await self._locked_head(conn)
            try:
                witness.verify(head["restore_public_key_b64"])
            except RestoreContractError as exc:
                raise ReleaseCompatibilityError(str(exc)) from exc
            claims = witness.claims
            actual_system = str(
                await conn.fetchval("SELECT system_identifier FROM pg_control_system()")
            )
            actual_database = await conn.fetchval("SELECT current_database()")
            identity = await schema_identity(conn, self.schema)
            await assert_retained_payload_integrity(conn)
            run = claims.cuts.get("run")
            existing = await conn.fetchrow(
                "SELECT * FROM release_grant_witnesses_v1 WHERE grant_id=$1 FOR UPDATE",
                claims.grant_id,
            )
            if existing is not None:
                if (
                    existing["grant_digest"] != witness.witness_digest
                    or existing["target_system_id"] != actual_system
                    or existing["target_database"] != actual_database
                    or existing["target_schema"] != self.schema
                    or claims.schema_digest != identity["sha256"]
                ):
                    raise ReleaseCompatibilityError("restore grant identity drift")
                session = await conn.fetchrow(
                    "SELECT * FROM release_sessions_v1 WHERE session_id=$1",
                    existing["session_id"],
                )
                if (
                    head["state"] != "ACTIVE"
                    or existing["activated_fence"] != head["fence_epoch"]
                    or session is None
                    or session["status"] != "ACTIVE"
                    or session["fence_epoch"] != head["fence_epoch"]
                    or not await conn.fetchval(
                        "SELECT lease_expires_at>clock_timestamp() "
                        "FROM release_sessions_v1 WHERE session_id=$1",
                        existing["session_id"],
                    )
                ):
                    raise ReleaseSessionFenced(
                        "restore grant target session is unavailable"
                    )
                if (
                    session["runtime_version"] != runtime_version
                    or session["actor_id"] != actor_id
                    or session["role"] != role
                ):
                    raise ReleaseCompatibilityError(
                        "restore activation retry identity drift"
                    )
                return ReleaseSession(
                    session["session_id"],
                    session["ordinal"],
                    session["fence_epoch"],
                    session["runtime_version"],
                    session["actor_id"],
                    session["role"],
                )
            if (
                claims.target_system_id != actual_system
                or claims.target_system_id == claims.source_system_id
                or claims.target_database != actual_database
                or claims.target_schema != self.schema
                or claims.schema_digest != identity["sha256"]
                or claims.source_system_id != head["source_system_id"]
                or claims.source_database != head["source_database"]
                or claims.source_schema != head["source_schema"]
                or claims.source_release_generation != head["release_generation"]
                or claims.source_release_fence != head["fence_epoch"]
                or claims.target_release_generation != head["release_generation"] + 1
                or claims.issuer_key_id != head["restore_key_id"]
                or claims.cuts_json != canonical(json.loads(head["quiesced_cuts"]))
                or claims.cuts_digest != head["quiesced_cuts_digest"]
                or claims.quiesce_manifest_digest != head["quiesce_manifest_digest"]
                or not isinstance(run, dict)
                or claims.run_id != run.get("run_id")
                or claims.deployment_id != run.get("deployment_id")
                or claims.revision_id != run.get("revision_id")
            ):
                raise ReleaseCompatibilityError("restore grant scope or cuts mismatch")
            if head["state"] != "QUIESCED" or await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM release_grant_witnesses_v1 "
                "WHERE target_release_generation=$1)",
                claims.target_release_generation,
            ):
                raise ReleaseCompatibilityError("restore target already activated")
            compatible = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM release_runtime_compatibility_v1 "
                "WHERE schema_version=$1 AND runtime_version=$2 AND enabled "
                "AND writer_semantics_digest=$3 AND feature_digest=$4)",
                head["schema_version"],
                runtime_version,
                head["writer_semantics_digest"],
                head["feature_digest"],
            )
            if not compatible:
                raise ReleaseCompatibilityError("restored runtime is incompatible")
            fence = head["fence_epoch"] + 1
            ordinal = head["next_session_ordinal"]
            session_id = (
                "release-session:"
                + str(ordinal)
                + ":"
                + digest(
                    [
                        self.schema,
                        fence,
                        ordinal,
                        runtime_version,
                        actor_id,
                        role,
                        claims.grant_id,
                    ]
                )
            )
            await conn.execute(
                "UPDATE release_heads_v1 SET state='ACTIVE',release_generation=$2,"
                "fence_epoch=$3,next_session_ordinal=next_session_ordinal+1,"
                "source_finalize_digest=$4,source_system_id=$5,source_database=$6,"
                "source_schema=$7,updated_at=clock_timestamp() "
                "WHERE release_id=$1",
                self.RELEASE_ID,
                claims.target_release_generation,
                fence,
                claims.source_finalize_digest,
                actual_system,
                actual_database,
                self.schema,
            )
            await conn.execute(
                "INSERT INTO release_sessions_v1(session_id,ordinal,fence_epoch,"
                "runtime_version,actor_id,role,status,lease_expires_at) "
                "VALUES($1,$2,$3,$4,$5,$6,'ACTIVE',"
                "clock_timestamp()+$7*interval '1 second')",
                session_id,
                ordinal,
                fence,
                runtime_version,
                actor_id,
                role,
                lease_seconds,
            )
            await conn.execute(
                "INSERT INTO release_grant_witnesses_v1("
                "grant_id,grant_digest,target_system_id,target_database,target_schema,"
                "cuts_digest,source_release_generation,target_release_generation,"
                "source_finalize_digest,activated_fence,session_id) "
                "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                claims.grant_id,
                witness.witness_digest,
                actual_system,
                actual_database,
                self.schema,
                claims.cuts_digest,
                claims.source_release_generation,
                claims.target_release_generation,
                claims.source_finalize_digest,
                fence,
                session_id,
            )
            return ReleaseSession(
                session_id, ordinal, fence, runtime_version, actor_id, role
            )
