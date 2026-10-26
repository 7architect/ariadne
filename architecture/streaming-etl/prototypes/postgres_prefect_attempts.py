"""Bounded Prefect execution-attempt authority over the epoch-2 runtime.

The control leader coordinates lifecycle, while this store fences the concrete
Prefect source and operator actors.  A controller transport restart reconnects
to the current attempt; only replacement execution infrastructure rotates it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import asyncpg
from postgres_barrier_coordinator import decoded, digest
from postgres_control_plane import ControlNotFound, PostgresControlPlane, token_digest


class PrefectAttemptError(RuntimeError):
    pass


class FeatureDisabled(PrefectAttemptError):
    pass


class ResumeIdentityMismatch(PrefectAttemptError):
    pass


class AttemptFenced(PrefectAttemptError):
    pass


class AttemptHorizonExpired(PrefectAttemptError):
    pass


class RestoringSwapUnsupported(PrefectAttemptError):
    pass


@dataclass(frozen=True)
class SourceBinding:
    channel: str
    partition: str
    producer_id: str


@dataclass(frozen=True)
class OperatorBinding:
    participant_id: str
    owner_id: str
    artifact_id: str
    artifact_digest: str
    prefect_task_run_id: str | None = None


@dataclass(frozen=True)
class ExecutionAttempt:
    attempt_id: str
    run_id: str
    ordinal: int
    attempt_epoch: int
    prefect_flow_run_id: str
    graph_digest: str
    code_version: str
    status: str


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class PostgresPrefectAttempts:
    def __init__(self, control: PostgresControlPlane):
        self.control = control
        self.stream = control.stream
        self.pool = control.pool
        self.schema = control.schema

    @classmethod
    async def install(cls, control: PostgresControlPlane) -> PostgresPrefectAttempts:
        store = cls(control)
        await store._initialize()
        return store

    @classmethod
    def connect_existing(cls, control: PostgresControlPlane) -> PostgresPrefectAttempts:
        return cls(control)

    async def _initialize(self, conn: asyncpg.Connection | None = None) -> None:
        ddl = r"""
        CREATE TABLE IF NOT EXISTS prefect_attempt_heads_v1(
          run_id text PRIMARY KEY REFERENCES control_runs_v1,
          revision_id text NOT NULL UNIQUE REFERENCES deployment_revisions,
          graph_digest text NOT NULL,code_version text NOT NULL,
          current_attempt_id text NOT NULL UNIQUE,
          current_ordinal bigint NOT NULL CHECK(current_ordinal>0),
          current_epoch bigint NOT NULL CHECK(current_epoch>0),
          retired_through bigint NOT NULL DEFAULT 0 CHECK(retired_through>=0),
          retired_count bigint NOT NULL DEFAULT 0 CHECK(retired_count>=0),
          retired_digest text NOT NULL DEFAULT repeat('0',64),
          max_hot_attempts bigint NOT NULL CHECK(max_hot_attempts BETWEEN 1 AND 16),
          last_heartbeat_at timestamptz NOT NULL DEFAULT clock_timestamp());
        CREATE TABLE IF NOT EXISTS prefect_execution_attempts_v1(
          attempt_id text PRIMARY KEY,run_id text NOT NULL REFERENCES control_runs_v1,
          ordinal bigint NOT NULL CHECK(ordinal>0),attempt_epoch bigint NOT NULL CHECK(attempt_epoch>0),
          prefect_flow_run_id text NOT NULL CHECK(prefect_flow_run_id<>''),
          graph_digest text NOT NULL,code_version text NOT NULL,
          status text NOT NULL CHECK(status IN('ACTIVE','REPLACED','DRAINED','CANCELLED','FAILED')),
          replacement_reason text NOT NULL,created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          terminal_at timestamptz,UNIQUE(run_id,ordinal),UNIQUE(run_id,prefect_flow_run_id));
        ALTER TABLE prefect_attempt_heads_v1 DROP CONSTRAINT IF EXISTS prefect_attempt_head_fk;
        ALTER TABLE prefect_attempt_heads_v1 ADD CONSTRAINT prefect_attempt_head_fk
          FOREIGN KEY(current_attempt_id) REFERENCES prefect_execution_attempts_v1(attempt_id)
          DEFERRABLE INITIALLY DEFERRED;
        CREATE TABLE IF NOT EXISTS prefect_attempt_source_caps_v1(
          attempt_id text NOT NULL REFERENCES prefect_execution_attempts_v1 ON DELETE CASCADE,
          run_id text NOT NULL REFERENCES control_runs_v1,
          channel text NOT NULL,partition_id text NOT NULL,producer_id text NOT NULL,
          token_digest text NOT NULL REFERENCES control_capabilities_v1,
          enabled boolean NOT NULL DEFAULT true,
          PRIMARY KEY(attempt_id,channel,partition_id),UNIQUE(attempt_id,token_digest));
        CREATE UNIQUE INDEX IF NOT EXISTS prefect_one_live_source_actor
          ON prefect_attempt_source_caps_v1(run_id,channel,partition_id) WHERE enabled;
        CREATE TABLE IF NOT EXISTS prefect_attempt_operators_v1(
          attempt_id text NOT NULL REFERENCES prefect_execution_attempts_v1 ON DELETE CASCADE,
          run_id text NOT NULL REFERENCES control_runs_v1,
          revision_id text NOT NULL,participant_id text NOT NULL,
          artifact_id text NOT NULL,artifact_digest text NOT NULL,
          prefect_task_run_id text,owner_id text NOT NULL,owner_epoch bigint NOT NULL CHECK(owner_epoch>0),
          binding_version bigint NOT NULL DEFAULT 1 CHECK(binding_version>0),
          replacement_count bigint NOT NULL DEFAULT 0 CHECK(replacement_count>=0),
          replacement_digest text NOT NULL DEFAULT repeat('0',64),
          status text NOT NULL
            CHECK(status IN('ACTIVE','REPLACED','DRAINED','CANCELLED','FAILED')),
          PRIMARY KEY(attempt_id,participant_id));

        CREATE OR REPLACE FUNCTION prefect_attempt_identity_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF (NEW.attempt_id,NEW.run_id,NEW.ordinal,NEW.attempt_epoch,
              NEW.prefect_flow_run_id,NEW.graph_digest,NEW.code_version,NEW.created_at)
             IS DISTINCT FROM
             (OLD.attempt_id,OLD.run_id,OLD.ordinal,OLD.attempt_epoch,
              OLD.prefect_flow_run_id,OLD.graph_digest,OLD.code_version,OLD.created_at)
          THEN RAISE EXCEPTION 'Prefect attempt identity immutable'; END IF;
          IF OLD.status<>'ACTIVE' OR NEW.status NOT IN('REPLACED','DRAINED','CANCELLED','FAILED')
             OR NEW.terminal_at IS NULL
          THEN RAISE EXCEPTION 'illegal Prefect attempt transition'; END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS prefect_attempt_identity_guard ON prefect_execution_attempts_v1;
        CREATE TRIGGER prefect_attempt_identity_guard BEFORE UPDATE ON prefect_execution_attempts_v1
          FOR EACH ROW EXECUTE FUNCTION prefect_attempt_identity_guard();
        CREATE OR REPLACE FUNCTION prefect_attempt_delete_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF current_setting('ariadne.prefect_retention',true) IS DISTINCT FROM 'on'
          THEN RAISE EXCEPTION 'Prefect attempt deletion requires retention authority'; END IF;
          IF OLD.status='ACTIVE' THEN RAISE EXCEPTION 'active Prefect attempt cannot retire'; END IF;
          RETURN OLD;
        END $$;
        DROP TRIGGER IF EXISTS prefect_attempt_delete_guard ON prefect_execution_attempts_v1;
        CREATE TRIGGER prefect_attempt_delete_guard BEFORE DELETE ON prefect_execution_attempts_v1
          FOR EACH ROW EXECUTE FUNCTION prefect_attempt_delete_guard();
        CREATE OR REPLACE FUNCTION prefect_source_cap_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF (NEW.attempt_id,NEW.run_id,NEW.channel,NEW.partition_id,
              NEW.producer_id,NEW.token_digest) IS DISTINCT FROM
             (OLD.attempt_id,OLD.run_id,OLD.channel,OLD.partition_id,
              OLD.producer_id,OLD.token_digest)
          THEN RAISE EXCEPTION 'Prefect source capability identity immutable'; END IF;
          IF NOT OLD.enabled OR NEW.enabled THEN
            RAISE EXCEPTION 'Prefect source capability cannot reactivate';
          END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS prefect_source_cap_guard ON prefect_attempt_source_caps_v1;
        CREATE TRIGGER prefect_source_cap_guard BEFORE UPDATE ON prefect_attempt_source_caps_v1
          FOR EACH ROW EXECUTE FUNCTION prefect_source_cap_guard();

        CREATE OR REPLACE FUNCTION prefect_join_worker_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE allowed boolean; DECLARE regional boolean; BEGIN
          IF NOT EXISTS(SELECT 1 FROM prefect_attempt_heads_v1 h
                        WHERE h.revision_id=NEW.revision_id) THEN RETURN NEW; END IF;
          SELECT EXISTS(
            SELECT 1 FROM prefect_attempt_heads_v1 h
            JOIN prefect_execution_attempts_v1 a ON a.attempt_id=h.current_attempt_id
            JOIN prefect_attempt_operators_v1 o ON o.attempt_id=a.attempt_id
             AND o.participant_id=NEW.participant_id
            JOIN control_runs_v1 r ON r.run_id=h.run_id
            WHERE h.revision_id=NEW.revision_id AND a.status='ACTIVE'
              AND r.desired_state<>'CANCELLED' AND o.status='ACTIVE'
              AND (o.owner_id,o.owner_epoch)=(NEW.owner_id,NEW.owner_epoch)) INTO allowed;
          IF to_regclass('regional_recovery_workers_v1') IS NOT NULL THEN
            EXECUTE 'SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 g '
              'JOIN region_recovery_plans_v1 p USING(plan_id) '
              'WHERE p.revision_id=$1 AND g.participant_id=$2 '
              'AND (g.recovery_owner_id,g.recovery_owner_epoch)=($3,$4) '
              'AND g.status IN (''RESTORING'',''READY'',''ACTIVE''))'
              INTO regional USING NEW.revision_id,NEW.participant_id,NEW.owner_id,NEW.owner_epoch;
          ELSE regional=false; END IF;
          IF NOT allowed AND NOT regional THEN
            RAISE EXCEPTION 'Prefect operator execution attempt fenced';
          END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION prefect_join_terminal_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ DECLARE participant text; DECLARE epoch bigint; BEGIN
          participant=NEW.participant_id;
          IF TG_TABLE_NAME='join_turns_v3' THEN epoch=NEW.writer_epoch;
          ELSE epoch=NEW.writer_epoch; END IF;
          IF EXISTS(SELECT 1 FROM prefect_attempt_heads_v1 h WHERE h.revision_id=NEW.revision_id)
             AND NOT EXISTS(
               SELECT 1 FROM prefect_attempt_heads_v1 h
               JOIN prefect_execution_attempts_v1 a ON a.attempt_id=h.current_attempt_id
               JOIN prefect_attempt_operators_v1 o ON o.attempt_id=a.attempt_id
                AND o.participant_id=participant
               JOIN join_workers w ON w.revision_id=h.revision_id
                AND w.participant_id=participant
               WHERE h.revision_id=NEW.revision_id AND a.status='ACTIVE'
                 AND o.status='ACTIVE' AND o.owner_epoch=epoch
                 AND (w.owner_id,w.owner_epoch)=(o.owner_id,o.owner_epoch))
          THEN RAISE EXCEPTION 'Prefect terminal execution attempt fenced'; END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS prefect_join_worker_guard ON join_workers;
        CREATE TRIGGER prefect_join_worker_guard BEFORE UPDATE ON join_workers
          FOR EACH ROW EXECUTE FUNCTION prefect_join_worker_guard();
        DROP TRIGGER IF EXISTS prefect_join_invocation_guard ON join_invocations_v3;
        CREATE TRIGGER prefect_join_invocation_guard BEFORE INSERT ON join_invocations_v3
          FOR EACH ROW EXECUTE FUNCTION prefect_join_terminal_guard();
        DROP TRIGGER IF EXISTS prefect_join_turn_guard ON join_turns_v3;
        CREATE TRIGGER prefect_join_turn_guard BEFORE INSERT ON join_turns_v3
          FOR EACH ROW EXECUTE FUNCTION prefect_join_terminal_guard();
        CREATE OR REPLACE FUNCTION prefect_regional_start_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$ BEGIN
          IF EXISTS(
            SELECT 1 FROM prefect_attempt_heads_v1 h
            JOIN prefect_execution_attempts_v1 a
              ON a.attempt_id=h.current_attempt_id AND a.status='ACTIVE'
            WHERE h.revision_id=NEW.revision_id)
          THEN RAISE EXCEPTION
            'active Prefect attempt rejects regional recovery start'; END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS prefect_regional_start_guard
          ON regional_recovery_workers_v1;
        CREATE TRIGGER prefect_regional_start_guard
          BEFORE INSERT ON regional_recovery_workers_v1
          FOR EACH ROW EXECUTE FUNCTION prefect_regional_start_guard();
        """
        if conn is not None:
            await conn.execute(ddl)
            return
        async with self.pool.acquire() as acquired:
            await acquired.execute(ddl)

    async def install_in_transaction(self, conn: asyncpg.Connection) -> None:
        """Install the real adapter authority inside a release migration."""
        await self._initialize(conn)

    async def _authorized_run(
        self, conn: asyncpg.Connection, token: str, run_id: str
    ) -> asyncpg.Record:
        tenant = await self.control._authorize(token, "control")
        await self.control._assert_authorized(conn, token, tenant, "control")
        run = await self.control._tenant_row(
            conn, "control_runs_v1", "run_id", run_id, tenant
        )
        await self.control._lock_revision(conn, run["revision_id"])
        return await conn.fetchrow(
            "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE", run_id
        )

    async def _contract(
        self, conn: asyncpg.Connection, run: asyncpg.Record
    ) -> tuple[asyncpg.Record, list[dict[str, Any]], list[dict[str, str]]]:
        revision = await conn.fetchrow(
            "SELECT * FROM deployment_revisions WHERE revision_id=$1 FOR SHARE",
            run["revision_id"],
        )
        if revision is None or run["execution_mode"] != "NATIVE_JOIN":
            raise ResumeIdentityMismatch("Prefect adapter requires native run")
        if (run["desired_state"], run["lifecycle"]) != ("ACTIVE", "ACTIVE"):
            raise AttemptFenced("Prefect execution activation requires active run")
        participants = decoded(revision["participant_manifest"])
        sources = decoded(revision["source_manifest"])
        return revision, participants, sources

    @staticmethod
    def _validate_bindings(
        participants: list[dict[str, Any]],
        sources: list[dict[str, str]],
        source_bindings: list[SourceBinding],
        operator_bindings: list[OperatorBinding],
    ) -> None:
        if len(sources) != 1:
            raise ResumeIdentityMismatch(
                "prototype Prefect adapter requires exactly one source partition"
            )
        expected_sources = sorted((x["channel"], x["partition"]) for x in sources)
        actual_sources = sorted((x.channel, x.partition) for x in source_bindings)
        if actual_sources != expected_sources or len(actual_sources) != len(
            set(actual_sources)
        ):
            raise ResumeIdentityMismatch("source actor manifest drift")
        expected_operators = sorted(x["participant_id"] for x in participants)
        actual_operators = sorted(x.participant_id for x in operator_bindings)
        if actual_operators != expected_operators or len(actual_operators) != len(
            set(actual_operators)
        ):
            raise ResumeIdentityMismatch("operator actor manifest drift")
        if any(not x.producer_id for x in source_bindings) or any(
            not x.owner_id or not x.artifact_id or not x.artifact_digest
            for x in operator_bindings
        ):
            raise ValueError("complete source and operator actor identity required")
        expected_code = {
            item["participant_id"]: item["code_digest"] for item in participants
        }
        if any(
            binding.artifact_digest != expected_code[binding.participant_id]
            for binding in operator_bindings
        ):
            raise ResumeIdentityMismatch("operator artifact code identity drift")

    async def _assert_not_restoring(
        self, conn: asyncpg.Connection, revision_id: str
    ) -> None:
        if await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM join_workers WHERE revision_id=$1 AND phase='RESTORING')",
            revision_id,
        ):
            raise RestoringSwapUnsupported(
                "attempt swap during join restore unsupported"
            )
        if await conn.fetchval(
            "SELECT to_regclass('regional_recovery_workers_v1') IS NOT NULL"
        ) and await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 g "
            "JOIN region_recovery_plans_v1 p USING(plan_id) "
            "WHERE p.revision_id=$1 AND g.status<>'ACTIVE')",
            revision_id,
        ):
            raise RestoringSwapUnsupported(
                "attempt swap during regional recovery unsupported"
            )

    async def _insert_capability(
        self,
        conn: asyncpg.Connection,
        tenant_id: str,
        source_token: str,
    ) -> str:
        if not source_token:
            raise ValueError("source attempt capability required")
        hashed = token_digest(source_token)
        existing = await conn.fetchrow(
            "SELECT * FROM control_capabilities_v1 WHERE token_digest=$1 FOR UPDATE",
            hashed,
        )
        if existing is not None:
            raise ResumeIdentityMismatch("source attempt capability already used")
        await conn.execute(
            "INSERT INTO control_capabilities_v1(token_digest,tenant_id,capabilities,disabled) "
            "VALUES($1,$2,'[\"source\"]'::jsonb,false)",
            hashed,
            tenant_id,
        )
        return hashed

    async def _insert_attempt(
        self,
        conn: asyncpg.Connection,
        *,
        run: asyncpg.Record,
        revision: asyncpg.Record,
        ordinal: int,
        attempt_epoch: int,
        prefect_flow_run_id: str,
        graph_digest: str,
        code_version: str,
        reason: str,
        source_token: str,
        source_bindings: list[SourceBinding],
        operator_bindings: list[OperatorBinding],
    ) -> ExecutionAttempt:
        if not prefect_flow_run_id or not code_version:
            raise ValueError("flow and code identity required")
        if graph_digest != revision["graph_digest"]:
            raise ResumeIdentityMismatch("graph digest mismatch")
        attempt_id = "prefect-attempt:" + digest(
            [run["run_id"], ordinal, attempt_epoch, prefect_flow_run_id]
        )
        capability_digest = await self._insert_capability(
            conn, run["tenant_id"], source_token
        )
        for binding in source_bindings:
            if not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM producer_progress WHERE channel=$1 "
                "AND partition_id=$2 AND producer_id=$3)",
                binding.channel,
                binding.partition,
                binding.producer_id,
            ):
                raise ResumeIdentityMismatch(
                    "Prefect source producer is not registered"
                )
        await conn.execute(
            "INSERT INTO prefect_execution_attempts_v1("
            "attempt_id,run_id,ordinal,attempt_epoch,prefect_flow_run_id,graph_digest,"
            "code_version,status,replacement_reason) VALUES($1,$2,$3,$4,$5,$6,$7,'ACTIVE',$8)",
            attempt_id,
            run["run_id"],
            ordinal,
            attempt_epoch,
            prefect_flow_run_id,
            graph_digest,
            code_version,
            reason,
        )
        await conn.executemany(
            "INSERT INTO prefect_attempt_source_caps_v1 VALUES($1,$2,$3,$4,$5,$6,true)",
            [
                (
                    attempt_id,
                    run["run_id"],
                    binding.channel,
                    binding.partition,
                    binding.producer_id,
                    capability_digest,
                )
                for binding in source_bindings
            ],
        )
        workers = {
            row["participant_id"]: row
            for row in await conn.fetch(
                "SELECT * FROM join_workers WHERE revision_id=$1 ORDER BY participant_id FOR UPDATE",
                run["revision_id"],
            )
        }
        operator_rows = []
        for binding in operator_bindings:
            worker = workers[binding.participant_id]
            owner_epoch = worker["owner_epoch"] + 1
            operator_rows.append(
                (
                    attempt_id,
                    run["run_id"],
                    run["revision_id"],
                    binding.participant_id,
                    binding.artifact_id,
                    binding.artifact_digest,
                    binding.prefect_task_run_id,
                    binding.owner_id,
                    owner_epoch,
                    1,
                    0,
                    "0" * 64,
                    "ACTIVE",
                )
            )
        await conn.executemany(
            "INSERT INTO prefect_attempt_operators_v1 VALUES("
            "$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            operator_rows,
        )
        # On replacement, publish the new binding before worker fencing so the
        # storage trigger can authorize only the new owner epoch.  The deferred
        # head FK keeps this atomic with the newly inserted attempt.
        await conn.execute(
            "UPDATE prefect_attempt_heads_v1 SET current_attempt_id=$2,"
            "current_ordinal=$3,current_epoch=$4,last_heartbeat_at=clock_timestamp() "
            "WHERE run_id=$1",
            run["run_id"],
            attempt_id,
            ordinal,
            attempt_epoch,
        )
        await conn.execute(
            "DELETE FROM join_invocations_v3 WHERE revision_id=$1", run["revision_id"]
        )
        for row in operator_rows:
            await conn.execute(
                "UPDATE join_workers SET owner_id=$3,owner_epoch=$4,"
                "lease_expires_at=clock_timestamp()+interval '30 seconds' "
                "WHERE revision_id=$1 AND participant_id=$2",
                run["revision_id"],
                row[3],
                row[7],
                row[8],
            )
        return ExecutionAttempt(
            attempt_id,
            run["run_id"],
            ordinal,
            attempt_epoch,
            prefect_flow_run_id,
            graph_digest,
            code_version,
            "ACTIVE",
        )

    async def activate_attempt(
        self,
        token: str,
        *,
        control_run_id: str,
        prefect_flow_run_id: str,
        graph_digest: str,
        code_version: str,
        source_token: str,
        source_bindings: list[SourceBinding],
        operator_bindings: list[OperatorBinding],
        max_hot_attempts: int = 2,
    ) -> ExecutionAttempt:
        if not 1 <= max_hot_attempts <= 16:
            raise ValueError("hot attempt horizon must be in [1,16]")
        async with self.pool.acquire() as conn, conn.transaction():
            run = await self._authorized_run(conn, token, control_run_id)
            revision, participants, sources = await self._contract(conn, run)
            self._validate_bindings(
                participants, sources, source_bindings, operator_bindings
            )
            await self._assert_not_restoring(conn, run["revision_id"])
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head is not None:
                current = await conn.fetchrow(
                    "SELECT * FROM prefect_execution_attempts_v1 WHERE attempt_id=$1",
                    head["current_attempt_id"],
                )
                if (
                    current["prefect_flow_run_id"],
                    current["graph_digest"],
                    current["code_version"],
                ) == (prefect_flow_run_id, graph_digest, code_version):
                    if current["status"] != "ACTIVE":
                        raise AttemptFenced("Prefect execution attempt is terminal")
                    actual_sources = sorted(
                        (
                            row["channel"],
                            row["partition_id"],
                            row["producer_id"],
                            row["token_digest"],
                        )
                        for row in await conn.fetch(
                            "SELECT * FROM prefect_attempt_source_caps_v1 "
                            "WHERE attempt_id=$1",
                            current["attempt_id"],
                        )
                    )
                    requested_sources = sorted(
                        (
                            row.channel,
                            row.partition,
                            row.producer_id,
                            token_digest(source_token),
                        )
                        for row in source_bindings
                    )
                    actual_operators = sorted(
                        (
                            row["participant_id"],
                            row["owner_id"],
                            row["artifact_id"],
                            row["artifact_digest"],
                        )
                        for row in await conn.fetch(
                            "SELECT participant_id,owner_id,artifact_id,artifact_digest FROM "
                            "prefect_attempt_operators_v1 WHERE attempt_id=$1",
                            current["attempt_id"],
                        )
                    )
                    requested_operators = sorted(
                        (
                            row.participant_id,
                            row.owner_id,
                            row.artifact_id,
                            row.artifact_digest,
                        )
                        for row in operator_bindings
                    )
                    if actual_sources != requested_sources or (
                        actual_operators != requested_operators
                    ):
                        raise ResumeIdentityMismatch(
                            "Prefect attempt actor identity drift"
                        )
                    return ExecutionAttempt(
                        current["attempt_id"],
                        current["run_id"],
                        current["ordinal"],
                        current["attempt_epoch"],
                        current["prefect_flow_run_id"],
                        current["graph_digest"],
                        current["code_version"],
                        current["status"],
                    )
                raise ResumeIdentityMismatch("run already has another Prefect attempt")
            attempt = await self._insert_attempt(
                conn,
                run=run,
                revision=revision,
                ordinal=1,
                attempt_epoch=1,
                prefect_flow_run_id=prefect_flow_run_id,
                graph_digest=graph_digest,
                code_version=code_version,
                reason="INITIAL",
                source_token=source_token,
                source_bindings=source_bindings,
                operator_bindings=operator_bindings,
            )
            await conn.execute(
                "INSERT INTO prefect_attempt_heads_v1("
                "run_id,revision_id,graph_digest,code_version,current_attempt_id,"
                "current_ordinal,current_epoch,max_hot_attempts) VALUES($1,$2,$3,$4,$5,1,1,$6)",
                control_run_id,
                run["revision_id"],
                graph_digest,
                code_version,
                attempt.attempt_id,
                max_hot_attempts,
            )
            return attempt

    async def reconnect_attempt(
        self,
        token: str,
        *,
        control_run_id: str,
        attempt_id: str,
        ordinal: int,
        prefect_flow_run_id: str,
        graph_digest: str,
        code_version: str,
    ) -> ExecutionAttempt:
        async with self.pool.acquire() as conn, conn.transaction():
            await self._authorized_run(conn, token, control_run_id)
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head is None:
                raise ControlNotFound("Prefect attempt authority missing")
            if ordinal <= head["retired_through"]:
                raise AttemptHorizonExpired("Prefect attempt outside hot horizon")
            row = await conn.fetchrow(
                "SELECT * FROM prefect_execution_attempts_v1 WHERE attempt_id=$1 AND run_id=$2",
                attempt_id,
                control_run_id,
            )
            if row is None or (
                row["ordinal"],
                row["prefect_flow_run_id"],
                row["graph_digest"],
                row["code_version"],
            ) != (ordinal, prefect_flow_run_id, graph_digest, code_version):
                raise ResumeIdentityMismatch("Prefect resume identity drift")
            if (
                row["attempt_id"] != head["current_attempt_id"]
                or row["status"] != "ACTIVE"
            ):
                raise AttemptFenced("Prefect execution attempt is not current")
            await conn.execute(
                "UPDATE prefect_attempt_heads_v1 SET last_heartbeat_at=clock_timestamp() WHERE run_id=$1",
                control_run_id,
            )
            return ExecutionAttempt(
                row["attempt_id"],
                row["run_id"],
                row["ordinal"],
                row["attempt_epoch"],
                row["prefect_flow_run_id"],
                row["graph_digest"],
                row["code_version"],
                row["status"],
            )

    async def replace_execution(
        self,
        token: str,
        *,
        control_run_id: str,
        expected_attempt_id: str,
        prefect_flow_run_id: str,
        graph_digest: str,
        code_version: str,
        source_token: str,
        source_bindings: list[SourceBinding],
        operator_bindings: list[OperatorBinding],
        reason: str,
    ) -> ExecutionAttempt:
        tenant = await self.control._authorize(token, "control")
        async with self.pool.acquire() as conn, conn.transaction():
            await self.control._assert_authorized(conn, token, tenant, "control")
            visible = await self.control._tenant_row(
                conn, "control_runs_v1", "run_id", control_run_id, tenant
            )
            observed_head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1",
                control_run_id,
            )
            if (
                observed_head is None
                or observed_head["current_attempt_id"] != expected_attempt_id
            ):
                raise AttemptFenced("Prefect attempt replacement lost authority")
            old_caps = list(
                await conn.fetch(
                    "SELECT * FROM prefect_attempt_source_caps_v1 "
                    "WHERE attempt_id=$1 ORDER BY token_digest FOR UPDATE",
                    expected_attempt_id,
                )
            )
            # Capability locks precede revision/worker writes in replacement paths.
            for cap in old_caps:
                await conn.fetchrow(
                    "SELECT token_digest FROM control_capabilities_v1 "
                    "WHERE token_digest=$1 FOR UPDATE",
                    cap["token_digest"],
                )
            await self.control._lock_revision(conn, visible["revision_id"])
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head["current_attempt_id"] != expected_attempt_id:
                raise AttemptFenced("Prefect attempt replacement lost authority")
            revision, participants, sources = await self._contract(conn, run)
            self._validate_bindings(
                participants, sources, source_bindings, operator_bindings
            )
            if (graph_digest, code_version) != (
                head["graph_digest"],
                head["code_version"],
            ):
                raise ResumeIdentityMismatch("replacement graph or code drift")
            await self._assert_not_restoring(conn, run["revision_id"])
            await conn.execute(
                "UPDATE prefect_attempt_source_caps_v1 SET enabled=false WHERE attempt_id=$1",
                expected_attempt_id,
            )
            await conn.execute(
                "UPDATE prefect_attempt_operators_v1 SET status='REPLACED' "
                "WHERE attempt_id=$1 AND status='ACTIVE'",
                expected_attempt_id,
            )
            await conn.execute(
                "UPDATE control_capabilities_v1 SET disabled=true WHERE token_digest=ANY($1::text[])",
                [cap["token_digest"] for cap in old_caps],
            )
            await conn.execute(
                "UPDATE prefect_execution_attempts_v1 SET status='REPLACED',"
                "terminal_at=clock_timestamp() WHERE attempt_id=$1",
                expected_attempt_id,
            )
            ordinal = head["current_ordinal"] + 1
            epoch = head["current_epoch"] + 1
            attempt = await self._insert_attempt(
                conn,
                run=run,
                revision=revision,
                ordinal=ordinal,
                attempt_epoch=epoch,
                prefect_flow_run_id=prefect_flow_run_id,
                graph_digest=graph_digest,
                code_version=code_version,
                reason=reason,
                source_token=source_token,
                source_bindings=source_bindings,
                operator_bindings=operator_bindings,
            )
            await conn.execute(
                "UPDATE prefect_attempt_heads_v1 SET current_attempt_id=$2,"
                "current_ordinal=$3,current_epoch=$4,last_heartbeat_at=clock_timestamp() "
                "WHERE run_id=$1",
                control_run_id,
                attempt.attempt_id,
                ordinal,
                epoch,
            )
            await self._retire_hot(conn, control_run_id)
            return attempt

    async def _retire_hot(self, conn: asyncpg.Connection, run_id: str) -> None:
        head = await conn.fetchrow(
            "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE", run_id
        )
        rows = list(
            await conn.fetch(
                "SELECT * FROM prefect_execution_attempts_v1 WHERE run_id=$1 "
                "ORDER BY ordinal FOR UPDATE",
                run_id,
            )
        )
        retire = rows[: max(0, len(rows) - head["max_hot_attempts"])]
        for row in retire:
            if row["status"] == "ACTIVE":
                raise AttemptFenced("active Prefect attempt selected for retirement")
            sources = [
                dict(x)
                for x in await conn.fetch(
                    "SELECT channel,partition_id,producer_id,token_digest FROM "
                    "prefect_attempt_source_caps_v1 WHERE attempt_id=$1 ORDER BY channel,partition_id",
                    row["attempt_id"],
                )
            ]
            operators = [
                dict(x)
                for x in await conn.fetch(
                    "SELECT participant_id,artifact_id,artifact_digest,"
                    "prefect_task_run_id,owner_id,owner_epoch,"
                    "replacement_count,replacement_digest,status "
                    "FROM prefect_attempt_operators_v1 "
                    "WHERE attempt_id=$1 ORDER BY participant_id",
                    row["attempt_id"],
                )
            ]
            attempt_summary = {
                key: row[key]
                for key in (
                    "attempt_id",
                    "run_id",
                    "ordinal",
                    "attempt_epoch",
                    "prefect_flow_run_id",
                    "graph_digest",
                    "code_version",
                    "status",
                    "replacement_reason",
                )
            }
            attempt_summary["created_at"] = row["created_at"].isoformat()
            attempt_summary["terminal_at"] = row["terminal_at"].isoformat()
            retired_digest = digest(
                [head["retired_digest"], attempt_summary, sources, operators]
            )
            await conn.execute(
                "UPDATE prefect_attempt_heads_v1 SET retired_through=$2,"
                "retired_count=retired_count+1,retired_digest=$3 WHERE run_id=$1",
                run_id,
                row["ordinal"],
                retired_digest,
            )
            await conn.execute("SET LOCAL ariadne.prefect_retention='on'")
            await conn.execute(
                "DELETE FROM prefect_execution_attempts_v1 WHERE attempt_id=$1",
                row["attempt_id"],
            )
            await conn.execute(
                "DELETE FROM control_capabilities_v1 WHERE token_digest=ANY($1::text[])",
                [source["token_digest"] for source in sources],
            )
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1", run_id
            )

    async def bind_operator_task(
        self,
        token: str,
        *,
        control_run_id: str,
        attempt_id: str,
        participant_id: str,
        prefect_task_run_id: str,
    ) -> dict[str, Any]:
        if not prefect_task_run_id:
            raise ValueError("task run identity required")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._authorized_run(conn, token, control_run_id)
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head is None or head["current_attempt_id"] != attempt_id:
                raise AttemptFenced("operator task belongs to stale attempt")
            row = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 "
                "AND participant_id=$2 FOR UPDATE",
                attempt_id,
                participant_id,
            )
            if row is None:
                raise ResumeIdentityMismatch("operator is not in attempt manifest")
            if row["prefect_task_run_id"] not in (None, prefect_task_run_id):
                raise ResumeIdentityMismatch("operator task run identity drift")
            if row["prefect_task_run_id"] is None:
                await conn.execute(
                    "UPDATE prefect_attempt_operators_v1 SET prefect_task_run_id=$3 "
                    "WHERE attempt_id=$1 AND participant_id=$2",
                    attempt_id,
                    participant_id,
                    prefect_task_run_id,
                )
            return dict(
                await conn.fetchrow(
                    "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 "
                    "AND participant_id=$2",
                    attempt_id,
                    participant_id,
                )
            )

    async def finish_attempt(
        self,
        token: str,
        *,
        control_run_id: str,
        attempt_id: str,
        terminal_status: str,
    ) -> None:
        expected_lifecycle = {
            "DRAINED": "DRAINED",
            "CANCELLED": "CANCELLED",
            "FAILED": None,
        }
        if terminal_status not in expected_lifecycle:
            raise ValueError("unsupported Prefect attempt terminal state")
        tenant = await self.control._authorize(token, "control")
        async with self.pool.acquire() as conn, conn.transaction():
            await self.control._assert_authorized(conn, token, tenant, "control")
            visible = await self.control._tenant_row(
                conn, "control_runs_v1", "run_id", control_run_id, tenant
            )
            observed_head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1",
                control_run_id,
            )
            if (
                observed_head is None
                or observed_head["current_attempt_id"] != attempt_id
            ):
                raise AttemptFenced("terminal attempt is no longer current")
            caps = list(
                await conn.fetch(
                    "SELECT token_digest FROM prefect_attempt_source_caps_v1 "
                    "WHERE attempt_id=$1 ORDER BY token_digest FOR UPDATE",
                    attempt_id,
                )
            )
            for cap in caps:
                await conn.fetchrow(
                    "SELECT token_digest FROM control_capabilities_v1 "
                    "WHERE token_digest=$1 FOR UPDATE",
                    cap["token_digest"],
                )
            await self.control._lock_revision(conn, visible["revision_id"])
            run = await conn.fetchrow(
                "SELECT * FROM control_runs_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head["current_attempt_id"] != attempt_id:
                raise AttemptFenced("terminal attempt is no longer current")
            current = await conn.fetchrow(
                "SELECT * FROM prefect_execution_attempts_v1 WHERE attempt_id=$1 FOR UPDATE",
                attempt_id,
            )
            if current["status"] == terminal_status:
                return
            if current["status"] != "ACTIVE":
                raise AttemptFenced("Prefect attempt is already terminal")
            lifecycle = expected_lifecycle[terminal_status]
            if lifecycle is not None and run["lifecycle"] != lifecycle:
                raise ResumeIdentityMismatch(
                    "Prefect terminal state precedes durable control lifecycle"
                )
            await conn.execute(
                "UPDATE prefect_attempt_source_caps_v1 SET enabled=false "
                "WHERE attempt_id=$1 AND enabled",
                attempt_id,
            )
            await conn.execute(
                "UPDATE prefect_attempt_operators_v1 SET status=$2 "
                "WHERE attempt_id=$1 AND status='ACTIVE'",
                attempt_id,
                terminal_status,
            )
            await conn.execute(
                "UPDATE control_capabilities_v1 SET disabled=true "
                "WHERE token_digest=ANY($1::text[])",
                [row["token_digest"] for row in caps],
            )
            await conn.execute(
                "UPDATE prefect_execution_attempts_v1 SET status=$2,"
                "terminal_at=clock_timestamp() WHERE attempt_id=$1",
                attempt_id,
                terminal_status,
            )

    async def replace_operator(
        self,
        token: str,
        *,
        control_run_id: str,
        attempt_id: str,
        participant_id: str,
        prefect_task_run_id: str,
        owner_id: str,
        reason: str,
    ) -> dict[str, Any]:
        async with self.pool.acquire() as conn, conn.transaction():
            run = await self._authorized_run(conn, token, control_run_id)
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head is None or head["current_attempt_id"] != attempt_id:
                raise AttemptFenced("operator replacement belongs to stale attempt")
            worker = await conn.fetchrow(
                "SELECT * FROM join_workers WHERE revision_id=$1 AND participant_id=$2 FOR UPDATE",
                run["revision_id"],
                participant_id,
            )
            if worker is None:
                raise ResumeIdentityMismatch("operator worker missing")
            if worker["phase"] != "LIVE":
                raise RestoringSwapUnsupported(
                    "operator restore must finish before rebind"
                )
            old = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 "
                "AND participant_id=$2 FOR UPDATE",
                attempt_id,
                participant_id,
            )
            if old is None or old["status"] != "ACTIVE":
                raise AttemptFenced("operator binding is not active")
            new_epoch = worker["owner_epoch"] + 1
            replacement_digest = digest(
                [
                    old["replacement_digest"],
                    old["owner_id"],
                    old["owner_epoch"],
                    old["prefect_task_run_id"],
                    reason,
                ]
            )
            await conn.execute(
                "UPDATE prefect_attempt_operators_v1 SET prefect_task_run_id=$3,"
                "owner_id=$4,owner_epoch=$5,binding_version=binding_version+1,"
                "replacement_count=replacement_count+1,replacement_digest=$6 "
                "WHERE attempt_id=$1 AND participant_id=$2",
                attempt_id,
                participant_id,
                prefect_task_run_id,
                owner_id,
                new_epoch,
                replacement_digest,
            )
            await conn.execute(
                "DELETE FROM join_invocations_v3 WHERE revision_id=$1 AND participant_id=$2",
                run["revision_id"],
                participant_id,
            )
            await conn.execute(
                "UPDATE join_workers SET owner_id=$3,owner_epoch=$4,"
                "lease_expires_at=clock_timestamp()+interval '30 seconds' "
                "WHERE revision_id=$1 AND participant_id=$2",
                run["revision_id"],
                participant_id,
                owner_id,
                new_epoch,
            )
            return dict(
                await conn.fetchrow(
                    "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 "
                    "AND participant_id=$2",
                    attempt_id,
                    participant_id,
                )
            )

    async def renew_operator_lease(
        self,
        token: str,
        *,
        control_run_id: str,
        attempt_id: str,
        participant_id: str,
        prefect_task_run_id: str,
        owner_id: str,
        lease_seconds: float,
    ) -> dict[str, Any]:
        """Renew a live actor or fence/reacquire its expired lease atomically."""
        if lease_seconds <= 0:
            raise ValueError("positive operator lease required")
        async with self.pool.acquire() as conn, conn.transaction():
            run = await self._authorized_run(conn, token, control_run_id)
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1 FOR UPDATE",
                control_run_id,
            )
            if head is None or head["current_attempt_id"] != attempt_id:
                raise AttemptFenced("operator lease belongs to stale attempt")
            binding = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 "
                "AND participant_id=$2 FOR UPDATE",
                attempt_id,
                participant_id,
            )
            if binding is None or binding["status"] != "ACTIVE":
                raise AttemptFenced("operator lease binding is not active")
            if (binding["prefect_task_run_id"], binding["owner_id"]) != (
                prefect_task_run_id,
                owner_id,
            ):
                raise AttemptFenced("operator lease actor identity drift")
            worker = await conn.fetchrow(
                "SELECT * FROM join_workers WHERE revision_id=$1 "
                "AND participant_id=$2 FOR UPDATE",
                run["revision_id"],
                participant_id,
            )
            if worker is None or worker["phase"] != "LIVE":
                raise RestoringSwapUnsupported(
                    "operator restore must finish before lease renewal"
                )
            if (worker["owner_id"], worker["owner_epoch"]) != (
                binding["owner_id"],
                binding["owner_epoch"],
            ):
                raise AttemptFenced("operator lease authority drift")
            live = await conn.fetchval(
                "SELECT $1::timestamptz>clock_timestamp()",
                worker["lease_expires_at"],
            )
            owner_epoch = binding["owner_epoch"]
            if not live:
                owner_epoch += 1
                replacement_digest = digest(
                    [
                        binding["replacement_digest"],
                        binding["owner_id"],
                        binding["owner_epoch"],
                        binding["prefect_task_run_id"],
                        "LEASE_REACQUIRED",
                    ]
                )
                await conn.execute(
                    "UPDATE prefect_attempt_operators_v1 SET owner_epoch=$3,"
                    "binding_version=binding_version+1,"
                    "replacement_count=replacement_count+1,replacement_digest=$4 "
                    "WHERE attempt_id=$1 AND participant_id=$2",
                    attempt_id,
                    participant_id,
                    owner_epoch,
                    replacement_digest,
                )
                await conn.execute(
                    "DELETE FROM join_invocations_v3 WHERE revision_id=$1 "
                    "AND participant_id=$2",
                    run["revision_id"],
                    participant_id,
                )
            await conn.execute(
                "UPDATE join_workers SET lease_expires_at="
                "clock_timestamp()+$3*interval '1 second',owner_epoch=$4 "
                "WHERE revision_id=$1 AND participant_id=$2",
                run["revision_id"],
                participant_id,
                lease_seconds,
                owner_epoch,
            )
            return dict(
                await conn.fetchrow(
                    "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 "
                    "AND participant_id=$2",
                    attempt_id,
                    participant_id,
                )
            )

    async def project_attempt(
        self, token: str, *, control_run_id: str
    ) -> dict[str, Any]:
        tenant = await self.control._authorize(token, "read")
        async with self.pool.acquire() as conn, conn.transaction(readonly=True):
            await self.control._assert_authorized(
                conn, token, tenant, "read", lock=False
            )
            await self.control._tenant_row(
                conn, "control_runs_v1", "run_id", control_run_id, tenant
            )
            head = await conn.fetchrow(
                "SELECT * FROM prefect_attempt_heads_v1 WHERE run_id=$1",
                control_run_id,
            )
            if head is None:
                raise ControlNotFound("Prefect attempt not found")
            attempt = await conn.fetchrow(
                "SELECT * FROM prefect_execution_attempts_v1 WHERE attempt_id=$1",
                head["current_attempt_id"],
            )
            sources = await conn.fetch(
                "SELECT channel,partition_id,producer_id,enabled FROM "
                "prefect_attempt_source_caps_v1 WHERE attempt_id=$1 ORDER BY channel,partition_id",
                head["current_attempt_id"],
            )
            operators = await conn.fetch(
                "SELECT participant_id,artifact_id,artifact_digest,"
                "prefect_task_run_id,owner_id,owner_epoch,"
                "binding_version,replacement_count,status FROM prefect_attempt_operators_v1 "
                "WHERE attempt_id=$1 ORDER BY participant_id",
                head["current_attempt_id"],
            )
            hot_attempts = await conn.fetchval(
                "SELECT count(*) FROM prefect_execution_attempts_v1 WHERE run_id=$1",
                control_run_id,
            )
            return {
                "control_run_id": control_run_id,
                "revision_id": head["revision_id"],
                "graph_digest": head["graph_digest"],
                "code_version": head["code_version"],
                "current": {
                    "attempt_id": attempt["attempt_id"],
                    "ordinal": attempt["ordinal"],
                    "attempt_epoch": attempt["attempt_epoch"],
                    "prefect_flow_run_id": attempt["prefect_flow_run_id"],
                    "status": attempt["status"],
                    "replacement_reason": attempt["replacement_reason"],
                },
                "sources": [dict(x) for x in sources],
                "operators": [dict(x) for x in operators],
                "retired_through": head["retired_through"],
                "retired_count": head["retired_count"],
                "retired_digest": head["retired_digest"],
                "hot_attempts": hot_attempts,
                "max_hot_attempts": head["max_hot_attempts"],
            }
