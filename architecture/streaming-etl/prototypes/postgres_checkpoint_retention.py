"""Rolling checkpoint and ordered-log retirement for protocol epoch 2.

This R3-R2 prototype deliberately refuses checkpoints referenced by regional
recovery authority.  Region-plan/cut retirement is the following R3-R3 slice.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import asyncpg
from postgres_barrier_coordinator import (
    PostgresBarrierCoordinator,
    canonical,
    decoded,
    digest,
)
from postgres_stream_v2 import PostgresProtocolError


class CheckpointHorizonExpired(PostgresProtocolError):
    """The requested exact checkpoint evidence is below the archive horizon."""


class RegionalArchiveRequired(PostgresProtocolError):
    """The prefix still has regional authority that R3-R2 cannot summarize."""


@dataclass(frozen=True)
class CheckpointRetentionOutcome:
    operation_id: str
    revision_id: str
    retired_checkpoint_through: int
    retired_authority_through: int
    retired_checkpoints: int
    retired_authority_events: int
    retired_log_entries: int
    log_cuts: dict[str, int]
    rolling_digest: str
    idempotent_retry: bool = False


class PostgresCheckpointRetention:
    def __init__(self, coordinator: PostgresBarrierCoordinator):
        self.coordinator = coordinator
        self.pool = coordinator.pool
        self.schema = coordinator.schema

    @classmethod
    async def install(cls, coordinator: PostgresBarrierCoordinator):
        value = cls(coordinator)
        await value._initialize()
        return value

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS checkpoint_archive_heads_v1(
          revision_id text PRIMARY KEY REFERENCES deployment_revisions,
          retired_checkpoint_through bigint NOT NULL CHECK(retired_checkpoint_through>=0),
          retired_authority_through bigint NOT NULL CHECK(retired_authority_through>=0),
          retired_checkpoints bigint NOT NULL CHECK(retired_checkpoints>=0),
          retired_authority_events bigint NOT NULL CHECK(retired_authority_events>=0),
          retired_metadata_rows bigint NOT NULL CHECK(retired_metadata_rows>=0),
          prior_digest text NOT NULL, rolling_digest text NOT NULL UNIQUE,
          operation_id text NOT NULL UNIQUE,
          updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS partition_log_archive_heads_v1(
          channel text NOT NULL,partition_id text NOT NULL,
          retired_log_through bigint NOT NULL CHECK(retired_log_through>=0),
          retired_entries bigint NOT NULL CHECK(retired_entries>=0),
          prior_digest text NOT NULL,rolling_digest text NOT NULL,
          operation_id text NOT NULL,
          PRIMARY KEY(channel,partition_id), UNIQUE(rolling_digest)
        );
        CREATE TABLE IF NOT EXISTS join_retirement_archive_heads_v1(
          revision_id text NOT NULL,participant_id text NOT NULL,
          retired_apply_through bigint NOT NULL,
          retired_arrival_through bigint NOT NULL,
          archived_summary_digest text NOT NULL,
          rolling_digest text NOT NULL,operation_id text NOT NULL,
          PRIMARY KEY(revision_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS checkpoint_retirement_operations_v1(
          revision_id text PRIMARY KEY REFERENCES deployment_revisions,
          operation_id text NOT NULL UNIQUE,input_digest text NOT NULL,
          plan_json jsonb NOT NULL,outcome_json jsonb,
          status text NOT NULL CHECK(status IN('PLANNED','COMPLETED')),
          created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
          completed_at timestamptz
        );
        CREATE TABLE IF NOT EXISTS checkpoint_retirement_rows_v1(
          operation_id text NOT NULL,
          table_name text NOT NULL,row_key text NOT NULL,row_json jsonb NOT NULL,
          PRIMARY KEY(operation_id,table_name,row_key)
        );
        CREATE OR REPLACE FUNCTION checkpoint_retention_reject_update()
        RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
          RAISE EXCEPTION 'checkpoint archive authority is immutable';
        END $$;
        CREATE OR REPLACE FUNCTION checkpoint_retention_operation_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        BEGIN
          op=current_setting('ariadne.checkpoint_retirement_operation',true);
          IF TG_OP='DELETE' OR op IS NULL OR NEW.operation_id<>op
             OR (TG_OP='INSERT' AND NEW.status<>'PLANNED')
             OR (TG_OP='UPDATE' AND NOT(
                  (OLD.status='COMPLETED' AND NEW.status='PLANNED'
                   AND NEW.revision_id=OLD.revision_id)
                  OR (OLD.status='PLANNED' AND NEW.status='COMPLETED'
                      AND NEW.operation_id=OLD.operation_id
                      AND NEW.input_digest=OLD.input_digest
                      AND NEW.plan_json=OLD.plan_json
                      AND NEW.outcome_json IS NOT NULL)))
          THEN RAISE EXCEPTION 'illegal checkpoint retirement operation mutation'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION checkpoint_retention_head_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        BEGIN
          op=current_setting('ariadne.checkpoint_retirement_operation',true);
          IF op IS NULL OR NEW.operation_id<>op OR NOT EXISTS(
            SELECT 1 FROM checkpoint_retirement_operations_v1 o
             WHERE o.operation_id=op AND o.status='PLANNED')
          THEN RAISE EXCEPTION 'archive head lacks active operation'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION checkpoint_retirement_witness_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text; DECLARE present boolean; DECLARE plan jsonb;
        BEGIN
          op=current_setting('ariadne.checkpoint_retirement_operation',true);
          IF TG_OP='UPDATE' OR op IS NULL
             OR (TG_OP='DELETE' AND OLD.operation_id IS DISTINCT FROM op)
             OR (TG_OP='INSERT' AND NEW.operation_id IS DISTINCT FROM op)
          THEN RAISE EXCEPTION 'illegal archive witness mutation'; END IF;
          IF TG_OP='DELETE' THEN RETURN OLD; END IF;
          IF NEW.table_name NOT IN(
            'partition_log_entries','coordinator_authority_events',
            'checkpoint_intents_v2','checkpoint_lifecycle_v2',
            'source_barrier_observations','participant_checkpoint_acks',
            'checkpoint_snapshot_objects','checkpoint_completions_v2',
            'checkpoint_aborts_v2','recovery_plans_v2','participant_output_roots',
            'checkpoints','join_alignments_v3','join_barriers_v3',
            'join_recoveries_v3','join_retirement_manifests_v1',
            'join_retirement_rows_v1')
             OR NOT EXISTS(SELECT 1 FROM checkpoint_retirement_operations_v1 o
               WHERE o.operation_id=op AND o.status='PLANNED')
          THEN RAISE EXCEPTION 'archive witness table/operation is invalid'; END IF;
          SELECT o.plan_json INTO plan FROM checkpoint_retirement_operations_v1 o
           WHERE o.operation_id=op AND o.status='PLANNED';
          IF NEW.table_name NOT IN('coordinator_authority_events','partition_log_entries')
             AND (EXISTS(SELECT 1 FROM jsonb_array_elements_text(plan->'checkpoint_ids') id
               WHERE NOT EXISTS(SELECT 1 FROM checkpoint_intents_v2 i
                 JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                 WHERE i.checkpoint_id=id.value
                   AND i.revision_id=(SELECT revision_id FROM checkpoint_retirement_operations_v1
                                      WHERE operation_id=op)
                   AND l.status IN('COMPLETED','ABORTED','RETIRED')
                   AND i.checkpoint_ordinal BETWEEN
                     (plan->>'retired_checkpoint_from')::bigint AND
                     (plan->>'retired_checkpoint_through')::bigint))
             OR EXISTS(SELECT 1 FROM recovery_plans_v2 p
                  WHERE p.checkpoint_id IN(
                    SELECT value FROM jsonb_array_elements_text(plan->'checkpoint_ids'))
                    AND p.status='RESTORING'))
          THEN RAISE EXCEPTION 'frozen checkpoint prefix is not retirement eligible'; END IF;
          EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I x WHERE to_jsonb(x)=$1)',
                         NEW.table_name) INTO present USING NEW.row_json;
          IF NOT present THEN RAISE EXCEPTION 'archive witness lacks exact live row'; END IF;
          IF NEW.table_name IN(
               'checkpoint_intents_v2','checkpoint_lifecycle_v2',
               'source_barrier_observations','participant_checkpoint_acks',
               'checkpoint_completions_v2','checkpoint_aborts_v2',
               'participant_output_roots','join_alignments_v3','join_barriers_v3')
             AND NOT(plan->'checkpoint_ids' ? (NEW.row_json->>'checkpoint_id'))
          THEN RAISE EXCEPTION 'archive witness checkpoint is outside frozen prefix'; END IF;
          IF NEW.table_name='recovery_plans_v2'
             AND NEW.row_json->>'status'<>'READY'
          THEN RAISE EXCEPTION 'active recovery plan cannot be archived'; END IF;
          IF NEW.table_name='partition_log_entries' AND (
             (NEW.row_json->>'log_sequence')::bigint>
               (plan->'log_cuts'->>((NEW.row_json->>'channel')||'/'||(NEW.row_json->>'partition_id')))::bigint
             OR (NEW.row_json->>'log_sequence')::bigint<=
               (plan->'prior_log_cuts'->>((NEW.row_json->>'channel')||'/'||(NEW.row_json->>'partition_id')))::bigint
             OR NOT EXISTS(SELECT 1 FROM checkpoint_retirement_operations_v1 o
                  JOIN deployment_revisions rev USING(revision_id)
                  WHERE o.operation_id=op AND (
                    EXISTS(SELECT 1 FROM revision_source_partitions s
                      WHERE s.revision_id=o.revision_id
                        AND s.channel=NEW.row_json->>'channel'
                        AND s.partition_id=NEW.row_json->>'partition_id')
                    OR EXISTS(SELECT 1 FROM revision_edges e
                      WHERE e.revision_id=o.revision_id
                        AND e.channel=NEW.row_json->>'channel'
                        AND e.partition_id=NEW.row_json->>'partition_id')
                    OR EXISTS(SELECT 1 FROM revision_participants p,
                           jsonb_array_elements(p.output_manifest) output
                      WHERE p.revision_id=o.revision_id
                        AND output->>'channel'=NEW.row_json->>'channel'
                        AND output->>'partition'=NEW.row_json->>'partition_id'))))
          THEN RAISE EXCEPTION 'ordered-log witness is outside frozen cut'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION checkpoint_retirement_converged()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE current_op checkpoint_retirement_operations_v1%ROWTYPE;
        BEGIN
          SELECT * INTO current_op FROM checkpoint_retirement_operations_v1
           WHERE revision_id=NEW.revision_id;
          IF current_op.status<>'COMPLETED' OR current_op.outcome_json IS NULL
             OR EXISTS(SELECT 1 FROM checkpoint_retirement_rows_v1 r
                        WHERE r.operation_id=current_op.operation_id)
             OR NOT EXISTS(SELECT 1 FROM checkpoint_archive_heads_v1 h
                   WHERE h.revision_id=current_op.revision_id
                     AND h.operation_id=current_op.operation_id
                     AND h.rolling_digest=current_op.outcome_json->>'rolling_digest'
                     AND h.retired_checkpoint_through=
                       (current_op.plan_json->>'retired_checkpoint_through')::bigint
                     AND h.retired_authority_through=
                       (current_op.plan_json->>'authority_cut')::bigint)
             OR EXISTS(SELECT 1 FROM jsonb_each_text(current_op.plan_json->'log_cuts') cut
                  WHERE NOT EXISTS(SELECT 1 FROM partition_log_archive_heads_v1 h
                    JOIN partition_log_heads live USING(channel,partition_id)
                    WHERE h.channel||'/'||h.partition_id=cut.key
                      AND h.operation_id=current_op.operation_id
                      AND h.retired_log_through=cut.value::bigint
                      AND live.used_log_entries=(SELECT count(*) FROM partition_log_entries l
                        WHERE l.channel=h.channel AND l.partition_id=h.partition_id)
                      AND NOT EXISTS(SELECT 1 FROM partition_log_entries l
                        WHERE l.channel=h.channel AND l.partition_id=h.partition_id
                          AND l.log_sequence<=h.retired_log_through)))
             OR NOT EXISTS(SELECT 1 FROM checkpoint_intents_v2 i
                  JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                  JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                  WHERE i.revision_id=current_op.revision_id AND l.status='COMPLETED'
                    AND i.checkpoint_id=current_op.plan_json->>'latest_retained_checkpoint_id'
                    AND i.checkpoint_ordinal=
                      (current_op.plan_json->>'latest_retained_checkpoint_ordinal')::bigint)
             OR EXISTS(SELECT 1 FROM checkpoint_intents_v2 i
                  WHERE i.revision_id=current_op.revision_id
                    AND i.checkpoint_ordinal<=
                      (current_op.plan_json->>'retired_checkpoint_through')::bigint)
             OR EXISTS(SELECT 1 FROM coordinator_authority_events e
                  WHERE e.revision_id=current_op.revision_id
                    AND e.authority_ordinal<=
                      (current_op.plan_json->>'authority_cut')::bigint)
             OR EXISTS(SELECT 1 FROM participant_checkpoint_acks a
                  JOIN checkpoint_intents_v2 i USING(checkpoint_id),
                       jsonb_each(a.output_barriers) output
                  WHERE i.revision_id=current_op.revision_id AND NOT EXISTS(
                    SELECT 1 FROM partition_log_entries l
                    WHERE l.channel=split_part(output.key,'/',1)
                      AND l.partition_id=substring(output.key from
                        length(split_part(output.key,'/',1))+2)
                      AND l.log_sequence=(output.value->>'barrier_log_sequence')::bigint
                      AND l.entry_kind='BARRIER' AND l.barrier_role='OUTPUT'
                      AND l.checkpoint_id=a.checkpoint_id
                      AND l.cut_data_sequence=
                        (output.value->>'cut_data_sequence')::bigint
                      AND l.intent_digest=i.intent_digest
                      AND l.revision_id=i.revision_id))
             OR EXISTS(SELECT 1 FROM join_retirement_archive_heads_v1 a
                  WHERE a.revision_id=current_op.revision_id AND NOT EXISTS(
                    SELECT 1 FROM join_retention_anchors_v1 hot
                    JOIN join_retirement_manifests_v1 m ON m.operation_id=hot.operation_id
                    WHERE hot.revision_id=a.revision_id
                      AND hot.participant_id=a.participant_id
                      AND m.prior_summary_digest=a.archived_summary_digest))
          THEN RAISE EXCEPTION 'checkpoint retirement did not converge'; END IF;
          RETURN NULL;
        END $$;
        CREATE OR REPLACE FUNCTION checkpoint_retention_delete_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text; DECLARE region_op text; DECLARE authorized boolean;
        BEGIN
          op=current_setting('ariadne.checkpoint_retirement_operation',true);
          authorized=op IS NOT NULL AND EXISTS(
            SELECT 1 FROM checkpoint_retirement_operations_v1 o
            JOIN checkpoint_retirement_rows_v1 r USING(operation_id)
            WHERE o.operation_id=op AND o.status='PLANNED'
              AND r.table_name=TG_TABLE_NAME AND r.row_json=to_jsonb(OLD));
          IF NOT authorized AND to_regclass('regional_retirement_operations_v1') IS NOT NULL THEN
            region_op=current_setting('ariadne.regional_retirement_operation',true);
            EXECUTE 'SELECT EXISTS(SELECT 1 FROM regional_retirement_operations_v1 o
              JOIN regional_retirement_rows_v1 r USING(operation_id)
              WHERE o.operation_id=$1 AND o.status=''PLANNED''
                AND r.table_name=$2 AND r.row_json=$3)'
              INTO authorized USING region_op,TG_TABLE_NAME,to_jsonb(OLD);
          END IF;
          IF NOT authorized THEN
            RAISE EXCEPTION 'checkpoint metadata delete lacks exact archive witness';
          END IF;
          RETURN OLD;
        END $$;
        CREATE OR REPLACE FUNCTION checkpoint_retention_log_head_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text; DECLARE expected bigint;
        BEGIN
          IF (NEW.channel,NEW.partition_id) IS DISTINCT FROM
             (OLD.channel,OLD.partition_id)
          THEN RAISE EXCEPTION 'ordered-log identity is immutable'; END IF;
          IF NEW.used_log_entries>=OLD.used_log_entries THEN
            IF NEW.next_log_sequence-OLD.next_log_sequence<>
                 NEW.used_log_entries-OLD.used_log_entries
               OR (OLD.used_log_entries>0 AND
                   NEW.max_log_entries IS DISTINCT FROM OLD.max_log_entries)
            THEN
              RAISE EXCEPTION 'ordered-log append counters/capacity are invalid';
            END IF;
            RETURN NEW;
          END IF;
          IF NEW.next_log_sequence IS DISTINCT FROM OLD.next_log_sequence
             OR NEW.max_log_entries IS DISTINCT FROM OLD.max_log_entries
          THEN RAISE EXCEPTION 'ordered-log retirement changed identity'; END IF;
          op=current_setting('ariadne.checkpoint_retirement_operation',true);
          SELECT (o.plan_json->'log_counts'->>(NEW.channel||'/'||NEW.partition_id))::bigint
            INTO expected FROM checkpoint_retirement_operations_v1 o
           WHERE o.operation_id=op AND o.status='PLANNED';
          IF expected IS NULL OR OLD.used_log_entries-NEW.used_log_entries<>expected
             OR NEW.used_log_entries<>(SELECT count(*) FROM partition_log_entries l
                  WHERE l.channel=NEW.channel AND l.partition_id=NEW.partition_id)
          THEN RAISE EXCEPTION 'ordered-log credit lacks exact archive plan'; END IF;
          RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS checkpoint_retirement_operation_authority ON checkpoint_retirement_operations_v1;
        CREATE TRIGGER checkpoint_retirement_operation_authority BEFORE INSERT OR UPDATE OR DELETE
          ON checkpoint_retirement_operations_v1 FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_operation_guard();
        DROP TRIGGER IF EXISTS checkpoint_retirement_operation_convergence ON checkpoint_retirement_operations_v1;
        CREATE CONSTRAINT TRIGGER checkpoint_retirement_operation_convergence
          AFTER INSERT OR UPDATE ON checkpoint_retirement_operations_v1
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION checkpoint_retirement_converged();
        DROP TRIGGER IF EXISTS checkpoint_retirement_witness_authority ON checkpoint_retirement_rows_v1;
        CREATE TRIGGER checkpoint_retirement_witness_authority BEFORE INSERT OR UPDATE OR DELETE
          ON checkpoint_retirement_rows_v1 FOR EACH ROW EXECUTE FUNCTION checkpoint_retirement_witness_guard();
        DROP TRIGGER IF EXISTS checkpoint_archive_head_authority ON checkpoint_archive_heads_v1;
        CREATE TRIGGER checkpoint_archive_head_authority BEFORE INSERT OR UPDATE
          ON checkpoint_archive_heads_v1 FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_head_guard();
        DROP TRIGGER IF EXISTS checkpoint_archive_head_no_delete ON checkpoint_archive_heads_v1;
        CREATE TRIGGER checkpoint_archive_head_no_delete BEFORE DELETE ON checkpoint_archive_heads_v1
          FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_reject_update();
        DROP TRIGGER IF EXISTS partition_log_archive_head_authority ON partition_log_archive_heads_v1;
        CREATE TRIGGER partition_log_archive_head_authority BEFORE INSERT OR UPDATE
          ON partition_log_archive_heads_v1 FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_head_guard();
        DROP TRIGGER IF EXISTS partition_log_archive_head_no_delete ON partition_log_archive_heads_v1;
        CREATE TRIGGER partition_log_archive_head_no_delete BEFORE DELETE ON partition_log_archive_heads_v1
          FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_reject_update();
        DROP TRIGGER IF EXISTS join_retirement_archive_head_authority ON join_retirement_archive_heads_v1;
        CREATE TRIGGER join_retirement_archive_head_authority BEFORE INSERT OR UPDATE
          ON join_retirement_archive_heads_v1 FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_head_guard();
        DROP TRIGGER IF EXISTS join_retirement_archive_head_no_delete ON join_retirement_archive_heads_v1;
        CREATE TRIGGER join_retirement_archive_head_no_delete BEFORE DELETE ON join_retirement_archive_heads_v1
          FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_reject_update();
        DROP TRIGGER IF EXISTS partition_log_head_guard ON partition_log_heads;
        CREATE TRIGGER partition_log_head_guard BEFORE UPDATE ON partition_log_heads
          FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_log_head_guard();
        """
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(ddl)
            await self._install_delete_guards(conn)

    async def _install_delete_guards(self, conn: asyncpg.Connection) -> None:
        # Existing UPDATE semantics remain; only DELETE becomes archive-authorized.
        combined = {
            "partition_log_entries": "partition_log_immutable",
            "coordinator_authority_events": "coordinator_event_immutable",
            "checkpoint_intents_v2": "checkpoint_intent_v2_immutable",
            "source_barrier_observations": "source_barrier_immutable",
            "participant_checkpoint_acks": "participant_ack_immutable",
            "checkpoint_snapshot_objects": "checkpoint_snapshot_object_immutable",
            "checkpoint_completions_v2": "completion_v2_immutable",
            "checkpoint_aborts_v2": "abort_v2_immutable",
            "participant_output_roots": "participant_output_roots_immutable",
        }
        for table, trigger in combined.items():
            await conn.execute(
                f"""DROP TRIGGER IF EXISTS {trigger} ON {table};
                CREATE TRIGGER {trigger}_update BEFORE UPDATE ON {table}
                  FOR EACH ROW EXECUTE FUNCTION coordinator_reject_immutable_mutation();
                DROP TRIGGER IF EXISTS checkpoint_retention_delete ON {table};
                CREATE TRIGGER checkpoint_retention_delete BEFORE DELETE ON {table}
                  FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_delete_guard()"""
            )
        for table in ("checkpoint_lifecycle_v2", "recovery_plans_v2", "checkpoints"):
            if table == "recovery_plans_v2":
                await conn.execute(
                    "DROP TRIGGER IF EXISTS recovery_plan_delete ON recovery_plans_v2"
                )
            await conn.execute(
                f"""DROP TRIGGER IF EXISTS checkpoint_retention_delete ON {table};
                CREATE TRIGGER checkpoint_retention_delete BEFORE DELETE ON {table}
                  FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_delete_guard()"""
            )
        optional = {
            "join_barriers_v3": "immutable",
            "join_alignments_v3": "immutable",
            "join_recoveries_v3": "immutable_delete",
            "join_retirement_manifests_v1": "join_retirement_manifest_no_delete",
            "join_retirement_rows_v1": "join_retirement_rows_immutable",
        }
        for table, trigger in optional.items():
            if not await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table):
                continue
            await conn.execute(f"DROP TRIGGER IF EXISTS {trigger} ON {table}")
            if table in {"join_barriers_v3", "join_alignments_v3"}:
                await conn.execute(
                    f"""CREATE TRIGGER {trigger}_update BEFORE UPDATE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION coordinator_reject_immutable_mutation()"""
                )
            await conn.execute(
                f"""DROP TRIGGER IF EXISTS checkpoint_retention_delete ON {table};
                CREATE TRIGGER checkpoint_retention_delete BEFORE DELETE ON {table}
                FOR EACH ROW EXECUTE FUNCTION checkpoint_retention_delete_guard()"""
            )

    @staticmethod
    def _row_json(row: asyncpg.Record) -> str:
        material = dict(row)
        for key, value in material.items():
            if isinstance(value, str) and value[:1] in {"{", "["}:
                try:
                    material[key] = json.loads(value)
                except (TypeError, ValueError):
                    pass
        return json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)

    @classmethod
    def _material(cls, row: asyncpg.Record) -> dict[str, Any]:
        return json.loads(cls._row_json(row))

    async def _witness(
        self,
        conn: asyncpg.Connection,
        operation_id: str,
        table: str,
        rows: list[asyncpg.Record],
    ) -> None:
        identities = {
            "partition_log_entries": ("channel", "partition_id", "log_sequence"),
            "coordinator_authority_events": ("revision_id", "authority_ordinal"),
            "checkpoint_intents_v2": ("checkpoint_id",),
            "checkpoint_lifecycle_v2": ("checkpoint_id",),
            "source_barrier_observations": (
                "checkpoint_id",
                "channel",
                "partition_id",
            ),
            "participant_checkpoint_acks": ("checkpoint_id", "participant_id"),
            "checkpoint_snapshot_objects": ("object_key",),
            "checkpoint_completions_v2": ("checkpoint_id",),
            "checkpoint_aborts_v2": ("checkpoint_id",),
            "recovery_plans_v2": ("recovery_id",),
            "participant_output_roots": (
                "checkpoint_id",
                "participant_id",
                "channel",
                "partition_id",
            ),
            "checkpoints": ("checkpoint_id",),
            "join_alignments_v3": (
                "revision_id",
                "participant_id",
                "checkpoint_id",
            ),
            "join_barriers_v3": (
                "revision_id",
                "participant_id",
                "checkpoint_id",
                "channel",
                "partition_id",
            ),
            "join_recoveries_v3": (
                "revision_id",
                "participant_id",
                "recovery_id",
            ),
            "join_retirement_manifests_v1": ("operation_id",),
            "join_retirement_rows_v1": ("operation_id", "row_kind", "row_key"),
        }
        columns = identities[table]
        for index, row in enumerate(rows):
            where = " AND ".join(
                f"{name}=${offset + 1}" for offset, name in enumerate(columns)
            )
            db_json = await conn.fetchval(
                f"SELECT to_jsonb(x) FROM {table} x WHERE {where}",
                *[row[name] for name in columns],
            )
            if db_json is None:
                raise PostgresProtocolError("archive witness source disappeared")
            await conn.execute(
                """INSERT INTO checkpoint_retirement_rows_v1
                   VALUES($1,$2,$3,$4::jsonb)""",
                operation_id,
                table,
                f"{index}:{digest(json.loads(self._row_json(row)))}",
                canonical(decoded(db_json)),
            )

    async def retire_prefix(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        operation_id: str,
        *,
        fault: Callable[[str], None] | None = None,
    ) -> CheckpointRetentionOutcome:
        if not operation_id:
            raise ValueError("operation_id must be non-empty")
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(conn, revision_id)
            self.coordinator._check_graph(revision, graph_version, graph_digest)
            await self.coordinator._assert_owner(
                conn, revision_id, owner_id, owner_epoch
            )
            current = await conn.fetchrow(
                "SELECT * FROM checkpoint_retirement_operations_v1 WHERE revision_id=$1 FOR UPDATE",
                revision_id,
            )
            if current is not None and current["operation_id"] == operation_id:
                if current["status"] != "COMPLETED":
                    raise PostgresProtocolError(
                        "checkpoint retirement operation incomplete"
                    )
                return self._outcome(decoded(current["outcome_json"]), True)
            await self.coordinator._assert_no_active_stream_retention(conn, revision_id)
            prior = await conn.fetchrow(
                "SELECT * FROM checkpoint_archive_heads_v1 WHERE revision_id=$1 FOR UPDATE",
                revision_id,
            )
            prior_cp = 0 if prior is None else prior["retired_checkpoint_through"]
            latest = await conn.fetchrow(
                """SELECT i.checkpoint_id,i.checkpoint_ordinal FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status='COMPLETED'
                   ORDER BY i.checkpoint_ordinal DESC LIMIT 1""",
                revision_id,
            )
            if latest is None:
                raise PostgresProtocolError(
                    "checkpoint retirement requires a completed checkpoint"
                )
            active_global = await conn.fetch(
                """SELECT i.checkpoint_ordinal FROM recovery_plans_v2 p
                   JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                   WHERE p.revision_id=$1 AND p.status='RESTORING'
                     AND NOT EXISTS(SELECT 1 FROM region_recovery_plans_v1 r
                       WHERE to_regclass('region_recovery_plans_v1') IS NOT NULL
                         AND r.plan_id=p.recovery_id)"""
                if await conn.fetchval(
                    "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL"
                )
                else """SELECT i.checkpoint_ordinal FROM recovery_plans_v2 p
                         JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                         WHERE p.revision_id=$1 AND p.status='RESTORING'""",
                revision_id,
            )
            retain_floor = min(
                [latest["checkpoint_ordinal"]]
                + [row["checkpoint_ordinal"] for row in active_global]
            )
            candidates = list(
                await conn.fetch(
                    """SELECT i.*,l.status FROM checkpoint_intents_v2 i
                       JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                       WHERE i.revision_id=$1 AND i.checkpoint_ordinal>$2
                         AND i.checkpoint_ordinal<$3
                         AND l.status IN('COMPLETED','ABORTED')
                       ORDER BY i.checkpoint_ordinal""",
                    revision_id,
                    prior_cp,
                    retain_floor,
                )
            )
            if not candidates:
                raise PostgresProtocolError("checkpoint retirement has no safe prefix")
            expected = list(
                range(prior_cp + 1, candidates[-1]["checkpoint_ordinal"] + 1)
            )
            if [row["checkpoint_ordinal"] for row in candidates] != expected:
                raise PostgresProtocolError(
                    "checkpoint retirement prefix is not contiguous terminal history"
                )
            checkpoint_ids = [row["checkpoint_id"] for row in candidates]
            await self._reject_regional_history(conn, checkpoint_ids)
            log_rows, log_cuts = await self._select_log_prefix(
                conn, revision_id, checkpoint_ids
            )
            prior_log_cuts = {
                key: log_cuts[key] - len(rows) for key, rows in log_rows.items()
            }
            authority_cut = await self._planned_authority_cut(
                conn, revision_id, checkpoint_ids
            )
            log_counts: dict[str, int] = {}
            for location, rows in log_rows.items():
                log_counts[location] = len(rows)
            input_identity = {
                "revision_id": revision_id,
                "prior_checkpoint_through": prior_cp,
                "checkpoint_ids": checkpoint_ids,
                "retired_checkpoint_from": prior_cp + 1,
                "retired_checkpoint_through": candidates[-1]["checkpoint_ordinal"],
                "latest_retained_checkpoint_id": latest["checkpoint_id"],
                "latest_retained_checkpoint_ordinal": latest["checkpoint_ordinal"],
                "log_cuts": log_cuts,
                "prior_log_cuts": prior_log_cuts,
                "authority_cut": authority_cut,
                "prior_digest": "0" * 64 if prior is None else prior["rolling_digest"],
            }
            plan = {**input_identity, "log_counts": log_counts}
            input_digest = digest(input_identity)
            await conn.execute(
                "SELECT set_config('ariadne.checkpoint_retirement_operation',$1,true)",
                operation_id,
            )
            if current is None:
                await conn.execute(
                    """INSERT INTO checkpoint_retirement_operations_v1
                       VALUES($1,$2,$3,$4::jsonb,NULL,'PLANNED',clock_timestamp(),NULL)""",
                    revision_id,
                    operation_id,
                    input_digest,
                    canonical(plan),
                )
            else:
                await conn.execute(
                    """UPDATE checkpoint_retirement_operations_v1 SET
                       operation_id=$2,input_digest=$3,plan_json=$4::jsonb,
                       outcome_json=NULL,status='PLANNED',created_at=clock_timestamp(),completed_at=NULL
                       WHERE revision_id=$1""",
                    revision_id,
                    operation_id,
                    input_digest,
                    canonical(plan),
                )
            if fault is not None:
                fault("checkpoint_archive_planned")
            retired_event_ordinals = []
            for item in candidates:
                event_ordinal = await self.coordinator._authority_event(
                    conn,
                    revision,
                    "RETIRE",
                    owner_epoch,
                    {
                        "operation_id": operation_id,
                        "checkpoint_ordinal": item["checkpoint_ordinal"],
                    },
                    checkpoint_id=item["checkpoint_id"],
                )
                retired_event_ordinals.append(event_ordinal)
                await conn.execute(
                    """UPDATE checkpoint_lifecycle_v2 SET status='RETIRED',last_authority_ordinal=$2
                       WHERE checkpoint_id=$1""",
                    item["checkpoint_id"],
                    event_ordinal,
                )
            await self._release_roots(conn, checkpoint_ids)
            metadata = await self._collect_metadata(conn, revision_id, checkpoint_ids)
            for table, rows in metadata.items():
                await self._witness(conn, operation_id, table, rows)
            for location, rows in log_rows.items():
                await self._witness(conn, operation_id, "partition_log_entries", rows)
            await self._delete_metadata(conn, metadata, checkpoint_ids, operation_id)
            if fault is not None:
                fault("checkpoint_metadata_deleted")
            retired_logs = 0
            for location, rows in log_rows.items():
                if not rows:
                    continue
                channel, partition = location.split("/", 1)
                await conn.execute(
                    """DELETE FROM partition_log_entries WHERE channel=$1 AND partition_id=$2
                       AND log_sequence<=$3""",
                    channel,
                    partition,
                    log_cuts[location],
                )
                await conn.execute(
                    """UPDATE partition_log_heads SET used_log_entries=used_log_entries-$3
                       WHERE channel=$1 AND partition_id=$2""",
                    channel,
                    partition,
                    len(rows),
                )
                old = await conn.fetchrow(
                    "SELECT * FROM partition_log_archive_heads_v1 WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                    channel,
                    partition,
                )
                prior_log_digest = "0" * 64 if old is None else old["rolling_digest"]
                log_digest = hashlib.sha256(
                    (
                        prior_log_digest
                        + canonical([self._material(row) for row in rows])
                    ).encode()
                ).hexdigest()
                await conn.execute(
                    """INSERT INTO partition_log_archive_heads_v1 VALUES($1,$2,$3,$4,$5,$6,$7)
                       ON CONFLICT(channel,partition_id) DO UPDATE SET
                         retired_log_through=EXCLUDED.retired_log_through,
                         retired_entries=partition_log_archive_heads_v1.retired_entries+$4,
                         prior_digest=EXCLUDED.prior_digest,rolling_digest=EXCLUDED.rolling_digest,
                         operation_id=EXCLUDED.operation_id""",
                    channel,
                    partition,
                    log_cuts[location],
                    len(rows),
                    prior_log_digest,
                    log_digest,
                    operation_id,
                )
                retired_logs += len(rows)
            if fault is not None:
                fault("ordered_log_prefix_deleted")
            authority_cut, authority_rows = await self._retire_authority_prefix(
                conn, revision_id, operation_id, authority_cut
            )
            material = {
                "input": input_identity,
                "metadata": {
                    table: [self._material(row) for row in rows]
                    for table, rows in metadata.items()
                },
                "logs": {
                    key: [self._material(row) for row in rows]
                    for key, rows in log_rows.items()
                },
                "authority": [self._material(row) for row in authority_rows],
            }
            prior_digest = "0" * 64 if prior is None else prior["rolling_digest"]
            rolling = hashlib.sha256(
                (prior_digest + canonical(material)).encode()
            ).hexdigest()
            prior_counts = (
                (0, 0, 0)
                if prior is None
                else (
                    prior["retired_checkpoints"],
                    prior["retired_authority_events"],
                    prior["retired_metadata_rows"],
                )
            )
            metadata_count = sum(len(rows) for rows in metadata.values())
            await conn.execute(
                """INSERT INTO checkpoint_archive_heads_v1 VALUES(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp())
                   ON CONFLICT(revision_id) DO UPDATE SET
                     retired_checkpoint_through=EXCLUDED.retired_checkpoint_through,
                     retired_authority_through=EXCLUDED.retired_authority_through,
                     retired_checkpoints=EXCLUDED.retired_checkpoints,
                     retired_authority_events=EXCLUDED.retired_authority_events,
                     retired_metadata_rows=EXCLUDED.retired_metadata_rows,
                     prior_digest=EXCLUDED.prior_digest,rolling_digest=EXCLUDED.rolling_digest,
                     operation_id=EXCLUDED.operation_id,updated_at=clock_timestamp()""",
                revision_id,
                candidates[-1]["checkpoint_ordinal"],
                authority_cut,
                prior_counts[0] + len(candidates),
                prior_counts[1] + len(authority_rows),
                prior_counts[2] + metadata_count,
                prior_digest,
                rolling,
                operation_id,
            )
            outcome = {
                "operation_id": operation_id,
                "revision_id": revision_id,
                "retired_checkpoint_through": candidates[-1]["checkpoint_ordinal"],
                "retired_authority_through": authority_cut,
                "retired_checkpoints": len(candidates),
                "retired_authority_events": len(authority_rows),
                "retired_log_entries": retired_logs,
                "log_cuts": log_cuts,
                "rolling_digest": rolling,
            }
            await conn.execute(
                "DELETE FROM checkpoint_retirement_rows_v1 WHERE operation_id=$1",
                operation_id,
            )
            await conn.execute(
                """UPDATE checkpoint_retirement_operations_v1 SET status='COMPLETED',
                   outcome_json=$2::jsonb,completed_at=clock_timestamp()
                   WHERE revision_id=$1""",
                revision_id,
                canonical(outcome),
            )
            await self.coordinator._assert_metadata_budget(conn, revision_id)
            if fault is not None:
                fault("checkpoint_archive_before_commit")
        if fault is not None:
            fault("checkpoint_archive_after_commit")
        return self._outcome(outcome, False)

    async def _reject_regional_history(self, conn, checkpoint_ids: list[str]) -> None:
        for table in ("region_edge_cuts_v1", "region_recovery_plans_v1"):
            if await conn.fetchval(
                "SELECT to_regclass($1) IS NOT NULL", table
            ) and await conn.fetchval(
                f"SELECT EXISTS(SELECT 1 FROM {table} WHERE checkpoint_id=ANY($1::text[]))",
                checkpoint_ids,
            ):
                raise RegionalArchiveRequired(
                    "historical regional authority requires R3-R3"
                )

    async def _select_log_prefix(
        self, conn, revision_id: str, retiring_checkpoint_ids: list[str]
    ):
        heads = await conn.fetch(
            """SELECT h.* FROM partition_log_heads h JOIN(
                 SELECT channel,partition_id FROM revision_source_partitions
                  WHERE revision_id=$1
                 UNION SELECT channel,partition_id FROM revision_edges
                  WHERE revision_id=$1
                 UNION SELECT item->>'channel',item->>'partition'
                  FROM revision_participants p,
                       jsonb_array_elements(p.output_manifest) item
                  WHERE p.revision_id=$1) owned USING(channel,partition_id)
               ORDER BY h.channel,h.partition_id FOR UPDATE OF h""",
            revision_id,
        )
        result: dict[str, list[asyncpg.Record]] = {}
        cuts: dict[str, int] = {}
        for head in heads:
            channel, partition = head["channel"], head["partition_id"]
            archive = await conn.fetchrow(
                "SELECT * FROM partition_log_archive_heads_v1 WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            floor = 0 if archive is None else archive["retired_log_through"]
            needed = (
                await conn.fetchval(
                    """SELECT min(value) FROM (
                   SELECT barrier_log_sequence AS value FROM source_barrier_observations b
                    JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                    WHERE i.revision_id=$1 AND b.channel=$2 AND b.partition_id=$3
                      AND NOT(b.checkpoint_id=ANY($4::text[]))
                   UNION ALL SELECT log_sequence FROM join_barriers_v3
                    WHERE to_regclass('join_barriers_v3') IS NOT NULL AND revision_id=$1
                      AND channel=$2 AND partition_id=$3
                      AND NOT(checkpoint_id=ANY($4::text[]))
                   UNION ALL SELECT log_sequence FROM join_receipts_v3
                    WHERE to_regclass('join_receipts_v3') IS NOT NULL AND revision_id=$1
                      AND channel=$2 AND partition_id=$3
                   UNION ALL SELECT (entry.value->>'barrier_log_sequence')::bigint
                    FROM participant_checkpoint_acks a
                    JOIN checkpoint_intents_v2 i USING(checkpoint_id),
                         jsonb_each(a.output_barriers) entry
                    WHERE i.revision_id=$1 AND entry.key=$2||'/'||$3
                      AND NOT(a.checkpoint_id=ANY($4::text[]))) q""",
                    revision_id,
                    channel,
                    partition,
                    retiring_checkpoint_ids,
                )
                if await conn.fetchval(
                    "SELECT to_regclass('join_receipts_v3') IS NOT NULL"
                )
                else await conn.fetchval(
                    """SELECT min(barrier_log_sequence) FROM source_barrier_observations b
                   JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND b.channel=$2 AND b.partition_id=$3
                     AND NOT(b.checkpoint_id=ANY($4::text[]))""",
                    revision_id,
                    channel,
                    partition,
                    retiring_checkpoint_ids,
                )
            )
            if await conn.fetchval(
                "SELECT to_regclass('region_edge_cuts_v1') IS NOT NULL"
            ):
                region_needed = await conn.fetchval(
                    """SELECT min(barrier_log_sequence) FROM region_edge_cuts_v1
                       WHERE revision_id=$1 AND channel=$2 AND partition_id=$3
                         AND NOT(checkpoint_id=ANY($4::text[]))""",
                    revision_id,
                    channel,
                    partition,
                    retiring_checkpoint_ids,
                )
                if region_needed is not None:
                    needed = (
                        region_needed if needed is None else min(needed, region_needed)
                    )
            cut = (head["next_log_sequence"] - 1) if needed is None else needed - 1
            cut = max(floor, cut)
            rows = list(
                await conn.fetch(
                    """SELECT * FROM partition_log_entries WHERE channel=$1 AND partition_id=$2
                   AND log_sequence>$3 AND log_sequence<=$4 ORDER BY log_sequence""",
                    channel,
                    partition,
                    floor,
                    cut,
                )
            )
            if rows and [row["log_sequence"] for row in rows] != list(
                range(floor + 1, cut + 1)
            ):
                raise PostgresProtocolError("ordered-log retirement prefix has a gap")
            result[f"{channel}/{partition}"] = rows
            cuts[f"{channel}/{partition}"] = cut
        return result, cuts

    async def _release_roots(self, conn, checkpoint_ids: list[str]) -> None:
        refs = await conn.fetch(
            """SELECT retention_ref_id FROM source_barrier_observations
               WHERE checkpoint_id=ANY($1::text[]) UNION
               SELECT retention_ref_id FROM participant_output_roots
               WHERE checkpoint_id=ANY($1::text[])""",
            checkpoint_ids,
        )
        for row in refs:
            await conn.execute(
                "UPDATE checkpoints SET status='RELEASED' WHERE checkpoint_id=$1 AND status='SEALED'",
                row["retention_ref_id"],
            )

    async def _collect_metadata(
        self, conn, revision_id: str, checkpoint_ids: list[str]
    ):
        result: dict[str, list[asyncpg.Record]] = {}
        queries = {
            "join_alignments_v3": "SELECT * FROM join_alignments_v3 WHERE checkpoint_id=ANY($1::text[])",
            "join_barriers_v3": "SELECT * FROM join_barriers_v3 WHERE checkpoint_id=ANY($1::text[])",
            "join_recoveries_v3": """SELECT j.* FROM join_recoveries_v3 j JOIN recovery_plans_v2 p
                ON p.recovery_id=j.recovery_id WHERE p.checkpoint_id=ANY($1::text[])""",
            "recovery_plans_v2": "SELECT * FROM recovery_plans_v2 WHERE checkpoint_id=ANY($1::text[]) AND status='READY'",
            "participant_output_roots": "SELECT * FROM participant_output_roots WHERE checkpoint_id=ANY($1::text[])",
            "source_barrier_observations": "SELECT * FROM source_barrier_observations WHERE checkpoint_id=ANY($1::text[])",
            "participant_checkpoint_acks": "SELECT * FROM participant_checkpoint_acks WHERE checkpoint_id=ANY($1::text[])",
            "checkpoint_completions_v2": "SELECT * FROM checkpoint_completions_v2 WHERE checkpoint_id=ANY($1::text[])",
            "checkpoint_aborts_v2": "SELECT * FROM checkpoint_aborts_v2 WHERE checkpoint_id=ANY($1::text[])",
            "checkpoint_lifecycle_v2": "SELECT * FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=ANY($1::text[])",
            "checkpoint_intents_v2": "SELECT * FROM checkpoint_intents_v2 WHERE checkpoint_id=ANY($1::text[])",
        }
        for table, query in queries.items():
            if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table):
                result[table] = list(await conn.fetch(query, checkpoint_ids))
        # Archive every old R1 manifest except the manifest named by the rolling hot anchor.
        if await conn.fetchval(
            "SELECT to_regclass('join_retirement_manifests_v1') IS NOT NULL"
        ):
            manifests = list(
                await conn.fetch(
                    """SELECT m.* FROM join_retirement_manifests_v1 m
                   WHERE m.checkpoint_id=ANY($1::text[]) AND NOT EXISTS(
                     SELECT 1 FROM join_retention_anchors_v1 a WHERE a.operation_id=m.operation_id)""",
                    checkpoint_ids,
                )
            )
            result["join_retirement_manifests_v1"] = manifests
            operation_ids = [row["operation_id"] for row in manifests]
            result["join_retirement_rows_v1"] = (
                []
                if not operation_ids
                else list(
                    await conn.fetch(
                        "SELECT * FROM join_retirement_rows_v1 WHERE operation_id=ANY($1::text[])",
                        operation_ids,
                    )
                )
            )
        snapshots = {
            row["snapshot_object_key"]
            for row in result.get("participant_checkpoint_acks", [])
        }
        if snapshots:
            result["checkpoint_snapshot_objects"] = list(
                await conn.fetch(
                    """SELECT * FROM checkpoint_snapshot_objects o WHERE object_key=ANY($1::text[])
                   AND NOT EXISTS(SELECT 1 FROM participant_checkpoint_acks a
                     WHERE a.snapshot_object_key=o.object_key AND NOT(a.checkpoint_id=ANY($2::text[])))
                   AND NOT EXISTS(SELECT 1 FROM join_retention_anchors_v1 a
                     WHERE to_regclass('join_retention_anchors_v1') IS NOT NULL
                       AND a.snapshot_object_key=o.object_key)""",
                    list(snapshots),
                    checkpoint_ids,
                )
            )
        roots = {
            row["retention_ref_id"]
            for row in result.get("source_barrier_observations", [])
        }
        roots |= {
            row["retention_ref_id"]
            for row in result.get("participant_output_roots", [])
        }
        result["checkpoints"] = (
            []
            if not roots
            else list(
                await conn.fetch(
                    "SELECT * FROM checkpoints WHERE checkpoint_id=ANY($1::text[]) AND status='RELEASED'",
                    list(roots),
                )
            )
        )
        return result

    async def _delete_metadata(self, conn, rows, checkpoint_ids, operation_id):
        order = [
            "join_recoveries_v3",
            "recovery_plans_v2",
            "join_alignments_v3",
            "join_barriers_v3",
            "join_retirement_rows_v1",
            "join_retirement_manifests_v1",
            "participant_output_roots",
            "source_barrier_observations",
            "participant_checkpoint_acks",
            "checkpoint_snapshot_objects",
            "checkpoint_completions_v2",
            "checkpoint_aborts_v2",
            "checkpoint_lifecycle_v2",
            "checkpoint_intents_v2",
            "checkpoints",
        ]
        for table in order:
            selected = rows.get(table, [])
            if not selected:
                continue
            # Exact witnesses prevent this broad predicate from deleting unplanned rows.
            if table in {
                "checkpoint_intents_v2",
                "checkpoint_lifecycle_v2",
                "checkpoint_completions_v2",
                "checkpoint_aborts_v2",
                "source_barrier_observations",
                "participant_checkpoint_acks",
                "participant_output_roots",
                "join_alignments_v3",
                "join_barriers_v3",
            }:
                await conn.execute(
                    f"DELETE FROM {table} WHERE checkpoint_id=ANY($1::text[])",
                    checkpoint_ids,
                )
            elif table in {"recovery_plans_v2"}:
                await conn.execute(
                    f"DELETE FROM {table} WHERE checkpoint_id=ANY($1::text[]) AND status='READY'",
                    checkpoint_ids,
                )
            elif table == "join_recoveries_v3":
                await conn.execute(
                    "DELETE FROM join_recoveries_v3 WHERE recovery_id=ANY($1::text[])",
                    [row["recovery_id"] for row in selected],
                )
            elif table == "checkpoint_snapshot_objects":
                await conn.execute(
                    "DELETE FROM checkpoint_snapshot_objects WHERE object_key=ANY($1::text[])",
                    [row["object_key"] for row in selected],
                )
            elif table == "checkpoints":
                await conn.execute(
                    "DELETE FROM checkpoints WHERE checkpoint_id=ANY($1::text[])",
                    [row["checkpoint_id"] for row in selected],
                )
            elif table == "join_retirement_rows_v1":
                await conn.execute(
                    "DELETE FROM join_retirement_rows_v1 WHERE operation_id=ANY($1::text[])",
                    [row["operation_id"] for row in selected],
                )
            elif table == "join_retirement_manifests_v1":
                for row in selected:
                    prior = await conn.fetchrow(
                        "SELECT * FROM join_retirement_archive_heads_v1 WHERE revision_id=$1 AND participant_id=$2",
                        row["revision_id"],
                        row["participant_id"],
                    )
                    rolling = hashlib.sha256(
                        (
                            ("0" * 64 if prior is None else prior["rolling_digest"])
                            + canonical(self._material(row))
                        ).encode()
                    ).hexdigest()
                    await conn.execute(
                        """INSERT INTO join_retirement_archive_heads_v1 VALUES($1,$2,$3,$4,$5,$6,$7)
                      ON CONFLICT(revision_id,participant_id) DO UPDATE SET
                       retired_apply_through=EXCLUDED.retired_apply_through,
                       retired_arrival_through=EXCLUDED.retired_arrival_through,
                       archived_summary_digest=EXCLUDED.archived_summary_digest,
                       rolling_digest=EXCLUDED.rolling_digest,operation_id=EXCLUDED.operation_id""",
                        row["revision_id"],
                        row["participant_id"],
                        row["retired_apply_through"],
                        row["retired_arrival_through"],
                        row["summary_digest"],
                        rolling,
                        operation_id,
                    )
                await conn.execute(
                    "DELETE FROM join_retirement_manifests_v1 WHERE operation_id=ANY($1::text[])",
                    [row["operation_id"] for row in selected],
                )

    async def _planned_authority_cut(self, conn, revision_id, retiring_checkpoint_ids):
        required = await conn.fetchval(
            """SELECT min(v) FROM (
              SELECT intent_authority_ordinal v FROM checkpoint_intents_v2
               WHERE revision_id=$1 AND NOT(checkpoint_id=ANY($2::text[]))
              UNION ALL SELECT b.barrier_authority_ordinal
               FROM source_barrier_observations b JOIN checkpoint_intents_v2 i USING(checkpoint_id)
               WHERE i.revision_id=$1 AND NOT(b.checkpoint_id=ANY($2::text[]))
              UNION ALL SELECT a.ack_authority_ordinal
               FROM participant_checkpoint_acks a JOIN checkpoint_intents_v2 i USING(checkpoint_id)
               WHERE i.revision_id=$1 AND NOT(a.checkpoint_id=ANY($2::text[]))
              UNION ALL SELECT c.completion_authority_ordinal
               FROM checkpoint_completions_v2 c JOIN checkpoint_intents_v2 i USING(checkpoint_id)
               WHERE i.revision_id=$1 AND NOT(c.checkpoint_id=ANY($2::text[]))
              UNION ALL SELECT p.fence_authority_ordinal FROM recovery_plans_v2 p
               WHERE p.revision_id=$1 AND NOT(p.checkpoint_id=ANY($2::text[]))
              UNION ALL SELECT p.ready_authority_ordinal FROM recovery_plans_v2 p
               WHERE p.revision_id=$1 AND p.ready_authority_ordinal IS NOT NULL
                 AND NOT(p.checkpoint_id=ANY($2::text[]))) q""",
            revision_id,
            retiring_checkpoint_ids,
        )
        return 0 if required is None else required - 1

    async def _retire_authority_prefix(
        self, conn, revision_id, operation_id, planned_cut
    ):
        required = await conn.fetchval(
            """SELECT min(v) FROM (
          SELECT intent_authority_ordinal v FROM checkpoint_intents_v2 WHERE revision_id=$1
          UNION ALL SELECT b.barrier_authority_ordinal FROM source_barrier_observations b JOIN checkpoint_intents_v2 i USING(checkpoint_id) WHERE i.revision_id=$1
          UNION ALL SELECT a.ack_authority_ordinal FROM participant_checkpoint_acks a JOIN checkpoint_intents_v2 i USING(checkpoint_id) WHERE i.revision_id=$1
          UNION ALL SELECT c.completion_authority_ordinal FROM checkpoint_completions_v2 c JOIN checkpoint_intents_v2 i USING(checkpoint_id) WHERE i.revision_id=$1
          UNION ALL SELECT p.fence_authority_ordinal FROM recovery_plans_v2 p WHERE p.revision_id=$1
          UNION ALL SELECT p.ready_authority_ordinal FROM recovery_plans_v2 p WHERE p.revision_id=$1 AND p.ready_authority_ordinal IS NOT NULL
        ) q""",
            revision_id,
        )
        cut = 0 if required is None else required - 1
        if cut != planned_cut:
            raise PostgresProtocolError("authority retirement cut changed")
        prior = await conn.fetchrow(
            "SELECT retired_authority_through FROM checkpoint_archive_heads_v1 WHERE revision_id=$1",
            revision_id,
        )
        floor = 0 if prior is None else prior["retired_authority_through"]
        rows = list(
            await conn.fetch(
                "SELECT * FROM coordinator_authority_events WHERE revision_id=$1 AND authority_ordinal>$2 AND authority_ordinal<=$3 ORDER BY authority_ordinal",
                revision_id,
                floor,
                cut,
            )
        )
        await self._witness(conn, operation_id, "coordinator_authority_events", rows)
        if rows:
            await conn.execute(
                "DELETE FROM coordinator_authority_events WHERE revision_id=$1 AND authority_ordinal<=$2",
                revision_id,
                cut,
            )
        return cut, rows

    async def assert_available(self, revision_id: str, checkpoint_id: str) -> None:
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM checkpoint_intents_v2 WHERE checkpoint_id=$1 AND revision_id=$2)",
                checkpoint_id,
                revision_id,
            )
            head = await conn.fetchrow(
                "SELECT * FROM checkpoint_archive_heads_v1 WHERE revision_id=$1",
                revision_id,
            )
        if not exists and head is not None:
            raise CheckpointHorizonExpired(
                f"checkpoint evidence retired through ordinal {head['retired_checkpoint_through']}"
            )
        if not exists:
            raise PostgresProtocolError("unknown checkpoint")

    @staticmethod
    def _outcome(value: dict[str, Any], retry: bool) -> CheckpointRetentionOutcome:
        return CheckpointRetentionOutcome(
            value["operation_id"],
            value["revision_id"],
            value["retired_checkpoint_through"],
            value["retired_authority_through"],
            value["retired_checkpoints"],
            value["retired_authority_events"],
            value["retired_log_entries"],
            dict(value["log_cuts"]),
            value["rolling_digest"],
            retry,
        )
