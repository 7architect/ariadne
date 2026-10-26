"""Bounded archive of completed historical regional recovery authority (R3-R3)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from postgres_barrier_coordinator import PostgresBarrierCoordinator, canonical, decoded
from postgres_stream_v2 import PostgresProtocolError


class RegionalRecoveryHorizonExpired(PostgresProtocolError):
    """The requested regional recovery attempt is below the rolling horizon."""


@dataclass(frozen=True)
class RegionalRetentionOutcome:
    operation_id: str
    revision_id: str
    retired_plans: int
    retired_attempts: int
    retired_generations: int
    retired_cuts: int
    rolling_digest: str
    idempotent_retry: bool = False


class PostgresRegionalRetention:
    def __init__(self, coordinator: PostgresBarrierCoordinator):
        self.coordinator = coordinator
        self.pool = coordinator.pool

    @classmethod
    async def install(cls, coordinator: PostgresBarrierCoordinator):
        value = cls(coordinator)
        await value._initialize()
        return value

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS regional_archive_heads_v1(
          revision_id text PRIMARY KEY REFERENCES deployment_revisions,
          retired_plans bigint NOT NULL CHECK(retired_plans>=0),
          retired_attempts bigint NOT NULL CHECK(retired_attempts>=0),
          retired_generations bigint NOT NULL CHECK(retired_generations>=0),
          retired_cuts bigint NOT NULL CHECK(retired_cuts>=0),
          last_plan_digest text NOT NULL,prior_digest text NOT NULL,
          material_digest text NOT NULL,rolling_digest text NOT NULL UNIQUE,
          operation_id text NOT NULL UNIQUE,
          updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS regional_retirement_operations_v1(
          revision_id text PRIMARY KEY REFERENCES deployment_revisions,
          operation_id text NOT NULL UNIQUE,input_digest text NOT NULL,
          plan_json jsonb NOT NULL,outcome_json jsonb,
          status text NOT NULL CHECK(status IN('PLANNED','COMPLETED')),
          completed_at timestamptz
        );
        CREATE TABLE IF NOT EXISTS regional_retirement_rows_v1(
          operation_id text NOT NULL,table_name text NOT NULL,row_key text NOT NULL,
          row_json jsonb NOT NULL,PRIMARY KEY(operation_id,table_name,row_key)
        );
        CREATE OR REPLACE FUNCTION regional_retention_delete_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        BEGIN
          op=current_setting('ariadne.regional_retirement_operation',true);
          IF op IS NULL OR NOT EXISTS(
            SELECT 1 FROM regional_retirement_operations_v1 o
            JOIN regional_retirement_rows_v1 r USING(operation_id)
            WHERE o.operation_id=op AND o.status='PLANNED'
              AND r.table_name=TG_TABLE_NAME AND r.row_json=to_jsonb(OLD))
          THEN RAISE EXCEPTION 'regional delete lacks exact archive witness'; END IF;
          RETURN OLD;
        END $$;
        CREATE OR REPLACE FUNCTION regional_retention_witness_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text; DECLARE present boolean; DECLARE plan jsonb;
        BEGIN
          op=current_setting('ariadne.regional_retirement_operation',true);
          IF TG_OP='UPDATE' OR op IS NULL
             OR (TG_OP='INSERT' AND NEW.operation_id<>op)
             OR (TG_OP='DELETE' AND OLD.operation_id<>op)
          THEN RAISE EXCEPTION 'illegal regional witness mutation'; END IF;
          IF TG_OP='DELETE' THEN RETURN OLD; END IF;
          IF NEW.table_name NOT IN(
            'region_edge_cuts_v1','region_recovery_plans_v1',
            'regional_recovery_workers_v1','regional_recovery_attempts_v1',
            'regional_recovery_attempt_workers_v1','join_recoveries_v3',
            'recovery_plans_v2','checkpoints')
          THEN RAISE EXCEPTION 'unknown regional archive table'; END IF;
          SELECT o.plan_json INTO plan FROM regional_retirement_operations_v1 o
           WHERE o.operation_id=op AND o.status='PLANNED';
          EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I x WHERE to_jsonb(x)=$1)',
                         NEW.table_name) INTO present USING NEW.row_json;
          IF NOT present THEN RAISE EXCEPTION 'regional witness lacks live row'; END IF;
          IF NEW.table_name IN(
               'regional_recovery_workers_v1','regional_recovery_attempts_v1',
               'regional_recovery_attempt_workers_v1')
             AND NOT(plan->'plan_ids' ? (NEW.row_json->>'plan_id'))
          THEN RAISE EXCEPTION 'regional witness is outside frozen plans'; END IF;
          IF NEW.table_name IN('join_recoveries_v3','recovery_plans_v2')
             AND NOT(plan->'plan_ids' ? (NEW.row_json->>'recovery_id'))
          THEN RAISE EXCEPTION 'compatibility witness is outside frozen plans'; END IF;
          IF NEW.table_name='region_recovery_plans_v1'
             AND NOT(plan->'plan_ids' ? (NEW.row_json->>'plan_id'))
          THEN RAISE EXCEPTION 'region plan witness is outside frozen plans'; END IF;
          IF NEW.table_name='region_edge_cuts_v1' AND (
             NOT(plan->'checkpoint_ids' ? (NEW.row_json->>'checkpoint_id'))
             OR NOT(plan->'root_ids' ? (NEW.row_json->>'retention_ref_id')))
          THEN RAISE EXCEPTION 'region cut witness is outside frozen cuts'; END IF;
          IF NEW.table_name='checkpoints' AND (
             NOT(plan->'root_ids' ? (NEW.row_json->>'checkpoint_id'))
             OR NEW.row_json->>'status'<>'RELEASED')
          THEN RAISE EXCEPTION 'root witness is outside frozen region cuts'; END IF;
          IF NEW.table_name='regional_recovery_workers_v1'
             AND NEW.row_json->>'status'<>'ACTIVE'
          THEN RAISE EXCEPTION 'active regional generation cannot be archived'; END IF;
          IF NEW.table_name='region_recovery_plans_v1' AND NOT EXISTS(
             SELECT 1 FROM checkpoint_intents_v2 i
              JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
             WHERE i.checkpoint_id=NEW.row_json->>'checkpoint_id'
               AND i.checkpoint_ordinal<(SELECT max(i2.checkpoint_ordinal)
                 FROM checkpoint_intents_v2 i2 JOIN checkpoint_lifecycle_v2 l2
                   USING(checkpoint_id) WHERE i2.revision_id=i.revision_id
                   AND l2.status='COMPLETED')
               AND NOT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 w
                 WHERE w.plan_id=NEW.row_json->>'plan_id' AND w.status<>'ACTIVE'))
          THEN RAISE EXCEPTION 'regional plan is not historical ACTIVE authority'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_retention_operation_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        BEGIN
          op=current_setting('ariadne.regional_retirement_operation',true);
          IF TG_OP='DELETE' OR op IS NULL OR NEW.operation_id<>op
             OR (TG_OP='INSERT' AND NEW.status<>'PLANNED')
             OR (TG_OP='UPDATE' AND NOT(
               (OLD.status='COMPLETED' AND NEW.status='PLANNED'
                AND OLD.revision_id=NEW.revision_id)
               OR (OLD.status='PLANNED' AND NEW.status='COMPLETED'
                   AND OLD.operation_id=NEW.operation_id
                   AND OLD.input_digest=NEW.input_digest
                   AND OLD.plan_json=NEW.plan_json AND NEW.outcome_json IS NOT NULL)))
          THEN RAISE EXCEPTION 'illegal regional operation mutation'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_retention_head_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text; DECLARE plan jsonb; DECLARE material text;
        DECLARE prior regional_archive_heads_v1%ROWTYPE;
        DECLARE plan_count bigint; DECLARE attempt_count bigint;
        DECLARE generation_count bigint; DECLARE cut_count bigint;
        BEGIN
          op=current_setting('ariadne.regional_retirement_operation',true);
          SELECT o.plan_json INTO plan FROM regional_retirement_operations_v1 o
             WHERE o.operation_id=op AND o.status='PLANNED';
          SELECT * INTO prior FROM regional_archive_heads_v1
             WHERE revision_id=NEW.revision_id;
          SELECT count(*) FILTER(WHERE table_name='region_recovery_plans_v1'),
                 count(*) FILTER(WHERE table_name='regional_recovery_attempts_v1'),
                 count(*) FILTER(WHERE table_name='regional_recovery_workers_v1'),
                 count(*) FILTER(WHERE table_name='region_edge_cuts_v1')
            INTO plan_count,attempt_count,generation_count,cut_count
            FROM regional_retirement_rows_v1 WHERE operation_id=op;
          SELECT encode(sha256(convert_to(COALESCE(jsonb_agg(jsonb_build_object(
                   'table',table_name,'key',row_key,'row',row_json)
                   ORDER BY table_name,row_key),'[]'::jsonb)::text,'UTF8')),'hex')
            INTO material FROM regional_retirement_rows_v1 WHERE operation_id=op;
          IF NEW.operation_id IS DISTINCT FROM op OR plan IS NULL
             OR NOT(plan ?& ARRAY['plan_ids','checkpoint_ids','root_ids',
                                   'plan_digests','prior_totals'])
             OR jsonb_array_length(plan->'plan_ids')=0
             OR jsonb_array_length(plan->'checkpoint_ids')=0
             OR jsonb_array_length(plan->'root_ids')=0
             OR plan->>'prior_digest' IS DISTINCT FROM NEW.prior_digest
             OR (plan->'prior_totals'->>'retired_plans')::bigint
                  IS DISTINCT FROM COALESCE(prior.retired_plans,0)
             OR (plan->'prior_totals'->>'retired_attempts')::bigint
                  IS DISTINCT FROM COALESCE(prior.retired_attempts,0)
             OR (plan->'prior_totals'->>'retired_generations')::bigint
                  IS DISTINCT FROM COALESCE(prior.retired_generations,0)
             OR (plan->'prior_totals'->>'retired_cuts')::bigint
                  IS DISTINCT FROM COALESCE(prior.retired_cuts,0)
             OR NEW.prior_digest IS DISTINCT FROM COALESCE(prior.rolling_digest,repeat('0',64))
             OR NEW.material_digest IS DISTINCT FROM material
             OR NEW.rolling_digest IS DISTINCT FROM encode(sha256(convert_to(
                  NEW.prior_digest||material,'UTF8')),'hex')
             OR NEW.retired_plans IS DISTINCT FROM
                  (plan->'prior_totals'->>'retired_plans')::bigint+plan_count
             OR NEW.retired_attempts IS DISTINCT FROM
                  (plan->'prior_totals'->>'retired_attempts')::bigint+attempt_count
             OR NEW.retired_generations IS DISTINCT FROM
                  (plan->'prior_totals'->>'retired_generations')::bigint+generation_count
             OR NEW.retired_cuts IS DISTINCT FROM
                  (plan->'prior_totals'->>'retired_cuts')::bigint+cut_count
             OR NEW.last_plan_digest IS DISTINCT FROM (plan->'plan_digests'->>
                  (jsonb_array_length(plan->'plan_digests')-1))
          THEN RAISE EXCEPTION 'regional archive head lacks active operation'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION regional_retention_converged()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE current_op regional_retirement_operations_v1%ROWTYPE;
        BEGIN
          SELECT * INTO current_op FROM regional_retirement_operations_v1
           WHERE revision_id=NEW.revision_id;
          IF current_op.status<>'COMPLETED' OR current_op.outcome_json IS NULL
             OR EXISTS(SELECT 1 FROM regional_retirement_rows_v1 r
                        WHERE r.operation_id=current_op.operation_id)
             OR NOT EXISTS(SELECT 1 FROM regional_archive_heads_v1 h
                  WHERE h.revision_id=current_op.revision_id
                    AND h.operation_id=current_op.operation_id
                    AND h.rolling_digest IS NOT DISTINCT FROM current_op.outcome_json->>'rolling_digest'
                    AND h.material_digest IS NOT DISTINCT FROM current_op.outcome_json->>'material_digest'
                    AND h.retired_plans IS NOT DISTINCT FROM (current_op.plan_json->'prior_totals'->>'retired_plans')::bigint+
                      (current_op.outcome_json->>'retired_plans')::bigint
                    AND h.retired_attempts IS NOT DISTINCT FROM (current_op.plan_json->'prior_totals'->>'retired_attempts')::bigint+
                      (current_op.outcome_json->>'retired_attempts')::bigint
                    AND h.retired_generations IS NOT DISTINCT FROM (current_op.plan_json->'prior_totals'->>'retired_generations')::bigint+
                      (current_op.outcome_json->>'retired_generations')::bigint
                    AND h.retired_cuts IS NOT DISTINCT FROM (current_op.plan_json->'prior_totals'->>'retired_cuts')::bigint+
                      (current_op.outcome_json->>'retired_cuts')::bigint)
             OR EXISTS(SELECT 1 FROM jsonb_array_elements_text(
                    current_op.plan_json->'plan_ids') plan
                  WHERE EXISTS(SELECT 1 FROM region_recovery_plans_v1 r
                                WHERE r.plan_id=plan.value)
                     OR EXISTS(SELECT 1 FROM regional_recovery_workers_v1 w
                                WHERE w.plan_id=plan.value)
                     OR EXISTS(SELECT 1 FROM regional_recovery_attempts_v1 a
                                WHERE a.plan_id=plan.value)
                     OR EXISTS(SELECT 1 FROM regional_recovery_attempt_workers_v1 a
                                WHERE a.plan_id=plan.value))
             OR EXISTS(SELECT 1 FROM regional_recovery_workers_v1 w
                  WHERE w.revision_id=current_op.revision_id AND w.status<>'ACTIVE')
             OR EXISTS(SELECT 1 FROM recovery_plans_v2 p
                  WHERE p.revision_id=current_op.revision_id AND p.status='RESTORING'
                    AND NOT EXISTS(SELECT 1 FROM region_recovery_plans_v1 r
                      WHERE r.plan_id=p.recovery_id))
          THEN RAISE EXCEPTION 'regional retirement did not converge'; END IF;
          RETURN NULL;
        END $$;
        DROP TRIGGER IF EXISTS regional_retirement_operation_guard
          ON regional_retirement_operations_v1;
        CREATE TRIGGER regional_retirement_operation_guard BEFORE INSERT OR UPDATE OR DELETE
          ON regional_retirement_operations_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_retention_operation_guard();
        DROP TRIGGER IF EXISTS regional_retirement_operation_convergence
          ON regional_retirement_operations_v1;
        CREATE CONSTRAINT TRIGGER regional_retirement_operation_convergence
          AFTER INSERT OR UPDATE ON regional_retirement_operations_v1
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION regional_retention_converged();
        DROP TRIGGER IF EXISTS regional_retirement_witness_guard
          ON regional_retirement_rows_v1;
        CREATE TRIGGER regional_retirement_witness_guard BEFORE INSERT OR UPDATE OR DELETE
          ON regional_retirement_rows_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_retention_witness_guard();
        DROP TRIGGER IF EXISTS regional_archive_head_guard ON regional_archive_heads_v1;
        CREATE TRIGGER regional_archive_head_guard BEFORE INSERT OR UPDATE
          ON regional_archive_heads_v1 FOR EACH ROW
          EXECUTE FUNCTION regional_retention_head_guard();
        DROP TRIGGER IF EXISTS regional_archive_head_no_delete ON regional_archive_heads_v1;
        CREATE TRIGGER regional_archive_head_no_delete BEFORE DELETE
          ON regional_archive_heads_v1 FOR EACH ROW
          EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        """
        async with self.pool.acquire() as conn, conn.transaction():
            if not await conn.fetchval(
                "SELECT to_regclass('checkpoint_retirement_operations_v1') IS NOT NULL"
            ):
                raise PostgresProtocolError("R3-R3 requires R3-R2 retention guards")
            await conn.execute(ddl)
            replacements = {
                "region_edge_cuts_v1": "region_edge_cut_immutable",
                "region_recovery_plans_v1": "region_plan_immutable",
                "regional_recovery_workers_v1": "regional_generation_no_delete",
                "regional_recovery_attempts_v1": "regional_attempt_header_immutable",
                "regional_recovery_attempt_workers_v1": "regional_attempt_worker_immutable",
            }
            for table, trigger in replacements.items():
                await conn.execute(f"DROP TRIGGER IF EXISTS {trigger} ON {table}")
                if table in {
                    "region_edge_cuts_v1",
                    "region_recovery_plans_v1",
                    "regional_recovery_attempts_v1",
                    "regional_recovery_attempt_workers_v1",
                }:
                    function = (
                        "region_reject_mutation"
                        if table.startswith("region_")
                        else "regional_reject_mutation"
                    )
                    await conn.execute(
                        f"""CREATE TRIGGER {trigger}_update BEFORE UPDATE ON {table}
                        FOR EACH ROW EXECUTE FUNCTION {function}()"""
                    )
                await conn.execute(
                    f"""DROP TRIGGER IF EXISTS regional_retention_delete ON {table};
                    CREATE TRIGGER regional_retention_delete BEFORE DELETE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION regional_retention_delete_guard()"""
                )

    @staticmethod
    def _material(value: Any) -> Any:
        if isinstance(value, str) and value[:1] in {"{", "["}:
            try:
                return json.loads(value)
            except (TypeError, ValueError):
                return value
        return value

    async def _witness(self, conn, operation_id: str, table: str, rows):
        for index, row in enumerate(rows):
            db_json = await conn.fetchval(
                f"SELECT to_jsonb(x) FROM {table} x WHERE to_jsonb(x)=$1::jsonb",
                json.dumps(
                    {key: self._material(value) for key, value in dict(row).items()},
                    default=str,
                ),
            )
            if db_json is None:
                # Timestamp textual rendering differs between Python and PostgreSQL;
                # select by stable identity for those tables.
                keys = {
                    "region_edge_cuts_v1": ("checkpoint_id", "edge_id"),
                    "region_recovery_plans_v1": ("plan_id",),
                    "regional_recovery_workers_v1": ("plan_id", "participant_id"),
                    "regional_recovery_attempts_v1": ("plan_id", "attempt_ordinal"),
                    "regional_recovery_attempt_workers_v1": (
                        "plan_id",
                        "attempt_ordinal",
                        "participant_id",
                    ),
                    "join_recoveries_v3": (
                        "revision_id",
                        "participant_id",
                        "recovery_id",
                    ),
                    "recovery_plans_v2": ("recovery_id",),
                    "checkpoints": ("checkpoint_id",),
                }[table]
                where = " AND ".join(
                    f"{key}=${offset + 1}" for offset, key in enumerate(keys)
                )
                db_json = await conn.fetchval(
                    f"SELECT to_jsonb(x) FROM {table} x WHERE {where}",
                    *[row[key] for key in keys],
                )
            await conn.execute(
                "INSERT INTO regional_retirement_rows_v1 VALUES($1,$2,$3,$4::jsonb)",
                operation_id,
                table,
                f"{index}",
                canonical(decoded(db_json)),
            )

    async def retire_historical(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        operation_id: str,
    ) -> RegionalRetentionOutcome:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(conn, revision_id)
            self.coordinator._check_graph(revision, graph_version, graph_digest)
            await self.coordinator._assert_owner(
                conn, revision_id, owner_id, owner_epoch
            )
            current = await conn.fetchrow(
                "SELECT * FROM regional_retirement_operations_v1 WHERE revision_id=$1 FOR UPDATE",
                revision_id,
            )
            if current is not None and current["operation_id"] == operation_id:
                if current["status"] != "COMPLETED":
                    raise PostgresProtocolError("regional retirement incomplete")
                return self._outcome(decoded(current["outcome_json"]), True)
            if await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 p
                   WHERE p.revision_id=$1 AND p.status='RESTORING'
                     AND NOT EXISTS(SELECT 1 FROM region_recovery_plans_v1 r
                       WHERE r.plan_id=p.recovery_id))""",
                revision_id,
            ):
                raise PostgresProtocolError(
                    "active global recovery pins regional history"
                )
            if await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1
                   WHERE revision_id=$1 AND status<>'ACTIVE')""",
                revision_id,
            ):
                raise PostgresProtocolError("active regional generation pins history")
            latest = await conn.fetchval(
                """SELECT max(i.checkpoint_ordinal) FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status='COMPLETED'""",
                revision_id,
            )
            plans = list(
                await conn.fetch(
                    """SELECT r.* FROM region_recovery_plans_v1 r
                       JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                       WHERE r.revision_id=$1 AND i.checkpoint_ordinal<$2
                         AND NOT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 w
                           WHERE w.plan_id=r.plan_id AND w.status<>'ACTIVE')
                       ORDER BY i.checkpoint_ordinal,r.plan_id""",
                    revision_id,
                    latest,
                )
            )
            if not plans:
                raise PostgresProtocolError(
                    "regional retirement has no historical plan"
                )
            plan_ids = [row["plan_id"] for row in plans]
            if await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM join_workers w
                   WHERE w.revision_id=$1 AND (w.phase<>'LIVE' OR EXISTS(
                     SELECT 1 FROM join_invocations_v3 i WHERE i.revision_id=w.revision_id
                       AND i.participant_id=w.participant_id)))""",
                revision_id,
            ):
                raise PostgresProtocolError(
                    "current join recovery still needs region history"
                )
            rows = {
                "regional_recovery_attempt_workers_v1": list(
                    await conn.fetch(
                        "SELECT * FROM regional_recovery_attempt_workers_v1 WHERE plan_id=ANY($1::text[])",
                        plan_ids,
                    )
                ),
                "regional_recovery_attempts_v1": list(
                    await conn.fetch(
                        "SELECT * FROM regional_recovery_attempts_v1 WHERE plan_id=ANY($1::text[])",
                        plan_ids,
                    )
                ),
                "regional_recovery_workers_v1": list(
                    await conn.fetch(
                        "SELECT * FROM regional_recovery_workers_v1 WHERE plan_id=ANY($1::text[])",
                        plan_ids,
                    )
                ),
                "join_recoveries_v3": list(
                    await conn.fetch(
                        "SELECT * FROM join_recoveries_v3 WHERE recovery_id=ANY($1::text[])",
                        plan_ids,
                    )
                ),
                "recovery_plans_v2": list(
                    await conn.fetch(
                        "SELECT * FROM recovery_plans_v2 WHERE recovery_id=ANY($1::text[])",
                        plan_ids,
                    )
                ),
                "region_edge_cuts_v1": list(
                    await conn.fetch(
                        "SELECT * FROM region_edge_cuts_v1 WHERE checkpoint_id=ANY($1::text[])",
                        [row["checkpoint_id"] for row in plans],
                    )
                ),
                "region_recovery_plans_v1": plans,
            }
            root_ids = [row["retention_ref_id"] for row in rows["region_edge_cuts_v1"]]
            rows["checkpoints"] = (
                []
                if not root_ids
                else list(
                    await conn.fetch(
                        "SELECT * FROM checkpoints WHERE checkpoint_id=ANY($1::text[])",
                        root_ids,
                    )
                )
            )
            prior = await conn.fetchrow(
                "SELECT * FROM regional_archive_heads_v1 WHERE revision_id=$1 FOR UPDATE",
                revision_id,
            )
            totals = (
                (0, 0, 0, 0)
                if prior is None
                else (
                    prior["retired_plans"],
                    prior["retired_attempts"],
                    prior["retired_generations"],
                    prior["retired_cuts"],
                )
            )
            identity = {
                "revision_id": revision_id,
                "plan_ids": plan_ids,
                "checkpoint_ids": sorted({row["checkpoint_id"] for row in plans}),
                "root_ids": sorted(root_ids),
                "latest_completed_ordinal": latest,
                "plan_digests": [row["plan_digest"] for row in plans],
                "prior_digest": "0" * 64 if prior is None else prior["rolling_digest"],
                "prior_totals": {
                    "retired_plans": totals[0],
                    "retired_attempts": totals[1],
                    "retired_generations": totals[2],
                    "retired_cuts": totals[3],
                },
            }
            input_digest = hashlib.sha256(canonical(identity).encode()).hexdigest()
            await conn.execute(
                "SELECT set_config('ariadne.regional_retirement_operation',$1,true)",
                operation_id,
            )
            if current is None:
                await conn.execute(
                    "INSERT INTO regional_retirement_operations_v1 VALUES($1,$2,$3,$4::jsonb,NULL,'PLANNED',NULL)",
                    revision_id,
                    operation_id,
                    input_digest,
                    canonical(identity),
                )
            else:
                await conn.execute(
                    """UPDATE regional_retirement_operations_v1 SET operation_id=$2,
                       input_digest=$3,plan_json=$4::jsonb,outcome_json=NULL,
                       status='PLANNED',completed_at=NULL WHERE revision_id=$1""",
                    revision_id,
                    operation_id,
                    input_digest,
                    canonical(identity),
                )
            for table, table_rows in rows.items():
                if table == "checkpoints":
                    continue
                await self._witness(conn, operation_id, table, table_rows)
            for table in (
                "regional_recovery_attempt_workers_v1",
                "regional_recovery_attempts_v1",
                "regional_recovery_workers_v1",
                "join_recoveries_v3",
                "recovery_plans_v2",
            ):
                await conn.execute(
                    f"DELETE FROM {table} WHERE "
                    + (
                        "recovery_id=ANY($1::text[])"
                        if table in {"join_recoveries_v3", "recovery_plans_v2"}
                        else "plan_id=ANY($1::text[])"
                    ),
                    plan_ids,
                )
            await conn.execute(
                "DELETE FROM region_recovery_plans_v1 WHERE plan_id=ANY($1::text[])",
                plan_ids,
            )
            await conn.execute(
                "DELETE FROM region_edge_cuts_v1 WHERE checkpoint_id=ANY($1::text[])",
                [row["checkpoint_id"] for row in plans],
            )
            for root_id in root_ids:
                await conn.execute(
                    "UPDATE checkpoints SET status='RELEASED' WHERE checkpoint_id=$1 AND status='SEALED'",
                    root_id,
                )
            if root_ids:
                rows["checkpoints"] = list(
                    await conn.fetch(
                        "SELECT * FROM checkpoints WHERE checkpoint_id=ANY($1::text[])",
                        root_ids,
                    )
                )
                await self._witness(
                    conn, operation_id, "checkpoints", rows["checkpoints"]
                )
                await conn.execute(
                    "DELETE FROM checkpoints WHERE checkpoint_id=ANY($1::text[])",
                    root_ids,
                )
            prior_digest = "0" * 64 if prior is None else prior["rolling_digest"]
            material_digest = await conn.fetchval(
                """SELECT encode(sha256(convert_to(COALESCE(jsonb_agg(
                     jsonb_build_object('table',table_name,'key',row_key,'row',row_json)
                     ORDER BY table_name,row_key),'[]'::jsonb)::text,'UTF8')),'hex')
                   FROM regional_retirement_rows_v1 WHERE operation_id=$1""",
                operation_id,
            )
            rolling = hashlib.sha256(
                (prior_digest + material_digest).encode()
            ).hexdigest()
            await conn.execute(
                """INSERT INTO regional_archive_heads_v1(
                   revision_id,retired_plans,retired_attempts,retired_generations,
                   retired_cuts,last_plan_digest,prior_digest,material_digest,
                   rolling_digest,operation_id,updated_at) VALUES(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp())
                   ON CONFLICT(revision_id) DO UPDATE SET
                    retired_plans=EXCLUDED.retired_plans,
                    retired_attempts=EXCLUDED.retired_attempts,
                    retired_generations=EXCLUDED.retired_generations,
                    retired_cuts=EXCLUDED.retired_cuts,
                    last_plan_digest=EXCLUDED.last_plan_digest,
                    prior_digest=EXCLUDED.prior_digest,
                    material_digest=EXCLUDED.material_digest,
                    rolling_digest=EXCLUDED.rolling_digest,
                    operation_id=EXCLUDED.operation_id,updated_at=clock_timestamp()""",
                revision_id,
                totals[0] + len(plans),
                totals[1] + len(rows["regional_recovery_attempts_v1"]),
                totals[2] + len(rows["regional_recovery_workers_v1"]),
                totals[3] + len(rows["region_edge_cuts_v1"]),
                plans[-1]["plan_digest"],
                prior_digest,
                material_digest,
                rolling,
                operation_id,
            )
            outcome = {
                "operation_id": operation_id,
                "revision_id": revision_id,
                "retired_plans": len(plans),
                "retired_attempts": len(rows["regional_recovery_attempts_v1"]),
                "retired_generations": len(rows["regional_recovery_workers_v1"]),
                "retired_cuts": len(rows["region_edge_cuts_v1"]),
                "material_digest": material_digest,
                "rolling_digest": rolling,
            }
            await conn.execute(
                "DELETE FROM regional_retirement_rows_v1 WHERE operation_id=$1",
                operation_id,
            )
            await conn.execute(
                """UPDATE regional_retirement_operations_v1 SET status='COMPLETED',
                   outcome_json=$2::jsonb,completed_at=clock_timestamp()
                   WHERE revision_id=$1""",
                revision_id,
                canonical(outcome),
            )
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        return self._outcome(outcome, False)

    async def assert_plan_available(self, revision_id: str, plan_id: str) -> None:
        async with self.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM region_recovery_plans_v1 WHERE plan_id=$1 AND revision_id=$2)",
                plan_id,
                revision_id,
            )
            head = await conn.fetchrow(
                "SELECT * FROM regional_archive_heads_v1 WHERE revision_id=$1",
                revision_id,
            )
        if not exists and head is not None:
            raise RegionalRecoveryHorizonExpired(
                f"regional history retired through {head['last_plan_digest']}"
            )
        if not exists:
            raise PostgresProtocolError("unknown regional recovery plan")

    @staticmethod
    def _outcome(value, retry):
        return RegionalRetentionOutcome(
            value["operation_id"],
            value["revision_id"],
            value["retired_plans"],
            value["retired_attempts"],
            value["retired_generations"],
            value["retired_cuts"],
            value["rolling_digest"],
            retry,
        )
