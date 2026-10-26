"""Bounded join-mailbox retirement anchored by the latest completed snapshot.

This focused R3-R1 prototype retires only join-local receipts, turns, and
invocation history.  It does not archive coordinator checkpoints, ordered-log
entries, regional plans/cuts, or stream append/object metadata.
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

FaultHook = Callable[[str], None] | None


class JoinReplayHorizonExpired(PostgresProtocolError):
    """The requested checkpoint needs mailbox evidence already summarized."""


@dataclass(frozen=True)
class JoinRetentionOutcome:
    operation_id: str
    revision_id: str
    participant_id: str
    checkpoint_id: str
    retired_apply_through: int
    retired_arrival_through: int
    retired_turns: int
    retired_receipts: int
    retired_invocations: int
    input_cuts: dict[str, int]
    summary_digest: str
    idempotent_retry: bool = False


def _hit(fault: FaultHook, point: str) -> None:
    if fault is not None:
        fault(point)


class PostgresJoinRetention:
    def __init__(self, coordinator: PostgresBarrierCoordinator):
        self.coordinator = coordinator
        self.pool = coordinator.pool
        self.schema = coordinator.schema

    @classmethod
    async def install(cls, coordinator: PostgresBarrierCoordinator):
        retention = cls(coordinator)
        await retention._initialize()
        return retention

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS join_retention_anchors_v1 (
            revision_id text NOT NULL,
            participant_id text NOT NULL,
            retired_apply_through bigint NOT NULL CHECK(retired_apply_through>=0),
            retired_arrival_through bigint NOT NULL CHECK(retired_arrival_through>=0),
            retained_checkpoint_id text NOT NULL REFERENCES checkpoint_completions_v2,
            snapshot_object_key text NOT NULL REFERENCES checkpoint_snapshot_objects,
            snapshot_payload_digest text NOT NULL,
            mailbox_highwater bigint NOT NULL CHECK(mailbox_highwater>=retired_apply_through),
            state_json jsonb NOT NULL,
            state_digest text NOT NULL,
            input_cuts jsonb NOT NULL,
            summary_digest text NOT NULL UNIQUE,
            operation_id text NOT NULL UNIQUE,
            retired_turns bigint NOT NULL CHECK(retired_turns>=0),
            retired_receipts bigint NOT NULL CHECK(retired_receipts>=0),
            retired_invocations bigint NOT NULL CHECK(retired_invocations>=0),
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(revision_id,participant_id),
            FOREIGN KEY(revision_id,participant_id)
              REFERENCES join_workers(revision_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS join_retirement_manifests_v1 (
            operation_id text PRIMARY KEY,
            revision_id text NOT NULL,
            participant_id text NOT NULL,
            prior_summary_digest text NOT NULL,
            checkpoint_id text NOT NULL REFERENCES checkpoint_completions_v2,
            snapshot_object_key text NOT NULL REFERENCES checkpoint_snapshot_objects,
            snapshot_payload_digest text NOT NULL,
            mailbox_highwater bigint NOT NULL,
            retired_apply_from bigint NOT NULL,
            retired_apply_through bigint NOT NULL,
            retired_arrival_from bigint NOT NULL,
            retired_arrival_through bigint NOT NULL,
            state_json jsonb NOT NULL,
            state_digest text NOT NULL,
            input_cuts jsonb NOT NULL,
            row_counts jsonb NOT NULL,
            summary_digest text NOT NULL UNIQUE,
            manifest_digest text NOT NULL UNIQUE,
            status text NOT NULL CHECK(status IN('PLANNED','COMPLETED')),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            completed_at timestamptz,
            FOREIGN KEY(revision_id,participant_id)
              REFERENCES join_workers(revision_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS join_retirement_rows_v1 (
            operation_id text NOT NULL REFERENCES join_retirement_manifests_v1,
            row_kind text NOT NULL CHECK(row_kind IN('TURN','RECEIPT','INVOCATION')),
            row_key text NOT NULL,
            row_json jsonb NOT NULL,
            row_digest text NOT NULL,
            PRIMARY KEY(operation_id,row_kind,row_key)
        );

        CREATE OR REPLACE FUNCTION join_retention_reject_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          RAISE EXCEPTION 'join retirement authority is immutable';
        END $$;
        CREATE OR REPLACE FUNCTION join_retention_row_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        DECLARE kind text;
        DECLARE key text;
        BEGIN
          IF TG_OP='UPDATE' THEN
            RAISE EXCEPTION 'join mailbox evidence is immutable';
          END IF;
          op=current_setting('ariadne.retirement_operation',true);
          IF TG_TABLE_NAME='join_turns_v3' THEN
            kind='TURN'; key=OLD.apply_ordinal::text;
          ELSIF TG_TABLE_NAME='join_receipts_v3' THEN
            kind='RECEIPT'; key=OLD.arrival_ordinal::text;
          ELSE
            kind='INVOCATION'; key=OLD.token;
          END IF;
          IF op IS NULL OR NOT EXISTS(
            SELECT 1 FROM join_retirement_manifests_v1 m
            JOIN join_retirement_rows_v1 r USING(operation_id)
            WHERE m.operation_id=op AND m.status='PLANNED'
              AND m.revision_id=OLD.revision_id
              AND m.participant_id=OLD.participant_id
              AND r.row_kind=kind AND r.row_key=key
              AND r.row_json=to_jsonb(OLD))
          THEN RAISE EXCEPTION 'join mailbox delete lacks retirement manifest'; END IF;
          RETURN OLD;
        END $$;
        CREATE OR REPLACE FUNCTION join_retirement_row_insert_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE m join_retirement_manifests_v1%ROWTYPE;
        BEGIN
          SELECT * INTO m FROM join_retirement_manifests_v1
           WHERE operation_id=NEW.operation_id AND status='PLANNED';
          IF m.operation_id IS NULL
             OR (NEW.row_kind='TURN' AND (
                  NEW.row_key::bigint NOT BETWEEN m.retired_apply_from
                                              AND m.retired_apply_through
                  OR NOT EXISTS(SELECT 1 FROM join_turns_v3 t
                    WHERE t.revision_id=m.revision_id
                      AND t.participant_id=m.participant_id
                      AND t.apply_ordinal=NEW.row_key::bigint
                      AND to_jsonb(t)=NEW.row_json)))
             OR (NEW.row_kind='RECEIPT' AND (
                  NEW.row_key::bigint NOT BETWEEN m.retired_arrival_from
                                              AND m.retired_arrival_through
                  OR NOT EXISTS(SELECT 1 FROM join_receipts_v3 r
                    WHERE r.revision_id=m.revision_id
                      AND r.participant_id=m.participant_id
                      AND r.arrival_ordinal=NEW.row_key::bigint
                      AND to_jsonb(r)=NEW.row_json)))
             OR (NEW.row_kind='INVOCATION' AND NOT EXISTS(
                  SELECT 1 FROM join_invocation_history_v3 h
                   WHERE h.revision_id=m.revision_id
                     AND h.participant_id=m.participant_id
                     AND h.token=NEW.row_key
                     AND h.arrival_ordinal BETWEEN m.retired_arrival_from
                                               AND m.retired_arrival_through
                     AND to_jsonb(h)=NEW.row_json))
          THEN RAISE EXCEPTION 'join retirement row lacks live exact evidence'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_pin_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        DECLARE expected bigint;
        BEGIN
          IF OLD.ref_kind<>'JOIN_INPUT' THEN RETURN NEW; END IF;
          IF (NEW.ref_kind,NEW.ref_id,NEW.channel,NEW.partition_id,NEW.status)
               IS DISTINCT FROM
             (OLD.ref_kind,OLD.ref_id,OLD.channel,OLD.partition_id,OLD.status)
          THEN RAISE EXCEPTION 'join replay root identity is immutable'; END IF;
          IF NEW.sequence=OLD.sequence THEN RETURN NEW; END IF;
          op=current_setting('ariadne.retirement_operation',true);
          SELECT (m.input_cuts->>(NEW.channel||'/'||NEW.partition_id))::bigint
            INTO expected FROM join_retirement_manifests_v1 m
           WHERE m.operation_id=op AND m.status='PLANNED';
          IF op IS NULL OR NEW.sequence<=OLD.sequence OR NEW.sequence IS DISTINCT FROM expected
          THEN RAISE EXCEPTION 'JOIN_INPUT advance lacks retirement manifest'; END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_retention_anchor_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE op text;
        DECLARE m join_retirement_manifests_v1%ROWTYPE;
        BEGIN
          op=current_setting('ariadne.retirement_operation',true);
          SELECT * INTO m FROM join_retirement_manifests_v1
           WHERE operation_id=op AND status='PLANNED';
          IF m.operation_id IS NULL
             OR (NEW.revision_id,NEW.participant_id,NEW.retired_apply_through,
                 NEW.retired_arrival_through,NEW.retained_checkpoint_id,
                 NEW.snapshot_object_key,NEW.snapshot_payload_digest,
                 NEW.mailbox_highwater,NEW.state_json,NEW.state_digest,
                 NEW.input_cuts,NEW.summary_digest,NEW.operation_id)
                IS DISTINCT FROM
                (m.revision_id,m.participant_id,m.retired_apply_through,
                 m.retired_arrival_through,m.checkpoint_id,m.snapshot_object_key,
                 m.snapshot_payload_digest,m.mailbox_highwater,m.state_json,
                 m.state_digest,m.input_cuts,m.summary_digest,m.operation_id)
             OR (TG_OP='UPDATE' AND (
                 NEW.retired_apply_through<=OLD.retired_apply_through
                 OR NEW.retired_arrival_through<=OLD.retired_arrival_through
                 OR NEW.retired_turns<OLD.retired_turns
                 OR NEW.retired_receipts<OLD.retired_receipts
                 OR NEW.retired_invocations<OLD.retired_invocations))
          THEN RAISE EXCEPTION 'join retention anchor lacks exact manifest'; END IF;
          NEW.updated_at=clock_timestamp();
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_retirement_manifest_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF NEW.operation_id IS DISTINCT FROM OLD.operation_id
             OR NEW.revision_id IS DISTINCT FROM OLD.revision_id
             OR NEW.participant_id IS DISTINCT FROM OLD.participant_id
             OR NEW.prior_summary_digest IS DISTINCT FROM OLD.prior_summary_digest
             OR NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id
             OR NEW.snapshot_object_key IS DISTINCT FROM OLD.snapshot_object_key
             OR NEW.snapshot_payload_digest IS DISTINCT FROM OLD.snapshot_payload_digest
             OR NEW.mailbox_highwater IS DISTINCT FROM OLD.mailbox_highwater
             OR NEW.retired_apply_from IS DISTINCT FROM OLD.retired_apply_from
             OR NEW.retired_apply_through IS DISTINCT FROM OLD.retired_apply_through
             OR NEW.retired_arrival_from IS DISTINCT FROM OLD.retired_arrival_from
             OR NEW.retired_arrival_through IS DISTINCT FROM OLD.retired_arrival_through
             OR NEW.state_json IS DISTINCT FROM OLD.state_json
             OR NEW.state_digest IS DISTINCT FROM OLD.state_digest
             OR NEW.input_cuts IS DISTINCT FROM OLD.input_cuts
             OR NEW.row_counts IS DISTINCT FROM OLD.row_counts
             OR NEW.summary_digest IS DISTINCT FROM OLD.summary_digest
             OR NEW.manifest_digest IS DISTINCT FROM OLD.manifest_digest
             OR NEW.created_at IS DISTINCT FROM OLD.created_at
             OR OLD.status<>'PLANNED' OR NEW.status<>'COMPLETED'
             OR NOT EXISTS(SELECT 1 FROM join_retention_anchors_v1 a
                 WHERE a.operation_id=NEW.operation_id
                   AND a.summary_digest=NEW.summary_digest)
             OR EXISTS(SELECT 1 FROM join_retirement_rows_v1 r
                 WHERE r.operation_id=NEW.operation_id AND (
                   (r.row_kind='TURN' AND EXISTS(SELECT 1 FROM join_turns_v3 t
                     WHERE t.revision_id=NEW.revision_id AND t.participant_id=NEW.participant_id
                       AND t.apply_ordinal=r.row_key::bigint))
                   OR (r.row_kind='RECEIPT' AND EXISTS(SELECT 1 FROM join_receipts_v3 x
                     WHERE x.revision_id=NEW.revision_id AND x.participant_id=NEW.participant_id
                       AND x.arrival_ordinal=r.row_key::bigint))
                   OR (r.row_kind='INVOCATION' AND EXISTS(SELECT 1 FROM join_invocation_history_v3 h
                     WHERE h.revision_id=NEW.revision_id AND h.participant_id=NEW.participant_id
                       AND h.token=r.row_key))))
          THEN RAISE EXCEPTION 'join retirement completion is not atomic'; END IF;
          NEW.completed_at=clock_timestamp();
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_retirement_manifest_commit_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE m join_retirement_manifests_v1%ROWTYPE;
        DECLARE anchor join_retention_anchors_v1%ROWTYPE;
        DECLARE ack participant_checkpoint_acks%ROWTYPE;
        DECLARE latest_checkpoint text;
        DECLARE turn_count bigint;
        DECLARE receipt_count bigint;
        DECLARE invocation_count bigint;
        BEGIN
          SELECT * INTO m FROM join_retirement_manifests_v1
           WHERE operation_id=NEW.operation_id;
          SELECT i.checkpoint_id INTO latest_checkpoint
            FROM checkpoint_intents_v2 i
            JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
            JOIN checkpoint_completions_v2 c USING(checkpoint_id)
           WHERE i.revision_id=m.revision_id AND l.status='COMPLETED'
           ORDER BY i.checkpoint_ordinal DESC LIMIT 1;
          SELECT * INTO ack FROM participant_checkpoint_acks
           WHERE checkpoint_id=m.checkpoint_id AND participant_id=m.participant_id;
          SELECT * INTO anchor FROM join_retention_anchors_v1
           WHERE revision_id=m.revision_id AND participant_id=m.participant_id;
          SELECT count(*) INTO turn_count FROM join_retirement_rows_v1
           WHERE operation_id=m.operation_id AND row_kind='TURN';
          SELECT count(*) INTO receipt_count FROM join_retirement_rows_v1
           WHERE operation_id=m.operation_id AND row_kind='RECEIPT';
          SELECT count(*) INTO invocation_count FROM join_retirement_rows_v1
           WHERE operation_id=m.operation_id AND row_kind='INVOCATION';
          IF m.status<>'COMPLETED' OR latest_checkpoint IS DISTINCT FROM m.checkpoint_id
             OR (ack.snapshot_object_key,ack.snapshot_payload_digest,ack.mailbox_highwater)
                  IS DISTINCT FROM
                (m.snapshot_object_key,m.snapshot_payload_digest,m.mailbox_highwater)
             OR (m.row_counts->>'turns')::bigint<>turn_count
             OR (m.row_counts->>'receipts')::bigint<>receipt_count
             OR (m.row_counts->>'invocations')::bigint<>invocation_count
             OR turn_count<>m.retired_apply_through-m.retired_apply_from+1
             OR receipt_count<>m.retired_arrival_through-m.retired_arrival_from+1
             OR (anchor.operation_id,anchor.summary_digest,
                 anchor.retired_apply_through,anchor.retired_arrival_through)
                  IS DISTINCT FROM
                (m.operation_id,m.summary_digest,
                 m.retired_apply_through,m.retired_arrival_through)
             OR NOT EXISTS(SELECT 1 FROM join_retirement_rows_v1 r
                 WHERE r.operation_id=m.operation_id AND r.row_kind='TURN'
                   AND r.row_key=m.retired_apply_through::text
                   AND r.row_json->'after_state'=m.state_json)
             OR EXISTS(SELECT 1 FROM jsonb_each_text(m.input_cuts) cut
                 WHERE NOT EXISTS(SELECT 1 FROM join_inputs_v3 i
                   JOIN durable_refs d ON d.ref_kind='JOIN_INPUT' AND d.ref_id=i.retention_ref
                   WHERE i.revision_id=m.revision_id AND i.participant_id=m.participant_id
                     AND i.channel||'/'||i.partition_id=cut.key
                     AND d.sequence=cut.value::bigint))
             OR (SELECT count(*) FROM jsonb_object_keys(m.input_cuts))<>
                (SELECT count(*) FROM join_inputs_v3 i
                  WHERE i.revision_id=m.revision_id
                    AND i.participant_id=m.participant_id)
             OR EXISTS(SELECT 1 FROM join_turns_v3 t
                  WHERE t.revision_id=m.revision_id
                    AND t.participant_id=m.participant_id
                    AND t.apply_ordinal<=m.retired_apply_through)
             OR EXISTS(SELECT 1 FROM join_receipts_v3 r
                  WHERE r.revision_id=m.revision_id
                    AND r.participant_id=m.participant_id
                    AND r.arrival_ordinal<=m.retired_arrival_through)
             OR EXISTS(SELECT 1 FROM join_invocation_history_v3 h
                  WHERE h.revision_id=m.revision_id
                    AND h.participant_id=m.participant_id
                    AND h.arrival_ordinal<=m.retired_arrival_through)
             OR (m.retired_apply_from=1
                 AND m.prior_summary_digest<>repeat('0',64))
             OR (m.retired_apply_from>1 AND NOT EXISTS(
                  SELECT 1 FROM join_retirement_manifests_v1 p
                   WHERE p.revision_id=m.revision_id
                     AND p.participant_id=m.participant_id
                     AND p.status='COMPLETED'
                     AND p.summary_digest=m.prior_summary_digest
                     AND p.retired_apply_through=m.retired_apply_from-1
                     AND p.retired_arrival_through=m.retired_arrival_from-1))
          THEN RAISE EXCEPTION 'join retirement manifest did not converge'; END IF;
          RETURN NULL;
        END $$;

        DROP TRIGGER IF EXISTS join_retention_anchor_authority ON join_retention_anchors_v1;
        CREATE TRIGGER join_retention_anchor_authority BEFORE INSERT OR UPDATE
          ON join_retention_anchors_v1 FOR EACH ROW
          EXECUTE FUNCTION join_retention_anchor_guard();
        DROP TRIGGER IF EXISTS join_retention_anchor_no_delete ON join_retention_anchors_v1;
        CREATE TRIGGER join_retention_anchor_no_delete BEFORE DELETE
          ON join_retention_anchors_v1 FOR EACH ROW
          EXECUTE FUNCTION join_retention_reject_mutation();
        DROP TRIGGER IF EXISTS join_retirement_manifest_update ON join_retirement_manifests_v1;
        CREATE TRIGGER join_retirement_manifest_update BEFORE UPDATE
          ON join_retirement_manifests_v1 FOR EACH ROW
          EXECUTE FUNCTION join_retirement_manifest_guard();
        DROP TRIGGER IF EXISTS join_retirement_manifest_no_delete ON join_retirement_manifests_v1;
        CREATE TRIGGER join_retirement_manifest_no_delete BEFORE DELETE
          ON join_retirement_manifests_v1 FOR EACH ROW
          EXECUTE FUNCTION join_retention_reject_mutation();
        DROP TRIGGER IF EXISTS join_retirement_manifest_commit
          ON join_retirement_manifests_v1;
        CREATE CONSTRAINT TRIGGER join_retirement_manifest_commit
          AFTER INSERT ON join_retirement_manifests_v1
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
          EXECUTE FUNCTION join_retirement_manifest_commit_guard();
        DROP TRIGGER IF EXISTS join_retirement_rows_immutable ON join_retirement_rows_v1;
        CREATE TRIGGER join_retirement_rows_immutable BEFORE UPDATE OR DELETE
          ON join_retirement_rows_v1 FOR EACH ROW
          EXECUTE FUNCTION join_retention_reject_mutation();
        DROP TRIGGER IF EXISTS join_retirement_row_insert_authority
          ON join_retirement_rows_v1;
        CREATE TRIGGER join_retirement_row_insert_authority BEFORE INSERT
          ON join_retirement_rows_v1 FOR EACH ROW
          EXECUTE FUNCTION join_retirement_row_insert_guard();
        DROP TRIGGER IF EXISTS join_pin_guard ON durable_refs;
        CREATE TRIGGER join_pin_guard BEFORE UPDATE ON durable_refs
          FOR EACH ROW EXECUTE FUNCTION join_pin_guard();
        """
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(ddl)
            for table in (
                "join_turns_v3",
                "join_receipts_v3",
                "join_invocation_history_v3",
            ):
                await conn.execute(
                    f"""DROP TRIGGER IF EXISTS immutable ON {table};
                    CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION join_retention_row_guard()"""
                )

    async def retire_latest(
        self,
        revision_id: str,
        participant_id: str,
        operation_id: str,
        *,
        fault: FaultHook = None,
    ) -> JoinRetentionOutcome:
        if not operation_id:
            raise ValueError("operation_id must be non-empty")
        async with self.pool.acquire() as conn, conn.transaction():
            await self.coordinator._locked_revision(conn, revision_id)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|join-retirement|{revision_id}|{participant_id}",
            )
            existing = await conn.fetchrow(
                "SELECT * FROM join_retirement_manifests_v1 WHERE operation_id=$1",
                operation_id,
            )
            if existing is not None:
                if (existing["revision_id"], existing["participant_id"]) != (
                    revision_id,
                    participant_id,
                ):
                    raise PostgresProtocolError("join retirement operation drift")
                if existing["status"] != "COMPLETED":
                    raise PostgresProtocolError("join retirement operation incomplete")
                return self._outcome(existing, True)
            worker = await conn.fetchrow(
                """SELECT * FROM join_workers WHERE revision_id=$1
                   AND participant_id=$2 FOR UPDATE""",
                revision_id,
                participant_id,
            )
            if worker is None:
                raise PostgresProtocolError("join retirement worker is missing")
            if await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM join_invocations_v3
                   WHERE revision_id=$1 AND participant_id=$2)""",
                revision_id,
                participant_id,
            ):
                raise PostgresProtocolError("join retirement has an active invocation")
            has_regional_authority = await conn.fetchval(
                "SELECT to_regclass('regional_recovery_workers_v1') IS NOT NULL"
            )
            if has_regional_authority and await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1
                   WHERE revision_id=$1 AND status<>'ACTIVE')""",
                revision_id,
            ):
                raise PostgresProtocolError(
                    "active regional recovery pins join mailbox"
                )
            active_global = (
                await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM recovery_plans_v2 p
                       WHERE p.revision_id=$1 AND p.status='RESTORING'
                         AND NOT EXISTS(SELECT 1 FROM region_recovery_plans_v1 r
                           WHERE r.plan_id=p.recovery_id))""",
                    revision_id,
                )
                if has_regional_authority
                else await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM recovery_plans_v2
                       WHERE revision_id=$1 AND status='RESTORING')""",
                    revision_id,
                )
            )
            if active_global:
                raise PostgresProtocolError("active global recovery pins join mailbox")
            if worker["phase"] != "LIVE":
                raise PostgresProtocolError("join retirement requires a LIVE worker")
            checkpoint = await conn.fetchrow(
                """SELECT i.checkpoint_id,i.checkpoint_ordinal,a.mailbox_highwater,
                          a.snapshot_object_key,a.snapshot_payload_digest,o.payload
                   FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                   JOIN participant_checkpoint_acks a USING(checkpoint_id)
                   JOIN checkpoint_snapshot_objects o
                     ON o.object_key=a.snapshot_object_key
                   WHERE i.revision_id=$1 AND a.participant_id=$2
                     AND l.status='COMPLETED'
                   ORDER BY i.checkpoint_ordinal DESC LIMIT 1""",
                revision_id,
                participant_id,
            )
            if checkpoint is None:
                raise PostgresProtocolError(
                    "join retirement requires completed snapshot"
                )
            if (
                hashlib.sha256(bytes(checkpoint["payload"])).hexdigest()
                != checkpoint["snapshot_payload_digest"]
            ):
                raise PostgresProtocolError("join retirement snapshot is corrupt")
            prior = await conn.fetchrow(
                """SELECT * FROM join_retention_anchors_v1
                   WHERE revision_id=$1 AND participant_id=$2 FOR UPDATE""",
                revision_id,
                participant_id,
            )
            prior_apply = 0 if prior is None else prior["retired_apply_through"]
            prior_arrival = 0 if prior is None else prior["retired_arrival_through"]
            prior_summary = "0" * 64 if prior is None else prior["summary_digest"]
            turns = list(
                await conn.fetch(
                    """SELECT * FROM join_turns_v3 WHERE revision_id=$1
                       AND participant_id=$2 AND apply_ordinal>$3
                       AND apply_ordinal<=$4 ORDER BY apply_ordinal""",
                    revision_id,
                    participant_id,
                    prior_apply,
                    checkpoint["mailbox_highwater"],
                )
            )
            retire_apply, retire_arrival = prior_apply, prior_arrival
            arrivals: set[int] = set()
            for turn in turns:
                arrivals.add(turn["arrival_ordinal"])
                count = turn["apply_ordinal"] - prior_apply
                if arrivals == set(range(prior_arrival + 1, prior_arrival + count + 1)):
                    retire_apply = turn["apply_ordinal"]
                    retire_arrival = prior_arrival + count
            if retire_apply == prior_apply:
                raise PostgresProtocolError(
                    "join retirement has no contiguous safe prefix"
                )
            retired_turns = [
                row for row in turns if row["apply_ordinal"] <= retire_apply
            ]
            receipts = list(
                await conn.fetch(
                    """SELECT * FROM join_receipts_v3 WHERE revision_id=$1
                       AND participant_id=$2 AND arrival_ordinal>$3
                       AND arrival_ordinal<=$4 ORDER BY arrival_ordinal""",
                    revision_id,
                    participant_id,
                    prior_arrival,
                    retire_arrival,
                )
            )
            if [row["arrival_ordinal"] for row in receipts] != list(
                range(prior_arrival + 1, retire_arrival + 1)
            ):
                raise PostgresProtocolError("join retirement receipt prefix has a gap")
            invocations = list(
                await conn.fetch(
                    """SELECT * FROM join_invocation_history_v3
                       WHERE revision_id=$1 AND participant_id=$2
                         AND arrival_ordinal>$3 AND arrival_ordinal<=$4
                       ORDER BY token""",
                    revision_id,
                    participant_id,
                    prior_arrival,
                    retire_arrival,
                )
            )
            state = decoded(retired_turns[-1]["after_state"])
            state_digest = digest(state)
            inputs = list(
                await conn.fetch(
                    """SELECT * FROM join_inputs_v3 WHERE revision_id=$1
                       AND participant_id=$2 ORDER BY channel,partition_id FOR UPDATE""",
                    revision_id,
                    participant_id,
                )
            )
            barrier_vector = decoded(
                (
                    await conn.fetchrow(
                        """SELECT barrier_vector FROM participant_checkpoint_acks
                           WHERE checkpoint_id=$1 AND participant_id=$2""",
                        checkpoint["checkpoint_id"],
                        participant_id,
                    )
                )["barrier_vector"]
            )
            input_cuts: dict[str, int] = {}
            for item in inputs:
                location = item["channel"] + "/" + item["partition_id"]
                remaining = await conn.fetchval(
                    """SELECT min(data_sequence) FROM join_receipts_v3
                       WHERE revision_id=$1 AND participant_id=$2
                         AND channel=$3 AND partition_id=$4
                         AND arrival_ordinal>$5""",
                    revision_id,
                    participant_id,
                    item["channel"],
                    item["partition_id"],
                    retire_arrival,
                )
                cut = barrier_vector[location]["cut_data_sequence"] + 1
                input_cuts[location] = cut if remaining is None else min(cut, remaining)
                if input_cuts[location] < item["completed"] + 1 and remaining is None:
                    raise PostgresProtocolError("join retention input cut regressed")
            counts = {
                "turns": len(retired_turns),
                "receipts": len(receipts),
                "invocations": len(invocations),
            }
            summary_identity = {
                "prior_summary_digest": prior_summary,
                "revision_id": revision_id,
                "participant_id": participant_id,
                "checkpoint_id": checkpoint["checkpoint_id"],
                "snapshot_object_key": checkpoint["snapshot_object_key"],
                "snapshot_payload_digest": checkpoint["snapshot_payload_digest"],
                "mailbox_highwater": checkpoint["mailbox_highwater"],
                "retired_apply_through": retire_apply,
                "retired_arrival_through": retire_arrival,
                "state": state,
                "state_digest": state_digest,
                "input_cuts": input_cuts,
                "row_counts": counts,
            }
            summary_digest = digest(summary_identity)
            manifest_identity = {
                "operation_id": operation_id,
                **summary_identity,
                "retired_apply_from": prior_apply + 1,
                "retired_arrival_from": prior_arrival + 1,
                "summary_digest": summary_digest,
            }
            manifest_digest = digest(manifest_identity)
            await conn.execute(
                """INSERT INTO join_retirement_manifests_v1 VALUES(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15::jsonb,
                   $16::jsonb,$17,$18,'PLANNED',clock_timestamp(),NULL)""",
                operation_id,
                revision_id,
                participant_id,
                prior_summary,
                checkpoint["checkpoint_id"],
                checkpoint["snapshot_object_key"],
                checkpoint["snapshot_payload_digest"],
                checkpoint["mailbox_highwater"],
                prior_apply + 1,
                retire_apply,
                prior_arrival + 1,
                retire_arrival,
                canonical(state),
                state_digest,
                canonical(input_cuts),
                canonical(counts),
                summary_digest,
                manifest_digest,
            )
            rows = (
                [("TURN", str(row["apply_ordinal"]), row) for row in retired_turns]
                + [("RECEIPT", str(row["arrival_ordinal"]), row) for row in receipts]
                + [("INVOCATION", row["token"], row) for row in invocations]
            )
            for kind, key, row in rows:
                material = dict(row)
                if kind == "TURN":
                    material["before_state"] = decoded(material["before_state"])
                    material["after_state"] = decoded(material["after_state"])
                await conn.execute(
                    """INSERT INTO join_retirement_rows_v1 VALUES(
                       $1,$2,$3,$4::jsonb,$5)""",
                    operation_id,
                    kind,
                    key,
                    json.dumps(
                        material, sort_keys=True, separators=(",", ":"), default=str
                    ),
                    digest(json.loads(json.dumps(material, default=str))),
                )
            _hit(fault, "join_retirement_manifest_planned")
            await conn.execute(
                "SELECT set_config('ariadne.retirement_operation',$1,true)",
                operation_id,
            )
            await conn.execute(
                """DELETE FROM join_turns_v3 WHERE revision_id=$1
                   AND participant_id=$2 AND apply_ordinal<=$3""",
                revision_id,
                participant_id,
                retire_apply,
            )
            await conn.execute(
                """DELETE FROM join_invocation_history_v3 WHERE revision_id=$1
                   AND participant_id=$2 AND arrival_ordinal<=$3""",
                revision_id,
                participant_id,
                retire_arrival,
            )
            await conn.execute(
                """DELETE FROM join_receipts_v3 WHERE revision_id=$1
                   AND participant_id=$2 AND arrival_ordinal<=$3""",
                revision_id,
                participant_id,
                retire_arrival,
            )
            _hit(fault, "join_retirement_rows_deleted")
            for item in inputs:
                location = item["channel"] + "/" + item["partition_id"]
                await conn.execute(
                    """UPDATE durable_refs SET sequence=$2
                       WHERE ref_kind='JOIN_INPUT' AND ref_id=$1 AND sequence<$2""",
                    item["retention_ref"],
                    input_cuts[location],
                )
            prior_totals = (
                (0, 0, 0)
                if prior is None
                else (
                    prior["retired_turns"],
                    prior["retired_receipts"],
                    prior["retired_invocations"],
                )
            )
            await conn.execute(
                """INSERT INTO join_retention_anchors_v1 VALUES(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11::jsonb,$12,$13,$14,$15,$16,
                   clock_timestamp())
                   ON CONFLICT(revision_id,participant_id) DO UPDATE SET
                     retired_apply_through=EXCLUDED.retired_apply_through,
                     retired_arrival_through=EXCLUDED.retired_arrival_through,
                     retained_checkpoint_id=EXCLUDED.retained_checkpoint_id,
                     snapshot_object_key=EXCLUDED.snapshot_object_key,
                     snapshot_payload_digest=EXCLUDED.snapshot_payload_digest,
                     mailbox_highwater=EXCLUDED.mailbox_highwater,
                     state_json=EXCLUDED.state_json,state_digest=EXCLUDED.state_digest,
                     input_cuts=EXCLUDED.input_cuts,summary_digest=EXCLUDED.summary_digest,
                     operation_id=EXCLUDED.operation_id,
                     retired_turns=EXCLUDED.retired_turns,
                     retired_receipts=EXCLUDED.retired_receipts,
                     retired_invocations=EXCLUDED.retired_invocations""",
                revision_id,
                participant_id,
                retire_apply,
                retire_arrival,
                checkpoint["checkpoint_id"],
                checkpoint["snapshot_object_key"],
                checkpoint["snapshot_payload_digest"],
                checkpoint["mailbox_highwater"],
                canonical(state),
                state_digest,
                canonical(input_cuts),
                summary_digest,
                operation_id,
                prior_totals[0] + counts["turns"],
                prior_totals[1] + counts["receipts"],
                prior_totals[2] + counts["invocations"],
            )
            _hit(fault, "join_retirement_anchor_advanced")
            await conn.execute(
                """UPDATE join_retirement_manifests_v1 SET status='COMPLETED'
                   WHERE operation_id=$1""",
                operation_id,
            )
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        _hit(fault, "join_retirement_committed")
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM join_retirement_manifests_v1 WHERE operation_id=$1",
                operation_id,
            )
        return self._outcome(row, False)

    async def assert_checkpoint_replayable(
        self, revision_id: str, participant_id: str, checkpoint_id: str
    ) -> None:
        async with self.pool.acquire() as conn:
            anchor = await conn.fetchrow(
                """SELECT * FROM join_retention_anchors_v1
                   WHERE revision_id=$1 AND participant_id=$2""",
                revision_id,
                participant_id,
            )
            ack = await conn.fetchrow(
                """SELECT mailbox_highwater FROM participant_checkpoint_acks
                   WHERE checkpoint_id=$1 AND participant_id=$2""",
                checkpoint_id,
                participant_id,
            )
        if ack is None:
            raise PostgresProtocolError("checkpoint participant snapshot is missing")
        if (
            anchor is not None
            and ack["mailbox_highwater"] < anchor["retired_apply_through"]
        ):
            raise JoinReplayHorizonExpired(
                f"checkpoint {checkpoint_id} mailbox {ack['mailbox_highwater']} "
                f"retired through {anchor['retired_apply_through']}"
            )

    async def pressure(self, revision_id: str, participant_id: str) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            counts = {}
            for name, table in (
                ("receipts", "join_receipts_v3"),
                ("turns", "join_turns_v3"),
                ("invocations", "join_invocation_history_v3"),
            ):
                counts[name] = await conn.fetchval(
                    f"SELECT count(*) FROM {table} WHERE revision_id=$1 AND participant_id=$2",
                    revision_id,
                    participant_id,
                )
            anchor = await conn.fetchrow(
                """SELECT * FROM join_retention_anchors_v1
                   WHERE revision_id=$1 AND participant_id=$2""",
                revision_id,
                participant_id,
            )
        return {"hot_rows": counts, "anchor": None if anchor is None else dict(anchor)}

    @staticmethod
    def _outcome(row: asyncpg.Record, retry: bool) -> JoinRetentionOutcome:
        counts = decoded(row["row_counts"])
        return JoinRetentionOutcome(
            row["operation_id"],
            row["revision_id"],
            row["participant_id"],
            row["checkpoint_id"],
            row["retired_apply_through"],
            row["retired_arrival_through"],
            counts["turns"],
            counts["receipts"],
            counts["invocations"],
            dict(decoded(row["input_cuts"])),
            row["summary_digest"],
            retry,
        )
