"""PostgreSQL coordinator authority for ordered DATA/BARRIER checkpoints."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import asyncpg
from postgres_stream_v2 import (
    PostgresProtocolError,
    PostgresStream,
    StaleOwner,
    content_object_key,
)

FaultHook = Callable[[str], None] | None


class CheckpointControlPressure(PostgresProtocolError):
    """A frozen multi-transaction retention operation must finish first."""


def hit(fault: FaultHook, point: str) -> None:
    if fault is not None:
        fault(point)


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def location_key(channel: str, partition: str) -> str:
    return f"{channel}/{partition}"


def canonical_locations(
    locations: list[tuple[str, str]],
) -> list[dict[str, str]]:
    unique = sorted(set(locations))
    if not unique or len(unique) != len(locations):
        raise ValueError("locations must be non-empty and unique")
    return [
        {"channel": channel, "partition": partition} for channel, partition in unique
    ]


def canonical_participants(
    participants: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized = []
    seen: set[str] = set()
    for participant in participants:
        participant_id = participant["participant_id"]
        if participant_id in seen:
            raise ValueError("participant IDs must be unique")
        seen.add(participant_id)
        inputs = canonical_locations(
            [(item["channel"], item["partition"]) for item in participant["inputs"]]
        )
        outputs_raw = participant.get("outputs", [])
        outputs = (
            []
            if not outputs_raw
            else canonical_locations(
                [(item["channel"], item["partition"]) for item in outputs_raw]
            )
        )
        normalized.append(
            {
                "participant_id": participant_id,
                "inputs": inputs,
                "outputs": outputs,
                "code_digest": participant["code_digest"],
                "state_schema": participant["state_schema"],
                "state_version": participant["state_version"],
            }
        )
    if not normalized:
        raise ValueError("participants must be non-empty")
    return sorted(normalized, key=lambda item: item["participant_id"])


@dataclass(frozen=True)
class Revision:
    revision_id: str
    deployment_id: str
    graph_version: str
    graph_digest: str
    source_manifest: list[dict[str, str]]
    participant_manifest: list[dict[str, Any]]


@dataclass(frozen=True)
class Checkpoint:
    checkpoint_id: str
    checkpoint_ordinal: int
    intent_digest: str
    status: str


@dataclass(frozen=True)
class BarrierPosition:
    checkpoint_id: str
    channel: str
    partition: str
    log_sequence: int
    cut_data_sequence: int


@dataclass(frozen=True)
class RecoveryPlan:
    recovery_id: str
    checkpoint_id: str
    revision_id: str
    target_data_sequences: dict[str, int]
    participant_snapshots: dict[str, str]
    plan_digest: str
    status: str


class PostgresBarrierCoordinator:
    """Separate coordinator authority in the stream's durability domain."""

    def __init__(self, stream: PostgresStream):
        self.stream = stream
        self.pool = stream.pool
        self.schema = stream.schema

    @asynccontextmanager
    async def _participant_connection(self, connection):
        if connection is not None:
            yield connection
        else:
            async with self.pool.acquire() as conn:
                yield conn

    @classmethod
    async def install(
        cls,
        stream: PostgresStream,
    ) -> PostgresBarrierCoordinator:
        coordinator = cls(stream)
        await coordinator._initialize()
        return coordinator

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS partition_log_heads (
            channel text NOT NULL,
            partition_id text NOT NULL,
            next_log_sequence bigint NOT NULL DEFAULT 1
                CHECK(next_log_sequence > 0),
            max_log_entries bigint NOT NULL DEFAULT 1000000
                CHECK(max_log_entries > 0),
            used_log_entries bigint NOT NULL DEFAULT 0
                CHECK(used_log_entries >= 0 AND used_log_entries <= max_log_entries),
            PRIMARY KEY(channel,partition_id),
            FOREIGN KEY(channel,partition_id)
                REFERENCES partitions(channel,partition_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS partition_log_entries (
            channel text NOT NULL,
            partition_id text NOT NULL,
            log_sequence bigint NOT NULL CHECK(log_sequence > 0),
            entry_kind text NOT NULL CHECK(entry_kind IN('DATA','BARRIER')),
            barrier_role text CHECK(barrier_role IN('SOURCE','OUTPUT')),
            data_sequence bigint,
            record_id text,
            checkpoint_id text,
            cut_data_sequence bigint,
            intent_digest text,
            writer_epoch bigint,
            revision_id text,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(channel,partition_id,log_sequence),
            UNIQUE(channel,partition_id,data_sequence),
            CHECK(
                (entry_kind='DATA' AND barrier_role IS NULL
                 AND data_sequence IS NOT NULL AND record_id IS NOT NULL
                 AND checkpoint_id IS NULL AND cut_data_sequence IS NULL
                 AND intent_digest IS NULL AND writer_epoch IS NULL
                 AND revision_id IS NULL)
                OR
                (entry_kind='BARRIER' AND barrier_role IS NOT NULL
                 AND data_sequence IS NULL AND record_id IS NULL
                 AND checkpoint_id IS NOT NULL AND cut_data_sequence IS NOT NULL
                 AND intent_digest IS NOT NULL AND writer_epoch IS NOT NULL
                 AND revision_id IS NOT NULL)
            ),
            FOREIGN KEY(channel,partition_id)
                REFERENCES partitions(channel,partition_id) ON DELETE CASCADE
        );
        CREATE UNIQUE INDEX IF NOT EXISTS one_barrier_role_per_checkpoint_location
            ON partition_log_entries(
                checkpoint_id,channel,partition_id,barrier_role
            ) WHERE entry_kind='BARRIER';

        CREATE TABLE IF NOT EXISTS deployment_revisions (
            revision_id text PRIMARY KEY,
            deployment_id text NOT NULL,
            graph_version text NOT NULL,
            graph_digest text NOT NULL UNIQUE,
            code_manifest_digest text NOT NULL,
            state_contract_digest text NOT NULL,
            source_manifest jsonb NOT NULL,
            participant_manifest jsonb NOT NULL,
            next_checkpoint_ordinal bigint NOT NULL DEFAULT 1,
            next_authority_ordinal bigint NOT NULL DEFAULT 1,
            max_inflight_checkpoints integer NOT NULL
                CHECK(max_inflight_checkpoints > 0),
            max_retained_checkpoints integer NOT NULL
                CHECK(max_retained_checkpoints > 0),
            max_checkpoint_metadata_bytes bigint NOT NULL
                CHECK(max_checkpoint_metadata_bytes > 0),
            max_checkpoint_age_seconds double precision NOT NULL
                CHECK(max_checkpoint_age_seconds > 0),
            max_authority_events bigint NOT NULL
                CHECK(max_authority_events > 0),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            UNIQUE(deployment_id,graph_version)
        );
        CREATE TABLE IF NOT EXISTS revision_source_partitions (
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            channel text NOT NULL,
            partition_id text NOT NULL,
            PRIMARY KEY(revision_id,channel,partition_id),
            UNIQUE(channel,partition_id),
            FOREIGN KEY(channel,partition_id)
                REFERENCES partitions(channel,partition_id)
        );
        CREATE TABLE IF NOT EXISTS revision_participants (
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            participant_id text NOT NULL,
            input_manifest jsonb NOT NULL,
            output_manifest jsonb NOT NULL,
            code_digest text NOT NULL,
            state_schema text NOT NULL,
            state_version integer NOT NULL CHECK(state_version > 0),
            PRIMARY KEY(revision_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS revision_edges (
            revision_id text NOT NULL REFERENCES deployment_revisions,
            channel text NOT NULL, partition_id text NOT NULL,
            producer_id text NOT NULL, target_id text NOT NULL,
            PRIMARY KEY(revision_id,channel,partition_id,target_id)
        );
        CREATE TABLE IF NOT EXISTS coordinator_leases (
            revision_id text PRIMARY KEY REFERENCES deployment_revisions(revision_id),
            owner_id text NOT NULL,
            owner_epoch bigint NOT NULL CHECK(owner_epoch > 0),
            lease_expires_at timestamptz NOT NULL
        );
        CREATE TABLE IF NOT EXISTS coordinator_authority_events (
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            authority_ordinal bigint NOT NULL CHECK(authority_ordinal > 0),
            event_type text NOT NULL CHECK(event_type IN(
                'CLAIM','INTENT','SOURCE_BARRIERS','PARTICIPANT_ACK',
                'COMPLETE','ABORT','RECOVERY_FENCE','RECOVERY_READY','RETIRE'
            )),
            checkpoint_id text,
            recovery_id text,
            writer_epoch bigint NOT NULL CHECK(writer_epoch > 0),
            payload jsonb NOT NULL,
            event_digest text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(revision_id,authority_ordinal)
        );
        CREATE TABLE IF NOT EXISTS checkpoint_intents_v2 (
            checkpoint_id text PRIMARY KEY,
            request_id text NOT NULL UNIQUE,
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            checkpoint_ordinal bigint NOT NULL CHECK(checkpoint_ordinal > 0),
            graph_digest text NOT NULL,
            source_manifest jsonb NOT NULL,
            participant_manifest jsonb NOT NULL,
            intent_digest text NOT NULL UNIQUE,
            created_by_epoch bigint NOT NULL,
            intent_authority_ordinal bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            UNIQUE(revision_id,checkpoint_ordinal),
            FOREIGN KEY(revision_id,intent_authority_ordinal)
                REFERENCES coordinator_authority_events(
                    revision_id,authority_ordinal
                )
        );
        CREATE TABLE IF NOT EXISTS checkpoint_lifecycle_v2 (
            checkpoint_id text PRIMARY KEY REFERENCES checkpoint_intents_v2(checkpoint_id),
            status text NOT NULL CHECK(status IN(
                'STARTED','ALIGNING','ALIGNED','COMPLETED','ABORTED','RETIRED'
            )),
            last_authority_ordinal bigint NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS source_barrier_observations (
            checkpoint_id text NOT NULL REFERENCES checkpoint_intents_v2(checkpoint_id),
            channel text NOT NULL,
            partition_id text NOT NULL,
            barrier_log_sequence bigint NOT NULL,
            cut_data_sequence bigint NOT NULL CHECK(cut_data_sequence >= 0),
            intent_digest text NOT NULL,
            writer_epoch bigint NOT NULL,
            barrier_authority_ordinal bigint NOT NULL,
            retention_ref_id text NOT NULL UNIQUE,
            PRIMARY KEY(checkpoint_id,channel,partition_id),
            FOREIGN KEY(channel,partition_id,barrier_log_sequence)
                REFERENCES partition_log_entries(
                    channel,partition_id,log_sequence
                )
        );
        CREATE TABLE IF NOT EXISTS participant_checkpoint_acks (
            checkpoint_id text NOT NULL REFERENCES checkpoint_intents_v2(checkpoint_id),
            participant_id text NOT NULL,
            barrier_vector jsonb NOT NULL,
            snapshot_object_key text NOT NULL,
            snapshot_payload_digest text NOT NULL,
            mailbox_highwater bigint NOT NULL CHECK(mailbox_highwater >= 0),
            replay_tail_floor jsonb NOT NULL,
            output_barriers jsonb NOT NULL,
            state_schema text NOT NULL,
            state_version integer NOT NULL CHECK(state_version > 0),
            code_digest text NOT NULL,
            ack_digest text NOT NULL UNIQUE,
            writer_epoch bigint NOT NULL,
            ack_authority_ordinal bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(checkpoint_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS checkpoint_snapshot_objects (
            object_key text PRIMARY KEY,
            payload bytea NOT NULL,
            payload_digest text NOT NULL,
            payload_size bigint NOT NULL CHECK(payload_size >= 0),
            schema_id text NOT NULL,
            published_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        ALTER TABLE participant_checkpoint_acks
            DROP CONSTRAINT IF EXISTS participant_checkpoint_acks_snapshot_object_key_fkey;
        DO $$ BEGIN
            IF NOT EXISTS(
                SELECT 1 FROM pg_constraint
                WHERE conname='participant_ack_snapshot_object_fk'
                  AND conrelid='participant_checkpoint_acks'::regclass
            ) THEN
                ALTER TABLE participant_checkpoint_acks
                    ADD CONSTRAINT participant_ack_snapshot_object_fk
                    FOREIGN KEY(snapshot_object_key)
                    REFERENCES checkpoint_snapshot_objects(object_key);
            END IF;
        END $$;
        CREATE TABLE IF NOT EXISTS participant_scan_positions (
            revision_id text NOT NULL,
            participant_id text NOT NULL,
            channel text NOT NULL,
            partition_id text NOT NULL,
            scanned_log_through bigint NOT NULL DEFAULT 0,
            last_checkpoint_id text,
            PRIMARY KEY(revision_id,participant_id,channel,partition_id)
        );
        CREATE TABLE IF NOT EXISTS checkpoint_completions_v2 (
            checkpoint_id text PRIMARY KEY REFERENCES checkpoint_intents_v2(checkpoint_id),
            completion_digest text NOT NULL UNIQUE,
            source_barriers_digest text NOT NULL,
            participant_acks_digest text NOT NULL,
            completion_authority_ordinal bigint NOT NULL,
            writer_epoch bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS checkpoint_aborts_v2 (
            checkpoint_id text PRIMARY KEY REFERENCES checkpoint_intents_v2(checkpoint_id),
            reason text NOT NULL,
            abort_authority_ordinal bigint NOT NULL,
            writer_epoch bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS recovery_plans_v2 (
            recovery_id text PRIMARY KEY,
            request_id text NOT NULL UNIQUE,
            revision_id text NOT NULL REFERENCES deployment_revisions(revision_id),
            checkpoint_id text NOT NULL REFERENCES checkpoint_completions_v2(checkpoint_id),
            fence_authority_ordinal bigint NOT NULL,
            target_data_sequences jsonb NOT NULL,
            participant_snapshots jsonb NOT NULL,
            plan_digest text NOT NULL UNIQUE,
            status text NOT NULL CHECK(status IN('RESTORING','READY')),
            writer_epoch bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            ready_at timestamptz,
            ready_authority_ordinal bigint,
            ready_writer_epoch bigint
        );
        CREATE TABLE IF NOT EXISTS participant_output_roots (
            checkpoint_id text NOT NULL REFERENCES checkpoint_intents_v2,
            participant_id text NOT NULL, channel text NOT NULL, partition_id text NOT NULL,
            retention_ref_id text NOT NULL UNIQUE REFERENCES checkpoints,
            PRIMARY KEY(checkpoint_id,participant_id,channel,partition_id)
        );
        CREATE TABLE IF NOT EXISTS retired_checkpoint_summaries (
            checkpoint_id text PRIMARY KEY,
            revision_id text NOT NULL,
            checkpoint_ordinal bigint NOT NULL,
            completion_digest text NOT NULL,
            summary_digest text NOT NULL UNIQUE,
            retired_authority_ordinal bigint NOT NULL,
            retired_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        """
        functions_and_triggers = """
        CREATE OR REPLACE FUNCTION coordinator_reject_immutable_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'coordinator authority row is immutable';
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_guard_revision_identity()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.revision_id,NEW.deployment_id,NEW.graph_version,
                NEW.graph_digest,NEW.code_manifest_digest,
                NEW.state_contract_digest,NEW.source_manifest,
                NEW.participant_manifest,NEW.max_inflight_checkpoints,
                NEW.max_retained_checkpoints,NEW.max_checkpoint_metadata_bytes,
                NEW.max_checkpoint_age_seconds,NEW.max_authority_events,
                NEW.created_at)
               IS DISTINCT FROM
               (OLD.revision_id,OLD.deployment_id,OLD.graph_version,
                OLD.graph_digest,OLD.code_manifest_digest,
                OLD.state_contract_digest,OLD.source_manifest,
                OLD.participant_manifest,OLD.max_inflight_checkpoints,
                OLD.max_retained_checkpoints,OLD.max_checkpoint_metadata_bytes,
                OLD.max_checkpoint_age_seconds,OLD.max_authority_events,
                OLD.created_at)
            THEN RAISE EXCEPTION 'deployment revision is immutable'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_guard_lifecycle()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.checkpoint_id IS DISTINCT FROM OLD.checkpoint_id THEN
                RAISE EXCEPTION 'checkpoint identity is immutable';
            END IF;
            IF NOT (
                (OLD.status='STARTED' AND NEW.status IN('ALIGNING','ALIGNED','ABORTED'))
                OR (OLD.status='ALIGNING' AND NEW.status IN('ALIGNED','ABORTED'))
                OR (OLD.status='ALIGNED' AND NEW.status IN('COMPLETED','ABORTED'))
                OR (OLD.status='COMPLETED' AND NEW.status='RETIRED')
            ) THEN RAISE EXCEPTION 'illegal checkpoint lifecycle transition'; END IF;
            IF NEW.last_authority_ordinal <= OLD.last_authority_ordinal THEN
                RAISE EXCEPTION 'checkpoint authority ordinal must advance';
            END IF;
            NEW.updated_at=clock_timestamp();
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_guard_recovery_plan()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.recovery_id,NEW.request_id,NEW.revision_id,NEW.checkpoint_id,
                NEW.fence_authority_ordinal,NEW.target_data_sequences,
                NEW.participant_snapshots,NEW.plan_digest,NEW.created_at)
               IS DISTINCT FROM
               (OLD.recovery_id,OLD.request_id,OLD.revision_id,OLD.checkpoint_id,
                OLD.fence_authority_ordinal,OLD.target_data_sequences,
                OLD.participant_snapshots,OLD.plan_digest,OLD.created_at)
            THEN RAISE EXCEPTION 'recovery plan identity is immutable'; END IF;
            IF NOT (OLD.status='RESTORING' AND NEW.status='READY'
                    AND OLD.ready_at IS NULL AND NEW.ready_at IS NOT NULL
                    AND OLD.ready_authority_ordinal IS NULL
                    AND NEW.ready_authority_ordinal IS NOT NULL
                    AND OLD.ready_writer_epoch IS NULL
                    AND NEW.ready_writer_epoch IS NOT NULL)
            THEN RAISE EXCEPTION 'illegal recovery plan transition'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_validate_authority_link()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            ev coordinator_authority_events%ROWTYPE;
            rev text;
        BEGIN
            IF TG_TABLE_NAME='checkpoint_intents_v2' THEN
                SELECT * INTO ev FROM coordinator_authority_events
                 WHERE revision_id=NEW.revision_id
                   AND authority_ordinal=NEW.intent_authority_ordinal;
                IF ev.event_type IS DISTINCT FROM 'INTENT'
                   OR ev.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
                   OR ev.writer_epoch IS DISTINCT FROM NEW.created_by_epoch
                   OR ev.payload->>'intent_digest' IS DISTINCT FROM NEW.intent_digest
                THEN RAISE EXCEPTION 'invalid intent authority link'; END IF;
            ELSIF TG_TABLE_NAME='source_barrier_observations' THEN
                SELECT i.revision_id INTO rev FROM checkpoint_intents_v2 i
                 WHERE i.checkpoint_id=NEW.checkpoint_id;
                SELECT * INTO ev FROM coordinator_authority_events
                 WHERE revision_id=rev
                   AND authority_ordinal=NEW.barrier_authority_ordinal;
                IF ev.event_type IS DISTINCT FROM 'SOURCE_BARRIERS'
                   OR ev.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
                   OR ev.writer_epoch IS DISTINCT FROM NEW.writer_epoch
                   OR ev.payload->>'channel' IS DISTINCT FROM NEW.channel
                   OR ev.payload->>'partition' IS DISTINCT FROM NEW.partition_id
                   OR (ev.payload->>'log_sequence')::bigint
                        IS DISTINCT FROM NEW.barrier_log_sequence
                   OR (ev.payload->>'cut_data_sequence')::bigint
                        IS DISTINCT FROM NEW.cut_data_sequence
                THEN RAISE EXCEPTION 'invalid barrier authority link'; END IF;
            ELSIF TG_TABLE_NAME='participant_checkpoint_acks' THEN
                SELECT i.revision_id INTO rev FROM checkpoint_intents_v2 i
                 WHERE i.checkpoint_id=NEW.checkpoint_id;
                SELECT * INTO ev FROM coordinator_authority_events
                 WHERE revision_id=rev
                   AND authority_ordinal=NEW.ack_authority_ordinal;
                IF ev.event_type IS DISTINCT FROM 'PARTICIPANT_ACK'
                   OR ev.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
                   OR ev.writer_epoch IS DISTINCT FROM NEW.writer_epoch
                   OR ev.payload->>'participant_id'
                        IS DISTINCT FROM NEW.participant_id
                   OR ev.payload->>'ack_digest' IS DISTINCT FROM NEW.ack_digest
                THEN RAISE EXCEPTION 'invalid participant authority link'; END IF;
            ELSIF TG_TABLE_NAME='checkpoint_completions_v2' THEN
                SELECT i.revision_id INTO rev FROM checkpoint_intents_v2 i
                 WHERE i.checkpoint_id=NEW.checkpoint_id;
                SELECT * INTO ev FROM coordinator_authority_events
                 WHERE revision_id=rev
                   AND authority_ordinal=NEW.completion_authority_ordinal;
                IF ev.event_type IS DISTINCT FROM 'COMPLETE'
                   OR ev.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
                   OR ev.writer_epoch IS DISTINCT FROM NEW.writer_epoch
                   OR ev.payload->>'completion_digest'
                        IS DISTINCT FROM NEW.completion_digest
                THEN RAISE EXCEPTION 'invalid completion authority link'; END IF;
            ELSIF TG_TABLE_NAME='checkpoint_aborts_v2' THEN
                SELECT i.revision_id INTO rev FROM checkpoint_intents_v2 i
                 WHERE i.checkpoint_id=NEW.checkpoint_id;
                SELECT * INTO ev FROM coordinator_authority_events
                 WHERE revision_id=rev
                   AND authority_ordinal=NEW.abort_authority_ordinal;
                IF ev.event_type IS DISTINCT FROM 'ABORT'
                   OR ev.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id
                   OR ev.writer_epoch IS DISTINCT FROM NEW.writer_epoch
                   OR ev.payload->>'reason' IS DISTINCT FROM NEW.reason
                THEN RAISE EXCEPTION 'invalid abort authority link'; END IF;
            ELSIF TG_TABLE_NAME='recovery_plans_v2' THEN
                IF TG_OP='INSERT' THEN
                    SELECT * INTO ev FROM coordinator_authority_events
                     WHERE revision_id=NEW.revision_id
                       AND authority_ordinal=NEW.fence_authority_ordinal;
                    IF ev.event_type IS DISTINCT FROM 'RECOVERY_FENCE'
                       OR ev.recovery_id IS DISTINCT FROM NEW.recovery_id
                       OR ev.writer_epoch IS DISTINCT FROM NEW.writer_epoch
                       OR ev.payload->>'request_id' IS DISTINCT FROM NEW.request_id
                    THEN RAISE EXCEPTION 'invalid recovery fence authority link'; END IF;
                ELSIF NEW.status='READY' THEN
                    SELECT * INTO ev FROM coordinator_authority_events
                     WHERE revision_id=NEW.revision_id
                       AND authority_ordinal=NEW.ready_authority_ordinal;
                    IF ev.event_type IS DISTINCT FROM 'RECOVERY_READY'
                       OR ev.recovery_id IS DISTINCT FROM NEW.recovery_id
                       OR ev.writer_epoch IS DISTINCT FROM NEW.ready_writer_epoch
                       OR ev.payload->>'plan_digest' IS DISTINCT FROM NEW.plan_digest
                    THEN RAISE EXCEPTION 'invalid recovery ready authority link'; END IF;
                END IF;
            END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_capture_data_log()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE allocated bigint;
        BEGIN
            INSERT INTO partition_log_heads(
                channel,partition_id,next_log_sequence,max_log_entries)
                SELECT NEW.channel,NEW.partition_id,1,p.max_metadata_records
                FROM partitions p
                WHERE p.channel=NEW.channel AND p.partition_id=NEW.partition_id
                ON CONFLICT(channel,partition_id) DO NOTHING;
            UPDATE partition_log_heads
                SET next_log_sequence=next_log_sequence+1,
                    used_log_entries=used_log_entries+1
                WHERE channel=NEW.channel AND partition_id=NEW.partition_id
                  AND used_log_entries < max_log_entries
                RETURNING next_log_sequence-1 INTO allocated;
            IF allocated IS NULL THEN
                RAISE EXCEPTION 'ordered-log metadata budget exhausted';
            END IF;
            INSERT INTO partition_log_entries(
                channel,partition_id,log_sequence,entry_kind,
                data_sequence,record_id)
                VALUES(NEW.channel,NEW.partition_id,allocated,'DATA',
                       NEW.sequence,NEW.record_id);
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_guard_retention_release()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.status='SEALED' AND NEW.status='RELEASED' AND EXISTS(
                SELECT 1 FROM (
                  SELECT checkpoint_id,retention_ref_id FROM source_barrier_observations
                  UNION ALL SELECT checkpoint_id,retention_ref_id FROM participant_output_roots
                ) b JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                WHERE b.retention_ref_id=OLD.checkpoint_id
                  AND l.status NOT IN('ABORTED','RETIRED')
            ) THEN
                RAISE EXCEPTION 'coordinator replay-tail pin is still authoritative';
            END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION coordinator_guard_log_head()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.channel,NEW.partition_id) IS DISTINCT FROM
               (OLD.channel,OLD.partition_id)
            THEN RAISE EXCEPTION 'ordered-log partition identity is immutable'; END IF;
            IF NEW.next_log_sequence < OLD.next_log_sequence
               OR NEW.used_log_entries < OLD.used_log_entries
               OR NEW.next_log_sequence-OLD.next_log_sequence
                    <> NEW.used_log_entries-OLD.used_log_entries
            THEN RAISE EXCEPTION 'ordered-log counters must advance together'; END IF;
            IF OLD.used_log_entries > 0
               AND NEW.max_log_entries IS DISTINCT FROM OLD.max_log_entries
            THEN RAISE EXCEPTION 'active ordered-log capacity is immutable'; END IF;
            RETURN NEW;
        END $$;

        DROP TRIGGER IF EXISTS partition_log_immutable ON partition_log_entries;
        CREATE TRIGGER partition_log_immutable BEFORE UPDATE OR DELETE
            ON partition_log_entries FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS coordinator_event_immutable ON coordinator_authority_events;
        CREATE TRIGGER coordinator_event_immutable BEFORE UPDATE OR DELETE
            ON coordinator_authority_events FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS checkpoint_intent_v2_immutable ON checkpoint_intents_v2;
        CREATE TRIGGER checkpoint_intent_v2_immutable BEFORE UPDATE OR DELETE
            ON checkpoint_intents_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS source_barrier_immutable ON source_barrier_observations;
        CREATE TRIGGER source_barrier_immutable BEFORE UPDATE OR DELETE
            ON source_barrier_observations FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS participant_ack_immutable ON participant_checkpoint_acks;
        CREATE TRIGGER participant_ack_immutable BEFORE UPDATE OR DELETE
            ON participant_checkpoint_acks FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS checkpoint_snapshot_object_immutable
            ON checkpoint_snapshot_objects;
        CREATE TRIGGER checkpoint_snapshot_object_immutable BEFORE UPDATE OR DELETE
            ON checkpoint_snapshot_objects FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS completion_v2_immutable ON checkpoint_completions_v2;
        CREATE TRIGGER completion_v2_immutable BEFORE UPDATE OR DELETE
            ON checkpoint_completions_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS abort_v2_immutable ON checkpoint_aborts_v2;
        CREATE TRIGGER abort_v2_immutable BEFORE UPDATE OR DELETE
            ON checkpoint_aborts_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS recovery_plan_delete ON recovery_plans_v2;
        CREATE TRIGGER recovery_plan_delete BEFORE DELETE
            ON recovery_plans_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS recovery_plan_update ON recovery_plans_v2;
        CREATE TRIGGER recovery_plan_update BEFORE UPDATE
            ON recovery_plans_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_guard_recovery_plan();
        DROP TRIGGER IF EXISTS intent_authority_link ON checkpoint_intents_v2;
        CREATE TRIGGER intent_authority_link BEFORE INSERT OR UPDATE
            ON checkpoint_intents_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_validate_authority_link();
        DROP TRIGGER IF EXISTS barrier_authority_link ON source_barrier_observations;
        CREATE TRIGGER barrier_authority_link BEFORE INSERT OR UPDATE
            ON source_barrier_observations FOR EACH ROW
            EXECUTE FUNCTION coordinator_validate_authority_link();
        DROP TRIGGER IF EXISTS participant_authority_link ON participant_checkpoint_acks;
        CREATE TRIGGER participant_authority_link BEFORE INSERT OR UPDATE
            ON participant_checkpoint_acks FOR EACH ROW
            EXECUTE FUNCTION coordinator_validate_authority_link();
        DROP TRIGGER IF EXISTS completion_authority_link ON checkpoint_completions_v2;
        CREATE TRIGGER completion_authority_link BEFORE INSERT OR UPDATE
            ON checkpoint_completions_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_validate_authority_link();
        DROP TRIGGER IF EXISTS abort_authority_link ON checkpoint_aborts_v2;
        CREATE TRIGGER abort_authority_link BEFORE INSERT OR UPDATE
            ON checkpoint_aborts_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_validate_authority_link();
        DROP TRIGGER IF EXISTS recovery_authority_link ON recovery_plans_v2;
        CREATE TRIGGER recovery_authority_link BEFORE INSERT OR UPDATE
            ON recovery_plans_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_validate_authority_link();
        DROP TRIGGER IF EXISTS revision_sources_immutable ON revision_source_partitions;
        CREATE TRIGGER revision_sources_immutable BEFORE UPDATE OR DELETE
            ON revision_source_partitions FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS revision_participants_immutable ON revision_participants;
        CREATE TRIGGER revision_participants_immutable BEFORE UPDATE OR DELETE
            ON revision_participants FOR EACH ROW
            EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS revision_edges_immutable ON revision_edges;
        CREATE TRIGGER revision_edges_immutable BEFORE UPDATE OR DELETE ON revision_edges
            FOR EACH ROW EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS participant_output_roots_immutable ON participant_output_roots;
        CREATE TRIGGER participant_output_roots_immutable BEFORE UPDATE OR DELETE ON participant_output_roots
            FOR EACH ROW EXECUTE FUNCTION coordinator_reject_immutable_mutation();
        DROP TRIGGER IF EXISTS revision_identity_guard ON deployment_revisions;
        CREATE TRIGGER revision_identity_guard BEFORE UPDATE
            ON deployment_revisions FOR EACH ROW
            EXECUTE FUNCTION coordinator_guard_revision_identity();
        DROP TRIGGER IF EXISTS checkpoint_lifecycle_v2_guard ON checkpoint_lifecycle_v2;
        CREATE TRIGGER checkpoint_lifecycle_v2_guard BEFORE UPDATE
            ON checkpoint_lifecycle_v2 FOR EACH ROW
            EXECUTE FUNCTION coordinator_guard_lifecycle();
        DROP TRIGGER IF EXISTS coordinator_retention_release_guard ON checkpoints;
        CREATE TRIGGER coordinator_retention_release_guard BEFORE UPDATE OF status
            ON checkpoints FOR EACH ROW
            EXECUTE FUNCTION coordinator_guard_retention_release();

        INSERT INTO partition_log_heads(
            channel,partition_id,next_log_sequence,max_log_entries)
            SELECT channel,partition_id,1,max_metadata_records FROM partitions
            ON CONFLICT(channel,partition_id) DO NOTHING;
        WITH ranked AS (
            SELECT channel,partition_id,sequence,record_id,
                   row_number() OVER(
                       PARTITION BY channel,partition_id ORDER BY sequence
                   ) AS log_sequence
            FROM records
        )
        INSERT INTO partition_log_entries(
            channel,partition_id,log_sequence,entry_kind,data_sequence,record_id)
            SELECT channel,partition_id,log_sequence,'DATA',sequence,record_id
            FROM ranked ON CONFLICT DO NOTHING;
        UPDATE partition_log_heads h
            SET next_log_sequence=x.next_log_sequence,
                used_log_entries=x.next_log_sequence-1
            FROM (
                SELECT channel,partition_id,max(log_sequence)+1 AS next_log_sequence
                FROM partition_log_entries GROUP BY channel,partition_id
            ) x
            WHERE h.channel=x.channel AND h.partition_id=x.partition_id
              AND h.next_log_sequence<x.next_log_sequence;
        DROP TRIGGER IF EXISTS partition_log_head_guard ON partition_log_heads;
        CREATE TRIGGER partition_log_head_guard BEFORE UPDATE
            ON partition_log_heads FOR EACH ROW
            EXECUTE FUNCTION coordinator_guard_log_head();
        DROP TRIGGER IF EXISTS records_capture_partition_log ON records;
        CREATE TRIGGER records_capture_partition_log AFTER INSERT ON records
            FOR EACH ROW EXECUTE FUNCTION coordinator_capture_data_log();
        """
        async with self.pool.acquire() as conn:
            await conn.execute(ddl)
            await conn.execute(functions_and_triggers)

    async def configure_ordered_log_capacity(
        self,
        channel: str,
        partition: str,
        max_log_entries: int,
    ) -> None:
        if max_log_entries <= 0:
            raise ValueError("ordered-log capacity must be positive")
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{channel}|{partition}",
            )
            part = await conn.fetchrow(
                """SELECT 1 FROM partitions WHERE channel=$1 AND partition_id=$2
                   FOR UPDATE""",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown ordered-log partition")
            row = await conn.fetchrow(
                """INSERT INTO partition_log_heads(
                     channel,partition_id,next_log_sequence,max_log_entries,
                     used_log_entries)
                   VALUES($1,$2,1,$3,0)
                   ON CONFLICT(channel,partition_id) DO UPDATE
                     SET max_log_entries=EXCLUDED.max_log_entries
                     WHERE partition_log_heads.used_log_entries=0
                       AND partition_log_heads.next_log_sequence=1
                   RETURNING *""",
                channel,
                partition,
                max_log_entries,
            )
            if row is None or row["max_log_entries"] != max_log_entries:
                raise PostgresProtocolError(
                    "active ordered-log capacity cannot be changed"
                )

    async def _locked_revision(
        self,
        conn: asyncpg.Connection,
        revision_id: str,
    ) -> asyncpg.Record:
        revision = await conn.fetchrow(
            "SELECT * FROM deployment_revisions WHERE revision_id=$1 FOR UPDATE",
            revision_id,
        )
        if revision is None:
            raise PostgresProtocolError("unknown deployment revision")
        return revision

    @staticmethod
    def _check_graph(
        revision: asyncpg.Record,
        graph_version: str,
        graph_digest: str,
    ) -> None:
        if (revision["graph_version"], revision["graph_digest"]) != (
            graph_version,
            graph_digest,
        ):
            raise PostgresProtocolError("checkpoint graph identity drift")

    async def _assert_owner(
        self,
        conn: asyncpg.Connection,
        revision_id: str,
        owner_id: str,
        owner_epoch: int,
    ) -> asyncpg.Record:
        lease = await conn.fetchrow(
            "SELECT * FROM coordinator_leases WHERE revision_id=$1 FOR UPDATE",
            revision_id,
        )
        current = (
            False
            if lease is None
            else await conn.fetchval(
                "SELECT $1::timestamptz>clock_timestamp()", lease["lease_expires_at"]
            )
        )
        if (
            lease is None
            or not current
            or (lease["owner_id"], lease["owner_epoch"]) != (owner_id, owner_epoch)
        ):
            raise StaleOwner("checkpoint coordinator fenced")
        return lease

    async def _authority_event(
        self,
        conn: asyncpg.Connection,
        revision: asyncpg.Record,
        event_type: str,
        writer_epoch: int,
        payload: dict[str, Any],
        *,
        checkpoint_id: str | None = None,
        recovery_id: str | None = None,
    ) -> int:
        ordinal = revision["next_authority_ordinal"]
        live_events = await conn.fetchval(
            "SELECT count(*) FROM coordinator_authority_events WHERE revision_id=$1",
            revision["revision_id"],
        )
        if live_events >= revision["max_authority_events"]:
            raise PostgresProtocolError("checkpoint authority-event budget exhausted")
        identity = {
            "revision_id": revision["revision_id"],
            "authority_ordinal": ordinal,
            "event_type": event_type,
            "checkpoint_id": checkpoint_id,
            "recovery_id": recovery_id,
            "writer_epoch": writer_epoch,
            "payload": payload,
        }
        await conn.execute(
            """INSERT INTO coordinator_authority_events(
               revision_id,authority_ordinal,event_type,checkpoint_id,recovery_id,
               writer_epoch,payload,event_digest)
               VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)""",
            revision["revision_id"],
            ordinal,
            event_type,
            checkpoint_id,
            recovery_id,
            writer_epoch,
            canonical(payload),
            digest(identity),
        )
        await conn.execute(
            "UPDATE deployment_revisions SET next_authority_ordinal=$2 "
            "WHERE revision_id=$1",
            revision["revision_id"],
            ordinal + 1,
        )
        await self._assert_metadata_budget(conn, revision["revision_id"])
        return ordinal

    @staticmethod
    async def _metadata_bytes(
        conn: asyncpg.Connection,
        revision_id: str,
    ) -> int:
        used = await conn.fetchval(
            """SELECT
               (SELECT COALESCE(sum(octet_length(row_to_json(i)::text)),0)
                  FROM checkpoint_intents_v2 i WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(l)::text)),0)
                  FROM checkpoint_lifecycle_v2 l JOIN checkpoint_intents_v2 i
                    USING(checkpoint_id) WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(b)::text)),0)
                  FROM source_barrier_observations b JOIN checkpoint_intents_v2 i
                    USING(checkpoint_id) WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(a)::text)),0)
                  FROM participant_checkpoint_acks a JOIN checkpoint_intents_v2 i
                    USING(checkpoint_id) WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(c)::text)),0)
                  FROM checkpoint_completions_v2 c JOIN checkpoint_intents_v2 i
                    USING(checkpoint_id) WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(a)::text)),0)
                  FROM checkpoint_aborts_v2 a JOIN checkpoint_intents_v2 i
                    USING(checkpoint_id) WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(e)::text)),0)
                  FROM coordinator_authority_events e WHERE e.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(r)::text)),0)
                  FROM recovery_plans_v2 r WHERE r.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(s)::text)),0)
                  FROM participant_scan_positions s WHERE s.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(p)::text)),0)
                  FROM partition_log_entries p WHERE p.revision_id=$1)
               +(SELECT COALESCE(sum(octet_length(row_to_json(root)::text)),0)
                  FROM checkpoints root WHERE EXISTS(
                    SELECT 1 FROM (
                      SELECT checkpoint_id,retention_ref_id FROM source_barrier_observations
                      UNION ALL SELECT checkpoint_id,retention_ref_id FROM participant_output_roots
                    ) b
                    JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                    WHERE i.revision_id=$1
                      AND b.retention_ref_id=root.checkpoint_id))
               +(SELECT COALESCE(sum(octet_length(row_to_json(r)::text)),0)
                  FROM participant_output_roots r JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                  WHERE i.revision_id=$1)
               +(SELECT COALESCE(sum(o.payload_size),0)
                  FROM checkpoint_snapshot_objects o WHERE EXISTS(
                    SELECT 1 FROM participant_checkpoint_acks a
                    JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                    WHERE i.revision_id=$1 AND a.snapshot_object_key=o.object_key))""",
            revision_id,
        )
        for table in (
            "region_graph_nodes_v1",
            "region_graph_edges_v1",
            "region_graph_seals_v1",
            "region_edge_cuts_v1",
            "region_recovery_plans_v1",
            "regional_topology_bindings_v1",
            "regional_topology_seals_v1",
            "regional_recovery_workers_v1",
            "checkpoint_archive_heads_v1",
            "join_retirement_archive_heads_v1",
            "checkpoint_retirement_operations_v1",
            "regional_archive_heads_v1",
            "regional_retirement_operations_v1",
            "streaming_retention_operations_v1",
            "join_retention_anchors_v1",
            "join_retirement_manifests_v1",
        ):
            if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table):
                used += await conn.fetchval(
                    f"""SELECT COALESCE(sum(octet_length(row_to_json(x)::text)),0)
                    FROM {table} x WHERE revision_id=$1""",
                    revision_id,
                )
        if await conn.fetchval(
            "SELECT to_regclass('join_retirement_rows_v1') IS NOT NULL"
        ):
            used += await conn.fetchval(
                """SELECT COALESCE(sum(octet_length(row_to_json(r)::text)),0)
                FROM join_retirement_rows_v1 r JOIN join_retirement_manifests_v1 m USING(operation_id)
                WHERE m.revision_id=$1""",
                revision_id,
            )
        if await conn.fetchval(
            "SELECT to_regclass('partition_log_archive_heads_v1') IS NOT NULL"
        ):
            used += await conn.fetchval(
                """SELECT COALESCE(sum(octet_length(row_to_json(h)::text)),0)
                FROM partition_log_archive_heads_v1 h WHERE EXISTS(SELECT 1 FROM revision_source_partitions s
                  WHERE s.revision_id=$1 AND s.channel=h.channel AND s.partition_id=h.partition_id)
                OR EXISTS(SELECT 1 FROM revision_participants p,jsonb_array_elements(p.output_manifest) o
                  WHERE p.revision_id=$1 AND o->>'channel'=h.channel AND o->>'partition'=h.partition_id)""",
                revision_id,
            )
        for table in (
            "regional_recovery_attempts_v1",
            "regional_recovery_attempt_workers_v1",
        ):
            if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table):
                used += await conn.fetchval(
                    f"""SELECT COALESCE(sum(octet_length(row_to_json(x)::text)),0)
                    FROM {table} x JOIN region_recovery_plans_v1 p USING(plan_id)
                    WHERE p.revision_id=$1""",
                    revision_id,
                )
        return used

    async def _assert_metadata_budget(
        self,
        conn: asyncpg.Connection,
        revision_id: str,
    ) -> None:
        revision = await conn.fetchrow(
            """SELECT max_checkpoint_metadata_bytes FROM deployment_revisions
               WHERE revision_id=$1""",
            revision_id,
        )
        used = await self._metadata_bytes(conn, revision_id)
        if used > revision["max_checkpoint_metadata_bytes"]:
            raise PostgresProtocolError("checkpoint metadata-byte budget exhausted")

    async def checkpoint_pressure(
        self,
        revision_id: str,
    ) -> dict[str, Any]:
        async with (
            self.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            revision = await conn.fetchrow(
                "SELECT * FROM deployment_revisions WHERE revision_id=$1",
                revision_id,
            )
            if revision is None:
                raise PostgresProtocolError("unknown deployment revision")
            counts = await conn.fetchrow(
                """SELECT
                   count(*) FILTER(WHERE l.status NOT IN(
                     'COMPLETED','ABORTED','RETIRED')) AS inflight,
                   count(*) FILTER(WHERE l.status<>'RETIRED') AS retained,
                   EXTRACT(EPOCH FROM clock_timestamp()-min(i.created_at))
                     ::double precision AS oldest_age
                   FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE i.revision_id=$1""",
                revision_id,
            )
            metadata_bytes = await self._metadata_bytes(conn, revision_id)
            authority_events = await conn.fetchval(
                "SELECT count(*) FROM coordinator_authority_events WHERE revision_id=$1",
                revision_id,
            )
            dimensions = []
            stream_retention_pending = False
            try:
                await self._assert_no_active_stream_retention(conn, revision_id)
            except CheckpointControlPressure:
                stream_retention_pending = True
                dimensions.append("streaming_retention_pending")
            if counts["inflight"] >= revision["max_inflight_checkpoints"]:
                dimensions.append("inflight_checkpoints")
            if counts["retained"] >= revision["max_retained_checkpoints"]:
                dimensions.append("retained_checkpoints")
            if metadata_bytes >= revision["max_checkpoint_metadata_bytes"]:
                dimensions.append("metadata_bytes")
            if (
                counts["oldest_age"] is not None
                and counts["oldest_age"] >= revision["max_checkpoint_age_seconds"]
            ):
                dimensions.append("metadata_age")
            if authority_events >= revision["max_authority_events"]:
                dimensions.append("authority_events")
            return {
                "revision_id": revision_id,
                "inflight_checkpoints": counts["inflight"],
                "max_inflight_checkpoints": revision["max_inflight_checkpoints"],
                "retained_checkpoints": counts["retained"],
                "max_retained_checkpoints": revision["max_retained_checkpoints"],
                "metadata_bytes": metadata_bytes,
                "max_checkpoint_metadata_bytes": revision[
                    "max_checkpoint_metadata_bytes"
                ],
                "oldest_age_seconds": counts["oldest_age"],
                "max_checkpoint_age_seconds": revision["max_checkpoint_age_seconds"],
                "authority_events": authority_events,
                "max_authority_events": revision["max_authority_events"],
                "streaming_retention_pending": stream_retention_pending,
                "binding_dimensions": dimensions,
            }

    async def register_revision(
        self,
        *,
        revision_id: str,
        deployment_id: str,
        graph_version: str,
        sources: list[tuple[str, str]],
        participants: list[dict[str, Any]],
        code_manifest_digest: str,
        state_contract_digest: str,
        max_inflight_checkpoints: int = 2,
        max_retained_checkpoints: int = 4,
        max_checkpoint_metadata_bytes: int = 1_000_000,
        max_checkpoint_age_seconds: float = 3600,
        max_authority_events: int = 10_000,
    ) -> Revision:
        if (
            min(
                max_inflight_checkpoints,
                max_retained_checkpoints,
                max_checkpoint_metadata_bytes,
                max_checkpoint_age_seconds,
                max_authority_events,
            )
            <= 0
        ):
            raise ValueError("checkpoint limits must be positive")
        source_manifest = canonical_locations(sources)
        participant_manifest = canonical_participants(participants)
        source_set = {(item["channel"], item["partition"]) for item in source_manifest}
        producers = {}
        for participant in participant_manifest:
            for output in participant["outputs"]:
                key = (output["channel"], output["partition"])
                if key in source_set or key in producers:
                    raise ValueError("partition must have one immutable producer")
                producers[key] = participant["participant_id"]
        dependencies = {p["participant_id"]: set() for p in participant_manifest}
        for participant in participant_manifest:
            for item in participant["inputs"]:
                key = (item["channel"], item["partition"])
                if key not in source_set and key not in producers:
                    raise ValueError("participant input has no declared producer")
                if key in producers:
                    dependencies[participant["participant_id"]].add(producers[key])
        visited = set()
        while len(visited) < len(dependencies):
            ready = {
                node
                for node, deps in dependencies.items()
                if node not in visited and deps <= visited
            }
            if not ready:
                raise ValueError("stream graph must be acyclic")
            visited.update(ready)
        graph_identity = {
            "revision_id": revision_id,
            "deployment_id": deployment_id,
            "graph_version": graph_version,
            "code_manifest_digest": code_manifest_digest,
            "state_contract_digest": state_contract_digest,
            "source_manifest": source_manifest,
            "participant_manifest": participant_manifest,
        }
        graph_digest = digest(graph_identity)
        all_locations = source_set | {
            (item["channel"], item["partition"])
            for participant in participant_manifest
            for item in participant["outputs"]
        }
        async with self.pool.acquire() as conn, conn.transaction():
            missing = 0
            for channel, partition in sorted(all_locations):
                if not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM partitions
                       WHERE channel=$1 AND partition_id=$2)""",
                    channel,
                    partition,
                ):
                    missing += 1
            if missing:
                raise PostgresProtocolError("deployment revision partition is unknown")
            inserted = await conn.execute(
                """INSERT INTO deployment_revisions(
                   revision_id,deployment_id,graph_version,graph_digest,
                   code_manifest_digest,state_contract_digest,source_manifest,
                   participant_manifest,max_inflight_checkpoints,
                   max_retained_checkpoints,max_checkpoint_metadata_bytes,
                   max_checkpoint_age_seconds,max_authority_events)
                   VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13)
                   ON CONFLICT DO NOTHING""",
                revision_id,
                deployment_id,
                graph_version,
                graph_digest,
                code_manifest_digest,
                state_contract_digest,
                canonical(source_manifest),
                canonical(participant_manifest),
                max_inflight_checkpoints,
                max_retained_checkpoints,
                max_checkpoint_metadata_bytes,
                max_checkpoint_age_seconds,
                max_authority_events,
            )
            row = await self._locked_revision(conn, revision_id)
            actual = (
                row["deployment_id"],
                row["graph_version"],
                row["graph_digest"],
                row["code_manifest_digest"],
                row["state_contract_digest"],
                decoded(row["source_manifest"]),
                decoded(row["participant_manifest"]),
                row["max_inflight_checkpoints"],
                row["max_retained_checkpoints"],
                row["max_checkpoint_metadata_bytes"],
                row["max_checkpoint_age_seconds"],
                row["max_authority_events"],
            )
            expected = (
                deployment_id,
                graph_version,
                graph_digest,
                code_manifest_digest,
                state_contract_digest,
                source_manifest,
                participant_manifest,
                max_inflight_checkpoints,
                max_retained_checkpoints,
                max_checkpoint_metadata_bytes,
                float(max_checkpoint_age_seconds),
                max_authority_events,
            )
            if actual != expected:
                raise PostgresProtocolError("deployment revision identity drift")
            if inserted == "INSERT 0 1":
                await conn.executemany(
                    """INSERT INTO revision_source_partitions(
                       revision_id,channel,partition_id) VALUES($1,$2,$3)""",
                    [
                        (revision_id, item["channel"], item["partition"])
                        for item in source_manifest
                    ],
                )
                await conn.executemany(
                    """INSERT INTO revision_participants(
                       revision_id,participant_id,input_manifest,output_manifest,
                       code_digest,state_schema,state_version)
                       VALUES($1,$2,$3::jsonb,$4::jsonb,$5,$6,$7)""",
                    [
                        (
                            revision_id,
                            participant["participant_id"],
                            canonical(participant["inputs"]),
                            canonical(participant["outputs"]),
                            participant["code_digest"],
                            participant["state_schema"],
                            participant["state_version"],
                        )
                        for participant in participant_manifest
                    ],
                )
                await conn.executemany(
                    """INSERT INTO revision_edges VALUES($1,$2,$3,$4,$5)""",
                    [
                        (
                            revision_id,
                            item["channel"],
                            item["partition"],
                            producers.get(
                                (item["channel"], item["partition"]), "SOURCE"
                            ),
                            p["participant_id"],
                        )
                        for p in participant_manifest
                        for item in p["inputs"]
                    ],
                )
        return Revision(
            revision_id,
            deployment_id,
            graph_version,
            graph_digest,
            source_manifest,
            participant_manifest,
        )

    async def claim_coordinator(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        *,
        lease_seconds: float,
    ) -> int:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            lease = await conn.fetchrow(
                "SELECT * FROM coordinator_leases WHERE revision_id=$1 FOR UPDATE",
                revision_id,
            )
            if (
                lease is not None
                and lease["owner_id"] == owner_id
                and await conn.fetchval(
                    "SELECT $1::timestamptz>clock_timestamp()",
                    lease["lease_expires_at"],
                )
            ):
                await conn.execute(
                    """UPDATE coordinator_leases SET
                       lease_expires_at=clock_timestamp()
                         +$2::double precision*interval '1 second'
                       WHERE revision_id=$1""",
                    revision_id,
                    lease_seconds,
                )
                return lease["owner_epoch"]
            if lease is not None and await conn.fetchval(
                "SELECT $1::timestamptz>clock_timestamp()", lease["lease_expires_at"]
            ):
                raise StaleOwner("checkpoint coordinator lease is held")
            epoch = 1 if lease is None else lease["owner_epoch"] + 1
            ordinal = await self._authority_event(
                conn,
                revision,
                "CLAIM",
                epoch,
                {"owner_id": owner_id, "owner_epoch": epoch},
            )
            if lease is None:
                await conn.execute(
                    """INSERT INTO coordinator_leases(
                       revision_id,owner_id,owner_epoch,lease_expires_at)
                       VALUES($1,$2,$3,clock_timestamp()
                         +$4::double precision*interval '1 second')""",
                    revision_id,
                    owner_id,
                    epoch,
                    lease_seconds,
                )
            else:
                await conn.execute(
                    """UPDATE coordinator_leases SET owner_id=$2,owner_epoch=$3,
                       lease_expires_at=clock_timestamp()
                         +$4::double precision*interval '1 second'
                       WHERE revision_id=$1""",
                    revision_id,
                    owner_id,
                    epoch,
                    lease_seconds,
                )
            assert ordinal > 0
        return epoch

    async def renew_coordinator(
        self,
        revision_id: str,
        owner_id: str,
        owner_epoch: int,
        *,
        lease_seconds: float,
    ) -> None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be positive")
        async with self.pool.acquire() as conn, conn.transaction():
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            await conn.execute(
                """UPDATE coordinator_leases SET
                   lease_expires_at=clock_timestamp()
                     +$4::double precision*interval '1 second'
                   WHERE revision_id=$1 AND owner_id=$2 AND owner_epoch=$3""",
                revision_id,
                owner_id,
                owner_epoch,
                lease_seconds,
            )

    async def _assert_no_active_stream_retention(
        self, conn: asyncpg.Connection, revision_id: str
    ) -> None:
        if await conn.fetchval(
            "SELECT to_regclass('streaming_retention_operations_v1') IS NOT NULL"
        ) and await conn.fetchval(
            """SELECT EXISTS(SELECT 1 FROM streaming_retention_operations_v1
               WHERE revision_id=$1 AND status<>'COMPLETED')""",
            revision_id,
        ):
            raise CheckpointControlPressure(
                "checkpoint control blocked by incomplete streaming retention"
            )

    async def start_checkpoint(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        request_id: str,
        *,
        fault: FaultHook = None,
    ) -> Checkpoint:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            existing = await conn.fetchrow(
                """SELECT i.*,l.status FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE i.request_id=$1""",
                request_id,
            )
            if existing is not None:
                if existing["revision_id"] != revision_id:
                    raise PostgresProtocolError("checkpoint request identity drift")
                return Checkpoint(
                    existing["checkpoint_id"],
                    existing["checkpoint_ordinal"],
                    existing["intent_digest"],
                    existing["status"],
                )
            await self._assert_no_active_stream_retention(conn, revision_id)
            inflight = await conn.fetchval(
                """SELECT count(*) FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE i.revision_id=$1
                     AND l.status NOT IN('COMPLETED','ABORTED','RETIRED')""",
                revision_id,
            )
            retained = await conn.fetchval(
                """SELECT count(*) FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status<>'RETIRED'""",
                revision_id,
            )
            oldest_age = await conn.fetchval(
                """SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(i.created_at))
                   ::double precision FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status<>'RETIRED'""",
                revision_id,
            )
            metadata_bytes = await self._metadata_bytes(conn, revision_id)
            if inflight >= revision["max_inflight_checkpoints"]:
                raise PostgresProtocolError("checkpoint inflight budget exhausted")
            if (
                inflight
                and await conn.fetchval(
                    "SELECT to_regclass('join_workers') IS NOT NULL"
                )
                and await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM join_workers WHERE revision_id=$1)",
                    revision_id,
                )
            ):
                raise PostgresProtocolError("aligned DAG permits one active checkpoint")
            if retained >= revision["max_retained_checkpoints"]:
                raise PostgresProtocolError("checkpoint retention budget exhausted")
            if metadata_bytes >= revision["max_checkpoint_metadata_bytes"]:
                raise PostgresProtocolError("checkpoint metadata-byte budget exhausted")
            if (
                oldest_age is not None
                and oldest_age >= revision["max_checkpoint_age_seconds"]
            ):
                raise PostgresProtocolError("checkpoint metadata-age budget exhausted")
            checkpoint_ordinal = revision["next_checkpoint_ordinal"]
            checkpoint_id = "checkpoint:" + digest(
                {
                    "revision_id": revision_id,
                    "checkpoint_ordinal": checkpoint_ordinal,
                    "request_id": request_id,
                }
            )
            identity = {
                "checkpoint_id": checkpoint_id,
                "request_id": request_id,
                "revision_id": revision_id,
                "checkpoint_ordinal": checkpoint_ordinal,
                "graph_digest": graph_digest,
                "source_manifest": decoded(revision["source_manifest"]),
                "participant_manifest": decoded(revision["participant_manifest"]),
            }
            intent_digest = digest(identity)
            authority_ordinal = await self._authority_event(
                conn,
                revision,
                "INTENT",
                owner_epoch,
                {"intent_digest": intent_digest},
                checkpoint_id=checkpoint_id,
            )
            await conn.execute(
                """INSERT INTO checkpoint_intents_v2(
                   checkpoint_id,request_id,revision_id,checkpoint_ordinal,
                   graph_digest,source_manifest,participant_manifest,intent_digest,
                   created_by_epoch,intent_authority_ordinal)
                   VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10)""",
                checkpoint_id,
                request_id,
                revision_id,
                checkpoint_ordinal,
                graph_digest,
                canonical(decoded(revision["source_manifest"])),
                canonical(decoded(revision["participant_manifest"])),
                intent_digest,
                owner_epoch,
                authority_ordinal,
            )
            await conn.execute(
                """INSERT INTO checkpoint_lifecycle_v2(
                   checkpoint_id,status,last_authority_ordinal)
                   VALUES($1,'STARTED',$2)""",
                checkpoint_id,
                authority_ordinal,
            )
            await conn.execute(
                "UPDATE deployment_revisions SET next_checkpoint_ordinal=$2 "
                "WHERE revision_id=$1",
                revision_id,
                checkpoint_ordinal + 1,
            )
            await self._assert_metadata_budget(conn, revision_id)
            hit(fault, "intent_before_commit")
        hit(fault, "intent_after_commit")
        return Checkpoint(checkpoint_id, checkpoint_ordinal, intent_digest, "STARTED")

    async def inject_source_barrier(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        checkpoint_id: str,
        channel: str,
        partition: str,
        *,
        fault: FaultHook = None,
    ) -> BarrierPosition:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            intent = await conn.fetchrow(
                "SELECT * FROM checkpoint_intents_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            lifecycle = await conn.fetchrow(
                "SELECT * FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1 FOR UPDATE",
                checkpoint_id,
            )
            if (
                intent is None
                or lifecycle is None
                or intent["revision_id"] != revision_id
            ):
                raise PostgresProtocolError("unknown checkpoint intent")
            existing = await conn.fetchrow(
                """SELECT * FROM source_barrier_observations
                   WHERE checkpoint_id=$1 AND channel=$2 AND partition_id=$3""",
                checkpoint_id,
                channel,
                partition,
            )
            if existing is not None:
                return BarrierPosition(
                    checkpoint_id,
                    channel,
                    partition,
                    existing["barrier_log_sequence"],
                    existing["cut_data_sequence"],
                )
            if lifecycle["status"] not in ("STARTED", "ALIGNING"):
                raise PostgresProtocolError(
                    "checkpoint does not accept source barriers"
                )
            if not await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM revision_source_partitions
                   WHERE revision_id=$1 AND channel=$2 AND partition_id=$3)""",
                revision_id,
                channel,
                partition,
            ):
                raise PostgresProtocolError("barrier input is outside source manifest")
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{channel}|{partition}",
            )
            hit(fault, "barrier_after_partition_lock")
            part = await conn.fetchrow(
                """SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2
                   FOR UPDATE""",
                channel,
                partition,
            )
            head = await conn.fetchrow(
                """UPDATE partition_log_heads
                   SET next_log_sequence=next_log_sequence+1,
                       used_log_entries=used_log_entries+1
                   WHERE channel=$1 AND partition_id=$2
                     AND used_log_entries < max_log_entries
                   RETURNING next_log_sequence-1 AS allocated""",
                channel,
                partition,
            )
            if part is None or head is None:
                raise PostgresProtocolError("source partition log is unavailable")
            log_sequence = head["allocated"]
            cut = part["next_sequence"] - 1
            await conn.execute(
                """INSERT INTO partition_log_entries(
                   channel,partition_id,log_sequence,entry_kind,barrier_role,
                   checkpoint_id,cut_data_sequence,intent_digest,writer_epoch,
                   revision_id)
                   VALUES($1,$2,$3,'BARRIER','SOURCE',$4,$5,$6,$7,$8)""",
                channel,
                partition,
                log_sequence,
                checkpoint_id,
                cut,
                intent["intent_digest"],
                owner_epoch,
                revision_id,
            )
            ref_id = f"{checkpoint_id}:{channel}:{partition}"
            await conn.execute(
                """INSERT INTO checkpoints(
                   checkpoint_id,channel,partition_id,cut_sequence,status)
                   VALUES($1,$2,$3,$4,'SEALED')""",
                ref_id,
                channel,
                partition,
                cut,
            )
            observed = 1 + await conn.fetchval(
                "SELECT count(*) FROM source_barrier_observations WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            expected = len(decoded(intent["source_manifest"]))
            revision = await self._locked_revision(conn, revision_id)
            event_ordinal = await self._authority_event(
                conn,
                revision,
                "SOURCE_BARRIERS",
                owner_epoch,
                {
                    "channel": channel,
                    "partition": partition,
                    "log_sequence": log_sequence,
                    "cut_data_sequence": cut,
                    "observed": observed,
                    "expected": expected,
                },
                checkpoint_id=checkpoint_id,
            )
            await conn.execute(
                """INSERT INTO source_barrier_observations(
                   checkpoint_id,channel,partition_id,barrier_log_sequence,
                   cut_data_sequence,intent_digest,writer_epoch,retention_ref_id,
                   barrier_authority_ordinal)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                checkpoint_id,
                channel,
                partition,
                log_sequence,
                cut,
                intent["intent_digest"],
                owner_epoch,
                ref_id,
                event_ordinal,
            )
            new_status = "ALIGNED" if observed == expected else "ALIGNING"
            await conn.execute(
                """UPDATE checkpoint_lifecycle_v2 SET status=$2,
                   last_authority_ordinal=$3 WHERE checkpoint_id=$1""",
                checkpoint_id,
                new_status,
                event_ordinal,
            )
            await self._assert_metadata_budget(conn, revision_id)
            hit(fault, "barrier_before_commit")
        hit(fault, "barrier_after_commit")
        return BarrierPosition(checkpoint_id, channel, partition, log_sequence, cut)

    async def acknowledge_participant(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        checkpoint_id: str,
        participant_id: str,
        snapshot_state: Any,
        *,
        mailbox_highwater: int,
        state_version: int,
        fault: FaultHook = None,
        connection: asyncpg.Connection | None = None,
    ) -> str:
        if mailbox_highwater < 0 or state_version <= 0:
            raise ValueError("invalid snapshot version or mailbox highwater")
        async with self._participant_connection(connection) as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            intent = await conn.fetchrow(
                "SELECT * FROM checkpoint_intents_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            lifecycle = await conn.fetchrow(
                "SELECT * FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1 FOR UPDATE",
                checkpoint_id,
            )
            participant = await conn.fetchrow(
                """SELECT * FROM revision_participants
                   WHERE revision_id=$1 AND participant_id=$2""",
                revision_id,
                participant_id,
            )
            if (
                intent is None
                or lifecycle is None
                or intent["revision_id"] != revision_id
            ):
                raise PostgresProtocolError("unknown checkpoint intent")
            if participant is None:
                raise PostgresProtocolError("unknown checkpoint participant")
            if state_version != participant["state_version"]:
                raise PostgresProtocolError("participant state version is incompatible")
            if lifecycle["status"] not in ("ALIGNED", "COMPLETED"):
                raise PostgresProtocolError("checkpoint source barriers are incomplete")
            observations = {
                location_key(row["channel"], row["partition_id"]): row
                for row in await conn.fetch(
                    """SELECT * FROM source_barrier_observations
                       WHERE checkpoint_id=$1""",
                    checkpoint_id,
                )
            }
            for row in await conn.fetch(
                """SELECT channel,partition_id,
                log_sequence AS barrier_log_sequence,cut_data_sequence
                FROM partition_log_entries WHERE checkpoint_id=$1
                  AND revision_id=$2 AND barrier_role='OUTPUT' """,
                checkpoint_id,
                revision_id,
            ):
                observations[location_key(row["channel"], row["partition_id"])] = row
            input_manifest = decoded(participant["input_manifest"])
            barrier_vector: dict[str, dict[str, int]] = {}
            replay_tail_floor: dict[str, int] = {}
            for item in input_manifest:
                key = location_key(item["channel"], item["partition"])
                observation = observations.get(key)
                if observation is None:
                    raise PostgresProtocolError(
                        "participant barrier vector is incomplete"
                    )
                barrier_vector[key] = {
                    "cut_data_sequence": observation["cut_data_sequence"],
                    "barrier_log_sequence": observation["barrier_log_sequence"],
                }
                replay_tail_floor[key] = observation["cut_data_sequence"] + 1

            snapshot_payload = canonical(snapshot_state).encode()
            snapshot_object_key = content_object_key(
                snapshot_payload, participant["state_schema"]
            )
            payload_digest = hashlib.sha256(snapshot_payload).hexdigest()
            existing_ack = await conn.fetchrow(
                """SELECT * FROM participant_checkpoint_acks
                   WHERE checkpoint_id=$1 AND participant_id=$2""",
                checkpoint_id,
                participant_id,
            )
            if existing_ack is not None:
                retry_identity = {
                    "checkpoint_id": checkpoint_id,
                    "participant_id": participant_id,
                    "intent_digest": intent["intent_digest"],
                    "barrier_vector": barrier_vector,
                    "snapshot_object_key": snapshot_object_key,
                    "snapshot_payload_digest": payload_digest,
                    "mailbox_highwater": mailbox_highwater,
                    "replay_tail_floor": replay_tail_floor,
                    "output_barriers": decoded(existing_ack["output_barriers"]),
                    "state_schema": participant["state_schema"],
                    "state_version": state_version,
                    "code_digest": participant["code_digest"],
                }
                if existing_ack["ack_digest"] != digest(retry_identity):
                    raise PostgresProtocolError("participant acknowledgement drift")
                return existing_ack["ack_digest"]
            if lifecycle["status"] == "COMPLETED":
                raise PostgresProtocolError(
                    "completed checkpoint is missing participant acknowledgement"
                )

            output_barriers: dict[str, dict[str, int]] = {}
            for item in decoded(participant["output_manifest"]):
                channel = item["channel"]
                partition = item["partition"]
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"{self.schema}|{channel}|{partition}",
                )
                part = await conn.fetchrow(
                    """SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2
                       FOR UPDATE""",
                    channel,
                    partition,
                )
                head = await conn.fetchrow(
                    """UPDATE partition_log_heads
                       SET next_log_sequence=next_log_sequence+1,
                           used_log_entries=used_log_entries+1
                       WHERE channel=$1 AND partition_id=$2
                         AND used_log_entries < max_log_entries
                       RETURNING next_log_sequence-1 AS allocated""",
                    channel,
                    partition,
                )
                if part is None or head is None:
                    raise PostgresProtocolError("output partition log is unavailable")
                await conn.execute(
                    """INSERT INTO partition_log_entries(
                       channel,partition_id,log_sequence,entry_kind,barrier_role,
                       checkpoint_id,cut_data_sequence,intent_digest,writer_epoch,
                       revision_id)
                       VALUES($1,$2,$3,'BARRIER','OUTPUT',$4,$5,$6,$7,$8)""",
                    channel,
                    partition,
                    head["allocated"],
                    checkpoint_id,
                    part["next_sequence"] - 1,
                    intent["intent_digest"],
                    owner_epoch,
                    revision_id,
                )
                output_barriers[location_key(channel, partition)] = {
                    "cut_data_sequence": part["next_sequence"] - 1,
                    "barrier_log_sequence": head["allocated"],
                }
                ref_id = f"{checkpoint_id}:output:{channel}:{partition}"
                await conn.execute(
                    """INSERT INTO checkpoints VALUES($1,$2,$3,$4,'SEALED')""",
                    ref_id,
                    channel,
                    partition,
                    part["next_sequence"] - 1,
                )
                await conn.execute(
                    """INSERT INTO participant_output_roots VALUES($1,$2,$3,$4,$5)""",
                    checkpoint_id,
                    participant_id,
                    channel,
                    partition,
                    ref_id,
                )

            existing_object = await conn.fetchrow(
                "SELECT * FROM checkpoint_snapshot_objects WHERE object_key=$1 FOR UPDATE",
                snapshot_object_key,
            )
            if existing_object is None:
                await conn.execute(
                    """INSERT INTO checkpoint_snapshot_objects(
                       object_key,payload,payload_digest,payload_size,schema_id,
                       published_at) VALUES($1,$2,$3,$4,$5,clock_timestamp())""",
                    snapshot_object_key,
                    snapshot_payload,
                    payload_digest,
                    len(snapshot_payload),
                    participant["state_schema"],
                )
            elif (
                bytes(existing_object["payload"]),
                existing_object["payload_digest"],
                existing_object["payload_size"],
                existing_object["schema_id"],
                existing_object["published_at"] is not None,
            ) != (
                snapshot_payload,
                payload_digest,
                len(snapshot_payload),
                participant["state_schema"],
                True,
            ):
                raise PostgresProtocolError("snapshot object publication drift")
            ack_identity = {
                "checkpoint_id": checkpoint_id,
                "participant_id": participant_id,
                "intent_digest": intent["intent_digest"],
                "barrier_vector": barrier_vector,
                "snapshot_object_key": snapshot_object_key,
                "snapshot_payload_digest": payload_digest,
                "mailbox_highwater": mailbox_highwater,
                "replay_tail_floor": replay_tail_floor,
                "output_barriers": output_barriers,
                "state_schema": participant["state_schema"],
                "state_version": state_version,
                "code_digest": participant["code_digest"],
            }
            ack_digest = digest(ack_identity)
            revision = await self._locked_revision(conn, revision_id)
            authority_ordinal = await self._authority_event(
                conn,
                revision,
                "PARTICIPANT_ACK",
                owner_epoch,
                {"participant_id": participant_id, "ack_digest": ack_digest},
                checkpoint_id=checkpoint_id,
            )
            await conn.execute(
                """INSERT INTO participant_checkpoint_acks(
                   checkpoint_id,participant_id,barrier_vector,snapshot_object_key,
                   snapshot_payload_digest,mailbox_highwater,replay_tail_floor,
                   output_barriers,state_schema,state_version,code_digest,ack_digest,
                   writer_epoch,ack_authority_ordinal)
                   VALUES($1,$2,$3::jsonb,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,
                          $11,$12,$13,$14)""",
                checkpoint_id,
                participant_id,
                canonical(barrier_vector),
                snapshot_object_key,
                payload_digest,
                mailbox_highwater,
                canonical(replay_tail_floor),
                canonical(output_barriers),
                participant["state_schema"],
                state_version,
                participant["code_digest"],
                ack_digest,
                owner_epoch,
                authority_ordinal,
            )
            for item in input_manifest:
                key = location_key(item["channel"], item["partition"])
                await conn.execute(
                    """INSERT INTO participant_scan_positions(
                       revision_id,participant_id,channel,partition_id,
                       scanned_log_through,last_checkpoint_id)
                       VALUES($1,$2,$3,$4,$5,$6)
                       ON CONFLICT(revision_id,participant_id,channel,partition_id)
                       DO UPDATE SET
                         scanned_log_through=GREATEST(
                           participant_scan_positions.scanned_log_through,
                           EXCLUDED.scanned_log_through),
                         last_checkpoint_id=EXCLUDED.last_checkpoint_id""",
                    revision_id,
                    participant_id,
                    item["channel"],
                    item["partition"],
                    barrier_vector[key]["barrier_log_sequence"],
                    checkpoint_id,
                )
            await self._assert_metadata_budget(conn, revision_id)
            hit(fault, "participant_before_commit")
        hit(fault, "participant_after_commit")
        return ack_digest

    async def complete_checkpoint(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        checkpoint_id: str,
        *,
        fault: FaultHook = None,
    ) -> str:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            intent = await conn.fetchrow(
                "SELECT * FROM checkpoint_intents_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            lifecycle = await conn.fetchrow(
                "SELECT * FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1 FOR UPDATE",
                checkpoint_id,
            )
            if (
                intent is None
                or lifecycle is None
                or intent["revision_id"] != revision_id
            ):
                raise PostgresProtocolError("unknown checkpoint intent")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM checkpoint_aborts_v2 WHERE checkpoint_id=$1)",
                checkpoint_id,
            ):
                raise PostgresProtocolError("aborted checkpoint cannot complete")
            observations = [
                dict(row)
                for row in await conn.fetch(
                    """SELECT channel,partition_id,barrier_log_sequence,
                       cut_data_sequence,intent_digest,retention_ref_id
                       FROM source_barrier_observations WHERE checkpoint_id=$1
                       ORDER BY channel,partition_id""",
                    checkpoint_id,
                )
            ]
            observed_manifest = [
                {"channel": row["channel"], "partition": row["partition_id"]}
                for row in observations
            ]
            if observed_manifest != decoded(intent["source_manifest"]):
                raise PostgresProtocolError("source barrier manifest is incomplete")
            if any(
                row["intent_digest"] != intent["intent_digest"] for row in observations
            ):
                raise PostgresProtocolError("source barrier intent drift")
            for row in observations:
                if not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM checkpoints
                       WHERE checkpoint_id=$1 AND channel=$2 AND partition_id=$3
                         AND cut_sequence=$4 AND status='SEALED')""",
                    row["retention_ref_id"],
                    row["channel"],
                    row["partition_id"],
                    row["cut_data_sequence"],
                ):
                    raise PostgresProtocolError("checkpoint replay-tail pin is missing")
            acks = [
                dict(row)
                for row in await conn.fetch(
                    """SELECT * FROM participant_checkpoint_acks
                       WHERE checkpoint_id=$1 ORDER BY participant_id""",
                    checkpoint_id,
                )
            ]
            expected_participants = [
                item["participant_id"]
                for item in decoded(intent["participant_manifest"])
            ]
            if [row["participant_id"] for row in acks] != expected_participants:
                raise PostgresProtocolError(
                    "participant acknowledgement manifest is incomplete"
                )
            observations_by_key = {
                location_key(row["channel"], row["partition_id"]): row
                for row in observations
            }
            for row in await conn.fetch(
                """SELECT channel,partition_id,
                log_sequence AS barrier_log_sequence,cut_data_sequence
                FROM partition_log_entries WHERE checkpoint_id=$1
                  AND revision_id=$2 AND barrier_role='OUTPUT' """,
                checkpoint_id,
                revision_id,
            ):
                observations_by_key[
                    location_key(row["channel"], row["partition_id"])
                ] = row
            participant_specs = {
                item["participant_id"]: item
                for item in decoded(intent["participant_manifest"])
            }
            for ack in acks:
                spec = participant_specs[ack["participant_id"]]
                expected_vector = {}
                for item in spec["inputs"]:
                    key = location_key(item["channel"], item["partition"])
                    observation = observations_by_key.get(key)
                    if observation is None:
                        raise PostgresProtocolError(
                            "participant barrier vector is incomplete"
                        )
                    expected_vector[key] = {
                        "cut_data_sequence": observation["cut_data_sequence"],
                        "barrier_log_sequence": observation["barrier_log_sequence"],
                    }
                expected_floor = {
                    key: value["cut_data_sequence"] + 1
                    for key, value in expected_vector.items()
                }
                expected_outputs = {
                    location_key(item["channel"], item["partition"])
                    for item in spec["outputs"]
                }
                output_barriers = decoded(ack["output_barriers"])
                if (
                    decoded(ack["barrier_vector"]) != expected_vector
                    or decoded(ack["replay_tail_floor"]) != expected_floor
                    or set(output_barriers) != expected_outputs
                    or (
                        ack["code_digest"],
                        ack["state_schema"],
                        ack["state_version"],
                    )
                    != (
                        spec["code_digest"],
                        spec["state_schema"],
                        spec["state_version"],
                    )
                ):
                    raise PostgresProtocolError(
                        "participant acknowledgement is incompatible"
                    )
                snapshot = await conn.fetchrow(
                    """SELECT * FROM checkpoint_snapshot_objects
                       WHERE object_key=$1 AND published_at IS NOT NULL""",
                    ack["snapshot_object_key"],
                )
                if snapshot is None:
                    raise PostgresProtocolError("participant snapshot is unavailable")
                snapshot_payload = bytes(snapshot["payload"])
                if (
                    hashlib.sha256(snapshot_payload).hexdigest(),
                    content_object_key(snapshot_payload, ack["state_schema"]),
                    snapshot["payload_size"],
                    snapshot["schema_id"],
                ) != (
                    ack["snapshot_payload_digest"],
                    ack["snapshot_object_key"],
                    len(snapshot_payload),
                    ack["state_schema"],
                ):
                    raise PostgresProtocolError("participant snapshot is corrupted")
                for key, output in output_barriers.items():
                    output_channel, output_partition = key.split("/", 1)
                    if not await conn.fetchval(
                        """SELECT EXISTS(SELECT 1 FROM partition_log_entries
                           WHERE channel=$1 AND partition_id=$2
                             AND log_sequence=$3 AND entry_kind='BARRIER'
                             AND barrier_role='OUTPUT' AND checkpoint_id=$4
                             AND cut_data_sequence=$5 AND intent_digest=$6)""",
                        output_channel,
                        output_partition,
                        output["barrier_log_sequence"],
                        checkpoint_id,
                        output["cut_data_sequence"],
                        intent["intent_digest"],
                    ):
                        raise PostgresProtocolError(
                            "output barrier authority is missing"
                        )
                ack_identity = {
                    "checkpoint_id": checkpoint_id,
                    "participant_id": ack["participant_id"],
                    "intent_digest": intent["intent_digest"],
                    "barrier_vector": expected_vector,
                    "snapshot_object_key": ack["snapshot_object_key"],
                    "snapshot_payload_digest": ack["snapshot_payload_digest"],
                    "mailbox_highwater": ack["mailbox_highwater"],
                    "replay_tail_floor": expected_floor,
                    "output_barriers": output_barriers,
                    "state_schema": ack["state_schema"],
                    "state_version": ack["state_version"],
                    "code_digest": ack["code_digest"],
                }
                if ack["ack_digest"] != digest(ack_identity):
                    raise PostgresProtocolError(
                        "participant acknowledgement digest is invalid"
                    )
                ack_event = await conn.fetchrow(
                    """SELECT * FROM coordinator_authority_events
                       WHERE revision_id=$1 AND authority_ordinal=$2""",
                    revision_id,
                    ack["ack_authority_ordinal"],
                )
                if ack_event is None or (
                    ack_event["event_type"],
                    ack_event["checkpoint_id"],
                    ack_event["writer_epoch"],
                    decoded(ack_event["payload"]),
                ) != (
                    "PARTICIPANT_ACK",
                    checkpoint_id,
                    ack["writer_epoch"],
                    {
                        "participant_id": ack["participant_id"],
                        "ack_digest": ack["ack_digest"],
                    },
                ):
                    raise PostgresProtocolError(
                        "participant acknowledgement authority is invalid"
                    )
            source_barriers_digest = digest(
                [
                    {
                        "channel": row["channel"],
                        "partition": row["partition_id"],
                        "barrier_log_sequence": row["barrier_log_sequence"],
                        "cut_data_sequence": row["cut_data_sequence"],
                        "intent_digest": row["intent_digest"],
                    }
                    for row in observations
                ]
            )
            participant_acks_digest = digest(
                [
                    {
                        "participant_id": row["participant_id"],
                        "ack_digest": row["ack_digest"],
                    }
                    for row in acks
                ]
            )
            completion_identity = {
                "checkpoint_id": checkpoint_id,
                "revision_id": revision_id,
                "intent_digest": intent["intent_digest"],
                "source_barriers_digest": source_barriers_digest,
                "participant_acks_digest": participant_acks_digest,
            }
            completion_digest = digest(completion_identity)
            existing = await conn.fetchrow(
                "SELECT * FROM checkpoint_completions_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            if existing is not None:
                if existing["completion_digest"] != completion_digest:
                    raise PostgresProtocolError("checkpoint completion identity drift")
                return completion_digest
            if lifecycle["status"] != "ALIGNED":
                raise PostgresProtocolError("checkpoint is not aligned")
            revision = await self._locked_revision(conn, revision_id)
            authority_ordinal = await self._authority_event(
                conn,
                revision,
                "COMPLETE",
                owner_epoch,
                {"completion_digest": completion_digest},
                checkpoint_id=checkpoint_id,
            )
            hit(fault, "complete_after_validation")
            await conn.execute(
                """INSERT INTO checkpoint_completions_v2(
                   checkpoint_id,completion_digest,source_barriers_digest,
                   participant_acks_digest,completion_authority_ordinal,writer_epoch)
                   VALUES($1,$2,$3,$4,$5,$6)""",
                checkpoint_id,
                completion_digest,
                source_barriers_digest,
                participant_acks_digest,
                authority_ordinal,
                owner_epoch,
            )
            await conn.execute(
                """UPDATE checkpoint_lifecycle_v2 SET status='COMPLETED',
                   last_authority_ordinal=$2 WHERE checkpoint_id=$1""",
                checkpoint_id,
                authority_ordinal,
            )
            await self._assert_metadata_budget(conn, revision_id)
            hit(fault, "complete_before_commit")
        hit(fault, "complete_after_commit")
        return completion_digest

    async def abort_checkpoint(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        checkpoint_id: str,
        reason: str,
    ) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            intent = await conn.fetchrow(
                "SELECT * FROM checkpoint_intents_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            lifecycle = await conn.fetchrow(
                "SELECT * FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1 FOR UPDATE",
                checkpoint_id,
            )
            if (
                intent is None
                or lifecycle is None
                or intent["revision_id"] != revision_id
            ):
                raise PostgresProtocolError("unknown checkpoint intent")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM checkpoint_completions_v2 WHERE checkpoint_id=$1)",
                checkpoint_id,
            ):
                raise PostgresProtocolError("completed checkpoint cannot abort")
            existing = await conn.fetchrow(
                "SELECT * FROM checkpoint_aborts_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            if existing is not None:
                if existing["reason"] != reason:
                    raise PostgresProtocolError("checkpoint abort identity drift")
                return
            if lifecycle["status"] not in ("STARTED", "ALIGNING", "ALIGNED"):
                raise PostgresProtocolError("checkpoint cannot abort")
            authority_ordinal = await self._authority_event(
                conn,
                revision,
                "ABORT",
                owner_epoch,
                {"reason": reason},
                checkpoint_id=checkpoint_id,
            )
            await conn.execute(
                """INSERT INTO checkpoint_aborts_v2(
                   checkpoint_id,reason,abort_authority_ordinal,writer_epoch)
                   VALUES($1,$2,$3,$4)""",
                checkpoint_id,
                reason,
                authority_ordinal,
                owner_epoch,
            )
            await conn.execute(
                """UPDATE checkpoint_lifecycle_v2 SET status='ABORTED',
                   last_authority_ordinal=$2 WHERE checkpoint_id=$1""",
                checkpoint_id,
                authority_ordinal,
            )
            await conn.execute(
                """UPDATE checkpoints SET status='RELEASED'
                   WHERE status='SEALED' AND checkpoint_id IN(
                     SELECT retention_ref_id FROM source_barrier_observations
                     WHERE checkpoint_id=$1 UNION ALL
                     SELECT retention_ref_id FROM participant_output_roots
                     WHERE checkpoint_id=$1)""",
                checkpoint_id,
            )
            await self._assert_metadata_budget(conn, revision_id)

    async def latest_completed(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
    ) -> Checkpoint | None:
        async with (
            self.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            revision = await conn.fetchrow(
                "SELECT * FROM deployment_revisions WHERE revision_id=$1",
                revision_id,
            )
            if revision is None:
                raise PostgresProtocolError("unknown deployment revision")
            self._check_graph(revision, graph_version, graph_digest)
            row = await conn.fetchrow(
                """SELECT i.checkpoint_id,i.checkpoint_ordinal,i.intent_digest,l.status
                   FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status='COMPLETED'
                   ORDER BY i.checkpoint_ordinal DESC LIMIT 1""",
                revision_id,
            )
            if row is None:
                return None
            return Checkpoint(
                row["checkpoint_id"],
                row["checkpoint_ordinal"],
                row["intent_digest"],
                row["status"],
            )

    async def begin_recovery(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        request_id: str,
        *,
        fault: FaultHook = None,
    ) -> RecoveryPlan:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            existing = await conn.fetchrow(
                "SELECT * FROM recovery_plans_v2 WHERE request_id=$1",
                request_id,
            )
            if existing is not None:
                await self._assert_global_recovery_scope(conn, existing["recovery_id"])
                if existing["revision_id"] != revision_id:
                    raise PostgresProtocolError("recovery request identity drift")
                return self._recovery_plan(existing)
            recovery_id = "recovery:" + digest(
                {
                    "revision_id": revision_id,
                    "request_id": request_id,
                    "fence_authority_ordinal": revision["next_authority_ordinal"],
                }
            )
            fence_ordinal = await self._authority_event(
                conn,
                revision,
                "RECOVERY_FENCE",
                owner_epoch,
                {"request_id": request_id},
                recovery_id=recovery_id,
            )
            selected = await conn.fetchrow(
                """SELECT i.*,c.completion_digest,c.completion_authority_ordinal
                   FROM checkpoint_intents_v2 i
                   JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                   JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status='COMPLETED'
                     AND c.completion_authority_ordinal<$2
                   ORDER BY i.checkpoint_ordinal DESC LIMIT 1""",
                revision_id,
                fence_ordinal,
            )
            if selected is None:
                raise PostgresProtocolError("no compatible completed checkpoint")
            observations = await self._checkpoint_boundaries(
                conn, selected["checkpoint_id"]
            )
            target_data_sequences: dict[str, int] = {}
            for observation in observations:
                part = await conn.fetchrow(
                    """SELECT next_sequence FROM partitions
                       WHERE channel=$1 AND partition_id=$2""",
                    observation["channel"],
                    observation["partition_id"],
                )
                if not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM checkpoints
                       WHERE checkpoint_id=$1 AND status='SEALED')""",
                    observation["retention_ref_id"],
                ):
                    raise PostgresProtocolError(
                        "completed checkpoint tail is unavailable"
                    )
                target_data_sequences[
                    location_key(observation["channel"], observation["partition_id"])
                ] = part["next_sequence"] - 1
            acks = await conn.fetch(
                """SELECT participant_id,snapshot_object_key
                   FROM participant_checkpoint_acks WHERE checkpoint_id=$1
                   ORDER BY participant_id""",
                selected["checkpoint_id"],
            )
            participant_snapshots = {
                row["participant_id"]: row["snapshot_object_key"] for row in acks
            }
            plan_identity = {
                "recovery_id": recovery_id,
                "request_id": request_id,
                "revision_id": revision_id,
                "checkpoint_id": selected["checkpoint_id"],
                "completion_digest": selected["completion_digest"],
                "fence_authority_ordinal": fence_ordinal,
                "target_data_sequences": target_data_sequences,
                "participant_snapshots": participant_snapshots,
            }
            plan_digest = digest(plan_identity)
            await conn.execute(
                """INSERT INTO recovery_plans_v2(
                   recovery_id,request_id,revision_id,checkpoint_id,
                   fence_authority_ordinal,target_data_sequences,
                   participant_snapshots,plan_digest,status,writer_epoch)
                   VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,'RESTORING',$9)""",
                recovery_id,
                request_id,
                revision_id,
                selected["checkpoint_id"],
                fence_ordinal,
                canonical(target_data_sequences),
                canonical(participant_snapshots),
                plan_digest,
                owner_epoch,
            )
            await self._assert_metadata_budget(conn, revision_id)
            hit(fault, "recovery_plan_before_commit")
        hit(fault, "recovery_plan_after_commit")
        row = await self._recovery_row(recovery_id)
        return self._recovery_plan(row)

    async def finish_recovery(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        recovery_id: str,
        *,
        fault: FaultHook = None,
    ) -> RecoveryPlan:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self._locked_revision(conn, revision_id)
            self._check_graph(revision, graph_version, graph_digest)
            await self._assert_owner(conn, revision_id, owner_id, owner_epoch)
            plan = await conn.fetchrow(
                "SELECT * FROM recovery_plans_v2 WHERE recovery_id=$1 FOR UPDATE",
                recovery_id,
            )
            if plan is None or plan["revision_id"] != revision_id:
                raise PostgresProtocolError("unknown recovery plan")
            await self._assert_global_recovery_scope(conn, recovery_id)
            if plan["status"] == "READY":
                return self._recovery_plan(plan)
            observations = await self._checkpoint_boundaries(
                conn, plan["checkpoint_id"]
            )
            targets = decoded(plan["target_data_sequences"])
            for observation in observations:
                key = location_key(observation["channel"], observation["partition_id"])
                start = observation["cut_data_sequence"] + 1
                target = targets[key]
                if not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM checkpoints
                       WHERE checkpoint_id=$1 AND channel=$2 AND partition_id=$3
                         AND cut_sequence=$4 AND status='SEALED')""",
                    observation["retention_ref_id"],
                    observation["channel"],
                    observation["partition_id"],
                    observation["cut_data_sequence"],
                ):
                    raise PostgresProtocolError("recovery replay-tail pin is missing")
                tail = await conn.fetch(
                    """SELECT r.sequence,r.object_key,r.payload_digest,r.payload_size,
                              r.schema_id,o.payload,o.payload_digest AS object_digest,
                              o.payload_size AS object_size,o.schema_id AS object_schema,
                              o.published_at
                       FROM records r JOIN objects o USING(object_key)
                       WHERE r.channel=$1 AND r.partition_id=$2
                         AND r.sequence BETWEEN $3 AND $4 ORDER BY r.sequence""",
                    observation["channel"],
                    observation["partition_id"],
                    start,
                    target,
                )
                if len(tail) != max(0, target - start + 1):
                    raise PostgresProtocolError("recovery replay tail is incomplete")
                for row in tail:
                    payload = bytes(row["payload"])
                    if row["published_at"] is None or (
                        hashlib.sha256(payload).hexdigest(),
                        len(payload),
                        row["payload_digest"],
                        row["payload_size"],
                        row["schema_id"],
                    ) != (
                        row["object_digest"],
                        row["object_size"],
                        row["object_digest"],
                        row["object_size"],
                        row["object_schema"],
                    ):
                        raise PostgresProtocolError(
                            "recovery replay-tail object is corrupted"
                        )
            for object_key in decoded(plan["participant_snapshots"]).values():
                if not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM checkpoint_snapshot_objects
                       WHERE object_key=$1 AND published_at IS NOT NULL)""",
                    object_key,
                ):
                    raise PostgresProtocolError("recovery snapshot is unavailable")
            authority_ordinal = await self._authority_event(
                conn,
                revision,
                "RECOVERY_READY",
                owner_epoch,
                {"plan_digest": plan["plan_digest"]},
                recovery_id=recovery_id,
            )
            await conn.execute(
                """UPDATE recovery_plans_v2 SET status='READY',
                   ready_at=clock_timestamp(),ready_authority_ordinal=$3,
                   ready_writer_epoch=$2
                   WHERE recovery_id=$1""",
                recovery_id,
                owner_epoch,
                authority_ordinal,
            )
            await self._assert_metadata_budget(conn, revision_id)
            assert authority_ordinal > plan["fence_authority_ordinal"]
            hit(fault, "recovery_ready_before_commit")
        hit(fault, "recovery_ready_after_commit")
        row = await self._recovery_row(recovery_id)
        return self._recovery_plan(row)

    async def _recovery_row(self, recovery_id: str) -> asyncpg.Record:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM recovery_plans_v2 WHERE recovery_id=$1", recovery_id
            )
        if row is None:
            raise PostgresProtocolError("unknown recovery plan")
        return row

    async def _assert_global_recovery_scope(self, conn, recovery_id):
        if await conn.fetchval(
            "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL"
        ) and await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM region_recovery_plans_v1 WHERE plan_id=$1)",
            recovery_id,
        ):
            raise PostgresProtocolError(
                "regional recovery requires region-wide replay and activation"
            )

    async def _checkpoint_boundaries(self, conn, checkpoint_id):
        return await conn.fetch(
            """SELECT channel,partition_id,cut_data_sequence,
                  retention_ref_id FROM source_barrier_observations
                WHERE checkpoint_id=$1
            UNION ALL SELECT l.channel,l.partition_id,l.cut_data_sequence,r.retention_ref_id
                FROM participant_output_roots r JOIN partition_log_entries l
                  ON l.checkpoint_id=r.checkpoint_id AND l.channel=r.channel
                    AND l.partition_id=r.partition_id AND l.barrier_role='OUTPUT'
                WHERE r.checkpoint_id=$1 ORDER BY channel,partition_id""",
            checkpoint_id,
        )

    @staticmethod
    def _recovery_plan(row: asyncpg.Record) -> RecoveryPlan:
        return RecoveryPlan(
            recovery_id=row["recovery_id"],
            checkpoint_id=row["checkpoint_id"],
            revision_id=row["revision_id"],
            target_data_sequences=dict(decoded(row["target_data_sequences"])),
            participant_snapshots=dict(decoded(row["participant_snapshots"])),
            plan_digest=row["plan_digest"],
            status=row["status"],
        )

    async def checkpoint_status(self, checkpoint_id: str) -> str:
        async with self.pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1",
                checkpoint_id,
            )
        if status is None:
            raise PostgresProtocolError("unknown checkpoint intent")
        return status

    async def ordered_log(
        self,
        channel: str,
        partition: str,
    ) -> list[dict[str, Any]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM partition_log_entries WHERE channel=$1
                   AND partition_id=$2 ORDER BY log_sequence""",
                channel,
                partition,
            )
        return [dict(row) for row in rows]
