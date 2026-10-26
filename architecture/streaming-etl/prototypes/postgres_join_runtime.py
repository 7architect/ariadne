"""Live aligned multi-input generator prototype on the PostgreSQL data plane.

The bounded prototype serializes a participant mailbox. Client effects are
outside its transaction; a process loss can execute client Python again.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import uuid
from dataclasses import dataclass
from typing import Any

from postgres_barrier_coordinator import (
    PostgresBarrierCoordinator,
    canonical,
    decoded,
    digest,
    hit,
    location_key,
)
from postgres_checkpoint_runtime import PostgresCheckpointRuntime, SharedStateContract
from postgres_generator_runtime import OutputSpec
from postgres_join_retention import JoinReplayHorizonExpired
from postgres_stream_v2 import PostgresProtocolError, StaleOwner, content_object_key


class JoinPressure(PostgresProtocolError):
    pass


@dataclass(frozen=True)
class JoinContext:
    participant_id: str
    input_key: str
    input_sequence: int
    managed_state: dict[str, Any]


class PostgresJoinRuntime:
    def __init__(
        self, coordinator: PostgresBarrierCoordinator, revision, participant: str
    ):
        self.coordinator = coordinator
        self.stream = coordinator.stream
        self.pool = coordinator.pool
        self.revision = revision
        self.participant = participant
        self.producer_id = "join-producer:" + digest(
            [revision.revision_id, participant]
        )
        specs = [
            x
            for x in revision.participant_manifest
            if x["participant_id"] == participant
        ]
        if len(specs) != 1 or len(specs[0]["outputs"]) != 1:
            raise PostgresProtocolError("join requires one declared participant output")
        self.spec = specs[0]

    async def install(
        self,
        *,
        initial_state=None,
        max_buffer_records=100,
        max_buffer_bytes=1_000_000,
        max_mailbox_records=10000,
        max_metadata_bytes=10_000_000,
    ):
        if (
            min(
                max_buffer_records,
                max_buffer_bytes,
                max_mailbox_records,
                max_metadata_bytes,
            )
            <= 0
        ):
            raise ValueError("join limits must be positive")
        state = {} if initial_state is None else initial_state
        ddl = """
        CREATE TABLE IF NOT EXISTS join_workers (
            revision_id text NOT NULL REFERENCES deployment_revisions,
            participant_id text NOT NULL,
            config jsonb NOT NULL, config_digest text NOT NULL,
            state jsonb NOT NULL, next_arrival bigint NOT NULL DEFAULT 1,
            next_apply bigint NOT NULL DEFAULT 1,
            owner_id text, owner_epoch bigint NOT NULL DEFAULT 0,
            lease_expires_at timestamptz,
            phase text NOT NULL DEFAULT 'LIVE' CHECK(phase IN('LIVE','RESTORING')),
            active_checkpoint text, last_aligned_ordinal bigint NOT NULL DEFAULT 0,
            PRIMARY KEY(revision_id,participant_id)
        );
        CREATE TABLE IF NOT EXISTS join_inputs_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            channel text NOT NULL, partition_id text NOT NULL,
            scanned bigint NOT NULL DEFAULT 0,
            completed bigint NOT NULL DEFAULT 0,
            blocked_checkpoint text,
            retention_ref text NOT NULL UNIQUE,
            PRIMARY KEY(revision_id,participant_id,channel,partition_id),
            FOREIGN KEY(revision_id,participant_id) REFERENCES join_workers
        );
        CREATE TABLE IF NOT EXISTS join_receipts_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            arrival_ordinal bigint NOT NULL,
            channel text NOT NULL, partition_id text NOT NULL,
            log_sequence bigint NOT NULL, data_sequence bigint NOT NULL,
            record_id text NOT NULL, object_key text NOT NULL,
            payload_digest text NOT NULL, payload_size bigint NOT NULL,
            blocked_checkpoint text,
            receipt_digest text NOT NULL,
            PRIMARY KEY(revision_id,participant_id,arrival_ordinal),
            UNIQUE(revision_id,participant_id,channel,partition_id,data_sequence),
            FOREIGN KEY(channel,partition_id,log_sequence)
              REFERENCES partition_log_entries(channel,partition_id,log_sequence)
        );
        CREATE TABLE IF NOT EXISTS join_invocation_history_v3 (
            token text PRIMARY KEY,revision_id text NOT NULL,participant_id text NOT NULL,
            arrival_ordinal bigint NOT NULL,writer_epoch bigint NOT NULL,
            expected_apply bigint NOT NULL,
            FOREIGN KEY(revision_id,participant_id,arrival_ordinal) REFERENCES join_receipts_v3
        );
        CREATE TABLE IF NOT EXISTS join_turns_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            apply_ordinal bigint NOT NULL, arrival_ordinal bigint NOT NULL,
            before_state jsonb NOT NULL, after_state jsonb NOT NULL,
            terminal text NOT NULL CHECK(terminal IN('YIELD','RETURN')),
            output_record_id text, output_digest text,
            writer_epoch bigint NOT NULL, turn_digest text NOT NULL,
            invocation_token text NOT NULL REFERENCES join_invocation_history_v3(token),
            PRIMARY KEY(revision_id,participant_id,apply_ordinal),
            UNIQUE(revision_id,participant_id,arrival_ordinal),
            FOREIGN KEY(revision_id,participant_id,arrival_ordinal)
              REFERENCES join_receipts_v3
        );
        CREATE TABLE IF NOT EXISTS join_barriers_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            checkpoint_id text NOT NULL REFERENCES checkpoint_intents_v2,
            channel text NOT NULL, partition_id text NOT NULL,
            log_sequence bigint NOT NULL, cut_sequence bigint NOT NULL,
            PRIMARY KEY(revision_id,participant_id,checkpoint_id,channel,partition_id),
            FOREIGN KEY(channel,partition_id,log_sequence)
              REFERENCES partition_log_entries(channel,partition_id,log_sequence)
        );
        CREATE TABLE IF NOT EXISTS join_alignments_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            checkpoint_id text NOT NULL, mailbox_highwater bigint NOT NULL,
            ack_digest text NOT NULL, state_digest text NOT NULL,
            PRIMARY KEY(revision_id,participant_id,checkpoint_id),
            FOREIGN KEY(checkpoint_id,participant_id)
              REFERENCES participant_checkpoint_acks
        );
        CREATE TABLE IF NOT EXISTS join_recoveries_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            recovery_id text NOT NULL REFERENCES recovery_plans_v2,
            target_apply bigint NOT NULL, replayed_through bigint NOT NULL,
            state jsonb NOT NULL, status text NOT NULL CHECK(status IN('RESTORING','READY')),
            PRIMARY KEY(revision_id,participant_id,recovery_id)
        );
        CREATE TABLE IF NOT EXISTS join_invocations_v3 (
            revision_id text NOT NULL, participant_id text NOT NULL,
            token text NOT NULL UNIQUE, arrival_ordinal bigint NOT NULL,
            writer_epoch bigint NOT NULL, expected_apply bigint NOT NULL,
            PRIMARY KEY(revision_id,participant_id),
            FOREIGN KEY(revision_id,participant_id,arrival_ordinal)
              REFERENCES join_receipts_v3
        );
        CREATE OR REPLACE FUNCTION join_pin_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.ref_kind='JOIN_INPUT' AND
              (NEW.status IS DISTINCT FROM OLD.status OR
               NEW.sequence IS DISTINCT FROM OLD.sequence OR
               NEW.channel IS DISTINCT FROM OLD.channel OR
               NEW.partition_id IS DISTINCT FROM OLD.partition_id)
            THEN RAISE EXCEPTION 'join replay root is immutable'; END IF;
            RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS join_pin_guard ON durable_refs;
        CREATE TRIGGER join_pin_guard BEFORE UPDATE ON durable_refs
          FOR EACH ROW EXECUTE FUNCTION join_pin_guard();
        CREATE OR REPLACE FUNCTION join_worker_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE t join_turns_v3%ROWTYPE;
        BEGIN
            IF (NEW.revision_id,NEW.participant_id,NEW.config,NEW.config_digest)
              IS DISTINCT FROM (OLD.revision_id,OLD.participant_id,OLD.config,OLD.config_digest)
            THEN RAISE EXCEPTION 'join configuration is immutable'; END IF;
            IF NEW.next_arrival IS DISTINCT FROM OLD.next_arrival AND
               (NEW.next_arrival<>OLD.next_arrival+1 OR NOT EXISTS(
                 SELECT 1 FROM join_receipts_v3 r WHERE r.revision_id=OLD.revision_id
                 AND r.participant_id=OLD.participant_id AND r.arrival_ordinal=OLD.next_arrival))
            THEN RAISE EXCEPTION 'join arrival lacks receipt'; END IF;
            IF NEW.next_apply IS DISTINCT FROM OLD.next_apply OR NEW.state IS DISTINCT FROM OLD.state THEN
                SELECT * INTO t FROM join_turns_v3 WHERE revision_id=OLD.revision_id
                  AND participant_id=OLD.participant_id AND apply_ordinal=OLD.next_apply;
                IF NEW.next_apply<>OLD.next_apply+1 OR t.before_state IS DISTINCT FROM OLD.state
                   OR t.after_state IS DISTINCT FROM NEW.state OR t.writer_epoch IS DISTINCT FROM OLD.owner_epoch
                THEN RAISE EXCEPTION 'join state lacks terminal witness'; END IF;
            END IF;
            IF NEW.owner_epoch<OLD.owner_epoch OR NEW.owner_epoch>OLD.owner_epoch+1
            THEN RAISE EXCEPTION 'join epoch regression or gap'; END IF;
            IF NEW.phase='LIVE' AND OLD.phase='RESTORING' AND NOT EXISTS(
              SELECT 1 FROM join_recoveries_v3 r WHERE r.revision_id=NEW.revision_id
                AND r.participant_id=NEW.participant_id AND r.status='READY'
                AND r.target_apply=NEW.next_apply-1 AND r.state=NEW.state)
            THEN RAISE EXCEPTION 'join READY lacks restored terminal prefix'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_insert_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
          w join_workers%ROWTYPE;
          i join_inputs_v3%ROWTYPE;
          l partition_log_entries%ROWTYPE;
          r records%ROWTYPE;
          claim join_invocations_v3%ROWTYPE;
          ack participant_checkpoint_acks%ROWTYPE;
          cp checkpoint_intents_v2%ROWTYPE;
        BEGIN
          SELECT * INTO w FROM join_workers WHERE revision_id=NEW.revision_id
            AND participant_id=NEW.participant_id FOR UPDATE;
          IF w.participant_id IS NULL THEN RAISE EXCEPTION 'unknown join participant'; END IF;
          IF TG_TABLE_NAME='join_receipts_v3' OR TG_TABLE_NAME='join_barriers_v3' THEN
            SELECT * INTO i FROM join_inputs_v3 WHERE revision_id=NEW.revision_id
              AND participant_id=NEW.participant_id AND channel=NEW.channel AND partition_id=NEW.partition_id;
            SELECT * INTO l FROM partition_log_entries WHERE channel=NEW.channel
              AND partition_id=NEW.partition_id AND log_sequence=NEW.log_sequence;
            IF i.channel IS NULL OR NEW.log_sequence<>i.scanned+1
            THEN RAISE EXCEPTION 'join receipt/barrier input binding mismatch'; END IF;
          END IF;
          IF TG_TABLE_NAME='join_receipts_v3' THEN
            SELECT * INTO r FROM records WHERE channel=NEW.channel AND partition_id=NEW.partition_id
              AND sequence=NEW.data_sequence;
            IF w.phase<>'LIVE' OR w.lease_expires_at<=clock_timestamp()
              OR NEW.arrival_ordinal<>w.next_arrival OR l.entry_kind IS DISTINCT FROM 'DATA'
              OR (l.data_sequence,l.record_id) IS DISTINCT FROM (NEW.data_sequence,NEW.record_id)
              OR (r.record_id,r.object_key,r.payload_digest,r.payload_size)
                 IS DISTINCT FROM (NEW.record_id,NEW.object_key,NEW.payload_digest,NEW.payload_size)
              OR NEW.blocked_checkpoint IS DISTINCT FROM i.blocked_checkpoint
            THEN RAISE EXCEPTION 'join DATA receipt lacks source authority'; END IF;
          ELSIF TG_TABLE_NAME='join_turns_v3' THEN
            SELECT * INTO claim FROM join_invocations_v3 WHERE revision_id=NEW.revision_id
              AND participant_id=NEW.participant_id;
            IF w.phase<>'LIVE' OR w.lease_expires_at<=clock_timestamp()
              OR NEW.invocation_token IS DISTINCT FROM claim.token
              OR NEW.arrival_ordinal IS DISTINCT FROM claim.arrival_ordinal
              OR NEW.apply_ordinal IS DISTINCT FROM claim.expected_apply
              OR NEW.writer_epoch IS DISTINCT FROM claim.writer_epoch
              OR NEW.writer_epoch IS DISTINCT FROM w.owner_epoch
              OR NEW.apply_ordinal<>w.next_apply OR NEW.before_state IS DISTINCT FROM w.state
              OR NOT EXISTS(SELECT 1 FROM join_invocation_history_v3 h
                WHERE h.token=NEW.invocation_token AND h.revision_id=NEW.revision_id
                  AND h.participant_id=NEW.participant_id AND h.arrival_ordinal=NEW.arrival_ordinal
                  AND h.writer_epoch=NEW.writer_epoch AND h.expected_apply=NEW.apply_ordinal)
            THEN RAISE EXCEPTION 'join terminal lacks active fenced invocation'; END IF;
            IF NEW.terminal='YIELD' THEN
              SELECT * INTO r FROM records WHERE record_id=NEW.output_record_id;
              IF r.record_id IS NULL OR r.payload_digest IS DISTINCT FROM NEW.output_digest
                OR r.record_id IS DISTINCT FROM 'join-output:'||r.lineage_digest
                OR NOT EXISTS(SELECT 1 FROM append_attempts a WHERE a.attempt_id='join-append:'||r.lineage_digest
                  AND a.record_id=r.record_id AND a.status='APPENDED')
              THEN RAISE EXCEPTION 'join terminal lacks output witness'; END IF;
            ELSIF NEW.output_record_id IS NOT NULL OR NEW.output_digest IS NOT NULL THEN
              RAISE EXCEPTION 'join RETURN cannot have output';
            END IF;
          ELSIF TG_TABLE_NAME='join_barriers_v3' THEN
            SELECT * INTO cp FROM checkpoint_intents_v2 WHERE checkpoint_id=NEW.checkpoint_id;
            IF cp.revision_id IS DISTINCT FROM NEW.revision_id OR l.entry_kind IS DISTINCT FROM 'BARRIER'
              OR l.checkpoint_id IS DISTINCT FROM NEW.checkpoint_id OR l.intent_digest IS DISTINCT FROM cp.intent_digest
              OR l.cut_data_sequence IS DISTINCT FROM NEW.cut_sequence OR i.completed<>NEW.cut_sequence
              OR cp.checkpoint_ordinal<=w.last_aligned_ordinal
              OR (w.active_checkpoint IS NOT NULL AND w.active_checkpoint<>NEW.checkpoint_id)
              OR NOT (EXISTS(SELECT 1 FROM source_barrier_observations b
                  JOIN revision_edges e ON e.revision_id=NEW.revision_id AND e.target_id=NEW.participant_id
                    AND e.channel=b.channel AND e.partition_id=b.partition_id AND e.producer_id='SOURCE'
                  WHERE b.checkpoint_id=NEW.checkpoint_id AND b.channel=NEW.channel
                    AND b.partition_id=NEW.partition_id AND b.barrier_log_sequence=NEW.log_sequence)
                OR EXISTS(SELECT 1 FROM participant_checkpoint_acks a JOIN revision_edges e
                  ON e.revision_id=NEW.revision_id AND e.producer_id=a.participant_id
                    AND e.target_id=NEW.participant_id AND e.channel=NEW.channel AND e.partition_id=NEW.partition_id
                  WHERE a.checkpoint_id=NEW.checkpoint_id AND
                    (a.output_barriers->(NEW.channel||'/'||NEW.partition_id)->>'barrier_log_sequence')::bigint=NEW.log_sequence))
            THEN RAISE EXCEPTION 'join barrier lacks input checkpoint authority'; END IF;
          ELSIF TG_TABLE_NAME='join_alignments_v3' THEN
            SELECT * INTO ack FROM participant_checkpoint_acks WHERE checkpoint_id=NEW.checkpoint_id
              AND participant_id=NEW.participant_id;
            IF ack.ack_digest IS DISTINCT FROM NEW.ack_digest
              OR ack.mailbox_highwater IS DISTINCT FROM NEW.mailbox_highwater
              OR NEW.mailbox_highwater<>w.next_apply-1
              OR (SELECT count(*) FROM join_barriers_v3 b WHERE b.revision_id=NEW.revision_id
                AND b.participant_id=NEW.participant_id AND b.checkpoint_id=NEW.checkpoint_id)
                <>jsonb_array_length(w.config->'spec'->'inputs')
              OR NOT EXISTS(SELECT 1 FROM checkpoint_snapshot_objects o WHERE o.object_key=ack.snapshot_object_key
                 AND convert_from(o.payload,'UTF8')::jsonb=w.state)
            THEN RAISE EXCEPTION 'join alignment lacks snapshot authority'; END IF;
          ELSIF TG_TABLE_NAME='join_invocation_history_v3' THEN
            IF w.phase<>'LIVE' OR w.lease_expires_at<=clock_timestamp()
              OR NEW.writer_epoch<>w.owner_epoch OR NEW.expected_apply<>w.next_apply
              OR EXISTS(SELECT 1 FROM join_turns_v3 t WHERE t.revision_id=NEW.revision_id
                  AND t.participant_id=NEW.participant_id AND t.arrival_ordinal=NEW.arrival_ordinal)
            THEN RAISE EXCEPTION 'join invocation lacks pending DATA authority'; END IF;
          ELSIF TG_TABLE_NAME='join_recoveries_v3' THEN
            SELECT * INTO ack FROM participant_checkpoint_acks WHERE participant_id=NEW.participant_id
              AND checkpoint_id=(SELECT checkpoint_id FROM recovery_plans_v2 WHERE recovery_id=NEW.recovery_id
                AND revision_id=NEW.revision_id);
            IF ack.participant_id IS NULL OR NEW.status<>'RESTORING'
              OR NEW.target_apply<>w.next_apply-1 OR NEW.replayed_through<>ack.mailbox_highwater
              OR NOT EXISTS(SELECT 1 FROM checkpoint_snapshot_objects o
                WHERE o.object_key=ack.snapshot_object_key AND convert_from(o.payload,'UTF8')::jsonb=NEW.state)
            THEN RAISE EXCEPTION 'join recovery lacks frozen snapshot authority'; END IF;
          END IF;
          RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_input_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (NEW.revision_id,NEW.participant_id,NEW.channel,NEW.partition_id,NEW.retention_ref)
              IS DISTINCT FROM (OLD.revision_id,OLD.participant_id,OLD.channel,OLD.partition_id,OLD.retention_ref)
            THEN RAISE EXCEPTION 'join input binding is immutable'; END IF;
            IF NEW.completed IS DISTINCT FROM OLD.completed AND
              (NEW.completed<>OLD.completed+1 OR NOT EXISTS(
                SELECT 1 FROM join_turns_v3 t JOIN join_receipts_v3 r
                  USING(revision_id,participant_id,arrival_ordinal)
                WHERE r.revision_id=NEW.revision_id AND r.participant_id=NEW.participant_id
                  AND r.channel=NEW.channel AND r.partition_id=NEW.partition_id
                  AND r.data_sequence=NEW.completed))
            THEN RAISE EXCEPTION 'join cursor lacks terminal witness'; END IF;
            IF NEW.scanned IS DISTINCT FROM OLD.scanned AND (NEW.scanned<>OLD.scanned+1 OR NOT EXISTS(
              SELECT 1 FROM partition_log_entries l WHERE l.channel=NEW.channel
                AND l.partition_id=NEW.partition_id AND l.log_sequence=NEW.scanned AND (
                  EXISTS(SELECT 1 FROM join_receipts_v3 r WHERE r.revision_id=NEW.revision_id
                    AND r.participant_id=NEW.participant_id AND r.channel=l.channel
                    AND r.partition_id=l.partition_id AND r.log_sequence=l.log_sequence)
                  OR EXISTS(SELECT 1 FROM join_barriers_v3 b WHERE b.revision_id=NEW.revision_id
                    AND b.participant_id=NEW.participant_id AND b.channel=l.channel
                    AND b.partition_id=l.partition_id AND b.log_sequence=l.log_sequence)
                  OR EXISTS(SELECT 1 FROM checkpoint_aborts_v2 a WHERE a.checkpoint_id=l.checkpoint_id))))
            THEN RAISE EXCEPTION 'join scan lacks log witness'; END IF;
            RETURN NEW;
        END $$;
        CREATE OR REPLACE FUNCTION join_recovery_guard() RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE t join_turns_v3%ROWTYPE;
        BEGIN
            IF (NEW.revision_id,NEW.participant_id,NEW.recovery_id,NEW.target_apply)
              IS DISTINCT FROM (OLD.revision_id,OLD.participant_id,OLD.recovery_id,OLD.target_apply)
            THEN RAISE EXCEPTION 'join recovery target is immutable'; END IF;
            IF OLD.status='READY' THEN RAISE EXCEPTION 'join recovery already READY'; END IF;
            IF NEW.replayed_through IS DISTINCT FROM OLD.replayed_through THEN
              SELECT * INTO t FROM join_turns_v3 WHERE revision_id=OLD.revision_id
                AND participant_id=OLD.participant_id AND apply_ordinal=OLD.replayed_through+1;
              IF NEW.replayed_through<>OLD.replayed_through+1
                OR t.before_state IS DISTINCT FROM OLD.state OR t.after_state IS DISTINCT FROM NEW.state
              THEN RAISE EXCEPTION 'join replay lacks exact turn'; END IF;
            ELSIF NEW.state IS DISTINCT FROM OLD.state THEN
              RAISE EXCEPTION 'join replay state changed without turn';
            END IF;
            IF NEW.status='READY' AND NEW.replayed_through<>NEW.target_apply
            THEN RAISE EXCEPTION 'join replay truncated'; END IF;
            RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS worker_guard ON join_workers;
        CREATE TRIGGER worker_guard BEFORE UPDATE ON join_workers FOR EACH ROW EXECUTE FUNCTION join_worker_guard();
        DROP TRIGGER IF EXISTS input_guard ON join_inputs_v3;
        CREATE TRIGGER input_guard BEFORE UPDATE ON join_inputs_v3 FOR EACH ROW EXECUTE FUNCTION join_input_guard();
        DROP TRIGGER IF EXISTS recovery_guard ON join_recoveries_v3;
        CREATE TRIGGER recovery_guard BEFORE UPDATE ON join_recoveries_v3 FOR EACH ROW EXECUTE FUNCTION join_recovery_guard();
        """
        config = {
            "initial_state": state,
            "spec": self.spec,
            "max_buffer_records": max_buffer_records,
            "max_buffer_bytes": max_buffer_bytes,
            "max_mailbox_records": max_mailbox_records,
            "max_metadata_bytes": max_metadata_bytes,
        }
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(ddl)
            revision = await self.coordinator._locked_revision(
                conn, self.revision.revision_id
            )
            self.coordinator._check_graph(
                revision, self.revision.graph_version, self.revision.graph_digest
            )
            if (
                decoded(revision["participant_manifest"])
                != self.revision.participant_manifest
            ):
                raise PostgresProtocolError(
                    "join revision manifest lacks durable authority"
                )
            # Deployment-time locks prevent a compaction race with root creation.
            for item in self.spec["inputs"]:
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"{self.stream.schema}|{item['channel']}|{item['partition']}",
                )
                part = await conn.fetchrow(
                    "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                    item["channel"],
                    item["partition"],
                )
                if part is None:
                    raise PostgresProtocolError("join input partition missing")
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM compaction_tombstones WHERE channel=$1 AND partition_id=$2)",
                    item["channel"],
                    item["partition"],
                ):
                    raise PostgresProtocolError(
                        "join cannot attach after input prefix was reclaimed"
                    )
            await conn.execute(
                """INSERT INTO join_workers(revision_id,participant_id,
                config,config_digest,state) VALUES($1,$2,$3::jsonb,$4,$5::jsonb)
                ON CONFLICT DO NOTHING""",
                self.revision.revision_id,
                self.participant,
                canonical(config),
                digest(config),
                canonical(state),
            )
            worker = await self._worker(conn)
            if worker["config_digest"] != digest(config):
                raise PostgresProtocolError("join configuration drift")
            for item in self.spec["inputs"]:
                ref = digest([self.revision.revision_id, self.participant, item])
                await conn.execute(
                    """INSERT INTO durable_refs VALUES(
                    'JOIN_INPUT',$1,$2,$3,1,'ACTIVE') ON CONFLICT DO NOTHING""",
                    ref,
                    item["channel"],
                    item["partition"],
                )
                await conn.execute(
                    """INSERT INTO join_inputs_v3(revision_id,
                    participant_id,channel,partition_id,retention_ref)
                    VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING""",
                    self.revision.revision_id,
                    self.participant,
                    item["channel"],
                    item["partition"],
                    ref,
                )
            for table in (
                "join_receipts_v3",
                "join_turns_v3",
                "join_barriers_v3",
                "join_alignments_v3",
                "join_invocation_history_v3",
            ):
                await conn.execute(f"""DROP TRIGGER IF EXISTS immutable ON {table};
                    CREATE TRIGGER immutable BEFORE UPDATE OR DELETE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION coordinator_reject_immutable_mutation()""")
                await conn.execute(f"""DROP TRIGGER IF EXISTS authority_insert ON {table};
                    CREATE TRIGGER authority_insert BEFORE INSERT ON {table}
                    FOR EACH ROW EXECUTE FUNCTION join_insert_guard()""")
            await conn.execute("""DROP TRIGGER IF EXISTS authority_insert ON join_recoveries_v3;
                CREATE TRIGGER authority_insert BEFORE INSERT ON join_recoveries_v3
                FOR EACH ROW EXECUTE FUNCTION join_insert_guard();
                DROP TRIGGER IF EXISTS immutable_delete ON join_recoveries_v3;
                CREATE TRIGGER immutable_delete BEFORE DELETE ON join_recoveries_v3
                FOR EACH ROW EXECUTE FUNCTION coordinator_reject_immutable_mutation()""")
            await self._budget(conn, worker)
        output = self.spec["outputs"][0]
        await self.stream.register_producer(
            output["channel"], output["partition"], self.producer_id
        )

    async def _worker(self, conn):
        row = await conn.fetchrow(
            """SELECT * FROM join_workers
            WHERE revision_id=$1 AND participant_id=$2 FOR UPDATE""",
            self.revision.revision_id,
            self.participant,
        )
        if row is None:
            raise PostgresProtocolError("join is not installed")
        return row

    async def _owner(self, conn, owner, epoch):
        row = await self._worker(conn)
        if decoded(row["config"])["spec"] != self.spec:
            raise PostgresProtocolError("join runtime contract drift")
        if (row["owner_id"], row["owner_epoch"]) != (
            owner,
            epoch,
        ) or not await conn.fetchval(
            "SELECT $1::timestamptz > clock_timestamp()", row["lease_expires_at"]
        ):
            raise StaleOwner("join worker fenced")
        return row

    async def claim(self, owner, *, lease_seconds=30):
        if lease_seconds <= 0:
            raise ValueError("positive lease required")
        async with self.pool.acquire() as conn, conn.transaction():
            row = await self._worker(conn)
            live = row["lease_expires_at"] is not None and await conn.fetchval(
                "SELECT $1::timestamptz>clock_timestamp()", row["lease_expires_at"]
            )
            if live and row["owner_id"] != owner:
                raise StaleOwner("join has an active owner")
            epoch = row["owner_epoch"] if live else row["owner_epoch"] + 1
            if not live:
                await conn.execute(
                    """DELETE FROM join_invocations_v3
                    WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    self.participant,
                )
            await conn.execute(
                """UPDATE join_workers SET owner_id=$3,owner_epoch=$4,
                lease_expires_at=clock_timestamp()+$5*interval '1 second'
                WHERE revision_id=$1 AND participant_id=$2""",
                self.revision.revision_id,
                self.participant,
                owner,
                epoch,
                lease_seconds,
            )
            return epoch

    async def _budget(self, conn, worker):
        config = decoded(worker["config"])
        pending = await conn.fetchrow(
            """SELECT count(*) AS n,
            COALESCE(sum(r.payload_size),0) AS bytes FROM join_receipts_v3 r
            WHERE r.revision_id=$1 AND r.participant_id=$2 AND NOT EXISTS(
              SELECT 1 FROM join_turns_v3 t WHERE t.revision_id=r.revision_id
                AND t.participant_id=r.participant_id AND t.arrival_ordinal=r.arrival_ordinal)
              AND r.blocked_checkpoint IS NOT NULL""",
            self.revision.revision_id,
            self.participant,
        )
        total = await conn.fetchval(
            """SELECT count(*) FROM join_receipts_v3
            WHERE revision_id=$1 AND participant_id=$2""",
            self.revision.revision_id,
            self.participant,
        )
        metadata = 0
        for table in (
            "join_receipts_v3",
            "join_turns_v3",
            "join_barriers_v3",
            "join_alignments_v3",
            "join_recoveries_v3",
            "join_inputs_v3",
            "join_workers",
            "join_invocations_v3",
            "join_invocation_history_v3",
        ):
            metadata += await conn.fetchval(
                f"""SELECT COALESCE(sum(octet_length(row_to_json(x)::text)),0)
                FROM {table} x WHERE revision_id=$1 AND participant_id=$2""",
                self.revision.revision_id,
                self.participant,
            )
        if (
            pending["n"] > config["max_buffer_records"]
            or pending["bytes"] > config["max_buffer_bytes"]
        ):
            raise JoinPressure("join post-barrier buffer capacity exhausted")
        if (
            total > config["max_mailbox_records"]
            or metadata > config["max_metadata_bytes"]
        ):
            raise JoinPressure("join retained mailbox metadata capacity exhausted")

    async def step(
        self,
        input_key,
        task,
        owner,
        epoch,
        *,
        coordinator_owner,
        coordinator_epoch,
        fault=None,
    ):
        """Read the next durable log entry; blocked DATA is buffered, never applied."""
        if (
            not (inspect.isgeneratorfunction(task) or inspect.isasyncgenfunction(task))
            or getattr(task, "__ariadne_stream_protocol__", None) != "yield-v2"
        ):
            raise PostgresProtocolError("join requires declared generator")
        invocation = None
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(
                conn, self.revision.revision_id
            )
            self.coordinator._check_graph(
                revision, self.revision.graph_version, self.revision.graph_digest
            )
            worker = await self._owner(conn, owner, epoch)
            if worker["phase"] != "LIVE":
                raise PostgresProtocolError("join is restoring")
            if await conn.fetchval(
                """SELECT EXISTS(SELECT 1 FROM join_invocations_v3
                WHERE revision_id=$1 AND participant_id=$2)""",
                self.revision.revision_id,
                self.participant,
            ):
                raise PostgresProtocolError("join client invocation is active")
            if worker["active_checkpoint"] is not None and await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM checkpoint_aborts_v2 WHERE checkpoint_id=$1)",
                worker["active_checkpoint"],
            ):
                await conn.execute(
                    """UPDATE join_inputs_v3 SET blocked_checkpoint=NULL
                    WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    self.participant,
                )
                await conn.execute(
                    """UPDATE join_workers SET active_checkpoint=NULL
                    WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    self.participant,
                )
                worker = await self._worker(conn)
            inputs = await conn.fetch(
                """SELECT * FROM join_inputs_v3
                WHERE revision_id=$1 AND participant_id=$2 ORDER BY channel,partition_id""",
                self.revision.revision_id,
                self.participant,
            )
            selected = next(
                (
                    x
                    for x in inputs
                    if location_key(x["channel"], x["partition_id"]) == input_key
                ),
                None,
            )
            if selected is None:
                raise PostgresProtocolError("undeclared join input")
            # Once locally aligned, drain previously admitted buffered records
            # before a new arrival can alter persisted mailbox order.
            pending = await conn.fetchrow(
                """SELECT r.* FROM join_receipts_v3 r
                WHERE r.revision_id=$1 AND r.participant_id=$2 AND NOT EXISTS(
                  SELECT 1 FROM join_turns_v3 t WHERE t.revision_id=r.revision_id
                   AND t.participant_id=r.participant_id AND t.arrival_ordinal=r.arrival_ordinal)
                  AND (r.blocked_checkpoint IS NULL OR $3::text IS NULL)
                ORDER BY arrival_ordinal LIMIT 1""",
                self.revision.revision_id,
                self.participant,
                worker["active_checkpoint"],
            )
            if pending is not None and (
                worker["active_checkpoint"] is None
                or pending["blocked_checkpoint"] is None
            ):
                invocation = await self._claim_invocation(conn, worker, pending)
                result = "DRAINED"
            else:
                entry = await conn.fetchrow(
                    """SELECT * FROM partition_log_entries
                    WHERE channel=$1 AND partition_id=$2 AND log_sequence=$3""",
                    selected["channel"],
                    selected["partition_id"],
                    selected["scanned"] + 1,
                )
                if entry is None:
                    return "EMPTY"
                if entry["entry_kind"] == "BARRIER":
                    result = await self._barrier(
                        conn,
                        worker,
                        selected,
                        entry,
                        inputs,
                        coordinator_owner,
                        coordinator_epoch,
                        fault,
                    )
                else:
                    source = await self._source(
                        conn,
                        entry["channel"],
                        entry["partition_id"],
                        entry["data_sequence"],
                    )
                    if source["record_id"] != entry["record_id"]:
                        raise PostgresProtocolError("join DATA authority mismatch")
                    receipt = {
                        "revision_id": self.revision.revision_id,
                        "participant_id": self.participant,
                        "arrival_ordinal": worker["next_arrival"],
                        "channel": entry["channel"],
                        "partition_id": entry["partition_id"],
                        "log_sequence": entry["log_sequence"],
                        "data_sequence": entry["data_sequence"],
                        "record_id": source["record_id"],
                        "object_key": source["object_key"],
                        "payload_digest": source["payload_digest"],
                        "payload_size": source["payload_size"],
                        "blocked_checkpoint": selected["blocked_checkpoint"],
                    }
                    await conn.execute(
                        """INSERT INTO join_receipts_v3 VALUES(
                        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)""",
                        *receipt.values(),
                        digest(receipt),
                    )
                    await conn.execute(
                        """UPDATE join_workers SET next_arrival=next_arrival+1
                        WHERE revision_id=$1 AND participant_id=$2""",
                        self.revision.revision_id,
                        self.participant,
                    )
                    await self._advance_scan(conn, selected, entry["log_sequence"])
                    if selected["blocked_checkpoint"] is not None:
                        result = "BUFFERED"
                    else:
                        invocation = await self._claim_invocation(conn, worker, receipt)
                        result = "APPLIED"
            await self._budget(conn, worker)
            hit(fault, "step_before_commit")
        hit(fault, "step_after_commit")
        if invocation is not None:
            token, receipt, payload, state = invocation
            try:
                terminal = await self._execute(task, state, receipt, payload)
                hit(fault, "terminal_observed")
                async with self.pool.acquire() as conn, conn.transaction():
                    await self.coordinator._locked_revision(
                        conn, self.revision.revision_id
                    )
                    worker = await self._owner(conn, owner, epoch)
                    claim = await conn.fetchrow(
                        """SELECT * FROM join_invocations_v3
                        WHERE revision_id=$1 AND participant_id=$2""",
                        self.revision.revision_id,
                        self.participant,
                    )
                    if claim is None or (
                        claim["token"],
                        claim["writer_epoch"],
                        claim["expected_apply"],
                    ) != (token, epoch, worker["next_apply"]):
                        raise StaleOwner("join invocation fenced")
                    await self._apply(conn, worker, receipt, terminal, fault)
                    await conn.execute(
                        "DELETE FROM join_invocations_v3 WHERE token=$1", token
                    )
                    await self._budget(conn, worker)
                    hit(fault, "terminal_before_commit")
                hit(fault, "terminal_after_commit")
            except BaseException:
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        "DELETE FROM join_invocations_v3 WHERE token=$1", token
                    )
                raise
        return result

    async def _claim_invocation(self, conn, worker, receipt):
        source = await self._source(
            conn, receipt["channel"], receipt["partition_id"], receipt["data_sequence"]
        )
        token = uuid.uuid4().hex
        await conn.execute(
            """INSERT INTO join_invocation_history_v3 VALUES($1,$2,$3,$4,$5,$6)""",
            token,
            self.revision.revision_id,
            self.participant,
            receipt["arrival_ordinal"],
            worker["owner_epoch"],
            worker["next_apply"],
        )
        await conn.execute(
            """INSERT INTO join_invocations_v3 VALUES($1,$2,$3,$4,$5,$6)""",
            self.revision.revision_id,
            self.participant,
            token,
            receipt["arrival_ordinal"],
            worker["owner_epoch"],
            worker["next_apply"],
        )
        return token, dict(receipt), bytes(source["payload"]), decoded(worker["state"])

    async def _advance_scan(self, conn, selected, sequence):
        await conn.execute(
            """UPDATE join_inputs_v3 SET scanned=$5
            WHERE revision_id=$1 AND participant_id=$2 AND channel=$3 AND partition_id=$4""",
            self.revision.revision_id,
            self.participant,
            selected["channel"],
            selected["partition_id"],
            sequence,
        )

    async def _source(self, conn, channel, partition, sequence):
        row = await conn.fetchrow(
            """SELECT r.*,o.payload,o.published_at,
              o.payload_digest AS object_digest,o.payload_size AS object_size
            FROM records r JOIN objects o USING(object_key)
            WHERE r.channel=$1 AND r.partition_id=$2 AND r.sequence=$3""",
            channel,
            partition,
            sequence,
        )
        if (
            row is None
            or row["published_at"] is None
            or (
                hashlib.sha256(bytes(row["payload"])).hexdigest(),
                len(row["payload"]),
            )
            != (row["payload_digest"], row["payload_size"])
            or (row["object_digest"], row["object_size"])
            != (row["payload_digest"], row["payload_size"])
        ):
            raise PostgresProtocolError(
                "join replay DATA payload unavailable or corrupt"
            )
        return row

    def _invoke(self, task, state, receipt, payload):
        context = JoinContext(
            self.participant,
            location_key(receipt["channel"], receipt["partition_id"]),
            receipt["data_sequence"],
            json.loads(canonical(state)),
        )
        generator = task(context, payload)
        try:
            spec = next(generator)
        except StopIteration as stop:
            new_state = context.managed_state if stop.value is None else stop.value
            return "RETURN", json.loads(canonical(new_state)), None
        try:
            if not isinstance(spec, OutputSpec):
                raise PostgresProtocolError("yield must contain OutputSpec")
            return "YIELD", json.loads(canonical(spec.managed_state)), spec
        finally:
            generator.close()

    async def _execute(self, task, state, receipt, payload):
        if not inspect.isasyncgenfunction(task):
            return await asyncio.to_thread(self._invoke, task, state, receipt, payload)
        context = JoinContext(
            self.participant,
            location_key(receipt["channel"], receipt["partition_id"]),
            receipt["data_sequence"],
            json.loads(canonical(state)),
        )
        generator = task(context, payload)
        try:
            try:
                spec = await generator.__anext__()
            except StopAsyncIteration:
                return "RETURN", json.loads(canonical(context.managed_state)), None
            if not isinstance(spec, OutputSpec):
                raise PostgresProtocolError("yield must contain OutputSpec")
            return "YIELD", json.loads(canonical(spec.managed_state)), spec
        finally:
            await generator.aclose()

    async def _apply(self, conn, worker, receipt, terminal_result, fault):
        source = await self._source(
            conn, receipt["channel"], receipt["partition_id"], receipt["data_sequence"]
        )
        if (source["record_id"], source["object_key"], source["payload_digest"]) != (
            receipt["record_id"],
            receipt["object_key"],
            receipt["payload_digest"],
        ):
            raise PostgresProtocolError("join receipt input identity drift")
        before = decoded(worker["state"])
        terminal, after, spec = terminal_result
        output_id = output_digest = None
        if spec is not None:
            identity = digest(
                [
                    self.revision.revision_id,
                    self.participant,
                    receipt["channel"],
                    receipt["partition_id"],
                    receipt["record_id"],
                ]
            )
            output_id = "join-output:" + identity
            output_digest = hashlib.sha256(spec.payload).hexdigest()
            output = self.spec["outputs"][0]
            producer_sequence = 1 + await conn.fetchval(
                """SELECT accepted_through FROM producer_progress
                WHERE channel=$1 AND partition_id=$2 AND producer_id=$3""",
                output["channel"],
                output["partition"],
                self.producer_id,
            )
            await self.stream._append_with_connection(
                conn,
                "join-append:" + identity,
                output["channel"],
                output["partition"],
                output_id,
                content_object_key(spec.payload, spec.schema_id),
                spec.payload,
                spec.schema_id,
                identity,
                self.producer_id,
                producer_sequence,
            )
        turn = {
            "revision_id": self.revision.revision_id,
            "participant_id": self.participant,
            "apply_ordinal": worker["next_apply"],
            "arrival_ordinal": receipt["arrival_ordinal"],
            "before_state": before,
            "after_state": after,
            "terminal": terminal,
            "output_record_id": output_id,
            "output_digest": output_digest,
            "writer_epoch": worker["owner_epoch"],
            "invocation_token": await conn.fetchval(
                """SELECT token FROM join_invocations_v3
                WHERE revision_id=$1 AND participant_id=$2""",
                self.revision.revision_id,
                self.participant,
            ),
        }
        await conn.execute(
            """INSERT INTO join_turns_v3 VALUES(
            $1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10,$11,$12)""",
            self.revision.revision_id,
            self.participant,
            worker["next_apply"],
            receipt["arrival_ordinal"],
            canonical(before),
            canonical(after),
            terminal,
            output_id,
            output_digest,
            worker["owner_epoch"],
            digest(turn),
            turn["invocation_token"],
        )
        updated = await conn.execute(
            """UPDATE join_inputs_v3 SET completed=$5
            WHERE revision_id=$1 AND participant_id=$2 AND channel=$3 AND partition_id=$4
              AND completed=$5-1""",
            self.revision.revision_id,
            self.participant,
            receipt["channel"],
            receipt["partition_id"],
            receipt["data_sequence"],
        )
        if updated != "UPDATE 1":
            raise PostgresProtocolError("join terminal DATA prefix gap")
        await conn.execute(
            """UPDATE join_workers SET state=$3::jsonb,next_apply=next_apply+1
            WHERE revision_id=$1 AND participant_id=$2""",
            self.revision.revision_id,
            self.participant,
            canonical(after),
        )

    async def _barrier(
        self, conn, worker, selected, entry, inputs, owner, epoch, fault
    ):
        checkpoint = await conn.fetchrow(
            "SELECT * FROM checkpoint_intents_v2 WHERE checkpoint_id=$1",
            entry["checkpoint_id"],
        )
        lifecycle = await conn.fetchval(
            "SELECT status FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1",
            entry["checkpoint_id"],
        )
        if (
            checkpoint is None
            or checkpoint["revision_id"] != self.revision.revision_id
            or (checkpoint["intent_digest"] != entry["intent_digest"])
        ):
            raise PostgresProtocolError("join barrier checkpoint authority mismatch")
        if lifecycle == "ABORTED":
            await self._advance_scan(conn, selected, entry["log_sequence"])
            return "ABORTED"
        if worker["active_checkpoint"] not in (None, entry["checkpoint_id"]):
            raise PostgresProtocolError(
                "overlapping checkpoint must wait for alignment"
            )
        if selected["blocked_checkpoint"] is not None:
            raise PostgresProtocolError("input already blocked at a barrier")
        if checkpoint["checkpoint_ordinal"] <= worker["last_aligned_ordinal"]:
            raise PostgresProtocolError("join checkpoint ordinal regression")
        if selected["completed"] != entry["cut_data_sequence"]:
            raise PostgresProtocolError(
                "join barrier cut differs from terminal DATA prefix"
            )
        await conn.execute(
            """INSERT INTO join_barriers_v3 VALUES($1,$2,$3,$4,$5,$6,$7)""",
            self.revision.revision_id,
            self.participant,
            entry["checkpoint_id"],
            entry["channel"],
            entry["partition_id"],
            entry["log_sequence"],
            entry["cut_data_sequence"],
        )
        await self._advance_scan(conn, selected, entry["log_sequence"])
        await conn.execute(
            """UPDATE join_inputs_v3 SET blocked_checkpoint=$5
            WHERE revision_id=$1 AND participant_id=$2 AND channel=$3 AND partition_id=$4""",
            self.revision.revision_id,
            self.participant,
            entry["channel"],
            entry["partition_id"],
            entry["checkpoint_id"],
        )
        await conn.execute(
            """UPDATE join_workers SET active_checkpoint=$3
            WHERE revision_id=$1 AND participant_id=$2""",
            self.revision.revision_id,
            self.participant,
            entry["checkpoint_id"],
        )
        count = await conn.fetchval(
            """SELECT count(*) FROM join_barriers_v3
            WHERE revision_id=$1 AND participant_id=$2 AND checkpoint_id=$3""",
            self.revision.revision_id,
            self.participant,
            entry["checkpoint_id"],
        )
        if count != len(inputs):
            return "WAITING"
        ack = await self.coordinator.acknowledge_participant(
            self.revision.revision_id,
            self.revision.graph_version,
            self.revision.graph_digest,
            owner,
            epoch,
            entry["checkpoint_id"],
            self.participant,
            decoded(worker["state"]),
            mailbox_highwater=worker["next_apply"] - 1,
            state_version=self.spec["state_version"],
            connection=conn,
        )
        hit(fault, "alignment_after_snapshot")
        await conn.execute(
            """INSERT INTO join_alignments_v3 VALUES($1,$2,$3,$4,$5,$6)""",
            self.revision.revision_id,
            self.participant,
            entry["checkpoint_id"],
            worker["next_apply"] - 1,
            ack,
            digest(decoded(worker["state"])),
        )
        await conn.execute(
            """UPDATE join_workers SET active_checkpoint=NULL,last_aligned_ordinal=$3
            WHERE revision_id=$1 AND participant_id=$2""",
            self.revision.revision_id,
            self.participant,
            checkpoint["checkpoint_ordinal"],
        )
        await conn.execute(
            """UPDATE join_inputs_v3 SET blocked_checkpoint=NULL
            WHERE revision_id=$1 AND participant_id=$2""",
            self.revision.revision_id,
            self.participant,
        )
        return "ALIGNED"

    async def position(self):
        async with self.pool.acquire() as conn:
            worker = await conn.fetchrow(
                "SELECT * FROM join_workers WHERE revision_id=$1 AND participant_id=$2",
                self.revision.revision_id,
                self.participant,
            )
            return dict(worker)

    async def restore(self, recovery_id, task, owner, epoch, *, fault=None):
        """Restore an authoritative snapshot and reexecute its exact mailbox tail.

        Persisted completion and output identities remain unchanged. Each replay
        is checked against the original terminal state and output digest.
        """
        async with self.pool.acquire() as conn, conn.transaction():
            await self.coordinator._locked_revision(conn, self.revision.revision_id)
            await self.coordinator._assert_global_recovery_scope(conn, recovery_id)
            worker = await self._owner(conn, owner, epoch)
            plan = await conn.fetchrow(
                """SELECT p.*,a.mailbox_highwater,a.snapshot_object_key,
                a.state_schema,a.state_version,a.code_digest,o.payload
                FROM recovery_plans_v2 p JOIN participant_checkpoint_acks a USING(checkpoint_id)
                JOIN checkpoint_snapshot_objects o ON o.object_key=a.snapshot_object_key
                WHERE p.recovery_id=$1 AND a.participant_id=$2""",
                recovery_id,
                self.participant,
            )
            if plan is None or plan["revision_id"] != self.revision.revision_id:
                raise PostgresProtocolError("join recovery lacks durable authority")
            if await conn.fetchval(
                "SELECT to_regclass('join_retention_anchors_v1') IS NOT NULL"
            ):
                anchor = await conn.fetchrow(
                    """SELECT * FROM join_retention_anchors_v1
                    WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    self.participant,
                )
                if (
                    anchor is not None
                    and plan["mailbox_highwater"] < anchor["retired_apply_through"]
                ):
                    raise JoinReplayHorizonExpired(
                        f"checkpoint {plan['checkpoint_id']} retired through mailbox {anchor['retired_apply_through']}"
                    )
            # Use the existing state adapter to validate durable plan/digest,
            # completion/fence, snapshot bytes, and exact state/code contract.
            from_plan = self.coordinator._recovery_plan(plan)
            adapter = PostgresCheckpointRuntime(
                self.coordinator,
                revision_id=self.revision.revision_id,
                graph_version=self.revision.graph_version,
                graph_digest=self.revision.graph_digest,
                owner_id=owner,
                owner_epoch=epoch,
                contract=SharedStateContract(
                    self.participant,
                    self.spec["state_schema"],
                    self.spec["state_version"],
                    self.spec["code_digest"],
                ),
            )
            restored = await adapter.restore_frozen_plan(from_plan)
            existing = await conn.fetchrow(
                """SELECT * FROM join_recoveries_v3
                WHERE revision_id=$1 AND participant_id=$2 AND recovery_id=$3""",
                self.revision.revision_id,
                self.participant,
                recovery_id,
            )
            if existing is None:
                targets = decoded(plan["target_data_sequences"])
                receipts = await conn.fetch(
                    """SELECT r.* FROM join_receipts_v3 r
                    JOIN join_turns_v3 t USING(revision_id,participant_id,arrival_ordinal)
                    WHERE r.revision_id=$1 AND r.participant_id=$2""",
                    self.revision.revision_id,
                    self.participant,
                )
                if any(
                    r["data_sequence"]
                    > targets.get(location_key(r["channel"], r["partition_id"]), -1)
                    for r in receipts
                ):
                    raise PostgresProtocolError(
                        "join progressed beyond frozen recovery fence"
                    )
                await conn.execute(
                    """INSERT INTO join_recoveries_v3 VALUES(
                    $1,$2,$3,$4,$5,$6::jsonb,'RESTORING')""",
                    self.revision.revision_id,
                    self.participant,
                    recovery_id,
                    worker["next_apply"] - 1,
                    plan["mailbox_highwater"],
                    canonical(restored.state),
                )
                await conn.execute(
                    """UPDATE join_workers SET phase='RESTORING'
                    WHERE revision_id=$1 AND participant_id=$2""",
                    self.revision.revision_id,
                    self.participant,
                )
            elif existing["status"] == "READY":
                return decoded(existing["state"])
            await self._budget(conn, worker)
            hit(fault, "restore_begin_before_commit")
        hit(fault, "restore_begin_after_commit")
        while True:
            async with self.pool.acquire() as conn, conn.transaction():
                worker = await self._owner(conn, owner, epoch)
                progress = await conn.fetchrow(
                    """SELECT * FROM join_recoveries_v3
                    WHERE revision_id=$1 AND participant_id=$2 AND recovery_id=$3 FOR UPDATE""",
                    self.revision.revision_id,
                    self.participant,
                    recovery_id,
                )
                if progress["replayed_through"] == progress["target_apply"]:
                    state = decoded(progress["state"])
                    if state != decoded(worker["state"]):
                        raise PostgresProtocolError(
                            "join restored state differs from durable terminal prefix"
                        )
                    await conn.execute(
                        """UPDATE join_recoveries_v3 SET status='READY'
                        WHERE revision_id=$1 AND participant_id=$2 AND recovery_id=$3""",
                        self.revision.revision_id,
                        self.participant,
                        recovery_id,
                    )
                    await conn.execute(
                        """UPDATE join_workers SET phase='LIVE'
                        WHERE revision_id=$1 AND participant_id=$2""",
                        self.revision.revision_id,
                        self.participant,
                    )
                    hit(fault, "restore_ready_before_commit")
                    done = True
                else:
                    turn = await conn.fetchrow(
                        """SELECT * FROM join_turns_v3 WHERE
                        revision_id=$1 AND participant_id=$2 AND apply_ordinal=$3""",
                        self.revision.revision_id,
                        self.participant,
                        progress["replayed_through"] + 1,
                    )
                    if turn is None:
                        raise PostgresProtocolError("join replay mailbox gap")
                    receipt = await conn.fetchrow(
                        """SELECT * FROM join_receipts_v3 WHERE
                        revision_id=$1 AND participant_id=$2 AND arrival_ordinal=$3""",
                        self.revision.revision_id,
                        self.participant,
                        turn["arrival_ordinal"],
                    )
                    source = await self._source(
                        conn,
                        receipt["channel"],
                        receipt["partition_id"],
                        receipt["data_sequence"],
                    )
                    before = decoded(progress["state"])
                    payload = bytes(source["payload"])
                    done = False
            if not done:
                terminal, after, output = await self._execute(
                    task, before, receipt, payload
                )
                output_digest = (
                    None
                    if output is None
                    else hashlib.sha256(output.payload).hexdigest()
                )
                if (before, after, terminal, output_digest) != (
                    decoded(turn["before_state"]),
                    decoded(turn["after_state"]),
                    turn["terminal"],
                    turn["output_digest"],
                ):
                    raise PostgresProtocolError(
                        "client replay diverged from durable terminal identity"
                    )
                async with self.pool.acquire() as conn, conn.transaction():
                    worker = await self._owner(conn, owner, epoch)
                    current = await conn.fetchrow(
                        """SELECT * FROM join_recoveries_v3
                        WHERE revision_id=$1 AND participant_id=$2 AND recovery_id=$3 FOR UPDATE""",
                        self.revision.revision_id,
                        self.participant,
                        recovery_id,
                    )
                    if (
                        current["replayed_through"] != progress["replayed_through"]
                        or worker["phase"] != "RESTORING"
                    ):
                        raise StaleOwner("join replay claim changed")
                    if output is not None:
                        output_row = await conn.fetchrow(
                            "SELECT object_key,schema_id FROM records WHERE record_id=$1",
                            turn["output_record_id"],
                        )
                        if output_row is None or (
                            output_row["object_key"],
                            output_row["schema_id"],
                        ) != (
                            content_object_key(output.payload, output.schema_id),
                            output.schema_id,
                        ):
                            raise PostgresProtocolError(
                                "client replay output schema drift"
                            )
                    await conn.execute(
                        """UPDATE join_recoveries_v3 SET replayed_through=$4,state=$5::jsonb
                        WHERE revision_id=$1 AND participant_id=$2 AND recovery_id=$3""",
                        self.revision.revision_id,
                        self.participant,
                        recovery_id,
                        turn["apply_ordinal"],
                        canonical(after),
                    )
                    hit(fault, "replay_turn_before_commit")
            hit(
                fault,
                "restore_ready_after_commit" if done else "replay_turn_after_commit",
            )
            if done:
                return state
