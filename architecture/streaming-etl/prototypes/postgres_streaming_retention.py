"""R3-R4 composition of checkpoint/log retirement with stream metadata GC."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from postgres_barrier_coordinator import PostgresBarrierCoordinator, decoded
from postgres_stream_v2 import (
    PostgresProtocolError,
    PostgresStream,
    _canonical_metadata_cuts,
)


@dataclass(frozen=True)
class StreamingRetentionOutcome:
    operation_id: str
    revision_id: str
    checkpoint_id: str
    cuts: dict[str, int]
    reclaimed_records: int
    reclaimed_bytes: int
    metadata_outcome: dict[str, Any]
    idempotent_retry: bool = False


class PostgresStreamingRetention:
    def __init__(self, stream: PostgresStream, coordinator: PostgresBarrierCoordinator):
        self.stream = stream
        self.coordinator = coordinator
        self.pool = stream.pool

    @classmethod
    async def install(cls, stream: PostgresStream, coordinator):
        value = cls(stream, coordinator)
        async with stream.pool.acquire() as conn:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS streaming_retention_operations_v1(
                   revision_id text PRIMARY KEY REFERENCES deployment_revisions,
                   operation_id text NOT NULL UNIQUE,checkpoint_id text NOT NULL,
                   cuts_digest text NOT NULL,cuts_json jsonb NOT NULL,
                   status text NOT NULL CHECK(status IN(
                     'PLANNED','DATA_COMPACTED','COMPLETED')),
                   reclaimed_records bigint NOT NULL DEFAULT 0,
                   reclaimed_bytes bigint NOT NULL DEFAULT 0,
                   outcome_json jsonb,updated_at timestamptz NOT NULL DEFAULT clock_timestamp())"""
            )
            await conn.execute(
                """CREATE OR REPLACE FUNCTION streaming_retention_expected_cuts(cp text)
                   RETURNS jsonb LANGUAGE sql STABLE AS $$
                   WITH raw(location,cut) AS (
                     SELECT channel||'/'||partition_id,cut_data_sequence
                       FROM source_barrier_observations WHERE checkpoint_id=cp
                     UNION ALL
                     SELECT output.key,(output.value->>'cut_data_sequence')::bigint
                       FROM participant_checkpoint_acks a,
                            jsonb_each(a.output_barriers) output
                       WHERE a.checkpoint_id=cp),
                   grouped AS (
                     SELECT location,min(cut) lo,max(cut) hi FROM raw GROUP BY location)
                   SELECT CASE WHEN count(*) FILTER (WHERE lo>0)=0
                                      OR bool_or(lo<>hi) THEN NULL
                     ELSE jsonb_agg(jsonb_build_object(
                       'channel',split_part(location,'/',1),
                       'partition',substring(location from
                         length(split_part(location,'/',1))+2),
                       'through',lo) ORDER BY location) FILTER (WHERE lo>0) END
                   FROM grouped
                   $$;
                   CREATE OR REPLACE FUNCTION streaming_retention_operation_guard()
                   RETURNS trigger LANGUAGE plpgsql AS $$
                   DECLARE op text; DECLARE expected jsonb;
                   BEGIN
                     op=current_setting('ariadne.streaming_retention_operation',true);
                     IF TG_OP='DELETE' OR op IS NULL OR NEW.operation_id<>op THEN
                       RAISE EXCEPTION 'stream retention operation lacks authority';
                     END IF;
                     SELECT streaming_retention_expected_cuts(NEW.checkpoint_id)
                       INTO expected;
                     IF expected IS NULL OR expected IS DISTINCT FROM NEW.cuts_json OR NOT EXISTS(
                       SELECT 1 FROM checkpoint_intents_v2 i
                       JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                       JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                       WHERE i.checkpoint_id=NEW.checkpoint_id
                         AND i.revision_id=NEW.revision_id AND l.status='COMPLETED')
                     THEN RAISE EXCEPTION 'stream retention checkpoint/cuts drift'; END IF;
                     IF TG_OP='INSERT' THEN
                       IF NEW.status<>'PLANNED' OR NEW.outcome_json IS NOT NULL
                          OR NEW.reclaimed_records<>0 OR NEW.reclaimed_bytes<>0
                       THEN RAISE EXCEPTION 'illegal initial stream retention operation'; END IF;
                     ELSIF NOT(
                       OLD.status='PLANNED' AND NEW.status='DATA_COMPACTED'
                         AND NEW.revision_id=OLD.revision_id
                         AND NEW.operation_id=OLD.operation_id
                         AND NEW.checkpoint_id=OLD.checkpoint_id
                         AND NEW.cuts_digest=OLD.cuts_digest
                         AND NEW.cuts_json=OLD.cuts_json
                         AND NEW.outcome_json IS NULL
                       OR OLD.status='DATA_COMPACTED' AND NEW.status='COMPLETED'
                         AND NEW.revision_id=OLD.revision_id
                         AND NEW.operation_id=OLD.operation_id
                         AND NEW.checkpoint_id=OLD.checkpoint_id
                         AND NEW.cuts_digest=OLD.cuts_digest
                         AND NEW.cuts_json=OLD.cuts_json
                         AND NEW.reclaimed_records=OLD.reclaimed_records
                         AND NEW.reclaimed_bytes=OLD.reclaimed_bytes
                         AND NEW.outcome_json IS NOT NULL
                       OR OLD.status='COMPLETED' AND NEW.status='PLANNED'
                         AND NEW.revision_id=OLD.revision_id
                         AND NEW.operation_id<>OLD.operation_id
                         AND NEW.outcome_json IS NULL
                         AND NEW.reclaimed_records=0 AND NEW.reclaimed_bytes=0)
                     THEN RAISE EXCEPTION 'illegal stream retention transition'; END IF;
                     RETURN NEW;
                   END $$;
                   CREATE OR REPLACE FUNCTION streaming_retention_converged()
                   RETURNS trigger LANGUAGE plpgsql AS $$
                   BEGIN
                     IF NEW.status='COMPLETED' AND (
                       jsonb_typeof(NEW.outcome_json) IS DISTINCT FROM 'object'
                       OR NEW.outcome_json->>'operation_id' IS DISTINCT FROM NEW.operation_id
                       OR NEW.outcome_json->>'revision_id' IS DISTINCT FROM NEW.revision_id
                       OR NEW.outcome_json->>'checkpoint_id' IS DISTINCT FROM NEW.checkpoint_id
                       OR NEW.outcome_json->'canonical_cuts' IS DISTINCT FROM NEW.cuts_json
                       OR (NEW.outcome_json->>'reclaimed_records')::bigint IS DISTINCT FROM
                          NEW.reclaimed_records
                       OR (NEW.outcome_json->>'reclaimed_bytes')::bigint IS DISTINCT FROM
                          NEW.reclaimed_bytes
                       OR EXISTS(SELECT 1 FROM jsonb_array_elements(NEW.cuts_json) cut
                          WHERE NOT EXISTS(SELECT 1 FROM metadata_summaries s
                            WHERE s.channel=cut.value->>'channel'
                              AND s.partition_id=cut.value->>'partition'
                              AND s.retired_through=(cut.value->>'through')::bigint
                              AND s.operation_id='metadata:'||NEW.cuts_digest)
                             OR EXISTS(SELECT 1 FROM records r
                               WHERE r.channel=cut.value->>'channel'
                                 AND r.partition_id=cut.value->>'partition'
                                 AND r.sequence<=(cut.value->>'through')::bigint))
                       OR NOT EXISTS(SELECT 1 FROM metadata_operations m
                            WHERE m.operation_id='metadata:'||NEW.cuts_digest
                              AND m.cuts=NEW.cuts_json))
                     THEN RAISE EXCEPTION 'stream retention did not converge'; END IF;
                     RETURN NULL;
                   END $$;
                   DROP TRIGGER IF EXISTS streaming_retention_operation_authority
                     ON streaming_retention_operations_v1;
                   CREATE TRIGGER streaming_retention_operation_authority
                     BEFORE INSERT OR UPDATE OR DELETE
                     ON streaming_retention_operations_v1 FOR EACH ROW
                     EXECUTE FUNCTION streaming_retention_operation_guard();
                   DROP TRIGGER IF EXISTS streaming_retention_operation_convergence
                     ON streaming_retention_operations_v1;
                   CREATE CONSTRAINT TRIGGER streaming_retention_operation_convergence
                     AFTER INSERT OR UPDATE ON streaming_retention_operations_v1
                     DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
                     EXECUTE FUNCTION streaming_retention_converged()"""
            )
        return value

    async def compact_latest(
        self,
        revision_id: str,
        graph_version: str,
        graph_digest: str,
        owner_id: str,
        owner_epoch: int,
        operation_id: str,
        *,
        fault: Callable[[str], None] | None = None,
    ) -> StreamingRetentionOutcome:
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(conn, revision_id)
            self.coordinator._check_graph(revision, graph_version, graph_digest)
            await self.coordinator._assert_owner(
                conn, revision_id, owner_id, owner_epoch
            )
            existing = await conn.fetchrow(
                """SELECT * FROM streaming_retention_operations_v1
                   WHERE revision_id=$1 FOR UPDATE""",
                revision_id,
            )
            exact_retry = (
                existing is not None and existing["operation_id"] == operation_id
            )
            if exact_retry and existing["status"] == "COMPLETED":
                return self._outcome(decoded(existing["outcome_json"]), True)
            checkpoint = await conn.fetchrow(
                """SELECT i.checkpoint_id,i.checkpoint_ordinal,i.intent_digest
                   FROM checkpoint_intents_v2 i JOIN checkpoint_lifecycle_v2 l
                     USING(checkpoint_id) JOIN checkpoint_completions_v2 c
                     USING(checkpoint_id)
                   WHERE i.revision_id=$1 AND l.status='COMPLETED'
                     AND ($2::text IS NULL OR i.checkpoint_id=$2)
                   ORDER BY i.checkpoint_ordinal DESC LIMIT 1""",
                revision_id,
                existing["checkpoint_id"] if exact_retry else None,
            )
            if checkpoint is None:
                raise PostgresProtocolError("stream GC requires completed checkpoint")
            cuts = await self._checkpoint_cuts(conn, revision_id, checkpoint)
            canonical_cuts = [
                (location.split("/", 1)[0], location.split("/", 1)[1], cut)
                for location, cut in sorted(cuts.items())
            ]
            _, _, cuts_json, cuts_digest = _canonical_metadata_cuts(canonical_cuts)
            await conn.execute(
                "SELECT set_config('ariadne.streaming_retention_operation',$1,true)",
                operation_id,
            )
            if exact_retry:
                if (
                    existing["checkpoint_id"],
                    existing["cuts_digest"],
                    json.loads(existing["cuts_json"]),
                ) != (checkpoint["checkpoint_id"], cuts_digest, json.loads(cuts_json)):
                    raise PostgresProtocolError("stream GC operation identity drift")
            elif existing is None:
                await conn.execute(
                    """INSERT INTO streaming_retention_operations_v1(
                       revision_id,operation_id,checkpoint_id,cuts_digest,cuts_json,status)
                       VALUES($1,$2,$3,$4,$5::jsonb,'PLANNED')""",
                    revision_id,
                    operation_id,
                    checkpoint["checkpoint_id"],
                    cuts_digest,
                    cuts_json,
                )
            else:
                if existing["status"] != "COMPLETED":
                    raise PostgresProtocolError("prior stream GC operation incomplete")
                await conn.execute(
                    """UPDATE streaming_retention_operations_v1 SET
                       operation_id=$2,checkpoint_id=$3,cuts_digest=$4,cuts_json=$5::jsonb,
                       status='PLANNED',reclaimed_records=0,reclaimed_bytes=0,
                       outcome_json=NULL,updated_at=clock_timestamp()
                       WHERE revision_id=$1""",
                    revision_id,
                    operation_id,
                    checkpoint["checkpoint_id"],
                    cuts_digest,
                    cuts_json,
                )
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        if fault is not None:
            fault("stream_gc_planned")
        # These counters are evidence only, but they must remain exact across a
        # process loss after an individual `compact` transaction.  Derive them
        # from the durable tombstone interval below instead of trusting the
        # process-local return values.
        data_already_compacted = exact_retry and existing["status"] == "DATA_COMPACTED"
        reclaimed_records = (
            existing["reclaimed_records"] if data_already_compacted else 0
        )
        reclaimed_bytes = existing["reclaimed_bytes"] if data_already_compacted else 0
        for channel, partition, cut in canonical_cuts:
            async with self.pool.acquire() as conn:
                retired = await conn.fetchval(
                    """SELECT retired_through FROM metadata_summaries
                       WHERE channel=$1 AND partition_id=$2""",
                    channel,
                    partition,
                )
                retired = 0 if retired is None else retired
                if retired >= cut:
                    continue
                if await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM partition_log_entries
                       WHERE channel=$1 AND partition_id=$2 AND entry_kind='DATA'
                         AND data_sequence<=$3)""",
                    channel,
                    partition,
                    cut,
                ):
                    raise PostgresProtocolError(
                        "stream GC requires R2 DATA-log retirement through cut"
                    )
                has_records = await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM records
                       WHERE channel=$1 AND partition_id=$2 AND sequence<=$3)""",
                    channel,
                    partition,
                    cut,
                )
            if has_records:
                result = await self.stream.compact(channel, partition, cut)
                if result["safe_through"] != cut:
                    raise PostgresProtocolError(
                        "active stream root prevented exact checkpoint compaction"
                    )
                if result["reclaimed_records"] <= 0:
                    raise PostgresProtocolError(
                        "stream data compaction made no progress"
                    )
        if not data_already_compacted:
            async with self.pool.acquire() as conn, conn.transaction():
                revision = await self.coordinator._locked_revision(conn, revision_id)
                self.coordinator._check_graph(revision, graph_version, graph_digest)
                await self.coordinator._assert_owner(
                    conn, revision_id, owner_id, owner_epoch
                )
                await conn.execute(
                    "SELECT set_config('ariadne.streaming_retention_operation',$1,true)",
                    operation_id,
                )
                for channel, partition, cut in canonical_cuts:
                    if await conn.fetchval(
                        """SELECT EXISTS(SELECT 1 FROM records
                           WHERE channel=$1 AND partition_id=$2 AND sequence<=$3)""",
                        channel,
                        partition,
                        cut,
                    ):
                        raise PostgresProtocolError(
                            "stream data prefix did not converge"
                        )
                    retired = (
                        await conn.fetchval(
                            """SELECT retired_through FROM metadata_summaries
                               WHERE channel=$1 AND partition_id=$2""",
                            channel,
                            partition,
                        )
                        or 0
                    )
                    reclaimed_records += await conn.fetchval(
                        """SELECT count(*) FROM compaction_tombstones
                           WHERE channel=$1 AND partition_id=$2
                             AND sequence>$3 AND sequence<=$4""",
                        channel,
                        partition,
                        retired,
                        cut,
                    )
                    reclaimed_bytes += int(
                        await conn.fetchval(
                            """SELECT COALESCE(sum(payload_size),0) FROM (
                           SELECT object_key,max(payload_size) AS payload_size
                           FROM compaction_tombstones t
                           WHERE t.channel=$1 AND t.partition_id=$2
                             AND t.sequence>$3 AND t.sequence<=$4
                             AND NOT EXISTS(SELECT 1 FROM records r
                               WHERE r.channel=t.channel AND r.partition_id=t.partition_id
                                 AND r.object_key=t.object_key)
                           GROUP BY object_key) reclaimed""",
                            channel,
                            partition,
                            retired,
                            cut,
                        )
                    )
                updated = await conn.execute(
                    """UPDATE streaming_retention_operations_v1 SET
                       status='DATA_COMPACTED',reclaimed_records=$2,reclaimed_bytes=$3,
                       updated_at=clock_timestamp() WHERE revision_id=$1
                         AND operation_id=$4 AND status='PLANNED'""",
                    revision_id,
                    reclaimed_records,
                    reclaimed_bytes,
                    operation_id,
                )
                if updated != "UPDATE 1":
                    raise PostgresProtocolError("stream GC data-stage claim changed")
                await self.coordinator._assert_metadata_budget(conn, revision_id)
        if fault is not None:
            fault("stream_gc_data_compacted")
        async with self.pool.acquire() as conn:
            floors = {
                f"{channel}/{partition}": (
                    await conn.fetchval(
                        """SELECT retired_through FROM metadata_summaries
                           WHERE channel=$1 AND partition_id=$2""",
                        channel,
                        partition,
                    )
                    or 0
                )
                for channel, partition, _ in canonical_cuts
            }
        if all(floors[location] == cut for location, cut in cuts.items()):
            async with self.pool.acquire() as conn:
                stored_metadata = await conn.fetchrow(
                    "SELECT * FROM metadata_operations WHERE operation_id=$1",
                    f"metadata:{cuts_digest}",
                )
            if stored_metadata is None or decoded(
                stored_metadata["cuts"]
            ) != json.loads(cuts_json):
                raise PostgresProtocolError("metadata summary lacks exact operation")
            metadata_outcome = decoded(stored_metadata["outcome"])
            metadata_outcome["idempotent_retry"] = True
        elif any(floors[location] >= cut for location, cut in cuts.items()):
            raise PostgresProtocolError("stream metadata floors diverged across cut")
        else:
            metadata_outcome = await self.stream.compact_metadata(
                canonical_cuts, operation_id=f"metadata:{cuts_digest}"
            )
        if fault is not None:
            fault("stream_gc_metadata_compacted")
        outcome = {
            "operation_id": operation_id,
            "revision_id": revision_id,
            "checkpoint_id": checkpoint["checkpoint_id"],
            "cuts": cuts,
            "canonical_cuts": json.loads(cuts_json),
            "reclaimed_records": reclaimed_records,
            "reclaimed_bytes": reclaimed_bytes,
            "metadata_outcome": metadata_outcome,
        }
        async with self.pool.acquire() as conn, conn.transaction():
            revision = await self.coordinator._locked_revision(conn, revision_id)
            self.coordinator._check_graph(revision, graph_version, graph_digest)
            await self.coordinator._assert_owner(
                conn, revision_id, owner_id, owner_epoch
            )
            await conn.execute(
                "SELECT set_config('ariadne.streaming_retention_operation',$1,true)",
                operation_id,
            )
            for location, cut in cuts.items():
                channel, partition = location.split("/", 1)
                summary = await conn.fetchrow(
                    """SELECT * FROM metadata_summaries
                       WHERE channel=$1 AND partition_id=$2""",
                    channel,
                    partition,
                )
                if summary is None or summary["retired_through"] != cut:
                    raise PostgresProtocolError("stream metadata cut did not converge")
            updated = await conn.execute(
                """UPDATE streaming_retention_operations_v1 SET status='COMPLETED',
                   outcome_json=$2::jsonb,updated_at=clock_timestamp()
                   WHERE revision_id=$1 AND operation_id=$3
                     AND status='DATA_COMPACTED'""",
                revision_id,
                json.dumps(outcome, sort_keys=True),
                operation_id,
            )
            if updated != "UPDATE 1":
                raise PostgresProtocolError("stream GC completion claim changed")
            await self.coordinator._assert_metadata_budget(conn, revision_id)
        if fault is not None:
            fault("stream_gc_completed")
        return self._outcome(outcome, False)

    async def _checkpoint_cuts(self, conn, revision_id, checkpoint):
        cuts: dict[str, int] = {}
        sources = await conn.fetch(
            """SELECT * FROM source_barrier_observations WHERE checkpoint_id=$1""",
            checkpoint["checkpoint_id"],
        )
        expected_sources = await conn.fetchval(
            "SELECT count(*) FROM revision_source_partitions WHERE revision_id=$1",
            revision_id,
        )
        if len(sources) != expected_sources:
            raise PostgresProtocolError("latest source barrier manifest is incomplete")
        for row in sources:
            root = await conn.fetchrow(
                "SELECT * FROM checkpoints WHERE checkpoint_id=$1",
                row["retention_ref_id"],
            )
            if root is None or (
                root["status"],
                root["channel"],
                root["partition_id"],
                root["cut_sequence"],
            ) != (
                "SEALED",
                row["channel"],
                row["partition_id"],
                row["cut_data_sequence"],
            ):
                raise PostgresProtocolError("latest source root is not authoritative")
            cuts[f"{row['channel']}/{row['partition_id']}"] = row["cut_data_sequence"]
        acks = await conn.fetch(
            "SELECT * FROM participant_checkpoint_acks WHERE checkpoint_id=$1",
            checkpoint["checkpoint_id"],
        )
        for ack in acks:
            for location, output in decoded(ack["output_barriers"]).items():
                channel, partition = location.split("/", 1)
                root = await conn.fetchrow(
                    """SELECT c.* FROM participant_output_roots r JOIN checkpoints c
                       ON c.checkpoint_id=r.retention_ref_id
                       WHERE r.checkpoint_id=$1 AND r.participant_id=$2
                         AND r.channel=$3 AND r.partition_id=$4""",
                    checkpoint["checkpoint_id"],
                    ack["participant_id"],
                    channel,
                    partition,
                )
                if root is None or (
                    root["status"],
                    root["cut_sequence"],
                ) != ("SEALED", output["cut_data_sequence"]):
                    raise PostgresProtocolError(
                        "latest output root is not authoritative"
                    )
                prior = cuts.get(location)
                if prior is not None and prior != output["cut_data_sequence"]:
                    raise PostgresProtocolError("checkpoint data cut is ambiguous")
                cuts[location] = output["cut_data_sequence"]
        cuts = {location: cut for location, cut in cuts.items() if cut > 0}
        if not cuts:
            raise PostgresProtocolError("stream GC requires nonzero checkpoint cuts")
        return cuts

    @staticmethod
    def _outcome(value: dict[str, Any], retry: bool) -> StreamingRetentionOutcome:
        return StreamingRetentionOutcome(
            value["operation_id"],
            value["revision_id"],
            value["checkpoint_id"],
            dict(value["cuts"]),
            value["reclaimed_records"],
            value["reclaimed_bytes"],
            dict(value["metadata_outcome"]),
            retry,
        )
