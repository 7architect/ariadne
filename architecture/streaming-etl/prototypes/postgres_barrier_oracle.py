"""Independent read-only oracle for PostgreSQL coordinator checkpoints."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from typing import Any

import asyncpg


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def content_key(payload: bytes, schema_id: str) -> str:
    return "sha256:" + hashlib.sha256(schema_id.encode() + b"\0" + payload).hexdigest()


def location_key(channel: str, partition: str) -> str:
    return f"{channel}/{partition}"


async def evaluate(dsn: str, schema: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
        raise ValueError("unsafe schema")
    conn = await asyncpg.connect(
        dsn, server_settings={"search_path": f'"{schema}",public'}
    )
    violations: list[str] = []
    observations: dict[str, Any] = {}
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            revisions = await conn.fetch(
                "SELECT * FROM deployment_revisions ORDER BY revision_id"
            )
            intents = await conn.fetch("SELECT * FROM checkpoint_intents_v2")
            intent_by_id = {row["checkpoint_id"]: row for row in intents}
            lifecycles = await conn.fetch("SELECT * FROM checkpoint_lifecycle_v2")
            lifecycle_by_id = {row["checkpoint_id"]: row for row in lifecycles}
            barriers = await conn.fetch("SELECT * FROM source_barrier_observations")
            acks = await conn.fetch("SELECT * FROM participant_checkpoint_acks")
            completions = await conn.fetch("SELECT * FROM checkpoint_completions_v2")
            completion_by_id = {row["checkpoint_id"]: row for row in completions}
            aborts = await conn.fetch("SELECT * FROM checkpoint_aborts_v2")
            abort_by_id = {row["checkpoint_id"]: row for row in aborts}
            events = await conn.fetch(
                "SELECT * FROM coordinator_authority_events ORDER BY revision_id,authority_ordinal"
            )
            logs = await conn.fetch(
                "SELECT * FROM partition_log_entries ORDER BY channel,partition_id,log_sequence"
            )
            heads = await conn.fetch("SELECT * FROM partition_log_heads")
            checkpoint_archives = {}
            log_archives = {}
            if await conn.fetchval(
                "SELECT to_regclass('checkpoint_archive_heads_v1') IS NOT NULL"
            ):
                checkpoint_archives = {
                    r["revision_id"]: r
                    for r in await conn.fetch(
                        "SELECT * FROM checkpoint_archive_heads_v1"
                    )
                }
                log_archives = {
                    (r["channel"], r["partition_id"]): r
                    for r in await conn.fetch(
                        "SELECT * FROM partition_log_archive_heads_v1"
                    )
                }
            records = await conn.fetch(
                "SELECT channel,partition_id,sequence,record_id FROM records"
            )
            tombstones = await conn.fetch(
                "SELECT channel,partition_id,sequence,record_id FROM compaction_tombstones"
            )
            record_identity = {
                (row["channel"], row["partition_id"], row["sequence"]): row["record_id"]
                for row in [*records, *tombstones]
            }
            objects = {
                row["object_key"]: row
                for row in await conn.fetch("SELECT * FROM checkpoint_snapshot_objects")
            }
            refs = {
                row["checkpoint_id"]: row
                for row in await conn.fetch("SELECT * FROM checkpoints")
            }
            events_by_revision: dict[str, list[asyncpg.Record]] = {}
            event_by_key: dict[tuple[str, int], asyncpg.Record] = {}
            for event in events:
                events_by_revision.setdefault(event["revision_id"], []).append(event)
                event_by_key[(event["revision_id"], event["authority_ordinal"])] = event
                identity = {
                    "revision_id": event["revision_id"],
                    "authority_ordinal": event["authority_ordinal"],
                    "event_type": event["event_type"],
                    "checkpoint_id": event["checkpoint_id"],
                    "recovery_id": event["recovery_id"],
                    "writer_epoch": event["writer_epoch"],
                    "payload": decoded(event["payload"]),
                }
                if event["event_digest"] != digest(identity):
                    violations.append(
                        f"authority_event_digest:{event['revision_id']}:"
                        f"{event['authority_ordinal']}"
                    )

            for revision in revisions:
                source_manifest = decoded(revision["source_manifest"])
                participant_manifest = decoded(revision["participant_manifest"])
                graph_identity = {
                    "revision_id": revision["revision_id"],
                    "deployment_id": revision["deployment_id"],
                    "graph_version": revision["graph_version"],
                    "code_manifest_digest": revision["code_manifest_digest"],
                    "state_contract_digest": revision["state_contract_digest"],
                    "source_manifest": source_manifest,
                    "participant_manifest": participant_manifest,
                }
                if revision["graph_digest"] != digest(graph_identity):
                    violations.append(f"revision_digest:{revision['revision_id']}")
                source_rows = [
                    {"channel": row["channel"], "partition": row["partition_id"]}
                    for row in await conn.fetch(
                        """SELECT channel,partition_id FROM revision_source_partitions
                           WHERE revision_id=$1 ORDER BY channel,partition_id""",
                        revision["revision_id"],
                    )
                ]
                if source_rows != source_manifest:
                    violations.append(f"revision_sources:{revision['revision_id']}")
                participant_rows = await conn.fetch(
                    """SELECT * FROM revision_participants WHERE revision_id=$1
                       ORDER BY participant_id""",
                    revision["revision_id"],
                )
                projected = [
                    {
                        "participant_id": row["participant_id"],
                        "inputs": decoded(row["input_manifest"]),
                        "outputs": decoded(row["output_manifest"]),
                        "code_digest": row["code_digest"],
                        "state_schema": row["state_schema"],
                        "state_version": row["state_version"],
                    }
                    for row in participant_rows
                ]
                if projected != participant_manifest:
                    violations.append(
                        f"revision_participants:{revision['revision_id']}"
                    )
                producers = {
                    (o["channel"], o["partition"]): p["participant_id"]
                    for p in participant_manifest
                    for o in p["outputs"]
                }
                expected_edges = sorted(
                    (
                        i["channel"],
                        i["partition"],
                        producers.get((i["channel"], i["partition"]), "SOURCE"),
                        p["participant_id"],
                    )
                    for p in participant_manifest
                    for i in p["inputs"]
                )
                edge_rows = await conn.fetch(
                    """SELECT channel,partition_id,producer_id,target_id
                    FROM revision_edges WHERE revision_id=$1""",
                    revision["revision_id"],
                )
                if sorted(tuple(x.values()) for x in edge_rows) != expected_edges:
                    violations.append(f"revision_edges:{revision['revision_id']}")
                revision_events = events_by_revision.get(revision["revision_id"], [])
                archive = checkpoint_archives.get(revision["revision_id"])
                authority_floor = (
                    0 if archive is None else archive["retired_authority_through"]
                )
                if [row["authority_ordinal"] for row in revision_events] != list(
                    range(authority_floor + 1, revision["next_authority_ordinal"])
                ):
                    violations.append(f"authority_coverage:{revision['revision_id']}")
                if archive is not None:
                    operation = await conn.fetchrow(
                        "SELECT * FROM checkpoint_retirement_operations_v1 WHERE revision_id=$1",
                        revision["revision_id"],
                    )
                    outcome = (
                        {} if operation is None else decoded(operation["outcome_json"])
                    )
                    if (
                        operation is None
                        or operation["status"] != "COMPLETED"
                        or (
                            operation["operation_id"] != archive["operation_id"]
                            or outcome.get("rolling_digest")
                            != archive["rolling_digest"]
                            or outcome.get("retired_checkpoint_through")
                            != archive["retired_checkpoint_through"]
                            or outcome.get("retired_authority_through")
                            != authority_floor
                        )
                    ):
                        violations.append(
                            f"checkpoint_archive_authority:{revision['revision_id']}"
                        )
                    if await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM checkpoint_retirement_rows_v1 WHERE operation_id=$1)",
                        archive["operation_id"],
                    ):
                        violations.append(
                            f"checkpoint_archive_transient:{revision['revision_id']}"
                        )
                    hot_ordinals = sorted(
                        i["checkpoint_ordinal"]
                        for i in intents
                        if i["revision_id"] == revision["revision_id"]
                    )
                    if hot_ordinals != list(
                        range(
                            archive["retired_checkpoint_through"] + 1,
                            revision["next_checkpoint_ordinal"],
                        )
                    ):
                        violations.append(
                            f"checkpoint_archive_prefix:{revision['revision_id']}"
                        )
                if await conn.fetchval(
                    "SELECT to_regclass('regional_archive_heads_v1') IS NOT NULL"
                ):
                    regional_archive = await conn.fetchrow(
                        "SELECT * FROM regional_archive_heads_v1 WHERE revision_id=$1",
                        revision["revision_id"],
                    )
                    if regional_archive is not None:
                        regional_operation = await conn.fetchrow(
                            "SELECT * FROM regional_retirement_operations_v1 WHERE revision_id=$1",
                            revision["revision_id"],
                        )
                        outcome = (
                            {}
                            if regional_operation is None
                            else decoded(regional_operation["outcome_json"])
                        )
                        if (
                            regional_operation is None
                            or regional_operation["status"] != "COMPLETED"
                            or regional_operation["operation_id"]
                            != regional_archive["operation_id"]
                            or outcome.get("rolling_digest")
                            != regional_archive["rolling_digest"]
                        ):
                            violations.append(
                                f"regional_archive_authority:{revision['revision_id']}"
                            )
                        else:
                            retired_plan_ids = decoded(regional_operation["plan_json"])[
                                "plan_ids"
                            ]
                            for table in (
                                "region_recovery_plans_v1",
                                "regional_recovery_workers_v1",
                                "regional_recovery_attempts_v1",
                                "regional_recovery_attempt_workers_v1",
                            ):
                                if await conn.fetchval(
                                    f"SELECT EXISTS(SELECT 1 FROM {table} WHERE plan_id=ANY($1::text[]))",
                                    retired_plan_ids,
                                ):
                                    violations.append(
                                        f"regional_archive_hot_rows:{revision['revision_id']}:{table}"
                                    )
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM regional_retirement_rows_v1 WHERE operation_id=$1)",
                            regional_archive["operation_id"],
                        ):
                            violations.append(
                                f"regional_archive_transient:{revision['revision_id']}"
                            )
                if len(revision_events) > revision["max_authority_events"]:
                    violations.append(f"authority_budget:{revision['revision_id']}")
                local_intents = [
                    row
                    for row in intents
                    if row["revision_id"] == revision["revision_id"]
                    and row["checkpoint_id"] in lifecycle_by_id
                ]
                retained = sum(
                    lifecycle_by_id[row["checkpoint_id"]]["status"] != "RETIRED"
                    for row in local_intents
                )
                inflight = sum(
                    lifecycle_by_id[row["checkpoint_id"]]["status"]
                    not in ("COMPLETED", "ABORTED", "RETIRED")
                    for row in local_intents
                )
                if retained > revision["max_retained_checkpoints"]:
                    violations.append(f"checkpoint_budget:{revision['revision_id']}")
                if inflight > revision["max_inflight_checkpoints"]:
                    violations.append(
                        f"checkpoint_inflight_budget:{revision['revision_id']}"
                    )
                oldest_age = await conn.fetchval(
                    """SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(i.created_at))
                       ::double precision FROM checkpoint_intents_v2 i
                       JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id)
                       WHERE i.revision_id=$1 AND l.status<>'RETIRED'""",
                    revision["revision_id"],
                )
                if oldest_age is not None and (
                    oldest_age > revision["max_checkpoint_age_seconds"]
                ):
                    violations.append(
                        f"checkpoint_age_budget:{revision['revision_id']}"
                    )
                metadata_bytes = await conn.fetchval(
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
                            WHERE i.revision_id=$1
                              AND a.snapshot_object_key=o.object_key))""",
                    revision["revision_id"],
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
                        metadata_bytes += await conn.fetchval(
                            f"""SELECT COALESCE(sum(octet_length(row_to_json(x)::text)),0)
                            FROM {table} x WHERE revision_id=$1""",
                            revision["revision_id"],
                        )
                if await conn.fetchval(
                    "SELECT to_regclass('join_retirement_rows_v1') IS NOT NULL"
                ):
                    metadata_bytes += await conn.fetchval(
                        """SELECT COALESCE(sum(octet_length(row_to_json(r)::text)),0)
                        FROM join_retirement_rows_v1 r JOIN join_retirement_manifests_v1 m USING(operation_id)
                        WHERE m.revision_id=$1""",
                        revision["revision_id"],
                    )
                if await conn.fetchval(
                    "SELECT to_regclass('partition_log_archive_heads_v1') IS NOT NULL"
                ):
                    metadata_bytes += await conn.fetchval(
                        """SELECT COALESCE(sum(octet_length(row_to_json(h)::text)),0)
                        FROM partition_log_archive_heads_v1 h WHERE EXISTS(SELECT 1 FROM revision_source_partitions s
                          WHERE s.revision_id=$1 AND s.channel=h.channel AND s.partition_id=h.partition_id)
                        OR EXISTS(SELECT 1 FROM revision_participants p,jsonb_array_elements(p.output_manifest) o
                          WHERE p.revision_id=$1 AND o->>'channel'=h.channel AND o->>'partition'=h.partition_id)""",
                        revision["revision_id"],
                    )
                for table in (
                    "regional_recovery_attempts_v1",
                    "regional_recovery_attempt_workers_v1",
                ):
                    if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table):
                        metadata_bytes += await conn.fetchval(
                            f"""SELECT COALESCE(sum(octet_length(row_to_json(x)::text)),0)
                            FROM {table} x JOIN region_recovery_plans_v1 p USING(plan_id)
                            WHERE p.revision_id=$1""",
                            revision["revision_id"],
                        )
                if metadata_bytes > revision["max_checkpoint_metadata_bytes"]:
                    violations.append(
                        f"checkpoint_byte_budget:{revision['revision_id']}"
                    )

            logs_by_location: dict[tuple[str, str], list[asyncpg.Record]] = {}
            for row in logs:
                logs_by_location.setdefault(
                    (row["channel"], row["partition_id"]), []
                ).append(row)
            for head in heads:
                location = (head["channel"], head["partition_id"])
                local = logs_by_location.get(location, [])
                log_archive = log_archives.get(location)
                log_floor = (
                    0 if log_archive is None else log_archive["retired_log_through"]
                )
                if [row["log_sequence"] for row in local] != list(
                    range(log_floor + 1, head["next_log_sequence"])
                ):
                    violations.append(f"log_coverage:{location[0]}:{location[1]}")
                next_data = await conn.fetchval(
                    "SELECT next_sequence FROM partitions WHERE channel=$1 AND partition_id=$2",
                    *location,
                )
                data_floor = (
                    next_data - 1 - sum(r["entry_kind"] == "DATA" for r in local)
                )
                if sorted(
                    r["data_sequence"] for r in local if r["entry_kind"] == "DATA"
                ) != list(range(data_floor + 1, next_data)):
                    violations.append(f"log_data_suffix:{location[0]}:{location[1]}")
                if (
                    head["used_log_entries"] != len(local)
                    or head["used_log_entries"] > head["max_log_entries"]
                ):
                    violations.append(f"log_budget:{location[0]}:{location[1]}")
                for row in local:
                    if row["entry_kind"] == "DATA":
                        key = (*location, row["data_sequence"])
                        if record_identity.get(key) != row["record_id"]:
                            violations.append(
                                f"log_data_authority:{location[0]}:{location[1]}:"
                                f"{row['log_sequence']}"
                            )
                    else:
                        preceding = [
                            item["data_sequence"]
                            for item in local
                            if item["entry_kind"] == "DATA"
                            and item["log_sequence"] < row["log_sequence"]
                        ]
                        if row["cut_data_sequence"] != max(
                            preceding, default=data_floor
                        ):
                            violations.append(
                                f"barrier_cut:{location[0]}:{location[1]}:"
                                f"{row['log_sequence']}"
                            )

            barrier_by_checkpoint: dict[str, list[asyncpg.Record]] = {}
            ack_by_checkpoint: dict[str, list[asyncpg.Record]] = {}
            for row in barriers:
                barrier_by_checkpoint.setdefault(row["checkpoint_id"], []).append(row)
            for row in acks:
                ack_by_checkpoint.setdefault(row["checkpoint_id"], []).append(row)

            for intent in intents:
                checkpoint_id = intent["checkpoint_id"]
                identity = {
                    "checkpoint_id": checkpoint_id,
                    "request_id": intent["request_id"],
                    "revision_id": intent["revision_id"],
                    "checkpoint_ordinal": intent["checkpoint_ordinal"],
                    "graph_digest": intent["graph_digest"],
                    "source_manifest": decoded(intent["source_manifest"]),
                    "participant_manifest": decoded(intent["participant_manifest"]),
                }
                expected_id = "checkpoint:" + digest(
                    {
                        "revision_id": intent["revision_id"],
                        "checkpoint_ordinal": intent["checkpoint_ordinal"],
                        "request_id": intent["request_id"],
                    }
                )
                if checkpoint_id != expected_id or intent["intent_digest"] != digest(
                    identity
                ):
                    violations.append(f"intent_identity:{checkpoint_id}")
                intent_event = event_by_key.get(
                    (intent["revision_id"], intent["intent_authority_ordinal"])
                )
                if intent_event is None or (
                    intent_event["event_type"],
                    intent_event["checkpoint_id"],
                    intent_event["writer_epoch"],
                    decoded(intent_event["payload"]).get("intent_digest"),
                ) != (
                    "INTENT",
                    checkpoint_id,
                    intent["created_by_epoch"],
                    intent["intent_digest"],
                ):
                    violations.append(f"intent_authority:{checkpoint_id}")
                lifecycle = lifecycle_by_id.get(checkpoint_id)
                if lifecycle is None:
                    violations.append(f"missing_lifecycle:{checkpoint_id}")
                    continue
                completed = checkpoint_id in completion_by_id
                aborted = checkpoint_id in abort_by_id
                if completed and aborted:
                    violations.append(f"complete_abort_conflict:{checkpoint_id}")
                if (lifecycle["status"] == "COMPLETED") != completed:
                    violations.append(f"completion_projection:{checkpoint_id}")
                if (lifecycle["status"] == "ABORTED") != aborted:
                    violations.append(f"abort_projection:{checkpoint_id}")
                local_barriers = sorted(
                    barrier_by_checkpoint.get(checkpoint_id, []),
                    key=lambda row: (row["channel"], row["partition_id"]),
                )
                barrier_manifest = [
                    {"channel": row["channel"], "partition": row["partition_id"]}
                    for row in local_barriers
                ]
                if lifecycle["status"] in ("ALIGNED", "COMPLETED") and (
                    barrier_manifest != decoded(intent["source_manifest"])
                ):
                    violations.append(f"aligned_manifest:{checkpoint_id}")
                for barrier in local_barriers:
                    barrier_event = event_by_key.get(
                        (intent["revision_id"], barrier["barrier_authority_ordinal"])
                    )
                    barrier_payload = (
                        {}
                        if barrier_event is None
                        else decoded(barrier_event["payload"])
                    )
                    if barrier_event is None or (
                        barrier_event["event_type"],
                        barrier_event["checkpoint_id"],
                        barrier_event["writer_epoch"],
                        barrier_payload.get("channel"),
                        barrier_payload.get("partition"),
                        barrier_payload.get("log_sequence"),
                        barrier_payload.get("cut_data_sequence"),
                    ) != (
                        "SOURCE_BARRIERS",
                        checkpoint_id,
                        barrier["writer_epoch"],
                        barrier["channel"],
                        barrier["partition_id"],
                        barrier["barrier_log_sequence"],
                        barrier["cut_data_sequence"],
                    ):
                        violations.append(
                            f"barrier_event:{checkpoint_id}:"
                            f"{barrier['channel']}:{barrier['partition_id']}"
                        )
                    log = next(
                        (
                            row
                            for row in logs
                            if (
                                row["channel"],
                                row["partition_id"],
                                row["log_sequence"],
                            )
                            == (
                                barrier["channel"],
                                barrier["partition_id"],
                                barrier["barrier_log_sequence"],
                            )
                        ),
                        None,
                    )
                    ref = refs.get(barrier["retention_ref_id"])
                    if log is None or (
                        log["entry_kind"],
                        log["barrier_role"],
                        log["checkpoint_id"],
                        log["cut_data_sequence"],
                        log["intent_digest"],
                    ) != (
                        "BARRIER",
                        "SOURCE",
                        checkpoint_id,
                        barrier["cut_data_sequence"],
                        intent["intent_digest"],
                    ):
                        violations.append(
                            f"barrier_authority:{checkpoint_id}:"
                            f"{barrier['channel']}:{barrier['partition_id']}"
                        )
                    if ref is None or (
                        ref["channel"],
                        ref["partition_id"],
                        ref["cut_sequence"],
                        ref["status"],
                    ) != (
                        barrier["channel"],
                        barrier["partition_id"],
                        barrier["cut_data_sequence"],
                        "RELEASED" if lifecycle["status"] == "ABORTED" else "SEALED",
                    ):
                        violations.append(
                            f"barrier_retention:{checkpoint_id}:"
                            f"{barrier['channel']}:{barrier['partition_id']}"
                        )

                participant_specs = {
                    item["participant_id"]: item
                    for item in decoded(intent["participant_manifest"])
                }
                local_acks = sorted(
                    ack_by_checkpoint.get(checkpoint_id, []),
                    key=lambda row: row["participant_id"],
                )
                for ack in local_acks:
                    spec = participant_specs.get(ack["participant_id"])
                    obj = objects.get(ack["snapshot_object_key"])
                    if spec is None or obj is None or obj["published_at"] is None:
                        violations.append(
                            f"participant_snapshot:{checkpoint_id}:{ack['participant_id']}"
                        )
                        continue
                    if (
                        ack["code_digest"],
                        ack["state_schema"],
                        ack["state_version"],
                    ) != (
                        spec["code_digest"],
                        spec["state_schema"],
                        spec["state_version"],
                    ):
                        violations.append(
                            f"participant_contract:{checkpoint_id}:"
                            f"{ack['participant_id']}"
                        )
                    payload = bytes(obj["payload"])
                    if (
                        hashlib.sha256(payload).hexdigest(),
                        content_key(payload, ack["state_schema"]),
                    ) != (
                        ack["snapshot_payload_digest"],
                        ack["snapshot_object_key"],
                    ):
                        violations.append(
                            f"participant_snapshot:{checkpoint_id}:{ack['participant_id']}"
                        )
                    barrier_vector = decoded(ack["barrier_vector"])
                    expected_vector = {}
                    for item in spec["inputs"]:
                        match = next(
                            (
                                row
                                for row in local_barriers
                                if (row["channel"], row["partition_id"])
                                == (item["channel"], item["partition"])
                            ),
                            None,
                        )
                        if match is not None:
                            expected_vector[
                                location_key(item["channel"], item["partition"])
                            ] = {
                                "cut_data_sequence": match["cut_data_sequence"],
                                "barrier_log_sequence": match["barrier_log_sequence"],
                            }
                        else:
                            output_log = next(
                                (
                                    row
                                    for row in logs
                                    if row["checkpoint_id"] == checkpoint_id
                                    and row["barrier_role"] == "OUTPUT"
                                    and row["channel"] == item["channel"]
                                    and row["partition_id"] == item["partition"]
                                ),
                                None,
                            )
                            if output_log is not None:
                                expected_vector[
                                    location_key(item["channel"], item["partition"])
                                ] = {
                                    "cut_data_sequence": output_log[
                                        "cut_data_sequence"
                                    ],
                                    "barrier_log_sequence": output_log["log_sequence"],
                                }
                    if barrier_vector != expected_vector:
                        violations.append(
                            f"participant_vector:{checkpoint_id}:{ack['participant_id']}"
                        )
                    for item in spec["inputs"]:
                        key = location_key(item["channel"], item["partition"])
                        expected_input = expected_vector.get(key)
                        if expected_input is None:
                            violations.append(
                                f"participant_missing_input:{checkpoint_id}:"
                                f"{ack['participant_id']}:{key}"
                            )
                            continue
                        scanned = await conn.fetchrow(
                            """SELECT * FROM participant_scan_positions
                               WHERE revision_id=$1 AND participant_id=$2
                                 AND channel=$3 AND partition_id=$4""",
                            intent["revision_id"],
                            ack["participant_id"],
                            item["channel"],
                            item["partition"],
                        )
                        if scanned is None or (
                            scanned["scanned_log_through"]
                            < expected_input["barrier_log_sequence"]
                        ):
                            violations.append(
                                f"participant_scan:{checkpoint_id}:"
                                f"{ack['participant_id']}:{key}"
                            )
                    expected_floor = {
                        key: value["cut_data_sequence"] + 1
                        for key, value in expected_vector.items()
                    }
                    if decoded(ack["replay_tail_floor"]) != expected_floor:
                        violations.append(
                            f"participant_tail_floor:{checkpoint_id}:"
                            f"{ack['participant_id']}"
                        )
                    output_barriers = decoded(ack["output_barriers"])
                    expected_outputs = {
                        location_key(item["channel"], item["partition"])
                        for item in spec["outputs"]
                    }
                    if set(output_barriers) != expected_outputs:
                        violations.append(
                            f"participant_output_manifest:{checkpoint_id}:"
                            f"{ack['participant_id']}"
                        )
                    for key, output in output_barriers.items():
                        channel, partition = key.split("/", 1)
                        output_log = next(
                            (
                                row
                                for row in logs
                                if (
                                    row["channel"],
                                    row["partition_id"],
                                    row["log_sequence"],
                                )
                                == (channel, partition, output["barrier_log_sequence"])
                            ),
                            None,
                        )
                        if output_log is None or (
                            output_log["entry_kind"],
                            output_log["barrier_role"],
                            output_log["checkpoint_id"],
                            output_log["cut_data_sequence"],
                            output_log["intent_digest"],
                        ) != (
                            "BARRIER",
                            "OUTPUT",
                            checkpoint_id,
                            output["cut_data_sequence"],
                            intent["intent_digest"],
                        ):
                            violations.append(
                                f"participant_output_barrier:{checkpoint_id}:"
                                f"{ack['participant_id']}:{key}"
                            )
                        root_id = await conn.fetchval(
                            """SELECT retention_ref_id
                            FROM participant_output_roots WHERE checkpoint_id=$1
                              AND participant_id=$2 AND channel=$3 AND partition_id=$4""",
                            checkpoint_id,
                            ack["participant_id"],
                            channel,
                            partition,
                        )
                        root = refs.get(root_id)
                        if root is None or (
                            root["channel"],
                            root["partition_id"],
                            root["cut_sequence"],
                            root["status"],
                        ) != (
                            channel,
                            partition,
                            output["cut_data_sequence"],
                            "RELEASED" if aborted else "SEALED",
                        ):
                            violations.append(
                                f"participant_output_retention:{checkpoint_id}:{key}"
                            )
                    ack_identity = {
                        "checkpoint_id": checkpoint_id,
                        "participant_id": ack["participant_id"],
                        "intent_digest": intent["intent_digest"],
                        "barrier_vector": barrier_vector,
                        "snapshot_object_key": ack["snapshot_object_key"],
                        "snapshot_payload_digest": ack["snapshot_payload_digest"],
                        "mailbox_highwater": ack["mailbox_highwater"],
                        "replay_tail_floor": decoded(ack["replay_tail_floor"]),
                        "output_barriers": decoded(ack["output_barriers"]),
                        "state_schema": ack["state_schema"],
                        "state_version": ack["state_version"],
                        "code_digest": ack["code_digest"],
                    }
                    if ack["ack_digest"] != digest(ack_identity):
                        violations.append(
                            f"participant_ack_digest:{checkpoint_id}:"
                            f"{ack['participant_id']}"
                        )
                    ack_event = event_by_key.get(
                        (intent["revision_id"], ack["ack_authority_ordinal"])
                    )
                    ack_payload = (
                        {} if ack_event is None else decoded(ack_event["payload"])
                    )
                    if ack_event is None or (
                        ack_event["event_type"],
                        ack_event["checkpoint_id"],
                        ack_event["writer_epoch"],
                        ack_payload.get("participant_id"),
                        ack_payload.get("ack_digest"),
                    ) != (
                        "PARTICIPANT_ACK",
                        checkpoint_id,
                        ack["writer_epoch"],
                        ack["participant_id"],
                        ack["ack_digest"],
                    ):
                        violations.append(
                            f"participant_ack_authority:{checkpoint_id}:"
                            f"{ack['participant_id']}"
                        )

                completion = completion_by_id.get(checkpoint_id)
                if completion is not None:
                    completion_event = event_by_key.get(
                        (
                            intent["revision_id"],
                            completion["completion_authority_ordinal"],
                        )
                    )
                    completion_payload = (
                        {}
                        if completion_event is None
                        else decoded(completion_event["payload"])
                    )
                    if completion_event is None or (
                        completion_event["event_type"],
                        completion_event["checkpoint_id"],
                        completion_event["writer_epoch"],
                        completion_payload.get("completion_digest"),
                    ) != (
                        "COMPLETE",
                        checkpoint_id,
                        completion["writer_epoch"],
                        completion["completion_digest"],
                    ):
                        violations.append(f"completion_authority:{checkpoint_id}")
                    source_digest = digest(
                        [
                            {
                                "channel": row["channel"],
                                "partition": row["partition_id"],
                                "barrier_log_sequence": row["barrier_log_sequence"],
                                "cut_data_sequence": row["cut_data_sequence"],
                                "intent_digest": row["intent_digest"],
                            }
                            for row in local_barriers
                        ]
                    )
                    ack_digest = digest(
                        [
                            {
                                "participant_id": row["participant_id"],
                                "ack_digest": row["ack_digest"],
                            }
                            for row in local_acks
                        ]
                    )
                    completion_identity = {
                        "checkpoint_id": checkpoint_id,
                        "revision_id": intent["revision_id"],
                        "intent_digest": intent["intent_digest"],
                        "source_barriers_digest": source_digest,
                        "participant_acks_digest": ack_digest,
                    }
                    if (
                        completion["source_barriers_digest"],
                        completion["participant_acks_digest"],
                        completion["completion_digest"],
                    ) != (source_digest, ack_digest, digest(completion_identity)):
                        violations.append(f"completion_digest:{checkpoint_id}")
                    if [row["participant_id"] for row in local_acks] != sorted(
                        participant_specs
                    ):
                        violations.append(f"completion_participants:{checkpoint_id}")
                abort = abort_by_id.get(checkpoint_id)
                if abort is not None:
                    abort_event = event_by_key.get(
                        (intent["revision_id"], abort["abort_authority_ordinal"])
                    )
                    abort_payload = (
                        {} if abort_event is None else decoded(abort_event["payload"])
                    )
                    if abort_event is None or (
                        abort_event["event_type"],
                        abort_event["checkpoint_id"],
                        abort_event["writer_epoch"],
                        abort_payload.get("reason"),
                    ) != (
                        "ABORT",
                        checkpoint_id,
                        abort["writer_epoch"],
                        abort["reason"],
                    ):
                        violations.append(f"abort_authority:{checkpoint_id}")

            recovery_plans = await conn.fetch("SELECT * FROM recovery_plans_v2")
            has_regions = await conn.fetchval(
                "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL"
            )
            for plan in recovery_plans:
                region = (
                    await conn.fetchrow(
                        "SELECT * FROM region_recovery_plans_v1 WHERE plan_id=$1",
                        plan["recovery_id"],
                    )
                    if has_regions
                    else None
                )
                if region is None and plan["recovery_id"] != "recovery:" + digest(
                    {
                        "revision_id": plan["revision_id"],
                        "request_id": plan["request_id"],
                        "fence_authority_ordinal": plan["fence_authority_ordinal"],
                    }
                ):
                    violations.append(f"recovery_identity:{plan['recovery_id']}")
                fence = next(
                    (
                        event
                        for event in events_by_revision.get(plan["revision_id"], [])
                        if event["authority_ordinal"] == plan["fence_authority_ordinal"]
                    ),
                    None,
                )
                if fence is None or (
                    fence["event_type"],
                    fence["recovery_id"],
                    fence["writer_epoch"],
                    decoded(fence["payload"]).get("request_id"),
                ) != (
                    "RECOVERY_FENCE",
                    plan["recovery_id"],
                    plan["writer_epoch"],
                    plan["request_id"],
                ):
                    violations.append(f"recovery_fence:{plan['recovery_id']}")
                completion = completion_by_id.get(plan["checkpoint_id"])
                if completion is None or (
                    completion["completion_authority_ordinal"]
                    >= plan["fence_authority_ordinal"]
                ):
                    violations.append(f"recovery_selection:{plan['recovery_id']}")
                    continue
                later = await conn.fetchval(
                    """SELECT count(*) FROM checkpoint_intents_v2 i
                       JOIN checkpoint_completions_v2 c USING(checkpoint_id)
                       WHERE i.revision_id=$1
                         AND i.checkpoint_ordinal>(SELECT checkpoint_ordinal
                           FROM checkpoint_intents_v2 WHERE checkpoint_id=$2)
                         AND c.completion_authority_ordinal<$3""",
                    plan["revision_id"],
                    plan["checkpoint_id"],
                    plan["fence_authority_ordinal"],
                )
                if later and region is None:
                    violations.append(f"recovery_not_latest:{plan['recovery_id']}")
                intent = intent_by_id.get(plan["checkpoint_id"])
                if intent is None:
                    violations.append(f"recovery_intent:{plan['recovery_id']}")
                    continue
                local_acks = sorted(
                    ack_by_checkpoint.get(plan["checkpoint_id"], []),
                    key=lambda row: row["participant_id"],
                )
                participant_snapshots = {
                    row["participant_id"]: row["snapshot_object_key"]
                    for row in local_acks
                }
                if region is not None:
                    region_nodes = set(decoded(region["region_json"]))
                    participant_snapshots = {
                        k: v
                        for k, v in participant_snapshots.items()
                        if k in region_nodes
                    }
                    region_identity = {
                        key: region[key]
                        for key in (
                            "plan_id",
                            "request_id",
                            "revision_id",
                            "checkpoint_id",
                            "failed_node",
                            "graph_manifest_digest",
                            "completion_digest",
                            "completion_authority_ordinal",
                            "fence_authority_ordinal",
                        )
                    }
                    for key in ("region", "boundaries", "source_rewinds", "reasons"):
                        region_identity[key] = decoded(region[key + "_json"])
                    if region["plan_digest"] != digest(region_identity) or (
                        region["checkpoint_id"],
                        region["revision_id"],
                        region["completion_digest"],
                        region["fence_authority_ordinal"],
                    ) != (
                        plan["checkpoint_id"],
                        plan["revision_id"],
                        completion["completion_digest"],
                        plan["fence_authority_ordinal"],
                    ):
                        violations.append(
                            f"regional_plan_authority:{plan['recovery_id']}"
                        )
                    region_targets = {
                        location_key(b["channel"], b["partition"]): b[
                            "target_data_sequence"
                        ]
                        for b in decoded(region["boundaries_json"])
                    }
                    if decoded(plan["target_data_sequences"]) != region_targets:
                        violations.append(
                            f"regional_boundary_targets:{plan['recovery_id']}"
                        )
                    if await conn.fetchval(
                        "SELECT to_regclass('regional_recovery_workers_v1') IS NOT NULL"
                    ):
                        generations = await conn.fetch(
                            """SELECT * FROM regional_recovery_workers_v1
                            WHERE plan_id=$1 ORDER BY participant_id""",
                            plan["recovery_id"],
                        )
                        expected_nodes = region_nodes - {"SOURCE"}
                        if {g["participant_id"] for g in generations} != expected_nodes:
                            violations.append(
                                f"regional_generation_manifest:{plan['recovery_id']}"
                            )
                        effective = {
                            g["participant_id"]: {
                                "effective_owner_id": g["recovery_owner_id"],
                                "effective_owner_epoch": g["recovery_owner_epoch"],
                            }
                            for g in generations
                        }
                        if await conn.fetchval(
                            "SELECT to_regclass('regional_recovery_attempts_v1') IS NOT NULL"
                        ):
                            attempts = await conn.fetch(
                                """SELECT * FROM regional_recovery_attempts_v1
                                WHERE plan_id=$1 ORDER BY attempt_ordinal""",
                                plan["recovery_id"],
                            )
                            if [a["attempt_ordinal"] for a in attempts] != list(
                                range(1, len(attempts) + 1)
                            ) or not attempts:
                                violations.append(
                                    f"regional_attempt_prefix:{plan['recovery_id']}"
                                )
                            previous = {
                                g["participant_id"]: (
                                    g["old_owner_id"],
                                    g["old_owner_epoch"],
                                )
                                for g in generations
                            }
                            for attempt in attempts:
                                rows = await conn.fetch(
                                    """SELECT * FROM regional_recovery_attempt_workers_v1
                                    WHERE plan_id=$1 AND attempt_ordinal=$2 ORDER BY participant_id""",
                                    plan["recovery_id"],
                                    attempt["attempt_ordinal"],
                                )
                                manifest = [
                                    {
                                        key: r[key]
                                        for key in (
                                            "participant_id",
                                            "prior_owner_id",
                                            "prior_owner_epoch",
                                            "effective_owner_epoch",
                                            "generation_digest",
                                        )
                                    }
                                    for r in rows
                                ]
                                identity = {
                                    "plan_id": plan["recovery_id"],
                                    "attempt_ordinal": attempt["attempt_ordinal"],
                                    "request_id": attempt["request_id"],
                                    "owner_id": attempt["owner_id"],
                                    "workers": manifest,
                                }
                                if {
                                    r["participant_id"] for r in rows
                                } != expected_nodes or (
                                    decoded(attempt["worker_manifest"]) != manifest
                                    or attempt["attempt_digest"] != digest(identity)
                                ):
                                    violations.append(
                                        f"regional_attempt_manifest:{plan['recovery_id']}:{attempt['attempt_ordinal']}"
                                    )
                                for r in rows:
                                    frozen = next(
                                        (
                                            g
                                            for g in generations
                                            if g["participant_id"]
                                            == r["participant_id"]
                                        ),
                                        None,
                                    )
                                    if (
                                        frozen is None
                                        or previous.get(r["participant_id"])
                                        != (r["prior_owner_id"], r["prior_owner_epoch"])
                                        or (
                                            r["effective_owner_id"]
                                            != attempt["owner_id"]
                                            or r["effective_owner_epoch"]
                                            != r["prior_owner_epoch"] + 1
                                            or r["generation_digest"]
                                            != frozen["generation_digest"]
                                        )
                                    ):
                                        violations.append(
                                            f"regional_attempt_chain:{plan['recovery_id']}:{r['participant_id']}"
                                        )
                                    previous[r["participant_id"]] = (
                                        r["effective_owner_id"],
                                        r["effective_owner_epoch"],
                                    )
                                effective = {r["participant_id"]: r for r in rows}
                        for generation in generations:
                            pid = generation["participant_id"]
                            current = effective.get(pid)
                            current_owner = (
                                generation["recovery_owner_id"]
                                if current is None
                                else current["effective_owner_id"]
                            )
                            current_epoch = (
                                generation["recovery_owner_epoch"]
                                if current is None
                                else current["effective_owner_epoch"]
                            )
                            acknowledgement = next(
                                (a for a in local_acks if a["participant_id"] == pid),
                                None,
                            )
                            worker = await conn.fetchrow(
                                """SELECT * FROM join_workers
                                WHERE revision_id=$1 AND participant_id=$2""",
                                plan["revision_id"],
                                pid,
                            )
                            progress = await conn.fetchrow(
                                """SELECT * FROM join_recoveries_v3
                                WHERE revision_id=$1 AND participant_id=$2 AND recovery_id=$3""",
                                plan["revision_id"],
                                pid,
                                plan["recovery_id"],
                            )
                            generation_identity = {
                                key: generation[key]
                                for key in (
                                    "plan_id",
                                    "revision_id",
                                    "participant_id",
                                    "checkpoint_id",
                                    "old_owner_id",
                                    "old_owner_epoch",
                                    "recovery_owner_id",
                                    "recovery_owner_epoch",
                                    "worker_config_digest",
                                    "snapshot_object_key",
                                    "snapshot_payload_digest",
                                    "mailbox_highwater",
                                    "target_apply",
                                )
                            }
                            if (
                                acknowledgement is None
                                or worker is None
                                or progress is None
                                or (
                                    generation["generation_digest"]
                                    != digest(generation_identity)
                                    or generation["checkpoint_id"]
                                    != plan["checkpoint_id"]
                                    or generation["revision_id"] != plan["revision_id"]
                                    or generation["recovery_owner_epoch"]
                                    != generation["old_owner_epoch"] + 1
                                    or generation["worker_config_digest"]
                                    != worker["config_digest"]
                                    or (
                                        generation["snapshot_object_key"],
                                        generation["snapshot_payload_digest"],
                                        generation["mailbox_highwater"],
                                    )
                                    != (
                                        acknowledgement["snapshot_object_key"],
                                        acknowledgement["snapshot_payload_digest"],
                                        acknowledgement["mailbox_highwater"],
                                    )
                                    or generation["target_apply"]
                                    != progress["target_apply"]
                                )
                            ):
                                violations.append(
                                    f"regional_generation_authority:{plan['recovery_id']}:{pid}"
                                )
                                continue
                            if generation["status"] in ("READY", "ACTIVE") and (
                                progress["status"] != "READY"
                                or progress["replayed_through"]
                                != progress["target_apply"]
                            ):
                                violations.append(
                                    f"regional_generation_replay:{plan['recovery_id']}:{pid}"
                                )
                            if generation["status"] != "ACTIVE" and (
                                worker["owner_id"],
                                worker["owner_epoch"],
                                worker["phase"],
                            ) != (
                                current_owner,
                                current_epoch,
                                "RESTORING",
                            ):
                                violations.append(
                                    f"regional_generation_fence:{plan['recovery_id']}:{pid}"
                                )
                            if (
                                generation["status"] == "ACTIVE"
                                and worker["owner_epoch"] == current_epoch
                                and (
                                    worker["owner_id"] != current_owner
                                    or worker["phase"] != "LIVE"
                                )
                            ):
                                violations.append(
                                    f"regional_generation_activation:{plan['recovery_id']}:{pid}"
                                )
                plan_identity = {
                    "recovery_id": plan["recovery_id"],
                    "request_id": plan["request_id"],
                    "revision_id": plan["revision_id"],
                    "checkpoint_id": plan["checkpoint_id"],
                    "completion_digest": completion["completion_digest"],
                    "fence_authority_ordinal": plan["fence_authority_ordinal"],
                    "target_data_sequences": decoded(plan["target_data_sequences"]),
                    "participant_snapshots": participant_snapshots,
                }
                if (
                    decoded(plan["participant_snapshots"]) != participant_snapshots
                    or plan["plan_digest"] != digest(plan_identity)
                    or intent["revision_id"] != plan["revision_id"]
                ):
                    violations.append(f"recovery_plan_digest:{plan['recovery_id']}")
                targets = decoded(plan["target_data_sequences"])
                recovery_barriers = list(
                    barrier_by_checkpoint.get(plan["checkpoint_id"], [])
                )
                if region is not None:
                    recovery_barriers = []
                    for boundary in decoded(region["boundaries_json"]):
                        cut = await conn.fetchrow(
                            """SELECT * FROM region_edge_cuts_v1
                            WHERE checkpoint_id=$1 AND edge_id=$2""",
                            plan["checkpoint_id"],
                            boundary["edge_id"],
                        )
                        if cut is None:
                            violations.append(
                                f"regional_boundary_cut:{plan['recovery_id']}:{boundary['edge_id']}"
                            )
                        else:
                            recovery_barriers.append(cut)
                else:
                    for root in await conn.fetch(
                        """SELECT r.retention_ref_id,l.channel,l.partition_id,
                        l.cut_data_sequence FROM participant_output_roots r JOIN partition_log_entries l
                        ON l.checkpoint_id=r.checkpoint_id AND l.channel=r.channel AND l.partition_id=r.partition_id
                        AND l.barrier_role='OUTPUT' WHERE r.checkpoint_id=$1""",
                        plan["checkpoint_id"],
                    ):
                        recovery_barriers.append(root)
                for barrier in recovery_barriers:
                    key = location_key(barrier["channel"], barrier["partition_id"])
                    target = targets.get(key)
                    if target is None or target < barrier["cut_data_sequence"]:
                        violations.append(
                            f"recovery_target:{plan['recovery_id']}:{key}"
                        )
                        continue
                    tail_count = sum(
                        row["entry_kind"] == "DATA"
                        and row["channel"] == barrier["channel"]
                        and row["partition_id"] == barrier["partition_id"]
                        and barrier["cut_data_sequence"]
                        < row["data_sequence"]
                        <= target
                        for row in logs
                    )
                    if tail_count != target - barrier["cut_data_sequence"]:
                        violations.append(f"recovery_tail:{plan['recovery_id']}:{key}")
                    ref = refs.get(barrier["retention_ref_id"])
                    if ref is None or ref["status"] != "SEALED":
                        violations.append(
                            f"recovery_retention:{plan['recovery_id']}:{key}"
                        )
                    live_tail = await conn.fetch(
                        """SELECT r.sequence,r.object_key,r.payload_digest,r.payload_size,
                                  r.schema_id,o.payload,o.payload_digest AS object_digest,
                                  o.payload_size AS object_size,
                                  o.schema_id AS object_schema,o.published_at
                           FROM records r JOIN objects o USING(object_key)
                           WHERE r.channel=$1 AND r.partition_id=$2
                             AND r.sequence>$3 AND r.sequence<=$4
                           ORDER BY r.sequence""",
                        barrier["channel"],
                        barrier["partition_id"],
                        barrier["cut_data_sequence"],
                        target,
                    )
                    if len(live_tail) != target - barrier["cut_data_sequence"]:
                        violations.append(
                            f"recovery_payload_tail:{plan['recovery_id']}:{key}"
                        )
                    for row in live_tail:
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
                            violations.append(
                                f"recovery_payload_identity:{plan['recovery_id']}:{key}:"
                                f"{row['sequence']}"
                            )
                if plan["status"] == "READY":
                    ready_event = (
                        event_by_key.get(
                            (plan["revision_id"], plan["ready_authority_ordinal"])
                        )
                        if plan["ready_authority_ordinal"] is not None
                        else None
                    )
                    ready_payload = (
                        {} if ready_event is None else decoded(ready_event["payload"])
                    )
                    if ready_event is None or (
                        ready_event["event_type"],
                        ready_event["recovery_id"],
                        ready_event["writer_epoch"],
                        ready_payload.get("plan_digest"),
                    ) != (
                        "RECOVERY_READY",
                        plan["recovery_id"],
                        plan["ready_writer_epoch"],
                        plan["plan_digest"],
                    ):
                        violations.append(
                            f"recovery_ready_authority:{plan['recovery_id']}"
                        )

            observations = {
                "revisions": len(revisions),
                "log_entries": len(logs),
                "checkpoint_intents": len(intents),
                "source_barriers": len(barriers),
                "participant_acks": len(acks),
                "completions": len(completions),
                "recovery_plans": len(recovery_plans),
            }
    finally:
        await conn.close()
    return {
        "passed": not violations,
        "violations": violations,
        "observations": observations,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("schema")
    args = parser.parse_args()
    try:
        report = await evaluate(args.dsn, args.schema)
    except Exception as exc:  # noqa: BLE001 -- malformed storage must produce JSON evidence
        report = {
            "passed": False,
            "violations": [f"oracle_totality:{type(exc).__name__}:{exc}"],
            "observations": {},
        }
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
