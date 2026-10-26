"""Independent read-only oracle for the PostgreSQL stream data plane."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from typing import Any

import asyncpg


def payload_digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def content_object_key(payload: bytes, schema_id: str) -> str:
    return "sha256:" + hashlib.sha256(schema_id.encode() + b"\0" + payload).hexdigest()


def identity_digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


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
            partitions = await conn.fetch(
                "SELECT * FROM partitions ORDER BY channel,partition_id"
            )
            records = await conn.fetch("SELECT * FROM records")
            tombstones = await conn.fetch("SELECT * FROM compaction_tombstones")
            summaries = await conn.fetch("SELECT * FROM metadata_summaries")
            metadata_operations = await conn.fetch("SELECT * FROM metadata_operations")
            operation_by_id = {row["operation_id"]: row for row in metadata_operations}
            summary_by_location = {
                (row["channel"], row["partition_id"]): row for row in summaries
            }
            objects = {
                row["object_key"]: row
                for row in await conn.fetch("SELECT * FROM objects")
            }
            object_identities = {
                row["object_key"]: row
                for row in await conn.fetch("SELECT * FROM object_identities")
            }
            by_location = {
                (row["channel"], row["partition_id"], row["sequence"]): row
                for row in records
            }
            tomb_by_location = {
                (row["channel"], row["partition_id"], row["sequence"]): row
                for row in tombstones
            }
            all_by_location = {**tomb_by_location, **by_location}
            for part in partitions:
                location = (part["channel"], part["partition_id"])
                live = sorted(
                    row["sequence"]
                    for row in records
                    if (row["channel"], row["partition_id"]) == location
                )
                dead = sorted(
                    row["sequence"]
                    for row in tombstones
                    if (row["channel"], row["partition_id"]) == location
                )
                summary = summary_by_location.get(location)
                retired = 0 if summary is None else summary["retired_through"]
                if sorted(live + dead) != list(
                    range(retired + 1, part["next_sequence"])
                ):
                    violations.append(f"sequence_coverage:{location[0]}:{location[1]}")
                if set(live) & set(dead):
                    violations.append(f"sequence_overlap:{location[0]}:{location[1]}")
                local_records = [
                    row
                    for row in records
                    if (row["channel"], row["partition_id"]) == location
                ]
                used_bytes = sum(
                    objects[key]["payload_size"]
                    for key in {row["object_key"] for row in local_records}
                    if key in objects
                )
                if (
                    part["used_records"] != len(local_records)
                    or part["used_bytes"] != used_bytes
                    or part["used_records"] > part["max_records"]
                    or part["used_bytes"] > part["max_bytes"]
                    or part["used_metadata_records"]
                    != sum(
                        1
                        for attempt in await conn.fetch(
                            "SELECT status FROM append_attempts WHERE channel=$1 "
                            "AND partition_id=$2",
                            *location,
                        )
                        if attempt["status"] == "APPENDED"
                    )
                    or part["used_metadata_records"] > part["max_metadata_records"]
                ):
                    violations.append(f"capacity:{location[0]}:{location[1]}")
            for row in records:
                obj = objects.get(row["object_key"])
                identity = object_identities.get(row["object_key"])
                if (
                    obj is None
                    or obj["published_at"] is None
                    or (
                        obj["payload_digest"],
                        obj["payload_size"],
                        obj["schema_id"],
                        payload_digest(bytes(obj["payload"])),
                    )
                    != (
                        row["payload_digest"],
                        row["payload_size"],
                        row["schema_id"],
                        row["payload_digest"],
                    )
                ):
                    violations.append(
                        f"object_identity:{row['channel']}:{row['partition_id']}:{row['sequence']}"
                    )
                if identity is None or (
                    identity["payload_digest"],
                    identity["payload_size"],
                    identity["schema_id"],
                ) != (row["payload_digest"], row["payload_size"], row["schema_id"]):
                    violations.append(f"object_registry:{row['object_key']}")
            for row in tombstones:
                identity = object_identities.get(row["object_key"])
                if identity is None or (
                    identity["payload_digest"],
                    identity["payload_size"],
                    identity["schema_id"],
                ) != (row["payload_digest"], row["payload_size"], row["schema_id"]):
                    violations.append(f"object_history:{row['object_key']}")
            for key, obj in objects.items():
                identity = object_identities.get(key)
                if identity is None or (
                    identity["payload_digest"],
                    identity["payload_size"],
                    identity["schema_id"],
                ) != (obj["payload_digest"], obj["payload_size"], obj["schema_id"]):
                    violations.append(f"object_registry:{key}")
                if key.startswith("sha256:") and key != content_object_key(
                    bytes(obj["payload"]), obj["schema_id"]
                ):
                    violations.append(f"content_address:{key}")
            referenced = {row["object_key"] for row in records}
            for key, obj in objects.items():
                if obj["published_at"] is not None and key not in referenced:
                    violations.append(f"published_orphan:{key}")

            attempts = await conn.fetch("SELECT * FROM append_attempts")
            attempt_by_id = {row["attempt_id"]: row for row in attempts}
            completions = await conn.fetch("SELECT * FROM turn_completions")
            consumers = await conn.fetch("SELECT * FROM consumers")
            consumer_by_id = {row["consumer_id"]: row for row in consumers}
            for consumer in consumers:
                part = next(
                    row
                    for row in partitions
                    if (row["channel"], row["partition_id"])
                    == (consumer["channel"], consumer["partition_id"])
                )
                live = [
                    row["sequence"]
                    for row in records
                    if (row["channel"], row["partition_id"])
                    == (consumer["channel"], consumer["partition_id"])
                ]
                floor = min(live, default=part["next_sequence"])
                recovery_root = consumer["completed_through"] + 1
                if recovery_root < floor or recovery_root > part["next_sequence"]:
                    violations.append(
                        f"consumer_retained_floor:{consumer['consumer_id']}"
                    )
                owned = sorted(
                    row["input_sequence"]
                    for row in completions
                    if row["consumer_id"] == consumer["consumer_id"]
                )
                expected_completion_start = max(
                    consumer["start_sequence"],
                    consumer["completion_retired_through"] + 1,
                )
                if owned != list(
                    range(expected_completion_start, consumer["completed_through"] + 1)
                ):
                    violations.append(f"completion_coverage:{consumer['consumer_id']}")
                if consumer["completed_through"] >= consumer["start_sequence"]:
                    latest = next(
                        (
                            row
                            for row in completions
                            if row["consumer_id"] == consumer["consumer_id"]
                            and row["input_sequence"] == consumer["completed_through"]
                        ),
                        None,
                    )
                    if latest is None or (
                        decoded(latest["managed_state"]),
                        latest["state_schema"],
                        latest["state_version"],
                    ) != (
                        decoded(consumer["managed_state"]),
                        consumer["state_schema"],
                        consumer["state_version"],
                    ):
                        violations.append(
                            f"consumer_state_projection:{consumer['consumer_id']}"
                        )
                if consumer["in_flight_sequence"] is not None and (
                    consumer["in_flight_sequence"] != recovery_root
                    or consumer["in_flight_end"] is None
                    or consumer["in_flight_end"] < recovery_root
                    or any(
                        (consumer["channel"], consumer["partition_id"], sequence)
                        not in all_by_location
                        for sequence in range(
                            recovery_root, consumer["in_flight_end"] + 1
                        )
                    )
                ):
                    violations.append(f"consumer_in_flight:{consumer['consumer_id']}")
                if (
                    consumer["in_flight_sequence"] is None
                    and consumer["in_flight_end"] is not None
                ):
                    violations.append(
                        f"consumer_in_flight_end:{consumer['consumer_id']}"
                    )

            for completion in completions:
                consumer = consumer_by_id.get(completion["consumer_id"])
                input_location = (
                    None
                    if consumer is None
                    else (
                        consumer["channel"],
                        consumer["partition_id"],
                        completion["input_sequence"],
                    )
                )
                input_summary = (
                    None
                    if consumer is None
                    else summary_by_location.get(
                        (consumer["channel"], consumer["partition_id"])
                    )
                )
                input_is_retired = (
                    input_summary is not None
                    and completion["input_sequence"] <= input_summary["retired_through"]
                )
                if consumer is None or (
                    input_location not in all_by_location and not input_is_retired
                ):
                    violations.append(
                        f"completion_input:{completion['consumer_id']}:{completion['input_sequence']}"
                    )
                if completion["completion_kind"] == "YIELD":
                    output_location = (
                        completion["output_channel"],
                        completion["output_partition"],
                        completion["output_sequence"],
                    )
                    output_summary = summary_by_location.get(output_location[:2])
                    output_is_retired = (
                        output_summary is not None
                        and output_location[2] <= output_summary["retired_through"]
                    )
                    if output_location not in all_by_location and not output_is_retired:
                        violations.append(
                            f"completion_output:{completion['consumer_id']}:{completion['input_sequence']}"
                        )
                    turn_id = f"turn:{completion['consumer_id']}:{completion['input_sequence']}"
                    attempt = attempt_by_id.get(turn_id)
                    witness = all_by_location.get(output_location)
                    if (attempt is None and not output_is_retired) or (
                        attempt is not None
                        and (
                            attempt["status"] != "APPENDED"
                            or (
                                attempt["channel"],
                                attempt["partition_id"],
                                attempt["sequence"],
                            )
                            != output_location
                            or witness is None
                            or (
                                attempt["record_id"],
                                attempt["object_key"],
                                attempt["payload_digest"],
                                attempt["payload_size"],
                                attempt["schema_id"],
                                attempt["lineage_digest"],
                            )
                            != (
                                witness["record_id"],
                                witness["object_key"],
                                witness["payload_digest"],
                                witness["payload_size"],
                                witness["schema_id"],
                                witness["lineage_digest"],
                            )
                        )
                    ):
                        violations.append(
                            f"completion_output_binding:{completion['consumer_id']}:"
                            f"{completion['input_sequence']}"
                        )
                elif any(
                    completion[name] is not None
                    for name in (
                        "output_channel",
                        "output_partition",
                        "output_sequence",
                    )
                ):
                    violations.append(
                        f"completion_return:{completion['consumer_id']}:{completion['input_sequence']}"
                    )
                elif (
                    f"turn:{completion['consumer_id']}:{completion['input_sequence']}"
                    in attempt_by_id
                ):
                    violations.append(
                        f"completion_return_attempt:{completion['consumer_id']}:"
                        f"{completion['input_sequence']}"
                    )

            for attempt in attempts:
                identity = {
                    "attempt_id": attempt["attempt_id"],
                    "channel": attempt["channel"],
                    "partition": attempt["partition_id"],
                    "record_id": attempt["record_id"],
                    "object_key": attempt["object_key"],
                    "payload_digest": attempt["payload_digest"],
                    "payload_size": attempt["payload_size"],
                    "schema_id": attempt["schema_id"],
                    "lineage_digest": attempt["lineage_digest"],
                    "producer_id": attempt["producer_id"],
                    "producer_sequence": attempt["producer_sequence"],
                }
                if attempt["identity_digest"] != identity_digest(identity):
                    violations.append(f"append_identity:{attempt['attempt_id']}")
                if attempt["producer_id"] is not None and not attempt[
                    "object_key"
                ].startswith("sha256:"):
                    violations.append(f"structured_content_key:{attempt['attempt_id']}")
                if attempt["status"] == "APPENDED":
                    location = (
                        attempt["channel"],
                        attempt["partition_id"],
                        attempt["sequence"],
                    )
                    witness = by_location.get(location) or tomb_by_location.get(
                        location
                    )
                    if witness is None or (
                        witness["record_id"],
                        witness["object_key"],
                        witness["payload_digest"],
                        witness["payload_size"],
                        witness["schema_id"],
                        witness["lineage_digest"],
                    ) != (
                        attempt["record_id"],
                        attempt["object_key"],
                        attempt["payload_digest"],
                        attempt["payload_size"],
                        attempt["schema_id"],
                        attempt["lineage_digest"],
                    ):
                        violations.append(f"append_result:{attempt['attempt_id']}")

            appended_attempts = [
                attempt for attempt in attempts if attempt["status"] == "APPENDED"
            ]
            for witness in [*records, *tombstones]:
                matching = [
                    attempt
                    for attempt in appended_attempts
                    if (
                        attempt["channel"],
                        attempt["partition_id"],
                        attempt["sequence"],
                        attempt["record_id"],
                        attempt["object_key"],
                        attempt["payload_digest"],
                        attempt["payload_size"],
                        attempt["schema_id"],
                        attempt["lineage_digest"],
                    )
                    == (
                        witness["channel"],
                        witness["partition_id"],
                        witness["sequence"],
                        witness["record_id"],
                        witness["object_key"],
                        witness["payload_digest"],
                        witness["payload_size"],
                        witness["schema_id"],
                        witness["lineage_digest"],
                    )
                ]
                if len(matching) != 1:
                    violations.append(
                        f"record_append_binding:{witness['channel']}:"
                        f"{witness['partition_id']}:{witness['sequence']}"
                    )

            checkpoints = await conn.fetch(
                "SELECT * FROM checkpoints WHERE status='SEALED'"
            )
            refs = await conn.fetch("SELECT * FROM durable_refs WHERE status='ACTIVE'")
            part_by_location = {
                (row["channel"], row["partition_id"]): row for row in partitions
            }
            current_roots: dict[tuple[str, str], list[int]] = {}
            for consumer in consumers:
                location = (consumer["channel"], consumer["partition_id"])
                current_roots.setdefault(location, []).append(
                    consumer["completed_through"] + 1
                )
            for ref in refs:
                location = (ref["channel"], ref["partition_id"])
                current_roots.setdefault(location, []).append(ref["sequence"])
                if (*location, ref["sequence"]) not in by_location:
                    future_join_root = False
                    if ref["ref_kind"] == "JOIN_INPUT" and await conn.fetchval(
                        "SELECT to_regclass('join_inputs_v3') IS NOT NULL"
                    ):
                        binding = await conn.fetchrow(
                            """SELECT * FROM join_inputs_v3
                            WHERE retention_ref=$1 AND channel=$2 AND partition_id=$3""",
                            ref["ref_id"],
                            ref["channel"],
                            ref["partition_id"],
                        )
                        part = part_by_location.get(location)
                        future_join_root = (
                            binding is not None
                            and part is not None
                            and ref["sequence"] == part["next_sequence"]
                            and binding["completed"] >= ref["sequence"] - 1
                        )
                    if not future_join_root:
                        violations.append(
                            f"durable_ref_not_live:{ref['ref_kind']}:{ref['ref_id']}"
                        )
            for checkpoint in checkpoints:
                location = (checkpoint["channel"], checkpoint["partition_id"])
                recovery_sequence = checkpoint["cut_sequence"] + 1
                current_roots.setdefault(location, []).append(recovery_sequence)
                part = part_by_location.get(location)
                if part is None or any(
                    (*location, sequence) not in by_location
                    for sequence in range(recovery_sequence, part["next_sequence"])
                ):
                    violations.append(
                        f"checkpoint_not_restorable:{checkpoint['checkpoint_id']}"
                    )
            for location, roots in current_roots.items():
                root = min(roots)
                if any(
                    row["sequence"] >= root
                    for row in tombstones
                    if (row["channel"], row["partition_id"]) == location
                ):
                    violations.append(
                        f"retained_root_loss:{location[0]}:{location[1]}:{root}"
                    )

            tomb_by_manifest: dict[int, list[int]] = {}
            for row in tombstones:
                tomb_by_manifest.setdefault(row["manifest_id"], []).append(
                    row["sequence"]
                )
            for manifest in await conn.fetch("SELECT * FROM compaction_manifests"):
                roots = decoded(manifest["roots"])
                sequences = decoded(manifest["sequences"])
                expected_safe = min(
                    manifest["requested_through"],
                    min(
                        (root["sequence"] - 1 for root in roots),
                        default=manifest["requested_through"],
                    ),
                )
                if manifest["safe_through"] != expected_safe:
                    violations.append(f"manifest_safe:{manifest['manifest_id']}")
                if manifest["status"] != "COMPLETED" or sorted(sequences) != sorted(
                    tomb_by_manifest.get(manifest["manifest_id"], [])
                ):
                    violations.append(f"manifest_tombstones:{manifest['manifest_id']}")

            for invocation in await conn.fetch("SELECT * FROM runtime_invocations"):
                consumer = consumer_by_id.get(invocation["consumer_id"])
                if consumer is None or (
                    invocation["input_sequence"],
                    invocation["attempt"],
                    invocation["owner_id"],
                    invocation["owner_epoch"],
                ) != (
                    consumer["in_flight_sequence"],
                    consumer["delivery_attempt"],
                    consumer["owner_id"],
                    consumer["owner_epoch"],
                ):
                    violations.append(f"invocation_token:{invocation['consumer_id']}")
            producer_progress = await conn.fetch("SELECT * FROM producer_progress")
            for progress in producer_progress:
                if not (
                    0 <= progress["retired_through"] <= progress["accepted_through"]
                ):
                    violations.append(
                        f"producer_watermark:{progress['channel']}:"
                        f"{progress['partition_id']}:{progress['producer_id']}"
                    )
                hot_sequences = sorted(
                    attempt["producer_sequence"]
                    for attempt in appended_attempts
                    if (
                        attempt["channel"],
                        attempt["partition_id"],
                        attempt["producer_id"],
                    )
                    == (
                        progress["channel"],
                        progress["partition_id"],
                        progress["producer_id"],
                    )
                    and attempt["producer_sequence"] is not None
                    and attempt["producer_sequence"] > progress["retired_through"]
                )
                if hot_sequences != list(
                    range(
                        progress["retired_through"] + 1,
                        progress["accepted_through"] + 1,
                    )
                ):
                    violations.append(
                        f"producer_hot_attempts:{progress['channel']}:"
                        f"{progress['partition_id']}:{progress['producer_id']}"
                    )
            for part in partitions:
                producer_count = await conn.fetchval(
                    "SELECT count(*) FROM producer_progress WHERE channel=$1 "
                    "AND partition_id=$2",
                    part["channel"],
                    part["partition_id"],
                )
                if producer_count > part["max_producers"]:
                    violations.append(
                        f"producer_capacity:{part['channel']}:{part['partition_id']}"
                    )
                consumer_count = sum(
                    1
                    for consumer in consumers
                    if (consumer["channel"], consumer["partition_id"])
                    == (part["channel"], part["partition_id"])
                )
                if consumer_count > part["max_consumers"]:
                    violations.append(
                        f"consumer_capacity:{part['channel']}:{part['partition_id']}"
                    )
            for summary in summaries:
                if summary["operation_id"] not in operation_by_id:
                    violations.append(
                        f"metadata_operation_missing:{summary['channel']}:"
                        f"{summary['partition_id']}"
                    )
            for part in partitions:
                location = (part["channel"], part["partition_id"])
                if location in summary_by_location:
                    continue
                if any(
                    row["retired_through"] != 0
                    for row in producer_progress
                    if (row["channel"], row["partition_id"]) == location
                ) or any(
                    row["completion_retired_through"] != 0
                    for row in consumers
                    if (row["channel"], row["partition_id"]) == location
                ):
                    violations.append(
                        f"retired_without_summary:{location[0]}:{location[1]}"
                    )
            if len(metadata_operations) > len(summaries):
                violations.append("metadata_operation_capacity")
            for operation in metadata_operations:
                cuts = decoded(operation["cuts"])
                canonical = json.dumps(cuts, sort_keys=True, separators=(",", ":"))
                if (
                    hashlib.sha256(canonical.encode()).hexdigest()
                    != operation["cuts_digest"]
                ):
                    violations.append(
                        f"metadata_operation_digest:{operation['operation_id']}"
                    )
                outcome = decoded(operation["outcome"])
                locations = outcome.get("locations", {})
                expected_locations = {
                    f"{cut.get('channel')}/{cut.get('partition')}" for cut in cuts
                }
                if (
                    outcome.get("operation_id") != operation["operation_id"]
                    or outcome.get("idempotent_retry") is not False
                    or set(locations) != expected_locations
                ):
                    violations.append(
                        f"metadata_operation_outcome:{operation['operation_id']}"
                    )
                referenced = False
                for cut in cuts:
                    location = (cut.get("channel"), cut.get("partition"))
                    summary = summary_by_location.get(location)
                    outcome_location = locations.get(f"{location[0]}/{location[1]}", {})
                    if summary is None or summary["retired_through"] < cut.get(
                        "through", -1
                    ):
                        violations.append(
                            f"metadata_operation_cut:{operation['operation_id']}"
                        )
                    if outcome_location.get("retired_through") != cut.get("through"):
                        violations.append(
                            f"metadata_operation_outcome_cut:{operation['operation_id']}"
                        )
                    if (
                        summary is not None
                        and summary["operation_id"] == operation["operation_id"]
                    ):
                        referenced = True
                        if (
                            outcome_location.get("rolling_digest")
                            != summary["rolling_digest"]
                        ):
                            violations.append(
                                f"metadata_operation_outcome_digest:"
                                f"{operation['operation_id']}"
                            )
                        expected_anchors = {
                            "producers": [
                                {
                                    "producer_id": row["producer_id"],
                                    "retired_through": row["retired_through"],
                                }
                                for row in sorted(
                                    (
                                        value
                                        for value in producer_progress
                                        if (value["channel"], value["partition_id"])
                                        == location
                                        and value["retired_through"] > 0
                                    ),
                                    key=lambda value: value["producer_id"],
                                )
                            ],
                            "consumers": [
                                {
                                    "consumer_id": row["consumer_id"],
                                    "completion_retired_through": row[
                                        "completion_retired_through"
                                    ],
                                }
                                for row in sorted(
                                    (
                                        value
                                        for value in consumers
                                        if (value["channel"], value["partition_id"])
                                        == location
                                        and value["completion_retired_through"] > 0
                                    ),
                                    key=lambda value: value["consumer_id"],
                                )
                            ],
                        }
                        encoded_anchors = json.dumps(
                            expected_anchors, sort_keys=True, separators=(",", ":")
                        )
                        if (
                            outcome_location.get("anchors") != expected_anchors
                            or outcome_location.get("anchor_digest")
                            != hashlib.sha256(encoded_anchors.encode()).hexdigest()
                        ):
                            violations.append(
                                f"metadata_operation_anchors:"
                                f"{operation['operation_id']}"
                            )
                if not referenced:
                    violations.append(
                        f"metadata_operation_orphan:{operation['operation_id']}"
                    )
            observations = {
                "partitions": len(partitions),
                "records": len(records),
                "tombstones": len(tombstones),
                "consumers": len(consumers),
                "completions": len(completions),
                "object_identities": len(object_identities),
                "metadata_summaries": len(summaries),
                "metadata_operations": len(metadata_operations),
            }
    finally:
        await conn.close()
    return {
        "passed": not violations,
        "violations": sorted(set(violations)),
        "observations": observations,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("schema")
    args = parser.parse_args()
    report = await evaluate(args.dsn, args.schema)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
