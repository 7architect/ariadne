"""Independent read-only control/stream authority checks.

The optional HTTP projection comparison requires a quiescent test fixture;
independent HTTP and SQL reads do not share a distributed snapshot.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any

import asyncpg


def decoded(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _rows(rows, fields):
    return [{name: row[name] for name in fields} for row in rows]


def wire(value: Any) -> Any:
    return json.loads(
        json.dumps(
            value,
            default=lambda item: (
                item.isoformat() if isinstance(item, (date, datetime)) else str(item)
            ),
        )
    )


async def evaluate(
    dsn: str, schema: str, projection: dict[str, Any] | None = None
) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
        raise ValueError("unsafe schema")
    errors: list[str] = []
    observations: dict[str, Any] = {}
    projection_matched = False
    conn = await asyncpg.connect(dsn, server_settings={"search_path": schema})
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            deployments = {
                row["deployment_id"]: row
                for row in await conn.fetch("SELECT * FROM control_deployments_v1")
            }
            revisions = {
                row["revision_id"]: row
                for row in await conn.fetch("SELECT * FROM deployment_revisions")
            }
            for deployment in deployments.values():
                revision = revisions.get(deployment["revision_id"])
                if revision is None or (
                    deployment["graph_version"],
                    deployment["graph_digest"],
                ) != (revision["graph_version"], revision["graph_digest"]):
                    errors.append("deployment_identity:" + deployment["deployment_id"])
                if revision is not None:
                    expected_locations = {
                        (item["channel"], item["partition"])
                        for item in decoded(revision["source_manifest"])
                    } | {
                        (output["channel"], output["partition"])
                        for participant in decoded(revision["participant_manifest"])
                        for output in participant["outputs"]
                    }
                    owned = await conn.fetch(
                        "SELECT * FROM control_partition_ownership_v1 WHERE deployment_id=$1",
                        deployment["deployment_id"],
                    )
                    source_locations = {
                        (item["channel"], item["partition"])
                        for item in decoded(revision["source_manifest"])
                    }
                    if {
                        (row["channel"], row["partition_id"]) for row in owned
                    } != expected_locations or any(
                        (row["tenant_id"], row["revision_id"])
                        != (deployment["tenant_id"], deployment["revision_id"])
                        or row["partition_role"]
                        != (
                            "SOURCE"
                            if (row["channel"], row["partition_id"]) in source_locations
                            else "OUTPUT"
                        )
                        for row in owned
                    ):
                        errors.append(
                            "deployment_partitions:" + deployment["deployment_id"]
                        )
            runs = await conn.fetch("SELECT * FROM control_runs_v1 ORDER BY run_id")
            for run in runs:
                run_id = run["run_id"]
                deployment = deployments.get(run["deployment_id"])
                if deployment is None or (run["tenant_id"], run["revision_id"]) != (
                    deployment["tenant_id"],
                    deployment["revision_id"],
                ):
                    errors.append("run_scope:" + run_id)
                    continue
                allowed = {
                    "ACTIVE": {"ACTIVE"},
                    "DRAINING": {"DRAINING", "DRAINED"},
                    "CANCELLED": {"CANCELLED"},
                }
                if run["lifecycle"] not in allowed.get(run["desired_state"], set()):
                    errors.append("run_lifecycle:" + run_id)
                revision = revisions[run["revision_id"]]
                native_workers = []
                if await conn.fetchval(
                    "SELECT to_regclass('join_workers') IS NOT NULL"
                ):
                    native_workers = await conn.fetch(
                        "SELECT * FROM join_workers WHERE revision_id=$1",
                        run["revision_id"],
                    )
                declared = {
                    item["participant_id"]: item
                    for item in decoded(revision["participant_manifest"])
                }
                if run["execution_mode"] == "NATIVE_JOIN":
                    if {item["participant_id"] for item in native_workers} != set(
                        declared
                    ):
                        errors.append("native_participant_manifest:" + run_id)
                    for item in native_workers:
                        config = decoded(item["config"])
                        calculated = hashlib.sha256(
                            json.dumps(
                                config, sort_keys=True, separators=(",", ":")
                            ).encode()
                        ).hexdigest()
                        if (
                            config.get("spec") != declared.get(item["participant_id"])
                            or item["config_digest"] != calculated
                        ):
                            errors.append("native_configuration:" + run_id)
                    inputs = await conn.fetch(
                        "SELECT participant_id,channel,partition_id FROM join_inputs_v3 WHERE revision_id=$1",
                        run["revision_id"],
                    )
                    expected_inputs = {
                        (node, item["channel"], item["partition"])
                        for node, participant in declared.items()
                        for item in participant["inputs"]
                    }
                    if {tuple(item.values()) for item in inputs} != expected_inputs:
                        errors.append("native_input_manifest:" + run_id)
                    required_tables = {
                        "join_workers": 2,
                        "join_inputs_v3": 1,
                        "join_receipts_v3": 1,
                        "join_turns_v3": 1,
                        "join_barriers_v3": 1,
                        "join_alignments_v3": 1,
                        "join_invocations_v3": 1,
                        "join_recoveries_v3": 1,
                        "region_recovery_plans_v1": 1,
                        "regional_recovery_workers_v1": 1,
                        "regional_recovery_attempts_v1": 1,
                        "regional_recovery_attempt_workers_v1": 1,
                    }
                    for table_name, minimum in required_tables.items():
                        if not await conn.fetchval(
                            "SELECT to_regclass($1) IS NOT NULL", table_name
                        ):
                            errors.append("native_schema_missing:" + table_name)
                        elif (
                            await conn.fetchval(
                                "SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal AND tgenabled<>'D' AND left(tgname,8)='control_'",
                                table_name,
                            )
                            < minimum
                        ):
                            errors.append("native_control_hooks:" + table_name)
                    for table_name in (
                        "region_graph_seals_v1",
                        "regional_topology_seals_v1",
                    ):
                        if not await conn.fetchval(
                            f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE revision_id=$1)",
                            run["revision_id"],
                        ):
                            errors.append("native_topology_seal:" + run_id)
                elif run["execution_mode"] != "LEGACY_SOURCE" or native_workers:
                    errors.append("execution_mode:" + run_id)
                expected_sources = sorted(
                    (item["channel"], item["partition"])
                    for item in decoded(revision["source_manifest"])
                )
                ownership = await conn.fetch(
                    "SELECT * FROM control_source_ownership_v1 WHERE run_id=$1",
                    run_id,
                )
                if sorted(
                    (row["channel"], row["partition_id"]) for row in ownership
                ) != expected_sources or any(
                    row["revision_id"] != run["revision_id"] for row in ownership
                ):
                    errors.append("source_scope:" + run_id)
                admissions = await conn.fetch(
                    "SELECT * FROM control_source_admissions_v1 WHERE run_id=$1",
                    run_id,
                )
                producers = await conn.fetch(
                    "SELECT * FROM control_admission_producers_v1 WHERE run_id=$1",
                    run_id,
                )
                if (
                    len(admissions) != run["used_admission_records"]
                    or sum(row["payload_size"] for row in admissions)
                    != run["used_admission_bytes"]
                ):
                    errors.append("admission_credit:" + run_id)
                if (
                    len(admissions) > run["max_admission_records"]
                    or sum(row["payload_size"] for row in admissions)
                    > run["max_admission_bytes"]
                ):
                    errors.append("admission_budget:" + run_id)
                if (
                    len(admissions) + sum(row["retired_records"] for row in producers)
                    != run["next_admission_ordinal"] - 1
                ):
                    errors.append("admission_ordinal_coverage:" + run_id)
                for producer in producers:
                    local = sorted(
                        row["producer_sequence"]
                        for row in admissions
                        if (row["channel"], row["partition_id"], row["producer_id"])
                        == (
                            producer["channel"],
                            producer["partition_id"],
                            producer["producer_id"],
                        )
                    )
                    if local != list(
                        range(
                            producer["retired_through"] + 1,
                            producer["accepted_through"] + 1,
                        )
                    ):
                        errors.append("admission_producer_coverage:" + run_id)
                unpublished = False
                for admission in admissions:
                    if admission["status"] == "ADMITTED":
                        unpublished = True
                        if admission["sequence"] is not None:
                            errors.append(
                                "unpublished_sequence:" + admission["admission_id"]
                            )
                        continue
                    record = await conn.fetchrow(
                        "SELECT * FROM records WHERE channel=$1 AND partition_id=$2 AND sequence=$3",
                        admission["channel"],
                        admission["partition_id"],
                        admission["sequence"],
                    )
                    if record is None:
                        retired = await conn.fetchval(
                            "SELECT retired_through FROM metadata_summaries WHERE channel=$1 AND partition_id=$2",
                            admission["channel"],
                            admission["partition_id"],
                        )
                        if retired is None or retired < admission["sequence"]:
                            errors.append(
                                "admission_publication_missing:"
                                + admission["admission_id"]
                            )
                    elif (
                        record["record_id"],
                        record["payload_size"],
                        record["payload_digest"],
                        record["object_key"],
                    ) != (
                        admission["record_id"],
                        admission["payload_size"],
                        admission["payload_digest"],
                        admission["object_key"],
                    ):
                        errors.append(
                            "admission_record_identity:" + admission["admission_id"]
                        )
                consumers = await conn.fetch(
                    "SELECT c.* FROM control_run_consumers_v1 b JOIN consumers c USING(consumer_id) WHERE b.run_id=$1 ORDER BY c.consumer_id",
                    run_id,
                )
                all_locations = set(expected_sources) | {
                    (output["channel"], output["partition"])
                    for participant in decoded(revision["participant_manifest"])
                    for output in participant["outputs"]
                }
                installed_consumers = [
                    row
                    for row in await conn.fetch(
                        "SELECT consumer_id,channel,partition_id FROM consumers"
                    )
                    if (row["channel"], row["partition_id"]) in all_locations
                ]
                if {row["consumer_id"] for row in installed_consumers} != {
                    row["consumer_id"] for row in consumers
                }:
                    errors.append("consumer_manifest:" + run_id)
                for consumer in consumers:
                    if (
                        consumer["channel"],
                        consumer["partition_id"],
                    ) not in expected_sources:
                        errors.append("consumer_scope:" + run_id)
                participant_manifest = decoded(revision["participant_manifest"])
                workers = []
                native_inputs = []
                if await conn.fetchval(
                    "SELECT to_regclass('join_workers') IS NOT NULL"
                ):
                    workers = await conn.fetch(
                        "SELECT participant_id,config,config_digest FROM join_workers "
                        "WHERE revision_id=$1 ORDER BY participant_id",
                        run["revision_id"],
                    )
                    native_inputs = await conn.fetch(
                        "SELECT participant_id,channel,partition_id FROM join_inputs_v3 "
                        "WHERE revision_id=$1 ORDER BY participant_id,channel,partition_id",
                        run["revision_id"],
                    )
                expected_by_id = {
                    item["participant_id"]: item for item in participant_manifest
                }
                expected_inputs = sorted(
                    (
                        item["participant_id"],
                        source["channel"],
                        source["partition"],
                    )
                    for item in participant_manifest
                    for source in item["inputs"]
                )
                native_manifest_valid = (
                    bool(workers)
                    and [row["participant_id"] for row in workers]
                    == sorted(expected_by_id)
                    and [
                        (row["participant_id"], row["channel"], row["partition_id"])
                        for row in native_inputs
                    ]
                    == expected_inputs
                )
                for worker in workers:
                    config = decoded(worker["config"])
                    config_digest = hashlib.sha256(
                        json.dumps(
                            config, sort_keys=True, separators=(",", ":")
                        ).encode()
                    ).hexdigest()
                    native_manifest_valid = native_manifest_valid and (
                        config_digest == worker["config_digest"]
                        and config.get("spec")
                        == expected_by_id.get(worker["participant_id"])
                    )
                topology_sealed = False
                if await conn.fetchval(
                    "SELECT to_regclass('region_graph_seals_v1') IS NOT NULL "
                    "AND to_regclass('regional_topology_seals_v1') IS NOT NULL"
                ):
                    topology_sealed = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM region_graph_seals_v1 g "
                        "JOIN regional_topology_seals_v1 t USING(revision_id) "
                        "WHERE g.revision_id=$1)",
                        run["revision_id"],
                    )
                required_native_tables = (
                    "region_recovery_plans_v1",
                    "regional_recovery_workers_v1",
                    "regional_recovery_attempts_v1",
                    "regional_recovery_attempt_workers_v1",
                )
                native_hooks_valid = all(
                    [
                        await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table)
                        for table in required_native_tables
                    ]
                )
                if native_hooks_valid:
                    required_pairs = {
                        ("join_workers", "control_join_claim_guard"),
                        ("join_workers", "control_join_manifest_worker_guard"),
                        ("join_inputs_v3", "control_join_manifest_input_guard"),
                        ("join_receipts_v3", "control_join_receipt_guard"),
                        ("join_turns_v3", "control_join_turn_guard"),
                        ("join_barriers_v3", "control_join_barrier_guard"),
                        ("join_alignments_v3", "control_join_alignment_guard"),
                        ("join_invocations_v3", "control_join_invocation_guard"),
                        ("join_recoveries_v3", "control_join_recovery_guard"),
                        ("region_recovery_plans_v1", "control_region_plan_guard"),
                        (
                            "regional_recovery_workers_v1",
                            "control_regional_worker_guard",
                        ),
                        (
                            "regional_recovery_attempts_v1",
                            "control_regional_attempt_guard",
                        ),
                        (
                            "regional_recovery_attempt_workers_v1",
                            "control_regional_attempt_worker_guard",
                        ),
                    }
                    installed_pairs = {
                        (row["relname"], row["tgname"])
                        for row in await conn.fetch(
                            "SELECT c.relname,t.tgname FROM pg_trigger t "
                            "JOIN pg_class c ON c.oid=t.tgrelid "
                            "JOIN pg_namespace n ON n.oid=c.relnamespace "
                            "WHERE n.nspname=current_schema() AND NOT t.tgisinternal "
                            "AND t.tgenabled<>'D'"
                        )
                    }
                    native_hooks_valid = required_pairs <= installed_pairs
                if run["execution_mode"] == "NATIVE_JOIN":
                    if (
                        consumers
                        or not native_manifest_valid
                        or not topology_sealed
                        or not native_hooks_valid
                    ):
                        errors.append("native_execution_manifest:" + run_id)
                elif run["execution_mode"] == "LEGACY_SOURCE":
                    legacy_has_routed_input = any(
                        (source["channel"], source["partition"])
                        not in set(expected_sources)
                        for participant in participant_manifest
                        for source in participant["inputs"]
                    )
                    if workers or not consumers or legacy_has_routed_input:
                        errors.append("legacy_execution_manifest:" + run_id)
                else:
                    errors.append("execution_mode:" + run_id)
                if run["lifecycle"] == "DRAINED":
                    if await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM checkpoint_intents_v2 i JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) WHERE i.revision_id=$1 AND l.status NOT IN('COMPLETED','ABORTED','RETIRED'))",
                        run["revision_id"],
                    ):
                        errors.append("drained_pending_checkpoint:" + run_id)
                    if unpublished:
                        errors.append("drained_unpublished:" + run_id)
                    for consumer in consumers:
                        tail = await conn.fetchval(
                            "SELECT next_sequence-1 FROM partitions WHERE channel=$1 AND partition_id=$2",
                            consumer["channel"],
                            consumer["partition_id"],
                        )
                        if (
                            consumer["completed_through"] != tail
                            or consumer["in_flight_sequence"] is not None
                        ):
                            errors.append("drained_consumer_pending:" + run_id)
                    if await conn.fetchval(
                        "SELECT to_regclass('join_workers') IS NOT NULL"
                    ):
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM join_inputs_v3 i JOIN partitions p USING(channel,partition_id) JOIN partition_log_heads h USING(channel,partition_id) WHERE i.revision_id=$1 AND (i.completed<>p.next_sequence-1 OR i.scanned<>h.next_log_sequence-1 OR i.blocked_checkpoint IS NOT NULL))",
                            run["revision_id"],
                        ):
                            errors.append("drained_operator_pending:" + run_id)
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM join_invocations_v3 WHERE revision_id=$1)",
                            run["revision_id"],
                        ):
                            errors.append("drained_invocation:" + run_id)
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM join_workers WHERE revision_id=$1 AND phase<>'LIVE')",
                            run["revision_id"],
                        ):
                            errors.append("drained_operator_restoring:" + run_id)
                if run["lifecycle"] in ("DRAINED", "CANCELLED") and any(
                    row["in_flight_sequence"] is not None or row["owner_id"] is not None
                    for row in consumers
                ):
                    errors.append("terminal_consumer_authority:" + run_id)
                observations[run_id] = {
                    "desired_state": run["desired_state"],
                    "lifecycle": run["lifecycle"],
                    "hot_admissions": len(admissions),
                    "retired_admissions": sum(
                        row["retired_records"] for row in producers
                    ),
                }
                if (
                    projection is not None
                    and projection.get("control", {}).get("run_id") == run_id
                ):
                    projection_matched = True
                    for name in (
                        "tenant_id",
                        "deployment_id",
                        "revision_id",
                        "execution_mode",
                        "desired_state",
                        "lifecycle",
                        "drain_admission_cut",
                        "control_version",
                        "execution_mode",
                    ):
                        if projection["control"].get(name) != run[name]:
                            errors.append("projection_control:" + name)
                    fields = (
                        "consumer_id",
                        "channel",
                        "partition_id",
                        "completed_through",
                        "in_flight_sequence",
                        "in_flight_end",
                        "owner_id",
                        "owner_epoch",
                        "lease_expires_at",
                    )
                    if wire(_rows(projection.get("consumers", []), fields)) != wire(
                        _rows(consumers, fields)
                    ):
                        errors.append("projection_consumers:" + run_id)
                    locations = set(expected_sources) | {
                        (output["channel"], output["partition"])
                        for participant in decoded(revision["participant_manifest"])
                        for output in participant["outputs"]
                    }
                    expected_partitions = []
                    for channel, partition in sorted(locations):
                        expected_partitions.append(
                            dict(
                                await conn.fetchrow(
                                    "SELECT channel,partition_id,next_sequence,used_records,used_bytes FROM partitions WHERE channel=$1 AND partition_id=$2",
                                    channel,
                                    partition,
                                )
                            )
                        )
                    if projection.get("partitions") != expected_partitions:
                        errors.append("projection_partitions:" + run_id)
                    if await conn.fetchval(
                        "SELECT to_regclass('join_workers') IS NOT NULL"
                    ):
                        operator_rows = await conn.fetch(
                            "SELECT participant_id,owner_id,owner_epoch,lease_expires_at,"
                            "phase,next_arrival,next_apply,state FROM join_workers "
                            "WHERE revision_id=$1 ORDER BY participant_id",
                            run["revision_id"],
                        )
                        operator_fields = (
                            "participant_id",
                            "owner_id",
                            "owner_epoch",
                            "lease_expires_at",
                            "phase",
                            "next_arrival",
                            "next_apply",
                            "state_digest",
                        )
                        expected_operators = []
                        for row in operator_rows:
                            item = dict(row)
                            item["state_digest"] = hashlib.sha256(
                                json.dumps(
                                    decoded(item.pop("state")),
                                    sort_keys=True,
                                    separators=(",", ":"),
                                ).encode()
                            ).hexdigest()
                            expected_operators.append(item)
                        if wire(
                            _rows(projection.get("operators", []), operator_fields)
                        ) != wire(expected_operators):
                            errors.append("projection_operators:" + run_id)
                        input_fields = (
                            "participant_id",
                            "channel",
                            "partition_id",
                            "scanned",
                            "completed",
                            "blocked_checkpoint",
                        )
                        inputs = await conn.fetch(
                            "SELECT participant_id,channel,partition_id,scanned,completed,blocked_checkpoint FROM join_inputs_v3 WHERE revision_id=$1 ORDER BY participant_id,channel,partition_id",
                            run["revision_id"],
                        )
                        if _rows(projection.get("inputs", []), input_fields) != _rows(
                            inputs, input_fields
                        ):
                            errors.append("projection_inputs:" + run_id)
                    expected_barrier = await conn.fetchrow(
                        "SELECT i.checkpoint_id,i.checkpoint_ordinal,i.intent_digest,l.status,l.last_authority_ordinal FROM checkpoint_intents_v2 i JOIN checkpoint_lifecycle_v2 l USING(checkpoint_id) WHERE i.revision_id=$1 ORDER BY i.checkpoint_ordinal DESC LIMIT 1",
                        run["revision_id"],
                    )
                    barrier_value = (
                        None
                        if expected_barrier is None
                        else {**dict(expected_barrier), "authority": "HOT"}
                    )
                    retired_barrier = await conn.fetchrow(
                        "SELECT checkpoint_id,checkpoint_ordinal,completion_digest,retired_authority_ordinal FROM retired_checkpoint_summaries WHERE revision_id=$1 ORDER BY checkpoint_ordinal DESC LIMIT 1",
                        run["revision_id"],
                    )
                    if retired_barrier is not None and (
                        barrier_value is None
                        or retired_barrier["checkpoint_ordinal"]
                        > barrier_value["checkpoint_ordinal"]
                    ):
                        barrier_value = {
                            **dict(retired_barrier),
                            "status": "RETIRED",
                            "authority": "ARCHIVED",
                        }
                    if projection.get("barrier") != barrier_value:
                        errors.append("projection_barrier:" + run_id)
                    leader = await conn.fetchrow(
                        "SELECT leader_id,leader_epoch,lease_expires_at FROM control_leaders_v1 WHERE deployment_id=$1",
                        run["deployment_id"],
                    )
                    leader_fields = ("leader_id", "leader_epoch", "lease_expires_at")
                    actual_leader = projection.get("leader")
                    if (
                        None
                        if actual_leader is None
                        else wire({name: actual_leader[name] for name in leader_fields})
                    ) != (None if leader is None else wire(dict(leader))):
                        errors.append("projection_leader:" + run_id)
                    if projection.get("admission_pressure") != {
                        "used_records": run["used_admission_records"],
                        "max_records": run["max_admission_records"],
                        "used_bytes": run["used_admission_bytes"],
                        "max_bytes": run["max_admission_bytes"],
                    }:
                        errors.append("projection_admission_pressure:" + run_id)
                regional_plans = []
                if await conn.fetchval(
                    "SELECT to_regclass('region_recovery_plans_v1') IS NOT NULL"
                ):
                    regional_plans = await conn.fetch(
                        "SELECT * FROM region_recovery_plans_v1 WHERE revision_id=$1",
                        run["revision_id"],
                    )
                regional_ids = {item["plan_id"] for item in regional_plans}
                active_regions = []
                if await conn.fetchval(
                    "SELECT to_regclass('regional_recovery_workers_v1') IS NOT NULL"
                ):
                    for item in regional_plans:
                        if await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 WHERE plan_id=$1 AND status<>'ACTIVE')",
                            item["plan_id"],
                        ):
                            active_regions.append(item)
                global_plans = [
                    item
                    for item in await conn.fetch(
                        "SELECT * FROM recovery_plans_v2 WHERE revision_id=$1",
                        run["revision_id"],
                    )
                    if item["status"] == "RESTORING"
                    and item["recovery_id"] not in regional_ids
                ]
                if run["lifecycle"] == "DRAINED" and global_plans:
                    errors.append("drained_global_recovery:" + run_id)
                if (
                    projection is not None
                    and projection.get("control", {}).get("run_id") == run_id
                ):
                    expected_recovery = None
                    if active_regions:
                        latest = max(
                            active_regions,
                            key=lambda item: item["fence_authority_ordinal"],
                        )
                        expected_recovery = {
                            name: latest[name]
                            for name in (
                                "plan_id",
                                "checkpoint_id",
                                "failed_node",
                                "created_at",
                            )
                        }
                        expected_recovery.update(
                            kind="REGION", reasons=decoded(latest["reasons_json"])
                        )
                    elif global_plans:
                        latest = max(
                            global_plans,
                            key=lambda item: item["fence_authority_ordinal"],
                        )
                        expected_recovery = {
                            name: latest[name]
                            for name in (
                                "recovery_id",
                                "checkpoint_id",
                                "status",
                                "created_at",
                            )
                        }
                        expected_recovery["kind"] = "GLOBAL"
                    if wire(projection.get("recovery")) != wire(expected_recovery):
                        errors.append("projection_recovery:" + run_id)
            observations["run_count"] = len(runs)
            if projection is not None and not projection_matched:
                errors.append("projection_unknown_run")
    except Exception as exc:  # noqa: BLE001 - total oracle must report malformed state
        errors.append(f"oracle_totality:{type(exc).__name__}:{exc}")
    finally:
        await conn.close()
    return {"passed": not errors, "violations": errors, "observations": observations}


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dsn")
    parser.add_argument("schema")
    parser.add_argument("--projection", type=Path)
    args = parser.parse_args()
    projection = (
        None if args.projection is None else json.loads(args.projection.read_text())
    )
    report = await evaluate(args.dsn, args.schema, projection)
    print(json.dumps(report, sort_keys=True, indent=2))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
