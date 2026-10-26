"""Read-only independent checks of bounded Prefect execution authority.

An optional public projection must be sampled on a quiescent fixture. This
module does not call attempt-store helpers or reconstruct missing history.
"""

from __future__ import annotations

import json
import re

import asyncpg


def decoded(value):
    return json.loads(value) if isinstance(value, str) else value


async def evaluate(dsn: str, schema: str, projection=None):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
        raise ValueError("unsafe schema")
    errors, observations = [], {}
    matched = False
    conn = await asyncpg.connect(dsn, server_settings={"search_path": schema})
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            observed_at = await conn.fetchval("SELECT transaction_timestamp()")
            for head in await conn.fetch(
                "SELECT * FROM prefect_attempt_heads_v1 ORDER BY run_id"
            ):
                run_id = head["run_id"]
                run = await conn.fetchrow(
                    "SELECT * FROM control_runs_v1 WHERE run_id=$1", run_id
                )
                revision = await conn.fetchrow(
                    "SELECT * FROM deployment_revisions WHERE revision_id=$1",
                    head["revision_id"],
                )
                attempts = await conn.fetch(
                    "SELECT * FROM prefect_execution_attempts_v1 WHERE run_id=$1 ORDER BY ordinal",
                    run_id,
                )
                current = next(
                    (
                        row
                        for row in attempts
                        if row["attempt_id"] == head["current_attempt_id"]
                    ),
                    None,
                )
                if run is None or revision is None or current is None:
                    errors.append("missing_authority:" + run_id)
                    continue
                if (
                    run["revision_id"],
                    head["graph_digest"],
                    head["code_version"],
                    head["current_ordinal"],
                    head["current_epoch"],
                ) != (
                    head["revision_id"],
                    revision["graph_digest"],
                    current["code_version"],
                    current["ordinal"],
                    current["attempt_epoch"],
                ):
                    errors.append("head_identity:" + run_id)
                if head["retired_count"] != head["retired_through"] or not re.fullmatch(
                    r"[0-9a-f]{64}", head["retired_digest"]
                ):
                    errors.append("retirement_head:" + run_id)
                if len(attempts) > head["max_hot_attempts"] or [
                    a["ordinal"] for a in attempts
                ] != list(range(head["retired_through"] + 1, current["ordinal"] + 1)):
                    errors.append("hot_horizon:" + run_id)
                expected_nodes = {
                    p["participant_id"]: p
                    for p in decoded(revision["participant_manifest"])
                }
                expected_sources = {
                    (s["channel"], s["partition"])
                    for s in decoded(revision["source_manifest"])
                }
                current_sources, current_operators = [], []
                for attempt in attempts:
                    is_current = attempt["attempt_id"] == head["current_attempt_id"]
                    if (
                        attempt["graph_digest"],
                        attempt["code_version"],
                        attempt["attempt_epoch"],
                    ) != (
                        head["graph_digest"],
                        head["code_version"],
                        attempt["ordinal"],
                    ) or (not is_current and attempt["status"] == "ACTIVE"):
                        errors.append("attempt_identity:" + attempt["attempt_id"])
                    if (attempt["status"] == "ACTIVE") != (
                        attempt["terminal_at"] is None
                    ):
                        errors.append("attempt_terminal:" + attempt["attempt_id"])
                    sources = await conn.fetch(
                        "SELECT * FROM prefect_attempt_source_caps_v1 WHERE attempt_id=$1 ORDER BY channel,partition_id",
                        attempt["attempt_id"],
                    )
                    operators = await conn.fetch(
                        "SELECT * FROM prefect_attempt_operators_v1 WHERE attempt_id=$1 ORDER BY participant_id",
                        attempt["attempt_id"],
                    )
                    if {
                        (s["channel"], s["partition_id"]) for s in sources
                    } != expected_sources or {
                        o["participant_id"] for o in operators
                    } != expected_nodes.keys():
                        errors.append("actor_manifest:" + attempt["attempt_id"])
                    for source in sources:
                        cap = await conn.fetchrow(
                            "SELECT * FROM control_capabilities_v1 WHERE token_digest=$1",
                            source["token_digest"],
                        )
                        active = is_current and attempt["status"] == "ACTIVE"
                        if (
                            cap is None
                            or source["run_id"] != run_id
                            or source["enabled"] != active
                            or cap["disabled"] == active
                            or cap["tenant_id"] != run["tenant_id"]
                            or decoded(cap["capabilities"]) != ["source"]
                            or (
                                active
                                and cap["expires_at"] is not None
                                and cap["expires_at"] <= observed_at
                            )
                        ):
                            errors.append("source_capability:" + attempt["attempt_id"])
                        if not await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM control_admission_producers_v1 WHERE run_id=$1 AND channel=$2 AND partition_id=$3 AND producer_id=$4)",
                            run_id,
                            source["channel"],
                            source["partition_id"],
                            source["producer_id"],
                        ):
                            # Producer binding is established on first admission.
                            has_binding = await conn.fetchval(
                                "SELECT EXISTS(SELECT 1 FROM control_admission_producers_v1 WHERE run_id=$1 AND channel=$2 AND partition_id=$3)",
                                run_id,
                                source["channel"],
                                source["partition_id"],
                            )
                            if has_binding:
                                errors.append(
                                    "source_producer:" + attempt["attempt_id"]
                                )
                    for operator in operators:
                        participant = operator["participant_id"]
                        if (
                            operator["revision_id"] != head["revision_id"]
                            or operator["run_id"] != run_id
                            or operator["artifact_digest"]
                            != expected_nodes.get(participant, {}).get("code_digest")
                            or not operator["artifact_id"]
                            or operator["binding_version"]
                            != operator["replacement_count"] + 1
                            or not re.fullmatch(
                                r"[0-9a-f]{64}", operator["replacement_digest"]
                            )
                        ):
                            errors.append("operator_identity:" + participant)
                        if not is_current and operator["status"] == "ACTIVE":
                            errors.append("old_operator_live:" + participant)
                        if is_current and operator["status"] != attempt["status"]:
                            errors.append("current_operator_status:" + participant)
                        if (
                            is_current
                            and attempt["status"] == "ACTIVE"
                            and operator["status"] == "ACTIVE"
                        ):
                            worker = await conn.fetchrow(
                                "SELECT * FROM join_workers WHERE revision_id=$1 AND participant_id=$2",
                                head["revision_id"],
                                participant,
                            )
                            if worker is None or (
                                worker["owner_id"],
                                worker["owner_epoch"],
                            ) != (operator["owner_id"], operator["owner_epoch"]):
                                errors.append(
                                    "operator_worker_authority:" + participant
                                )
                    if is_current:
                        current_sources = [
                            {
                                key: s[key]
                                for key in (
                                    "channel",
                                    "partition_id",
                                    "producer_id",
                                    "enabled",
                                )
                            }
                            for s in sources
                        ]
                        current_operators = [
                            {
                                key: o[key]
                                for key in (
                                    "participant_id",
                                    "artifact_id",
                                    "artifact_digest",
                                    "prefect_task_run_id",
                                    "owner_id",
                                    "owner_epoch",
                                    "binding_version",
                                    "replacement_count",
                                    "status",
                                )
                            }
                            for o in operators
                        ]
                if (
                    projection is not None
                    and projection.get("control_run_id") == run_id
                ):
                    expected = {
                        "control_run_id": run_id,
                        "revision_id": head["revision_id"],
                        "graph_digest": head["graph_digest"],
                        "code_version": head["code_version"],
                        "current": {
                            key: current[key]
                            for key in (
                                "attempt_id",
                                "ordinal",
                                "attempt_epoch",
                                "prefect_flow_run_id",
                                "status",
                                "replacement_reason",
                            )
                        },
                        "sources": current_sources,
                        "operators": current_operators,
                        "retired_through": head["retired_through"],
                        "retired_count": head["retired_count"],
                        "retired_digest": head["retired_digest"],
                        "hot_attempts": len(attempts),
                        "max_hot_attempts": head["max_hot_attempts"],
                    }
                    matched = True
                    if projection != expected:
                        errors.append("projection_mismatch:" + run_id)
                observations[run_id] = {
                    "hot_attempts": len(attempts),
                    "retired_through": head["retired_through"],
                    "ordinal": current["ordinal"],
                }
            if projection is not None and not matched:
                errors.append("projection_run_missing")
    finally:
        await conn.close()
    return {"passed": not errors, "violations": errors, "observations": observations}
