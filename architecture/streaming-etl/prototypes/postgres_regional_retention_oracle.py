"""Independent read-only checks for the bounded R3-R3 regional archive."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import asyncpg


@dataclass(frozen=True)
class RegionalArchiveReport:
    violations: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.violations


async def evaluate(dsn: str, schema: str, revision_id: str) -> RegionalArchiveReport:
    errors: list[str] = []
    conn = await asyncpg.connect(dsn, server_settings={"search_path": schema})
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            head = await conn.fetchrow(
                "SELECT * FROM regional_archive_heads_v1 WHERE revision_id=$1",
                revision_id,
            )
            operation = await conn.fetchrow(
                "SELECT * FROM regional_retirement_operations_v1 WHERE revision_id=$1",
                revision_id,
            )
            if head is not None:
                if operation is None or operation["status"] != "COMPLETED":
                    errors.append("regional_archive_operation")
                else:
                    try:
                        outcome = json.loads(operation["outcome_json"])
                        plan = json.loads(operation["plan_json"])
                    except (TypeError, ValueError):
                        outcome = {}
                        plan = {}
                        errors.append("regional_archive_outcome_shape")
                    canonical_plan = json.dumps(
                        plan, sort_keys=True, separators=(",", ":")
                    )
                    if (
                        hashlib.sha256(canonical_plan.encode()).hexdigest()
                        != operation["input_digest"]
                    ):
                        errors.append("regional_archive_input_digest")
                    required_arrays = (
                        "plan_ids",
                        "checkpoint_ids",
                        "root_ids",
                        "plan_digests",
                    )
                    if any(
                        not isinstance(plan.get(key), list) or not plan.get(key)
                        for key in required_arrays
                    ) or not isinstance(plan.get("prior_totals"), dict):
                        errors.append("regional_archive_plan_shape")
                    expected_rolling = hashlib.sha256(
                        (head["prior_digest"] + head["material_digest"]).encode()
                    ).hexdigest()
                    if expected_rolling != head["rolling_digest"]:
                        errors.append("regional_archive_rolling_digest")
                    if plan.get("prior_digest") != head["prior_digest"]:
                        errors.append("regional_archive_prior_digest")
                    if (
                        operation["operation_id"] != head["operation_id"]
                        or outcome.get("rolling_digest") != head["rolling_digest"]
                        or outcome.get("material_digest") != head["material_digest"]
                        or outcome.get("operation_id") != operation["operation_id"]
                        or outcome.get("revision_id") != revision_id
                    ):
                        errors.append("regional_archive_head_outcome")
                    prior_totals = plan.get("prior_totals", {})
                    for field in (
                        "retired_plans",
                        "retired_attempts",
                        "retired_generations",
                        "retired_cuts",
                    ):
                        if (
                            not isinstance(prior_totals.get(field), int)
                            or not isinstance(outcome.get(field), int)
                            or head[field]
                            != prior_totals.get(field, 0) + outcome.get(field, 0)
                        ):
                            errors.append("regional_archive_counter:" + field)
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM regional_retirement_rows_v1)"
            ):
                errors.append("regional_archive_witness_leak")
            retired_plan_ids = (
                []
                if operation is None
                else json.loads(operation["plan_json"]).get("plan_ids", [])
            )
            if retired_plan_ids and (
                await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM region_recovery_plans_v1 WHERE plan_id=ANY($1::text[]))",
                    retired_plan_ids,
                )
                or await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 WHERE plan_id=ANY($1::text[]))",
                    retired_plan_ids,
                )
            ):
                errors.append("regional_retired_plan_still_hot")
            plans = await conn.fetch(
                "SELECT * FROM region_recovery_plans_v1 WHERE revision_id=$1",
                revision_id,
            )
            for plan in plans:
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM regional_recovery_workers_v1 WHERE plan_id=$1)",
                    plan["plan_id"],
                ) and not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM recovery_plans_v2
                       WHERE recovery_id=$1 AND revision_id=$2)""",
                    plan["plan_id"],
                    revision_id,
                ):
                    errors.append("regional_compatibility_plan:" + plan["plan_id"])
            cuts = await conn.fetch(
                "SELECT * FROM region_edge_cuts_v1 WHERE revision_id=$1", revision_id
            )
            for cut in cuts:
                if not await conn.fetchval(
                    """SELECT EXISTS(SELECT 1 FROM checkpoints
                       WHERE checkpoint_id=$1 AND status='SEALED'
                         AND channel=$2 AND partition_id=$3 AND cut_sequence=$4)""",
                    cut["retention_ref_id"],
                    cut["channel"],
                    cut["partition_id"],
                    cut["cut_data_sequence"],
                ):
                    errors.append("regional_cut_root:" + cut["edge_id"])
            if head is not None and not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM region_graph_seals_v1 WHERE revision_id=$1)",
                revision_id,
            ):
                errors.append("regional_graph_seal_missing")
    except Exception as exc:  # noqa: BLE001 - total oracle reports malformed DB
        errors.append(f"oracle_totality:{type(exc).__name__}:{exc}")
    finally:
        await conn.close()
    return RegionalArchiveReport(tuple(errors))
