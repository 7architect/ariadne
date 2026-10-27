"""Independent read-only oracle for the R3-R2 rolling archive."""

from __future__ import annotations

import json
from dataclasses import dataclass

import asyncpg


@dataclass(frozen=True)
class CheckpointArchiveReport:
    violations: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.violations


async def evaluate(dsn: str, schema: str, revision_id: str) -> CheckpointArchiveReport:
    violations: list[str] = []
    conn = await asyncpg.connect(dsn, server_settings={"search_path": schema})
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            revision = await conn.fetchrow(
                "SELECT * FROM deployment_revisions WHERE revision_id=$1", revision_id
            )
            if revision is None:
                return CheckpointArchiveReport(("revision_missing",))
            head = await conn.fetchrow(
                "SELECT * FROM checkpoint_archive_heads_v1 WHERE revision_id=$1",
                revision_id,
            )
            retired_cp = 0 if head is None else head["retired_checkpoint_through"]
            retired_auth = 0 if head is None else head["retired_authority_through"]
            operation = await conn.fetchrow(
                "SELECT * FROM checkpoint_retirement_operations_v1 WHERE revision_id=$1",
                revision_id,
            )
            if head is not None:
                if operation is None or operation["status"] != "COMPLETED":
                    violations.append("archive_operation_not_completed")
                else:
                    try:
                        outcome = json.loads(operation["outcome_json"])
                    except (TypeError, ValueError):
                        outcome = {}
                        violations.append("archive_outcome_malformed")
                    if (
                        operation["operation_id"] != head["operation_id"]
                        or outcome.get("rolling_digest") != head["rolling_digest"]
                        or outcome.get("retired_checkpoint_through") != retired_cp
                        or outcome.get("retired_authority_through") != retired_auth
                    ):
                        violations.append("archive_head_outcome_mismatch")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM checkpoint_retirement_rows_v1)"
            ):
                violations.append("archive_transient_witness_leaked")
            intents = await conn.fetch(
                """SELECT checkpoint_ordinal FROM checkpoint_intents_v2
                   WHERE revision_id=$1 ORDER BY checkpoint_ordinal""",
                revision_id,
            )
            expected_intents = list(
                range(retired_cp + 1, revision["next_checkpoint_ordinal"])
            )
            if [row["checkpoint_ordinal"] for row in intents] != expected_intents:
                violations.append("checkpoint_hot_prefix_gap")
            events = await conn.fetch(
                """SELECT authority_ordinal FROM coordinator_authority_events
                   WHERE revision_id=$1 ORDER BY authority_ordinal""",
                revision_id,
            )
            expected_events = list(
                range(retired_auth + 1, revision["next_authority_ordinal"])
            )
            if [row["authority_ordinal"] for row in events] != expected_events:
                violations.append("authority_hot_prefix_gap")
            if len(events) > revision["max_authority_events"]:
                violations.append("authority_live_budget")
            log_heads = await conn.fetch(
                "SELECT * FROM partition_log_heads ORDER BY channel,partition_id"
            )
            for log_head in log_heads:
                archive = await conn.fetchrow(
                    """SELECT * FROM partition_log_archive_heads_v1
                       WHERE channel=$1 AND partition_id=$2""",
                    log_head["channel"],
                    log_head["partition_id"],
                )
                floor = 0 if archive is None else archive["retired_log_through"]
                rows = await conn.fetch(
                    """SELECT log_sequence FROM partition_log_entries
                       WHERE channel=$1 AND partition_id=$2 ORDER BY log_sequence""",
                    log_head["channel"],
                    log_head["partition_id"],
                )
                if [row["log_sequence"] for row in rows] != list(
                    range(floor + 1, log_head["next_log_sequence"])
                ):
                    violations.append(
                        f"ordered_log_hot_prefix:{log_head['channel']}/{log_head['partition_id']}"
                    )
                if log_head["used_log_entries"] != len(rows):
                    violations.append(
                        f"ordered_log_credit:{log_head['channel']}/{log_head['partition_id']}"
                    )
            if await conn.fetchval(
                "SELECT to_regclass('join_retirement_archive_heads_v1') IS NOT NULL"
            ):
                r1_heads = await conn.fetch(
                    "SELECT * FROM join_retirement_archive_heads_v1 WHERE revision_id=$1",
                    revision_id,
                )
                for r1_head in r1_heads:
                    hot = await conn.fetchrow(
                        """SELECT m.* FROM join_retention_anchors_v1 a
                           JOIN join_retirement_manifests_v1 m
                             ON m.operation_id=a.operation_id
                           WHERE a.revision_id=$1 AND a.participant_id=$2""",
                        revision_id,
                        r1_head["participant_id"],
                    )
                    if hot is None or (
                        hot["prior_summary_digest"]
                        != r1_head["archived_summary_digest"]
                    ):
                        violations.append(
                            f"join_archive_chain:{r1_head['participant_id']}"
                        )
            for table in ("region_edge_cuts_v1", "region_recovery_plans_v1"):
                if await conn.fetchval(
                    "SELECT to_regclass($1) IS NOT NULL", table
                ) and await conn.fetchval(
                    f"""SELECT EXISTS(SELECT 1 FROM {table} r
                            JOIN checkpoint_intents_v2 i USING(checkpoint_id)
                            WHERE i.revision_id=$1 AND i.checkpoint_ordinal<=$2)""",
                    revision_id,
                    retired_cp,
                ):
                    violations.append(f"regional_reference_below_horizon:{table}")
    except Exception as exc:  # noqa: BLE001 - total oracle must report malformed DB
        violations.append(f"oracle_totality:{type(exc).__name__}:{exc}")
    finally:
        await conn.close()
    return CheckpointArchiveReport(tuple(violations))
