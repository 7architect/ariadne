"""Read-only oracle for Prefect-driven checkpoint/stream retirement."""

from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

import asyncpg


async def evaluate(dsn: str, schema: str, revision_id: str) -> dict[str, Any]:
    violations: list[str] = []
    conn = await asyncpg.connect(
        dsn, server_settings={"search_path": f'"{schema}",public'}
    )
    try:
        await conn.execute("SET default_transaction_read_only=on")
        async with conn.transaction(readonly=True):
            operation = await conn.fetchrow(
                "SELECT * FROM streaming_retention_operations_v1 WHERE revision_id=$1",
                revision_id,
            )
            if operation is None or operation["status"] != "COMPLETED":
                violations.append("maintenance_stream_retention_incomplete")
            else:
                authority: dict[str, list[int]] = defaultdict(list)
                for row in await conn.fetch(
                    "SELECT channel,partition_id,cut_data_sequence FROM "
                    "source_barrier_observations WHERE checkpoint_id=$1",
                    operation["checkpoint_id"],
                ):
                    authority[f"{row['channel']}/{row['partition_id']}"].append(
                        row["cut_data_sequence"]
                    )
                for row in await conn.fetch(
                    "SELECT output_barriers FROM participant_checkpoint_acks "
                    "WHERE checkpoint_id=$1",
                    operation["checkpoint_id"],
                ):
                    for location, value in json.loads(row["output_barriers"]).items():
                        authority[location].append(value["cut_data_sequence"])
                if any(len(set(values)) != 1 for values in authority.values()):
                    violations.append("maintenance_checkpoint_cut_ambiguous")
                expected = [
                    {
                        "channel": location.split("/", 1)[0],
                        "partition": location.split("/", 1)[1],
                        "through": values[0],
                    }
                    for location, values in sorted(authority.items())
                    if values[0] > 0
                ]
                actual = json.loads(operation["cuts_json"])
                if actual != expected:
                    violations.append("maintenance_positive_cut_projection")
                if any(item["through"] <= 0 for item in actual):
                    violations.append("maintenance_zero_cut_summarized")
                outcome = json.loads(operation["outcome_json"])
                if outcome.get("canonical_cuts") != expected:
                    violations.append("maintenance_outcome_cut_projection")
    except Exception as exc:  # noqa: BLE001 - total hostile reader
        violations.append(f"maintenance_oracle_error:{type(exc).__name__}")
    finally:
        await conn.close()
    return {"passed": not violations, "violations": violations}
