"""Abrupt process-loss worker for regional mailbox replay."""

from __future__ import annotations

import argparse
import asyncio
import os

from postgres_barrier_coordinator import PostgresBarrierCoordinator, Revision, decoded
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_runtime import PostgresJoinRuntime
from postgres_recovery_regions import PostgresRecoveryRegionPlanner
from postgres_regional_recovery import PostgresRegionalRecovery
from postgres_stream_v2 import PostgresStream


@stream_task
def pass_document(ctx, payload):
    state = {"documents": ctx.managed_state.get("documents", []) + [payload.decode()]}
    yield OutputSpec(payload, "bytes/v1", state)


async def main() -> int:
    parser = argparse.ArgumentParser()
    for name in (
        "dsn",
        "schema",
        "revision_id",
        "participant_id",
        "plan_id",
        "owner_id",
        "owner_epoch",
        "fault_point",
    ):
        parser.add_argument(name)
    args = parser.parse_args()
    stream = await PostgresStream.connect_existing(args.dsn, args.schema)
    coordinator = PostgresBarrierCoordinator(stream)
    async with stream.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM deployment_revisions WHERE revision_id=$1",
            args.revision_id,
        )
    revision = Revision(
        row["revision_id"],
        row["deployment_id"],
        row["graph_version"],
        row["graph_digest"],
        decoded(row["source_manifest"]),
        decoded(row["participant_manifest"]),
    )
    planner = PostgresRecoveryRegionPlanner(coordinator)
    regional = PostgresRegionalRecovery(coordinator, planner, revision)
    runtime = PostgresJoinRuntime(coordinator, revision, args.participant_id)

    def crash(point: str) -> None:
        if point == args.fault_point:
            os._exit(86)

    await regional.replay_worker(
        args.plan_id,
        runtime,
        pass_document,
        args.owner_id,
        int(args.owner_epoch),
        fault=crash,
    )
    await stream.close()
    return 3


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
