"""Deterministic abrupt process loss in the live PostgreSQL join runtime."""

import argparse
import asyncio
import os

from postgres_barrier_coordinator import PostgresBarrierCoordinator, Revision, decoded
from postgres_join_runtime import PostgresJoinRuntime
from postgres_stream_v2 import PostgresStream
from test_postgres_join_runtime import ordered_documents


async def main():
    p = argparse.ArgumentParser()
    for name in (
        "dsn",
        "schema",
        "revision",
        "participant",
        "owner",
        "epoch",
        "operation",
        "identity",
        "point",
    ):
        p.add_argument(name)
    a = p.parse_args()
    stream = await PostgresStream.connect_existing(a.dsn, a.schema)
    coordinator = PostgresBarrierCoordinator(stream)
    async with stream.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM deployment_revisions WHERE revision_id=$1", a.revision
        )
        lease = await conn.fetchrow(
            "SELECT * FROM coordinator_leases WHERE revision_id=$1", a.revision
        )
    revision = Revision(
        row["revision_id"],
        row["deployment_id"],
        row["graph_version"],
        row["graph_digest"],
        decoded(row["source_manifest"]),
        decoded(row["participant_manifest"]),
    )
    runtime = PostgresJoinRuntime(coordinator, revision, a.participant)

    def crash(point):
        if point == a.point:
            os._exit(86)

    if a.operation == "step":
        await runtime.step(
            a.identity,
            ordered_documents,
            a.owner,
            int(a.epoch),
            coordinator_owner=lease["owner_id"],
            coordinator_epoch=lease["owner_epoch"],
            fault=crash,
        )
    elif a.operation == "restore":
        await runtime.restore(
            a.identity, ordered_documents, a.owner, int(a.epoch), fault=crash
        )
    else:
        raise ValueError("unknown operation")
    await stream.close()
    return 3


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
