from __future__ import annotations

import argparse
import asyncio
import os

from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_checkpoint_retention import PostgresCheckpointRetention
from postgres_stream_v2 import PostgresStream


async def run(args) -> None:
    store = await PostgresStream.connect_existing(args.dsn, args.schema)
    try:
        coordinator = PostgresBarrierCoordinator(store)
        retention = PostgresCheckpointRetention(coordinator)
        async with store.pool.acquire() as conn:
            revision = await conn.fetchrow(
                "SELECT * FROM deployment_revisions WHERE revision_id=$1", args.revision
            )
            lease = await conn.fetchrow(
                "SELECT * FROM coordinator_leases WHERE revision_id=$1", args.revision
            )

        def crash(point: str) -> None:
            if point == args.point:
                os._exit(86)

        await retention.retire_prefix(
            args.revision,
            revision["graph_version"],
            revision["graph_digest"],
            lease["owner_id"],
            lease["owner_epoch"],
            args.operation,
            fault=crash,
        )
    finally:
        await store.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--operation", required=True)
    parser.add_argument("--point", required=True)
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
