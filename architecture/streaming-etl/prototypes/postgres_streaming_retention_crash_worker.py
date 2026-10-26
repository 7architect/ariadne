"""Real process-loss worker for the multi-transaction R3-R4 composition."""

from __future__ import annotations

import argparse
import asyncio
import os

from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_stream_v2 import PostgresStream
from postgres_streaming_retention import PostgresStreamingRetention


async def run(args: argparse.Namespace) -> None:
    stream = await PostgresStream.connect_existing(args.dsn, args.schema)
    try:
        retention = PostgresStreamingRetention(
            stream, PostgresBarrierCoordinator(stream)
        )

        def crash(point: str) -> None:
            if point == args.fault:
                os._exit(86)

        await retention.compact_latest(
            args.revision,
            args.graph_version,
            args.graph_digest,
            args.owner,
            args.owner_epoch,
            args.operation,
            fault=crash,
        )
    finally:
        await stream.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("schema")
    parser.add_argument("revision")
    parser.add_argument("graph_version")
    parser.add_argument("graph_digest")
    parser.add_argument("owner")
    parser.add_argument("owner_epoch", type=int)
    parser.add_argument("operation")
    parser.add_argument("fault")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
