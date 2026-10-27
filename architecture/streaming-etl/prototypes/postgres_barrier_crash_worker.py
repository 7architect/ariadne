"""Abrupt-exit worker for PostgreSQL coordinator checkpoint boundaries."""

from __future__ import annotations

import argparse
import asyncio
import os

from postgres_barrier_coordinator import PostgresBarrierCoordinator
from postgres_stream_v2 import PostgresStream


def crash_at(expected: str):
    def fault(point: str) -> None:
        if point == expected:
            os._exit(86)

    return fault


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("schema")
    parser.add_argument("revision_id")
    parser.add_argument("graph_version")
    parser.add_argument("graph_digest")
    parser.add_argument("owner_id")
    parser.add_argument("owner_epoch", type=int)
    parser.add_argument("operation")
    parser.add_argument("identity")
    parser.add_argument("fault_point")
    args = parser.parse_args()
    stream = await PostgresStream.connect_existing(args.dsn, args.schema)
    coordinator = PostgresBarrierCoordinator(stream)
    fault = crash_at(args.fault_point)
    try:
        common = (
            args.revision_id,
            args.graph_version,
            args.graph_digest,
            args.owner_id,
            args.owner_epoch,
        )
        if args.operation == "intent":
            await coordinator.start_checkpoint(*common, args.identity, fault=fault)
        elif args.operation == "barrier":
            await coordinator.inject_source_barrier(
                *common, args.identity, "input", "left", fault=fault
            )
        elif args.operation == "participant":
            await coordinator.acknowledge_participant(
                *common,
                args.identity,
                "join",
                {"count": 2},
                mailbox_highwater=2,
                state_version=1,
                fault=fault,
            )
        elif args.operation == "complete":
            await coordinator.complete_checkpoint(*common, args.identity, fault=fault)
        elif args.operation == "recovery_plan":
            await coordinator.begin_recovery(*common, args.identity, fault=fault)
        elif args.operation == "recovery_ready":
            await coordinator.finish_recovery(*common, args.identity, fault=fault)
        else:
            raise ValueError("unknown operation")
    finally:
        await stream.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
