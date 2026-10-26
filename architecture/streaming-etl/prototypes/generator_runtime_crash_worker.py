from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from durable_stream_v2 import DurableStream
from generator_runtime_v2 import GeneratorRuntime, OutputSpec


def sync_task(ctx, envelope):
    yield OutputSpec(b"result-1", managed_state={"seen": envelope.sequence})


async def async_task(ctx, envelope):
    yield OutputSpec(b"result-1", managed_state={"seen": envelope.sequence})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    parser.add_argument("mode", choices=("sync", "async"))
    parser.add_argument("point")
    parser.add_argument("--recover", action="store_true")
    args = parser.parse_args()
    store = DurableStream(args.database)
    runtime = GeneratorRuntime(store, output_channel="output")

    def fault(point: str) -> None:
        if not args.recover and point == args.point:
            os._exit(86)

    position = store.position("consumer")
    owner_id = position["owner_id"]
    owner_epoch = position["owner_epoch"]
    if args.recover and position["completed_through"] == 0:
        store.advance_time(100)
        store.claim_consumer("consumer", "tanya", 2, lease_ttl=100)
        owner_id, owner_epoch = "tanya", 2
    should_run = (
        position["completed_through"] == 0
        if not args.recover
        else position["completed_through"] < 2
    )
    if should_run:
        if args.mode == "sync":
            runtime.run_sync(
                "consumer", owner_id, owner_epoch, sync_task,
                completion_fault=fault, adapter_fault=fault,
            )
        else:
            asyncio.run(
                runtime.run_async(
                    "consumer", owner_id, owner_epoch, async_task,
                    completion_fault=fault, adapter_fault=fault,
                )
            )
    store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
