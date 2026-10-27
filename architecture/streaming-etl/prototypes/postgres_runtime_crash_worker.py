from __future__ import annotations

import argparse
import asyncio
import os

from postgres_generator_runtime import OutputSpec, PostgresGeneratorRuntime, stream_task
from postgres_stream_v2 import PostgresStream


@stream_task
def sync_task(ctx, envelope):
    yield OutputSpec(
        f"signed-{envelope.sequence}".encode(),
        managed_state={"count": ctx.managed_state.get("count", 0) + 1},
    )


@stream_task
async def async_task(ctx, envelope):
    yield OutputSpec(
        f"signed-{envelope.sequence}".encode(),
        managed_state={"count": ctx.managed_state.get("count", 0) + 1},
    )


@stream_task
def sync_return(ctx, envelope):
    if False:
        yield OutputSpec(b"never")
    return {"count": ctx.managed_state.get("count", 0) + 1}


@stream_task
async def async_return(ctx, envelope):
    if False:
        yield OutputSpec(b"never")


async def execute(args: argparse.Namespace) -> int:
    store = await PostgresStream.open(args.dsn, args.schema)
    position = await store.position("reviewer")
    owner, epoch = position["owner_id"], position["owner_epoch"]
    if args.recover and position["completed_through"] == 0:
        async with store.pool.acquire() as conn:
            await conn.execute(
                "UPDATE consumers SET lease_expires_at=clock_timestamp()-interval '1 second' "
                "WHERE consumer_id='reviewer'"
            )
            await conn.execute(
                "UPDATE runtime_invocations SET expires_at=clock_timestamp()-interval '1 second' "
                "WHERE consumer_id='reviewer'"
            )
        owner = "tanya"
        epoch = await store.claim_consumer("reviewer", owner, lease_seconds=60)
    runtime = PostgresGeneratorRuntime(
        store, output_channel="output", state_schema="counter", state_version=1
    )

    def fault(point: str) -> None:
        if not args.recover and point == args.point:
            os._exit(86)

    should_run = (
        position["completed_through"] == 0
        if not args.recover
        else position["completed_through"] < 2
    )
    if should_run:
        sync_callable = sync_task if args.kind == "yield" else sync_return
        async_callable = async_task if args.kind == "yield" else async_return
        if args.mode == "sync":
            await runtime.run_sync("reviewer", owner, epoch, sync_callable, fault=fault)
        else:
            await runtime.run_async(
                "reviewer", owner, epoch, async_callable, fault=fault
            )
    await store.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("schema")
    parser.add_argument("mode", choices=("sync", "async"))
    parser.add_argument("point")
    parser.add_argument("--kind", choices=("yield", "return"), default="yield")
    parser.add_argument("--recover", action="store_true")
    return asyncio.run(execute(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
