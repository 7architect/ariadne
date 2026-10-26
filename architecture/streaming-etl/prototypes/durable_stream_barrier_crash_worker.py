"""DS-004b worker that exits at named DATA/BARRIER durability boundaries."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from durable_stream_barrier_v2 import ComposedDurableStream


EXIT_CODE = 86


def crash_at(target: str):
    def crash(candidate: str) -> None:
        if candidate == target:
            os._exit(EXIT_CODE)

    return crash


def durable_ack(path: Path) -> None:
    path.write_text("ACK")
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def run(args: argparse.Namespace) -> None:
    store = ComposedDurableStream(args.db)
    fault = crash_at(args.point)
    if args.scenario == "inject":
        intent = store.connection.execute(
            "SELECT intent_digest FROM checkpoint_intents WHERE checkpoint = 1"
        ).fetchone()[0]
        store.inject_barrier(1, "left", "coord", 1, intent, fault=fault)
    elif args.scenario == "observe_first":
        store.process_next("left", fault=fault)
    elif args.scenario == "align_final":
        store.process_next("right", fault=fault)
    elif args.scenario == "complete":
        store.complete_checkpoint(1, fault=fault)
    elif args.scenario == "fence":
        store.begin_recovery(fault=fault)
    elif args.scenario == "recover":
        store.recover(fault=fault)
    else:
        raise ValueError(f"unknown scenario: {args.scenario}")

    fault(f"{args.scenario}:after_durable_before_ack")
    durable_ack(args.ack)
    fault(f"{args.scenario}:after_ack")
    raise RuntimeError(f"crash point was not reached: {args.point}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--ack", type=Path, required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--point", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
