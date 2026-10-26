"""Fresh-process exact retry and convergence worker for DS-004b."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from durable_stream_barrier_v2 import ComposedDurableStream


def append(store: ComposedDurableStream, input_id: str, sequence: int) -> None:
    store.append_data(
        input_id,
        "petya",
        1,
        f"{input_id}-{sequence}",
        f"ref:{input_id}:{sequence}",
    )


def run(args: argparse.Namespace) -> None:
    store = ComposedDurableStream(args.db)
    result: dict[str, object] = {"scenario": args.scenario}
    if args.scenario == "inject":
        intent = store.connection.execute(
            "SELECT intent_digest FROM checkpoint_intents WHERE checkpoint = 1"
        ).fetchone()[0]
        store.inject_barrier(1, "left", "coord", 1, intent)
        append(store, "left", 2)
        store.process_next("left")
        store.process_next("left")
        store.process_next("left")
        store.complete_checkpoint(1)
    elif args.scenario == "observe_first":
        if store.progress()["left"]["blocked_checkpoint"] is None:
            store.process_next("left")
        entry = store.process_next("right")
        if entry is None or entry.data_sequence != 2:
            raise AssertionError("right input did not remain live")
        store.process_next("right")
        store.complete_checkpoint(1)
    elif args.scenario == "align_final":
        if store.status(1) == "STARTED":
            store.process_next("right")
        store.complete_checkpoint(1)
    elif args.scenario == "complete":
        store.complete_checkpoint(1)
    elif args.scenario == "fence":
        first = store.begin_recovery()
        if store.begin_recovery() != first:
            raise AssertionError("recovery fence retry changed identity")
        result["fence_ordinal"] = first
    elif args.scenario == "recover":
        outputs_before = store.outputs()
        recovery = store.recover()
        if store.outputs() != outputs_before:
            raise AssertionError("recovery duplicated output identity")
        append(store, "left", 4)
        store.process_next("left")
        generation = store.connection.execute(
            "SELECT recovery_generation FROM turns "
            "ORDER BY apply_ordinal DESC LIMIT 1"
        ).fetchone()[0]
        if generation != 1:
            raise AssertionError("post-recovery turn has wrong generation")
        result.update(recovery)
        result["post_recovery_generation"] = generation
    else:
        raise ValueError(f"unknown scenario: {args.scenario}")
    result["status"] = store.status(1)
    result["outputs"] = len(store.outputs())
    print(json.dumps(result, sort_keys=True))
    store.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--scenario", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
