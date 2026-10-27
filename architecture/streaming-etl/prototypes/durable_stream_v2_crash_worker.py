"""Subprocess worker used by the protocol-epoch-2 abrupt-crash matrix."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from durable_object_store import DurableObjectStore
from durable_stream_v2 import (
    CompletionKind,
    DurableStream,
    ObjectRef,
    PendingOutput,
)

EXIT_CODE = 86


def crash_at(target: str):
    def crash(candidate: str) -> None:
        if candidate == target:
            os._exit(EXIT_CODE)

    return crash


def record_client_effect(path: Path) -> None:
    count = int(path.read_text()) if path.exists() else 0
    path.write_text(str(count + 1))
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def run(args: argparse.Namespace) -> None:
    stream = DurableStream(args.db)
    fault = crash_at(args.point)

    if args.scenario == "upload":
        payload = b"payload"
        ref = ObjectRef.for_payload("uploaded-object", payload, "raw/v1")
        DurableObjectStore(args.objects).upload(stream, ref, payload, fault=fault)
        fault("upload:after_ack")
    elif args.scenario == "append":
        stream.append(
            "petya",
            "p0",
            "petya-worker",
            1,
            "document-1",
            input_ref(1),
            "source:1",
            fault=fault,
        )
    elif args.scenario == "delivery":
        stream.deliver_next("anna", "anna-worker", 1, fault=fault)
    elif args.scenario in {"observe_yield", "observe_return"}:
        record_client_effect(args.effect)
        if args.scenario == "observe_yield":
            event = next(client_generator())
            if event != "yielded":
                raise AssertionError("generator did not yield")
            fault("observe:yield_before_completion")
        else:
            result = client_return()
            if result != "returned":
                raise AssertionError("function did not return")
            fault("observe:return_before_completion")
    elif args.scenario in {"complete_yield", "complete_return"}:
        token, _ = stream.deliver_next("anna", "anna-worker", 1)
        if token.sequence != args.sequence:
            raise AssertionError(
                f"expected input {args.sequence}, got {token.sequence}"
            )
        if args.scenario == "complete_yield":
            stream.complete(
                token,
                CompletionKind.YIELD,
                output=pending_output(args.sequence),
                managed_state={"handled": args.sequence},
                fault=fault,
            )
        else:
            stream.complete(
                token,
                CompletionKind.RETURN,
                managed_state={"handled": args.sequence},
                fault=fault,
            )
        fault("progress:after_ack")
    else:
        raise ValueError(f"unknown scenario: {args.scenario}")
    raise RuntimeError(f"crash point was not reached: {args.point}")


def client_generator():
    yield "yielded"


def client_return() -> str:
    return "returned"


def input_ref(sequence: int) -> ObjectRef:
    payload = f"input-{sequence}".encode()
    return ObjectRef.for_payload(f"input-{sequence}", payload, "document/v1")


def output_ref() -> ObjectRef:
    return ObjectRef.for_payload("output-1", b"signed-1", "signed/v1")


def pending_output(sequence: int = 1) -> PendingOutput:
    return PendingOutput(
        "anna-output",
        "p0",
        f"signed-{sequence}",
        output_ref(),
        f"petya:{sequence}",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--objects", type=Path, required=True)
    parser.add_argument("--effect", type=Path, required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--point", required=True)
    parser.add_argument("--sequence", type=int, default=1)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
