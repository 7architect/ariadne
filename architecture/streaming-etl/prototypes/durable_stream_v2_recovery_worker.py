"""Fresh-process recovery worker for the sequence-200 acceptance scenarios."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from durable_stream_v2 import CompletionKind, DeliveryToken, DurableStream
from durable_stream_v2_crash_worker import pending_output


def run(args: argparse.Namespace) -> None:
    stream = DurableStream(args.db)
    kind = CompletionKind(args.kind.upper())
    if args.resume == 201:
        old_token = DeliveryToken("anna", "petya", "p0", 200, 200, "anna-worker", 1)
        stream.complete(
            old_token,
            kind,
            output=pending_output(200) if kind is CompletionKind.YIELD else None,
            managed_state={"handled": 200},
        )

    stream.advance_time(10)
    stream.claim_consumer("anna", "tanya-worker", 2)
    token, envelope = stream.deliver_next("anna", "tanya-worker", 2)
    if envelope.sequence != args.resume:
        raise AssertionError(
            f"expected Tanya to receive {args.resume}, got {envelope.sequence}"
        )
    if args.resume == 200:
        stream.complete(
            token,
            kind,
            output=pending_output(200) if kind is CompletionKind.YIELD else None,
            managed_state={"handled": 200},
        )
        token, envelope = stream.deliver_next("anna", "tanya-worker", 2)
        if envelope.sequence != 201:
            raise AssertionError(f"expected 201 after retry, got {envelope.sequence}")
    output_count = len(stream.records("anna-output", "p0"))
    expected_outputs = 200 if kind is CompletionKind.YIELD else 0
    if output_count != expected_outputs:
        raise AssertionError(f"expected {expected_outputs} outputs, got {output_count}")
    print(
        json.dumps(
            {
                "received": args.resume,
                "next": envelope.sequence,
                "output_count": output_count,
            },
            sort_keys=True,
        )
    )
    stream.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--resume", type=int, choices=(200, 201), required=True)
    parser.add_argument("--kind", choices=("yield", "return"), required=True)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
