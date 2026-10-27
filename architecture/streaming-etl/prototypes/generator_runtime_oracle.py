"""Independent raw-store oracle for the generator adapter."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def evaluate(path: Path) -> dict[str, object]:
    db = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    violations: list[str] = []
    try:
        records = db.execute("SELECT * FROM records").fetchall()
        record_by_location = {
            (row["channel_id"], row["partition_id"], row["sequence"]): row
            for row in records
        }
        objects = {
            row["object_key"]: row for row in db.execute("SELECT * FROM objects")
        }
        completions = db.execute("SELECT * FROM turn_completions").fetchall()
        for row in records:
            obj = objects.get(row["payload_key"])
            if obj is None or (
                obj["digest"],
                obj["size"],
                obj["schema_id"],
                obj["durable"],
            ) != (row["payload_digest"], row["payload_size"], row["schema_id"], 1):
                violations.append(
                    f"record_object:{row['channel_id']}:{row['sequence']}"
                )
        for consumer in db.execute("SELECT * FROM consumers"):
            owned = sorted(
                row["input_sequence"]
                for row in completions
                if row["consumer_id"] == consumer["consumer_id"]
            )
            if owned != list(range(1, consumer["completed_through"] + 1)):
                violations.append(f"completion_coverage:{consumer['consumer_id']}")
            if consumer["in_flight_sequence"] is not None and (
                consumer["in_flight_sequence"] != consumer["completed_through"] + 1
            ):
                violations.append(f"in_flight_position:{consumer['consumer_id']}")
            expected_state = "{}"
            if consumer["completed_through"] > 0:
                latest = next(
                    (
                        row
                        for row in completions
                        if row["consumer_id"] == consumer["consumer_id"]
                        and row["input_sequence"] == consumer["completed_through"]
                    ),
                    None,
                )
                expected_state = None if latest is None else latest["state_json"]
            if expected_state is None or consumer["state_json"] != expected_state:
                violations.append(
                    f"consumer_state_projection:{consumer['consumer_id']}"
                )
        referenced_outputs: set[tuple[str, str, int]] = set()
        for completion in completions:
            try:
                state = json.loads(completion["state_json"])
                manifest = json.loads(completion["output_manifest_json"])
            except json.JSONDecodeError:
                violations.append(
                    f"completion_json:{completion['consumer_id']}:{completion['input_sequence']}"
                )
                continue
            if completion["state_digest"] != digest(state):
                violations.append(
                    f"completion_state_digest:{completion['consumer_id']}:{completion['input_sequence']}"
                )
            if completion["output_manifest_digest"] != digest(manifest):
                violations.append(
                    f"completion_manifest_digest:{completion['consumer_id']}:{completion['input_sequence']}"
                )
            input_row = record_by_location.get(
                (
                    completion["input_channel_id"],
                    completion["input_partition_id"],
                    completion["input_sequence"],
                )
            )
            if (
                input_row is None
                or input_row["record_id"] != completion["input_record_id"]
            ):
                violations.append(
                    f"completion_input:{completion['consumer_id']}:{completion['input_sequence']}"
                )
            if completion["kind"] == "YIELD":
                location = (
                    completion["output_channel_id"],
                    completion["output_partition_id"],
                    completion["output_sequence"],
                )
                output = record_by_location.get(location)
                expected_manifest = (
                    None
                    if output is None
                    else {
                        "channel": output["channel_id"],
                        "partition": output["partition_id"],
                        "sequence": output["sequence"],
                        "record_id": output["record_id"],
                        "payload": {
                            "key": output["payload_key"],
                            "digest": output["payload_digest"],
                            "size": output["payload_size"],
                            "schema_id": output["schema_id"],
                        },
                        "lineage": output["lineage_digest"],
                        "producer_epoch": output["producer_epoch"],
                    }
                )
                if (
                    output is None
                    or output["record_id"] != completion["output_record_id"]
                    or manifest != expected_manifest
                ):
                    violations.append(
                        f"completion_output:{completion['consumer_id']}:{completion['input_sequence']}"
                    )
                referenced_outputs.add(location)
            elif completion["kind"] == "RETURN":
                if manifest is not None or any(
                    completion[name] is not None
                    for name in (
                        "output_channel_id",
                        "output_partition_id",
                        "output_sequence",
                        "output_record_id",
                    )
                ):
                    violations.append(
                        f"completion_return:{completion['consumer_id']}:{completion['input_sequence']}"
                    )
            else:
                violations.append(
                    f"completion_kind:{completion['consumer_id']}:{completion['input_sequence']}"
                )
        for row in records:
            location = (row["channel_id"], row["partition_id"], row["sequence"])
            if row["channel_id"] == "output" and location not in referenced_outputs:
                violations.append(f"orphan_output:{row['sequence']}")
        consumers = {
            row["consumer_id"]: row for row in db.execute("SELECT * FROM consumers")
        }
        for invocation in db.execute("SELECT * FROM runtime_invocations"):
            consumer = consumers.get(invocation["consumer_id"])
            if (
                consumer is None
                or invocation["input_sequence"] <= consumer["completed_through"]
            ):
                violations.append(f"terminal_invocation:{invocation['consumer_id']}")
                continue
            if (
                invocation["input_sequence"],
                invocation["attempt"],
                invocation["owner_id"],
                invocation["owner_epoch"],
            ) != (
                consumer["in_flight_sequence"],
                consumer["in_flight_attempt"],
                consumer["owner_id"],
                consumer["owner_epoch"],
            ):
                violations.append(f"invocation_token:{invocation['consumer_id']}")
        return {
            "passed": not violations,
            "violations": sorted(set(violations)),
            "observations": {
                "completions": len(completions),
                "outputs": len(referenced_outputs),
            },
        }
    finally:
        db.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    report = evaluate(parser.parse_args().database)
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
