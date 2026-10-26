"""Independent read-only oracle for reference-safe retention and pressure."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def evaluate(path: Path) -> dict[str, Any]:
    db = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    violations: list[str] = []
    try:
        config = db.execute("SELECT * FROM channel_config").fetchone()
        state = db.execute("SELECT * FROM partition_state").fetchone()
        records = db.execute("SELECT * FROM records ORDER BY sequence").fetchall()
        tombstones = db.execute(
            "SELECT * FROM compaction_tombstones ORDER BY sequence"
        ).fetchall()
        objects = {row["object_key"]: row for row in db.execute("SELECT * FROM objects")}
        sequences = [row["sequence"] for row in records]
        tombstone_sequences = [row["sequence"] for row in tombstones]
        covered = sorted(sequences + tombstone_sequences)
        if covered != list(range(1, state["next_sequence"])):
            violations.append("sequence_coverage")
        if set(sequences) & set(tombstone_sequences):
            violations.append("sequence_overlap")
        if sequences and sequences[-1] >= state["next_sequence"]:
            violations.append("partition_head")
        used_bytes = sum(
            objects[key]["payload_size"] for key in {row["object_key"] for row in records}
            if key in objects
        )
        if len(records) > config["max_records"] or used_bytes > config["max_bytes"]:
            violations.append("capacity_exceeded")
        for row in records:
            obj = objects.get(row["object_key"])
            if obj is None or (
                obj["payload_digest"], obj["payload_size"]
            ) != (row["payload_digest"], row["payload_size"]):
                violations.append(f"object_identity:{row['sequence']}")
        referenced = {row["object_key"] for row in records}
        for key in objects:
            if key not in referenced:
                violations.append(f"orphan_object:{key}")
        live = set(sequences)
        max_sequence = state["next_sequence"] - 1
        roots: list[tuple[str, str, int]] = []
        expected_events: set[tuple[str, str, int, str]] = set()
        declared_consumers = {
            row["consumer_id"]: row["start_sequence"]
            for row in db.execute("SELECT * FROM consumer_manifest")
        }
        consumers = db.execute("SELECT * FROM consumers").fetchall()
        if {row["consumer_id"] for row in consumers} != set(declared_consumers):
            violations.append("consumer_manifest")
        for row in consumers:
            start = declared_consumers.get(row["consumer_id"])
            if start is None:
                continue
            completions = [
                item for item in db.execute(
                    "SELECT sequence FROM consumer_completions "
                    "WHERE consumer_id = ? ORDER BY sequence",
                    (row["consumer_id"],),
                )
            ]
            if [item["sequence"] for item in completions] != list(
                range(start, row["completed_through"] + 1)
            ):
                violations.append(f"consumer_completion:{row['consumer_id']}")
            for completion in db.execute(
                "SELECT * FROM consumer_completions WHERE consumer_id = ?",
                (row["consumer_id"],),
            ):
                if completion["witness_id"] != digest(
                    {"consumer": row["consumer_id"], "sequence": completion["sequence"]}
                ):
                    violations.append(f"completion_identity:{row['consumer_id']}")
            if row["in_flight_sequence"] is not None and (
                row["in_flight_sequence"] != row["completed_through"] + 1
                or row["in_flight_sequence"] not in live
            ):
                violations.append(f"consumer_in_flight:{row['consumer_id']}")
            roots.append(("CONSUMER", row["consumer_id"], row["completed_through"] + 1))
            expected_events.add(("CONSUMER", row["consumer_id"], start, "ACTIVE"))
            if row["in_flight_sequence"] is not None:
                roots.append(("IN_FLIGHT", row["consumer_id"], row["in_flight_sequence"]))
        for row in db.execute("SELECT * FROM checkpoints"):
            if row["intent_digest"] != digest(
                {"checkpoint": row["checkpoint"], "cut": row["cut_sequence"]}
            ):
                violations.append(f"checkpoint_identity:{row['checkpoint']}")
            expected_events.add(
                ("CHECKPOINT", str(row["checkpoint"]), row["cut_sequence"] + 1, "ACTIVE")
            )
            if row["status"] == "RELEASED":
                expected_events.add(
                    ("CHECKPOINT", str(row["checkpoint"]), row["cut_sequence"] + 1, "RELEASED")
                )
            if row["status"] == "SEALED":
                roots.append(("CHECKPOINT", str(row["checkpoint"]), row["cut_sequence"] + 1))
        for row in db.execute("SELECT * FROM durable_refs"):
            expected_events.add(
                (row["ref_kind"], row["ref_id"], row["sequence"], "ACTIVE")
            )
            if row["status"] == "RELEASED":
                expected_events.add(
                    (row["ref_kind"], row["ref_id"], row["sequence"], "RELEASED")
                )
            if row["status"] == "ACTIVE":
                roots.append((row["ref_kind"], row["ref_id"], row["sequence"]))
        actual_events: set[tuple[str, str, int, str]] = set()
        for event in db.execute("SELECT * FROM root_lifecycle_events"):
            identity = {
                "event_id": event["event_id"],
                "root_kind": event["root_kind"],
                "root_id": event["root_id"],
                "sequence": event["sequence"],
                "transition": event["transition"],
            }
            if event["identity_digest"] != digest(identity):
                violations.append(f"root_event_identity:{event['event_id']}")
            expected_event_id = (
                f"{event['root_kind']}:{event['root_id']}:{event['transition']}"
            )
            if event["event_id"] != expected_event_id:
                violations.append(f"root_event_id:{event['event_id']}")
            actual_events.add(
                (
                    event["root_kind"], event["root_id"], event["sequence"],
                    event["transition"],
                )
            )
        if actual_events != expected_events:
            violations.append("root_lifecycle_authority")
        for kind, ref_id, sequence in roots:
            if sequence <= max_sequence and sequence not in live:
                violations.append(f"missing_root:{kind}:{ref_id}:{sequence}")
        deleted_before: set[int] = set()
        tombstones_by_manifest: dict[int, list[sqlite3.Row]] = {}
        for tombstone in tombstones:
            tombstones_by_manifest.setdefault(tombstone["manifest_id"], []).append(tombstone)
        for row in db.execute("SELECT * FROM compaction_manifests ORDER BY manifest_id"):
            value = {
                "requested_through": row["requested_through"],
                "safe_through": row["safe_through"],
                "roots": json.loads(row["roots_json"]),
                "records": json.loads(row["records_json"]),
                "objects": json.loads(row["objects_json"]),
            }
            if row["manifest_digest"] != digest(value):
                violations.append(f"manifest_digest:{row['manifest_id']}")
            oldest = min(
                (root["sequence"] - 1 for root in value["roots"]),
                default=row["requested_through"],
            )
            if row["safe_through"] != min(row["requested_through"], oldest):
                violations.append(f"manifest_safe_cut:{row['manifest_id']}")
            expected_records = [
                sequence for sequence in range(1, row["safe_through"] + 1)
                if sequence not in deleted_before
            ]
            if value["records"] != expected_records:
                violations.append(f"manifest_record_set:{row['manifest_id']}")
            if row["status"] == "COMPLETED":
                if any(sequence in live for sequence in value["records"]):
                    violations.append(f"manifest_records_visible:{row['manifest_id']}")
                if any(key in objects for key in value["objects"]):
                    violations.append(f"manifest_objects_visible:{row['manifest_id']}")
                actual = sorted(
                    item["sequence"]
                    for item in tombstones_by_manifest.get(row["manifest_id"], [])
                )
                if actual != value["records"]:
                    violations.append(f"manifest_tombstones:{row['manifest_id']}")
                deleted_before.update(actual)
            else:
                violations.append(f"manifest_incomplete:{row['manifest_id']}")
        for attempt in db.execute("SELECT * FROM append_attempts"):
            identity = {
                "attempt_id": attempt["attempt_id"],
                "record_id": attempt["record_id"],
                "object_key": attempt["object_key"],
                "payload_digest": attempt["payload_digest"],
                "payload_size": attempt["payload_size"],
                "schema_id": attempt["schema_id"],
                "lineage_digest": attempt["lineage_digest"],
            }
            if attempt["identity_digest"] != digest(identity):
                violations.append(f"append_attempt_identity:{attempt['attempt_id']}")
            if attempt["status"] == "APPENDED":
                record = next(
                    (item for item in records if item["sequence"] == attempt["sequence"]),
                    None,
                )
                tombstone = next(
                    (item for item in tombstones if item["sequence"] == attempt["sequence"]),
                    None,
                )
                witness = record or tombstone
                if record is not None:
                    obj = objects.get(record["object_key"])
                    schema_id = None if obj is None else obj["schema_id"]
                else:
                    schema_id = None if tombstone is None else tombstone["schema_id"]
                actual_identity = None if witness is None else (
                    witness["record_id"], witness["object_key"],
                    witness["payload_digest"], witness["payload_size"], schema_id,
                    witness["lineage_digest"],
                )
                expected_identity = (
                    attempt["record_id"], attempt["object_key"],
                    attempt["payload_digest"], attempt["payload_size"],
                    attempt["schema_id"], attempt["lineage_digest"],
                )
                if actual_identity != expected_identity:
                    violations.append(f"append_attempt_result:{attempt['attempt_id']}")
            elif any(
                item["record_id"] == attempt["record_id"]
                for item in [*records, *tombstones]
            ):
                violations.append(f"blocked_attempt_visible:{attempt['attempt_id']}")
        return {
            "passed": not violations,
            "violations": sorted(set(violations)),
            "observations": {
                "records": len(records),
                "bytes": used_bytes,
                "next_sequence": state["next_sequence"],
                "roots": sorted(roots),
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
