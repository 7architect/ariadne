"""Independent read-only safety oracle for the durable stream v2 prototype."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class OracleReport:
    violations: tuple[str, ...]
    observations: dict[str, Any]
    orphan_blobs: tuple[str, ...]
    staging_uploads: tuple[str, ...]
    resume_by_consumer: dict[str, int]

    @property
    def passed(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "violations": list(self.violations),
            "observations": self.observations,
            "orphan_blobs": list(self.orphan_blobs),
            "staging_uploads": list(self.staging_uploads),
            "resume_by_consumer": self.resume_by_consumer,
        }


class DurableStreamOracle:
    """Reconstructs declared graph invariants without runtime imports."""

    @classmethod
    def evaluate(
        cls, database: Path, object_root: Path, manifest_path: Path
    ) -> OracleReport:
        violations: list[str] = []
        manifest = cls._load_manifest(manifest_path, violations)
        connection = sqlite3.connect(
            f"file:{database.resolve()}?mode=ro", uri=True
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        resume: dict[str, int] = {}
        observations: dict[str, Any] = {}
        try:
            if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                violations.append("sqlite_integrity")
            if connection.execute("PRAGMA foreign_key_check").fetchall():
                violations.append("foreign_key_integrity")

            objects = {
                row["object_key"]: row
                for row in connection.execute("SELECT * FROM objects")
            }
            published, staging = cls._filesystem_observation(object_root)
            cls._check_objects(objects, published, violations)
            records = cls._check_records(connection, objects, violations)
            cls._check_declared_consumers(
                connection,
                manifest,
                records,
                violations,
                observations,
                resume,
            )
            orphan_blobs = tuple(sorted(set(published) - set(objects)))
        finally:
            connection.rollback()
            connection.close()
        return OracleReport(
            tuple(sorted(set(violations))),
            observations,
            orphan_blobs,
            staging,
            resume,
        )

    @staticmethod
    def _load_manifest(
        path: Path, violations: list[str]
    ) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError):
            violations.append("fixture_manifest_invalid")
            return {"consumers": []}
        if not isinstance(value, dict) or not isinstance(
            value.get("consumers"), list
        ):
            violations.append("fixture_manifest_invalid")
            return {"consumers": []}
        return value

    @staticmethod
    def _filesystem_observation(
        root: Path,
    ) -> tuple[dict[str, Path], tuple[str, ...]]:
        if not root.exists():
            return {}, ()
        published = {
            path.name: path
            for path in root.iterdir()
            if path.is_file() and not path.name.startswith(".")
        }
        staging = tuple(
            sorted(
                path.name
                for path in root.iterdir()
                if path.is_file() and path.name.startswith(".")
            )
        )
        return published, staging

    @staticmethod
    def _check_objects(
        objects: dict[str, sqlite3.Row],
        published: dict[str, Path],
        violations: list[str],
    ) -> None:
        for key, witness in objects.items():
            if Path(key).name != key or key in {"", ".", ".."}:
                violations.append(f"blob_witness_integrity:unsafe_key:{key}")
            path = published.get(key)
            if witness["durable"] != 1:
                continue
            if path is None:
                violations.append(f"blob_witness_integrity:missing_blob:{key}")
                continue
            payload = path.read_bytes()
            if len(payload) != witness["size"]:
                violations.append(f"blob_witness_integrity:size:{key}")
            if hashlib.sha256(payload).hexdigest() != witness["digest"]:
                violations.append(f"blob_witness_integrity:digest:{key}")

    @staticmethod
    def _check_records(
        connection: sqlite3.Connection,
        objects: dict[str, sqlite3.Row],
        violations: list[str],
    ) -> dict[tuple[str, str], list[sqlite3.Row]]:
        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in connection.execute(
            "SELECT * FROM records ORDER BY channel_id, partition_id, sequence"
        ):
            group = (row["channel_id"], row["partition_id"])
            groups.setdefault(group, []).append(row)
            witness = objects.get(row["payload_key"])
            if witness is None or witness["durable"] != 1:
                violations.append(
                    "visible_payload_is_durable:"
                    f"{group[0]}:{group[1]}:{row['sequence']}"
                )
            elif (
                row["payload_digest"],
                row["payload_size"],
                row["schema_id"],
            ) != (witness["digest"], witness["size"], witness["schema_id"]):
                violations.append(
                    "visible_payload_witness_mismatch:"
                    f"{group[0]}:{group[1]}:{row['sequence']}"
                )
        for (channel, partition), rows in groups.items():
            sequences = [row["sequence"] for row in rows]
            if sequences != list(range(1, max(sequences, default=0) + 1)):
                violations.append(
                    f"partition_sequence_is_contiguous:{channel}:{partition}"
                )
            record_ids = [row["record_id"] for row in rows]
            if len(record_ids) != len(set(record_ids)):
                violations.append(
                    f"partition_record_id_is_unique:{channel}:{partition}"
                )
        return groups

    @classmethod
    def _check_declared_consumers(
        cls,
        connection: sqlite3.Connection,
        manifest: dict[str, Any],
        records: dict[tuple[str, str], list[sqlite3.Row]],
        violations: list[str],
        observations: dict[str, Any],
        resume: dict[str, int],
    ) -> None:
        declarations = manifest["consumers"]
        declared_ids = [
            value.get("consumer_id")
            for value in declarations
            if isinstance(value, dict)
            and isinstance(value.get("consumer_id"), str)
        ]
        stored_ids = {
            row["consumer_id"]
            for row in connection.execute("SELECT consumer_id FROM consumers")
        }
        if len(declared_ids) != len(set(declared_ids)):
            violations.append("fixture_consumer_set_is_exact:duplicate")
        for consumer_id in sorted(stored_ids - set(declared_ids)):
            violations.append(
                f"fixture_consumer_set_is_exact:missing:{consumer_id}"
            )
        for consumer_id in sorted(set(declared_ids) - stored_ids):
            violations.append(
                f"fixture_consumer_set_is_exact:unknown:{consumer_id}"
            )
        for declaration in declarations:
            if not isinstance(declaration, dict):
                violations.append("fixture_manifest_invalid_consumer")
                continue
            consumer_id = declaration.get("consumer_id")
            input_spec = declaration.get("input", {})
            output_spec = declaration.get("output", {})
            if not (
                isinstance(consumer_id, str)
                and isinstance(input_spec, dict)
                and isinstance(output_spec, dict)
                and isinstance(input_spec.get("channel"), str)
                and isinstance(input_spec.get("partition"), str)
                and declaration.get("completion_kind") in {"YIELD", "RETURN"}
                and isinstance(output_spec.get("channel"), str)
                and isinstance(output_spec.get("partition"), str)
                and isinstance(output_spec.get("record_prefix"), str)
                and isinstance(output_spec.get("lineage_prefix"), str)
                and output_spec.get("exclusive") is True
                and isinstance(output_spec.get("start_sequence"), int)
                and output_spec["start_sequence"] > 0
            ):
                violations.append("fixture_manifest_invalid_consumer")
                continue
            consumer = connection.execute(
                "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
            ).fetchone()
            if consumer is None:
                violations.append(f"declared_consumer_missing:{consumer_id}")
                continue
            input_key = (input_spec.get("channel"), input_spec.get("partition"))
            output_key = (
                output_spec.get("channel"),
                output_spec.get("partition"),
            )
            input_rows = records.get(input_key, [])
            output_rows = records.get(output_key, [])
            cls._check_consumer_position(
                consumer, input_spec, input_rows, violations
            )
            completions = connection.execute(
                """SELECT * FROM turn_completions
                   WHERE consumer_id = ? ORDER BY input_sequence""",
                (consumer_id,),
            ).fetchall()
            cls._check_completion_coverage(consumer, completions, violations)
            cls._check_state(consumer, completions, violations)
            cls._check_completions(
                connection,
                declaration,
                input_rows,
                output_rows,
                completions,
                violations,
            )
            completed = consumer["completed_through"]
            resume[consumer_id] = completed + 1
            observations[consumer_id] = {
                "completed_through": completed,
                "in_flight": consumer["in_flight_sequence"],
                "completion_count": len(completions),
                "output_count": len(output_rows),
            }

    @staticmethod
    def _check_consumer_position(
        consumer: sqlite3.Row,
        input_spec: dict[str, Any],
        input_rows: list[sqlite3.Row],
        violations: list[str],
    ) -> None:
        consumer_id = consumer["consumer_id"]
        maximum = max((row["sequence"] for row in input_rows), default=0)
        valid = (
            (consumer["channel_id"], consumer["partition_id"])
            == (input_spec.get("channel"), input_spec.get("partition"))
            and 0 <= consumer["completed_through"] <= maximum
            and consumer["owner_epoch"] >= 0
            and consumer["in_flight_attempt"] >= 0
        )
        in_flight = consumer["in_flight_sequence"]
        if in_flight is not None:
            valid = valid and (
                in_flight == consumer["completed_through"] + 1
                and in_flight <= maximum
                and consumer["in_flight_attempt"] > 0
                and consumer["owner_id"] is not None
                and consumer["owner_epoch"] > 0
            )
        if not valid:
            violations.append(f"consumer_position_is_valid:{consumer_id}")

    @staticmethod
    def _check_completion_coverage(
        consumer: sqlite3.Row,
        completions: list[sqlite3.Row],
        violations: list[str],
    ) -> None:
        consumer_id = consumer["consumer_id"]
        completed = consumer["completed_through"]
        sequences = [row["input_sequence"] for row in completions]
        if sequences != list(range(1, completed + 1)):
            violations.append(f"completion_coverage_is_exact:{consumer_id}")

    @staticmethod
    def _check_state(
        consumer: sqlite3.Row,
        completions: list[sqlite3.Row],
        violations: list[str],
    ) -> None:
        consumer_id = consumer["consumer_id"]
        state_json = consumer["state_json"]
        try:
            state = json.loads(state_json)
        except (TypeError, json.JSONDecodeError):
            violations.append(f"state_is_atomic:consumer_json:{consumer_id}")
            return
        if _canonical(state) != state_json:
            violations.append(f"state_is_atomic:consumer_canonical:{consumer_id}")
        completed = consumer["completed_through"]
        latest = next(
            (
                row
                for row in completions
                if row["input_sequence"] == completed
            ),
            None,
        )
        expected = "{}" if completed == 0 else (
            latest["state_json"] if latest is not None else None
        )
        if state_json != expected:
            violations.append(f"state_is_atomic:cursor_state:{consumer_id}")

    @classmethod
    def _check_completions(
        cls,
        connection: sqlite3.Connection,
        declaration: dict[str, Any],
        input_rows: list[sqlite3.Row],
        output_rows: list[sqlite3.Row],
        completions: list[sqlite3.Row],
        violations: list[str],
    ) -> None:
        consumer_id = declaration["consumer_id"]
        inputs = {row["sequence"]: row for row in input_rows}
        output_by_location = {
            (row["channel_id"], row["partition_id"], row["sequence"]): row
            for row in output_rows
        }
        referenced: list[tuple[str, str, int]] = []
        expected_kind = declaration.get("completion_kind")
        for completion in completions:
            identity = f"{consumer_id}:{completion['input_sequence']}"
            input_row = inputs.get(completion["input_sequence"])
            if input_row is None or (
                completion["input_channel_id"],
                completion["input_partition_id"],
                completion["input_record_id"],
            ) != (
                input_row["channel_id"],
                input_row["partition_id"],
                input_row["record_id"],
            ):
                violations.append(f"completion_input_is_exact:{identity}")
            if completion["kind"] != expected_kind:
                violations.append(f"completion_kind_matches_fixture:{identity}")
            state_valid, state = cls._load_json(
                completion["state_json"],
                f"state_is_atomic:completion_json:{identity}",
                violations,
            )
            if state_valid:
                if _canonical(state) != completion["state_json"]:
                    violations.append(
                        f"state_is_atomic:completion_canonical:{identity}"
                    )
                if _digest(state) != completion["state_digest"]:
                    violations.append(
                        f"state_is_atomic:completion_digest:{identity}"
                    )
            manifest_valid, manifest = cls._load_json(
                completion["output_manifest_json"],
                f"yield_manifest_is_exact:json:{identity}",
                violations,
            )
            if manifest_valid and (
                _canonical(manifest) != completion["output_manifest_json"]
                or _digest(manifest) != completion["output_manifest_digest"]
            ):
                violations.append(f"yield_manifest_is_exact:digest:{identity}")
            if completion["kind"] == "RETURN":
                if not manifest_valid or manifest is not None or any(
                    completion[name] is not None
                    for name in (
                        "output_channel_id",
                        "output_partition_id",
                        "output_sequence",
                        "output_record_id",
                    )
                ):
                    violations.append(f"return_has_no_output:{identity}")
                continue
            if completion["kind"] != "YIELD":
                continue
            location = (
                completion["output_channel_id"],
                completion["output_partition_id"],
                completion["output_sequence"],
            )
            output = output_by_location.get(location)
            if output is None:
                violations.append(f"yield_manifest_is_exact:missing:{identity}")
                continue
            referenced.append(location)
            expected_manifest = cls._envelope_manifest(output)
            output_spec = declaration["output"]
            sequence = completion["input_sequence"]
            expected_record = f"{output_spec['record_prefix']}{sequence}"
            expected_lineage = f"{output_spec['lineage_prefix']}{sequence}"
            if (
                manifest != expected_manifest
                or completion["output_record_id"] != output["record_id"]
                or output["record_id"] != expected_record
                or output["lineage_digest"] != expected_lineage
            ):
                violations.append(f"yield_manifest_is_exact:value:{identity}")

        if len(referenced) != len(set(referenced)):
            violations.append(
                f"output_completion_cursor_are_atomic:duplicate:{consumer_id}"
            )
        declared_locations = set(output_by_location)
        if set(referenced) != declared_locations:
            violations.append(
                f"output_completion_cursor_are_atomic:coverage:{consumer_id}"
            )
        output_spec = declaration["output"]
        referenced_sequences = [location[2] for location in referenced]
        expected_sequences = list(
            range(
                output_spec["start_sequence"],
                output_spec["start_sequence"] + len(referenced),
            )
        )
        if referenced_sequences != expected_sequences:
            violations.append(f"exclusive_output_is_ordered:{consumer_id}")

    @staticmethod
    def _load_json(
        value: str, name: str, violations: list[str]
    ) -> tuple[bool, Any]:
        try:
            return True, json.loads(value)
        except (TypeError, json.JSONDecodeError):
            violations.append(name)
            return False, None

    @staticmethod
    def _envelope_manifest(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "channel": row["channel_id"],
            "partition": row["partition_id"],
            "sequence": row["sequence"],
            "record_id": row["record_id"],
            "payload": {
                "key": row["payload_key"],
                "digest": row["payload_digest"],
                "size": row["payload_size"],
                "schema_id": row["schema_id"],
            },
            "lineage": row["lineage_digest"],
            "producer_epoch": row["producer_epoch"],
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--objects", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = DurableStreamOracle.evaluate(args.db, args.objects, args.manifest)
    print(json.dumps(report.as_dict(), sort_keys=True))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
