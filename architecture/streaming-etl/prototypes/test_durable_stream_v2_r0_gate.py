from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from durable_object_store import DurableObjectStore
from durable_stream_v2 import (
    CompletionKind,
    DurableStream,
    ObjectRef,
    PendingOutput,
    StreamProtocolError,
)
from durable_stream_v2_crash_worker import EXIT_CODE

SEQUENCE_200_MATRIX = {
    "observe_yield": ("observe:yield_before_completion",),
    "observe_return": ("observe:return_before_completion",),
    "complete_yield": (
        "completion:after_output_insert_before_marker",
        "completion:after_marker_before_progress",
        "progress:after_cursor_update_before_commit",
        "progress:after_commit_before_ack",
        "progress:after_ack",
    ),
    "complete_return": (
        "completion:after_marker_before_progress",
        "progress:after_cursor_update_before_commit",
        "progress:after_commit_before_ack",
        "progress:after_ack",
    ),
}

COMMITTED_POINTS = {
    "progress:after_commit_before_ack",
    "progress:after_ack",
}


class DurableStreamV2R0GateTests(unittest.TestCase):
    def test_sequence_200_crashes_have_one_resume_position(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bases = {}
            for kind in ("yield", "return"):
                base = root / f"base-{kind}"
                self.seed_through_199(base, kind)
                bases[kind] = base

            for scenario, points in SEQUENCE_200_MATRIX.items():
                kind = "yield" if scenario.endswith("yield") else "return"
                for point in points:
                    with self.subTest(scenario=scenario, point=point):
                        fixture = root / f"case-{scenario}-{safe_name(point)}"
                        self.clone_fixture(bases[kind], fixture)
                        if scenario.startswith("observe_"):
                            stream = DurableStream(fixture / "stream.sqlite")
                            token, envelope = stream.deliver_next(
                                "anna", "anna-worker", 1
                            )
                            self.assertEqual(token.sequence, 200)
                            self.assertEqual(envelope.record_id, "document-200")
                            stream.close()
                        self.run_crash(fixture, scenario, point)

                        committed = point in COMMITTED_POINTS
                        report = self.run_oracle(fixture, expect_pass=True)
                        expected_resume = 201 if committed else 200
                        self.assertEqual(
                            report["resume_by_consumer"]["anna"], expected_resume
                        )
                        observation = report["observations"]["anna"]
                        self.assertEqual(
                            observation["completed_through"],
                            200 if committed else 199,
                        )
                        expected_outputs = (
                            (200 if committed else 199) if kind == "yield" else 0
                        )
                        self.assertEqual(observation["output_count"], expected_outputs)

                        recovery = self.run_recovery(fixture, kind, expected_resume)
                        self.assertEqual(recovery["received"], expected_resume)
                        after = self.run_oracle(fixture, expect_pass=True)
                        self.assertEqual(after["resume_by_consumer"]["anna"], 201)
                        self.assertEqual(
                            after["observations"]["anna"]["output_count"],
                            200 if kind == "yield" else 0,
                        )

    def test_orphans_are_reported_but_never_visible(self) -> None:
        cases = {
            "upload:before_publish": ([], [".uploaded-object.uploading"]),
            "upload:after_blob_fsync_before_witness": (["uploaded-object"], []),
            "upload:after_witness_insert_before_commit": (["uploaded-object"], []),
        }
        for point, (expected_orphans, expected_staging) in cases.items():
            with self.subTest(point=point), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.seed_empty(root)
                self.run_crash(root, "upload", point)
                report = self.run_oracle(root, expect_pass=True)
                self.assertEqual(report["orphan_blobs"], expected_orphans)
                self.assertEqual(report["staging_uploads"], expected_staging)

                stream = DurableStream(root / "stream.sqlite")
                ref = ObjectRef.for_payload("uploaded-object", b"payload", "raw/v1")
                stream.claim_producer("check", "p0", "worker", 1)
                with self.assertRaisesRegex(StreamProtocolError, "must be durable"):
                    stream.append(
                        "check",
                        "p0",
                        "worker",
                        1,
                        "invisible",
                        ref,
                        "orphan",
                    )
                DurableObjectStore(root / "objects").upload(stream, ref, b"payload")
                with self.assertRaisesRegex(StreamProtocolError, "bytes drift"):
                    drifted = ObjectRef.for_payload(ref.key, b"divergent", "raw/v1")
                    DurableObjectStore(root / "objects").upload(
                        stream, drifted, b"divergent"
                    )
                stream.close()
                repaired = self.run_oracle(root, expect_pass=True)
                self.assertEqual(repaired["orphan_blobs"], [])
                self.assertEqual(repaired["staging_uploads"], [])

    def test_independent_oracle_rejects_unsafe_controls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            valid = root / "valid"
            self.seed_through_200(valid, "yield")
            controls = {
                "missing_blob": "blob_witness_integrity:missing_blob",
                "drifted_blob": "blob_witness_integrity:digest",
                "non_durable_record": "visible_payload_is_durable",
                "partition_gap": "partition_sequence_is_contiguous",
                "cursor_without_completion": "completion_coverage_is_exact",
                "completion_above_cursor": "completion_coverage_is_exact",
                "manifest_wrong_value": "yield_manifest_is_exact:value",
                "manifest_wrong_digest": "yield_manifest_is_exact:digest",
                "state_mismatch": "state_is_atomic:cursor_state",
                "partial_output": "output_completion_cursor_are_atomic:coverage",
                "malformed_json": "state_is_atomic:completion_json",
                "bad_in_flight": "consumer_position_is_valid",
                "manifest_omits_consumer": "fixture_consumer_set_is_exact:missing",
                "manifest_unknown_consumer": "fixture_consumer_set_is_exact:unknown",
                "manifest_duplicate_consumer": "fixture_consumer_set_is_exact:duplicate",
                "fixture_manifest_malformed": "fixture_manifest_invalid",
                "output_order_swap": "exclusive_output_is_ordered",
            }
            for name, expected in controls.items():
                with self.subTest(control=name):
                    fixture = root / name
                    if name in {
                        "cursor_without_completion",
                        "partial_output",
                        "bad_in_flight",
                    }:
                        self.seed_through_199(fixture, "yield")
                    else:
                        self.clone_fixture(valid, fixture)
                    self.corrupt(fixture, name)
                    report = self.run_oracle(fixture, expect_pass=False)
                    self.assertTrue(
                        any(
                            violation.startswith(expected)
                            for violation in report["violations"]
                        ),
                        report,
                    )

    @staticmethod
    def seed_empty(root: Path) -> None:
        root.mkdir(parents=True, exist_ok=True)
        (root / "objects").mkdir(exist_ok=True)
        stream = DurableStream(root / "stream.sqlite")
        stream.close()
        write_manifest(root / "manifest.json", "yield", consumers=False)

    def seed_through_199(self, root: Path, kind: str) -> None:
        self.seed(root, kind, completed=199)

    def seed_through_200(self, root: Path, kind: str) -> None:
        self.seed(root, kind, completed=200)

    def seed(self, root: Path, kind: str, *, completed: int) -> None:
        root.mkdir(parents=True)
        objects = root / "objects"
        stream = DurableStream(root / "stream.sqlite")
        store = DurableObjectStore(objects)
        input_ref = ObjectRef.for_payload("input-shared", b"input", "document/v1")
        output_ref = ObjectRef.for_payload("output-1", b"signed-1", "signed/v1")
        store.upload(stream, input_ref, b"input")
        store.upload(stream, output_ref, b"signed-1")
        stream.claim_producer("petya", "p0", "petya-worker", 1)
        stream.claim_producer("anna-output", "p0", "anna-runtime", 1)
        for sequence in range(1, 202):
            stream.append(
                "petya",
                "p0",
                "petya-worker",
                1,
                f"document-{sequence}",
                input_ref,
                f"source:{sequence}",
            )
        stream.create_consumer("anna", "petya", "p0")
        stream.claim_consumer("anna", "anna-worker", 1, lease_ttl=10)
        completion_kind = CompletionKind(kind.upper())
        for sequence in range(1, completed + 1):
            token, envelope = stream.deliver_next("anna", "anna-worker", 1)
            self.assertEqual(envelope.sequence, sequence)
            output = None
            if completion_kind is CompletionKind.YIELD:
                output = PendingOutput(
                    "anna-output",
                    "p0",
                    f"signed-{sequence}",
                    output_ref,
                    f"petya:{sequence}",
                )
            stream.complete(
                token,
                completion_kind,
                output=output,
                managed_state={"handled": sequence},
            )
        stream.close()
        write_manifest(root / "manifest.json", kind)

    @staticmethod
    def clone_fixture(source: Path, destination: Path) -> None:
        shutil.copytree(source, destination)

    def run_crash(self, root: Path, scenario: str, point: str) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "durable_stream_v2_crash_worker.py",
                "--db",
                str(root / "stream.sqlite"),
                "--objects",
                str(root / "objects"),
                "--effect",
                str(root / "client-effect"),
                "--scenario",
                scenario,
                "--point",
                point,
                "--sequence",
                "200",
            ],
            cwd=Path(__file__).parent,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            EXIT_CODE,
            msg=f"stdout={result.stdout!r}\nstderr={result.stderr!r}",
        )

    def run_recovery(self, root: Path, kind: str, resume: int) -> dict:
        result = subprocess.run(
            [
                sys.executable,
                "durable_stream_v2_recovery_worker.py",
                "--db",
                str(root / "stream.sqlite"),
                "--resume",
                str(resume),
                "--kind",
                kind,
            ],
            cwd=Path(__file__).parent,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout={result.stdout!r}\nstderr={result.stderr!r}",
        )
        return json.loads(result.stdout)

    def run_oracle(self, root: Path, *, expect_pass: bool) -> dict:
        result = subprocess.run(
            [
                sys.executable,
                "durable_stream_v2_oracle.py",
                "--db",
                str(root / "stream.sqlite"),
                "--objects",
                str(root / "objects"),
                "--manifest",
                str(root / "manifest.json"),
            ],
            cwd=Path(__file__).parent,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            result.returncode,
            0 if expect_pass else 2,
            msg=f"stdout={result.stdout!r}\nstderr={result.stderr!r}",
        )
        report = json.loads(result.stdout)
        self.assertEqual(report["passed"], expect_pass)
        return report

    def corrupt(self, root: Path, name: str) -> None:
        database = root / "stream.sqlite"
        objects = root / "objects"
        if name == "missing_blob":
            (objects / "output-1").unlink()
            return
        if name == "drifted_blob":
            (objects / "output-1").write_bytes(b"corrupt")
            return
        if name in {
            "manifest_omits_consumer",
            "manifest_unknown_consumer",
            "manifest_duplicate_consumer",
            "fixture_manifest_malformed",
        }:
            manifest_path = root / "manifest.json"
            if name == "fixture_manifest_malformed":
                manifest_path.write_text("{")
                return
            manifest = json.loads(manifest_path.read_text())
            if name == "manifest_omits_consumer":
                manifest["consumers"] = []
            elif name == "manifest_unknown_consumer":
                unknown = dict(manifest["consumers"][0])
                unknown["consumer_id"] = "ghost"
                manifest["consumers"].append(unknown)
            else:
                manifest["consumers"].append(dict(manifest["consumers"][0]))
            manifest_path.write_text(json.dumps(manifest, sort_keys=True))
            return
        stream = DurableStream(database)
        if name == "partial_output":
            output_ref = ObjectRef.for_payload("output-1", b"signed-1", "signed/v1")
            stream.append(
                "anna-output",
                "p0",
                "anna-runtime",
                1,
                "signed-200",
                output_ref,
                "petya:200",
            )
            stream.close()
            return
        stream.close()

        connection = sqlite3.connect(database)
        connection.execute("PRAGMA foreign_keys = OFF")
        if name == "non_durable_record":
            connection.execute("DROP TRIGGER objects_immutable")
            connection.execute(
                "UPDATE objects SET durable = 0 WHERE object_key = 'input-shared'"
            )
        elif name == "partition_gap":
            connection.execute("DROP TRIGGER records_delete_requires_retention")
            connection.execute(
                "DELETE FROM records WHERE channel_id = 'petya' AND sequence = 100"
            )
        elif name == "cursor_without_completion":
            connection.execute("DROP TRIGGER consumer_progress_guard")
            connection.execute(
                """UPDATE consumers SET completed_through = 200,
                   state_json = '{"handled":200}' WHERE consumer_id = 'anna'"""
            )
        elif name == "completion_above_cursor":
            connection.execute("DROP TRIGGER consumer_progress_guard")
            connection.execute(
                """UPDATE consumers SET completed_through = 199,
                   state_json = '{"handled":199}' WHERE consumer_id = 'anna'"""
            )
        elif name in {"manifest_wrong_value", "manifest_wrong_digest"}:
            connection.execute("DROP TRIGGER completions_immutable")
            row = connection.execute(
                """SELECT output_manifest_json FROM turn_completions
                   WHERE consumer_id = 'anna' AND input_sequence = 200"""
            ).fetchone()
            manifest = json.loads(row[0])
            if name == "manifest_wrong_value":
                manifest["lineage"] = "wrong"
                encoded = canonical(manifest)
                digest = digest_value(manifest)
            else:
                encoded = canonical(manifest)
                digest = "wrong-digest"
            connection.execute(
                """UPDATE turn_completions
                   SET output_manifest_json = ?, output_manifest_digest = ?
                   WHERE consumer_id = 'anna' AND input_sequence = 200""",
                (encoded, digest),
            )
        elif name == "state_mismatch":
            connection.execute("DROP TRIGGER consumer_progress_guard")
            connection.execute(
                "UPDATE consumers SET state_json = '{}' WHERE consumer_id = 'anna'"
            )
        elif name == "malformed_json":
            connection.execute("DROP TRIGGER completions_immutable")
            connection.execute(
                """UPDATE turn_completions
                   SET state_json = '{', output_manifest_json = '['
                   WHERE consumer_id = 'anna' AND input_sequence = 200"""
            )
        elif name == "bad_in_flight":
            connection.execute(
                """UPDATE consumers SET in_flight_sequence = 201,
                   in_flight_attempt = 200 WHERE consumer_id = 'anna'"""
            )
        elif name == "output_order_swap":
            self.swap_first_two_outputs(connection)
        else:
            raise AssertionError(f"unknown corruption: {name}")
        connection.commit()
        connection.close()

    @staticmethod
    def swap_first_two_outputs(connection: sqlite3.Connection) -> None:
        connection.execute("DROP TRIGGER records_immutable")
        connection.execute("DROP TRIGGER completions_immutable")
        connection.execute(
            """UPDATE records SET sequence = 999
               WHERE channel_id = 'anna-output' AND sequence = 1"""
        )
        connection.execute(
            """UPDATE records SET sequence = 1
               WHERE channel_id = 'anna-output' AND sequence = 2"""
        )
        connection.execute(
            """UPDATE records SET sequence = 2
               WHERE channel_id = 'anna-output' AND sequence = 999"""
        )
        connection.execute(
            """UPDATE turn_completions SET output_sequence = 999
               WHERE consumer_id = 'anna' AND input_sequence = 1"""
        )
        connection.execute(
            """UPDATE turn_completions SET output_sequence = 1
               WHERE consumer_id = 'anna' AND input_sequence = 2"""
        )
        connection.execute(
            """UPDATE turn_completions SET output_sequence = 2
               WHERE consumer_id = 'anna' AND input_sequence = 1"""
        )
        for input_sequence in (1, 2):
            output = connection.execute(
                """SELECT r.* FROM records r
                   JOIN turn_completions t
                     ON t.output_channel_id = r.channel_id
                    AND t.output_partition_id = r.partition_id
                    AND t.output_sequence = r.sequence
                   WHERE t.consumer_id = 'anna' AND t.input_sequence = ?""",
                (input_sequence,),
            ).fetchone()
            columns = [
                item[1] for item in connection.execute("PRAGMA table_info(records)")
            ]
            row = dict(zip(columns, output, strict=True))
            manifest = {
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
            connection.execute(
                """UPDATE turn_completions
                   SET output_manifest_json = ?, output_manifest_digest = ?
                   WHERE consumer_id = 'anna' AND input_sequence = ?""",
                (canonical(manifest), digest_value(manifest), input_sequence),
            )


def write_manifest(path: Path, kind: str, *, consumers: bool = True) -> None:
    declarations = []
    if consumers:
        declarations.append(
            {
                "consumer_id": "anna",
                "input": {"channel": "petya", "partition": "p0"},
                "completion_kind": kind.upper(),
                "output": {
                    "channel": "anna-output",
                    "partition": "p0",
                    "record_prefix": "signed-",
                    "lineage_prefix": "petya:",
                    "exclusive": True,
                    "start_sequence": 1,
                },
            }
        )
    path.write_text(json.dumps({"consumers": declarations}, sort_keys=True))


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest_value(value: object) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def safe_name(value: str) -> str:
    return value.replace(":", "-")


if __name__ == "__main__":
    unittest.main()
