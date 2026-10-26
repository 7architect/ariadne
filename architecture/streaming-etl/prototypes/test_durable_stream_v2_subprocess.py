from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from durable_object_store import DurableObjectStore
from durable_stream_v2 import (
    CompletionKind,
    DeliveryToken,
    DurableStream,
    ObjectRef,
    PendingOutput,
    StreamProtocolError,
)
from durable_stream_v2_crash_worker import EXIT_CODE


CRASH_MATRIX = {
    "upload": (
        "upload:before_publish",
        "upload:after_blob_fsync_before_witness",
        "upload:after_witness_insert_before_commit",
        "upload:after_commit_before_ack",
        "upload:after_ack",
    ),
    "append": (
        "append:before_insert",
        "append:after_insert_before_commit",
        "append:after_commit_before_ack",
    ),
    "delivery": (
        "delivery:before_claim",
        "delivery:after_claim_before_commit",
        "delivery:after_commit_before_ack",
    ),
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


class DurableStreamV2SubprocessCrashTests(unittest.TestCase):
    def test_named_crash_matrix_reopens_and_converges_without_sleeps(self) -> None:
        for scenario, points in CRASH_MATRIX.items():
            for point in points:
                with self.subTest(scenario=scenario, point=point):
                    with tempfile.TemporaryDirectory() as directory:
                        root = Path(directory)
                        db = root / "stream.sqlite"
                        objects = root / "objects"
                        effect = root / "client-effect"
                        self.seed(db, objects, scenario)
                        self.run_crashing_child(
                            db, objects, effect, scenario, point
                        )
                        stream = DurableStream(db)
                        try:
                            self.assert_storage_integrity(stream)
                            self.assert_phase_and_retry(
                                stream, objects, effect, scenario, point
                            )
                            self.assert_storage_integrity(stream)
                        finally:
                            stream.close()

    def test_no_fault_control_completes_normally(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / "stream.sqlite"
            objects = root / "objects"
            self.seed(db, objects, "complete_yield")
            stream = DurableStream(db)
            token, _ = stream.deliver_next("anna", "anna-worker", 1)
            stream.complete(
                token,
                CompletionKind.YIELD,
                output=self.pending_output(),
                managed_state={"handled": 1},
            )
            self.assert_completed(stream, yielded=True)
            self.assert_storage_integrity(stream)
            stream.close()

    def test_clean_exception_control_unwinds_in_the_live_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / "stream.sqlite"
            objects = root / "objects"
            self.seed(db, objects, "complete_yield")
            stream = DurableStream(db)
            token, _ = stream.deliver_next("anna", "anna-worker", 1)

            def raise_normally(point: str) -> None:
                if point == "completion:after_marker_before_progress":
                    raise RuntimeError("clean exception")

            with self.assertRaisesRegex(RuntimeError, "clean exception"):
                stream.complete(
                    token,
                    CompletionKind.YIELD,
                    output=self.pending_output(),
                    managed_state={"handled": 1},
                    fault=raise_normally,
                )
            self.assertEqual(stream.position("anna")["completed_through"], 0)
            self.assertFalse(stream.connection.in_transaction)
            stream.close()

    @staticmethod
    def seed(path: Path, object_root: Path, scenario: str) -> None:
        stream = DurableStream(path)
        store = DurableObjectStore(object_root)
        if scenario == "upload":
            stream.close()
            return
        stream.claim_producer("petya", "p0", "petya-worker", 1)
        store.upload(stream, input_ref(1), b"input-1")
        if scenario == "append":
            stream.close()
            return
        stream.append(
            "petya",
            "p0",
            "petya-worker",
            1,
            "document-1",
            input_ref(1),
            "source:1",
        )
        store.upload(stream, input_ref(2), b"input-2")
        stream.append(
            "petya",
            "p0",
            "petya-worker",
            1,
            "document-2",
            input_ref(2),
            "source:2",
        )
        stream.create_consumer("anna", "petya", "p0")
        stream.claim_consumer("anna", "anna-worker", 1, lease_ttl=10)
        if scenario == "delivery":
            stream.close()
            return
        stream.deliver_next("anna", "anna-worker", 1)
        stream.claim_producer("anna-output", "p0", "anna-runtime", 1)
        store.upload(stream, output_ref(), b"signed-1")
        stream.close()

    def run_crashing_child(
        self,
        db: Path,
        objects: Path,
        effect: Path,
        scenario: str,
        point: str,
    ) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "durable_stream_v2_crash_worker.py",
                "--db",
                str(db),
                "--objects",
                str(objects),
                "--effect",
                str(effect),
                "--scenario",
                scenario,
                "--point",
                point,
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

    def assert_phase_and_retry(
        self,
        stream: DurableStream,
        object_root: Path,
        effect: Path,
        scenario: str,
        point: str,
    ) -> None:
        if scenario == "upload":
            self.assert_upload_phase_and_retry(stream, object_root, point)
        elif scenario == "append":
            self.assert_append_phase_and_retry(stream, point)
        elif scenario == "delivery":
            self.assert_delivery_phase_and_retry(stream, point)
        elif scenario.startswith("observe_"):
            self.assert_observation_phase_and_retry(
                stream, effect, yielded=scenario.endswith("yield")
            )
        else:
            self.assert_completion_phase_and_retry(
                stream, point, yielded=scenario.endswith("yield")
            )

    def assert_upload_phase_and_retry(
        self, stream: DurableStream, object_root: Path, point: str
    ) -> None:
        ref = ObjectRef.for_payload("uploaded-object", b"payload", "raw/v1")
        published = (object_root / ref.key).exists()
        witnessed = stream.connection.execute(
            "SELECT COUNT(*) FROM objects WHERE object_key = ?", (ref.key,)
        ).fetchone()[0]
        if point == "upload:before_publish":
            self.assertFalse(published)
            self.assertEqual(witnessed, 0)
        elif point in {
            "upload:after_blob_fsync_before_witness",
            "upload:after_witness_insert_before_commit",
        }:
            self.assertTrue(published)
            self.assertEqual(witnessed, 0)
            stream.claim_producer("upload-check", "p0", "worker", 1)
            with self.assertRaisesRegex(StreamProtocolError, "must be durable"):
                stream.append(
                    "upload-check",
                    "p0",
                    "worker",
                    1,
                    "not-visible",
                    ref,
                    "upload-check",
                )
        else:
            self.assertTrue(published)
            self.assertEqual(witnessed, 1)

        DurableObjectStore(object_root).upload(stream, ref, b"payload")
        self.assertTrue((object_root / ref.key).exists())
        self.assertEqual(
            stream.connection.execute(
                "SELECT COUNT(*) FROM objects WHERE object_key = ?", (ref.key,)
            ).fetchone()[0],
            1,
        )
        with self.assertRaisesRegex(StreamProtocolError, "bytes drift"):
            drifted = ObjectRef.for_payload(ref.key, b"different", "raw/v1")
            DurableObjectStore(object_root).upload(stream, drifted, b"different")

    def assert_append_phase_and_retry(
        self, stream: DurableStream, point: str
    ) -> None:
        committed = point == "append:after_commit_before_ack"
        self.assertEqual(len(stream.records("petya", "p0")), int(committed))
        envelope = stream.append(
            "petya",
            "p0",
            "petya-worker",
            1,
            "document-1",
            input_ref(1),
            "source:1",
        )
        self.assertEqual(envelope.sequence, 1)
        self.assertEqual(len(stream.records("petya", "p0")), 1)
        with self.assertRaisesRegex(StreamProtocolError, "identity drift"):
            stream.append(
                "petya",
                "p0",
                "petya-worker",
                1,
                "document-1",
                input_ref(1),
                "divergent-lineage",
            )

    def assert_delivery_phase_and_retry(
        self, stream: DurableStream, point: str
    ) -> None:
        committed = point == "delivery:after_commit_before_ack"
        position = stream.position("anna")
        self.assertEqual(position["completed_through"], 0)
        self.assertEqual(position["in_flight"], 1 if committed else None)
        self.assertEqual(position["attempt"], 1 if committed else 0)
        stream.advance_time(10)
        stream.claim_consumer("anna", "tanya-worker", 2)
        token, envelope = stream.deliver_next("anna", "tanya-worker", 2)
        self.assertEqual(envelope.sequence, 1)
        self.assertEqual(token.attempt, 2 if committed else 1)

    def assert_observation_phase_and_retry(
        self, stream: DurableStream, effect: Path, *, yielded: bool
    ) -> None:
        self.assertEqual(effect.read_text(), "1")
        self.assert_incomplete(stream)
        token, _ = self.take_over_current(stream)
        effect.write_text("2")
        if yielded:
            stream.complete(
                token,
                CompletionKind.YIELD,
                output=self.pending_output(),
                managed_state={"handled": 1},
            )
        else:
            stream.complete(
                token,
                CompletionKind.RETURN,
                managed_state={"handled": 1},
            )
        self.assertEqual(effect.read_text(), "2")
        self.assert_completed(stream, yielded=yielded)

    def assert_completion_phase_and_retry(
        self, stream: DurableStream, point: str, *, yielded: bool
    ) -> None:
        committed = point in {
            "progress:after_commit_before_ack",
            "progress:after_ack",
        }
        if not committed:
            self.assert_incomplete(stream)
            token, _ = self.take_over_current(stream)
            if yielded:
                stream.complete(
                    token,
                    CompletionKind.YIELD,
                    output=self.pending_output(),
                    managed_state={"handled": 1},
                )
            else:
                stream.complete(
                    token,
                    CompletionKind.RETURN,
                    managed_state={"handled": 1},
                )
        else:
            self.assert_completed(stream, yielded=yielded)
            token = DeliveryToken(
                "anna", "petya", "p0", 1, 1, "anna-worker", 1
            )
            if yielded:
                stream.complete(
                    token,
                    CompletionKind.YIELD,
                    output=self.pending_output(),
                    managed_state={"handled": 1},
                )
            else:
                stream.complete(
                    token,
                    CompletionKind.RETURN,
                    managed_state={"handled": 1},
                )
        self.assert_completed(stream, yielded=yielded)
        position = stream.position("anna")
        _, next_envelope = stream.deliver_next(
            "anna", position["owner_id"], position["owner_epoch"]
        )
        self.assertEqual(next_envelope.sequence, 2)

    @staticmethod
    def take_over_current(stream: DurableStream):
        stream.advance_time(10)
        stream.claim_consumer("anna", "tanya-worker", 2)
        token, envelope = stream.deliver_next("anna", "tanya-worker", 2)
        if envelope.sequence != 1:
            raise AssertionError("replacement did not receive current input")
        return token, envelope

    def assert_incomplete(self, stream: DurableStream) -> None:
        position = stream.position("anna")
        self.assertEqual(position["completed_through"], 0)
        self.assertEqual(position["in_flight"], 1)
        self.assertEqual(position["managed_state"], {})
        self.assertEqual(len(stream.records("anna-output", "p0")), 0)
        self.assertEqual(
            stream.connection.execute(
                "SELECT COUNT(*) FROM turn_completions"
            ).fetchone()[0],
            0,
        )

    def assert_completed(self, stream: DurableStream, *, yielded: bool) -> None:
        position = stream.position("anna")
        self.assertEqual(position["completed_through"], 1)
        self.assertIsNone(position["in_flight"])
        self.assertEqual(position["managed_state"], {"handled": 1})
        self.assertEqual(
            len(stream.records("anna-output", "p0")), int(yielded)
        )
        completion = stream.connection.execute(
            "SELECT kind, input_sequence FROM turn_completions"
        ).fetchone()
        self.assertEqual(completion["input_sequence"], 1)
        self.assertEqual(
            completion["kind"],
            CompletionKind.YIELD.value if yielded else CompletionKind.RETURN.value,
        )

    def assert_storage_integrity(self, stream: DurableStream) -> None:
        self.assertEqual(
            stream.connection.execute("PRAGMA integrity_check").fetchone()[0],
            "ok",
        )
        self.assertEqual(
            stream.connection.execute("PRAGMA foreign_key_check").fetchall(), []
        )

    @staticmethod
    def pending_output() -> PendingOutput:
        return PendingOutput(
            "anna-output", "p0", "signed-1", output_ref(), "petya:1"
        )


def input_ref(sequence: int) -> ObjectRef:
    payload = f"input-{sequence}".encode()
    return ObjectRef.for_payload(f"input-{sequence}", payload, "document/v1")


def output_ref() -> ObjectRef:
    return ObjectRef.for_payload("output-1", b"signed-1", "signed/v1")


if __name__ == "__main__":
    unittest.main()
