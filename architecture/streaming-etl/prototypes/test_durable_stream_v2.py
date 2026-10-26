from pathlib import Path
from tempfile import TemporaryDirectory
import sqlite3
import unittest

from durable_stream_v2 import (
    CompletionKind,
    DurableStream,
    ObjectRef,
    PendingOutput,
    StreamProtocolError,
)


def ref(number: int, schema: str = "document/v1") -> ObjectRef:
    return ObjectRef(f"object-{number}", f"digest-{number}", number, schema)


class DurableStreamV2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "stream.db"
        self.stream = DurableStream(self.db_path)
        self.stream.claim_producer("petya", "p0", "petya-worker", 1)
        self.stream.claim_producer("anna-output", "p0", "anna-runtime", 1)
        for number in range(1, 501):
            value = ref(number)
            self.stream.register_object(value)
            self.stream.append(
                "petya", "p0", "petya-worker", 1, f"document-{number}", value,
                f"source-document-{number}",
            )
        self.stream.create_consumer("anna", "petya", "p0")
        self.stream.claim_consumer("anna", "anna-worker", 1, lease_ttl=10)

    def tearDown(self):
        self.stream.close()
        self.temp.cleanup()

    def complete_one(self, expected: int) -> None:
        token, envelope = self.stream.deliver_next("anna", "anna-worker", 1)
        self.assertEqual(envelope.sequence, expected)
        output_ref = ref(1000 + expected, "signed/v1")
        self.stream.register_object(output_ref)
        self.stream.complete(
            token,
            CompletionKind.YIELD,
            output=PendingOutput(
                "anna-output", "p0", f"signed-{expected}", output_ref,
                f"petya:{expected}",
            ),
            managed_state={"signed": expected},
        )

    def test_tanya_redelivers_200_when_anna_did_not_yield(self):
        for number in range(1, 200):
            self.complete_one(number)
        anna_token, envelope = self.stream.deliver_next("anna", "anna-worker", 1)
        self.assertEqual(envelope.sequence, 200)
        self.assertEqual(self.stream.position("anna")["completed_through"], 199)

        self.stream.advance_time(10)
        self.stream.claim_consumer("anna", "tanya-worker", 2)
        tanya_token, redelivered = self.stream.deliver_next(
            "anna", "tanya-worker", 2
        )

        self.assertEqual(redelivered.sequence, 200)
        self.assertEqual(tanya_token.attempt, anna_token.attempt + 1)
        self.assertEqual(tanya_token.owner_epoch, 2)

    def test_yield_advances_to_next_record(self):
        self.complete_one(1)
        token, envelope = self.stream.deliver_next("anna", "anna-worker", 1)
        self.assertEqual(token.sequence, 2)
        self.assertEqual(envelope.sequence, 2)

    def test_output_and_progress_roll_back_together(self):
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        output_ref = ref(1001, "signed/v1")
        self.stream.register_object(output_ref)
        with self.assertRaisesRegex(RuntimeError, "injected crash"):
            self.stream.complete(
                token,
                CompletionKind.YIELD,
                output=PendingOutput(
                    "anna-output", "p0", "signed-1", output_ref, "petya:1"
                ),
                fail_before_progress=True,
            )
        self.assertEqual(self.stream.records("anna-output", "p0"), [])
        self.assertEqual(self.stream.position("anna")["completed_through"], 0)

    def test_stale_anna_cannot_complete_after_tanya_takeover(self):
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        self.stream.advance_time(10)
        self.stream.claim_consumer("anna", "tanya-worker", 2)
        output_ref = ref(1001, "signed/v1")
        self.stream.register_object(output_ref)
        with self.assertRaisesRegex(StreamProtocolError, "active fenced owner"):
            self.stream.complete(
                token,
                CompletionKind.YIELD,
                output=PendingOutput(
                    "anna-output", "p0", "signed-1", output_ref, "petya:1"
                ),
            )

    def test_payload_must_be_durable_before_append(self):
        pending = ObjectRef("pending", "digest", 10, "document/v1")
        self.stream.register_object(pending, durable=False)
        with self.assertRaisesRegex(StreamProtocolError, "must be durable"):
            self.stream.append(
                "petya", "p0", "petya-worker", 1, "pending-record", pending,
                "pending-lineage",
            )

    def test_return_without_output_advances_and_saves_state(self):
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        self.stream.complete(
            token,
            CompletionKind.RETURN,
            managed_state={"ignored": 1},
        )
        position = self.stream.position("anna")
        self.assertEqual(position["completed_through"], 1)
        self.assertEqual(position["managed_state"], {"ignored": 1})

    def test_write_lock_closes_validation_takeover_race(self):
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        contender = DurableStream(self.db_path)
        contender.connection.execute("PRAGMA busy_timeout = 0")
        blocked = []

        def takeover_between_validation_and_write():
            with self.assertRaises(sqlite3.OperationalError):
                contender.advance_time(10)
            blocked.append(True)

        self.stream.complete(
            token,
            CompletionKind.RETURN,
            managed_state={"owner": "anna"},
            after_validation=takeover_between_validation_and_write,
        )
        self.assertEqual(blocked, [True])
        contender.advance_time(10)
        contender.claim_consumer("anna", "tanya-worker", 2)
        next_token, _ = contender.deliver_next("anna", "tanya-worker", 2)
        self.assertEqual(next_token.sequence, 2)
        contender.close()

    def test_storage_rejects_progress_forgery_and_witness_deletion(self):
        self.complete_one(1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.stream.connection.execute(
                "UPDATE consumers SET completed_through = 999 WHERE consumer_id = 'anna'"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.stream.connection.execute(
                "DELETE FROM turn_completions WHERE consumer_id = 'anna'"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.stream.connection.execute(
                "DELETE FROM records WHERE channel_id = 'anna-output'"
            )
        with self.assertRaises(sqlite3.IntegrityError):
            self.stream.connection.execute(
                "UPDATE objects SET digest = 'forged' WHERE object_key = 'object-1001'"
            )
        completion = self.stream.connection.execute(
            """SELECT input_record_id, output_record_id, output_manifest_json
               FROM turn_completions WHERE consumer_id = 'anna'"""
        ).fetchone()
        self.assertEqual(completion[0], "document-1")
        self.assertEqual(completion[1], "signed-1")
        self.assertIn('\"record_id\":\"signed-1\"', completion[2])

    def test_forged_completion_cannot_authorize_progress(self):
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        with self.assertRaises(sqlite3.IntegrityError):
            self.stream.connection.execute(
                """INSERT INTO turn_completions(
                    consumer_id, input_channel_id, input_partition_id,
                    input_sequence, input_record_id, attempt, owner_id,
                    owner_epoch, kind, output_channel_id, output_partition_id,
                    output_sequence, output_record_id, output_manifest_json,
                    output_manifest_digest, state_json, state_digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'RETURN', NULL, NULL, NULL,
                          NULL, 'null', 'forged', '{}', 'forged')""",
                (
                    token.consumer_id, token.channel_id, token.partition_id,
                    token.sequence, "ghost-input", 999, "attacker", 999,
                ),
            )
        self.assertEqual(self.stream.position("anna")["completed_through"], 0)

    def test_two_input_turns_cannot_share_one_output_envelope(self):
        shared = ref(1001, "signed/v1")
        self.stream.register_object(shared)
        first, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        output = PendingOutput(
            "anna-output", "p0", "shared-output", shared, "same-lineage"
        )
        self.stream.complete(first, CompletionKind.YIELD, output=output)
        second, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        with self.assertRaisesRegex(
            StreamProtocolError, "created by its completion transaction"
        ):
            self.stream.complete(second, CompletionKind.YIELD, output=output)
        self.assertEqual(self.stream.position("anna")["completed_through"], 1)
        self.assertEqual(len(self.stream.records("anna-output", "p0")), 1)

    def test_preexisting_output_cannot_be_adopted_by_turn(self):
        output_ref = ref(1001, "signed/v1")
        self.stream.register_object(output_ref)
        self.stream.append(
            "anna-output", "p0", "anna-runtime", 1,
            "preexisting-output", output_ref, "petya:1",
        )
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        with self.assertRaisesRegex(
            StreamProtocolError, "created by its completion transaction"
        ):
            self.stream.complete(
                token,
                CompletionKind.YIELD,
                output=PendingOutput(
                    "anna-output", "p0", "preexisting-output", output_ref,
                    "petya:1",
                ),
            )
        self.assertEqual(self.stream.position("anna")["completed_through"], 0)

    def test_exact_retry_rejects_complete_output_identity_drift(self):
        token, _ = self.stream.deliver_next("anna", "anna-worker", 1)
        output_ref = ref(1001, "signed/v1")
        self.stream.register_object(output_ref)
        output = PendingOutput(
            "anna-output", "p0", "signed-1", output_ref, "petya:1"
        )
        self.stream.complete(
            token, CompletionKind.YIELD, output=output,
            managed_state={"signed": 1},
        )
        drifted = PendingOutput(
            "totally-other", "wrong", "signed-1",
            ObjectRef(output_ref.key, output_ref.digest, 999, "evil-schema"),
            "petya:1",
        )
        with self.assertRaisesRegex(StreamProtocolError, "output drift"):
            self.stream.complete(
                token, CompletionKind.YIELD, output=drifted,
                managed_state={"signed": 1},
            )


if __name__ == "__main__":
    unittest.main()
