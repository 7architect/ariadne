from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from multisink_v1 import (
    CooperativeSinkV1,
    DurableCooperativeSink,
    ParticipantState,
    SinkProtocolError,
)


class CooperativeSinkV1ContractTests(unittest.TestCase):
    def test_transaction_coordinator_fence_rejects_only_same_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sink = DurableCooperativeSink(Path(directory) / "sink.sqlite", "sink-a")
            sink.claim_generation(1)
            sink.prepare("old", "payload", 4, 1)
            sink.fence("old", 5, 1)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "current owner"),
                sink.connection,
            ):
                sink.connection.execute(
                    """
                    UPDATE sink_transactions SET state = 'COMMITTED'
                    WHERE sink_id = 'sink-a' AND transaction_id = 'old'
                    """
                )
            with self.assertRaisesRegex(SinkProtocolError, "coordinator"):
                sink.prepare("old", "payload", 4, 1)
            sink.prepare("independent", "payload", 4, 1)
            token = sink.prepare("current", "payload", 5, 1)
            with self.assertRaisesRegex(SinkProtocolError, "coordinator"):
                sink.abort("current", token, 4, 1)

    def test_transaction_must_start_prepared(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sink = DurableCooperativeSink(Path(directory) / "sink.sqlite", "sink-a")
            sink.claim_generation(1)
            for state in ("COMMITTED", "ABORTED"):
                with (
                    self.subTest(state=state),
                    self.assertRaisesRegex(
                        sqlite3.IntegrityError, "must start prepared"
                    ),
                    sink.connection,
                ):
                    sink.connection.execute(
                        """
                        INSERT INTO sink_transactions
                        VALUES (
                            'sink-a', ?, 'forged-token', 'forged-digest',
                            ?, 4, 1
                        )
                        """,
                        (f"never-prepared-{state.lower()}", state),
                    )

    def test_prepare_token_and_recovery_are_stable_across_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sink.sqlite"
            sink = DurableCooperativeSink(path, "sink-a")
            self.assertIsInstance(sink, CooperativeSinkV1)
            sink.claim_generation(1)
            token = sink.prepare("tx-1", {"value": 7}, 4, 1)
            reopened = DurableCooperativeSink(path, "sink-a")
            self.assertEqual(reopened.prepare("tx-1", {"value": 7}, 4, 1), token)
            recovered = reopened.recover("tx-1")
            self.assertIsNotNone(recovered)
            self.assertEqual(recovered.token, token)
            self.assertEqual(recovered.state, ParticipantState.PREPARED)
            with self.assertRaisesRegex(SinkProtocolError, "payload drift"):
                reopened.prepare("tx-1", {"value": 8}, 4, 1)

    def test_participant_and_coordinator_fences_are_independent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sink.sqlite"
            old = DurableCooperativeSink(path, "sink-a")
            old.claim_generation(1)
            token = old.prepare("tx-1", "payload", 4, 1)
            replacement = DurableCooperativeSink(path, "sink-a")
            replacement.claim_generation(2)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "current owner"),
                replacement.connection,
            ):
                replacement.connection.execute(
                    """
                    UPDATE sink_transactions SET state = 'COMMITTED'
                    WHERE sink_id = 'sink-a' AND transaction_id = 'tx-1'
                    """
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "retention protocol"),
                replacement.connection,
            ):
                replacement.connection.execute(
                    "DELETE FROM sink_owners WHERE sink_id = 'sink-a'"
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "identity is immutable"),
                replacement.connection,
            ):
                replacement.connection.execute(
                    """
                    UPDATE sink_owners SET sink_id = 'moved'
                    WHERE sink_id = 'sink-a'
                    """
                )
            with self.assertRaisesRegex(SinkProtocolError, "participant"):
                old.commit("tx-1", token, 4, 1)
            with self.assertRaisesRegex(SinkProtocolError, "coordinator"):
                replacement.commit("tx-1", token, 3, 2)
            replacement.commit("tx-1", token, 5, 2)
            self.assertEqual(
                replacement.recover("tx-1").state, ParticipantState.COMMITTED
            )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "already exists"),
                replacement.connection,
            ):
                replacement.connection.execute(
                    """
                    INSERT OR REPLACE INTO sink_owners (sink_id, participant_epoch)
                    VALUES ('sink-a', 1)
                    """
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "not monotonic"),
                replacement.connection,
            ):
                replacement.connection.execute(
                    """
                    UPDATE sink_owners SET participant_epoch = 1
                    WHERE sink_id = 'sink-a'
                    """
                )

    def test_lost_abort_ack_is_recoverable_and_terminal_states_do_not_flip(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sink.sqlite"
            sink = DurableCooperativeSink(path, "sink-a")
            sink.claim_generation(1)
            token = sink.prepare("tx-abort", "payload", 4, 1)
            sink.abort("tx-abort", token, 4, 1)

            reopened = DurableCooperativeSink(path, "sink-a")
            reopened.abort("tx-abort", token, 4, 1)
            self.assertEqual(
                reopened.recover("tx-abort").state, ParticipantState.ABORTED
            )
            with self.assertRaisesRegex(SinkProtocolError, "cannot commit"):
                reopened.commit("tx-abort", token, 4, 1)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "already exists"),
                reopened.connection,
            ):
                reopened.connection.execute(
                    """
                    INSERT OR REPLACE INTO sink_transactions
                    VALUES (
                        'sink-a', 'tx-abort', 'forged-token', 'forged-digest',
                        'PREPARED', 4, 1
                    )
                    """
                )

            committed_token = reopened.prepare("tx-commit", "payload", 4, 1)
            reopened.commit("tx-commit", committed_token, 4, 1)
            with self.assertRaisesRegex(SinkProtocolError, "cannot abort"):
                reopened.abort("tx-commit", committed_token, 4, 1)
            with self.assertRaisesRegex(SinkProtocolError, "cannot prepare"):
                reopened.prepare("tx-commit", "payload", 4, 1)
            with (
                self.assertRaisesRegex(
                    sqlite3.IntegrityError, "already exists|must start prepared"
                ),
                reopened.connection,
            ):
                reopened.connection.execute(
                    """
                    INSERT OR REPLACE INTO sink_transactions
                    VALUES (
                        'sink-a', 'tx-commit', 'forged-token', 'forged-digest',
                        'ABORTED', 4, 1
                    )
                    """
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "retention protocol"),
                reopened.connection,
            ):
                reopened.connection.execute(
                    "DELETE FROM sink_transactions WHERE transaction_id = 'tx-commit'"
                )

    def test_terminal_retry_advances_both_fences(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sink.sqlite"
            sink = DurableCooperativeSink(path, "sink-a")
            sink.claim_generation(1)
            token = sink.prepare("tx-1", "payload", 4, 1)
            sink.commit("tx-1", token, 4, 1)
            sink.claim_generation(2)
            sink.commit("tx-1", token, 5, 2)
            recovered = sink.recover("tx-1")
            self.assertEqual(recovered.coordinator_epoch, 5)
            self.assertEqual(recovered.participant_epoch, 2)
            with self.assertRaisesRegex(SinkProtocolError, "coordinator"):
                sink.commit("tx-1", token, 4, 2)


if __name__ == "__main__":
    unittest.main()
