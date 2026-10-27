from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from durable_stream_barrier_v2 import (
    ComposedDurableStream,
    ComposedProtocolError,
    EntryKind,
)

HERE = Path(__file__).resolve().parent
ORACLE = HERE / "durable_stream_barrier_oracle.py"


class DurableStreamBarrierV2Tests(unittest.TestCase):
    def test_single_input_barrier_keeps_data_cursor_and_cut_independent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "single.sqlite"
            store = self.store(path, ("left",))
            self.append(store, "left", 1)
            self.append(store, "left", 2)
            intent = store.start_checkpoint(1, ("left",), "coord", 1)
            barrier = store.inject_barrier(1, "left", "coord", 1, intent)
            self.assertEqual(barrier.log_sequence, 3)
            self.assertEqual(barrier.cut_data_sequence, 2)
            self.append(store, "left", 3)

            self.assertEqual(store.process_next("left").kind, EntryKind.DATA)
            self.assertEqual(store.process_next("left").kind, EntryKind.DATA)
            self.assertEqual(store.process_next("left").kind, EntryKind.BARRIER)
            self.assertEqual(store.status(1), "ALIGNED")
            self.assertEqual(
                store.progress()["left"],
                {
                    "scanned_log_through": 3,
                    "completed_data_through": 2,
                    "blocked_checkpoint": None,
                },
            )
            self.assertEqual(store.snapshot(1)["cuts"]["left"]["data"], 2)

            # Local alignment releases DATA immediately; global completion is later.
            self.assertEqual(store.process_next("left").data_sequence, 3)
            self.assertEqual(store.status(1), "ALIGNED")
            self.assertEqual(store.progress()["left"]["completed_data_through"], 3)
            self.assertEqual(store.snapshot(1)["cuts"]["left"]["data"], 2)
            self.assertEqual(
                [event["kind"] for event in store.outputs()],
                ["DATA", "DATA", "BARRIER", "DATA"],
            )
            store.complete_checkpoint(1)
            store.close()

            recovered = ComposedDurableStream(path)
            recovered.begin_recovery()
            before = recovered.outputs()
            recovery = recovered.recover()
            self.assertEqual(recovery["phase"], "READY")
            self.assertEqual(recovery["checkpoint"], 1)
            self.assertEqual(recovery["replay"], [[3, "left", 3]])
            self.assertEqual(recovered.outputs(), before)
            recovered.close()

            report = self.oracle(path, True)
            self.assertEqual(report["plan"]["cuts"]["left"]["data"], 2)
            self.assertEqual(report["plan"]["replay"], [[3, "left", 3]])
            self.assertEqual(report["plan"]["next"], {"left": 4})

    def test_early_input_buffers_only_post_barrier_tail_and_replays_exact_order(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "join.sqlite"
            store = self.seed_aligned_join(path, complete=True)
            snapshot = store.snapshot(1)
            self.assertEqual(
                snapshot["cuts"],
                {
                    "left": {"barrier_log": 3, "data": 2},
                    "right": {"barrier_log": 3, "data": 2},
                },
            )
            self.assertEqual(snapshot["mailbox_highwater"], 4)
            self.assertEqual(store.progress()["left"]["completed_data_through"], 3)
            self.assertEqual(store.progress()["right"]["completed_data_through"], 3)
            outputs = store.outputs()
            barrier_index = next(
                index for index, row in enumerate(outputs) if row["kind"] == "BARRIER"
            )
            self.assertEqual(barrier_index, 4)
            store.close()

            recovered = ComposedDurableStream(path)
            recovered.begin_recovery()
            before_recovery_outputs = recovered.outputs()
            recovery = recovered.recover()
            self.assertEqual(recovery["checkpoint"], 1)
            self.assertEqual(recovery["phase"], "READY")
            self.assertEqual(
                recovery["replay"],
                [[5, "left", 3], [6, "right", 3]],
            )
            self.assertEqual(recovered.outputs(), before_recovery_outputs)

            report = self.oracle(path, True)
            self.assertEqual(
                report["plan"]["replay"],
                [[5, "left", 3], [6, "right", 3]],
            )
            self.assertEqual(report["plan"]["next"], {"left": 4, "right": 4})

            self.append(recovered, "left", 4)
            self.assertEqual(recovered.process_next("left").data_sequence, 4)
            self.assertEqual(recovered.progress()["left"]["completed_data_through"], 4)
            self.assertEqual(len(recovered.outputs()), len(before_recovery_outputs) + 1)
            self.assertEqual(recovered.recover()["phase"], "READY")
            recovered.close()
            continued = self.oracle(path, True)
            self.assertEqual(continued["plan"]["next"], {"left": 5, "right": 4})

    def test_incomplete_checkpoint_is_not_selected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "incomplete.sqlite"
            store = self.store(path, ("left",))
            self.append(store, "left", 1)
            intent = store.start_checkpoint(1, ("left",), "coord", 1)
            store.inject_barrier(1, "left", "coord", 1, intent)
            store.process_next("left")
            store.process_next("left")
            self.assertEqual(store.status(1), "ALIGNED")
            store.close()
            recovering = ComposedDurableStream(path)
            recovering.begin_recovery()
            with self.assertRaisesRegex(
                ComposedProtocolError, "no compatible completed checkpoint"
            ):
                recovering.recover()
            recovering.close()
            report = self.oracle(path, True)
            self.assertEqual(
                report["plan"],
                {"checkpoint": None, "reason": "no_completed_checkpoint"},
            )

    def test_document_200_boundary_is_unchanged_by_barrier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "document-200.sqlite"
            store = self.store(path, ("left",))
            for sequence in range(1, 200):
                self.append(store, "left", sequence)
            intent = store.start_checkpoint(1, ("left",), "coord", 1)
            store.inject_barrier(1, "left", "coord", 1, intent)
            self.append(store, "left", 200)
            for _ in range(199):
                store.process_next("left")
            self.assertEqual(store.progress()["left"]["completed_data_through"], 199)
            self.assertEqual(store.process_next("left").kind, EntryKind.BARRIER)
            self.assertEqual(store.progress()["left"]["completed_data_through"], 199)
            self.assertEqual(store.snapshot(1)["cuts"]["left"]["data"], 199)
            # A crash here redelivers DATA 200; after its terminal turn, next is 201.
            self.assertEqual(store.process_next("left").data_sequence, 200)
            self.assertEqual(store.progress()["left"]["completed_data_through"], 200)
            self.assertEqual(store.snapshot(1)["cuts"]["left"]["data"], 199)
            store.complete_checkpoint(1)
            store.close()
            report = self.oracle(path, True)
            self.assertEqual(report["plan"]["next"], {"left": 201})

    def test_authority_and_identity_controls_reject_invalid_barriers(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "authority.sqlite"
            store = self.store(path, ("left", "right"))
            with self.assertRaises(sqlite3.IntegrityError):
                store.inject_barrier(1, "left", "coord", 1, "missing")
            intent = store.start_checkpoint(1, ("left", "right"), "coord", 1)
            with self.assertRaises(sqlite3.IntegrityError):
                store.inject_barrier(1, "left", "coord", 1, "wrong")
            with self.assertRaisesRegex(ComposedProtocolError, "stale coordinator"):
                store.inject_barrier(1, "left", "coord", 2, intent)
            with self.assertRaisesRegex(ComposedProtocolError, "active"):
                store.start_checkpoint(2, ("left", "right"), "coord", 1)
            store.inject_barrier(1, "left", "coord", 1, intent)
            store.begin_recovery()
            with self.assertRaises(sqlite3.IntegrityError):
                store.inject_barrier(1, "right", "coord", 1, intent)
            with self.assertRaisesRegex(ComposedProtocolError, "recovery fence"):
                store.start_checkpoint(2, ("left", "right"), "coord", 1)
            with self.assertRaisesRegex(ComposedProtocolError, "processing"):
                store.process_next("left")
            with self.assertRaisesRegex(ComposedProtocolError, "stale producer"):
                store.append_data("left", "petya", 2, "late", "ref:late")
            self.append(store, "right", 1)
            same = store.append_data("right", "petya", 1, "right-1", "ref:right:1")
            self.assertEqual(same.data_sequence, 1)
            with self.assertRaisesRegex(ComposedProtocolError, "identity drift"):
                store.append_data("right", "petya", 1, "right-1", "changed")
            store.close()

    def test_storage_guards_reject_kind_gap_snapshot_and_output_rewrites(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "guards.sqlite"
            store = self.seed_aligned_join(path, complete=True)
            store.close()
            statements = (
                "UPDATE stream_entries SET kind = 'BARRIER' "
                "WHERE input_id = 'left' AND log_sequence = 1",
                "INSERT INTO stream_entries VALUES "
                "('left', 99, 'DATA', 'gap', 99, 'ref:gap', NULL, NULL, NULL, 1)",
                "UPDATE snapshots SET cuts_json = '{}' WHERE checkpoint = 1",
                "UPDATE turns SET apply_ordinal = 99 WHERE apply_ordinal = 1",
                "UPDATE output_events SET output_ordinal = -1 "
                "WHERE output_id = 'barrier-out:1'",
                "INSERT OR REPLACE INTO checkpoint_status VALUES (1, 'COMPLETED')",
            )
            for statement in statements:
                with self.subTest(statement=statement):
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.sql(path, statement)

    def test_independent_oracle_rejects_cursor_cut_mailbox_and_output_controls(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            store = self.seed_aligned_join(base, complete=True)
            store.close()

            cursor = root / "cursor.sqlite"
            shutil.copy2(base, cursor)
            self.sql(
                cursor,
                "UPDATE operator_progress SET completed_data_through = 4 "
                "WHERE input_id = 'left'",
            )
            report = self.oracle(cursor, False)
            self.assertIn("data_completion_cursor:left", report["violations"])

            cut = root / "cut.sqlite"
            shutil.copy2(base, cut)
            connection = sqlite3.connect(cut)
            connection.row_factory = sqlite3.Row
            row = connection.execute(
                "SELECT cuts_json FROM snapshots WHERE checkpoint = 1"
            ).fetchone()
            cuts = json.loads(row[0])
            cuts["left"]["data"] = 3
            with connection:
                connection.execute("DROP TRIGGER immutable_snapshot")
                connection.execute(
                    "UPDATE snapshots SET cuts_json = ? WHERE checkpoint = 1",
                    (json.dumps(cuts, sort_keys=True, separators=(",", ":")),),
                )
            connection.close()
            report = self.oracle(cut, False)
            self.assertIn("snapshot_cut_mismatch:1:left", report["violations"])

            mailbox = root / "mailbox.sqlite"
            shutil.copy2(base, mailbox)
            self.corrupt(
                mailbox,
                "immutable_turn",
                "UPDATE turns SET apply_ordinal = -1 WHERE apply_ordinal = 5",
            )
            report = self.oracle(mailbox, False)
            self.assertIn("mailbox_apply_order_gap", report["violations"])

            output = root / "output.sqlite"
            shutil.copy2(base, output)
            self.corrupt(
                output,
                "immutable_output",
                "UPDATE output_events SET output_ordinal = -1 "
                "WHERE output_id = 'barrier-out:1'",
            )
            report = self.oracle(output, False)
            self.assertIn("forwarded_barrier_overtook_data:1", report["violations"])

    def test_independent_oracle_rejects_barrier_authority_corruption(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            store = self.seed_aligned_join(base, complete=True)
            store.close()

            cases = (
                (
                    "barrier-digest.sqlite",
                    "entry_no_update",
                    "UPDATE stream_entries SET intent_digest = 'forged' "
                    "WHERE input_id = 'left' AND kind = 'BARRIER'",
                    "barrier_intent_digest:1:left",
                ),
                (
                    "barrier-epoch.sqlite",
                    "entry_no_update",
                    "UPDATE stream_entries SET writer_epoch = 99 "
                    "WHERE input_id = 'left' AND kind = 'BARRIER'",
                    "barrier_writer_epoch:1:left",
                ),
                (
                    "barrier-checkpoint.sqlite",
                    "entry_no_update",
                    "UPDATE stream_entries SET checkpoint = 99 "
                    "WHERE input_id = 'left' AND kind = 'BARRIER'",
                    "barrier_intent_missing:99:left",
                ),
                (
                    "observation-digest.sqlite",
                    "immutable_observation",
                    "UPDATE barrier_observations SET intent_digest = 'forged' "
                    "WHERE checkpoint = 1 AND input_id = 'left'",
                    "barrier_observation_identity:1:left",
                ),
            )
            for filename, trigger, statement, violation in cases:
                with self.subTest(filename=filename):
                    corrupted = root / filename
                    shutil.copy2(base, corrupted)
                    self.corrupt(corrupted, trigger, statement)
                    report = self.oracle(corrupted, False)
                    self.assertIn(violation, report["violations"])

    def test_truncated_recovery_witness_is_rejected_by_storage_and_oracle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            store = self.seed_aligned_join(base, complete=True)
            store.begin_recovery()
            store.close()

            target_bypass = root / "target-bypass.sqlite"
            shutil.copy2(base, target_bypass)
            with self.assertRaises(sqlite3.IntegrityError):
                self.sql(
                    target_bypass,
                    "INSERT INTO recovery_runs VALUES "
                    "(1, 1, 'RESTORING', "
                    "(SELECT state_value FROM snapshots WHERE checkpoint = 1), "
                    'NULL, \'{"left":2,"right":2}\', NULL, '
                    "(SELECT authority_ordinal FROM recovery_fence))",
                )

            suffix_bypass = root / "suffix-bypass.sqlite"
            shutil.copy2(base, suffix_bypass)
            self.sql(
                suffix_bypass,
                "INSERT INTO recovery_runs VALUES "
                "(1, 1, 'RESTORING', "
                "(SELECT state_value FROM snapshots WHERE checkpoint = 1), "
                'NULL, \'{"left":3,"right":3}\', NULL, '
                "(SELECT authority_ordinal FROM recovery_fence))",
            )
            with self.assertRaises(sqlite3.IntegrityError):
                self.sql(
                    suffix_bypass,
                    "UPDATE recovery_runs SET phase = 'READY', "
                    "recovered_state = (SELECT state_value FROM operator_state), "
                    "replayed_through_ordinal = 4 WHERE generation = 1",
                )

            oracle_bypass = root / "oracle-bypass.sqlite"
            shutil.copy2(base, oracle_bypass)
            connection = sqlite3.connect(oracle_bypass)
            with connection:
                connection.execute("DROP TRIGGER recovery_run_admission")
                connection.execute("DROP TRIGGER recovery_run_transition")
                connection.execute(
                    "INSERT INTO recovery_runs VALUES "
                    "(1, 1, 'RESTORING', "
                    "(SELECT state_value FROM snapshots WHERE checkpoint = 1), "
                    'NULL, \'{"left":2,"right":2}\', NULL, '
                    "(SELECT authority_ordinal FROM recovery_fence))"
                )
                connection.execute(
                    "UPDATE recovery_runs SET phase = 'READY', "
                    "recovered_state = (SELECT state_value FROM snapshots "
                    "WHERE checkpoint = 1), replayed_through_ordinal = 4 "
                    "WHERE generation = 1"
                )
            connection.close()
            report = self.oracle(oracle_bypass, False)
            self.assertIn("recovery_target_cursor_mismatch", report["violations"])
            self.assertIn("recovery_run_mismatch", report["violations"])

            replace_bypass = root / "replace-bypass.sqlite"
            replacement = self.seed_aligned_join(replace_bypass, complete=True)
            replacement.close()
            replacement = ComposedDurableStream(replace_bypass)
            replacement.begin_recovery()
            replacement.recover()
            self.append(replacement, "left", 4)
            replacement.process_next("left")
            replacement.close()
            with self.assertRaises(sqlite3.IntegrityError):
                self.sql(
                    replace_bypass,
                    "INSERT OR REPLACE INTO recovery_runs VALUES "
                    "(2, 1, 'RESTORING', "
                    "(SELECT state_value FROM snapshots WHERE checkpoint = 1), "
                    'NULL, \'{"left":4,"right":3}\', NULL, '
                    "(SELECT authority_ordinal FROM recovery_fence))",
                )
            self.oracle(replace_bypass, True)

            generation_corruption = root / "generation-corruption.sqlite"
            shutil.copy2(replace_bypass, generation_corruption)
            connection = sqlite3.connect(generation_corruption)
            with connection:
                connection.execute("DROP TRIGGER recovery_run_create_once")
                connection.execute("DROP TRIGGER recovery_run_admission")
                connection.execute("DROP TRIGGER recovery_run_transition")
                connection.execute(
                    "INSERT OR REPLACE INTO recovery_runs VALUES "
                    "(2, 1, 'RESTORING', "
                    "(SELECT state_value FROM snapshots WHERE checkpoint = 1), "
                    'NULL, \'{"left":4,"right":3}\', NULL, '
                    "(SELECT authority_ordinal FROM recovery_fence))"
                )
                connection.execute(
                    "UPDATE recovery_runs SET phase = 'READY', "
                    "recovered_state = (SELECT state_value FROM operator_state), "
                    "replayed_through_ordinal = "
                    "(SELECT MAX(apply_ordinal) FROM turns) WHERE generation = 2"
                )
            connection.close()
            report = self.oracle(generation_corruption, False)
            self.assertIn("recovery_generation_authority", report["violations"])

    def seed_aligned_join(self, path: Path, *, complete: bool) -> ComposedDurableStream:
        store = self.store(path, ("left", "right"))
        self.append(store, "left", 1)
        self.append(store, "right", 1)
        self.append(store, "left", 2)
        intent = store.start_checkpoint(1, ("left", "right"), "coord", 1)
        store.inject_barrier(1, "left", "coord", 1, intent)
        self.append(store, "left", 3)
        self.append(store, "right", 2)
        store.inject_barrier(1, "right", "coord", 1, intent)
        self.append(store, "right", 3)

        store.process_next("left")
        store.process_next("right")
        store.process_next("left")
        self.assertEqual(store.process_next("left").kind, EntryKind.BARRIER)
        self.assertIsNone(store.process_next("left"))
        before_right = len(store.outputs())
        store.process_next("right")
        self.assertEqual(len(store.outputs()), before_right + 1)
        self.assertEqual(store.process_next("right").kind, EntryKind.BARRIER)
        self.assertEqual(store.status(1), "ALIGNED")
        store.process_next("left")
        store.process_next("right")
        if complete:
            store.complete_checkpoint(1)
        return store

    @staticmethod
    def store(path: Path, inputs: tuple[str, ...]) -> ComposedDurableStream:
        store = ComposedDurableStream(path)
        store.claim_coordinator("coord", 1)
        for input_id in inputs:
            store.create_partition(input_id, "petya", 1)
        store.create_operator(inputs)
        return store

    @staticmethod
    def append(store: ComposedDurableStream, input_id: str, sequence: int) -> None:
        store.append_data(
            input_id,
            "petya",
            1,
            f"{input_id}-{sequence}",
            f"ref:{input_id}:{sequence}",
        )

    @staticmethod
    def oracle(path: Path, expect_pass: bool) -> dict[str, object]:
        result = subprocess.run(
            [sys.executable, str(ORACLE), str(path)],
            cwd=HERE,
            text=True,
            capture_output=True,
            check=False,
        )
        expected = 0 if expect_pass else 2
        if result.returncode != expected:
            raise AssertionError(
                f"oracle rc={result.returncode}, stdout={result.stdout}, "
                f"stderr={result.stderr}"
            )
        return json.loads(result.stdout)

    @staticmethod
    def sql(path: Path, statement: str) -> None:
        connection = sqlite3.connect(path)
        with connection:
            connection.execute(statement)
        connection.close()

    @staticmethod
    def corrupt(path: Path, trigger: str, statement: str) -> None:
        connection = sqlite3.connect(path)
        with connection:
            connection.execute(f"DROP TRIGGER {trigger}")
            connection.execute(statement)
        connection.close()


if __name__ == "__main__":
    unittest.main()
