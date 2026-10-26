from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from branch_join_v2 import (
    BranchJoinProtocolError,
    BranchJoinStore,
    Delivery,
)


HERE = Path(__file__).resolve().parent
ORACLE = HERE / "branch_join_oracle.py"


class BranchJoinV2Tests(unittest.TestCase):
    def test_fanout_anna_200_boris_300_and_tanya_resumes_only_anna(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fanout.sqlite"
            store = BranchJoinStore(path)
            store.create_fanout("deploy-1", ("anna", "boris"))
            for sequence in range(1, 302):
                self.assertEqual(
                    store.append_upstream(
                        f"document-{sequence}", f"ref:document:{sequence}"
                    ),
                    sequence,
                )
            self.complete_branch_range(store, "anna", 200)
            self.complete_branch_range(store, "boris", 300)
            before = store.branch_positions()
            self.assertEqual(before["anna"]["completed_through"], 200)
            self.assertEqual(before["boris"]["completed_through"], 300)
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM upstream_entries").fetchone()[0],
                301,
            )
            store.close()

            recovered = BranchJoinStore(path)
            recovered.claim_branch("anna", "tanya-worker", 2)
            anna_delivery = recovered.deliver_branch("anna", "tanya-worker", 2)
            self.assertEqual(anna_delivery.sequence, 201)
            anna_output = recovered.complete_branch(anna_delivery)
            self.assertEqual(recovered.complete_branch(anna_delivery), anna_output)

            boris_delivery = recovered.deliver_branch("boris", "boris-worker", 1)
            self.assertEqual(boris_delivery.sequence, 301)
            recovered.complete_branch(boris_delivery)
            positions = recovered.branch_positions()
            self.assertEqual(positions["anna"]["completed_through"], 201)
            self.assertEqual(positions["anna"]["owner_epoch"], 2)
            self.assertEqual(positions["boris"]["completed_through"], 301)
            self.assertEqual(positions["boris"]["owner_epoch"], 1)
            with self.assertRaisesRegex(BranchJoinProtocolError, "exact delivery"):
                recovered.complete_branch(
                    Delivery(
                        "anna",
                        202,
                        "document-202",
                        "ref:document:202",
                        "tanya-worker",
                        2,
                        1,
                    )
                )
            recovered.close()
            report = self.oracle(path, True)
            self.assertEqual(report["observations"]["branches"]["anna"]["next"], 202)
            self.assertEqual(report["observations"]["branches"]["boris"]["next"], 302)

    def test_healthy_branch_crosses_barrier_while_anna_lags(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "branch-barrier.sqlite"
            store = BranchJoinStore(path)
            store.create_fanout("deploy-1", ("anna", "boris"))
            store.append_upstream("document-1", "ref:1")
            store.append_upstream("document-2", "ref:2")
            self.complete_branch_range(store, "anna", 2)
            self.complete_branch_range(store, "boris", 2)
            store.start_fanout_checkpoint(1)
            store.append_upstream("document-3", "ref:3")

            self.assertEqual(store.observe_branch_barrier("boris", "boris-worker", 1), 1)
            self.assertEqual(
                store.connection.execute(
                    "SELECT status FROM fanout_checkpoints WHERE checkpoint = 1"
                ).fetchone()[0],
                "STARTED",
            )
            delivery = store.deliver_branch("boris", "boris-worker", 1)
            self.assertEqual(delivery.sequence, 3)
            store.complete_branch(delivery)
            self.assertEqual(store.branch_positions()["boris"]["completed_through"], 3)
            self.assertEqual(store.branch_positions()["anna"]["completed_through"], 2)

            store.observe_branch_barrier("anna", "anna-worker", 1)
            self.assertEqual(
                store.connection.execute(
                    "SELECT status FROM fanout_checkpoints WHERE checkpoint = 1"
                ).fetchone()[0],
                "ALIGNED",
            )
            store.close()
            self.oracle(path, True)

    def test_skewed_join_snapshots_exact_cut_then_drains_durable_buffer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "join.sqlite"
            store = self.seed_join_before_final_barrier(path)
            before = store.join_view()
            joined_sources = store.connection.execute(
                "SELECT e.input_id, e.data_sequence, e.source_output_id, "
                "o.branch_id, o.partition_id FROM join_entries AS e "
                "JOIN branch_outputs AS o ON o.output_id = e.source_output_id "
                "WHERE e.kind = 'DATA' ORDER BY e.arrival_ordinal"
            ).fetchall()
            self.assertEqual(
                [tuple(row) for row in joined_sources],
                [
                    ("fast", 1, self.branch_output_id(store, "boris", 1), "boris", "p0"),
                    ("fast", 2, self.branch_output_id(store, "boris", 2), "boris", "p0"),
                    ("fast", 3, self.branch_output_id(store, "boris", 3), "boris", "p0"),
                    ("fast", 4, self.branch_output_id(store, "boris", 4), "boris", "p0"),
                    ("slow", 1, self.branch_output_id(store, "anna", 1), "anna", "p0"),
                    ("slow", 2, self.branch_output_id(store, "anna", 2), "anna", "p0"),
                ],
            )
            self.assertEqual(
                [(row["input_id"], row["data_sequence"], row["drained"]) for row in before["buffer"]],
                [("fast", 3, 0), ("fast", 4, 0)],
            )
            self.assertEqual(before["progress"]["fast"]["blocked"], 1)
            self.assertEqual(before["progress"]["slow"]["completed"], 2)

            intent = store.connection.execute(
                "SELECT intent_digest FROM join_checkpoint_intents WHERE checkpoint = 1"
            ).fetchone()[0]
            store.inject_join_barrier(
                1, "slow", intent, self.branch_barrier_id(store, "anna", 1)
            )
            self.assertEqual(store.process_join_next("slow"), "BARRIER")
            snapshot = store.join_snapshot(1)
            self.assertEqual(snapshot["mailbox_highwater"], 4)
            self.assertEqual(
                snapshot["cuts"],
                {
                    "fast": {"barrier_log": 3, "data": 2},
                    "slow": {"barrier_log": 3, "data": 2},
                },
            )
            view = store.join_view()
            self.assertEqual(
                [(row["input_id"], row["data_sequence"], row["drain_apply_ordinal"]) for row in view["buffer"]],
                [("fast", 3, 5), ("fast", 4, 6)],
            )
            self.assertEqual(
                [(row["kind"], row["input_id"], row["data_sequence"]) for row in view["outputs"]],
                [
                    ("DATA", "fast", 1),
                    ("DATA", "fast", 2),
                    ("DATA", "slow", 1),
                    ("DATA", "slow", 2),
                    ("BARRIER", None, None),
                    ("DATA", "fast", 3),
                    ("DATA", "fast", 4),
                ],
            )
            store.complete_join_checkpoint(1)
            store.close()
            self.oracle(path, True)

    def test_join_rejects_ghost_without_completed_branch_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ghost.sqlite"
            store = BranchJoinStore(path)
            store.create_fanout("deploy-1", ("anna", "boris"))
            store.append_upstream("document-1", "ref:1")
            store.create_join(
                {"fast": ("boris", "p0"), "slow": ("anna", "p0")}
            )
            with self.assertRaisesRegex(
                BranchJoinProtocolError, "source output missing"
            ):
                store.append_join_data("fast", "ghost")
            self.assertEqual(
                store.connection.execute("SELECT COUNT(*) FROM join_entries").fetchone()[0],
                0,
            )
            store.close()
            self.oracle(path, True)

    def test_join_rejects_synthesized_barrier_without_fanout_authority(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "synthetic-barrier.sqlite"
            store = BranchJoinStore(path)
            store.create_fanout("deploy-1", ("anna", "boris"))
            for sequence in range(1, 3):
                store.append_upstream(f"document-{sequence}", f"ref:{sequence}")
            self.complete_branch_range(store, "anna", 2)
            self.complete_branch_range(store, "boris", 2)
            store.create_join({"fast": ("boris", "p0"), "slow": ("anna", "p0")})
            for sequence in range(1, 3):
                store.append_join_data(
                    "fast", self.branch_output_id(store, "boris", sequence)
                )
                store.append_join_data(
                    "slow", self.branch_output_id(store, "anna", sequence)
                )
                store.process_join_next("fast")
                store.process_join_next("slow")
            with self.assertRaisesRegex(
                BranchJoinProtocolError, "fanout checkpoint missing"
            ):
                store.start_join_checkpoint(1)
            store.close()
            self.oracle(path, True)

    def test_fresh_reopen_before_and_after_alignment_matches_failure_free(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paused = root / "paused.sqlite"
            store = self.seed_join_before_final_barrier(paused)
            store.close()
            expected = root / "expected.sqlite"
            shutil.copy2(paused, expected)

            expected_store = BranchJoinStore(expected)
            self.finish_join(expected_store)
            expected_view = expected_store.join_view()
            expected_snapshot = expected_store.join_snapshot(1)
            expected_store.close()

            reopened = BranchJoinStore(paused)
            self.finish_join(reopened)
            self.assertEqual(reopened.join_view(), expected_view)
            self.assertEqual(reopened.join_snapshot(1), expected_snapshot)
            reopened.close()

            after = BranchJoinStore(paused)
            self.assertEqual(after.join_view(), expected_view)
            self.assertEqual(after.join_snapshot(1), expected_snapshot)
            after.close()
            self.oracle(paused, True)

    def test_independent_oracle_rejects_branch_and_join_unsafe_controls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            store = self.seed_join_before_final_barrier(base)
            self.finish_join(store)
            store.close()
            self.oracle(base, True)

            cases = (
                (
                    "extra-branch.sqlite",
                    None,
                    "INSERT INTO branch_manifest VALUES ('charlie')",
                    "branch_manifest_membership",
                ),
                (
                    "shared-cursor.sqlite",
                    None,
                    "UPDATE branch_progress SET completed_through = 3 "
                    "WHERE branch_id = 'anna'",
                    "branch_cursor_coverage:anna",
                ),
                (
                    "cross-completion.sqlite",
                    "completion_immutable",
                    "UPDATE branch_completions SET record_id = 'document-3' "
                    "WHERE branch_id = 'anna' AND data_sequence = 2",
                    "branch_completion_identity:anna:2",
                ),
                (
                    "join-binding.sqlite",
                    "join_binding_immutable",
                    "UPDATE join_inputs SET branch_id = 'charlie' WHERE input_id = 'fast'",
                    "join_input_binding",
                ),
                (
                    "ghost-source.sqlite",
                    "join_entry_immutable",
                    "UPDATE join_entries SET source_output_id = 'ghost' "
                    "WHERE input_id = 'fast' AND data_sequence = 1",
                    "join_source_output:fast:1",
                ),
                (
                    "ghost-barrier-source.sqlite",
                    "join_entry_immutable",
                    "UPDATE join_entries SET source_barrier_id = 'ghost' "
                    "WHERE input_id = 'fast' AND kind = 'BARRIER'",
                    "join_source_barrier:1:fast",
                ),
                (
                    "branch-barrier-lineage.sqlite",
                    "branch_barrier_output_immutable",
                    "UPDATE branch_barrier_outputs SET lineage_digest = 'forged' "
                    "WHERE branch_id = 'boris' AND checkpoint = 1",
                    "branch_barrier_output_identity:boris:1",
                ),
                (
                    "mixed-cut.sqlite",
                    "join_snapshot_immutable",
                    "UPDATE join_snapshots SET cuts_json = "
                    "'{\"fast\":{\"barrier_log\":3,\"data\":3},"
                    "\"slow\":{\"barrier_log\":3,\"data\":2}}' "
                    "WHERE checkpoint = 1",
                    "join_snapshot_cut:1:fast",
                ),
                (
                    "barrier-digest.sqlite",
                    "join_entry_immutable",
                    "UPDATE join_entries SET intent_digest = 'forged' "
                    "WHERE input_id = 'fast' AND kind = 'BARRIER'",
                    "join_barrier_authority:1:fast",
                ),
                (
                    "barrier-cut.sqlite",
                    "join_entry_immutable",
                    "UPDATE join_entries SET cut_data_sequence = 3 "
                    "WHERE input_id = 'fast' AND kind = 'BARRIER'",
                    "join_barrier_cut:1:fast",
                ),
                (
                    "buffer-order.sqlite",
                    "join_buffer_identity_immutable",
                    "UPDATE join_buffer SET arrival_ordinal = -1 "
                    "WHERE input_id = 'fast' AND data_sequence = 4",
                    "join_buffer_identity:fast:4",
                ),
                (
                    "early-buffer.sqlite",
                    "join_turn_immutable",
                    "UPDATE join_turns SET source = 'DIRECT' WHERE apply_ordinal = 5",
                    "join_buffer_turn:fast:3",
                ),
                (
                    "missing-buffer.sqlite",
                    "join_buffer_no_delete",
                    "DELETE FROM join_buffer WHERE input_id = 'fast' "
                    "AND data_sequence = 3",
                    "join_buffer_turn_missing:5",
                ),
                (
                    "early-barrier.sqlite",
                    "join_output_immutable",
                    "UPDATE join_outputs SET output_ordinal = 0 "
                    "WHERE output_id = 'join-barrier:1'",
                    "join_barrier_overtook_data:1",
                ),
            )
            for filename, trigger, statement, expected in cases:
                with self.subTest(filename=filename):
                    corrupted = root / filename
                    shutil.copy2(base, corrupted)
                    self.corrupt(corrupted, trigger, statement)
                    report = self.oracle(corrupted, False)
                    self.assertIn(expected, report["violations"])

    def seed_join_before_final_barrier(self, path: Path) -> BranchJoinStore:
        store = BranchJoinStore(path)
        store.create_fanout("deploy-1", ("anna", "boris"))
        for sequence in range(1, 3):
            store.append_upstream(f"document-{sequence}", f"ref:{sequence}")
        self.complete_branch_range(store, "anna", 2)
        self.complete_branch_range(store, "boris", 2)
        store.start_fanout_checkpoint(1)
        store.observe_branch_barrier("boris", "boris-worker", 1)
        store.observe_branch_barrier("anna", "anna-worker", 1)
        for sequence in range(3, 5):
            store.append_upstream(f"document-{sequence}", f"ref:{sequence}")
        self.complete_branch_range(store, "boris", 4)
        store.create_join(
            {"fast": ("boris", "p0"), "slow": ("anna", "p0")}
        )
        self.populate_join_before_final(store)
        return store

    def populate_join_before_final(self, store: BranchJoinStore) -> None:
        store.append_join_data("fast", self.branch_output_id(store, "boris", 1))
        store.append_join_data("fast", self.branch_output_id(store, "boris", 2))
        intent = store.start_join_checkpoint(1)
        store.inject_join_barrier(
            1, "fast", intent, self.branch_barrier_id(store, "boris", 1)
        )
        store.append_join_data("fast", self.branch_output_id(store, "boris", 3))
        store.process_join_next("fast")
        store.process_join_next("fast")
        store.process_join_next("fast")
        store.append_join_data("fast", self.branch_output_id(store, "boris", 4))
        store.append_join_data("slow", self.branch_output_id(store, "anna", 1))
        store.append_join_data("slow", self.branch_output_id(store, "anna", 2))
        store.process_join_next("slow")
        store.process_join_next("slow")

    @classmethod
    def finish_join(cls, store: BranchJoinStore) -> None:
        intent = store.connection.execute(
            "SELECT intent_digest FROM join_checkpoint_intents WHERE checkpoint = 1"
        ).fetchone()[0]
        store.inject_join_barrier(
            1, "slow", intent, cls.branch_barrier_id(store, "anna", 1)
        )
        store.process_join_next("slow")
        store.complete_join_checkpoint(1)

    @staticmethod
    def complete_branch_range(
        store: BranchJoinStore, branch_id: str, through: int
    ) -> None:
        owner = store.branch_positions()[branch_id]
        for expected in range(owner["completed_through"] + 1, through + 1):
            delivery = store.deliver_branch(
                branch_id, owner["owner_id"], owner["owner_epoch"]
            )
            if delivery is None or delivery.sequence != expected:
                raise AssertionError((branch_id, expected, delivery))
            store.complete_branch(delivery)

    @staticmethod
    def branch_output_id(
        store: BranchJoinStore, branch_id: str, sequence: int
    ) -> str:
        row = store.connection.execute(
            "SELECT output_id FROM branch_outputs WHERE branch_id = ? "
            "AND upstream_data_sequence = ?",
            (branch_id, sequence),
        ).fetchone()
        if row is None:
            raise AssertionError((branch_id, sequence))
        return row[0]

    @staticmethod
    def branch_barrier_id(
        store: BranchJoinStore, branch_id: str, checkpoint: int
    ) -> str:
        row = store.connection.execute(
            "SELECT barrier_output_id FROM branch_barrier_outputs "
            "WHERE branch_id = ? AND checkpoint = ?",
            (branch_id, checkpoint),
        ).fetchone()
        if row is None:
            raise AssertionError((branch_id, checkpoint))
        return row[0]

    @staticmethod
    def corrupt(path: Path, trigger: str | None, statement: str) -> None:
        connection = sqlite3.connect(path)
        with connection:
            if trigger is not None:
                connection.execute(f"DROP TRIGGER {trigger}")
            connection.execute(statement)
        connection.close()

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


if __name__ == "__main__":
    unittest.main()
