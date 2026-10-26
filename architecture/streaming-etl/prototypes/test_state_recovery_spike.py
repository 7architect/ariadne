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

from state_recovery_spike import (
    StateClass,
    StateContract,
    StateRecoveryError,
    StateRecoveryStore,
    StateRequirements,
    apply_order_sensitive,
    digest,
    required_state_class,
    validate_contract,
)


HERE = Path(__file__).resolve().parent
ORACLE = HERE / "state_recovery_oracle.py"


class StateRecoverySpikeTests(unittest.TestCase):
    def test_state_classification_and_unsupported_contracts(self) -> None:
        self.assertEqual(
            required_state_class(
                StateRequirements(False, ("input:p0",), True, True)
            ),
            StateClass.EPHEMERAL,
        )
        local = StateRequirements(True, ("input:p0",), True, True)
        self.assertEqual(
            required_state_class(local), StateClass.PARTITION_LOCAL_PER_TURN
        )
        for requirements in (
            StateRequirements(
                True,
                ("left", "right"),
                False,
                False,
                shared_across_partitions=True,
            ),
            StateRequirements(
                True,
                ("input:p0",),
                True,
                False,
                has_timers_or_watermarks=True,
            ),
        ):
            self.assertEqual(
                required_state_class(requirements),
                StateClass.CHECKPOINT_MANAGED_SHARED,
            )

        with self.assertRaisesRegex(StateRecoveryError, "deterministic replay"):
            required_state_class(
                StateRequirements(
                    True,
                    ("input:p0",),
                    True,
                    True,
                    deterministic_replay=False,
                )
            )
        with self.assertRaisesRegex(StateRecoveryError, "mailbox order"):
            required_state_class(
                StateRequirements(
                    True,
                    ("left", "right"),
                    False,
                    False,
                    persisted_mailbox_order=False,
                )
            )
        for bindings in ((), ("left", "left"), ("",)):
            with self.subTest(bindings=bindings):
                with self.assertRaisesRegex(
                    StateRecoveryError, "nonempty unique input bindings"
                ):
                    required_state_class(
                        StateRequirements(True, bindings, True, True)
                    )
        with self.assertRaisesRegex(
            StateRecoveryError, "CHECKPOINT_MANAGED_SHARED"
        ):
            validate_contract(
                StateContract(
                    StateClass.PARTITION_LOCAL_PER_TURN,
                    ("left", "right"),
                    "state/v1",
                    "code-v1",
                    "turn_completion",
                    True,
                    True,
                ),
                StateRequirements(
                    True,
                    ("left", "right"),
                    False,
                    False,
                    shared_across_partitions=True,
                ),
            )
        with self.assertRaisesRegex(
            StateRecoveryError, "deterministic replay drift"
        ):
            validate_contract(
                StateContract(
                    StateClass.PARTITION_LOCAL_PER_TURN,
                    ("input:p0",),
                    "state/v1",
                    "code-v1",
                    "turn_completion",
                    False,
                    True,
                ),
                local,
            )

    def test_local_state_recovers_on_both_sides_of_document_200(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            before = root / "before.sqlite"
            self.seed_local(before, 199)
            report = self.oracle(before, True)
            self.assertEqual(report["plan"]["state"], {"signed": 199})
            self.assertEqual(report["plan"]["next"], 200)

            after = root / "after.sqlite"
            shutil.copy2(before, after)
            store = StateRecoveryStore(after)
            store.complete_local(
                200, {"signed": 200}, "signed-200", "counter/v1", "code-v1"
            )
            store.complete_local(
                200, {"signed": 200}, "signed-200", "counter/v1", "code-v1"
            )
            with self.assertRaisesRegex(StateRecoveryError, "identity drift"):
                store.complete_local(
                    200,
                    {"signed": 999},
                    "signed-200",
                    "counter/v1",
                    "code-v1",
                )
            store.close()
            report = self.oracle(after, True)
            self.assertEqual(report["plan"]["state"], {"signed": 200})
            self.assertEqual(report["plan"]["next"], 201)

    def test_state_contract_exact_retry_cannot_hide_declaration_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "contract.sqlite"
            store = StateRecoveryStore(path)
            contract = StateContract(
                StateClass.PARTITION_LOCAL_PER_TURN,
                ("input:p0",),
                "counter/v1",
                "code-v1",
                "turn_completion",
                True,
                True,
            )
            store.set_contract(contract, self.local_requirements())
            store.set_contract(contract, self.local_requirements())
            with self.assertRaisesRegex(StateRecoveryError, "identity drift"):
                store.set_contract(
                    StateContract(
                        StateClass.PARTITION_LOCAL_PER_TURN,
                        ("input:p0",),
                        "counter/v2",
                        "code-v1",
                        "turn_completion",
                        True,
                        True,
                    ),
                    self.local_requirements(),
                )
            store.close()

    def test_local_oracle_rejects_every_split_authority_control(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            self.seed_local(base, 199)

            controls = {
                "cursor_ahead": (
                    "UPDATE local_head SET completed_through = 200",
                    {"local_completion_cursor_atomicity"},
                ),
                "state_ahead": (
                    "UPDATE local_head SET state_json = '{\"signed\":200}'",
                    {"local_state_cursor_atomicity"},
                ),
                "output_without_completion": (
                    "INSERT INTO local_outputs VALUES (200, 'signed-200')",
                    {"local_output_completion_atomicity"},
                ),
                "schema_drift": (
                    "UPDATE local_completions SET state_schema = 'counter/v2' "
                    "WHERE sequence = 199",
                    {"local_state_compatibility:199"},
                ),
                "malformed_state": (
                    "UPDATE local_head SET state_json = '{broken'",
                    {"local_state_cursor_atomicity", "local_head_state_json"},
                ),
            }
            for name, (statement, expected) in controls.items():
                with self.subTest(name=name):
                    fixture = root / f"{name}.sqlite"
                    shutil.copy2(base, fixture)
                    self.sql(fixture, statement)
                    report = self.oracle(fixture, False)
                    self.assertTrue(expected.issubset(set(report["violations"])))

            mixed = root / "mixed.sqlite"
            shutil.copy2(base, mixed)
            store = StateRecoveryStore(mixed)
            store.add_checkpoint(
                1,
                "snapshot",
                {"input:p0": 100},
                state_schema="counter/v1",
                code_version="code-v1",
                completed=True,
            )
            store.close()
            report = self.oracle(mixed, False)
            self.assertIn("mixed_recovery_authority", report["violations"])

            split = root / "split-local-authority.sqlite"
            self.seed_shared(split)
            self.sql(
                split,
                "INSERT INTO local_outputs VALUES (1, 'local-1')",
                "INSERT INTO local_completions VALUES "
                "(1, '{}', 'shared/v1', 'code-v1', 'local-1')",
            )
            report = self.oracle(split, False)
            self.assertIn("mixed_recovery_authority", report["violations"])

    def test_checkpoint_state_restores_completed_cut_and_exact_ordered_tail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "shared.sqlite"
            self.seed_shared(path)
            report = self.oracle(path, True)
            plan = report["plan"]
            self.assertEqual(plan["checkpoint"], 1)
            self.assertEqual(plan["cuts"], {"left": 1, "right": 1})
            self.assertEqual(
                plan["replay_order"],
                [[3, "left", 2], [4, "right", 2], [5, "left", 3]],
            )
            self.assertEqual(plan["next"], {"left": 4, "right": 3})

    def test_checkpoint_oracle_rejects_unsafe_recovery_controls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            self.seed_shared(base)
            controls = {
                "missing_tail": (
                    "DELETE FROM mailbox WHERE input_id = 'left' "
                    "AND input_sequence = 2",
                    {"replay_tail_incomplete:left"},
                    None,
                ),
                "wrong_order": (
                    "UPDATE mailbox SET apply_ordinal = -1 "
                    "WHERE input_id = 'right' AND input_sequence = 2",
                    {"deterministic_mailbox_replay_mismatch"},
                    None,
                ),
                "retention_gap": (
                    "UPDATE recovery_targets SET retained_from = 3 "
                    "WHERE input_id = 'left'",
                    {"replay_tail_unavailable:left"},
                    None,
                ),
                "mixed_authority": (
                    "INSERT INTO local_head VALUES "
                    "(1, 0, '{}', 'shared/v1', 'code-v1')",
                    {"mixed_recovery_authority"},
                    None,
                ),
                "barrier_not_ordered_after_cut": (
                    "UPDATE barrier_witnesses SET ordered_log_sequence = 99 "
                    "WHERE checkpoint = 1 AND input_id = 'left'",
                    {"checkpoint_barrier_witness_invalid:1:left"},
                    "sealed_witness_no_update",
                ),
                "mixed_barrier_witness": (
                    "UPDATE barrier_witnesses SET intent_digest = 'checkpoint:0' "
                    "WHERE checkpoint = 1 AND input_id = 'right'",
                    {"checkpoint_barrier_witness_invalid:1:right"},
                    "sealed_witness_no_update",
                ),
                "snapshot_drift": (
                    "UPDATE checkpoint_intents SET snapshot_state = 'changed' "
                    "WHERE checkpoint = 1",
                    {"checkpoint_intent_digest_invalid:1"},
                    "sealed_intent_no_update",
                ),
                "unknown_mailbox_input": (
                    "INSERT INTO mailbox VALUES (6, 'unknown', 1, 'X')",
                    {"mailbox_input_unknown:unknown"},
                    None,
                ),
            }
            for name, (statement, expected, disabled_guard) in controls.items():
                with self.subTest(name=name):
                    fixture = root / f"{name}.sqlite"
                    shutil.copy2(base, fixture)
                    if disabled_guard is None:
                        self.sql(fixture, statement)
                    else:
                        self.corrupt_sql(fixture, disabled_guard, statement)
                    report = self.oracle(fixture, False)
                    self.assertTrue(expected.issubset(set(report["violations"])))

            incompatible = root / "incompatible.sqlite"
            shutil.copy2(base, incompatible)
            self.corrupt_sql(
                incompatible,
                "sealed_intent_no_update",
                "UPDATE checkpoint_intents SET state_schema = 'shared/v2' "
                "WHERE checkpoint = 1",
            )
            report = self.oracle(incompatible, False)
            self.assertIn("checkpoint_incompatible:1", report["violations"])
            self.assertIn(
                "completed_compatible_checkpoint_missing", report["violations"]
            )

    def test_checkpoint_authority_rows_are_storage_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "immutable.sqlite"
            self.seed_shared(path)
            final_state = "S"
            for input_id, sequence, value in (
                ("left", 2, "A"),
                ("right", 2, "B"),
                ("left", 3, "C"),
            ):
                final_state = apply_order_sensitive(
                    final_state, input_id, sequence, value
                )
            store = StateRecoveryStore(path)
            with self.assertRaises(sqlite3.IntegrityError):
                store.add_checkpoint(
                    3,
                    final_state,
                    {"left": 3, "right": 2},
                    state_schema="shared/v1",
                    code_version="code-v1",
                    completed=True,
                )
            store.close()
            statements = (
                "INSERT INTO authority_log(event_type, checkpoint) "
                "VALUES ('RECOVERY_FENCE', NULL)",
                "INSERT OR REPLACE INTO authority_log(event_type, checkpoint) "
                "VALUES ('RECOVERY_FENCE', NULL)",
                "INSERT INTO authority_log(ordinal, event_type, checkpoint) "
                "VALUES (0, 'CHECKPOINT_INTENT', 99)",
                "INSERT OR REPLACE INTO recovery_fence "
                "SELECT singleton, authority_ordinal FROM recovery_fence",
                "UPDATE recovery_fence SET authority_ordinal = 1",
                "UPDATE authority_log SET checkpoint = 99 WHERE ordinal = 1",
                "INSERT OR REPLACE INTO checkpoint_intents "
                "SELECT * FROM checkpoint_intents WHERE checkpoint = 1",
                "UPDATE checkpoint_intents SET snapshot_state = 'rewritten' "
                "WHERE checkpoint = 1",
                "UPDATE checkpoint_cuts SET cut_sequence = 3 "
                "WHERE checkpoint = 1 AND input_id = 'left'",
                "UPDATE barrier_witnesses SET cut_sequence = 3 "
                "WHERE checkpoint = 1 AND input_id = 'left'",
                "UPDATE checkpoint_seals SET intent_digest = 'rewritten' "
                "WHERE checkpoint = 1",
                "INSERT OR REPLACE INTO checkpoint_seals "
                "SELECT * FROM checkpoint_seals WHERE checkpoint = 1",
                "INSERT OR REPLACE INTO checkpoint_completions "
                "SELECT * FROM checkpoint_completions WHERE checkpoint = 1",
                "DELETE FROM checkpoint_completions WHERE checkpoint = 1",
            )
            for statement in statements:
                with self.subTest(statement=statement):
                    with self.assertRaises(sqlite3.IntegrityError):
                        self.sql(path, statement)

            store = StateRecoveryStore(path)
            with self.assertRaises(sqlite3.IntegrityError):
                store.complete_checkpoint(2)
            store.close()
            self.oracle(path, True)

    def test_empty_checkpoint_tail_requires_no_retained_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "empty-tail.sqlite"
            store = StateRecoveryStore(path)
            requirements = StateRequirements(
                True,
                ("left", "right"),
                False,
                False,
                shared_across_partitions=True,
            )
            store.set_contract(
                StateContract(
                    StateClass.CHECKPOINT_MANAGED_SHARED,
                    ("left", "right"),
                    "shared/v1",
                    "code-v1",
                    "checkpoint",
                    True,
                    True,
                ),
                requirements,
            )
            store.add_checkpoint(
                1,
                "S",
                {"left": 1, "right": 1},
                state_schema="shared/v1",
                code_version="code-v1",
                completed=True,
            )
            store.seal_recovery_boundary()
            store.set_target("left", 1, retained_from=999)
            store.set_target("right", 1, retained_from=999)
            store.set_failure_free_reference("S")
            store.close()
            report = self.oracle(path, True)
            self.assertEqual(report["plan"]["replay_order"], [])

    def test_checkpoint_completion_must_follow_its_intent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "completion-before-intent.sqlite"
            store = StateRecoveryStore(path)
            requirements = StateRequirements(
                True,
                ("left", "right"),
                False,
                False,
                shared_across_partitions=True,
            )
            store.set_contract(
                StateContract(
                    StateClass.CHECKPOINT_MANAGED_SHARED,
                    ("left", "right"),
                    "shared/v1",
                    "code-v1",
                    "checkpoint",
                    True,
                    True,
                ),
                requirements,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                store.connection.execute(
                    "INSERT INTO authority_log(ordinal, event_type, checkpoint) "
                    "VALUES (1, 'CHECKPOINT_COMPLETION', 1)"
                )
            store.connection.execute("DROP TRIGGER completion_event_requires_seal")
            store.connection.execute(
                "INSERT INTO authority_log(ordinal, event_type, checkpoint) "
                "VALUES (1, 'CHECKPOINT_COMPLETION', 1)"
            )
            store.connection.commit()
            store.add_checkpoint(
                1,
                "S",
                {"left": 1, "right": 1},
                state_schema="shared/v1",
                code_version="code-v1",
                completed=False,
            )
            store.seal_recovery_boundary()
            store.set_target("left", 1, retained_from=999)
            store.set_target("right", 1, retained_from=999)
            store.set_failure_free_reference("S")
            store.close()

            insert_early_completion = (
                "INSERT INTO checkpoint_completions "
                "SELECT checkpoint, intent_digest, 1 FROM checkpoint_intents "
                "WHERE checkpoint = 1"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                self.sql(path, insert_early_completion)

            self.corrupt_sql(
                path,
                (
                    "checkpoint_completion_valid_insert",
                    "checkpoint_completion_before_recovery",
                ),
                insert_early_completion,
            )
            report = self.oracle(path, False)
            self.assertIn(
                "checkpoint_completion_order_invalid:1", report["violations"]
            )

    def test_seal_and_completion_cannot_be_published_after_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "late-publication.sqlite"
            store = StateRecoveryStore(path)
            requirements = StateRequirements(
                True,
                ("left", "right"),
                False,
                False,
                shared_across_partitions=True,
            )
            store.set_contract(
                StateContract(
                    StateClass.CHECKPOINT_MANAGED_SHARED,
                    ("left", "right"),
                    "shared/v1",
                    "code-v1",
                    "checkpoint",
                    True,
                    True,
                ),
                requirements,
            )
            authorization_ordinal = store._append_authority_event(
                "CHECKPOINT_INTENT", 1
            )
            intent = {
                "checkpoint": 1,
                "authorization_ordinal": authorization_ordinal,
                "inputs": ["left", "right"],
                "cuts": {"left": 1, "right": 1},
                "snapshot_digest": hashlib.sha256(b"S").hexdigest(),
                "state_schema": "shared/v1",
                "code_version": "code-v1",
            }
            intent_digest = digest(intent)
            store.connection.execute(
                "INSERT INTO checkpoint_intents VALUES (?, ?, ?, ?, ?, ?)",
                (1, intent_digest, authorization_ordinal, "S", "shared/v1", "code-v1"),
            )
            for input_id in ("left", "right"):
                store.connection.execute(
                    "INSERT INTO checkpoint_cuts VALUES (1, ?, 1)", (input_id,)
                )
                store.connection.execute(
                    "INSERT INTO barrier_witnesses VALUES (1, ?, 1, 2, ?)",
                    (input_id, intent_digest),
                )
            store.connection.execute("DROP TRIGGER completion_event_requires_seal")
            completion_ordinal = store._append_authority_event(
                "CHECKPOINT_COMPLETION", 1
            )
            store.connection.commit()
            store.seal_recovery_boundary()
            with self.assertRaises(sqlite3.IntegrityError):
                store.connection.execute(
                    "INSERT INTO checkpoint_seals VALUES (?, ?, ?)",
                    (1, intent_digest, authorization_ordinal),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                store.connection.execute(
                    "INSERT INTO checkpoint_completions VALUES (?, ?, ?)",
                    (1, intent_digest, completion_ordinal),
                )
            store.close()

    @staticmethod
    def local_requirements() -> StateRequirements:
        return StateRequirements(True, ("input:p0",), True, True)

    @classmethod
    def seed_local(cls, path: Path, through: int) -> None:
        store = StateRecoveryStore(path)
        store.set_contract(
            StateContract(
                StateClass.PARTITION_LOCAL_PER_TURN,
                ("input:p0",),
                "counter/v1",
                "code-v1",
                "turn_completion",
                True,
                True,
            ),
            cls.local_requirements(),
        )
        for sequence in range(1, through + 1):
            store.complete_local(
                sequence,
                {"signed": sequence},
                f"signed-{sequence}",
                "counter/v1",
                "code-v1",
            )
        store.close()

    @staticmethod
    def seed_shared(path: Path) -> None:
        store = StateRecoveryStore(path)
        requirements = StateRequirements(
            True,
            ("left", "right"),
            False,
            False,
            shared_across_partitions=True,
        )
        store.set_contract(
            StateContract(
                StateClass.CHECKPOINT_MANAGED_SHARED,
                ("left", "right"),
                "shared/v1",
                "code-v1",
                "checkpoint",
                True,
                True,
            ),
            requirements,
        )
        store.add_checkpoint(
            1,
            "S",
            {"left": 1, "right": 1},
            state_schema="shared/v1",
            code_version="code-v1",
            completed=True,
        )
        store.add_checkpoint(
            2,
            "incomplete-must-not-be-selected",
            {"left": 2, "right": 1},
            state_schema="shared/v1",
            code_version="code-v1",
            completed=False,
        )
        store.seal_recovery_boundary()
        store.set_target("left", 3)
        store.set_target("right", 2)
        events = (
            (3, "left", 2, "A"),
            (4, "right", 2, "B"),
            (5, "left", 3, "C"),
        )
        reference = "S"
        for ordinal, input_id, sequence, value in events:
            store.add_mailbox_event(ordinal, input_id, sequence, value)
            reference = apply_order_sensitive(reference, input_id, sequence, value)
        store.set_failure_free_reference(reference)
        store.close()

    @staticmethod
    def sql(path: Path, *statements: str) -> None:
        connection = sqlite3.connect(path)
        with connection:
            for statement in statements:
                connection.execute(statement)
        connection.close()

    @staticmethod
    def corrupt_sql(
        path: Path, trigger: str | tuple[str, ...], *statements: str
    ) -> None:
        connection = sqlite3.connect(path)
        with connection:
            triggers = (trigger,) if isinstance(trigger, str) else trigger
            for trigger_name in triggers:
                connection.execute(f"DROP TRIGGER {trigger_name}")
            for statement in statements:
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
        expected_returncode = 0 if expect_pass else 2
        if result.returncode != expected_returncode:
            raise AssertionError(
                f"oracle rc={result.returncode}, stdout={result.stdout}, "
                f"stderr={result.stderr}"
            )
        return json.loads(result.stdout)


if __name__ == "__main__":
    unittest.main()
