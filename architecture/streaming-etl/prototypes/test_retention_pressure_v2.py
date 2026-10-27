from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from retention_pressure_v2 import CapacityBlocked, RetentionPressureStore

HERE = Path(__file__).resolve().parent
ORACLE = HERE / "retention_pressure_oracle.py"
WORKER = HERE / "retention_pressure_crash_worker.py"


class RetentionPressureTests(unittest.TestCase):
    def test_reference_graph_protects_suffix_and_shared_object(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "retention.sqlite"
            store = RetentionPressureStore(path, max_records=8, max_bytes=80)
            self.append(store, 1, object_key="shared")
            self.append(store, 2, object_key="shared")
            for sequence in range(3, 7):
                self.append(store, sequence)
            store.create_consumer("anna")
            store.create_consumer("boris")
            store.complete_through("anna", 2)
            self.assertEqual(store.deliver_next("anna"), 3)
            store.complete_through("boris", 5)
            store.seal_checkpoint(1, 1)
            store.add_ref("JOIN_BUFFER", "join-fast-4", 4)
            store.add_ref("BRANCH_OUTPUT", "boris-5", 5)
            store.add_ref("COMPLETION", "anna-2", 2)
            store.add_ref("RECOVERY_PIN", "repair-3", 3)

            first = store.compact(6)
            self.assertEqual(first["safe_through"], 1)
            self.assertEqual(first["records"], [1])
            self.assertEqual(first["objects"], [])
            self.assertIsNotNone(
                store.connection.execute(
                    "SELECT 1 FROM objects WHERE object_key = 'shared'"
                ).fetchone()
            )

            store.release_checkpoint(1)
            for kind, ref_id in (
                ("JOIN_BUFFER", "join-fast-4"),
                ("BRANCH_OUTPUT", "boris-5"),
                ("COMPLETION", "anna-2"),
                ("RECOVERY_PIN", "repair-3"),
            ):
                store.remove_ref(kind, ref_id)
            store.complete_through("anna", 4)
            store.complete_through("boris", 6)
            second = store.compact(6)
            self.assertEqual(second["safe_through"], 4)
            self.assertEqual(second["records"], [2, 3, 4])
            self.assertIn("shared", second["objects"])
            store.close()
            self.oracle(path, True)

    def test_full_block_compact_resume_preserves_sequence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "pressure.sqlite"
            store = RetentionPressureStore(path, max_records=3, max_bytes=30)
            for sequence in range(1, 4):
                self.append(store, sequence)
            with self.assertRaises(CapacityBlocked) as blocked:
                self.append(store, 4)
            self.assertEqual(blocked.exception.view["record_credits"], 0)
            self.assertEqual(
                store.connection.execute(
                    "SELECT next_sequence FROM partition_state"
                ).fetchone()[0],
                4,
            )
            self.assertEqual(
                store.connection.execute(
                    "SELECT COUNT(*) FROM append_attempts WHERE status = 'BLOCKED'"
                ).fetchone()[0],
                1,
            )
            store.create_consumer("anna")
            store.complete_through("anna", 3)
            manifest = store.compact(3)
            self.assertEqual(manifest["records"], [1, 2, 3])
            retried = self.append(store, 4)
            self.assertEqual(retried.sequence, 4)
            self.assertEqual(store.capacity()["record_credits"], 2)
            store.close()
            self.oracle(path, True)

    def test_concurrent_capacity_reservation_allows_only_one_append(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "race.sqlite"
            RetentionPressureStore(path, max_records=1, max_bytes=10).close()
            barrier = threading.Barrier(2)
            outcomes: list[str] = []

            def contender(number: int) -> None:
                store = RetentionPressureStore(path, max_records=1, max_bytes=10)
                barrier.wait()
                try:
                    self.append(store, number)
                except CapacityBlocked:
                    outcomes.append("BLOCKED")
                else:
                    outcomes.append("APPENDED")
                finally:
                    store.close()

            threads = [threading.Thread(target=contender, args=(n,)) for n in (1, 2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertCountEqual(outcomes, ["APPENDED", "BLOCKED"])
            self.oracle(path, True)

    def test_process_crashes_roll_back_or_publish_whole_transition(self) -> None:
        points = (
            ("append", "append:after_attempt_before_capacity", 2, 2, 0),
            ("append", "append:after_reservation_before_record", 2, 2, 0),
            ("append", "append:after_record_before_commit", 2, 2, 0),
            ("append", "append:after_commit_before_ack", 2, 3, 0),
            ("blocked", "append:blocked_after_commit_before_ack", 4, 4, 0),
            ("compact", "compaction:after_manifest_before_delete", 2, 2, 0),
            ("compact", "compaction:after_records_before_objects", 2, 2, 0),
            ("compact", "compaction:after_complete_before_commit", 2, 2, 0),
            ("compact", "compaction:after_commit_before_ack", 2, 0, 1),
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for operation, point, setup_records, records, manifests in points:
                with self.subTest(point=point):
                    path = root / f"{operation}-{point.replace(':', '-')}.sqlite"
                    store = RetentionPressureStore(path, max_records=4, max_bytes=40)
                    for sequence in range(1, setup_records + 1):
                        self.append(store, sequence)
                    store.close()
                    result = subprocess.run(
                        [sys.executable, str(WORKER), str(path), operation, point],
                        cwd=HERE,
                        check=False,
                    )
                    self.assertEqual(result.returncode, 86)
                    check = sqlite3.connect(path)
                    self.assertEqual(
                        check.execute("SELECT COUNT(*) FROM records").fetchone()[0],
                        records,
                    )
                    self.assertEqual(
                        check.execute(
                            "SELECT COUNT(*) FROM compaction_manifests WHERE status = 'COMPLETED'"
                        ).fetchone()[0],
                        manifests,
                    )
                    check.close()
                    reopened = RetentionPressureStore(path, max_records=4, max_bytes=40)
                    if (
                        operation == "append"
                        and point == "append:after_commit_before_ack"
                    ):
                        retried = self.append(reopened, 3)
                        self.assertEqual(retried.sequence, 3)
                        with self.assertRaisesRegex(
                            Exception, "attempt identity drift"
                        ):
                            reopened.append(
                                "attempt-3", "different", "different", b"x", "different"
                            )
                    if operation == "blocked":
                        attempt = reopened.connection.execute(
                            "SELECT status FROM append_attempts WHERE attempt_id = 'attempt-5'"
                        ).fetchone()
                        self.assertEqual(attempt[0], "BLOCKED")
                    reopened.close()
                    self.oracle(path, True)

    def test_independent_oracle_rejects_unsafe_controls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "base.sqlite"
            store = RetentionPressureStore(base, max_records=4, max_bytes=40)
            for sequence in range(1, 5):
                self.append(store, sequence)
            store.close()
            self.oracle(base, True)
            cases = (
                (
                    "gap.sqlite",
                    "DROP TRIGGER record_delete_guard",
                    "DELETE FROM records WHERE sequence = 2",
                    "sequence_coverage",
                ),
                (
                    "capacity.sqlite",
                    None,
                    "UPDATE channel_config SET max_records = 3",
                    "capacity_exceeded",
                ),
            )
            for filename, prelude, statement, expected in cases:
                with self.subTest(filename=filename):
                    path = root / filename
                    shutil.copy2(base, path)
                    db = sqlite3.connect(path)
                    with db:
                        if prelude:
                            db.execute(prelude)
                        db.execute(statement)
                    db.close()
                    report = self.oracle(path, False)
                    self.assertIn(expected, report["violations"])

            manifest_db = root / "manifest.sqlite"
            store = RetentionPressureStore(manifest_db, max_records=4, max_bytes=40)
            self.append(store, 1)
            store.compact(1)
            store.close()
            db = sqlite3.connect(manifest_db)
            with db:
                db.execute("DROP TRIGGER manifest_identity_immutable")
                db.execute("UPDATE compaction_manifests SET safe_through = 99")
            db.close()
            report = self.oracle(manifest_db, False)
            self.assertIn("manifest_digest:1", report["violations"])

            cursor_db = root / "cursor.sqlite"
            store = RetentionPressureStore(cursor_db, max_records=4, max_bytes=40)
            for sequence in range(1, 5):
                self.append(store, sequence)
            store.create_consumer("anna")
            store.close()
            db = sqlite3.connect(cursor_db)
            with db:
                db.execute("DROP TRIGGER consumer_progress_authority")
                db.execute("UPDATE consumers SET completed_through = 4")
            db.close()
            report = self.oracle(cursor_db, False)
            self.assertIn("consumer_completion:anna", report["violations"])

    def test_attempt_identity_lost_ack_and_byte_only_saturation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "attempt.sqlite"
            store = RetentionPressureStore(path, max_records=10, max_bytes=10)
            first = store.append("same", "r1", "o1", b"1234567890", "l1")
            self.assertEqual(first.sequence, 1)
            same = store.append("same", "r1", "o1", b"1234567890", "l1")
            self.assertEqual(same, first)
            with self.assertRaisesRegex(Exception, "attempt identity drift"):
                store.append("same", "r2", "o2", b"x", "l2")
            with self.assertRaises(CapacityBlocked):
                store.append("blocked", "r2", "o2", b"x", "l2")
            with self.assertRaisesRegex(Exception, "attempt identity drift"):
                store.append("blocked", "r3", "o3", b"yyy", "l3")
            self.assertEqual(store.capacity()["record_credits"], 9)
            store.close()
            self.oracle(path, True)

    def test_exact_append_retry_survives_compaction_with_full_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "retry-after-compaction.sqlite"
            store = RetentionPressureStore(path, max_records=2, max_bytes=20)
            original = store.append(
                "stable-attempt",
                "stable-record",
                "stable-object",
                b"payload",
                "stable-lineage",
                schema_id="document/v3",
            )
            store.compact(original.sequence)
            retried = store.append(
                "stable-attempt",
                "stable-record",
                "stable-object",
                b"payload",
                "stable-lineage",
                schema_id="document/v3",
            )
            self.assertEqual(retried, original)
            for changed in (
                (
                    "other-record",
                    "stable-object",
                    b"payload",
                    "stable-lineage",
                    "document/v3",
                ),
                (
                    "stable-record",
                    "other-object",
                    b"payload",
                    "stable-lineage",
                    "document/v3",
                ),
                (
                    "stable-record",
                    "stable-object",
                    b"changed",
                    "stable-lineage",
                    "document/v3",
                ),
                (
                    "stable-record",
                    "stable-object",
                    b"payload",
                    "other-lineage",
                    "document/v3",
                ),
                (
                    "stable-record",
                    "stable-object",
                    b"payload",
                    "stable-lineage",
                    "document/v4",
                ),
            ):
                with (
                    self.subTest(changed=changed),
                    self.assertRaisesRegex(Exception, "attempt identity drift"),
                ):
                    record_id, object_key, payload, lineage, schema = changed
                    store.append(
                        "stable-attempt",
                        record_id,
                        object_key,
                        payload,
                        lineage,
                        schema_id=schema,
                    )
            store.close()
            self.oracle(path, True)

    def test_late_roots_start_at_retained_floor_and_cannot_rewind(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "late-roots.sqlite"
            store = RetentionPressureStore(path, max_records=4, max_bytes=40)
            self.append(store, 1)
            self.append(store, 2)
            store.compact(1)
            self.assertEqual(store.retained_floor(), 2)
            store.create_consumer("late")
            self.assertEqual(store.deliver_next("late"), 2)
            with self.assertRaisesRegex(Exception, "outside retained suffix"):
                store.create_consumer("too-late", start_sequence=1)
            with self.assertRaisesRegex(Exception, "outside retained suffix"):
                store.seal_checkpoint(1, 0)
            with self.assertRaisesRegex(Exception, "outside retained suffix"):
                store.add_ref("RECOVERY_PIN", "too-late", 1)
            store.close()
            self.oracle(path, True)

    def test_replace_cannot_erase_any_authority_or_identity_history(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "replace.sqlite"
            store = RetentionPressureStore(path, max_records=3, max_bytes=30)
            self.append(store, 1)
            store.compact(1)
            self.append(store, 2)
            store.create_consumer("anna")
            store.seal_checkpoint(2, 1)
            store.add_ref("RECOVERY_PIN", "pin-2", 2)
            store.close()
            db = sqlite3.connect(path)
            statements = (
                "INSERT OR REPLACE INTO compaction_manifests VALUES "
                "(1, 0, 0, '[]', '[]', '[]', 'forged', 'COMPLETED')",
                "INSERT OR REPLACE INTO append_attempts VALUES "
                "('attempt-1','other','other','x',1,'bytes/v1','x','x','BLOCKED',NULL)",
                "INSERT OR REPLACE INTO checkpoints VALUES (2, 1, 'forged', 'RELEASED')",
                "INSERT OR REPLACE INTO durable_refs VALUES "
                "('RECOVERY_PIN', 'pin-2', 2, 'RELEASED')",
                "INSERT OR REPLACE INTO consumer_manifest VALUES ('anna', 2)",
                "INSERT OR REPLACE INTO consumers VALUES ('anna', 2, NULL)",
                "INSERT OR REPLACE INTO records VALUES "
                "(2, 'record-2', 'object-2', 'forged', 10, 'forged')",
                "INSERT OR REPLACE INTO objects VALUES "
                "('object-2', 'forged', 10, 'forged/v1')",
            )
            for statement in statements:
                with (
                    self.subTest(statement=statement),
                    self.assertRaises(sqlite3.IntegrityError),
                ):
                    with db:
                        db.execute(statement)
            db.close()
            self.oracle(path, True)

    def test_oracle_detects_root_table_replacement_from_lifecycle_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "forged-root.sqlite"
            store = RetentionPressureStore(path, max_records=2, max_bytes=20)
            self.append(store, 1)
            store.seal_checkpoint(1, 0)
            store.close()
            db = sqlite3.connect(path)
            with db:
                db.execute("DROP TRIGGER checkpoint_create_once")
                db.execute(
                    "INSERT OR REPLACE INTO checkpoints VALUES (1, 0, "
                    "(SELECT intent_digest FROM checkpoints WHERE checkpoint = 1), 'RELEASED')"
                )
            db.close()
            report = self.oracle(path, False)
            self.assertIn("root_lifecycle_authority", report["violations"])

    @staticmethod
    def append(
        store: RetentionPressureStore, sequence: int, *, object_key: str | None = None
    ):
        payload = (
            b"sharedxxxx"
            if object_key == "shared"
            else str(sequence).encode().ljust(10, b"x")
        )
        return store.append(
            f"attempt-{sequence}",
            f"record-{sequence}",
            object_key or f"object-{sequence}",
            payload,
            f"lineage-{sequence}",
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
            raise AssertionError((result.returncode, result.stdout, result.stderr))
        return json.loads(result.stdout)


if __name__ == "__main__":
    unittest.main()
