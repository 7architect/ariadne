from __future__ import annotations

import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from durable_stream_barrier_crash_worker import EXIT_CODE
from durable_stream_barrier_v2 import ComposedDurableStream, EntryKind


HERE = Path(__file__).resolve().parent
WORKER = HERE / "durable_stream_barrier_crash_worker.py"
RECOVERY_WORKER = HERE / "durable_stream_barrier_recovery_worker.py"
ORACLE = HERE / "durable_stream_barrier_oracle.py"

MATRIX = {
    "inject": (
        "barrier:before_insert",
        "barrier:after_entry_before_partition",
        "barrier:after_partition_before_commit",
        "inject:after_durable_before_ack",
        "inject:after_ack",
    ),
    "observe_first": (
        "observe:before_observation",
        "observe:after_observation_before_progress",
        "observe:after_progress_before_alignment",
        "observe_first:after_durable_before_ack",
        "observe_first:after_ack",
    ),
    "align_final": (
        "observe:after_observation_before_progress",
        "observe:after_progress_before_alignment",
        "align:after_snapshot_before_output",
        "align:after_output_before_status",
        "align:after_status_before_unblock",
        "align:after_unblock_before_commit",
        "align_final:after_durable_before_ack",
        "align_final:after_ack",
    ),
    "complete": (
        "complete:before_update",
        "complete:after_update_before_commit",
        "complete:after_durable_before_ack",
        "complete:after_ack",
    ),
    "fence": (
        "fence:after_authority_before_fence",
        "fence:after_insert_before_commit",
        "fence:after_durable_before_ack",
        "fence:after_ack",
    ),
    "recover": (
        "recover:after_restoring_before_validation",
        "recover:after_validation_before_restore",
        "recover:after_snapshot_restore",
        "recover:after_first_tail",
        "recover:after_final_tail",
        "recover:after_ready_before_commit",
        "recover:after_durable_before_ack",
        "recover:after_ack",
    ),
}


class DurableStreamBarrierSubprocessTests(unittest.TestCase):
    def test_named_crash_matrix_reopens_and_converges_without_sleeps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bases = {}
            for scenario in MATRIX:
                base = root / f"base-{scenario}.sqlite"
                self.seed(base, scenario)
                bases[scenario] = base

            for scenario, points in MATRIX.items():
                for index, point in enumerate(points):
                    with self.subTest(scenario=scenario, point=point):
                        path = root / f"case-{scenario}-{index}.sqlite"
                        ack = root / f"ack-{scenario}-{index}"
                        shutil.copy2(bases[scenario], path)
                        self.crash(path, ack, scenario, point)
                        self.assert_integrity(path)
                        self.assert_boundary(path, ack, scenario, point)
                        recovery = self.converge(path, scenario)
                        if scenario == "recover":
                            self.assertEqual(recovery["replayed_through_ordinal"], 6)
                            self.assertEqual(recovery["post_recovery_generation"], 1)
                        self.assert_integrity(path)
                        self.oracle(path)

    def seed(self, path: Path, scenario: str) -> None:
        if scenario == "inject":
            store = self.store(path, ("left",))
            self.append(store, "left", 1)
            store.start_checkpoint(1, ("left",), "coord", 1)
        elif scenario == "observe_first":
            store = self.store(path, ("left", "right"))
            self.append(store, "left", 1)
            self.append(store, "right", 1)
            self.append(store, "right", 2)
            intent = store.start_checkpoint(1, ("left", "right"), "coord", 1)
            store.inject_barrier(1, "left", "coord", 1, intent)
            store.inject_barrier(1, "right", "coord", 1, intent)
            store.process_next("left")
            store.process_next("right")
        elif scenario in {"align_final", "complete", "fence", "recover"}:
            store = self.seed_join(path)
            if scenario in {"complete", "fence", "recover"}:
                store.process_next("right")
            if scenario in {"fence", "recover"}:
                store.process_next("left")
                store.process_next("right")
                store.complete_checkpoint(1)
            if scenario == "recover":
                store.begin_recovery()
        else:
            raise AssertionError(scenario)
        store.close()

    def seed_join(self, path: Path) -> ComposedDurableStream:
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
        store.process_next("right")
        return store

    def assert_boundary(
        self, path: Path, ack: Path, scenario: str, point: str
    ) -> None:
        postcommit = point in {
            f"{scenario}:after_durable_before_ack",
            f"{scenario}:after_ack",
        }
        self.assertEqual(ack.exists(), point.endswith("after_ack"))
        connection = sqlite3.connect(path)
        connection.row_factory = sqlite3.Row
        if scenario == "inject":
            count = connection.execute(
                "SELECT COUNT(*) FROM stream_entries WHERE kind = 'BARRIER'"
            ).fetchone()[0]
            partition = connection.execute(
                "SELECT next_log_sequence, last_checkpoint FROM partitions "
                "WHERE input_id = 'left'"
            ).fetchone()
            self.assertEqual(count, int(postcommit))
            self.assertEqual(
                (partition["next_log_sequence"], partition["last_checkpoint"]),
                (3, 1) if postcommit else (2, 0),
            )
        elif scenario == "observe_first":
            count = connection.execute(
                "SELECT COUNT(*) FROM barrier_observations WHERE input_id = 'left'"
            ).fetchone()[0]
            progress = connection.execute(
                "SELECT * FROM operator_progress WHERE input_id = 'left'"
            ).fetchone()
            self.assertEqual(count, int(postcommit))
            self.assertEqual(progress["blocked_checkpoint"], 1 if postcommit else None)
            self.assertEqual(progress["scanned_log_through"], 2 if postcommit else 1)
        elif scenario == "align_final":
            observations = connection.execute(
                "SELECT COUNT(*) FROM barrier_observations"
            ).fetchone()[0]
            snapshots = connection.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
            barriers = connection.execute(
                "SELECT COUNT(*) FROM output_events WHERE kind = 'BARRIER'"
            ).fetchone()[0]
            status = connection.execute(
                "SELECT status FROM checkpoint_status WHERE checkpoint = 1"
            ).fetchone()[0]
            blocked = connection.execute(
                "SELECT COUNT(*) FROM operator_progress "
                "WHERE blocked_checkpoint IS NOT NULL"
            ).fetchone()[0]
            self.assertEqual(observations, 2 if postcommit else 1)
            self.assertEqual((snapshots, barriers), (1, 1) if postcommit else (0, 0))
            self.assertEqual(status, "ALIGNED" if postcommit else "STARTED")
            self.assertEqual(blocked, 0 if postcommit else 1)
        elif scenario == "complete":
            status = connection.execute(
                "SELECT status FROM checkpoint_status WHERE checkpoint = 1"
            ).fetchone()[0]
            self.assertEqual(status, "COMPLETED" if postcommit else "ALIGNED")
        elif scenario == "fence":
            fences = connection.execute("SELECT COUNT(*) FROM recovery_fence").fetchone()[0]
            authority = connection.execute("SELECT COUNT(*) FROM authority_log").fetchone()[0]
            self.assertEqual(fences, int(postcommit))
            self.assertEqual(authority, 2 if postcommit else 1)
        elif scenario == "recover":
            runs = connection.execute("SELECT COUNT(*) FROM recovery_runs").fetchone()[0]
            self.assertEqual(runs, int(postcommit))
            if postcommit:
                row = connection.execute("SELECT * FROM recovery_runs").fetchone()
                self.assertEqual((row["phase"], row["replayed_through_ordinal"]), ("READY", 6))
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM output_events").fetchone()[0], 7)
        connection.close()

    def converge(self, path: Path, scenario: str) -> dict[str, object]:
        result = subprocess.run(
            [
                sys.executable,
                str(RECOVERY_WORKER),
                "--db",
                str(path),
                "--scenario",
                scenario,
            ],
            cwd=HERE,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout={result.stdout!r}\nstderr={result.stderr!r}",
        )
        return json.loads(result.stdout)

    def crash(self, path: Path, ack: Path, scenario: str, point: str) -> None:
        result = subprocess.run(
            [
                sys.executable,
                str(WORKER),
                "--db",
                str(path),
                "--ack",
                str(ack),
                "--scenario",
                scenario,
                "--point",
                point,
            ],
            cwd=HERE,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            EXIT_CODE,
            msg=f"stdout={result.stdout!r}\nstderr={result.stderr!r}",
        )

    def oracle(self, path: Path) -> None:
        result = subprocess.run(
            [sys.executable, str(ORACLE), str(path)],
            cwd=HERE,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            result.returncode,
            0,
            msg=f"stdout={result.stdout!r}\nstderr={result.stderr!r}",
        )
        self.assertTrue(json.loads(result.stdout)["passed"])

    def assert_integrity(self, path: Path) -> None:
        connection = sqlite3.connect(path)
        self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        connection.close()

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


if __name__ == "__main__":
    unittest.main()
