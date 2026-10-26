from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from protocols import (
    DurableGraphStore,
    InjectedCrash,
    JoinSumOperator,
    MapOperator,
    MultiSinkCoordinator,
    PreparedSink,
    ProtocolConflict,
)


class SelectiveRecoveryPrototypeTests(unittest.TestCase):
    def test_failed_branch_and_tail_restore_from_completed_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = DurableGraphStore(Path(directory) / "graph.sqlite")
            branch_b = MapOperator("B", "a-to-b", "b-to-join", 10)
            branch_c = MapOperator("C", "a-to-c", "c-to-join", 100)
            join = JoinSumOperator("b-to-join", "c-to-join")

            store.append("a-to-b", "event-1", 1, 1)
            store.append("a-to-c", "event-1", 1, 1)
            branch_b.run_available(store)
            branch_c.run_available(store)
            join.run_available(store)
            self.assertEqual(join.total, 110)

            checkpoint_id = 1
            store.start_checkpoint(checkpoint_id, {"B", "C", "Join"})
            store.save_snapshot(checkpoint_id, "B", branch_b.snapshot())
            store.save_snapshot(checkpoint_id, "C", branch_c.snapshot())
            store.save_snapshot(checkpoint_id, "Join", join.snapshot())
            self.assertTrue(store.complete_checkpoint(checkpoint_id))

            store.append("a-to-b", "event-2", 2, 2)
            store.append("a-to-c", "event-2", 2, 2)
            branch_b.run_available(store)
            branch_c.run_available(store)
            join.run_available(store)
            self.assertEqual(join.total, 330)

            # B and the dependent tail fail. C remains live. Restore only B and
            # Join, then replay durable messages after checkpoint 1.
            restored_b = MapOperator("B", "a-to-b", "b-to-join", 10)
            restored_join = JoinSumOperator("b-to-join", "c-to-join")
            restored_b.restore(store.load_snapshot(checkpoint_id, "B"))
            restored_join.restore(store.load_snapshot(checkpoint_id, "Join"))
            restored_b.run_available(store)
            restored_join.run_available(store)

            self.assertEqual(restored_join.total, 330)
            self.assertEqual(restored_b.execution_count, 1)
            self.assertEqual(branch_c.cursor, 2, "healthy branch must not roll back")
            self.assertEqual(len(store.read_after("b-to-join", 0)), 2)

    def test_incomplete_barrier_cannot_replace_completed_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = DurableGraphStore(Path(directory) / "graph.sqlite")
            store.start_checkpoint(1, {"B", "C", "Join"})
            for node in ("B", "C", "Join"):
                store.save_snapshot(1, node, {"cursor": 1})
            self.assertTrue(store.complete_checkpoint(1))

            # Upstream participants reached barrier 2, but the join did not.
            store.start_checkpoint(2, {"B", "C", "Join"})
            store.save_snapshot(2, "B", {"cursor": 2})
            store.save_snapshot(2, "C", {"cursor": 2})
            self.assertFalse(store.complete_checkpoint(2))
            self.assertEqual(store.latest_checkpoint(), 1)

    def test_replay_identity_conflict_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = DurableGraphStore(Path(directory) / "graph.sqlite")
            store.append("edge", "event-1", 1, 10)
            store.append("edge", "event-1", 1, 10)
            with self.assertRaises(ProtocolConflict):
                store.append("edge", "event-1", 1, 999)

    def test_non_contiguous_edge_sequence_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = DurableGraphStore(Path(directory) / "graph.sqlite")
            with self.assertRaises(ProtocolConflict):
                store.append("edge", "event-3", 3, 30)

    def test_checkpoint_manifest_and_snapshot_are_immutable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = DurableGraphStore(Path(directory) / "graph.sqlite")
            store.start_checkpoint(1, {"B", "C", "Join"})
            for node in ("B", "C", "Join"):
                store.save_snapshot(1, node, {"cursor": 1})
            self.assertTrue(store.complete_checkpoint(1))

            with self.assertRaises(ProtocolConflict):
                store.start_checkpoint(1, {"B", "C"})
            with self.assertRaises(ProtocolConflict):
                store.save_snapshot(1, "B", {"cursor": 999})


class MultiSinkProtocolPrototypeTests(unittest.TestCase):
    def test_durable_commit_decision_converges_after_coordinator_crash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sinks = [
                PreparedSink(root / "sink-1.sqlite", "sink-1"),
                PreparedSink(root / "sink-2.sqlite", "sink-2"),
            ]
            coordinator_path = root / "coordinator.sqlite"
            coordinator = MultiSinkCoordinator(coordinator_path, sinks)

            with self.assertRaises(InjectedCrash):
                coordinator.write("checkpoint-7", "result", crash_after_sink=1)

            self.assertEqual(sinks[0].visible_values(), [("checkpoint-7", "result")])
            self.assertEqual(sinks[1].visible_values(), [])

            recovered = MultiSinkCoordinator(coordinator_path, sinks)
            recovered.recover()
            recovered.recover()  # recovery itself is idempotent

            expected = [("checkpoint-7", "result")]
            self.assertEqual(sinks[0].visible_values(), expected)
            self.assertEqual(sinks[1].visible_values(), expected)

    def test_transaction_manifest_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sink_1 = PreparedSink(root / "sink-1.sqlite", "sink-1")
            sink_2 = PreparedSink(root / "sink-2.sqlite", "sink-2")
            coordinator_path = root / "coordinator.sqlite"
            coordinator = MultiSinkCoordinator(coordinator_path, [sink_1])
            coordinator.write("checkpoint-7", "value-1")

            changed_value = MultiSinkCoordinator(coordinator_path, [sink_1])
            with self.assertRaises(ProtocolConflict):
                changed_value.write("checkpoint-7", "value-2")

            changed_participants = MultiSinkCoordinator(
                coordinator_path, [sink_1, sink_2]
            )
            with self.assertRaises(ProtocolConflict):
                changed_participants.write("checkpoint-7", "value-1")


if __name__ == "__main__":
    unittest.main()
