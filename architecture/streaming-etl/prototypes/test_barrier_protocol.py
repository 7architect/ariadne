from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from barrier_protocol import (
    AlignedJoin,
    BarrierProtocolError,
    ChannelRecord,
    DurableChannel,
    GraphEdge,
    RecordKind,
    plan_recovery,
)


class DurableChannelTests(unittest.TestCase):
    def test_ordered_data_and_barriers_survive_reopen(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "channel.sqlite"
            channel = DurableChannel(path)
            channel.claim_partition("edge", "p0", "worker-1", 1)
            channel.append_data(
                "edge", "p0", 1, "r1", 10, {"source": "offset-1"}, "worker-1", 1
            )
            channel.append_barrier("edge", "p0", 2, 1, "worker-1", 1)

            reopened = DurableChannel(path)
            records = reopened.read_after("edge", "p0", 0)
            self.assertEqual(
                [record.kind for record in records],
                [RecordKind.DATA, RecordKind.BARRIER],
            )
            self.assertEqual(records[1].checkpoint, 1)

    def test_partition_takeover_fences_old_writer_and_rejects_gaps(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = DurableChannel(Path(directory) / "channel.sqlite")
            channel.claim_partition("edge", "p0", "old", 1)
            channel.advance_clock(10)
            channel.claim_partition("edge", "p0", "new", 2)
            with self.assertRaisesRegex(BarrierProtocolError, "stale"):
                channel.append_data("edge", "p0", 1, "r1", 1, {"offset": 1}, "old", 1)
            with self.assertRaisesRegex(BarrierProtocolError, "non-contiguous"):
                channel.append_data("edge", "p0", 2, "r2", 2, {"offset": 2}, "new", 2)

    def test_partition_lease_renewal_expiry_and_takeover_are_deterministic(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "channel.sqlite"
            channel = DurableChannel(path)
            channel.claim_partition("edge", "p0", "old", 1, lease_ttl=5)
            clock_authority = DurableChannel(path)
            clock_authority.advance_clock(4)
            channel.renew_partition("edge", "p0", "old", 1, lease_ttl=5)
            channel.renew_partition("edge", "p0", "old", 1, lease_ttl=1)
            expiry = channel.connection.execute(
                "SELECT lease_expires_at FROM partition_owners"
            ).fetchone()[0]
            self.assertEqual(expiry, 9)
            clock_authority.advance_clock(8)
            with self.assertRaisesRegex(BarrierProtocolError, "still active"):
                channel.claim_partition("edge", "p0", "new", 2)
            channel.append_data("edge", "p0", 1, "r1", 1, {"offset": 1}, "old", 1)
            clock_authority.advance_clock(9)
            with self.assertRaisesRegex(BarrierProtocolError, "stale"):
                channel.append_data("edge", "p0", 2, "r2", 2, {"offset": 2}, "old", 1)
            with self.assertRaisesRegex(BarrierProtocolError, "expired"):
                channel.renew_partition("edge", "p0", "old", 1)
            with self.assertRaisesRegex(BarrierProtocolError, "backward"):
                channel.advance_clock(0)
            channel.claim_partition("edge", "p0", "new", 2)
            channel.append_data("edge", "p0", 2, "r2", 2, {"offset": 2}, "new", 2)

    def test_idempotent_retry_preserves_lineage_and_barriers_are_monotonic(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            channel = DurableChannel(Path(directory) / "channel.sqlite")
            channel.claim_partition("edge", "p0", "worker", 1)
            channel.append_data("edge", "p0", 1, "r1", 10, {"offset": 1}, "worker", 1)
            with self.assertRaisesRegex(BarrierProtocolError, "claim conflict"):
                channel.append_data(
                    "edge", "p0", 1, "r1", 10, {"offset": 999}, "worker", 1
                )
            channel.append_barrier("edge", "p0", 2, 2, "worker", 1)
            with self.assertRaisesRegex(BarrierProtocolError, "must increase"):
                channel.append_barrier("edge", "p0", 3, 1, "worker", 1)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "retention protocol"),
                channel.connection,
            ):
                channel.connection.execute("DELETE FROM channel_records")

    def test_abrupt_process_exit_preserves_committed_append(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "channel.sqlite"
            module_dir = Path(__file__).parent
            program = f"""
import os
from pathlib import Path
from barrier_protocol import DurableChannel
c = DurableChannel(Path({str(path)!r}))
c.claim_partition('edge', 'p0', 'worker', 1)
c.append_data('edge', 'p0', 1, 'r1', 7, {{'offset': 1}}, 'worker', 1)
os._exit(17)
"""
            result = subprocess.run(
                [sys.executable, "-c", program], cwd=module_dir, check=False
            )
            self.assertEqual(result.returncode, 17)
            records = DurableChannel(path).read_after("edge", "p0", 0)
            self.assertEqual(
                [(record.record_id, record.payload) for record in records], [("r1", 7)]
            )


class AlignedJoinTests(unittest.TestCase):
    def test_early_input_is_buffered_until_all_barriers_arrive(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "join.sqlite"
            left_channel = DurableChannel(Path(directory) / "left.sqlite")
            right_channel = DurableChannel(Path(directory) / "right.sqlite")
            for channel, edge in ((left_channel, "left"), (right_channel, "right")):
                channel.claim_partition(edge, "p0", "source", 1)
            left_channel.append_data(
                "left", "p0", 1, "l1", 2, {"offset": 1}, "source", 1
            )
            left_channel.append_barrier("left", "p0", 2, 1, "source", 1)
            left_channel.append_data(
                "left", "p0", 3, "l2", 100, {"offset": 2}, "source", 1
            )
            right_channel.append_data(
                "right", "p0", 1, "r1", 3, {"offset": 1}, "source", 1
            )
            right_channel.append_barrier("right", "p0", 2, 1, "source", 1)

            bindings = {"left": ("left", "p0"), "right": ("right", "p0")}
            join = AlignedJoin(path, "join", bindings)
            join.claim("worker", 1)
            left = left_channel.read_after("left", "p0", 0)
            right = right_channel.read_after("right", "p0", 0)
            join.ingest("left", left[0], "worker", 1)
            join.ingest("left", left[1], "worker", 1)
            join.ingest("left", left[2], "worker", 1)
            join.ingest("right", right[0], "worker", 1)
            self.assertEqual(join.state_value, 5)
            completed = join.ingest("right", right[1], "worker", 1)
            self.assertEqual(completed, 1)
            self.assertEqual(join.snapshot(1), (5, {"left": 2, "right": 2}))
            self.assertEqual(join.state_value, 105)
            join.ingest(
                "left",
                ChannelRecord(
                    "left", "p0", 4, RecordKind.BARRIER, "barrier:2", checkpoint=2
                ),
                "worker",
                1,
            )
            join.ingest(
                "right",
                ChannelRecord(
                    "right", "p0", 3, RecordKind.BARRIER, "barrier:2", checkpoint=2
                ),
                "worker",
                1,
            )
            snapshot_two = join.snapshot(2)

            reopened = AlignedJoin(path, "join", bindings)
            reopened.advance_clock(10)
            reopened.claim("replacement", 2)
            reopened.restore(1, "replacement", 2)
            self.assertEqual(reopened.state_value, 5)
            self.assertEqual(reopened.snapshot(2), snapshot_two)
            with self.assertRaisesRegex(BarrierProtocolError, "divergent"):
                reopened.ingest(
                    "left",
                    ChannelRecord(
                        "left", "p0", 3, RecordKind.DATA, "replacement-l2", 100
                    ),
                    "replacement",
                    2,
                )
            reopened.ingest("left", left[2], "replacement", 2)
            reopened.ingest(
                "left",
                ChannelRecord(
                    "left", "p0", 4, RecordKind.BARRIER, "barrier:2", checkpoint=2
                ),
                "replacement",
                2,
            )
            reopened.ingest(
                "right",
                ChannelRecord(
                    "right", "p0", 3, RecordKind.BARRIER, "barrier:2", checkpoint=2
                ),
                "replacement",
                2,
            )
            self.assertEqual(reopened.snapshot(2), snapshot_two)
            with self.assertRaisesRegex(BarrierProtocolError, "stale"):
                join.restore(1, "worker", 1)

    def test_join_lease_renewal_expiry_and_takeover_fence_mutations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "join.sqlite"
            join = AlignedJoin(
                path,
                "join",
                {"left": ("left", "p0"), "right": ("right", "p0")},
            )
            join.claim("old", 1, lease_ttl=5)
            clock_authority = AlignedJoin(
                path,
                "join",
                {"left": ("left", "p0"), "right": ("right", "p0")},
            )
            clock_authority.advance_clock(4)
            join.renew("old", 1, lease_ttl=5)
            join.renew("old", 1, lease_ttl=1)
            expiry = join.connection.execute(
                "SELECT lease_expires_at FROM join_owners"
            ).fetchone()[0]
            self.assertEqual(expiry, 9)
            clock_authority.advance_clock(8)
            with self.assertRaisesRegex(BarrierProtocolError, "still active"):
                join.claim("new", 2)
            clock_authority.advance_clock(9)
            with self.assertRaisesRegex(BarrierProtocolError, "stale"):
                join.ingest(
                    "left",
                    ChannelRecord("left", "p0", 1, RecordKind.DATA, "l1", 1),
                    "old",
                    1,
                )
            with self.assertRaisesRegex(BarrierProtocolError, "expired"):
                join.renew("old", 1)
            with self.assertRaisesRegex(BarrierProtocolError, "backward"):
                join.advance_clock(0)
            join.claim("new", 2)
            join.ingest(
                "left",
                ChannelRecord("left", "p0", 1, RecordKind.DATA, "l1", 1),
                "new",
                2,
            )
            with self.assertRaisesRegex(BarrierProtocolError, "stale"):
                join.ingest(
                    "left",
                    ChannelRecord("left", "p0", 2, RecordKind.DATA, "l2", 2),
                    "old",
                    1,
                )

    def test_input_binding_replay_identity_and_checkpoint_order_are_enforced(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            join = AlignedJoin(
                Path(directory) / "join.sqlite",
                "join",
                {"left": ("left", "p0"), "right": ("right", "p0")},
            )
            join.claim("worker", 1)
            wrong_edge = ChannelRecord("right", "p0", 1, RecordKind.DATA, "wrong", 7)
            with self.assertRaisesRegex(BarrierProtocolError, "input binding"):
                join.ingest("left", wrong_edge, "worker", 1)

            first = ChannelRecord("left", "p0", 1, RecordKind.DATA, "left-1", 2)
            join.ingest("left", first, "worker", 1)
            divergent = ChannelRecord(
                "left", "p0", 1, RecordKind.DATA, "left-conflict", 99
            )
            with self.assertRaisesRegex(BarrierProtocolError, "divergent"):
                join.ingest("left", divergent, "worker", 1)

            join.ingest(
                "left",
                ChannelRecord(
                    "left", "p0", 2, RecordKind.BARRIER, "barrier:2", checkpoint=2
                ),
                "worker",
                1,
            )
            join.ingest(
                "right",
                ChannelRecord(
                    "right", "p0", 1, RecordKind.BARRIER, "barrier:2", checkpoint=2
                ),
                "worker",
                1,
            )
            with self.assertRaisesRegex(BarrierProtocolError, "must increase"):
                join.ingest(
                    "left",
                    ChannelRecord(
                        "left",
                        "p0",
                        3,
                        RecordKind.BARRIER,
                        "barrier:1",
                        checkpoint=1,
                    ),
                    "worker",
                    1,
                )


class RecoveryPlannerTests(unittest.TestCase):
    def test_failed_branch_keeps_healthy_branch_outside_recovery_region(self) -> None:
        nodes = {"A", "B", "C", "Join", "Sink"}
        edges = (
            GraphEdge("A", "B", "A-B", True),
            GraphEdge("A", "C", "A-C", True),
            GraphEdge("B", "Join", "B-Join", True),
            GraphEdge("C", "Join", "C-Join", True),
            GraphEdge("Join", "Sink", "Join-Sink", True),
        )
        plan = plan_recovery(nodes, edges, "B", 1, {1: "manifest-1"})
        self.assertEqual(plan.region, frozenset({"B", "Join", "Sink"}))
        self.assertEqual({cut.edge_id for cut in plan.replay_cuts}, {"A-B", "C-Join"})
        self.assertTrue(all(cut.checkpoint == 1 for cut in plan.replay_cuts))
        self.assertTrue(all(cut.partition_positions for cut in plan.replay_cuts))

    def test_missing_replay_cut_expands_region_upstream(self) -> None:
        nodes = {"A", "B", "C", "Join", "Sink"}
        edges = (
            GraphEdge("A", "B", "A-B", True),
            GraphEdge("A", "C", "A-C", True),
            GraphEdge("B", "Join", "B-Join", True),
            GraphEdge("C", "Join", "C-Join", False),
            GraphEdge("Join", "Sink", "Join-Sink", True),
        )
        plan = plan_recovery(nodes, edges, "B", 1, {1: "manifest-1"})
        self.assertEqual(plan.region, frozenset({"B", "C", "Join", "Sink"}))
        self.assertEqual({cut.edge_id for cut in plan.replay_cuts}, {"A-B", "A-C"})
        self.assertIn("unavailable replay edge C-Join", plan.reasons["C"])

    def test_invalid_topology_is_rejected_and_unavailable_checkpoint_expands(
        self,
    ) -> None:
        with self.assertRaisesRegex(BarrierProtocolError, "not completed"):
            plan_recovery(
                {"A", "B"},
                (GraphEdge("A", "B", "A-B", True),),
                "B",
                2,
                {1: "manifest-1"},
            )
        with self.assertRaisesRegex(BarrierProtocolError, "endpoint"):
            plan_recovery(
                {"A", "B"},
                (GraphEdge("B", "Ghost", "B-Ghost", True),),
                "B",
                1,
                {1: "manifest-1"},
            )
        plan = plan_recovery(
            {"A", "B"},
            (
                GraphEdge(
                    "A",
                    "B",
                    "A-B",
                    True,
                    partitions=frozenset({"p0"}),
                    checkpoint_positions=((1, (("p0", "offset:10"),)),),
                ),
            ),
            "B",
            2,
            {2: "manifest-2"},
        )
        self.assertEqual(plan.checkpoint, 2)
        self.assertEqual(plan.checkpoint_manifest_digest, "manifest-2")
        self.assertEqual(plan.region, frozenset({"A", "B"}))
        self.assertEqual(plan.replay_cuts, ())

        with self.assertRaisesRegex(BarrierProtocolError, "replay-cut partition"):
            plan_recovery(
                {"A", "B"},
                (
                    GraphEdge(
                        "A",
                        "B",
                        "A-B",
                        True,
                        partitions=frozenset({"p0", "p1"}),
                        checkpoint_positions=((2, (("p0", "offset:10"),)),),
                    ),
                ),
                "B",
                2,
                {2: "manifest-2"},
            )


if __name__ == "__main__":
    unittest.main()
