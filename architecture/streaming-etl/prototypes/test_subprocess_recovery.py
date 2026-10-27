from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from barrier_protocol import AlignedJoin, ChannelRecord, DurableChannel, RecordKind
from integrated_protocol import Coordinator, Oracle
from test_integrated_protocol import ExternalWorld, intent, prepare_to_decision

FAILURE_POINTS = ("before", "after_durable_before_ack", "after_ack")


class SubprocessRecoveryMatrixTests(unittest.TestCase):
    def run_crashing_child(self, body: str) -> None:
        result = subprocess.run(
            [sys.executable, "-c", body],
            cwd=Path(__file__).parent,
            check=False,
        )
        self.assertEqual(result.returncode, 17)

    @staticmethod
    def crash_program(
        setup: str, action: str, point: str, acknowledgement: Path
    ) -> str:
        return f"""
import os
from pathlib import Path
from barrier_protocol import AlignedJoin, ChannelRecord, DurableChannel, RecordKind
{setup}
point = {point!r}
if point == 'before':
    os._exit(17)
{action}
if point == 'after_durable_before_ack':
    os._exit(17)
Path({str(acknowledgement)!r}).write_text('ack')
os._exit(17)
"""

    def test_channel_data_and_barrier_kill_matrix(self) -> None:
        for boundary in ("data", "barrier"):
            for point in FAILURE_POINTS:
                with (
                    self.subTest(boundary=boundary, point=point),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    path = Path(directory) / "channel.sqlite"
                    acknowledgement = Path(directory) / "ack"
                    channel = DurableChannel(path)
                    channel.claim_partition("edge", "p0", "worker", 1)
                    if boundary == "barrier":
                        channel.append_data(
                            "edge",
                            "p0",
                            1,
                            "r1",
                            7,
                            {"offset": 1},
                            "worker",
                            1,
                        )
                    setup = f"channel = DurableChannel(Path({str(path)!r}))"
                    if boundary == "data":
                        action = "channel.append_data('edge', 'p0', 1, 'r1', 7, {'offset': 1}, 'worker', 1)"
                    else:
                        action = (
                            "channel.append_barrier('edge', 'p0', 2, 1, 'worker', 1)"
                        )
                    self.run_crashing_child(
                        self.crash_program(setup, action, point, acknowledgement)
                    )

                    reopened = DurableChannel(path)
                    records = reopened.read_after("edge", "p0", 0)
                    expected_count = (
                        (0 if point == "before" else 1)
                        if boundary == "data"
                        else (1 if point == "before" else 2)
                    )
                    self.assertEqual(len(records), expected_count)
                    self.assertEqual(acknowledgement.exists(), point == "after_ack")

    def test_join_alignment_snapshot_kill_matrix(self) -> None:
        bindings = {"left": ("left", "p0"), "right": ("right", "p0")}
        for point in FAILURE_POINTS:
            with (
                self.subTest(point=point),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "join.sqlite"
                acknowledgement = Path(directory) / "ack"
                join = AlignedJoin(path, "join", bindings)
                join.claim("worker", 1)
                join.ingest(
                    "left",
                    ChannelRecord("left", "p0", 1, RecordKind.DATA, "l1", 2),
                    "worker",
                    1,
                )
                join.ingest(
                    "right",
                    ChannelRecord("right", "p0", 1, RecordKind.DATA, "r1", 3),
                    "worker",
                    1,
                )
                join.ingest(
                    "left",
                    ChannelRecord(
                        "left",
                        "p0",
                        2,
                        RecordKind.BARRIER,
                        "barrier:1",
                        checkpoint=1,
                    ),
                    "worker",
                    1,
                )
                setup = f"join = AlignedJoin(Path({str(path)!r}), 'join', {bindings!r})"
                action = """join.ingest(
    'right',
    ChannelRecord('right', 'p0', 2, RecordKind.BARRIER, 'barrier:1', checkpoint=1),
    'worker', 1,
)"""
                self.run_crashing_child(
                    self.crash_program(setup, action, point, acknowledgement)
                )

                reopened = AlignedJoin(path, "join", bindings)
                if point == "before":
                    with self.assertRaises(KeyError):
                        reopened.snapshot(1)
                    reopened.ingest(
                        "right",
                        ChannelRecord(
                            "right",
                            "p0",
                            2,
                            RecordKind.BARRIER,
                            "barrier:1",
                            checkpoint=1,
                        ),
                        "worker",
                        1,
                    )
                    self.assertEqual(reopened.snapshot(1), (5, {"left": 2, "right": 2}))
                else:
                    self.assertEqual(reopened.snapshot(1), (5, {"left": 2, "right": 2}))
                self.assertEqual(acknowledgement.exists(), point == "after_ack")

    def test_takeover_and_restore_kill_matrix(self) -> None:
        for boundary in ("takeover", "restore"):
            for point in FAILURE_POINTS:
                with (
                    self.subTest(boundary=boundary, point=point),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    acknowledgement = Path(directory) / "ack"
                    if boundary == "takeover":
                        path = Path(directory) / "channel.sqlite"
                        channel = DurableChannel(path)
                        channel.claim_partition("edge", "p0", "old", 1, lease_ttl=1)
                        channel.advance_clock(1)
                        setup = f"channel = DurableChannel(Path({str(path)!r}))"
                        action = "channel.claim_partition('edge', 'p0', 'new', 2)"
                    else:
                        path = Path(directory) / "join.sqlite"
                        bindings = {
                            "left": ("left", "p0"),
                            "right": ("right", "p0"),
                        }
                        join = AlignedJoin(path, "join", bindings)
                        join.claim("worker", 1)
                        self._build_two_join_snapshots(join)
                        setup = f"join = AlignedJoin(Path({str(path)!r}), 'join', {bindings!r})"
                        action = "join.restore(1, 'worker', 1)"

                    self.run_crashing_child(
                        self.crash_program(setup, action, point, acknowledgement)
                    )
                    self.assertEqual(acknowledgement.exists(), point == "after_ack")

                    if boundary == "takeover":
                        reopened = DurableChannel(path)
                        owner = reopened.connection.execute(
                            """
                            SELECT owner_id, generation FROM partition_owners
                            WHERE edge_id = 'edge' AND partition_id = 'p0'
                            """
                        ).fetchone()
                        expected = ("old", 1) if point == "before" else ("new", 2)
                        self.assertEqual(owner, expected)
                    else:
                        reopened = AlignedJoin(path, "join", bindings)
                        expected_state = 105 if point == "before" else 5
                        self.assertEqual(reopened.state_value, expected_state)
                        self.assertEqual(
                            reopened.snapshot(2),
                            (105, {"left": 4, "right": 3}),
                        )

    def test_post_barrier_buffer_kill_matrix(self) -> None:
        bindings = {"left": ("left", "p0"), "right": ("right", "p0")}
        for point in FAILURE_POINTS:
            with (
                self.subTest(point=point),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "join.sqlite"
                acknowledgement = Path(directory) / "ack"
                join = AlignedJoin(path, "join", bindings)
                join.claim("worker", 1)
                join.ingest(
                    "left",
                    ChannelRecord("left", "p0", 1, RecordKind.DATA, "l1", 2),
                    "worker",
                    1,
                )
                join.ingest(
                    "right",
                    ChannelRecord("right", "p0", 1, RecordKind.DATA, "r1", 3),
                    "worker",
                    1,
                )
                join.ingest(
                    "left",
                    ChannelRecord(
                        "left",
                        "p0",
                        2,
                        RecordKind.BARRIER,
                        "barrier:1",
                        checkpoint=1,
                    ),
                    "worker",
                    1,
                )
                setup = f"join = AlignedJoin(Path({str(path)!r}), 'join', {bindings!r})"
                action = """join.ingest(
    'left', ChannelRecord('left', 'p0', 3, RecordKind.DATA, 'l2', 100),
    'worker', 1,
)"""
                self.run_crashing_child(
                    self.crash_program(setup, action, point, acknowledgement)
                )

                reopened = AlignedJoin(path, "join", bindings)
                reopened.ingest(
                    "right",
                    ChannelRecord(
                        "right",
                        "p0",
                        2,
                        RecordKind.BARRIER,
                        "barrier:1",
                        checkpoint=1,
                    ),
                    "worker",
                    1,
                )
                self.assertEqual(reopened.snapshot(1), (5, {"left": 2, "right": 2}))
                expected_live_state = 5 if point == "before" else 105
                self.assertEqual(reopened.state_value, expected_live_state)
                self.assertEqual(acknowledgement.exists(), point == "after_ack")

    def test_combined_decision_authority_kill_matrix(self) -> None:
        for point in FAILURE_POINTS:
            with (
                self.subTest(point=point),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "coordinator.sqlite"
                acknowledgement = Path(directory) / "ack"
                checkpoint = intent()
                coordinator = Coordinator(path)
                prepare_to_decision(coordinator, checkpoint, ExternalWorld())
                setup = f"""
from integrated_protocol import Coordinator, Fault
from test_integrated_protocol import intent
class ExitFault(Fault):
    def hit(self, candidate):
        if self.point == candidate:
            os._exit(17)
coordinator = Coordinator(Path({str(path)!r}))
checkpoint = intent()
"""
                fault = (
                    "ExitFault('commit_decision:after_durable_before_ack')"
                    if point == "after_durable_before_ack"
                    else "None"
                )
                action = f"coordinator.decide_commit(checkpoint, {fault})"
                self.run_crashing_child(
                    self.crash_program(setup, action, point, acknowledgement)
                )

                reopened = Coordinator(path)
                observation = reopened.raw_observation(checkpoint)
                selected = reopened.latest_recovery_checkpoint(
                    checkpoint.job_id, checkpoint.graph_version_id
                )
                expected_selected = None if point == "before" else 1
                self.assertEqual(selected, expected_selected)
                self.assertTrue(
                    Oracle.evaluate(
                        observation,
                        selected_sequence=selected,
                        externally_committed_sinks=set(),
                        externally_acked_sources=set(),
                    ).passed
                )
                self.assertEqual(acknowledgement.exists(), point == "after_ack")

    @staticmethod
    def _build_two_join_snapshots(join: AlignedJoin) -> None:
        join.ingest(
            "left",
            ChannelRecord("left", "p0", 1, RecordKind.DATA, "l1", 2),
            "worker",
            1,
        )
        join.ingest(
            "right",
            ChannelRecord("right", "p0", 1, RecordKind.DATA, "r1", 3),
            "worker",
            1,
        )
        join.ingest(
            "left",
            ChannelRecord(
                "left", "p0", 2, RecordKind.BARRIER, "barrier:1", checkpoint=1
            ),
            "worker",
            1,
        )
        join.ingest(
            "right",
            ChannelRecord(
                "right", "p0", 2, RecordKind.BARRIER, "barrier:1", checkpoint=1
            ),
            "worker",
            1,
        )
        join.ingest(
            "left",
            ChannelRecord("left", "p0", 3, RecordKind.DATA, "l2", 100),
            "worker",
            1,
        )
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


if __name__ == "__main__":
    unittest.main()
