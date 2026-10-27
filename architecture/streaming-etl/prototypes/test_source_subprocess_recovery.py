from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from integrated_protocol import Coordinator
from source_oracle import inspect_source
from source_v1 import (
    AcceptedInputLedger,
    CombinedDecisionRef,
    SourceEnvelope,
    source_record_id,
    value_digest,
)
from test_integrated_protocol import ExternalWorld, intent, prepare_to_decision


def envelope(position: str) -> SourceEnvelope:
    payload = {"position": position}
    return SourceEnvelope(
        "p0",
        position,
        source_record_id("p0", position),
        f"lineage:p0:{position}",
        value_digest(payload),
        payload,
    )


def durable_ack(
    path: Path, partition: str, position: str, ordinal: int, epoch: int
) -> None:
    connection = sqlite3.connect(path)
    with connection:
        connection.execute(
            """CREATE TABLE IF NOT EXISTS acks(
               partition TEXT PRIMARY KEY,position TEXT NOT NULL,
               ordinal INTEGER NOT NULL,epoch INTEGER NOT NULL)"""
        )
        old = connection.execute(
            "SELECT position,ordinal,epoch FROM acks WHERE partition=?", (partition,)
        ).fetchone()
        if old is not None and old[1] > ordinal:
            raise AssertionError("ACK regression")
        if old is not None and old[1] == ordinal and old[0] != position:
            raise AssertionError("ACK identity drift")
        if old is not None and old[1] == ordinal and old[2] > epoch:
            raise AssertionError("stale ACK epoch")
        connection.execute(
            "INSERT OR REPLACE INTO acks VALUES(?,?,?,?)",
            (partition, position, ordinal, epoch),
        )


def external_state(path: Path) -> dict[str, tuple[int, str, int]]:
    if not path.exists():
        return {}
    connection = sqlite3.connect(path)
    try:
        return {
            partition: (ordinal, position, epoch)
            for partition, position, ordinal, epoch in connection.execute(
                "SELECT partition,position,ordinal,epoch FROM acks"
            )
        }
    except sqlite3.OperationalError:
        return {}


class SourceSubprocessRecoveryTests(unittest.TestCase):
    def run_child(self, program: str) -> None:
        result = subprocess.run(
            [sys.executable, "-c", program],
            cwd=Path(__file__).parent,
            check=False,
        )
        self.assertEqual(result.returncode, 17)

    def create_ledger(self, path: Path) -> AcceptedInputLedger:
        ledger = AcceptedInputLedger(path, path.with_name("coordinator.sqlite"))
        ledger.claim_partition("p0", "worker", 10, lambda _p, epoch: epoch)
        return ledger

    def test_accept_hard_crash_matrix(self) -> None:
        for point in (
            "before",
            "after_insert_before_commit",
            "after_durable_before_ack",
            "after_ack",
        ):
            with self.subTest(point=point), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger_path = root / "source.sqlite"
                ledger = self.create_ledger(ledger_path)
                ledger.connection.close()
                program = f"""
import os
from pathlib import Path
from source_v1 import AcceptedInputLedger,SourceEnvelope,source_record_id,value_digest
point={point!r}
ledger=AcceptedInputLedger(Path({str(ledger_path)!r}))
payload={{'position':'one'}}
record=SourceEnvelope('p0','one',source_record_id('p0','one'),'lineage:p0:one',value_digest(payload),payload)
if point=='before': os._exit(17)
if point=='after_insert_before_commit':
    ledger.accept(record,'worker',10,crash_hook=lambda _: os._exit(17))
ledger.accept(record,'worker',10)
if point=='after_durable_before_ack': os._exit(17)
Path({str(root / "caller_ack")!r}).write_text('ack')
os._exit(17)
"""
                self.run_child(program)
                observed = inspect_source(ledger_path, {})
                expected = 0 if point in {"before", "after_insert_before_commit"} else 1
                self.assertEqual(observed.accepted.get("p0", 0), expected)
                self.assertEqual(
                    observed.replay_start["p0"], None if expected == 0 else 1
                )
                self.assertEqual((root / "caller_ack").exists(), point == "after_ack")
                self.assertEqual(observed.violations, ())

    def test_ack_hard_crash_matrix_and_unknown_result_recovery(self) -> None:
        for point in (
            "before",
            "after_intent_before_external",
            "after_external_before_local",
            "after_local_before_caller_ack",
        ):
            with self.subTest(point=point), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                ledger_path = root / "source.sqlite"
                external_path = root / "external.sqlite"
                ledger = self.create_ledger(ledger_path)
                record = ledger.accept(envelope("one"), "worker", 10)
                cuts = (("p0", record.ordinal, record.opaque_position),)
                checkpoint = intent(sources=(("p0", "one"),))
                coordinator = Coordinator(root / "coordinator.sqlite")
                world = ExternalWorld()
                prepare_to_decision(coordinator, checkpoint, world)
                coordinator.decide_commit(checkpoint)
                decision = CombinedDecisionRef(
                    **coordinator.source_decision_reference(checkpoint)
                )
                ledger.bind_committed_checkpoint(decision, cuts)
                ledger.connection.close()
                program = f"""
import os,sqlite3
from pathlib import Path
from source_v1 import AcceptedInputLedger
point={point!r}
ledger=AcceptedInputLedger(Path({str(ledger_path)!r}))
if point=='before': os._exit(17)
def ack(partition,position,ordinal,epoch):
    db=sqlite3.connect({str(external_path)!r})
    with db:
        db.execute('CREATE TABLE IF NOT EXISTS acks(partition TEXT PRIMARY KEY,position TEXT NOT NULL,ordinal INTEGER NOT NULL,epoch INTEGER NOT NULL)')
        old=db.execute('SELECT position,ordinal,epoch FROM acks WHERE partition=?',(partition,)).fetchone()
        if old is not None and (old[1]>ordinal or (old[1]==ordinal and (old[0]!=position or old[2]>epoch))): raise AssertionError('ACK drift')
        db.execute('INSERT OR REPLACE INTO acks VALUES(?,?,?,?)',(partition,position,ordinal,epoch))
    if point=='after_external_before_local': os._exit(17)
if point=='after_intent_before_external':
    ledger.acknowledge({decision.checkpoint_id!r},{decision.digest!r},'p0','worker',10,ack,crash_hook=lambda _: os._exit(17))
ledger.acknowledge({decision.checkpoint_id!r},{decision.digest!r},'p0','worker',10,ack)
os._exit(17)
"""
                self.run_child(program)
                external = external_state(external_path)
                observed = inspect_source(
                    ledger_path, external, root / "coordinator.sqlite"
                )
                self.assertEqual(observed.violations, ())
                if point == "before":
                    self.assertEqual(observed.local_acknowledged["p0"], 0)
                    self.assertEqual(observed.blockers, ())
                elif point == "after_intent_before_external":
                    self.assertEqual(observed.local_acknowledged["p0"], 0)
                    self.assertEqual(observed.blockers[0][3], "IN_PROGRESS")
                    reopened = AcceptedInputLedger(
                        ledger_path, root / "coordinator.sqlite"
                    )
                    reopened.acknowledge(
                        decision.checkpoint_id,
                        decision.digest,
                        "p0",
                        "worker",
                        10,
                        lambda p, pos, ordinal, epoch: durable_ack(
                            external_path, p, pos, ordinal, epoch
                        ),
                    )
                    observed = inspect_source(
                        ledger_path,
                        external_state(external_path),
                        root / "coordinator.sqlite",
                    )
                    self.assertEqual(observed.violations, ())
                    self.assertEqual(observed.blockers, ())
                elif point == "after_external_before_local":
                    self.assertEqual(observed.local_acknowledged["p0"], 0)
                    self.assertEqual(observed.blockers[0][3], "IN_PROGRESS")
                    reopened = AcceptedInputLedger(
                        ledger_path, root / "coordinator.sqlite"
                    )
                    reopened.claim_partition(
                        "p0", "takeover", 11, lambda _p, epoch: epoch
                    )
                    reopened.acknowledge(
                        decision.checkpoint_id,
                        decision.digest,
                        "p0",
                        "takeover",
                        11,
                        lambda p, pos, ordinal, epoch: durable_ack(
                            external_path, p, pos, ordinal, epoch
                        ),
                    )
                    observed = inspect_source(
                        ledger_path,
                        external_state(external_path),
                        root / "coordinator.sqlite",
                    )
                    self.assertEqual(observed.violations, ())
                    self.assertEqual(observed.blockers, ())
                else:
                    self.assertEqual(observed.local_acknowledged["p0"], 1)
                    self.assertEqual(observed.blockers, ())
                if point != "before" or observed.local_acknowledged["p0"] == 0:
                    expected_replay = 1 if point == "before" else None
                    if point == "after_external_before_local":
                        expected_replay = None
                    self.assertEqual(observed.replay_start["p0"], expected_replay)

    def test_independent_oracle_detects_corrupted_runtime_storage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger_path = Path(directory) / "source.sqlite"
            ledger = self.create_ledger(ledger_path)
            ledger.accept(envelope("one"), "worker", 10)
            ledger.connection.close()
            bad_external = inspect_source(
                ledger_path,
                {"p0": (999, "ghost", 4), "unknown": (1, "x", 1)},
            )
            self.assertTrue(
                any(
                    "external ACK is not accepted" in v for v in bad_external.violations
                )
            )
            self.assertTrue(
                any(
                    "external ACK lacks committed cut" in v
                    for v in bad_external.violations
                )
            )
            self.assertTrue(
                any("unknown partition" in v for v in bad_external.violations)
            )
            raw = sqlite3.connect(ledger_path)
            with raw:
                raw.execute("DROP TRIGGER accepted_no_update")
                raw.execute(
                    "UPDATE accepted_records SET record_id='forged',payload_json='{' WHERE partition_id='p0'"
                )
            observed = inspect_source(ledger_path, {})
            self.assertIn("noncanonical record ID", observed.violations[0])
            self.assertTrue(
                any("invalid payload JSON" in v for v in observed.violations)
            )


if __name__ == "__main__":
    unittest.main()
