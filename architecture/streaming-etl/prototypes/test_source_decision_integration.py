from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from integrated_protocol import Coordinator, ProtocolError
from source_oracle import inspect_source
from source_v1 import (
    AcceptedInputLedger,
    CombinedDecisionRef,
    SourceEnvelope,
    SourceProtocolError,
    source_record_id,
    value_digest,
)
from test_integrated_protocol import ExternalWorld, intent, prepare_to_decision


class SourceDecisionIntegrationTests(unittest.TestCase):
    def test_repeated_cut_keeps_earliest_valid_ack_epoch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            coordinator = Coordinator(root / "coordinator.sqlite")
            world = ExternalWorld()
            first = intent(sources=(("p0", "one"),))
            prepare_to_decision(coordinator, first, world)
            coordinator.decide_commit(first)
            ledger = AcceptedInputLedger(
                root / "source.sqlite", root / "coordinator.sqlite"
            )
            ledger.claim_partition("p0", "worker", 10, lambda _p, epoch: epoch)
            payload = {"position": "one"}
            record = ledger.accept(
                SourceEnvelope(
                    "p0",
                    "one",
                    source_record_id("p0", "one"),
                    "lineage:p0:one",
                    value_digest(payload),
                    payload,
                ),
                "worker",
                10,
            )
            cut = (("p0", record.ordinal, "one"),)
            first_ref = CombinedDecisionRef(
                **coordinator.source_decision_reference(first)
            )
            ledger.bind_committed_checkpoint(first_ref, cut)
            external = {}

            def ack(p, pos, ordinal, epoch):
                external[p] = (ordinal, pos, epoch)

            ledger.acknowledge(
                first.checkpoint_id, first_ref.digest, "p0", "worker", 10, ack
            )
            ledger.claim_partition("p0", "takeover", 11, lambda _p, epoch: epoch)

            second = intent(sequence=2, sources=(("p0", "one"),))
            second = replace(
                second,
                generation_manifest=tuple(
                    (participant, 11 if participant == "p0" else epoch)
                    for participant, epoch in second.generation_manifest
                ),
            )
            prepare_to_decision(coordinator, second, world)
            coordinator.decide_commit(second)
            second_ref = CombinedDecisionRef(
                **coordinator.source_decision_reference(second)
            )
            ledger.bind_committed_checkpoint(second_ref, cut)
            ledger.acknowledge(
                second.checkpoint_id, second_ref.digest, "p0", "takeover", 11, ack
            )
            observed = inspect_source(
                root / "source.sqlite", external, root / "coordinator.sqlite"
            )
            self.assertEqual(observed.violations, ())
            self.assertEqual(external, {"p0": (1, "one", 10)})

    def test_source_cut_requires_exact_global_commit_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            checkpoint = intent()
            coordinator = Coordinator(root / "coordinator.sqlite")
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)

            ledger = AcceptedInputLedger(
                root / "source.sqlite", root / "coordinator.sqlite"
            )
            external_epochs: dict[str, int] = {}

            def fence(partition: str, epoch: int) -> int:
                external_epochs[partition] = epoch
                return epoch

            cuts = []
            for index, (partition, position) in enumerate(
                checkpoint.source_positions, start=1
            ):
                epoch = dict(checkpoint.generation_manifest)[partition]
                ledger.claim_partition(partition, "source-worker", epoch, fence)
                payload = {"position": position}
                record = ledger.accept(
                    SourceEnvelope(
                        partition,
                        position,
                        source_record_id(partition, position),
                        f"lineage:{partition}:{position}",
                        value_digest(payload),
                        payload,
                    ),
                    "source-worker",
                    epoch,
                )
                self.assertEqual(record.ordinal, 1)
                cuts.append((partition, record.ordinal, position))

            with self.assertRaisesRegex(ProtocolError, "durable COMMIT"):
                coordinator.source_decision_reference(checkpoint)

            coordinator.decide_commit(checkpoint)
            reference = CombinedDecisionRef(
                **coordinator.source_decision_reference(checkpoint)
            )
            ledger.bind_committed_checkpoint(reference, tuple(cuts))
            self.assertEqual(
                inspect_source(
                    root / "source.sqlite", {}, root / "coordinator.sqlite"
                ).violations,
                (),
            )
            stale_epoch = inspect_source(
                root / "source.sqlite",
                {cuts[0][0]: (cuts[0][1], cuts[0][2], 1)},
                root / "coordinator.sqlite",
            )
            self.assertTrue(
                any("external ACK has stale epoch" in v for v in stale_epoch.violations)
            )
            self.assertTrue(
                any(
                    "missing global COMMIT authority" in violation
                    for violation in inspect_source(
                        root / "source.sqlite", {}, None
                    ).violations
                )
            )
            partition, ordinal, position = cuts[0]
            with ledger.connection:
                ledger.connection.execute(
                    "INSERT INTO ack_attempts VALUES(?,?,?,?,?,?,?,1,'SUCCEEDED',NULL)",
                    (
                        reference.checkpoint_id,
                        reference.digest,
                        partition,
                        ordinal,
                        position,
                        "source-worker",
                        dict(checkpoint.generation_manifest)[partition],
                    ),
                )
            false_terminal = inspect_source(
                root / "source.sqlite", {}, root / "coordinator.sqlite"
            )
            self.assertTrue(
                any("false SUCCEEDED ACK" in v for v in false_terminal.violations)
            )
            with ledger.connection:
                ledger.connection.execute("DELETE FROM ack_attempts")

            forged = replace(reference, capability_digest="forged")
            self.assertFalse(coordinator.verify_source_decision(forged))
            with self.assertRaisesRegex(SourceProtocolError, "unverified"):
                ledger.bind_committed_checkpoint(forged, tuple(cuts))
            forged_transaction = replace(reference, transaction_id="forged")
            self.assertFalse(coordinator.verify_source_decision(forged_transaction))
            with ledger.connection:
                ledger.connection.execute("DROP TRIGGER checkpoint_commit")
                ledger.connection.execute(
                    "UPDATE checkpoints SET decision_digest='forged'"
                )
            digest_corruption = inspect_source(
                root / "source.sqlite", {}, root / "coordinator.sqlite"
            )
            self.assertTrue(
                any(
                    "canonical decision digest mismatch" in violation
                    for violation in digest_corruption.violations
                )
            )
            with ledger.connection:
                ledger.connection.execute(
                    "UPDATE checkpoints SET decision_json='{}',decision_digest=?",
                    (value_digest({}),),
                )
            structural_corruption = inspect_source(
                root / "source.sqlite", {}, root / "coordinator.sqlite"
            )
            self.assertTrue(
                any(
                    "invalid canonical decision structure" in violation
                    for violation in structural_corruption.violations
                )
            )


if __name__ == "__main__":
    unittest.main()
