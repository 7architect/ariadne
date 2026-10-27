from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from safety_contracts import (
    FAILURE_POINTS,
    CheckpointLifecycle,
    CheckpointObservation,
    CheckpointPhase,
    CombinedCheckpointIntent,
    CombinedProgressStore,
    ContractViolation,
    DecisionObservation,
    DeterministicCrash,
    FaultInjector,
    GuaranteeRequest,
    IdentityRegistry,
    ParticipantContract,
    ParticipantRole,
    SafetyOracle,
    SinkTier,
    StableIdentities,
    VisibleRecord,
    canonical_digest,
    negotiate_contracts,
)

GRAPH_SPEC = {
    "operators": ["source", "map", "sink"],
    "edges": [["source", "map"], ["map", "sink"]],
}


def valid_participants() -> list[ParticipantContract]:
    return [
        ParticipantContract(
            "source-1",
            ParticipantRole.SOURCE,
            stable_ids=True,
            replayable=True,
        ),
        ParticipantContract(
            "operator-1",
            ParticipantRole.OPERATOR,
            deterministic=True,
            managed_state=True,
        ),
        ParticipantContract(
            "channel-1",
            ParticipantRole.CHANNEL,
            stable_ids=True,
            replayable=True,
            durable=True,
            fenced_writes=True,
        ),
        ParticipantContract(
            "state-1",
            ParticipantRole.STATE_BACKEND,
            durable=True,
            fenced_writes=True,
        ),
        ParticipantContract(
            "sink-1",
            ParticipantRole.SINK,
            fenced_writes=True,
            idempotent_writes=True,
            prepare_commit_recover=True,
            sink_tier=SinkTier.TRANSACTIONAL,
        ),
    ]


class StableIdentityTests(unittest.TestCase):
    def test_all_protocol_ids_are_stable_after_restart(self) -> None:
        first = StableIdentities("tenant", "job", GRAPH_SPEC)
        second = StableIdentities(
            "tenant",
            "job",
            {
                "edges": [["source", "map"], ["map", "sink"]],
                "operators": ["source", "map", "sink"],
            },
        )

        edge = first.edge_id("source", "out", "map", "in")
        partition = first.partition_id(edge, 0)
        ids = (
            first.job_id,
            first.graph_version_id,
            first.operator_id("map"),
            edge,
            partition,
            first.record_id(partition, "offset-42"),
            first.checkpoint_id(7),
            first.generation_id(partition, 3),
        )
        restarted_edge = second.edge_id("source", "out", "map", "in")
        restarted_partition = second.partition_id(restarted_edge, 0)
        restarted_ids = (
            second.job_id,
            second.graph_version_id,
            second.operator_id("map"),
            restarted_edge,
            restarted_partition,
            second.record_id(restarted_partition, "offset-42"),
            second.checkpoint_id(7),
            second.generation_id(restarted_partition, 3),
        )

        self.assertEqual(ids, restarted_ids)
        self.assertEqual(len(ids), len(set(ids)))

    def test_identity_registry_rejects_changed_lineage_or_payload(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "identities.sqlite"
            registry = IdentityRegistry(path)
            manifest = {"lineage": ["source:0:42"], "payload": {"value": 10}}
            registry.claim("record", "record-42", manifest)

            restarted = IdentityRegistry(path)
            restarted.claim("record", "record-42", manifest)
            with self.assertRaises(ContractViolation):
                restarted.claim(
                    "record",
                    "record-42",
                    {"lineage": ["source:0:41"], "payload": {"value": 10}},
                )
            with self.assertRaises(ContractViolation):
                restarted.claim(
                    "record",
                    "record-42",
                    {"lineage": ["source:0:42"], "payload": {"value": 11}},
                )


class CheckpointLifecycleTests(unittest.TestCase):
    def test_cooperative_checkpoint_survives_reopen_and_completes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "checkpoint.sqlite"
            checkpoint = CheckpointLifecycle(path)
            checkpoint.start(
                "checkpoint-7", {"participants": ["a", "b"]}, requires_commit=True
            )
            checkpoint.advance("checkpoint-7", CheckpointPhase.ALIGNING)
            checkpoint.advance("checkpoint-7", CheckpointPhase.ALIGNED)
            checkpoint.advance("checkpoint-7", CheckpointPhase.PREPARING)
            checkpoint.advance("checkpoint-7", CheckpointPhase.PREPARED)

            reopened = CheckpointLifecycle(path)
            reopened.advance("checkpoint-7", CheckpointPhase.COMMIT_DECIDED)
            reopened.advance("checkpoint-7", CheckpointPhase.COMMITTING)
            reopened.advance("checkpoint-7", CheckpointPhase.COMPLETED)
            reopened.advance("checkpoint-7", CheckpointPhase.COMPLETED)

            self.assertEqual(reopened.phase("checkpoint-7"), CheckpointPhase.COMPLETED)
            self.assertEqual(
                reopened.history("checkpoint-7"),
                [
                    CheckpointPhase.STARTED,
                    CheckpointPhase.ALIGNING,
                    CheckpointPhase.ALIGNED,
                    CheckpointPhase.PREPARING,
                    CheckpointPhase.PREPARED,
                    CheckpointPhase.COMMIT_DECIDED,
                    CheckpointPhase.COMMITTING,
                    CheckpointPhase.COMPLETED,
                ],
            )

    def test_abort_and_rollback_are_rejected_after_commit_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = CheckpointLifecycle(Path(directory) / "checkpoint.sqlite")
            checkpoint.start(
                "checkpoint-7", {"participants": ["sink"]}, requires_commit=True
            )
            for phase in (
                CheckpointPhase.ALIGNING,
                CheckpointPhase.ALIGNED,
                CheckpointPhase.PREPARING,
                CheckpointPhase.PREPARED,
                CheckpointPhase.COMMIT_DECIDED,
            ):
                checkpoint.advance("checkpoint-7", phase)

            with self.assertRaises(ContractViolation):
                checkpoint.advance("checkpoint-7", CheckpointPhase.ABORTED)
            with self.assertRaises(ContractViolation):
                checkpoint.advance("checkpoint-7", CheckpointPhase.PREPARED)

    def test_cooperative_checkpoint_cannot_skip_durable_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = CheckpointLifecycle(Path(directory) / "checkpoint.sqlite")
            checkpoint.start(
                "checkpoint-7", {"participants": ["sink"]}, requires_commit=True
            )
            checkpoint.advance("checkpoint-7", CheckpointPhase.ALIGNING)
            checkpoint.advance("checkpoint-7", CheckpointPhase.ALIGNED)
            with self.assertRaises(ContractViolation):
                checkpoint.advance("checkpoint-7", CheckpointPhase.COMPLETED)


class CombinedProgressTests(unittest.TestCase):
    @staticmethod
    def intent(sequence: int = 2, **changes: object) -> CombinedCheckpointIntent:
        values: dict[str, object] = {
            "checkpoint_id": f"checkpoint-{sequence}",
            "checkpoint_sequence": sequence,
            "graph_version_id": "graph-v1",
            "generation_id": "generation-3",
            "operator_manifest": ("source", "map", "sink"),
            "sink_manifest": ("sink-a", "sink-b"),
            "source_position": sequence * 100,
        }
        values.update(changes)
        return CombinedCheckpointIntent(**values)  # type: ignore[arg-type]

    def test_source_ack_rejected_without_commit_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CombinedProgressStore(Path(directory) / "progress.sqlite")
            intent = self.intent()
            store.start(intent)
            store.prepared(intent)

            with self.assertRaisesRegex(ContractViolation, "COMMIT_DECIDED"):
                store.publish_and_ack(intent)
            self.assertIsNone(store.acknowledged_source_position())

    def test_combined_decision_rejects_every_manifest_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = CombinedProgressStore(Path(directory) / "progress.sqlite")
            intent = self.intent()
            store.start(intent)
            changes = (
                {"checkpoint_sequence": 9},
                {"graph_version_id": "graph-v2"},
                {"generation_id": "generation-4"},
                {"operator_manifest": ("source", "different", "sink")},
                {"sink_manifest": ("sink-a",)},
                {"source_position": 999},
            )
            for change in changes:
                with self.subTest(change=change), self.assertRaises(ContractViolation):
                    store.prepared(self.intent(**change))

    def test_crash_before_decision_uses_previous_completed_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.sqlite"
            store = CombinedProgressStore(path)
            first = self.intent(1)
            store.start(first)
            store.prepared(first)
            store.decide_commit(first)
            store.publish_and_ack(first)

            second = self.intent(2)
            store.start(second)
            store.prepared(second)
            with self.assertRaises(DeterministicCrash):
                store.decide_commit(
                    second,
                    FaultInjector("commit_decision:before"),
                )

            reopened = CombinedProgressStore(path)
            reopened.recover()
            self.assertEqual(reopened.latest_recovery_checkpoint(), 1)
            self.assertEqual(reopened.acknowledged_source_position(), 100)
            self.assertEqual(
                reopened.observation(second.checkpoint_id),
                ("PREPARED", None, False),
            )

    def test_crash_after_combined_decision_recovers_and_acknowledges_same_checkpoint(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.sqlite"
            store = CombinedProgressStore(path)
            intent = self.intent()
            store.start(intent)
            store.prepared(intent)
            with self.assertRaises(DeterministicCrash):
                store.decide_commit(
                    intent,
                    FaultInjector("commit_decision:after_durable_before_ack"),
                )

            reopened = CombinedProgressStore(path)
            self.assertEqual(reopened.latest_recovery_checkpoint(), 2)
            self.assertIsNone(reopened.acknowledged_source_position())
            reopened.recover()
            self.assertEqual(reopened.latest_recovery_checkpoint(), 2)
            self.assertEqual(reopened.acknowledged_source_position(), 200)
            self.assertEqual(
                reopened.observation(intent.checkpoint_id),
                ("COMPLETED", "COMMIT", True),
            )

    def test_lost_ack_retries_combined_decision_without_manifest_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress.sqlite"
            intent = self.intent()
            store = CombinedProgressStore(path)
            store.start(intent)
            store.prepared(intent)
            store.decide_commit(intent)
            with self.assertRaises(DeterministicCrash):
                store.publish_and_ack(
                    intent,
                    FaultInjector("source_ack:after_durable_before_ack"),
                )

            reopened = CombinedProgressStore(path)
            reopened.start(intent)
            reopened.decide_commit(intent)
            reopened.publish_and_ack(intent)
            self.assertEqual(reopened.acknowledged_source_position(), 200)
            self.assertEqual(
                reopened.observation(intent.checkpoint_id),
                ("COMPLETED", "COMMIT", True),
            )

    def test_oracle_detects_unsafe_ack_ahead_of_decision_control(self) -> None:
        report = SafetyOracle.evaluate(
            checkpoints=[CheckpointObservation(2, "COMPLETED")],
            selected_recovery_checkpoint=2,
            decisions=[],
            visible_records=[VisibleRecord("sink-a", "tx-2", "record-2", "payload")],
            sink_tier=SinkTier.TRANSACTIONAL,
        )
        self.assertFalse(report.invariants["transactional_visibility_requires_commit"])


class CapabilityNegotiationTests(unittest.TestCase):
    def test_versioned_contracts_negotiate_transactional_guarantee(self) -> None:
        negotiated = negotiate_contracts(
            GuaranteeRequest(minimum_sink_tier=SinkTier.TRANSACTIONAL),
            valid_participants(),
        )
        self.assertEqual(negotiated.sink_tier, SinkTier.TRANSACTIONAL)
        self.assertEqual(negotiated.protocol_version, 1)

    def test_unsupported_guarantees_are_rejected_before_deployment(self) -> None:
        participants = valid_participants()
        participants[0] = ParticipantContract(
            "source-1",
            ParticipantRole.SOURCE,
            stable_ids=True,
            replayable=False,
        )
        with self.assertRaisesRegex(ContractViolation, "source"):
            negotiate_contracts(GuaranteeRequest(), participants)

        participants = valid_participants()
        participants[-1] = ParticipantContract(
            "sink-1",
            ParticipantRole.SINK,
            idempotent_writes=True,
            sink_tier=SinkTier.IDEMPOTENT,
        )
        with self.assertRaisesRegex(ContractViolation, "below requested"):
            negotiate_contracts(
                GuaranteeRequest(minimum_sink_tier=SinkTier.TRANSACTIONAL),
                participants,
            )


class DeterministicFailureTests(unittest.TestCase):
    def test_every_named_durable_boundary_has_three_exact_failure_points(self) -> None:
        for failure_point in FAILURE_POINTS:
            boundary = failure_point.split(":", maxsplit=1)[0]
            effects: list[str] = []
            injector = FaultInjector(failure_point)
            with self.assertRaisesRegex(DeterministicCrash, failure_point):
                injector.run_boundary(
                    boundary,
                    lambda effects=effects: effects.append("durable"),
                    lambda effects=effects: effects.append("ack"),
                )

            offset = failure_point.split(":", maxsplit=1)[1]
            expected = {
                "before": [],
                "after_durable_before_ack": ["durable"],
                "after_ack": ["durable", "ack"],
            }[offset]
            self.assertEqual(effects, expected, failure_point)
            self.assertEqual(injector.visited[-1], failure_point)


class IndependentOracleTests(unittest.TestCase):
    def test_oracle_accepts_latest_complete_checkpoint_and_transactional_output(
        self,
    ) -> None:
        report = SafetyOracle.evaluate(
            checkpoints=[
                CheckpointObservation(1, "COMPLETED"),
                CheckpointObservation(2, "PREPARED"),
            ],
            selected_recovery_checkpoint=1,
            decisions=[DecisionObservation("tx-1", "COMMIT")],
            visible_records=[
                VisibleRecord(
                    "sink-1", "tx-1", "record-1", canonical_digest({"value": 1})
                )
            ],
            sink_tier=SinkTier.TRANSACTIONAL,
        )
        self.assertTrue(report.passed)
        self.assertEqual(report.recovery_checkpoint, 1)

    def test_oracle_names_recovery_decision_payload_and_duplicate_failures(
        self,
    ) -> None:
        report = SafetyOracle.evaluate(
            checkpoints=[
                CheckpointObservation(1, "COMPLETED"),
                CheckpointObservation(2, "PREPARED"),
            ],
            selected_recovery_checkpoint=2,
            decisions=[
                DecisionObservation("tx-1", "COMMIT"),
                DecisionObservation("tx-1", "ABORT"),
            ],
            visible_records=[
                VisibleRecord("sink-1", "tx-2", "record-1", "payload-a", occurrences=2),
                VisibleRecord("sink-1", "tx-2", "record-1", "payload-b"),
            ],
            sink_tier=SinkTier.TRANSACTIONAL,
        )
        self.assertFalse(report.passed)
        self.assertEqual(
            set(report.invariants),
            {
                "latest_completed_checkpoint_selected",
                "decision_is_immutable",
                "stable_record_payload",
                "duplicate_policy_holds",
                "transactional_visibility_requires_commit",
            },
        )
        self.assertTrue(all(not result for result in report.invariants.values()))


if __name__ == "__main__":
    unittest.main()
