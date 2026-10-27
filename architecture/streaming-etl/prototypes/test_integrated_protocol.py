from __future__ import annotations

import sqlite3
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from integrated_protocol import (
    FAILURE_POINTS,
    NEXT_PHASE,
    Capability,
    CheckpointIntent,
    Coordinator,
    Fault,
    IdentitySpace,
    InjectedFailure,
    Oracle,
    Phase,
    ProtocolError,
    RecoveredSink,
    Role,
    SinkTier,
    VisibleEffect,
    negotiate,
    transition_allowed,
)
from multisink_v1 import DurableCooperativeSink, ParticipantState

RECORD_LINEAGE = {
    "partition_id": "source-p0",
    "source_position": "opaque:0007",
}


def capabilities(
    *sink_tiers: SinkTier,
    valid: bool = True,
) -> list[Capability]:
    result = [
        Capability("source", Role.SOURCE, stable_ids=True, replayable=True),
        Capability("operator", Role.OPERATOR, deterministic=True, managed_state=True),
        Capability(
            "channel",
            Role.CHANNEL,
            stable_ids=True,
            replayable=True,
            durable=True,
            fenced=True,
        ),
        Capability("state", Role.STATE_BACKEND, durable=True, fenced=True),
    ]
    for index, tier in enumerate(sink_tiers):
        sink_id = f"sink-{chr(ord('a') + index)}"
        result.append(
            Capability(
                sink_id,
                Role.SINK,
                sink_tier=tier,
                idempotent=valid and tier in {SinkTier.I, SinkTier.T},
                prepare_commit_recover=valid and tier is SinkTier.T,
                fenced=valid and tier is SinkTier.T,
            )
        )
    return result


def intent(
    *,
    job: str = "job-a",
    graph: str = "graph-v1",
    sequence: int = 1,
    capability_digest: str | None = None,
    sources: tuple[tuple[str, str], ...] = (
        ("source-p0", "opaque:0007"),
        ("source-p1", "opaque:alpha"),
    ),
) -> CheckpointIntent:
    space = IdentitySpace.create("tenant", job, {"graph": graph})
    negotiated = negotiate(capabilities(SinkTier.T, SinkTier.T), SinkTier.T)
    checkpoint_id = space.checkpoint_id(sequence)
    return CheckpointIntent(
        job_id=space.job_id,
        graph_version_id=space.graph_version_id,
        checkpoint_id=checkpoint_id,
        transaction_id=space.transaction_id(checkpoint_id),
        sequence=sequence,
        coordinator_epoch=4,
        generation_manifest=tuple(
            [("operator", 3), ("source", 4), ("sink-a", 8), ("sink-b", 9)]
            + [
                (partition_id, 10 + index)
                for index, (partition_id, _) in enumerate(sources)
            ]
        ),
        operator_manifest=("operator", "source"),
        sink_manifest=("sink-b", "sink-a"),
        source_positions=sources,
        artifact_manifest=(
            ("channel_append", "record-1", "operator"),
            ("snapshot", "operator", "operator"),
        ),
        requested_tier=SinkTier.T,
        effective_tier=SinkTier.T,
        capability_digest=capability_digest or negotiated.capability_digest,
        capability_participants=negotiated.participant_roles,
        expected_effects=(
            ("sink-a", "result", "payload-v1"),
            ("sink-b", "result", "payload-v1"),
        ),
    )


def register(coordinator: Coordinator, checkpoint: CheckpointIntent) -> None:
    coordinator.register_deployment(
        checkpoint.job_id,
        checkpoint.graph_version_id,
        negotiate(capabilities(SinkTier.T, SinkTier.T), SinkTier.T),
    )


class ExternalWorld:
    def __init__(self) -> None:
        self.prepared: set[tuple[str, str]] = set()
        self.committed: set[tuple[str, str]] = set()
        self.acked: set[tuple[str, str]] = set()
        self.visible: list[VisibleEffect] = []
        self.fence_epochs: dict[tuple[str, str], int] = {}
        self.sink_states: dict[tuple[str, str], str] = {}
        self.tokens: dict[tuple[str, str], str] = {}

    def _fence(self, scope: tuple[str, str], coordinator_epoch: int) -> None:
        previous = self.fence_epochs.get(scope, 0)
        if coordinator_epoch < previous:
            raise ProtocolError("external participant rejected stale coordinator")
        self.fence_epochs[scope] = coordinator_epoch

    def fence(
        self,
        transaction_id: str,
        sink_id: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        if participant_epoch < 1:
            raise ProtocolError("invalid participant fence")
        self._fence((transaction_id, sink_id), coordinator_epoch)

    def prepare(
        self,
        transaction_id: str,
        sink_id: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> str:
        self._fence((transaction_id, sink_id), coordinator_epoch)
        if participant_epoch < 1:
            raise ProtocolError("invalid participant fence")
        transaction_sink = (transaction_id, sink_id)
        state = self.sink_states.get(transaction_sink)
        if state not in {None, "PREPARED"}:
            raise ProtocolError(f"cannot prepare {state.lower()} transaction")
        token = f"prepare:{transaction_id}:{sink_id}"
        self.prepared.add(transaction_sink)
        self.sink_states[transaction_sink] = "PREPARED"
        self.tokens[transaction_sink] = token
        return token

    def commit(
        self,
        transaction_id: str,
        sink_id: str,
        prepare_token: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        self._fence((transaction_id, sink_id), coordinator_epoch)
        if participant_epoch < 1:
            raise ProtocolError("invalid participant fence")
        if (transaction_id, sink_id) not in self.prepared:
            raise AssertionError("commit without prepare")
        if prepare_token != f"prepare:{transaction_id}:{sink_id}":
            raise AssertionError("commit prepare token mismatch")
        transaction_sink = (transaction_id, sink_id)
        if self.sink_states.get(transaction_sink) == "ABORTED":
            raise ProtocolError("cannot commit aborted transaction")
        if transaction_sink not in self.committed:
            self.visible.append(
                VisibleEffect(transaction_id, sink_id, "result", "payload-v1")
            )
        self.committed.add(transaction_sink)
        self.sink_states[transaction_sink] = "COMMITTED"

    def abort(
        self,
        transaction_id: str,
        sink_id: str,
        prepare_token: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        self._fence((transaction_id, sink_id), coordinator_epoch)
        if participant_epoch < 1:
            raise ProtocolError("invalid participant fence")
        transaction_sink = (transaction_id, sink_id)
        if self.tokens.get(transaction_sink) != prepare_token:
            raise ProtocolError("abort prepare token mismatch")
        if self.sink_states.get(transaction_sink) == "COMMITTED":
            raise ProtocolError("cannot abort committed transaction")
        self.sink_states[transaction_sink] = "ABORTED"

    def recover(self, transaction_id: str, sink_id: str) -> RecoveredSink | None:
        transaction_sink = (transaction_id, sink_id)
        state = self.sink_states.get(transaction_sink)
        if state is None:
            return None
        return RecoveredSink(self.tokens[transaction_sink], state)

    def acknowledge(
        self,
        transaction_id: str,
        partition_id: str,
        position: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        self._fence((transaction_id, partition_id), coordinator_epoch)
        if participant_epoch < 1:
            raise ProtocolError("invalid participant fence")
        if not position:
            raise AssertionError("empty source position")
        self.acked.add((transaction_id, partition_id))


def prepare_to_decision(
    coordinator: Coordinator,
    checkpoint: CheckpointIntent,
    world: ExternalWorld,
) -> None:
    register(coordinator, checkpoint)
    coordinator.start(checkpoint)
    coordinator.transition(
        checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
    )
    coordinator.record_artifact(
        checkpoint,
        "channel_append",
        "record-1",
        "operator",
        3,
        {"v": 1},
        record_lineage=RECORD_LINEAGE,
    )
    coordinator.record_artifact(
        checkpoint, "snapshot", "operator", "operator", 3, {"cursor": 1}
    )
    coordinator.transition(
        checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
    )
    coordinator.transition(
        checkpoint, Phase.ALIGNED, Phase.PREPARING, coordinator_epoch=4
    )
    coordinator.prepare_sink(checkpoint, "sink-a", 8, world.prepare)
    coordinator.prepare_sink(checkpoint, "sink-b", 9, world.prepare)
    coordinator.transition(
        checkpoint, Phase.PREPARING, Phase.PREPARED, coordinator_epoch=4
    )


class IdentityAndCapabilityTests(unittest.TestCase):
    def test_participant_ids_are_disjoint_across_operator_sink_and_source_roles(
        self,
    ) -> None:
        base = intent()
        with self.assertRaisesRegex(ProtocolError, "disjoint across roles"):
            replace(
                base,
                source_positions=(
                    ("sink-a", "opaque:0007"),
                    ("source-p1", "opaque:alpha"),
                ),
            )
        with self.assertRaisesRegex(ProtocolError, "disjoint across roles"):
            replace(base, operator_manifest=("operator", "sink-a"))

    def test_identity_space_copies_graph_semantics_into_digest(self) -> None:
        graph = {"operators": ["source", "sink"]}
        first = IdentitySpace.create("tenant", "job", graph)
        graph["operators"].append("mutated-after-construction")
        restarted = IdentitySpace.create(
            "tenant", "job", {"operators": ["source", "sink"]}
        )
        self.assertEqual(first, restarted)
        self.assertEqual(first.job_id, restarted.job_id)
        self.assertEqual(first.graph_version_id, restarted.graph_version_id)

        edge = first.edge_id("source", "out", "sink", "in")
        partition = first.partition_id(edge, 0)
        all_ids = (
            first.job_id,
            first.graph_version_id,
            first.operator_id("source"),
            edge,
            partition,
            first.record_id(partition, "opaque:42"),
            first.checkpoint_id(7),
            first.generation_id("source", 3),
        )
        restarted_edge = restarted.edge_id("source", "out", "sink", "in")
        restarted_partition = restarted.partition_id(restarted_edge, 0)
        self.assertEqual(
            all_ids,
            (
                restarted.job_id,
                restarted.graph_version_id,
                restarted.operator_id("source"),
                restarted_edge,
                restarted_partition,
                restarted.record_id(restarted_partition, "opaque:42"),
                restarted.checkpoint_id(7),
                restarted.generation_id("source", 3),
            ),
        )
        self.assertEqual(len(all_ids), len(set(all_ids)))

    def test_job_identity_is_stable_across_graph_versions(self) -> None:
        first = IdentitySpace.create("tenant", "job", {"operators": ["a"]})
        second = IdentitySpace.create("tenant", "job", {"operators": ["a", "b"]})
        self.assertEqual(first.job_id, second.job_id)
        self.assertNotEqual(first.graph_version_id, second.graph_version_id)

    def test_every_declared_sink_tier_validates_its_own_capabilities(self) -> None:
        for tier in SinkTier:
            with self.subTest(tier=tier):
                if tier is SinkTier.B:
                    negotiated = negotiate(capabilities(tier, valid=False), SinkTier.B)
                    self.assertEqual(negotiated.tier, SinkTier.B)
                else:
                    with self.assertRaises(ProtocolError):
                        negotiate(capabilities(tier, valid=False), SinkTier.B)

    def test_mixed_sink_group_downgrades_and_rejects_stronger_request(self) -> None:
        participants = capabilities(SinkTier.T, SinkTier.I)
        negotiated = negotiate(participants, SinkTier.I)
        self.assertEqual(negotiated.tier, SinkTier.I)
        with self.assertRaisesRegex(ProtocolError, "below requested"):
            negotiate(participants, SinkTier.T)


class StateMachineTests(unittest.TestCase):
    def test_every_phase_pair_matches_the_explicit_transition_table(self) -> None:
        for current in Phase:
            for target in Phase:
                expected = current is target or NEXT_PHASE.get(current) is target
                self.assertEqual(
                    transition_allowed(current, target),
                    expected,
                    f"{current.value} -> {target.value}",
                )

    def test_compare_and_swap_rejects_competing_transition_and_stale_epoch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            first = Coordinator(path, "owner-1")
            register(first, checkpoint)
            first.start(checkpoint)
            replacement = Coordinator(path, "owner-2")
            replacement.takeover(checkpoint, 5)

            with self.assertRaisesRegex(ProtocolError, "stale"):
                first.transition(
                    checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
                )
            replacement.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=5
            )

    def test_idempotent_retry_cannot_hide_an_illegal_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            with self.assertRaisesRegex(ProtocolError, "illegal transition"):
                coordinator.transition(
                    checkpoint,
                    Phase.PREPARED,
                    Phase.ALIGNING,
                    coordinator_epoch=4,
                )

    def test_abort_is_allowed_before_and_forbidden_after_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            early = Coordinator(path)
            early_intent = intent()
            register(early, early_intent)
            early.start(early_intent)
            early.abort_checkpoint(early_intent, lambda *_: None)

            world = ExternalWorld()
            decided_intent = intent(sequence=2)
            prepare_to_decision(early, decided_intent, world)
            early.decide_commit(decided_intent)
            with self.assertRaisesRegex(ProtocolError, "forbidden after COMMIT"):
                early.abort_checkpoint(decided_intent, lambda *_: None)


class IntegratedProgressTests(unittest.TestCase):
    def test_storage_schema_rejects_ack_and_commit_without_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "requires COMMIT"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE sources SET ack_status = 'ACKED'
                    WHERE job_id = ? AND graph_version_id = ?
                      AND checkpoint_id = ? AND partition_id = 'source-p0'
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(
                    sqlite3.IntegrityError, "requires COMMIT|illegal sink status"
                ),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE sinks SET status = 'COMMITTED'
                    WHERE job_id = ? AND graph_version_id = ?
                      AND checkpoint_id = ? AND sink_id = 'sink-a'
                    """,
                    Coordinator._key(checkpoint),
                )

            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            coordinator.recover(checkpoint, world.commit, world.acknowledge)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "not monotonic"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET phase = 'PREPARED', decision = NULL
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "intent is immutable"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET manifest_digest = 'forged'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "immutable"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE sources SET checkpoint_id = 'orphan'
                    WHERE job_id = ? AND graph_version_id = ?
                      AND checkpoint_id = ? AND partition_id = 'source-p0'
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "cannot be deleted"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    DELETE FROM checkpoints
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )

    def test_capabilities_are_bound_to_checkpoint_participant_identities(self) -> None:
        checkpoint = intent()
        roles = dict(checkpoint.capability_participants)
        roles["sink-a-renamed"] = roles.pop("sink-a")
        with self.assertRaisesRegex(ProtocolError, "sink manifest"):
            replace(checkpoint, capability_participants=tuple(roles.items()))

    def test_two_jobs_and_partition_vectors_are_scoped_without_numeric_max(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            coordinator = Coordinator(path)
            world = ExternalWorld()
            first = intent(job="job-a")
            second = intent(
                job="job-b",
                sources=(("source-p0", "token:z"), ("source-p9", "token:1")),
            )
            for checkpoint in (first, second):
                prepare_to_decision(coordinator, checkpoint, world)
                coordinator.decide_commit(checkpoint)
                coordinator.recover(checkpoint, world.commit, world.acknowledge)

            self.assertEqual(
                coordinator.latest_recovery_checkpoint(
                    first.job_id, first.graph_version_id
                ),
                1,
            )
            self.assertEqual(
                coordinator.latest_recovery_checkpoint(
                    second.job_id, second.graph_version_id
                ),
                1,
            )
            self.assertEqual(
                coordinator.acknowledged_positions(
                    second.job_id, second.graph_version_id
                ),
                {"source-p0": "token:z", "source-p9": "token:1"},
            )
            self.assertNotEqual(first.transaction_id, second.transaction_id)
            self.assertEqual(len(world.committed), 4)
            observations = [
                coordinator.raw_observation(first),
                coordinator.raw_observation(second),
            ]
            self.assertTrue(
                Oracle.evaluate_global_recovery(
                    observations,
                    {
                        (first.job_id, first.graph_version_id): 1,
                        (second.job_id, second.graph_version_id): 1,
                    },
                )
            )

    def test_manifest_capability_fencing_and_sequence_drift_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            with self.assertRaisesRegex(ProtocolError, "registered deployment"):
                coordinator.start(replace(checkpoint, capability_digest="changed"))
            with self.assertRaisesRegex(ProtocolError, "out-of-order"):
                coordinator.start(intent(sequence=3))

            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            coordinator.record_artifact(
                checkpoint,
                "channel_append",
                "record-1",
                "operator",
                3,
                {"v": 1},
                record_lineage=RECORD_LINEAGE,
            )
            coordinator.record_artifact(
                checkpoint,
                "snapshot",
                "operator",
                "operator",
                3,
                {"cursor": 1},
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNED, Phase.PREPARING, coordinator_epoch=4
            )
            with self.assertRaisesRegex(ProtocolError, "stale"):
                coordinator.prepare_sink(
                    checkpoint, "sink-a", 7, ExternalWorld().prepare
                )

    def test_completed_checkpoint_and_source_ack_require_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            with self.assertRaises(ProtocolError):
                coordinator.acknowledge_source(
                    checkpoint, "source-p0", ExternalWorld().acknowledge
                )
            with self.assertRaises(ProtocolError):
                coordinator.transition(
                    checkpoint,
                    Phase.STARTED,
                    Phase.COMPLETED,
                    coordinator_epoch=4,
                )

    def test_storage_rejects_forged_commit_decision_before_prepare(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "retention protocol"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    "DELETE FROM sinks WHERE checkpoint_id = ?",
                    (checkpoint.checkpoint_id,),
                )
            with coordinator.connection:
                for phase in ("ALIGNING", "ALIGNED", "PREPARING", "PREPARED"):
                    coordinator.connection.execute(
                        """
                        UPDATE checkpoints SET phase = ?
                        WHERE job_id = ? AND graph_version_id = ?
                          AND checkpoint_id = ?
                        """,
                        (phase, *Coordinator._key(checkpoint)),
                    )
            with (
                self.assertRaisesRegex(
                    sqlite3.IntegrityError, "requires prepared participants"
                ),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints
                    SET phase = 'COMMIT_DECIDED', decision = 'COMMIT'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            self.assertIsNone(
                coordinator.latest_recovery_checkpoint(
                    checkpoint.job_id, checkpoint.graph_version_id
                )
            )

    def test_storage_rejects_abort_decision_without_fence_advance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            coordinator.record_artifact(
                checkpoint,
                "channel_append",
                "record-1",
                "operator",
                3,
                {"v": 1},
                record_lineage=RECORD_LINEAGE,
            )
            coordinator.record_artifact(
                checkpoint, "snapshot", "operator", "operator", 3, {"cursor": 1}
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNED, Phase.PREPARING, coordinator_epoch=4
            )
            with self.assertRaises(ConnectionError):
                coordinator.prepare_sink(
                    checkpoint,
                    "sink-a",
                    8,
                    lambda *_: (_ for _ in ()).throw(ConnectionError("paused")),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "not monotonic"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET fence_epoch = 3
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "fence advance"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET active_owner_id = 'forged-owner'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "fence advance"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET phase = 'ABORTING', decision = 'ABORT'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            observation = coordinator.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.PREPARING.value)
            self.assertIsNone(observation["decision"])

    def test_storage_seals_sink_manifest_after_atomic_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "manifest is sealed"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    "INSERT INTO checkpoint_sink_manifest VALUES (?, ?, ?, 'sink-x')",
                    Coordinator._key(checkpoint),
                )

    def test_storage_rejects_sink_abort_and_rollback_after_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            with (
                self.assertRaisesRegex(
                    sqlite3.IntegrityError, "illegal sink status transition"
                ),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE sinks SET status = 'ABORTED'
                    WHERE job_id = ? AND graph_version_id = ?
                      AND checkpoint_id = ? AND sink_id = 'sink-a'
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "already exists"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    INSERT OR REPLACE INTO sinks
                    VALUES (
                        ?, ?, ?, 'sink-a', 'forged', 'forged', 8, 4, 'ABORTED'
                    )
                    """,
                    Coordinator._key(checkpoint),
                )

            coordinator.recover(checkpoint, world.commit, world.acknowledge)
            with (
                self.assertRaisesRegex(
                    sqlite3.IntegrityError, "illegal sink status transition"
                ),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE sinks SET status = 'PREPARED'
                    WHERE job_id = ? AND graph_version_id = ?
                      AND checkpoint_id = ? AND sink_id = 'sink-a'
                    """,
                    Coordinator._key(checkpoint),
                )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "already exists"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    INSERT OR REPLACE INTO sinks
                    VALUES (
                        ?, ?, ?, 'sink-a', 'forged', 'forged', 8, 4, 'PENDING'
                    )
                    """,
                    Coordinator._key(checkpoint),
                )

    def test_storage_rejects_completion_with_prepared_sinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            with (
                self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "completion requires committed sinks",
                ),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET phase = 'COMPLETED'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )

    def test_storage_rejects_completed_checkpoint_phase_rollback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            coordinator.recover(checkpoint, world.commit, world.acknowledge)
            self.assertEqual(
                coordinator.raw_observation(checkpoint)["phase"], Phase.COMPLETED.value
            )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "not monotonic"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    UPDATE checkpoints SET phase = 'COMMIT_DECIDED'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )

    def test_partial_prepare_aborts_durable_participant_after_lost_ack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            coordinator = Coordinator(root / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            coordinator.record_artifact(
                checkpoint,
                "channel_append",
                "record-1",
                "operator",
                3,
                {"v": 1},
                record_lineage=RECORD_LINEAGE,
            )
            coordinator.record_artifact(
                checkpoint,
                "snapshot",
                "operator",
                "operator",
                3,
                {"cursor": 1},
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNED, Phase.PREPARING, coordinator_epoch=4
            )

            sink_a = DurableCooperativeSink(root / "sink-a.sqlite", "sink-a")
            sink_a.claim_generation(1)
            sink_a.claim_generation(8)

            def prepare_a(
                transaction_id: str,
                sink_id: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> str:
                self.assertEqual(sink_id, "sink-a")
                return sink_a.prepare(
                    transaction_id,
                    "payload-v1",
                    coordinator_epoch,
                    participant_epoch,
                )

            coordinator.prepare_sink(checkpoint, "sink-a", 8, prepare_a)
            with self.assertRaises(ConnectionError):
                coordinator.prepare_sink(
                    checkpoint,
                    "sink-b",
                    9,
                    lambda *_: (_ for _ in ()).throw(ConnectionError("offline")),
                )
            with self.assertRaisesRegex(ProtocolError, "illegal transition"):
                coordinator.transition(
                    checkpoint,
                    Phase.PREPARING,
                    Phase.ABORTED,
                    coordinator_epoch=4,
                )

            def fence_sink(
                transaction_id: str,
                sink_id: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                if sink_id == "sink-a":
                    sink_a.fence(transaction_id, coordinator_epoch, participant_epoch)

            def recover_sink(transaction_id: str, sink_id: str) -> RecoveredSink | None:
                if sink_id != "sink-a":
                    return None
                recovered = sink_a.recover(transaction_id)
                if recovered is None:
                    return None
                return RecoveredSink(recovered.token, recovered.state.value)

            def abort_after_external_effect(
                transaction_id: str,
                sink_id: str,
                prepare_token: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                self.assertEqual(sink_id, "sink-a")
                sink_a.abort(
                    transaction_id,
                    prepare_token,
                    coordinator_epoch,
                    participant_epoch,
                )
                raise InjectedFailure("lost abort acknowledgement")

            with self.assertRaisesRegex(InjectedFailure, "lost abort"):
                coordinator.abort_checkpoint(
                    checkpoint,
                    abort_after_external_effect,
                    fence=fence_sink,
                    recover=recover_sink,
                )
            self.assertEqual(
                sink_a.recover(checkpoint.transaction_id).state,
                ParticipantState.ABORTED,
            )
            self.assertEqual(
                coordinator.raw_observation(checkpoint)["sinks"]["sink-a"],
                "PREPARED",
            )

            reopened = Coordinator(root / "coordinator.sqlite")

            def retry_abort(
                transaction_id: str,
                sink_id: str,
                prepare_token: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                sink_a.abort(
                    transaction_id,
                    prepare_token,
                    coordinator_epoch,
                    participant_epoch,
                )

            reopened.abort_checkpoint(
                checkpoint,
                retry_abort,
                fence=fence_sink,
                recover=recover_sink,
            )
            observation = reopened.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.ABORTED.value)
            self.assertEqual(
                observation["sinks"],
                {"sink-a": "ABORTED", "sink-b": "ABORTED"},
            )

            next_checkpoint = intent(sequence=2)
            reopened.start(next_checkpoint)
            reopened.transition(
                next_checkpoint,
                Phase.STARTED,
                Phase.ALIGNING,
                coordinator_epoch=4,
            )
            reopened.record_artifact(
                next_checkpoint,
                "channel_append",
                "record-1",
                "operator",
                3,
                {"v": 1},
                record_lineage=RECORD_LINEAGE,
            )
            reopened.record_artifact(
                next_checkpoint,
                "snapshot",
                "operator",
                "operator",
                3,
                {"cursor": 1},
            )
            reopened.transition(
                next_checkpoint,
                Phase.ALIGNING,
                Phase.ALIGNED,
                coordinator_epoch=4,
            )
            reopened.transition(
                next_checkpoint,
                Phase.ALIGNED,
                Phase.PREPARING,
                coordinator_epoch=4,
            )
            reopened.prepare_sink(next_checkpoint, "sink-a", 8, prepare_a)
            self.assertEqual(
                sink_a.recover(next_checkpoint.transaction_id).state,
                ParticipantState.PREPARED,
            )

    def test_abort_continues_past_unavailable_sink_and_resumes_after_reopen(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            coordinator = Coordinator(path)
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)

            sink_a_offline = True

            def one_fence_unavailable(
                transaction_id: str,
                sink_id: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                if sink_id == "sink-a" and sink_a_offline:
                    raise ConnectionError("sink-a unavailable")
                world.fence(
                    transaction_id,
                    sink_id,
                    coordinator_epoch,
                    participant_epoch,
                )

            coordinator.abort_checkpoint(
                checkpoint,
                world.abort,
                fence=one_fence_unavailable,
                recover=world.recover,
            )
            observation = coordinator.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.ABORTING.value)
            self.assertEqual(observation["decision"], "ABORT")
            self.assertEqual(
                observation["sinks"],
                {"sink-a": "PREPARED", "sink-b": "ABORTED"},
            )
            self.assertEqual(
                world.recover(checkpoint.transaction_id, "sink-a").state,
                "PREPARED",
            )
            self.assertEqual(
                world.recover(checkpoint.transaction_id, "sink-b").state,
                "ABORTED",
            )
            status = coordinator.recovery_status(checkpoint)
            self.assertEqual(status["sink-a"]["attempt_count"], 1)
            self.assertTrue(status["sink-a"]["blocked"])
            self.assertIn("ConnectionError", status["sink-a"]["last_error"])
            self.assertFalse(status["sink-b"]["blocked"])

            coordinator.connection.close()
            reopened = Coordinator(path)
            sink_a_offline = False
            reopened.abort_checkpoint(
                checkpoint,
                world.abort,
                fence=one_fence_unavailable,
                recover=world.recover,
            )
            observation = reopened.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.ABORTED.value)
            self.assertEqual(observation["decision"], "ABORT")
            self.assertEqual(
                observation["sinks"],
                {"sink-a": "ABORTED", "sink-b": "ABORTED"},
            )
            self.assertEqual(
                world.recover(checkpoint.transaction_id, "sink-a").state,
                "ABORTED",
            )
            status = reopened.recovery_status(checkpoint)
            self.assertEqual(status["sink-a"]["attempt_count"], 2)
            self.assertFalse(status["sink-a"]["blocked"])
            self.assertIsNone(status["sink-a"]["last_error"])

    def test_recovery_continues_past_unavailable_sink_and_reports_blocker(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            coordinator = Coordinator(path)
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)

            def one_sink_unavailable(
                transaction_id: str,
                sink_id: str,
                prepare_token: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                if sink_id == "sink-a":
                    raise ConnectionError("sink-a unavailable")
                world.commit(
                    transaction_id,
                    sink_id,
                    prepare_token,
                    coordinator_epoch,
                    participant_epoch,
                )

            coordinator.recover(checkpoint, one_sink_unavailable, world.acknowledge)
            observation = coordinator.raw_observation(checkpoint)
            self.assertEqual(observation["sinks"]["sink-a"], "PREPARED")
            self.assertEqual(observation["sinks"]["sink-b"], "COMMITTED")
            status = coordinator.recovery_status(checkpoint)
            self.assertTrue(status["sink-a"]["blocked"])
            self.assertIn("ConnectionError", status["sink-a"]["last_error"])
            self.assertFalse(status["sink-b"]["blocked"])

            reopened = Coordinator(path)
            reopened.recover(checkpoint, world.commit, world.acknowledge)
            status = reopened.recovery_status(checkpoint)
            self.assertEqual(status["sink-a"]["attempt_count"], 2)
            self.assertFalse(status["sink-a"]["blocked"])
            self.assertIsNone(status["sink-a"]["last_error"])
            self.assertEqual(
                reopened.raw_observation(checkpoint)["phase"], Phase.COMPLETED.value
            )

    def test_stale_commit_recovery_cannot_overwrite_takeover_diagnostics(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            old = Coordinator(path, "old-owner")
            prepare_to_decision(old, checkpoint, world)
            old.decide_commit(checkpoint)
            replacement = Coordinator(path, "new-owner")

            def takeover_then_lose_callback(
                transaction_id: str,
                sink_id: str,
                prepare_token: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                self.assertEqual(sink_id, "sink-a")
                replacement.takeover(checkpoint, 5)
                replacement.recover(checkpoint, world.commit, world.acknowledge)
                raise ConnectionError("old callback lost after takeover")

            old.recover(
                checkpoint,
                takeover_then_lose_callback,
                world.acknowledge,
            )

            self.assertEqual(
                replacement.raw_observation(checkpoint)["phase"],
                Phase.COMPLETED.value,
            )
            status = replacement.recovery_status(checkpoint)
            self.assertEqual(
                set(status),
                {"sink-a", "sink-b", "source-p0", "source-p1"},
            )
            for participant in status.values():
                self.assertEqual(participant["owner_id"], "new-owner")
                self.assertEqual(participant["fence_epoch"], 5)
                self.assertFalse(participant["blocked"])
                self.assertIsNone(participant["last_error"])

    def test_stale_abort_recovery_cannot_overwrite_takeover_diagnostics(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            old = Coordinator(path, "old-owner")
            prepare_to_decision(old, checkpoint, world)
            replacement = Coordinator(path, "new-owner")

            def takeover_then_lose_callback(
                transaction_id: str,
                sink_id: str,
                prepare_token: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                self.assertEqual(sink_id, "sink-a")
                replacement.takeover(checkpoint, 6)
                replacement.abort_checkpoint(
                    checkpoint,
                    world.abort,
                    fence=world.fence,
                    recover=world.recover,
                )
                raise ConnectionError("old callback lost after takeover")

            with self.assertRaisesRegex(ProtocolError, "stale checkpoint abort"):
                old.abort_checkpoint(
                    checkpoint,
                    takeover_then_lose_callback,
                    fence=world.fence,
                    recover=world.recover,
                )

            observation = replacement.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.ABORTED.value)
            self.assertEqual(observation["decision"], "ABORT")
            status = replacement.recovery_status(checkpoint)
            self.assertEqual(set(status), {"sink-a", "sink-b"})
            for participant in status.values():
                self.assertEqual(participant["owner_id"], "new-owner")
                self.assertEqual(participant["fence_epoch"], 6)
                self.assertFalse(participant["blocked"])
                self.assertIsNone(participant["last_error"])

    def test_sink_attempt_start_survives_crash_before_result_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            coordinator = Coordinator(path)
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            calls = 0
            failure_point = "sink_recovery_attempt:after_callback_before_result"

            def unavailable_sink(*_: object) -> None:
                nonlocal calls
                calls += 1
                raise ConnectionError("sink-a unavailable")

            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.recover(
                    checkpoint,
                    unavailable_sink,
                    world.acknowledge,
                    attempt_fault=Fault(failure_point),
                )
            status = coordinator.recovery_status(checkpoint)["sink-a"]
            self.assertEqual(status["attempt_count"], 1)
            self.assertEqual(status["outcome"], "IN_PROGRESS")
            self.assertTrue(status["blocked"])
            self.assertIn("outcome unknown", status["last_error"])

            coordinator.connection.close()
            reopened = Coordinator(path)

            def recovered_sink(*args: object) -> None:
                nonlocal calls
                if args[1] == "sink-a":
                    calls += 1
                world.commit(*args)

            reopened.recover(checkpoint, recovered_sink, world.acknowledge)
            status = reopened.recovery_status(checkpoint)["sink-a"]
            self.assertEqual(calls, 2)
            self.assertEqual(status["attempt_count"], 2)
            self.assertEqual(status["outcome"], "SUCCEEDED")
            self.assertFalse(status["blocked"])

    def test_source_attempt_start_survives_crash_before_result_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            coordinator = Coordinator(path)
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            coordinator.commit_sink(checkpoint, "sink-a", 8, world.commit)
            coordinator.commit_sink(checkpoint, "sink-b", 9, world.commit)
            calls = 0
            failure_point = "source_recovery_attempt:after_callback_before_result"

            def unavailable_source(*_: object) -> None:
                nonlocal calls
                calls += 1
                raise ConnectionError("source-p0 unavailable")

            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.recover(
                    checkpoint,
                    world.commit,
                    unavailable_source,
                    attempt_fault=Fault(failure_point),
                )
            status = coordinator.recovery_status(checkpoint)["source-p0"]
            self.assertEqual(status["attempt_count"], 1)
            self.assertEqual(status["outcome"], "IN_PROGRESS")

            coordinator.connection.close()
            reopened = Coordinator(path)

            def recovered_source(*args: object) -> None:
                nonlocal calls
                if args[1] == "source-p0":
                    calls += 1
                world.acknowledge(*args)

            reopened.recover(checkpoint, world.commit, recovered_source)
            status = reopened.recovery_status(checkpoint)["source-p0"]
            self.assertEqual(calls, 2)
            self.assertEqual(status["attempt_count"], 2)
            self.assertEqual(status["outcome"], "SUCCEEDED")

    def test_abort_attempt_start_survives_crash_before_result_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            coordinator = Coordinator(path)
            prepare_to_decision(coordinator, checkpoint, world)
            calls = 0
            failure_point = "abort_recovery_attempt:after_callback_before_result"

            def unavailable_abort(*_: object) -> None:
                nonlocal calls
                calls += 1
                raise ConnectionError("sink-a unavailable")

            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.abort_checkpoint(
                    checkpoint,
                    unavailable_abort,
                    fence=world.fence,
                    recover=world.recover,
                    attempt_fault=Fault(failure_point),
                )
            status = coordinator.recovery_status(checkpoint)["sink-a"]
            self.assertEqual(status["attempt_count"], 1)
            self.assertEqual(status["outcome"], "IN_PROGRESS")

            coordinator.connection.close()
            reopened = Coordinator(path)

            def recovered_abort(*args: object) -> None:
                nonlocal calls
                if args[1] == "sink-a":
                    calls += 1
                world.abort(*args)

            reopened.abort_checkpoint(
                checkpoint,
                recovered_abort,
                fence=world.fence,
                recover=world.recover,
            )
            status = reopened.recovery_status(checkpoint)["sink-a"]
            self.assertEqual(calls, 2)
            self.assertEqual(status["attempt_count"], 2)
            self.assertEqual(status["outcome"], "SUCCEEDED")
            self.assertEqual(
                reopened.raw_observation(checkpoint)["phase"], Phase.ABORTED.value
            )

    def test_terminal_sink_reconciles_crash_before_success_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            coordinator = Coordinator(path)
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            failure_point = "sink_recovery_attempt:after_callback_before_result"

            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.recover(
                    checkpoint,
                    world.commit,
                    world.acknowledge,
                    attempt_fault=Fault(failure_point),
                )
            self.assertEqual(
                coordinator.raw_observation(checkpoint)["sinks"]["sink-a"],
                "COMMITTED",
            )
            self.assertEqual(
                coordinator.recovery_status(checkpoint)["sink-a"]["outcome"],
                "IN_PROGRESS",
            )

            coordinator.connection.close()
            reopened = Coordinator(path)
            reopened.recover(checkpoint, world.commit, world.acknowledge)
            status = reopened.recovery_status(checkpoint)["sink-a"]
            self.assertEqual(status["attempt_count"], 1)
            self.assertEqual(status["outcome"], "SUCCEEDED")
            self.assertFalse(status["blocked"])
            self.assertEqual(
                reopened.raw_observation(checkpoint)["phase"], Phase.COMPLETED.value
            )

    def test_terminal_source_reconciles_crash_before_success_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            coordinator = Coordinator(path)
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            coordinator.commit_sink(checkpoint, "sink-a", 8, world.commit)
            coordinator.commit_sink(checkpoint, "sink-b", 9, world.commit)
            failure_point = "source_recovery_attempt:after_callback_before_result"

            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.recover(
                    checkpoint,
                    world.commit,
                    world.acknowledge,
                    attempt_fault=Fault(failure_point),
                )
            self.assertEqual(
                coordinator.raw_observation(checkpoint)["sources"]["source-p0"][
                    "status"
                ],
                "ACKED",
            )
            self.assertEqual(
                coordinator.recovery_status(checkpoint)["source-p0"]["outcome"],
                "IN_PROGRESS",
            )

            coordinator.connection.close()
            reopened = Coordinator(path)
            reopened.recover(checkpoint, world.commit, world.acknowledge)
            status = reopened.recovery_status(checkpoint)["source-p0"]
            self.assertEqual(status["attempt_count"], 1)
            self.assertEqual(status["outcome"], "SUCCEEDED")
            self.assertFalse(status["blocked"])

    def test_terminal_abort_reconciles_crash_before_success_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            coordinator = Coordinator(path)
            prepare_to_decision(coordinator, checkpoint, world)
            failure_point = "abort_recovery_attempt:after_callback_before_result"

            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.abort_checkpoint(
                    checkpoint,
                    world.abort,
                    fence=world.fence,
                    recover=world.recover,
                    attempt_fault=Fault(failure_point),
                )
            self.assertEqual(
                coordinator.raw_observation(checkpoint)["sinks"]["sink-a"],
                "ABORTED",
            )
            self.assertEqual(
                coordinator.recovery_status(checkpoint)["sink-a"]["outcome"],
                "IN_PROGRESS",
            )

            coordinator.connection.close()
            reopened = Coordinator(path)
            reopened.abort_checkpoint(
                checkpoint,
                world.abort,
                fence=world.fence,
                recover=world.recover,
            )
            status = reopened.recovery_status(checkpoint)["sink-a"]
            self.assertEqual(status["attempt_count"], 1)
            self.assertEqual(status["outcome"], "SUCCEEDED")
            self.assertFalse(status["blocked"])
            self.assertEqual(
                reopened.raw_observation(checkpoint)["phase"], Phase.ABORTED.value
            )

    def test_checkpoint_cannot_align_or_decide_without_required_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            with self.assertRaisesRegex(ProtocolError, "required artifacts"):
                coordinator.transition(
                    checkpoint,
                    Phase.ALIGNING,
                    Phase.ALIGNED,
                    coordinator_epoch=4,
                )

    def test_artifact_identity_includes_owner_and_payload_is_restorable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            base = intent()
            checkpoint = replace(
                base,
                artifact_manifest=(
                    ("snapshot", "shared", "operator"),
                    ("snapshot", "shared", "source"),
                ),
            )
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            coordinator.record_artifact(
                checkpoint, "snapshot", "shared", "operator", 3, {"owner": "operator"}
            )
            with self.assertRaisesRegex(ProtocolError, "required artifacts"):
                coordinator.transition(
                    checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
                )
            coordinator.record_artifact(
                checkpoint, "snapshot", "shared", "source", 4, {"owner": "source"}
            )
            self.assertEqual(
                coordinator.load_artifact(checkpoint, "snapshot", "shared", "operator"),
                {"owner": "operator"},
            )
            self.assertEqual(
                coordinator.load_artifact(checkpoint, "snapshot", "shared", "source"),
                {"owner": "source"},
            )
            coordinator.transition(
                checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
            )

    def test_stable_record_payload_conflict_is_global_across_checkpoints(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            first = intent()
            register(coordinator, first)
            coordinator.start(first)
            coordinator.transition(
                first, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            coordinator.record_artifact(
                first,
                "channel_append",
                "record-1",
                "operator",
                3,
                {"v": 1},
                record_lineage=RECORD_LINEAGE,
            )
            coordinator.abort_checkpoint(first, lambda *_: None)

            second = intent(sequence=2)
            coordinator.start(second)
            coordinator.transition(
                second, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            with self.assertRaisesRegex(ProtocolError, "record identity or payload"):
                coordinator.record_artifact(
                    second,
                    "channel_append",
                    "record-1",
                    "operator",
                    3,
                    {"v": 2},
                    record_lineage=RECORD_LINEAGE,
                )

            third = intent(sequence=3)
            coordinator.start(third)
            coordinator.transition(
                third, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            with self.assertRaisesRegex(ProtocolError, "record identity or payload"):
                coordinator.record_artifact(
                    third,
                    "channel_append",
                    "record-1",
                    "operator",
                    3,
                    {"v": 1},
                    record_lineage={
                        "partition_id": "source-p1",
                        "source_position": "opaque:alpha",
                    },
                )

    def test_commit_decision_survives_external_effect_before_local_ack(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            failure_point = "sink_commit:after_durable_before_ack"
            with self.assertRaisesRegex(InjectedFailure, failure_point):
                coordinator.commit_sink(
                    checkpoint,
                    "sink-a",
                    8,
                    world.commit,
                    Fault(failure_point),
                )
            self.assertIn((checkpoint.transaction_id, "sink-a"), world.committed)
            self.assertEqual(
                coordinator.raw_observation(checkpoint)["sinks"]["sink-a"],
                "PREPARED",
            )
            with (
                self.assertRaisesRegex(sqlite3.IntegrityError, "cannot be deleted"),
                coordinator.connection,
            ):
                coordinator.connection.execute(
                    """
                    DELETE FROM checkpoints
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    Coordinator._key(checkpoint),
                )
            coordinator.recover(checkpoint, world.commit, world.acknowledge)
            self.assertEqual(len(world.visible), 2)

    def test_takeover_between_validation_and_commit_decision_fences_old_owner(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            old = Coordinator(path, "old-owner")
            prepare_to_decision(old, checkpoint, world)
            replacement = Coordinator(path, "new-owner")

            class TakeoverBeforeWrite(Fault):
                def hit(self, point: str) -> None:
                    if point == "commit_decision:before":
                        replacement.takeover(checkpoint, 5)

            with self.assertRaisesRegex(ProtocolError, "stale commit decision"):
                old.decide_commit(checkpoint, TakeoverBeforeWrite())
            observation = replacement.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.PREPARED.value)
            self.assertIsNone(observation["decision"])

    def test_abort_decision_linearizes_against_same_owner_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            old = Coordinator(path, "old-owner")
            prepare_to_decision(old, checkpoint, world)

            def abort_after_decision_claim(
                transaction_id: str,
                sink_id: str,
                prepare_token: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> None:
                with self.assertRaisesRegex(ProtocolError, "requires PREPARED"):
                    old.decide_commit(checkpoint)
                world.abort(
                    transaction_id,
                    sink_id,
                    prepare_token,
                    coordinator_epoch,
                    participant_epoch,
                )

            old.abort_checkpoint(
                checkpoint,
                abort_after_decision_claim,
                fence=world.fence,
                recover=world.recover,
            )
            observation = old.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.ABORTED.value)
            self.assertEqual(observation["decision"], "ABORT")
            self.assertEqual(
                {state for state in world.sink_states.values()}, {"ABORTED"}
            )

    def test_same_owner_abort_fences_late_external_prepare(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            world = ExternalWorld()
            old = Coordinator(path, "old-owner")
            register(old, checkpoint)
            old.start(checkpoint)
            old.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            old.record_artifact(
                checkpoint,
                "channel_append",
                "record-1",
                "operator",
                3,
                {"v": 1},
                record_lineage=RECORD_LINEAGE,
            )
            old.record_artifact(
                checkpoint, "snapshot", "operator", "operator", 3, {"cursor": 1}
            )
            old.transition(
                checkpoint, Phase.ALIGNING, Phase.ALIGNED, coordinator_epoch=4
            )
            old.transition(
                checkpoint, Phase.ALIGNED, Phase.PREPARING, coordinator_epoch=4
            )

            def prepare_after_abort_claim(
                transaction_id: str,
                sink_id: str,
                coordinator_epoch: int,
                participant_epoch: int,
            ) -> str:
                old.abort_checkpoint(
                    checkpoint,
                    world.abort,
                    fence=world.fence,
                    recover=world.recover,
                )
                return world.prepare(
                    transaction_id,
                    sink_id,
                    coordinator_epoch,
                    participant_epoch,
                )

            with self.assertRaisesRegex(ProtocolError, "stale coordinator"):
                old.prepare_sink(
                    checkpoint,
                    "sink-a",
                    8,
                    prepare_after_abort_claim,
                )
            observation = old.raw_observation(checkpoint)
            self.assertEqual(observation["phase"], Phase.ABORTED.value)
            self.assertEqual(
                observation["sinks"],
                {"sink-a": "ABORTED", "sink-b": "ABORTED"},
            )
            self.assertIsNone(world.recover(checkpoint.transaction_id, "sink-a"))

    def test_oracle_rejects_completed_checkpoint_without_expected_effects(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            coordinator.recover(checkpoint, world.commit, world.acknowledge)
            report = Oracle.evaluate(
                coordinator.raw_observation(checkpoint),
                selected_sequence=1,
                externally_committed_sinks=world.committed,
                externally_acked_sources=world.acked,
                visible_effects=(),
            )
            self.assertFalse(report.invariants["visible_effects_are_complete"])

    def test_oracle_rejects_visible_effects_before_commit_decision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            visible = tuple(
                VisibleEffect(checkpoint.transaction_id, *effect)
                for effect in checkpoint.expected_effects
            )
            report = Oracle.evaluate(
                coordinator.raw_observation(checkpoint),
                selected_sequence=None,
                externally_committed_sinks=set(),
                externally_acked_sources=set(),
                visible_effects=visible,
            )
            self.assertFalse(
                report.invariants["external_sink_effect_requires_decision"]
            )
            self.assertFalse(report.invariants["visible_effects_are_complete"])
            self.assertFalse(report.passed)

    def test_oracle_requires_effects_for_each_externally_committed_sink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            world = ExternalWorld()
            prepare_to_decision(coordinator, checkpoint, world)
            coordinator.decide_commit(checkpoint)
            report = Oracle.evaluate(
                coordinator.raw_observation(checkpoint),
                selected_sequence=1,
                externally_committed_sinks={
                    (checkpoint.transaction_id, "sink-a"),
                    (checkpoint.transaction_id, "sink-outside-manifest"),
                },
                externally_acked_sources={
                    (checkpoint.transaction_id, "source-outside-manifest")
                },
                visible_effects=(),
            )
            self.assertFalse(report.invariants["committed_sink_effects_are_complete"])
            self.assertFalse(
                report.invariants["external_sink_participants_are_expected"]
            )
            self.assertFalse(
                report.invariants["external_source_participants_are_expected"]
            )
            self.assertFalse(report.passed)

    def test_oracle_reports_missing_participant_rows_without_raising(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            with coordinator.connection:
                coordinator.connection.execute(
                    """
                    DELETE FROM sources
                    WHERE job_id = ? AND graph_version_id = ?
                      AND checkpoint_id = ? AND partition_id = 'source-p0'
                    """,
                    Coordinator._key(checkpoint),
                )
            missing_source = Oracle.evaluate(
                coordinator.raw_observation(checkpoint),
                selected_sequence=None,
                externally_committed_sinks=set(),
                externally_acked_sources=set(),
            )
            self.assertFalse(missing_source.invariants["participant_sets_are_complete"])
            self.assertFalse(missing_source.passed)

        missing_sink_observation = {
            "checkpoint_id": "checkpoint-1",
            "transaction_id": "transaction-1",
            "sequence": 1,
            "phase": "COMPLETED",
            "decision": "COMMIT",
            "required_sinks": ("sink-a", "sink-b"),
            "sinks": {"sink-a": "COMMITTED"},
            "required_sources": {},
            "sources": {},
            "required_artifacts": set(),
            "artifacts": set(),
            "expected_effects": set(),
            "effective_tier": "T",
        }
        missing_sink = Oracle.evaluate(
            missing_sink_observation,
            selected_sequence=1,
            externally_committed_sinks={
                ("transaction-1", "sink-a"),
                ("transaction-1", "sink-b"),
            },
            externally_acked_sources=set(),
        )
        self.assertFalse(missing_sink.invariants["completed_has_every_sink"])
        self.assertFalse(missing_sink.invariants["participant_sets_are_complete"])
        self.assertFalse(missing_sink.passed)

    def test_artifacts_reject_orphans_manifest_drift_and_aborted_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            coordinator = Coordinator(Path(directory) / "coordinator.sqlite")
            checkpoint = intent()
            with self.assertRaises(KeyError):
                coordinator.record_artifact(
                    checkpoint,
                    "snapshot",
                    "operator",
                    "operator",
                    3,
                    {"cursor": 1},
                )

            register(coordinator, checkpoint)
            coordinator.start(checkpoint)
            coordinator.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            with self.assertRaisesRegex(ProtocolError, "manifest"):
                coordinator.record_artifact(
                    checkpoint,
                    "snapshot",
                    "forged",
                    "operator",
                    3,
                    {"cursor": 1},
                )
            with self.assertRaisesRegex(ProtocolError, "stale"):
                coordinator.record_artifact(
                    checkpoint,
                    "snapshot",
                    "operator",
                    "operator",
                    99,
                    {"cursor": 1},
                )
            coordinator.abort_checkpoint(checkpoint, lambda *_: None)
            with self.assertRaisesRegex(ProtocolError, "ALIGNING"):
                coordinator.record_artifact(
                    checkpoint,
                    "snapshot",
                    "operator",
                    "operator",
                    3,
                    {"cursor": 1},
                )

    def test_takeover_fences_old_owner_from_artifacts_and_external_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "coordinator.sqlite"
            checkpoint = intent()
            old = Coordinator(path, "old-owner")
            register(old, checkpoint)
            old.start(checkpoint)
            old.transition(
                checkpoint, Phase.STARTED, Phase.ALIGNING, coordinator_epoch=4
            )
            new = Coordinator(path, "new-owner")
            new.takeover(checkpoint, 5)
            with self.assertRaisesRegex(ProtocolError, "stale coordinator"):
                old.record_artifact(
                    checkpoint,
                    "snapshot",
                    "operator",
                    "operator",
                    3,
                    {"cursor": 1},
                )
            new.record_artifact(
                checkpoint,
                "snapshot",
                "operator",
                "operator",
                3,
                {"cursor": 1},
            )

    def test_unsafe_ack_and_missing_sink_controls_fail_named_oracle_invariants(
        self,
    ) -> None:
        observation = {
            "checkpoint_id": "checkpoint-1",
            "transaction_id": "transaction-1",
            "sequence": 1,
            "phase": "COMPLETED",
            "decision": None,
            "required_sinks": ("sink-a", "sink-b"),
            "sinks": {"sink-a": "COMMITTED", "sink-b": "PREPARED"},
            "required_sources": {"source-p0": "token"},
            "sources": {"source-p0": {"position": "token", "status": "PENDING"}},
            "required_artifacts": {("snapshot", "operator")},
            "artifacts": set(),
            "effective_tier": "T",
        }
        report = Oracle.evaluate(
            observation,
            selected_sequence=1,
            externally_committed_sinks={("transaction-1", "sink-a")},
            externally_acked_sources={("transaction-1", "source-p0")},
            visible_effects=(
                VisibleEffect("transaction-1", "sink-a", "record-1", "payload-a"),
                VisibleEffect("transaction-1", "sink-a", "record-1", "payload-b"),
            ),
        )
        self.assertFalse(report.invariants["selected_checkpoint_has_decision"])
        self.assertFalse(report.invariants["external_sink_effect_requires_decision"])
        self.assertFalse(report.invariants["external_source_ack_requires_decision"])
        self.assertFalse(report.invariants["completed_has_every_sink"])
        self.assertFalse(report.invariants["local_source_ack_matches_external"])
        self.assertFalse(
            report.invariants["decided_checkpoint_has_restorable_artifacts"]
        )
        self.assertFalse(report.invariants["visible_effect_payload_is_stable"])
        self.assertFalse(report.invariants["visible_duplicate_policy_holds"])


class FullFailureMatrixTests(unittest.TestCase):
    def test_every_guarded_predecision_phase_edge_recovers_at_each_offset(
        self,
    ) -> None:
        edges = (
            (Phase.STARTED, Phase.ALIGNING),
            (Phase.ALIGNING, Phase.ALIGNED),
            (Phase.ALIGNED, Phase.PREPARING),
            (Phase.PREPARING, Phase.PREPARED),
        )
        for expected, target in edges:
            for offset in ("before", "after_durable_before_ack", "after_ack"):
                failure_point = f"phase_transition:{offset}"
                with (
                    self.subTest(
                        edge=f"{expected.value}->{target.value}", offset=offset
                    ),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    path = Path(directory) / "coordinator.sqlite"
                    checkpoint = intent()
                    world = ExternalWorld()
                    coordinator = Coordinator(path, "owner-1")
                    register(coordinator, checkpoint)
                    coordinator.start(checkpoint)
                    if expected is not Phase.STARTED:
                        coordinator.transition(
                            checkpoint,
                            Phase.STARTED,
                            Phase.ALIGNING,
                            coordinator_epoch=4,
                        )
                    if expected in {
                        Phase.ALIGNING,
                        Phase.ALIGNED,
                        Phase.PREPARING,
                        Phase.PREPARED,
                    }:
                        coordinator.record_artifact(
                            checkpoint,
                            "channel_append",
                            "record-1",
                            "operator",
                            3,
                            {"v": 1},
                            record_lineage=RECORD_LINEAGE,
                        )
                        coordinator.record_artifact(
                            checkpoint,
                            "snapshot",
                            "operator",
                            "operator",
                            3,
                            {"cursor": 1},
                        )
                        if expected is not Phase.ALIGNING:
                            coordinator.transition(
                                checkpoint,
                                Phase.ALIGNING,
                                Phase.ALIGNED,
                                coordinator_epoch=4,
                            )
                    if expected in {Phase.PREPARING, Phase.PREPARED}:
                        coordinator.transition(
                            checkpoint,
                            Phase.ALIGNED,
                            Phase.PREPARING,
                            coordinator_epoch=4,
                        )
                        coordinator.prepare_sink(checkpoint, "sink-a", 8, world.prepare)
                        coordinator.prepare_sink(checkpoint, "sink-b", 9, world.prepare)
                    if expected is Phase.PREPARED:
                        coordinator.transition(
                            checkpoint,
                            Phase.PREPARING,
                            Phase.PREPARED,
                            coordinator_epoch=4,
                        )
                    with self.assertRaisesRegex(InjectedFailure, failure_point):
                        coordinator.transition(
                            checkpoint,
                            expected,
                            target,
                            coordinator_epoch=4,
                            fault=Fault(failure_point),
                        )

                    reopened = Coordinator(path, "owner-2")
                    reopened.takeover(checkpoint, 5)
                    reopened.transition(
                        checkpoint, expected, target, coordinator_epoch=5
                    )
                    self.assertEqual(
                        reopened.raw_observation(checkpoint)["phase"], target.value
                    )

    def test_actual_protocol_recovers_at_every_named_failure_point(self) -> None:
        for failure_point in FAILURE_POINTS:
            with (
                self.subTest(failure_point=failure_point),
                tempfile.TemporaryDirectory() as directory,
            ):
                path = Path(directory) / "coordinator.sqlite"
                checkpoint = intent()
                world = ExternalWorld()
                coordinator = Coordinator(path, "owner-1")
                register(coordinator, checkpoint)
                boundary = failure_point.split(":", 1)[0]

                def run_target(
                    current: Coordinator,
                    fault: Fault | None,
                    epoch: int,
                    *,
                    selected_boundary: str = boundary,
                    selected_checkpoint: CheckpointIntent = checkpoint,
                    selected_world: ExternalWorld = world,
                ) -> None:
                    if selected_boundary == "checkpoint_decision":
                        current.start(selected_checkpoint, fault)
                    elif selected_boundary == "phase_transition":
                        current.transition(
                            selected_checkpoint,
                            Phase.STARTED,
                            Phase.ALIGNING,
                            coordinator_epoch=epoch,
                            fault=fault,
                        )
                    elif selected_boundary == "channel_append":
                        current.record_artifact(
                            selected_checkpoint,
                            "channel_append",
                            "record-1",
                            "operator",
                            3,
                            {"v": 1},
                            fault,
                            record_lineage=RECORD_LINEAGE,
                        )
                    elif selected_boundary == "snapshot":
                        current.record_artifact(
                            selected_checkpoint,
                            "snapshot",
                            "operator",
                            "operator",
                            3,
                            {"cursor": 1},
                            fault,
                        )
                    elif selected_boundary == "sink_prepare":
                        current.prepare_sink(
                            selected_checkpoint,
                            "sink-a",
                            8,
                            selected_world.prepare,
                            fault,
                        )
                    elif selected_boundary == "commit_decision":
                        if (
                            current.raw_observation(selected_checkpoint)["decision"]
                            is None
                        ):
                            current.refence_prepared_sinks(
                                selected_checkpoint,
                                selected_world.fence,
                                selected_world.recover,
                            )
                        current.decide_commit(selected_checkpoint, fault)
                    elif selected_boundary == "sink_commit":
                        current.commit_sink(
                            selected_checkpoint,
                            "sink-a",
                            8,
                            selected_world.commit,
                            fault,
                        )
                    elif selected_boundary == "checkpoint_completion":
                        current.commit_sink(
                            selected_checkpoint,
                            "sink-b",
                            9,
                            selected_world.commit,
                            completion_fault=fault,
                        )
                    elif selected_boundary == "source_ack":
                        current.acknowledge_source(
                            selected_checkpoint,
                            "source-p0",
                            selected_world.acknowledge,
                            fault,
                        )
                    else:
                        raise AssertionError(selected_boundary)

                if boundary != "checkpoint_decision":
                    coordinator.start(checkpoint)
                if boundary not in {"checkpoint_decision", "phase_transition"}:
                    coordinator.transition(
                        checkpoint,
                        Phase.STARTED,
                        Phase.ALIGNING,
                        coordinator_epoch=4,
                    )
                if boundary not in {
                    "checkpoint_decision",
                    "phase_transition",
                    "channel_append",
                }:
                    coordinator.record_artifact(
                        checkpoint,
                        "channel_append",
                        "record-1",
                        "operator",
                        3,
                        {"v": 1},
                        record_lineage=RECORD_LINEAGE,
                    )
                if boundary not in {
                    "checkpoint_decision",
                    "phase_transition",
                    "channel_append",
                    "snapshot",
                }:
                    coordinator.record_artifact(
                        checkpoint,
                        "snapshot",
                        "operator",
                        "operator",
                        3,
                        {"cursor": 1},
                    )
                    coordinator.transition(
                        checkpoint,
                        Phase.ALIGNING,
                        Phase.ALIGNED,
                        coordinator_epoch=4,
                    )
                    coordinator.transition(
                        checkpoint,
                        Phase.ALIGNED,
                        Phase.PREPARING,
                        coordinator_epoch=4,
                    )
                if boundary not in {
                    "checkpoint_decision",
                    "phase_transition",
                    "channel_append",
                    "snapshot",
                    "sink_prepare",
                }:
                    coordinator.prepare_sink(checkpoint, "sink-a", 8, world.prepare)
                if boundary in {
                    "commit_decision",
                    "sink_commit",
                    "checkpoint_completion",
                    "source_ack",
                }:
                    coordinator.prepare_sink(checkpoint, "sink-b", 9, world.prepare)
                    coordinator.transition(
                        checkpoint,
                        Phase.PREPARING,
                        Phase.PREPARED,
                        coordinator_epoch=4,
                    )
                if boundary in {"sink_commit", "checkpoint_completion", "source_ack"}:
                    coordinator.decide_commit(checkpoint)
                if boundary == "checkpoint_completion":
                    coordinator.commit_sink(checkpoint, "sink-a", 8, world.commit)
                if boundary == "source_ack":
                    coordinator.commit_sink(checkpoint, "sink-a", 8, world.commit)
                    coordinator.commit_sink(checkpoint, "sink-b", 9, world.commit)

                with self.assertRaisesRegex(InjectedFailure, failure_point):
                    run_target(coordinator, Fault(failure_point), 4)

                reopened = Coordinator(path, "owner-2")
                try:
                    reopened.raw_observation(checkpoint)
                except KeyError:
                    recovery_epoch = 4
                else:
                    reopened.takeover(checkpoint, 5)
                    recovery_epoch = 5
                run_target(reopened, None, recovery_epoch)

                phase = reopened.raw_observation(checkpoint)["phase"]
                if phase == Phase.STARTED.value:
                    reopened.transition(
                        checkpoint,
                        Phase.STARTED,
                        Phase.ALIGNING,
                        coordinator_epoch=recovery_epoch,
                    )
                    phase = Phase.ALIGNING.value
                if phase == Phase.ALIGNING.value:
                    reopened.record_artifact(
                        checkpoint,
                        "channel_append",
                        "record-1",
                        "operator",
                        3,
                        {"v": 1},
                        record_lineage=RECORD_LINEAGE,
                    )
                    reopened.record_artifact(
                        checkpoint,
                        "snapshot",
                        "operator",
                        "operator",
                        3,
                        {"cursor": 1},
                    )
                    reopened.transition(
                        checkpoint,
                        Phase.ALIGNING,
                        Phase.ALIGNED,
                        coordinator_epoch=recovery_epoch,
                    )
                    reopened.transition(
                        checkpoint,
                        Phase.ALIGNED,
                        Phase.PREPARING,
                        coordinator_epoch=recovery_epoch,
                    )
                phase = reopened.raw_observation(checkpoint)["phase"]
                if phase == Phase.PREPARING.value:
                    reopened.prepare_sink(checkpoint, "sink-a", 8, world.prepare)
                    reopened.prepare_sink(checkpoint, "sink-b", 9, world.prepare)
                    reopened.transition(
                        checkpoint,
                        Phase.PREPARING,
                        Phase.PREPARED,
                        coordinator_epoch=recovery_epoch,
                    )
                if (
                    reopened.raw_observation(checkpoint)["phase"]
                    == Phase.PREPARED.value
                ):
                    reopened.decide_commit(checkpoint)
                reopened.recover(checkpoint, world.commit, world.acknowledge)

                observation = reopened.raw_observation(checkpoint)
                report = Oracle.evaluate(
                    observation,
                    selected_sequence=reopened.latest_recovery_checkpoint(
                        checkpoint.job_id, checkpoint.graph_version_id
                    ),
                    externally_committed_sinks=world.committed,
                    externally_acked_sources=world.acked,
                    visible_effects=tuple(world.visible),
                )
                self.assertTrue(report.passed, report.invariants)


if __name__ == "__main__":
    unittest.main()
