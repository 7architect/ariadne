"""Integrated Sprint 1 protocol model.

This is executable architecture evidence, not Prefect production code.  A
single SQLite row is the authority for checkpoint lifecycle and commit decision;
participant tables track replayable sink commits and source acknowledgements.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class ProtocolError(RuntimeError):
    pass


class InjectedFailure(RuntimeError):
    pass


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class Phase(str, Enum):
    STARTED = "STARTED"
    ALIGNING = "ALIGNING"
    ALIGNED = "ALIGNED"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMIT_DECIDED = "COMMIT_DECIDED"
    COMMITTING = "COMMITTING"
    COMPLETED = "COMPLETED"
    ABORTING = "ABORTING"
    ABORTED = "ABORTED"


NEXT_PHASE: dict[Phase, Phase] = {
    Phase.STARTED: Phase.ALIGNING,
    Phase.ALIGNING: Phase.ALIGNED,
    Phase.ALIGNED: Phase.PREPARING,
    Phase.PREPARING: Phase.PREPARED,
    Phase.PREPARED: Phase.COMMIT_DECIDED,
    Phase.COMMIT_DECIDED: Phase.COMMITTING,
    Phase.COMMITTING: Phase.COMPLETED,
}
ABORTABLE = {
    Phase.STARTED,
    Phase.ALIGNING,
    Phase.ALIGNED,
    Phase.PREPARING,
    Phase.PREPARED,
}


def transition_allowed(current: Phase, target: Phase) -> bool:
    if current is target:
        return True
    return NEXT_PHASE.get(current) is target


class Role(str, Enum):
    SOURCE = "source"
    OPERATOR = "operator"
    CHANNEL = "channel"
    STATE_BACKEND = "state_backend"
    SINK = "sink"


class SinkTier(str, Enum):
    B = "B"
    I = "I"
    T = "T"


TIER_RANK = {SinkTier.B: 0, SinkTier.I: 1, SinkTier.T: 2}


@dataclass(frozen=True)
class Capability:
    participant_id: str
    role: Role
    version: int = 1
    stable_ids: bool = False
    replayable: bool = False
    deterministic: bool = False
    managed_state: bool = False
    durable: bool = False
    fenced: bool = False
    idempotent: bool = False
    prepare_commit_recover: bool = False
    sink_tier: SinkTier | None = None

    @property
    def manifest(self) -> Mapping[str, Any]:
        return {
            "participant_id": self.participant_id,
            "role": self.role.value,
            "version": self.version,
            "stable_ids": self.stable_ids,
            "replayable": self.replayable,
            "deterministic": self.deterministic,
            "managed_state": self.managed_state,
            "durable": self.durable,
            "fenced": self.fenced,
            "idempotent": self.idempotent,
            "prepare_commit_recover": self.prepare_commit_recover,
            "sink_tier": self.sink_tier.value if self.sink_tier else None,
        }


@dataclass(frozen=True)
class Negotiated:
    protocol_version: int
    requested_tier: SinkTier
    tier: SinkTier
    capability_digest: str
    participant_roles: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class RecoveredSink:
    token: str
    state: str


def negotiate(
    participants: Sequence[Capability],
    minimum_tier: SinkTier,
    *,
    version: int = 1,
) -> Negotiated:
    ids = [participant.participant_id for participant in participants]
    if len(ids) != len(set(ids)):
        raise ProtocolError("duplicate participant identity")
    if any(participant.version != version for participant in participants):
        raise ProtocolError("protocol version mismatch")
    by_role = {
        role: [participant for participant in participants if participant.role is role]
        for role in Role
    }
    missing = [role.value for role, values in by_role.items() if not values]
    if missing:
        raise ProtocolError(f"missing roles: {', '.join(missing)}")
    if any(
        not (source.stable_ids and source.replayable) for source in by_role[Role.SOURCE]
    ):
        raise ProtocolError("source lacks stable replay")
    if any(
        not (operator.deterministic and operator.managed_state)
        for operator in by_role[Role.OPERATOR]
    ):
        raise ProtocolError("operator lacks deterministic managed state")
    if any(
        not (
            channel.stable_ids
            and channel.replayable
            and channel.durable
            and channel.fenced
        )
        for channel in by_role[Role.CHANNEL]
    ):
        raise ProtocolError("channel lacks durable fenced replay")
    if any(
        not (backend.durable and backend.fenced)
        for backend in by_role[Role.STATE_BACKEND]
    ):
        raise ProtocolError("state backend lacks durable fencing")

    sink_tiers: list[SinkTier] = []
    for sink in by_role[Role.SINK]:
        tier = sink.sink_tier or SinkTier.B
        sink_tiers.append(tier)
        if tier in {SinkTier.I, SinkTier.T} and not sink.idempotent:
            raise ProtocolError(f"tier {tier.value} sink lacks idempotency")
        if tier is SinkTier.T and not (sink.prepare_commit_recover and sink.fenced):
            raise ProtocolError("tier T sink lacks prepare/recover or fencing")
    effective = min(sink_tiers, key=TIER_RANK.__getitem__)
    if TIER_RANK[effective] < TIER_RANK[minimum_tier]:
        raise ProtocolError(
            f"effective tier {effective.value} is below requested {minimum_tier.value}"
        )
    manifests = sorted(
        (participant.manifest for participant in participants),
        key=lambda manifest: str(manifest["participant_id"]),
    )
    participant_roles = tuple(
        sorted(
            (participant.participant_id, participant.role.value)
            for participant in participants
        )
    )
    return Negotiated(
        version,
        minimum_tier,
        effective,
        digest(manifests),
        participant_roles,
    )


@dataclass(frozen=True)
class IdentitySpace:
    tenant: str
    job_name: str
    graph_spec_digest: str

    @classmethod
    def create(
        cls, tenant: str, job_name: str, graph_spec: Mapping[str, Any]
    ) -> IdentitySpace:
        return cls(tenant, job_name, digest(graph_spec))

    @property
    def job_id(self) -> str:
        return f"ariadne:job:{digest({'tenant': self.tenant, 'job': self.job_name})}"

    @property
    def graph_version_id(self) -> str:
        return f"ariadne:graph:{digest({'job_id': self.job_id, 'graph': self.graph_spec_digest})}"

    def make(self, kind: str, **lineage: Any) -> str:
        return f"ariadne:{kind}:{digest({'graph': self.graph_version_id, **lineage})}"

    def operator_id(self, name: str) -> str:
        return self.make("operator", name=name)

    def edge_id(
        self, source: str, source_port: str, target: str, target_port: str
    ) -> str:
        return self.make(
            "edge",
            source=self.operator_id(source),
            source_port=source_port,
            target=self.operator_id(target),
            target_port=target_port,
        )

    def partition_id(self, edge_id: str, partition: int) -> str:
        if partition < 0:
            raise ProtocolError("partition must be non-negative")
        return self.make("partition", edge_id=edge_id, partition=partition)

    def record_id(self, partition_id: str, source_position: str) -> str:
        return self.make(
            "record", partition_id=partition_id, source_position=source_position
        )

    def checkpoint_id(self, sequence: int) -> str:
        if sequence < 1:
            raise ProtocolError("checkpoint sequence must be positive")
        return self.make("checkpoint", sequence=sequence)

    def generation_id(self, participant_id: str, epoch: int) -> str:
        if epoch < 1:
            raise ProtocolError("generation epoch must be positive")
        return self.make("generation", participant_id=participant_id, epoch=epoch)

    def transaction_id(self, checkpoint_id: str) -> str:
        return f"ariadne:transaction:{digest({'job': self.job_id, 'graph': self.graph_version_id, 'checkpoint': checkpoint_id})}"


def _canonical_unique(values: Sequence[str], name: str) -> tuple[str, ...]:
    if len(values) != len(set(values)):
        raise ProtocolError(f"duplicate {name}")
    return tuple(sorted(values))


@dataclass(frozen=True)
class CheckpointIntent:
    job_id: str
    graph_version_id: str
    checkpoint_id: str
    transaction_id: str
    sequence: int
    coordinator_epoch: int
    generation_manifest: tuple[tuple[str, int], ...]
    operator_manifest: tuple[str, ...]
    sink_manifest: tuple[str, ...]
    source_positions: tuple[tuple[str, str], ...]
    artifact_manifest: tuple[tuple[str, str, str], ...]
    requested_tier: SinkTier
    effective_tier: SinkTier
    capability_digest: str
    capability_participants: tuple[tuple[str, str], ...]
    expected_effects: tuple[tuple[str, str, str], ...]

    def __post_init__(self) -> None:
        if self.sequence < 1 or self.coordinator_epoch < 1:
            raise ProtocolError("sequence and coordinator epoch must be positive")
        operators = _canonical_unique(self.operator_manifest, "operator")
        sinks = _canonical_unique(self.sink_manifest, "sink")
        generations = dict(self.generation_manifest)
        sources = dict(self.source_positions)
        if len(generations) != len(self.generation_manifest):
            raise ProtocolError("duplicate generation participant")
        if len(sources) != len(self.source_positions):
            raise ProtocolError("duplicate source partition")
        if any(epoch < 1 for epoch in generations.values()):
            raise ProtocolError("generation epochs must be positive")
        role_identity_count = len(operators) + len(sinks) + len(sources)
        required_generations = set(operators) | set(sinks) | set(sources)
        if len(required_generations) != role_identity_count:
            raise ProtocolError("participant identities must be disjoint across roles")
        if set(generations) != required_generations:
            raise ProtocolError("generation manifest must cover every participant")
        expected_checkpoint_id = f"ariadne:checkpoint:{digest({'graph': self.graph_version_id, 'sequence': self.sequence})}"
        if self.checkpoint_id != expected_checkpoint_id:
            raise ProtocolError("checkpoint identity does not match graph lineage")
        expected_transaction_id = "ariadne:transaction:" + digest(
            {
                "job": self.job_id,
                "graph": self.graph_version_id,
                "checkpoint": self.checkpoint_id,
            }
        )
        if self.transaction_id != expected_transaction_id:
            raise ProtocolError(
                "transaction identity does not match checkpoint lineage"
            )
        artifacts = tuple(sorted(self.artifact_manifest))
        if len(artifacts) != len(set(artifacts)):
            raise ProtocolError("duplicate required artifact")
        if any(kind not in {"channel_append", "snapshot"} for kind, _, _ in artifacts):
            raise ProtocolError("unknown required artifact kind")
        if any(owner not in generations for _, _, owner in artifacts):
            raise ProtocolError("artifact owner lacks a generation")
        if TIER_RANK[self.effective_tier] < TIER_RANK[self.requested_tier]:
            raise ProtocolError("effective tier is below requested tier")
        capability_participants = tuple(sorted(self.capability_participants))
        if len(capability_participants) != len(set(capability_participants)):
            raise ProtocolError("duplicate capability participant")
        capability_ids = [participant for participant, _ in capability_participants]
        if len(capability_ids) != len(set(capability_ids)):
            raise ProtocolError("duplicate capability participant identity")
        roles = {participant: role for participant, role in capability_participants}
        if {
            participant
            for participant, role in roles.items()
            if role == Role.SINK.value
        } != set(sinks):
            raise ProtocolError("sink manifest differs from negotiated participants")
        executable = {
            participant
            for participant, role in roles.items()
            if role in {Role.SOURCE.value, Role.OPERATOR.value}
        }
        if executable != set(operators):
            raise ProtocolError(
                "operator manifest differs from negotiated participants"
            )
        expected_effects = tuple(sorted(self.expected_effects))
        if len(expected_effects) != len(set(expected_effects)):
            raise ProtocolError("duplicate expected visible effect")
        if {sink for sink, _, _ in expected_effects} != set(sinks):
            raise ProtocolError("every sink requires an expected visible effect")
        object.__setattr__(self, "operator_manifest", operators)
        object.__setattr__(self, "sink_manifest", sinks)
        object.__setattr__(
            self, "generation_manifest", tuple(sorted(generations.items()))
        )
        object.__setattr__(self, "source_positions", tuple(sorted(sources.items())))
        object.__setattr__(self, "artifact_manifest", artifacts)
        object.__setattr__(self, "capability_participants", capability_participants)
        object.__setattr__(self, "expected_effects", expected_effects)

    @property
    def manifest_digest(self) -> str:
        return digest(
            {
                "job_id": self.job_id,
                "graph_version_id": self.graph_version_id,
                "checkpoint_id": self.checkpoint_id,
                "transaction_id": self.transaction_id,
                "sequence": self.sequence,
                "coordinator_epoch": self.coordinator_epoch,
                "generation_manifest": self.generation_manifest,
                "operator_manifest": self.operator_manifest,
                "sink_manifest": self.sink_manifest,
                "source_positions": self.source_positions,
                "artifact_manifest": self.artifact_manifest,
                "requested_tier": self.requested_tier.value,
                "effective_tier": self.effective_tier.value,
                "capability_digest": self.capability_digest,
                "capability_participants": self.capability_participants,
                "expected_effects": self.expected_effects,
            }
        )


BOUNDARIES = (
    "checkpoint_decision",
    "phase_transition",
    "channel_append",
    "snapshot",
    "sink_prepare",
    "commit_decision",
    "sink_commit",
    "checkpoint_completion",
    "source_ack",
)
OFFSETS = ("before", "after_durable_before_ack", "after_ack")
FAILURE_POINTS = tuple(
    f"{boundary}:{offset}" for boundary in BOUNDARIES for offset in OFFSETS
)
RECOVERY_ATTEMPT_FAILURE_POINTS = tuple(
    f"{participant}_recovery_attempt:{offset}"
    for participant in ("sink", "source", "abort")
    for offset in ("after_start", "after_callback_before_result")
)


class Fault:
    def __init__(self, point: str | None = None):
        if point is not None and point not in (
            *FAILURE_POINTS,
            *RECOVERY_ATTEMPT_FAILURE_POINTS,
        ):
            raise ValueError(point)
        self.point = point

    def hit(self, point: str) -> None:
        if self.point == point:
            raise InjectedFailure(point)

    def boundary(
        self,
        name: str,
        durable_write: Callable[[], None],
        acknowledge: Callable[[], None],
    ) -> None:
        self.hit(f"{name}:before")
        durable_write()
        self.hit(f"{name}:after_durable_before_ack")
        acknowledge()
        self.hit(f"{name}:after_ack")


class Coordinator:
    """Single durable authority for lifecycle, decision, and participant progress."""

    def __init__(self, path: Path, owner_id: str = "coordinator-owner"):
        self.owner_id = owner_id
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                coordinator_epoch INTEGER NOT NULL,
                manifest_digest TEXT NOT NULL,
                intent_json TEXT NOT NULL,
                phase TEXT NOT NULL,
                decision TEXT,
                active_owner_id TEXT NOT NULL,
                fence_epoch INTEGER NOT NULL,
                PRIMARY KEY (job_id, graph_version_id, checkpoint_id),
                UNIQUE (job_id, graph_version_id, sequence),
                CHECK (
                    (phase IN ('COMMIT_DECIDED', 'COMMITTING', 'COMPLETED')
                     AND decision = 'COMMIT')
                    OR
                    (phase IN ('ABORTING', 'ABORTED') AND decision = 'ABORT')
                    OR
                    (phase NOT IN (
                        'COMMIT_DECIDED', 'COMMITTING', 'COMPLETED',
                        'ABORTING', 'ABORTED'
                     )
                     AND decision IS NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS deployments (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                protocol_version INTEGER NOT NULL,
                requested_tier TEXT NOT NULL,
                effective_tier TEXT NOT NULL,
                capability_digest TEXT NOT NULL,
                participant_roles_json TEXT NOT NULL,
                PRIMARY KEY (job_id, graph_version_id)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_sink_manifest (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                sink_id TEXT NOT NULL,
                PRIMARY KEY (job_id, graph_version_id, checkpoint_id, sink_id),
                FOREIGN KEY (job_id, graph_version_id, checkpoint_id)
                    REFERENCES checkpoints(job_id, graph_version_id, checkpoint_id)
                    ON UPDATE RESTRICT ON DELETE RESTRICT
            );
            CREATE TABLE IF NOT EXISTS sinks (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                sink_id TEXT NOT NULL,
                prepare_token TEXT,
                prepare_token_digest TEXT,
                participant_epoch INTEGER,
                fenced_coordinator_epoch INTEGER,
                status TEXT NOT NULL CHECK (
                    status IN (
                        'PENDING', 'PREPARING', 'PREPARED',
                        'ABORTED', 'COMMITTED'
                    )
                ),
                PRIMARY KEY (job_id, graph_version_id, checkpoint_id, sink_id),
                FOREIGN KEY (job_id, graph_version_id, checkpoint_id)
                    REFERENCES checkpoints(job_id, graph_version_id, checkpoint_id)
                    ON UPDATE RESTRICT ON DELETE RESTRICT
            );
            CREATE TABLE IF NOT EXISTS sources (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                position_json TEXT NOT NULL,
                ack_status TEXT NOT NULL CHECK (ack_status IN ('PENDING', 'ACKED')),
                PRIMARY KEY (job_id, graph_version_id, checkpoint_id, partition_id),
                FOREIGN KEY (job_id, graph_version_id, checkpoint_id)
                    REFERENCES checkpoints(job_id, graph_version_id, checkpoint_id)
                    ON UPDATE RESTRICT ON DELETE RESTRICT
            );
            CREATE TABLE IF NOT EXISTS artifacts (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                artifact_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                value_digest TEXT NOT NULL,
                value_json TEXT NOT NULL,
                PRIMARY KEY (
                    job_id, graph_version_id, checkpoint_id,
                    kind, artifact_id, owner_id
                ),
                FOREIGN KEY (job_id, graph_version_id, checkpoint_id)
                    REFERENCES checkpoints(job_id, graph_version_id, checkpoint_id)
                    ON UPDATE RESTRICT ON DELETE RESTRICT
            );
            CREATE TABLE IF NOT EXISTS records (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                lineage_digest TEXT NOT NULL,
                value_digest TEXT NOT NULL,
                PRIMARY KEY (job_id, graph_version_id, record_id)
            );
            CREATE TABLE IF NOT EXISTS recovery_attempts (
                job_id TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                checkpoint_id TEXT NOT NULL,
                participant_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                fence_epoch INTEGER NOT NULL,
                attempt_count INTEGER NOT NULL CHECK (attempt_count > 0),
                blocked INTEGER NOT NULL CHECK (blocked IN (0, 1)),
                outcome TEXT NOT NULL CHECK (
                    outcome IN ('IN_PROGRESS', 'SUCCEEDED', 'BLOCKED')
                ),
                last_error TEXT,
                PRIMARY KEY (
                    job_id, graph_version_id, checkpoint_id, participant_id
                ),
                FOREIGN KEY (job_id, graph_version_id, checkpoint_id)
                    REFERENCES checkpoints(job_id, graph_version_id, checkpoint_id)
                    ON UPDATE RESTRICT ON DELETE RESTRICT
            );
            CREATE TRIGGER IF NOT EXISTS source_ack_requires_commit_on_update
            BEFORE UPDATE ON sources
            WHEN NEW.ack_status = 'ACKED' AND NOT EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = NEW.job_id
                  AND c.graph_version_id = NEW.graph_version_id
                  AND c.checkpoint_id = NEW.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'source ACK requires COMMIT');
            END;
            CREATE TRIGGER IF NOT EXISTS source_ack_requires_commit_on_insert
            BEFORE INSERT ON sources
            WHEN NEW.ack_status = 'ACKED' AND NOT EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = NEW.job_id
                  AND c.graph_version_id = NEW.graph_version_id
                  AND c.checkpoint_id = NEW.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'source ACK requires COMMIT');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_commit_requires_decision_on_update
            BEFORE UPDATE ON sinks
            WHEN NEW.status = 'COMMITTED' AND NOT EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = NEW.job_id
                  AND c.graph_version_id = NEW.graph_version_id
                  AND c.checkpoint_id = NEW.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink commit requires COMMIT');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_commit_requires_decision_on_insert
            BEFORE INSERT ON sinks
            WHEN NEW.status = 'COMMITTED' AND NOT EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = NEW.job_id
                  AND c.graph_version_id = NEW.graph_version_id
                  AND c.checkpoint_id = NEW.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink commit requires COMMIT');
            END;
            CREATE TRIGGER IF NOT EXISTS committed_decision_is_immutable
            BEFORE UPDATE OF decision ON checkpoints
            WHEN OLD.decision IS NOT NULL AND NEW.decision IS NOT OLD.decision
            BEGIN
                SELECT RAISE(ABORT, 'durable decision is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS commit_decision_cannot_be_inserted
            BEFORE INSERT ON checkpoints
            WHEN NEW.decision IS NOT NULL
            BEGIN
                SELECT RAISE(ABORT, 'COMMIT decision requires prepared participants');
            END;
            CREATE TRIGGER IF NOT EXISTS commit_decision_requires_prepared
            BEFORE UPDATE OF phase, decision ON checkpoints
            WHEN OLD.decision IS NULL AND NEW.decision = 'COMMIT'
            BEGIN
                SELECT CASE WHEN
                    OLD.phase != 'PREPARED'
                    OR NEW.phase != 'COMMIT_DECIDED'
                    OR NEW.active_owner_id != OLD.active_owner_id
                    OR NEW.fence_epoch != OLD.fence_epoch
                    OR EXISTS (
                        SELECT 1 FROM checkpoint_sink_manifest expected
                        LEFT JOIN sinks actual
                          ON actual.job_id = expected.job_id
                         AND actual.graph_version_id = expected.graph_version_id
                         AND actual.checkpoint_id = expected.checkpoint_id
                         AND actual.sink_id = expected.sink_id
                        WHERE expected.job_id = OLD.job_id
                          AND expected.graph_version_id = OLD.graph_version_id
                          AND expected.checkpoint_id = OLD.checkpoint_id
                          AND (
                              actual.sink_id IS NULL
                              OR actual.status != 'PREPARED'
                              OR actual.prepare_token IS NULL
                              OR actual.prepare_token_digest IS NULL
                              OR actual.participant_epoch IS NULL
                              OR actual.fenced_coordinator_epoch != NEW.fence_epoch
                          )
                    )
                    OR EXISTS (
                        SELECT 1 FROM sinks actual
                        LEFT JOIN checkpoint_sink_manifest expected
                          ON actual.job_id = expected.job_id
                         AND actual.graph_version_id = expected.graph_version_id
                         AND actual.checkpoint_id = expected.checkpoint_id
                         AND actual.sink_id = expected.sink_id
                        WHERE actual.job_id = OLD.job_id
                          AND actual.graph_version_id = OLD.graph_version_id
                          AND actual.checkpoint_id = OLD.checkpoint_id
                          AND expected.sink_id IS NULL
                    )
                    THEN RAISE(ABORT, 'COMMIT decision requires prepared participants')
                END;
            END;
            CREATE TRIGGER IF NOT EXISTS abort_decision_requires_fence_advance
            BEFORE UPDATE OF phase, decision, active_owner_id, fence_epoch
            ON checkpoints
            WHEN OLD.decision IS NULL AND NEW.decision = 'ABORT'
            BEGIN
                SELECT CASE WHEN
                    OLD.phase NOT IN (
                        'STARTED', 'ALIGNING', 'ALIGNED', 'PREPARING', 'PREPARED'
                    )
                    OR NEW.phase != 'ABORTING'
                    OR NEW.active_owner_id != OLD.active_owner_id
                    OR NEW.fence_epoch != OLD.fence_epoch + 1
                    THEN RAISE(
                        ABORT,
                        'ABORT decision requires owner-preserving fence advance'
                    )
                END;
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_identity_is_immutable
            BEFORE UPDATE OF job_id, graph_version_id, checkpoint_id ON checkpoints
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_intent_is_immutable
            BEFORE UPDATE OF sequence, coordinator_epoch, manifest_digest, intent_json
            ON checkpoints
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint intent is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_fence_is_monotonic
            BEFORE UPDATE OF fence_epoch ON checkpoints
            WHEN NEW.fence_epoch < OLD.fence_epoch
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint fence is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_owner_change_requires_fence
            BEFORE UPDATE OF active_owner_id, fence_epoch ON checkpoints
            WHEN NEW.active_owner_id IS NOT OLD.active_owner_id
              AND NEW.fence_epoch <= OLD.fence_epoch
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint owner change requires fence advance');
            END;
            CREATE TRIGGER IF NOT EXISTS referenced_checkpoint_cannot_be_deleted
            BEFORE DELETE ON checkpoints
            WHEN OLD.decision = 'COMMIT' OR EXISTS (
                SELECT 1 FROM sources s
                WHERE s.job_id = OLD.job_id
                  AND s.graph_version_id = OLD.graph_version_id
                  AND s.checkpoint_id = OLD.checkpoint_id
                  AND s.ack_status = 'ACKED'
            ) OR EXISTS (
                SELECT 1 FROM sinks s
                WHERE s.job_id = OLD.job_id
                  AND s.graph_version_id = OLD.graph_version_id
                  AND s.checkpoint_id = OLD.checkpoint_id
                  AND s.status = 'COMMITTED'
            )
            BEGIN
                SELECT RAISE(ABORT, 'referenced COMMIT decision cannot be deleted');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_identity_is_immutable
            BEFORE UPDATE OF job_id, graph_version_id, checkpoint_id, sink_id ON sinks
            BEGIN
                SELECT RAISE(ABORT, 'sink identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_prepare_token_is_immutable
            BEFORE UPDATE OF prepare_token, prepare_token_digest ON sinks
            WHEN OLD.prepare_token IS NOT NULL AND (
                NEW.prepare_token IS NOT OLD.prepare_token
                OR NEW.prepare_token_digest IS NOT OLD.prepare_token_digest
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink prepare token is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_participant_epoch_is_immutable
            BEFORE UPDATE OF participant_epoch ON sinks
            WHEN OLD.participant_epoch IS NOT NULL
              AND NEW.participant_epoch IS NOT OLD.participant_epoch
            BEGIN
                SELECT RAISE(ABORT, 'sink participant epoch is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_coordinator_fence_is_monotonic
            BEFORE UPDATE OF fenced_coordinator_epoch ON sinks
            WHEN OLD.fenced_coordinator_epoch IS NOT NULL
              AND NEW.fenced_coordinator_epoch < OLD.fenced_coordinator_epoch
            BEGIN
                SELECT RAISE(ABORT, 'sink coordinator fence is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_insert_requires_manifest
            BEFORE INSERT ON sinks
            WHEN NOT EXISTS (
                SELECT 1 FROM checkpoint_sink_manifest expected
                WHERE expected.job_id = NEW.job_id
                  AND expected.graph_version_id = NEW.graph_version_id
                  AND expected.checkpoint_id = NEW.checkpoint_id
                  AND expected.sink_id = NEW.sink_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink row requires immutable manifest');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_insert_is_create_only
            BEFORE INSERT ON sinks
            WHEN EXISTS (
                SELECT 1 FROM sinks current
                WHERE current.job_id = NEW.job_id
                  AND current.graph_version_id = NEW.graph_version_id
                  AND current.checkpoint_id = NEW.checkpoint_id
                  AND current.sink_id = NEW.sink_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink row already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_manifest_insert_requires_open_start
            BEFORE INSERT ON checkpoint_sink_manifest
            WHEN NOT EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = NEW.job_id
                  AND c.graph_version_id = NEW.graph_version_id
                  AND c.checkpoint_id = NEW.checkpoint_id
                  AND c.phase = 'STARTED'
                  AND c.decision IS NULL
            ) OR EXISTS (
                SELECT 1 FROM sinks s
                WHERE s.job_id = NEW.job_id
                  AND s.graph_version_id = NEW.graph_version_id
                  AND s.checkpoint_id = NEW.checkpoint_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink manifest is sealed');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_delete_requires_retention
            BEFORE DELETE ON sinks
            BEGIN
                SELECT RAISE(ABORT, 'sink deletion requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_manifest_is_immutable_on_update
            BEFORE UPDATE ON checkpoint_sink_manifest
            BEGIN
                SELECT RAISE(ABORT, 'sink manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_manifest_is_immutable_on_delete
            BEFORE DELETE ON checkpoint_sink_manifest
            BEGIN
                SELECT RAISE(ABORT, 'sink manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS source_identity_and_position_are_immutable
            BEFORE UPDATE OF job_id, graph_version_id, checkpoint_id,
                             partition_id, position_json ON sources
            BEGIN
                SELECT RAISE(ABORT, 'source identity and position are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS committed_sink_cannot_be_deleted
            BEFORE DELETE ON sinks
            WHEN EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = OLD.job_id
                  AND c.graph_version_id = OLD.graph_version_id
                  AND c.checkpoint_id = OLD.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'committed sink row cannot be deleted');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_status_transition_is_guarded
            BEFORE UPDATE OF status ON sinks
            WHEN NOT (
                NEW.status = OLD.status
                OR (
                    OLD.status = 'PENDING' AND NEW.status = 'PREPARING'
                    AND EXISTS (
                        SELECT 1 FROM checkpoints c
                        WHERE c.job_id = OLD.job_id
                          AND c.graph_version_id = OLD.graph_version_id
                          AND c.checkpoint_id = OLD.checkpoint_id
                          AND c.phase = 'PREPARING' AND c.decision IS NULL
                    )
                )
                OR (
                    OLD.status = 'PREPARING' AND NEW.status = 'PREPARED'
                    AND EXISTS (
                        SELECT 1 FROM checkpoints c
                        WHERE c.job_id = OLD.job_id
                          AND c.graph_version_id = OLD.graph_version_id
                          AND c.checkpoint_id = OLD.checkpoint_id
                          AND c.phase = 'PREPARING' AND c.decision IS NULL
                    )
                )
                OR (
                    OLD.status IN ('PENDING', 'PREPARING', 'PREPARED')
                    AND NEW.status = 'ABORTED'
                    AND EXISTS (
                        SELECT 1 FROM checkpoints c
                        WHERE c.job_id = OLD.job_id
                          AND c.graph_version_id = OLD.graph_version_id
                          AND c.checkpoint_id = OLD.checkpoint_id
                          AND c.decision = 'ABORT'
                          AND c.phase = 'ABORTING'
                    )
                )
                OR (
                    OLD.status = 'PREPARED' AND NEW.status = 'COMMITTED'
                    AND EXISTS (
                        SELECT 1 FROM checkpoints c
                        WHERE c.job_id = OLD.job_id
                          AND c.graph_version_id = OLD.graph_version_id
                          AND c.checkpoint_id = OLD.checkpoint_id
                          AND c.decision = 'COMMIT'
                          AND c.phase IN (
                              'COMMIT_DECIDED', 'COMMITTING', 'COMPLETED'
                          )
                    )
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'illegal sink status transition');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_abort_requires_sink_cleanup
            BEFORE UPDATE OF phase ON checkpoints
            WHEN NEW.phase = 'ABORTED' AND (
                EXISTS (
                    SELECT 1 FROM checkpoint_sink_manifest expected
                    LEFT JOIN sinks actual
                      ON actual.job_id = expected.job_id
                     AND actual.graph_version_id = expected.graph_version_id
                     AND actual.checkpoint_id = expected.checkpoint_id
                     AND actual.sink_id = expected.sink_id
                    WHERE expected.job_id = OLD.job_id
                      AND expected.graph_version_id = OLD.graph_version_id
                      AND expected.checkpoint_id = OLD.checkpoint_id
                      AND (
                          actual.sink_id IS NULL OR actual.status != 'ABORTED'
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM sinks actual
                    LEFT JOIN checkpoint_sink_manifest expected
                      ON actual.job_id = expected.job_id
                     AND actual.graph_version_id = expected.graph_version_id
                     AND actual.checkpoint_id = expected.checkpoint_id
                     AND actual.sink_id = expected.sink_id
                    WHERE actual.job_id = OLD.job_id
                      AND actual.graph_version_id = OLD.graph_version_id
                      AND actual.checkpoint_id = OLD.checkpoint_id
                      AND expected.sink_id IS NULL
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint abort requires sink cleanup');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_completion_requires_committed_sinks
            BEFORE UPDATE OF phase ON checkpoints
            WHEN NEW.phase = 'COMPLETED' AND (
                NEW.decision IS NOT 'COMMIT'
                OR EXISTS (
                    SELECT 1 FROM checkpoint_sink_manifest expected
                    LEFT JOIN sinks actual
                      ON actual.job_id = expected.job_id
                     AND actual.graph_version_id = expected.graph_version_id
                     AND actual.checkpoint_id = expected.checkpoint_id
                     AND actual.sink_id = expected.sink_id
                    WHERE expected.job_id = OLD.job_id
                      AND expected.graph_version_id = OLD.graph_version_id
                      AND expected.checkpoint_id = OLD.checkpoint_id
                      AND (
                          actual.sink_id IS NULL OR actual.status != 'COMMITTED'
                      )
                )
                OR EXISTS (
                    SELECT 1 FROM sinks actual
                    LEFT JOIN checkpoint_sink_manifest expected
                      ON actual.job_id = expected.job_id
                     AND actual.graph_version_id = expected.graph_version_id
                     AND actual.checkpoint_id = expected.checkpoint_id
                     AND actual.sink_id = expected.sink_id
                    WHERE actual.job_id = OLD.job_id
                      AND actual.graph_version_id = OLD.graph_version_id
                      AND actual.checkpoint_id = OLD.checkpoint_id
                      AND expected.sink_id IS NULL
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion requires committed sinks');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_phase_is_monotonic
            BEFORE UPDATE OF phase ON checkpoints
            WHEN NOT (
                NEW.phase = OLD.phase
                OR (OLD.phase = 'STARTED' AND NEW.phase = 'ALIGNING')
                OR (OLD.phase = 'ALIGNING' AND NEW.phase = 'ALIGNED')
                OR (OLD.phase = 'ALIGNED' AND NEW.phase = 'PREPARING')
                OR (OLD.phase = 'PREPARING' AND NEW.phase = 'PREPARED')
                OR (OLD.phase = 'PREPARED' AND NEW.phase = 'COMMIT_DECIDED')
                OR (OLD.phase = 'COMMIT_DECIDED' AND NEW.phase IN ('COMMITTING', 'COMPLETED'))
                OR (OLD.phase = 'COMMITTING' AND NEW.phase = 'COMPLETED')
                OR (
                    OLD.phase IN (
                        'STARTED', 'ALIGNING', 'ALIGNED', 'PREPARING', 'PREPARED'
                    ) AND NEW.phase = 'ABORTING'
                )
                OR (OLD.phase = 'ABORTING' AND NEW.phase = 'ABORTED')
            )
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint phase is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS committed_source_cannot_be_deleted
            BEFORE DELETE ON sources
            WHEN EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = OLD.job_id
                  AND c.graph_version_id = OLD.graph_version_id
                  AND c.checkpoint_id = OLD.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'committed source row cannot be deleted');
            END;
            CREATE TRIGGER IF NOT EXISTS source_ack_is_irreversible
            BEFORE UPDATE OF ack_status ON sources
            WHEN OLD.ack_status = 'ACKED' AND NEW.ack_status != 'ACKED'
            BEGIN
                SELECT RAISE(ABORT, 'source ACK is irreversible');
            END;
            CREATE TRIGGER IF NOT EXISTS artifact_is_immutable
            BEFORE UPDATE ON artifacts
            BEGIN
                SELECT RAISE(ABORT, 'artifact is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS committed_artifact_cannot_be_deleted
            BEFORE DELETE ON artifacts
            WHEN EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = OLD.job_id
                  AND c.graph_version_id = OLD.graph_version_id
                  AND c.checkpoint_id = OLD.checkpoint_id
                  AND c.decision = 'COMMIT'
            )
            BEGIN
                SELECT RAISE(ABORT, 'committed artifact cannot be deleted');
            END;
            CREATE TRIGGER IF NOT EXISTS record_claim_is_immutable_on_update
            BEFORE UPDATE ON records
            BEGIN
                SELECT RAISE(ABORT, 'stable record claim is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS record_claim_is_immutable_on_delete
            BEFORE DELETE ON records
            BEGIN
                SELECT RAISE(ABORT, 'stable record claim is immutable');
            END;
            """
        )

    @staticmethod
    def _key(intent: CheckpointIntent) -> tuple[str, str, str]:
        return intent.job_id, intent.graph_version_id, intent.checkpoint_id

    @staticmethod
    def _intent_json(intent: CheckpointIntent) -> str:
        return json.dumps(
            {
                "generation_manifest": intent.generation_manifest,
                "operator_manifest": intent.operator_manifest,
                "sink_manifest": intent.sink_manifest,
                "source_positions": intent.source_positions,
                "artifact_manifest": intent.artifact_manifest,
                "requested_tier": intent.requested_tier.value,
                "effective_tier": intent.effective_tier.value,
                "capability_digest": intent.capability_digest,
                "capability_participants": intent.capability_participants,
                "expected_effects": intent.expected_effects,
            },
            sort_keys=True,
        )

    def _row(self, intent: CheckpointIntent) -> tuple[Any, ...]:
        row = self.connection.execute(
            """
            SELECT sequence, coordinator_epoch, manifest_digest, phase, decision,
                   active_owner_id, fence_epoch
            FROM checkpoints
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
            """,
            self._key(intent),
        ).fetchone()
        if row is None:
            raise KeyError(intent.checkpoint_id)
        if row[:3] != (
            intent.sequence,
            intent.coordinator_epoch,
            intent.manifest_digest,
        ):
            raise ProtocolError("checkpoint intent or capability manifest drift")
        return row

    def register_deployment(
        self,
        job_id: str,
        graph_version_id: str,
        negotiated: Negotiated,
    ) -> None:
        expected = (
            negotiated.protocol_version,
            negotiated.requested_tier.value,
            negotiated.tier.value,
            negotiated.capability_digest,
            json.dumps(negotiated.participant_roles),
        )
        existing = self.connection.execute(
            """
            SELECT protocol_version, requested_tier, effective_tier,
                   capability_digest, participant_roles_json
            FROM deployments WHERE job_id = ? AND graph_version_id = ?
            """,
            (job_id, graph_version_id),
        ).fetchone()
        if existing is not None:
            if existing != expected:
                raise ProtocolError("deployment capability contract drift")
            return
        with self.connection:
            self.connection.execute(
                "INSERT INTO deployments VALUES (?, ?, ?, ?, ?, ?, ?)",
                (job_id, graph_version_id, *expected),
            )

    def takeover(self, intent: CheckpointIntent, new_epoch: int) -> None:
        row = self._row(intent)
        if new_epoch <= row[6]:
            raise ProtocolError("takeover epoch must increase")
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE checkpoints SET active_owner_id = ?, fence_epoch = ?
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                  AND fence_epoch = ? AND active_owner_id = ?
                """,
                (
                    self.owner_id,
                    new_epoch,
                    *self._key(intent),
                    row[6],
                    row[5],
                ),
            )
            if cursor.rowcount != 1:
                raise ProtocolError("coordinator takeover CAS failed")

    def _assert_owner(self, row: tuple[Any, ...], supplied_epoch: int) -> None:
        if row[5] != self.owner_id or row[6] != supplied_epoch:
            raise ProtocolError("stale coordinator owner or epoch")

    def _deployment(self, intent: CheckpointIntent) -> tuple[Any, ...]:
        row = self.connection.execute(
            """
            SELECT requested_tier, effective_tier, capability_digest,
                   participant_roles_json
            FROM deployments WHERE job_id = ? AND graph_version_id = ?
            """,
            (intent.job_id, intent.graph_version_id),
        ).fetchone()
        expected = (
            intent.requested_tier.value,
            intent.effective_tier.value,
            intent.capability_digest,
            json.dumps(intent.capability_participants),
        )
        if row != expected:
            raise ProtocolError("checkpoint does not match registered deployment")
        return row

    def _participant_rows(
        self, intent: CheckpointIntent
    ) -> tuple[dict[str, str], dict[str, tuple[str, str]]]:
        sinks = dict(
            self.connection.execute(
                """
                SELECT sink_id, status FROM sinks
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                """,
                self._key(intent),
            )
        )
        sources = {
            partition: (json.loads(position), status)
            for partition, position, status in self.connection.execute(
                """
                SELECT partition_id, position_json, ack_status FROM sources
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                """,
                self._key(intent),
            )
        }
        if set(sinks) != set(intent.sink_manifest):
            raise ProtocolError("durable sink rows differ from checkpoint manifest")
        expected_sources = dict(intent.source_positions)
        if set(sources) != set(expected_sources) or any(
            sources[partition][0] != position
            for partition, position in expected_sources.items()
        ):
            raise ProtocolError("durable source rows differ from checkpoint manifest")
        return sinks, sources

    def _run(
        self,
        boundary: str,
        write: Callable[[], None],
        acknowledge: Callable[[], None] = lambda: None,
        fault: Fault | None = None,
    ) -> None:
        (fault or Fault()).boundary(boundary, write, acknowledge)

    def start(self, intent: CheckpointIntent, fault: Fault | None = None) -> None:
        self._deployment(intent)
        existing = self.connection.execute(
            """
            SELECT 1 FROM checkpoints
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
            """,
            self._key(intent),
        ).fetchone()
        if existing:
            row = self._row(intent)
            self._assert_owner(row, row[6])
            return

        def write() -> None:
            latest = self.connection.execute(
                """
                SELECT MAX(sequence) FROM checkpoints
                WHERE job_id = ? AND graph_version_id = ?
                """,
                (intent.job_id, intent.graph_version_id),
            ).fetchone()[0]
            expected = 1 if latest is None else latest + 1
            if intent.sequence != expected:
                raise ProtocolError(
                    f"out-of-order checkpoint: expected {expected}, got {intent.sequence}"
                )
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO checkpoints VALUES (
                        ?, ?, ?, ?, ?, ?, ?, 'STARTED', NULL, ?, ?
                    )
                    """,
                    (
                        *self._key(intent),
                        intent.sequence,
                        intent.coordinator_epoch,
                        intent.manifest_digest,
                        self._intent_json(intent),
                        self.owner_id,
                        intent.coordinator_epoch,
                    ),
                )
                self.connection.executemany(
                    "INSERT INTO checkpoint_sink_manifest VALUES (?, ?, ?, ?)",
                    ((*self._key(intent), sink) for sink in intent.sink_manifest),
                )
                self.connection.executemany(
                    """
                    INSERT INTO sinks (
                        job_id, graph_version_id, checkpoint_id, sink_id,
                        prepare_token, prepare_token_digest, participant_epoch,
                        fenced_coordinator_epoch, status
                    ) VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, 'PENDING')
                    """,
                    ((*self._key(intent), sink) for sink in intent.sink_manifest),
                )
                self.connection.executemany(
                    "INSERT INTO sources VALUES (?, ?, ?, ?, ?, 'PENDING')",
                    (
                        (*self._key(intent), partition, json.dumps(position))
                        for partition, position in intent.source_positions
                    ),
                )

        try:
            self._run("checkpoint_decision", write, fault=fault)
        except sqlite3.IntegrityError as exc:
            raise ProtocolError("checkpoint identity or sequence conflict") from exc

    def transition(
        self,
        intent: CheckpointIntent,
        expected: Phase,
        target: Phase,
        *,
        coordinator_epoch: int,
        fault: Fault | None = None,
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, coordinator_epoch)
        if not transition_allowed(expected, target):
            raise ProtocolError(
                f"illegal transition {expected.value} -> {target.value}"
            )
        if row[3] == target.value:
            return
        if target in {
            Phase.COMMIT_DECIDED,
            Phase.COMMITTING,
            Phase.COMPLETED,
            Phase.ABORTING,
            Phase.ABORTED,
        }:
            raise ProtocolError("decision phases require dedicated protocol methods")
        if target is Phase.PREPARED:
            sinks, _ = self._participant_rows(intent)
            if any(status != "PREPARED" for status in sinks.values()):
                raise ProtocolError("checkpoint cannot be PREPARED before every sink")
        if target is Phase.ALIGNED:
            present = set(
                self.connection.execute(
                    """
                    SELECT kind, artifact_id, owner_id FROM artifacts
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                    """,
                    self._key(intent),
                )
            )
            required = set(intent.artifact_manifest)
            if present != required:
                raise ProtocolError(
                    "checkpoint cannot align without required artifacts"
                )

        def write() -> None:
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE checkpoints SET phase = ?
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND phase = ? AND fence_epoch = ?
                      AND active_owner_id = ?
                    """,
                    (
                        target.value,
                        *self._key(intent),
                        expected.value,
                        coordinator_epoch,
                        self.owner_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("stale phase or coordinator epoch")

        self._run("phase_transition", write, fault=fault)

    def record_artifact(
        self,
        intent: CheckpointIntent,
        kind: str,
        artifact_id: str,
        participant_id: str,
        participant_epoch: int,
        value: Any,
        fault: Fault | None = None,
        *,
        record_lineage: Mapping[str, Any] | None = None,
    ) -> None:
        if kind not in {"channel_append", "snapshot"}:
            raise ValueError(kind)
        row = self._row(intent)
        self._assert_owner(row, row[6])
        self._assert_generation(intent, participant_id, participant_epoch)
        if row[3] != Phase.ALIGNING.value:
            raise ProtocolError("artifacts are writable only while ALIGNING")
        if (kind, artifact_id, participant_id) not in intent.artifact_manifest:
            raise ProtocolError("artifact is absent from immutable checkpoint manifest")
        value_digest = digest(value)
        value_json = json.dumps(value, sort_keys=True, separators=(",", ":"))
        if kind == "channel_append" and record_lineage is None:
            raise ProtocolError("channel record requires explicit stable lineage")
        lineage_digest = digest(record_lineage) if record_lineage is not None else None

        def write() -> None:
            record = None
            if kind == "channel_append":
                record = self.connection.execute(
                    """
                    SELECT owner_id, lineage_digest, value_digest FROM records
                    WHERE job_id = ? AND graph_version_id = ? AND record_id = ?
                    """,
                    (intent.job_id, intent.graph_version_id, artifact_id),
                ).fetchone()
                if record is not None and record != (
                    participant_id,
                    lineage_digest,
                    value_digest,
                ):
                    raise ProtocolError("stable record identity or payload drift")
            existing = self.connection.execute(
                """
                SELECT a.value_digest FROM artifacts a
                JOIN checkpoints c USING(job_id, graph_version_id, checkpoint_id)
                WHERE a.job_id = ? AND a.graph_version_id = ? AND a.checkpoint_id = ?
                  AND a.kind = ? AND a.artifact_id = ? AND a.owner_id = ?
                  AND c.active_owner_id = ? AND c.fence_epoch = ?
                  AND c.phase = 'ALIGNING'
                """,
                (
                    *self._key(intent),
                    kind,
                    artifact_id,
                    participant_id,
                    self.owner_id,
                    row[6],
                ),
            ).fetchone()
            if existing and existing[0] != value_digest:
                raise ProtocolError("artifact identity drift")
            if not existing:
                with self.connection:
                    if kind == "channel_append" and record is None:
                        self.connection.execute(
                            """
                            INSERT OR IGNORE INTO records
                            SELECT ?, ?, ?, ?, ?, ?
                            WHERE EXISTS (
                                SELECT 1 FROM checkpoints
                                WHERE job_id = ? AND graph_version_id = ?
                                  AND checkpoint_id = ? AND phase = 'ALIGNING'
                                  AND active_owner_id = ? AND fence_epoch = ?
                            )
                            """,
                            (
                                intent.job_id,
                                intent.graph_version_id,
                                artifact_id,
                                participant_id,
                                lineage_digest,
                                value_digest,
                                *self._key(intent),
                                self.owner_id,
                                row[6],
                            ),
                        )
                        claimed = self.connection.execute(
                            """
                            SELECT owner_id, lineage_digest, value_digest FROM records
                            WHERE job_id = ? AND graph_version_id = ?
                              AND record_id = ?
                            """,
                            (intent.job_id, intent.graph_version_id, artifact_id),
                        ).fetchone()
                        if claimed is None:
                            raise ProtocolError(
                                "stale record owner or checkpoint phase"
                            )
                        if claimed != (
                            participant_id,
                            lineage_digest,
                            value_digest,
                        ):
                            raise ProtocolError(
                                "stable record identity or payload drift"
                            )
                    cursor = self.connection.execute(
                        """
                        INSERT INTO artifacts
                        SELECT ?, ?, ?, ?, ?, ?, ?, ?
                        WHERE EXISTS (
                            SELECT 1 FROM checkpoints
                            WHERE job_id = ? AND graph_version_id = ?
                              AND checkpoint_id = ? AND phase = 'ALIGNING'
                              AND active_owner_id = ? AND fence_epoch = ?
                        )
                        """,
                        (
                            *self._key(intent),
                            kind,
                            artifact_id,
                            participant_id,
                            value_digest,
                            value_json,
                            *self._key(intent),
                            self.owner_id,
                            row[6],
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ProtocolError("stale artifact owner or checkpoint phase")

        self._run(kind, write, fault=fault)

    def load_artifact(
        self,
        intent: CheckpointIntent,
        kind: str,
        artifact_id: str,
        owner_id: str,
    ) -> Any:
        row = self.connection.execute(
            """
            SELECT value_json FROM artifacts
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND kind = ? AND artifact_id = ? AND owner_id = ?
            """,
            (*self._key(intent), kind, artifact_id, owner_id),
        ).fetchone()
        if row is None:
            raise KeyError((kind, artifact_id, owner_id))
        return json.loads(row[0])

    def prepare_sink(
        self,
        intent: CheckpointIntent,
        sink_id: str,
        participant_epoch: int,
        prepare: Callable[[str, str, int, int], str],
        fault: Fault | None = None,
    ) -> None:
        self._assert_generation(intent, sink_id, participant_epoch)
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[3] != Phase.PREPARING.value:
            raise ProtocolError("sink prepare requires PREPARING checkpoint")

        current = self.connection.execute(
            """
            SELECT status, participant_epoch FROM sinks
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND sink_id = ?
            """,
            (*self._key(intent), sink_id),
        ).fetchone()
        if current is None or current[0] not in {"PENDING", "PREPARING", "PREPARED"}:
            raise ProtocolError("sink is not prepareable")
        if current[1] is not None and current[1] != participant_epoch:
            raise ProtocolError("sink prepare participant epoch drift")
        if current[0] == "PENDING":
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE sinks
                    SET status = 'PREPARING', participant_epoch = ?
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND sink_id = ? AND status = 'PENDING'
                      AND EXISTS (
                          SELECT 1 FROM checkpoints
                          WHERE job_id = ? AND graph_version_id = ?
                            AND checkpoint_id = ? AND phase = 'PREPARING'
                            AND active_owner_id = ? AND fence_epoch = ?
                      )
                    """,
                    (
                        participant_epoch,
                        *self._key(intent),
                        sink_id,
                        *self._key(intent),
                        self.owner_id,
                        row[6],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("stale sink prepare intent")

        prepare_token: str | None = None

        def durable() -> None:
            nonlocal prepare_token
            prepare_token = prepare(
                intent.transaction_id, sink_id, row[6], participant_epoch
            )
            if not prepare_token:
                raise ProtocolError("sink prepare returned an empty token")

        def acknowledge() -> None:
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE sinks
                    SET status = 'PREPARED', prepare_token = ?,
                        prepare_token_digest = ?, fenced_coordinator_epoch = ?
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND sink_id = ? AND status IN ('PREPARING', 'PREPARED')
                      AND EXISTS (
                          SELECT 1 FROM checkpoints
                          WHERE job_id = ? AND graph_version_id = ?
                            AND checkpoint_id = ? AND phase = 'PREPARING'
                            AND active_owner_id = ? AND fence_epoch = ?
                      )
                    """,
                    (
                        prepare_token,
                        digest(prepare_token),
                        row[6],
                        *self._key(intent),
                        sink_id,
                        *self._key(intent),
                        self.owner_id,
                        row[6],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("unknown or already committed sink")

        self._run("sink_prepare", durable, acknowledge, fault)

    def refence_prepared_sinks(
        self,
        intent: CheckpointIntent,
        fence: Callable[[str, str, int, int], None],
        recover: Callable[[str, str], RecoveredSink | None],
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[3] != Phase.PREPARED.value or row[4] is not None:
            raise ProtocolError("sink re-fence requires PREPARED checkpoint")
        expected_generations = dict(intent.generation_manifest)
        prepared = self.connection.execute(
            """
            SELECT sink_id, prepare_token, participant_epoch
            FROM sinks
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
            ORDER BY sink_id
            """,
            self._key(intent),
        ).fetchall()
        for sink_id, token, participant_epoch in prepared:
            if token is None or participant_epoch != expected_generations[sink_id]:
                raise ProtocolError("sink re-fence found incomplete prepare state")
            fence(intent.transaction_id, sink_id, row[6], participant_epoch)
            actual = recover(intent.transaction_id, sink_id)
            if actual is None or actual.state != "PREPARED" or actual.token != token:
                raise ProtocolError("sink re-fence found divergent participant state")
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE sinks SET fenced_coordinator_epoch = ?
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND sink_id = ? AND status = 'PREPARED'
                      AND prepare_token = ? AND participant_epoch = ?
                      AND EXISTS (
                          SELECT 1 FROM checkpoints
                          WHERE job_id = ? AND graph_version_id = ?
                            AND checkpoint_id = ? AND phase = 'PREPARED'
                            AND decision IS NULL AND active_owner_id = ?
                            AND fence_epoch = ?
                      )
                    """,
                    (
                        row[6],
                        *self._key(intent),
                        sink_id,
                        token,
                        participant_epoch,
                        *self._key(intent),
                        self.owner_id,
                        row[6],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("stale sink re-fence acknowledgement")

    def decide_commit(
        self, intent: CheckpointIntent, fault: Fault | None = None
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[3] in {
            Phase.COMMIT_DECIDED.value,
            Phase.COMMITTING.value,
            Phase.COMPLETED.value,
        }:
            return
        sinks, _ = self._participant_rows(intent)
        if row[3] != Phase.PREPARED.value or any(
            status != "PREPARED" for status in sinks.values()
        ):
            raise ProtocolError(
                "commit decision requires PREPARED checkpoint and sinks"
            )
        unfenced = self.connection.execute(
            """
            SELECT 1 FROM sinks
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND fenced_coordinator_epoch IS NOT ?
            LIMIT 1
            """,
            (*self._key(intent), row[6]),
        ).fetchone()
        if unfenced is not None:
            raise ProtocolError("commit decision requires participant re-fence")

        def write() -> None:
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE checkpoints SET phase = 'COMMIT_DECIDED', decision = 'COMMIT'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND phase = 'PREPARED' AND decision IS NULL
                      AND manifest_digest = ?
                      AND active_owner_id = ? AND fence_epoch = ?
                    """,
                    (
                        *self._key(intent),
                        intent.manifest_digest,
                        self.owner_id,
                        row[6],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("stale commit decision")

        self._run("commit_decision", write, fault=fault)

    def source_decision_reference(self, intent: CheckpointIntent) -> dict[str, Any]:
        """Return fields which a source ledger must bind to this COMMIT."""
        row = self._row(intent)
        if row[3] not in {
            Phase.COMMIT_DECIDED.value,
            Phase.COMMITTING.value,
            Phase.COMPLETED.value,
        } or row[4] != "COMMIT":
            raise ProtocolError("source decision reference requires durable COMMIT")
        return {
            "job_id": intent.job_id,
            "graph_version_id": intent.graph_version_id,
            "checkpoint_id": intent.checkpoint_id,
            "transaction_id": intent.transaction_id,
            "sequence": intent.sequence,
            "intent_digest": intent.manifest_digest,
            "capability_digest": intent.capability_digest,
            "coordinator_owner_id": row[5],
            "coordinator_epoch": row[6],
            "source_positions": tuple(
                (partition, position)
                for partition, position in intent.source_positions
            ),
            "source_epochs": tuple(
                (partition, dict(intent.generation_manifest)[partition])
                for partition, _ in intent.source_positions
            ),
            "decision": "COMMIT",
        }

    def verify_source_decision(self, reference: Any) -> bool:
        """Verify an exact source reference against the single decision authority."""
        row = self.connection.execute(
            """
            SELECT sequence, manifest_digest, phase, decision,
                   active_owner_id, fence_epoch, intent_json
            FROM checkpoints
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
            """,
            (
                reference.job_id,
                reference.graph_version_id,
                reference.checkpoint_id,
            ),
        ).fetchone()
        if row is None:
            return False
        intent_json = json.loads(row[6])
        expected_checkpoint_id = "ariadne:checkpoint:" + digest(
            {"graph": reference.graph_version_id, "sequence": reference.sequence}
        )
        expected_transaction_id = "ariadne:transaction:" + digest(
            {
                "job": reference.job_id,
                "graph": reference.graph_version_id,
                "checkpoint": reference.checkpoint_id,
            }
        )
        return (
            row[0] == reference.sequence
            and reference.checkpoint_id == expected_checkpoint_id
            and reference.transaction_id == expected_transaction_id
            and row[1] == reference.intent_digest
            and row[2]
            in {
                Phase.COMMIT_DECIDED.value,
                Phase.COMMITTING.value,
                Phase.COMPLETED.value,
            }
            and row[3] == reference.decision == "COMMIT"
            and row[4] == reference.coordinator_owner_id
            and row[5] == reference.coordinator_epoch
            and intent_json["capability_digest"] == reference.capability_digest
            and tuple(tuple(item) for item in intent_json["source_positions"])
            == tuple(reference.source_positions)
            and tuple(
                sorted(
                    (participant, epoch)
                    for participant, epoch in intent_json["generation_manifest"]
                    if participant in dict(reference.source_positions)
                )
            )
            == tuple(reference.source_epochs)
        )

    def abort_checkpoint(
        self,
        intent: CheckpointIntent,
        abort: Callable[[str, str, str, int, int], None],
        *,
        fence: Callable[[str, str, int, int], None] | None = None,
        recover: Callable[[str, str], RecoveredSink | None] | None = None,
        attempt_fault: Fault | None = None,
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[4] == "COMMIT":
            raise ProtocolError("abort forbidden after COMMIT decision")
        if row[4] == "ABORT":
            self._reconcile_terminal_attempts(
                intent,
                expected_owner_id=self.owner_id,
                expected_fence_epoch=row[6],
                sink_terminal_state="ABORTED",
                include_sources=False,
            )
            if row[3] == Phase.ABORTED.value:
                return
            if row[3] != Phase.ABORTING.value:
                raise ProtocolError("invalid durable ABORT state")
        else:
            if Phase(row[3]) not in ABORTABLE:
                raise ProtocolError("checkpoint phase is not abortable")
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE checkpoints
                    SET phase = 'ABORTING', decision = 'ABORT', fence_epoch = ?
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND phase = ? AND decision IS NULL
                      AND active_owner_id = ? AND fence_epoch = ?
                    """,
                    (
                        row[6] + 1,
                        *self._key(intent),
                        row[3],
                        self.owner_id,
                        row[6],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("stale ABORT decision")
            row = self._row(intent)
        generations = dict(intent.generation_manifest)
        attempted = self.connection.execute(
            """
            SELECT sink_id, status, prepare_token, participant_epoch,
                   fenced_coordinator_epoch
            FROM sinks
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND status IN ('PREPARING', 'PREPARED')
            ORDER BY sink_id
            """,
            self._key(intent),
        ).fetchall()
        for (
            sink_id,
            status,
            prepare_token,
            participant_epoch,
            fenced_epoch,
        ) in attempted:
            attempt_count = self._start_recovery_attempt(
                intent,
                sink_id,
                expected_owner_id=self.owner_id,
                expected_fence_epoch=row[6],
                terminal_state="ABORTED",
            )
            if attempt_count is None:
                continue
            if attempt_fault is not None:
                attempt_fault.hit("abort_recovery_attempt:after_start")
            try:
                expected_epoch = generations[sink_id]
                if participant_epoch != expected_epoch:
                    raise ProtocolError("attempted sink has participant epoch drift")
                actual: RecoveredSink | None = None
                needs_recovery = status == "PREPARING" or fenced_epoch != row[6]
                if needs_recovery:
                    if fence is None or recover is None:
                        raise ProtocolError(
                            "sink abort requires fence and recovery for attempted prepare"
                        )
                    fence(intent.transaction_id, sink_id, row[6], expected_epoch)
                    actual = recover(intent.transaction_id, sink_id)
                    if actual is not None and actual.state == "COMMITTED":
                        raise ProtocolError(
                            "cannot abort externally committed participant"
                        )
                    if actual is not None and actual.state not in {
                        "PREPARED",
                        "ABORTED",
                    }:
                        raise ProtocolError("unknown recovered sink state")
                    if actual is not None and actual.state == "PREPARED":
                        if prepare_token is not None and actual.token != prepare_token:
                            raise ProtocolError("recovered sink prepare token drift")
                        prepare_token = actual.token
                if actual is None and status == "PREPARED" and prepare_token is None:
                    raise ProtocolError("prepared sink lacks token")
                if actual is None and status == "PREPARING":
                    prepare_token = None
                if (
                    actual is None or actual.state != "ABORTED"
                ) and prepare_token is not None:
                    abort(
                        intent.transaction_id,
                        sink_id,
                        prepare_token,
                        row[6],
                        expected_epoch,
                    )
                with self.connection:
                    cursor = self.connection.execute(
                        """
                        UPDATE sinks SET status = 'ABORTED'
                        WHERE job_id = ? AND graph_version_id = ?
                          AND checkpoint_id = ? AND sink_id = ?
                          AND status IN ('PREPARING', 'PREPARED')
                          AND EXISTS (
                              SELECT 1 FROM checkpoints
                              WHERE job_id = ? AND graph_version_id = ?
                                AND checkpoint_id = ? AND decision = 'ABORT'
                                AND phase = 'ABORTING'
                                AND active_owner_id = ? AND fence_epoch = ?
                          )
                        """,
                        (
                            *self._key(intent),
                            sink_id,
                            *self._key(intent),
                            self.owner_id,
                            row[6],
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise ProtocolError("stale sink abort acknowledgement")
            except (ConnectionError, TimeoutError) as error:
                if attempt_fault is not None:
                    attempt_fault.hit(
                        "abort_recovery_attempt:after_callback_before_result"
                    )
                self._finish_recovery_attempt(
                    intent,
                    sink_id,
                    attempt_count,
                    error,
                    expected_owner_id=self.owner_id,
                    expected_fence_epoch=row[6],
                    terminal_state="ABORTED",
                )
            else:
                if attempt_fault is not None:
                    attempt_fault.hit(
                        "abort_recovery_attempt:after_callback_before_result"
                    )
                self._finish_recovery_attempt(
                    intent,
                    sink_id,
                    attempt_count,
                    None,
                    expected_owner_id=self.owner_id,
                    expected_fence_epoch=row[6],
                    terminal_state="ABORTED",
                )
        with self.connection:
            self.connection.execute(
                """
                UPDATE sinks SET status = 'ABORTED'
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                  AND status = 'PENDING'
                """,
                self._key(intent),
            )
            incomplete = self.connection.execute(
                """
                SELECT 1 FROM sinks
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                  AND status != 'ABORTED'
                LIMIT 1
                """,
                self._key(intent),
            ).fetchone()
            if incomplete is not None:
                return
            cursor = self.connection.execute(
                """
                UPDATE checkpoints SET phase = 'ABORTED'
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                  AND decision = 'ABORT' AND phase = 'ABORTING'
                  AND active_owner_id = ? AND fence_epoch = ?
                """,
                (*self._key(intent), self.owner_id, row[6]),
            )
            if cursor.rowcount != 1:
                raise ProtocolError("stale checkpoint abort")

    def commit_sink(
        self,
        intent: CheckpointIntent,
        sink_id: str,
        participant_epoch: int,
        commit: Callable[[str, str, str, int, int], None],
        fault: Fault | None = None,
        completion_fault: Fault | None = None,
    ) -> None:
        self._assert_generation(intent, sink_id, participant_epoch)
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[4] != "COMMIT":
            raise ProtocolError("sink commit requires durable COMMIT decision")
        status = self.connection.execute(
            """
            SELECT status, prepare_token FROM sinks
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND sink_id = ?
            """,
            (*self._key(intent), sink_id),
        ).fetchone()
        if status is None:
            raise ProtocolError("unknown sink")
        if status[0] == "COMMITTED":
            if row[3] != Phase.COMPLETED.value:
                self._complete_if_committed(intent, completion_fault)
            return
        if status[1] is None:
            raise ProtocolError("prepared sink lacks token")

        def durable() -> None:
            commit(
                intent.transaction_id,
                sink_id,
                status[1],
                row[6],
                participant_epoch,
            )

        def acknowledge() -> None:
            with self.connection:
                cursor = self.connection.execute(
                    """
                    UPDATE sinks SET status = 'COMMITTED'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND sink_id = ? AND status = 'PREPARED'
                      AND EXISTS (
                          SELECT 1 FROM checkpoints
                          WHERE job_id = ? AND graph_version_id = ?
                            AND checkpoint_id = ? AND decision = 'COMMIT'
                            AND active_owner_id = ? AND fence_epoch = ?
                      )
                    """,
                    (
                        *self._key(intent),
                        sink_id,
                        *self._key(intent),
                        self.owner_id,
                        row[6],
                    ),
                )
                if cursor.rowcount != 1:
                    raise ProtocolError("stale sink commit acknowledgement")
                phase_cursor = self.connection.execute(
                    """
                    UPDATE checkpoints SET phase = 'COMMITTING'
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND phase = 'COMMIT_DECIDED' AND decision = 'COMMIT'
                      AND active_owner_id = ? AND fence_epoch = ?
                    """,
                    (*self._key(intent), self.owner_id, row[6]),
                )
                if phase_cursor.rowcount not in {0, 1}:
                    raise ProtocolError("sink commit phase CAS failed")

        self._run("sink_commit", durable, acknowledge, fault)
        self._complete_if_committed(intent, completion_fault)

    def acknowledge_source(
        self,
        intent: CheckpointIntent,
        partition_id: str,
        acknowledge: Callable[[str, str, str, int, int], None],
        fault: Fault | None = None,
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[4] != "COMMIT":
            raise ProtocolError("source ACK requires durable COMMIT decision")
        source = self.connection.execute(
            """
            SELECT position_json, ack_status FROM sources
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND partition_id = ?
            """,
            (*self._key(intent), partition_id),
        ).fetchone()
        if source is None:
            raise ProtocolError("unknown source partition")
        if source[1] == "ACKED":
            return
        position = json.loads(source[0])
        participant_epoch = dict(intent.generation_manifest)[partition_id]

        def durable_intent() -> None:
            pass

        def external_ack() -> None:
            acknowledge(
                intent.transaction_id,
                partition_id,
                position,
                row[6],
                participant_epoch,
            )

        self._run("source_ack", durable_intent, external_ack, fault)
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE sources SET ack_status = 'ACKED'
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                  AND partition_id = ? AND ack_status = 'PENDING'
                  AND EXISTS (
                    SELECT 1 FROM checkpoints
                    WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                      AND decision = 'COMMIT'
                      AND active_owner_id = ? AND fence_epoch = ?
                  )
                """,
                (
                    *self._key(intent),
                    partition_id,
                    *self._key(intent),
                    self.owner_id,
                    row[6],
                ),
            )
            if cursor.rowcount != 1:
                raise ProtocolError("source ACK state changed concurrently")

    def recover(
        self,
        intent: CheckpointIntent,
        commit: Callable[[str, str, str, int, int], None],
        acknowledge: Callable[[str, str, str, int, int], None],
        *,
        attempt_fault: Fault | None = None,
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, row[6])
        if row[4] != "COMMIT":
            return
        self._reconcile_terminal_attempts(
            intent,
            expected_owner_id=self.owner_id,
            expected_fence_epoch=row[6],
            sink_terminal_state="COMMITTED",
            include_sources=True,
        )
        generations = dict(intent.generation_manifest)
        pending_sinks = self.connection.execute(
            """
            SELECT sink_id FROM sinks
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND status != 'COMMITTED'
            """,
            self._key(intent),
        ).fetchall()
        for (sink_id,) in pending_sinks:
            attempt_count = self._start_recovery_attempt(
                intent,
                sink_id,
                expected_owner_id=self.owner_id,
                expected_fence_epoch=row[6],
                terminal_state="COMMITTED",
            )
            if attempt_count is None:
                continue
            if attempt_fault is not None:
                attempt_fault.hit("sink_recovery_attempt:after_start")
            try:
                self.commit_sink(intent, sink_id, generations[sink_id], commit)
            except (ConnectionError, TimeoutError) as error:
                if attempt_fault is not None:
                    attempt_fault.hit(
                        "sink_recovery_attempt:after_callback_before_result"
                    )
                self._finish_recovery_attempt(
                    intent,
                    sink_id,
                    attempt_count,
                    error,
                    expected_owner_id=self.owner_id,
                    expected_fence_epoch=row[6],
                    terminal_state="COMMITTED",
                )
            else:
                if attempt_fault is not None:
                    attempt_fault.hit(
                        "sink_recovery_attempt:after_callback_before_result"
                    )
                self._finish_recovery_attempt(
                    intent,
                    sink_id,
                    attempt_count,
                    None,
                    expected_owner_id=self.owner_id,
                    expected_fence_epoch=row[6],
                    terminal_state="COMMITTED",
                )
        pending_sources = self.connection.execute(
            """
            SELECT partition_id FROM sources
            WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
              AND ack_status != 'ACKED'
            """,
            self._key(intent),
        ).fetchall()
        for (partition_id,) in pending_sources:
            attempt_count = self._start_recovery_attempt(
                intent,
                partition_id,
                expected_owner_id=self.owner_id,
                expected_fence_epoch=row[6],
                terminal_state="ACKED",
            )
            if attempt_count is None:
                continue
            if attempt_fault is not None:
                attempt_fault.hit("source_recovery_attempt:after_start")
            try:
                self.acknowledge_source(intent, partition_id, acknowledge)
            except (ConnectionError, TimeoutError) as error:
                if attempt_fault is not None:
                    attempt_fault.hit(
                        "source_recovery_attempt:after_callback_before_result"
                    )
                self._finish_recovery_attempt(
                    intent,
                    partition_id,
                    attempt_count,
                    error,
                    expected_owner_id=self.owner_id,
                    expected_fence_epoch=row[6],
                    terminal_state="ACKED",
                )
            else:
                if attempt_fault is not None:
                    attempt_fault.hit(
                        "source_recovery_attempt:after_callback_before_result"
                    )
                self._finish_recovery_attempt(
                    intent,
                    partition_id,
                    attempt_count,
                    None,
                    expected_owner_id=self.owner_id,
                    expected_fence_epoch=row[6],
                    terminal_state="ACKED",
                )

    def _start_recovery_attempt(
        self,
        intent: CheckpointIntent,
        participant_id: str,
        *,
        expected_owner_id: str,
        expected_fence_epoch: int,
        terminal_state: str,
    ) -> int | None:
        if terminal_state == "ACKED":
            participant_table = "sources"
            participant_column = "partition_id"
            state_column = "ack_status"
        elif terminal_state in {"COMMITTED", "ABORTED"}:
            participant_table = "sinks"
            participant_column = "sink_id"
            state_column = "status"
        else:
            raise ProtocolError("unknown recovery terminal state")
        with self.connection:
            row = self.connection.execute(
                f"""
                INSERT INTO recovery_attempts (
                    job_id, graph_version_id, checkpoint_id, participant_id,
                    owner_id, fence_epoch, attempt_count, blocked,
                    outcome, last_error
                )
                SELECT ?, ?, ?, ?, ?, ?, 1, 1, 'IN_PROGRESS',
                       'outcome unknown until participant reconciliation'
                WHERE EXISTS (
                    SELECT 1
                    FROM checkpoints c
                    JOIN {participant_table} p
                      ON p.job_id = c.job_id
                     AND p.graph_version_id = c.graph_version_id
                     AND p.checkpoint_id = c.checkpoint_id
                    WHERE c.job_id = ? AND c.graph_version_id = ?
                      AND c.checkpoint_id = ?
                      AND p.{participant_column} = ?
                      AND c.active_owner_id = ? AND c.fence_epoch = ?
                      AND p.{state_column} != ?
                )
                ON CONFLICT(job_id, graph_version_id, checkpoint_id, participant_id)
                DO UPDATE SET
                    attempt_count = recovery_attempts.attempt_count + 1,
                    owner_id = excluded.owner_id,
                    fence_epoch = excluded.fence_epoch,
                    blocked = 1,
                    outcome = 'IN_PROGRESS',
                    last_error = excluded.last_error
                RETURNING attempt_count
                """,
                (
                    *self._key(intent),
                    participant_id,
                    expected_owner_id,
                    expected_fence_epoch,
                    *self._key(intent),
                    participant_id,
                    expected_owner_id,
                    expected_fence_epoch,
                    terminal_state,
                ),
            ).fetchone()
        return None if row is None else row[0]

    def _reconcile_terminal_attempts(
        self,
        intent: CheckpointIntent,
        *,
        expected_owner_id: str,
        expected_fence_epoch: int,
        sink_terminal_state: str,
        include_sources: bool,
    ) -> None:
        if sink_terminal_state not in {"COMMITTED", "ABORTED"}:
            raise ProtocolError("unknown sink recovery terminal state")
        source_clause = (
            """
                OR EXISTS (
                    SELECT 1 FROM sources s
                    WHERE s.job_id = recovery_attempts.job_id
                      AND s.graph_version_id = recovery_attempts.graph_version_id
                      AND s.checkpoint_id = recovery_attempts.checkpoint_id
                      AND s.partition_id = recovery_attempts.participant_id
                      AND s.ack_status = 'ACKED'
                )
            """
            if include_sources
            else ""
        )
        with self.connection:
            self.connection.execute(
                f"""
                UPDATE recovery_attempts
                SET owner_id = ?, fence_epoch = ?, blocked = 0,
                    outcome = 'SUCCEEDED', last_error = NULL
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                  AND outcome != 'SUCCEEDED'
                  AND EXISTS (
                      SELECT 1 FROM checkpoints c
                      WHERE c.job_id = recovery_attempts.job_id
                        AND c.graph_version_id = recovery_attempts.graph_version_id
                        AND c.checkpoint_id = recovery_attempts.checkpoint_id
                        AND c.active_owner_id = ? AND c.fence_epoch = ?
                  )
                  AND (
                      EXISTS (
                          SELECT 1 FROM sinks s
                          WHERE s.job_id = recovery_attempts.job_id
                            AND s.graph_version_id = recovery_attempts.graph_version_id
                            AND s.checkpoint_id = recovery_attempts.checkpoint_id
                            AND s.sink_id = recovery_attempts.participant_id
                            AND s.status = ?
                      )
                      {source_clause}
                  )
                """,
                (
                    expected_owner_id,
                    expected_fence_epoch,
                    *self._key(intent),
                    expected_owner_id,
                    expected_fence_epoch,
                    sink_terminal_state,
                ),
            )

    def _finish_recovery_attempt(
        self,
        intent: CheckpointIntent,
        participant_id: str,
        attempt_count: int,
        error: Exception | None,
        *,
        expected_owner_id: str,
        expected_fence_epoch: int,
        terminal_state: str,
    ) -> None:
        if terminal_state == "ACKED":
            participant_table = "sources"
            participant_column = "partition_id"
            state_column = "ack_status"
        elif terminal_state in {"COMMITTED", "ABORTED"}:
            participant_table = "sinks"
            participant_column = "sink_id"
            state_column = "status"
        else:
            raise ProtocolError("unknown recovery terminal state")
        authority = """
            EXISTS (
                SELECT 1 FROM checkpoints c
                WHERE c.job_id = recovery_attempts.job_id
                  AND c.graph_version_id = recovery_attempts.graph_version_id
                  AND c.checkpoint_id = recovery_attempts.checkpoint_id
                  AND c.active_owner_id = recovery_attempts.owner_id
                  AND c.fence_epoch = recovery_attempts.fence_epoch
            )
        """
        participant_is = f"""
            EXISTS (
                SELECT 1 FROM {participant_table} p
                WHERE p.job_id = recovery_attempts.job_id
                  AND p.graph_version_id = recovery_attempts.graph_version_id
                  AND p.checkpoint_id = recovery_attempts.checkpoint_id
                  AND p.{participant_column} = recovery_attempts.participant_id
                  AND p.{state_column} = ?
            )
        """
        identity = """
            job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
            AND participant_id = ? AND owner_id = ? AND fence_epoch = ?
            AND attempt_count = ? AND outcome = 'IN_PROGRESS'
        """
        with self.connection:
            terminal = self.connection.execute(
                f"""
                UPDATE recovery_attempts
                SET blocked = 0, outcome = 'SUCCEEDED', last_error = NULL
                WHERE {identity} AND {authority} AND {participant_is}
                """,
                (
                    *self._key(intent),
                    participant_id,
                    expected_owner_id,
                    expected_fence_epoch,
                    attempt_count,
                    terminal_state,
                ),
            )
            if terminal.rowcount == 0 and error is not None:
                self.connection.execute(
                    f"""
                    UPDATE recovery_attempts
                    SET blocked = 1, outcome = 'BLOCKED', last_error = ?
                    WHERE {identity} AND {authority}
                      AND NOT {participant_is}
                    """,
                    (
                        f"{type(error).__name__}: {error}",
                        *self._key(intent),
                        participant_id,
                        expected_owner_id,
                        expected_fence_epoch,
                        attempt_count,
                        terminal_state,
                    ),
                )

    def recovery_status(self, intent: CheckpointIntent) -> dict[str, dict[str, Any]]:
        return {
            participant_id: {
                "owner_id": owner_id,
                "fence_epoch": fence_epoch,
                "attempt_count": attempt_count,
                "blocked": bool(blocked),
                "outcome": outcome,
                "last_error": last_error,
            }
            for (
                participant_id,
                owner_id,
                fence_epoch,
                attempt_count,
                blocked,
                outcome,
                last_error,
            ) in self.connection.execute(
                """
                SELECT participant_id, owner_id, fence_epoch,
                       attempt_count, blocked, outcome, last_error
                FROM recovery_attempts
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                ORDER BY participant_id
                """,
                self._key(intent),
            )
        }

    def latest_recovery_checkpoint(
        self, job_id: str, graph_version_id: str
    ) -> int | None:
        return self.connection.execute(
            """
            SELECT MAX(sequence) FROM checkpoints
            WHERE job_id = ? AND graph_version_id = ? AND decision = 'COMMIT'
            """,
            (job_id, graph_version_id),
        ).fetchone()[0]

    def acknowledged_positions(
        self, job_id: str, graph_version_id: str
    ) -> dict[str, str]:
        rows = self.connection.execute(
            """
            SELECT s.partition_id, s.position_json, c.sequence
            FROM sources s JOIN checkpoints c USING(job_id, graph_version_id, checkpoint_id)
            WHERE s.job_id = ? AND s.graph_version_id = ?
              AND s.ack_status = 'ACKED' AND c.decision = 'COMMIT'
            ORDER BY c.sequence
            """,
            (job_id, graph_version_id),
        )
        positions: dict[str, str] = {}
        for partition_id, position_json, _ in rows:
            positions[partition_id] = json.loads(position_json)
        return positions

    def raw_observation(self, intent: CheckpointIntent) -> Mapping[str, Any]:
        row = self._row(intent)
        sinks = dict(
            self.connection.execute(
                """
                SELECT sink_id, status FROM sinks
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                """,
                self._key(intent),
            )
        )
        sources = {
            partition: {"position": json.loads(position), "status": status}
            for partition, position, status in self.connection.execute(
                """
                SELECT partition_id, position_json, ack_status FROM sources
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                """,
                self._key(intent),
            )
        }
        artifacts = set(
            self.connection.execute(
                """
                SELECT kind, artifact_id, owner_id FROM artifacts
                WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                """,
                self._key(intent),
            )
        )
        return {
            "job_id": intent.job_id,
            "graph_version_id": intent.graph_version_id,
            "checkpoint_id": intent.checkpoint_id,
            "transaction_id": intent.transaction_id,
            "sequence": intent.sequence,
            "phase": row[3],
            "decision": row[4],
            "required_sinks": intent.sink_manifest,
            "sinks": sinks,
            "required_sources": dict(intent.source_positions),
            "sources": sources,
            "required_artifacts": set(intent.artifact_manifest),
            "artifacts": artifacts,
            "expected_effects": set(intent.expected_effects),
            "effective_tier": intent.effective_tier.value,
        }

    def _assert_generation(
        self, intent: CheckpointIntent, participant_id: str, epoch: int
    ) -> None:
        expected = dict(intent.generation_manifest).get(participant_id)
        if expected is None or expected != epoch:
            raise ProtocolError("stale or unknown participant generation")

    def _complete_if_committed(
        self, intent: CheckpointIntent, fault: Fault | None = None
    ) -> None:
        row = self._row(intent)
        self._assert_owner(row, row[6])
        sinks, _ = self._participant_rows(intent)
        if all(status == "COMMITTED" for status in sinks.values()):

            def write() -> None:
                with self.connection:
                    cursor = self.connection.execute(
                        """
                        UPDATE checkpoints SET phase = 'COMPLETED'
                        WHERE job_id = ? AND graph_version_id = ? AND checkpoint_id = ?
                          AND phase IN ('COMMIT_DECIDED', 'COMMITTING')
                          AND decision = 'COMMIT'
                          AND active_owner_id = ? AND fence_epoch = ?
                        """,
                        (*self._key(intent), self.owner_id, row[6]),
                    )
                    if cursor.rowcount != 1:
                        raise ProtocolError("checkpoint completion CAS failed")

            self._run("checkpoint_completion", write, fault=fault)


@dataclass(frozen=True)
class OracleResult:
    invariants: Mapping[str, bool]

    @property
    def passed(self) -> bool:
        return all(self.invariants.values())


@dataclass(frozen=True)
class VisibleEffect:
    transaction_id: str
    sink_id: str
    record_id: str
    payload_digest: str


class Oracle:
    """Independent checker over raw durable and external observations."""

    @staticmethod
    def evaluate(
        observation: Mapping[str, Any],
        *,
        selected_sequence: int | None,
        externally_committed_sinks: set[tuple[str, str]],
        externally_acked_sources: set[tuple[str, str]],
        visible_effects: Sequence[VisibleEffect] = (),
    ) -> OracleResult:
        transaction_id = str(observation["transaction_id"])
        decision = observation["decision"]
        required_sinks = set(observation["required_sinks"])
        local_sinks = observation["sinks"]
        required_sources = set(observation["required_sources"])
        local_sources = observation["sources"]
        required_artifacts = set(observation.get("required_artifacts", set()))
        artifacts = set(observation.get("artifacts", set()))
        expected_effects = set(observation.get("expected_effects", set()))
        externally_committed = {
            sink
            for transaction, sink in externally_committed_sinks
            if transaction == transaction_id
        }
        externally_acked = {
            source
            for transaction, source in externally_acked_sources
            if transaction == transaction_id
        }
        recoverable = observation["sequence"] if decision == "COMMIT" else None
        relevant_effects = [
            effect
            for effect in visible_effects
            if effect.transaction_id == transaction_id
        ]
        effect_keys = [
            (effect.sink_id, effect.record_id) for effect in relevant_effects
        ]
        observed_effects = {
            (effect.sink_id, effect.record_id, effect.payload_digest)
            for effect in relevant_effects
        }
        expected_for_committed_sinks = {
            effect for effect in expected_effects if effect[0] in externally_committed
        }
        observed_for_committed_sinks = {
            effect for effect in observed_effects if effect[0] in externally_committed
        }
        payloads: dict[tuple[str, str], set[str]] = {}
        for effect in relevant_effects:
            payloads.setdefault((effect.sink_id, effect.record_id), set()).add(
                effect.payload_digest
            )
        effective_tier = SinkTier(observation.get("effective_tier", "T"))
        return OracleResult(
            {
                "selected_checkpoint_has_decision": selected_sequence == recoverable,
                "external_sink_effect_requires_decision": (
                    not externally_committed and not relevant_effects
                )
                or decision == "COMMIT",
                "external_sink_participants_are_expected": externally_committed
                <= required_sinks,
                "committed_sink_effects_are_complete": (
                    observed_for_committed_sinks == expected_for_committed_sinks
                ),
                "external_source_ack_requires_decision": not externally_acked
                or decision == "COMMIT",
                "external_source_participants_are_expected": externally_acked
                <= required_sources,
                "completed_has_every_sink": observation["phase"] != "COMPLETED"
                or (
                    externally_committed == required_sinks
                    and all(
                        local_sinks.get(sink) == "COMMITTED" for sink in required_sinks
                    )
                ),
                "local_source_ack_matches_external": all(
                    (local_sources.get(source, {}).get("status") == "ACKED")
                    == (source in externally_acked)
                    for source in required_sources
                ),
                "participant_sets_are_complete": set(local_sinks) == required_sinks
                and set(local_sources) == required_sources,
                "decided_checkpoint_has_restorable_artifacts": (
                    decision != "COMMIT" and observation["phase"] != "COMPLETED"
                )
                or artifacts == required_artifacts,
                "visible_effect_payload_is_stable": all(
                    len(values) == 1 for values in payloads.values()
                ),
                "visible_effects_are_complete": (
                    decision != "COMMIT" and not observed_effects
                )
                or (
                    decision == "COMMIT"
                    and (
                        (
                            observation["phase"] == Phase.COMPLETED.value
                            and observed_effects == expected_effects
                        )
                        or (
                            observation["phase"] != Phase.COMPLETED.value
                            and observed_effects <= expected_effects
                        )
                    )
                ),
                "visible_duplicate_policy_holds": effective_tier is SinkTier.B
                or len(effect_keys) == len(set(effect_keys)),
            }
        )

    @staticmethod
    def evaluate_global_recovery(
        observations: Sequence[Mapping[str, Any]],
        selected: Mapping[tuple[str, str], int | None],
    ) -> bool:
        expected: dict[tuple[str, str], int | None] = {}
        for observation in observations:
            scope = (
                str(observation["job_id"]),
                str(observation["graph_version_id"]),
            )
            if observation["decision"] == "COMMIT":
                sequence = int(observation["sequence"])
                current = expected.get(scope)
                expected[scope] = (
                    sequence if current is None else max(current, sequence)
                )
            else:
                expected.setdefault(scope, None)
        return dict(selected) == expected
