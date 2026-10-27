"""Sprint 1 safety-contract prototypes for the streaming ETL ADRs.

The module is deliberately independent from Prefect production code.  It makes
identity, lifecycle, capability, fault, and oracle rules executable before the
runtime and storage APIs are selected.
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


class ContractViolation(RuntimeError):
    """Raised when a deployment or protocol operation violates its contract."""


class DeterministicCrash(RuntimeError):
    """Raised at an explicitly named failure point."""


def canonical_digest(value: Any) -> str:
    """Return a stable digest for JSON-compatible protocol data."""
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _identity(kind: str, manifest: Mapping[str, Any]) -> str:
    return f"ariadne:{kind}:{canonical_digest(manifest)}"


@dataclass(frozen=True)
class StableIdentities:
    """Derive canonical protocol identities from immutable lineage."""

    tenant: str
    job_name: str
    graph_spec: Mapping[str, Any]

    @property
    def job_id(self) -> str:
        return _identity("job", {"tenant": self.tenant, "job": self.job_name})

    @property
    def graph_version_id(self) -> str:
        return _identity(
            "graph",
            {"job_id": self.job_id, "graph_spec": self.graph_spec},
        )

    def operator_id(self, operator_name: str) -> str:
        return _identity(
            "operator",
            {"graph_version_id": self.graph_version_id, "name": operator_name},
        )

    def edge_id(
        self,
        source_operator: str,
        source_port: str,
        target_operator: str,
        target_port: str,
    ) -> str:
        return _identity(
            "edge",
            {
                "graph_version_id": self.graph_version_id,
                "source": self.operator_id(source_operator),
                "source_port": source_port,
                "target": self.operator_id(target_operator),
                "target_port": target_port,
            },
        )

    def partition_id(self, edge_id: str, partition: int) -> str:
        if partition < 0:
            raise ValueError("partition must be non-negative")
        return _identity(
            "partition",
            {
                "graph_version_id": self.graph_version_id,
                "edge_id": edge_id,
                "partition": partition,
            },
        )

    def record_id(self, source_partition_id: str, source_position: str | int) -> str:
        return _identity(
            "record",
            {
                "graph_version_id": self.graph_version_id,
                "source_partition_id": source_partition_id,
                "source_position": source_position,
            },
        )

    def checkpoint_id(self, sequence: int) -> str:
        if sequence < 1:
            raise ValueError("checkpoint sequence must be positive")
        return _identity(
            "checkpoint",
            {"graph_version_id": self.graph_version_id, "sequence": sequence},
        )

    def generation_id(self, partition_id: str, epoch: int) -> str:
        if epoch < 1:
            raise ValueError("generation epoch must be positive")
        return _identity(
            "generation",
            {
                "graph_version_id": self.graph_version_id,
                "partition_id": partition_id,
                "epoch": epoch,
            },
        )


class IdentityRegistry:
    """Persist immutable identity manifests and reject semantic reuse."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS identities (
                kind TEXT NOT NULL,
                identity TEXT NOT NULL,
                manifest_digest TEXT NOT NULL,
                PRIMARY KEY (kind, identity)
            )
            """
        )

    def claim(self, kind: str, identity: str, manifest: Mapping[str, Any]) -> None:
        digest = canonical_digest(manifest)
        existing = self.connection.execute(
            "SELECT manifest_digest FROM identities WHERE kind = ? AND identity = ?",
            (kind, identity),
        ).fetchone()
        if existing is not None:
            if existing[0] != digest:
                raise ContractViolation(
                    f"{kind} identity {identity!r} was reused with a different manifest"
                )
            return
        self.connection.execute(
            "INSERT INTO identities VALUES (?, ?, ?)",
            (kind, identity, digest),
        )
        self.connection.commit()


class CheckpointPhase(str, Enum):
    STARTED = "STARTED"
    ALIGNING = "ALIGNING"
    ALIGNED = "ALIGNED"
    PREPARING = "PREPARING"
    PREPARED = "PREPARED"
    COMMIT_DECIDED = "COMMIT_DECIDED"
    COMMITTING = "COMMITTING"
    COMPLETED = "COMPLETED"
    ABORTED = "ABORTED"


_ALLOWED_TRANSITIONS: dict[CheckpointPhase, frozenset[CheckpointPhase]] = {
    CheckpointPhase.STARTED: frozenset(
        {CheckpointPhase.ALIGNING, CheckpointPhase.ABORTED}
    ),
    CheckpointPhase.ALIGNING: frozenset(
        {CheckpointPhase.ALIGNED, CheckpointPhase.ABORTED}
    ),
    CheckpointPhase.ALIGNED: frozenset(
        {CheckpointPhase.PREPARING, CheckpointPhase.COMPLETED, CheckpointPhase.ABORTED}
    ),
    CheckpointPhase.PREPARING: frozenset(
        {CheckpointPhase.PREPARED, CheckpointPhase.ABORTED}
    ),
    CheckpointPhase.PREPARED: frozenset(
        {CheckpointPhase.COMMIT_DECIDED, CheckpointPhase.ABORTED}
    ),
    CheckpointPhase.COMMIT_DECIDED: frozenset(
        {CheckpointPhase.COMMITTING, CheckpointPhase.COMPLETED}
    ),
    CheckpointPhase.COMMITTING: frozenset({CheckpointPhase.COMPLETED}),
    CheckpointPhase.COMPLETED: frozenset(),
    CheckpointPhase.ABORTED: frozenset(),
}


class CheckpointLifecycle:
    """Durable checkpoint state machine with an immutable intent manifest."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS checkpoint_lifecycles (
                checkpoint_id TEXT PRIMARY KEY,
                manifest_digest TEXT NOT NULL,
                requires_commit INTEGER NOT NULL,
                phase TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoint_history (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                checkpoint_id TEXT NOT NULL,
                phase TEXT NOT NULL
            );
            """
        )

    def start(
        self,
        checkpoint_id: str,
        manifest: Mapping[str, Any],
        *,
        requires_commit: bool,
    ) -> None:
        digest = canonical_digest(manifest)
        existing = self.connection.execute(
            """
            SELECT manifest_digest, requires_commit FROM checkpoint_lifecycles
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        expected = (digest, int(requires_commit))
        if existing is not None:
            if existing != expected:
                raise ContractViolation(
                    f"checkpoint {checkpoint_id!r} intent changed after creation"
                )
            return
        with self.connection:
            self.connection.execute(
                "INSERT INTO checkpoint_lifecycles VALUES (?, ?, ?, ?)",
                (
                    checkpoint_id,
                    digest,
                    int(requires_commit),
                    CheckpointPhase.STARTED.value,
                ),
            )
            self.connection.execute(
                "INSERT INTO checkpoint_history(checkpoint_id, phase) VALUES (?, ?)",
                (checkpoint_id, CheckpointPhase.STARTED.value),
            )

    def phase(self, checkpoint_id: str) -> CheckpointPhase:
        row = self.connection.execute(
            "SELECT phase FROM checkpoint_lifecycles WHERE checkpoint_id = ?",
            (checkpoint_id,),
        ).fetchone()
        if row is None:
            raise KeyError(checkpoint_id)
        return CheckpointPhase(row[0])

    def advance(self, checkpoint_id: str, target: CheckpointPhase) -> None:
        row = self.connection.execute(
            """
            SELECT phase, requires_commit FROM checkpoint_lifecycles
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        if row is None:
            raise KeyError(checkpoint_id)
        current = CheckpointPhase(row[0])
        requires_commit = bool(row[1])
        if current == target:
            return
        if target not in _ALLOWED_TRANSITIONS[current]:
            raise ContractViolation(
                f"illegal checkpoint transition {current.value} -> {target.value}"
            )
        if (
            requires_commit
            and current is CheckpointPhase.ALIGNED
            and target is CheckpointPhase.COMPLETED
        ):
            raise ContractViolation(
                "cooperative sink checkpoint cannot complete before COMMIT_DECIDED"
            )
        if not requires_commit and target in {
            CheckpointPhase.PREPARING,
            CheckpointPhase.PREPARED,
            CheckpointPhase.COMMIT_DECIDED,
            CheckpointPhase.COMMITTING,
        }:
            raise ContractViolation("checkpoint has no cooperative sink participants")
        with self.connection:
            self.connection.execute(
                "UPDATE checkpoint_lifecycles SET phase = ? WHERE checkpoint_id = ?",
                (target.value, checkpoint_id),
            )
            self.connection.execute(
                "INSERT INTO checkpoint_history(checkpoint_id, phase) VALUES (?, ?)",
                (checkpoint_id, target.value),
            )

    def history(self, checkpoint_id: str) -> list[CheckpointPhase]:
        return [
            CheckpointPhase(row[0])
            for row in self.connection.execute(
                """
                SELECT phase FROM checkpoint_history
                WHERE checkpoint_id = ? ORDER BY sequence
                """,
                (checkpoint_id,),
            )
        ]


class ParticipantRole(str, Enum):
    SOURCE = "source"
    OPERATOR = "operator"
    CHANNEL = "channel"
    STATE_BACKEND = "state_backend"
    SINK = "sink"


class SinkTier(str, Enum):
    BASIC = "B"
    IDEMPOTENT = "I"
    TRANSACTIONAL = "T"


_SINK_TIER_RANK = {
    SinkTier.BASIC: 0,
    SinkTier.IDEMPOTENT: 1,
    SinkTier.TRANSACTIONAL: 2,
}


@dataclass(frozen=True)
class ParticipantContract:
    participant_id: str
    role: ParticipantRole
    protocol_version: int = 1
    stable_ids: bool = False
    replayable: bool = False
    deterministic: bool = False
    managed_state: bool = False
    durable: bool = False
    fenced_writes: bool = False
    idempotent_writes: bool = False
    prepare_commit_recover: bool = False
    sink_tier: SinkTier | None = None


@dataclass(frozen=True)
class GuaranteeRequest:
    protocol_version: int = 1
    exact_internal_state: bool = True
    minimum_sink_tier: SinkTier = SinkTier.BASIC


@dataclass(frozen=True)
class NegotiatedGuarantee:
    protocol_version: int
    exact_internal_state: bool
    sink_tier: SinkTier
    participant_ids: tuple[str, ...]


def negotiate_contracts(
    request: GuaranteeRequest,
    participants: Sequence[ParticipantContract],
) -> NegotiatedGuarantee:
    """Reject a deployment unless every requested guarantee is supported."""
    ids = [participant.participant_id for participant in participants]
    if len(ids) != len(set(ids)):
        raise ContractViolation("participant IDs must be unique")
    if any(
        participant.protocol_version != request.protocol_version
        for participant in participants
    ):
        raise ContractViolation("participant protocol version mismatch")

    by_role = {
        role: [participant for participant in participants if participant.role is role]
        for role in ParticipantRole
    }
    missing = [role.value for role, values in by_role.items() if not values]
    if missing:
        raise ContractViolation(f"missing participant roles: {', '.join(missing)}")

    if request.exact_internal_state:
        if any(
            not (source.stable_ids and source.replayable)
            for source in by_role[ParticipantRole.SOURCE]
        ):
            raise ContractViolation("source must provide stable IDs and replay")
        if any(
            not (operator.deterministic and operator.managed_state)
            for operator in by_role[ParticipantRole.OPERATOR]
        ):
            raise ContractViolation("operator must be deterministic with managed state")
        if any(
            not (
                channel.stable_ids
                and channel.replayable
                and channel.durable
                and channel.fenced_writes
            )
            for channel in by_role[ParticipantRole.CHANNEL]
        ):
            raise ContractViolation("channel lacks durable replay or fencing")
        if any(
            not (backend.durable and backend.fenced_writes)
            for backend in by_role[ParticipantRole.STATE_BACKEND]
        ):
            raise ContractViolation("state backend lacks durability or fencing")

    sink_tiers = [
        sink.sink_tier or SinkTier.BASIC for sink in by_role[ParticipantRole.SINK]
    ]
    effective_tier = min(sink_tiers, key=_SINK_TIER_RANK.__getitem__)
    if _SINK_TIER_RANK[effective_tier] < _SINK_TIER_RANK[request.minimum_sink_tier]:
        raise ContractViolation(
            f"effective sink tier {effective_tier.value} is below requested "
            f"{request.minimum_sink_tier.value}"
        )
    for sink in by_role[ParticipantRole.SINK]:
        if (
            request.minimum_sink_tier is SinkTier.IDEMPOTENT
            and not sink.idempotent_writes
        ):
            raise ContractViolation("tier I sink must provide idempotent writes")
        if request.minimum_sink_tier is SinkTier.TRANSACTIONAL and not (
            sink.prepare_commit_recover
            and sink.idempotent_writes
            and sink.fenced_writes
        ):
            raise ContractViolation(
                "tier T sink must provide prepare/commit/recover, idempotency, and fencing"
            )

    return NegotiatedGuarantee(
        protocol_version=request.protocol_version,
        exact_internal_state=request.exact_internal_state,
        sink_tier=effective_tier,
        participant_ids=tuple(sorted(ids)),
    )


DURABLE_BOUNDARIES = (
    "channel_append",
    "snapshot",
    "checkpoint_decision",
    "sink_prepare",
    "commit_decision",
    "sink_commit",
    "source_ack",
)
FAILURE_OFFSETS = ("before", "after_durable_before_ack", "after_ack")
FAILURE_POINTS = tuple(
    f"{boundary}:{offset}"
    for boundary in DURABLE_BOUNDARIES
    for offset in FAILURE_OFFSETS
)


class FaultInjector:
    """Inject one deterministic failure without timing or sleeps."""

    def __init__(self, crash_at: str | None = None):
        if crash_at is not None and crash_at not in FAILURE_POINTS:
            raise ValueError(f"unknown failure point {crash_at!r}")
        self.crash_at = crash_at
        self.visited: list[str] = []

    def hit(self, point: str) -> None:
        self.visited.append(point)
        if self.crash_at == point:
            raise DeterministicCrash(point)

    def run_boundary(
        self,
        boundary: str,
        durable_write: Callable[[], None],
        acknowledge: Callable[[], None],
    ) -> None:
        if boundary not in DURABLE_BOUNDARIES:
            raise ValueError(f"unknown durable boundary {boundary!r}")
        self.hit(f"{boundary}:before")
        durable_write()
        self.hit(f"{boundary}:after_durable_before_ack")
        acknowledge()
        self.hit(f"{boundary}:after_ack")


@dataclass(frozen=True)
class CombinedCheckpointIntent:
    """Immutable lineage shared by checkpoint, sink decision, and source ACK."""

    checkpoint_id: str
    checkpoint_sequence: int
    graph_version_id: str
    generation_id: str
    operator_manifest: tuple[str, ...]
    sink_manifest: tuple[str, ...]
    source_position: int

    @property
    def manifest_digest(self) -> str:
        return canonical_digest(
            {
                "checkpoint_id": self.checkpoint_id,
                "checkpoint_sequence": self.checkpoint_sequence,
                "graph_version_id": self.graph_version_id,
                "generation_id": self.generation_id,
                "operator_manifest": self.operator_manifest,
                "sink_manifest": self.sink_manifest,
                "source_position": self.source_position,
            }
        )


class CombinedProgressStore:
    """One durable lineage for checkpoint recovery, sink outcome, and source ACK."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS combined_progress (
                checkpoint_id TEXT PRIMARY KEY,
                checkpoint_sequence INTEGER NOT NULL UNIQUE,
                manifest_digest TEXT NOT NULL,
                graph_version_id TEXT NOT NULL,
                generation_id TEXT NOT NULL,
                operator_manifest_json TEXT NOT NULL,
                sink_manifest_json TEXT NOT NULL,
                source_position INTEGER NOT NULL,
                phase TEXT NOT NULL CHECK (
                    phase IN ('STARTED', 'PREPARED', 'COMMIT_DECIDED', 'COMPLETED')
                ),
                decision TEXT CHECK (decision IS NULL OR decision = 'COMMIT'),
                source_acknowledged INTEGER NOT NULL DEFAULT 0,
                CHECK (source_acknowledged = 0 OR decision = 'COMMIT')
            );
            """
        )

    @staticmethod
    def _row(intent: CombinedCheckpointIntent) -> tuple[Any, ...]:
        return (
            intent.checkpoint_id,
            intent.checkpoint_sequence,
            intent.manifest_digest,
            intent.graph_version_id,
            intent.generation_id,
            json.dumps(intent.operator_manifest),
            json.dumps(intent.sink_manifest),
            intent.source_position,
        )

    def _assert_manifest(self, intent: CombinedCheckpointIntent) -> tuple[Any, ...]:
        row = self.connection.execute(
            """
            SELECT checkpoint_sequence, manifest_digest, graph_version_id,
                   generation_id, operator_manifest_json, sink_manifest_json,
                   source_position, phase, decision, source_acknowledged
            FROM combined_progress WHERE checkpoint_id = ?
            """,
            (intent.checkpoint_id,),
        ).fetchone()
        if row is None:
            raise KeyError(intent.checkpoint_id)
        expected = (
            intent.checkpoint_sequence,
            intent.manifest_digest,
            intent.graph_version_id,
            intent.generation_id,
            json.dumps(intent.operator_manifest),
            json.dumps(intent.sink_manifest),
            intent.source_position,
        )
        if row[:7] != expected:
            raise ContractViolation(
                f"checkpoint {intent.checkpoint_id!r} combined manifest changed"
            )
        return row

    def start(self, intent: CombinedCheckpointIntent) -> None:
        existing = self.connection.execute(
            "SELECT checkpoint_id FROM combined_progress WHERE checkpoint_id = ?",
            (intent.checkpoint_id,),
        ).fetchone()
        if existing is not None:
            self._assert_manifest(intent)
            return
        try:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO combined_progress(
                        checkpoint_id, checkpoint_sequence, manifest_digest,
                        graph_version_id, generation_id, operator_manifest_json,
                        sink_manifest_json, source_position, phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'STARTED')
                    """,
                    self._row(intent),
                )
        except sqlite3.IntegrityError as exc:
            raise ContractViolation(
                f"checkpoint sequence {intent.checkpoint_sequence} was reused"
            ) from exc

    def prepared(self, intent: CombinedCheckpointIntent) -> None:
        row = self._assert_manifest(intent)
        phase = row[7]
        if phase in {"PREPARED", "COMMIT_DECIDED", "COMPLETED"}:
            return
        if phase != "STARTED":
            raise ContractViolation(f"cannot prepare checkpoint in phase {phase}")
        with self.connection:
            self.connection.execute(
                "UPDATE combined_progress SET phase = 'PREPARED' WHERE checkpoint_id = ?",
                (intent.checkpoint_id,),
            )

    def decide_commit(
        self,
        intent: CombinedCheckpointIntent,
        injector: FaultInjector | None = None,
    ) -> None:
        row = self._assert_manifest(intent)
        if row[7] in {"COMMIT_DECIDED", "COMPLETED"} and row[8] == "COMMIT":
            return
        if row[7] != "PREPARED":
            raise ContractViolation(
                "commit decision requires every participant prepared"
            )

        def durable_write() -> None:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE combined_progress
                    SET phase = 'COMMIT_DECIDED', decision = 'COMMIT'
                    WHERE checkpoint_id = ? AND manifest_digest = ?
                    """,
                    (intent.checkpoint_id, intent.manifest_digest),
                )

        if injector is None:
            durable_write()
        else:
            injector.run_boundary("commit_decision", durable_write, lambda: None)

    def publish_and_ack(
        self,
        intent: CombinedCheckpointIntent,
        injector: FaultInjector | None = None,
    ) -> None:
        row = self._assert_manifest(intent)
        if row[7] == "COMPLETED" and row[9] == 1:
            return
        if row[7] != "COMMIT_DECIDED" or row[8] != "COMMIT":
            raise ContractViolation(
                "source acknowledgement requires matching durable COMMIT_DECIDED"
            )

        def durable_write() -> None:
            with self.connection:
                self.connection.execute(
                    """
                    UPDATE combined_progress
                    SET phase = 'COMPLETED', source_acknowledged = 1
                    WHERE checkpoint_id = ? AND manifest_digest = ?
                          AND decision = 'COMMIT'
                    """,
                    (intent.checkpoint_id, intent.manifest_digest),
                )

        if injector is None:
            durable_write()
        else:
            injector.run_boundary("source_ack", durable_write, lambda: None)

    def recover(self) -> None:
        """Publish every durable decision; ignore undecided prepared work."""
        with self.connection:
            self.connection.execute(
                """
                UPDATE combined_progress
                SET phase = 'COMPLETED', source_acknowledged = 1
                WHERE phase = 'COMMIT_DECIDED' AND decision = 'COMMIT'
                """
            )

    def latest_recovery_checkpoint(self) -> int | None:
        row = self.connection.execute(
            """
            SELECT MAX(checkpoint_sequence) FROM combined_progress
            WHERE decision = 'COMMIT'
              AND phase IN ('COMMIT_DECIDED', 'COMPLETED')
            """
        ).fetchone()
        return row[0]

    def acknowledged_source_position(self) -> int | None:
        row = self.connection.execute(
            """
            SELECT MAX(source_position) FROM combined_progress
            WHERE source_acknowledged = 1 AND decision = 'COMMIT'
            """
        ).fetchone()
        return row[0]

    def observation(self, checkpoint_id: str) -> tuple[str, str | None, bool]:
        row = self.connection.execute(
            """
            SELECT phase, decision, source_acknowledged
            FROM combined_progress WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        if row is None:
            raise KeyError(checkpoint_id)
        return row[0], row[1], bool(row[2])


@dataclass(frozen=True)
class CheckpointObservation:
    checkpoint_sequence: int
    phase: str


@dataclass(frozen=True)
class DecisionObservation:
    transaction_id: str
    decision: str


@dataclass(frozen=True)
class VisibleRecord:
    sink_id: str
    transaction_id: str
    record_id: str
    payload_digest: str
    occurrences: int = 1


@dataclass(frozen=True)
class OracleReport:
    recovery_checkpoint: int | None
    invariants: Mapping[str, bool]

    @property
    def passed(self) -> bool:
        return all(self.invariants.values())


class SafetyOracle:
    """Evaluate raw observations without calling the protocol implementation."""

    @staticmethod
    def evaluate(
        *,
        checkpoints: Sequence[CheckpointObservation],
        selected_recovery_checkpoint: int | None,
        decisions: Sequence[DecisionObservation],
        visible_records: Sequence[VisibleRecord],
        sink_tier: SinkTier,
    ) -> OracleReport:
        completed = {
            checkpoint.checkpoint_sequence
            for checkpoint in checkpoints
            if checkpoint.phase == CheckpointPhase.COMPLETED.value
        }
        expected_recovery = max(completed) if completed else None

        decisions_by_transaction: dict[str, set[str]] = {}
        for decision in decisions:
            decisions_by_transaction.setdefault(decision.transaction_id, set()).add(
                decision.decision
            )
        decision_is_immutable = all(
            len(values) == 1 for values in decisions_by_transaction.values()
        )

        visible_by_identity: dict[tuple[str, str], set[str]] = {}
        for record in visible_records:
            visible_by_identity.setdefault(
                (record.sink_id, record.record_id), set()
            ).add(record.payload_digest)
        stable_payloads = all(
            len(payloads) == 1 for payloads in visible_by_identity.values()
        )
        duplicate_policy_holds = sink_tier is SinkTier.BASIC or all(
            record.occurrences == 1 for record in visible_records
        )
        visible_has_decision = sink_tier is not SinkTier.TRANSACTIONAL or all(
            decisions_by_transaction.get(record.transaction_id) == {"COMMIT"}
            for record in visible_records
        )

        return OracleReport(
            recovery_checkpoint=expected_recovery,
            invariants={
                "latest_completed_checkpoint_selected": selected_recovery_checkpoint
                == expected_recovery,
                "decision_is_immutable": decision_is_immutable,
                "stable_record_payload": stable_payloads,
                "duplicate_policy_holds": duplicate_policy_holds,
                "transactional_visibility_requires_commit": visible_has_decision,
            },
        )
