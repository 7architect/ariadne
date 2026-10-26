"""DS-004S executable spike for managed-state classification and recovery."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class StateRecoveryError(RuntimeError):
    pass


class StateClass(str, Enum):
    EPHEMERAL = "EPHEMERAL"
    PARTITION_LOCAL_PER_TURN = "PARTITION_LOCAL_PER_TURN"
    CHECKPOINT_MANAGED_SHARED = "CHECKPOINT_MANAGED_SHARED"
    CLIENT_EXTERNAL_EFFECT = "CLIENT_EXTERNAL_EFFECT"
    RECOVERY_METADATA = "RECOVERY_METADATA"


@dataclass(frozen=True)
class StateRequirements:
    mutable: bool
    input_bindings: tuple[str, ...]
    partition_local: bool
    per_turn_serializable: bool
    shared_across_partitions: bool = False
    has_timers_or_watermarks: bool = False
    deterministic_replay: bool = True
    persisted_mailbox_order: bool = True


@dataclass(frozen=True)
class StateContract:
    state_class: StateClass
    input_bindings: tuple[str, ...]
    state_schema: str
    code_version: str
    atomic_authority: str
    deterministic_replay: bool
    persisted_mailbox_order: bool


def required_state_class(requirements: StateRequirements) -> StateClass:
    if not requirements.mutable:
        return StateClass.EPHEMERAL
    if (
        not requirements.input_bindings
        or any(
            not isinstance(input_id, str) or not input_id
            for input_id in requirements.input_bindings
        )
        or len(set(requirements.input_bindings))
        != len(requirements.input_bindings)
    ):
        raise StateRecoveryError(
            "managed state requires nonempty unique input bindings"
        )
    if not requirements.deterministic_replay:
        raise StateRecoveryError("managed state requires deterministic replay")
    if (
        len(requirements.input_bindings) == 1
        and requirements.partition_local
        and requirements.per_turn_serializable
        and not requirements.shared_across_partitions
        and not requirements.has_timers_or_watermarks
    ):
        return StateClass.PARTITION_LOCAL_PER_TURN
    if len(requirements.input_bindings) > 1 and not requirements.persisted_mailbox_order:
        raise StateRecoveryError("multi-input state requires persisted mailbox order")
    return StateClass.CHECKPOINT_MANAGED_SHARED


def validate_contract(
    contract: StateContract, requirements: StateRequirements
) -> None:
    required = required_state_class(requirements)
    if contract.state_class is not required:
        raise StateRecoveryError(
            f"state requires {required.value}, not {contract.state_class.value}"
        )
    if contract.input_bindings != requirements.input_bindings:
        raise StateRecoveryError("state contract input binding drift")
    if contract.deterministic_replay != requirements.deterministic_replay:
        raise StateRecoveryError("state contract deterministic replay drift")
    if contract.persisted_mailbox_order != requirements.persisted_mailbox_order:
        raise StateRecoveryError("state contract mailbox-order drift")
    if not contract.deterministic_replay:
        raise StateRecoveryError("managed state requires deterministic replay")
    if not contract.state_schema or not contract.code_version:
        raise StateRecoveryError("state schema and code version are required")
    if contract.state_class is StateClass.PARTITION_LOCAL_PER_TURN:
        if contract.atomic_authority != "turn_completion":
            raise StateRecoveryError("local state must share turn atomic authority")
    elif contract.state_class is StateClass.CHECKPOINT_MANAGED_SHARED:
        if contract.atomic_authority != "checkpoint":
            raise StateRecoveryError("shared state must use checkpoint authority")
        if not contract.deterministic_replay:
            raise StateRecoveryError("shared state requires deterministic replay")
        if len(contract.input_bindings) > 1 and not contract.persisted_mailbox_order:
            raise StateRecoveryError("shared state requires persisted mailbox order")


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def apply_order_sensitive(state: str, input_id: str, sequence: int, value: str) -> str:
    return f"{state}|{input_id}:{sequence}:{value}"


class StateRecoveryStore:
    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS state_contract (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                contract_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS local_outputs (
                sequence INTEGER PRIMARY KEY,
                record_id TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS local_completions (
                sequence INTEGER PRIMARY KEY,
                state_json TEXT NOT NULL,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL,
                output_record_id TEXT NOT NULL UNIQUE,
                FOREIGN KEY (sequence) REFERENCES local_outputs(sequence)
            );
            CREATE TABLE IF NOT EXISTS local_head (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                completed_through INTEGER NOT NULL,
                state_json TEXT NOT NULL,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoint_intents (
                checkpoint INTEGER PRIMARY KEY,
                intent_digest TEXT NOT NULL UNIQUE,
                authorization_ordinal INTEGER NOT NULL UNIQUE,
                snapshot_state TEXT NOT NULL,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS authority_log (
                ordinal INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL CHECK (
                    event_type IN (
                        'CHECKPOINT_INTENT',
                        'CHECKPOINT_COMPLETION',
                        'RECOVERY_FENCE'
                    )
                ),
                checkpoint INTEGER,
                UNIQUE (event_type, checkpoint)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS one_recovery_fence_event
                ON authority_log(event_type)
                WHERE event_type = 'RECOVERY_FENCE';
            CREATE TABLE IF NOT EXISTS recovery_fence (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                authority_ordinal INTEGER NOT NULL UNIQUE,
                FOREIGN KEY (authority_ordinal) REFERENCES authority_log(ordinal)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_cuts (
                checkpoint INTEGER NOT NULL,
                input_id TEXT NOT NULL,
                cut_sequence INTEGER NOT NULL,
                PRIMARY KEY (checkpoint, input_id),
                FOREIGN KEY (checkpoint) REFERENCES checkpoint_intents(checkpoint)
            );
            CREATE TABLE IF NOT EXISTS barrier_witnesses (
                checkpoint INTEGER NOT NULL,
                input_id TEXT NOT NULL,
                cut_sequence INTEGER NOT NULL,
                ordered_log_sequence INTEGER NOT NULL,
                intent_digest TEXT NOT NULL,
                PRIMARY KEY (checkpoint, input_id)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_seals (
                checkpoint INTEGER PRIMARY KEY,
                intent_digest TEXT NOT NULL UNIQUE,
                authorization_ordinal INTEGER NOT NULL UNIQUE,
                FOREIGN KEY (checkpoint) REFERENCES checkpoint_intents(checkpoint),
                FOREIGN KEY (authorization_ordinal) REFERENCES authority_log(ordinal)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_completions (
                checkpoint INTEGER PRIMARY KEY,
                intent_digest TEXT NOT NULL UNIQUE,
                completion_ordinal INTEGER NOT NULL UNIQUE,
                FOREIGN KEY (checkpoint) REFERENCES checkpoint_intents(checkpoint),
                FOREIGN KEY (completion_ordinal) REFERENCES authority_log(ordinal)
            );
            CREATE TABLE IF NOT EXISTS recovery_targets (
                input_id TEXT PRIMARY KEY,
                completed_through INTEGER NOT NULL,
                retained_from INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mailbox (
                apply_ordinal INTEGER PRIMARY KEY,
                input_id TEXT NOT NULL,
                input_sequence INTEGER NOT NULL,
                value TEXT NOT NULL,
                UNIQUE (input_id, input_sequence)
            );
            CREATE TABLE IF NOT EXISTS failure_free_reference (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                state_value TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS authority_log_no_update
            BEFORE UPDATE ON authority_log BEGIN
                SELECT RAISE(ABORT, 'authority log is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS authority_log_no_delete
            BEFORE DELETE ON authority_log BEGIN
                SELECT RAISE(ABORT, 'authority log is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS authority_log_fence_create_once
            BEFORE INSERT ON authority_log
            WHEN NEW.event_type = 'RECOVERY_FENCE' AND EXISTS (
                SELECT 1 FROM authority_log
                WHERE event_type = 'RECOVERY_FENCE'
            ) BEGIN
                SELECT RAISE(ABORT, 'recovery fence event already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS authority_log_identity_create_once
            BEFORE INSERT ON authority_log
            WHEN NEW.event_type != 'RECOVERY_FENCE' AND EXISTS (
                SELECT 1 FROM authority_log
                WHERE event_type = NEW.event_type
                  AND checkpoint = NEW.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'authority event already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS authority_log_ordinal_create_once
            BEFORE INSERT ON authority_log
            WHEN NEW.ordinal IS NOT NULL AND EXISTS (
                SELECT 1 FROM authority_log WHERE ordinal = NEW.ordinal
            ) BEGIN
                SELECT RAISE(ABORT, 'authority ordinal already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS authority_log_monotonic_insert
            BEFORE INSERT ON authority_log
            WHEN NEW.ordinal != (
                SELECT COALESCE(MAX(ordinal), 0) + 1 FROM authority_log
            ) BEGIN
                SELECT RAISE(ABORT, 'authority ordinal must be next');
            END;
            CREATE TRIGGER IF NOT EXISTS completion_event_requires_seal
            BEFORE INSERT ON authority_log
            WHEN NEW.event_type = 'CHECKPOINT_COMPLETION' AND NOT EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = NEW.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion requires seal');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_fence_create_once
            BEFORE INSERT ON recovery_fence
            WHEN EXISTS (SELECT 1 FROM recovery_fence) BEGIN
                SELECT RAISE(ABORT, 'recovery fence already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_fence_no_update
            BEFORE UPDATE ON recovery_fence BEGIN
                SELECT RAISE(ABORT, 'recovery fence is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_fence_no_delete
            BEFORE DELETE ON recovery_fence BEGIN
                SELECT RAISE(ABORT, 'recovery fence is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_intent_create_once
            BEFORE INSERT ON checkpoint_intents
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_intents
                WHERE checkpoint = NEW.checkpoint
                   OR intent_digest = NEW.intent_digest
                   OR authorization_ordinal = NEW.authorization_ordinal
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint intent identity already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_intent_no_update
            BEFORE UPDATE ON checkpoint_intents
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = OLD.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed checkpoint intent is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_intent_no_delete
            BEFORE DELETE ON checkpoint_intents
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = OLD.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed checkpoint intent is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_cut_no_insert
            BEFORE INSERT ON checkpoint_cuts
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = NEW.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed checkpoint cuts are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_cut_no_update
            BEFORE UPDATE ON checkpoint_cuts
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = OLD.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed checkpoint cuts are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_cut_no_delete
            BEFORE DELETE ON checkpoint_cuts
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = OLD.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed checkpoint cuts are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_witness_no_insert
            BEFORE INSERT ON barrier_witnesses
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = NEW.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed barrier witnesses are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_witness_no_update
            BEFORE UPDATE ON barrier_witnesses
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = OLD.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed barrier witnesses are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sealed_witness_no_delete
            BEFORE DELETE ON barrier_witnesses
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = OLD.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'sealed barrier witnesses are immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_seal_no_update
            BEFORE UPDATE ON checkpoint_seals BEGIN
                SELECT RAISE(ABORT, 'checkpoint seal is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_seal_no_delete
            BEFORE DELETE ON checkpoint_seals BEGIN
                SELECT RAISE(ABORT, 'checkpoint seal is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_seal_create_once
            BEFORE INSERT ON checkpoint_seals
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_seals
                WHERE checkpoint = NEW.checkpoint
                   OR intent_digest = NEW.intent_digest
                   OR authorization_ordinal = NEW.authorization_ordinal
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint seal identity already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_seal_before_recovery
            BEFORE INSERT ON checkpoint_seals
            WHEN EXISTS (SELECT 1 FROM recovery_fence) BEGIN
                SELECT RAISE(ABORT, 'checkpoint seal after recovery fence');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_seal_matches_intent
            BEFORE INSERT ON checkpoint_seals
            WHEN NOT EXISTS (
                SELECT 1 FROM checkpoint_intents
                WHERE checkpoint = NEW.checkpoint
                  AND intent_digest = NEW.intent_digest
                  AND authorization_ordinal = NEW.authorization_ordinal
            ) OR EXISTS (
                SELECT 1 FROM barrier_witnesses
                WHERE checkpoint = NEW.checkpoint
                  AND intent_digest != NEW.intent_digest
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint seal does not match intent');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_completion_create_once
            BEFORE INSERT ON checkpoint_completions
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_completions
                WHERE checkpoint = NEW.checkpoint
                   OR intent_digest = NEW.intent_digest
                   OR completion_ordinal = NEW.completion_ordinal
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion identity already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_completion_valid_insert
            BEFORE INSERT ON checkpoint_completions
            WHEN NOT EXISTS (
                SELECT 1
                FROM checkpoint_seals AS seal
                JOIN authority_log AS event
                  ON event.ordinal = NEW.completion_ordinal
                WHERE seal.checkpoint = NEW.checkpoint
                  AND seal.intent_digest = NEW.intent_digest
                  AND event.event_type = 'CHECKPOINT_COMPLETION'
                  AND event.checkpoint = NEW.checkpoint
                  AND seal.authorization_ordinal < NEW.completion_ordinal
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion lifecycle invalid');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_completion_before_recovery
            BEFORE INSERT ON checkpoint_completions
            WHEN EXISTS (SELECT 1 FROM recovery_fence) BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion after recovery fence');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_completion_no_update
            BEFORE UPDATE ON checkpoint_completions BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_completion_no_delete
            BEFORE DELETE ON checkpoint_completions BEGIN
                SELECT RAISE(ABORT, 'checkpoint completion is immutable');
            END;
            """
        )

    def close(self) -> None:
        self.connection.close()

    def set_contract(
        self, contract: StateContract, requirements: StateRequirements
    ) -> None:
        validate_contract(contract, requirements)
        value = asdict(contract)
        value["state_class"] = contract.state_class.value
        value["input_bindings"] = list(contract.input_bindings)
        contract_json = canonical(value)
        with self.connection:
            existing = self.connection.execute(
                "SELECT contract_json FROM state_contract WHERE singleton = 1"
            ).fetchone()
            if existing is not None and existing["contract_json"] != contract_json:
                raise StateRecoveryError("state contract identity drift")
            if existing is None:
                self.connection.execute(
                    "INSERT INTO state_contract VALUES (1, ?)",
                    (contract_json,),
                )
            if contract.state_class is StateClass.PARTITION_LOCAL_PER_TURN:
                self.connection.execute(
                    "INSERT OR IGNORE INTO local_head VALUES (1, 0, '{}', ?, ?)",
                    (contract.state_schema, contract.code_version),
                )

    def complete_local(
        self,
        sequence: int,
        state: dict[str, Any],
        output_record_id: str,
        state_schema: str,
        code_version: str,
    ) -> None:
        state_json = canonical(state)
        with self.connection:
            existing = self.connection.execute(
                "SELECT * FROM local_completions WHERE sequence = ?", (sequence,)
            ).fetchone()
            expected = (state_json, state_schema, code_version, output_record_id)
            if existing is not None:
                actual = tuple(
                    existing[name]
                    for name in (
                        "state_json",
                        "state_schema",
                        "code_version",
                        "output_record_id",
                    )
                )
                if actual != expected:
                    raise StateRecoveryError("local completion identity drift")
                return
            head = self.connection.execute("SELECT * FROM local_head").fetchone()
            if head is None or sequence != head["completed_through"] + 1:
                raise StateRecoveryError("local completion is not contiguous")
            self.connection.execute(
                "INSERT INTO local_outputs VALUES (?, ?)",
                (sequence, output_record_id),
            )
            self.connection.execute(
                "INSERT INTO local_completions VALUES (?, ?, ?, ?, ?)",
                (sequence, *expected),
            )
            self.connection.execute(
                """UPDATE local_head SET completed_through = ?, state_json = ?,
                   state_schema = ?, code_version = ? WHERE singleton = 1""",
                (sequence, state_json, state_schema, code_version),
            )

    def add_checkpoint(
        self,
        checkpoint: int,
        snapshot_state: str,
        cuts: dict[str, int],
        *,
        state_schema: str,
        code_version: str,
        completed: bool,
        witness_checkpoint_by_input: dict[str, int] | None = None,
    ) -> None:
        witnesses = witness_checkpoint_by_input or {
            input_id: checkpoint for input_id in cuts
        }
        with self.connection:
            authorization_ordinal = self._append_authority_event(
                "CHECKPOINT_INTENT", checkpoint
            )
            intent = {
                "checkpoint": checkpoint,
                "authorization_ordinal": authorization_ordinal,
                "inputs": sorted(cuts),
                "cuts": cuts,
                "snapshot_digest": hashlib.sha256(
                    snapshot_state.encode()
                ).hexdigest(),
                "state_schema": state_schema,
                "code_version": code_version,
            }
            intent_digest = digest(intent)
            self.connection.execute(
                "INSERT INTO checkpoint_intents VALUES (?, ?, ?, ?, ?, ?)",
                (
                    checkpoint,
                    intent_digest,
                    authorization_ordinal,
                    snapshot_state,
                    state_schema,
                    code_version,
                ),
            )
            for input_id, cut in cuts.items():
                self.connection.execute(
                    "INSERT INTO checkpoint_cuts VALUES (?, ?, ?)",
                    (checkpoint, input_id, cut),
                )
                witness_checkpoint = witnesses[input_id]
                self.connection.execute(
                    "INSERT INTO barrier_witnesses VALUES (?, ?, ?, ?, ?)",
                    (
                        checkpoint,
                        input_id,
                        cut,
                        cut + 1,
                        intent_digest
                        if witness_checkpoint == checkpoint
                        else f"checkpoint:{witness_checkpoint}",
                    ),
                )
            self.connection.execute(
                "INSERT INTO checkpoint_seals VALUES (?, ?, ?)",
                (checkpoint, intent_digest, authorization_ordinal),
            )
            if completed:
                self._complete_checkpoint(checkpoint, intent_digest)

    def _complete_checkpoint(self, checkpoint: int, intent_digest: str) -> None:
        completion_ordinal = self._append_authority_event(
            "CHECKPOINT_COMPLETION", checkpoint
        )
        self.connection.execute(
            "INSERT INTO checkpoint_completions VALUES (?, ?, ?)",
            (checkpoint, intent_digest, completion_ordinal),
        )

    def _append_authority_event(
        self, event_type: str, checkpoint: int | None
    ) -> int:
        ordinal = int(
            self.connection.execute(
                "SELECT COALESCE(MAX(ordinal), 0) + 1 FROM authority_log"
            ).fetchone()[0]
        )
        self.connection.execute(
            "INSERT INTO authority_log(ordinal, event_type, checkpoint) "
            "VALUES (?, ?, ?)",
            (ordinal, event_type, checkpoint),
        )
        return ordinal

    def complete_checkpoint(self, checkpoint: int) -> None:
        with self.connection:
            intent = self.connection.execute(
                "SELECT intent_digest FROM checkpoint_intents WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            if intent is None:
                raise StateRecoveryError("checkpoint intent missing")
            existing = self.connection.execute(
                "SELECT intent_digest FROM checkpoint_completions "
                "WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            if existing is not None:
                if existing["intent_digest"] != intent["intent_digest"]:
                    raise StateRecoveryError("checkpoint completion identity drift")
                return
            self._complete_checkpoint(checkpoint, intent["intent_digest"])

    def seal_recovery_boundary(self) -> int:
        with self.connection:
            existing = self.connection.execute(
                "SELECT authority_ordinal FROM recovery_fence WHERE singleton = 1"
            ).fetchone()
            if existing is not None:
                return int(existing["authority_ordinal"])
            ordinal = self._append_authority_event("RECOVERY_FENCE", None)
            self.connection.execute(
                "INSERT INTO recovery_fence VALUES (1, ?)", (ordinal,)
            )
            return ordinal

    def set_target(self, input_id: str, completed: int, retained_from: int = 1) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO recovery_targets VALUES (?, ?, ?)",
                (input_id, completed, retained_from),
            )

    def add_mailbox_event(
        self, ordinal: int, input_id: str, sequence: int, value: str
    ) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT INTO mailbox VALUES (?, ?, ?, ?)",
                (ordinal, input_id, sequence, value),
            )

    def set_failure_free_reference(self, state: str) -> None:
        with self.connection:
            self.connection.execute(
                "INSERT OR REPLACE INTO failure_free_reference VALUES (1, ?)",
                (state,),
            )
