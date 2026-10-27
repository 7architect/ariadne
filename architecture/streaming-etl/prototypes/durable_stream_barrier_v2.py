"""DS-004a: one durable ordered log for DATA and BARRIER records."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator

FaultHook = Callable[[str], None] | None


def hit(fault: FaultHook, point: str) -> None:
    if fault is not None:
        fault(point)


class ComposedProtocolError(RuntimeError):
    pass


class EntryKind(str, Enum):
    DATA = "DATA"
    BARRIER = "BARRIER"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def apply_state(state: str, input_id: str, data_sequence: int, payload_ref: str) -> str:
    return f"{state}|{input_id}:{data_sequence}:{payload_ref}"


@dataclass(frozen=True)
class LogEntry:
    input_id: str
    log_sequence: int
    kind: EntryKind
    record_id: str
    data_sequence: int | None
    payload_ref: str | None
    checkpoint: int | None
    cut_data_sequence: int | None


class ComposedDurableStream:
    """SQLite executable model; not a production backend selection."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS coordinator_owner (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0)
            );
            CREATE TABLE IF NOT EXISTS partitions (
                input_id TEXT PRIMARY KEY,
                producer_owner TEXT NOT NULL,
                producer_epoch INTEGER NOT NULL CHECK (producer_epoch > 0),
                next_log_sequence INTEGER NOT NULL DEFAULT 1,
                next_data_sequence INTEGER NOT NULL DEFAULT 1,
                last_checkpoint INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS checkpoint_intents (
                checkpoint INTEGER PRIMARY KEY CHECK (checkpoint > 0),
                intent_digest TEXT NOT NULL UNIQUE,
                inputs_json TEXT NOT NULL,
                coordinator_epoch INTEGER NOT NULL,
                authority_ordinal INTEGER NOT NULL UNIQUE,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoint_intent_seals (
                checkpoint INTEGER PRIMARY KEY,
                intent_digest TEXT NOT NULL UNIQUE,
                FOREIGN KEY (checkpoint) REFERENCES checkpoint_intents(checkpoint)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_status (
                checkpoint INTEGER PRIMARY KEY,
                status TEXT NOT NULL CHECK (status IN ('STARTED', 'ALIGNED', 'COMPLETED')),
                FOREIGN KEY (checkpoint) REFERENCES checkpoint_intents(checkpoint)
            );
            CREATE TABLE IF NOT EXISTS authority_log (
                ordinal INTEGER PRIMARY KEY,
                event_type TEXT NOT NULL CHECK (
                    event_type IN ('CHECKPOINT_INTENT', 'RECOVERY_FENCE')
                ),
                checkpoint INTEGER
            );
            CREATE TABLE IF NOT EXISTS recovery_fence (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                authority_ordinal INTEGER NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS stream_entries (
                input_id TEXT NOT NULL,
                log_sequence INTEGER NOT NULL CHECK (log_sequence > 0),
                kind TEXT NOT NULL CHECK (kind IN ('DATA', 'BARRIER')),
                record_id TEXT NOT NULL,
                data_sequence INTEGER,
                payload_ref TEXT,
                checkpoint INTEGER,
                cut_data_sequence INTEGER,
                intent_digest TEXT,
                writer_epoch INTEGER NOT NULL,
                PRIMARY KEY (input_id, log_sequence),
                UNIQUE (input_id, record_id),
                UNIQUE (input_id, data_sequence),
                CHECK (
                    (kind = 'DATA' AND data_sequence IS NOT NULL
                     AND payload_ref IS NOT NULL AND checkpoint IS NULL
                     AND cut_data_sequence IS NULL AND intent_digest IS NULL)
                    OR
                    (kind = 'BARRIER' AND data_sequence IS NULL
                     AND payload_ref IS NULL AND checkpoint IS NOT NULL
                     AND cut_data_sequence IS NOT NULL AND intent_digest IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS operator_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                state_value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS operator_manifest (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                inputs_json TEXT NOT NULL,
                initial_state TEXT NOT NULL,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS operator_progress (
                input_id TEXT PRIMARY KEY,
                scanned_log_through INTEGER NOT NULL DEFAULT 0,
                completed_data_through INTEGER NOT NULL DEFAULT 0,
                blocked_checkpoint INTEGER
            );
            CREATE TABLE IF NOT EXISTS turns (
                apply_ordinal INTEGER PRIMARY KEY,
                input_id TEXT NOT NULL,
                data_sequence INTEGER NOT NULL,
                log_sequence INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                payload_ref TEXT NOT NULL,
                state_after TEXT NOT NULL,
                output_id TEXT NOT NULL UNIQUE,
                recovery_generation INTEGER NOT NULL DEFAULT 0
                    CHECK (recovery_generation >= 0),
                UNIQUE (input_id, data_sequence),
                FOREIGN KEY (input_id, log_sequence)
                    REFERENCES stream_entries(input_id, log_sequence)
            );
            CREATE TABLE IF NOT EXISTS barrier_observations (
                checkpoint INTEGER NOT NULL,
                input_id TEXT NOT NULL,
                log_sequence INTEGER NOT NULL,
                cut_data_sequence INTEGER NOT NULL,
                intent_digest TEXT NOT NULL,
                PRIMARY KEY (checkpoint, input_id),
                FOREIGN KEY (input_id, log_sequence)
                    REFERENCES stream_entries(input_id, log_sequence)
            );
            CREATE TABLE IF NOT EXISTS snapshots (
                checkpoint INTEGER PRIMARY KEY,
                state_value TEXT NOT NULL,
                mailbox_highwater INTEGER NOT NULL,
                cuts_json TEXT NOT NULL,
                intent_digest TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS output_events (
                output_ordinal INTEGER PRIMARY KEY,
                kind TEXT NOT NULL CHECK (kind IN ('DATA', 'BARRIER')),
                output_id TEXT NOT NULL UNIQUE,
                input_id TEXT,
                data_sequence INTEGER,
                checkpoint INTEGER,
                CHECK (
                    (kind = 'DATA' AND input_id IS NOT NULL
                     AND data_sequence IS NOT NULL AND checkpoint IS NULL)
                    OR
                    (kind = 'BARRIER' AND input_id IS NULL
                     AND data_sequence IS NULL AND checkpoint IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS recovery_runs (
                generation INTEGER PRIMARY KEY CHECK (generation > 0),
                checkpoint INTEGER NOT NULL,
                phase TEXT NOT NULL CHECK (phase IN ('RESTORING', 'READY')),
                restored_state TEXT NOT NULL,
                recovered_state TEXT,
                target_cursors_json TEXT NOT NULL,
                replayed_through_ordinal INTEGER,
                fence_ordinal INTEGER NOT NULL UNIQUE,
                FOREIGN KEY (checkpoint) REFERENCES snapshots(checkpoint)
            );

            CREATE TRIGGER IF NOT EXISTS entry_no_update
            BEFORE UPDATE ON stream_entries BEGIN
                SELECT RAISE(ABORT, 'stream entry is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS entry_no_delete
            BEFORE DELETE ON stream_entries BEGIN
                SELECT RAISE(ABORT, 'stream entry requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS entry_create_once
            BEFORE INSERT ON stream_entries
            WHEN EXISTS (
                SELECT 1 FROM stream_entries
                WHERE (input_id = NEW.input_id AND log_sequence = NEW.log_sequence)
                   OR (input_id = NEW.input_id AND record_id = NEW.record_id)
            ) BEGIN
                SELECT RAISE(ABORT, 'stream entry identity already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS entry_contiguous_log
            BEFORE INSERT ON stream_entries
            WHEN NEW.log_sequence != (
                SELECT COALESCE(MAX(log_sequence), 0) + 1
                FROM stream_entries WHERE input_id = NEW.input_id
            ) BEGIN
                SELECT RAISE(ABORT, 'stream log sequence gap');
            END;
            CREATE TRIGGER IF NOT EXISTS entry_contiguous_data
            BEFORE INSERT ON stream_entries
            WHEN NEW.kind = 'DATA' AND NEW.data_sequence != (
                SELECT COALESCE(MAX(data_sequence), 0) + 1
                FROM stream_entries
                WHERE input_id = NEW.input_id AND kind = 'DATA'
            ) BEGIN
                SELECT RAISE(ABORT, 'stream data sequence gap');
            END;
            CREATE TRIGGER IF NOT EXISTS barrier_admission
            BEFORE INSERT ON stream_entries
            WHEN NEW.kind = 'BARRIER' BEGIN
                SELECT CASE WHEN EXISTS (SELECT 1 FROM recovery_fence)
                    THEN RAISE(ABORT, 'barrier after recovery fence') END;
                SELECT CASE WHEN NOT EXISTS (
                    SELECT 1
                    FROM checkpoint_intents i
                    JOIN checkpoint_intent_seals s
                      ON s.checkpoint = i.checkpoint
                     AND s.intent_digest = i.intent_digest
                    JOIN checkpoint_status st
                      ON st.checkpoint = i.checkpoint AND st.status = 'STARTED'
                    JOIN coordinator_owner o ON o.singleton = 1
                    WHERE i.checkpoint = NEW.checkpoint
                      AND i.intent_digest = NEW.intent_digest
                      AND i.coordinator_epoch = NEW.writer_epoch
                      AND o.owner_epoch = NEW.writer_epoch
                ) THEN RAISE(ABORT, 'barrier lacks sealed coordinator intent') END;
                SELECT CASE WHEN NEW.cut_data_sequence != (
                    SELECT COALESCE(MAX(data_sequence), 0)
                    FROM stream_entries
                    WHERE input_id = NEW.input_id AND kind = 'DATA'
                ) THEN RAISE(ABORT, 'barrier cut is not ordered with data') END;
                SELECT CASE WHEN NEW.checkpoint <= COALESCE((
                    SELECT MAX(checkpoint) FROM stream_entries
                    WHERE input_id = NEW.input_id AND kind = 'BARRIER'
                ), 0) THEN RAISE(ABORT, 'checkpoint decreases on partition') END;
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_intent_update
            BEFORE UPDATE ON checkpoint_intents BEGIN
                SELECT RAISE(ABORT, 'checkpoint intent is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_intent_delete
            BEFORE DELETE ON checkpoint_intents BEGIN
                SELECT RAISE(ABORT, 'checkpoint intent is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_snapshot
            BEFORE UPDATE ON snapshots BEGIN
                SELECT RAISE(ABORT, 'snapshot is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS snapshot_before_recovery
            BEFORE INSERT ON snapshots
            WHEN EXISTS (SELECT 1 FROM recovery_fence) BEGIN
                SELECT RAISE(ABORT, 'snapshot after recovery fence');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_snapshot_delete
            BEFORE DELETE ON snapshots BEGIN
                SELECT RAISE(ABORT, 'snapshot is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_turn
            BEFORE UPDATE ON turns BEGIN
                SELECT RAISE(ABORT, 'turn is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_turn_delete
            BEFORE DELETE ON turns BEGIN
                SELECT RAISE(ABORT, 'turn is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_output
            BEFORE UPDATE ON output_events BEGIN
                SELECT RAISE(ABORT, 'output event is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_output_delete
            BEFORE DELETE ON output_events BEGIN
                SELECT RAISE(ABORT, 'output event is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_observation
            BEFORE UPDATE ON barrier_observations BEGIN
                SELECT RAISE(ABORT, 'barrier observation is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_observation_delete
            BEFORE DELETE ON barrier_observations BEGIN
                SELECT RAISE(ABORT, 'barrier observation is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_intent_seal
            BEFORE UPDATE ON checkpoint_intent_seals BEGIN
                SELECT RAISE(ABORT, 'checkpoint intent seal is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_intent_seal_delete
            BEFORE DELETE ON checkpoint_intent_seals BEGIN
                SELECT RAISE(ABORT, 'checkpoint intent seal is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_authority_update
            BEFORE UPDATE ON authority_log BEGIN
                SELECT RAISE(ABORT, 'authority log is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS immutable_authority_delete
            BEFORE DELETE ON authority_log BEGIN
                SELECT RAISE(ABORT, 'authority log is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS authority_contiguous
            BEFORE INSERT ON authority_log
            WHEN NEW.ordinal != (
                SELECT COALESCE(MAX(ordinal), 0) + 1 FROM authority_log
            ) BEGIN
                SELECT RAISE(ABORT, 'authority ordinal must be next');
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
            CREATE TRIGGER IF NOT EXISTS checkpoint_status_forward
            BEFORE UPDATE ON checkpoint_status
            WHEN NOT (
                (OLD.status = 'STARTED' AND NEW.status = 'ALIGNED')
                OR (OLD.status = 'ALIGNED' AND NEW.status = 'COMPLETED')
                OR OLD.status = NEW.status
            ) BEGIN
                SELECT RAISE(ABORT, 'illegal checkpoint status transition');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_status_create_once
            BEFORE INSERT ON checkpoint_status
            WHEN EXISTS (
                SELECT 1 FROM checkpoint_status
                WHERE checkpoint = NEW.checkpoint
            ) BEGIN
                SELECT RAISE(ABORT, 'checkpoint status already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_status_before_recovery
            BEFORE UPDATE ON checkpoint_status
            WHEN OLD.status != NEW.status AND EXISTS (SELECT 1 FROM recovery_fence)
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint transition after recovery fence');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_run_create_once
            BEFORE INSERT ON recovery_runs
            WHEN EXISTS (SELECT 1 FROM recovery_runs)
            BEGIN
                SELECT RAISE(ABORT, 'recovery run already exists for singleton fence');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_run_admission
            BEFORE INSERT ON recovery_runs
            WHEN NEW.phase != 'RESTORING'
              OR NEW.recovered_state IS NOT NULL
              OR NEW.replayed_through_ordinal IS NOT NULL
              OR NEW.generation != 1
              OR NEW.fence_ordinal != (
                  SELECT authority_ordinal FROM recovery_fence WHERE singleton = 1
              )
              OR NEW.target_cursors_json != (
                  SELECT json_group_object(input_id, completed_data_through)
                  FROM (
                      SELECT input_id, completed_data_through
                      FROM operator_progress ORDER BY input_id
                  )
              )
              OR NEW.checkpoint != COALESCE((
                  SELECT s.checkpoint
                  FROM snapshots s
                  JOIN checkpoint_status st ON st.checkpoint = s.checkpoint
                   AND st.status = 'COMPLETED'
                  JOIN checkpoint_intents i ON i.checkpoint = s.checkpoint
                  JOIN checkpoint_intent_seals seal ON seal.checkpoint = i.checkpoint
                   AND seal.intent_digest = i.intent_digest
                  JOIN operator_manifest m ON m.singleton = 1
                  WHERE s.intent_digest = i.intent_digest
                    AND i.inputs_json = m.inputs_json
                    AND i.state_schema = m.state_schema
                    AND i.code_version = m.code_version
                  ORDER BY s.checkpoint DESC LIMIT 1
              ), -1)
              OR NEW.restored_state IS NOT (
                  SELECT s.state_value
                  FROM snapshots s
                  JOIN checkpoint_status st ON st.checkpoint = s.checkpoint
                   AND st.status = 'COMPLETED'
                  JOIN checkpoint_intents i ON i.checkpoint = s.checkpoint
                  JOIN checkpoint_intent_seals seal ON seal.checkpoint = i.checkpoint
                   AND seal.intent_digest = i.intent_digest
                  JOIN operator_manifest m ON m.singleton = 1
                  WHERE s.intent_digest = i.intent_digest
                    AND i.inputs_json = m.inputs_json
                    AND i.state_schema = m.state_schema
                    AND i.code_version = m.code_version
                  ORDER BY s.checkpoint DESC LIMIT 1
              )
            BEGIN
                SELECT RAISE(ABORT, 'invalid recovery run admission');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_run_transition
            BEFORE UPDATE ON recovery_runs
            WHEN NOT (
                OLD.phase = 'RESTORING' AND NEW.phase = 'READY'
                AND OLD.generation = NEW.generation
                AND OLD.checkpoint = NEW.checkpoint
                AND OLD.restored_state = NEW.restored_state
                AND OLD.target_cursors_json = NEW.target_cursors_json
                AND OLD.fence_ordinal = NEW.fence_ordinal
                AND NEW.recovered_state IS NOT NULL
                AND NEW.replayed_through_ordinal IS NOT NULL
                AND NEW.recovered_state = (
                    SELECT state_value FROM operator_state WHERE singleton = 1
                )
                AND NEW.replayed_through_ordinal = (
                    SELECT COALESCE(MAX(apply_ordinal), 0) FROM turns
                )
            )
            BEGIN
                SELECT RAISE(ABORT, 'invalid recovery run transition');
            END;
            CREATE TRIGGER IF NOT EXISTS recovery_run_no_delete
            BEFORE DELETE ON recovery_runs BEGIN
                SELECT RAISE(ABORT, 'recovery run is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS turn_recovery_generation
            BEFORE INSERT ON turns
            WHEN NEW.recovery_generation != COALESCE((
                SELECT MAX(generation) FROM recovery_runs WHERE phase = 'READY'
            ), 0)
            BEGIN
                SELECT RAISE(ABORT, 'turn recovery generation mismatch');
            END;
            """
        )

    @contextmanager
    def _write(self) -> Iterator[None]:
        if self.connection.in_transaction:
            yield
            return
        self.connection.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        self.connection.close()

    def claim_coordinator(self, owner_id: str, owner_epoch: int) -> None:
        with self._write():
            row = self.connection.execute("SELECT * FROM coordinator_owner").fetchone()
            if row is None:
                if owner_epoch != 1:
                    raise ComposedProtocolError("first coordinator epoch must be one")
                self.connection.execute(
                    "INSERT INTO coordinator_owner VALUES (1, ?, ?)",
                    (owner_id, owner_epoch),
                )
            elif (row["owner_id"], row["owner_epoch"]) != (owner_id, owner_epoch):
                if owner_epoch <= row["owner_epoch"]:
                    raise ComposedProtocolError("stale coordinator epoch")
                self.connection.execute(
                    "UPDATE coordinator_owner SET owner_id = ?, owner_epoch = ?",
                    (owner_id, owner_epoch),
                )

    def create_partition(
        self, input_id: str, producer_owner: str, producer_epoch: int = 1
    ) -> None:
        if not input_id or producer_epoch < 1:
            raise ComposedProtocolError("invalid partition")
        with self._write():
            row = self.connection.execute(
                "SELECT producer_owner, producer_epoch FROM partitions WHERE input_id = ?",
                (input_id,),
            ).fetchone()
            if row is None:
                self.connection.execute(
                    "INSERT INTO partitions(input_id, producer_owner, producer_epoch) "
                    "VALUES (?, ?, ?)",
                    (input_id, producer_owner, producer_epoch),
                )
            elif tuple(row) != (producer_owner, producer_epoch):
                raise ComposedProtocolError("partition identity drift")

    def create_operator(
        self,
        inputs: tuple[str, ...],
        initial_state: str = "S",
        *,
        state_schema: str = "state/v1",
        code_version: str = "code-v1",
    ) -> None:
        if not inputs or len(set(inputs)) != len(inputs):
            raise ComposedProtocolError("operator inputs must be nonempty and unique")
        with self._write():
            known = {
                row["input_id"]
                for row in self.connection.execute("SELECT input_id FROM partitions")
            }
            if set(inputs) != known:
                raise ComposedProtocolError("operator input manifest drift")
            self.connection.execute(
                "INSERT OR IGNORE INTO operator_state VALUES (1, ?)",
                (initial_state,),
            )
            manifest = self.connection.execute(
                "SELECT inputs_json, initial_state, state_schema, code_version "
                "FROM operator_manifest"
            ).fetchone()
            expected_manifest = (
                canonical(sorted(inputs)),
                initial_state,
                state_schema,
                code_version,
            )
            if manifest is None:
                self.connection.execute(
                    "INSERT INTO operator_manifest VALUES (1, ?, ?, ?, ?)",
                    expected_manifest,
                )
            elif tuple(manifest) != expected_manifest:
                raise ComposedProtocolError("operator manifest drift")
            for input_id in inputs:
                self.connection.execute(
                    "INSERT OR IGNORE INTO operator_progress(input_id) VALUES (?)",
                    (input_id,),
                )

    def append_data(
        self,
        input_id: str,
        producer_owner: str,
        producer_epoch: int,
        record_id: str,
        payload_ref: str,
    ) -> LogEntry:
        with self._write():
            partition = self._partition(input_id)
            if (partition["producer_owner"], partition["producer_epoch"]) != (
                producer_owner,
                producer_epoch,
            ):
                raise ComposedProtocolError("stale producer")
            existing = self.connection.execute(
                "SELECT * FROM stream_entries WHERE input_id = ? AND record_id = ?",
                (input_id, record_id),
            ).fetchone()
            if existing is not None:
                if existing["kind"] != "DATA" or existing["payload_ref"] != payload_ref:
                    raise ComposedProtocolError("record identity drift")
                return self._entry(existing)
            log_sequence = partition["next_log_sequence"]
            data_sequence = partition["next_data_sequence"]
            self.connection.execute(
                "INSERT INTO stream_entries VALUES (?, ?, 'DATA', ?, ?, ?, NULL, NULL, NULL, ?)",
                (
                    input_id,
                    log_sequence,
                    record_id,
                    data_sequence,
                    payload_ref,
                    producer_epoch,
                ),
            )
            self.connection.execute(
                "UPDATE partitions SET next_log_sequence = ?, next_data_sequence = ? "
                "WHERE input_id = ?",
                (log_sequence + 1, data_sequence + 1, input_id),
            )
            return self._entry(
                self.connection.execute(
                    "SELECT * FROM stream_entries WHERE input_id = ? AND log_sequence = ?",
                    (input_id, log_sequence),
                ).fetchone()
            )

    def start_checkpoint(
        self,
        checkpoint: int,
        inputs: tuple[str, ...],
        coordinator_owner: str,
        coordinator_epoch: int,
        *,
        state_schema: str = "state/v1",
        code_version: str = "code-v1",
    ) -> str:
        with self._write():
            owner = self._coordinator(coordinator_owner, coordinator_epoch)
            if self.connection.execute("SELECT 1 FROM recovery_fence").fetchone():
                raise ComposedProtocolError("checkpoint after recovery fence")
            active = self.connection.execute(
                "SELECT checkpoint FROM checkpoint_status WHERE status != 'COMPLETED'"
            ).fetchone()
            if active is not None:
                raise ComposedProtocolError("another checkpoint is active")
            expected_inputs = tuple(
                row["input_id"]
                for row in self.connection.execute(
                    "SELECT input_id FROM operator_progress ORDER BY input_id"
                )
            )
            if tuple(sorted(inputs)) != expected_inputs:
                raise ComposedProtocolError("checkpoint input manifest drift")
            previous = self.connection.execute(
                "SELECT COALESCE(MAX(checkpoint), 0) FROM checkpoint_intents"
            ).fetchone()[0]
            if checkpoint != previous + 1:
                raise ComposedProtocolError("checkpoint must be next")
            authority_ordinal = self._append_authority("CHECKPOINT_INTENT", checkpoint)
            intent = {
                "checkpoint": checkpoint,
                "inputs": list(expected_inputs),
                "coordinator_epoch": owner["owner_epoch"],
                "authority_ordinal": authority_ordinal,
                "state_schema": state_schema,
                "code_version": code_version,
            }
            intent_digest = digest(intent)
            self.connection.execute(
                "INSERT INTO checkpoint_intents VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    checkpoint,
                    intent_digest,
                    canonical(list(expected_inputs)),
                    coordinator_epoch,
                    authority_ordinal,
                    state_schema,
                    code_version,
                ),
            )
            self.connection.execute(
                "INSERT INTO checkpoint_intent_seals VALUES (?, ?)",
                (checkpoint, intent_digest),
            )
            self.connection.execute(
                "INSERT INTO checkpoint_status VALUES (?, 'STARTED')", (checkpoint,)
            )
            return intent_digest

    def inject_barrier(
        self,
        checkpoint: int,
        input_id: str,
        coordinator_owner: str,
        coordinator_epoch: int,
        intent_digest: str,
        *,
        fault: FaultHook = None,
    ) -> LogEntry:
        with self._write():
            self._coordinator(coordinator_owner, coordinator_epoch)
            partition = self._partition(input_id)
            existing = self.connection.execute(
                "SELECT * FROM stream_entries WHERE input_id = ? AND checkpoint = ?",
                (input_id, checkpoint),
            ).fetchone()
            if existing is not None:
                if existing["intent_digest"] != intent_digest:
                    raise ComposedProtocolError("barrier identity drift")
                return self._entry(existing)
            hit(fault, "barrier:before_insert")
            log_sequence = partition["next_log_sequence"]
            cut = partition["next_data_sequence"] - 1
            self.connection.execute(
                "INSERT INTO stream_entries VALUES (?, ?, 'BARRIER', ?, NULL, NULL, ?, ?, ?, ?)",
                (
                    input_id,
                    log_sequence,
                    f"barrier:{checkpoint}:{input_id}",
                    checkpoint,
                    cut,
                    intent_digest,
                    coordinator_epoch,
                ),
            )
            hit(fault, "barrier:after_entry_before_partition")
            self.connection.execute(
                "UPDATE partitions SET next_log_sequence = ?, last_checkpoint = ? "
                "WHERE input_id = ?",
                (log_sequence + 1, checkpoint, input_id),
            )
            hit(fault, "barrier:after_partition_before_commit")
            return self._entry(
                self.connection.execute(
                    "SELECT * FROM stream_entries WHERE input_id = ? AND log_sequence = ?",
                    (input_id, log_sequence),
                ).fetchone()
            )

    def process_next(
        self, input_id: str, *, fault: FaultHook = None
    ) -> LogEntry | None:
        with self._write():
            if self.connection.execute("SELECT 1 FROM recovery_fence").fetchone():
                recovered = self.connection.execute(
                    "SELECT 1 FROM recovery_runs WHERE phase = 'READY'"
                ).fetchone()
                if recovered is None:
                    raise ComposedProtocolError("processing before recovery is ready")
            progress = self.connection.execute(
                "SELECT * FROM operator_progress WHERE input_id = ?", (input_id,)
            ).fetchone()
            if progress is None:
                raise ComposedProtocolError("unknown operator input")
            if progress["blocked_checkpoint"] is not None:
                return None
            row = self.connection.execute(
                "SELECT * FROM stream_entries WHERE input_id = ? AND log_sequence = ?",
                (input_id, progress["scanned_log_through"] + 1),
            ).fetchone()
            if row is None:
                return None
            entry = self._entry(row)
            if entry.kind is EntryKind.DATA:
                if entry.data_sequence != progress["completed_data_through"] + 1:
                    raise ComposedProtocolError("data completion is not contiguous")
                state = self.connection.execute(
                    "SELECT state_value FROM operator_state WHERE singleton = 1"
                ).fetchone()[0]
                new_state = apply_state(
                    state, input_id, entry.data_sequence, entry.payload_ref or ""
                )
                apply_ordinal = self._next("turns", "apply_ordinal")
                output_id = f"out:{input_id}:{entry.data_sequence}"
                self.connection.execute(
                    "INSERT INTO turns VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        apply_ordinal,
                        input_id,
                        entry.data_sequence,
                        entry.log_sequence,
                        entry.record_id,
                        entry.payload_ref,
                        new_state,
                        output_id,
                        self._active_recovery_generation(),
                    ),
                )
                self.connection.execute(
                    "INSERT INTO output_events VALUES (?, 'DATA', ?, ?, ?, NULL)",
                    (
                        self._next("output_events", "output_ordinal"),
                        output_id,
                        input_id,
                        entry.data_sequence,
                    ),
                )
                self.connection.execute(
                    "UPDATE operator_state SET state_value = ? WHERE singleton = 1",
                    (new_state,),
                )
                self.connection.execute(
                    "UPDATE operator_progress SET scanned_log_through = ?, "
                    "completed_data_through = ? WHERE input_id = ?",
                    (entry.log_sequence, entry.data_sequence, input_id),
                )
            else:
                intent = self.connection.execute(
                    "SELECT inputs_json FROM checkpoint_intents WHERE checkpoint = ?",
                    (entry.checkpoint,),
                ).fetchone()
                if intent is None:
                    raise ComposedProtocolError("barrier intent missing")
                active_blocks = {
                    row[0]
                    for row in self.connection.execute(
                        "SELECT DISTINCT blocked_checkpoint FROM operator_progress "
                        "WHERE blocked_checkpoint IS NOT NULL"
                    )
                }
                if active_blocks and active_blocks != {entry.checkpoint}:
                    raise ComposedProtocolError("mixed checkpoint alignment")
                if progress["completed_data_through"] != entry.cut_data_sequence:
                    raise ComposedProtocolError("barrier overtook data completion")
                hit(fault, "observe:before_observation")
                self.connection.execute(
                    "INSERT INTO barrier_observations VALUES (?, ?, ?, ?, ?)",
                    (
                        entry.checkpoint,
                        input_id,
                        entry.log_sequence,
                        entry.cut_data_sequence,
                        row["intent_digest"],
                    ),
                )
                hit(fault, "observe:after_observation_before_progress")
                self.connection.execute(
                    "UPDATE operator_progress SET scanned_log_through = ?, "
                    "blocked_checkpoint = ? WHERE input_id = ?",
                    (entry.log_sequence, entry.checkpoint, input_id),
                )
                hit(fault, "observe:after_progress_before_alignment")
                required = set(json.loads(intent["inputs_json"]))
                observed = {
                    row[0]
                    for row in self.connection.execute(
                        "SELECT input_id FROM barrier_observations WHERE checkpoint = ?",
                        (entry.checkpoint,),
                    )
                }
                if observed == required:
                    self._align(entry.checkpoint or 0, required, fault=fault)
            return entry

    def _align(
        self, checkpoint: int, inputs: set[str], *, fault: FaultHook = None
    ) -> None:
        cuts = {}
        barrier_logs = {}
        for input_id in sorted(inputs):
            observation = self.connection.execute(
                "SELECT * FROM barrier_observations WHERE checkpoint = ? AND input_id = ?",
                (checkpoint, input_id),
            ).fetchone()
            progress = self.connection.execute(
                "SELECT * FROM operator_progress WHERE input_id = ?", (input_id,)
            ).fetchone()
            if (
                observation is None
                or progress["blocked_checkpoint"] != checkpoint
                or progress["completed_data_through"]
                != observation["cut_data_sequence"]
            ):
                raise ComposedProtocolError("checkpoint vector is not aligned")
            cuts[input_id] = observation["cut_data_sequence"]
            barrier_logs[input_id] = observation["log_sequence"]
        intent = self.connection.execute(
            "SELECT intent_digest FROM checkpoint_intents WHERE checkpoint = ?",
            (checkpoint,),
        ).fetchone()
        state = self.connection.execute(
            "SELECT state_value FROM operator_state WHERE singleton = 1"
        ).fetchone()[0]
        highwater = self.connection.execute(
            "SELECT COALESCE(MAX(apply_ordinal), 0) FROM turns"
        ).fetchone()[0]
        snapshot_cuts = {
            input_id: {
                "data": cuts[input_id],
                "barrier_log": barrier_logs[input_id],
            }
            for input_id in sorted(inputs)
        }
        self.connection.execute(
            "INSERT INTO snapshots VALUES (?, ?, ?, ?, ?)",
            (checkpoint, state, highwater, canonical(snapshot_cuts), intent[0]),
        )
        hit(fault, "align:after_snapshot_before_output")
        self.connection.execute(
            "INSERT INTO output_events VALUES (?, 'BARRIER', ?, NULL, NULL, ?)",
            (
                self._next("output_events", "output_ordinal"),
                f"barrier-out:{checkpoint}",
                checkpoint,
            ),
        )
        hit(fault, "align:after_output_before_status")
        self.connection.execute(
            "UPDATE checkpoint_status SET status = 'ALIGNED' WHERE checkpoint = ?",
            (checkpoint,),
        )
        hit(fault, "align:after_status_before_unblock")
        self.connection.execute(
            "UPDATE operator_progress SET blocked_checkpoint = NULL "
            "WHERE blocked_checkpoint = ?",
            (checkpoint,),
        )
        hit(fault, "align:after_unblock_before_commit")

    def complete_checkpoint(self, checkpoint: int, *, fault: FaultHook = None) -> None:
        with self._write():
            if self.connection.execute("SELECT 1 FROM recovery_fence").fetchone():
                raise ComposedProtocolError("completion after recovery fence")
            hit(fault, "complete:before_update")
            cursor = self.connection.execute(
                "UPDATE checkpoint_status SET status = 'COMPLETED' "
                "WHERE checkpoint = ? AND status = 'ALIGNED'",
                (checkpoint,),
            )
            if cursor.rowcount != 1:
                row = self.connection.execute(
                    "SELECT status FROM checkpoint_status WHERE checkpoint = ?",
                    (checkpoint,),
                ).fetchone()
                if row is None or row[0] != "COMPLETED":
                    raise ComposedProtocolError("checkpoint is not aligned")
            hit(fault, "complete:after_update_before_commit")

    def begin_recovery(self, *, fault: FaultHook = None) -> int:
        with self._write():
            row = self.connection.execute(
                "SELECT authority_ordinal FROM recovery_fence"
            ).fetchone()
            if row is not None:
                return row[0]
            ordinal = self._append_authority("RECOVERY_FENCE", None)
            hit(fault, "fence:after_authority_before_fence")
            self.connection.execute(
                "INSERT INTO recovery_fence VALUES (1, ?)", (ordinal,)
            )
            hit(fault, "fence:after_insert_before_commit")
            return ordinal

    def recover(self, *, fault: FaultHook = None) -> dict[str, Any]:
        """Restore a completed snapshot and replay its exact durable turn tail."""
        with self._write():
            fence = self.connection.execute(
                "SELECT authority_ordinal FROM recovery_fence WHERE singleton = 1"
            ).fetchone()
            if fence is None:
                raise ComposedProtocolError("recovery fence is missing")
            existing = self.connection.execute(
                "SELECT * FROM recovery_runs ORDER BY generation DESC LIMIT 1"
            ).fetchone()
            if existing is not None:
                if existing["phase"] != "READY":
                    raise ComposedProtocolError("recovery is not ready")
                return self._recovery_report(existing)

            manifest = self.connection.execute(
                "SELECT * FROM operator_manifest WHERE singleton = 1"
            ).fetchone()
            if manifest is None:
                raise ComposedProtocolError("operator manifest is missing")
            snapshot = self.connection.execute(
                "SELECT s.*, i.inputs_json, i.state_schema, i.code_version, "
                "i.intent_digest AS authoritative_digest, i.coordinator_epoch, "
                "i.authority_ordinal "
                "FROM snapshots s "
                "JOIN checkpoint_status st ON st.checkpoint = s.checkpoint "
                "AND st.status = 'COMPLETED' "
                "JOIN checkpoint_intents i ON i.checkpoint = s.checkpoint "
                "JOIN checkpoint_intent_seals seal ON seal.checkpoint = i.checkpoint "
                "AND seal.intent_digest = i.intent_digest "
                "WHERE s.intent_digest = i.intent_digest "
                "AND i.inputs_json = ? AND i.state_schema = ? AND i.code_version = ? "
                "ORDER BY s.checkpoint DESC LIMIT 1",
                (
                    manifest["inputs_json"],
                    manifest["state_schema"],
                    manifest["code_version"],
                ),
            ).fetchone()
            if snapshot is None:
                raise ComposedProtocolError("no compatible completed checkpoint")

            expected_intent = {
                "checkpoint": snapshot["checkpoint"],
                "inputs": json.loads(snapshot["inputs_json"]),
                "coordinator_epoch": snapshot["coordinator_epoch"],
                "authority_ordinal": snapshot["authority_ordinal"],
                "state_schema": snapshot["state_schema"],
                "code_version": snapshot["code_version"],
            }
            if digest(expected_intent) != snapshot["authoritative_digest"]:
                raise ComposedProtocolError("checkpoint intent digest mismatch")
            if snapshot["authority_ordinal"] >= fence["authority_ordinal"]:
                raise ComposedProtocolError("checkpoint intent is not before recovery")

            target_cursors = {
                row["input_id"]: row["completed_data_through"]
                for row in self.connection.execute(
                    "SELECT * FROM operator_progress ORDER BY input_id"
                )
            }
            generation = self._next("recovery_runs", "generation")
            self.connection.execute(
                "INSERT INTO recovery_runs VALUES (?, ?, 'RESTORING', ?, NULL, ?, NULL, ?)",
                (
                    generation,
                    snapshot["checkpoint"],
                    snapshot["state_value"],
                    canonical(target_cursors),
                    fence["authority_ordinal"],
                ),
            )
            hit(fault, "recover:after_restoring_before_validation")

            cuts = json.loads(snapshot["cuts_json"])
            if set(cuts) != set(target_cursors):
                raise ComposedProtocolError("snapshot input manifest drift")
            for input_id, cut in cuts.items():
                barrier = self.connection.execute(
                    "SELECT * FROM stream_entries WHERE input_id = ? "
                    "AND log_sequence = ? AND kind = 'BARRIER'",
                    (input_id, cut["barrier_log"]),
                ).fetchone()
                observation = self.connection.execute(
                    "SELECT * FROM barrier_observations WHERE checkpoint = ? "
                    "AND input_id = ?",
                    (snapshot["checkpoint"], input_id),
                ).fetchone()
                if (
                    barrier is None
                    or barrier["checkpoint"] != snapshot["checkpoint"]
                    or barrier["cut_data_sequence"] != cut["data"]
                    or barrier["intent_digest"] != snapshot["authoritative_digest"]
                    or barrier["writer_epoch"] != snapshot["coordinator_epoch"]
                    or observation is None
                    or observation["log_sequence"] != barrier["log_sequence"]
                    or observation["cut_data_sequence"] != cut["data"]
                    or observation["intent_digest"] != snapshot["authoritative_digest"]
                ):
                    raise ComposedProtocolError("snapshot barrier authority mismatch")

            highwater = snapshot["mailbox_highwater"]
            if highwater == 0:
                state_at_highwater = manifest["initial_state"]
            else:
                highwater_turn = self.connection.execute(
                    "SELECT state_after FROM turns WHERE apply_ordinal = ?",
                    (highwater,),
                ).fetchone()
                state_at_highwater = (
                    None if highwater_turn is None else highwater_turn["state_after"]
                )
            prefix_ordinals = [
                row[0]
                for row in self.connection.execute(
                    "SELECT apply_ordinal FROM turns WHERE apply_ordinal <= ? "
                    "ORDER BY apply_ordinal",
                    (highwater,),
                )
            ]
            if (
                prefix_ordinals != list(range(1, highwater + 1))
                or state_at_highwater != snapshot["state_value"]
            ):
                raise ComposedProtocolError("snapshot mailbox highwater mismatch")
            hit(fault, "recover:after_validation_before_restore")
            durable_live_state = self.connection.execute(
                "SELECT state_value FROM operator_state WHERE singleton = 1"
            ).fetchone()[0]
            self.connection.execute(
                "UPDATE operator_state SET state_value = ? WHERE singleton = 1",
                (snapshot["state_value"],),
            )
            hit(fault, "recover:after_snapshot_restore")
            recovered_state = snapshot["state_value"]
            expected_ordinal = highwater + 1
            replay: list[list[int | str]] = []
            tail = self.connection.execute(
                "SELECT * FROM turns WHERE apply_ordinal > ? ORDER BY apply_ordinal",
                (highwater,),
            ).fetchall()
            for index, turn in enumerate(tail):
                if turn["apply_ordinal"] != expected_ordinal:
                    raise ComposedProtocolError("recovery mailbox order gap")
                if turn["input_id"] not in target_cursors:
                    raise ComposedProtocolError("recovery turn has unknown input")
                if not (
                    cuts[turn["input_id"]]["data"]
                    < turn["data_sequence"]
                    <= target_cursors[turn["input_id"]]
                ):
                    raise ComposedProtocolError("recovery turn is outside durable tail")
                output = self.connection.execute(
                    "SELECT * FROM output_events WHERE output_id = ?",
                    (turn["output_id"],),
                ).fetchone()
                if (
                    output is None
                    or output["kind"] != "DATA"
                    or output["input_id"] != turn["input_id"]
                    or output["data_sequence"] != turn["data_sequence"]
                ):
                    raise ComposedProtocolError("recovery output identity mismatch")
                recovered_state = apply_state(
                    recovered_state,
                    turn["input_id"],
                    turn["data_sequence"],
                    turn["payload_ref"],
                )
                if recovered_state != turn["state_after"]:
                    raise ComposedProtocolError("recovery state transition mismatch")
                replay.append(
                    [turn["apply_ordinal"], turn["input_id"], turn["data_sequence"]]
                )
                self.connection.execute(
                    "UPDATE operator_state SET state_value = ? WHERE singleton = 1",
                    (recovered_state,),
                )
                if index == 0:
                    hit(fault, "recover:after_first_tail")
                expected_ordinal += 1
            hit(fault, "recover:after_final_tail")

            for input_id, target in target_cursors.items():
                durable_sequences = [
                    row[0]
                    for row in self.connection.execute(
                        "SELECT data_sequence FROM turns WHERE input_id = ? "
                        "ORDER BY data_sequence",
                        (input_id,),
                    )
                ]
                if durable_sequences != list(range(1, target + 1)):
                    raise ComposedProtocolError("recovery cursor does not match turns")

            if recovered_state != durable_live_state:
                raise ComposedProtocolError(
                    "recovered state differs from durable state"
                )
            replayed_through = tail[-1]["apply_ordinal"] if tail else highwater
            self.connection.execute(
                "UPDATE recovery_runs SET phase = 'READY', recovered_state = ?, "
                "replayed_through_ordinal = ? WHERE generation = ?",
                (recovered_state, replayed_through, generation),
            )
            hit(fault, "recover:after_ready_before_commit")
            row = self.connection.execute(
                "SELECT * FROM recovery_runs WHERE generation = ?", (generation,)
            ).fetchone()
            report = self._recovery_report(row)
            report["replay"] = replay
            return report

    def recovery_status(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT * FROM recovery_runs ORDER BY generation DESC LIMIT 1"
        ).fetchone()
        return None if row is None else self._recovery_report(row)

    def status(self, checkpoint: int) -> str:
        row = self.connection.execute(
            "SELECT status FROM checkpoint_status WHERE checkpoint = ?", (checkpoint,)
        ).fetchone()
        if row is None:
            raise ComposedProtocolError("unknown checkpoint")
        return row[0]

    def progress(self) -> dict[str, dict[str, int | None]]:
        return {
            row["input_id"]: {
                "scanned_log_through": row["scanned_log_through"],
                "completed_data_through": row["completed_data_through"],
                "blocked_checkpoint": row["blocked_checkpoint"],
            }
            for row in self.connection.execute(
                "SELECT * FROM operator_progress ORDER BY input_id"
            )
        }

    def snapshot(self, checkpoint: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM snapshots WHERE checkpoint = ?", (checkpoint,)
        ).fetchone()
        if row is None:
            raise ComposedProtocolError("snapshot missing")
        return {
            "state": row["state_value"],
            "mailbox_highwater": row["mailbox_highwater"],
            "cuts": json.loads(row["cuts_json"]),
        }

    def outputs(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM output_events ORDER BY output_ordinal"
            )
        ]

    @staticmethod
    def _recovery_report(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "generation": row["generation"],
            "checkpoint": row["checkpoint"],
            "phase": row["phase"],
            "restored_state": row["restored_state"],
            "recovered_state": row["recovered_state"],
            "target_cursors": json.loads(row["target_cursors_json"]),
            "replayed_through_ordinal": row["replayed_through_ordinal"],
        }

    def _append_authority(self, event_type: str, checkpoint: int | None) -> int:
        ordinal = self._next("authority_log", "ordinal")
        self.connection.execute(
            "INSERT INTO authority_log VALUES (?, ?, ?)",
            (ordinal, event_type, checkpoint),
        )
        return ordinal

    def _active_recovery_generation(self) -> int:
        row = self.connection.execute(
            "SELECT MAX(generation) FROM recovery_runs WHERE phase = 'READY'"
        ).fetchone()
        return 0 if row[0] is None else int(row[0])

    def _partition(self, input_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM partitions WHERE input_id = ?", (input_id,)
        ).fetchone()
        if row is None:
            raise ComposedProtocolError("unknown partition")
        return row

    def _coordinator(self, owner_id: str, owner_epoch: int) -> sqlite3.Row:
        row = self.connection.execute("SELECT * FROM coordinator_owner").fetchone()
        if row is None or (row["owner_id"], row["owner_epoch"]) != (
            owner_id,
            owner_epoch,
        ):
            raise ComposedProtocolError("stale coordinator")
        return row

    @staticmethod
    def _entry(row: sqlite3.Row) -> LogEntry:
        return LogEntry(
            input_id=row["input_id"],
            log_sequence=row["log_sequence"],
            kind=EntryKind(row["kind"]),
            record_id=row["record_id"],
            data_sequence=row["data_sequence"],
            payload_ref=row["payload_ref"],
            checkpoint=row["checkpoint"],
            cut_data_sequence=row["cut_data_sequence"],
        )

    def _next(self, table: str, column: str) -> int:
        return int(
            self.connection.execute(
                f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}"
            ).fetchone()[0]
        )
