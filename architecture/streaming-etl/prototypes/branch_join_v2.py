"""DS-005a/b: independent fan-out cursors and a durable aligned-join buffer."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator


class BranchJoinProtocolError(RuntimeError):
    pass


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def apply_state(state: str, input_id: str, sequence: int, payload_ref: str) -> str:
    return f"{state}|{input_id}:{sequence}:{payload_ref}"


@dataclass(frozen=True)
class Delivery:
    branch_id: str
    sequence: int
    record_id: str
    payload_ref: str
    owner_id: str
    owner_epoch: int
    attempt: int


class BranchJoinStore:
    """Focused SQLite model; storage capacity is assumed available in W6."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS deployment (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                deployment_id TEXT NOT NULL UNIQUE,
                branches_json TEXT NOT NULL,
                manifest_digest TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS branch_manifest (
                branch_id TEXT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS upstream_entries (
                log_sequence INTEGER PRIMARY KEY CHECK (log_sequence > 0),
                kind TEXT NOT NULL CHECK (kind IN ('DATA', 'BARRIER')),
                record_id TEXT NOT NULL UNIQUE,
                data_sequence INTEGER UNIQUE,
                payload_ref TEXT,
                payload_digest TEXT,
                checkpoint INTEGER,
                cut_data_sequence INTEGER,
                intent_digest TEXT,
                writer_epoch INTEGER NOT NULL,
                CHECK (
                    (kind = 'DATA' AND data_sequence IS NOT NULL
                     AND payload_ref IS NOT NULL AND payload_digest IS NOT NULL
                     AND checkpoint IS NULL AND cut_data_sequence IS NULL
                     AND intent_digest IS NULL)
                    OR
                    (kind = 'BARRIER' AND data_sequence IS NULL
                     AND payload_ref IS NULL AND payload_digest IS NULL
                     AND checkpoint IS NOT NULL AND cut_data_sequence IS NOT NULL
                     AND intent_digest IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS fanout_checkpoints (
                checkpoint INTEGER PRIMARY KEY CHECK (checkpoint > 0),
                status TEXT NOT NULL CHECK (status IN ('STARTED', 'ALIGNED')),
                intent_digest TEXT NOT NULL UNIQUE,
                coordinator_epoch INTEGER NOT NULL,
                cut_data_sequence INTEGER NOT NULL,
                branches_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS branch_progress (
                branch_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
                scanned_log_through INTEGER NOT NULL DEFAULT 0,
                completed_through INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id)
            );
            CREATE TABLE IF NOT EXISTS branch_in_flight (
                branch_id TEXT PRIMARY KEY,
                log_sequence INTEGER NOT NULL,
                data_sequence INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL,
                attempt INTEGER NOT NULL CHECK (attempt > 0),
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id),
                FOREIGN KEY (log_sequence) REFERENCES upstream_entries(log_sequence)
            );
            CREATE TABLE IF NOT EXISTS branch_delivery_history (
                branch_id TEXT NOT NULL,
                data_sequence INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                PRIMARY KEY (branch_id, data_sequence, attempt)
            );
            CREATE TABLE IF NOT EXISTS branch_completions (
                branch_id TEXT NOT NULL,
                data_sequence INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                output_id TEXT NOT NULL UNIQUE,
                lineage_digest TEXT NOT NULL,
                PRIMARY KEY (branch_id, data_sequence),
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id),
                FOREIGN KEY (output_id) REFERENCES branch_outputs(output_id)
            );
            CREATE TABLE IF NOT EXISTS branch_outputs (
                output_id TEXT PRIMARY KEY,
                branch_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                upstream_data_sequence INTEGER NOT NULL,
                upstream_record_id TEXT NOT NULL,
                payload_ref TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                lineage_digest TEXT NOT NULL,
                UNIQUE (branch_id, partition_id, upstream_data_sequence),
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id),
                FOREIGN KEY (upstream_data_sequence)
                    REFERENCES upstream_entries(data_sequence)
            );
            CREATE TABLE IF NOT EXISTS branch_barrier_outputs (
                barrier_output_id TEXT PRIMARY KEY,
                branch_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                checkpoint INTEGER NOT NULL,
                upstream_log_sequence INTEGER NOT NULL,
                cut_data_sequence INTEGER NOT NULL,
                intent_digest TEXT NOT NULL,
                writer_epoch INTEGER NOT NULL,
                lineage_digest TEXT NOT NULL,
                UNIQUE (branch_id, partition_id, checkpoint),
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id),
                FOREIGN KEY (checkpoint) REFERENCES fanout_checkpoints(checkpoint),
                FOREIGN KEY (upstream_log_sequence)
                    REFERENCES upstream_entries(log_sequence)
            );
            CREATE TABLE IF NOT EXISTS branch_barrier_observations (
                branch_id TEXT NOT NULL,
                checkpoint INTEGER NOT NULL,
                log_sequence INTEGER NOT NULL,
                cut_data_sequence INTEGER NOT NULL,
                intent_digest TEXT NOT NULL,
                barrier_output_id TEXT NOT NULL UNIQUE,
                PRIMARY KEY (branch_id, checkpoint),
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id),
                FOREIGN KEY (log_sequence) REFERENCES upstream_entries(log_sequence),
                FOREIGN KEY (barrier_output_id)
                    REFERENCES branch_barrier_outputs(barrier_output_id)
            );

            CREATE TABLE IF NOT EXISTS join_manifest (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                inputs_json TEXT NOT NULL,
                bindings_json TEXT NOT NULL,
                initial_state TEXT NOT NULL,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS join_inputs (
                input_id TEXT PRIMARY KEY,
                branch_id TEXT NOT NULL UNIQUE,
                partition_id TEXT NOT NULL,
                UNIQUE (branch_id, partition_id),
                FOREIGN KEY (branch_id) REFERENCES branch_manifest(branch_id)
            );
            CREATE TABLE IF NOT EXISTS join_entries (
                input_id TEXT NOT NULL,
                log_sequence INTEGER NOT NULL CHECK (log_sequence > 0),
                arrival_ordinal INTEGER NOT NULL UNIQUE CHECK (arrival_ordinal > 0),
                kind TEXT NOT NULL CHECK (kind IN ('DATA', 'BARRIER')),
                record_id TEXT NOT NULL,
                source_output_id TEXT,
                source_barrier_id TEXT,
                data_sequence INTEGER,
                payload_ref TEXT,
                payload_digest TEXT,
                checkpoint INTEGER,
                cut_data_sequence INTEGER,
                intent_digest TEXT,
                writer_epoch INTEGER NOT NULL,
                PRIMARY KEY (input_id, log_sequence),
                UNIQUE (input_id, record_id),
                UNIQUE (input_id, data_sequence),
                UNIQUE (source_output_id),
                UNIQUE (source_barrier_id),
                FOREIGN KEY (input_id) REFERENCES join_inputs(input_id),
                FOREIGN KEY (source_output_id) REFERENCES branch_outputs(output_id),
                FOREIGN KEY (source_barrier_id)
                    REFERENCES branch_barrier_outputs(barrier_output_id),
                CHECK (
                    (kind = 'DATA' AND data_sequence IS NOT NULL
                     AND source_output_id IS NOT NULL
                     AND source_barrier_id IS NULL
                     AND payload_ref IS NOT NULL AND payload_digest IS NOT NULL
                     AND checkpoint IS NULL AND cut_data_sequence IS NULL
                     AND intent_digest IS NULL)
                    OR
                    (kind = 'BARRIER' AND data_sequence IS NULL
                     AND source_output_id IS NULL
                     AND source_barrier_id IS NOT NULL
                     AND payload_ref IS NULL AND payload_digest IS NULL
                     AND checkpoint IS NOT NULL AND cut_data_sequence IS NOT NULL
                     AND intent_digest IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS join_checkpoint_intents (
                checkpoint INTEGER PRIMARY KEY,
                intent_digest TEXT NOT NULL UNIQUE,
                inputs_json TEXT NOT NULL,
                coordinator_epoch INTEGER NOT NULL,
                state_schema TEXT NOT NULL,
                code_version TEXT NOT NULL,
                fanout_intent_digest TEXT NOT NULL,
                fanout_writer_epoch INTEGER NOT NULL,
                fanout_cut_data_sequence INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('STARTED', 'ALIGNED', 'COMPLETED'))
            );
            CREATE TABLE IF NOT EXISTS join_progress (
                input_id TEXT PRIMARY KEY,
                scanned_log_through INTEGER NOT NULL DEFAULT 0,
                completed_data_through INTEGER NOT NULL DEFAULT 0,
                blocked_checkpoint INTEGER,
                FOREIGN KEY (input_id) REFERENCES join_inputs(input_id)
            );
            CREATE TABLE IF NOT EXISTS join_observations (
                checkpoint INTEGER NOT NULL,
                input_id TEXT NOT NULL,
                log_sequence INTEGER NOT NULL,
                cut_data_sequence INTEGER NOT NULL,
                intent_digest TEXT NOT NULL,
                PRIMARY KEY (checkpoint, input_id),
                FOREIGN KEY (input_id, log_sequence)
                    REFERENCES join_entries(input_id, log_sequence)
            );
            CREATE TABLE IF NOT EXISTS join_buffer (
                checkpoint INTEGER NOT NULL,
                input_id TEXT NOT NULL,
                data_sequence INTEGER NOT NULL,
                log_sequence INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                arrival_ordinal INTEGER NOT NULL UNIQUE,
                payload_ref TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                drained INTEGER NOT NULL DEFAULT 0 CHECK (drained IN (0, 1)),
                drain_apply_ordinal INTEGER UNIQUE,
                PRIMARY KEY (checkpoint, input_id, data_sequence),
                FOREIGN KEY (input_id, log_sequence)
                    REFERENCES join_entries(input_id, log_sequence),
                CHECK (
                    (drained = 0 AND drain_apply_ordinal IS NULL)
                    OR (drained = 1 AND drain_apply_ordinal IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS join_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                state_value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS join_turns (
                apply_ordinal INTEGER PRIMARY KEY,
                input_id TEXT NOT NULL,
                data_sequence INTEGER NOT NULL,
                log_sequence INTEGER NOT NULL,
                payload_ref TEXT NOT NULL,
                state_after TEXT NOT NULL,
                output_id TEXT NOT NULL UNIQUE,
                source TEXT NOT NULL CHECK (source IN ('DIRECT', 'BUFFER')),
                UNIQUE (input_id, data_sequence)
            );
            CREATE TABLE IF NOT EXISTS join_snapshots (
                checkpoint INTEGER PRIMARY KEY,
                state_value TEXT NOT NULL,
                mailbox_highwater INTEGER NOT NULL,
                cuts_json TEXT NOT NULL,
                intent_digest TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS join_outputs (
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

            CREATE TRIGGER IF NOT EXISTS deployment_immutable
            BEFORE UPDATE ON deployment BEGIN
                SELECT RAISE(ABORT, 'deployment manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS deployment_create_once
            BEFORE INSERT ON deployment
            WHEN EXISTS (SELECT 1 FROM deployment) BEGIN
                SELECT RAISE(ABORT, 'deployment manifest already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS deployment_no_delete
            BEFORE DELETE ON deployment BEGIN
                SELECT RAISE(ABORT, 'deployment manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS branch_manifest_no_delete
            BEFORE DELETE ON branch_manifest BEGIN
                SELECT RAISE(ABORT, 'branch manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS upstream_immutable
            BEFORE UPDATE ON upstream_entries BEGIN
                SELECT RAISE(ABORT, 'upstream entry is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS upstream_no_delete
            BEFORE DELETE ON upstream_entries BEGIN
                SELECT RAISE(ABORT, 'upstream retention is outside W6');
            END;
            CREATE TRIGGER IF NOT EXISTS completion_immutable
            BEFORE UPDATE ON branch_completions BEGIN
                SELECT RAISE(ABORT, 'branch completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS completion_no_delete
            BEFORE DELETE ON branch_completions BEGIN
                SELECT RAISE(ABORT, 'branch completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS branch_output_immutable
            BEFORE UPDATE ON branch_outputs BEGIN
                SELECT RAISE(ABORT, 'branch output is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS branch_output_no_delete
            BEFORE DELETE ON branch_outputs BEGIN
                SELECT RAISE(ABORT, 'branch output is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS branch_barrier_output_immutable
            BEFORE UPDATE ON branch_barrier_outputs BEGIN
                SELECT RAISE(ABORT, 'branch barrier output is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS branch_barrier_output_no_delete
            BEFORE DELETE ON branch_barrier_outputs BEGIN
                SELECT RAISE(ABORT, 'branch barrier output is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_binding_immutable
            BEFORE UPDATE ON join_inputs BEGIN
                SELECT RAISE(ABORT, 'join input binding is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_binding_no_delete
            BEFORE DELETE ON join_inputs BEGIN
                SELECT RAISE(ABORT, 'join input binding is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_manifest_create_once
            BEFORE INSERT ON join_manifest
            WHEN EXISTS (SELECT 1 FROM join_manifest) BEGIN
                SELECT RAISE(ABORT, 'join manifest already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS join_manifest_immutable
            BEFORE UPDATE ON join_manifest BEGIN
                SELECT RAISE(ABORT, 'join manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_manifest_no_delete
            BEFORE DELETE ON join_manifest BEGIN
                SELECT RAISE(ABORT, 'join manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_entry_immutable
            BEFORE UPDATE ON join_entries BEGIN
                SELECT RAISE(ABORT, 'join entry is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_entry_no_delete
            BEFORE DELETE ON join_entries BEGIN
                SELECT RAISE(ABORT, 'join entry is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_turn_immutable
            BEFORE UPDATE ON join_turns BEGIN
                SELECT RAISE(ABORT, 'join turn is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_turn_no_delete
            BEFORE DELETE ON join_turns BEGIN
                SELECT RAISE(ABORT, 'join turn is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_snapshot_immutable
            BEFORE UPDATE ON join_snapshots BEGIN
                SELECT RAISE(ABORT, 'join snapshot is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_snapshot_no_delete
            BEFORE DELETE ON join_snapshots BEGIN
                SELECT RAISE(ABORT, 'join snapshot is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_output_immutable
            BEFORE UPDATE ON join_outputs BEGIN
                SELECT RAISE(ABORT, 'join output is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_output_no_delete
            BEFORE DELETE ON join_outputs BEGIN
                SELECT RAISE(ABORT, 'join output is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_buffer_identity_immutable
            BEFORE UPDATE OF checkpoint, input_id, data_sequence, log_sequence,
                record_id, arrival_ordinal, payload_ref, payload_digest
            ON join_buffer BEGIN
                SELECT RAISE(ABORT, 'join buffer identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_buffer_no_delete
            BEFORE DELETE ON join_buffer BEGIN
                SELECT RAISE(ABORT, 'join buffer is immutable');
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

    def create_fanout(self, deployment_id: str, branches: tuple[str, ...]) -> None:
        ordered = tuple(sorted(branches))
        if not deployment_id or not ordered or len(set(ordered)) != len(ordered):
            raise BranchJoinProtocolError("invalid branch manifest")
        manifest = {"deployment_id": deployment_id, "branches": list(ordered)}
        with self._write():
            row = self.connection.execute("SELECT * FROM deployment").fetchone()
            expected = (deployment_id, canonical(list(ordered)), digest(manifest))
            if row is None:
                self.connection.execute(
                    "INSERT INTO deployment VALUES (1, ?, ?, ?)", expected
                )
                for branch_id in ordered:
                    self.connection.execute(
                        "INSERT INTO branch_manifest VALUES (?)", (branch_id,)
                    )
                    self.connection.execute(
                        "INSERT INTO branch_progress(branch_id, owner_id, owner_epoch) "
                        "VALUES (?, ?, 1)",
                        (branch_id, f"{branch_id}-worker"),
                    )
            elif tuple(row)[1:] != expected:
                raise BranchJoinProtocolError("branch manifest drift")

    def append_upstream(self, record_id: str, payload_ref: str) -> int:
        with self._write():
            existing = self.connection.execute(
                "SELECT * FROM upstream_entries WHERE record_id = ?", (record_id,)
            ).fetchone()
            payload_digest = digest({"payload_ref": payload_ref})
            if existing is not None:
                if (
                    existing["kind"] != "DATA"
                    or existing["payload_ref"] != payload_ref
                    or existing["payload_digest"] != payload_digest
                ):
                    raise BranchJoinProtocolError("upstream record identity drift")
                return existing["data_sequence"]
            log_sequence = self._next("upstream_entries", "log_sequence")
            data_sequence = self.connection.execute(
                "SELECT COALESCE(MAX(data_sequence), 0) + 1 "
                "FROM upstream_entries WHERE kind = 'DATA'"
            ).fetchone()[0]
            self.connection.execute(
                "INSERT INTO upstream_entries VALUES "
                "(?, 'DATA', ?, ?, ?, ?, NULL, NULL, NULL, 1)",
                (
                    log_sequence,
                    record_id,
                    data_sequence,
                    payload_ref,
                    payload_digest,
                ),
            )
            return int(data_sequence)

    def start_fanout_checkpoint(
        self, checkpoint: int, coordinator_epoch: int = 1
    ) -> str:
        with self._write():
            deployment = self.connection.execute("SELECT * FROM deployment").fetchone()
            if deployment is None:
                raise BranchJoinProtocolError("deployment missing")
            if self.connection.execute(
                "SELECT 1 FROM fanout_checkpoints WHERE status != 'ALIGNED'"
            ).fetchone():
                raise BranchJoinProtocolError("fanout checkpoint active")
            previous = self.connection.execute(
                "SELECT COALESCE(MAX(checkpoint), 0) FROM fanout_checkpoints"
            ).fetchone()[0]
            if checkpoint != previous + 1:
                raise BranchJoinProtocolError("fanout checkpoint must be next")
            cut = self.connection.execute(
                "SELECT COALESCE(MAX(data_sequence), 0) FROM upstream_entries "
                "WHERE kind = 'DATA'"
            ).fetchone()[0]
            intent = {
                "checkpoint": checkpoint,
                "coordinator_epoch": coordinator_epoch,
                "cut": cut,
                "branches": json.loads(deployment["branches_json"]),
            }
            intent_digest = digest(intent)
            self.connection.execute(
                "INSERT INTO fanout_checkpoints VALUES (?, 'STARTED', ?, ?, ?, ?)",
                (
                    checkpoint,
                    intent_digest,
                    coordinator_epoch,
                    cut,
                    deployment["branches_json"],
                ),
            )
            self.connection.execute(
                "INSERT INTO upstream_entries VALUES "
                "(?, 'BARRIER', ?, NULL, NULL, NULL, ?, ?, ?, ?)",
                (
                    self._next("upstream_entries", "log_sequence"),
                    f"barrier:{checkpoint}",
                    checkpoint,
                    cut,
                    intent_digest,
                    coordinator_epoch,
                ),
            )
            return intent_digest

    def claim_branch(self, branch_id: str, owner_id: str, owner_epoch: int) -> None:
        with self._write():
            row = self._branch(branch_id)
            if owner_epoch <= row["owner_epoch"]:
                if (owner_id, owner_epoch) == (row["owner_id"], row["owner_epoch"]):
                    return
                raise BranchJoinProtocolError("stale branch owner")
            self.connection.execute(
                "UPDATE branch_progress SET owner_id = ?, owner_epoch = ? "
                "WHERE branch_id = ?",
                (owner_id, owner_epoch, branch_id),
            )
            self.connection.execute(
                "DELETE FROM branch_in_flight WHERE branch_id = ?", (branch_id,)
            )

    def deliver_branch(
        self, branch_id: str, owner_id: str, owner_epoch: int
    ) -> Delivery | None:
        with self._write():
            progress = self._branch_owner(branch_id, owner_id, owner_epoch)
            existing = self.connection.execute(
                "SELECT * FROM branch_in_flight WHERE branch_id = ?", (branch_id,)
            ).fetchone()
            if existing is not None:
                return Delivery(
                    branch_id,
                    existing["data_sequence"],
                    existing["record_id"],
                    self.connection.execute(
                        "SELECT payload_ref FROM upstream_entries WHERE log_sequence = ?",
                        (existing["log_sequence"],),
                    ).fetchone()[0],
                    owner_id,
                    owner_epoch,
                    existing["attempt"],
                )
            entry = self.connection.execute(
                "SELECT * FROM upstream_entries WHERE log_sequence = ?",
                (progress["scanned_log_through"] + 1,),
            ).fetchone()
            if entry is None or entry["kind"] == "BARRIER":
                return None
            if entry["data_sequence"] != progress["completed_through"] + 1:
                raise BranchJoinProtocolError("branch data cursor gap")
            attempt = self.connection.execute(
                "SELECT COALESCE(MAX(attempt), 0) + 1 FROM branch_delivery_history "
                "WHERE branch_id = ? AND data_sequence = ?",
                (branch_id, entry["data_sequence"]),
            ).fetchone()[0]
            self.connection.execute(
                "INSERT INTO branch_in_flight VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    branch_id,
                    entry["log_sequence"],
                    entry["data_sequence"],
                    entry["record_id"],
                    owner_id,
                    owner_epoch,
                    attempt,
                ),
            )
            self.connection.execute(
                "INSERT INTO branch_delivery_history VALUES (?, ?, ?, ?, ?, ?)",
                (
                    branch_id,
                    entry["data_sequence"],
                    attempt,
                    owner_id,
                    owner_epoch,
                    entry["record_id"],
                ),
            )
            return Delivery(
                branch_id,
                entry["data_sequence"],
                entry["record_id"],
                entry["payload_ref"],
                owner_id,
                owner_epoch,
                attempt,
            )

    def complete_branch(
        self, delivery: Delivery, output_payload_ref: str | None = None
    ) -> str:
        with self._write():
            self._branch_owner(
                delivery.branch_id, delivery.owner_id, delivery.owner_epoch
            )
            completed = self.connection.execute(
                "SELECT * FROM branch_completions WHERE branch_id = ? "
                "AND data_sequence = ?",
                (delivery.branch_id, delivery.sequence),
            ).fetchone()
            deployment_id = self.connection.execute(
                "SELECT deployment_id FROM deployment"
            ).fetchone()[0]
            lineage = {
                "deployment": deployment_id,
                "branch": delivery.branch_id,
                "record_id": delivery.record_id,
            }
            output_id = "branch-out:" + digest(lineage)
            lineage_digest = digest(lineage)
            output_payload_ref = output_payload_ref or (
                f"ref:branch-output:{delivery.branch_id}:{delivery.record_id}"
            )
            output_payload_digest = digest({"payload_ref": output_payload_ref})
            if completed is not None:
                output = self.connection.execute(
                    "SELECT * FROM branch_outputs WHERE output_id = ?", (output_id,)
                ).fetchone()
                if (
                    completed["record_id"] != delivery.record_id
                    or completed["output_id"] != output_id
                    or completed["lineage_digest"] != lineage_digest
                    or output is None
                    or output["payload_ref"] != output_payload_ref
                    or output["payload_digest"] != output_payload_digest
                ):
                    raise BranchJoinProtocolError("branch completion identity drift")
                return output_id
            progress = self.connection.execute(
                "SELECT * FROM branch_progress WHERE branch_id = ?",
                (delivery.branch_id,),
            ).fetchone()
            in_flight = self.connection.execute(
                "SELECT * FROM branch_in_flight WHERE branch_id = ?",
                (delivery.branch_id,),
            ).fetchone()
            if (
                in_flight is None
                or in_flight["data_sequence"] != delivery.sequence
                or in_flight["record_id"] != delivery.record_id
                or in_flight["owner_id"] != delivery.owner_id
                or in_flight["owner_epoch"] != delivery.owner_epoch
                or delivery.sequence != progress["completed_through"] + 1
            ):
                raise BranchJoinProtocolError("branch completion lacks exact delivery")
            self.connection.execute(
                "INSERT INTO branch_outputs VALUES (?, ?, 'p0', ?, ?, ?, ?, ?)",
                (
                    output_id,
                    delivery.branch_id,
                    delivery.sequence,
                    delivery.record_id,
                    output_payload_ref,
                    output_payload_digest,
                    lineage_digest,
                ),
            )
            self.connection.execute(
                "INSERT INTO branch_completions VALUES (?, ?, ?, ?, ?)",
                (
                    delivery.branch_id,
                    delivery.sequence,
                    delivery.record_id,
                    output_id,
                    lineage_digest,
                ),
            )
            self.connection.execute(
                "UPDATE branch_progress SET completed_through = ?, "
                "scanned_log_through = ? WHERE branch_id = ?",
                (delivery.sequence, in_flight["log_sequence"], delivery.branch_id),
            )
            self.connection.execute(
                "DELETE FROM branch_in_flight WHERE branch_id = ?",
                (delivery.branch_id,),
            )
            return output_id

    def observe_branch_barrier(
        self, branch_id: str, owner_id: str, owner_epoch: int
    ) -> int | None:
        with self._write():
            progress = self._branch_owner(branch_id, owner_id, owner_epoch)
            if self.connection.execute(
                "SELECT 1 FROM branch_in_flight WHERE branch_id = ?", (branch_id,)
            ).fetchone():
                raise BranchJoinProtocolError("branch has in-flight DATA")
            entry = self.connection.execute(
                "SELECT * FROM upstream_entries WHERE log_sequence = ?",
                (progress["scanned_log_through"] + 1,),
            ).fetchone()
            if entry is None or entry["kind"] != "BARRIER":
                return None
            intent = self.connection.execute(
                "SELECT * FROM fanout_checkpoints WHERE checkpoint = ?",
                (entry["checkpoint"],),
            ).fetchone()
            if (
                intent is None
                or intent["intent_digest"] != entry["intent_digest"]
                or intent["coordinator_epoch"] != entry["writer_epoch"]
                or entry["cut_data_sequence"] != progress["completed_through"]
            ):
                raise BranchJoinProtocolError("fanout barrier authority mismatch")
            deployment_id = self.connection.execute(
                "SELECT deployment_id FROM deployment"
            ).fetchone()[0]
            barrier_lineage = {
                "deployment": deployment_id,
                "branch": branch_id,
                "partition": "p0",
                "checkpoint": entry["checkpoint"],
                "upstream_log_sequence": entry["log_sequence"],
                "cut": entry["cut_data_sequence"],
                "intent_digest": entry["intent_digest"],
                "writer_epoch": entry["writer_epoch"],
            }
            barrier_output_id = "branch-barrier-out:" + digest(barrier_lineage)
            self.connection.execute(
                "INSERT INTO branch_barrier_outputs VALUES "
                "(?, ?, 'p0', ?, ?, ?, ?, ?, ?)",
                (
                    barrier_output_id,
                    branch_id,
                    entry["checkpoint"],
                    entry["log_sequence"],
                    entry["cut_data_sequence"],
                    entry["intent_digest"],
                    entry["writer_epoch"],
                    digest(barrier_lineage),
                ),
            )
            self.connection.execute(
                "INSERT OR IGNORE INTO branch_barrier_observations "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    branch_id,
                    entry["checkpoint"],
                    entry["log_sequence"],
                    entry["cut_data_sequence"],
                    entry["intent_digest"],
                    barrier_output_id,
                ),
            )
            self.connection.execute(
                "UPDATE branch_progress SET scanned_log_through = ? WHERE branch_id = ?",
                (entry["log_sequence"], branch_id),
            )
            observed = self.connection.execute(
                "SELECT COUNT(*) FROM branch_barrier_observations WHERE checkpoint = ?",
                (entry["checkpoint"],),
            ).fetchone()[0]
            required = len(json.loads(intent["branches_json"]))
            if observed == required:
                self.connection.execute(
                    "UPDATE fanout_checkpoints SET status = 'ALIGNED' "
                    "WHERE checkpoint = ?",
                    (entry["checkpoint"],),
                )
            return entry["checkpoint"]

    def branch_positions(self) -> dict[str, dict[str, Any]]:
        positions = {}
        for row in self.connection.execute(
            "SELECT * FROM branch_progress ORDER BY branch_id"
        ):
            in_flight = self.connection.execute(
                "SELECT data_sequence FROM branch_in_flight WHERE branch_id = ?",
                (row["branch_id"],),
            ).fetchone()
            positions[row["branch_id"]] = {
                "owner_id": row["owner_id"],
                "owner_epoch": row["owner_epoch"],
                "scanned_log_through": row["scanned_log_through"],
                "completed_through": row["completed_through"],
                "in_flight": None if in_flight is None else in_flight[0],
            }
        return positions

    def create_join(
        self,
        bindings: dict[str, tuple[str, str]],
        *,
        initial_state: str = "J",
        state_schema: str = "join-state/v1",
        code_version: str = "join-code/v1",
    ) -> None:
        if not bindings or len({v for v in bindings.values()}) != len(bindings):
            raise BranchJoinProtocolError("join bindings must be unique")
        inputs = sorted(bindings)
        bindings_value = {
            input_id: {
                "branch": bindings[input_id][0],
                "partition": bindings[input_id][1],
            }
            for input_id in inputs
        }
        expected = (
            canonical(inputs),
            canonical(bindings_value),
            initial_state,
            state_schema,
            code_version,
        )
        with self._write():
            row = self.connection.execute("SELECT * FROM join_manifest").fetchone()
            if row is None:
                self.connection.execute(
                    "INSERT INTO join_manifest VALUES (1, ?, ?, ?, ?, ?)", expected
                )
                self.connection.execute(
                    "INSERT INTO join_state VALUES (1, ?)", (initial_state,)
                )
                for input_id in inputs:
                    branch_id, partition_id = bindings[input_id]
                    self.connection.execute(
                        "INSERT INTO join_inputs VALUES (?, ?, ?)",
                        (input_id, branch_id, partition_id),
                    )
                    self.connection.execute(
                        "INSERT INTO join_progress(input_id) VALUES (?)", (input_id,)
                    )
            elif tuple(row)[1:] != expected:
                raise BranchJoinProtocolError("join manifest drift")

    def start_join_checkpoint(self, checkpoint: int, coordinator_epoch: int = 1) -> str:
        with self._write():
            manifest = self.connection.execute("SELECT * FROM join_manifest").fetchone()
            if manifest is None:
                raise BranchJoinProtocolError("join manifest missing")
            if self.connection.execute(
                "SELECT 1 FROM join_checkpoint_intents WHERE status != 'COMPLETED'"
            ).fetchone():
                raise BranchJoinProtocolError("join checkpoint active")
            previous = self.connection.execute(
                "SELECT COALESCE(MAX(checkpoint), 0) FROM join_checkpoint_intents"
            ).fetchone()[0]
            if checkpoint != previous + 1:
                raise BranchJoinProtocolError("join checkpoint must be next")
            fanout = self.connection.execute(
                "SELECT * FROM fanout_checkpoints WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            if fanout is None:
                raise BranchJoinProtocolError("fanout checkpoint missing")
            intent = {
                "checkpoint": checkpoint,
                "inputs": json.loads(manifest["inputs_json"]),
                "coordinator_epoch": coordinator_epoch,
                "state_schema": manifest["state_schema"],
                "code_version": manifest["code_version"],
                "fanout_intent_digest": fanout["intent_digest"],
                "fanout_writer_epoch": fanout["coordinator_epoch"],
                "fanout_cut_data_sequence": fanout["cut_data_sequence"],
            }
            intent_digest = digest(intent)
            self.connection.execute(
                "INSERT INTO join_checkpoint_intents VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, 'STARTED')",
                (
                    checkpoint,
                    intent_digest,
                    manifest["inputs_json"],
                    coordinator_epoch,
                    manifest["state_schema"],
                    manifest["code_version"],
                    fanout["intent_digest"],
                    fanout["coordinator_epoch"],
                    fanout["cut_data_sequence"],
                ),
            )
            return intent_digest

    def append_join_data(self, input_id: str, source_output_id: str) -> int:
        with self._write():
            binding = self._join_input(input_id)
            source = self.connection.execute(
                "SELECT * FROM branch_outputs WHERE output_id = ?",
                (source_output_id,),
            ).fetchone()
            if source is None:
                raise BranchJoinProtocolError("join source output missing")
            if (source["branch_id"], source["partition_id"]) != (
                binding["branch_id"],
                binding["partition_id"],
            ):
                raise BranchJoinProtocolError("join source violates input binding")
            record_id = source_output_id
            payload_ref = source["payload_ref"]
            existing = self.connection.execute(
                "SELECT * FROM join_entries WHERE input_id = ? AND record_id = ?",
                (input_id, record_id),
            ).fetchone()
            payload_digest = source["payload_digest"]
            if existing is not None:
                if (
                    existing["kind"] != "DATA"
                    or existing["source_output_id"] != source_output_id
                    or existing["payload_ref"] != payload_ref
                    or existing["payload_digest"] != payload_digest
                ):
                    raise BranchJoinProtocolError("join record identity drift")
                return existing["data_sequence"]
            log_sequence = self._next_for_input(
                "join_entries", input_id, "log_sequence"
            )
            data_sequence = self._next_for_input(
                "join_entries", input_id, "data_sequence"
            )
            if source["upstream_data_sequence"] != data_sequence:
                raise BranchJoinProtocolError("join source sequence is not contiguous")
            arrival_ordinal = self._next("join_entries", "arrival_ordinal")
            self.connection.execute(
                "INSERT INTO join_entries "
                "(input_id, log_sequence, arrival_ordinal, kind, record_id, "
                "source_output_id, source_barrier_id, data_sequence, payload_ref, "
                "payload_digest, checkpoint, cut_data_sequence, intent_digest, "
                "writer_epoch) VALUES "
                "(?, ?, ?, 'DATA', ?, ?, NULL, ?, ?, ?, NULL, NULL, NULL, 1)",
                (
                    input_id,
                    log_sequence,
                    arrival_ordinal,
                    record_id,
                    source_output_id,
                    data_sequence,
                    payload_ref,
                    payload_digest,
                ),
            )
            progress = self.connection.execute(
                "SELECT * FROM join_progress WHERE input_id = ?", (input_id,)
            ).fetchone()
            if progress["blocked_checkpoint"] is not None:
                self.connection.execute(
                    "INSERT INTO join_buffer VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)",
                    (
                        progress["blocked_checkpoint"],
                        input_id,
                        data_sequence,
                        log_sequence,
                        record_id,
                        arrival_ordinal,
                        payload_ref,
                        payload_digest,
                    ),
                )
            return int(data_sequence)

    def inject_join_barrier(
        self,
        checkpoint: int,
        input_id: str,
        intent_digest: str,
        source_barrier_id: str,
        writer_epoch: int = 1,
    ) -> int:
        with self._write():
            binding = self._join_input(input_id)
            intent = self.connection.execute(
                "SELECT * FROM join_checkpoint_intents WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            if (
                intent is None
                or intent["status"] != "STARTED"
                or intent["intent_digest"] != intent_digest
                or intent["coordinator_epoch"] != writer_epoch
                or input_id not in json.loads(intent["inputs_json"])
            ):
                raise BranchJoinProtocolError("join barrier lacks exact intent")
            source = self.connection.execute(
                "SELECT * FROM branch_barrier_outputs WHERE barrier_output_id = ?",
                (source_barrier_id,),
            ).fetchone()
            if source is None:
                raise BranchJoinProtocolError("join source barrier missing")
            if (source["branch_id"], source["partition_id"]) != (
                binding["branch_id"],
                binding["partition_id"],
            ):
                raise BranchJoinProtocolError(
                    "join source barrier violates input binding"
                )
            if (
                source["checkpoint"] != checkpoint
                or source["intent_digest"] != intent["fanout_intent_digest"]
                or source["writer_epoch"] != intent["fanout_writer_epoch"]
                or source["cut_data_sequence"] != intent["fanout_cut_data_sequence"]
            ):
                raise BranchJoinProtocolError("join source barrier authority mismatch")
            existing = self.connection.execute(
                "SELECT * FROM join_entries WHERE input_id = ? AND checkpoint = ?",
                (input_id, checkpoint),
            ).fetchone()
            if existing is not None:
                if (
                    existing["intent_digest"] != intent_digest
                    or existing["source_barrier_id"] != source_barrier_id
                ):
                    raise BranchJoinProtocolError("join barrier identity drift")
                return existing["log_sequence"]
            log_sequence = self._next_for_input(
                "join_entries", input_id, "log_sequence"
            )
            cut = self.connection.execute(
                "SELECT COALESCE(MAX(data_sequence), 0) FROM join_entries "
                "WHERE input_id = ? AND kind = 'DATA'",
                (input_id,),
            ).fetchone()[0]
            if cut != source["cut_data_sequence"]:
                raise BranchJoinProtocolError("join source barrier cut mismatch")
            self.connection.execute(
                "INSERT INTO join_entries "
                "(input_id, log_sequence, arrival_ordinal, kind, record_id, "
                "source_output_id, source_barrier_id, data_sequence, payload_ref, "
                "payload_digest, checkpoint, cut_data_sequence, intent_digest, "
                "writer_epoch) VALUES "
                "(?, ?, ?, 'BARRIER', ?, NULL, ?, NULL, NULL, NULL, ?, ?, ?, ?)",
                (
                    input_id,
                    log_sequence,
                    self._next("join_entries", "arrival_ordinal"),
                    source_barrier_id,
                    source_barrier_id,
                    checkpoint,
                    cut,
                    intent_digest,
                    writer_epoch,
                ),
            )
            return int(log_sequence)

    def process_join_next(self, input_id: str) -> str | None:
        with self._write():
            progress = self.connection.execute(
                "SELECT * FROM join_progress WHERE input_id = ?", (input_id,)
            ).fetchone()
            if progress is None:
                raise BranchJoinProtocolError("unknown join input")
            if progress["blocked_checkpoint"] is not None:
                return None
            entry = self.connection.execute(
                "SELECT * FROM join_entries WHERE input_id = ? AND log_sequence = ?",
                (input_id, progress["scanned_log_through"] + 1),
            ).fetchone()
            if entry is None:
                return None
            if entry["kind"] == "DATA":
                if entry["data_sequence"] != progress["completed_data_through"] + 1:
                    raise BranchJoinProtocolError("join DATA cursor gap")
                self._apply_join_data(entry, source="DIRECT")
                self.connection.execute(
                    "UPDATE join_progress SET scanned_log_through = ?, "
                    "completed_data_through = ? WHERE input_id = ?",
                    (entry["log_sequence"], entry["data_sequence"], input_id),
                )
                return "DATA"
            if entry["cut_data_sequence"] != progress["completed_data_through"]:
                raise BranchJoinProtocolError("join barrier overtook DATA")
            intent = self.connection.execute(
                "SELECT * FROM join_checkpoint_intents WHERE checkpoint = ?",
                (entry["checkpoint"],),
            ).fetchone()
            if (
                intent is None
                or intent["status"] != "STARTED"
                or intent["intent_digest"] != entry["intent_digest"]
                or intent["coordinator_epoch"] != entry["writer_epoch"]
            ):
                raise BranchJoinProtocolError("join observed mixed barrier")
            self.connection.execute(
                "INSERT INTO join_observations VALUES (?, ?, ?, ?, ?)",
                (
                    entry["checkpoint"],
                    input_id,
                    entry["log_sequence"],
                    entry["cut_data_sequence"],
                    entry["intent_digest"],
                ),
            )
            self.connection.execute(
                "UPDATE join_progress SET scanned_log_through = ?, "
                "blocked_checkpoint = ? WHERE input_id = ?",
                (entry["log_sequence"], entry["checkpoint"], input_id),
            )
            existing_tail = self.connection.execute(
                "SELECT * FROM join_entries WHERE input_id = ? AND kind = 'DATA' "
                "AND log_sequence > ? ORDER BY arrival_ordinal",
                (input_id, entry["log_sequence"]),
            ).fetchall()
            for tail in existing_tail:
                self.connection.execute(
                    "INSERT OR IGNORE INTO join_buffer VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)",
                    (
                        entry["checkpoint"],
                        input_id,
                        tail["data_sequence"],
                        tail["log_sequence"],
                        tail["record_id"],
                        tail["arrival_ordinal"],
                        tail["payload_ref"],
                        tail["payload_digest"],
                    ),
                )
            required = set(json.loads(intent["inputs_json"]))
            observed = {
                row[0]
                for row in self.connection.execute(
                    "SELECT input_id FROM join_observations WHERE checkpoint = ?",
                    (entry["checkpoint"],),
                )
            }
            if observed == required:
                self._align_and_drain(entry["checkpoint"], required)
            return "BARRIER"

    def _align_and_drain(self, checkpoint: int, inputs: set[str]) -> None:
        intent = self.connection.execute(
            "SELECT * FROM join_checkpoint_intents WHERE checkpoint = ?",
            (checkpoint,),
        ).fetchone()
        cuts = {}
        for input_id in sorted(inputs):
            observation = self.connection.execute(
                "SELECT * FROM join_observations WHERE checkpoint = ? AND input_id = ?",
                (checkpoint, input_id),
            ).fetchone()
            progress = self.connection.execute(
                "SELECT * FROM join_progress WHERE input_id = ?", (input_id,)
            ).fetchone()
            if (
                observation is None
                or progress["blocked_checkpoint"] != checkpoint
                or progress["completed_data_through"]
                != observation["cut_data_sequence"]
            ):
                raise BranchJoinProtocolError("join vector is not aligned")
            cuts[input_id] = {
                "data": observation["cut_data_sequence"],
                "barrier_log": observation["log_sequence"],
            }
        state = self.connection.execute(
            "SELECT state_value FROM join_state"
        ).fetchone()[0]
        highwater = self.connection.execute(
            "SELECT COALESCE(MAX(apply_ordinal), 0) FROM join_turns"
        ).fetchone()[0]
        self.connection.execute(
            "INSERT INTO join_snapshots VALUES (?, ?, ?, ?, ?)",
            (checkpoint, state, highwater, canonical(cuts), intent["intent_digest"]),
        )
        self.connection.execute(
            "INSERT INTO join_outputs VALUES (?, 'BARRIER', ?, NULL, NULL, ?)",
            (
                self._next("join_outputs", "output_ordinal"),
                f"join-barrier:{checkpoint}",
                checkpoint,
            ),
        )
        self.connection.execute(
            "UPDATE join_checkpoint_intents SET status = 'ALIGNED' WHERE checkpoint = ?",
            (checkpoint,),
        )
        self.connection.execute(
            "UPDATE join_progress SET blocked_checkpoint = NULL "
            "WHERE blocked_checkpoint = ?",
            (checkpoint,),
        )
        buffered = self.connection.execute(
            "SELECT b.*, e.kind FROM join_buffer b "
            "JOIN join_entries e ON e.input_id = b.input_id "
            "AND e.log_sequence = b.log_sequence "
            "WHERE b.checkpoint = ? AND b.drained = 0 ORDER BY b.arrival_ordinal",
            (checkpoint,),
        ).fetchall()
        for row in buffered:
            apply_ordinal = self._apply_join_data(row, source="BUFFER")
            self.connection.execute(
                "UPDATE join_buffer SET drained = 1, drain_apply_ordinal = ? "
                "WHERE checkpoint = ? AND input_id = ? AND data_sequence = ?",
                (apply_ordinal, checkpoint, row["input_id"], row["data_sequence"]),
            )
            self.connection.execute(
                "UPDATE join_progress SET scanned_log_through = ?, "
                "completed_data_through = ? WHERE input_id = ?",
                (row["log_sequence"], row["data_sequence"], row["input_id"]),
            )

    def _apply_join_data(self, entry: sqlite3.Row, *, source: str) -> int:
        state = self.connection.execute(
            "SELECT state_value FROM join_state"
        ).fetchone()[0]
        new_state = apply_state(
            state, entry["input_id"], entry["data_sequence"], entry["payload_ref"]
        )
        apply_ordinal = self._next("join_turns", "apply_ordinal")
        output_id = f"join-out:{entry['input_id']}:{entry['data_sequence']}"
        self.connection.execute(
            "INSERT INTO join_turns VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                apply_ordinal,
                entry["input_id"],
                entry["data_sequence"],
                entry["log_sequence"],
                entry["payload_ref"],
                new_state,
                output_id,
                source,
            ),
        )
        self.connection.execute(
            "INSERT INTO join_outputs VALUES (?, 'DATA', ?, ?, ?, NULL)",
            (
                self._next("join_outputs", "output_ordinal"),
                output_id,
                entry["input_id"],
                entry["data_sequence"],
            ),
        )
        self.connection.execute("UPDATE join_state SET state_value = ?", (new_state,))
        return apply_ordinal

    def complete_join_checkpoint(self, checkpoint: int) -> None:
        with self._write():
            cursor = self.connection.execute(
                "UPDATE join_checkpoint_intents SET status = 'COMPLETED' "
                "WHERE checkpoint = ? AND status = 'ALIGNED'",
                (checkpoint,),
            )
            if cursor.rowcount != 1:
                row = self.connection.execute(
                    "SELECT status FROM join_checkpoint_intents WHERE checkpoint = ?",
                    (checkpoint,),
                ).fetchone()
                if row is None or row[0] != "COMPLETED":
                    raise BranchJoinProtocolError("join checkpoint is not aligned")

    def join_view(self) -> dict[str, Any]:
        return {
            "state": self.connection.execute(
                "SELECT state_value FROM join_state"
            ).fetchone()[0],
            "progress": {
                row["input_id"]: {
                    "scanned": row["scanned_log_through"],
                    "completed": row["completed_data_through"],
                    "blocked": row["blocked_checkpoint"],
                }
                for row in self.connection.execute(
                    "SELECT * FROM join_progress ORDER BY input_id"
                )
            },
            "buffer": [
                dict(row)
                for row in self.connection.execute(
                    "SELECT * FROM join_buffer ORDER BY arrival_ordinal"
                )
            ],
            "outputs": [
                dict(row)
                for row in self.connection.execute(
                    "SELECT * FROM join_outputs ORDER BY output_ordinal"
                )
            ],
        }

    def join_snapshot(self, checkpoint: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM join_snapshots WHERE checkpoint = ?", (checkpoint,)
        ).fetchone()
        if row is None:
            raise BranchJoinProtocolError("join snapshot missing")
        return {
            "state": row["state_value"],
            "mailbox_highwater": row["mailbox_highwater"],
            "cuts": json.loads(row["cuts_json"]),
        }

    def _branch(self, branch_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM branch_progress WHERE branch_id = ?", (branch_id,)
        ).fetchone()
        if row is None:
            raise BranchJoinProtocolError("unknown branch")
        return row

    def _branch_owner(
        self, branch_id: str, owner_id: str, owner_epoch: int
    ) -> sqlite3.Row:
        row = self._branch(branch_id)
        if (row["owner_id"], row["owner_epoch"]) != (owner_id, owner_epoch):
            raise BranchJoinProtocolError("stale branch owner")
        return row

    def _join_input(self, input_id: str) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM join_inputs WHERE input_id = ?", (input_id,)
        ).fetchone()
        if row is None:
            raise BranchJoinProtocolError("unknown join input")
        return row

    def _next(self, table: str, column: str) -> int:
        return int(
            self.connection.execute(
                f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table}"
            ).fetchone()[0]
        )

    def _next_for_input(self, table: str, input_id: str, column: str) -> int:
        return int(
            self.connection.execute(
                f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table} "
                "WHERE input_id = ? AND kind = 'DATA'"
                if column == "data_sequence"
                else f"SELECT COALESCE(MAX({column}), 0) + 1 FROM {table} "
                "WHERE input_id = ?",
                (input_id,),
            ).fetchone()[0]
        )
