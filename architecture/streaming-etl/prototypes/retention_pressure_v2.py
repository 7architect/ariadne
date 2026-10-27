"""DS-005c/d model: reference-safe compaction and atomic capacity credits."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator


class RetentionProtocolError(RuntimeError):
    pass


class CapacityBlocked(RetentionProtocolError):
    def __init__(self, view: dict[str, int]):
        super().__init__("durable channel capacity exhausted")
        self.view = view


FaultHook = Callable[[str], None]


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


@dataclass(frozen=True)
class AppendResult:
    sequence: int
    record_id: str
    object_key: str


class RetentionPressureStore:
    def __init__(
        self, path: Path, *, max_records: int = 100, max_bytes: int = 1_000_000
    ):
        if max_records < 1 or max_bytes < 1:
            raise RetentionProtocolError("capacity must be positive")
        self.connection = sqlite3.connect(path, timeout=10)
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            CREATE TABLE IF NOT EXISTS channel_config (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                max_records INTEGER NOT NULL CHECK (max_records > 0),
                max_bytes INTEGER NOT NULL CHECK (max_bytes > 0),
                overload_policy TEXT NOT NULL CHECK (overload_policy = 'BLOCK')
            );
            CREATE TABLE IF NOT EXISTS partition_state (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                next_sequence INTEGER NOT NULL CHECK (next_sequence > 0)
            );
            CREATE TABLE IF NOT EXISTS objects (
                object_key TEXT PRIMARY KEY,
                payload_digest TEXT NOT NULL,
                payload_size INTEGER NOT NULL CHECK (payload_size >= 0),
                schema_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS records (
                sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
                record_id TEXT NOT NULL UNIQUE,
                object_key TEXT NOT NULL REFERENCES objects(object_key),
                payload_digest TEXT NOT NULL,
                payload_size INTEGER NOT NULL,
                lineage_digest TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS consumers (
                consumer_id TEXT PRIMARY KEY,
                completed_through INTEGER NOT NULL DEFAULT 0,
                in_flight_sequence INTEGER
            );
            CREATE TABLE IF NOT EXISTS consumer_manifest (
                consumer_id TEXT PRIMARY KEY,
                start_sequence INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS consumer_completions (
                consumer_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                witness_id TEXT NOT NULL UNIQUE,
                PRIMARY KEY (consumer_id, sequence),
                FOREIGN KEY (consumer_id) REFERENCES consumer_manifest(consumer_id)
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                checkpoint INTEGER PRIMARY KEY,
                cut_sequence INTEGER NOT NULL,
                intent_digest TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK (status IN ('SEALED', 'RELEASED'))
            );
            CREATE TABLE IF NOT EXISTS durable_refs (
                ref_kind TEXT NOT NULL,
                ref_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED')),
                PRIMARY KEY (ref_kind, ref_id)
            );
            CREATE TABLE IF NOT EXISTS root_lifecycle_events (
                event_id TEXT PRIMARY KEY,
                root_kind TEXT NOT NULL,
                root_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                transition TEXT NOT NULL CHECK (transition IN ('ACTIVE', 'RELEASED')),
                identity_digest TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS compaction_manifests (
                manifest_id INTEGER PRIMARY KEY,
                requested_through INTEGER NOT NULL,
                safe_through INTEGER NOT NULL,
                roots_json TEXT NOT NULL,
                records_json TEXT NOT NULL,
                objects_json TEXT NOT NULL,
                manifest_digest TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK (status IN ('PLANNED', 'COMPLETED'))
            );
            CREATE TABLE IF NOT EXISTS compaction_tombstones (
                sequence INTEGER PRIMARY KEY,
                manifest_id INTEGER NOT NULL REFERENCES compaction_manifests(manifest_id),
                record_id TEXT NOT NULL UNIQUE,
                object_key TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                payload_size INTEGER NOT NULL,
                schema_id TEXT NOT NULL,
                lineage_digest TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS append_attempts (
                attempt_id TEXT PRIMARY KEY,
                record_id TEXT NOT NULL,
                object_key TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                payload_size INTEGER NOT NULL,
                schema_id TEXT NOT NULL,
                lineage_digest TEXT NOT NULL,
                identity_digest TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK (status IN ('BLOCKED', 'APPENDED')),
                sequence INTEGER,
                CHECK (
                    (status = 'BLOCKED' AND sequence IS NULL)
                    OR (status = 'APPENDED' AND sequence IS NOT NULL)
                )
            );

            CREATE TRIGGER IF NOT EXISTS record_immutable
            BEFORE UPDATE ON records BEGIN
                SELECT RAISE(ABORT, 'record is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS record_create_once
            BEFORE INSERT ON records
            WHEN EXISTS (
                SELECT 1 FROM records
                WHERE sequence = NEW.sequence OR record_id = NEW.record_id
            ) BEGIN
                SELECT RAISE(ABORT, 'record identity already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS object_immutable
            BEFORE UPDATE ON objects BEGIN
                SELECT RAISE(ABORT, 'object is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS object_create_once
            BEFORE INSERT ON objects
            WHEN EXISTS (SELECT 1 FROM objects WHERE object_key = NEW.object_key)
            BEGIN
                SELECT RAISE(ABORT, 'object identity already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS manifest_identity_immutable
            BEFORE UPDATE OF requested_through, safe_through, roots_json,
                records_json, objects_json, manifest_digest
            ON compaction_manifests BEGIN
                SELECT RAISE(ABORT, 'compaction manifest identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS manifest_no_delete
            BEFORE DELETE ON compaction_manifests BEGIN
                SELECT RAISE(ABORT, 'compaction manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS manifest_create_once
            BEFORE INSERT ON compaction_manifests
            WHEN EXISTS (
                SELECT 1 FROM compaction_manifests
                WHERE manifest_id = NEW.manifest_id OR manifest_digest = NEW.manifest_digest
            ) BEGIN
                SELECT RAISE(ABORT, 'compaction manifest already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS manifest_status_forward
            BEFORE UPDATE OF status ON compaction_manifests
            WHEN NOT (OLD.status = 'PLANNED' AND NEW.status = 'COMPLETED') BEGIN
                SELECT RAISE(ABORT, 'illegal compaction manifest transition');
            END;
            CREATE TRIGGER IF NOT EXISTS tombstone_immutable
            BEFORE UPDATE ON compaction_tombstones BEGIN
                SELECT RAISE(ABORT, 'compaction tombstone is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS tombstone_no_delete
            BEFORE DELETE ON compaction_tombstones BEGIN
                SELECT RAISE(ABORT, 'compaction tombstone is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS tombstone_create_once
            BEFORE INSERT ON compaction_tombstones
            WHEN EXISTS (SELECT 1 FROM compaction_tombstones WHERE sequence = NEW.sequence)
            BEGIN
                SELECT RAISE(ABORT, 'compaction tombstone already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS consumer_manifest_no_delete
            BEFORE DELETE ON consumer_manifest BEGIN
                SELECT RAISE(ABORT, 'consumer manifest is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS consumer_manifest_create_once
            BEFORE INSERT ON consumer_manifest
            WHEN EXISTS (
                SELECT 1 FROM consumer_manifest WHERE consumer_id = NEW.consumer_id
            ) BEGIN
                SELECT RAISE(ABORT, 'consumer manifest already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS consumer_create_once
            BEFORE INSERT ON consumers
            WHEN EXISTS (SELECT 1 FROM consumers WHERE consumer_id = NEW.consumer_id)
            BEGIN
                SELECT RAISE(ABORT, 'consumer authority already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS consumer_no_delete
            BEFORE DELETE ON consumers BEGIN
                SELECT RAISE(ABORT, 'consumer authority is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS consumer_progress_authority
            BEFORE UPDATE OF completed_through ON consumers
            WHEN NEW.completed_through != OLD.completed_through BEGIN
                SELECT CASE WHEN NEW.completed_through != OLD.completed_through + 1
                    OR OLD.in_flight_sequence != NEW.completed_through
                    OR NEW.in_flight_sequence IS NOT NULL
                    OR NOT EXISTS (
                        SELECT 1 FROM consumer_completions c
                        WHERE c.consumer_id = OLD.consumer_id
                          AND c.sequence = NEW.completed_through
                    )
                    THEN RAISE(ABORT, 'consumer progress lacks exact completion') END;
            END;
            CREATE TRIGGER IF NOT EXISTS completion_immutable
            BEFORE UPDATE ON consumer_completions BEGIN
                SELECT RAISE(ABORT, 'consumer completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS completion_no_delete
            BEFORE DELETE ON consumer_completions BEGIN
                SELECT RAISE(ABORT, 'consumer completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_identity_immutable
            BEFORE UPDATE OF cut_sequence, intent_digest ON checkpoints BEGIN
                SELECT RAISE(ABORT, 'checkpoint identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_status_forward
            BEFORE UPDATE OF status ON checkpoints
            WHEN NOT (OLD.status = 'SEALED' AND NEW.status = 'RELEASED') BEGIN
                SELECT RAISE(ABORT, 'illegal checkpoint transition');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_no_delete
            BEFORE DELETE ON checkpoints BEGIN
                SELECT RAISE(ABORT, 'checkpoint is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS checkpoint_create_once
            BEFORE INSERT ON checkpoints
            WHEN EXISTS (SELECT 1 FROM checkpoints WHERE checkpoint = NEW.checkpoint)
            BEGIN
                SELECT RAISE(ABORT, 'checkpoint already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS durable_ref_identity_immutable
            BEFORE UPDATE OF ref_kind, ref_id, sequence ON durable_refs BEGIN
                SELECT RAISE(ABORT, 'durable reference identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS durable_ref_status_forward
            BEFORE UPDATE OF status ON durable_refs
            WHEN NOT (OLD.status = 'ACTIVE' AND NEW.status = 'RELEASED') BEGIN
                SELECT RAISE(ABORT, 'illegal reference transition');
            END;
            CREATE TRIGGER IF NOT EXISTS durable_ref_no_delete
            BEFORE DELETE ON durable_refs BEGIN
                SELECT RAISE(ABORT, 'durable reference is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS durable_ref_create_once
            BEFORE INSERT ON durable_refs
            WHEN EXISTS (
                SELECT 1 FROM durable_refs
                WHERE ref_kind = NEW.ref_kind AND ref_id = NEW.ref_id
            ) BEGIN
                SELECT RAISE(ABORT, 'durable reference already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS root_event_immutable
            BEFORE UPDATE ON root_lifecycle_events BEGIN
                SELECT RAISE(ABORT, 'root lifecycle event is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS root_event_no_delete
            BEFORE DELETE ON root_lifecycle_events BEGIN
                SELECT RAISE(ABORT, 'root lifecycle event is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS root_event_create_once
            BEFORE INSERT ON root_lifecycle_events
            WHEN EXISTS (
                SELECT 1 FROM root_lifecycle_events
                WHERE event_id = NEW.event_id OR identity_digest = NEW.identity_digest
            ) BEGIN
                SELECT RAISE(ABORT, 'root lifecycle event already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS append_attempt_identity_immutable
            BEFORE UPDATE OF record_id, object_key, payload_digest, payload_size,
                schema_id, lineage_digest, identity_digest
            ON append_attempts BEGIN
                SELECT RAISE(ABORT, 'append attempt identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS append_attempt_status_forward
            BEFORE UPDATE OF status, sequence ON append_attempts
            WHEN NOT (
                OLD.status = 'BLOCKED' AND OLD.sequence IS NULL
                AND NEW.status = 'APPENDED' AND NEW.sequence IS NOT NULL
            ) BEGIN
                SELECT RAISE(ABORT, 'illegal append attempt transition');
            END;
            CREATE TRIGGER IF NOT EXISTS append_attempt_no_delete
            BEFORE DELETE ON append_attempts BEGIN
                SELECT RAISE(ABORT, 'append attempt is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS append_attempt_create_once
            BEFORE INSERT ON append_attempts
            WHEN EXISTS (SELECT 1 FROM append_attempts WHERE attempt_id = NEW.attempt_id)
            BEGIN
                SELECT RAISE(ABORT, 'append attempt already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS record_delete_guard
            BEFORE DELETE ON records BEGIN
                SELECT CASE WHEN NOT EXISTS (
                    SELECT 1 FROM compaction_manifests m, json_each(m.records_json) j
                    WHERE m.status = 'PLANNED' AND j.value = OLD.sequence
                ) THEN RAISE(ABORT, 'record deletion lacks compaction manifest') END;
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM consumers c
                    WHERE c.completed_through + 1 <= OLD.sequence
                       OR c.in_flight_sequence = OLD.sequence
                ) OR EXISTS (
                    SELECT 1 FROM checkpoints k
                    WHERE k.status = 'SEALED' AND k.cut_sequence + 1 <= OLD.sequence
                ) OR EXISTS (
                    SELECT 1 FROM durable_refs d
                    WHERE d.status = 'ACTIVE' AND d.sequence <= OLD.sequence
                ) THEN RAISE(ABORT, 'record still belongs to recovery suffix') END;
            END;
            CREATE TRIGGER IF NOT EXISTS object_delete_guard
            BEFORE DELETE ON objects BEGIN
                SELECT CASE WHEN EXISTS (
                    SELECT 1 FROM records r WHERE r.object_key = OLD.object_key
                ) THEN RAISE(ABORT, 'object still referenced') END;
                SELECT CASE WHEN NOT EXISTS (
                    SELECT 1 FROM compaction_manifests m, json_each(m.objects_json) j
                    WHERE m.status = 'PLANNED' AND j.value = OLD.object_key
                ) THEN RAISE(ABORT, 'object deletion lacks compaction manifest') END;
            END;
            """
        )
        with self._write():
            config = self.connection.execute("SELECT * FROM channel_config").fetchone()
            expected = (max_records, max_bytes, "BLOCK")
            if config is None:
                self.connection.execute(
                    "INSERT INTO channel_config VALUES (1, ?, ?, 'BLOCK')",
                    (max_records, max_bytes),
                )
                self.connection.execute("INSERT INTO partition_state VALUES (1, 1)")
            elif tuple(config)[1:] != expected:
                raise RetentionProtocolError("capacity configuration drift")

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

    def capacity(self) -> dict[str, int]:
        config = self.connection.execute("SELECT * FROM channel_config").fetchone()
        used_records = self.connection.execute(
            "SELECT COUNT(*) FROM records"
        ).fetchone()[0]
        used_bytes = self.connection.execute(
            "SELECT COALESCE(SUM(payload_size), 0) FROM objects "
            "WHERE object_key IN (SELECT DISTINCT object_key FROM records)"
        ).fetchone()[0]
        return {
            "max_records": config["max_records"],
            "max_bytes": config["max_bytes"],
            "used_records": used_records,
            "used_bytes": used_bytes,
            "record_credits": config["max_records"] - used_records,
            "byte_credits": config["max_bytes"] - used_bytes,
        }

    def retained_floor(self) -> int:
        """First sequence for which a new recovery root can still be created."""
        row = self.connection.execute("SELECT MIN(sequence) FROM records").fetchone()
        if row[0] is not None:
            return int(row[0])
        return int(
            self.connection.execute(
                "SELECT next_sequence FROM partition_state WHERE singleton = 1"
            ).fetchone()[0]
        )

    def _root_event(
        self, root_kind: str, root_id: str, sequence: int, transition: str
    ) -> None:
        event_id = f"{root_kind}:{root_id}:{transition}"
        identity = {
            "event_id": event_id,
            "root_kind": root_kind,
            "root_id": root_id,
            "sequence": sequence,
            "transition": transition,
        }
        self.connection.execute(
            "INSERT INTO root_lifecycle_events VALUES (?, ?, ?, ?, ?, ?)",
            (event_id, root_kind, root_id, sequence, transition, digest(identity)),
        )

    @staticmethod
    def _witness_identity(row: sqlite3.Row, schema_id: str) -> tuple[Any, ...]:
        return (
            row["record_id"],
            row["object_key"],
            row["payload_digest"],
            row["payload_size"],
            schema_id,
            row["lineage_digest"],
        )

    def append(
        self,
        attempt_id: str,
        record_id: str,
        object_key: str,
        payload: bytes,
        lineage_digest: str,
        *,
        schema_id: str = "bytes/v1",
        fault: FaultHook | None = None,
    ) -> AppendResult:
        payload_digest = hashlib.sha256(payload).hexdigest()
        identity = {
            "attempt_id": attempt_id,
            "record_id": record_id,
            "object_key": object_key,
            "payload_digest": payload_digest,
            "payload_size": len(payload),
            "schema_id": schema_id,
            "lineage_digest": lineage_digest,
        }
        identity_digest = digest(identity)
        blocked_view: dict[str, int] | None = None
        result: AppendResult | None = None
        with self._write():
            attempt = self.connection.execute(
                "SELECT * FROM append_attempts WHERE attempt_id = ?", (attempt_id,)
            ).fetchone()
            expected_attempt = (
                record_id,
                object_key,
                payload_digest,
                len(payload),
                schema_id,
                lineage_digest,
                identity_digest,
            )
            if attempt is not None:
                if (
                    attempt["record_id"],
                    attempt["object_key"],
                    attempt["payload_digest"],
                    attempt["payload_size"],
                    attempt["schema_id"],
                    attempt["lineage_digest"],
                    attempt["identity_digest"],
                ) != expected_attempt:
                    raise RetentionProtocolError("append attempt identity drift")
                if attempt["status"] == "APPENDED":
                    record = self.connection.execute(
                        "SELECT * FROM records WHERE sequence = ?",
                        (attempt["sequence"],),
                    ).fetchone()
                    if record is not None:
                        obj = self.connection.execute(
                            "SELECT schema_id FROM objects WHERE object_key = ?",
                            (record["object_key"],),
                        ).fetchone()
                        witness = (
                            None
                            if obj is None
                            else self._witness_identity(record, obj["schema_id"])
                        )
                    else:
                        tombstone = self.connection.execute(
                            "SELECT * FROM compaction_tombstones WHERE sequence = ?",
                            (attempt["sequence"],),
                        ).fetchone()
                        witness = (
                            None
                            if tombstone is None
                            else self._witness_identity(
                                tombstone, tombstone["schema_id"]
                            )
                        )
                    if witness != expected_attempt[:-1]:
                        raise RetentionProtocolError("append result witness missing")
                    return AppendResult(attempt["sequence"], record_id, object_key)
            else:
                if self.connection.execute(
                    "SELECT 1 FROM records WHERE record_id = ? UNION ALL "
                    "SELECT 1 FROM compaction_tombstones WHERE record_id = ?",
                    (record_id, record_id),
                ).fetchone():
                    raise RetentionProtocolError("record exists without append attempt")
                self.connection.execute(
                    "INSERT INTO append_attempts VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, 'BLOCKED', NULL)",
                    (
                        attempt_id,
                        record_id,
                        object_key,
                        payload_digest,
                        len(payload),
                        schema_id,
                        lineage_digest,
                        identity_digest,
                    ),
                )
                if fault is not None:
                    fault("append:after_attempt_before_capacity")
            obj = self.connection.execute(
                "SELECT * FROM objects WHERE object_key = ?", (object_key,)
            ).fetchone()
            if obj is not None and tuple(obj)[1:] != (
                payload_digest,
                len(payload),
                schema_id,
            ):
                raise RetentionProtocolError("object identity drift")
            view = self.capacity()
            added_bytes = 0 if obj is not None else len(payload)
            if view["used_records"] + 1 > view["max_records"] or (
                view["used_bytes"] + added_bytes > view["max_bytes"]
            ):
                blocked_view = view
            else:
                if fault is not None:
                    fault("append:after_reservation_before_record")
                if obj is None:
                    self.connection.execute(
                        "INSERT INTO objects VALUES (?, ?, ?, ?)",
                        (object_key, payload_digest, len(payload), schema_id),
                    )
                sequence = self.connection.execute(
                    "SELECT next_sequence FROM partition_state"
                ).fetchone()[0]
                self.connection.execute(
                    "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        sequence,
                        record_id,
                        object_key,
                        payload_digest,
                        len(payload),
                        lineage_digest,
                    ),
                )
                self.connection.execute(
                    "UPDATE partition_state SET next_sequence = next_sequence + 1"
                )
                self.connection.execute(
                    "UPDATE append_attempts SET status = 'APPENDED', sequence = ? "
                    "WHERE attempt_id = ? AND status = 'BLOCKED'",
                    (sequence, attempt_id),
                )
                if fault is not None:
                    fault("append:after_record_before_commit")
                result = AppendResult(sequence, record_id, object_key)
        if blocked_view is not None:
            if fault is not None:
                fault("append:blocked_after_commit_before_ack")
            raise CapacityBlocked(blocked_view)
        if result is None:
            raise AssertionError("append produced neither result nor block")
        if fault is not None:
            fault("append:after_commit_before_ack")
        return result

    def create_consumer(
        self, consumer_id: str, start_sequence: int | None = None
    ) -> None:
        with self._write():
            exists = self.connection.execute(
                "SELECT start_sequence FROM consumer_manifest WHERE consumer_id = ?",
                (consumer_id,),
            ).fetchone()
            if exists:
                if (
                    start_sequence is not None
                    and exists["start_sequence"] != start_sequence
                ):
                    raise RetentionProtocolError("consumer start identity drift")
                return
            floor = self.retained_floor()
            tail_next = self.connection.execute(
                "SELECT next_sequence FROM partition_state WHERE singleton = 1"
            ).fetchone()[0]
            start = floor if start_sequence is None else start_sequence
            if start < floor or start > tail_next:
                raise RetentionProtocolError("consumer start outside retained suffix")
            self.connection.execute(
                "INSERT INTO consumer_manifest VALUES (?, ?)", (consumer_id, start)
            )
            self.connection.execute(
                "INSERT INTO consumers VALUES (?, ?, NULL)", (consumer_id, start - 1)
            )
            self._root_event("CONSUMER", consumer_id, start, "ACTIVE")

    def deliver_next(self, consumer_id: str) -> int | None:
        with self._write():
            row = self.connection.execute(
                "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
            ).fetchone()
            if row is None:
                raise RetentionProtocolError("unknown consumer")
            if row["in_flight_sequence"] is not None:
                return int(row["in_flight_sequence"])
            sequence = row["completed_through"] + 1
            if not self.connection.execute(
                "SELECT 1 FROM records WHERE sequence = ?", (sequence,)
            ).fetchone():
                return None
            self.connection.execute(
                "UPDATE consumers SET in_flight_sequence = ? WHERE consumer_id = ?",
                (sequence, consumer_id),
            )
            return sequence

    def complete_consumer(self, consumer_id: str, sequence: int) -> None:
        with self._write():
            row = self.connection.execute(
                "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
            ).fetchone()
            if row is None or row["in_flight_sequence"] != sequence:
                raise RetentionProtocolError(
                    "completion lacks exact in-flight delivery"
                )
            witness = digest({"consumer": consumer_id, "sequence": sequence})
            self.connection.execute(
                "INSERT INTO consumer_completions VALUES (?, ?, ?)",
                (consumer_id, sequence, witness),
            )
            self.connection.execute(
                "UPDATE consumers SET completed_through = ?, in_flight_sequence = NULL "
                "WHERE consumer_id = ?",
                (sequence, consumer_id),
            )

    def complete_through(self, consumer_id: str, through: int) -> None:
        while True:
            current = self.connection.execute(
                "SELECT completed_through FROM consumers WHERE consumer_id = ?",
                (consumer_id,),
            ).fetchone()[0]
            if current >= through:
                return
            sequence = self.deliver_next(consumer_id)
            if sequence is None:
                raise RetentionProtocolError("consumer completion exceeds durable tail")
            self.complete_consumer(consumer_id, sequence)

    def seal_checkpoint(self, checkpoint: int, cut: int) -> None:
        intent = digest({"checkpoint": checkpoint, "cut": cut})
        with self._write():
            existing = self.connection.execute(
                "SELECT * FROM checkpoints WHERE checkpoint = ?", (checkpoint,)
            ).fetchone()
            if existing is None:
                root = cut + 1
                tail_next = self.connection.execute(
                    "SELECT next_sequence FROM partition_state WHERE singleton = 1"
                ).fetchone()[0]
                if root < self.retained_floor() or root > tail_next:
                    raise RetentionProtocolError("checkpoint outside retained suffix")
                self.connection.execute(
                    "INSERT INTO checkpoints VALUES (?, ?, ?, 'SEALED')",
                    (checkpoint, cut, intent),
                )
                self._root_event("CHECKPOINT", str(checkpoint), root, "ACTIVE")
            elif (existing["cut_sequence"], existing["intent_digest"]) != (cut, intent):
                raise RetentionProtocolError("checkpoint identity drift")

    def release_checkpoint(self, checkpoint: int) -> None:
        with self._write():
            cursor = self.connection.execute(
                "UPDATE checkpoints SET status = 'RELEASED' "
                "WHERE checkpoint = ? AND status = 'SEALED'",
                (checkpoint,),
            )
            if cursor.rowcount != 1:
                raise RetentionProtocolError("checkpoint is not sealed")
            row = self.connection.execute(
                "SELECT cut_sequence FROM checkpoints WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            self._root_event(
                "CHECKPOINT", str(checkpoint), row["cut_sequence"] + 1, "RELEASED"
            )

    def add_ref(self, kind: str, ref_id: str, sequence: int) -> None:
        if kind not in {"JOIN_BUFFER", "BRANCH_OUTPUT", "COMPLETION", "RECOVERY_PIN"}:
            raise RetentionProtocolError("unsupported durable reference")
        with self._write():
            tail_next = self.connection.execute(
                "SELECT next_sequence FROM partition_state WHERE singleton = 1"
            ).fetchone()[0]
            if sequence < self.retained_floor() or sequence >= tail_next:
                raise RetentionProtocolError("reference outside retained suffix")
            existing = self.connection.execute(
                "SELECT * FROM durable_refs WHERE ref_kind = ? AND ref_id = ?",
                (kind, ref_id),
            ).fetchone()
            if existing is not None:
                if existing["sequence"] != sequence:
                    raise RetentionProtocolError("reference identity drift")
                if existing["status"] != "ACTIVE":
                    raise RetentionProtocolError("reference already released")
                return
            self.connection.execute(
                "INSERT INTO durable_refs VALUES (?, ?, ?, 'ACTIVE')",
                (kind, ref_id, sequence),
            )
            self._root_event(kind, ref_id, sequence, "ACTIVE")

    def remove_ref(self, kind: str, ref_id: str) -> None:
        with self._write():
            cursor = self.connection.execute(
                "UPDATE durable_refs SET status = 'RELEASED' "
                "WHERE ref_kind = ? AND ref_id = ? AND status = 'ACTIVE'",
                (kind, ref_id),
            )
            if cursor.rowcount != 1:
                raise RetentionProtocolError("reference is not active")
            row = self.connection.execute(
                "SELECT sequence FROM durable_refs WHERE ref_kind = ? AND ref_id = ?",
                (kind, ref_id),
            ).fetchone()
            self._root_event(kind, ref_id, row["sequence"], "RELEASED")

    def roots(self) -> list[dict[str, Any]]:
        roots: list[dict[str, Any]] = []
        for row in self.connection.execute(
            "SELECT * FROM consumers ORDER BY consumer_id"
        ):
            roots.append(
                {
                    "kind": "CONSUMER",
                    "id": row["consumer_id"],
                    "sequence": row["completed_through"] + 1,
                }
            )
            if row["in_flight_sequence"] is not None:
                roots.append(
                    {
                        "kind": "IN_FLIGHT",
                        "id": row["consumer_id"],
                        "sequence": row["in_flight_sequence"],
                    }
                )
        for row in self.connection.execute(
            "SELECT * FROM checkpoints WHERE status = 'SEALED' ORDER BY checkpoint"
        ):
            roots.append(
                {
                    "kind": "CHECKPOINT",
                    "id": str(row["checkpoint"]),
                    "sequence": row["cut_sequence"] + 1,
                }
            )
        for row in self.connection.execute(
            "SELECT * FROM durable_refs WHERE status = 'ACTIVE' ORDER BY ref_kind, ref_id"
        ):
            roots.append(
                {
                    "kind": row["ref_kind"],
                    "id": row["ref_id"],
                    "sequence": row["sequence"],
                }
            )
        return roots

    def compact(
        self, requested_through: int, *, fault: FaultHook | None = None
    ) -> dict[str, Any]:
        result: dict[str, Any]
        with self._write():
            roots = self.roots()
            safe = min(
                requested_through,
                min((r["sequence"] - 1 for r in roots), default=requested_through),
            )
            records = [
                row[0]
                for row in self.connection.execute(
                    "SELECT sequence FROM records WHERE sequence <= ? ORDER BY sequence",
                    (safe,),
                )
            ]
            placeholders = ",".join("?" for _ in records)
            if records:
                objects = [
                    row[0]
                    for row in self.connection.execute(
                        f"SELECT object_key FROM objects WHERE object_key IN ("
                        f"SELECT object_key FROM records WHERE sequence IN ({placeholders})) "
                        f"AND object_key NOT IN (SELECT object_key FROM records WHERE sequence NOT IN ({placeholders})) "
                        "ORDER BY object_key",
                        (*records, *records),
                    )
                ]
            else:
                objects = []
            value = {
                "requested_through": requested_through,
                "safe_through": safe,
                "roots": roots,
                "records": records,
                "objects": objects,
            }
            manifest_id = self.connection.execute(
                "SELECT COALESCE(MAX(manifest_id), 0) + 1 FROM compaction_manifests"
            ).fetchone()[0]
            self.connection.execute(
                "INSERT INTO compaction_manifests VALUES (?, ?, ?, ?, ?, ?, ?, 'PLANNED')",
                (
                    manifest_id,
                    requested_through,
                    safe,
                    canonical(roots),
                    canonical(records),
                    canonical(objects),
                    digest(value),
                ),
            )
            if fault is not None:
                fault("compaction:after_manifest_before_delete")
            for sequence in records:
                row = self.connection.execute(
                    "SELECT * FROM records WHERE sequence = ?", (sequence,)
                ).fetchone()
                obj = self.connection.execute(
                    "SELECT schema_id FROM objects WHERE object_key = ?",
                    (row["object_key"],),
                ).fetchone()
                self.connection.execute(
                    "INSERT INTO compaction_tombstones VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        row["sequence"],
                        manifest_id,
                        row["record_id"],
                        row["object_key"],
                        row["payload_digest"],
                        row["payload_size"],
                        obj["schema_id"],
                        row["lineage_digest"],
                    ),
                )
                self.connection.execute(
                    "DELETE FROM records WHERE sequence = ?", (sequence,)
                )
            if fault is not None:
                fault("compaction:after_records_before_objects")
            for object_key in objects:
                self.connection.execute(
                    "DELETE FROM objects WHERE object_key = ?", (object_key,)
                )
            self.connection.execute(
                "UPDATE compaction_manifests SET status = 'COMPLETED' WHERE manifest_id = ?",
                (manifest_id,),
            )
            if fault is not None:
                fault("compaction:after_complete_before_commit")
            result = {"manifest_id": manifest_id, **value}
        if fault is not None:
            fault("compaction:after_commit_before_ack")
        return result
