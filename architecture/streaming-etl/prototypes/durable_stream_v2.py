"""Protocol epoch 2: durable yielded channels and per-consumer progress.

This is an executable protocol model, not a production storage choice.  It
models one in-flight DATA record per consumer partition.  Client code owns the
determinism and idempotency of effects outside this store.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator


class StreamProtocolError(RuntimeError):
    pass


FaultHook = Callable[[str], None]


def _hit(fault: FaultHook | None, point: str) -> None:
    if fault is not None:
        fault(point)


class CompletionKind(str, Enum):
    YIELD = "YIELD"
    RETURN = "RETURN"


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass(frozen=True)
class ObjectRef:
    key: str
    digest: str
    size: int
    schema_id: str

    @classmethod
    def for_payload(cls, key: str, payload: bytes, schema_id: str) -> ObjectRef:
        return cls(key, hashlib.sha256(payload).hexdigest(), len(payload), schema_id)


@dataclass(frozen=True)
class DataEnvelope:
    channel_id: str
    partition_id: str
    sequence: int
    record_id: str
    payload_ref: ObjectRef
    lineage_digest: str
    producer_epoch: int


@dataclass(frozen=True)
class DeliveryToken:
    consumer_id: str
    channel_id: str
    partition_id: str
    sequence: int
    attempt: int
    owner_id: str
    owner_epoch: int


@dataclass(frozen=True)
class PendingOutput:
    channel_id: str
    partition_id: str
    record_id: str
    payload_ref: ObjectRef
    lineage_digest: str


class DurableStream:
    """SQLite safety model for the platform-owned stream data plane."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.row_factory = sqlite3.Row
        self.connection.create_function(
            "canonical_digest",
            1,
            lambda value: _digest(json.loads(value)),
            deterministic=True,
        )
        self.connection.executescript(
            """
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS clock (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                logical_time INTEGER NOT NULL CHECK (logical_time >= 0)
            );
            INSERT OR IGNORE INTO clock VALUES (1, 0);

            CREATE TABLE IF NOT EXISTS objects (
                object_key TEXT PRIMARY KEY,
                digest TEXT NOT NULL,
                size INTEGER NOT NULL CHECK (size >= 0),
                schema_id TEXT NOT NULL,
                durable INTEGER NOT NULL CHECK (durable IN (0, 1))
            );

            CREATE TABLE IF NOT EXISTS producer_owners (
                channel_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL CHECK (owner_epoch > 0),
                PRIMARY KEY (channel_id, partition_id)
            );

            CREATE TABLE IF NOT EXISTS records (
                channel_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                sequence INTEGER NOT NULL CHECK (sequence > 0),
                record_id TEXT NOT NULL,
                payload_key TEXT NOT NULL REFERENCES objects(object_key),
                payload_digest TEXT NOT NULL,
                payload_size INTEGER NOT NULL,
                schema_id TEXT NOT NULL,
                lineage_digest TEXT NOT NULL,
                producer_epoch INTEGER NOT NULL,
                PRIMARY KEY (channel_id, partition_id, sequence),
                UNIQUE (channel_id, partition_id, record_id)
            );

            CREATE TABLE IF NOT EXISTS consumers (
                consumer_id TEXT PRIMARY KEY,
                channel_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                completed_through INTEGER NOT NULL DEFAULT 0,
                in_flight_sequence INTEGER,
                in_flight_attempt INTEGER NOT NULL DEFAULT 0,
                owner_id TEXT,
                owner_epoch INTEGER NOT NULL DEFAULT 0,
                lease_expires_at INTEGER,
                state_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS turn_completions (
                consumer_id TEXT NOT NULL,
                input_channel_id TEXT NOT NULL,
                input_partition_id TEXT NOT NULL,
                input_sequence INTEGER NOT NULL,
                input_record_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL,
                kind TEXT NOT NULL CHECK (kind IN ('YIELD', 'RETURN')),
                output_channel_id TEXT,
                output_partition_id TEXT,
                output_sequence INTEGER,
                output_record_id TEXT,
                output_manifest_json TEXT NOT NULL,
                output_manifest_digest TEXT NOT NULL,
                state_json TEXT NOT NULL,
                state_digest TEXT NOT NULL,
                PRIMARY KEY (consumer_id, input_sequence),
                UNIQUE (output_channel_id, output_partition_id, output_sequence),
                FOREIGN KEY (input_channel_id, input_partition_id, input_sequence)
                    REFERENCES records(channel_id, partition_id, sequence),
                FOREIGN KEY (output_channel_id, output_partition_id, output_sequence)
                    REFERENCES records(channel_id, partition_id, sequence),
                CHECK (
                    (kind = 'RETURN' AND output_channel_id IS NULL
                     AND output_partition_id IS NULL AND output_sequence IS NULL
                     AND output_record_id IS NULL)
                    OR
                    (kind = 'YIELD' AND output_channel_id IS NOT NULL
                     AND output_partition_id IS NOT NULL AND output_sequence IS NOT NULL
                     AND output_record_id IS NOT NULL)
                )
            );

            CREATE TABLE IF NOT EXISTS runtime_invocations (
                consumer_id TEXT PRIMARY KEY REFERENCES consumers(consumer_id),
                input_sequence INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                owner_id TEXT NOT NULL,
                owner_epoch INTEGER NOT NULL,
                holder_id TEXT NOT NULL UNIQUE,
                expires_at INTEGER NOT NULL
            );

            CREATE TRIGGER IF NOT EXISTS records_immutable
            BEFORE UPDATE ON records BEGIN
                SELECT RAISE(ABORT, 'stream record is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS records_delete_requires_retention
            BEFORE DELETE ON records BEGIN
                SELECT RAISE(ABORT, 'stream record deletion requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS objects_immutable
            BEFORE UPDATE ON objects BEGIN
                SELECT RAISE(ABORT, 'object witness is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS objects_delete_requires_retention
            BEFORE DELETE ON objects BEGIN
                SELECT RAISE(ABORT, 'object deletion requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS completions_immutable
            BEFORE UPDATE ON turn_completions BEGIN
                SELECT RAISE(ABORT, 'turn completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS completions_delete_forbidden
            BEFORE DELETE ON turn_completions BEGIN
                SELECT RAISE(ABORT, 'turn completion is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS completion_admission_guard
            BEFORE INSERT ON turn_completions BEGIN
                SELECT CASE WHEN json_valid(NEW.state_json) != 1
                    OR NEW.state_digest != canonical_digest(NEW.state_json)
                    OR json_valid(NEW.output_manifest_json) != 1
                    OR NEW.output_manifest_digest != canonical_digest(NEW.output_manifest_json)
                    THEN RAISE(ABORT, 'completion digest mismatch') END;
                SELECT CASE WHEN NOT EXISTS (
                    SELECT 1 FROM consumers c
                    JOIN records r
                      ON r.channel_id = c.channel_id
                     AND r.partition_id = c.partition_id
                     AND r.sequence = c.in_flight_sequence
                    JOIN clock k ON k.singleton = 1
                    WHERE c.consumer_id = NEW.consumer_id
                      AND c.channel_id = NEW.input_channel_id
                      AND c.partition_id = NEW.input_partition_id
                      AND c.in_flight_sequence = NEW.input_sequence
                      AND c.in_flight_attempt = NEW.attempt
                      AND c.owner_id = NEW.owner_id
                      AND c.owner_epoch = NEW.owner_epoch
                      AND c.lease_expires_at > k.logical_time
                      AND r.record_id = NEW.input_record_id
                ) THEN RAISE(ABORT, 'completion does not match active delivery') END;
                SELECT CASE WHEN NEW.kind = 'RETURN' AND NEW.output_manifest_json != 'null'
                    THEN RAISE(ABORT, 'return manifest must be null') END;
                SELECT CASE WHEN NEW.kind = 'YIELD' AND NOT EXISTS (
                    SELECT 1 FROM records o
                    WHERE o.channel_id = NEW.output_channel_id
                      AND o.partition_id = NEW.output_partition_id
                      AND o.sequence = NEW.output_sequence
                      AND o.record_id = NEW.output_record_id
                      AND json_extract(NEW.output_manifest_json, '$.channel') = o.channel_id
                      AND json_extract(NEW.output_manifest_json, '$.partition') = o.partition_id
                      AND json_extract(NEW.output_manifest_json, '$.sequence') = o.sequence
                      AND json_extract(NEW.output_manifest_json, '$.record_id') = o.record_id
                      AND json_extract(NEW.output_manifest_json, '$.producer_epoch') = o.producer_epoch
                      AND json_extract(NEW.output_manifest_json, '$.payload.key') = o.payload_key
                      AND json_extract(NEW.output_manifest_json, '$.payload.digest') = o.payload_digest
                      AND json_extract(NEW.output_manifest_json, '$.payload.size') = o.payload_size
                      AND json_extract(NEW.output_manifest_json, '$.payload.schema_id') = o.schema_id
                      AND json_extract(NEW.output_manifest_json, '$.lineage') = o.lineage_digest
                ) THEN RAISE(ABORT, 'yield manifest does not match output envelope') END;
            END;
            CREATE TRIGGER IF NOT EXISTS consumer_progress_guard
            BEFORE UPDATE OF completed_through, in_flight_sequence, state_json ON consumers
            WHEN NEW.completed_through != OLD.completed_through
              OR (OLD.in_flight_sequence IS NOT NULL AND NEW.in_flight_sequence IS NULL)
              OR NEW.state_json != OLD.state_json
            BEGIN
                SELECT CASE WHEN NOT (
                    NEW.completed_through = OLD.completed_through + 1
                    AND OLD.in_flight_sequence = NEW.completed_through
                    AND NEW.in_flight_sequence IS NULL
                    AND EXISTS (
                        SELECT 1 FROM turn_completions t
                        WHERE t.consumer_id = OLD.consumer_id
                          AND t.input_sequence = NEW.completed_through
                          AND t.input_channel_id = OLD.channel_id
                          AND t.input_partition_id = OLD.partition_id
                          AND t.attempt = OLD.in_flight_attempt
                          AND t.owner_id = OLD.owner_id
                          AND t.owner_epoch = OLD.owner_epoch
                          AND t.state_json = NEW.state_json
                          AND OLD.lease_expires_at > (
                              SELECT logical_time FROM clock WHERE singleton = 1
                          )
                    )
                ) THEN RAISE(ABORT, 'consumer progress requires exact completion') END;
            END;
            """
        )

    @contextmanager
    def _write_transaction(self) -> Iterator[None]:
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

    def advance_time(self, amount: int) -> None:
        if amount < 1:
            raise StreamProtocolError("time advance must be positive")
        with self._write_transaction():
            self.connection.execute(
                "UPDATE clock SET logical_time = logical_time + ? WHERE singleton = 1",
                (amount,),
            )

    def _now(self) -> int:
        return int(
            self.connection.execute(
                "SELECT logical_time FROM clock WHERE singleton = 1"
            ).fetchone()[0]
        )

    def register_object(
        self,
        ref: ObjectRef,
        *,
        durable: bool = True,
        fault: FaultHook | None = None,
    ) -> None:
        if not ref.key or not ref.digest or not ref.schema_id or ref.size < 0:
            raise StreamProtocolError("invalid object reference")
        values = (ref.key, ref.digest, ref.size, ref.schema_id, int(durable))
        _hit(fault, "object_witness:before_insert")
        with self._write_transaction():
            existing = self.connection.execute(
                "SELECT digest, size, schema_id, durable FROM objects WHERE object_key = ?",
                (ref.key,),
            ).fetchone()
            if existing is None:
                self.connection.execute(
                    "INSERT INTO objects VALUES (?, ?, ?, ?, ?)", values
                )
            elif tuple(existing) != values[1:]:
                raise StreamProtocolError("object reference drift")
            _hit(fault, "object_witness:after_insert_before_commit")
        _hit(fault, "object_witness:after_commit_before_ack")

    def claim_producer(
        self,
        channel_id: str,
        partition_id: str,
        owner_id: str,
        owner_epoch: int,
    ) -> None:
        if owner_epoch < 1:
            raise StreamProtocolError("producer epoch must be positive")
        with self._write_transaction():
            current = self.connection.execute(
                """SELECT owner_id, owner_epoch FROM producer_owners
                   WHERE channel_id = ? AND partition_id = ?""",
                (channel_id, partition_id),
            ).fetchone()
            if current is None:
                self.connection.execute(
                    "INSERT INTO producer_owners VALUES (?, ?, ?, ?)",
                    (channel_id, partition_id, owner_id, owner_epoch),
                )
            elif tuple(current) == (owner_id, owner_epoch):
                return
            elif owner_epoch <= current["owner_epoch"]:
                raise StreamProtocolError("stale producer epoch")
            else:
                self.connection.execute(
                    """UPDATE producer_owners SET owner_id = ?, owner_epoch = ?
                       WHERE channel_id = ? AND partition_id = ?""",
                    (owner_id, owner_epoch, channel_id, partition_id),
                )

    def append(
        self,
        channel_id: str,
        partition_id: str,
        owner_id: str,
        owner_epoch: int,
        record_id: str,
        payload_ref: ObjectRef,
        lineage_digest: str,
        *,
        fault: FaultHook | None = None,
    ) -> DataEnvelope:
        _hit(fault, "append:before_insert")
        with self._write_transaction():
            envelope = self._append_in_transaction(
                channel_id,
                partition_id,
                owner_id,
                owner_epoch,
                record_id,
                payload_ref,
                lineage_digest,
            )
            _hit(fault, "append:after_insert_before_commit")
        _hit(fault, "append:after_commit_before_ack")
        return envelope

    def _append_in_transaction(
        self,
        channel_id: str,
        partition_id: str,
        owner_id: str,
        owner_epoch: int,
        record_id: str,
        payload_ref: ObjectRef,
        lineage_digest: str,
        *,
        allow_existing: bool = True,
    ) -> DataEnvelope:
        owner = self.connection.execute(
            """SELECT owner_id, owner_epoch FROM producer_owners
               WHERE channel_id = ? AND partition_id = ?""",
            (channel_id, partition_id),
        ).fetchone()
        if owner is None or tuple(owner) != (owner_id, owner_epoch):
            raise StreamProtocolError("producer is not the active fenced owner")
        obj = self.connection.execute(
            """SELECT digest, size, schema_id, durable FROM objects
               WHERE object_key = ?""",
            (payload_ref.key,),
        ).fetchone()
        if obj is None or not obj["durable"]:
            raise StreamProtocolError("payload must be durable before append")
        if tuple(obj)[:3] != (
            payload_ref.digest,
            payload_ref.size,
            payload_ref.schema_id,
        ):
            raise StreamProtocolError("payload reference does not match durable object")

        existing = self.connection.execute(
            """SELECT * FROM records
               WHERE channel_id = ? AND partition_id = ? AND record_id = ?""",
            (channel_id, partition_id, record_id),
        ).fetchone()
        identity = (
            payload_ref.key,
            payload_ref.digest,
            payload_ref.size,
            payload_ref.schema_id,
            lineage_digest,
        )
        if existing is not None:
            if not allow_existing:
                raise StreamProtocolError(
                    "turn output must be created by its completion transaction"
                )
            if (
                tuple(
                    existing[key]
                    for key in (
                        "payload_key",
                        "payload_digest",
                        "payload_size",
                        "schema_id",
                        "lineage_digest",
                    )
                )
                != identity
            ):
                raise StreamProtocolError("record identity drift")
            return self._row_to_envelope(existing)

        sequence = int(
            self.connection.execute(
                """SELECT COALESCE(MAX(sequence), 0) + 1 FROM records
                   WHERE channel_id = ? AND partition_id = ?""",
                (channel_id, partition_id),
            ).fetchone()[0]
        )
        self.connection.execute(
            "INSERT INTO records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                channel_id,
                partition_id,
                sequence,
                record_id,
                *identity,
                owner_epoch,
            ),
        )
        row = self.connection.execute(
            """SELECT * FROM records WHERE channel_id = ? AND partition_id = ?
               AND sequence = ?""",
            (channel_id, partition_id, sequence),
        ).fetchone()
        return self._row_to_envelope(row)

    @staticmethod
    def _row_to_envelope(row: sqlite3.Row) -> DataEnvelope:
        return DataEnvelope(
            channel_id=row["channel_id"],
            partition_id=row["partition_id"],
            sequence=row["sequence"],
            record_id=row["record_id"],
            payload_ref=ObjectRef(
                key=row["payload_key"],
                digest=row["payload_digest"],
                size=row["payload_size"],
                schema_id=row["schema_id"],
            ),
            lineage_digest=row["lineage_digest"],
            producer_epoch=row["producer_epoch"],
        )

    def create_consumer(
        self, consumer_id: str, channel_id: str, partition_id: str
    ) -> None:
        with self._write_transaction():
            current = self.connection.execute(
                "SELECT channel_id, partition_id FROM consumers WHERE consumer_id = ?",
                (consumer_id,),
            ).fetchone()
            if current is None:
                self.connection.execute(
                    """INSERT INTO consumers(consumer_id, channel_id, partition_id)
                       VALUES (?, ?, ?)""",
                    (consumer_id, channel_id, partition_id),
                )
            elif tuple(current) != (channel_id, partition_id):
                raise StreamProtocolError("consumer binding drift")

    def claim_consumer(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        *,
        lease_ttl: int = 10,
    ) -> None:
        if owner_epoch < 1 or lease_ttl < 1:
            raise StreamProtocolError("invalid consumer lease")
        with self._write_transaction():
            row = self.connection.execute(
                "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
            ).fetchone()
            if row is None:
                raise StreamProtocolError("unknown consumer")
            now = self._now()
            if row["owner_id"] is None:
                if owner_epoch != 1:
                    raise StreamProtocolError("first consumer epoch must be one")
            elif (row["owner_id"], row["owner_epoch"]) == (owner_id, owner_epoch):
                if now >= row["lease_expires_at"]:
                    raise StreamProtocolError("expired lease requires takeover")
                return
            else:
                if now < row["lease_expires_at"]:
                    raise StreamProtocolError("consumer lease is still active")
                if owner_epoch <= row["owner_epoch"]:
                    raise StreamProtocolError("takeover epoch must increase")
            next_attempt = row["in_flight_attempt"]
            if row["owner_id"] is not None and row["in_flight_sequence"] is not None:
                next_attempt += 1
            cursor = self.connection.execute(
                """UPDATE consumers SET owner_id = ?, owner_epoch = ?,
                   lease_expires_at = ?, in_flight_attempt = ?
                   WHERE consumer_id = ? AND owner_epoch = ?
                     AND owner_id IS ? AND lease_expires_at IS ?""",
                (
                    owner_id,
                    owner_epoch,
                    now + lease_ttl,
                    next_attempt,
                    consumer_id,
                    row["owner_epoch"],
                    row["owner_id"],
                    row["lease_expires_at"],
                ),
            )
            if cursor.rowcount != 1:
                raise StreamProtocolError("consumer takeover CAS failed")

    def deliver_next(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        *,
        fault: FaultHook | None = None,
    ) -> tuple[DeliveryToken, DataEnvelope] | None:
        _hit(fault, "delivery:before_claim")
        with self._write_transaction():
            consumer = self._active_consumer(consumer_id, owner_id, owner_epoch)
            sequence = consumer["in_flight_sequence"]
            attempt = consumer["in_flight_attempt"]
            if sequence is None:
                sequence = consumer["completed_through"] + 1
                record = self.connection.execute(
                    """SELECT * FROM records WHERE channel_id = ? AND partition_id = ?
                       AND sequence = ?""",
                    (consumer["channel_id"], consumer["partition_id"], sequence),
                ).fetchone()
                if record is None:
                    return None
                attempt += 1
                cursor = self.connection.execute(
                    """UPDATE consumers SET in_flight_sequence = ?, in_flight_attempt = ?
                       WHERE consumer_id = ? AND owner_id = ? AND owner_epoch = ?
                         AND completed_through = ? AND in_flight_sequence IS NULL
                         AND lease_expires_at > ?""",
                    (
                        sequence,
                        attempt,
                        consumer_id,
                        owner_id,
                        owner_epoch,
                        consumer["completed_through"],
                        self._now(),
                    ),
                )
                if cursor.rowcount != 1:
                    raise StreamProtocolError("delivery claim CAS failed")
                _hit(fault, "delivery:after_claim_before_commit")
            else:
                record = self.connection.execute(
                    """SELECT * FROM records WHERE channel_id = ? AND partition_id = ?
                       AND sequence = ?""",
                    (consumer["channel_id"], consumer["partition_id"], sequence),
                ).fetchone()
            token = DeliveryToken(
                consumer_id=consumer_id,
                channel_id=consumer["channel_id"],
                partition_id=consumer["partition_id"],
                sequence=sequence,
                attempt=attempt,
                owner_id=owner_id,
                owner_epoch=owner_epoch,
            )
            result = token, self._row_to_envelope(record)
        _hit(fault, "delivery:after_commit_before_ack")
        return result

    def _active_consumer(
        self, consumer_id: str, owner_id: str, owner_epoch: int
    ) -> sqlite3.Row:
        row = self.connection.execute(
            "SELECT * FROM consumers WHERE consumer_id = ?", (consumer_id,)
        ).fetchone()
        if row is None:
            raise StreamProtocolError("unknown consumer")
        if (row["owner_id"], row["owner_epoch"]) != (owner_id, owner_epoch):
            raise StreamProtocolError("consumer is not the active fenced owner")
        if self._now() >= row["lease_expires_at"]:
            raise StreamProtocolError("consumer lease expired")
        return row

    def acquire_invocation(
        self,
        token: DeliveryToken,
        holder_id: str,
        *,
        invocation_ttl: int = 10,
    ) -> None:
        """Durably serialize client-code execution for one consumer delivery."""
        if invocation_ttl < 1:
            raise StreamProtocolError("invalid invocation lease")
        with self._write_transaction():
            consumer = self._active_consumer(
                token.consumer_id, token.owner_id, token.owner_epoch
            )
            if (consumer["in_flight_sequence"], consumer["in_flight_attempt"]) != (
                token.sequence,
                token.attempt,
            ):
                raise StreamProtocolError("invocation token is not current")
            now = self._now()
            current = self.connection.execute(
                "SELECT * FROM runtime_invocations WHERE consumer_id = ?",
                (token.consumer_id,),
            ).fetchone()
            if current is None:
                self.connection.execute(
                    "INSERT INTO runtime_invocations VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        token.consumer_id,
                        token.sequence,
                        token.attempt,
                        token.owner_id,
                        token.owner_epoch,
                        holder_id,
                        now + invocation_ttl,
                    ),
                )
                return
            if current["holder_id"] == holder_id:
                return
            if (
                current["expires_at"] > now
                or token.owner_epoch <= current["owner_epoch"]
                or token.attempt <= current["attempt"]
            ):
                raise StreamProtocolError("consumer invocation already active")
            cursor = self.connection.execute(
                """UPDATE runtime_invocations SET input_sequence=?, attempt=?,
                   owner_id=?, owner_epoch=?, holder_id=?, expires_at=?
                   WHERE consumer_id=? AND holder_id=? AND expires_at=?""",
                (
                    token.sequence,
                    token.attempt,
                    token.owner_id,
                    token.owner_epoch,
                    holder_id,
                    now + invocation_ttl,
                    token.consumer_id,
                    current["holder_id"],
                    current["expires_at"],
                ),
            )
            if cursor.rowcount != 1:
                raise StreamProtocolError("invocation takeover CAS failed")

    def release_invocation(self, consumer_id: str, holder_id: str) -> None:
        with self._write_transaction():
            self.connection.execute(
                "DELETE FROM runtime_invocations WHERE consumer_id=? AND holder_id=?",
                (consumer_id, holder_id),
            )

    def complete(
        self,
        token: DeliveryToken,
        kind: CompletionKind,
        *,
        output: PendingOutput | None = None,
        managed_state: dict[str, Any] | None = None,
        fail_before_progress: bool = False,
        after_validation: Callable[[], None] | None = None,
        fault: FaultHook | None = None,
        invocation_holder_id: str | None = None,
    ) -> None:
        if kind is CompletionKind.YIELD and output is None:
            raise StreamProtocolError("YIELD completion requires one output")
        if kind is CompletionKind.RETURN and output is not None:
            raise StreamProtocolError("RETURN completion cannot append output in v2")
        state = managed_state or {}
        state_json = json.dumps(state, sort_keys=True, separators=(",", ":"))
        state_digest = _digest(state)
        with self._write_transaction():
            existing = self.connection.execute(
                """SELECT *
                   FROM turn_completions
                   WHERE consumer_id = ? AND input_sequence = ?""",
                (token.consumer_id, token.sequence),
            ).fetchone()
            if existing is not None:
                if (
                    existing["attempt"],
                    existing["owner_id"],
                    existing["owner_epoch"],
                    existing["kind"],
                    existing["state_json"],
                    existing["state_digest"],
                ) != (
                    token.attempt,
                    token.owner_id,
                    token.owner_epoch,
                    kind.value,
                    state_json,
                    state_digest,
                ):
                    raise StreamProtocolError("turn completion drift")
                if output is None:
                    if existing["output_record_id"] is not None:
                        raise StreamProtocolError("turn completion output drift")
                else:
                    row = self.connection.execute(
                        """SELECT * FROM records WHERE channel_id = ?
                           AND partition_id = ? AND sequence = ?""",
                        (
                            existing["output_channel_id"],
                            existing["output_partition_id"],
                            existing["output_sequence"],
                        ),
                    ).fetchone()
                    if row is None or (
                        row["channel_id"],
                        row["partition_id"],
                        row["record_id"],
                        row["payload_key"],
                        row["payload_digest"],
                        row["payload_size"],
                        row["schema_id"],
                        row["lineage_digest"],
                    ) != (
                        output.channel_id,
                        output.partition_id,
                        output.record_id,
                        output.payload_ref.key,
                        output.payload_ref.digest,
                        output.payload_ref.size,
                        output.payload_ref.schema_id,
                        output.lineage_digest,
                    ):
                        raise StreamProtocolError("turn completion output drift")
                    stored_manifest = {
                        "channel": row["channel_id"],
                        "partition": row["partition_id"],
                        "sequence": row["sequence"],
                        "record_id": row["record_id"],
                        "payload": {
                            "key": row["payload_key"],
                            "digest": row["payload_digest"],
                            "size": row["payload_size"],
                            "schema_id": row["schema_id"],
                        },
                        "lineage": row["lineage_digest"],
                        "producer_epoch": row["producer_epoch"],
                    }
                    canonical = json.dumps(
                        stored_manifest, sort_keys=True, separators=(",", ":")
                    )
                    if existing["output_manifest_json"] != canonical or existing[
                        "output_manifest_digest"
                    ] != _digest(stored_manifest):
                        raise StreamProtocolError("stored output manifest drift")
                return

            invocation = self.connection.execute(
                "SELECT * FROM runtime_invocations WHERE consumer_id = ?",
                (token.consumer_id,),
            ).fetchone()
            if invocation is not None and (
                invocation_holder_id is None
                or (
                    invocation["input_sequence"],
                    invocation["attempt"],
                    invocation["owner_id"],
                    invocation["owner_epoch"],
                    invocation["holder_id"],
                )
                != (
                    token.sequence,
                    token.attempt,
                    token.owner_id,
                    token.owner_epoch,
                    invocation_holder_id,
                )
            ):
                raise StreamProtocolError("completion lacks exact invocation holder")
            if invocation_holder_id is not None and invocation is None:
                raise StreamProtocolError("completion invocation holder is missing")

            consumer = self._active_consumer(
                token.consumer_id, token.owner_id, token.owner_epoch
            )
            if (
                consumer["channel_id"],
                consumer["partition_id"],
                consumer["in_flight_sequence"],
                consumer["in_flight_attempt"],
            ) != (
                token.channel_id,
                token.partition_id,
                token.sequence,
                token.attempt,
            ):
                raise StreamProtocolError("delivery token is not current")

            input_row = self.connection.execute(
                """SELECT record_id FROM records WHERE channel_id = ?
                   AND partition_id = ? AND sequence = ?""",
                (token.channel_id, token.partition_id, token.sequence),
            ).fetchone()
            if input_row is None:
                raise StreamProtocolError("input record witness is missing")
            if after_validation is not None:
                after_validation()

            output_envelope = None
            if output is not None:
                producer = self.connection.execute(
                    """SELECT owner_id, owner_epoch FROM producer_owners
                       WHERE channel_id = ? AND partition_id = ?""",
                    (output.channel_id, output.partition_id),
                ).fetchone()
                if producer is None:
                    raise StreamProtocolError("downstream producer is unclaimed")
                output_envelope = self._append_in_transaction(
                    output.channel_id,
                    output.partition_id,
                    producer["owner_id"],
                    producer["owner_epoch"],
                    output.record_id,
                    output.payload_ref,
                    output.lineage_digest,
                    allow_existing=False,
                )
                _hit(fault, "completion:after_output_insert_before_marker")
            if fail_before_progress:
                raise RuntimeError("injected crash before yield/return progress")
            manifest = (
                None
                if output_envelope is None
                else {
                    "channel": output_envelope.channel_id,
                    "partition": output_envelope.partition_id,
                    "sequence": output_envelope.sequence,
                    "record_id": output_envelope.record_id,
                    "payload": output_envelope.payload_ref.__dict__,
                    "lineage": output_envelope.lineage_digest,
                    "producer_epoch": output_envelope.producer_epoch,
                }
            )
            manifest_json = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
            manifest_digest = _digest(manifest)
            self.connection.execute(
                """INSERT INTO turn_completions VALUES (
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                   )""",
                (
                    token.consumer_id,
                    token.channel_id,
                    token.partition_id,
                    token.sequence,
                    input_row["record_id"],
                    token.attempt,
                    token.owner_id,
                    token.owner_epoch,
                    kind.value,
                    None if output_envelope is None else output_envelope.channel_id,
                    None if output_envelope is None else output_envelope.partition_id,
                    None if output_envelope is None else output_envelope.sequence,
                    None if output_envelope is None else output_envelope.record_id,
                    manifest_json,
                    manifest_digest,
                    state_json,
                    state_digest,
                ),
            )
            _hit(fault, "completion:after_marker_before_progress")
            cursor = self.connection.execute(
                """UPDATE consumers SET completed_through = ?,
                   in_flight_sequence = NULL, state_json = ?
                   WHERE consumer_id = ? AND owner_id = ? AND owner_epoch = ?
                     AND in_flight_sequence = ? AND in_flight_attempt = ?
                     AND lease_expires_at > ?""",
                (
                    token.sequence,
                    state_json,
                    token.consumer_id,
                    token.owner_id,
                    token.owner_epoch,
                    token.sequence,
                    token.attempt,
                    self._now(),
                ),
            )
            if cursor.rowcount != 1:
                raise StreamProtocolError("turn completion CAS failed")
            if invocation_holder_id is not None:
                retired = self.connection.execute(
                    "DELETE FROM runtime_invocations WHERE consumer_id=? AND holder_id=?",
                    (token.consumer_id, invocation_holder_id),
                )
                if retired.rowcount != 1:
                    raise StreamProtocolError("completion invocation retire failed")
            _hit(fault, "progress:after_cursor_update_before_commit")
        _hit(fault, "progress:after_commit_before_ack")

    def position(self, consumer_id: str) -> dict[str, Any]:
        row = self.connection.execute(
            """SELECT completed_through, in_flight_sequence, in_flight_attempt,
                      owner_id, owner_epoch, state_json
               FROM consumers WHERE consumer_id = ?""",
            (consumer_id,),
        ).fetchone()
        if row is None:
            raise StreamProtocolError("unknown consumer")
        return {
            "completed_through": row["completed_through"],
            "in_flight": row["in_flight_sequence"],
            "attempt": row["in_flight_attempt"],
            "owner_id": row["owner_id"],
            "owner_epoch": row["owner_epoch"],
            "managed_state": json.loads(row["state_json"]),
        }

    def records(self, channel_id: str, partition_id: str) -> list[DataEnvelope]:
        rows = self.connection.execute(
            """SELECT * FROM records WHERE channel_id = ? AND partition_id = ?
               ORDER BY sequence""",
            (channel_id, partition_id),
        ).fetchall()
        return [self._row_to_envelope(row) for row in rows]
