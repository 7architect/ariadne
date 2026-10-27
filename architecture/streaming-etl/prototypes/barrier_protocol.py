"""Sprint 2 executable model for durable barriers, aligned joins, and recovery cuts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any


class BarrierProtocolError(RuntimeError):
    pass


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class RecordKind(str, Enum):
    DATA = "DATA"
    BARRIER = "BARRIER"


@dataclass(frozen=True)
class ChannelRecord:
    edge_id: str
    partition_id: str
    sequence: int
    kind: RecordKind
    record_id: str
    payload: Any = None
    checkpoint: int | None = None


class DurableChannel:
    """SQLite channel model with ordered records and generation fencing."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS protocol_clock (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                logical_time INTEGER NOT NULL CHECK (logical_time >= 0)
            );
            INSERT OR IGNORE INTO protocol_clock VALUES (1, 0);
            CREATE TABLE IF NOT EXISTS partition_owners (
                edge_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK (generation > 0),
                lease_expires_at INTEGER NOT NULL CHECK (lease_expires_at > 0),
                PRIMARY KEY (edge_id, partition_id)
            );
            CREATE TABLE IF NOT EXISTS channel_records (
                edge_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                sequence INTEGER NOT NULL CHECK (sequence > 0),
                kind TEXT NOT NULL CHECK (kind IN ('DATA', 'BARRIER')),
                record_id TEXT NOT NULL,
                payload_json TEXT,
                checkpoint INTEGER,
                writer_generation INTEGER NOT NULL,
                PRIMARY KEY (edge_id, partition_id, sequence),
                UNIQUE (edge_id, partition_id, record_id),
                CHECK (
                    (kind = 'DATA' AND checkpoint IS NULL AND payload_json IS NOT NULL)
                    OR (kind = 'BARRIER' AND checkpoint IS NOT NULL)
                )
            );
            CREATE TABLE IF NOT EXISTS record_claims (
                edge_id TEXT NOT NULL,
                partition_id TEXT NOT NULL,
                record_id TEXT NOT NULL,
                lineage_digest TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                PRIMARY KEY (edge_id, partition_id, record_id)
            );
            CREATE TRIGGER IF NOT EXISTS channel_record_immutable
            BEFORE UPDATE ON channel_records
            BEGIN
                SELECT RAISE(ABORT, 'channel record is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS channel_record_delete_requires_gc_protocol
            BEFORE DELETE ON channel_records
            BEGIN
                SELECT RAISE(ABORT, 'channel record deletion requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS record_claim_immutable
            BEFORE UPDATE ON record_claims
            BEGIN
                SELECT RAISE(ABORT, 'record claim is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS record_claim_delete_requires_gc_protocol
            BEFORE DELETE ON record_claims
            BEGIN
                SELECT RAISE(ABORT, 'record claim deletion requires retention protocol');
            END;
            """
        )

    def claim_partition(
        self,
        edge_id: str,
        partition_id: str,
        owner_id: str,
        generation: int,
        lease_ttl: int = 10,
    ) -> None:
        if generation < 1:
            raise BarrierProtocolError("generation must be positive")
        if lease_ttl < 1:
            raise BarrierProtocolError("lease ttl must be positive")
        with self.connection:
            self._lock_clock()
            now = self._logical_time()
            current = self.connection.execute(
                """
                SELECT owner_id, generation, lease_expires_at FROM partition_owners
                WHERE edge_id = ? AND partition_id = ?
                """,
                (edge_id, partition_id),
            ).fetchone()
            if current is None:
                if generation != 1:
                    raise BarrierProtocolError("first generation must be one")
                self.connection.execute(
                    "INSERT INTO partition_owners VALUES (?, ?, ?, ?, ?)",
                    (edge_id, partition_id, owner_id, generation, now + lease_ttl),
                )
                return
            if current[:2] == (owner_id, generation):
                if now >= current[2]:
                    raise BarrierProtocolError(
                        "expired partition lease requires a new generation"
                    )
                return
            if generation <= current[1]:
                raise BarrierProtocolError("takeover generation must increase")
            if now < current[2]:
                raise BarrierProtocolError("partition lease is still active")
            cursor = self.connection.execute(
                """
                UPDATE partition_owners
                SET owner_id = ?, generation = ?, lease_expires_at = ?
                WHERE edge_id = ? AND partition_id = ?
                  AND owner_id = ? AND generation = ? AND lease_expires_at = ?
                """,
                (
                    owner_id,
                    generation,
                    now + lease_ttl,
                    edge_id,
                    partition_id,
                    *current,
                ),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("partition takeover CAS failed")

    def renew_partition(
        self,
        edge_id: str,
        partition_id: str,
        owner_id: str,
        generation: int,
        lease_ttl: int = 10,
    ) -> None:
        if lease_ttl < 1:
            raise BarrierProtocolError("lease ttl must be positive")
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE partition_owners
                SET lease_expires_at = MAX(
                    lease_expires_at,
                    (SELECT logical_time FROM protocol_clock WHERE singleton = 1) + ?
                )
                WHERE edge_id = ? AND partition_id = ?
                  AND owner_id = ? AND generation = ?
                  AND lease_expires_at > (
                      SELECT logical_time FROM protocol_clock WHERE singleton = 1
                  )
                """,
                (
                    lease_ttl,
                    edge_id,
                    partition_id,
                    owner_id,
                    generation,
                ),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("stale or expired partition lease")

    def _logical_time(self) -> int:
        return self.connection.execute(
            "SELECT logical_time FROM protocol_clock WHERE singleton = 1"
        ).fetchone()[0]

    def _lock_clock(self) -> None:
        cursor = self.connection.execute(
            """
            UPDATE protocol_clock SET logical_time = logical_time
            WHERE singleton = 1
            """
        )
        if cursor.rowcount != 1:
            raise BarrierProtocolError("logical clock lock failed")

    def advance_clock(self, target: int) -> None:
        if target < 0:
            raise BarrierProtocolError("logical clock cannot be negative")
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE protocol_clock SET logical_time = ?
                WHERE singleton = 1 AND logical_time <= ?
                """,
                (target, target),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("logical clock cannot move backward")

    def append_data(
        self,
        edge_id: str,
        partition_id: str,
        sequence: int,
        record_id: str,
        payload: Any,
        lineage: Any,
        owner_id: str,
        generation: int,
    ) -> None:
        if lineage is None:
            raise BarrierProtocolError("data record requires explicit lineage")
        if record_id.startswith("barrier:"):
            raise BarrierProtocolError("data record uses reserved barrier namespace")
        self._append(
            ChannelRecord(
                edge_id,
                partition_id,
                sequence,
                RecordKind.DATA,
                record_id,
                payload,
            ),
            owner_id,
            generation,
            lineage,
        )

    def append_barrier(
        self,
        edge_id: str,
        partition_id: str,
        sequence: int,
        checkpoint: int,
        owner_id: str,
        generation: int,
    ) -> None:
        if checkpoint < 1:
            raise BarrierProtocolError("checkpoint must be positive")
        self._append(
            ChannelRecord(
                edge_id,
                partition_id,
                sequence,
                RecordKind.BARRIER,
                f"barrier:{checkpoint}",
                checkpoint=checkpoint,
            ),
            owner_id,
            generation,
            None,
        )

    def _append(
        self,
        record: ChannelRecord,
        owner_id: str,
        generation: int,
        lineage: Any,
    ) -> None:
        payload_json = (
            None
            if record.kind is RecordKind.BARRIER
            else json.dumps(record.payload, sort_keys=True, separators=(",", ":"))
        )
        expected_row = (
            record.kind.value,
            record.record_id,
            payload_json,
            record.checkpoint,
        )
        with self.connection:
            guard = self.connection.execute(
                """
                UPDATE partition_owners SET generation = generation
                WHERE edge_id = ? AND partition_id = ?
                  AND owner_id = ? AND generation = ?
                  AND lease_expires_at > (
                      SELECT logical_time FROM protocol_clock WHERE singleton = 1
                  )
                """,
                (record.edge_id, record.partition_id, owner_id, generation),
            )
            if guard.rowcount != 1:
                raise BarrierProtocolError("stale partition writer")
            claim = None
            if record.kind is RecordKind.DATA:
                claim = (_digest(lineage), _digest(record.payload))
                existing_claim = self.connection.execute(
                    """
                    SELECT lineage_digest, payload_digest FROM record_claims
                    WHERE edge_id = ? AND partition_id = ? AND record_id = ?
                    """,
                    (record.edge_id, record.partition_id, record.record_id),
                ).fetchone()
                if existing_claim is not None and existing_claim != claim:
                    raise BarrierProtocolError("stable record claim conflict")
            existing = self.connection.execute(
                """
                SELECT kind, record_id, payload_json, checkpoint
                FROM channel_records
                WHERE edge_id = ? AND partition_id = ? AND sequence = ?
                """,
                (record.edge_id, record.partition_id, record.sequence),
            ).fetchone()
            if existing is not None:
                if existing != expected_row:
                    raise BarrierProtocolError("channel sequence identity conflict")
                return
            reused_identity = self.connection.execute(
                """
                SELECT sequence FROM channel_records
                WHERE edge_id = ? AND partition_id = ? AND record_id = ?
                """,
                (record.edge_id, record.partition_id, record.record_id),
            ).fetchone()
            if reused_identity is not None:
                raise BarrierProtocolError(
                    "channel record identity reused at new sequence"
                )
            latest = self.connection.execute(
                """
                SELECT MAX(sequence) FROM channel_records
                WHERE edge_id = ? AND partition_id = ?
                """,
                (record.edge_id, record.partition_id),
            ).fetchone()[0]
            expected_sequence = 1 if latest is None else latest + 1
            if record.sequence != expected_sequence:
                raise BarrierProtocolError(
                    f"non-contiguous append: expected {expected_sequence}"
                )
            if record.kind is RecordKind.DATA:
                self.connection.execute(
                    "INSERT OR IGNORE INTO record_claims VALUES (?, ?, ?, ?, ?)",
                    (
                        record.edge_id,
                        record.partition_id,
                        record.record_id,
                        *claim,
                    ),
                )
            else:
                latest_checkpoint = self.connection.execute(
                    """
                    SELECT MAX(checkpoint) FROM channel_records
                    WHERE edge_id = ? AND partition_id = ? AND kind = 'BARRIER'
                    """,
                    (record.edge_id, record.partition_id),
                ).fetchone()[0]
                if (
                    latest_checkpoint is not None
                    and record.checkpoint <= latest_checkpoint
                ):
                    raise BarrierProtocolError("barrier checkpoint must increase")
            cursor = self.connection.execute(
                """
                INSERT INTO channel_records
                SELECT ?, ?, ?, ?, ?, ?, ?, ?
                WHERE EXISTS (
                    SELECT 1 FROM partition_owners
                    WHERE edge_id = ? AND partition_id = ?
                      AND owner_id = ? AND generation = ?
                      AND lease_expires_at > (
                          SELECT logical_time FROM protocol_clock WHERE singleton = 1
                      )
                )
                """,
                (
                    record.edge_id,
                    record.partition_id,
                    record.sequence,
                    record.kind.value,
                    record.record_id,
                    payload_json,
                    record.checkpoint,
                    generation,
                    record.edge_id,
                    record.partition_id,
                    owner_id,
                    generation,
                ),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("partition owner changed during append")

    def read_after(
        self, edge_id: str, partition_id: str, sequence: int
    ) -> list[ChannelRecord]:
        rows = self.connection.execute(
            """
            SELECT sequence, kind, record_id, payload_json, checkpoint
            FROM channel_records
            WHERE edge_id = ? AND partition_id = ? AND sequence > ?
            ORDER BY sequence
            """,
            (edge_id, partition_id, sequence),
        )
        return [
            ChannelRecord(
                edge_id,
                partition_id,
                item_sequence,
                RecordKind(kind),
                record_id,
                json.loads(payload) if payload is not None else None,
                checkpoint,
            )
            for item_sequence, kind, record_id, payload, checkpoint in rows
        ]


class AlignedJoin:
    """Durable aligned barrier model for a commutative integer-sum operator."""

    def __init__(
        self,
        path: Path,
        join_id: str,
        bindings: dict[str, tuple[str, str]],
    ):
        if len(bindings) < 2 or len(bindings) != len(set(bindings)):
            raise BarrierProtocolError("join requires at least two unique inputs")
        self.join_id = join_id
        self.bindings = dict(sorted(bindings.items()))
        self.inputs = tuple(self.bindings)
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS protocol_clock (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                logical_time INTEGER NOT NULL CHECK (logical_time >= 0)
            );
            INSERT OR IGNORE INTO protocol_clock VALUES (1, 0);
            CREATE TABLE IF NOT EXISTS join_meta (
                join_id TEXT PRIMARY KEY,
                inputs_json TEXT NOT NULL,
                state_value INTEGER NOT NULL,
                active_checkpoint INTEGER,
                last_aligned_checkpoint INTEGER NOT NULL,
                checkpoint_high_water INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS join_owners (
                join_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                generation INTEGER NOT NULL CHECK (generation > 0),
                lease_expires_at INTEGER NOT NULL CHECK (lease_expires_at > 0)
            );
            CREATE TABLE IF NOT EXISTS join_cursors (
                join_id TEXT NOT NULL,
                input_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                PRIMARY KEY (join_id, input_id)
            );
            CREATE TABLE IF NOT EXISTS join_barriers (
                join_id TEXT NOT NULL,
                checkpoint INTEGER NOT NULL,
                input_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                PRIMARY KEY (join_id, checkpoint, input_id)
            );
            CREATE TABLE IF NOT EXISTS join_buffer (
                join_id TEXT NOT NULL,
                input_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                record_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (join_id, input_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS join_consumed (
                join_id TEXT NOT NULL,
                input_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                record_digest TEXT NOT NULL,
                PRIMARY KEY (join_id, input_id, sequence)
            );
            CREATE TRIGGER IF NOT EXISTS join_consumed_immutable_on_update
            BEFORE UPDATE ON join_consumed
            BEGIN
                SELECT RAISE(ABORT, 'join consumed identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_consumed_delete_requires_gc_protocol
            BEFORE DELETE ON join_consumed
            BEGIN
                SELECT RAISE(ABORT, 'join consumed deletion requires retention protocol');
            END;
            CREATE TABLE IF NOT EXISTS join_snapshots (
                join_id TEXT NOT NULL,
                checkpoint INTEGER NOT NULL,
                state_value INTEGER NOT NULL,
                barrier_positions_json TEXT NOT NULL,
                PRIMARY KEY (join_id, checkpoint)
            );
            CREATE TRIGGER IF NOT EXISTS join_snapshot_immutable_on_update
            BEFORE UPDATE ON join_snapshots
            BEGIN
                SELECT RAISE(ABORT, 'join snapshot is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS join_snapshot_delete_requires_gc_protocol
            BEFORE DELETE ON join_snapshots
            BEGIN
                SELECT RAISE(ABORT, 'join snapshot deletion requires retention protocol');
            END;
            """
        )
        manifest = json.dumps(self.bindings, sort_keys=True)
        with self.connection:
            existing = self.connection.execute(
                "SELECT inputs_json FROM join_meta WHERE join_id = ?", (join_id,)
            ).fetchone()
            if existing is None:
                self.connection.execute(
                    "INSERT INTO join_meta VALUES (?, ?, 0, NULL, 0, 0)",
                    (join_id, manifest),
                )
                self.connection.executemany(
                    "INSERT INTO join_cursors VALUES (?, ?, 0)",
                    ((join_id, input_id) for input_id in self.inputs),
                )
            elif existing[0] != manifest:
                raise BarrierProtocolError("join input manifest drift")

    def claim(self, owner_id: str, generation: int, lease_ttl: int = 10) -> None:
        if generation < 1:
            raise BarrierProtocolError("generation must be positive")
        if lease_ttl < 1:
            raise BarrierProtocolError("lease ttl must be positive")
        with self.connection:
            self._lock_clock()
            now = self._logical_time()
            current = self.connection.execute(
                """
                SELECT owner_id, generation, lease_expires_at
                FROM join_owners WHERE join_id = ?
                """,
                (self.join_id,),
            ).fetchone()
            if current is None:
                if generation != 1:
                    raise BarrierProtocolError("first join generation must be one")
                self.connection.execute(
                    "INSERT INTO join_owners VALUES (?, ?, ?, ?)",
                    (self.join_id, owner_id, generation, now + lease_ttl),
                )
                return
            if current[:2] == (owner_id, generation):
                if now >= current[2]:
                    raise BarrierProtocolError(
                        "expired join lease requires a new generation"
                    )
                return
            if generation <= current[1]:
                raise BarrierProtocolError("join takeover generation must increase")
            if now < current[2]:
                raise BarrierProtocolError("join lease is still active")
            cursor = self.connection.execute(
                """
                UPDATE join_owners
                SET owner_id = ?, generation = ?, lease_expires_at = ?
                WHERE join_id = ? AND owner_id = ? AND generation = ?
                  AND lease_expires_at = ?
                """,
                (owner_id, generation, now + lease_ttl, self.join_id, *current),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("join takeover CAS failed")

    def renew(
        self,
        owner_id: str,
        generation: int,
        lease_ttl: int = 10,
    ) -> None:
        if lease_ttl < 1:
            raise BarrierProtocolError("lease ttl must be positive")
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE join_owners
                SET lease_expires_at = MAX(
                    lease_expires_at,
                    (SELECT logical_time FROM protocol_clock WHERE singleton = 1) + ?
                )
                WHERE join_id = ? AND owner_id = ? AND generation = ?
                  AND lease_expires_at > (
                      SELECT logical_time FROM protocol_clock WHERE singleton = 1
                  )
                """,
                (lease_ttl, self.join_id, owner_id, generation),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("stale or expired join lease")

    def _logical_time(self) -> int:
        return self.connection.execute(
            "SELECT logical_time FROM protocol_clock WHERE singleton = 1"
        ).fetchone()[0]

    def _lock_clock(self) -> None:
        cursor = self.connection.execute(
            """
            UPDATE protocol_clock SET logical_time = logical_time
            WHERE singleton = 1
            """
        )
        if cursor.rowcount != 1:
            raise BarrierProtocolError("logical clock lock failed")

    def advance_clock(self, target: int) -> None:
        if target < 0:
            raise BarrierProtocolError("logical clock cannot be negative")
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE protocol_clock SET logical_time = ?
                WHERE singleton = 1 AND logical_time <= ?
                """,
                (target, target),
            )
            if cursor.rowcount != 1:
                raise BarrierProtocolError("logical clock cannot move backward")

    def ingest(
        self,
        input_id: str,
        record: ChannelRecord,
        owner_id: str,
        generation: int,
    ) -> int | None:
        if input_id not in self.inputs:
            raise BarrierProtocolError("unknown join input")
        if (record.edge_id, record.partition_id) != self.bindings[input_id]:
            raise BarrierProtocolError("record does not match join input binding")
        record_digest = _digest(
            {
                "edge_id": record.edge_id,
                "partition_id": record.partition_id,
                "sequence": record.sequence,
                "kind": record.kind.value,
                "record_id": record.record_id,
                "payload": record.payload,
                "checkpoint": record.checkpoint,
            }
        )
        with self.connection:
            guard = self.connection.execute(
                """
                UPDATE join_owners SET generation = generation
                WHERE join_id = ? AND owner_id = ? AND generation = ?
                  AND lease_expires_at > (
                      SELECT logical_time FROM protocol_clock WHERE singleton = 1
                  )
                """,
                (self.join_id, owner_id, generation),
            )
            if guard.rowcount != 1:
                raise BarrierProtocolError("stale join writer")
            consumed = self.connection.execute(
                """
                SELECT record_digest FROM join_consumed
                WHERE join_id = ? AND input_id = ? AND sequence = ?
                """,
                (self.join_id, input_id, record.sequence),
            ).fetchone()
            cursor = self.connection.execute(
                """
                SELECT sequence FROM join_cursors
                WHERE join_id = ? AND input_id = ?
                """,
                (self.join_id, input_id),
            ).fetchone()[0]
            if consumed is not None and consumed[0] != record_digest:
                raise BarrierProtocolError("divergent join input replay")
            if record.sequence <= cursor:
                if consumed is None:
                    raise BarrierProtocolError("join cursor lacks consumed identity")
                return None
            if record.sequence != cursor + 1:
                raise BarrierProtocolError("join input sequence gap")
            meta = self.connection.execute(
                """
                SELECT state_value, active_checkpoint, last_aligned_checkpoint,
                       checkpoint_high_water
                FROM join_meta
                WHERE join_id = ?
                """,
                (self.join_id,),
            ).fetchone()
            blocked = False
            if meta[1] is not None:
                blocked = (
                    self.connection.execute(
                        """
                        SELECT 1 FROM join_barriers
                        WHERE join_id = ? AND checkpoint = ? AND input_id = ?
                        """,
                        (self.join_id, meta[1], input_id),
                    ).fetchone()
                    is not None
                )
            completed_checkpoint = None
            if record.kind is RecordKind.DATA:
                if blocked:
                    self.connection.execute(
                        "INSERT INTO join_buffer VALUES (?, ?, ?, ?, ?)",
                        (
                            self.join_id,
                            input_id,
                            record.sequence,
                            record.record_id,
                            json.dumps(record.payload),
                        ),
                    )
                else:
                    self.connection.execute(
                        "UPDATE join_meta SET state_value = state_value + ? WHERE join_id = ?",
                        (int(record.payload), self.join_id),
                    )
            else:
                if blocked:
                    raise BarrierProtocolError(
                        "next barrier cannot overtake current alignment"
                    )
                if record.checkpoint <= meta[2]:
                    raise BarrierProtocolError("join checkpoint must increase")
                known_snapshot = self.connection.execute(
                    """
                    SELECT 1 FROM join_snapshots
                    WHERE join_id = ? AND checkpoint = ?
                    """,
                    (self.join_id, record.checkpoint),
                ).fetchone()
                if record.checkpoint <= meta[3] and known_snapshot is None:
                    raise BarrierProtocolError(
                        "unknown checkpoint below high-water mark"
                    )
                if meta[1] is not None and meta[1] != record.checkpoint:
                    raise BarrierProtocolError("checkpoint alignment mismatch")
                self.connection.execute(
                    """
                    UPDATE join_meta SET active_checkpoint = COALESCE(active_checkpoint, ?)
                    WHERE join_id = ?
                    """,
                    (record.checkpoint, self.join_id),
                )
                self.connection.execute(
                    "INSERT INTO join_barriers VALUES (?, ?, ?, ?)",
                    (self.join_id, record.checkpoint, input_id, record.sequence),
                )
                positions = dict(
                    self.connection.execute(
                        """
                        SELECT input_id, sequence FROM join_barriers
                        WHERE join_id = ? AND checkpoint = ?
                        """,
                        (self.join_id, record.checkpoint),
                    )
                )
                if set(positions) == set(self.inputs):
                    state = self.connection.execute(
                        "SELECT state_value FROM join_meta WHERE join_id = ?",
                        (self.join_id,),
                    ).fetchone()[0]
                    positions_json = json.dumps(positions, sort_keys=True)
                    existing_snapshot = self.connection.execute(
                        """
                        SELECT state_value, barrier_positions_json
                        FROM join_snapshots
                        WHERE join_id = ? AND checkpoint = ?
                        """,
                        (self.join_id, record.checkpoint),
                    ).fetchone()
                    snapshot_value = (state, positions_json)
                    if existing_snapshot is None:
                        self.connection.execute(
                            "INSERT INTO join_snapshots VALUES (?, ?, ?, ?)",
                            (self.join_id, record.checkpoint, *snapshot_value),
                        )
                    elif existing_snapshot != snapshot_value:
                        raise BarrierProtocolError("immutable join snapshot conflict")
                    buffered = self.connection.execute(
                        """
                        SELECT payload_json FROM join_buffer
                        WHERE join_id = ? ORDER BY sequence, input_id
                        """,
                        (self.join_id,),
                    ).fetchall()
                    self.connection.execute(
                        "DELETE FROM join_buffer WHERE join_id = ?", (self.join_id,)
                    )
                    self.connection.execute(
                        "DELETE FROM join_barriers WHERE join_id = ?", (self.join_id,)
                    )
                    self.connection.execute(
                        """
                        UPDATE join_meta
                        SET active_checkpoint = NULL,
                            last_aligned_checkpoint = ?,
                            checkpoint_high_water = MAX(checkpoint_high_water, ?)
                        WHERE join_id = ?
                        """,
                        (record.checkpoint, record.checkpoint, self.join_id),
                    )
                    for (payload,) in buffered:
                        self.connection.execute(
                            "UPDATE join_meta SET state_value = state_value + ? WHERE join_id = ?",
                            (int(json.loads(payload)), self.join_id),
                        )
                    completed_checkpoint = record.checkpoint
            if consumed is None:
                self.connection.execute(
                    "INSERT INTO join_consumed VALUES (?, ?, ?, ?)",
                    (self.join_id, input_id, record.sequence, record_digest),
                )
            self.connection.execute(
                """
                UPDATE join_cursors SET sequence = ?
                WHERE join_id = ? AND input_id = ?
                """,
                (record.sequence, self.join_id, input_id),
            )
            return completed_checkpoint

    @property
    def state_value(self) -> int:
        return self.connection.execute(
            "SELECT state_value FROM join_meta WHERE join_id = ?", (self.join_id,)
        ).fetchone()[0]

    def snapshot(self, checkpoint: int) -> tuple[int, dict[str, int]]:
        row = self.connection.execute(
            """
            SELECT state_value, barrier_positions_json FROM join_snapshots
            WHERE join_id = ? AND checkpoint = ?
            """,
            (self.join_id, checkpoint),
        ).fetchone()
        if row is None:
            raise KeyError(checkpoint)
        return row[0], json.loads(row[1])

    def restore(
        self,
        checkpoint: int,
        owner_id: str,
        generation: int,
    ) -> None:
        with self.connection:
            guard = self.connection.execute(
                """
                UPDATE join_owners SET generation = generation
                WHERE join_id = ? AND owner_id = ? AND generation = ?
                  AND lease_expires_at > (
                      SELECT logical_time FROM protocol_clock WHERE singleton = 1
                  )
                """,
                (self.join_id, owner_id, generation),
            )
            if guard.rowcount != 1:
                raise BarrierProtocolError("stale join writer")
            state, positions = self.snapshot(checkpoint)
            self.connection.execute(
                """
                UPDATE join_meta
                SET state_value = ?, active_checkpoint = NULL,
                    last_aligned_checkpoint = ?
                WHERE join_id = ?
                """,
                (state, checkpoint, self.join_id),
            )
            self.connection.execute(
                "DELETE FROM join_buffer WHERE join_id = ?", (self.join_id,)
            )
            self.connection.execute(
                "DELETE FROM join_barriers WHERE join_id = ?", (self.join_id,)
            )
            self.connection.executemany(
                """
                UPDATE join_cursors SET sequence = ?
                WHERE join_id = ? AND input_id = ?
                """,
                (
                    (sequence, self.join_id, input_id)
                    for input_id, sequence in positions.items()
                ),
            )


@dataclass(frozen=True)
class GraphEdge:
    source: str
    target: str
    edge_id: str
    replayable: bool
    partitions: frozenset[str] = frozenset({"p0"})
    checkpoint_positions: tuple[tuple[int, tuple[tuple[str, str], ...]], ...] = (
        (1, (("p0", "position:0"),)),
    )


@dataclass(frozen=True)
class ReplayCut:
    edge_id: str
    checkpoint: int
    partition_positions: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class RecoveryPlan:
    checkpoint: int
    checkpoint_manifest_digest: str
    region: frozenset[str]
    replay_cuts: tuple[ReplayCut, ...]
    reasons: dict[str, str]


def plan_recovery(
    nodes: set[str],
    edges: tuple[GraphEdge, ...],
    failed: str,
    checkpoint: int,
    completed_checkpoints: dict[int, str],
) -> RecoveryPlan:
    if failed not in nodes:
        raise BarrierProtocolError("failed node is absent from graph")
    if checkpoint < 1:
        raise BarrierProtocolError("recovery checkpoint must be positive")
    if checkpoint not in completed_checkpoints:
        raise BarrierProtocolError("recovery checkpoint is not completed")
    checkpoint_manifest_digest = completed_checkpoints[checkpoint]
    if not checkpoint_manifest_digest:
        raise BarrierProtocolError("completed checkpoint lacks manifest digest")
    edge_ids = [edge.edge_id for edge in edges]
    if len(edge_ids) != len(set(edge_ids)):
        raise BarrierProtocolError("duplicate graph edge identity")
    if any(edge.source not in nodes or edge.target not in nodes for edge in edges):
        raise BarrierProtocolError("graph edge endpoint is absent from node manifest")
    for edge in edges:
        if not edge.partitions:
            raise BarrierProtocolError("edge partition manifest is empty")
        checkpoints = [
            item_checkpoint for item_checkpoint, _ in edge.checkpoint_positions
        ]
        if len(checkpoints) != len(set(checkpoints)):
            raise BarrierProtocolError("duplicate replay-cut checkpoint")
        if any(
            len(positions) != len(dict(positions))
            or set(dict(positions)) != set(edge.partitions)
            for _, positions in edge.checkpoint_positions
        ):
            raise BarrierProtocolError("incomplete or duplicate replay-cut partition")

    def descendants(seed: str) -> set[str]:
        result = {seed}
        changed = True
        while changed:
            changed = False
            for edge in edges:
                if edge.source in result and edge.target not in result:
                    result.add(edge.target)
                    changed = True
        return result

    region = descendants(failed)
    reasons = {failed: "failed operator"}
    for node in region - {failed}:
        reasons[node] = f"downstream of {failed}"
    while True:
        edge_positions = {
            edge.edge_id: dict(edge.checkpoint_positions) for edge in edges
        }
        unavailable = [
            edge
            for edge in edges
            if edge.target in region
            and edge.source not in region
            and (not edge.replayable or checkpoint not in edge_positions[edge.edge_id])
        ]
        if not unavailable:
            break
        for edge in unavailable:
            expanded = descendants(edge.source)
            for node in expanded - region:
                reasons[node] = (
                    f"expanded through unavailable replay edge {edge.edge_id} "
                    f"at checkpoint {checkpoint}"
                )
            region |= expanded
    replay_cuts = tuple(
        ReplayCut(
            edge.edge_id,
            checkpoint,
            tuple(sorted(dict(edge.checkpoint_positions)[checkpoint])),
        )
        for edge in edges
        if edge.target in region
        and edge.source not in region
        and edge.replayable
        and checkpoint in dict(edge.checkpoint_positions)
    )
    return RecoveryPlan(
        checkpoint,
        checkpoint_manifest_digest,
        frozenset(region),
        tuple(sorted(replay_cuts, key=lambda cut: cut.edge_id)),
        reasons,
    )
