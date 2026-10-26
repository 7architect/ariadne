"""Executable protocol sketches for the streaming ETL ADRs.

These prototypes intentionally depend only on the Python standard library. They
model protocol invariants; they are not Prefect production code.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class InjectedCrash(RuntimeError):
    """Raised by a test at a protocol boundary."""


class ProtocolConflict(RuntimeError):
    """Raised when a stable protocol identity is reused with different data."""


class DurableGraphStore:
    """Durable channels, operator snapshots, and checkpoint decisions."""

    def __init__(self, path: Path):
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS messages (
                edge TEXT NOT NULL,
                logical_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                payload INTEGER NOT NULL,
                PRIMARY KEY (edge, logical_id),
                UNIQUE (edge, sequence)
            );
            CREATE TABLE IF NOT EXISTS edge_heads (
                edge TEXT PRIMARY KEY,
                last_sequence INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshots (
                checkpoint_id INTEGER NOT NULL,
                node TEXT NOT NULL,
                state_json TEXT NOT NULL,
                PRIMARY KEY (checkpoint_id, node)
            );
            CREATE TABLE IF NOT EXISTS checkpoint_decisions (
                checkpoint_id INTEGER PRIMARY KEY,
                required_nodes_json TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('STARTED', 'COMPLETED'))
            );
            """
        )

    def append(self, edge: str, logical_id: str, sequence: int, payload: int) -> None:
        """Append idempotently using a stable logical message identity."""
        existing = self.connection.execute(
            """
            SELECT sequence, payload FROM messages
            WHERE edge = ? AND logical_id = ?
            """,
            (edge, logical_id),
        ).fetchone()
        if existing is not None:
            if existing != (sequence, payload):
                raise ProtocolConflict(
                    f"message identity {(edge, logical_id)!r} changed from "
                    f"{existing!r} to {(sequence, payload)!r}"
                )
            return
        head = self.connection.execute(
            "SELECT last_sequence FROM edge_heads WHERE edge = ?", (edge,)
        ).fetchone()
        expected_sequence = 1 if head is None else head[0] + 1
        if sequence != expected_sequence:
            raise ProtocolConflict(
                f"edge {edge!r} expected contiguous sequence "
                f"{expected_sequence}, got {sequence}"
            )
        self.connection.execute(
            "INSERT INTO messages VALUES (?, ?, ?, ?)",
            (edge, logical_id, sequence, payload),
        )
        self.connection.execute(
            """
            INSERT INTO edge_heads VALUES (?, ?)
            ON CONFLICT(edge) DO UPDATE SET last_sequence = excluded.last_sequence
            """,
            (edge, sequence),
        )
        self.connection.commit()

    def start_checkpoint(
        self, checkpoint_id: int, required_nodes: Iterable[str]
    ) -> None:
        required_nodes_json = json.dumps(sorted(set(required_nodes)))
        existing = self.connection.execute(
            """
            SELECT required_nodes_json FROM checkpoint_decisions
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        if existing is not None:
            if existing[0] != required_nodes_json:
                raise ProtocolConflict(
                    f"checkpoint {checkpoint_id} participant manifest changed"
                )
            return
        self.connection.execute(
            "INSERT INTO checkpoint_decisions VALUES (?, ?, 'STARTED')",
            (checkpoint_id, required_nodes_json),
        )
        self.connection.commit()

    def read_after(self, edge: str, sequence: int) -> list[tuple[str, int, int]]:
        return list(
            self.connection.execute(
                """
                SELECT logical_id, sequence, payload
                FROM messages
                WHERE edge = ? AND sequence > ?
                ORDER BY sequence
                """,
                (edge, sequence),
            )
        )

    def save_snapshot(
        self, checkpoint_id: int, node: str, state: dict[str, Any]
    ) -> None:
        checkpoint = self.connection.execute(
            """
            SELECT required_nodes_json, status FROM checkpoint_decisions
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        if checkpoint is None:
            raise ProtocolConflict(f"checkpoint {checkpoint_id} was not started")
        required_nodes = set(json.loads(checkpoint[0]))
        if node not in required_nodes:
            raise ProtocolConflict(
                f"node {node!r} is not in checkpoint {checkpoint_id} manifest"
            )
        state_json = json.dumps(state, sort_keys=True)
        existing = self.connection.execute(
            """
            SELECT state_json FROM snapshots
            WHERE checkpoint_id = ? AND node = ?
            """,
            (checkpoint_id, node),
        ).fetchone()
        if existing is not None:
            if existing[0] != state_json:
                raise ProtocolConflict(
                    f"snapshot {(checkpoint_id, node)!r} is immutable"
                )
            return
        self.connection.execute(
            "INSERT INTO snapshots VALUES (?, ?, ?)",
            (checkpoint_id, node, state_json),
        )
        self.connection.commit()

    def complete_checkpoint(self, checkpoint_id: int) -> bool:
        checkpoint = self.connection.execute(
            """
            SELECT required_nodes_json FROM checkpoint_decisions
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        ).fetchone()
        if checkpoint is None:
            raise ProtocolConflict(f"checkpoint {checkpoint_id} was not started")
        required = set(json.loads(checkpoint[0]))
        present = {
            row[0]
            for row in self.connection.execute(
                "SELECT node FROM snapshots WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )
        }
        if present != required:
            return False
        self.connection.execute(
            """
            UPDATE checkpoint_decisions SET status = 'COMPLETED'
            WHERE checkpoint_id = ?
            """,
            (checkpoint_id,),
        )
        self.connection.commit()
        return True

    def latest_checkpoint(self) -> int | None:
        row = self.connection.execute(
            """
            SELECT MAX(checkpoint_id) FROM checkpoint_decisions
            WHERE status = 'COMPLETED'
            """
        ).fetchone()
        return row[0]

    def load_snapshot(self, checkpoint_id: int, node: str) -> dict[str, Any]:
        row = self.connection.execute(
            """
            SELECT state_json FROM snapshots
            WHERE checkpoint_id = ? AND node = ?
            """,
            (checkpoint_id, node),
        ).fetchone()
        if row is None:
            raise KeyError((checkpoint_id, node))
        return json.loads(row[0])


@dataclass
class MapOperator:
    name: str
    input_edge: str
    output_edge: str
    multiplier: int
    cursor: int = 0
    execution_count: int = 0

    def run_available(self, store: DurableGraphStore) -> None:
        for logical_id, sequence, payload in store.read_after(
            self.input_edge, self.cursor
        ):
            self.execution_count += 1
            store.append(
                self.output_edge,
                logical_id,
                sequence,
                payload * self.multiplier,
            )
            self.cursor = sequence

    def snapshot(self) -> dict[str, int]:
        return {"cursor": self.cursor}

    def restore(self, snapshot: dict[str, Any]) -> None:
        self.cursor = int(snapshot["cursor"])


@dataclass
class JoinSumOperator:
    left_edge: str
    right_edge: str
    left_cursor: int = 0
    right_cursor: int = 0
    total: int = 0

    def run_available(self, store: DurableGraphStore) -> None:
        for _, sequence, payload in store.read_after(self.left_edge, self.left_cursor):
            self.total += payload
            self.left_cursor = sequence
        for _, sequence, payload in store.read_after(
            self.right_edge, self.right_cursor
        ):
            self.total += payload
            self.right_cursor = sequence

    def snapshot(self) -> dict[str, int]:
        return {
            "left_cursor": self.left_cursor,
            "right_cursor": self.right_cursor,
            "total": self.total,
        }

    def restore(self, snapshot: dict[str, Any]) -> None:
        self.left_cursor = int(snapshot["left_cursor"])
        self.right_cursor = int(snapshot["right_cursor"])
        self.total = int(snapshot["total"])


class PreparedSink:
    """A sink participant with durable prepare and idempotent commit."""

    def __init__(self, path: Path, sink_id: str):
        self.sink_id = sink_id
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS prepared (
                transaction_id TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS visible (
                transaction_id TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )

    def prepare(self, transaction_id: str, value: str) -> None:
        existing = self.connection.execute(
            "SELECT value FROM prepared WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        if existing is not None:
            if existing[0] != value:
                raise ProtocolConflict(
                    f"sink {self.sink_id!r} transaction {transaction_id!r} "
                    "changed value"
                )
            return
        self.connection.execute(
            "INSERT INTO prepared VALUES (?, ?)", (transaction_id, value)
        )
        self.connection.commit()

    def commit(self, transaction_id: str) -> None:
        row = self.connection.execute(
            "SELECT value FROM prepared WHERE transaction_id = ?",
            (transaction_id,),
        ).fetchone()
        if row is None:
            raise KeyError(transaction_id)
        self.connection.execute(
            "INSERT OR IGNORE INTO visible VALUES (?, ?)",
            (transaction_id, row[0]),
        )
        self.connection.commit()

    def visible_values(self) -> list[tuple[str, str]]:
        return list(
            self.connection.execute(
                "SELECT transaction_id, value FROM visible ORDER BY transaction_id"
            )
        )


class MultiSinkCoordinator:
    """A durable-decision coordinator for prepared sink participants."""

    def __init__(self, path: Path, sinks: list[PreparedSink]):
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS decisions (
                transaction_id TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                participants_json TEXT NOT NULL,
                decision TEXT CHECK (decision IN ('COMMIT'))
            )
            """
        )
        self.sinks = sinks

    def write(
        self,
        transaction_id: str,
        value: str,
        crash_after_sink: int | None = None,
    ) -> None:
        participant_ids = [sink.sink_id for sink in self.sinks]
        if len(participant_ids) != len(set(participant_ids)):
            raise ProtocolConflict("sink IDs must be unique")
        participants_json = json.dumps(participant_ids)
        existing = self.connection.execute(
            """
            SELECT value, participants_json FROM decisions
            WHERE transaction_id = ?
            """,
            (transaction_id,),
        ).fetchone()
        if existing is not None and existing != (value, participants_json):
            raise ProtocolConflict(f"transaction {transaction_id!r} manifest changed")
        if existing is None:
            self.connection.execute(
                "INSERT INTO decisions VALUES (?, ?, ?, NULL)",
                (transaction_id, value, participants_json),
            )
            self.connection.commit()

        for sink in self.sinks:
            sink.prepare(transaction_id, value)

        # The decision must be durable before the first external commit.
        self.connection.execute(
            "UPDATE decisions SET decision = 'COMMIT' WHERE transaction_id = ?",
            (transaction_id,),
        )
        self.connection.commit()

        for index, sink in enumerate(self.sinks, start=1):
            sink.commit(transaction_id)
            if crash_after_sink == index:
                raise InjectedCrash(f"crash after committing sink {index}")

    def recover(self) -> None:
        transactions = [
            row
            for row in self.connection.execute(
                """
                SELECT transaction_id, participants_json
                FROM decisions WHERE decision = 'COMMIT'
                """
            )
        ]
        participant_ids = [sink.sink_id for sink in self.sinks]
        for transaction_id, participants_json in transactions:
            if participant_ids != json.loads(participants_json):
                raise ProtocolConflict(
                    f"transaction {transaction_id!r} recovery participants changed"
                )
            for sink in self.sinks:
                sink.commit(transaction_id)
