"""Sprint 3 cooperative sink contract and durable reference participant."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


class SinkProtocolError(RuntimeError):
    pass


class ParticipantState(str, Enum):
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"


@dataclass(frozen=True)
class SinkRecovery:
    transaction_id: str
    token: str
    payload_digest: str
    state: ParticipantState
    coordinator_epoch: int
    participant_epoch: int


@runtime_checkable
class CooperativeSinkV1(Protocol):
    protocol_version: int
    sink_id: str

    def fence(
        self,
        transaction_id: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None: ...

    def prepare(
        self,
        transaction_id: str,
        payload: Any,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> str: ...

    def commit(
        self,
        transaction_id: str,
        token: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None: ...

    def abort(
        self,
        transaction_id: str,
        token: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None: ...

    def recover(self, transaction_id: str) -> SinkRecovery | None: ...


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class DurableCooperativeSink:
    """SQLite reference sink; protocol evidence, not a production adapter."""

    protocol_version = 1

    def __init__(self, path: Path, sink_id: str):
        self.sink_id = sink_id
        self.connection = sqlite3.connect(path)
        self.connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS sink_owners (
                sink_id TEXT PRIMARY KEY,
                participant_epoch INTEGER NOT NULL CHECK (participant_epoch > 0)
            );
            CREATE TABLE IF NOT EXISTS sink_transaction_fences (
                sink_id TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                coordinator_epoch INTEGER NOT NULL CHECK (coordinator_epoch > 0),
                participant_epoch INTEGER NOT NULL CHECK (participant_epoch > 0),
                PRIMARY KEY (sink_id, transaction_id)
            );
            CREATE TABLE IF NOT EXISTS sink_transactions (
                sink_id TEXT NOT NULL,
                transaction_id TEXT NOT NULL,
                token TEXT NOT NULL,
                payload_digest TEXT NOT NULL,
                state TEXT NOT NULL CHECK (
                    state IN ('PREPARED', 'COMMITTED', 'ABORTED')
                ),
                coordinator_epoch INTEGER NOT NULL CHECK (coordinator_epoch > 0),
                participant_epoch INTEGER NOT NULL CHECK (participant_epoch > 0),
                PRIMARY KEY (sink_id, transaction_id)
            );
            CREATE TRIGGER IF NOT EXISTS sink_transaction_identity_immutable
            BEFORE UPDATE OF sink_id, transaction_id, token, payload_digest
            ON sink_transactions
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_owner_insert_is_create_only
            BEFORE INSERT ON sink_owners
            WHEN EXISTS (
                SELECT 1 FROM sink_owners current
                WHERE current.sink_id = NEW.sink_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink owner already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_owner_epoch_is_monotonic
            BEFORE UPDATE OF participant_epoch ON sink_owners
            WHEN NEW.participant_epoch < OLD.participant_epoch
            BEGIN
                SELECT RAISE(ABORT, 'sink owner epoch is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_fence_identity_is_immutable
            BEFORE UPDATE OF sink_id, transaction_id ON sink_transaction_fences
            BEGIN
                SELECT RAISE(ABORT, 'sink fence identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_fence_epoch_is_monotonic
            BEFORE UPDATE OF coordinator_epoch ON sink_transaction_fences
            WHEN NEW.coordinator_epoch < OLD.coordinator_epoch
            BEGIN
                SELECT RAISE(ABORT, 'sink coordinator epoch is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_fence_participant_is_immutable
            BEFORE UPDATE OF participant_epoch ON sink_transaction_fences
            WHEN NEW.participant_epoch < OLD.participant_epoch
            BEGIN
                SELECT RAISE(ABORT, 'sink fence participant epoch is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_fence_insert_is_create_only
            BEFORE INSERT ON sink_transaction_fences
            WHEN EXISTS (
                SELECT 1 FROM sink_transaction_fences current
                WHERE current.sink_id = NEW.sink_id
                  AND current.transaction_id = NEW.transaction_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink fence already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_fence_delete_requires_retention
            BEFORE DELETE ON sink_transaction_fences
            BEGIN
                SELECT RAISE(ABORT, 'sink fence deletion requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_owner_identity_is_immutable
            BEFORE UPDATE OF sink_id ON sink_owners
            BEGIN
                SELECT RAISE(ABORT, 'sink owner identity is immutable');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_owner_delete_requires_retention
            BEFORE DELETE ON sink_owners
            BEGIN
                SELECT RAISE(ABORT, 'sink owner deletion requires retention protocol');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_transaction_insert_is_create_only
            BEFORE INSERT ON sink_transactions
            WHEN EXISTS (
                SELECT 1 FROM sink_transactions current
                WHERE current.sink_id = NEW.sink_id
                  AND current.transaction_id = NEW.transaction_id
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction already exists');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_transaction_insert_requires_owner
            BEFORE INSERT ON sink_transactions
            WHEN NOT EXISTS (
                SELECT 1 FROM sink_owners owner
                WHERE owner.sink_id = NEW.sink_id
                  AND owner.participant_epoch = NEW.participant_epoch
            ) OR NOT EXISTS (
                SELECT 1 FROM sink_transaction_fences fence
                WHERE fence.sink_id = NEW.sink_id
                  AND fence.transaction_id = NEW.transaction_id
                  AND fence.coordinator_epoch = NEW.coordinator_epoch
                  AND fence.participant_epoch = NEW.participant_epoch
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction requires current owner');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_transaction_must_start_prepared
            BEFORE INSERT ON sink_transactions
            WHEN NEW.state != 'PREPARED'
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction must start prepared');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_transaction_fences_are_monotonic
            BEFORE UPDATE OF coordinator_epoch, participant_epoch
            ON sink_transactions
            WHEN NEW.coordinator_epoch < OLD.coordinator_epoch
              OR NEW.participant_epoch < OLD.participant_epoch
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction fence is not monotonic');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_transaction_update_requires_owner
            BEFORE UPDATE ON sink_transactions
            WHEN NOT EXISTS (
                SELECT 1 FROM sink_owners owner
                WHERE owner.sink_id = NEW.sink_id
                  AND owner.participant_epoch = NEW.participant_epoch
            ) OR NOT EXISTS (
                SELECT 1 FROM sink_transaction_fences fence
                WHERE fence.sink_id = NEW.sink_id
                  AND fence.transaction_id = NEW.transaction_id
                  AND fence.coordinator_epoch = NEW.coordinator_epoch
                  AND fence.participant_epoch = NEW.participant_epoch
            )
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction requires current owner');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_terminal_state_is_irreversible
            BEFORE UPDATE OF state ON sink_transactions
            WHEN
                (OLD.state = 'COMMITTED' AND NEW.state != 'COMMITTED')
                OR (OLD.state = 'ABORTED' AND NEW.state != 'ABORTED')
            BEGIN
                SELECT RAISE(ABORT, 'sink terminal state is irreversible');
            END;
            CREATE TRIGGER IF NOT EXISTS sink_transaction_delete_requires_retention
            BEFORE DELETE ON sink_transactions
            BEGIN
                SELECT RAISE(ABORT, 'sink transaction deletion requires retention protocol');
            END;
            """
        )

    def claim_generation(self, participant_epoch: int) -> None:
        if participant_epoch < 1:
            raise SinkProtocolError("participant epoch must be positive")
        with self.connection:
            current = self.connection.execute(
                "SELECT participant_epoch FROM sink_owners WHERE sink_id = ?",
                (self.sink_id,),
            ).fetchone()
            if current is None:
                if participant_epoch != 1:
                    raise SinkProtocolError("first participant epoch must be one")
                self.connection.execute(
                    """
                    INSERT INTO sink_owners (sink_id, participant_epoch)
                    VALUES (?, ?)
                    """,
                    (self.sink_id, participant_epoch),
                )
                return
            if current[0] == participant_epoch:
                return
            if participant_epoch <= current[0]:
                raise SinkProtocolError("participant epoch must increase")
            cursor = self.connection.execute(
                """
                UPDATE sink_owners SET participant_epoch = ?
                WHERE sink_id = ? AND participant_epoch = ?
                """,
                (participant_epoch, self.sink_id, current[0]),
            )
            if cursor.rowcount != 1:
                raise SinkProtocolError("participant generation CAS failed")

    def fence(
        self,
        transaction_id: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        if coordinator_epoch < 1:
            raise SinkProtocolError("coordinator epoch must be positive")
        with self.connection:
            self._guard_owner(participant_epoch)
            self._advance_coordinator_fence(
                transaction_id, coordinator_epoch, participant_epoch
            )

    def prepare(
        self,
        transaction_id: str,
        payload: Any,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> str:
        payload_digest = _digest(payload)
        token = _digest(
            {
                "version": self.protocol_version,
                "sink_id": self.sink_id,
                "transaction_id": transaction_id,
                "payload_digest": payload_digest,
            }
        )
        with self.connection:
            self._guard_owner(participant_epoch)
            self._advance_coordinator_fence(
                transaction_id, coordinator_epoch, participant_epoch
            )
            existing = self.connection.execute(
                """
                SELECT token, payload_digest, state, coordinator_epoch,
                       participant_epoch
                FROM sink_transactions
                WHERE sink_id = ? AND transaction_id = ?
                """,
                (self.sink_id, transaction_id),
            ).fetchone()
            if existing is None:
                self.connection.execute(
                    "INSERT INTO sink_transactions VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        self.sink_id,
                        transaction_id,
                        token,
                        payload_digest,
                        ParticipantState.PREPARED.value,
                        coordinator_epoch,
                        participant_epoch,
                    ),
                )
                return token
            if existing[:2] != (token, payload_digest):
                raise SinkProtocolError("prepare identity or payload drift")
            self._guard_coordinator(existing[3], coordinator_epoch)
            if ParticipantState(existing[2]) is not ParticipantState.PREPARED:
                raise SinkProtocolError(
                    f"cannot prepare {existing[2].lower()} transaction"
                )
            self.connection.execute(
                """
                UPDATE sink_transactions
                SET coordinator_epoch = MAX(coordinator_epoch, ?),
                    participant_epoch = ?
                WHERE sink_id = ? AND transaction_id = ?
                """,
                (
                    coordinator_epoch,
                    participant_epoch,
                    self.sink_id,
                    transaction_id,
                ),
            )
            return token

    def commit(
        self,
        transaction_id: str,
        token: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        self._finish(
            transaction_id,
            token,
            coordinator_epoch,
            participant_epoch,
            ParticipantState.COMMITTED,
        )

    def abort(
        self,
        transaction_id: str,
        token: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        self._finish(
            transaction_id,
            token,
            coordinator_epoch,
            participant_epoch,
            ParticipantState.ABORTED,
        )

    def _finish(
        self,
        transaction_id: str,
        token: str,
        coordinator_epoch: int,
        participant_epoch: int,
        target: ParticipantState,
    ) -> None:
        with self.connection:
            self._guard_owner(participant_epoch)
            self._advance_coordinator_fence(
                transaction_id, coordinator_epoch, participant_epoch
            )
            row = self.connection.execute(
                """
                SELECT token, state, coordinator_epoch FROM sink_transactions
                WHERE sink_id = ? AND transaction_id = ?
                """,
                (self.sink_id, transaction_id),
            ).fetchone()
            if row is None:
                raise SinkProtocolError("unknown sink transaction")
            if row[0] != token:
                raise SinkProtocolError("prepare token mismatch")
            self._guard_coordinator(row[2], coordinator_epoch)
            current = ParticipantState(row[1])
            if current is target:
                self.connection.execute(
                    """
                    UPDATE sink_transactions
                    SET coordinator_epoch = ?, participant_epoch = ?
                    WHERE sink_id = ? AND transaction_id = ? AND state = ?
                    """,
                    (
                        coordinator_epoch,
                        participant_epoch,
                        self.sink_id,
                        transaction_id,
                        target.value,
                    ),
                )
                return
            if current is not ParticipantState.PREPARED:
                raise SinkProtocolError(
                    f"cannot {target.value.lower()} {current.value.lower()} transaction"
                )
            self.connection.execute(
                """
                UPDATE sink_transactions
                SET state = ?, coordinator_epoch = ?, participant_epoch = ?
                WHERE sink_id = ? AND transaction_id = ? AND state = 'PREPARED'
                """,
                (
                    target.value,
                    coordinator_epoch,
                    participant_epoch,
                    self.sink_id,
                    transaction_id,
                ),
            )

    def recover(self, transaction_id: str) -> SinkRecovery | None:
        row = self.connection.execute(
            """
            SELECT token, payload_digest, state, coordinator_epoch,
                   participant_epoch
            FROM sink_transactions
            WHERE sink_id = ? AND transaction_id = ?
            """,
            (self.sink_id, transaction_id),
        ).fetchone()
        if row is None:
            return None
        return SinkRecovery(
            transaction_id,
            row[0],
            row[1],
            ParticipantState(row[2]),
            row[3],
            row[4],
        )

    def _guard_owner(self, participant_epoch: int) -> None:
        cursor = self.connection.execute(
            """
            UPDATE sink_owners SET participant_epoch = participant_epoch
            WHERE sink_id = ? AND participant_epoch = ?
            """,
            (self.sink_id, participant_epoch),
        )
        if cursor.rowcount != 1:
            raise SinkProtocolError("stale participant generation")

    def _advance_coordinator_fence(
        self,
        transaction_id: str,
        coordinator_epoch: int,
        participant_epoch: int,
    ) -> None:
        current = self.connection.execute(
            """
            SELECT coordinator_epoch, participant_epoch
            FROM sink_transaction_fences
            WHERE sink_id = ? AND transaction_id = ?
            """,
            (self.sink_id, transaction_id),
        ).fetchone()
        if current is None:
            self.connection.execute(
                "INSERT INTO sink_transaction_fences VALUES (?, ?, ?, ?)",
                (
                    self.sink_id,
                    transaction_id,
                    coordinator_epoch,
                    participant_epoch,
                ),
            )
            return
        if participant_epoch < current[1]:
            raise SinkProtocolError("stale participant generation for transaction")
        self._guard_coordinator(current[0], coordinator_epoch)
        self.connection.execute(
            """
            UPDATE sink_transaction_fences
            SET coordinator_epoch = ?, participant_epoch = ?
            WHERE sink_id = ? AND transaction_id = ? AND coordinator_epoch <= ?
            """,
            (
                coordinator_epoch,
                participant_epoch,
                self.sink_id,
                transaction_id,
                coordinator_epoch,
            ),
        )

    @staticmethod
    def _guard_coordinator(current: int, proposed: int) -> None:
        if proposed < current:
            raise SinkProtocolError("stale coordinator epoch")
