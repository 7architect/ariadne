"""ST-033 replayable-source and accepted-input ledger prototype."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class SourceProtocolError(RuntimeError):
    pass


class SourceInjectedFailure(RuntimeError):
    pass


def value_digest(v: Any) -> str:
    return hashlib.sha256(
        json.dumps(v, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def source_record_id(partition: str, position: str) -> str:
    raw = json.dumps(["source-record-v1", partition, position], separators=(",", ":"))
    return "source-record:" + hashlib.sha256(raw.encode()).hexdigest()


@dataclass(frozen=True)
class CombinedDecisionRef:
    job_id: str
    graph_version_id: str
    checkpoint_id: str
    transaction_id: str
    sequence: int
    intent_digest: str
    capability_digest: str
    coordinator_owner_id: str
    coordinator_epoch: int
    source_positions: tuple[tuple[str, str | None], ...]
    source_epochs: tuple[tuple[str, int], ...]
    decision: str = "COMMIT"

    @property
    def canonical_json(self) -> str:
        return json.dumps(self.__dict__, sort_keys=True, separators=(",", ":"))

    @property
    def digest(self) -> str:
        return value_digest(json.loads(self.canonical_json))


@dataclass(frozen=True)
class SourceEnvelope:
    partition_id: str
    opaque_position: str
    record_id: str
    lineage_digest: str
    payload_digest: str
    payload: Any


@dataclass(frozen=True)
class SourceRecord(SourceEnvelope):
    ordinal: int


class ReplayableSourceV1(Protocol):
    def fence(self, partition_id: str, participant_epoch: int) -> int: ...
    def read(
        self, partition_id: str, after_position: str | None
    ) -> Iterable[SourceEnvelope]: ...
    # The effect key is (partition, position, ordinal). Epoch only fences callers;
    # a higher fenced owner may confirm the same durable effect after takeover.
    def acknowledge(
        self, partition_id: str, position: str, ordinal: int, epoch: int
    ) -> None: ...
    def recover_ack(self, partition_id: str) -> str | None: ...


class AcceptedInputLedger:
    def __init__(self, path: Path, decision_db_path: Path | None = None):
        self.decision_db_path = decision_db_path
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.create_function(
            "source_record_id", 2, source_record_id, deterministic=True
        )
        self.connection.create_function(
            "payload_digest",
            1,
            lambda payload: value_digest(json.loads(payload)),
            deterministic=True,
        )
        self.connection.executescript("""
        CREATE TABLE IF NOT EXISTS partitions(
          partition_id TEXT PRIMARY KEY, owner_id TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>0),
          state TEXT NOT NULL CHECK(state IN('FENCING','ACTIVE')),
          ack_ordinal INTEGER NOT NULL DEFAULT 0 CHECK(ack_ordinal>=0));
        CREATE TABLE IF NOT EXISTS fence_acks(
          partition_id TEXT NOT NULL, owner_id TEXT NOT NULL, epoch INTEGER NOT NULL,
          PRIMARY KEY(partition_id,owner_id,epoch),
          FOREIGN KEY(partition_id) REFERENCES partitions(partition_id));
        CREATE TABLE IF NOT EXISTS accepted_records(
          partition_id TEXT NOT NULL, ordinal INTEGER NOT NULL, record_id TEXT NOT NULL UNIQUE,
          position TEXT NOT NULL, lineage TEXT NOT NULL, payload_digest TEXT NOT NULL, payload_json TEXT NOT NULL,
          writer_owner_id TEXT NOT NULL, writer_epoch INTEGER NOT NULL,
          PRIMARY KEY(partition_id,ordinal), UNIQUE(partition_id,position),
          FOREIGN KEY(partition_id) REFERENCES partitions(partition_id));
        CREATE VIEW IF NOT EXISTS channel_records AS SELECT * FROM accepted_records;
        CREATE TABLE IF NOT EXISTS checkpoints(
          checkpoint_id TEXT PRIMARY KEY, decision_digest TEXT NOT NULL UNIQUE,
          decision_json TEXT NOT NULL, expected INTEGER NOT NULL CHECK(expected>0),
          phase TEXT NOT NULL CHECK(phase IN('BUILDING','COMMIT')));
        CREATE TABLE IF NOT EXISTS cuts(
          checkpoint_id TEXT NOT NULL, partition_id TEXT NOT NULL, ordinal INTEGER NOT NULL, position TEXT,
          PRIMARY KEY(checkpoint_id,partition_id), FOREIGN KEY(checkpoint_id) REFERENCES checkpoints(checkpoint_id),
          FOREIGN KEY(partition_id) REFERENCES partitions(partition_id),
          CHECK((ordinal=0 AND position IS NULL) OR (ordinal>0 AND position IS NOT NULL)));
        CREATE TABLE IF NOT EXISTS ack_attempts(
          checkpoint_id TEXT NOT NULL, manifest TEXT NOT NULL, partition_id TEXT NOT NULL,
          ordinal INTEGER NOT NULL, position TEXT NOT NULL, owner_id TEXT NOT NULL, epoch INTEGER NOT NULL,
          attempt_count INTEGER NOT NULL, outcome TEXT NOT NULL CHECK(outcome IN('IN_PROGRESS','BLOCKED','SUCCEEDED','SUBSUMED')),
          last_error TEXT, PRIMARY KEY(checkpoint_id,partition_id),
          FOREIGN KEY(checkpoint_id,partition_id) REFERENCES cuts(checkpoint_id,partition_id));
        CREATE TRIGGER IF NOT EXISTS accepted_contiguous BEFORE INSERT ON accepted_records
        WHEN NEW.ordinal!=(SELECT COALESCE(MAX(ordinal),0)+1 FROM accepted_records WHERE partition_id=NEW.partition_id)
        BEGIN SELECT RAISE(ABORT,'accepted source ordinal is not contiguous'); END;
        CREATE TRIGGER IF NOT EXISTS accepted_identity BEFORE INSERT ON accepted_records
        WHEN NEW.record_id!=source_record_id(NEW.partition_id,NEW.position)
          OR NEW.payload_digest!=payload_digest(NEW.payload_json)
          OR NOT EXISTS(SELECT 1 FROM partitions p WHERE p.partition_id=NEW.partition_id
                        AND p.owner_id=NEW.writer_owner_id AND p.epoch=NEW.writer_epoch
                        AND p.state='ACTIVE')
        BEGIN SELECT RAISE(ABORT,'noncanonical accepted record'); END;
        CREATE TRIGGER IF NOT EXISTS accepted_no_update BEFORE UPDATE ON accepted_records BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS accepted_no_delete BEFORE DELETE ON accepted_records BEGIN SELECT RAISE(ABORT,'cannot delete'); END;
        CREATE TRIGGER IF NOT EXISTS cuts_no_update BEFORE UPDATE ON cuts BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER IF NOT EXISTS cuts_no_delete BEFORE DELETE ON cuts BEGIN SELECT RAISE(ABORT,'cannot delete'); END;
        CREATE TRIGGER IF NOT EXISTS cuts_create_only BEFORE INSERT ON cuts
        WHEN EXISTS(SELECT 1 FROM cuts WHERE checkpoint_id=NEW.checkpoint_id AND partition_id=NEW.partition_id)
        BEGIN SELECT RAISE(ABORT,'cut already exists'); END;
        CREATE TRIGGER IF NOT EXISTS cut_valid BEFORE INSERT ON cuts WHEN
          NOT EXISTS(SELECT 1 FROM checkpoints WHERE checkpoint_id=NEW.checkpoint_id AND phase='BUILDING') OR
          (NEW.ordinal=0 AND EXISTS(SELECT 1 FROM accepted_records WHERE partition_id=NEW.partition_id)) OR
          (NEW.ordinal>0 AND NOT EXISTS(SELECT 1 FROM accepted_records WHERE partition_id=NEW.partition_id AND ordinal=NEW.ordinal AND position=NEW.position))
        BEGIN SELECT RAISE(ABORT,'cut not accepted'); END;
        CREATE TRIGGER IF NOT EXISTS checkpoint_initial_state BEFORE INSERT ON checkpoints WHEN NEW.phase!='BUILDING'
        BEGIN SELECT RAISE(ABORT,'checkpoint must start BUILDING'); END;
        CREATE TRIGGER IF NOT EXISTS checkpoint_create_only BEFORE INSERT ON checkpoints
        WHEN EXISTS(SELECT 1 FROM checkpoints WHERE checkpoint_id=NEW.checkpoint_id)
        BEGIN SELECT RAISE(ABORT,'checkpoint already exists'); END;
        CREATE TRIGGER IF NOT EXISTS checkpoint_commit BEFORE UPDATE ON checkpoints WHEN
          OLD.phase!='BUILDING' OR NEW.phase!='COMMIT'
          OR NEW.checkpoint_id!=OLD.checkpoint_id OR NEW.decision_digest!=OLD.decision_digest
          OR NEW.decision_json!=OLD.decision_json OR NEW.expected!=OLD.expected
          OR NEW.expected!=(SELECT COUNT(*) FROM cuts WHERE checkpoint_id=NEW.checkpoint_id)
        BEGIN SELECT RAISE(ABORT,'COMMIT requires exact cuts'); END;
        CREATE TRIGGER IF NOT EXISTS checkpoint_no_delete BEFORE DELETE ON checkpoints
        BEGIN SELECT RAISE(ABORT,'checkpoint cannot be deleted'); END;
        CREATE TRIGGER IF NOT EXISTS partition_create_only BEFORE INSERT ON partitions
        WHEN EXISTS(SELECT 1 FROM partitions WHERE partition_id=NEW.partition_id)
        BEGIN SELECT RAISE(ABORT,'partition already exists'); END;
        CREATE TRIGGER IF NOT EXISTS partition_initial_state BEFORE INSERT ON partitions
        WHEN NEW.state!='FENCING' OR NEW.ack_ordinal!=0 OR EXISTS(SELECT 1 FROM checkpoints)
        BEGIN SELECT RAISE(ABORT,'partition must start FENCING at zero'); END;
        CREATE TRIGGER IF NOT EXISTS partition_no_delete BEFORE DELETE ON partitions
        BEGIN SELECT RAISE(ABORT,'partition cannot be deleted'); END;
        CREATE TRIGGER IF NOT EXISTS partition_transition_guard BEFORE UPDATE ON partitions
        WHEN NEW.partition_id!=OLD.partition_id OR NEW.epoch<OLD.epoch
          OR (NEW.owner_id!=OLD.owner_id AND NEW.epoch<=OLD.epoch)
          OR NOT (
            (NEW.epoch>OLD.epoch AND NEW.state='FENCING' AND NEW.ack_ordinal=OLD.ack_ordinal)
            OR (NEW.epoch=OLD.epoch AND NEW.owner_id=OLD.owner_id
                AND OLD.state='FENCING' AND NEW.state='ACTIVE' AND NEW.ack_ordinal=OLD.ack_ordinal
                AND EXISTS(SELECT 1 FROM fence_acks f WHERE f.partition_id=NEW.partition_id
                           AND f.owner_id=NEW.owner_id AND f.epoch=NEW.epoch))
            OR (NEW.epoch=OLD.epoch AND NEW.owner_id=OLD.owner_id
                AND OLD.state='ACTIVE' AND NEW.state='ACTIVE' AND NEW.ack_ordinal>=OLD.ack_ordinal)
          )
        BEGIN SELECT RAISE(ABORT,'illegal partition authority transition'); END;
        CREATE TRIGGER IF NOT EXISTS partition_ack_guard BEFORE UPDATE OF ack_ordinal ON partitions
        WHEN NEW.ack_ordinal!=OLD.ack_ordinal AND (
          NEW.ack_ordinal>(SELECT COALESCE(MAX(ordinal),0) FROM accepted_records WHERE partition_id=NEW.partition_id)
          OR NOT EXISTS(SELECT 1 FROM checkpoints d JOIN cuts c USING(checkpoint_id)
                        WHERE d.phase='COMMIT' AND c.partition_id=NEW.partition_id
                          AND c.ordinal=NEW.ack_ordinal)
        )
        BEGIN SELECT RAISE(ABORT,'ACK requires committed accepted cut'); END;
        CREATE TRIGGER IF NOT EXISTS fence_ack_guard BEFORE INSERT ON fence_acks
        WHEN NOT EXISTS(SELECT 1 FROM partitions p WHERE p.partition_id=NEW.partition_id
                        AND p.owner_id=NEW.owner_id AND p.epoch=NEW.epoch AND p.state='FENCING')
        BEGIN SELECT RAISE(ABORT,'fence ACK requires matching FENCING claim'); END;
        CREATE TRIGGER IF NOT EXISTS fence_ack_no_update BEFORE UPDATE ON fence_acks
        BEGIN SELECT RAISE(ABORT,'fence ACK is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS fence_ack_no_delete BEFORE DELETE ON fence_acks
        BEGIN SELECT RAISE(ABORT,'fence ACK cannot be deleted'); END;
        """)

    def claim_partition(
        self, p: str, owner: str, epoch: int, fence: Callable[[str, int], int]
    ) -> None:
        if epoch <= 0:
            raise SourceProtocolError("source epoch must be positive")
        row = self.connection.execute(
            "SELECT owner_id,epoch,state FROM partitions WHERE partition_id=?", (p,)
        ).fetchone()
        if row == (owner, epoch, "ACTIVE"):
            return
        if row and (epoch < row[1] or (epoch == row[1] and owner != row[0])):
            raise SourceProtocolError("stale source epoch")
        with self.connection:
            if row is None:
                self.connection.execute(
                    "INSERT INTO partitions VALUES(?,?,?,'FENCING',0)",
                    (p, owner, epoch),
                )
            elif row[:2] != (owner, epoch):
                changed = self.connection.execute(
                    "UPDATE partitions SET owner_id=?,epoch=?,state='FENCING' WHERE partition_id=? AND owner_id=? AND epoch=?",
                    (owner, epoch, p, row[0], row[1]),
                )
                if changed.rowcount != 1:
                    raise SourceProtocolError("source takeover CAS lost")
        if fence(p, epoch) != epoch:
            raise SourceProtocolError("external source did not confirm exact epoch")
        with self.connection:
            self.connection.execute(
                "INSERT OR IGNORE INTO fence_acks VALUES(?,?,?)", (p, owner, epoch)
            )
            c = self.connection.execute(
                "UPDATE partitions SET state='ACTIVE' WHERE partition_id=? AND owner_id=? AND epoch=? AND state='FENCING'",
                (p, owner, epoch),
            )
            if c.rowcount != 1:
                raise SourceProtocolError("stale fence acknowledgement")

    def _owner(self, p: str, o: str, e: int) -> None:
        if self.connection.execute(
            "SELECT owner_id,epoch,state FROM partitions WHERE partition_id=?", (p,)
        ).fetchone() != (o, e, "ACTIVE"):
            raise SourceProtocolError("stale source owner")

    def accept(
        self,
        x: SourceEnvelope,
        owner: str,
        epoch: int,
        *,
        fault_point: str | None = None,
        race_hook: Callable[[], None] | None = None,
        crash_hook: Callable[[str], None] | None = None,
    ) -> SourceRecord:
        self._owner(x.partition_id, owner, epoch)
        if x.record_id != source_record_id(x.partition_id, x.opaque_position):
            raise SourceProtocolError("noncanonical record ID")
        if x.payload_digest != value_digest(x.payload):
            raise SourceProtocolError("payload digest mismatch")
        encoded = json.dumps(x.payload, sort_keys=True, separators=(",", ":"))
        old = self.connection.execute(
            "SELECT ordinal,record_id,lineage,payload_digest,payload_json FROM accepted_records WHERE partition_id=? AND position=?",
            (x.partition_id, x.opaque_position),
        ).fetchone()
        if old:
            if old[1:] != (x.record_id, x.lineage_digest, x.payload_digest, encoded):
                raise SourceProtocolError("identity drift")
            return SourceRecord(**x.__dict__, ordinal=old[0])
        if race_hook is not None:
            race_hook()
        try:
            with self.connection:
                r = self.connection.execute(
                    """INSERT INTO accepted_records SELECT ?,COALESCE(MAX(a.ordinal),0)+1,?,?,?,?,?,?,? FROM partitions p LEFT JOIN accepted_records a ON a.partition_id=p.partition_id WHERE p.partition_id=? AND p.owner_id=? AND p.epoch=? AND p.state='ACTIVE' GROUP BY p.partition_id RETURNING ordinal""",
                    (
                        x.partition_id,
                        x.record_id,
                        x.opaque_position,
                        x.lineage_digest,
                        x.payload_digest,
                        encoded,
                        owner,
                        epoch,
                        x.partition_id,
                        owner,
                        epoch,
                    ),
                ).fetchone()
                if r is None:
                    raise SourceProtocolError("stale source owner")
                if crash_hook is not None:
                    crash_hook("accept:after_insert_before_commit")
                if fault_point == "accept:before_commit":
                    raise SourceInjectedFailure(fault_point)
        except sqlite3.IntegrityError as err:
            old = self.connection.execute(
                "SELECT ordinal,record_id,lineage,payload_digest,payload_json FROM accepted_records WHERE partition_id=? AND position=?",
                (x.partition_id, x.opaque_position),
            ).fetchone()
            if old and old[1:] == (
                x.record_id,
                x.lineage_digest,
                x.payload_digest,
                encoded,
            ):
                return SourceRecord(**x.__dict__, ordinal=old[0])
            raise SourceProtocolError("position or identity conflict") from err
        if fault_point == "accept:after_commit_before_ack":
            raise SourceInjectedFailure(fault_point)
        return SourceRecord(**x.__dict__, ordinal=r[0])

    def _verify_combined_decision(self, decision: CombinedDecisionRef) -> bool:
        if self.decision_db_path is None:
            return False
        try:
            authority = sqlite3.connect(
                f"file:{self.decision_db_path}?mode=ro", uri=True
            )
            row = authority.execute(
                """SELECT sequence,manifest_digest,phase,decision,active_owner_id,fence_epoch,intent_json
                   FROM checkpoints WHERE job_id=? AND graph_version_id=? AND checkpoint_id=?""",
                (decision.job_id, decision.graph_version_id, decision.checkpoint_id),
            ).fetchone()
            authority.close()
        except sqlite3.OperationalError:
            return False
        if row is None:
            return False
        intent = json.loads(row[6])
        expected_transaction = "ariadne:transaction:" + value_digest(
            {
                "job": decision.job_id,
                "graph": decision.graph_version_id,
                "checkpoint": decision.checkpoint_id,
            }
        )
        return (
            decision.decision == "COMMIT"
            and decision.transaction_id == expected_transaction
            and row[0] == decision.sequence
            and row[1] == decision.intent_digest
            and row[2] in {"COMMIT_DECIDED", "COMMITTING", "COMPLETED"}
            and row[3] == "COMMIT"
            and row[4] == decision.coordinator_owner_id
            and row[5] == decision.coordinator_epoch
            and intent["capability_digest"] == decision.capability_digest
            and tuple(tuple(item) for item in intent["source_positions"])
            == decision.source_positions
            and tuple(
                sorted(
                    (participant, epoch)
                    for participant, epoch in intent["generation_manifest"]
                    if participant in dict(decision.source_positions)
                )
            )
            == decision.source_epochs
        )

    def bind_committed_checkpoint(
        self,
        decision: CombinedDecisionRef,
        cuts: tuple[tuple[str, int, str | None], ...],
    ) -> None:
        if not self._verify_combined_decision(decision):
            raise SourceProtocolError("unverified combined COMMIT decision")
        parts = {
            r[0] for r in self.connection.execute("SELECT partition_id FROM partitions")
        }
        canonical_cuts = tuple(sorted(cuts))
        if {p for p, _, _ in canonical_cuts} != parts or len(canonical_cuts) != len(
            parts
        ):
            raise SourceProtocolError("exact partition vector required")
        current_epochs = dict(
            self.connection.execute("SELECT partition_id,epoch FROM partitions")
        )
        expected_epochs = dict(decision.source_epochs)
        if set(expected_epochs) != parts or any(
            current_epochs[p] < expected_epochs[p] for p in parts
        ):
            raise SourceProtocolError("source epochs differ from combined decision")
        if tuple((p, pos) for p, _, pos in canonical_cuts) != tuple(
            sorted(decision.source_positions)
        ):
            raise SourceProtocolError("source vector differs from combined decision")
        cid = decision.checkpoint_id
        old = self.connection.execute(
            "SELECT decision_digest,decision_json,phase FROM checkpoints WHERE checkpoint_id=?",
            (cid,),
        ).fetchone()
        if old:
            stored = tuple(
                self.connection.execute(
                    "SELECT partition_id,ordinal,position FROM cuts WHERE checkpoint_id=? ORDER BY partition_id",
                    (cid,),
                )
            )
            if (
                old == (decision.digest, decision.canonical_json, "COMMIT")
                and stored == canonical_cuts
            ):
                return
            raise SourceProtocolError("checkpoint decision or cut drift")
        with self.connection:
            self.connection.execute(
                "INSERT INTO checkpoints VALUES(?,?,?,?,'BUILDING')",
                (cid, decision.digest, decision.canonical_json, len(parts)),
            )
            self.connection.executemany(
                "INSERT INTO cuts VALUES(?,?,?,?)", ((cid, *c) for c in canonical_cuts)
            )
            self.connection.execute(
                "UPDATE checkpoints SET phase='COMMIT' WHERE checkpoint_id=?", (cid,)
            )

    def _reconcile_covered_attempts(self, p: str) -> None:
        with self.connection:
            self.connection.execute(
                """UPDATE ack_attempts
              SET outcome=CASE
                    WHEN ordinal=(SELECT ack_ordinal FROM partitions WHERE partition_id=?) THEN 'SUCCEEDED'
                    ELSE 'SUBSUMED' END,
                  last_error=NULL
              WHERE partition_id=? AND ordinal<=(SELECT ack_ordinal FROM partitions WHERE partition_id=?)
                AND (outcome NOT IN('SUCCEEDED','SUBSUMED')
                     OR ordinal<(SELECT ack_ordinal FROM partitions WHERE partition_id=?))""",
                (p, p, p, p),
            )

    def acknowledge(
        self,
        cid: str,
        decision_digest: str,
        p: str,
        owner: str,
        epoch: int,
        external: Callable[[str, str, int, int], None],
        *,
        fault_point: str | None = None,
        crash_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._owner(p, owner, epoch)
        self._reconcile_covered_attempts(p)
        cut = self.connection.execute(
            "SELECT c.ordinal,c.position FROM cuts c JOIN checkpoints d USING(checkpoint_id) WHERE c.checkpoint_id=? AND c.partition_id=? AND d.decision_digest=? AND d.phase='COMMIT'",
            (cid, p, decision_digest),
        ).fetchone()
        if cut is None:
            raise SourceProtocolError("ACK requires exact COMMIT cut")
        if cut[0] == 0:
            return
        current = self.connection.execute(
            "SELECT ack_ordinal FROM partitions WHERE partition_id=? AND owner_id=? AND epoch=? AND state='ACTIVE'",
            (p, owner, epoch),
        ).fetchone()
        if current is None:
            raise SourceProtocolError("stale source owner")
        if current[0] >= cut[0]:
            return
        with self.connection:
            attempt = self.connection.execute(
                """INSERT INTO ack_attempts VALUES(?,?,?,?,?,?,?,1,'IN_PROGRESS',NULL)
              ON CONFLICT(checkpoint_id,partition_id) DO UPDATE SET
                attempt_count=ack_attempts.attempt_count+1,owner_id=excluded.owner_id,epoch=excluded.epoch,
                outcome='IN_PROGRESS',last_error=NULL
              WHERE ack_attempts.manifest=excluded.manifest AND ack_attempts.ordinal=excluded.ordinal
                AND ack_attempts.position=excluded.position
              RETURNING attempt_count""",
                (cid, decision_digest, p, cut[0], cut[1], owner, epoch),
            ).fetchone()
            if attempt is None:
                raise SourceProtocolError("ACK attempt identity drift")
        if crash_hook is not None:
            crash_hook("ack:after_intent_before_external")
        try:
            external(p, cut[1], cut[0], epoch)
        except (ConnectionError, TimeoutError) as error:
            with self.connection:
                changed = self.connection.execute(
                    """UPDATE ack_attempts SET outcome='BLOCKED',last_error=?
                  WHERE checkpoint_id=? AND partition_id=? AND attempt_count=? AND owner_id=? AND epoch=?
                    AND EXISTS(SELECT 1 FROM partitions WHERE partition_id=? AND owner_id=? AND epoch=? AND state='ACTIVE' AND ack_ordinal<?)""",
                    (
                        f"{type(error).__name__}: {error}",
                        cid,
                        p,
                        attempt[0],
                        owner,
                        epoch,
                        p,
                        owner,
                        epoch,
                        cut[0],
                    ),
                )
                if changed.rowcount == 0:
                    self._reconcile_covered_attempts(p)
            raise
        if fault_point == "ack:after_external_before_local":
            raise SourceInjectedFailure(fault_point)
        with self.connection:
            q = self.connection.execute(
                "UPDATE partitions SET ack_ordinal=? WHERE partition_id=? AND owner_id=? AND epoch=? AND state='ACTIVE' AND ack_ordinal<?",
                (cut[0], p, owner, epoch, cut[0]),
            )
            if q.rowcount != 1:
                raise SourceProtocolError("stale ACK")
            q = self.connection.execute(
                "UPDATE ack_attempts SET outcome='SUCCEEDED',last_error=NULL WHERE checkpoint_id=? AND partition_id=? AND attempt_count=? AND owner_id=? AND epoch=?",
                (cid, p, attempt[0], owner, epoch),
            )
            if q.rowcount != 1:
                raise SourceProtocolError("stale ACK attempt result")
            self._reconcile_covered_attempts(p)

    def replay_plan(self, p: str) -> tuple[SourceRecord, ...]:
        ack = self.connection.execute(
            "SELECT ack_ordinal FROM partitions WHERE partition_id=?", (p,)
        ).fetchone()[0]
        return tuple(
            SourceRecord(p, pos, rid, lin, dig, json.loads(payload), ordinal)
            for ordinal, rid, pos, lin, dig, payload in self.connection.execute(
                "SELECT ordinal,record_id,position,lineage,payload_digest,payload_json FROM accepted_records WHERE partition_id=? AND ordinal>? ORDER BY ordinal",
                (p, ack),
            )
        )

    def ack_status(self, cid: str, p: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            "SELECT ordinal,position,owner_id,epoch,attempt_count,outcome,last_error FROM ack_attempts WHERE checkpoint_id=? AND partition_id=?",
            (cid, p),
        ).fetchone()
        if row is None:
            return None
        return dict(
            zip(
                (
                    "ordinal",
                    "position",
                    "owner_id",
                    "epoch",
                    "attempt_count",
                    "outcome",
                    "last_error",
                ),
                row,
                strict=True,
            )
        )
