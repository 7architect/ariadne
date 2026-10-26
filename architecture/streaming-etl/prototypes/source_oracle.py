"""Independent read-only oracle for ST-033 source safety evidence."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SourceObservation:
    accepted: dict[str, int]
    committed: dict[str, tuple[int, str | None]]
    external_acknowledged: dict[str, tuple[int, str, int]]
    local_acknowledged: dict[str, int]
    replay_start: dict[str, int | None]
    blockers: tuple[tuple[str, str, int, str], ...]
    violations: tuple[str, ...]


def _record_id(partition: str, position: str) -> str:
    raw = json.dumps(["source-record-v1", partition, position], separators=(",", ":"))
    return "source-record:" + hashlib.sha256(raw.encode()).hexdigest()


def _payload_digest(payload_json: str) -> str:
    canonical = json.dumps(
        json.loads(payload_json), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def inspect_source(
    ledger_path: Path,
    external_acknowledged: dict[str, tuple[int, str, int]],
    coordinator_path: Path | None = None,
) -> SourceObservation:
    connection = sqlite3.connect(f"file:{ledger_path}?mode=ro", uri=True)
    partitions = {
        partition: ack
        for partition, ack in connection.execute(
            "SELECT partition_id,ack_ordinal FROM partitions"
        )
    }
    records: dict[str, list[tuple[int, str, str, str]]] = {}
    violations: list[str] = []
    for partition, ordinal, record_id, position, digest, payload in connection.execute(
        """SELECT partition_id,ordinal,record_id,position,payload_digest,payload_json
           FROM accepted_records ORDER BY partition_id,ordinal"""
    ):
        records.setdefault(partition, []).append(
            (ordinal, position, record_id, digest)
        )
        if record_id != _record_id(partition, position):
            violations.append(f"{partition}#{ordinal}: noncanonical record ID")
        try:
            actual_payload_digest = _payload_digest(payload)
        except (json.JSONDecodeError, TypeError, ValueError):
            violations.append(f"{partition}#{ordinal}: invalid payload JSON")
        else:
            if digest != actual_payload_digest:
                violations.append(f"{partition}#{ordinal}: payload digest mismatch")

    for partition, rows in records.items():
        ordinals = [row[0] for row in rows]
        if ordinals != list(range(1, len(rows) + 1)):
            violations.append(f"{partition}: accepted ordinals are not contiguous")

    committed: dict[str, tuple[int, str | None]] = {}
    committed_exact: set[tuple[str, int, str | None]] = set()
    authorized_cuts: dict[tuple[str, int, str | None], int] = {}
    checkpoint_digests: dict[str, str] = {}
    checkpoint_cuts: dict[tuple[str, str], tuple[int, str | None]] = {}
    authority = None
    if coordinator_path is not None:
        try:
            authority = sqlite3.connect(
                f"file:{coordinator_path}?mode=ro", uri=True
            )
        except sqlite3.OperationalError:
            authority = None
    for checkpoint, decision_digest, decision_json, expected, phase in connection.execute(
        "SELECT checkpoint_id,decision_digest,decision_json,expected,phase FROM checkpoints"
    ):
        cuts = list(
            connection.execute(
                "SELECT partition_id,ordinal,position FROM cuts WHERE checkpoint_id=? ORDER BY partition_id",
                (checkpoint,),
            )
        )
        checkpoint_digests[checkpoint] = decision_digest
        decision: dict[str, Any] | None = None
        expected_positions: list[tuple[str, str | None]] | None = None
        source_epochs: dict[str, int] = {}
        global_valid = False
        try:
            candidate = json.loads(decision_json)
            if not isinstance(candidate, dict):
                raise TypeError("decision must be an object")
            decision = candidate
            if decision_digest != _digest(decision):
                violations.append(f"{checkpoint}: canonical decision digest mismatch")
            required = {
                "job_id", "graph_version_id", "transaction_id", "sequence",
                "intent_digest", "capability_digest", "coordinator_owner_id",
                "coordinator_epoch", "source_positions", "source_epochs", "decision",
            }
            if not required.issubset(decision):
                raise ValueError("missing required decision fields")
            expected_positions = [tuple(item) for item in decision["source_positions"]]
            source_epochs = dict(tuple(item) for item in decision["source_epochs"])
            if set(source_epochs) != {partition for partition, _ in expected_positions}:
                raise ValueError("source epoch vector differs from positions")
            authority_row = None
            if authority is not None:
                authority_row = authority.execute(
                    """SELECT sequence,manifest_digest,phase,decision,active_owner_id,
                              fence_epoch,intent_json FROM checkpoints
                       WHERE job_id=? AND graph_version_id=? AND checkpoint_id=?""",
                    (decision["job_id"],decision["graph_version_id"],checkpoint),
                ).fetchone()
            expected_transaction = "ariadne:transaction:" + _digest(
                {"job": decision["job_id"], "graph": decision["graph_version_id"],
                 "checkpoint": checkpoint}
            )
            if authority_row is None:
                violations.append(f"{checkpoint}: missing global COMMIT authority")
            else:
                authority_intent = json.loads(authority_row[6])
                authority_positions = tuple(
                    tuple(item) for item in authority_intent["source_positions"]
                )
                authority_epochs = tuple(
                    sorted(
                        (participant,epoch)
                        for participant,epoch in authority_intent["generation_manifest"]
                        if participant in dict(expected_positions)
                    )
                )
                global_valid = (
                    authority_row[0] == decision["sequence"]
                    and authority_row[1] == decision["intent_digest"]
                    and authority_row[2] in {"COMMIT_DECIDED","COMMITTING","COMPLETED"}
                    and authority_row[3] == decision["decision"] == "COMMIT"
                    and authority_row[4] == decision["coordinator_owner_id"]
                    and authority_row[5] == decision["coordinator_epoch"]
                    and decision["transaction_id"] == expected_transaction
                    and authority_intent["capability_digest"] == decision["capability_digest"]
                    and authority_positions == tuple(expected_positions)
                    and authority_epochs == tuple(sorted(source_epochs.items()))
                )
                if not global_valid:
                    violations.append(f"{checkpoint}: global COMMIT reference mismatch")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, sqlite3.Error):
            violations.append(f"{checkpoint}: invalid canonical decision structure")
        actual_positions = [(partition, position) for partition, _, position in cuts]
        if phase != "COMMIT" or len(cuts) != expected:
            violations.append(f"{checkpoint}: incomplete committed vector")
        if expected_positions is None or actual_positions != expected_positions:
            violations.append(f"{checkpoint}: vector differs from decision")
        for partition, ordinal, position in cuts:
            committed_exact.add((partition,ordinal,position))
            checkpoint_cuts[(checkpoint,partition)] = (ordinal,position)
            if global_valid and partition in source_epochs:
                cut_key=(partition,ordinal,position)
                authorized_cuts[cut_key]=min(
                    authorized_cuts.get(cut_key,source_epochs[partition]),
                    source_epochs[partition],
                )
            if ordinal > 0 and not any(
                row[0] == ordinal and row[1] == position
                for row in records.get(partition, [])
            ):
                violations.append(f"{checkpoint}/{partition}: cut is not accepted")
            previous = committed.get(partition)
            if previous is None or ordinal > previous[0]:
                committed[partition] = (ordinal, position)

    replay_start: dict[str, int | None] = {}
    for partition, (ordinal, position, epoch) in external_acknowledged.items():
        if partition not in partitions:
            violations.append(f"{partition}: external ACK has unknown partition")
            continue
        if not any(
            row[0] == ordinal and row[1] == position
            for row in records.get(partition, [])
        ):
            violations.append(f"{partition}: external ACK is not accepted")
        if (partition,ordinal,position) not in committed_exact:
            violations.append(f"{partition}: external ACK lacks committed cut")
        required_epoch = authorized_cuts.get((partition,ordinal,position))
        if required_epoch is None:
            violations.append(f"{partition}: external ACK lacks global authority")
        elif epoch < required_epoch:
            violations.append(f"{partition}: external ACK has stale epoch")
    for partition, local_ack in partitions.items():
        external = external_acknowledged.get(partition)
        if local_ack > (external[0] if external else 0):
            violations.append(f"{partition}: local ACK exceeds external ACK")
        if local_ack > 0 and not any(
            cut[0] == local_ack
            for cut in connection.execute(
                """SELECT c.ordinal FROM cuts c JOIN checkpoints d USING(checkpoint_id)
                   WHERE c.partition_id=? AND d.phase='COMMIT'""",
                (partition,),
            )
        ):
            violations.append(f"{partition}: local ACK lacks committed cut")
        replay = [row[0] for row in records.get(partition, []) if row[0] > local_ack]
        replay_start[partition] = replay[0] if replay else None

    attempts = tuple(
        connection.execute(
            """SELECT checkpoint_id,manifest,partition_id,ordinal,position,outcome,last_error
               FROM ack_attempts ORDER BY checkpoint_id,partition_id"""
        )
    )
    blockers = tuple(
        (checkpoint,partition,ordinal,outcome)
        for checkpoint,_manifest,partition,ordinal,_position,outcome,_error in attempts
        if outcome in {"IN_PROGRESS","BLOCKED"}
    )
    for checkpoint,manifest,partition,ordinal,position,outcome,_error in attempts:
        if manifest != checkpoint_digests.get(checkpoint):
            violations.append(f"{checkpoint}/{partition}: ACK decision digest mismatch")
        if checkpoint_cuts.get((checkpoint,partition)) != (ordinal,position):
            violations.append(f"{checkpoint}/{partition}: ACK attempt differs from cut")
        local_ack=partitions.get(partition,-1)
        external=external_acknowledged.get(partition)
        if outcome=="SUCCEEDED" and not (
            local_ack>=ordinal and external is not None
            and external[0]==ordinal and external[1]==position
        ):
            violations.append(f"{checkpoint}/{partition}: false SUCCEEDED ACK")
        if outcome=="SUBSUMED" and not (
            local_ack>ordinal and external is not None and external[0]>=local_ack
        ):
            violations.append(f"{checkpoint}/{partition}: false SUBSUMED ACK")
        if outcome in {"IN_PROGRESS","BLOCKED"} and ordinal <= local_ack:
            violations.append(
                f"{checkpoint}/{partition}: stale {outcome} blocker below local ACK"
            )

    return SourceObservation(
        accepted={partition: len(rows) for partition, rows in records.items()},
        committed=committed,
        external_acknowledged=dict(external_acknowledged),
        local_acknowledged=partitions,
        replay_start=replay_start,
        blockers=blockers,
        violations=tuple(violations),
    )
