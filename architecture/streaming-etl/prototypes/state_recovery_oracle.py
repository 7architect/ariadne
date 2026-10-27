"""Independent read-only oracle for the DS-004S state recovery equations."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StateOracleReport:
    violations: tuple[str, ...]
    plan: dict[str, Any]

    @property
    def passed(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "violations": list(self.violations),
            "plan": self.plan,
        }


def apply_state(state: str, input_id: str, sequence: int, value: str) -> str:
    return f"{state}|{input_id}:{sequence}:{value}"


class StateRecoveryOracle:
    @classmethod
    def evaluate(cls, path: Path) -> StateOracleReport:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        violations: list[str] = []
        plan: dict[str, Any] = {}
        try:
            contract_row = connection.execute(
                "SELECT contract_json FROM state_contract WHERE singleton = 1"
            ).fetchone()
            if contract_row is None:
                violations.append("state_contract_missing")
            else:
                try:
                    contract = json.loads(contract_row[0])
                except json.JSONDecodeError:
                    violations.append("state_contract_invalid")
                    contract = {}
                state_class = contract.get("state_class")
                if state_class == "PARTITION_LOCAL_PER_TURN":
                    if (
                        len(contract.get("input_bindings", [])) != 1
                        or contract.get("atomic_authority") != "turn_completion"
                    ):
                        violations.append("local_contract_ineligible")
                    plan = cls._local_plan(connection, contract, violations)
                elif state_class == "CHECKPOINT_MANAGED_SHARED":
                    if (
                        contract.get("atomic_authority") != "checkpoint"
                        or not contract.get("deterministic_replay")
                        or (
                            len(contract.get("input_bindings", [])) > 1
                            and not contract.get("persisted_mailbox_order")
                        )
                    ):
                        violations.append("checkpoint_contract_ineligible")
                    plan = cls._checkpoint_plan(connection, contract, violations)
                else:
                    violations.append("managed_state_class_invalid")
        finally:
            connection.rollback()
            connection.close()
        return StateOracleReport(tuple(sorted(set(violations))), plan)

    @staticmethod
    def _local_plan(
        connection: sqlite3.Connection,
        contract: dict[str, Any],
        violations: list[str],
    ) -> dict[str, Any]:
        head = connection.execute("SELECT * FROM local_head").fetchone()
        completions = connection.execute(
            "SELECT * FROM local_completions ORDER BY sequence"
        ).fetchall()
        outputs = connection.execute(
            "SELECT * FROM local_outputs ORDER BY sequence"
        ).fetchall()
        checkpoint_authority_tables = (
            "checkpoint_intents",
            "checkpoint_cuts",
            "barrier_witnesses",
            "checkpoint_seals",
            "checkpoint_completions",
            "recovery_targets",
            "mailbox",
            "failure_free_reference",
            "authority_log",
            "recovery_fence",
        )
        if any(
            connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            for table in checkpoint_authority_tables
        ):
            violations.append("mixed_recovery_authority")
        if head is None:
            violations.append("local_head_missing")
            return {}
        cursor = head["completed_through"]
        expected_sequences = list(range(1, cursor + 1))
        completion_sequences = [row["sequence"] for row in completions]
        output_sequences = [row["sequence"] for row in outputs]
        if completion_sequences != expected_sequences:
            violations.append("local_completion_cursor_atomicity")
        if output_sequences != expected_sequences:
            violations.append("local_output_completion_atomicity")
        if cursor == 0:
            expected_state = "{}"
        else:
            latest = next(
                (row for row in completions if row["sequence"] == cursor), None
            )
            expected_state = latest["state_json"] if latest is not None else None
        if head["state_json"] != expected_state:
            violations.append("local_state_cursor_atomicity")
        try:
            json.loads(head["state_json"])
        except json.JSONDecodeError:
            violations.append("local_head_state_json")
        expected_schema = contract.get("state_schema")
        expected_code = contract.get("code_version")
        if (
            head["state_schema"] != expected_schema
            or head["code_version"] != expected_code
        ):
            violations.append("local_state_compatibility")
        for completion in completions:
            output = next(
                (row for row in outputs if row["sequence"] == completion["sequence"]),
                None,
            )
            if (
                completion["state_schema"] != expected_schema
                or completion["code_version"] != expected_code
            ):
                violations.append(f"local_state_compatibility:{completion['sequence']}")
            if output is None or (
                output["record_id"] != completion["output_record_id"]
            ):
                violations.append(
                    f"local_output_completion_atomicity:{completion['sequence']}"
                )
            try:
                json.loads(completion["state_json"])
            except json.JSONDecodeError:
                violations.append(f"local_state_json:{completion['sequence']}")
        recovered_state = None
        if expected_state is not None:
            try:
                recovered_state = json.loads(expected_state)
            except json.JSONDecodeError:
                violations.append("local_recovery_state_json")
        return {
            "mode": "local_atomic_head",
            "state": recovered_state,
            "next": cursor + 1,
        }

    @classmethod
    def _checkpoint_plan(
        cls,
        connection: sqlite3.Connection,
        contract: dict[str, Any],
        violations: list[str],
    ) -> dict[str, Any]:
        inputs = contract.get("input_bindings", [])
        if any(
            connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
            for table in ("local_head", "local_outputs", "local_completions")
        ):
            violations.append("mixed_recovery_authority")
        fence_events = connection.execute(
            "SELECT * FROM authority_log WHERE event_type = 'RECOVERY_FENCE'"
        ).fetchall()
        if len(fence_events) != 1:
            violations.append("recovery_fence_event_cardinality")
        fence = connection.execute(
            "SELECT authority_ordinal FROM recovery_fence WHERE singleton = 1"
        ).fetchone()
        if fence is None:
            violations.append("recovery_fence_missing")
            return {}
        fence_ordinal = fence["authority_ordinal"]
        fence_event = connection.execute(
            "SELECT * FROM authority_log WHERE ordinal = ?",
            (fence_ordinal,),
        ).fetchone()
        if (
            fence_event is None
            or fence_event["event_type"] != "RECOVERY_FENCE"
            or fence_event["checkpoint"] is not None
            or len(fence_events) != 1
            or fence_events[0]["ordinal"] != fence_ordinal
        ):
            violations.append("recovery_fence_witness_invalid")
        targets = {
            row["input_id"]: row
            for row in connection.execute("SELECT * FROM recovery_targets")
        }
        if set(targets) != set(inputs):
            violations.append("checkpoint_target_vector_invalid")
            return {}
        compatible: list[tuple[sqlite3.Row, dict[str, int]]] = []
        intents = connection.execute(
            "SELECT * FROM checkpoint_intents ORDER BY checkpoint DESC"
        ).fetchall()
        for intent in intents:
            checkpoint = intent["checkpoint"]
            valid = True
            authorization = connection.execute(
                "SELECT * FROM authority_log WHERE ordinal = ?",
                (intent["authorization_ordinal"],),
            ).fetchone()
            if (
                authorization is None
                or authorization["event_type"] != "CHECKPOINT_INTENT"
                or authorization["checkpoint"] != checkpoint
            ):
                violations.append(
                    f"checkpoint_authorization_witness_invalid:{checkpoint}"
                )
                valid = False
            if intent["authorization_ordinal"] >= fence_ordinal:
                violations.append(f"retroactive_checkpoint:{checkpoint}")
                valid = False
            seal = connection.execute(
                "SELECT * FROM checkpoint_seals WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            if (
                seal is None
                or seal["intent_digest"] != intent["intent_digest"]
                or seal["authorization_ordinal"] != intent["authorization_ordinal"]
            ):
                violations.append(f"checkpoint_seal_invalid:{checkpoint}")
                valid = False
            if intent["state_schema"] != contract.get("state_schema") or intent[
                "code_version"
            ] != contract.get("code_version"):
                violations.append(f"checkpoint_incompatible:{checkpoint}")
                valid = False
            cuts = {
                row["input_id"]: row["cut_sequence"]
                for row in connection.execute(
                    "SELECT * FROM checkpoint_cuts WHERE checkpoint = ?",
                    (checkpoint,),
                )
            }
            witnesses = {
                row["input_id"]: row
                for row in connection.execute(
                    "SELECT * FROM barrier_witnesses WHERE checkpoint = ?",
                    (checkpoint,),
                )
            }
            expected_intent = {
                "checkpoint": checkpoint,
                "authorization_ordinal": intent["authorization_ordinal"],
                "inputs": sorted(cuts),
                "cuts": cuts,
                "snapshot_digest": hashlib.sha256(
                    intent["snapshot_state"].encode()
                ).hexdigest(),
                "state_schema": intent["state_schema"],
                "code_version": intent["code_version"],
            }
            expected_digest = hashlib.sha256(
                json.dumps(
                    expected_intent, sort_keys=True, separators=(",", ":")
                ).encode()
            ).hexdigest()
            if intent["intent_digest"] != expected_digest:
                violations.append(f"checkpoint_intent_digest_invalid:{checkpoint}")
                valid = False
            if set(cuts) != set(inputs) or set(witnesses) != set(inputs):
                violations.append(f"checkpoint_cut_vector_invalid:{checkpoint}")
                valid = False
            for input_id in inputs:
                witness = witnesses.get(input_id)
                if witness is None:
                    continue
                if (
                    witness["cut_sequence"] != cuts.get(input_id)
                    or witness["ordered_log_sequence"] != cuts.get(input_id, -2) + 1
                    or witness["intent_digest"] != intent["intent_digest"]
                ):
                    violations.append(
                        f"checkpoint_barrier_witness_invalid:{checkpoint}:{input_id}"
                    )
                    valid = False
            completion = connection.execute(
                "SELECT * FROM checkpoint_completions WHERE checkpoint = ?",
                (checkpoint,),
            ).fetchone()
            if completion is None:
                continue
            completion_event = connection.execute(
                "SELECT * FROM authority_log WHERE ordinal = ?",
                (completion["completion_ordinal"],),
            ).fetchone()
            if (
                completion["intent_digest"] != intent["intent_digest"]
                or completion_event is None
                or completion_event["event_type"] != "CHECKPOINT_COMPLETION"
                or completion_event["checkpoint"] != checkpoint
            ):
                violations.append(f"checkpoint_completion_witness_invalid:{checkpoint}")
                valid = False
            if completion["completion_ordinal"] <= intent["authorization_ordinal"]:
                violations.append(f"checkpoint_completion_order_invalid:{checkpoint}")
                valid = False
            if completion["completion_ordinal"] >= fence_ordinal:
                violations.append(f"retroactive_checkpoint_completion:{checkpoint}")
                valid = False
            if valid:
                compatible.append((intent, cuts))
        if not compatible:
            violations.append("completed_compatible_checkpoint_missing")
            return {}

        selected, cuts = compatible[0]
        unknown_mailbox_inputs = [
            row["input_id"]
            for row in connection.execute(
                "SELECT DISTINCT input_id FROM mailbox ORDER BY input_id"
            )
            if row["input_id"] not in inputs
        ]
        for input_id in unknown_mailbox_inputs:
            violations.append(f"mailbox_input_unknown:{input_id}")
        events = []
        for input_id in inputs:
            target = targets[input_id]["completed_through"]
            cut = cuts[input_id]
            if target < cut or (
                target > cut and targets[input_id]["retained_from"] > cut + 1
            ):
                violations.append(f"replay_tail_unavailable:{input_id}")
                continue
            rows = connection.execute(
                """SELECT * FROM mailbox WHERE input_id = ?
                   AND input_sequence > ? AND input_sequence <= ?""",
                (input_id, cut, target),
            ).fetchall()
            sequences = sorted(row["input_sequence"] for row in rows)
            if sequences != list(range(cut + 1, target + 1)):
                violations.append(f"replay_tail_incomplete:{input_id}")
            events.extend(rows)
        events.sort(key=lambda row: row["apply_ordinal"])
        ordinals = [row["apply_ordinal"] for row in events]
        if len(ordinals) != len(set(ordinals)):
            violations.append("mailbox_order_not_unique")
        state = selected["snapshot_state"]
        order = []
        for event in events:
            state = apply_state(
                state,
                event["input_id"],
                event["input_sequence"],
                event["value"],
            )
            order.append(
                [
                    event["apply_ordinal"],
                    event["input_id"],
                    event["input_sequence"],
                ]
            )
        reference = connection.execute(
            "SELECT state_value FROM failure_free_reference WHERE singleton = 1"
        ).fetchone()
        if reference is None or state != reference["state_value"]:
            violations.append("deterministic_mailbox_replay_mismatch")
        return {
            "mode": "completed_checkpoint_plus_tail",
            "checkpoint": selected["checkpoint"],
            "restore_state": selected["snapshot_state"],
            "cuts": cuts,
            "replay_order": order,
            "recovered_state": state,
            "next": {
                input_id: targets[input_id]["completed_through"] + 1
                for input_id in inputs
            },
        }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    return parser.parse_args()


def main() -> int:
    report = StateRecoveryOracle.evaluate(parse_args().database)
    print(json.dumps(report.as_dict(), sort_keys=True))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
