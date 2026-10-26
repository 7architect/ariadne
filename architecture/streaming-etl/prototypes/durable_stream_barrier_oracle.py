"""Independent read-only oracle for DS-004a DATA/BARRIER composition."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def apply_state(state: str, input_id: str, sequence: int, payload_ref: str) -> str:
    return f"{state}|{input_id}:{sequence}:{payload_ref}"


@dataclass(frozen=True)
class CompositionReport:
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


class CompositionOracle:
    @classmethod
    def evaluate(cls, path: Path) -> CompositionReport:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        violations: list[str] = []
        plan: dict[str, Any] = {}
        try:
            manifest = connection.execute("SELECT * FROM operator_manifest").fetchone()
            state_row = connection.execute("SELECT * FROM operator_state").fetchone()
            if manifest is None or state_row is None:
                violations.append("operator_manifest_missing")
                return CompositionReport(tuple(violations), plan)
            inputs = json.loads(manifest["inputs_json"])
            progress = {
                row["input_id"]: row
                for row in connection.execute("SELECT * FROM operator_progress")
            }
            if set(progress) != set(inputs):
                violations.append("operator_progress_manifest_mismatch")

            intents = {
                row["checkpoint"]: row
                for row in connection.execute("SELECT * FROM checkpoint_intents")
            }
            seals = {
                row["checkpoint"]: row
                for row in connection.execute("SELECT * FROM checkpoint_intent_seals")
            }

            entries_by_input: dict[str, list[sqlite3.Row]] = {}
            for input_id in inputs:
                rows = connection.execute(
                    "SELECT * FROM stream_entries WHERE input_id = ? "
                    "ORDER BY log_sequence",
                    (input_id,),
                ).fetchall()
                entries_by_input[input_id] = rows
                logs = [row["log_sequence"] for row in rows]
                if logs != list(range(1, len(logs) + 1)):
                    violations.append(f"stream_log_gap:{input_id}")
                data = [row for row in rows if row["kind"] == "DATA"]
                if [row["data_sequence"] for row in data] != list(
                    range(1, len(data) + 1)
                ):
                    violations.append(f"stream_data_gap:{input_id}")
                for barrier in (row for row in rows if row["kind"] == "BARRIER"):
                    checkpoint = barrier["checkpoint"]
                    intent = intents.get(checkpoint)
                    if intent is None:
                        violations.append(
                            f"barrier_intent_missing:{checkpoint}:{input_id}"
                        )
                    else:
                        intent_inputs = json.loads(intent["inputs_json"])
                        if input_id not in intent_inputs:
                            violations.append(
                                f"barrier_input_manifest:{checkpoint}:{input_id}"
                            )
                        if barrier["intent_digest"] != intent["intent_digest"]:
                            violations.append(
                                f"barrier_intent_digest:{checkpoint}:{input_id}"
                            )
                        if barrier["writer_epoch"] != intent["coordinator_epoch"]:
                            violations.append(
                                f"barrier_writer_epoch:{checkpoint}:{input_id}"
                            )
                        seal = seals.get(checkpoint)
                        if (
                            seal is None
                            or seal["intent_digest"] != barrier["intent_digest"]
                        ):
                            violations.append(
                                f"barrier_intent_unsealed:{checkpoint}:{input_id}"
                            )
                    cut = max(
                        (
                            row["data_sequence"]
                            for row in data
                            if row["log_sequence"] < barrier["log_sequence"]
                        ),
                        default=0,
                    )
                    if barrier["cut_data_sequence"] != cut:
                        violations.append(
                            f"barrier_cut_not_ordered:{barrier['checkpoint']}:{input_id}"
                        )

            fence = connection.execute("SELECT * FROM recovery_fence").fetchone()
            for checkpoint, intent in intents.items():
                expected = {
                    "checkpoint": checkpoint,
                    "inputs": json.loads(intent["inputs_json"]),
                    "coordinator_epoch": intent["coordinator_epoch"],
                    "authority_ordinal": intent["authority_ordinal"],
                    "state_schema": intent["state_schema"],
                    "code_version": intent["code_version"],
                }
                if digest(expected) != intent["intent_digest"]:
                    violations.append(f"checkpoint_intent_digest:{checkpoint}")
                seal = connection.execute(
                    "SELECT * FROM checkpoint_intent_seals WHERE checkpoint = ?",
                    (checkpoint,),
                ).fetchone()
                if seal is None or seal["intent_digest"] != intent["intent_digest"]:
                    violations.append(f"checkpoint_intent_unsealed:{checkpoint}")
                if fence is not None and intent["authority_ordinal"] >= fence["authority_ordinal"]:
                    violations.append(f"retroactive_checkpoint_intent:{checkpoint}")

            observations = connection.execute(
                "SELECT * FROM barrier_observations"
            ).fetchall()
            for observation in observations:
                entry = connection.execute(
                    "SELECT * FROM stream_entries WHERE input_id = ? AND log_sequence = ?",
                    (observation["input_id"], observation["log_sequence"]),
                ).fetchone()
                if (
                    entry is None
                    or entry["kind"] != "BARRIER"
                    or entry["checkpoint"] != observation["checkpoint"]
                    or entry["cut_data_sequence"]
                    != observation["cut_data_sequence"]
                    or entry["intent_digest"] != observation["intent_digest"]
                ):
                    violations.append(
                        "barrier_observation_identity:"
                        f"{observation['checkpoint']}:{observation['input_id']}"
                    )
                    continue
                intent = intents.get(observation["checkpoint"])
                if (
                    intent is None
                    or observation["input_id"] not in json.loads(intent["inputs_json"])
                    or observation["intent_digest"] != intent["intent_digest"]
                ):
                    violations.append(
                        "barrier_observation_authority:"
                        f"{observation['checkpoint']}:{observation['input_id']}"
                    )

            turns = connection.execute(
                "SELECT * FROM turns ORDER BY apply_ordinal"
            ).fetchall()
            ordinals = [row["apply_ordinal"] for row in turns]
            if ordinals != list(range(1, len(ordinals) + 1)):
                violations.append("mailbox_apply_order_gap")
            state = manifest["initial_state"]
            state_at_ordinal = {0: state}
            turns_by_input: dict[str, list[sqlite3.Row]] = {i: [] for i in inputs}
            for turn in turns:
                if turn["input_id"] not in turns_by_input:
                    violations.append(f"mailbox_input_unknown:{turn['input_id']}")
                    continue
                entry = next(
                    (
                        row
                        for row in entries_by_input[turn["input_id"]]
                        if row["log_sequence"] == turn["log_sequence"]
                    ),
                    None,
                )
                if (
                    entry is None
                    or entry["kind"] != "DATA"
                    or entry["data_sequence"] != turn["data_sequence"]
                    or entry["record_id"] != turn["record_id"]
                    or entry["payload_ref"] != turn["payload_ref"]
                ):
                    violations.append(
                        f"turn_data_identity:{turn['input_id']}:{turn['data_sequence']}"
                    )
                state = apply_state(
                    state,
                    turn["input_id"],
                    turn["data_sequence"],
                    turn["payload_ref"],
                )
                if turn["state_after"] != state:
                    violations.append(f"mailbox_state:{turn['apply_ordinal']}")
                state_at_ordinal[turn["apply_ordinal"]] = state
                turns_by_input[turn["input_id"]].append(turn)
            if state_row["state_value"] != state:
                violations.append("operator_state_mailbox_mismatch")

            outputs = connection.execute(
                "SELECT * FROM output_events ORDER BY output_ordinal"
            ).fetchall()
            output_by_id = {row["output_id"]: row for row in outputs}
            for turn in turns:
                output = output_by_id.get(turn["output_id"])
                if (
                    output is None
                    or output["kind"] != "DATA"
                    or output["input_id"] != turn["input_id"]
                    or output["data_sequence"] != turn["data_sequence"]
                ):
                    violations.append(
                        f"turn_output_identity:{turn['input_id']}:{turn['data_sequence']}"
                    )

            for input_id, row in progress.items():
                input_turns = turns_by_input.get(input_id, [])
                sequences = [turn["data_sequence"] for turn in input_turns]
                expected = list(range(1, row["completed_data_through"] + 1))
                if sequences != expected:
                    violations.append(f"data_completion_cursor:{input_id}")
                scanned = [
                    entry
                    for entry in entries_by_input.get(input_id, [])
                    if entry["log_sequence"] <= row["scanned_log_through"]
                ]
                for entry in scanned:
                    if (
                        entry["kind"] == "DATA"
                        and entry["data_sequence"] > row["completed_data_through"]
                    ):
                        violations.append(f"scanned_data_without_completion:{input_id}")
                    if entry["kind"] == "BARRIER":
                        observation = connection.execute(
                            "SELECT 1 FROM barrier_observations "
                            "WHERE checkpoint = ? AND input_id = ? AND log_sequence = ?",
                            (entry["checkpoint"], input_id, entry["log_sequence"]),
                        ).fetchone()
                        if observation is None:
                            violations.append(
                                f"scanned_barrier_unobserved:{entry['checkpoint']}:{input_id}"
                            )

            snapshots = {
                row["checkpoint"]: row
                for row in connection.execute("SELECT * FROM snapshots")
            }
            statuses = {
                row["checkpoint"]: row["status"]
                for row in connection.execute("SELECT * FROM checkpoint_status")
            }
            for checkpoint, status in statuses.items():
                if status in ("ALIGNED", "COMPLETED") and checkpoint not in snapshots:
                    violations.append(f"checkpoint_snapshot_missing:{checkpoint}")
                if status == "STARTED" and checkpoint in snapshots:
                    violations.append(f"checkpoint_snapshot_before_alignment:{checkpoint}")
            for checkpoint, snapshot in snapshots.items():
                intent = intents.get(checkpoint)
                cuts = json.loads(snapshot["cuts_json"])
                if intent is None or snapshot["intent_digest"] != intent["intent_digest"]:
                    violations.append(f"snapshot_intent:{checkpoint}")
                    continue
                if set(cuts) != set(json.loads(intent["inputs_json"])):
                    violations.append(f"snapshot_cut_manifest:{checkpoint}")
                highwater = snapshot["mailbox_highwater"]
                if snapshot["state_value"] != state_at_ordinal.get(highwater):
                    violations.append(f"snapshot_state_highwater:{checkpoint}")
                for input_id, cut in cuts.items():
                    observation = connection.execute(
                        "SELECT * FROM barrier_observations "
                        "WHERE checkpoint = ? AND input_id = ?",
                        (checkpoint, input_id),
                    ).fetchone()
                    if (
                        observation is None
                        or cut["data"] != observation["cut_data_sequence"]
                        or cut["barrier_log"] != observation["log_sequence"]
                    ):
                        violations.append(f"snapshot_cut_mismatch:{checkpoint}:{input_id}")
                    before = [
                        turn
                        for turn in turns_by_input.get(input_id, [])
                        if turn["apply_ordinal"] <= highwater
                    ]
                    if [turn["data_sequence"] for turn in before] != list(
                        range(1, cut["data"] + 1)
                    ):
                        violations.append(
                            f"snapshot_pre_post_boundary:{checkpoint}:{input_id}"
                        )
                barrier_output = output_by_id.get(f"barrier-out:{checkpoint}")
                if barrier_output is None:
                    violations.append(f"forwarded_barrier_missing:{checkpoint}")
                else:
                    pre_outputs = [
                        output_by_id[turn["output_id"]]["output_ordinal"]
                        for turn in turns
                        if turn["apply_ordinal"] <= highwater
                    ]
                    post_outputs = [
                        output_by_id[turn["output_id"]]["output_ordinal"]
                        for turn in turns
                        if turn["apply_ordinal"] > highwater
                    ]
                    if pre_outputs and max(pre_outputs) >= barrier_output["output_ordinal"]:
                        violations.append(f"forwarded_barrier_overtook_data:{checkpoint}")
                    if post_outputs and min(post_outputs) <= barrier_output["output_ordinal"]:
                        violations.append(f"post_barrier_output_order:{checkpoint}")

            completed = [
                checkpoint
                for checkpoint, status in statuses.items()
                if status == "COMPLETED"
                and checkpoint in snapshots
                and checkpoint in intents
                and snapshots[checkpoint]["intent_digest"]
                == intents[checkpoint]["intent_digest"]
                and intents[checkpoint]["inputs_json"] == manifest["inputs_json"]
                and intents[checkpoint]["state_schema"] == manifest["state_schema"]
                and intents[checkpoint]["code_version"] == manifest["code_version"]
            ]
            if completed:
                selected = max(completed)
                snapshot = snapshots[selected]
                highwater = snapshot["mailbox_highwater"]
                recovered = snapshot["state_value"]
                replay = []
                for turn in turns:
                    if turn["apply_ordinal"] <= highwater:
                        continue
                    recovered = apply_state(
                        recovered,
                        turn["input_id"],
                        turn["data_sequence"],
                        turn["payload_ref"],
                    )
                    replay.append(
                        [turn["apply_ordinal"], turn["input_id"], turn["data_sequence"]]
                    )
                if recovered != state_row["state_value"]:
                    violations.append("recovery_tail_state_mismatch")
                plan = {
                    "checkpoint": selected,
                    "cuts": json.loads(snapshot["cuts_json"]),
                    "snapshot_state": snapshot["state_value"],
                    "replay": replay,
                    "recovered_state": recovered,
                    "next": {
                        input_id: row["completed_data_through"] + 1
                        for input_id, row in progress.items()
                    },
                }
                recovery_runs = connection.execute(
                    "SELECT * FROM recovery_runs ORDER BY generation"
                ).fetchall()
                recovery = recovery_runs[-1] if recovery_runs else None
                if recovery is not None:
                    if (
                        len(recovery_runs) != 1
                        or recovery["generation"] != 1
                        or fence is None
                        or recovery["fence_ordinal"] != fence["authority_ordinal"]
                    ):
                        violations.append("recovery_generation_authority")
                    if (
                        recovery["phase"] != "READY"
                        or recovery["replayed_through_ordinal"] is None
                        or recovery["recovered_state"] is None
                    ):
                        violations.append("recovery_run_mismatch")
                    else:
                        target_cursors = json.loads(
                            recovery["target_cursors_json"]
                        )
                        recovery_state = snapshot["state_value"]
                        replayed_through = recovery["replayed_through_ordinal"]
                        expected_ordinal = highwater + 1
                        pre_recovery = [
                            turn
                            for turn in turns
                            if turn["recovery_generation"]
                            < recovery["generation"]
                        ]
                        expected_through = (
                            pre_recovery[-1]["apply_ordinal"]
                            if pre_recovery
                            else 0
                        )
                        cursors_at_ready = {input_id: 0 for input_id in inputs}
                        for turn in pre_recovery:
                            cursors_at_ready[turn["input_id"]] = turn["data_sequence"]
                        if cursors_at_ready != target_cursors:
                            violations.append("recovery_target_cursor_mismatch")
                        for turn in turns:
                            if turn["apply_ordinal"] <= highwater:
                                continue
                            if turn["apply_ordinal"] > replayed_through:
                                if (
                                    turn["recovery_generation"]
                                    != recovery["generation"]
                                ):
                                    violations.append(
                                        "post_recovery_turn_generation:"
                                        f"{turn['apply_ordinal']}"
                                    )
                                continue
                            if turn["apply_ordinal"] != expected_ordinal:
                                violations.append("recovery_suffix_order_gap")
                            if (
                                turn["recovery_generation"]
                                >= recovery["generation"]
                            ):
                                violations.append(
                                    "recovery_suffix_generation:"
                                    f"{turn['apply_ordinal']}"
                                )
                            recovery_state = apply_state(
                                recovery_state,
                                turn["input_id"],
                                turn["data_sequence"],
                                turn["payload_ref"],
                            )
                            expected_ordinal += 1
                        if replayed_through not in state_at_ordinal:
                            violations.append("recovery_replayed_through_missing")
                        if (
                            recovery["checkpoint"] != selected
                            or recovery["restored_state"]
                            != snapshot["state_value"]
                            or recovery["recovered_state"] != recovery_state
                            or replayed_through != expected_through
                            or recovery["recovered_state"]
                            != state_at_ordinal.get(replayed_through)
                        ):
                            violations.append("recovery_run_mismatch")
                else:
                    for turn in turns:
                        if turn["recovery_generation"] != 0:
                            violations.append(
                                f"turn_without_recovery_generation:{turn['apply_ordinal']}"
                            )
            else:
                plan = {"checkpoint": None, "reason": "no_completed_checkpoint"}
        finally:
            connection.rollback()
            connection.close()
        return CompositionReport(tuple(sorted(set(violations))), plan)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    report = CompositionOracle.evaluate(parser.parse_args().database)
    print(json.dumps(report.as_dict(), sort_keys=True))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
