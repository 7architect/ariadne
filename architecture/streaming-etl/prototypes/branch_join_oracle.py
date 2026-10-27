"""Independent read-only oracle for DS-005a/b fan-out and join evidence."""

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
class Report:
    violations: tuple[str, ...]
    observations: dict[str, Any]

    @property
    def passed(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "violations": list(self.violations),
            "observations": self.observations,
        }


class BranchJoinOracle:
    @classmethod
    def evaluate(cls, path: Path) -> Report:
        connection = sqlite3.connect(f"file:{path.resolve()}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("BEGIN")
        violations: list[str] = []
        observations: dict[str, Any] = {}
        try:
            branch_outputs = connection.execute(
                "SELECT * FROM branch_outputs ORDER BY branch_id, upstream_data_sequence"
            ).fetchall()
            branch_output_by_id = {row["output_id"]: row for row in branch_outputs}
            branch_barriers = connection.execute(
                "SELECT * FROM branch_barrier_outputs ORDER BY branch_id, checkpoint"
            ).fetchall()
            branch_barrier_by_id = {
                row["barrier_output_id"]: row for row in branch_barriers
            }
            deployment = connection.execute("SELECT * FROM deployment").fetchone()
            if deployment is not None:
                branches = json.loads(deployment["branches_json"])
                expected = {
                    "deployment_id": deployment["deployment_id"],
                    "branches": branches,
                }
                if digest(expected) != deployment["manifest_digest"]:
                    violations.append("branch_manifest_digest")
                stored_branches = {
                    row[0]
                    for row in connection.execute(
                        "SELECT branch_id FROM branch_manifest"
                    )
                }
                if set(branches) != stored_branches:
                    violations.append("branch_manifest_membership")

                entries = connection.execute(
                    "SELECT * FROM upstream_entries ORDER BY log_sequence"
                ).fetchall()
                if [row["log_sequence"] for row in entries] != list(
                    range(1, len(entries) + 1)
                ):
                    violations.append("upstream_log_gap")
                data_entries = [row for row in entries if row["kind"] == "DATA"]
                if [row["data_sequence"] for row in data_entries] != list(
                    range(1, len(data_entries) + 1)
                ):
                    violations.append("upstream_data_gap")
                data_by_sequence = {row["data_sequence"]: row for row in data_entries}
                entry_by_log = {row["log_sequence"]: row for row in entries}
                for barrier in (row for row in entries if row["kind"] == "BARRIER"):
                    checkpoint = connection.execute(
                        "SELECT * FROM fanout_checkpoints WHERE checkpoint = ?",
                        (barrier["checkpoint"],),
                    ).fetchone()
                    if (
                        checkpoint is None
                        or barrier["cut_data_sequence"]
                        != checkpoint["cut_data_sequence"]
                        or barrier["intent_digest"] != checkpoint["intent_digest"]
                        or barrier["writer_epoch"] != checkpoint["coordinator_epoch"]
                    ):
                        violations.append(
                            f"fanout_barrier_authority:{barrier['checkpoint']}"
                        )

                branch_view = {}
                for branch_id in branches:
                    progress = connection.execute(
                        "SELECT * FROM branch_progress WHERE branch_id = ?",
                        (branch_id,),
                    ).fetchone()
                    if progress is None:
                        violations.append(f"branch_progress_missing:{branch_id}")
                        continue
                    completions = connection.execute(
                        "SELECT * FROM branch_completions WHERE branch_id = ? "
                        "ORDER BY data_sequence",
                        (branch_id,),
                    ).fetchall()
                    sequences = [row["data_sequence"] for row in completions]
                    if sequences != list(range(1, progress["completed_through"] + 1)):
                        violations.append(f"branch_cursor_coverage:{branch_id}")
                    for completion in completions:
                        entry = data_by_sequence.get(completion["data_sequence"])
                        output = branch_output_by_id.get(completion["output_id"])
                        lineage = {
                            "deployment": deployment["deployment_id"],
                            "branch": branch_id,
                            "record_id": completion["record_id"],
                        }
                        if (
                            entry is None
                            or entry["record_id"] != completion["record_id"]
                            or completion["lineage_digest"] != digest(lineage)
                            or completion["output_id"]
                            != "branch-out:" + digest(lineage)
                            or output is None
                            or output["branch_id"] != branch_id
                            or output["partition_id"] != "p0"
                            or output["upstream_data_sequence"]
                            != completion["data_sequence"]
                            or output["upstream_record_id"] != completion["record_id"]
                            or output["payload_digest"]
                            != digest({"payload_ref": output["payload_ref"]})
                            or output["lineage_digest"] != completion["lineage_digest"]
                        ):
                            violations.append(
                                f"branch_completion_identity:{branch_id}:"
                                f"{completion['data_sequence']}"
                            )
                    scanned = [
                        row
                        for row in entries
                        if row["log_sequence"] <= progress["scanned_log_through"]
                    ]
                    for entry in scanned:
                        if (
                            entry["kind"] == "DATA"
                            and entry["data_sequence"] not in sequences
                        ):
                            violations.append(
                                f"branch_scanned_without_completion:{branch_id}"
                            )
                        if entry["kind"] == "BARRIER":
                            observation = connection.execute(
                                "SELECT * FROM branch_barrier_observations "
                                "WHERE branch_id = ? AND checkpoint = ?",
                                (branch_id, entry["checkpoint"]),
                            ).fetchone()
                            if (
                                observation is None
                                or observation["log_sequence"] != entry["log_sequence"]
                                or observation["cut_data_sequence"]
                                != entry["cut_data_sequence"]
                                or observation["intent_digest"]
                                != entry["intent_digest"]
                                or observation["barrier_output_id"]
                                not in branch_barrier_by_id
                            ):
                                violations.append(
                                    f"branch_barrier_observation:{branch_id}:"
                                    f"{entry['checkpoint']}"
                                )
                    in_flight = connection.execute(
                        "SELECT * FROM branch_in_flight WHERE branch_id = ?",
                        (branch_id,),
                    ).fetchone()
                    if in_flight is not None:
                        entry = entry_by_log.get(in_flight["log_sequence"])
                        if (
                            entry is None
                            or entry["kind"] != "DATA"
                            or entry["data_sequence"]
                            != progress["completed_through"] + 1
                            or entry["record_id"] != in_flight["record_id"]
                            or (in_flight["owner_id"], in_flight["owner_epoch"])
                            != (progress["owner_id"], progress["owner_epoch"])
                        ):
                            violations.append(f"branch_in_flight_identity:{branch_id}")
                    branch_view[branch_id] = {
                        "completed_through": progress["completed_through"],
                        "next": progress["completed_through"] + 1,
                        "owner_epoch": progress["owner_epoch"],
                    }

                completions_by_output = {
                    row["output_id"]: row
                    for row in connection.execute("SELECT * FROM branch_completions")
                }
                for output in branch_outputs:
                    completion = completions_by_output.get(output["output_id"])
                    if (
                        completion is None
                        or completion["branch_id"] != output["branch_id"]
                        or completion["data_sequence"]
                        != output["upstream_data_sequence"]
                        or completion["record_id"] != output["upstream_record_id"]
                        or completion["lineage_digest"] != output["lineage_digest"]
                    ):
                        violations.append(f"branch_output_orphan:{output['output_id']}")

                observations_by_barrier = {
                    row["barrier_output_id"]: row
                    for row in connection.execute(
                        "SELECT * FROM branch_barrier_observations"
                    )
                }
                for barrier_output in branch_barriers:
                    observation = observations_by_barrier.get(
                        barrier_output["barrier_output_id"]
                    )
                    upstream = entry_by_log.get(barrier_output["upstream_log_sequence"])
                    lineage = {
                        "deployment": deployment["deployment_id"],
                        "branch": barrier_output["branch_id"],
                        "partition": barrier_output["partition_id"],
                        "checkpoint": barrier_output["checkpoint"],
                        "upstream_log_sequence": barrier_output[
                            "upstream_log_sequence"
                        ],
                        "cut": barrier_output["cut_data_sequence"],
                        "intent_digest": barrier_output["intent_digest"],
                        "writer_epoch": barrier_output["writer_epoch"],
                    }
                    if (
                        observation is None
                        or upstream is None
                        or upstream["kind"] != "BARRIER"
                        or upstream["checkpoint"] != barrier_output["checkpoint"]
                        or upstream["cut_data_sequence"]
                        != barrier_output["cut_data_sequence"]
                        or upstream["intent_digest"] != barrier_output["intent_digest"]
                        or upstream["writer_epoch"] != barrier_output["writer_epoch"]
                        or observation["branch_id"] != barrier_output["branch_id"]
                        or observation["checkpoint"] != barrier_output["checkpoint"]
                        or observation["log_sequence"]
                        != barrier_output["upstream_log_sequence"]
                        or barrier_output["lineage_digest"] != digest(lineage)
                        or barrier_output["barrier_output_id"]
                        != "branch-barrier-out:" + digest(lineage)
                    ):
                        violations.append(
                            "branch_barrier_output_identity:"
                            f"{barrier_output['branch_id']}:"
                            f"{barrier_output['checkpoint']}"
                        )

                checkpoints = connection.execute(
                    "SELECT * FROM fanout_checkpoints"
                ).fetchall()
                for checkpoint in checkpoints:
                    expected_intent = {
                        "checkpoint": checkpoint["checkpoint"],
                        "coordinator_epoch": checkpoint["coordinator_epoch"],
                        "cut": checkpoint["cut_data_sequence"],
                        "branches": json.loads(checkpoint["branches_json"]),
                    }
                    if digest(expected_intent) != checkpoint["intent_digest"]:
                        violations.append(
                            f"fanout_checkpoint_intent:{checkpoint['checkpoint']}"
                        )
                    barrier = next(
                        (
                            row
                            for row in entries
                            if row["kind"] == "BARRIER"
                            and row["checkpoint"] == checkpoint["checkpoint"]
                        ),
                        None,
                    )
                    if barrier is None:
                        violations.append(
                            f"fanout_checkpoint_barrier_missing:{checkpoint['checkpoint']}"
                        )
                    observed = connection.execute(
                        "SELECT COUNT(*) FROM branch_barrier_observations WHERE checkpoint = ?",
                        (checkpoint["checkpoint"],),
                    ).fetchone()[0]
                    aligned = observed == len(branches)
                    if (checkpoint["status"] == "ALIGNED") != aligned:
                        violations.append(
                            f"fanout_checkpoint_status:{checkpoint['checkpoint']}"
                        )
                observations["branches"] = branch_view

            manifest = connection.execute("SELECT * FROM join_manifest").fetchone()
            if manifest is not None:
                inputs = json.loads(manifest["inputs_json"])
                bindings = json.loads(manifest["bindings_json"])
                stored_bindings = {
                    row["input_id"]: {
                        "branch": row["branch_id"],
                        "partition": row["partition_id"],
                    }
                    for row in connection.execute("SELECT * FROM join_inputs")
                }
                if set(inputs) != set(stored_bindings) or stored_bindings != bindings:
                    violations.append("join_input_binding")

                def valid_join_source(input_id: str, entry: sqlite3.Row | None) -> bool:
                    if entry is None or entry["kind"] != "DATA":
                        return False
                    source = branch_output_by_id.get(entry["source_output_id"])
                    binding = stored_bindings.get(input_id)
                    if source is None or binding is None:
                        return False
                    return (
                        source["branch_id"] == binding["branch"]
                        and source["partition_id"] == binding["partition"]
                        and source["upstream_data_sequence"] == entry["data_sequence"]
                        and source["output_id"] == entry["record_id"]
                        and source["payload_ref"] == entry["payload_ref"]
                        and source["payload_digest"] == entry["payload_digest"]
                        and source["payload_digest"]
                        == digest({"payload_ref": source["payload_ref"]})
                    )

                def valid_join_barrier(
                    input_id: str, entry: sqlite3.Row | None
                ) -> bool:
                    if entry is None or entry["kind"] != "BARRIER":
                        return False
                    source = branch_barrier_by_id.get(entry["source_barrier_id"])
                    binding = stored_bindings.get(input_id)
                    intent = connection.execute(
                        "SELECT * FROM join_checkpoint_intents WHERE checkpoint = ?",
                        (entry["checkpoint"],),
                    ).fetchone()
                    if source is None or binding is None or intent is None:
                        return False
                    return (
                        source["branch_id"] == binding["branch"]
                        and source["partition_id"] == binding["partition"]
                        and source["barrier_output_id"] == entry["record_id"]
                        and source["checkpoint"] == entry["checkpoint"]
                        and source["cut_data_sequence"] == entry["cut_data_sequence"]
                        and source["intent_digest"] == intent["fanout_intent_digest"]
                        and source["writer_epoch"] == intent["fanout_writer_epoch"]
                        and source["cut_data_sequence"]
                        == intent["fanout_cut_data_sequence"]
                    )

                intents = connection.execute(
                    "SELECT * FROM join_checkpoint_intents ORDER BY checkpoint"
                ).fetchall()
                for intent in intents:
                    expected_intent = {
                        "checkpoint": intent["checkpoint"],
                        "inputs": json.loads(intent["inputs_json"]),
                        "coordinator_epoch": intent["coordinator_epoch"],
                        "state_schema": intent["state_schema"],
                        "code_version": intent["code_version"],
                        "fanout_intent_digest": intent["fanout_intent_digest"],
                        "fanout_writer_epoch": intent["fanout_writer_epoch"],
                        "fanout_cut_data_sequence": intent["fanout_cut_data_sequence"],
                    }
                    if digest(expected_intent) != intent["intent_digest"]:
                        violations.append(
                            f"join_checkpoint_intent:{intent['checkpoint']}"
                        )

                entries_by_input: dict[str, list[sqlite3.Row]] = {}
                for input_id in inputs:
                    rows = connection.execute(
                        "SELECT * FROM join_entries WHERE input_id = ? ORDER BY log_sequence",
                        (input_id,),
                    ).fetchall()
                    entries_by_input[input_id] = rows
                    if [row["log_sequence"] for row in rows] != list(
                        range(1, len(rows) + 1)
                    ):
                        violations.append(f"join_log_gap:{input_id}")
                    data = [row for row in rows if row["kind"] == "DATA"]
                    if [row["data_sequence"] for row in data] != list(
                        range(1, len(data) + 1)
                    ):
                        violations.append(f"join_data_gap:{input_id}")
                    for row in data:
                        if not valid_join_source(input_id, row):
                            violations.append(
                                f"join_source_output:{input_id}:{row['data_sequence']}"
                            )
                    for barrier in (row for row in rows if row["kind"] == "BARRIER"):
                        cut = max(
                            (
                                row["data_sequence"]
                                for row in data
                                if row["log_sequence"] < barrier["log_sequence"]
                            ),
                            default=0,
                        )
                        intent = connection.execute(
                            "SELECT * FROM join_checkpoint_intents WHERE checkpoint = ?",
                            (barrier["checkpoint"],),
                        ).fetchone()
                        if barrier["cut_data_sequence"] != cut:
                            violations.append(
                                f"join_barrier_cut:{barrier['checkpoint']}:{input_id}"
                            )
                        if not valid_join_barrier(input_id, barrier):
                            violations.append(
                                f"join_source_barrier:{barrier['checkpoint']}:{input_id}"
                            )
                        if (
                            intent is None
                            or barrier["intent_digest"] != intent["intent_digest"]
                            or barrier["writer_epoch"] != intent["coordinator_epoch"]
                            or input_id not in json.loads(intent["inputs_json"])
                        ):
                            violations.append(
                                f"join_barrier_authority:{barrier['checkpoint']}:{input_id}"
                            )

                turns = connection.execute(
                    "SELECT * FROM join_turns ORDER BY apply_ordinal"
                ).fetchall()
                if [row["apply_ordinal"] for row in turns] != list(
                    range(1, len(turns) + 1)
                ):
                    violations.append("join_mailbox_order")
                state = manifest["initial_state"]
                states = {0: state}
                turns_by_input = {input_id: [] for input_id in inputs}
                outputs = connection.execute(
                    "SELECT * FROM join_outputs ORDER BY output_ordinal"
                ).fetchall()
                output_by_id = {row["output_id"]: row for row in outputs}
                for turn in turns:
                    entry = next(
                        (
                            row
                            for row in entries_by_input.get(turn["input_id"], [])
                            if row["log_sequence"] == turn["log_sequence"]
                        ),
                        None,
                    )
                    if (
                        entry is None
                        or entry["kind"] != "DATA"
                        or entry["data_sequence"] != turn["data_sequence"]
                        or entry["payload_ref"] != turn["payload_ref"]
                        or not valid_join_source(turn["input_id"], entry)
                    ):
                        violations.append(f"join_turn_identity:{turn['apply_ordinal']}")
                    state = apply_state(
                        state,
                        turn["input_id"],
                        turn["data_sequence"],
                        turn["payload_ref"],
                    )
                    if turn["state_after"] != state:
                        violations.append(f"join_turn_state:{turn['apply_ordinal']}")
                    states[turn["apply_ordinal"]] = state
                    turns_by_input.setdefault(turn["input_id"], []).append(turn)
                    output = output_by_id.get(turn["output_id"])
                    if (
                        output is None
                        or output["kind"] != "DATA"
                        or output["input_id"] != turn["input_id"]
                        or output["data_sequence"] != turn["data_sequence"]
                    ):
                        violations.append(
                            f"join_output_identity:{turn['apply_ordinal']}"
                        )
                live_state = connection.execute(
                    "SELECT state_value FROM join_state"
                ).fetchone()[0]
                if live_state != state:
                    violations.append("join_live_state")

                progress = {
                    row["input_id"]: row
                    for row in connection.execute("SELECT * FROM join_progress")
                }
                for input_id, row in progress.items():
                    sequences = [
                        turn["data_sequence"] for turn in turns_by_input[input_id]
                    ]
                    if sequences != list(range(1, row["completed_data_through"] + 1)):
                        violations.append(f"join_cursor_coverage:{input_id}")

                buffers = connection.execute(
                    "SELECT * FROM join_buffer ORDER BY arrival_ordinal"
                ).fetchall()
                buffer_arrivals = [row["arrival_ordinal"] for row in buffers]
                if buffer_arrivals != sorted(set(buffer_arrivals)):
                    violations.append("join_buffer_arrival_order")
                drained_apply = [
                    row["drain_apply_ordinal"] for row in buffers if row["drained"]
                ]
                if drained_apply != sorted(drained_apply):
                    violations.append("join_buffer_drain_order")
                for row in buffers:
                    entry = next(
                        (
                            entry
                            for entry in entries_by_input.get(row["input_id"], [])
                            if entry["log_sequence"] == row["log_sequence"]
                        ),
                        None,
                    )
                    barrier = next(
                        (
                            entry
                            for entry in entries_by_input.get(row["input_id"], [])
                            if entry["kind"] == "BARRIER"
                            and entry["checkpoint"] == row["checkpoint"]
                        ),
                        None,
                    )
                    if (
                        entry is None
                        or entry["kind"] != "DATA"
                        or entry["data_sequence"] != row["data_sequence"]
                        or entry["record_id"] != row["record_id"]
                        or entry["payload_digest"] != row["payload_digest"]
                        or entry["arrival_ordinal"] != row["arrival_ordinal"]
                        or not valid_join_source(row["input_id"], entry)
                        or barrier is None
                        or entry["log_sequence"] <= barrier["log_sequence"]
                    ):
                        violations.append(
                            f"join_buffer_identity:{row['input_id']}:{row['data_sequence']}"
                        )
                    if row["drained"]:
                        turn = next(
                            (
                                turn
                                for turn in turns
                                if turn["apply_ordinal"] == row["drain_apply_ordinal"]
                            ),
                            None,
                        )
                        if (
                            turn is None
                            or turn["source"] != "BUFFER"
                            or turn["input_id"] != row["input_id"]
                            or turn["data_sequence"] != row["data_sequence"]
                        ):
                            violations.append(
                                f"join_buffer_turn:{row['input_id']}:{row['data_sequence']}"
                            )
                for turn in turns:
                    if turn["source"] != "BUFFER":
                        continue
                    buffered = next(
                        (
                            row
                            for row in buffers
                            if row["drain_apply_ordinal"] == turn["apply_ordinal"]
                        ),
                        None,
                    )
                    if buffered is None:
                        violations.append(
                            f"join_buffer_turn_missing:{turn['apply_ordinal']}"
                        )

                snapshots = connection.execute(
                    "SELECT * FROM join_snapshots"
                ).fetchall()
                snapshot_by_checkpoint = {row["checkpoint"]: row for row in snapshots}
                for intent in intents:
                    has_snapshot = intent["checkpoint"] in snapshot_by_checkpoint
                    if (intent["status"] in ("ALIGNED", "COMPLETED")) != has_snapshot:
                        violations.append(
                            f"join_checkpoint_snapshot_status:{intent['checkpoint']}"
                        )
                for snapshot in snapshots:
                    checkpoint = snapshot["checkpoint"]
                    intent = connection.execute(
                        "SELECT * FROM join_checkpoint_intents WHERE checkpoint = ?",
                        (checkpoint,),
                    ).fetchone()
                    cuts = json.loads(snapshot["cuts_json"])
                    if (
                        intent is None
                        or snapshot["intent_digest"] != intent["intent_digest"]
                        or set(cuts) != set(inputs)
                    ):
                        violations.append(f"join_snapshot_authority:{checkpoint}")
                        continue
                    if snapshot["state_value"] != states.get(
                        snapshot["mailbox_highwater"]
                    ):
                        violations.append(f"join_snapshot_state:{checkpoint}")
                    for input_id, cut in cuts.items():
                        observation = connection.execute(
                            "SELECT * FROM join_observations "
                            "WHERE checkpoint = ? AND input_id = ?",
                            (checkpoint, input_id),
                        ).fetchone()
                        if (
                            observation is None
                            or cut["data"] != observation["cut_data_sequence"]
                            or cut["barrier_log"] != observation["log_sequence"]
                        ):
                            violations.append(
                                f"join_snapshot_cut:{checkpoint}:{input_id}"
                            )
                    barrier_output = output_by_id.get(f"join-barrier:{checkpoint}")
                    if barrier_output is None:
                        violations.append(f"join_forwarded_barrier:{checkpoint}")
                    else:
                        pre = [
                            output_by_id[turn["output_id"]]["output_ordinal"]
                            for turn in turns
                            if turn["apply_ordinal"] <= snapshot["mailbox_highwater"]
                        ]
                        drained = [
                            output_by_id[turn["output_id"]]["output_ordinal"]
                            for turn in turns
                            if turn["source"] == "BUFFER"
                        ]
                        if pre and max(pre) >= barrier_output["output_ordinal"]:
                            violations.append(
                                f"join_barrier_overtook_data:{checkpoint}"
                            )
                        if drained and min(drained) <= barrier_output["output_ordinal"]:
                            violations.append(
                                f"join_buffer_before_barrier:{checkpoint}"
                            )
                observations["join"] = {
                    "state": state,
                    "progress": {
                        input_id: row["completed_data_through"]
                        for input_id, row in progress.items()
                    },
                    "buffer_order": [
                        [row["input_id"], row["data_sequence"]] for row in buffers
                    ],
                }
        finally:
            connection.rollback()
            connection.close()
        return Report(tuple(sorted(set(violations))), observations)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    report = BranchJoinOracle.evaluate(parser.parse_args().database)
    print(json.dumps(report.as_dict(), sort_keys=True))
    return 0 if report.passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
