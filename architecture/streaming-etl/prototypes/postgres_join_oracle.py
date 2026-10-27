"""Independent read-only PostgreSQL aligned-join invariant reader."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json

import asyncpg


def decode(value):
    return json.loads(value) if isinstance(value, str) else value


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def verified_record(conn, record_id):
    row = await conn.fetchrow(
        """SELECT r.*,o.payload,o.published_at,
        o.payload_digest AS object_digest,o.payload_size AS object_size,
        o.schema_id AS object_schema,oi.payload_digest AS identity_digest,
        oi.payload_size AS identity_size,oi.schema_id AS identity_schema
        FROM records r JOIN objects o USING(object_key)
        JOIN object_identities oi USING(object_key) WHERE r.record_id=$1""",
        record_id,
    )
    if row is None:
        return None, False
    payload = bytes(row["payload"])
    identity = (hashlib.sha256(payload).hexdigest(), len(payload), row["schema_id"])
    valid = (
        row["published_at"] is not None
        and identity
        == (row["payload_digest"], row["payload_size"], row["object_schema"])
        and identity
        == (row["object_digest"], row["object_size"], row["identity_schema"])
        and identity == (row["identity_digest"], row["identity_size"], row["schema_id"])
    )
    if row["object_key"].startswith("sha256:"):
        valid = (
            valid
            and row["object_key"]
            == "sha256:"
            + hashlib.sha256(row["schema_id"].encode() + b"\0" + payload).hexdigest()
        )
    return row, valid


async def retirement_evidence(conn, rev, pid, errors):
    if not await conn.fetchval(
        "SELECT to_regclass('join_retention_anchors_v1') IS NOT NULL"
    ):
        return None, {}, {}
    anchor = await conn.fetchrow(
        "SELECT * FROM join_retention_anchors_v1 WHERE revision_id=$1 AND participant_id=$2",
        rev,
        pid,
    )
    if anchor is None:
        return None, {}, {}
    tag = rev + ":" + pid
    manifest = await conn.fetchrow(
        "SELECT * FROM join_retirement_manifests_v1 WHERE operation_id=$1",
        anchor["operation_id"],
    )
    if manifest is None:
        errors.append("retirement_manifest:" + tag)
        return anchor, {}, {}
    summary = {
        key: manifest[key]
        for key in (
            "prior_summary_digest",
            "revision_id",
            "participant_id",
            "checkpoint_id",
            "snapshot_object_key",
            "snapshot_payload_digest",
            "mailbox_highwater",
            "retired_apply_through",
            "retired_arrival_through",
            "state_digest",
        )
    }
    summary.update(
        state=decode(manifest["state_json"]),
        input_cuts=decode(manifest["input_cuts"]),
        row_counts=decode(manifest["row_counts"]),
    )
    identity = {
        "operation_id": manifest["operation_id"],
        **summary,
        "retired_apply_from": manifest["retired_apply_from"],
        "retired_arrival_from": manifest["retired_arrival_from"],
        "summary_digest": manifest["summary_digest"],
    }
    if (
        manifest["status"] != "COMPLETED"
        or digest(summary) != manifest["summary_digest"]
        or (
            digest(identity) != manifest["manifest_digest"]
            or anchor["summary_digest"] != manifest["summary_digest"]
            or anchor["state_digest"] != digest(decode(anchor["state_json"]))
            or decode(anchor["state_json"]) != decode(manifest["state_json"])
            or decode(anchor["input_cuts"]) != decode(manifest["input_cuts"])
        )
    ):
        errors.append("retirement_anchor_authority:" + tag)
    if manifest["retired_apply_from"] > 1:
        prior = await conn.fetchrow(
            """SELECT retired_apply_through,retired_arrival_through FROM join_retirement_manifests_v1
            WHERE summary_digest=$1 AND revision_id=$2 AND participant_id=$3 AND status='COMPLETED' """,
            manifest["prior_summary_digest"],
            rev,
            pid,
        )
        if prior is None and await conn.fetchval(
            "SELECT to_regclass('join_retirement_archive_heads_v1') IS NOT NULL"
        ):
            prior = await conn.fetchrow(
                """SELECT retired_apply_through,retired_arrival_through FROM join_retirement_archive_heads_v1
                WHERE revision_id=$1 AND participant_id=$2 AND archived_summary_digest=$3""",
                rev,
                pid,
                manifest["prior_summary_digest"],
            )
        if prior is None or (
            prior["retired_apply_through"],
            prior["retired_arrival_through"],
        ) != (manifest["retired_apply_from"] - 1, manifest["retired_arrival_from"] - 1):
            errors.append("retirement_prior_summary:" + tag)
    archived = {"TURN": {}, "RECEIPT": {}, "INVOCATION": {}}
    for row in await conn.fetch(
        """SELECT r.* FROM join_retirement_rows_v1 r
        JOIN join_retirement_manifests_v1 m USING(operation_id)
        WHERE m.revision_id=$1 AND m.participant_id=$2""",
        rev,
        pid,
    ):
        material = decode(row["row_json"])
        if row["row_digest"] != digest(material):
            errors.append("retirement_row_digest:" + tag)
        archived[row["row_kind"]][row["row_key"]] = material
    contiguous = sorted(int(k) for k in archived["TURN"]) == list(
        range(1, anchor["retired_apply_through"] + 1)
    ) and sorted(int(k) for k in archived["RECEIPT"]) == list(
        range(1, anchor["retired_arrival_through"] + 1)
    )
    return anchor, archived if contiguous else {}, archived["INVOCATION"]


async def evaluate(dsn, schema):
    conn = await asyncpg.connect(
        dsn, server_settings={"search_path": f'"{schema}",public'}
    )
    errors = []
    observations = []
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            for w in await conn.fetch("SELECT * FROM join_workers"):
                rev, pid = w["revision_id"], w["participant_id"]
                tag = rev + ":" + pid
                cfg = decode(w["config"])
                anchor, archive, archived_claims = await retirement_evidence(
                    conn, rev, pid, errors
                )
                floor_apply = (
                    anchor["retired_apply_through"]
                    if anchor is not None and not archive
                    else 0
                )
                floor_arrival = (
                    anchor["retired_arrival_through"]
                    if anchor is not None and not archive
                    else 0
                )
                input_bases = decode(anchor["input_cuts"]) if floor_apply else {}
                if w["config_digest"] != digest(cfg):
                    errors.append("config:" + tag)
                spec = await conn.fetchrow(
                    "SELECT * FROM revision_participants WHERE revision_id=$1 AND participant_id=$2",
                    rev,
                    pid,
                )
                if spec is None or (
                    decode(spec["input_manifest"]),
                    decode(spec["output_manifest"]),
                    spec["code_digest"],
                    spec["state_schema"],
                    spec["state_version"],
                ) != (
                    cfg["spec"]["inputs"],
                    cfg["spec"]["outputs"],
                    cfg["spec"]["code_digest"],
                    cfg["spec"]["state_schema"],
                    cfg["spec"]["state_version"],
                ):
                    errors.append("contract:" + tag)
                receipts = await conn.fetch(
                    "SELECT * FROM join_receipts_v3 WHERE revision_id=$1 AND participant_id=$2 ORDER BY arrival_ordinal",
                    rev,
                    pid,
                )
                turns = await conn.fetch(
                    "SELECT * FROM join_turns_v3 WHERE revision_id=$1 AND participant_id=$2 ORDER BY apply_ordinal",
                    rev,
                    pid,
                )
                hot_receipts = list(receipts)
                receipts = list(receipts)
                turns = list(turns)
                if archive:
                    receipts = sorted(
                        [*archive["RECEIPT"].values(), *receipts],
                        key=lambda r: r["arrival_ordinal"],
                    )
                    turns = sorted(
                        [*archive["TURN"].values(), *turns],
                        key=lambda t: t["apply_ordinal"],
                    )
                by_arrival = {r["arrival_ordinal"]: r for r in receipts}
                applied = {t["arrival_ordinal"] for t in turns}
                if [r["arrival_ordinal"] for r in receipts] != list(
                    range(floor_arrival + 1, w["next_arrival"])
                ):
                    errors.append("arrival_prefix:" + tag)
                if [t["apply_ordinal"] for t in turns] != list(
                    range(floor_apply + 1, w["next_apply"])
                ):
                    errors.append("apply_prefix:" + tag)
                for r in receipts:
                    identity = dict(r)
                    identity.pop("receipt_digest")
                    if digest(identity) != r["receipt_digest"]:
                        errors.append("receipt_digest:" + tag)
                    log = await conn.fetchrow(
                        "SELECT * FROM partition_log_entries WHERE channel=$1 AND partition_id=$2 AND log_sequence=$3",
                        r["channel"],
                        r["partition_id"],
                        r["log_sequence"],
                    )
                    if log is None or (
                        log["entry_kind"],
                        log["data_sequence"],
                        log["record_id"],
                    ) != ("DATA", r["data_sequence"], r["record_id"]):
                        errors.append("receipt_authority:" + tag)
                    source, valid = await verified_record(conn, r["record_id"])
                    if (
                        not valid
                        and anchor is not None
                        and r["arrival_ordinal"] <= anchor["retired_arrival_through"]
                    ):
                        source = await conn.fetchrow(
                            "SELECT *,sequence AS data_sequence FROM compaction_tombstones WHERE record_id=$1",
                            r["record_id"],
                        )
                        valid = source is not None
                    if not valid or (
                        source["channel"],
                        source["partition_id"],
                        source["sequence"],
                        source["object_key"],
                        source["payload_digest"],
                        source["payload_size"],
                    ) != (
                        r["channel"],
                        r["partition_id"],
                        r["data_sequence"],
                        r["object_key"],
                        r["payload_digest"],
                        r["payload_size"],
                    ):
                        errors.append("receipt_object_identity:" + tag)
                state = (
                    decode(anchor["state_json"])
                    if floor_apply
                    else cfg["initial_state"]
                )
                for t in turns:
                    identity = dict(t)
                    identity.pop("turn_digest")
                    identity["before_state"] = decode(identity["before_state"])
                    identity["after_state"] = decode(identity["after_state"])
                    if digest(identity) != t["turn_digest"]:
                        errors.append("turn_digest:" + tag)
                    if state != decode(t["before_state"]):
                        errors.append("state_chain:" + tag)
                    state = decode(t["after_state"])
                    r = by_arrival.get(t["arrival_ordinal"])
                    if r is None:
                        errors.append("turn_receipt:" + tag)
                        continue
                    claim = await conn.fetchrow(
                        "SELECT * FROM join_invocation_history_v3 WHERE token=$1",
                        t["invocation_token"],
                    )
                    if claim is None:
                        claim = archived_claims.get(t["invocation_token"])
                    if claim is None or (
                        claim["revision_id"],
                        claim["participant_id"],
                        claim["arrival_ordinal"],
                        claim["writer_epoch"],
                        claim["expected_apply"],
                    ) != (
                        rev,
                        pid,
                        t["arrival_ordinal"],
                        t["writer_epoch"],
                        t["apply_ordinal"],
                    ):
                        errors.append("terminal_invocation:" + tag)
                    if t["terminal"] == "YIELD":
                        output, valid_output = await verified_record(
                            conn, t["output_record_id"]
                        )
                        if (
                            not valid_output
                            and anchor is not None
                            and t["apply_ordinal"] <= anchor["retired_apply_through"]
                        ):
                            output = await conn.fetchrow(
                                "SELECT * FROM compaction_tombstones WHERE record_id=$1",
                                t["output_record_id"],
                            )
                            valid_output = output is not None
                        if (
                            not valid_output
                            or output["payload_digest"] != t["output_digest"]
                            or (output["channel"], output["partition_id"])
                            != (
                                cfg["spec"]["outputs"][0]["channel"],
                                cfg["spec"]["outputs"][0]["partition"],
                            )
                        ):
                            errors.append("turn_output:" + tag)
                        expected_id = "join-output:" + digest(
                            [rev, pid, r["channel"], r["partition_id"], r["record_id"]]
                        )
                        if t["output_record_id"] != expected_id:
                            errors.append("stable_output_identity:" + tag)
                    elif t["output_record_id"] is not None:
                        errors.append("return_output:" + tag)
                if state != decode(w["state"]):
                    errors.append("live_state:" + tag)
                invocation = await conn.fetchrow(
                    "SELECT * FROM join_invocations_v3 WHERE revision_id=$1 AND participant_id=$2",
                    rev,
                    pid,
                )
                if invocation is not None:
                    history = await conn.fetchrow(
                        "SELECT * FROM join_invocation_history_v3 WHERE token=$1",
                        invocation["token"],
                    )
                    if (
                        history is None
                        or (
                            history["revision_id"],
                            history["participant_id"],
                            history["arrival_ordinal"],
                            history["writer_epoch"],
                            history["expected_apply"],
                        )
                        != (
                            rev,
                            pid,
                            invocation["arrival_ordinal"],
                            invocation["writer_epoch"],
                            invocation["expected_apply"],
                        )
                        or (invocation["writer_epoch"], invocation["expected_apply"])
                        != (w["owner_epoch"], w["next_apply"])
                        or (
                            invocation["arrival_ordinal"] not in by_arrival
                            or invocation["arrival_ordinal"] in applied
                        )
                    ):
                        errors.append("live_invocation_authority:" + tag)
                inputs = await conn.fetch(
                    "SELECT * FROM join_inputs_v3 WHERE revision_id=$1 AND participant_id=$2",
                    rev,
                    pid,
                )
                if {(r["channel"], r["partition_id"]) for r in inputs} != {
                    (r["channel"], r["partition"]) for r in cfg["spec"]["inputs"]
                }:
                    errors.append("input_manifest:" + tag)
                for inp in inputs:
                    local = [
                        r
                        for r in receipts
                        if (r["channel"], r["partition_id"])
                        == (inp["channel"], inp["partition_id"])
                    ]
                    completed = sorted(
                        r["data_sequence"]
                        for r in local
                        if r["arrival_ordinal"] in applied
                    )
                    base = (
                        input_bases.get(inp["channel"] + "/" + inp["partition_id"], 1)
                        - 1
                    )
                    if completed != list(range(base + 1, inp["completed"] + 1)):
                        errors.append("completion_prefix:" + tag)
                    root = await conn.fetchrow(
                        "SELECT * FROM durable_refs WHERE ref_kind='JOIN_INPUT' AND ref_id=$1",
                        inp["retention_ref"],
                    )
                    if root is None or (
                        root["status"],
                        root["sequence"],
                        root["channel"],
                        root["partition_id"],
                    ) != (
                        "ACTIVE",
                        1
                        if anchor is None
                        else decode(anchor["input_cuts"]).get(
                            inp["channel"] + "/" + inp["partition_id"], 1
                        ),
                        inp["channel"],
                        inp["partition_id"],
                    ):
                        errors.append("replay_root:" + tag)
                    if any(r["log_sequence"] > inp["scanned"] for r in local):
                        errors.append("scan_prefix:" + tag)
                pending = [
                    r
                    for r in receipts
                    if r["arrival_ordinal"] not in applied
                    and r["blocked_checkpoint"] is not None
                ]
                if (
                    len(pending) > cfg["max_buffer_records"]
                    or sum(r["payload_size"] for r in pending) > cfg["max_buffer_bytes"]
                ):
                    errors.append("buffer_budget:" + tag)
                if len(hot_receipts) > cfg["max_mailbox_records"]:
                    errors.append("mailbox_budget:" + tag)
                metadata = 0
                for table in (
                    "join_workers",
                    "join_inputs_v3",
                    "join_receipts_v3",
                    "join_turns_v3",
                    "join_barriers_v3",
                    "join_alignments_v3",
                    "join_recoveries_v3",
                    "join_invocations_v3",
                    "join_invocation_history_v3",
                ):
                    metadata += await conn.fetchval(
                        f"SELECT COALESCE(sum(octet_length(row_to_json(x)::text)),0) FROM {table} x WHERE revision_id=$1 AND participant_id=$2",
                        rev,
                        pid,
                    )
                if metadata > cfg["max_metadata_bytes"]:
                    errors.append("metadata_budget:" + tag)
                for a in await conn.fetch(
                    "SELECT * FROM join_alignments_v3 WHERE revision_id=$1 AND participant_id=$2",
                    rev,
                    pid,
                ):
                    cp = a["checkpoint_id"]
                    if a["mailbox_highwater"] < floor_apply:
                        continue  # immutable history is outside the replay horizon
                    ack = await conn.fetchrow(
                        "SELECT * FROM participant_checkpoint_acks WHERE checkpoint_id=$1 AND participant_id=$2",
                        cp,
                        pid,
                    )
                    bars = await conn.fetch(
                        "SELECT * FROM join_barriers_v3 WHERE revision_id=$1 AND participant_id=$2 AND checkpoint_id=$3",
                        rev,
                        pid,
                        cp,
                    )
                    if len(bars) != len(inputs) or ack is None:
                        errors.append("alignment_manifest:" + tag)
                        continue
                    vector = decode(ack["barrier_vector"])
                    for b in bars:
                        key = b["channel"] + "/" + b["partition_id"]
                        if vector.get(key) != {
                            "cut_data_sequence": b["cut_sequence"],
                            "barrier_log_sequence": b["log_sequence"],
                        }:
                            errors.append("alignment_vector:" + tag)
                        pre = [
                            by_arrival[t["arrival_ordinal"]]["data_sequence"]
                            for t in turns
                            if t["apply_ordinal"] <= a["mailbox_highwater"]
                            and t["arrival_ordinal"] in by_arrival
                            and (
                                by_arrival[t["arrival_ordinal"]]["channel"],
                                by_arrival[t["arrival_ordinal"]]["partition_id"],
                            )
                            == (b["channel"], b["partition_id"])
                        ]
                        if sorted(pre) != list(
                            range(input_bases.get(key, 1), b["cut_sequence"] + 1)
                        ):
                            errors.append("snapshot_mixed_cut:" + tag)
                    snapshot_state = (
                        cfg["initial_state"]
                        if a["mailbox_highwater"] == 0
                        else next(
                            (
                                decode(t["after_state"])
                                for t in turns
                                if t["apply_ordinal"] == a["mailbox_highwater"]
                            ),
                            None,
                        )
                    )
                    if a["mailbox_highwater"] == floor_apply and floor_apply:
                        snapshot_state = decode(anchor["state_json"])
                    obj = await conn.fetchrow(
                        "SELECT * FROM checkpoint_snapshot_objects WHERE object_key=$1",
                        ack["snapshot_object_key"],
                    )
                    if (
                        obj is None
                        or json.loads(bytes(obj["payload"])) != snapshot_state
                        or a["state_digest"] != digest(snapshot_state)
                        or a["ack_digest"] != ack["ack_digest"]
                        or a["mailbox_highwater"] != ack["mailbox_highwater"]
                    ):
                        errors.append("snapshot_state:" + tag)
                    for key, pos in decode(ack["output_barriers"]).items():
                        for t in turns:
                            if t["output_record_id"] is None:
                                continue
                            outlog = await conn.fetchrow(
                                "SELECT log_sequence FROM partition_log_entries WHERE record_id=$1 AND entry_kind='DATA'",
                                t["output_record_id"],
                            )
                            if outlog is None or (
                                (outlog["log_sequence"] < pos["barrier_log_sequence"])
                                != (t["apply_ordinal"] <= a["mailbox_highwater"])
                            ):
                                errors.append("output_barrier_order:" + tag)
                for recovery in await conn.fetch(
                    "SELECT * FROM join_recoveries_v3 WHERE revision_id=$1 AND participant_id=$2",
                    rev,
                    pid,
                ):
                    plan = await conn.fetchrow(
                        "SELECT * FROM recovery_plans_v2 WHERE recovery_id=$1",
                        recovery["recovery_id"],
                    )
                    ack = (
                        None
                        if plan is None
                        else await conn.fetchrow(
                            "SELECT * FROM participant_checkpoint_acks WHERE checkpoint_id=$1 AND participant_id=$2",
                            plan["checkpoint_id"],
                            pid,
                        )
                    )
                    if (
                        plan is None
                        or ack is None
                        or plan["revision_id"] != rev
                        or (
                            decode(plan["participant_snapshots"]).get(pid)
                            != ack["snapshot_object_key"]
                        )
                        or (
                            recovery["replayed_through"] < ack["mailbox_highwater"]
                            or recovery["target_apply"] < ack["mailbox_highwater"]
                            or recovery["target_apply"] >= w["next_apply"]
                        )
                    ):
                        errors.append("recovery_plan_authority:" + tag)
                    ordinal = recovery["replayed_through"]
                    expected = (
                        cfg["initial_state"]
                        if ordinal == 0
                        else next(
                            (
                                decode(t["after_state"])
                                for t in turns
                                if t["apply_ordinal"] == ordinal
                            ),
                            None,
                        )
                    )
                    if ordinal == floor_apply and floor_apply:
                        expected = decode(anchor["state_json"])
                    if (
                        expected != decode(recovery["state"])
                        or ordinal > recovery["target_apply"]
                    ):
                        errors.append("recovery_state:" + tag)
                    if (
                        recovery["status"] == "READY"
                        and ordinal != recovery["target_apply"]
                    ):
                        errors.append("truncated_replay:" + tag)
                observations.append(
                    {
                        "participant": tag,
                        "receipts": len(receipts),
                        "turns": len(turns),
                        "buffered": len(pending),
                    }
                )
    finally:
        await conn.close()
    return {"passed": not errors, "violations": errors, "observations": observations}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dsn")
    parser.add_argument("schema")
    args = parser.parse_args()
    try:
        result = await evaluate(args.dsn, args.schema)
    except Exception as exc:  # noqa: BLE001 -- malformed storage must produce JSON evidence
        result = {
            "passed": False,
            "violations": [f"oracle_totality:{type(exc).__name__}:{exc}"],
        }
    print(json.dumps(result, sort_keys=True))
    return 0 if result["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
