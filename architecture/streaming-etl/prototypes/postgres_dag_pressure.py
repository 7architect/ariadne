"""Read-only pressure projection of durable DAG edge and mailbox authority.

Admission remains the data-plane partition/terminal transaction. This reader
does not issue producer credits or claim that a pressure snapshot reserves them.
"""

from __future__ import annotations

from postgres_barrier_coordinator import decoded, location_key


class PostgresDagPressure:
    def __init__(self, coordinator):
        self.pool = coordinator.pool

    async def snapshot(self, revision_id):
        async with (
            self.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            edges = await conn.fetch(
                "SELECT * FROM revision_edges WHERE revision_id=$1 ORDER BY channel,partition_id,target_id",
                revision_id,
            )
            edge_reports = []
            for edge in edges:
                part = await conn.fetchrow(
                    "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2",
                    edge["channel"],
                    edge["partition_id"],
                )
                head = await conn.fetchrow(
                    "SELECT * FROM partition_log_heads WHERE channel=$1 AND partition_id=$2",
                    edge["channel"],
                    edge["partition_id"],
                )
                progress = await conn.fetchrow(
                    """SELECT * FROM join_inputs_v3 WHERE revision_id=$1
                    AND participant_id=$2 AND channel=$3 AND partition_id=$4""",
                    revision_id,
                    edge["target_id"],
                    edge["channel"],
                    edge["partition_id"],
                )
                age = await conn.fetchval(
                    """SELECT COALESCE(EXTRACT(EPOCH FROM clock_timestamp()-min(created_at)),0)::double precision
                    FROM records WHERE channel=$1 AND partition_id=$2""",
                    edge["channel"],
                    edge["partition_id"],
                )
                dimensions = []
                for name, used, limit in (
                    ("records", part["used_records"], part["max_records"]),
                    ("bytes", part["used_bytes"], part["max_bytes"]),
                    (
                        "metadata",
                        part["used_metadata_records"],
                        part["max_metadata_records"],
                    ),
                    ("ordered_log", head["used_log_entries"], head["max_log_entries"]),
                ):
                    if used >= limit:
                        dimensions.append(name)
                if age >= part["max_age_seconds"]:
                    dimensions.append("age")
                completed = 0 if progress is None else progress["completed"]
                edge_reports.append(
                    {
                        "edge": location_key(edge["channel"], edge["partition_id"]),
                        "producer": edge["producer_id"],
                        "target": edge["target_id"],
                        "completed": completed,
                        "head": part["next_sequence"] - 1,
                        "lag_records": part["next_sequence"] - 1 - completed,
                        "used_records": part["used_records"],
                        "used_bytes": part["used_bytes"],
                        "oldest_record_age_seconds": age,
                        "binding_dimensions": dimensions,
                    }
                )
            workers = []
            for w in await conn.fetch(
                "SELECT * FROM join_workers WHERE revision_id=$1 ORDER BY participant_id",
                revision_id,
            ):
                cfg = decoded(w["config"])
                pending = await conn.fetchrow(
                    """SELECT count(*) AS records,COALESCE(sum(r.payload_size),0) AS bytes,
                    count(*) FILTER(WHERE blocked_checkpoint IS NOT NULL) AS buffered_records,
                    COALESCE(sum(r.payload_size) FILTER(WHERE blocked_checkpoint IS NOT NULL),0) AS buffered_bytes
                    FROM join_receipts_v3 r WHERE r.revision_id=$1 AND r.participant_id=$2
                    AND NOT EXISTS(SELECT 1 FROM join_turns_v3 t WHERE t.revision_id=r.revision_id
                      AND t.participant_id=r.participant_id AND t.arrival_ordinal=r.arrival_ordinal)""",
                    revision_id,
                    w["participant_id"],
                )
                alignment_age = 0
                if w["active_checkpoint"] is not None:
                    alignment_age = await conn.fetchval(
                        """SELECT EXTRACT(EPOCH FROM clock_timestamp()-created_at)::double precision
                        FROM checkpoint_intents_v2 WHERE checkpoint_id=$1""",
                        w["active_checkpoint"],
                    )
                workers.append(
                    {
                        "participant": w["participant_id"],
                        "phase": w["phase"],
                        "pending_records": pending["records"],
                        "pending_bytes": pending["bytes"],
                        "buffered_records": pending["buffered_records"],
                        "buffered_bytes": pending["buffered_bytes"],
                        "buffer_record_limit": cfg["max_buffer_records"],
                        "buffer_byte_limit": cfg["max_buffer_bytes"],
                        "alignment_age_seconds": alignment_age,
                        "binding_dimensions": [
                            key
                            for key, used, limit in (
                                (
                                    "buffer_records",
                                    pending["buffered_records"],
                                    cfg["max_buffer_records"],
                                ),
                                (
                                    "buffer_bytes",
                                    pending["buffered_bytes"],
                                    cfg["max_buffer_bytes"],
                                ),
                            )
                            if used >= limit
                        ],
                    }
                )
            return {
                "revision_id": revision_id,
                "edges": edge_reports,
                "workers": workers,
                "snapshot_reserves_credit": False,
            }
