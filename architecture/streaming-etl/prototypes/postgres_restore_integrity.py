"""Verify retained managed bytes before exporting or activating a restore."""

from __future__ import annotations

from postgres_restore_contracts import RestoreContractError


async def assert_retained_payload_integrity(conn) -> None:
    for table in ("objects", "checkpoint_snapshot_objects"):
        if await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table):
            corrupt = await conn.fetchval(
                f"SELECT EXISTS(SELECT 1 FROM {table} WHERE "
                "payload_size<>octet_length(payload) OR "
                "payload_digest<>pg_catalog.encode(pg_catalog.sha256(payload),'hex') OR "
                "object_key<>'sha256:'||pg_catalog.encode(pg_catalog.sha256("
                "pg_catalog.convert_to(schema_id,'UTF8')||pg_catalog.decode('00','hex')||payload),'hex'))"
            )
            if corrupt:
                raise RestoreContractError("retained managed object content is corrupt")
    if await conn.fetchval("SELECT to_regclass('records') IS NOT NULL"):
        missing = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM records r LEFT JOIN objects o USING(object_key) "
            "LEFT JOIN object_identities i USING(object_key) WHERE o.object_key IS NULL "
            "OR i.object_key IS NULL OR o.published_at IS NULL OR "
            "(r.payload_digest,r.payload_size,r.schema_id) IS DISTINCT FROM "
            "(o.payload_digest,o.payload_size,o.schema_id) OR "
            "(r.payload_digest,r.payload_size,r.schema_id) IS DISTINCT FROM "
            "(i.payload_digest,i.payload_size,i.schema_id))"
        )
        if missing:
            raise RestoreContractError("retained record object authority is missing")
    if await conn.fetchval(
        "SELECT to_regclass('participant_checkpoint_acks') IS NOT NULL"
    ):
        missing = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM participant_checkpoint_acks a "
            "LEFT JOIN checkpoint_snapshot_objects s ON s.object_key=a.snapshot_object_key "
            "WHERE s.object_key IS NULL OR s.published_at IS NULL OR "
            "a.snapshot_payload_digest<>s.payload_digest)"
        )
        if missing:
            raise RestoreContractError(
                "checkpoint snapshot object authority is missing"
            )
