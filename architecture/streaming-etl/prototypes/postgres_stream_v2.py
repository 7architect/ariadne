"""Production-shaped PostgreSQL data plane for protocol epoch 2.

This prototype intentionally depends only on asyncpg and PostgreSQL 17.  It is
kept outside Prefect runtime code until the storage/load and replacement gates
have passed.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import asyncpg


class PostgresProtocolError(RuntimeError):
    pass


class StaleOwner(PostgresProtocolError):
    pass


class CapacityBlocked(PostgresProtocolError):
    def __init__(self, attempt_ids: list[str], pressure: dict[str, Any]):
        self.attempt_ids = tuple(attempt_ids)
        self.pressure = pressure
        dimensions = ",".join(pressure["binding_dimensions"])
        super().__init__(
            f"durable channel capacity exhausted ({dimensions}); "
            f"attempts={','.join(attempt_ids)}"
        )


class IdempotencyHorizonExpired(PostgresProtocolError):
    pass


@dataclass(frozen=True)
class DeliveryToken:
    consumer_id: str
    sequence: int
    owner_id: str
    owner_epoch: int
    attempt: int


@dataclass(frozen=True)
class Envelope:
    channel: str
    partition: str
    sequence: int
    record_id: str
    object_key: str
    payload_digest: str
    payload_size: int
    schema_id: str
    lineage_digest: str
    payload: bytes


@dataclass(frozen=True)
class AppendResult:
    sequence: int
    record_id: str
    object_key: str


@dataclass(frozen=True)
class Output:
    channel: str
    partition: str
    record_id: str
    object_key: str
    payload: bytes
    schema_id: str
    lineage_digest: str


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def content_object_key(payload: bytes, schema_id: str) -> str:
    digest = hashlib.sha256(schema_id.encode() + b"\0" + payload).hexdigest()
    return f"sha256:{digest}"


def _canonical_metadata_cuts(
    cuts: list[tuple[str, str, int]],
) -> tuple[dict[tuple[str, str], int], list[dict[str, Any]], str, str]:
    cut_map = {(channel, partition): through for channel, partition, through in cuts}
    if not cuts or len(cut_map) != len(cuts):
        raise ValueError("metadata cuts must be non-empty and unique")
    canonical = [
        {
            "channel": channel,
            "partition": partition,
            "through": cut_map[(channel, partition)],
        }
        for channel, partition in sorted(cut_map)
    ]
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    return cut_map, canonical, encoded, digest


def metadata_operation_id(cuts: list[tuple[str, str, int]]) -> str:
    """Return the immutable operation identity for one full metadata cut map."""
    return f"metadata:{_canonical_metadata_cuts(cuts)[3]}"


def _identity_digest(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class PostgresStream:
    def __init__(self, pool: asyncpg.Pool, schema: str):
        self.pool = pool
        self.schema = schema

    @classmethod
    async def open(cls, dsn: str, schema: str) -> "PostgresStream":
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
            raise ValueError("unsafe schema name")
        bootstrap = await asyncpg.connect(dsn)
        try:
            await bootstrap.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        finally:
            await bootstrap.close()
        store = await cls.connect_existing(dsn, schema)
        await store._initialize()
        return store

    @classmethod
    async def connect_existing(cls, dsn: str, schema: str) -> "PostgresStream":
        """Create a worker pool without taking deployment-time DDL locks."""
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,47}", schema):
            raise ValueError("unsafe schema name")
        pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=12,
            server_settings={"search_path": f'"{schema}",public'},
        )
        return cls(pool, schema)

    async def close(self) -> None:
        await self.pool.close()

    async def _initialize(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS partitions (
            channel text NOT NULL,
            partition_id text NOT NULL,
            next_sequence bigint NOT NULL DEFAULT 1 CHECK (next_sequence > 0),
            max_records bigint NOT NULL CHECK (max_records > 0),
            max_bytes bigint NOT NULL CHECK (max_bytes > 0),
            max_age_seconds double precision NOT NULL DEFAULT 3600
                CHECK (max_age_seconds > 0),
            max_metadata_records bigint NOT NULL DEFAULT 1000000
                CHECK (max_metadata_records > 0),
            max_producers integer NOT NULL DEFAULT 128 CHECK (max_producers > 0),
            max_consumers integer NOT NULL DEFAULT 128 CHECK (max_consumers > 0),
            used_records bigint NOT NULL DEFAULT 0 CHECK (used_records >= 0),
            used_bytes bigint NOT NULL DEFAULT 0 CHECK (used_bytes >= 0),
            used_metadata_records bigint NOT NULL DEFAULT 0
                CHECK (used_metadata_records >= 0),
            PRIMARY KEY (channel, partition_id),
            CHECK (used_records <= max_records),
            CHECK (used_bytes <= max_bytes),
            CHECK (used_metadata_records <= max_metadata_records)
        );
        CREATE TABLE IF NOT EXISTS object_identities (
            object_key text PRIMARY KEY,
            payload_digest text NOT NULL,
            payload_size bigint NOT NULL CHECK (payload_size >= 0),
            schema_id text NOT NULL,
            first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS objects (
            object_key text PRIMARY KEY REFERENCES object_identities(object_key),
            payload bytea NOT NULL,
            payload_digest text NOT NULL,
            payload_size bigint NOT NULL CHECK (payload_size >= 0),
            schema_id text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            published_at timestamptz
        );
        CREATE TABLE IF NOT EXISTS append_attempts (
            attempt_id text PRIMARY KEY,
            channel text NOT NULL,
            partition_id text NOT NULL,
            record_id text NOT NULL,
            object_key text NOT NULL,
            payload_digest text NOT NULL,
            payload_size bigint NOT NULL,
            schema_id text NOT NULL,
            lineage_digest text NOT NULL,
            identity_digest text NOT NULL UNIQUE,
            status text NOT NULL CHECK (status IN ('BLOCKED', 'APPENDED')),
            sequence bigint,
            block_reason text,
            pressure_snapshot jsonb,
            producer_id text,
            producer_sequence bigint,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            CHECK ((status = 'BLOCKED' AND sequence IS NULL)
                OR (status = 'APPENDED' AND sequence IS NOT NULL))
        );
        CREATE TABLE IF NOT EXISTS records (
            channel text NOT NULL,
            partition_id text NOT NULL,
            sequence bigint NOT NULL CHECK (sequence > 0),
            record_id text NOT NULL UNIQUE,
            object_key text NOT NULL REFERENCES objects(object_key),
            payload_digest text NOT NULL,
            payload_size bigint NOT NULL,
            schema_id text NOT NULL,
            lineage_digest text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (channel, partition_id, sequence),
            FOREIGN KEY (channel, partition_id)
                REFERENCES partitions(channel, partition_id)
        );
        CREATE INDEX IF NOT EXISTS records_partition_created_idx
            ON records(channel, partition_id, created_at);
        CREATE TABLE IF NOT EXISTS consumers (
            consumer_id text PRIMARY KEY,
            channel text NOT NULL,
            partition_id text NOT NULL,
            start_sequence bigint NOT NULL CHECK (start_sequence > 0),
            completed_through bigint NOT NULL,
            in_flight_sequence bigint,
            in_flight_end bigint,
            delivery_attempt bigint NOT NULL DEFAULT 0,
            owner_id text,
            owner_epoch bigint NOT NULL DEFAULT 0,
            lease_expires_at timestamptz,
            state_schema text NOT NULL DEFAULT 'none/v1',
            state_version integer NOT NULL DEFAULT 1,
            managed_state jsonb NOT NULL DEFAULT '{}'::jsonb,
            completion_retired_through bigint NOT NULL DEFAULT 0,
            FOREIGN KEY (channel, partition_id)
                REFERENCES partitions(channel, partition_id)
        );
        CREATE TABLE IF NOT EXISTS runtime_invocations (
            consumer_id text PRIMARY KEY REFERENCES consumers(consumer_id),
            input_sequence bigint NOT NULL,
            attempt bigint NOT NULL,
            owner_id text NOT NULL,
            owner_epoch bigint NOT NULL,
            holder_id text NOT NULL UNIQUE,
            expires_at timestamptz NOT NULL
        );
        CREATE TABLE IF NOT EXISTS turn_completions (
            consumer_id text NOT NULL REFERENCES consumers(consumer_id),
            input_sequence bigint NOT NULL,
            completion_kind text NOT NULL CHECK (completion_kind IN ('YIELD', 'RETURN')),
            owner_id text NOT NULL,
            owner_epoch bigint NOT NULL,
            output_channel text,
            output_partition text,
            output_sequence bigint,
            state_schema text NOT NULL,
            state_version integer NOT NULL,
            managed_state jsonb NOT NULL,
            completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (consumer_id, input_sequence),
            CHECK ((completion_kind = 'RETURN' AND output_sequence IS NULL)
                OR (completion_kind = 'YIELD' AND output_sequence IS NOT NULL))
        );
        CREATE TABLE IF NOT EXISTS checkpoints (
            checkpoint_id text PRIMARY KEY,
            channel text NOT NULL,
            partition_id text NOT NULL,
            cut_sequence bigint NOT NULL,
            status text NOT NULL CHECK (status IN ('SEALED','RELEASED'))
        );
        CREATE TABLE IF NOT EXISTS durable_refs (
            ref_kind text NOT NULL,
            ref_id text NOT NULL,
            channel text NOT NULL,
            partition_id text NOT NULL,
            sequence bigint NOT NULL,
            status text NOT NULL CHECK (status IN ('ACTIVE','RELEASED')),
            PRIMARY KEY(ref_kind,ref_id)
        );
        CREATE TABLE IF NOT EXISTS compaction_manifests (
            manifest_id bigserial PRIMARY KEY,
            channel text NOT NULL,
            partition_id text NOT NULL,
            requested_through bigint NOT NULL,
            safe_through bigint NOT NULL,
            roots jsonb NOT NULL,
            sequences jsonb NOT NULL,
            status text NOT NULL CHECK (status IN ('PLANNED','COMPLETED')),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        CREATE TABLE IF NOT EXISTS compaction_tombstones (
            channel text NOT NULL,
            partition_id text NOT NULL,
            sequence bigint NOT NULL,
            manifest_id bigint NOT NULL REFERENCES compaction_manifests(manifest_id),
            record_id text NOT NULL UNIQUE,
            object_key text NOT NULL,
            payload_digest text NOT NULL,
            payload_size bigint NOT NULL,
            schema_id text NOT NULL,
            lineage_digest text NOT NULL,
            PRIMARY KEY(channel,partition_id,sequence)
        );
        CREATE TABLE IF NOT EXISTS producer_progress (
            channel text NOT NULL,
            partition_id text NOT NULL,
            producer_id text NOT NULL,
            accepted_through bigint NOT NULL DEFAULT 0,
            retired_through bigint NOT NULL DEFAULT 0,
            PRIMARY KEY(channel,partition_id,producer_id),
            FOREIGN KEY(channel,partition_id)
                REFERENCES partitions(channel,partition_id)
        );
        CREATE TABLE IF NOT EXISTS metadata_summaries (
            channel text NOT NULL,
            partition_id text NOT NULL,
            retired_through bigint NOT NULL DEFAULT 0,
            retired_records bigint NOT NULL DEFAULT 0,
            retired_completions bigint NOT NULL DEFAULT 0,
            rolling_digest text NOT NULL,
            prior_digest text NOT NULL,
            operation_id text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(channel,partition_id),
            FOREIGN KEY(channel,partition_id)
                REFERENCES partitions(channel,partition_id)
        );
        CREATE TABLE IF NOT EXISTS metadata_operations (
            operation_id text PRIMARY KEY,
            cuts_digest text NOT NULL,
            cuts jsonb NOT NULL,
            outcome jsonb NOT NULL,
            completed_at timestamptz NOT NULL DEFAULT clock_timestamp()
        );
        ALTER TABLE consumers ADD COLUMN IF NOT EXISTS in_flight_end bigint;
        ALTER TABLE partitions ADD COLUMN IF NOT EXISTS max_age_seconds
            double precision NOT NULL DEFAULT 3600 CHECK (max_age_seconds > 0);
        ALTER TABLE partitions ADD COLUMN IF NOT EXISTS max_metadata_records
            bigint NOT NULL DEFAULT 1000000 CHECK (max_metadata_records > 0);
        ALTER TABLE partitions ADD COLUMN IF NOT EXISTS max_producers
            integer NOT NULL DEFAULT 128 CHECK (max_producers > 0);
        ALTER TABLE partitions ADD COLUMN IF NOT EXISTS max_consumers
            integer NOT NULL DEFAULT 128 CHECK (max_consumers > 0);
        ALTER TABLE partitions ADD COLUMN IF NOT EXISTS used_metadata_records
            bigint NOT NULL DEFAULT 0 CHECK (used_metadata_records >= 0);
        ALTER TABLE append_attempts ADD COLUMN IF NOT EXISTS block_reason text;
        ALTER TABLE append_attempts ADD COLUMN IF NOT EXISTS pressure_snapshot jsonb;
        ALTER TABLE append_attempts ADD COLUMN IF NOT EXISTS producer_id text;
        ALTER TABLE append_attempts ADD COLUMN IF NOT EXISTS producer_sequence bigint;
        ALTER TABLE append_attempts ADD COLUMN IF NOT EXISTS created_at
            timestamptz NOT NULL DEFAULT clock_timestamp();
        ALTER TABLE consumers ADD COLUMN IF NOT EXISTS completion_retired_through
            bigint NOT NULL DEFAULT 0;
        INSERT INTO object_identities(object_key,payload_digest,payload_size,schema_id)
            SELECT object_key,payload_digest,payload_size,schema_id FROM objects
            ON CONFLICT (object_key) DO NOTHING;
        INSERT INTO object_identities(object_key,payload_digest,payload_size,schema_id)
            SELECT object_key,payload_digest,payload_size,schema_id
            FROM compaction_tombstones
            ON CONFLICT (object_key) DO NOTHING;
        """
        async with self.pool.acquire() as conn:
            await conn.execute(ddl)

    async def reset(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "TRUNCATE metadata_operations, metadata_summaries, producer_progress, "
                "compaction_tombstones, "
                "compaction_manifests, durable_refs, "
                "checkpoints, turn_completions, runtime_invocations, consumers, "
                "records, append_attempts, "
                "objects, object_identities, partitions CASCADE"
            )

    async def create_partition(
        self,
        channel: str,
        partition: str,
        *,
        max_records: int,
        max_bytes: int,
        max_age_seconds: float = 3600,
        max_metadata_records: int | None = None,
        max_producers: int = 128,
        max_consumers: int = 128,
    ) -> None:
        metadata_limit = (
            max_records * 4 if max_metadata_records is None else max_metadata_records
        )
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                """INSERT INTO partitions(
                   channel,partition_id,max_records,max_bytes,max_age_seconds,
                   max_metadata_records,max_producers,max_consumers)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING""",
                channel,
                partition,
                max_records,
                max_bytes,
                max_age_seconds,
                metadata_limit,
                max_producers,
                max_consumers,
            )
            if result == "INSERT 0 0":
                row = await conn.fetchrow(
                    "SELECT max_records,max_bytes,max_age_seconds,max_metadata_records,"
                    "max_producers,max_consumers FROM partitions "
                    "WHERE channel=$1 AND partition_id=$2",
                    channel,
                    partition,
                )
                if (
                    row["max_records"],
                    row["max_bytes"],
                    row["max_age_seconds"],
                    row["max_metadata_records"],
                    row["max_producers"],
                    row["max_consumers"],
                ) != (
                    max_records,
                    max_bytes,
                    float(max_age_seconds),
                    metadata_limit,
                    max_producers,
                    max_consumers,
                ):
                    raise PostgresProtocolError("partition capacity drift")

    @staticmethod
    def _pressure_values(
        part: asyncpg.Record,
        *,
        added_records: int,
        added_bytes: int,
        added_metadata_records: int,
        oldest_age_seconds: float | None,
    ) -> dict[str, Any]:
        projected_records = part["used_records"] + added_records
        projected_bytes = part["used_bytes"] + added_bytes
        projected_metadata = part["used_metadata_records"] + added_metadata_records
        dimensions = []
        if projected_records > part["max_records"]:
            dimensions.append("records")
        if projected_bytes > part["max_bytes"]:
            dimensions.append("bytes")
        if projected_metadata > part["max_metadata_records"]:
            dimensions.append("metadata")
        if (
            oldest_age_seconds is not None
            and oldest_age_seconds >= part["max_age_seconds"]
        ):
            dimensions.append("age")
        return {
            "channel": part["channel"],
            "partition": part["partition_id"],
            "used_records": part["used_records"],
            "max_records": part["max_records"],
            "projected_records": projected_records,
            "used_bytes": part["used_bytes"],
            "max_bytes": part["max_bytes"],
            "projected_bytes": projected_bytes,
            "used_metadata_records": part["used_metadata_records"],
            "max_metadata_records": part["max_metadata_records"],
            "projected_metadata_records": projected_metadata,
            "oldest_age_seconds": oldest_age_seconds,
            "max_age_seconds": part["max_age_seconds"],
            "binding_dimensions": dimensions,
            "retry_condition": "retention root advances and compaction returns credit",
        }

    async def pressure_snapshot(
        self,
        channel: str,
        partition: str,
        *,
        proposed_records: int = 0,
        proposed_bytes: int = 0,
        proposed_metadata_records: int = 0,
        proposed_consumers: int = 0,
    ) -> dict[str, Any]:
        """Return public, predictive record/byte/time pressure state."""
        if (
            min(
                proposed_records,
                proposed_bytes,
                proposed_metadata_records,
                proposed_consumers,
            )
            < 0
        ):
            raise ValueError("proposed pressure must be non-negative")
        async with (
            self.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            oldest_age = await conn.fetchval(
                """SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(created_at))
                   ::double precision FROM records
                   WHERE channel=$1 AND partition_id=$2""",
                channel,
                partition,
            )
            pressure = self._pressure_values(
                part,
                added_records=proposed_records,
                added_bytes=proposed_bytes,
                added_metadata_records=proposed_metadata_records,
                oldest_age_seconds=oldest_age,
            )
            counts = {}
            counts["append_attempts"] = await conn.fetchval(
                "SELECT count(*) FROM append_attempts WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            )
            counts["tombstones"] = await conn.fetchval(
                "SELECT count(*) FROM compaction_tombstones WHERE channel=$1 "
                "AND partition_id=$2",
                channel,
                partition,
            )
            counts["completions"] = await conn.fetchval(
                """SELECT count(*) FROM turn_completions t JOIN consumers c USING(consumer_id)
                   WHERE c.channel=$1 AND c.partition_id=$2""",
                channel,
                partition,
            )
            counts["manifests"] = await conn.fetchval(
                "SELECT count(*) FROM compaction_manifests WHERE channel=$1 "
                "AND partition_id=$2",
                channel,
                partition,
            )
            counts["producers"] = await conn.fetchval(
                "SELECT count(*) FROM producer_progress WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            )
            counts["consumers"] = await conn.fetchval(
                "SELECT count(*) FROM consumers WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            )
            pressure["used_consumers"] = counts["consumers"]
            pressure["max_consumers"] = part["max_consumers"]
            pressure["projected_consumers"] = counts["consumers"] + proposed_consumers
            if pressure["projected_consumers"] > part["max_consumers"]:
                pressure["binding_dimensions"].append("consumers")
            counts["metadata_operations"] = await conn.fetchval(
                """SELECT count(*) FROM metadata_operations
                   WHERE cuts @> $1::jsonb""",
                json.dumps([{"channel": channel, "partition": partition}]),
            )
            pressure["hot_metadata_rows"] = counts
            pressure["hot_metadata_rows_total"] = sum(counts.values())
            pressure["oldest_metadata_age_seconds"] = await conn.fetchval(
                """SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(created_at))
                   ::double precision FROM append_attempts
                   WHERE channel=$1 AND partition_id=$2""",
                channel,
                partition,
            )
            return pressure

    @staticmethod
    async def _bind_object_identity(
        conn: asyncpg.Connection,
        object_key: str,
        payload_digest: str,
        payload_size: int,
        schema_id: str,
    ) -> None:
        await conn.execute(
            """INSERT INTO object_identities(
               object_key,payload_digest,payload_size,schema_id)
               VALUES($1,$2,$3,$4) ON CONFLICT (object_key) DO NOTHING""",
            object_key,
            payload_digest,
            payload_size,
            schema_id,
        )
        identity = await conn.fetchrow(
            "SELECT * FROM object_identities WHERE object_key=$1 FOR UPDATE",
            object_key,
        )
        if identity is None or (
            identity["payload_digest"],
            identity["payload_size"],
            identity["schema_id"],
        ) != (payload_digest, payload_size, schema_id):
            raise PostgresProtocolError("object identity drift")

    @staticmethod
    async def _bind_object_identities(
        conn: asyncpg.Connection,
        identities: dict[str, tuple[str, int, str]],
    ) -> None:
        """Bind and lock a batch of identities in deterministic key order."""
        keys = sorted(identities)
        await conn.execute(
            """INSERT INTO object_identities(
               object_key,payload_digest,payload_size,schema_id)
               SELECT * FROM unnest($1::text[],$2::text[],$3::bigint[],$4::text[])
               ON CONFLICT (object_key) DO NOTHING""",
            keys,
            [identities[key][0] for key in keys],
            [identities[key][1] for key in keys],
            [identities[key][2] for key in keys],
        )
        rows = await conn.fetch(
            "SELECT * FROM object_identities WHERE object_key=ANY($1::text[]) "
            "ORDER BY object_key FOR UPDATE",
            keys,
        )
        if len(rows) != len(keys):
            raise PostgresProtocolError("object identity witness missing")
        for row in rows:
            expected = identities[row["object_key"]]
            if (
                row["payload_digest"],
                row["payload_size"],
                row["schema_id"],
            ) != expected:
                raise PostgresProtocolError("object identity drift")

    async def stage_object(
        self, object_key: str, payload: bytes, schema_id: str
    ) -> None:
        payload_digest = _sha256(payload)
        async with self.pool.acquire() as conn, conn.transaction():
            await self._bind_object_identity(
                conn, object_key, payload_digest, len(payload), schema_id
            )
            row = await conn.fetchrow(
                "SELECT * FROM objects WHERE object_key=$1 FOR UPDATE", object_key
            )
            if row is None:
                await conn.execute(
                    "INSERT INTO objects(object_key,payload,payload_digest,payload_size,schema_id) "
                    "VALUES($1,$2,$3,$4,$5)",
                    object_key,
                    payload,
                    payload_digest,
                    len(payload),
                    schema_id,
                )
            elif (
                bytes(row["payload"]),
                row["payload_digest"],
                row["payload_size"],
                row["schema_id"],
            ) != (payload, payload_digest, len(payload), schema_id):
                raise PostgresProtocolError("object identity drift")

    async def register_producer(
        self, channel: str, partition: str, producer_id: str
    ) -> None:
        if not producer_id:
            raise ValueError("producer_id is required")
        async with self.pool.acquire() as conn, conn.transaction():
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            current = await conn.fetchval(
                "SELECT count(*) FROM producer_progress WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            )
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM producer_progress WHERE channel=$1 "
                "AND partition_id=$2 AND producer_id=$3)",
                channel,
                partition,
                producer_id,
            )
            if not exists and current >= part["max_producers"]:
                raise CapacityBlocked(
                    [f"producer:{producer_id}"],
                    {
                        **self._pressure_values(
                            part,
                            added_records=0,
                            added_bytes=0,
                            added_metadata_records=0,
                            oldest_age_seconds=None,
                        ),
                        "binding_dimensions": ["producers"],
                    },
                )
            await conn.execute(
                """INSERT INTO producer_progress(channel,partition_id,producer_id)
                   VALUES($1,$2,$3) ON CONFLICT DO NOTHING""",
                channel,
                partition,
                producer_id,
            )

    @staticmethod
    async def _check_producer(
        conn: asyncpg.Connection,
        channel: str,
        partition: str,
        producer_id: str | None,
        producer_sequence: int | None,
        object_key: str,
        payload: bytes,
        schema_id: str,
    ) -> asyncpg.Record | None:
        if (producer_id is None) != (producer_sequence is None):
            raise PostgresProtocolError("producer identity requires id and sequence")
        if producer_id is None:
            return None
        if producer_sequence is None or producer_sequence <= 0:
            raise PostgresProtocolError("producer sequence must be positive")
        if object_key != content_object_key(payload, schema_id):
            raise PostgresProtocolError(
                "structured producer requires content object key"
            )
        progress = await conn.fetchrow(
            "SELECT * FROM producer_progress WHERE channel=$1 AND partition_id=$2 "
            "AND producer_id=$3 FOR UPDATE",
            channel,
            partition,
            producer_id,
        )
        if progress is None:
            raise PostgresProtocolError("producer is not registered")
        if producer_sequence <= progress["retired_through"]:
            raise IdempotencyHorizonExpired(
                f"producer sequence {producer_sequence} retired through "
                f"{progress['retired_through']}"
            )
        if producer_sequence <= progress["accepted_through"]:
            raise PostgresProtocolError(
                "producer sequence detail missing inside hot horizon"
            )
        if producer_sequence != progress["accepted_through"] + 1:
            raise PostgresProtocolError("producer sequence gap")
        return progress

    @staticmethod
    def _append_identity(
        attempt_id: str,
        channel: str,
        partition: str,
        record_id: str,
        object_key: str,
        payload: bytes,
        schema_id: str,
        lineage_digest: str,
        producer_id: str | None = None,
        producer_sequence: int | None = None,
    ) -> tuple[dict[str, Any], str]:
        identity = {
            "attempt_id": attempt_id,
            "channel": channel,
            "partition": partition,
            "record_id": record_id,
            "object_key": object_key,
            "payload_digest": _sha256(payload),
            "payload_size": len(payload),
            "schema_id": schema_id,
            "lineage_digest": lineage_digest,
            "producer_id": producer_id,
            "producer_sequence": producer_sequence,
        }
        return identity, _identity_digest(identity)

    async def append(
        self,
        attempt_id: str,
        channel: str,
        partition: str,
        record_id: str,
        object_key: str,
        payload: bytes,
        schema_id: str,
        lineage_digest: str,
        *,
        producer_id: str | None = None,
        producer_sequence: int | None = None,
    ) -> AppendResult:
        async with self.pool.acquire() as conn:
            return await self._append_with_connection(
                conn,
                attempt_id,
                channel,
                partition,
                record_id,
                object_key,
                payload,
                schema_id,
                lineage_digest,
                producer_id,
                producer_sequence,
            )

    async def append_batch(
        self,
        items: list[tuple],
    ) -> list[AppendResult]:
        """Group record acknowledgements behind one WAL durability boundary."""
        if not items:
            return []
        normalized = []
        for item in items:
            if len(item) == 8:
                normalized.append((*item, None, None))
            elif len(item) == 10:
                normalized.append(item)
            else:
                raise ValueError("append batch items require 8 or 10 fields")
        items = normalized
        locations = {(item[1], item[2]) for item in items}
        if len(locations) != 1:
            raise PostgresProtocolError("one append batch must target one partition")
        if len({item[0] for item in items}) != len(items):
            raise PostgresProtocolError("duplicate attempt in append batch")
        channel, partition = next(iter(locations))
        identities = [
            self._append_identity(
                attempt_id,
                item_channel,
                item_partition,
                record_id,
                object_key,
                payload,
                schema_id,
                lineage_digest,
                producer_id,
                producer_sequence,
            )
            for (
                attempt_id,
                item_channel,
                item_partition,
                record_id,
                object_key,
                payload,
                schema_id,
                lineage_digest,
                producer_id,
                producer_sequence,
            ) in items
        ]
        blocked = False
        pressure: dict[str, Any] | None = None
        results: list[AppendResult] = []
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{channel}|{partition}",
            )
            attempt_ids = [item[0] for item in items]
            existing_attempts = await conn.fetch(
                "SELECT attempt_id FROM append_attempts WHERE attempt_id=ANY($1::text[])",
                attempt_ids,
            )
            if existing_attempts:
                for item in items:
                    results.append(await self._append_with_connection(conn, *item))
                return results
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            producer_pairs = [(item[8], item[9]) for item in items]
            if any(pair != (None, None) for pair in producer_pairs):
                if any(pair == (None, None) for pair in producer_pairs):
                    raise PostgresProtocolError(
                        "cannot mix structured and legacy producers"
                    )
                producer_ids = {pair[0] for pair in producer_pairs}
                if len(producer_ids) != 1:
                    raise PostgresProtocolError("one batch requires one producer")
                producer_id = producer_pairs[0][0]
                sequences = [pair[1] for pair in producer_pairs]
                for item in items:
                    if item[4] != content_object_key(item[5], item[6]):
                        raise PostgresProtocolError(
                            "structured producer requires content object key"
                        )
                progress = await conn.fetchrow(
                    "SELECT * FROM producer_progress WHERE channel=$1 AND partition_id=$2 "
                    "AND producer_id=$3 FOR UPDATE",
                    channel,
                    partition,
                    producer_id,
                )
                if progress is None:
                    raise PostgresProtocolError("producer is not registered")
                if any(
                    sequence <= progress["retired_through"] for sequence in sequences
                ):
                    raise IdempotencyHorizonExpired("producer batch is retired")
                expected = list(
                    range(
                        progress["accepted_through"] + 1,
                        progress["accepted_through"] + len(items) + 1,
                    )
                )
                if sequences != expected:
                    raise PostgresProtocolError("producer sequence gap")
            else:
                producer_id = None
                sequences = []
            object_keys = sorted({item[4] for item in items})
            representative: dict[str, tuple[bytes, str, str]] = {}
            for item, (identity, _) in zip(items, identities):
                key, payload, schema_id = item[4], item[5], item[6]
                expected = (payload, identity["payload_digest"], schema_id)
                if key in representative and representative[key] != expected:
                    raise PostgresProtocolError("object identity drift within batch")
                representative[key] = expected
            await self._bind_object_identities(
                conn,
                {
                    key: (payload_digest, len(payload), schema_id)
                    for key, (
                        payload,
                        payload_digest,
                        schema_id,
                    ) in representative.items()
                },
            )
            # Another partition may have published the same content-addressed
            # object while this transaction waited for the identity row.  The
            # identity locks serialize all contenders, so this statement sees
            # any object published by a transaction that we waited for.
            existing_objects = {
                row["object_key"]: row
                for row in await conn.fetch(
                    "SELECT * FROM objects WHERE object_key=ANY($1::text[]) FOR UPDATE",
                    object_keys,
                )
            }
            for key, expected in representative.items():
                existing = existing_objects.get(key)
                if (
                    existing is not None
                    and (
                        bytes(existing["payload"]),
                        existing["payload_digest"],
                        existing["schema_id"],
                    )
                    != expected
                ):
                    raise PostgresProtocolError("object identity drift")
            if await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM records WHERE record_id=ANY($1::text[])) "
                "OR EXISTS(SELECT 1 FROM compaction_tombstones "
                "WHERE record_id=ANY($1::text[]))",
                [item[3] for item in items],
            ):
                raise PostgresProtocolError("record exists without append attempt")
            locally_used = {
                row["object_key"]
                for row in await conn.fetch(
                    "SELECT DISTINCT object_key FROM records WHERE channel=$1 "
                    "AND partition_id=$2 AND object_key=ANY($3::text[])",
                    channel,
                    partition,
                    object_keys,
                )
            }
            added_bytes = sum(
                len(representative[key][0])
                for key in object_keys
                if key not in locally_used
            )
            oldest_age = await conn.fetchval(
                """SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(created_at))
                   ::double precision FROM records
                   WHERE channel=$1 AND partition_id=$2""",
                channel,
                partition,
            )
            pressure = self._pressure_values(
                part,
                added_records=len(items),
                added_bytes=added_bytes,
                added_metadata_records=len(items),
                oldest_age_seconds=oldest_age,
            )
            blocked = bool(pressure["binding_dimensions"])
            await conn.executemany(
                """INSERT INTO append_attempts(
                   attempt_id,channel,partition_id,record_id,object_key,payload_digest,
                   payload_size,schema_id,lineage_digest,identity_digest,status,
                   producer_id,producer_sequence)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'BLOCKED',$11,$12)""",
                [
                    (
                        item[0],
                        item[1],
                        item[2],
                        item[3],
                        item[4],
                        identity["payload_digest"],
                        identity["payload_size"],
                        item[6],
                        item[7],
                        identity_digest,
                        item[8],
                        item[9],
                    )
                    for item, (identity, identity_digest) in zip(items, identities)
                ],
            )
            if blocked:
                await conn.execute(
                    """UPDATE append_attempts SET block_reason=$2,
                       pressure_snapshot=$3::jsonb
                       WHERE attempt_id=ANY($1::text[])""",
                    attempt_ids,
                    ",".join(pressure["binding_dimensions"]),
                    json.dumps(pressure, sort_keys=True),
                )
            if not blocked:
                new_keys = [key for key in object_keys if key not in existing_objects]
                await conn.executemany(
                    """INSERT INTO objects(
                       object_key,payload,payload_digest,payload_size,schema_id,published_at)
                       VALUES($1,$2,$3,$4,$5,clock_timestamp())""",
                    [
                        (
                            key,
                            representative[key][0],
                            representative[key][1],
                            len(representative[key][0]),
                            representative[key][2],
                        )
                        for key in new_keys
                    ],
                )
                staged_keys = [
                    key
                    for key in object_keys
                    if key in existing_objects
                    and existing_objects[key]["published_at"] is None
                ]
                if staged_keys:
                    await conn.execute(
                        "UPDATE objects SET published_at=clock_timestamp() "
                        "WHERE object_key=ANY($1::text[])",
                        staged_keys,
                    )
                first_sequence = part["next_sequence"]
                record_rows = []
                for offset, (item, (identity, _)) in enumerate(zip(items, identities)):
                    sequence = first_sequence + offset
                    record_rows.append(
                        (
                            channel,
                            partition,
                            sequence,
                            item[3],
                            item[4],
                            identity["payload_digest"],
                            identity["payload_size"],
                            item[6],
                            item[7],
                        )
                    )
                    results.append(AppendResult(sequence, item[3], item[4]))
                await conn.executemany(
                    """INSERT INTO records(
                       channel,partition_id,sequence,record_id,object_key,payload_digest,
                       payload_size,schema_id,lineage_digest)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                    record_rows,
                )
                await conn.execute(
                    """UPDATE partitions SET next_sequence=next_sequence+$3,
                       used_records=used_records+$3,used_bytes=used_bytes+$4,
                       used_metadata_records=used_metadata_records+$3
                       WHERE channel=$1 AND partition_id=$2""",
                    channel,
                    partition,
                    len(items),
                    added_bytes,
                )
                await conn.executemany(
                    "UPDATE append_attempts SET status='APPENDED',sequence=$2,"
                    "block_reason=NULL,pressure_snapshot=NULL "
                    "WHERE attempt_id=$1 AND status='BLOCKED'",
                    [
                        (item[0], first_sequence + offset)
                        for offset, item in enumerate(items)
                    ],
                )
                if producer_id is not None:
                    await conn.execute(
                        """UPDATE producer_progress SET accepted_through=$4
                           WHERE channel=$1 AND partition_id=$2 AND producer_id=$3""",
                        channel,
                        partition,
                        producer_id,
                        sequences[-1],
                    )
        if blocked:
            assert pressure is not None
            raise CapacityBlocked([item[0] for item in items], pressure)
        return results

    async def _append_with_connection(
        self,
        conn: asyncpg.Connection,
        attempt_id: str,
        channel: str,
        partition: str,
        record_id: str,
        object_key: str,
        payload: bytes,
        schema_id: str,
        lineage_digest: str,
        producer_id: str | None = None,
        producer_sequence: int | None = None,
    ) -> AppendResult:
        identity, identity_digest = self._append_identity(
            attempt_id,
            channel,
            partition,
            record_id,
            object_key,
            payload,
            schema_id,
            lineage_digest,
            producer_id,
            producer_sequence,
        )
        blocked = False
        pressure: dict[str, Any] | None = None
        result: AppendResult | None = None
        async with conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{channel}|{partition}",
            )
            attempt = await conn.fetchrow(
                "SELECT * FROM append_attempts WHERE attempt_id=$1 FOR UPDATE",
                attempt_id,
            )
            if attempt is not None:
                if attempt["identity_digest"] != identity_digest:
                    raise PostgresProtocolError("append attempt identity drift")
                if attempt["status"] == "APPENDED":
                    witness = await conn.fetchrow(
                        "SELECT * FROM records WHERE channel=$1 AND partition_id=$2 "
                        "AND sequence=$3",
                        channel,
                        partition,
                        attempt["sequence"],
                    )
                    if witness is None:
                        witness = await conn.fetchrow(
                            "SELECT * FROM compaction_tombstones WHERE channel=$1 "
                            "AND partition_id=$2 AND sequence=$3",
                            channel,
                            partition,
                            attempt["sequence"],
                        )
                    expected = (
                        record_id,
                        object_key,
                        identity["payload_digest"],
                        identity["payload_size"],
                        schema_id,
                        lineage_digest,
                    )
                    actual = (
                        None
                        if witness is None
                        else (
                            witness["record_id"],
                            witness["object_key"],
                            witness["payload_digest"],
                            witness["payload_size"],
                            witness["schema_id"],
                            witness["lineage_digest"],
                        )
                    )
                    if actual != expected:
                        raise PostgresProtocolError("append result witness missing")
                    return AppendResult(attempt["sequence"], record_id, object_key)
            else:
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM records WHERE record_id=$1) OR "
                    "EXISTS(SELECT 1 FROM compaction_tombstones WHERE record_id=$1)",
                    record_id,
                ):
                    raise PostgresProtocolError("record exists without append attempt")
                await conn.execute(
                    """INSERT INTO append_attempts(
                        attempt_id,channel,partition_id,record_id,object_key,
                        payload_digest,payload_size,schema_id,lineage_digest,
                        identity_digest,status,producer_id,producer_sequence)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'BLOCKED',$11,$12)""",
                    attempt_id,
                    channel,
                    partition,
                    record_id,
                    object_key,
                    identity["payload_digest"],
                    identity["payload_size"],
                    schema_id,
                    lineage_digest,
                    identity_digest,
                    producer_id,
                    producer_sequence,
                )
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            progress = await self._check_producer(
                conn,
                channel,
                partition,
                producer_id,
                producer_sequence,
                object_key,
                payload,
                schema_id,
            )
            await self._bind_object_identity(
                conn, object_key, identity["payload_digest"], len(payload), schema_id
            )
            obj = await conn.fetchrow(
                "SELECT * FROM objects WHERE object_key=$1 FOR UPDATE", object_key
            )
            if obj is not None and (
                bytes(obj["payload"]),
                obj["payload_digest"],
                obj["payload_size"],
                obj["schema_id"],
            ) != (payload, identity["payload_digest"], len(payload), schema_id):
                raise PostgresProtocolError("object identity drift")
            partition_has_object = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM records WHERE channel=$1 AND partition_id=$2 "
                "AND object_key=$3)",
                channel,
                partition,
                object_key,
            )
            added_bytes = 0 if partition_has_object else len(payload)
            oldest_age = await conn.fetchval(
                """SELECT EXTRACT(EPOCH FROM clock_timestamp()-min(created_at))
                   ::double precision FROM records
                   WHERE channel=$1 AND partition_id=$2""",
                channel,
                partition,
            )
            pressure = self._pressure_values(
                part,
                added_records=1,
                added_bytes=added_bytes,
                added_metadata_records=1,
                oldest_age_seconds=oldest_age,
            )
            blocked = bool(pressure["binding_dimensions"])
            if blocked:
                await conn.execute(
                    """UPDATE append_attempts SET block_reason=$2,
                       pressure_snapshot=$3::jsonb WHERE attempt_id=$1""",
                    attempt_id,
                    ",".join(pressure["binding_dimensions"]),
                    json.dumps(pressure, sort_keys=True),
                )
            else:
                if obj is None:
                    await conn.execute(
                        """INSERT INTO objects(
                            object_key,payload,payload_digest,payload_size,schema_id,published_at)
                           VALUES($1,$2,$3,$4,$5,clock_timestamp())""",
                        object_key,
                        payload,
                        identity["payload_digest"],
                        len(payload),
                        schema_id,
                    )
                elif obj["published_at"] is None:
                    await conn.execute(
                        "UPDATE objects SET published_at=clock_timestamp() WHERE object_key=$1",
                        object_key,
                    )
                sequence = part["next_sequence"]
                await conn.execute(
                    """INSERT INTO records(
                        channel,partition_id,sequence,record_id,object_key,payload_digest,
                        payload_size,schema_id,lineage_digest)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
                    channel,
                    partition,
                    sequence,
                    record_id,
                    object_key,
                    identity["payload_digest"],
                    len(payload),
                    schema_id,
                    lineage_digest,
                )
                await conn.execute(
                    """UPDATE partitions SET next_sequence=next_sequence+1,
                       used_records=used_records+1, used_bytes=used_bytes+$3,
                       used_metadata_records=used_metadata_records+1
                       WHERE channel=$1 AND partition_id=$2""",
                    channel,
                    partition,
                    added_bytes,
                )
                await conn.execute(
                    "UPDATE append_attempts SET status='APPENDED',sequence=$2,"
                    "block_reason=NULL,pressure_snapshot=NULL "
                    "WHERE attempt_id=$1 AND status='BLOCKED'",
                    attempt_id,
                    sequence,
                )
                if progress is not None:
                    await conn.execute(
                        """UPDATE producer_progress SET accepted_through=$4
                           WHERE channel=$1 AND partition_id=$2 AND producer_id=$3""",
                        channel,
                        partition,
                        producer_id,
                        producer_sequence,
                    )
                result = AppendResult(sequence, record_id, object_key)
        if blocked:
            assert pressure is not None
            raise CapacityBlocked([attempt_id], pressure)
        if result is None:
            raise AssertionError("append produced neither result nor block")
        return result

    async def create_consumer(
        self,
        consumer_id: str,
        channel: str,
        partition: str,
        *,
        start_sequence: int | None = None,
    ) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{channel}|{partition}",
            )
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM consumers WHERE consumer_id=$1)",
                consumer_id,
            )
            consumer_count = await conn.fetchval(
                "SELECT count(*) FROM consumers WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            )
            if not exists and consumer_count >= part["max_consumers"]:
                pressure = self._pressure_values(
                    part,
                    added_records=0,
                    added_bytes=0,
                    added_metadata_records=0,
                    oldest_age_seconds=None,
                )
                pressure.update(
                    {
                        "used_consumers": consumer_count,
                        "max_consumers": part["max_consumers"],
                        "projected_consumers": consumer_count + 1,
                        "binding_dimensions": ["consumers"],
                    }
                )
                raise CapacityBlocked([f"consumer:{consumer_id}"], pressure)
            floor = await conn.fetchval(
                "SELECT COALESCE(min(sequence),$3) FROM records WHERE channel=$1 "
                "AND partition_id=$2",
                channel,
                partition,
                part["next_sequence"],
            )
            start = floor if start_sequence is None else start_sequence
            if start < floor or start > part["next_sequence"]:
                raise PostgresProtocolError("consumer start outside retained suffix")
            await conn.execute(
                """INSERT INTO consumers(
                    consumer_id,channel,partition_id,start_sequence,completed_through)
                   VALUES($1,$2,$3,$4::bigint,$4::bigint-1) ON CONFLICT DO NOTHING""",
                consumer_id,
                channel,
                partition,
                start,
            )
            row = await conn.fetchrow(
                "SELECT channel,partition_id,start_sequence FROM consumers WHERE consumer_id=$1",
                consumer_id,
            )
            if tuple(row) != (channel, partition, start):
                raise PostgresProtocolError("consumer identity drift")

    async def retained_floor(self, channel: str, partition: str) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT COALESCE((SELECT min(sequence) FROM records WHERE channel=$1 "
                "AND partition_id=$2), (SELECT next_sequence FROM partitions "
                "WHERE channel=$1 AND partition_id=$2))",
                channel,
                partition,
            )

    async def seal_checkpoint(
        self, checkpoint_id: str, channel: str, partition: str, cut_sequence: int
    ) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            floor = await conn.fetchval(
                "SELECT COALESCE((SELECT min(sequence) FROM records WHERE channel=$1 "
                "AND partition_id=$2), (SELECT next_sequence FROM partitions "
                "WHERE channel=$1 AND partition_id=$2))",
                channel,
                partition,
            )
            if cut_sequence + 1 < floor or cut_sequence + 1 > part["next_sequence"]:
                raise PostgresProtocolError("checkpoint outside retained suffix")
            await conn.execute(
                "INSERT INTO checkpoints VALUES($1,$2,$3,$4,'SEALED')",
                checkpoint_id,
                channel,
                partition,
                cut_sequence,
            )

    async def release_checkpoint(self, checkpoint_id: str) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            root = await conn.fetchrow(
                "SELECT channel,partition_id FROM checkpoints WHERE checkpoint_id=$1",
                checkpoint_id,
            )
            if root is None:
                raise PostgresProtocolError("checkpoint is not sealed")
            await conn.fetchrow(
                "SELECT 1 FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                root["channel"],
                root["partition_id"],
            )
            result = await conn.execute(
                "UPDATE checkpoints SET status='RELEASED' WHERE checkpoint_id=$1 "
                "AND status='SEALED'",
                checkpoint_id,
            )
            if result != "UPDATE 1":
                raise PostgresProtocolError("checkpoint is not sealed")

    async def add_ref(
        self, kind: str, ref_id: str, channel: str, partition: str, sequence: int
    ) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            floor = await conn.fetchval(
                "SELECT COALESCE((SELECT min(sequence) FROM records WHERE channel=$1 "
                "AND partition_id=$2), (SELECT next_sequence FROM partitions "
                "WHERE channel=$1 AND partition_id=$2))",
                channel,
                partition,
            )
            if sequence < floor or sequence >= part["next_sequence"]:
                raise PostgresProtocolError("reference outside retained suffix")
            await conn.execute(
                "INSERT INTO durable_refs VALUES($1,$2,$3,$4,$5,'ACTIVE')",
                kind,
                ref_id,
                channel,
                partition,
                sequence,
            )

    async def release_ref(self, kind: str, ref_id: str) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            root = await conn.fetchrow(
                "SELECT channel,partition_id FROM durable_refs WHERE ref_kind=$1 AND ref_id=$2",
                kind,
                ref_id,
            )
            if root is None:
                raise PostgresProtocolError("reference is not active")
            await conn.fetchrow(
                "SELECT 1 FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                root["channel"],
                root["partition_id"],
            )
            result = await conn.execute(
                "UPDATE durable_refs SET status='RELEASED' WHERE ref_kind=$1 "
                "AND ref_id=$2 AND status='ACTIVE'",
                kind,
                ref_id,
            )
            if result != "UPDATE 1":
                raise PostgresProtocolError("reference is not active")

    async def compact(
        self,
        channel: str,
        partition: str,
        requested_through: int,
        *,
        after_root_snapshot: Callable[[], Awaitable[None]] | None = None,
    ) -> dict[str, Any]:
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|{channel}|{partition}",
            )
            part = await conn.fetchrow(
                "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                channel,
                partition,
            )
            if part is None:
                raise PostgresProtocolError("unknown partition")
            roots: list[dict[str, Any]] = []
            for row in await conn.fetch(
                "SELECT consumer_id,completed_through,in_flight_sequence FROM consumers "
                "WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
            ):
                roots.append(
                    {
                        "kind": "CONSUMER",
                        "id": row["consumer_id"],
                        "sequence": row["completed_through"] + 1,
                    }
                )
                if row["in_flight_sequence"] is not None:
                    roots.append(
                        {
                            "kind": "IN_FLIGHT",
                            "id": row["consumer_id"],
                            "sequence": row["in_flight_sequence"],
                        }
                    )
            for row in await conn.fetch(
                "SELECT checkpoint_id,cut_sequence FROM checkpoints WHERE channel=$1 "
                "AND partition_id=$2 AND status='SEALED'",
                channel,
                partition,
            ):
                roots.append(
                    {
                        "kind": "CHECKPOINT",
                        "id": row["checkpoint_id"],
                        "sequence": row["cut_sequence"] + 1,
                    }
                )
            for row in await conn.fetch(
                "SELECT ref_kind,ref_id,sequence FROM durable_refs WHERE channel=$1 "
                "AND partition_id=$2 AND status='ACTIVE'",
                channel,
                partition,
            ):
                roots.append(
                    {
                        "kind": row["ref_kind"],
                        "id": row["ref_id"],
                        "sequence": row["sequence"],
                    }
                )
            if after_root_snapshot is not None:
                await after_root_snapshot()
            safe = min(
                requested_through,
                min(
                    (root["sequence"] - 1 for root in roots), default=requested_through
                ),
            )
            rows = await conn.fetch(
                "SELECT * FROM records WHERE channel=$1 AND partition_id=$2 "
                "AND sequence<=$3 ORDER BY sequence",
                channel,
                partition,
                safe,
            )
            sequences = [row["sequence"] for row in rows]
            manifest_id = await conn.fetchval(
                """INSERT INTO compaction_manifests(
                    channel,partition_id,requested_through,safe_through,roots,sequences,status)
                   VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb,'PLANNED') RETURNING manifest_id""",
                channel,
                partition,
                requested_through,
                safe,
                json.dumps(roots, sort_keys=True),
                json.dumps(sequences),
            )
            await conn.executemany(
                """INSERT INTO compaction_tombstones VALUES(
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
                [
                    (
                        channel,
                        partition,
                        row["sequence"],
                        manifest_id,
                        row["record_id"],
                        row["object_key"],
                        row["payload_digest"],
                        row["payload_size"],
                        row["schema_id"],
                        row["lineage_digest"],
                    )
                    for row in rows
                ],
            )
            candidate_keys = sorted({row["object_key"] for row in rows})
            if sequences:
                await conn.execute(
                    "DELETE FROM records WHERE channel=$1 AND partition_id=$2 "
                    "AND sequence=ANY($3::bigint[])",
                    channel,
                    partition,
                    sequences,
                )
            remaining_local = {
                row["object_key"]
                for row in await conn.fetch(
                    "SELECT DISTINCT object_key FROM records WHERE channel=$1 "
                    "AND partition_id=$2 AND object_key=ANY($3::text[])",
                    channel,
                    partition,
                    candidate_keys,
                )
            }
            size_by_key = {row["object_key"]: row["payload_size"] for row in rows}
            reclaimed_bytes = sum(
                size_by_key[key] for key in candidate_keys if key not in remaining_local
            )
            if candidate_keys:
                await conn.execute(
                    "DELETE FROM objects o WHERE o.object_key=ANY($1::text[]) "
                    "AND NOT EXISTS(SELECT 1 FROM records r "
                    "WHERE r.object_key=o.object_key)",
                    candidate_keys,
                )
            await conn.execute(
                "UPDATE partitions SET used_records=used_records-$3,used_bytes=used_bytes-$4 "
                "WHERE channel=$1 AND partition_id=$2",
                channel,
                partition,
                len(rows),
                reclaimed_bytes,
            )
            await conn.execute(
                "UPDATE compaction_manifests SET status='COMPLETED' WHERE manifest_id=$1",
                manifest_id,
            )
            return {
                "manifest_id": manifest_id,
                "requested_through": requested_through,
                "safe_through": safe,
                "roots": roots,
                "sequences": sequences,
                "reclaimed_records": len(rows),
                "reclaimed_bytes": reclaimed_bytes,
            }

    async def compact_metadata(
        self,
        cuts: list[tuple[str, str, int]],
        *,
        operation_id: str,
    ) -> dict[str, Any]:
        """Atomically retire one contiguous hot-metadata prefix per partition.

        Exact identity retries are supported only above the resulting producer
        and consumer retired watermarks.  The rolling summary proves aggregate
        coverage; exact historical audit requires a separate sealed archive.
        """
        if not cuts or not operation_id:
            raise ValueError("cuts and operation_id are required")
        cut_map, canonical_cuts, cuts_json, cuts_digest = _canonical_metadata_cuts(cuts)
        if operation_id != f"metadata:{cuts_digest}":
            raise PostgresProtocolError("metadata operation identity drift")
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                f"{self.schema}|metadata-operation|{operation_id}",
            )
            prior_operation = await conn.fetchrow(
                "SELECT * FROM metadata_operations WHERE operation_id=$1 FOR UPDATE",
                operation_id,
            )
            if prior_operation is not None:
                if (
                    prior_operation["cuts_digest"] != cuts_digest
                    or json.dumps(
                        json.loads(prior_operation["cuts"]),
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    != cuts_json
                ):
                    raise PostgresProtocolError("metadata operation identity drift")
                outcome = json.loads(prior_operation["outcome"])
                outcome["idempotent_retry"] = True
                return outcome
            parts: dict[tuple[str, str], asyncpg.Record] = {}
            summaries: dict[tuple[str, str], asyncpg.Record | None] = {}
            for location in sorted(cut_map):
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    f"{self.schema}|{location[0]}|{location[1]}",
                )
                part = await conn.fetchrow(
                    "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 FOR UPDATE",
                    *location,
                )
                if part is None:
                    raise PostgresProtocolError("unknown metadata partition")
                parts[location] = part
                summaries[location] = await conn.fetchrow(
                    "SELECT * FROM metadata_summaries WHERE channel=$1 AND partition_id=$2 "
                    "FOR UPDATE",
                    *location,
                )
            material: dict[tuple[str, str], dict[str, Any]] = {}
            all_object_keys: set[str] = set()
            for location, through in cut_map.items():
                summary = summaries[location]
                retired = 0 if summary is None else summary["retired_through"]
                if through <= retired:
                    raise IdempotencyHorizonExpired("metadata cut is already retired")
                if through >= parts[location]["next_sequence"]:
                    raise PostgresProtocolError("metadata cut exceeds appended prefix")
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM records WHERE channel=$1 "
                    "AND partition_id=$2 AND sequence<=$3)",
                    *location,
                    through,
                ):
                    raise PostgresProtocolError(
                        "metadata retirement requires data compaction"
                    )
                tombstones = await conn.fetch(
                    "SELECT * FROM compaction_tombstones WHERE channel=$1 "
                    "AND partition_id=$2 AND sequence>$3 AND sequence<=$4 ORDER BY sequence",
                    *location,
                    retired,
                    through,
                )
                if [row["sequence"] for row in tombstones] != list(
                    range(retired + 1, through + 1)
                ):
                    raise PostgresProtocolError(
                        "metadata retirement prefix is not contiguous"
                    )
                if await conn.fetchval(
                    "SELECT EXISTS(SELECT 1 FROM consumers WHERE channel=$1 "
                    "AND partition_id=$2 AND in_flight_sequence<=$3)",
                    *location,
                    through,
                ):
                    raise PostgresProtocolError(
                        "metadata prefix has in-flight delivery"
                    )
                attempts = await conn.fetch(
                    "SELECT * FROM append_attempts WHERE channel=$1 AND partition_id=$2 "
                    "AND status='APPENDED' AND sequence>$3 AND sequence<=$4 ORDER BY sequence",
                    *location,
                    retired,
                    through,
                )
                if len(attempts) != through - retired or any(
                    row["producer_id"] is None or row["producer_sequence"] is None
                    for row in attempts
                ):
                    raise PostgresProtocolError(
                        "metadata prefix contains legacy or missing append authority"
                    )
                for row in attempts:
                    if not row["object_key"].startswith("sha256:"):
                        raise PostgresProtocolError(
                            "metadata prefix is not content addressed"
                        )
                    all_object_keys.add(row["object_key"])
                completions = await conn.fetch(
                    """SELECT t.* FROM turn_completions t JOIN consumers c USING(consumer_id)
                       WHERE c.channel=$1 AND c.partition_id=$2
                         AND t.input_sequence>$3 AND t.input_sequence<=$4
                       ORDER BY t.consumer_id,t.input_sequence""",
                    *location,
                    retired,
                    through,
                )
                material[location] = {
                    "retired": retired,
                    "through": through,
                    "attempts": attempts,
                    "tombstones": tombstones,
                    "completions": completions,
                }

            for location, rows in material.items():
                for completion in rows["completions"]:
                    if completion["completion_kind"] != "YIELD":
                        continue
                    output_location = (
                        completion["output_channel"],
                        completion["output_partition"],
                    )
                    output_cut = cut_map.get(output_location)
                    if output_cut is None or completion["output_sequence"] > output_cut:
                        raise PostgresProtocolError(
                            "YIELD metadata requires atomic output-partition retirement"
                        )

            result_locations: dict[str, Any] = {}
            for location, rows in material.items():
                summary = summaries[location]
                prior_digest = (
                    "0" * 64 if summary is None else summary["rolling_digest"]
                )
                canonical = {
                    "location": location,
                    "from": rows["retired"] + 1,
                    "through": rows["through"],
                    "attempts": [dict(row) for row in rows["attempts"]],
                    "tombstones": [dict(row) for row in rows["tombstones"]],
                    "completions": [dict(row) for row in rows["completions"]],
                }
                rolling = hashlib.sha256(
                    (
                        prior_digest
                        + json.dumps(
                            canonical,
                            sort_keys=True,
                            separators=(",", ":"),
                            default=str,
                        )
                    ).encode()
                ).hexdigest()
                completion_delete_count = await conn.fetchval(
                    """WITH deleted AS (
                       DELETE FROM turn_completions t USING consumers c
                       WHERE t.consumer_id=c.consumer_id AND c.channel=$1
                         AND c.partition_id=$2 AND t.input_sequence<=$3
                         AND t.input_sequence<c.completed_through
                       RETURNING 1) SELECT count(*) FROM deleted""",
                    *location,
                    rows["through"],
                )
                await conn.execute(
                    """UPDATE consumers SET completion_retired_through=GREATEST(
                       completion_retired_through,LEAST($3,completed_through-1))
                       WHERE channel=$1 AND partition_id=$2""",
                    *location,
                    rows["through"],
                )
                for attempt in rows["attempts"]:
                    await conn.execute(
                        """UPDATE producer_progress SET retired_through=GREATEST(
                           retired_through,$4) WHERE channel=$1 AND partition_id=$2
                           AND producer_id=$3""",
                        *location,
                        attempt["producer_id"],
                        attempt["producer_sequence"],
                    )
                await conn.execute(
                    "DELETE FROM compaction_tombstones WHERE channel=$1 "
                    "AND partition_id=$2 AND sequence<=$3",
                    *location,
                    rows["through"],
                )
                deleted_attempts = await conn.fetchval(
                    """WITH deleted AS (
                       DELETE FROM append_attempts WHERE channel=$1 AND partition_id=$2
                         AND status='APPENDED' AND sequence<=$3 RETURNING 1)
                       SELECT count(*) FROM deleted""",
                    *location,
                    rows["through"],
                )
                await conn.execute(
                    """UPDATE partitions SET used_metadata_records=
                       used_metadata_records-$3 WHERE channel=$1 AND partition_id=$2""",
                    *location,
                    deleted_attempts,
                )
                previous_records = 0 if summary is None else summary["retired_records"]
                previous_completions = (
                    0 if summary is None else summary["retired_completions"]
                )
                await conn.execute(
                    """INSERT INTO metadata_summaries(
                       channel,partition_id,retired_through,retired_records,
                       retired_completions,rolling_digest,prior_digest,operation_id)
                       VALUES($1,$2,$3,$4,$5,$6,$7,$8)
                       ON CONFLICT(channel,partition_id) DO UPDATE SET
                         retired_through=EXCLUDED.retired_through,
                         retired_records=EXCLUDED.retired_records,
                         retired_completions=EXCLUDED.retired_completions,
                         rolling_digest=EXCLUDED.rolling_digest,
                         prior_digest=EXCLUDED.prior_digest,
                         operation_id=EXCLUDED.operation_id,
                         updated_at=clock_timestamp()""",
                    *location,
                    rows["through"],
                    previous_records + deleted_attempts,
                    previous_completions + completion_delete_count,
                    rolling,
                    prior_digest,
                    operation_id,
                )
                producer_anchors = [
                    dict(row)
                    for row in await conn.fetch(
                        """SELECT producer_id,retired_through
                           FROM producer_progress
                           WHERE channel=$1 AND partition_id=$2
                             AND retired_through>0
                           ORDER BY producer_id""",
                        *location,
                    )
                ]
                consumer_anchors = [
                    dict(row)
                    for row in await conn.fetch(
                        """SELECT consumer_id,completion_retired_through
                           FROM consumers
                           WHERE channel=$1 AND partition_id=$2
                             AND completion_retired_through>0
                           ORDER BY consumer_id""",
                        *location,
                    )
                ]
                anchors = {
                    "producers": producer_anchors,
                    "consumers": consumer_anchors,
                }
                anchor_digest = hashlib.sha256(
                    json.dumps(anchors, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                result_locations[f"{location[0]}/{location[1]}"] = {
                    "retired_through": rows["through"],
                    "retired_attempts": deleted_attempts,
                    "retired_completions": completion_delete_count,
                    "rolling_digest": rolling,
                    "anchors": anchors,
                    "anchor_digest": anchor_digest,
                }

            await conn.execute(
                "DELETE FROM compaction_manifests m WHERE NOT EXISTS(SELECT 1 FROM "
                "compaction_tombstones t WHERE t.manifest_id=m.manifest_id)"
            )
            if all_object_keys:
                await conn.execute(
                    """DELETE FROM object_identities i WHERE i.object_key=ANY($1::text[])
                       AND i.object_key LIKE 'sha256:%'
                       AND NOT EXISTS(SELECT 1 FROM objects o WHERE o.object_key=i.object_key)
                       AND NOT EXISTS(SELECT 1 FROM records r WHERE r.object_key=i.object_key)
                       AND NOT EXISTS(SELECT 1 FROM compaction_tombstones t
                                      WHERE t.object_key=i.object_key)
                       AND NOT EXISTS(SELECT 1 FROM append_attempts a
                                      WHERE a.object_key=i.object_key)""",
                    sorted(all_object_keys),
                )
            outcome = {
                "operation_id": operation_id,
                "idempotent_retry": False,
                "locations": result_locations,
            }
            await conn.execute(
                """INSERT INTO metadata_operations(
                   operation_id,cuts_digest,cuts,outcome)
                   VALUES($1,$2,$3::jsonb,$4::jsonb)""",
                operation_id,
                cuts_digest,
                cuts_json,
                json.dumps(outcome, sort_keys=True),
            )
            await conn.execute(
                """DELETE FROM metadata_operations o
                   WHERE NOT EXISTS(
                     SELECT 1 FROM metadata_summaries s
                     WHERE s.operation_id=o.operation_id)"""
            )
            return outcome

    async def claim_consumer(
        self, consumer_id: str, owner_id: str, *, lease_seconds: float
    ) -> int:
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT * FROM consumers WHERE consumer_id=$1 FOR UPDATE", consumer_id
            )
            if row is None:
                raise PostgresProtocolError("unknown consumer")
            available = await conn.fetchval(
                "SELECT $1::timestamptz IS NULL OR $1 <= clock_timestamp() OR $2=$3",
                row["lease_expires_at"],
                row["owner_id"],
                owner_id,
            )
            if not available:
                raise PostgresProtocolError("consumer lease held")
            return await conn.fetchval(
                """UPDATE consumers SET owner_id=$2,owner_epoch=owner_epoch+1,
                   lease_expires_at=clock_timestamp()+make_interval(secs=>$3),
                   delivery_attempt=delivery_attempt+CASE
                     WHEN in_flight_sequence IS NOT NULL
                     THEN 1 ELSE 0 END
                   WHERE consumer_id=$1 RETURNING owner_epoch""",
                consumer_id,
                owner_id,
                lease_seconds,
            )

    async def renew(
        self, consumer_id: str, owner_id: str, owner_epoch: int, *, lease_seconds: float
    ) -> None:
        async with self.pool.acquire() as conn:
            value = await conn.fetchval(
                """UPDATE consumers SET
                   lease_expires_at=clock_timestamp()+make_interval(secs=>$4)
                   WHERE consumer_id=$1 AND owner_id=$2 AND owner_epoch=$3
                     AND lease_expires_at > clock_timestamp()
                   RETURNING owner_epoch""",
                consumer_id,
                owner_id,
                owner_epoch,
                lease_seconds,
            )
            if value is None:
                raise StaleOwner("lease renewal fenced")

    async def renew_runtime_leases(
        self,
        token: DeliveryToken,
        holder_id: str,
        *,
        lease_seconds: float,
    ) -> None:
        """Atomically renew the consumer and exact invocation holder leases."""
        async with self.pool.acquire() as conn, conn.transaction():
            consumer = await conn.fetchrow(
                "SELECT *,lease_expires_at>clock_timestamp() AS lease_valid "
                "FROM consumers WHERE consumer_id=$1 FOR UPDATE",
                token.consumer_id,
            )
            if consumer is None:
                raise PostgresProtocolError("unknown consumer")
            self._assert_owner(consumer, token.owner_id, token.owner_epoch)
            invocation = await conn.fetchrow(
                "SELECT *,expires_at>clock_timestamp() AS lease_valid "
                "FROM runtime_invocations WHERE consumer_id=$1 FOR UPDATE",
                token.consumer_id,
            )
            if invocation is None or (
                invocation["input_sequence"],
                invocation["attempt"],
                invocation["owner_id"],
                invocation["owner_epoch"],
                invocation["holder_id"],
                invocation["lease_valid"],
            ) != (
                token.sequence,
                token.attempt,
                token.owner_id,
                token.owner_epoch,
                holder_id,
                True,
            ):
                raise StaleOwner("runtime invocation renewal fenced")
            await conn.execute(
                """UPDATE consumers SET
                   lease_expires_at=clock_timestamp()+make_interval(secs=>$4)
                   WHERE consumer_id=$1 AND owner_id=$2 AND owner_epoch=$3""",
                token.consumer_id,
                token.owner_id,
                token.owner_epoch,
                lease_seconds,
            )
            await conn.execute(
                """UPDATE runtime_invocations SET
                   expires_at=clock_timestamp()+make_interval(secs=>$3)
                   WHERE consumer_id=$1 AND holder_id=$2""",
                token.consumer_id,
                holder_id,
                lease_seconds,
            )

    @staticmethod
    def _assert_owner(row: asyncpg.Record, owner_id: str, owner_epoch: int) -> None:
        if row["owner_id"] != owner_id or row["owner_epoch"] != owner_epoch:
            raise StaleOwner("owner epoch fenced")
        if row["lease_valid"] is not True:
            raise StaleOwner("owner lease expired")

    async def deliver_next(
        self, consumer_id: str, owner_id: str, owner_epoch: int
    ) -> tuple[DeliveryToken, Envelope] | None:
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT *, lease_expires_at > clock_timestamp() AS lease_valid "
                "FROM consumers WHERE consumer_id=$1 FOR UPDATE",
                consumer_id,
            )
            if row is None:
                raise PostgresProtocolError("unknown consumer")
            self._assert_owner(row, owner_id, owner_epoch)
            sequence = row["in_flight_sequence"] or row["completed_through"] + 1
            if row["in_flight_sequence"] is not None and row["in_flight_end"] not in (
                None,
                row["in_flight_sequence"],
            ):
                raise PostgresProtocolError("bounded batch is already in flight")
            record = await conn.fetchrow(
                """SELECT r.*,o.payload FROM records r JOIN objects o USING(object_key)
                   WHERE r.channel=$1 AND r.partition_id=$2 AND r.sequence=$3
                     AND o.published_at IS NOT NULL""",
                row["channel"],
                row["partition_id"],
                sequence,
            )
            if record is None:
                return None
            attempt = row["delivery_attempt"]
            if row["in_flight_sequence"] is None:
                attempt += 1
                await conn.execute(
                    "UPDATE consumers SET in_flight_sequence=$2,in_flight_end=$2,"
                    "delivery_attempt=$3 "
                    "WHERE consumer_id=$1",
                    consumer_id,
                    sequence,
                    attempt,
                )
            token = DeliveryToken(consumer_id, sequence, owner_id, owner_epoch, attempt)
            envelope = Envelope(
                record["channel"],
                record["partition_id"],
                record["sequence"],
                record["record_id"],
                record["object_key"],
                record["payload_digest"],
                record["payload_size"],
                record["schema_id"],
                record["lineage_digest"],
                bytes(record["payload"]),
            )
            return token, envelope

    async def deliver_batch(
        self,
        consumer_id: str,
        owner_id: str,
        owner_epoch: int,
        *,
        max_records: int = 8,
    ) -> list[tuple[DeliveryToken, Envelope]]:
        """Durably deliver one bounded contiguous range.

        The consumer may process records individually, but progress for the
        range is committed atomically by `complete_return_batch`.  A crash
        before that commit redelivers the whole range from its first record.
        """
        if not 1 <= max_records <= 32:
            raise ValueError("max_records must be between 1 and 32")
        async with self.pool.acquire() as conn, conn.transaction():
            consumer = await conn.fetchrow(
                "SELECT *,lease_expires_at>clock_timestamp() AS lease_valid "
                "FROM consumers WHERE consumer_id=$1 FOR UPDATE",
                consumer_id,
            )
            if consumer is None:
                raise PostgresProtocolError("unknown consumer")
            self._assert_owner(consumer, owner_id, owner_epoch)
            if consumer["in_flight_sequence"] is None:
                first = consumer["completed_through"] + 1
                last = None
                attempt = consumer["delivery_attempt"] + 1
            else:
                first = consumer["in_flight_sequence"]
                last = consumer["in_flight_end"] or first
                attempt = consumer["delivery_attempt"]
            limit = max_records if last is None else last - first + 1
            records = await conn.fetch(
                """SELECT r.*,o.payload FROM records r JOIN objects o USING(object_key)
                   WHERE r.channel=$1 AND r.partition_id=$2 AND r.sequence>=$3
                     AND ($4::bigint IS NULL OR r.sequence<=$4)
                     AND o.published_at IS NOT NULL
                   ORDER BY r.sequence LIMIT $5""",
                consumer["channel"],
                consumer["partition_id"],
                first,
                last,
                limit,
            )
            if not records:
                return []
            expected = list(range(first, first + len(records)))
            if [record["sequence"] for record in records] != expected:
                raise PostgresProtocolError("delivery batch is not contiguous")
            if last is not None and records[-1]["sequence"] != last:
                raise PostgresProtocolError("in-flight delivery batch is incomplete")
            if consumer["in_flight_sequence"] is None:
                last = records[-1]["sequence"]
                await conn.execute(
                    """UPDATE consumers SET in_flight_sequence=$2,in_flight_end=$3,
                       delivery_attempt=$4 WHERE consumer_id=$1""",
                    consumer_id,
                    first,
                    last,
                    attempt,
                )
            return [
                (
                    DeliveryToken(
                        consumer_id, record["sequence"], owner_id, owner_epoch, attempt
                    ),
                    Envelope(
                        record["channel"],
                        record["partition_id"],
                        record["sequence"],
                        record["record_id"],
                        record["object_key"],
                        record["payload_digest"],
                        record["payload_size"],
                        record["schema_id"],
                        record["lineage_digest"],
                        bytes(record["payload"]),
                    ),
                )
                for record in records
            ]

    async def acquire_invocation(
        self, token: DeliveryToken, holder_id: str, *, lease_seconds: float = 30
    ) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            consumer = await conn.fetchrow(
                "SELECT *,lease_expires_at>clock_timestamp() AS lease_valid "
                "FROM consumers WHERE consumer_id=$1 FOR UPDATE",
                token.consumer_id,
            )
            if consumer is None:
                raise PostgresProtocolError("unknown consumer")
            self._assert_owner(consumer, token.owner_id, token.owner_epoch)
            if (consumer["in_flight_sequence"], consumer["delivery_attempt"]) != (
                token.sequence,
                token.attempt,
            ):
                raise PostgresProtocolError("invocation token is not current")
            current = await conn.fetchrow(
                "SELECT *,expires_at<=clock_timestamp() AS expired "
                "FROM runtime_invocations WHERE consumer_id=$1 FOR UPDATE",
                token.consumer_id,
            )
            if current is None:
                await conn.execute(
                    """INSERT INTO runtime_invocations VALUES(
                       $1,$2,$3,$4,$5,$6,clock_timestamp()+make_interval(secs=>$7))""",
                    token.consumer_id,
                    token.sequence,
                    token.attempt,
                    token.owner_id,
                    token.owner_epoch,
                    holder_id,
                    lease_seconds,
                )
                await conn.execute(
                    """UPDATE consumers SET
                       lease_expires_at=clock_timestamp()+make_interval(secs=>$4)
                       WHERE consumer_id=$1 AND owner_id=$2 AND owner_epoch=$3""",
                    token.consumer_id,
                    token.owner_id,
                    token.owner_epoch,
                    lease_seconds,
                )
                return
            if current["holder_id"] == holder_id:
                await conn.execute(
                    "UPDATE runtime_invocations SET "
                    "expires_at=clock_timestamp()+make_interval(secs=>$3) "
                    "WHERE consumer_id=$1 AND holder_id=$2",
                    token.consumer_id,
                    holder_id,
                    lease_seconds,
                )
                await conn.execute(
                    """UPDATE consumers SET
                       lease_expires_at=clock_timestamp()+make_interval(secs=>$4)
                       WHERE consumer_id=$1 AND owner_id=$2 AND owner_epoch=$3""",
                    token.consumer_id,
                    token.owner_id,
                    token.owner_epoch,
                    lease_seconds,
                )
                return
            if (
                not current["expired"]
                or token.owner_epoch <= current["owner_epoch"]
                or token.attempt <= current["attempt"]
            ):
                raise PostgresProtocolError("consumer invocation already active")
            await conn.execute(
                """UPDATE runtime_invocations SET input_sequence=$2,attempt=$3,
                   owner_id=$4,owner_epoch=$5,holder_id=$6,
                   expires_at=clock_timestamp()+make_interval(secs=>$7)
                   WHERE consumer_id=$1""",
                token.consumer_id,
                token.sequence,
                token.attempt,
                token.owner_id,
                token.owner_epoch,
                holder_id,
                lease_seconds,
            )
            await conn.execute(
                """UPDATE consumers SET
                   lease_expires_at=clock_timestamp()+make_interval(secs=>$4)
                   WHERE consumer_id=$1 AND owner_id=$2 AND owner_epoch=$3""",
                token.consumer_id,
                token.owner_id,
                token.owner_epoch,
                lease_seconds,
            )

    async def release_invocation(self, consumer_id: str, holder_id: str) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM runtime_invocations WHERE consumer_id=$1 AND holder_id=$2",
                consumer_id,
                holder_id,
            )

    async def complete(
        self,
        token: DeliveryToken,
        kind: str,
        *,
        output: Output | None = None,
        managed_state: dict[str, Any] | None = None,
        state_schema: str = "dict/v1",
        state_version: int = 1,
        invocation_holder_id: str | None = None,
        fault: Callable[[str], None] | None = None,
    ) -> int | None:
        if kind not in {"YIELD", "RETURN"}:
            raise PostgresProtocolError("unsupported completion kind")
        if (kind == "YIELD") != (output is not None):
            raise PostgresProtocolError("YIELD requires output and RETURN forbids it")
        state = managed_state or {}
        result: int | None = None
        async with self.pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                "SELECT *, lease_expires_at > clock_timestamp() AS lease_valid "
                "FROM consumers WHERE consumer_id=$1 FOR UPDATE",
                token.consumer_id,
            )
            if row is None:
                raise PostgresProtocolError("unknown consumer")
            self._assert_owner(row, token.owner_id, token.owner_epoch)
            if token.sequence <= row["completion_retired_through"]:
                raise IdempotencyHorizonExpired(
                    "completion retry is outside hot horizon"
                )
            prior = await conn.fetchrow(
                "SELECT * FROM turn_completions WHERE consumer_id=$1 AND input_sequence=$2",
                token.consumer_id,
                token.sequence,
            )
            if prior is not None:
                stored_state = prior["managed_state"]
                if isinstance(stored_state, str):
                    stored_state = json.loads(stored_state)
                exact = (
                    prior["owner_id"] == token.owner_id
                    and prior["owner_epoch"] == token.owner_epoch
                    and prior["completion_kind"] == kind
                    and prior["state_schema"] == state_schema
                    and prior["state_version"] == state_version
                    and stored_state == state
                )
                if output is None:
                    exact = exact and prior["output_sequence"] is None
                else:
                    witness = await conn.fetchrow(
                        """SELECT record_id,object_key,payload_digest,payload_size,
                           schema_id,lineage_digest FROM records
                           WHERE channel=$1 AND partition_id=$2 AND sequence=$3
                           UNION ALL
                           SELECT record_id,object_key,payload_digest,payload_size,
                           schema_id,lineage_digest FROM compaction_tombstones
                           WHERE channel=$1 AND partition_id=$2 AND sequence=$3
                           LIMIT 1""",
                        prior["output_channel"],
                        prior["output_partition"],
                        prior["output_sequence"],
                    )
                    exact = (
                        exact
                        and witness is not None
                        and (
                            prior["output_channel"],
                            prior["output_partition"],
                            witness["record_id"],
                            witness["object_key"],
                            witness["payload_digest"],
                            witness["payload_size"],
                            witness["schema_id"],
                            witness["lineage_digest"],
                        )
                        == (
                            output.channel,
                            output.partition,
                            output.record_id,
                            output.object_key,
                            _sha256(output.payload),
                            len(output.payload),
                            output.schema_id,
                            output.lineage_digest,
                        )
                    )
                if not exact:
                    raise PostgresProtocolError("completion identity drift")
                return prior["output_sequence"]
            invocation = await conn.fetchrow(
                "SELECT * FROM runtime_invocations WHERE consumer_id=$1 FOR UPDATE",
                token.consumer_id,
            )
            if invocation is not None and (
                invocation_holder_id is None
                or (
                    invocation["input_sequence"],
                    invocation["attempt"],
                    invocation["owner_id"],
                    invocation["owner_epoch"],
                    invocation["holder_id"],
                )
                != (
                    token.sequence,
                    token.attempt,
                    token.owner_id,
                    token.owner_epoch,
                    invocation_holder_id,
                )
            ):
                raise PostgresProtocolError("completion lacks exact invocation holder")
            if invocation_holder_id is not None and invocation is None:
                raise PostgresProtocolError("completion invocation holder missing")
            if (
                row["in_flight_sequence"] != token.sequence
                or row["in_flight_end"] not in (None, token.sequence)
                or row["delivery_attempt"] != token.attempt
            ):
                raise PostgresProtocolError("completion lacks exact in-flight delivery")
            output_sequence: int | None = None
            if output is not None:
                output_producer = (
                    f"turn:{token.consumer_id}"
                    if invocation_holder_id is not None
                    else None
                )
                if output_producer is not None:
                    registered = await conn.fetchval(
                        "SELECT EXISTS(SELECT 1 FROM producer_progress WHERE channel=$1 "
                        "AND partition_id=$2 AND producer_id=$3)",
                        output.channel,
                        output.partition,
                        output_producer,
                    )
                    if not registered:
                        output_part = await conn.fetchrow(
                            "SELECT * FROM partitions WHERE channel=$1 AND partition_id=$2 "
                            "FOR UPDATE",
                            output.channel,
                            output.partition,
                        )
                        count = await conn.fetchval(
                            "SELECT count(*) FROM producer_progress WHERE channel=$1 "
                            "AND partition_id=$2",
                            output.channel,
                            output.partition,
                        )
                        if output_part is None or count >= output_part["max_producers"]:
                            raise PostgresProtocolError("output producer registry full")
                        await conn.execute(
                            """INSERT INTO producer_progress(
                               channel,partition_id,producer_id) VALUES($1,$2,$3)""",
                            output.channel,
                            output.partition,
                            output_producer,
                        )
                appended = await self._append_with_connection(
                    conn,
                    f"turn:{token.consumer_id}:{token.sequence}",
                    output.channel,
                    output.partition,
                    output.record_id,
                    output.object_key,
                    output.payload,
                    output.schema_id,
                    output.lineage_digest,
                    output_producer,
                    (
                        token.sequence - row["start_sequence"] + 1
                        if output_producer is not None
                        else None
                    ),
                )
                output_sequence = appended.sequence
                if fault is not None:
                    fault("completion:after_output_before_marker")
            await conn.execute(
                """INSERT INTO turn_completions(
                    consumer_id,input_sequence,completion_kind,owner_id,owner_epoch,
                    output_channel,output_partition,output_sequence,state_schema,
                    state_version,managed_state)
                   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)""",
                token.consumer_id,
                token.sequence,
                kind,
                token.owner_id,
                token.owner_epoch,
                None if output is None else output.channel,
                None if output is None else output.partition,
                output_sequence,
                state_schema,
                state_version,
                json.dumps(state),
            )
            if fault is not None:
                fault("completion:after_marker_before_progress")
            advanced = await conn.fetchval(
                """UPDATE consumers SET completed_through=$4,in_flight_sequence=NULL,
                   in_flight_end=NULL,
                   managed_state=$5::jsonb,state_schema=$6,state_version=$7
                   WHERE consumer_id=$1 AND owner_id=$2 AND owner_epoch=$3
                     AND lease_expires_at > clock_timestamp()
                     AND in_flight_sequence=$4 RETURNING completed_through""",
                token.consumer_id,
                token.owner_id,
                token.owner_epoch,
                token.sequence,
                json.dumps(state),
                state_schema,
                state_version,
            )
            if advanced is None:
                raise StaleOwner("completion fenced")
            if invocation_holder_id is not None:
                deleted = await conn.execute(
                    "DELETE FROM runtime_invocations WHERE consumer_id=$1 AND holder_id=$2",
                    token.consumer_id,
                    invocation_holder_id,
                )
                if deleted != "DELETE 1":
                    raise PostgresProtocolError("completion invocation retire failed")
            if fault is not None:
                fault("completion:after_progress_before_commit")
            result = output_sequence
        if fault is not None:
            fault("completion:after_commit_before_ack")
        return result

    async def complete_return_batch(
        self,
        deliveries: list[tuple[DeliveryToken, Envelope]],
        *,
        managed_states: list[dict[str, Any]] | None = None,
        state_schema: str = "batch/v1",
        state_version: int = 1,
    ) -> int:
        """Commit a previously delivered RETURN-only range in one transaction."""
        if not deliveries:
            return 0
        tokens = [token for token, _ in deliveries]
        states = managed_states or [{"last": token.sequence} for token in tokens]
        if len(states) != len(tokens):
            raise ValueError("one managed state is required per delivered record")
        head = tokens[0]
        expected_sequences = list(range(head.sequence, head.sequence + len(tokens)))
        if [token.sequence for token in tokens] != expected_sequences:
            raise PostgresProtocolError("completion batch is not contiguous")
        if any(
            (token.consumer_id, token.owner_id, token.owner_epoch, token.attempt)
            != (head.consumer_id, head.owner_id, head.owner_epoch, head.attempt)
            for token in tokens
        ):
            raise PostgresProtocolError("completion batch token identity drift")
        async with self.pool.acquire() as conn, conn.transaction():
            consumer = await conn.fetchrow(
                "SELECT *,lease_expires_at>clock_timestamp() AS lease_valid "
                "FROM consumers WHERE consumer_id=$1 FOR UPDATE",
                head.consumer_id,
            )
            if consumer is None:
                raise PostgresProtocolError("unknown consumer")
            self._assert_owner(consumer, head.owner_id, head.owner_epoch)
            final = tokens[-1].sequence
            if head.sequence <= consumer["completion_retired_through"]:
                raise IdempotencyHorizonExpired(
                    "completion batch intersects retired horizon"
                )
            witnesses = await conn.fetch(
                """SELECT r.channel,r.partition_id,r.sequence,r.record_id,r.object_key,
                          r.payload_digest,r.payload_size,r.schema_id,r.lineage_digest,
                          o.payload,false AS compacted
                   FROM records r JOIN objects o USING(object_key)
                   WHERE r.channel=$1 AND r.partition_id=$2
                     AND r.sequence BETWEEN $3 AND $4
                   UNION ALL
                   SELECT t.channel,t.partition_id,t.sequence,t.record_id,t.object_key,
                          t.payload_digest,t.payload_size,t.schema_id,t.lineage_digest,
                          NULL::bytea AS payload,true AS compacted
                   FROM compaction_tombstones t
                   WHERE t.channel=$1 AND t.partition_id=$2
                     AND t.sequence BETWEEN $3 AND $4
                   ORDER BY sequence""",
                consumer["channel"],
                consumer["partition_id"],
                head.sequence,
                final,
            )
            if [row["sequence"] for row in witnesses] != expected_sequences:
                raise PostgresProtocolError("completion input witness missing")
            for (_, envelope), witness in zip(deliveries, witnesses):
                identity_matches = (
                    envelope.channel,
                    envelope.partition,
                    envelope.sequence,
                    envelope.record_id,
                    envelope.object_key,
                    envelope.payload_digest,
                    envelope.payload_size,
                    envelope.schema_id,
                    envelope.lineage_digest,
                ) == (
                    witness["channel"],
                    witness["partition_id"],
                    witness["sequence"],
                    witness["record_id"],
                    witness["object_key"],
                    witness["payload_digest"],
                    witness["payload_size"],
                    witness["schema_id"],
                    witness["lineage_digest"],
                )
                payload_matches = (
                    len(envelope.payload) == witness["payload_size"]
                    and _sha256(envelope.payload) == witness["payload_digest"]
                    and (
                        witness["payload"] is None
                        or envelope.payload == bytes(witness["payload"])
                    )
                )
                if not identity_matches or not payload_matches:
                    raise PostgresProtocolError("completion envelope identity drift")
            prior_rows = await conn.fetch(
                "SELECT * FROM turn_completions WHERE consumer_id=$1 "
                "AND input_sequence BETWEEN $2 AND $3 ORDER BY input_sequence",
                head.consumer_id,
                head.sequence,
                final,
            )
            if prior_rows:
                exact = len(prior_rows) == len(tokens)
                for prior, token, state in zip(prior_rows, tokens, states):
                    stored_state = prior["managed_state"]
                    if isinstance(stored_state, str):
                        stored_state = json.loads(stored_state)
                    exact = exact and (
                        prior["input_sequence"] == token.sequence
                        and prior["completion_kind"] == "RETURN"
                        and prior["owner_id"] == token.owner_id
                        and prior["owner_epoch"] == token.owner_epoch
                        and prior["state_schema"] == state_schema
                        and prior["state_version"] == state_version
                        and stored_state == state
                    )
                if not exact or consumer["completed_through"] < final:
                    raise PostgresProtocolError("completion batch identity drift")
                return len(tokens)
            if (
                consumer["in_flight_sequence"],
                consumer["in_flight_end"],
                consumer["delivery_attempt"],
                consumer["completed_through"] + 1,
            ) != (head.sequence, final, head.attempt, head.sequence):
                raise PostgresProtocolError("completion lacks exact in-flight batch")
            if any(row["compacted"] for row in witnesses):
                raise PostgresProtocolError("completion input witness missing")
            completion_rows = []
            for (token, _), state in zip(deliveries, states):
                completion_rows.append(
                    (
                        head.consumer_id,
                        token.sequence,
                        head.owner_id,
                        head.owner_epoch,
                        state_schema,
                        state_version,
                        json.dumps(state),
                    )
                )
            await conn.executemany(
                """INSERT INTO turn_completions(
                   consumer_id,input_sequence,completion_kind,owner_id,owner_epoch,
                   state_schema,state_version,managed_state)
                   VALUES($1,$2,'RETURN',$3,$4,$5,$6,$7::jsonb)""",
                completion_rows,
            )
            advanced = await conn.fetchval(
                """UPDATE consumers SET completed_through=$4,in_flight_sequence=NULL,
                   in_flight_end=NULL,managed_state=$5::jsonb,state_schema=$6,
                   state_version=$7 WHERE consumer_id=$1 AND owner_id=$2
                   AND owner_epoch=$3 AND lease_expires_at>clock_timestamp()
                   AND in_flight_sequence=$8 AND in_flight_end=$4
                   RETURNING completed_through""",
                head.consumer_id,
                head.owner_id,
                head.owner_epoch,
                final,
                json.dumps(states[-1]),
                state_schema,
                state_version,
                head.sequence,
            )
            if advanced is None:
                raise StaleOwner("batch completion fenced")
            return len(tokens)

    async def position(self, consumer_id: str) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM consumers WHERE consumer_id=$1", consumer_id
            )
            return dict(row)

    async def verified_position(self, consumer_id: str) -> dict[str, Any]:
        """Read managed state only when its terminal completion agrees exactly."""
        async with (
            self.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            row = await conn.fetchrow(
                "SELECT * FROM consumers WHERE consumer_id=$1", consumer_id
            )
            if row is None:
                raise PostgresProtocolError("unknown consumer")
            if row["completed_through"] >= row["start_sequence"]:
                completion = await conn.fetchrow(
                    "SELECT managed_state,state_schema,state_version "
                    "FROM turn_completions WHERE consumer_id=$1 AND input_sequence=$2",
                    consumer_id,
                    row["completed_through"],
                )
                if completion is None or (
                    completion["managed_state"],
                    completion["state_schema"],
                    completion["state_version"],
                ) != (
                    row["managed_state"],
                    row["state_schema"],
                    row["state_version"],
                ):
                    raise PostgresProtocolError("managed state projection corrupted")
            return dict(row)

    async def orphan_objects(self) -> list[str]:
        async with self.pool.acquire() as conn:
            return [
                row["object_key"]
                for row in await conn.fetch(
                    "SELECT object_key FROM objects WHERE published_at IS NULL ORDER BY object_key"
                )
            ]

    async def invariant_report(self) -> dict[str, Any]:
        async with self.pool.acquire() as conn:
            gaps = await conn.fetchval(
                """SELECT count(*) FROM partitions p, LATERAL generate_series(
                    1, p.next_sequence-1) s(sequence)
                   WHERE NOT EXISTS (SELECT 1 FROM records r WHERE r.channel=p.channel
                     AND r.partition_id=p.partition_id AND r.sequence=s.sequence)
                     AND NOT EXISTS (SELECT 1 FROM compaction_tombstones t
                     WHERE t.channel=p.channel AND t.partition_id=p.partition_id
                       AND t.sequence=s.sequence)
                     AND NOT EXISTS (SELECT 1 FROM metadata_summaries m
                     WHERE m.channel=p.channel AND m.partition_id=p.partition_id
                       AND s.sequence<=m.retired_through)"""
            )
            bad_objects = await conn.fetchval(
                """SELECT count(*) FROM records r LEFT JOIN objects o USING(object_key)
                   WHERE o.object_key IS NULL OR o.published_at IS NULL
                     OR r.payload_digest<>o.payload_digest OR r.payload_size<>o.payload_size
                     OR r.schema_id<>o.schema_id"""
            )
            bad_capacity = await conn.fetchval(
                """SELECT count(*) FROM partitions p WHERE
                   used_records<>(SELECT count(*) FROM records r WHERE r.channel=p.channel
                     AND r.partition_id=p.partition_id)
                   OR used_bytes<>(SELECT coalesce(sum(payload_size),0) FROM objects o
                     WHERE o.published_at IS NOT NULL AND EXISTS(
                       SELECT 1 FROM records r WHERE r.channel=p.channel
                         AND r.partition_id=p.partition_id AND r.object_key=o.object_key))
                   OR used_records>max_records OR used_bytes>max_bytes"""
            )
            bad_progress = await conn.fetchval(
                """SELECT count(*) FROM consumers c WHERE completed_through>=start_sequence
                   AND (SELECT count(*) FROM turn_completions t WHERE t.consumer_id=c.consumer_id
                     AND t.input_sequence BETWEEN
                       GREATEST(c.start_sequence,c.completion_retired_through+1)
                       AND c.completed_through)
                     <> GREATEST(0,completed_through-
                       GREATEST(start_sequence,completion_retired_through+1)+1)"""
            )
            bad_invocations = await conn.fetchval(
                """SELECT count(*) FROM runtime_invocations i JOIN consumers c USING(consumer_id)
                   WHERE i.input_sequence<=c.completed_through
                     OR i.input_sequence IS DISTINCT FROM c.in_flight_sequence
                     OR i.attempt<>c.delivery_attempt OR i.owner_id<>c.owner_id
                     OR i.owner_epoch<>c.owner_epoch"""
            )
            return {
                "passed": gaps
                == bad_objects
                == bad_capacity
                == bad_progress
                == bad_invocations
                == 0,
                "sequence_gaps": gaps,
                "bad_objects": bad_objects,
                "bad_capacity": bad_capacity,
                "bad_progress": bad_progress,
                "bad_invocations": bad_invocations,
            }
