"""Persistent PostgreSQL streaming-DAG workload for the separate long soak.

This process is intentionally independent from CI job lifetime.  The controller
starts it as a detached container and sequential monitor jobs only observe it.
The measured profile is a normal Python yield DAG::

    source -> A -> (B, C) -> J -> D

Every completed batch emits a global barrier, retains the latest snapshots, and
returns data/log/idempotency credits through the production-shaped retirement
prototypes.  The service owns no business credentials; its PostgreSQL account
and release session exist only inside the isolated soak deployment.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import signal
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, ClassVar

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from postgres_barrier_coordinator import PostgresBarrierCoordinator, decoded
from postgres_checkpoint_retention import PostgresCheckpointRetention
from postgres_generator_runtime import OutputSpec, stream_task
from postgres_join_retention import PostgresJoinRetention
from postgres_join_runtime import PostgresJoinRuntime
from postgres_release_authority import PostgresReleaseAuthority, RuntimeContract
from postgres_restore_contracts import public_key_b64
from postgres_stream_v2 import PostgresStream, content_object_key
from postgres_streaming_retention import PostgresStreamingRetention

HERE = Path(__file__).resolve().parent


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def json_default(value: Any):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    raise TypeError(f"unsupported JSON evidence type {type(value).__name__}")


def rolling(prior: str, payload: bytes) -> str:
    return hashlib.sha256(bytes.fromhex(prior) + payload).hexdigest()


@stream_task
def aggregate_document(ctx, payload):
    state = {
        "count": ctx.managed_state.get("count", 0) + 1,
        "rolling_digest": rolling(
            ctx.managed_state.get("rolling_digest", "0" * 64), payload
        ),
    }
    yield OutputSpec(payload, "bytes/v1", state)


@dataclass(frozen=True)
class ServiceConfig:
    run_id: str
    dsn: str
    schema: str
    state_dir: str
    target_seconds: int
    smoke: bool
    records_per_second: float
    checkpoint_every: int
    owner_fault_every: int
    max_partition_records: int
    max_partition_bytes: int

    @classmethod
    def from_env(cls) -> ServiceConfig:
        value = cls(
            run_id=os.environ["SOAK_RUN_ID"],
            dsn=os.environ["SOAK_DSN"],
            schema=os.environ["SOAK_SCHEMA"],
            state_dir=os.environ.get("SOAK_STATE_DIR", "/state"),
            target_seconds=int(os.environ.get("SOAK_TARGET_SECONDS", "259200")),
            smoke=os.environ.get("SOAK_SMOKE", "false").lower() == "true",
            records_per_second=float(os.environ.get("SOAK_RECORDS_PER_SECOND", "1")),
            checkpoint_every=int(os.environ.get("SOAK_CHECKPOINT_EVERY", "20")),
            owner_fault_every=int(os.environ.get("SOAK_OWNER_FAULT_EVERY", "101")),
            max_partition_records=int(
                os.environ.get("SOAK_MAX_PARTITION_RECORDS", "1000")
            ),
            max_partition_bytes=int(
                os.environ.get("SOAK_MAX_PARTITION_BYTES", "16777216")
            ),
        )
        if (
            not value.run_id
            or not value.schema
            or value.target_seconds <= 0
            or value.records_per_second <= 0
            or min(
                value.checkpoint_every,
                value.owner_fault_every,
                value.max_partition_records,
                value.max_partition_bytes,
            )
            <= 0
            or value.checkpoint_every >= value.max_partition_records // 2
        ):
            raise ValueError("invalid bounded long-soak profile")
        return value


class LongSoakService:
    topology: ClassVar = {
        "A": (("source",), "a"),
        "B": (("a",), "b"),
        "C": (("a",), "c"),
        "J": (("b", "c"), "j"),
        "D": (("j",), "d"),
    }

    def __init__(self, config: ServiceConfig):
        self.config = config
        self.state_dir = Path(config.state_dir)
        self.stop = asyncio.Event()
        self.boot_id = uuid.uuid4().hex
        self.owner_ids = {
            participant: f"soak:{config.run_id}:{participant}:{self.boot_id}"
            for participant in self.topology
        }
        self.epochs: dict[str, int] = {}
        self.phase = "BOOTING"
        self.stream = None
        self.coordinator = None
        self.revision = None
        self.runtimes = {}
        self.release = None
        self.release_session_id = None

    def _write_status(self, value: dict[str, Any]) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        path = self.state_dir / "service-status.json"
        temporary = path.with_name(path.name + ".tmp")
        envelope = {
            "run_id": self.config.run_id,
            "boot_id": self.boot_id,
            "observed_at": datetime.now(UTC).isoformat(),
            "phase": self.phase,
            **value,
        }
        temporary.write_text(
            json.dumps(envelope, sort_keys=True, indent=2, default=json_default) + "\n"
        )
        temporary.replace(path)

    async def _bootstrap(self) -> None:
        probe = await PostgresStream.connect_existing(
            self.config.dsn, self.config.schema
        )
        async with probe.pool.acquire() as conn:
            release_preexisting = await conn.fetchval(
                "SELECT to_regclass('release_heads_v1') IS NOT NULL"
            )
        if release_preexisting:
            release = PostgresReleaseAuthority(probe.pool, self.config.schema)
            session = await release.open_session(
                runtime_version="soak-service-v1",
                actor_id="soak-service:" + self.config.run_id,
                role="EXECUTION",
                lease_seconds=300,
            )
            bound_pool = await release.open_bound_pool(
                self.config.dsn, session.session_id, min_size=1, max_size=12
            )
            await probe.close()
            bootstrap = PostgresStream(bound_pool, self.config.schema)
            release = PostgresReleaseAuthority.connect_existing(
                bound_pool, self.config.schema
            )
        else:
            await probe.close()
            bootstrap = await PostgresStream.open(self.config.dsn, self.config.schema)
        for partition in ("source", "a", "b", "c", "j", "d"):
            await bootstrap.create_partition(
                "soak",
                partition,
                max_records=self.config.max_partition_records,
                max_bytes=self.config.max_partition_bytes,
            )
        await bootstrap.register_producer("soak", "source", "soak-source")
        coordinator = (
            PostgresBarrierCoordinator(bootstrap)
            if release_preexisting
            else await PostgresBarrierCoordinator.install(bootstrap)
        )
        for partition in ("source", "a", "b", "c", "j", "d"):
            async with bootstrap.pool.acquire() as conn:
                capacity = await conn.fetchrow(
                    "SELECT max_log_entries,used_log_entries FROM partition_log_heads "
                    "WHERE channel='soak' AND partition_id=$1",
                    partition,
                )
            expected_log_capacity = self.config.max_partition_records * 2
            if capacity is None or capacity["used_log_entries"] == 0:
                await coordinator.configure_ordered_log_capacity(
                    "soak", partition, expected_log_capacity
                )
            elif capacity["max_log_entries"] != expected_log_capacity:
                raise RuntimeError("ordered-log capacity identity drift")
        participants = [
            {
                "participant_id": participant,
                "inputs": [{"channel": "soak", "partition": item} for item in inputs],
                "outputs": [{"channel": "soak", "partition": output}],
                "code_digest": "soak-aggregate-v1",
                "state_schema": "soak-aggregate/v1",
                "state_version": 1,
            }
            for participant, (inputs, output) in self.topology.items()
        ]
        revision = await coordinator.register_revision(
            revision_id="soak-r1",
            deployment_id="soak-d1",
            graph_version="v1",
            sources=[("soak", "source")],
            participants=participants,
            code_manifest_digest="soak-code-v1",
            state_contract_digest="soak-state-v1",
            max_retained_checkpoints=4,
            max_checkpoint_metadata_bytes=16 * 1024 * 1024,
        )
        runtimes = {
            participant: PostgresJoinRuntime(coordinator, revision, participant)
            for participant in self.topology
        }
        initial = {"count": 0, "rolling_digest": "0" * 64}
        if not release_preexisting:
            for runtime in runtimes.values():
                await runtime.install(
                    initial_state=initial,
                    max_buffer_records=self.config.checkpoint_every * 4,
                    max_buffer_bytes=self.config.max_partition_bytes,
                    max_mailbox_records=self.config.max_partition_records * 4,
                    max_metadata_bytes=16 * 1024 * 1024,
                )
            await PostgresJoinRetention.install(coordinator)
            await PostgresCheckpointRetention.install(coordinator)
            await PostgresStreamingRetention.install(bootstrap, coordinator)
        async with bootstrap.pool.acquire() as conn:
            await conn.execute(
                """CREATE TABLE IF NOT EXISTS soak_run_v1(
                run_id text PRIMARY KEY,started_at timestamptz NOT NULL,
                target_seconds bigint NOT NULL,smoke boolean NOT NULL,
                source_completed bigint NOT NULL DEFAULT 0,
                last_checkpoint_source bigint NOT NULL DEFAULT 0,
                expected_d_digest text NOT NULL DEFAULT repeat('0',64),
                owner_faults bigint NOT NULL DEFAULT 0,
                process_boots bigint NOT NULL DEFAULT 0,
                last_progress_at timestamptz NOT NULL DEFAULT clock_timestamp(),
                completed_at timestamptz)"""
            )
            row = await conn.fetchrow("SELECT * FROM soak_run_v1")
            if row is None:
                await conn.execute(
                    "INSERT INTO soak_run_v1(run_id,started_at,target_seconds,smoke) "
                    "VALUES($1,clock_timestamp(),$2,$3)",
                    self.config.run_id,
                    self.config.target_seconds,
                    self.config.smoke,
                )
            elif (
                row["run_id"],
                row["target_seconds"],
                row["smoke"],
            ) != (
                self.config.run_id,
                self.config.target_seconds,
                self.config.smoke,
            ):
                raise RuntimeError("long-soak durable identity drift")
        key = Ed25519PrivateKey.from_private_bytes(
            hashlib.sha256(("soak-release:" + self.config.run_id).encode()).digest()
        )
        semantics = hashlib.sha256((HERE / "service.py").read_bytes()).hexdigest()
        if not release_preexisting:
            release = await PostgresReleaseAuthority.install(
                bootstrap.pool,
                self.config.schema,
                schema_version="soak-v1",
                runtime_contracts=(
                    RuntimeContract("soak-service-v1", semantics, "5" * 64),
                ),
                restore_public_key_b64=public_key_b64(key),
                max_hot_sessions=4,
            )
            session = await release.open_session(
                runtime_version="soak-service-v1",
                actor_id="soak-service:" + self.config.run_id,
                role="EXECUTION",
                lease_seconds=300,
            )
            bound_pool = await release.open_bound_pool(
                self.config.dsn, session.session_id, min_size=1, max_size=12
            )
            await bootstrap.close()
            bootstrap = PostgresStream(bound_pool, self.config.schema)
            release = PostgresReleaseAuthority.connect_existing(
                bound_pool, self.config.schema
            )
        self.stream = bootstrap
        self.release = release
        self.release_session_id = session.session_id
        self.coordinator = PostgresBarrierCoordinator(self.stream)
        self.revision = revision
        self.runtimes = {
            participant: PostgresJoinRuntime(
                self.coordinator, self.revision, participant
            )
            for participant in self.topology
        }
        self.join_retention = PostgresJoinRetention(self.coordinator)
        self.checkpoint_retention = PostgresCheckpointRetention(self.coordinator)
        self.streaming_retention = PostgresStreamingRetention(
            self.stream, self.coordinator
        )
        self.coordinator_epoch = await self.coordinator.claim_coordinator(
            "soak-r1",
            "v1",
            revision.graph_digest,
            "soak-coordinator",
            lease_seconds=300,
        )
        async with self.stream.pool.acquire() as conn:
            current_workers = {
                row["participant_id"]: dict(row)
                for row in await conn.fetch(
                    "SELECT participant_id,owner_id,lease_expires_at FROM join_workers "
                    "WHERE revision_id='soak-r1'"
                )
            }
        for participant, runtime in self.runtimes.items():
            current = current_workers[participant]
            # A process boot is a new execution owner.  If the former process
            # died while client code was outside a DB transaction, its worker
            # lease is the durable takeover boundary.  Wait for that short
            # lease rather than reusing the old owner identity (which would
            # preserve the abandoned invocation indefinitely).
            if current["owner_id"] not in {None, self.owner_ids[participant]}:
                remaining = max(
                    0.0,
                    (current["lease_expires_at"] - datetime.now(UTC)).total_seconds(),
                )
                if remaining:
                    await asyncio.sleep(remaining + 0.05)
            self.epochs[participant] = await runtime.claim(
                self.owner_ids[participant], lease_seconds=5
            )
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "UPDATE soak_run_v1 SET process_boots=process_boots+1,"
                "last_progress_at=clock_timestamp() WHERE run_id=$1",
                self.config.run_id,
            )

    def _coordinator_args(self):
        return (
            "soak-r1",
            "v1",
            self.revision.graph_digest,
            "soak-coordinator",
            self.coordinator_epoch,
        )

    async def _renew(self) -> None:
        await self.release.renew_session(self.release_session_id, lease_seconds=300)
        self.coordinator_epoch = await self.coordinator.claim_coordinator(
            *self._coordinator_args()[:3],
            "soak-coordinator",
            lease_seconds=300,
        )
        for participant, runtime in self.runtimes.items():
            self.epochs[participant] = await runtime.claim(
                self.owner_ids[participant], lease_seconds=5
            )

    async def _input_completed(self, participant: str, partition: str) -> int:
        async with self.stream.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT completed FROM join_inputs_v3 WHERE revision_id='soak-r1' "
                "AND participant_id=$1 AND channel='soak' AND partition_id=$2",
                participant,
                partition,
            )

    async def _step(self, participant: str, partition: str, *, fault=None) -> str:
        return await self.runtimes[participant].step(
            "soak/" + partition,
            aggregate_document,
            self.owner_ids[participant],
            self.epochs[participant],
            coordinator_owner="soak-coordinator",
            coordinator_epoch=self.coordinator_epoch,
            fault=fault,
        )

    async def _through(self, participant: str, partition: str, target: int) -> None:
        while await self._input_completed(participant, partition) < target:
            result = await self._step(participant, partition)
            if result not in {"APPLIED", "DRAINED"}:
                raise RuntimeError(
                    f"unexpected DATA result {participant}/{partition}: {result}"
                )

    async def _owner_loss(self, number: int) -> None:
        participant = "B" if (number // self.config.owner_fault_every) % 2 else "C"
        if await self._input_completed(participant, "a") >= number:
            return
        owner = self.owner_ids[participant]
        self.epochs[participant] = await self.runtimes[participant].claim(
            owner, lease_seconds=0.25
        )

        def fault(point: str) -> None:
            if point == "terminal_before_commit":
                raise RuntimeError("declared owner-loss boundary")

        try:
            await self._step(participant, "a", fault=fault)
        except RuntimeError as exc:
            if str(exc) != "declared owner-loss boundary":
                raise
        else:
            raise RuntimeError("owner-loss boundary did not fire")
        await asyncio.sleep(0.3)
        replacement = f"soak:{self.config.run_id}:{participant}:replacement:{number}"
        self.owner_ids[participant] = replacement
        self.epochs[participant] = await self.runtimes[participant].claim(
            replacement, lease_seconds=5
        )
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "UPDATE soak_run_v1 SET owner_faults=owner_faults+1 WHERE run_id=$1",
                self.config.run_id,
            )

    async def _process(self, number: int) -> None:
        payload = f"document-{number}".encode()
        await self.stream.append(
            f"soak-source-attempt-{number}",
            "soak",
            "source",
            f"soak-document-{number}",
            content_object_key(payload, "bytes/v1"),
            payload,
            "bytes/v1",
            f"soak-lineage-{number}",
            producer_id="soak-source",
            producer_sequence=number,
        )
        await self._through("A", "source", number)
        if number % self.config.owner_fault_every == 0:
            await self._owner_loss(number)
        await self._through("B", "a", number)
        await self._through("C", "a", number)
        await self._through("J", "b", number)
        await self._through("J", "c", number)
        await self._through("D", "j", number * 2)
        async with self.stream.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT source_completed,expected_d_digest FROM soak_run_v1 "
                "WHERE run_id=$1 FOR UPDATE",
                self.config.run_id,
            )
            if row["source_completed"] < number:
                expected = rolling(rolling(row["expected_d_digest"], payload), payload)
                await conn.execute(
                    "UPDATE soak_run_v1 SET source_completed=$2,expected_d_digest=$3,"
                    "last_progress_at=clock_timestamp() WHERE run_id=$1",
                    self.config.run_id,
                    number,
                    expected,
                )

    async def _accepted_source_through(self) -> int:
        async with self.stream.pool.acquire() as conn:
            return int(
                await conn.fetchval(
                    "SELECT accepted_through FROM producer_progress "
                    "WHERE channel='soak' AND partition_id='source' "
                    "AND producer_id='soak-source'"
                )
            )

    async def _barrier_input(
        self, checkpoint_id: str, participant: str, partition: str
    ) -> None:
        async with self.stream.pool.acquire() as conn:
            exists = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM join_barriers_v3 WHERE "
                "revision_id='soak-r1' AND participant_id=$1 AND checkpoint_id=$2 "
                "AND channel='soak' AND partition_id=$3)",
                participant,
                checkpoint_id,
                partition,
            )
        if not exists:
            result = await self._step(participant, partition)
            if result not in {"WAITING", "ALIGNED"}:
                raise RuntimeError(
                    f"unexpected BARRIER result {participant}/{partition}: {result}"
                )

    async def _checkpoint(self, frontier: int) -> None:
        request_id = f"soak-checkpoint-{frontier}"
        checkpoint = await self.coordinator.start_checkpoint(
            *self._coordinator_args(), request_id
        )
        async with self.stream.pool.acquire() as conn:
            observed = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM source_barrier_observations "
                "WHERE checkpoint_id=$1 AND channel='soak' AND partition_id='source')",
                checkpoint.checkpoint_id,
            )
        if not observed:
            await self.coordinator.inject_source_barrier(
                *self._coordinator_args(), checkpoint.checkpoint_id, "soak", "source"
            )
        for participant, partition in (
            ("A", "source"),
            ("B", "a"),
            ("C", "a"),
            ("J", "b"),
            ("J", "c"),
            ("D", "j"),
        ):
            await self._barrier_input(checkpoint.checkpoint_id, participant, partition)
        async with self.stream.pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM checkpoint_lifecycle_v2 WHERE checkpoint_id=$1",
                checkpoint.checkpoint_id,
            )
        if status != "COMPLETED":
            await self.coordinator.complete_checkpoint(
                *self._coordinator_args(), checkpoint.checkpoint_id
            )
        for participant in self.topology:
            await self.join_retention.retire_latest(
                "soak-r1",
                participant,
                f"soak-join-retire-{participant}-{frontier}",
            )
        if checkpoint.checkpoint_ordinal > 1:
            await self.checkpoint_retention.retire_prefix(
                *self._coordinator_args(), f"soak-checkpoint-retire-{frontier}"
            )
            await self.streaming_retention.compact_latest(
                *self._coordinator_args(), f"soak-stream-gc-{frontier}"
            )
        async with self.stream.pool.acquire() as conn:
            await conn.execute(
                "UPDATE soak_run_v1 SET last_checkpoint_source=$2,"
                "last_progress_at=clock_timestamp() WHERE run_id=$1",
                self.config.run_id,
                frontier,
            )

    async def _snapshot(self) -> dict[str, Any]:
        async with self.stream.pool.acquire() as conn:
            run = dict(await conn.fetchrow("SELECT * FROM soak_run_v1"))
            states = {
                row["participant_id"]: decoded(row["state"])
                for row in await conn.fetch(
                    "SELECT participant_id,state FROM join_workers "
                    "WHERE revision_id='soak-r1'"
                )
            }
            pressure = [
                dict(row)
                for row in await conn.fetch(
                    "SELECT channel,partition_id,used_records,max_records,used_bytes,"
                    "max_bytes,used_metadata_records FROM partitions "
                    "WHERE channel='soak' ORDER BY partition_id"
                )
            ]
            elapsed = await conn.fetchval(
                "SELECT extract(epoch FROM clock_timestamp()-started_at) "
                "FROM soak_run_v1"
            )
            database = {
                "database_bytes": await conn.fetchval(
                    "SELECT pg_database_size(current_database())"
                ),
                "schema_relation_bytes": await conn.fetchval(
                    "SELECT COALESCE(sum(pg_total_relation_size(c.oid)),0) "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=current_schema()"
                ),
                "synchronous_commit": await conn.fetchval("SHOW synchronous_commit"),
                "replicas": [
                    {
                        key: (
                            str(value)
                            if key.endswith("_lag") and value is not None
                            else value
                        )
                        for key, value in dict(row).items()
                    }
                    for row in await conn.fetch(
                        "SELECT application_name,state,sync_state,"
                        "write_lag,flush_lag,replay_lag FROM pg_stat_replication"
                    )
                ],
                "release_session_remaining_seconds": float(
                    await conn.fetchval(
                        "SELECT extract(epoch FROM lease_expires_at-clock_timestamp()) "
                        "FROM release_sessions_v1 WHERE session_id=$1 AND status='ACTIVE'",
                        self.release_session_id,
                    )
                ),
            }
        for key in ("started_at", "last_progress_at", "completed_at"):
            if run[key] is not None:
                run[key] = run[key].isoformat()
        return {
            "run": run,
            "elapsed_seconds": float(elapsed),
            "states": states,
            "pressure": pressure,
            "database": database,
        }

    async def run(self) -> None:
        await self._bootstrap()
        loop = asyncio.get_running_loop()
        for name in ("SIGTERM", "SIGINT"):
            loop.add_signal_handler(getattr(signal, name), self.stop.set)
        interval = 1 / self.config.records_per_second
        while not self.stop.is_set():
            snapshot = await self._snapshot()
            run = snapshot["run"]
            elapsed = snapshot["elapsed_seconds"]
            # A hard process loss can leave one durably appended source record
            # whose downstream tail is incomplete.  Finish that exact record
            # before accepting the wall-clock target; otherwise the final
            # snapshot could expose A ahead of B/C/J/D.
            accepted = await self._accepted_source_through()
            if accepted > run["source_completed"]:
                if accepted != run["source_completed"] + 1:
                    raise RuntimeError("source recovery gap")
                self.phase = "RECOVERING_TAIL"
                await self._renew()
                await self._process(run["source_completed"] + 1)
                self.phase = "IDLE"
                self._write_status(await self._snapshot())
                continue
            if elapsed >= self.config.target_seconds:
                self.phase = "TARGET_REACHED"
                async with self.stream.pool.acquire() as conn:
                    await conn.execute(
                        "UPDATE soak_run_v1 SET completed_at=COALESCE(completed_at,"
                        "clock_timestamp()) WHERE run_id=$1",
                        self.config.run_id,
                    )
                self._write_status(await self._snapshot())
                await asyncio.sleep(min(30.0, interval))
                continue
            if (
                run["source_completed"] > 0
                and run["source_completed"] % self.config.checkpoint_every == 0
                and run["last_checkpoint_source"] < run["source_completed"]
            ):
                self.phase = "CHECKPOINTING"
                await self._renew()
                await self._checkpoint(run["source_completed"])
                self.phase = "IDLE"
                self._write_status(await self._snapshot())
                continue
            self.phase = "PROCESSING"
            number = run["source_completed"] + 1
            started = asyncio.get_running_loop().time()
            await self._renew()
            await self._process(number)
            if (
                number % self.config.checkpoint_every == 0
                and run["last_checkpoint_source"] < number
            ):
                self.phase = "CHECKPOINTING"
                await self._checkpoint(number)
            self.phase = "IDLE"
            self._write_status(await self._snapshot())
            delay = interval - (asyncio.get_running_loop().time() - started)
            try:
                await asyncio.wait_for(self.stop.wait(), max(0.001, delay))
            except TimeoutError:
                pass


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-config", action="store_true")
    args = parser.parse_args()
    config = ServiceConfig.from_env()
    if args.print_config:
        print(canonical(asdict(config)))
        return 0
    service = LongSoakService(config)
    try:
        await service.run()
    except BaseException as exc:
        service.phase = "FAILED"
        service._write_status(
            {"error_type": type(exc).__name__, "error": str(exc)[:500]}
        )
        raise
    finally:
        if service.stream is not None:
            await service.stream.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
