"""Build an isolated PostgreSQL sync pair and measure restart/promotion."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import subprocess
import time
from pathlib import Path

import asyncpg
from postgres_stream_v2 import Output, PostgresStream

PRIMARY = "ariadne-ha-primary"
STANDBY = "ariadne-ha-standby"
NETWORK = "ariadne-ha-net"
PRIMARY_VOLUME = "ariadne-ha-primary-data"
STANDBY_VOLUME = "ariadne-ha-standby-data"
PRIMARY_DSN = "postgresql://ariadne:ariadne@127.0.0.1:55442/streaming"
STANDBY_DSN = "postgresql://ariadne:ariadne@127.0.0.1:55443/streaming"


def run(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=check, text=True, capture_output=True)


def cleanup() -> None:
    for name in (PRIMARY, STANDBY):
        run("docker", "rm", "-f", name, check=False)
    run("docker", "network", "rm", NETWORK, check=False)
    for name in (PRIMARY_VOLUME, STANDBY_VOLUME):
        run("docker", "volume", "rm", name, check=False)


async def connect_when_ready(dsn: str, timeout: float = 15) -> asyncpg.Connection:
    deadline = time.monotonic() + timeout
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            return await asyncpg.connect(dsn, timeout=1)
        except Exception as exc:  # the harness records the bounded wait result
            last = exc
            await asyncio.sleep(0.1)
    raise RuntimeError(f"database did not become ready: {last}")


async def wait_for_replica(primary: asyncpg.Connection) -> dict[str, str]:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        row = await primary.fetchrow(
            "SELECT application_name,state,sync_state FROM pg_stat_replication "
            "WHERE state='streaming'"
        )
        if row is not None:
            return dict(row)
        await asyncio.sleep(0.1)
    raise RuntimeError("standby did not enter streaming replication")


async def exercise() -> dict[str, object]:
    cleanup()
    run("docker", "network", "create", NETWORK)
    run("docker", "volume", "create", PRIMARY_VOLUME)
    run("docker", "volume", "create", STANDBY_VOLUME)
    run(
        "docker",
        "run",
        "-d",
        "--name",
        PRIMARY,
        "--network",
        NETWORK,
        "-p",
        "55442:5432",
        "-e",
        "POSTGRES_USER=ariadne",
        "-e",
        "POSTGRES_PASSWORD=ariadne",
        "-e",
        "POSTGRES_DB=streaming",
        "-v",
        f"{PRIMARY_VOLUME}:/var/lib/postgresql/data",
        "postgres:17-alpine",
        "-c",
        "wal_level=replica",
        "-c",
        "max_wal_senders=10",
        "-c",
        "hot_standby=on",
    )
    primary = await connect_when_ready(PRIMARY_DSN)
    await primary.execute(
        "CREATE ROLE ariadne_repl WITH REPLICATION LOGIN PASSWORD 'ariadne_repl'"
    )
    run(
        "docker",
        "exec",
        PRIMARY,
        "sh",
        "-c",
        "echo 'host replication ariadne_repl 0.0.0.0/0 scram-sha-256' "
        ">> /var/lib/postgresql/data/pg_hba.conf",
    )
    await primary.fetchval("SELECT pg_reload_conf()")
    run(
        "docker",
        "run",
        "--rm",
        "--network",
        NETWORK,
        "-e",
        "PGPASSWORD=ariadne_repl",
        "-v",
        f"{STANDBY_VOLUME}:/var/lib/postgresql/data",
        "postgres:17-alpine",
        "sh",
        "-ec",
        "rm -rf /var/lib/postgresql/data/*; "
        "chown -R postgres:postgres /var/lib/postgresql/data; "
        f"gosu postgres pg_basebackup -h {PRIMARY} -U ariadne_repl "
        "-D /var/lib/postgresql/data -Fp -Xs -R -P",
    )
    run(
        "docker",
        "run",
        "-d",
        "--name",
        STANDBY,
        "--network",
        NETWORK,
        "-p",
        "55443:5432",
        "-v",
        f"{STANDBY_VOLUME}:/var/lib/postgresql/data",
        "postgres:17-alpine",
    )
    standby = await connect_when_ready(STANDBY_DSN)
    replication = await wait_for_replica(primary)
    await primary.execute("ALTER SYSTEM SET synchronous_standby_names='*'")
    await primary.execute("ALTER SYSTEM SET synchronous_commit='remote_apply'")
    await primary.fetchval("SELECT pg_reload_conf()")
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        replication = await wait_for_replica(primary)
        if replication["sync_state"] == "sync":
            break
        await asyncio.sleep(0.1)
    if replication["sync_state"] != "sync":
        raise RuntimeError("standby did not become synchronous")
    await primary.close()
    stream = await PostgresStream.open(PRIMARY_DSN, "ha_stream")
    await stream.create_partition("input", "p0", max_records=20, max_bytes=100_000)
    await stream.create_partition("output", "p0", max_records=20, max_bytes=100_000)
    first = await stream.append(
        "input-attempt-1",
        "input",
        "p0",
        "input-record-1",
        "input-object-1",
        b"document-one",
        "bytes/v1",
        "source-one",
    )
    await stream.append(
        "input-attempt-2",
        "input",
        "p0",
        "input-record-2",
        "input-object-2",
        b"document-two",
        "bytes/v1",
        "source-two",
    )
    await stream.create_consumer("reviewer", "input", "p0")
    epoch = await stream.claim_consumer("reviewer", "anna", lease_seconds=300)
    token, _ = await stream.deliver_next("reviewer", "anna", epoch)
    output = Output(
        "output",
        "p0",
        "signed-record-1",
        "signed-object-1",
        b"signed-one",
        "bytes/v1",
        "review-lineage-1",
    )
    output_sequence = await stream.complete(
        token,
        "YIELD",
        output=output,
        managed_state={"signed": 1},
        state_schema="review/v1",
        state_version=1,
    )
    async with stream.pool.acquire() as conn:
        commit_lsn = await conn.fetchval("SELECT pg_current_wal_flush_lsn()")
        commit_lsn_text = await conn.fetchval("SELECT pg_current_wal_flush_lsn()::text")
        server_version = await conn.fetchval("SHOW server_version")
        primary_settings = {
            "synchronous_commit": await conn.fetchval("SHOW synchronous_commit"),
            "synchronous_standby_names": await conn.fetchval(
                "SHOW synchronous_standby_names"
            ),
        }
    await stream.close()
    await standby.execute('SET search_path TO "ha_stream",public')
    replayed_payload = bytes(
        await standby.fetchval(
            "SELECT payload FROM objects WHERE object_key='signed-object-1'"
        )
    )
    replayed_cursor = await standby.fetchval(
        "SELECT completed_through FROM consumers WHERE consumer_id='reviewer'"
    )
    replay_lsn = await standby.fetchval("SELECT pg_last_wal_replay_lsn()")
    replay_lsn_text = await standby.fetchval("SELECT pg_last_wal_replay_lsn()::text")
    lsn_caught_up = await standby.fetchval(
        "SELECT pg_wal_lsn_diff($1::pg_lsn,$2::pg_lsn)>=0", replay_lsn, commit_lsn
    )
    if (replayed_payload, replayed_cursor, lsn_caught_up) != (b"signed-one", 1, True):
        raise RuntimeError("remote_apply data-plane state was not visible on standby")
    await standby.close()

    restart_started = time.monotonic()
    run("docker", "restart", PRIMARY)
    primary = await connect_when_ready(PRIMARY_DSN)
    restart_seconds = time.monotonic() - restart_started
    await primary.close()
    restarted_stream = await PostgresStream.open(PRIMARY_DSN, "ha_stream")
    after_restart = await restarted_stream.position("reviewer")
    retry_after_restart = await restarted_stream.complete(
        token,
        "YIELD",
        output=output,
        managed_state={"signed": 1},
        state_schema="review/v1",
        state_version=1,
    )
    async with restarted_stream.pool.acquire() as conn:
        replication_after_restart = dict(
            await conn.fetchrow(
                "SELECT application_name,state,sync_state FROM pg_stat_replication"
            )
        )

    # Exercise the real append/completion path while the writable primary is
    # lost.  Every acknowledged append is later checked on the promoted node;
    # every planned attempt is then retried with the same immutable identity.
    load_total = 200
    load_acked: list[int] = []
    load_completed = 0
    load_errors: list[str] = []
    load_threshold = asyncio.Event()
    await restarted_stream.create_partition(
        "failover-load", "p0", max_records=load_total, max_bytes=2_000_000
    )
    await restarted_stream.create_consumer(
        "failover-consumer", "failover-load", "p0", start_sequence=1
    )
    load_epoch = await restarted_stream.claim_consumer(
        "failover-consumer", "before-loss", lease_seconds=0.5
    )

    async def load_producer() -> None:
        try:
            for number in range(1, load_total + 1):
                await restarted_stream.append(
                    f"failover-attempt-{number}",
                    "failover-load",
                    "p0",
                    f"failover-record-{number}",
                    f"failover-object-{number}",
                    f"payload-{number}".encode(),
                    "bytes/v1",
                    f"source-{number}",
                )
                load_acked.append(number)
                if len(load_acked) >= 20:
                    load_threshold.set()
        except Exception as exc:
            load_errors.append(f"producer:{type(exc).__name__}:{exc}")

    async def load_consumer() -> None:
        nonlocal load_completed
        try:
            while True:
                delivery = await restarted_stream.deliver_next(
                    "failover-consumer", "before-loss", load_epoch
                )
                if delivery is None:
                    await asyncio.sleep(0)
                    continue
                token, _ = delivery
                await restarted_stream.complete(
                    token,
                    "RETURN",
                    managed_state={"completed": token.sequence},
                    state_schema="failover-load/v1",
                    state_version=1,
                )
                load_completed = token.sequence
        except Exception as exc:
            load_errors.append(f"consumer:{type(exc).__name__}:{exc}")

    producer_task = asyncio.create_task(load_producer())
    consumer_task = asyncio.create_task(load_consumer())
    await asyncio.wait_for(load_threshold.wait(), timeout=10)

    failover_started = time.monotonic()
    await asyncio.to_thread(run, "docker", "kill", PRIMARY)
    await asyncio.wait_for(
        asyncio.gather(producer_task, consumer_task, return_exceptions=True), timeout=10
    )
    restarted_stream.pool.terminate()
    run("docker", "rm", PRIMARY)
    fenced_start = run("docker", "start", PRIMARY, check=False)
    run(
        "docker",
        "exec",
        "-u",
        "postgres",
        STANDBY,
        "pg_ctl",
        "promote",
        "-D",
        "/var/lib/postgresql/data",
        "-w",
    )
    promoted = await connect_when_ready(STANDBY_DSN)
    while await promoted.fetchval("SELECT pg_is_in_recovery()"):
        await asyncio.sleep(0.05)
    await promoted.close()
    promoted_stream = await PostgresStream.open(STANDBY_DSN, "ha_stream")
    promoted_position = await promoted_stream.position("reviewer")
    load_position_after_promotion = await promoted_stream.position("failover-consumer")
    acknowledged_before_loss = list(load_acked)
    for first_number in range(1, load_total + 1, 8):
        last_number = min(load_total, first_number + 7)
        await promoted_stream.append_batch(
            [
                (
                    f"failover-attempt-{number}",
                    "failover-load",
                    "p0",
                    f"failover-record-{number}",
                    f"failover-object-{number}",
                    f"payload-{number}".encode(),
                    "bytes/v1",
                    f"source-{number}",
                )
                for number in range(first_number, last_number + 1)
            ]
        )
    await asyncio.sleep(0.55)
    promoted_load_epoch = await promoted_stream.claim_consumer(
        "failover-consumer", "after-promotion", lease_seconds=30
    )
    while True:
        delivery = await promoted_stream.deliver_next(
            "failover-consumer", "after-promotion", promoted_load_epoch
        )
        if delivery is None:
            break
        promoted_token, _ = delivery
        await promoted_stream.complete(
            promoted_token,
            "RETURN",
            managed_state={"completed": promoted_token.sequence},
            state_schema="failover-load/v1",
            state_version=1,
        )
    recovered_load_position = await promoted_stream.position("failover-consumer")
    retry_after_promotion = await promoted_stream.append(
        "input-attempt-1",
        "input",
        "p0",
        "input-record-1",
        "input-object-1",
        b"document-one",
        "bytes/v1",
        "source-one",
    )
    next_append = await promoted_stream.append(
        "input-attempt-3",
        "input",
        "p0",
        "input-record-3",
        "input-object-3",
        b"document-three",
        "bytes/v1",
        "source-three",
    )
    output_payload = None
    async with promoted_stream.pool.acquire() as conn:
        output_payload = bytes(
            await conn.fetchval(
                "SELECT payload FROM objects WHERE object_key='signed-object-1'"
            )
        )
        completion_count = await conn.fetchval(
            "SELECT count(*) FROM turn_completions WHERE consumer_id='reviewer'"
        )
        load_sequences = await conn.fetch(
            "SELECT sequence,record_id FROM records WHERE channel='failover-load' "
            "AND partition_id='p0' ORDER BY sequence"
        )
        acknowledged_survivors = await conn.fetchval(
            "SELECT count(*) FROM records WHERE channel='failover-load' "
            "AND partition_id='p0' AND sequence=ANY($1::bigint[])",
            acknowledged_before_loss,
        )
        promoted_lsn = await conn.fetchval("SELECT pg_current_wal_flush_lsn()::text")
    invariants = await promoted_stream.invariant_report()
    await promoted_stream.close()
    failover_seconds = time.monotonic() - failover_started
    image_digest = run(
        "docker", "image", "inspect", "postgres:17-alpine", "--format", "{{.Id}}"
    ).stdout.strip()
    git_revision = run("git", "rev-parse", "HEAD").stdout.strip()
    script_digest = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    data_plane_digest = hashlib.sha256(
        Path(__file__).with_name("postgres_stream_v2.py").read_bytes()
    ).hexdigest()
    release_authority_digest = hashlib.sha256(
        Path(__file__).with_name("postgres_release_authority.py").read_bytes()
    ).hexdigest()
    return {
        "passed": (
            after_restart["completed_through"] == 1
            and retry_after_restart == output_sequence
            and promoted_position["completed_through"] == 1
            and retry_after_promotion == first
            and next_append.sequence == 3
            and output_payload == b"signed-one"
            and completion_count == 1
            and len(acknowledged_before_loss) >= 20
            and acknowledged_survivors == len(acknowledged_before_loss)
            and [row["sequence"] for row in load_sequences]
            == list(range(1, load_total + 1))
            and recovered_load_position["completed_through"] == load_total
            and invariants["passed"]
            and fenced_start.returncode != 0
            and restart_seconds <= 15
            and failover_seconds <= 15
        ),
        "postgres_version": server_version,
        "postgres_image_digest": image_digest,
        "git_revision": git_revision,
        "harness_sha256": script_digest,
        "data_plane_sha256": data_plane_digest,
        "release_authority_sha256": release_authority_digest,
        "evidence_scope": (
            "isolated PostgreSQL physical synchronous-storage restart/promotion and "
            "durable stream retry; release migration/session guards are provenance-only "
            "and are not exercised by this storage-layer campaign"
        ),
        "durability_profile": "synchronous remote_apply",
        "primary_settings": primary_settings,
        "primary_commit_lsn": commit_lsn_text,
        "standby_replay_lsn_before_loss": replay_lsn_text,
        "promoted_flush_lsn": promoted_lsn,
        "replication_before": replication,
        "replication_after_restart": replication_after_restart,
        "restart_seconds": restart_seconds,
        "promotion_seconds": failover_seconds,
        "former_primary_fence": {
            "mechanism": "container removed; persistent volume quarantined",
            "restart_returncode": fenced_start.returncode,
        },
        "data_plane_after_promotion": {
            "cursor": promoted_position["completed_through"],
            "managed_state": promoted_position["managed_state"],
            "output_payload_sha256": hashlib.sha256(output_payload).hexdigest(),
            "completion_count": completion_count,
            "exact_retry_sequence": retry_after_promotion.sequence,
            "next_contiguous_sequence": next_append.sequence,
            "invariants": invariants,
        },
        "failover_during_stream_load": {
            "planned_records": load_total,
            "acknowledged_before_loss": len(acknowledged_before_loss),
            "acknowledged_survivors": acknowledged_survivors,
            "completed_before_loss_observed_by_client": load_completed,
            "durable_completed_after_promotion": load_position_after_promotion[
                "completed_through"
            ],
            "completed_after_recovery": recovered_load_position["completed_through"],
            "contiguous_sequences": [row["sequence"] for row in load_sequences]
            == list(range(1, load_total + 1)),
            "client_errors_at_loss": load_errors,
            "recovery": "retry all immutable attempts, lease takeover, drain suffix",
        },
        "commands": {
            "primary_profile": [
                "postgres:17-alpine",
                "wal_level=replica",
                "max_wal_senders=10",
                "hot_standby=on",
            ],
            "standby_seed": "pg_basebackup -Fp -Xs -R",
            "promotion": "pg_ctl promote -w",
        },
        "restart_slo_seconds": 15,
        "promotion_slo_seconds": 15,
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--keep", action="store_true")
    args = parser.parse_args()
    try:
        report = await exercise()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
        print(json.dumps(report, sort_keys=True))
        return 0 if report["passed"] else 2
    finally:
        if not args.keep:
            cleanup()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
