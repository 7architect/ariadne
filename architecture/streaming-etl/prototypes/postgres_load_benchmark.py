"""DS-011 reproducible load, pressure, stalled-consumer and catch-up benchmark."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import multiprocessing
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from postgres_stream_v2 import (
    IdempotencyHorizonExpired,
    PostgresStream,
    content_object_key,
    metadata_operation_id,
)

DSN = os.environ.get(
    "ARIADNE_STREAM_DSN",
    "postgresql://ariadne:ariadne@127.0.0.1:55432/streaming",
)
TARGET_RATE = 500
OFFERED_RATE = 510
BATCH_SIZE = 3
CATCHUP_BATCH_SIZE = 8
PARTITION_COUNT = 4
OUTAGE_SECONDS = 10
BACKLOG_SECONDS = 60
IDLE_POLL_SECONDS = 0.020
CATCHUP_RATE_LIMIT = 900
MEAN_PAYLOAD_BYTES = int(0.80 * 1024 + 0.15 * 16 * 1024 + 0.05 * 256 * 1024)


def percentile(values: list[float], quantile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, math.ceil(len(ordered) * quantile) - 1)
    return ordered[index]


def payload_for(number: int, distribution_number: int | None = None) -> bytes:
    bucket = (number if distribution_number is None else distribution_number) % 20
    if bucket == 0:
        size = 256 * 1024
    elif bucket < 4:
        size = 16 * 1024
    else:
        size = 1024
    prefix = number.to_bytes(8, "big")
    return prefix + b"x" * (size - len(prefix))


def run_consumer_process(
    schema: str,
    consumer_epochs: list[int],
    catchup_started_value: object,
    backlog_records_value: object,
    appended: object,
    producers_done: object,
    ready: object,
    go: object,
    result_queue: object,
) -> None:
    """Connect before measurement, then run catch-up after the explicit GO."""

    async def consume() -> dict[str, object]:
        consumer_store = await PostgresStream.connect_existing(DSN, schema)
        ready.set()
        await asyncio.to_thread(go.wait)
        catchup_started = float(catchup_started_value.value)
        backlog_records = int(backlog_records_value.value)
        caught_up = 0
        drained = asyncio.Event()
        backlog_recovered = False
        recovered_at: float | None = None
        recovered_completions: int | None = None
        recovered_lag: int | None = None
        recovered_while_ingress_active = False

        async def catch_up(partition_number: int) -> None:
            nonlocal caught_up, backlog_recovered, recovered_at
            nonlocal recovered_completions, recovered_lag
            nonlocal recovered_while_ingress_active
            consumer_id = f"stalled-consumer-{partition_number}"
            owner = f"catchup-worker-{partition_number}"
            epoch = consumer_epochs[partition_number]
            partition_completed = 0
            per_partition_rate = CATCHUP_RATE_LIMIT / PARTITION_COUNT
            while not drained.is_set():
                due = catchup_started + partition_completed / per_partition_rate
                delay = due - time.monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                deliveries = await consumer_store.deliver_batch(
                    consumer_id, owner, epoch, max_records=CATCHUP_BATCH_SIZE
                )
                if not deliveries:
                    if producers_done.is_set() and caught_up >= sum(appended):
                        drained.set()
                        return
                    await asyncio.sleep(IDLE_POLL_SECONDS)
                    continue
                await consumer_store.complete_return_batch(
                    deliveries,
                    managed_states=[
                        {"last": token.sequence} for token, _ in deliveries
                    ],
                    state_schema="benchmark/v1",
                    state_version=1,
                )
                caught_up += len(deliveries)
                partition_completed += len(deliveries)
                live_lag = sum(appended) - caught_up
                if (
                    not backlog_recovered
                    and caught_up >= backlog_records
                    and live_lag <= CATCHUP_BATCH_SIZE * PARTITION_COUNT
                ):
                    recovered_at = time.monotonic()
                    recovered_completions = caught_up
                    recovered_lag = live_lag
                    recovered_while_ingress_active = not producers_done.is_set()
                    backlog_recovered = True
                if producers_done.is_set() and caught_up >= sum(appended):
                    drained.set()

        try:
            await asyncio.gather(
                *(catch_up(number) for number in range(PARTITION_COUNT))
            )
        finally:
            await consumer_store.close()
        if not backlog_recovered:
            recovered_at = time.monotonic()
            recovered_completions = caught_up
            recovered_lag = sum(appended) - caught_up
        assert recovered_at is not None and recovered_completions is not None
        return {
            "caught_up": caught_up,
            "recovered_at": recovered_at,
            "recovered_completions": recovered_completions,
            "recovered_lag": recovered_lag,
            "recovered_while_ingress_active": recovered_while_ingress_active,
        }

    try:
        result_queue.put({"result": asyncio.run(consume())})
    except BaseException as exc:
        result_queue.put({"error": f"{type(exc).__name__}:{exc}"})


async def benchmark(duration: float, schema: str) -> dict[str, object]:
    store = await PostgresStream.open(DSN, schema)
    capacity_records = 15_000
    capacity_bytes = 300 * 1024 * 1024
    capacity_age_seconds = 65
    for partition_number in range(PARTITION_COUNT):
        await store.create_partition(
            "load",
            f"p{partition_number}",
            max_records=capacity_records,
            max_bytes=capacity_bytes,
            max_age_seconds=capacity_age_seconds,
            max_metadata_records=capacity_records * 2,
        )
        await store.register_producer(
            "load", f"p{partition_number}", f"load-producer-p{partition_number}"
        )
    consumer_epochs: list[int] = []
    for partition_number in range(PARTITION_COUNT):
        consumer_id = f"stalled-consumer-{partition_number}"
        await store.create_consumer(
            consumer_id, "load", f"p{partition_number}", start_sequence=1
        )
        consumer_epochs.append(
            await store.claim_consumer(
                consumer_id, f"catchup-worker-{partition_number}", lease_seconds=300
            )
        )

    # The measured interval is steady-state ingress.  Warm distinct producer
    # connections and prepare the stalled consumer before the clock starts.
    async def warm_connection() -> int:
        async with store.pool.acquire() as conn:
            return int(
                await conn.fetchval("SELECT pg_backend_pid() FROM pg_sleep(0.02)")
            )

    producer_backend_pids = await asyncio.gather(
        *(warm_connection() for _ in range(PARTITION_COUNT))
    )
    process_context = multiprocessing.get_context("spawn")
    appended = process_context.Array("q", [0] * PARTITION_COUNT, lock=False)
    producers_done = process_context.Event()
    consumer_ready = process_context.Event()
    consumer_go = process_context.Event()
    catchup_started_value = process_context.Value("d", 0.0)
    backlog_records_value = process_context.Value("q", 0)
    result_queue = process_context.Queue()
    consumer_process = process_context.Process(
        target=run_consumer_process,
        args=(
            schema,
            consumer_epochs,
            catchup_started_value,
            backlog_records_value,
            appended,
            producers_done,
            consumer_ready,
            consumer_go,
            result_queue,
        ),
    )
    consumer_process.start()
    if not await asyncio.to_thread(consumer_ready.wait, 30):
        consumer_process.terminate()
        consumer_process.join()
        raise TimeoutError("consumer process did not become ready")

    async def postgres_stats() -> dict[str, object]:
        async with store.pool.acquire() as conn:
            return {
                "checkpointer": json.loads(
                    await conn.fetchval(
                        "SELECT to_jsonb(s)::text FROM pg_stat_checkpointer s"
                    )
                ),
                "wal": json.loads(
                    await conn.fetchval("SELECT to_jsonb(s)::text FROM pg_stat_wal s")
                ),
                "database": json.loads(
                    await conn.fetchval(
                        "SELECT to_jsonb(s)::text FROM pg_stat_database s "
                        "WHERE datname=current_database()"
                    )
                ),
            }

    postgres_stats_before = await postgres_stats()
    started = time.monotonic()
    deadline = started + duration
    latencies_ms: list[float] = []
    latencies_by_payload: dict[int, list[float]] = {
        1024: [],
        16 * 1024: [],
        256 * 1024: [],
    }
    scheduler_delay_ms: list[float] = []
    queue_wait_ms: list[float] = []
    encoding_ms: list[float] = []
    append_round_trip_ms: list[float] = []
    batch_timings: list[dict[str, object]] = []
    errors: list[str] = []

    async def producer(partition_number: int) -> None:
        partition = f"p{partition_number}"
        local_sequence = 0
        local_batch = 0
        while True:
            numbers = list(range(local_sequence + 1, local_sequence + BATCH_SIZE + 1))
            global_batch = local_batch * PARTITION_COUNT + partition_number
            due_times = [
                started + (global_batch * BATCH_SIZE + offset + 1) / OFFERED_RATE
                for offset in range(BATCH_SIZE)
            ]
            if due_times[-1] > deadline:
                return
            delay = due_times[-1] - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            woke = time.monotonic()
            scheduler_delay_ms.append((woke - due_times[-1]) * 1000)
            queue_wait_ms.append((woke - due_times[0]) * 1000)
            items = []
            for offset, number in enumerate(numbers):
                global_number = global_batch * BATCH_SIZE + offset + 1
                payload = payload_for(global_number)
                items.append(
                    (
                        f"load-attempt-{partition}-{number}",
                        "load",
                        partition,
                        f"load-record-{partition}-{number}",
                        content_object_key(payload, "bytes/v1"),
                        payload,
                        "bytes/v1",
                        f"load-lineage-{partition}-{number}",
                        f"load-producer-{partition}",
                        number,
                    )
                )
            payload_ready = time.monotonic()
            encoding_ms.append((payload_ready - woke) * 1000)
            append_call_started = time.monotonic()
            try:
                await store.append_batch(items)
            except Exception as exc:
                errors.append(f"{type(exc).__name__}:{exc}")
                return
            acknowledged = time.monotonic()
            append_round_trip_ms.append((acknowledged - append_call_started) * 1000)
            for due, item in zip(due_times, items):
                latency = (acknowledged - due) * 1000
                latencies_ms.append(latency)
                latencies_by_payload[len(item[5])].append(latency)
            batch_timings.append(
                {
                    "partition": partition,
                    "local_first_sequence": numbers[0],
                    "local_last_sequence": numbers[-1],
                    "scheduled_first_second": due_times[0] - started,
                    "scheduled_last_second": due_times[-1] - started,
                    "wake_second": woke - started,
                    "payload_ready_second": payload_ready - started,
                    "append_call_start_second": append_call_started - started,
                    "ack_second": acknowledged - started,
                    "queue_wait_ms": (woke - due_times[0]) * 1000,
                    "wake_delay_ms": (woke - due_times[-1]) * 1000,
                    "encoding_ms": (payload_ready - woke) * 1000,
                    "db_call_ms": (acknowledged - append_call_started) * 1000,
                    "worst_offer_to_ack_ms": (acknowledged - due_times[0]) * 1000,
                }
            )
            local_sequence += BATCH_SIZE
            local_batch += 1
            appended[partition_number] = local_sequence

    producer_tasks = [
        asyncio.create_task(producer(number)) for number in range(PARTITION_COUNT)
    ]
    await asyncio.sleep(min(OUTAGE_SECONDS, duration))
    backlog_records = sum(appended)
    catchup_started = time.monotonic()
    backlog_records_value.value = backlog_records
    catchup_started_value.value = catchup_started
    consumer_go.set()
    await asyncio.gather(*producer_tasks)
    postgres_stats_after = await postgres_stats()
    producers_done.set()
    await asyncio.wait_for(asyncio.to_thread(consumer_process.join, 120), timeout=125)
    if consumer_process.is_alive():
        consumer_process.terminate()
        consumer_process.join()
        raise TimeoutError("consumer process did not drain")
    consumer_message = result_queue.get(timeout=1)
    if "error" in consumer_message:
        raise RuntimeError(consumer_message["error"])
    consumer_result = consumer_message["result"]
    caught_up = consumer_result["caught_up"]
    recovered_at = consumer_result["recovered_at"]
    recovered_completions = consumer_result["recovered_completions"]
    recovered_lag = consumer_result["recovered_lag"]
    recovered_while_ingress_active = consumer_result["recovered_while_ingress_active"]
    catchup_elapsed = recovered_at - catchup_started
    catchup_rate = (
        recovered_completions / catchup_elapsed if catchup_elapsed else float("inf")
    )
    catchup_started_second = catchup_started - started
    top_outliers = sorted(
        batch_timings,
        key=lambda item: float(item["worst_offer_to_ack_ms"]),
        reverse=True,
    )[:20]
    for item in top_outliers:
        item["catchup_active"] = float(item["ack_second"]) >= catchup_started_second
    elapsed = time.monotonic() - started
    ingested = sum(appended)
    ingress_rate = ingested / duration

    compaction_started = time.monotonic()
    compaction_results = [
        await store.compact("load", f"p{number}", appended[number])
        for number in range(PARTITION_COUNT)
    ]
    compaction_seconds = time.monotonic() - compaction_started
    compaction_rate = (
        sum(result["reclaimed_records"] for result in compaction_results)
        / compaction_seconds
        if compaction_seconds
        else float("inf")
    )
    compaction = {
        "scope": "live ingress partitions with completed consumer history",
        "reclaimed_records": sum(
            result["reclaimed_records"] for result in compaction_results
        ),
        "reclaimed_bytes": sum(
            result["reclaimed_bytes"] for result in compaction_results
        ),
        "safe_through": {
            f"p{number}": result["safe_through"]
            for number, result in enumerate(compaction_results)
        },
        "reclaimed_records_per_second": compaction_rate,
    }
    metadata_started = time.monotonic()
    metadata_cuts = [
        ("load", f"p{number}", appended[number]) for number in range(PARTITION_COUNT)
    ]
    metadata_id = metadata_operation_id(metadata_cuts)
    metadata_compaction = await store.compact_metadata(
        metadata_cuts,
        operation_id=metadata_id,
    )
    metadata_retry = await store.compact_metadata(
        metadata_cuts,
        operation_id=metadata_id,
    )
    metadata_compaction["idempotent_retry"] = metadata_retry["idempotent_retry"]
    metadata_seconds = time.monotonic() - metadata_started
    expired_retry_rejected = False
    first_payload = payload_for(1)
    try:
        await store.append(
            "load-attempt-p0-1",
            "load",
            "p0",
            "load-record-p0-1",
            content_object_key(first_payload, "bytes/v1"),
            first_payload,
            "bytes/v1",
            "load-lineage-p0-1",
            producer_id="load-producer-p0",
            producer_sequence=1,
        )
    except IdempotencyHorizonExpired:
        expired_retry_rejected = True
    invariants = await store.invariant_report()
    async with store.pool.acquire() as conn:
        parts = [
            dict(row)
            for row in await conn.fetch(
                "SELECT * FROM partitions WHERE channel='load' ORDER BY partition_id"
            )
        ]
        compaction["credits_after"] = {
            row["partition_id"]: {
                "used_records": row["used_records"],
                "used_bytes": row["used_bytes"],
            }
            for row in parts
        }
        metadata_pressure = {
            f"p{number}": await store.pressure_snapshot("load", f"p{number}")
            for number in range(PARTITION_COUNT)
        }
        replication = [
            dict(row)
            for row in await conn.fetch(
                "SELECT application_name,state,sync_state FROM pg_stat_replication"
            )
        ]
        settings = {
            "synchronous_commit": await conn.fetchval("SHOW synchronous_commit"),
            "synchronous_standby_names": await conn.fetchval(
                "SHOW synchronous_standby_names"
            ),
            "server_version": await conn.fetchval("SHOW server_version"),
            "wal_level": await conn.fetchval("SHOW wal_level"),
            "max_wal_size": await conn.fetchval("SHOW max_wal_size"),
            "min_wal_size": await conn.fetchval("SHOW min_wal_size"),
            "checkpoint_timeout": await conn.fetchval("SHOW checkpoint_timeout"),
            "checkpoint_completion_target": await conn.fetchval(
                "SHOW checkpoint_completion_target"
            ),
            "shared_buffers": await conn.fetchval("SHOW shared_buffers"),
        }
    await store.close()
    oracle_process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).with_name("postgres_stream_oracle.py")),
        DSN,
        schema,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    oracle_stdout, oracle_stderr = await oracle_process.communicate()
    oracle = (
        json.loads(oracle_stdout)
        if oracle_stdout
        else {"passed": False, "violations": [oracle_stderr.decode()]}
    )
    latency = {
        "p50_ms": percentile(latencies_ms, 0.50),
        "p95_ms": percentile(latencies_ms, 0.95),
        "p99_ms": percentile(latencies_ms, 0.99),
        "mean_ms": statistics.fmean(latencies_ms),
    }
    thresholds = {
        "duration_seconds": 60,
        "ingress_records_per_second": TARGET_RATE,
        "append_p95_ms": 40,
        "append_p99_ms": 100,
        "catchup_records_per_second": max(TARGET_RATE, ingress_rate) * 1.25,
        "compaction_records_per_second": max(TARGET_RATE, ingress_rate) * 1.25,
        "backlog_seconds": BACKLOG_SECONDS,
    }
    checks = {
        "duration": duration >= 60,
        "ingress": ingress_rate >= thresholds["ingress_records_per_second"],
        "p95": latency["p95_ms"] <= thresholds["append_p95_ms"],
        "p99": latency["p99_ms"] <= thresholds["append_p99_ms"],
        "catchup": catchup_rate >= thresholds["catchup_records_per_second"],
        "backlog_recovered_during_ingress": recovered_while_ingress_active,
        "record_horizon": sum(part["max_records"] for part in parts)
        >= ingress_rate * BACKLOG_SECONDS,
        "byte_horizon": sum(part["max_bytes"] for part in parts)
        >= ingress_rate * BACKLOG_SECONDS * MEAN_PAYLOAD_BYTES,
        "time_horizon": min(part["max_age_seconds"] for part in parts)
        >= BACKLOG_SECONDS,
        "compaction": (
            compaction["reclaimed_records"] == ingested
            and compaction_rate >= thresholds["compaction_records_per_second"]
            and all(
                part["used_records"] == 0 and part["used_bytes"] == 0 for part in parts
            )
        ),
        "metadata_compaction": (
            expired_retry_rejected
            and metadata_compaction["idempotent_retry"]
            and all(part["used_metadata_records"] == 0 for part in parts)
            and all(
                pressure["hot_metadata_rows"]["append_attempts"] == 0
                and pressure["hot_metadata_rows"]["tombstones"] == 0
                and pressure["hot_metadata_rows"]["manifests"] == 0
                for pressure in metadata_pressure.values()
            )
        ),
        "replication": any(row["sync_state"] == "sync" for row in replication),
        "remote_apply": settings["synchronous_commit"] == "remote_apply",
        "invariants": invariants["passed"],
        "independent_oracle": oracle_process.returncode == 0 and oracle["passed"],
        "no_errors": not errors,
    }
    return {
        "passed": all(checks.values()) and duration >= 60,
        "schema": schema,
        "payload_distribution": {
            "1KiB": 0.80,
            "16KiB": 0.15,
            "256KiB": 0.05,
            "offer_schedule": (
                "deterministic global batch ordinals, one three-record batch "
                "every 3/510 seconds, round-robin across four partitions"
            ),
        },
        "mean_payload_bytes": MEAN_PAYLOAD_BYTES,
        "offered_records_per_second": OFFERED_RATE,
        "duration_seconds": elapsed,
        "ingested_records": ingested,
        "ingress_records_per_second": ingress_rate,
        "latency": {
            **latency,
            "definition": "per offered record from scheduled offer time through group commit ACK",
            "group_size": BATCH_SIZE,
            "by_payload_bytes": {
                str(size): {
                    "count": len(values),
                    "p50_ms": percentile(values, 0.50),
                    "p95_ms": percentile(values, 0.95),
                    "p99_ms": percentile(values, 0.99),
                }
                for size, values in latencies_by_payload.items()
            },
            "batch_scheduler_delay_ms": {
                "p95": percentile(scheduler_delay_ms, 0.95),
                "p99": percentile(scheduler_delay_ms, 0.99),
                "max": max(scheduler_delay_ms),
            },
            "batch_queue_wait_ms": {
                "definition": (
                    "scheduled first record through event-loop wake; includes "
                    "the intentional three-record group-fill interval"
                ),
                "p95": percentile(queue_wait_ms, 0.95),
                "p99": percentile(queue_wait_ms, 0.99),
                "max": max(queue_wait_ms),
            },
            "payload_encoding_ms": {
                "p95": percentile(encoding_ms, 0.95),
                "p99": percentile(encoding_ms, 0.99),
                "max": max(encoding_ms),
            },
            "append_round_trip_ms": {
                "p95": percentile(append_round_trip_ms, 0.95),
                "p99": percentile(append_round_trip_ms, 0.99),
                "max": max(append_round_trip_ms),
            },
            "top_20_batch_outliers": top_outliers,
        },
        "stalled_consumer": {
            "outage_seconds": OUTAGE_SECONDS,
            "idle_poll_seconds": IDLE_POLL_SECONDS,
            "backlog_records": backlog_records,
            "caught_up_records": caught_up,
            "catchup_seconds": catchup_elapsed,
            "catchup_records_per_second": catchup_rate,
            "configured_catchup_limit_records_per_second": CATCHUP_RATE_LIMIT,
            "completions_at_backlog_recovery": recovered_completions,
            "lag_at_backlog_recovery": recovered_lag,
            "live_ingress_continued": True,
            "backlog_recovered_during_ingress": recovered_while_ingress_active,
            "backlog_basis": "records durably appended during measured 10-second outage",
            "catchup_rate_definition": (
                "gross durable completions per second from consumer start until "
                "the outage backlog is first eliminated"
            ),
        },
        "capacity": {
            "partitions": PARTITION_COUNT,
            "max_records": sum(part["max_records"] for part in parts),
            "max_bytes": sum(part["max_bytes"] for part in parts),
            "min_max_age_seconds": min(part["max_age_seconds"] for part in parts),
            "required_records_for_60s": ingress_rate * BACKLOG_SECONDS,
            "required_bytes_for_60s": ingress_rate
            * BACKLOG_SECONDS
            * MEAN_PAYLOAD_BYTES,
            "required_age_seconds": BACKLOG_SECONDS,
            "used_records": sum(part["used_records"] for part in parts),
            "used_bytes": sum(part["used_bytes"] for part in parts),
        },
        "compaction": {**compaction, "seconds": compaction_seconds},
        "metadata_compaction": {
            **metadata_compaction,
            "seconds": metadata_seconds,
            "expired_retry_rejected": expired_retry_rejected,
            "pressure_after": metadata_pressure,
        },
        "settings": settings,
        "postgres_stats": {
            "before_measured_ingress": postgres_stats_before,
            "after_measured_ingress": postgres_stats_after,
        },
        "replication": replication,
        "thresholds": thresholds,
        "checks": checks,
        "invariants": invariants,
        "independent_oracle": oracle,
        "provenance": {
            "git_revision": subprocess.run(
                ["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=True
            ).stdout.strip(),
            "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "data_plane_sha256": hashlib.sha256(
                Path(__file__).with_name("postgres_stream_v2.py").read_bytes()
            ).hexdigest(),
            "oracle_sha256": hashlib.sha256(
                Path(__file__).with_name("postgres_stream_oracle.py").read_bytes()
            ).hexdigest(),
            "postgres_image_digest": subprocess.run(
                [
                    "docker",
                    "image",
                    "inspect",
                    "postgres:17-alpine",
                    "--format",
                    "{{.Id}}",
                ],
                text=True,
                capture_output=True,
                check=True,
            ).stdout.strip(),
            "process_topology": {
                "producer_processes": 1,
                "producer_coroutines": PARTITION_COUNT,
                "producer_backend_pids": producer_backend_pids,
                "producer_connections_warm_before_measurement": True,
                "consumer_processes": 1,
                "consumer_start_method": process_context.get_start_method(),
                "consumer_ready_before_measurement": True,
                "consumer_go_at_second": catchup_started_second,
                "available_cpu_count": os.cpu_count(),
                "process_cpu_affinity": sorted(os.sched_getaffinity(0)),
            },
        },
        "errors": errors[:10],
    }


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument("--schema", default="ds011_benchmark_20261023")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = await benchmark(args.duration, args.schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True))
    return 0 if report["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
