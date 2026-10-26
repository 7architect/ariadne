"""Read-only streaming metrics, native Prefect state and actionable alerts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from postgres_control_plane import ControlNotFound, PostgresControlPlane
from postgres_stream_telemetry import StreamTelemetry


@dataclass(frozen=True)
class AlertRules:
    lag_records: int = 100
    checkpoint_age_seconds: float = 30
    recovery_age_seconds: float = 60
    capacity_fraction: float = 0.9

    def __post_init__(self):
        if (
            self.lag_records <= 0
            or self.checkpoint_age_seconds <= 0
            or self.recovery_age_seconds <= 0
            or not 0 < self.capacity_fraction <= 1
        ):
            raise ValueError(
                "positive alert thresholds and capacity fraction <=1 required"
            )


def _time(value) -> datetime:
    return datetime.fromisoformat(value) if isinstance(value, str) else value


class PrefectStateReader:
    """Read the real private Prefect API; never infer its state from stream intent."""

    def __init__(self, api_url: str, *, headers: dict[str, str] | None = None):
        self.client = httpx.AsyncClient(
            base_url=api_url.rstrip("/") + "/",
            headers=headers,
            timeout=2,
            follow_redirects=False,
        )

    async def read(self, flow_run_id: str) -> dict[str, Any]:
        try:
            flow_run_id = str(UUID(flow_run_id))
            response = await self.client.get("flow_runs/" + flow_run_id)
            response.raise_for_status()
            flow = response.json()
            tasks = await self.client.post(
                "task_runs/filter", json={"flow_runs": {"id": {"any_": [flow_run_id]}}}
            )
            tasks.raise_for_status()
            return {
                "configured": True,
                "available": True,
                "flow_run_id": flow["id"],
                "flow_name": flow["name"],
                "flow_state_type": flow.get("state_type"),
                "flow_state_name": flow.get("state_name"),
                "tasks": [
                    {
                        "task_run_id": row["id"],
                        "name": row["name"],
                        "state_type": row.get("state_type"),
                        "state_name": row.get("state_name"),
                    }
                    for row in tasks.json()
                ],
            }
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            return {
                "configured": True,
                "available": False,
                "flow_run_id": flow_run_id,
                "error_type": type(exc).__name__,
            }

    async def close(self):
        await self.client.aclose()


class StreamObservability:
    def __init__(
        self,
        control: PostgresControlPlane,
        *,
        attempts=None,
        prefect_reader: PrefectStateReader | None = None,
        rules: AlertRules | None = None,
        telemetry: StreamTelemetry | None = None,
    ):
        self.control = control
        self.attempts = attempts
        self.prefect_reader = prefect_reader
        self.rules = AlertRules() if rules is None else rules
        self._owns_telemetry = telemetry is None
        self.telemetry = StreamTelemetry() if telemetry is None else telemetry

    async def sample(self, token: str, run_id: str) -> dict[str, Any]:
        started = datetime.now(UTC)
        projection = await self.control.project_run(token, run_id=run_id)
        with self.telemetry.span(
            "stream.observe",
            {
                "stream.run_id": run_id,
                "stream.revision_id": projection["control"]["revision_id"],
            },
        ):
            return await self._collect(token, run_id, started, projection)

    async def _collect(self, token, run_id, started, projection):
        partition_tails = {
            (row["channel"], row["partition_id"]): row["next_sequence"] - 1
            for row in projection["partitions"]
        }
        inputs = [
            {**row, "node": row["participant_id"]} for row in projection["inputs"]
        ]
        inputs += [
            {
                "node": "legacy:" + row["consumer_id"],
                "channel": row["channel"],
                "partition_id": row["partition_id"],
                "completed": row["completed_through"],
                "in_flight": row["in_flight_sequence"] is not None,
                "in_flight_sequence": row["in_flight_sequence"],
                "owner_id": row["owner_id"],
                "owner_epoch": row["owner_epoch"],
                "lease_expires_at": row["lease_expires_at"],
            }
            for row in projection["consumers"]
        ]
        nodes = {row["participant_id"]: row for row in projection["operators"]}
        for row in inputs:
            row["lag_records"] = (
                partition_tails[(row["channel"], row["partition_id"])]
                - row["completed"]
            )
            if row["node"] in nodes:
                node = nodes[row["node"]]
                row.update(
                    {
                        key: node[key]
                        for key in (
                            "owner_id",
                            "owner_epoch",
                            "lease_expires_at",
                            "phase",
                        )
                    }
                )
        async with (
            self.control.pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            sources = {
                (row["channel"], row["partition_id"])
                for row in await conn.fetch(
                    "SELECT channel,partition_id FROM control_source_ownership_v1 WHERE run_id=$1",
                    run_id,
                )
            }
            pending = {}
            active = {}
            if await conn.fetchval(
                "SELECT to_regclass('join_receipts_v3') IS NOT NULL"
            ):
                pending = {
                    row["participant_id"]: row["n"]
                    for row in await conn.fetch(
                        "SELECT r.participant_id,count(*) n FROM join_receipts_v3 r WHERE r.revision_id=$1 AND NOT EXISTS(SELECT 1 FROM join_turns_v3 t WHERE t.revision_id=r.revision_id AND t.participant_id=r.participant_id AND t.arrival_ordinal=r.arrival_ordinal) GROUP BY r.participant_id",
                        projection["control"]["revision_id"],
                    )
                }
                active = {
                    (row["participant_id"], row["channel"], row["partition_id"]): row[
                        "data_sequence"
                    ]
                    for row in await conn.fetch(
                        "SELECT i.participant_id,r.channel,r.partition_id,r.data_sequence "
                        "FROM join_invocations_v3 i JOIN join_receipts_v3 r "
                        "USING(revision_id,participant_id,arrival_ordinal) WHERE i.revision_id=$1",
                        projection["control"]["revision_id"],
                    )
                }
            checkpoint_created = None
            if projection["barrier"] is not None:
                checkpoint_created = await conn.fetchval(
                    "SELECT created_at FROM checkpoint_intents_v2 WHERE checkpoint_id=$1",
                    projection["barrier"]["checkpoint_id"],
                )
        for row in inputs:
            sequence = active.get((row["node"], row["channel"], row["partition_id"]))
            row.setdefault("in_flight_sequence", sequence)
            row["in_flight"] = row.get("in_flight", sequence is not None)
            row["node_pending_receipts"] = pending.get(row["node"], 0)
        now = datetime.now(UTC)
        recovery = projection["recovery"]
        metrics = {
            "inputs": inputs,
            "source_records": sum(partition_tails[location] for location in sources),
            "admission": dict(projection["admission_pressure"]),
            "checkpoint_age_seconds": None
            if checkpoint_created is None
            else max(0, (now - checkpoint_created).total_seconds()),
            "recovery_age_seconds": None
            if recovery is None
            else max(0, (now - _time(recovery["created_at"])).total_seconds()),
        }
        attempt = None
        if self.attempts is not None:
            try:
                attempt = await self.attempts.project_attempt(
                    token, control_run_id=run_id
                )
            except ControlNotFound:
                pass
        native = {"configured": False, "available": False}
        if attempt is not None and self.prefect_reader is not None:
            native = await self.prefect_reader.read(
                attempt["current"]["prefect_flow_run_id"]
            )
        alerts = self._alerts(projection, metrics, native, now)
        return {
            **projection,
            "metrics": metrics,
            "attempt": attempt,
            "prefect": native,
            "alerts": alerts,
            "sample_started_at": started.isoformat(),
            "sample_finished_at": datetime.now(UTC).isoformat(),
        }

    def _alerts(self, projection, metrics, native, now):
        alerts = []

        def add(kind, subject, message, severity="warning"):
            alerts.append(
                {
                    "code": kind,
                    "subject": subject,
                    "message": message,
                    "severity": severity,
                    "runbook": "/runbook#" + kind,
                }
            )

        running = projection["control"]["lifecycle"] in ("ACTIVE", "DRAINING")
        admission = metrics["admission"]
        if (
            running
            and max(
                admission["used_records"] / admission["max_records"],
                admission["used_bytes"] / admission["max_bytes"],
            )
            >= self.rules.capacity_fraction
        ):
            add("admission-capacity", "source", "Источник ждёт возврата ёмкости.")
        for row in metrics["inputs"]:
            if row["lag_records"] < 0:
                add(
                    "invalid-progress",
                    row["node"],
                    "Позиция обработки превышает подтверждённый хвост.",
                    "error",
                )
            elif running and row["lag_records"] >= self.rules.lag_records:
                add("input-lag", row["node"], "На входе узла накопилось отставание.")
            expiry = row.get("lease_expires_at")
            if (
                running
                and row["lag_records"] > 0
                and row.get("phase") != "RESTORING"
                and (
                    row.get("owner_id") is None
                    or expiry is None
                    or _time(expiry) <= now
                )
            ):
                add(
                    "missing-owner",
                    row["node"],
                    "Узел с необработанными данными не имеет действующего владельца.",
                    "error",
                )
        checkpoint = projection["barrier"]
        if (
            running
            and checkpoint is not None
            and checkpoint["status"] not in ("COMPLETED", "ABORTED", "RETIRED")
            and metrics["checkpoint_age_seconds"] is not None
            and metrics["checkpoint_age_seconds"] > self.rules.checkpoint_age_seconds
        ):
            add(
                "stalled-checkpoint",
                checkpoint["checkpoint_id"],
                "Барьер не завершён за заданное время.",
            )
        if (
            running
            and metrics["recovery_age_seconds"] is not None
            and metrics["recovery_age_seconds"] > self.rules.recovery_age_seconds
        ):
            add(
                "stalled-recovery",
                "recovery",
                "Восстановление превысило порог наблюдения.",
            )
        if running and native["configured"] and not native["available"]:
            add(
                "prefect-unavailable",
                "orchestrator",
                "Состояние Prefect временно недоступно.",
            )
        if running and native.get("flow_state_type") in ("FAILED", "CRASHED"):
            add(
                "failed-execution",
                "orchestrator",
                "Исполнение прервано; поток требует явного переподключения.",
                "error",
            )
        return alerts

    async def close(self):
        if self.prefect_reader is not None:
            await self.prefect_reader.close()
        if self._owns_telemetry:
            self.telemetry.close()


def prometheus(snapshot: dict[str, Any]) -> str:
    """Per-run prototype scrape; archive/remove completed run targets externally.

    Run/node labels are bounded inside one graph, not across unlimited run IDs.
    Source positions are cumulative DATA coordinates, not physical retained rows.
    """

    def labels(**values):
        def escaped(value):
            return (
                str(value)
                .replace("\\", "\\\\")
                .replace('"', '\\"')
                .replace("\n", "\\n")
            )

        return (
            "{"
            + ",".join(f'{key}="{escaped(value)}"' for key, value in values.items())
            + "}"
        )

    run_id = snapshot["control"]["run_id"]
    lines = ["# Projected gauges; native cursor authority is unchanged."]
    for row in snapshot["metrics"]["inputs"]:
        tags = labels(
            run_id=run_id,
            node=row["node"],
            channel=row["channel"],
            partition=row["partition_id"],
        )
        for name, value in (
            ("input_lag_records", row["lag_records"]),
            ("input_completed", row["completed"]),
            ("worker_in_flight", int(row["in_flight"])),
            ("owner_epoch", row["owner_epoch"]),
        ):
            lines.append(f"ariadne_stream_{name}{tags} {value}")
    for name, key in (
        ("source_admission_records", "used_records"),
        ("source_admission_bytes", "used_bytes"),
    ):
        lines.append(
            f"ariadne_stream_{name}{labels(run_id=run_id)} {snapshot['metrics']['admission'][key]}"
        )
    for key in ("checkpoint_age_seconds", "recovery_age_seconds"):
        if snapshot["metrics"][key] is not None:
            lines.append(
                f"ariadne_stream_{key}{labels(run_id=run_id)} {snapshot['metrics'][key]}"
            )
    lines.append(
        f"ariadne_stream_alerts{labels(run_id=run_id)} {len(snapshot['alerts'])}"
    )
    return "\n".join(lines) + "\n"
