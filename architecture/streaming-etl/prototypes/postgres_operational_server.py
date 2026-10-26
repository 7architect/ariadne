"""Authenticated operational view over stream and native Prefect authority."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from dataclasses import dataclass
from pathlib import Path

from postgres_control_plane import PostgresControlPlane
from postgres_control_plane_server import JsonHttpServer
from postgres_prefect_attempts import PostgresPrefectAttempts
from postgres_stream_observability import (
    PrefectStateReader,
    StreamObservability,
    prometheus,
)
from postgres_stream_v2 import PostgresStream

HERE = Path(__file__).resolve().parent


@dataclass(frozen=True)
class RawBody:
    body: bytes
    content_type: str


class OperationalHttpServer(JsonHttpServer):
    def __init__(self, service: StreamObservability):
        super().__init__(service.control)
        self.service = service

    def _encode_payload(self, payload):
        if isinstance(payload, RawBody):
            return payload.body, payload.content_type
        return super()._encode_payload(payload)

    async def _dispatch(self, method, path, headers, body):
        if method == "GET" and path in ("/dashboard", "/runbook"):
            file = (
                "stream_dashboard.html"
                if path == "/dashboard"
                else "stream_runbook.html"
            )
            return 200, RawBody((HERE / file).read_bytes(), "text/html; charset=utf-8")
        parts = [part for part in path.split("/") if part]
        if method == "GET" and len(parts) == 4 and parts[:2] == ["v1", "runs"]:
            run_id = parts[2]
            if parts[3] in ("observability", "metrics"):
                token = self._token(headers)
                with self.service.telemetry.span(
                    "stream.http.observe", {"stream.run_id": run_id}, carrier=headers
                ):
                    sampled = await self.service.sample(token, run_id)
                if parts[3] == "metrics":
                    return 200, RawBody(
                        prometheus(sampled).encode(),
                        "text/plain; version=0.0.4; charset=utf-8",
                    )
                return 200, sampled
            if parts[3] == "traces":
                await self.control.project_run(self._token(headers), run_id=run_id)
                return 200, {
                    "spans": self.service.telemetry.exporter.for_run(run_id),
                    "events": self.service.telemetry.events_for_run(run_id),
                }
        return await super()._dispatch(method, path, headers, body)

    async def close(self):
        await super().close()
        await self.service.close()


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file", type=Path)
    parser.add_argument("--prefect-api-url")
    args = parser.parse_args()
    stream = await PostgresStream.connect_existing(args.dsn, args.schema)
    control = PostgresControlPlane.connect_existing(stream)
    async with stream.pool.acquire() as conn:
        has_attempts = await conn.fetchval(
            "SELECT to_regclass('prefect_attempt_heads_v1') IS NOT NULL"
        )
    attempts = PostgresPrefectAttempts(control) if has_attempts else None
    api_key = os.environ.get("ARIADNE_PREFECT_API_KEY")
    reader = (
        None
        if args.prefect_api_url is None
        else PrefectStateReader(
            args.prefect_api_url,
            headers=None if api_key is None else {"Authorization": "Bearer " + api_key},
        )
    )
    server = OperationalHttpServer(
        StreamObservability(control, attempts=attempts, prefect_reader=reader)
    )
    port = await server.start(args.host, args.port)
    if args.port_file is not None:
        args.port_file.write_text(str(port))
    print(
        json.dumps({"port": port, "dashboard": f"http://{args.host}:{port}/dashboard"}),
        flush=True,
    )
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    await stop.wait()
    await server.close()
    await stream.close()


if __name__ == "__main__":
    asyncio.run(main())
