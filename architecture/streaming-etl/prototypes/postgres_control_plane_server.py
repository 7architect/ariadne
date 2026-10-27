"""Small stdlib HTTP server for the isolated control-plane prototype."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import json
import os
import signal
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import asyncpg
from postgres_control_plane import (
    AuthenticationFailed,
    AuthorizationFailed,
    ControlConflict,
    ControlNotFound,
    PostgresControlPlane,
    StaleLeader,
)
from postgres_stream_v2 import PostgresProtocolError, PostgresStream


class JsonHttpServer:
    def __init__(
        self, control: PostgresControlPlane, *, allow_crash_fault: bool = False
    ):
        self.control = control
        self.allow_crash_fault = allow_crash_fault
        self._server: asyncio.Server | None = None

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> int:
        self._server = await asyncio.start_server(self._handle, host, port)
        return int(self._server.sockets[0].getsockname()[1])

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def serve_forever(self) -> None:
        if self._server is None:
            raise RuntimeError("server is not started")
        async with self._server:
            await self._server.serve_forever()

    @staticmethod
    def _json_default(value: Any) -> str:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        raise TypeError(f"not JSON serializable: {type(value).__name__}")

    @staticmethod
    def _token(headers: dict[str, str]) -> str:
        authorization = headers.get("authorization", "")
        if not authorization.startswith("Bearer "):
            raise AuthenticationFailed("bearer token is required")
        return authorization.removeprefix("Bearer ")

    @staticmethod
    def _leader(headers: dict[str, str]) -> tuple[str, int]:
        try:
            return headers["x-leader-id"], int(headers["x-leader-epoch"])
        except (KeyError, ValueError) as exc:
            raise ValueError("leader headers are required") from exc

    async def _read_request(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str, dict[str, str], dict[str, Any]]:
        head = await reader.readuntil(b"\r\n\r\n")
        if len(head) > 64 * 1024:
            raise ValueError("HTTP headers too large")
        lines = head.decode("ascii").split("\r\n")
        method, target, version = lines[0].split(" ", 2)
        if version != "HTTP/1.1":
            raise ValueError("HTTP/1.1 required")
        headers = {}
        for line in lines[1:]:
            if not line:
                continue
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()
        length = int(headers.get("content-length", "0"))
        if length < 0 or length > 1024 * 1024:
            raise ValueError("invalid content length")
        raw = await reader.readexactly(length) if length else b""
        body = json.loads(raw) if raw else {}
        if not isinstance(body, dict):
            raise TypeError("JSON object required")
        return method, unquote(urlsplit(target).path), headers, body

    async def _dispatch(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        body: dict[str, Any],
    ) -> tuple[int, Any]:
        token = self._token(headers)
        parts = [part for part in path.split("/") if part]
        if method == "POST" and parts == ["v1", "deployments"]:
            return 201, await self.control.register_deployment(token, **body)
        if method == "POST" and parts == ["internal", "v1", "leaders", "claim"]:
            lease = await self.control.claim_leader(token, **body)
            return 200, {
                "deployment_id": lease.deployment_id,
                "leader_id": lease.leader_id,
                "leader_epoch": lease.leader_epoch,
            }
        if method == "POST" and parts == ["v1", "runs"]:
            leader_id, leader_epoch = self._leader(headers)
            return 201, await self.control.create_run(
                token, leader_id=leader_id, leader_epoch=leader_epoch, **body
            )
        if len(parts) >= 3 and parts[:2] == ["v1", "runs"]:
            run_id = parts[2]
            if method == "GET" and len(parts) == 3:
                return 200, await self.control.project_run(token, run_id=run_id)
            if method == "POST" and parts[3:] == ["admissions"]:
                return 201, await self.control.admit_source(
                    token, run_id=run_id, **body
                )
            if (
                method == "POST"
                and len(parts) == 6
                and parts[3] == "admissions"
                and parts[5] == "publish"
            ):
                if set(body) != {"payload_b64"} or not isinstance(
                    body["payload_b64"], str
                ):
                    raise ValueError("payload_b64 is required")
                try:
                    payload = base64.b64decode(body["payload_b64"], validate=True)
                except (binascii.Error, ValueError) as exc:
                    raise ValueError("payload_b64 is invalid") from exc
                return 200, await self.control.publish_admission(
                    token,
                    run_id=run_id,
                    admission_id=parts[4],
                    payload=payload,
                )
            if method == "POST" and parts[3:] in (["drain"], ["cancel"]):
                leader_id, leader_epoch = self._leader(headers)
                operation = (
                    self.control.drain_run
                    if parts[3] == "drain"
                    else self.control.cancel_run
                )
                return 200, await operation(
                    token,
                    run_id=run_id,
                    leader_id=leader_id,
                    leader_epoch=leader_epoch,
                    **body,
                )
            if method == "POST" and parts[3:] == ["reconcile"]:
                leader_id, leader_epoch = self._leader(headers)
                return 200, await self.control.reconcile_run(
                    token,
                    run_id=run_id,
                    leader_id=leader_id,
                    leader_epoch=leader_epoch,
                )
            if method == "POST" and parts[3:] == ["admissions", "retire"]:
                leader_id, leader_epoch = self._leader(headers)
                return 200, await self.control.retire_admissions(
                    token,
                    run_id=run_id,
                    leader_id=leader_id,
                    leader_epoch=leader_epoch,
                )
        raise ControlNotFound("route not found")

    def _encode_payload(self, payload: Any) -> tuple[bytes, str]:
        return (
            json.dumps(payload, sort_keys=True, default=self._json_default).encode(),
            "application/json",
        )

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        status = 500
        payload: Any = {"error": "internal server error"}
        try:
            method, path, headers, body = await self._read_request(reader)
            if headers.get("x-crash-after-commit") and not self.allow_crash_fault:
                raise AuthorizationFailed("fault injection is disabled")
            status, payload = await self._dispatch(method, path, headers, body)
            if self.allow_crash_fault and headers.get("x-crash-after-commit") == "86":
                os._exit(86)
        except AuthenticationFailed as exc:
            status, payload = 401, {"error": str(exc), "error_type": type(exc).__name__}
        except AuthorizationFailed as exc:
            status, payload = 403, {"error": str(exc), "error_type": type(exc).__name__}
        except ControlNotFound as exc:
            status, payload = 404, {"error": str(exc), "error_type": type(exc).__name__}
        except (ControlConflict, StaleLeader) as exc:
            status, payload = 409, {"error": str(exc), "error_type": type(exc).__name__}
        except asyncpg.DeadlockDetectedError:
            status, payload = (
                409,
                {
                    "error": "transient database serialization conflict; retry exact request",
                    "error_type": "RetryableTransactionConflict",
                },
            )
        except (
            TypeError,
            ValueError,
            json.JSONDecodeError,
            asyncio.IncompleteReadError,
        ) as exc:
            status, payload = 400, {"error": str(exc), "error_type": type(exc).__name__}
        except PostgresProtocolError as exc:
            status, payload = 409, {"error": str(exc), "error_type": type(exc).__name__}
        except Exception as exc:  # noqa: BLE001  # pragma: no cover - total HTTP boundary
            status, payload = (
                500,
                {"error": type(exc).__name__, "error_type": type(exc).__name__},
            )
        reason = {
            200: "OK",
            201: "Created",
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            409: "Conflict",
            500: "Internal Server Error",
        }[status]
        encoded, content_type = self._encode_payload(payload)
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n".encode()
            + f"Content-Type: {content_type}\r\n".encode()
            + f"Content-Length: {len(encoded)}\r\n".encode()
            + b"Cache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\n"
            + b"Connection: close\r\n\r\n"
            + encoded
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--port-file", type=Path)
    parser.add_argument("--allow-crash-fault", action="store_true")
    args = parser.parse_args()
    stream = await PostgresStream.connect_existing(args.dsn, args.schema)
    server = JsonHttpServer(
        PostgresControlPlane.connect_existing(stream),
        allow_crash_fault=args.allow_crash_fault,
    )
    port = await server.start(args.host, args.port)
    if args.port_file is not None:
        args.port_file.write_text(str(port))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop.set)
    await stop.wait()
    await server.close()
    await stream.close()


if __name__ == "__main__":
    asyncio.run(_main())
