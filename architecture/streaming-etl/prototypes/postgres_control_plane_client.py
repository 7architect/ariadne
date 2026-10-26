"""Async socket client for the isolated streaming orchestration API."""

from __future__ import annotations

import base64
from collections.abc import Callable
from typing import Any, Self
from urllib.parse import quote

import httpx


class ControlHttpError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        reason: str,
        *,
        code: str | None = None,
        payload: dict[str, Any] | None = None,
    ):
        self.status_code = status_code
        self.code = code
        self.payload = payload
        super().__init__(f"control API {status_code}: {reason}")


class ControlHttpClient:
    def __init__(
        self,
        endpoint: str,
        token: str,
        *,
        timeout: float = 10,
        trace_headers_provider: Callable[[], dict[str, str]] | None = None,
    ):
        if not token or timeout <= 0:
            raise ValueError("token and positive timeout required")
        self._client = httpx.AsyncClient(
            base_url=endpoint.rstrip("/"),
            headers={"Authorization": "Bearer " + token},
            timeout=timeout,
            follow_redirects=False,
        )
        self._trace_headers_provider = trace_headers_provider

    async def __aenter__(self) -> Self:
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *args) -> None:
        await self._client.__aexit__(*args)

    async def request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        leader_id: str | None = None,
        leader_epoch: int | None = None,
    ) -> dict[str, Any]:
        headers = (
            {}
            if self._trace_headers_provider is None
            else {
                key: value
                for key, value in self._trace_headers_provider().items()
                if key.lower() in {"traceparent", "tracestate"}
            }
        )
        if leader_id is not None:
            if leader_epoch is None or leader_epoch <= 0:
                raise ValueError("positive leader epoch required")
            headers.update(
                {
                    "X-Leader-Id": leader_id,
                    "X-Leader-Epoch": str(leader_epoch),
                }
            )
        response = await self._client.request(method, path, json=body, headers=headers)
        try:
            result = response.json()
        except ValueError as exc:
            raise ControlHttpError(
                response.status_code, "invalid JSON response"
            ) from exc
        if not response.is_success:
            reason = (
                result.get("error", "request rejected")
                if isinstance(result, dict)
                else "request rejected"
            )
            raise ControlHttpError(
                response.status_code,
                str(reason),
                code=result.get("error_type") if isinstance(result, dict) else None,
                payload=result if isinstance(result, dict) else None,
            )
        if not isinstance(result, dict):
            raise ControlHttpError(response.status_code, "JSON object required")
        return result

    @staticmethod
    def _run_path(run_id: str) -> str:
        return "/v1/runs/" + quote(run_id, safe="")

    async def deploy(self, **identity) -> dict[str, Any]:
        return await self.request("POST", "/v1/deployments", identity)

    async def claim_leader(
        self, deployment_id: str, leader_id: str, lease_seconds: float
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            "/internal/v1/leaders/claim",
            {
                "deployment_id": deployment_id,
                "leader_id": leader_id,
                "lease_seconds": lease_seconds,
            },
        )

    async def create_run(
        self, *, leader_id: str, leader_epoch: int, **identity
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            "/v1/runs",
            identity,
            leader_id=leader_id,
            leader_epoch=leader_epoch,
        )

    async def project(self, run_id: str) -> dict[str, Any]:
        return await self.request("GET", self._run_path(run_id))

    async def command(
        self,
        run_id: str,
        command: str,
        request_id: str,
        *,
        leader_id: str,
        leader_epoch: int,
    ) -> dict[str, Any]:
        if command not in {"drain", "cancel"}:
            raise ValueError("unknown lifecycle command")
        return await self.request(
            "POST",
            self._run_path(run_id) + "/" + command,
            {"request_id": request_id},
            leader_id=leader_id,
            leader_epoch=leader_epoch,
        )

    async def reconcile(
        self, run_id: str, *, leader_id: str, leader_epoch: int
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            self._run_path(run_id) + "/reconcile",
            {},
            leader_id=leader_id,
            leader_epoch=leader_epoch,
        )

    async def admit_source(self, run_id: str, **identity) -> dict[str, Any]:
        return await self.request(
            "POST", self._run_path(run_id) + "/admissions", identity
        )

    async def publish_source(
        self, run_id: str, admission_id: str, payload: bytes
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            self._run_path(run_id)
            + "/admissions/"
            + quote(admission_id, safe="")
            + "/publish",
            {"payload_b64": base64.b64encode(payload).decode("ascii")},
        )

    async def retire_admissions(
        self, run_id: str, *, leader_id: str, leader_epoch: int
    ) -> dict[str, Any]:
        return await self.request(
            "POST",
            self._run_path(run_id) + "/admissions/retire",
            {},
            leader_id=leader_id,
            leader_epoch=leader_epoch,
        )
