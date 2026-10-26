from __future__ import annotations

import json
import unittest
from urllib.parse import quote

import httpx
import test_postgres_control_plane as fixtures
from postgres_operational_server import OperationalHttpServer
from postgres_stream_observability import AlertRules, StreamObservability
from postgres_stream_telemetry import StreamTelemetry, trace_headers
from postgres_stream_v2 import metadata_operation_id


class PostgresStreamOperationsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.PostgresControlPlaneTests("runTest")
        await self.fixture.asyncSetUp()
        self.telemetry = StreamTelemetry(capacity=8)
        self.service = StreamObservability(
            self.fixture.control,
            rules=AlertRules(lag_records=1),
            telemetry=self.telemetry,
        )
        self.server = OperationalHttpServer(self.service)
        self.port = await self.server.start()
        self.client = httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{self.port}", timeout=5
        )
        self.path = "/v1/runs/" + quote(self.fixture.run_id, safe="")
        self.headers = {"Authorization": "Bearer " + self.fixture.read_token}

    async def asyncTearDown(self):
        if hasattr(self, "client"):
            await self.client.aclose()
            await self.server.close()
            self.telemetry.close()
        await self.fixture.asyncTearDown()

    async def get(self, suffix="observability"):
        response = await self.client.get(self.path + "/" + suffix, headers=self.headers)
        self.assertEqual(response.status_code, 200, response.text)
        return response

    async def test_authenticated_readonly_metrics_and_tenant_isolation(self):
        f = self.fixture
        await f._admit_append(1)
        before = await f.control.project_run(f.token, run_id=f.run_id)
        sample = (await self.get()).json()
        self.assertEqual(sample["metrics"]["source_records"], 1)
        self.assertEqual(sample["metrics"]["inputs"][0]["lag_records"], 1)
        self.assertFalse(sample["prefect"]["configured"])
        self.assertEqual(
            {row["code"] for row in sample["alerts"]},
            {"input-lag", "missing-owner"},
        )
        for token, expected in ((None, 401), (f.other_token, 404)):
            for suffix in ("observability", "metrics", "traces"):
                response = await self.client.get(
                    self.path + "/" + suffix,
                    headers={}
                    if token is None
                    else {"Authorization": "Bearer " + token},
                )
                self.assertEqual(response.status_code, expected, response.text)
                self.assertNotIn(f.run_id, response.text)
        metrics = await self.get("metrics")
        self.assertIn("text/plain", metrics.headers["content-type"])
        self.assertIn("ariadne_stream_input_lag_records", metrics.text)
        self.assertIn('node="legacy:anna"', metrics.text)
        self.assertEqual(metrics.headers["cache-control"], "no-store")
        after = await f.control.project_run(f.token, run_id=f.run_id)
        self.assertEqual(before, after)
        await f._assert_oracle(after)

    async def test_alert_runbook_capacity_drill_retires_only_safe_history(self):
        f = self.fixture
        await f._admit_append(1)
        await f._admit_append(2)
        self.assertIn(
            "admission-capacity",
            {row["code"] for row in (await self.get()).json()["alerts"]},
        )
        leader_headers = {
            "Authorization": "Bearer " + f.token,
            "X-Leader-Id": f.leader.leader_id,
            "X-Leader-Epoch": str(f.leader.leader_epoch),
        }
        blocked = await self.client.post(
            self.path + "/admissions/retire", headers=leader_headers, json={}
        )
        self.assertEqual(blocked.status_code, 200, blocked.text)
        self.assertEqual(blocked.json()["retired_records"], 0)
        await f._complete_one()
        self.assertEqual((await f.stream.compact("input", "p0", 1))["safe_through"], 1)
        cuts = [("input", "p0", 1)]
        await f.stream.compact_metadata(cuts, operation_id=metadata_operation_id(cuts))
        retired = await self.client.post(
            self.path + "/admissions/retire", headers=leader_headers, json={}
        )
        self.assertEqual(retired.status_code, 200, retired.text)
        self.assertEqual(retired.json()["retired_records"], 1)
        recovered = (await self.get()).json()
        self.assertNotIn(
            "admission-capacity", {row["code"] for row in recovered["alerts"]}
        )
        self.assertEqual(recovered["metrics"]["inputs"][0]["completed"], 1)
        await f._admit_append(3)
        self.assertEqual((await self.get()).json()["metrics"]["source_records"], 3)
        await f._assert_oracle()

    async def test_w3c_trace_correlation_secret_filter_and_api_restart(self):
        f = self.fixture
        with self.telemetry.span("operator.turn", {"stream.run_id": f.run_id}):
            self.telemetry.event(
                "yield.observed",
                {
                    "stream.run_id": f.run_id,
                    "Authorization": "secret-token",
                    "payload": "secret-body",
                },
            )
            carrier = trace_headers()
            response = await self.client.get(
                self.path + "/observability", headers={**self.headers, **carrier}
            )
            self.assertEqual(response.status_code, 200, response.text)
        trace = (await self.get("traces")).json()
        spans = {row["name"]: row for row in trace["spans"]}
        self.assertEqual(len({row["trace_id"] for row in spans.values()}), 1)
        self.assertEqual(
            spans["stream.http.observe"]["parent_span_id"],
            spans["operator.turn"]["span_id"],
        )
        self.assertEqual(
            spans["stream.observe"]["parent_span_id"],
            spans["stream.http.observe"]["span_id"],
        )
        self.assertEqual(
            trace["events"][0]["trace_id"], spans["operator.turn"]["trace_id"]
        )
        self.assertNotIn("secret", json.dumps(trace))
        # API lifecycle does not close a telemetry provider owned by the worker.
        await self.server.close()
        service = StreamObservability(f.control, telemetry=self.telemetry)
        self.server = OperationalHttpServer(service)
        port = await self.server.start()
        response = await self.client.get(
            f"http://127.0.0.1:{port}" + self.path + "/observability",
            headers=self.headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        with self.telemetry.span("after.api.restart", {"stream.run_id": f.run_id}):
            pass
        self.assertIn(
            "after.api.restart",
            {row["name"] for row in self.telemetry.exporter.for_run(f.run_id)},
        )
        for i in range(20):
            with self.telemetry.span("bounded", {"stream.run_id": f.run_id}):
                self.telemetry.event(
                    "bounded", {"stream.run_id": f.run_id, "stream.input.sequence": i}
                )
        self.assertEqual(len(self.telemetry.exporter.for_run(f.run_id)), 8)
        self.assertEqual(len(self.telemetry.events_for_run(f.run_id)), 8)

    async def test_public_static_runbook_has_every_alert_action(self):
        dashboard = await self.client.get("/dashboard")
        runbook = await self.client.get("/runbook")
        for response in (dashboard, runbook):
            self.assertEqual(response.status_code, 200)
            self.assertIn("text/html", response.headers["content-type"])
            self.assertNotIn(self.fixture.token, response.text)
            self.assertNotIn(self.fixture.run_id, response.text)
        for alert in (
            "admission-capacity",
            "input-lag",
            "missing-owner",
            "stalled-checkpoint",
            "stalled-recovery",
            "prefect-unavailable",
            "failed-execution",
            "invalid-progress",
        ):
            self.assertIn('id="' + alert + '"', runbook.text)
        self.assertNotIn("localStorage", dashboard.text)
        self.assertNotIn("innerHTML", dashboard.text)


if __name__ == "__main__":
    unittest.main()
