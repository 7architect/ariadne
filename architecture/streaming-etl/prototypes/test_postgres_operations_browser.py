"""Real Chromium checks of the operator console against actual HTTP and SQL."""

from __future__ import annotations

import asyncio
import base64
import errno
import json
import os
import shutil
import signal
import tempfile
import unittest
from pathlib import Path

import httpx
import test_postgres_stream_operations as fixtures
import websockets


class PostgresOperationsBrowserTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.fixture = fixtures.PostgresStreamOperationsTests("runTest")
        await self.fixture.asyncSetUp()
        self.profile = tempfile.TemporaryDirectory(prefix="ariadne-operations-browser-")
        chromium = shutil.which("chromium")
        self.assertIsNotNone(chromium, "Chromium is required for this release profile")
        self.browser = await asyncio.create_subprocess_exec(
            chromium,
            "--headless=new",
            "--no-sandbox",
            "--disable-gpu",
            "--remote-debugging-port=0",
            "--user-data-dir=" + self.profile.name,
            "about:blank",
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,
        )
        active = Path(self.profile.name) / "DevToolsActivePort"
        for _ in range(200):
            if active.exists():
                break
            await asyncio.sleep(0.05)
        self.assertTrue(active.exists(), "Chromium did not start")
        port = int(active.read_text().splitlines()[0])
        async with httpx.AsyncClient() as client:
            targets = (await client.get(f"http://127.0.0.1:{port}/json/list")).json()
        target = next(row for row in targets if row["type"] == "page")
        self.ws = await websockets.connect(
            target["webSocketDebuggerUrl"], max_size=8_000_000
        )
        self.serial = 0
        self.errors = []
        await self.cdp("Runtime.enable")
        await self.cdp("Page.enable")

    async def asyncTearDown(self):
        try:
            if hasattr(self, "ws"):
                try:
                    # Graceful close drains browser profile writes and children.
                    await self.ws.send(
                        json.dumps({"id": self.serial + 1, "method": "Browser.close"})
                    )
                except websockets.ConnectionClosed:
                    pass
                finally:
                    await self.ws.close()
            if hasattr(self, "browser"):
                try:
                    await asyncio.wait_for(self.browser.wait(), 5)
                except TimeoutError:
                    os.killpg(self.browser.pid, signal.SIGKILL)
                    await self.browser.wait()
                # A separate process session confines cleanup to this browser,
                # including descendants that outlive its launcher.
                try:
                    os.killpg(self.browser.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                for attempt in range(40):
                    try:
                        self.profile.cleanup()
                        break
                    except OSError as exc:
                        if (
                            exc.errno not in (errno.ENOTEMPTY, errno.EBUSY)
                            or attempt == 39
                        ):
                            raise
                        await asyncio.sleep(0.05)
        finally:
            await self.fixture.asyncTearDown()

    async def cdp(self, method, params=None):
        self.serial += 1
        request = self.serial
        await self.ws.send(
            json.dumps({"id": request, "method": method, "params": params or {}})
        )
        while True:
            message = json.loads(await asyncio.wait_for(self.ws.recv(), 10))
            if message.get("method") == "Runtime.exceptionThrown":
                self.errors.append(message["params"]["exceptionDetails"]["text"])
            if message.get("id") == request:
                self.assertNotIn("error", message, message)
                return message["result"]

    async def js(self, expression):
        result = await self.cdp(
            "Runtime.evaluate",
            {"expression": expression, "returnByValue": True, "awaitPromise": True},
        )
        self.assertNotIn("exceptionDetails", result, result)
        return result["result"].get("value")

    async def until(self, expression):
        for _ in range(200):
            if await self.js(expression):
                return
            await asyncio.sleep(0.05)
        self.fail("Browser condition did not become true: " + expression)

    async def test_actual_browser_observation_drain_mobile_and_auth_cleanup(self):
        f = self.fixture.fixture
        await f._admit_append(1)
        await self.cdp(
            "Emulation.setDeviceMetricsOverride",
            {"width": 1280, "height": 1050, "deviceScaleFactor": 1, "mobile": False},
        )
        await self.cdp(
            "Page.navigate", {"url": f"http://127.0.0.1:{self.fixture.port}/dashboard"}
        )
        await self.until("Boolean(document.getElementById('connect'))")
        await self.js(
            "document.getElementById('run-id').value="
            + json.dumps(f.run_id)
            + ";document.getElementById('token').value="
            + json.dumps(f.token)
            + ";document.getElementById('connect').requestSubmit()"
        )
        await self.until("document.getElementById('received').textContent==='1'")
        self.assertEqual(
            await self.js("document.querySelectorAll('#nodes tr').length"), 1
        )
        self.assertIn(
            "Нет владельца",
            await self.js("document.getElementById('nodes').textContent"),
        )
        self.assertEqual(await self.js("localStorage.length+sessionStorage.length"), 0)
        screenshot = await self.cdp(
            "Page.captureScreenshot", {"format": "png", "captureBeyondViewport": False}
        )
        output = Path(
            os.environ.get(
                "ARIADNE_BROWSER_SCREENSHOT", "/tmp/ariadne-stream-dashboard.png"
            )
        )
        output.write_bytes(base64.b64decode(screenshot["data"]))
        # Editing the password field cannot change an established command session.
        await self.js(
            "document.getElementById('token').value='wrong-edited-token';document.getElementById('drain').click()"
        )
        await self.until(
            "document.getElementById('action-status').textContent.includes('Команда принята')"
        )
        projection = await f.control.project_run(f.token, run_id=f.run_id)
        self.assertEqual(projection["control"]["lifecycle"], "DRAINING")
        await f._complete_one()
        await f.control.reconcile_run(
            f.token,
            run_id=f.run_id,
            leader_id=f.leader.leader_id,
            leader_epoch=f.leader.leader_epoch,
        )
        await self.until(
            "document.getElementById('stream-state').textContent==='Поток закрыт'"
        )
        await self.cdp(
            "Emulation.setDeviceMetricsOverride",
            {"width": 390, "height": 844, "deviceScaleFactor": 1, "mobile": True},
        )
        self.assertTrue(
            await self.js("document.documentElement.scrollWidth<=window.innerWidth")
        )
        await self.js("document.getElementById('disconnect').click()")
        self.assertTrue(await self.js("document.getElementById('workspace').hidden"))
        self.assertEqual(
            await self.js("document.getElementById('stream-id').textContent"), ""
        )
        await self.js(
            "document.getElementById('token').value='invalid';document.getElementById('connect').requestSubmit()"
        )
        await self.until(
            "document.getElementById('connection').textContent.includes('Нет свежих данных')"
        )
        self.assertEqual(await self.js("document.getElementById('token').value"), "")
        self.assertTrue(await self.js("document.getElementById('workspace').hidden"))
        self.assertEqual(self.errors, [])
        await f._assert_oracle()


if __name__ == "__main__":
    unittest.main()
