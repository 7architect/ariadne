from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

import review  # noqa: E402
import service  # noqa: E402
import test_review  # noqa: E402


class TargetLivenessTests(unittest.IsolatedAsyncioTestCase):
    async def setup_service(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        instance = service.LongSoakService(
            SimpleNamespace(
                state_dir=temporary.name,
                run_id="target-tail",
                records_per_second=1,
                target_seconds=1,
            )
        )
        instance._bootstrap = AsyncMock()
        instance._accepted_source_through = AsyncMock(return_value=1)
        instance._process = AsyncMock()
        instance._checkpoint = AsyncMock()
        connection = SimpleNamespace(execute=AsyncMock())
        acquire = Mock()
        acquire.return_value.__aenter__ = AsyncMock(return_value=connection)
        acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        instance.stream = SimpleNamespace(pool=SimpleNamespace(acquire=acquire))
        return instance

    async def test_target_tail_longer_than_two_leases_stays_live_without_new_data(self):
        instance = await self.setup_service()
        clock = {"now": 0.0, "lease_until": 300.0}
        observed = []

        async def renew():
            clock["lease_until"] = clock["now"] + 300

        async def snapshot():
            return {
                "elapsed_seconds": clock["now"] + 2,
                "run": {"source_completed": 1, "last_checkpoint_source": 1},
                "database": {
                    "release_session_remaining_seconds": clock["lease_until"]
                    - clock["now"]
                },
                "states": {"digest": "unchanged"},
            }

        async def tick(delay):
            clock["now"] += delay
            if clock["now"] >= 610:
                instance.stop.set()

        instance._renew = AsyncMock(side_effect=renew)
        instance._snapshot = AsyncMock(side_effect=snapshot)
        instance._write_status = observed.append
        with (
            patch.object(asyncio.get_running_loop(), "add_signal_handler"),
            patch.object(service.asyncio, "sleep", side_effect=tick),
        ):
            await instance.run()
        self.assertGreater(clock["now"], 600)
        self.assertGreater(len(observed), 1)
        self.assertGreater(instance._renew.await_count, 1)
        self.assertTrue(
            all(
                v["database"]["release_session_remaining_seconds"] > 0 for v in observed
            )
        )
        self.assertTrue(
            all(
                v["run"] == {"source_completed": 1, "last_checkpoint_source": 1}
                for v in observed
            )
        )
        self.assertTrue(all(v["states"] == {"digest": "unchanged"} for v in observed))
        instance._process.assert_not_awaited()
        instance._checkpoint.assert_not_awaited()

    async def test_target_renewal_failure_propagates(self):
        instance = await self.setup_service()
        instance._snapshot = AsyncMock(
            return_value={"elapsed_seconds": 2, "run": {"source_completed": 1}}
        )
        instance._renew = AsyncMock(side_effect=RuntimeError("authority unavailable"))
        instance._write_status = Mock()

        async def stop_after_one_idle(_delay):
            instance.stop.set()

        with (
            patch.object(asyncio.get_running_loop(), "add_signal_handler"),
            patch.object(service.asyncio, "sleep", side_effect=stop_after_one_idle),
        ):
            with self.assertRaisesRegex(RuntimeError, "authority unavailable"):
                await instance.run()
        instance._write_status.assert_not_called()

    async def test_reviewer_still_rejects_expired_lease_at_target(self):
        root, run_id = test_review.LongSoakReviewerTests.fixture(self)
        path = root / run_id / "segments/segment-00.json"
        segment = json.loads(path.read_text())
        segment["samples"][0]["database"]["release_session_remaining_seconds"] = -1
        path.write_text(json.dumps(segment))
        report = review.review(root, run_id)
        self.assertFalse(report["smoke_passed"])
        self.assertIn("release_session_expired:0", report["violations"])


if __name__ == "__main__":
    unittest.main()
