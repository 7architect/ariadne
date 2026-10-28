from __future__ import annotations

import io
import json
import signal
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import arc_job  # noqa: E402
import collect_job  # noqa: E402


class ArcObservationTests(unittest.TestCase):
    def fixture(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        directory = root / "arc-test"
        directory.mkdir()
        (directory / "manifest.json").write_text(
            json.dumps({"segment_seconds": 14400, "smoke": False})
        )
        return SimpleNamespace(
            root=root,
            run_id="arc-test",
            segment=5,
            public_output=root / "public",
        )

    def test_insufficient_daemon_capacity_fails_and_removes_only_probe_volume(self):
        args = self.fixture()
        args.required_free_bytes = 20 * 1024**3
        with (
            patch.object(arc_job.secrets, "token_hex", return_value="test-nonce"),
            patch.object(
                arc_job.shutil,
                "disk_usage",
                return_value=SimpleNamespace(free=30 * 1024**3),
            ),
            patch.object(
                arc_job.cli, "docker", return_value=SimpleNamespace(stdout="1024\n")
            ) as docker,
        ):
            with self.assertRaisesRegex(RuntimeError, "insufficient free disk"):
                arc_job.preflight(args)
        self.assertEqual(
            docker.call_args.args, ("volume", "rm", "ariadne-soak-capacity-test-nonce")
        )

    def test_failed_oracle_preserves_public_export_and_stops_only_service(self):
        args = self.fixture()
        with (
            patch.object(arc_job.threading, "Thread"),
            patch.object(arc_job.cli, "watch", return_value={"passed": False}),
            patch.object(arc_job.cli, "export_public") as export,
            patch.object(arc_job.cli, "stop") as stop,
        ):
            with self.assertRaisesRegex(RuntimeError, "oracle failed"):
                arc_job.run_segment(args)
        self.assertEqual(export.call_args.args[0].output, args.public_output)
        self.assertFalse(stop.call_args.args[0].cleanup)
        self.assertEqual(args.duration, 14400)
        self.assertEqual(args.poll_seconds, 60)

    def test_signal_interruption_preserves_partial_export(self):
        args = self.fixture()
        with (
            patch.object(arc_job.threading, "Thread"),
            patch.object(arc_job.cli, "watch", side_effect=SystemExit(143)),
            patch.object(arc_job.cli, "export_public") as export,
            patch.object(arc_job.cli, "stop") as stop,
        ):
            with self.assertRaises(SystemExit):
                arc_job.run_segment(args)
        export.assert_called_once()
        stop.assert_called_once()
        with self.assertRaises(SystemExit) as raised:
            arc_job.interrupted(signal.SIGTERM, None)
        self.assertEqual(raised.exception.code, 143)

    def test_progress_excludes_arbitrary_status_and_credentials(self):
        args = self.fixture()
        directory = args.root / args.run_id
        (directory / "service-status.json").write_text(
            json.dumps(
                {
                    "elapsed_seconds": 123,
                    "error": "private error text",
                    "dsn": "private connection string",
                    "run": {
                        "source_completed": 10,
                        "process_boots": 2,
                        "owner_faults": 1,
                    },
                }
            )
        )
        receipt = arc_job.progress(directory, 5)
        self.assertEqual(receipt["source_completed"], 10)
        self.assertEqual(
            set(receipt),
            {
                "segment",
                "elapsed_seconds",
                "source_completed",
                "process_boots",
                "owner_faults",
            },
        )
        self.assertNotIn("private", json.dumps(receipt))


class FreshCollectorTests(unittest.TestCase):
    def test_signed_storage_download_receives_no_repository_token(self):
        redirected = urllib.error.HTTPError(
            "https://api.github.com/example",
            302,
            "Found",
            {"Location": "https://example.blob.core.windows.net/job?signature=private"},
            None,
        )
        opener = Mock()
        opener.open.return_value = io.BytesIO(b"job-log")
        with (
            patch.object(collect_job, "api", side_effect=redirected),
            patch.object(
                collect_job.urllib.request, "build_opener", return_value=opener
            ),
        ):
            self.assertEqual(collect_job.download_log("owner/repo", 1), "job-log")
        request = opener.open.call_args.args[0]
        self.assertFalse(request.has_header("Authorization"))

    def test_unexpected_redirect_is_rejected(self):
        error = urllib.error.HTTPError(
            "https://api.github.com/example",
            302,
            "Found",
            {"Location": "https://unrelated.example/log"},
            None,
        )
        with patch.object(collect_job, "api", side_effect=error):
            with self.assertRaisesRegex(ValueError, "unexpected"):
                collect_job.download_log("owner/repo", 1)

    def test_source_sha_mismatch_exports_a_failed_diagnostic(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary) / "public"
            with (
                patch.object(
                    sys, "argv", ["collect_job", "--output", str(destination)]
                ),
                patch.dict(
                    collect_job.os.environ,
                    {
                        "GITHUB_REPOSITORY": "owner/repo",
                        "GITHUB_RUN_ID": "123",
                        "GITHUB_RUN_ATTEMPT": "1",
                        "GITHUB_SHA": "a" * 40,
                    },
                ),
                patch.object(
                    collect_job, "source_job", return_value={"head_sha": "b" * 40}
                ),
                patch.object(collect_job, "download_log") as download,
            ):
                self.assertEqual(collect_job.main(), 2)
            self.assertFalse(
                json.loads((destination / "transport-review.json").read_text())[
                    "passed"
                ]
            )
            download.assert_not_called()

    def test_log_download_is_bounded(self):
        with self.assertRaisesRegex(ValueError, "exceeds"):
            collect_job.read_bounded(io.BytesIO(b"too-large"), limit=2)


if __name__ == "__main__":
    unittest.main()
