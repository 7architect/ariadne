from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import cli as soak_cli  # noqa: E402 -- also support direct unittest discovery here.
import review as soak_review  # noqa: E402


class LongSoakReviewerTests(unittest.TestCase):
    def fixture(self, *, missing_oracle: bool = False) -> tuple[Path, str]:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        run_id = "smoke-review"
        directory = root / run_id
        (directory / "segments").mkdir(parents=True)
        now = datetime.now(UTC)
        source_hashes = soak_review.source_hashes()
        core_hashes = soak_review.core_hashes()
        identity = soak_review.current_runner_identity()
        manifest = {
            "protocol": "ariadne-long-soak/v1",
            "run_id": run_id,
            "git_commit": "local",
            "git_provenance": {"worktree_clean": False, "status_sha256": "local"},
            "source_sha256": source_hashes,
            "source_manifest_digest": soak_review.digest(source_hashes),
            "core_source_sha256": core_hashes,
            "runner_identity": identity,
            "runner_identity_digest": soak_review.digest(identity),
            "dependencies": {
                "python": "3.12",
                "uv": "0.12.23",
                "asyncpg": "0.30.0",
                "cryptography": "45.0.7",
                "python_image": soak_review.PYTHON_IMAGE,
                "uv_image": soak_review.UV_IMAGE,
                "postgres_image": soak_review.POSTGRES_IMAGE,
            },
            "target_seconds": 2,
            "segment_seconds": 1,
            "segment_count": 1,
            "smoke": True,
            "fault_schedule": {
                "owner_loss_every_records": 101,
                "process_loss_segments": [],
            },
        }
        (directory / "manifest.json").write_text(json.dumps(manifest))
        states = soak_review.expected_states(2)
        status = {
            "run_id": run_id,
            "phase": "TARGET_REACHED",
            "elapsed_seconds": 2.1,
            "run": {
                "source_completed": 2,
                "expected_d_digest": states["D"]["rolling_digest"],
                "last_checkpoint_source": 2,
                "owner_faults": 0,
                "process_boots": 1,
                "completed_at": now.isoformat(),
            },
            "states": states,
        }
        (directory / "service-status.json").write_text(json.dumps(status))
        oracles = {
            name: {"passed": True, "returncode": 0}
            for name in ("stream", "barrier", "join", "release")
        }
        if missing_oracle:
            oracles.pop("release")
        sample = {
            "elapsed_seconds": 2.1,
            "source_completed": 2,
            "container_restart_count": 0,
            "database": {
                "schema_relation_bytes": 1000,
                "synchronous_commit": "remote_apply",
                "release_session_remaining_seconds": 299,
                "replicas": [{"state": "streaming", "sync_state": "sync"}],
            },
            "pressure": [
                {
                    "channel": "soak",
                    "partition_id": "source",
                    "used_records": 1,
                    "max_records": 10,
                    "used_bytes": 10,
                    "max_bytes": 100,
                }
            ],
            "metrics": {"primary_data_bytes": 1000, "service_rss_bytes": 1000},
        }
        segment = {
            "run_id": run_id,
            "segment": 0,
            "started_at": (now - timedelta(seconds=1)).isoformat(),
            "ended_at": now.isoformat(),
            "duration_seconds": 1,
            "manifest_digest": soak_review.digest(manifest),
            "previous_segment_digest": None,
            "process_loss_injected": False,
            "passed": not missing_oracle,
            "oracles": oracles,
            "samples": [sample],
        }
        (directory / "segments" / "segment-00.json").write_text(json.dumps(segment))
        return root, run_id

    def test_smoke_passes_without_claiming_long_duration(self):
        root, run_id = self.fixture()
        report = soak_review.review(root, run_id)
        self.assertTrue(report["smoke_passed"])
        self.assertFalse(report["long_soak_passed"])
        self.assertEqual(report["violations"], [])

    def test_missing_independent_oracle_fails(self):
        root, run_id = self.fixture(missing_oracle=True)
        report = soak_review.review(root, run_id)
        self.assertFalse(report["smoke_passed"])
        self.assertIn("oracle_set:0", report["violations"])

    def test_partial_public_export_excludes_credentials(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        run_id = "partial-export"
        run = root / run_id
        run.mkdir()
        (run / "manifest.json").write_text(
            json.dumps({"run_id": run_id, "segment_count": 18})
        )
        (run / "review.json").write_text('{"long_soak_passed":false}\n')
        (run / "credentials.json").write_text('{"password":"secret"}\n')
        output = root / "public"
        result = soak_cli.export_public(
            SimpleNamespace(root=root, run_id=run_id, output=output)
        )
        self.assertFalse(result["complete"])
        self.assertEqual(result["observed_segments"], 0)
        self.assertEqual(result["files"], ["manifest.json", "review.json"])
        self.assertFalse((output / "credentials.json").exists())

    def test_cleanup_removes_partial_bootstrap_without_manifest(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        run_id = "partial-cleanup"
        run = root / run_id
        run.mkdir()
        (run / "credentials.json").write_text('{"password":"secret"}\n')
        with patch.object(soak_cli, "docker") as docker:
            result = soak_cli.stop(
                SimpleNamespace(root=root, run_id=run_id, cleanup=True)
            )
        self.assertFalse(result["manifest_present"])
        self.assertFalse((run / "credentials.json").exists())
        self.assertGreaterEqual(docker.call_count, 7)

    def test_docker_failure_redacts_environment_sql_and_dsn(self):
        secrets = ("db-password", "replica-secret", "dsn-secret")
        stderr = (
            "POSTGRES_PASSWORD=db-password; "
            "CREATE ROLE r PASSWORD 'replica-secret'; "
            "postgresql://user:dsn-secret@db/streaming"
        )
        completed = subprocess.CompletedProcess([], 17, "", stderr)
        with (
            patch.object(soak_cli.subprocess, "run", return_value=completed),
            self.assertRaises(soak_cli.DockerCommandError) as captured,
        ):
            soak_cli.docker(
                "run",
                "-e",
                "POSTGRES_PASSWORD=db-password",
                "postgresql://user:dsn-secret@db/streaming",
            )
        message = str(captured.exception)
        for secret in secrets:
            self.assertNotIn(secret, message)
        self.assertIn("<redacted>", message)


if __name__ == "__main__":
    unittest.main()
