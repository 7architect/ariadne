from __future__ import annotations

import base64
import gzip
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import transport

RUN_ID = "123456789"
ATTEMPT = 2
SOURCE_SHA = "a" * 40
CAMPAIGN = f"gh-{RUN_ID}-{ATTEMPT}"


def json_bytes(value):
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()


def oracle_set():
    return {
        name: {"passed": True, "returncode": 0, "violations": []}
        for name in ("stream", "barrier", "join", "release")
    }


class EvidenceFixture:
    def __init__(self, root: Path, *, smoke: bool):
        self.root = root
        self.smoke = smoke
        self.segment_count = 1 if smoke else 18
        self.target_seconds = 45 if smoke else 72 * 60 * 60
        self.segment_seconds = 60 if smoke else 4 * 60 * 60
        self.losses = [0] if smoke else [5, 11]
        source_hashes = {"service.py": "b" * 64, "transport.py": "c" * 64}
        core_hashes = {"postgres_stream_v2.py": "d" * 64}
        runner = {
            "runner_name": "arc-runner",
            "hostname": "source-pod",
            "machine_id_sha256": "e" * 64,
        }
        self.manifest = {
            "protocol": "ariadne-long-soak/v1",
            "run_id": CAMPAIGN,
            "git_commit": SOURCE_SHA,
            "source_sha256": source_hashes,
            "source_manifest_digest": transport.legacy_digest(source_hashes),
            "core_source_sha256": core_hashes,
            "core_source_manifest_digest": transport.legacy_digest(core_hashes),
            "runner_identity": runner,
            "runner_identity_digest": transport.legacy_digest(runner),
            "target_seconds": self.target_seconds,
            "segment_seconds": self.segment_seconds,
            "segment_count": self.segment_count,
            "smoke": smoke,
            "fault_schedule": {
                "owner_loss_every_records": 3,
                "process_loss_segments": self.losses,
            },
        }
        self.root.mkdir(parents=True)
        (self.root / "segments").mkdir()
        (self.root / "manifest.json").write_bytes(json_bytes(self.manifest))
        self._write_segments_and_review()

    def _write_segments_and_review(self):
        previous = None
        start = datetime(2026, 1, 1, tzinfo=UTC)
        final_source = 0
        for index in range(self.segment_count):
            final_source = index + 1 if not self.smoke else 3
            segment = {
                "protocol": "ariadne-long-soak-segment/v1",
                "run_id": CAMPAIGN,
                "segment": index,
                "started_at": (
                    start + timedelta(seconds=index * self.segment_seconds)
                ).isoformat(),
                "ended_at": (
                    start + timedelta(seconds=(index + 1) * self.segment_seconds)
                ).isoformat(),
                "duration_seconds": self.segment_seconds,
                "manifest_digest": transport.legacy_digest(self.manifest),
                "previous_segment_digest": previous,
                "runner_identity_digest": self.manifest["runner_identity_digest"],
                "source_manifest_digest": self.manifest["source_manifest_digest"],
                "process_loss_injected": index in self.losses,
                "samples": [
                    {
                        "elapsed_seconds": (index + 1) * self.segment_seconds,
                        "source_completed": final_source,
                    }
                ],
                "oracles": oracle_set(),
                "passed": True,
            }
            (self.root / f"segments/segment-{index:02d}.json").write_bytes(
                json_bytes(segment)
            )
            previous = transport.legacy_digest(segment)
        review = {
            "protocol": "ariadne-long-soak-review/v1",
            "run_id": CAMPAIGN,
            "manifest_digest": transport.legacy_digest(self.manifest),
            "segment_count": self.segment_count,
            "elapsed_seconds": (
                65 if self.smoke else self.segment_count * self.segment_seconds
            ),
            "source_completed": final_source,
            "owner_faults": final_source // 3,
            "process_boots": 1 + len(self.losses),
            "violations": [],
            "smoke_passed": self.smoke,
            "long_soak_passed": not self.smoke,
            "resource_policy": {
                "profile": (
                    "smoke_absolute_caps"
                    if self.smoke
                    else "long_absolute_and_rate_caps"
                ),
                "absolute_caps_enforced": True,
                "growth_rate_caps_enforced": not self.smoke,
                "growth_rates_descriptive": self.smoke,
                "absolute_caps_bytes": {
                    "primary_data": 8 * 1024**3,
                    "schema_relation": 512 * 1024**2,
                    "service_rss": 768 * 1024**2,
                },
                "growth_rate_caps_bytes_per_hour": {
                    "primary_data": 32 * 1024**2,
                    "schema_relation": 8 * 1024**2,
                    "service_rss": 16 * 1024**2,
                },
            },
        }
        (self.root / "review.json").write_bytes(json_bytes(review))
        verdict = "SMOKE PASS" if self.smoke else "LONG SOAK PASS"
        passed_name = "smoke_passed" if self.smoke else "long_soak_passed"
        (self.root / "review.md").write_text(
            f"# Long-soak review: {verdict}\n\n"
            f"- Run: `{CAMPAIGN}`\n"
            f"- `{passed_name}`: `true`\n"
        )

    def emit_log(self, *, final: bool = True) -> str:
        output = io.StringIO()
        environment = {
            "GITHUB_RUN_ID": RUN_ID,
            "GITHUB_RUN_ATTEMPT": str(ATTEMPT),
            "GITHUB_SHA": SOURCE_SHA,
        }
        with patch.dict(os.environ, environment, clear=False), redirect_stdout(output):
            transport.emit(self.root, "bootstrap")
            for index in range(self.segment_count):
                transport.emit(self.root, "segment", index)
            if final:
                transport.emit(self.root, "final")
        return "".join(
            f"2026-01-01T00:00:00Z {line}\n" for line in output.getvalue().splitlines()
        )


def unsafe_frame(path: str, binding: transport.FrameBinding) -> str:
    plain = transport.canonical(
        {"protocol": transport.PAYLOAD_PROTOCOL, "files": {path: "secret"}}
    ).encode()
    compressed = gzip.compress(plain, compresslevel=9, mtime=0)
    compressed_sha = transport.sha256(compressed)
    begin = {
        "type": "BEGIN",
        "version": 1,
        "run_id": binding.run_id,
        "attempt": binding.attempt,
        "source_sha": binding.source_sha,
        "kind": binding.kind,
        "segment": binding.segment,
        "plain_size": len(plain),
        "compressed_size": len(compressed),
        "plain_sha256": transport.sha256(plain),
        "compressed_sha256": compressed_sha,
        "chunks": 1,
    }
    frame_id = transport.sha256(
        transport.canonical(
            {key: value for key, value in begin.items() if key != "type"}
        ).encode()
    )
    begin["frame_id"] = frame_id
    chunk = {
        "type": "CHUNK",
        "frame_id": frame_id,
        "index": 0,
        "chunks": 1,
        "chunk_sha256": compressed_sha,
        "data": base64.b64encode(compressed).decode(),
    }
    end = {
        "type": "END",
        "frame_id": frame_id,
        "chunks": 1,
        "compressed_sha256": compressed_sha,
    }
    return "\n".join(
        transport.PREFIX + transport.canonical(item) for item in (begin, chunk, end)
    )


class EvidenceTransportTests(unittest.TestCase):
    def fixture(self, *, smoke=False):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        return EvidenceFixture(Path(temporary.name) / "public", smoke=smoke)

    def test_full_eighteen_segment_campaign_round_trips_and_collects(self):
        fixture = self.fixture()
        log = fixture.emit_log()
        files = transport.decode_log(log, RUN_ID, ATTEMPT, SOURCE_SHA)
        self.assertEqual(len(files), 21)
        self.assertEqual(len(files.frames), 20)
        report = transport.validate_evidence(files, "success", CAMPAIGN, SOURCE_SHA)
        self.assertTrue(report["passed"], report["violations"])
        self.assertFalse(report["collector_live_review_performed"])
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        log_path = Path(temporary.name) / "job.log"
        output = Path(temporary.name) / "output"
        log_path.write_text(log)
        self.assertEqual(
            transport.collect(log_path, output, RUN_ID, ATTEMPT, SOURCE_SHA, "success"),
            0,
        )
        stored = json.loads((output / "transport-review.json").read_text())
        self.assertTrue(stored["passed"])
        self.assertTrue((output / "segments/segment-17.json").is_file())

    def test_smoke_campaign_uses_its_original_review_verdict(self):
        fixture = self.fixture(smoke=True)
        files = transport.decode_log(fixture.emit_log(), RUN_ID, ATTEMPT, SOURCE_SHA)
        report = transport.validate_evidence(files, "success", CAMPAIGN, SOURCE_SHA)
        self.assertTrue(report["passed"], report["violations"])
        review = json.loads(files["review.json"])
        self.assertTrue(review["smoke_passed"])
        self.assertFalse(review["long_soak_passed"])

    def test_missing_final_review_is_partial_and_fails(self):
        fixture = self.fixture()
        files = transport.decode_log(
            fixture.emit_log(final=False), RUN_ID, ATTEMPT, SOURCE_SHA
        )
        report = transport.validate_evidence(files, "success", CAMPAIGN, SOURCE_SHA)
        self.assertFalse(report["passed"])
        self.assertIn("missing:review.json", report["violations"])
        self.assertIn("missing:review.md", report["violations"])

    def test_weakened_numeric_resource_caps_fail_despite_embedded_pass(self):
        for smoke in (False, True):
            for group in ("absolute_caps_bytes", "growth_rate_caps_bytes_per_hour"):
                with self.subTest(smoke=smoke, group=group):
                    fixture = self.fixture(smoke=smoke)
                    path = fixture.root / "review.json"
                    review = json.loads(path.read_text())
                    review["resource_policy"][group]["primary_data"] *= 2
                    path.write_bytes(json_bytes(review))
                    files = transport.decode_log(
                        fixture.emit_log(), RUN_ID, ATTEMPT, SOURCE_SHA
                    )
                    report = transport.validate_evidence(
                        files, "success", CAMPAIGN, SOURCE_SHA
                    )
                    self.assertFalse(report["passed"])
                    self.assertIn("review_resource_thresholds", report["violations"])

    def test_failed_source_job_rejects_otherwise_complete_evidence(self):
        fixture = self.fixture()
        files = transport.decode_log(fixture.emit_log(), RUN_ID, ATTEMPT, SOURCE_SHA)
        report = transport.validate_evidence(files, "failure", CAMPAIGN, SOURCE_SHA)
        self.assertFalse(report["passed"])
        self.assertIn("source_job_not_successful", report["violations"])

    def test_wrong_run_binding_is_rejected(self):
        fixture = self.fixture(smoke=True)
        with self.assertRaises(transport.TransportError) as captured:
            transport.decode_log(fixture.emit_log(), "999999999", ATTEMPT, SOURCE_SHA)
        self.assertTrue(
            any("source binding" in item for item in captured.exception.violations)
        )
        self.assertEqual(captured.exception.partial_files, {})

    def test_corrupt_chunk_preserves_an_earlier_valid_frame(self):
        fixture = self.fixture(smoke=True)
        log_lines = fixture.emit_log().splitlines()
        chunk_lines = [
            index for index, line in enumerate(log_lines) if '"type":"CHUNK"' in line
        ]
        corrupt_index = chunk_lines[1]
        prefix, encoded = log_lines[corrupt_index].split(transport.PREFIX, 1)
        item = json.loads(encoded)
        item["data"] = "!" + item["data"][1:]
        log_lines[corrupt_index] = prefix + transport.PREFIX + transport.canonical(item)
        with self.assertRaises(transport.TransportError) as captured:
            transport.decode_log("\n".join(log_lines), RUN_ID, ATTEMPT, SOURCE_SHA)
        self.assertIn("manifest.json", captured.exception.partial_files)
        self.assertNotIn("segments/segment-00.json", captured.exception.partial_files)

    def test_path_traversal_payload_is_rejected(self):
        binding = transport.FrameBinding(RUN_ID, ATTEMPT, SOURCE_SHA, "bootstrap")
        with self.assertRaises(transport.TransportError) as captured:
            transport.decode_log(
                unsafe_frame("../credentials.json", binding),
                RUN_ID,
                ATTEMPT,
                SOURCE_SHA,
            )
        self.assertTrue(
            any("not allowed" in item for item in captured.exception.violations)
        )
        self.assertEqual(captured.exception.partial_files, {})

    def test_collect_preserves_valid_partial_on_malformed_later_frame(self):
        fixture = self.fixture(smoke=True)
        lines = fixture.emit_log().splitlines()
        end_index = next(
            index for index, line in enumerate(lines) if '"type":"END"' in line
        )
        malformed = "\n".join(lines[: end_index + 1] + [transport.PREFIX + "{"])
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        log_path = Path(temporary.name) / "job.log"
        output = Path(temporary.name) / "partial"
        log_path.write_text(malformed)
        exit_code = transport.collect(
            log_path, output, RUN_ID, ATTEMPT, SOURCE_SHA, "cancelled"
        )
        self.assertEqual(exit_code, 2)
        self.assertTrue((output / "manifest.json").is_file())
        self.assertFalse((output / "segments/segment-00.json").exists())
        report = json.loads((output / "transport-review.json").read_text())
        self.assertFalse(report["passed"])
        self.assertTrue(report["transport_violations"])

    def test_declared_plain_size_above_limit_is_rejected_without_allocation(self):
        fixture = self.fixture(smoke=True)
        lines = fixture.emit_log().splitlines()
        begin_index = next(
            index for index, line in enumerate(lines) if '"type":"BEGIN"' in line
        )
        prefix, encoded = lines[begin_index].split(transport.PREFIX, 1)
        item = json.loads(encoded)
        item["plain_size"] = transport.MAX_PLAIN_BYTES + 1
        lines[begin_index] = prefix + transport.PREFIX + transport.canonical(item)
        with self.assertRaises(transport.TransportError) as captured:
            transport.decode_log("\n".join(lines), RUN_ID, ATTEMPT, SOURCE_SHA)
        self.assertTrue(
            any("plain size" in item for item in captured.exception.violations)
        )


if __name__ == "__main__":
    unittest.main()
