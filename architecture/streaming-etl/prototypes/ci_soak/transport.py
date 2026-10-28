"""Credential-free evidence transport for a single-job ARC soak campaign.

The original runner remains the authority for the live review because a collector
runs with a different runner identity.  This module only frames the public
evidence, reconstructs it from the original job log, and validates its internal
bindings before a fresh collector publishes an artifact.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import gzip
import hashlib
import json
import os
import re
import sys
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

PREFIX = "ARIADNE_SOAK_EVIDENCE_V1:"
PAYLOAD_PROTOCOL = "ariadne-soak-evidence-payload/v1"
REVIEW_PROTOCOL = "ariadne-soak-transport-review/v1"
MAX_COMPRESSED_BYTES = 16 * 1024**2
MAX_PLAIN_BYTES = 128 * 1024**2
CHUNK_BYTES = 9 * 1024
MAX_CHUNKS = (MAX_COMPRESSED_BYTES + CHUNK_BYTES - 1) // CHUNK_BYTES
SHA_PATTERN = re.compile(r"[0-9a-f]{40,64}")
SEGMENT_PATTERN = re.compile(r"segments/segment-(0[0-9]|1[0-7])\.json")
BASE_FILES = frozenset({"manifest.json", "review.json", "review.md"})


def canonical(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def legacy_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_unique(value: str | bytes) -> Any:
    def object_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    return json.loads(value, object_pairs_hook=object_pairs)


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise ValueError(f"{label} shape is invalid")


def _checked_sha(value: str) -> str:
    normalized = value.lower()
    if SHA_PATTERN.fullmatch(normalized) is None:
        raise ValueError("source SHA must contain 40-64 hexadecimal characters")
    return normalized


def _allowed_path(path: str) -> bool:
    return path in BASE_FILES or SEGMENT_PATTERN.fullmatch(path) is not None


def _safe_output_path(root: Path, relative: str) -> Path:
    if not _allowed_path(relative):
        raise ValueError(f"evidence path is not allowed: {relative}")
    path = PurePosixPath(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"evidence path is unsafe: {relative}")
    return root.joinpath(*path.parts)


@dataclass(frozen=True)
class FrameBinding:
    run_id: str
    attempt: int
    source_sha: str
    kind: str
    segment: int | None = None

    def __post_init__(self) -> None:
        if not self.run_id or not self.run_id.isdecimal():
            raise ValueError("run ID must be decimal")
        if isinstance(self.attempt, bool) or self.attempt <= 0:
            raise ValueError("run attempt must be positive")
        object.__setattr__(self, "source_sha", _checked_sha(self.source_sha))
        if self.kind not in {"bootstrap", "segment", "final"}:
            raise ValueError("frame kind is invalid")
        if self.kind == "segment":
            if (
                isinstance(self.segment, bool)
                or not isinstance(self.segment, int)
                or not 0 <= self.segment <= 17
            ):
                raise ValueError("segment frame needs an index from 0 through 17")
        elif self.segment is not None:
            raise ValueError("only segment frames can carry a segment index")


class DecodedFiles(dict[str, bytes]):
    """Decoded files with accepted frame metadata for diagnostics."""

    def __init__(self) -> None:
        super().__init__()
        self.frames: list[dict[str, Any]] = []


class TransportError(ValueError):
    """A fail-closed transport error that retains independently valid frames."""

    def __init__(self, violations: list[str], partial_files: DecodedFiles):
        self.violations = sorted(set(violations))
        self.partial_files = partial_files
        super().__init__("; ".join(self.violations))


def _expected_files(binding: FrameBinding) -> tuple[set[str], bool]:
    if binding.kind == "bootstrap":
        return {"manifest.json"}, True
    if binding.kind == "segment":
        return {f"segments/segment-{binding.segment:02d}.json"}, True
    return {"manifest.json", "review.json", "review.md"}, False


def _validate_frame_files(binding: FrameBinding, files: set[str]) -> None:
    allowed, exact = _expected_files(binding)
    if exact and files != allowed:
        raise ValueError(f"{binding.kind} frame file set is invalid")
    if not exact and ("manifest.json" not in files or not files <= allowed):
        raise ValueError("final frame file set is invalid")


def _payload_bytes(files: Mapping[str, bytes]) -> bytes:
    text_files: dict[str, str] = {}
    for path, content in sorted(files.items()):
        if not _allowed_path(path):
            raise ValueError(f"evidence path is not allowed: {path}")
        try:
            text_files[path] = content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(f"evidence file is not UTF-8: {path}") from error
    payload = {"protocol": PAYLOAD_PROTOCOL, "files": text_files}
    plain = canonical(payload).encode()
    if len(plain) > MAX_PLAIN_BYTES:
        raise ValueError("plain evidence frame exceeds 128 MiB")
    return plain


def _frame_lines(
    files: Mapping[str, bytes],
    binding: FrameBinding,
    *,
    validate_selection: bool = True,
) -> list[str]:
    if validate_selection:
        _validate_frame_files(binding, set(files))
    plain = _payload_bytes(files)
    compressed = gzip.compress(plain, compresslevel=9, mtime=0)
    if len(compressed) > MAX_COMPRESSED_BYTES:
        raise ValueError("compressed evidence frame exceeds 16 MiB")
    compressed_sha = sha256(compressed)
    plain_sha = sha256(plain)
    chunks = [
        compressed[index : index + CHUNK_BYTES]
        for index in range(0, len(compressed), CHUNK_BYTES)
    ]
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
        "plain_sha256": plain_sha,
        "compressed_sha256": compressed_sha,
        "chunks": len(chunks),
    }
    frame_id = sha256(
        canonical(
            {key: value for key, value in begin.items() if key != "type"}
        ).encode()
    )
    begin["frame_id"] = frame_id
    result = [PREFIX + canonical(begin)]
    for index, chunk in enumerate(chunks):
        result.append(
            PREFIX
            + canonical(
                {
                    "type": "CHUNK",
                    "frame_id": frame_id,
                    "index": index,
                    "chunks": len(chunks),
                    "chunk_sha256": sha256(chunk),
                    "data": base64.b64encode(chunk).decode("ascii"),
                }
            )
        )
    result.append(
        PREFIX
        + canonical(
            {
                "type": "END",
                "frame_id": frame_id,
                "chunks": len(chunks),
                "compressed_sha256": compressed_sha,
            }
        )
    )
    return result


def emit(root: Path, kind: str, segment: int | None = None) -> list[str]:
    binding = FrameBinding(
        run_id=os.environ.get("GITHUB_RUN_ID", ""),
        attempt=int(os.environ.get("GITHUB_RUN_ATTEMPT", "0")),
        source_sha=os.environ.get("GITHUB_SHA", ""),
        kind=kind,
        segment=segment,
    )
    root = root.resolve()
    expected, exact = _expected_files(binding)
    if exact:
        selected = expected
    else:
        selected = {path for path in expected if (root / path).is_file()}
    files: dict[str, bytes] = {}
    for relative in sorted(selected):
        path = _safe_output_path(root, relative)
        if not path.is_file():
            raise ValueError(f"required evidence file is missing: {relative}")
        files[relative] = path.read_bytes()
    lines = _frame_lines(files, binding)
    for line in lines:
        print(line, flush=True)
    return lines


@dataclass
class _Accumulator:
    begin: dict[str, Any]
    chunks: list[bytes]
    next_index: int = 0
    invalid: bool = False


BEGIN_KEYS = {
    "type",
    "version",
    "frame_id",
    "run_id",
    "attempt",
    "source_sha",
    "kind",
    "segment",
    "plain_size",
    "compressed_size",
    "plain_sha256",
    "compressed_sha256",
    "chunks",
}
CHUNK_KEYS = {
    "type",
    "frame_id",
    "index",
    "chunks",
    "chunk_sha256",
    "data",
}
END_KEYS = {"type", "frame_id", "chunks", "compressed_sha256"}


def _checked_integer(value: Any, minimum: int, maximum: int, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{label} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{label} is outside its bound")
    return value


def _parse_begin(
    item: Mapping[str, Any],
    expected_run_id: str,
    expected_attempt: int,
    expected_sha: str,
) -> _Accumulator:
    _exact_keys(item, BEGIN_KEYS, "BEGIN")
    if (
        item["type"] != "BEGIN"
        or isinstance(item["version"], bool)
        or item["version"] != 1
    ):
        raise ValueError("BEGIN protocol version is invalid")
    binding = FrameBinding(
        run_id=item["run_id"],
        attempt=item["attempt"],
        source_sha=item["source_sha"],
        kind=item["kind"],
        segment=item["segment"],
    )
    if (
        binding.run_id != expected_run_id
        or binding.attempt != expected_attempt
        or binding.source_sha != expected_sha
    ):
        raise ValueError("frame source binding does not match the collector")
    _checked_integer(item["plain_size"], 1, MAX_PLAIN_BYTES, "plain size")
    compressed_size = _checked_integer(
        item["compressed_size"], 1, MAX_COMPRESSED_BYTES, "compressed size"
    )
    chunks = _checked_integer(item["chunks"], 1, MAX_CHUNKS, "chunk count")
    if chunks != (compressed_size + CHUNK_BYTES - 1) // CHUNK_BYTES:
        raise ValueError("chunk count does not match compressed size")
    for key in ("frame_id", "plain_sha256", "compressed_sha256"):
        if (
            not isinstance(item[key], str)
            or re.fullmatch(r"[0-9a-f]{64}", item[key]) is None
        ):
            raise ValueError(f"{key} is invalid")
    expected_frame_id = sha256(
        canonical(
            {
                key: value
                for key, value in item.items()
                if key not in {"type", "frame_id"}
            }
        ).encode()
    )
    if item["frame_id"] != expected_frame_id:
        raise ValueError("BEGIN frame identity is invalid")
    return _Accumulator(begin=dict(item), chunks=[])


def _safe_gunzip(value: bytes) -> bytes:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    result = bytearray()
    pending = value
    while pending:
        remaining = MAX_PLAIN_BYTES + 1 - len(result)
        if remaining <= 0:
            raise ValueError("plain evidence frame exceeds 128 MiB")
        result.extend(decoder.decompress(pending, remaining))
        pending = decoder.unconsumed_tail
        if not pending:
            break
    remaining = MAX_PLAIN_BYTES + 1 - len(result)
    result.extend(decoder.flush(max(1, remaining)))
    if len(result) > MAX_PLAIN_BYTES:
        raise ValueError("plain evidence frame exceeds 128 MiB")
    if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError("gzip evidence frame is incomplete or has trailing data")
    return bytes(result)


def _decode_payload(value: bytes, begin: Mapping[str, Any]) -> dict[str, bytes]:
    if (
        len(value) != begin["compressed_size"]
        or sha256(value) != begin["compressed_sha256"]
    ):
        raise ValueError("compressed frame size or digest differs")
    plain = _safe_gunzip(value)
    if len(plain) != begin["plain_size"] or sha256(plain) != begin["plain_sha256"]:
        raise ValueError("plain frame size or digest differs")
    payload = _json_unique(plain)
    if not isinstance(payload, dict):
        raise TypeError("payload is not an object")
    _exact_keys(payload, {"protocol", "files"}, "payload")
    if payload["protocol"] != PAYLOAD_PROTOCOL or not isinstance(
        payload["files"], dict
    ):
        raise ValueError("payload protocol or files are invalid")
    if canonical(payload).encode() != plain:
        raise ValueError("payload is not canonical")
    binding = FrameBinding(
        run_id=begin["run_id"],
        attempt=begin["attempt"],
        source_sha=begin["source_sha"],
        kind=begin["kind"],
        segment=begin["segment"],
    )
    invalid_paths = [path for path in payload["files"] if not _allowed_path(path)]
    if invalid_paths:
        raise ValueError(
            "evidence path is not allowed: " + ",".join(sorted(invalid_paths))
        )
    _validate_frame_files(binding, set(payload["files"]))
    files: dict[str, bytes] = {}
    for path, content in payload["files"].items():
        if not isinstance(path, str) or not isinstance(content, str):
            raise TypeError("payload path and content must be strings")
        if not _allowed_path(path):
            raise ValueError(f"evidence path is not allowed: {path}")
        files[path] = content.encode()
    return files


def decode_log(
    log: str,
    expected_run_id: str,
    expected_attempt: int,
    expected_sha: str,
) -> dict[str, bytes]:
    """Decode complete valid frames; raise with valid partial files on any error."""

    expected_sha = _checked_sha(expected_sha)
    if (
        not expected_run_id.isdecimal()
        or isinstance(expected_attempt, bool)
        or expected_attempt <= 0
    ):
        raise ValueError("collector run binding is invalid")
    decoded = DecodedFiles()
    violations: list[str] = []
    active: _Accumulator | None = None
    observed_lines = 0
    observed_frames: set[str] = set()
    observed_frame_scopes: set[tuple[str, int | None]] = set()
    declared_compressed_total = 0
    declared_plain_total = 0
    for line_number, line in enumerate(log.splitlines(), start=1):
        marker = line.find(PREFIX)
        if marker < 0:
            continue
        observed_lines += 1
        raw = line[marker + len(PREFIX) :].strip()
        try:
            item = _json_unique(raw)
            if not isinstance(item, dict) or not isinstance(item.get("type"), str):
                raise TypeError("frame line is not an object with a type")
        except (json.JSONDecodeError, TypeError, ValueError) as error:
            violations.append(f"line_{line_number}:invalid_json:{type(error).__name__}")
            if active is not None:
                active.invalid = True
            continue
        kind = item["type"]
        if kind == "BEGIN":
            if active is not None:
                violations.append(f"line_{line_number}:unfinished_frame")
            try:
                active = _parse_begin(
                    item, expected_run_id, expected_attempt, expected_sha
                )
                declared_compressed_total += active.begin["compressed_size"]
                declared_plain_total += active.begin["plain_size"]
                if declared_compressed_total > MAX_COMPRESSED_BYTES:
                    active.invalid = True
                    violations.append(
                        f"line_{line_number}:aggregate_compressed_limit"
                    )
                if declared_plain_total > MAX_PLAIN_BYTES:
                    active.invalid = True
                    violations.append(f"line_{line_number}:aggregate_plain_limit")
                if active.begin["frame_id"] in observed_frames:
                    active.invalid = True
                    violations.append(f"line_{line_number}:duplicate_frame")
                scope = (active.begin["kind"], active.begin["segment"])
                if scope in observed_frame_scopes:
                    active.invalid = True
                    violations.append(f"line_{line_number}:duplicate_frame_scope")
            except (KeyError, TypeError, ValueError) as error:
                violations.append(f"line_{line_number}:invalid_begin:{error}")
                active = None
            continue
        if active is None:
            violations.append(f"line_{line_number}:frame_line_without_begin")
            continue
        if kind == "CHUNK":
            try:
                _exact_keys(item, CHUNK_KEYS, "CHUNK")
                if (
                    item["frame_id"] != active.begin["frame_id"]
                    or _checked_integer(
                        item["chunks"], 1, MAX_CHUNKS, "CHUNK chunk count"
                    )
                    != active.begin["chunks"]
                    or _checked_integer(item["index"], 0, MAX_CHUNKS - 1, "CHUNK index")
                    != active.next_index
                ):
                    raise ValueError("chunk identity or order differs")
                if (
                    not isinstance(item["data"], str)
                    or len(item["data"]) > ((CHUNK_BYTES + 2) // 3) * 4
                ):
                    raise ValueError("chunk base64 size is invalid")
                data = base64.b64decode(item["data"], validate=True)
                if len(data) > CHUNK_BYTES or sha256(data) != item["chunk_sha256"]:
                    raise ValueError("chunk size or digest differs")
                active.chunks.append(data)
                active.next_index += 1
            except (KeyError, TypeError, ValueError, binascii.Error) as error:
                active.invalid = True
                violations.append(f"line_{line_number}:invalid_chunk:{error}")
            continue
        if kind == "END":
            try:
                _exact_keys(item, END_KEYS, "END")
                if (
                    item["frame_id"] != active.begin["frame_id"]
                    or _checked_integer(
                        item["chunks"], 1, MAX_CHUNKS, "END chunk count"
                    )
                    != active.begin["chunks"]
                    or item["compressed_sha256"] != active.begin["compressed_sha256"]
                    or active.next_index != active.begin["chunks"]
                ):
                    raise ValueError("END identity or chunk count differs")
                if active.invalid:
                    raise ValueError("frame contains an earlier invalid line")
                files = _decode_payload(b"".join(active.chunks), active.begin)
                divergent = [
                    path
                    for path, content in files.items()
                    if path in decoded and decoded[path] != content
                ]
                if divergent:
                    raise ValueError(
                        "file differs across frames: " + ",".join(sorted(divergent))
                    )
                decoded.update(files)
                decoded.frames.append(
                    {
                        key: active.begin[key]
                        for key in (
                            "frame_id",
                            "kind",
                            "segment",
                            "plain_sha256",
                            "compressed_sha256",
                        )
                    }
                )
                observed_frames.add(active.begin["frame_id"])
                observed_frame_scopes.add(
                    (active.begin["kind"], active.begin["segment"])
                )
            except (KeyError, TypeError, ValueError) as error:
                violations.append(f"line_{line_number}:invalid_end:{error}")
            active = None
            continue
        active.invalid = True
        violations.append(f"line_{line_number}:unknown_frame_line_type")
    if active is not None:
        violations.append("end_of_log:unfinished_frame")
    if not observed_lines:
        violations.append("evidence_prefix_missing")
    if violations:
        raise TransportError(violations, decoded)
    return decoded


def _json_file(files: Mapping[str, bytes], path: str, violations: list[str]) -> Any:
    content = files.get(path)
    if content is None:
        violations.append(f"missing:{path}")
        return None
    try:
        return _json_unique(content)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        violations.append(f"invalid_json:{path}")
        return None


def _strict_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _validate_evidence(
    files: Mapping[str, bytes],
    source_conclusion: str,
    expected_campaign: str,
    expected_sha: str,
) -> dict[str, Any]:
    violations: list[str] = []
    allowed = [path for path in files if not _allowed_path(path)]
    if allowed:
        violations.append("unknown_evidence_paths")
    if source_conclusion != "success":
        violations.append("source_job_not_successful")
    manifest = _json_file(files, "manifest.json", violations)
    review = _json_file(files, "review.json", violations)
    review_markdown = files.get("review.md")
    if review_markdown is None:
        violations.append("missing:review.md")
    segment_count = 0
    smoke: bool | None = None
    manifest_digest: str | None = None
    source_completed: int | None = None
    if not isinstance(manifest, dict):
        violations.append("manifest_not_object")
    else:
        if (
            manifest.get("protocol") != "ariadne-long-soak/v1"
            or manifest.get("run_id") != expected_campaign
            or manifest.get("git_commit") != expected_sha
        ):
            violations.append("manifest_identity")
        smoke = _strict_bool(manifest.get("smoke"))
        if smoke is None:
            violations.append("manifest_smoke_type")
        segment_value = manifest.get("segment_count")
        if isinstance(segment_value, bool) or not isinstance(segment_value, int):
            violations.append("manifest_segment_count_type")
        else:
            segment_count = segment_value
        source_hashes = manifest.get("source_sha256")
        core_hashes = manifest.get("core_source_sha256")
        if not isinstance(source_hashes, dict) or legacy_digest(
            source_hashes
        ) != manifest.get("source_manifest_digest"):
            violations.append("manifest_source_digest")
        if not isinstance(core_hashes, dict) or legacy_digest(
            core_hashes
        ) != manifest.get("core_source_manifest_digest"):
            violations.append("manifest_core_digest")
        manifest_digest = legacy_digest(manifest)
        runner_identity = manifest.get("runner_identity")
        if not isinstance(runner_identity, dict) or legacy_digest(
            runner_identity
        ) != manifest.get("runner_identity_digest"):
            violations.append("manifest_runner_identity")
        if smoke is False and (
            manifest.get("target_seconds") != 72 * 60 * 60
            or manifest.get("segment_seconds") != 4 * 60 * 60
            or segment_count != 18
        ):
            violations.append("long_campaign_shape")
        if smoke is True and segment_count != 1:
            violations.append("smoke_campaign_shape")

    present_segments = {
        int(match.group(1))
        for path in files
        if (match := SEGMENT_PATTERN.fullmatch(path)) is not None
    }
    expected_segments = set(range(segment_count)) if 0 <= segment_count <= 18 else set()
    if present_segments != expected_segments:
        violations.append("segment_file_set")
    if isinstance(files, DecodedFiles):
        observed_scopes = {
            (frame.get("kind"), frame.get("segment")) for frame in files.frames
        }
        expected_scopes = {
            ("bootstrap", None),
            ("final", None),
            *(("segment", index) for index in expected_segments),
        }
        if observed_scopes != expected_scopes or len(files.frames) != len(
            expected_scopes
        ):
            violations.append("transport_frame_scope_set")
    previous_digest: str | None = None
    previous_elapsed = -1.0
    previous_source = -1
    observed_losses: list[int] = []
    final_source: int | None = None
    previous_end: datetime | None = None
    for index in sorted(expected_segments):
        path = f"segments/segment-{index:02d}.json"
        segment = _json_file(files, path, violations)
        if not isinstance(segment, dict):
            violations.append(f"segment_not_object:{index}")
            continue
        if (
            segment.get("protocol") != "ariadne-long-soak-segment/v1"
            or segment.get("run_id") != expected_campaign
            or segment.get("segment") != index
            or segment.get("manifest_digest") != manifest_digest
            or segment.get("previous_segment_digest") != previous_digest
        ):
            violations.append(f"segment_identity:{index}")
        if manifest is not None and (
            segment.get("source_manifest_digest")
            != manifest.get("source_manifest_digest")
            or segment.get("runner_identity_digest")
            != manifest.get("runner_identity_digest")
        ):
            violations.append(f"segment_authority:{index}")
        if segment.get("passed") is not True:
            violations.append(f"segment_failed:{index}")
        oracles = segment.get("oracles")
        if not isinstance(oracles, dict) or set(oracles) != {
            "stream",
            "barrier",
            "join",
            "release",
        }:
            violations.append(f"oracle_set:{index}")
        else:
            for name, oracle in oracles.items():
                if (
                    not isinstance(oracle, dict)
                    or oracle.get("passed") is not True
                    or oracle.get("returncode") != 0
                    or oracle.get("violations", []) != []
                ):
                    violations.append(f"oracle:{index}:{name}")
        if segment.get("process_loss_injected") is True:
            observed_losses.append(index)
        if smoke is False:
            duration = segment.get("duration_seconds")
            declared_duration = (
                manifest.get("segment_seconds") if isinstance(manifest, dict) else None
            )
            if (
                not isinstance(duration, (int, float))
                or isinstance(duration, bool)
                or not isinstance(declared_duration, (int, float))
                or isinstance(declared_duration, bool)
                or duration < declared_duration * 0.98
            ):
                violations.append(f"segment_duration:{index}")
        try:
            started = datetime.fromisoformat(segment["started_at"])
            ended = datetime.fromisoformat(segment["ended_at"])
            if ended < started:
                raise ValueError("negative segment duration")
            if previous_end is not None:
                gap = (started - previous_end).total_seconds()
                if gap < 0 or gap > 20 * 60:
                    violations.append(f"segment_observation_gap:{index}")
            previous_end = ended
        except (KeyError, TypeError, ValueError):
            violations.append(f"segment_time:{index}")
        samples = segment.get("samples")
        if not isinstance(samples, list) or not samples:
            violations.append(f"segment_samples:{index}")
        else:
            for sample in samples:
                if not isinstance(sample, dict):
                    violations.append(f"segment_sample_shape:{index}")
                    continue
                try:
                    elapsed = float(sample["elapsed_seconds"])
                    source = int(sample["source_completed"])
                except (KeyError, TypeError, ValueError):
                    violations.append(f"segment_sample_cursor:{index}")
                    continue
                if elapsed < previous_elapsed:
                    violations.append(f"elapsed_regression:{index}")
                if source < previous_source:
                    violations.append(f"source_cursor_regression:{index}")
                previous_elapsed, previous_source = elapsed, source
                final_source = source
        previous_digest = legacy_digest(segment)

    if isinstance(manifest, dict):
        schedule = manifest.get("fault_schedule")
        expected_losses = (
            schedule.get("process_loss_segments")
            if isinstance(schedule, dict)
            else None
        )
        if expected_losses != observed_losses:
            violations.append("process_loss_schedule")
        if smoke is False and expected_losses != [5, 11]:
            violations.append("long_process_loss_schedule")

    if not isinstance(review, dict):
        violations.append("review_not_object")
    else:
        if (
            review.get("protocol") != "ariadne-long-soak-review/v1"
            or review.get("run_id") != expected_campaign
            or review.get("manifest_digest") != manifest_digest
            or review.get("segment_count") != segment_count
            or review.get("violations") != []
        ):
            violations.append("review_binding")
        if smoke is True and (
            review.get("smoke_passed") is not True
            or review.get("long_soak_passed") is not False
        ):
            violations.append("smoke_review_verdict")
        if smoke is False and (
            review.get("long_soak_passed") is not True
            or review.get("smoke_passed") is not False
        ):
            violations.append("long_review_verdict")
        elapsed = review.get("elapsed_seconds")
        if smoke is False and (
            not isinstance(elapsed, (int, float))
            or isinstance(elapsed, bool)
            or elapsed < 72 * 60 * 60
        ):
            violations.append("long_review_elapsed")
        source_completed = review.get("source_completed")
        if source_completed != final_source:
            violations.append("review_source_cursor")
        policy = review.get("resource_policy")
        expected_policy = (
            ("smoke_absolute_caps", True, False, True)
            if smoke is True
            else ("long_absolute_and_rate_caps", True, True, False)
        )
        if (
            not isinstance(policy, dict)
            or (
                policy.get("profile"),
                policy.get("absolute_caps_enforced"),
                policy.get("growth_rate_caps_enforced"),
                policy.get("growth_rates_descriptive"),
            )
            != expected_policy
        ):
            violations.append("review_resource_policy")
        if isinstance(manifest, dict):
            schedule = manifest.get("fault_schedule")
            owner_every = (
                schedule.get("owner_loss_every_records")
                if isinstance(schedule, dict)
                else None
            )
            if (
                isinstance(source_completed, int)
                and isinstance(owner_every, int)
                and owner_every > 0
                and review.get("owner_faults") != source_completed // owner_every
            ):
                violations.append("review_owner_faults")
        if review.get("process_boots") != 1 + len(observed_losses):
            violations.append("review_process_boots")
    if review_markdown is not None:
        try:
            markdown = review_markdown.decode()
        except UnicodeDecodeError:
            violations.append("review_markdown_encoding")
        else:
            expected_line = (
                "- `long_soak_passed`: `true`"
                if smoke is False
                else "- `smoke_passed`: `true`"
            )
            if expected_campaign not in markdown or expected_line not in markdown:
                violations.append("review_markdown_binding")
    return {
        "protocol": REVIEW_PROTOCOL,
        "passed": not violations,
        "source_conclusion": source_conclusion,
        "expected_campaign": expected_campaign,
        "expected_source_sha": expected_sha,
        "observed_files": sorted(files),
        "segment_count": segment_count,
        "source_completed": source_completed,
        "violations": sorted(set(violations)),
        "collector_live_review_performed": False,
        "collector_live_review_reason": "collector runner identity differs from the source runner",
    }


def validate_evidence(
    files: Mapping[str, bytes],
    source_conclusion: str,
    expected_campaign: str,
    expected_sha: str,
) -> dict[str, Any]:
    """Validate reconstructed public evidence without re-observing live state."""

    try:
        expected_sha = _checked_sha(expected_sha)
        if source_conclusion not in {"success", "failure", "cancelled"}:
            raise ValueError("source conclusion is invalid")
        if not expected_campaign:
            raise ValueError("expected campaign is empty")
        return _validate_evidence(
            files, source_conclusion, expected_campaign, expected_sha
        )
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        return {
            "protocol": REVIEW_PROTOCOL,
            "passed": False,
            "source_conclusion": source_conclusion,
            "expected_campaign": expected_campaign,
            "expected_source_sha": expected_sha,
            "observed_files": sorted(str(path) for path in files),
            "violations": [f"validator_input:{type(error).__name__}:{error}"],
            "collector_live_review_performed": False,
            "collector_live_review_reason": "collector runner identity differs from the source runner",
        }


def _write_partial(output: Path, files: Mapping[str, bytes]) -> None:
    if output.exists() and any(output.iterdir()):
        raise ValueError("collector output directory must be empty")
    output.mkdir(parents=True, exist_ok=True)
    for relative, content in sorted(files.items()):
        destination = _safe_output_path(output, relative)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_suffix(destination.suffix + ".tmp")
        temporary.write_bytes(content)
        temporary.replace(destination)


def collect_report(
    log_path: Path,
    output: Path,
    run_id: str,
    attempt: int,
    source_sha: str,
    source_conclusion: str,
) -> dict[str, Any]:
    transport_violations: list[str] = []
    files: Mapping[str, bytes] = {}
    try:
        log = log_path.read_text(encoding="utf-8", errors="replace")
        files = decode_log(log, run_id, attempt, source_sha)
    except TransportError as error:
        files = error.partial_files
        transport_violations.extend(f"transport:{item}" for item in error.violations)
    except (OSError, TypeError, ValueError) as error:
        transport_violations.append(f"transport:{type(error).__name__}:{error}")
    campaign = f"gh-{run_id}-{attempt}"
    report = validate_evidence(files, source_conclusion, campaign, source_sha)
    report["transport_violations"] = sorted(set(transport_violations))
    if transport_violations:
        report["passed"] = False
        report["violations"] = sorted(
            set(report.get("violations", [])) | set(transport_violations)
        )
    try:
        _write_partial(output, files)
    except (OSError, ValueError) as error:
        report["passed"] = False
        report["violations"] = sorted(
            set(report.get("violations", []))
            | {f"output:{type(error).__name__}:{error}"}
        )
        output.mkdir(parents=True, exist_ok=True)
    review_path = output / "transport-review.json"
    temporary = review_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    temporary.replace(review_path)
    return report


def collect(
    log_path: Path,
    output: Path,
    run_id: str,
    attempt: int,
    sha: str,
    source_conclusion: str,
) -> int:
    """Collect a job log into public evidence and return a process exit code."""

    report = collect_report(log_path, output, run_id, attempt, sha, source_conclusion)
    return 0 if report["passed"] else 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    emit_parser = commands.add_parser("emit")
    emit_parser.add_argument("--root", type=Path, required=True)
    emit_parser.add_argument(
        "--kind", choices=("bootstrap", "segment", "final"), required=True
    )
    emit_parser.add_argument("--segment", type=int)
    collect_parser = commands.add_parser("collect")
    collect_parser.add_argument("--log", type=Path, required=True)
    collect_parser.add_argument("--output", type=Path, required=True)
    collect_parser.add_argument("--run-id", required=True)
    collect_parser.add_argument("--attempt", type=int, required=True)
    collect_parser.add_argument("--sha", required=True)
    collect_parser.add_argument(
        "--source-conclusion",
        choices=("success", "failure", "cancelled"),
        required=True,
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "emit":
            emit(args.root, args.kind, args.segment)
            return 0
        report = collect_report(
            args.log,
            args.output,
            args.run_id,
            args.attempt,
            args.sha,
            args.source_conclusion,
        )
        print(json.dumps(report, sort_keys=True, indent=2))
        return 0 if report["passed"] else 2
    except (OSError, TypeError, ValueError) as error:
        print(f"transport error: {type(error).__name__}: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
