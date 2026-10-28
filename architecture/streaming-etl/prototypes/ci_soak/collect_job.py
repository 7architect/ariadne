"""Fetch the original job's logs with fresh, read-only job credentials."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import transport

MAX_LOG_BYTES = 128 * 1024**2
SOURCE_JOB_NAME = "Prepare on an existing self-hosted runner"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_bounded(response, limit=MAX_LOG_BYTES):
    value = response.read(limit + 1)
    if len(value) > limit:
        raise ValueError("job log exceeds transport limit")
    return value


def api(path: str):
    request = urllib.request.Request(
        "https://api.github.com" + path,
        headers={
            "Authorization": "Bearer " + os.environ["GH_TOKEN"],
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    opener = urllib.request.build_opener(NoRedirect())
    return opener.open(request, timeout=60)


def source_job(repository, run_id, attempt):
    with api(
        f"/repos/{repository}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100"
    ) as response:
        jobs = json.loads(read_bounded(response, 2 * 1024**2))["jobs"]
    matches = [job for job in jobs if job["name"] == SOURCE_JOB_NAME]
    if len(matches) != 1 or matches[0]["status"] != "completed":
        raise ValueError("original ARC job is not uniquely completed")
    return matches[0]


def download_log(repository, job_id):
    for retry in range(12):
        try:
            with api(f"/repos/{repository}/actions/jobs/{job_id}/logs") as response:
                return read_bounded(response).decode("utf-8", errors="replace")
        except urllib.error.HTTPError as error:
            if error.code in (301, 302, 303, 307):
                location = error.headers.get("Location", "")
                target = urllib.parse.urlsplit(location)
                approved = any(
                    (target.hostname or "").endswith(suffix)
                    for suffix in (
                        ".blob.core.windows.net",
                        ".githubusercontent.com",
                        ".actions.githubusercontent.com",
                    )
                )
                if target.scheme != "https" or target.username or not approved:
                    raise ValueError("unexpected job-log storage destination") from None
                # The signed storage URL receives no GitHub Authorization header.
                request = urllib.request.Request(location)
                with urllib.request.build_opener(NoRedirect()).open(
                    request, timeout=60
                ) as response:
                    return read_bounded(response).decode("utf-8", errors="replace")
            if error.code != 404 or retry == 11:
                raise RuntimeError(f"job-log API returned HTTP {error.code}") from None
            time.sleep(5)
    raise RuntimeError("job log unavailable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    repository = os.environ["GITHUB_REPOSITORY"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise ValueError("invalid repository")
    run_id = str(int(os.environ["GITHUB_RUN_ID"]))
    attempt = int(os.environ["GITHUB_RUN_ATTEMPT"])
    sha = os.environ["GITHUB_SHA"]
    diagnostic = {"passed": False, "run_id": run_id, "run_attempt": attempt}
    try:
        job = source_job(repository, run_id, attempt)
        if job.get("head_sha") != sha:
            raise ValueError("original job source SHA mismatch")
        text = download_log(repository, job["id"])
        log_path = args.output.parent / "original-arc-job.log"
        log_path.write_text(text)
        # The transport CLI writes public partial evidence even on rejection.
        result = transport.collect(
            log_path=log_path,
            output=args.output,
            run_id=run_id,
            attempt=attempt,
            sha=sha,
            source_conclusion=job["conclusion"],
        )
        report = json.loads((args.output / "transport-review.json").read_text())
        print("ARC_COLLECTION_RESULT_JSON=" + json.dumps(report, sort_keys=True))
        return result
    except Exception as error:
        diagnostic["failure_type"] = type(error).__name__
        (args.output / "transport-review.json").write_text(
            json.dumps(diagnostic, sort_keys=True, indent=2) + "\n"
        )
        print("ARC evidence collection failed: " + type(error).__name__)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
