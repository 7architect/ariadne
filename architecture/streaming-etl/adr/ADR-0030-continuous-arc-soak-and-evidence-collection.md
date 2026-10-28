# ADR-0030: Continuous ARC soak and fresh-job evidence collection

- Status: `progress`
- Date: 2026-10-28
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0028](ADR-0028-self-hosted-soak-ci-and-reviewer.md), [ADR-0027](ADR-0027-short-acceptance-and-separate-long-soak.md)
- Delivery: DS-052a in [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

The confirmed available runner is the ephemeral ARC scale set
`anacreon-runners`. The static-profile campaign waited without an assigned
runner and was cancelled before any workload ran. The
[replacement smoke](../evidence/ci-arc-smoke-observed-40650444560.json) passed,
but it did not execute the requested 72-hour test. The user directed completion
of the necessary adaptation. The 18-job static design in ADR-0028 cannot keep
its local Docker pair and state across different ARC pods.

## Theory

One ARC job can retain one pod, Docker daemon and shared temporary directory
for a continuous 72-hour campaign. GitHub's documented self-hosted job ceiling
is five days. A 4500-minute job timeout leaves three hours beyond the unchanged
259200-second workload for bootstrap, segment-boundary checks and finalization.
Each four-hour monitor is a separate step with a 270-minute timeout, within the
documented 360-minute step limit.

The original repository token has a maximum 24-hour lifetime. After bootstrap,
the long job only needs local Python, Docker and filesystem operations. It can
stream bounded, credential-free evidence into its normal job log. A dependent
ARC collector receives fresh read-only credentials, downloads that completed
job's log, validates the bound evidence and publishes the final artifact.

References: [Actions limits](https://docs.github.com/en/actions/reference/limits),
[workflow timeouts and token lifetime](https://docs.github.com/en/actions/writing-workflows/workflow-syntax-for-github-actions#jobsjob_idtimeout-minutes),
[job-log download API](https://docs.github.com/en/rest/actions/workflow-jobs?apiVersion=2022-11-28#download-job-logs-for-a-workflow-run).

## Proposed solution and agreement

`profile=72h, execution=arc` uses `anacreon-runners` and one
`$RUNNER_TEMP/ariadne-soak` directory, after the bidirectional Docker mount probe.
An observed capacity probe checks the state filesystem and a temporary local
Docker volume; the long profile requires at least 20 GiB free on each observed
filesystem. It removes only its own probe volume. This is an admission check,
not a guarantee against future node pressure or eviction.

Bootstrap, source identity, clean checkout, PostgreSQL synchronous replication,
the Python yield DAG, all resource caps and the deterministic reviewer remain
unchanged. Eighteen sequential monitor steps observe the same workload, with
scheduled process losses in segments 5 and 11. A failed step prevents later
monitors. Bounded progress receipts give live source position and elapsed time.
TERM/INT attempts public partial export and stops the owned service; final
workflow steps attempt review, export and owned cleanup even after failure.

Evidence frames contain only the existing public whitelist. Bootstrap emits
the manifest; each segment emits its own immutable JSON; finalization emits
the manifest and review files. Deterministic compression, bounded chunks,
content hashes and run/attempt/source bindings detect truncation or alteration.
The fresh collector checks the authenticated original job's identity and
conclusion, frame integrity, manifest/review binding and segment chain. It does
not claim to observe the original Docker pair from its different pod.
Credentials, connection strings and the private state directory are excluded.

The collector uses `actions: read` and `contents: read`; the original job keeps
`contents: read`. A signed log-storage redirect receives no repository token.
Rejected partial evidence is still uploaded with a failed transport report.
A final upload failure fails the workflow. The original runner's successful
review and successful collector/publication are both required for acceptance.

The existing static chain remains selectable with `execution=static`. Its
historical evidence and its persistent-runner checks remain intact.

## Stories

- As a developer, I want the available ARC scale set to execute the long test
  without creating another runner or adding credentials.
- As a user, I want one real continuous workload, with its source and results
  visible even though final publication uses a new short-lived job.
- As a system, I want missing, corrupted or incomplete evidence to fail the
  campaign while preserving the available public files.

## Acceptance criteria

- As a developer, I can run the complete original-job-to-fresh-collector path
  in a short ARC control, including process recovery and uploaded evidence.
- As a system, I can reject incorrect identity, missing or damaged chunks,
  non-whitelisted paths, oversized payloads, broken segment chains, and a
  failed original job even if an embedded report claims success.
- As a user, I got a real 259200-second workload, 18 linked four-hour segments,
  the unchanged faults/oracles/cursors/checksums/resource checks, and
  `long_soak_passed=true` from the original pod.
- As a user, I can retrieve a successfully published artifact whose fresh
  collector report passes and binds to that same run, attempt and source SHA.
- As a system, I got no long-duration PASS from a short control, cancellation,
  pod loss, missing final frame or failed artifact publication.

## Evidence

Implementation lives in the [workflow](../../../.github/workflows/streaming-etl-soak.yml),
[ARC observer](../prototypes/ci_soak/arc_job.py),
[fresh collector](../prototypes/ci_soak/collect_job.py) and
[transport](../prototypes/ci_soak/transport.py). The
[operator guide](../soak_ci.md) describes invocation and failure handling.
The [local validation](../evidence/ci-arc-single-job-validation-2026-10-28.json)
binds the tested source bytes and
[25 passing regressions](../evidence/ci-arc-single-job-tests-2026-10-28.log).
Actionlint and all four profile/backend configuration cases pass. The existing
ETL workload, CLI, supervisor and reviewer bytes remain unchanged.
The [Principal review](../evidence/ci-arc-single-job-principal-review-2026-10-28.json)
requires exact numeric resource-threshold binding in the collector. The
[successor validation](../evidence/ci-arc-single-job-successor-validation-2026-10-28.json)
adds that check and a negative control; all
[26 regressions pass](../evidence/ci-arc-single-job-successor-tests-2026-10-28.log).
The first hosted transport control at source `77cbaa2` remains a distinct
observation; a fresh successor control is required before long dispatch.
The [documentation graph](../evidence/adr-graph-arc-single-job-2026-10-28.json)
checks retained statuses and links.
Hosted controls and the actual dispatch will be linked after execution. This
ADR remains `progress` until the real long run and all criteria have evidence.

The first [hosted transport control](../evidence/ci-arc-transport-smoke-observed-40669929793.json)
passed on source `77cbaa2`. The
[numeric-threshold successor](../evidence/ci-arc-transport-smoke-observed-40670304409.json)
passed on source `fd701b6`. Both original and fresh collector jobs passed,
including artifact upload. They observed about 89.5 GiB free on both probed
filesystems. These are short transport observations, not long-duration PASS.

## Target-tail authority correction

Principal review identified a separate harness defect before long dispatch:
after reaching the workload target, the service kept emitting final samples
but stopped renewing its 300-second release authority. Accumulated monitor and
oracle overhead could therefore produce an expired final sample even after
correct processing. The active path already renews this authority normally.

The service now also renews it in `TARGET_REACHED`, before its final snapshot,
until explicit shutdown. It produces no further DATA or checkpoints and
retains the existing completion timestamp logic. Renewal failure propagates.
The reviewer and all numeric bounds remain unchanged.

The [reproduction and correction](../evidence/ci-arc-target-tail-validation-2026-10-28.json)
preserve the initial interrupted test-harness trial, a bounded three-test red
run with two failures, and the full 29-test green successor. A virtual clock
crosses 610 seconds with a 300-second lease and checks multiple positive-lease
snapshots, unchanged cursors/state, and no DATA/checkpoint calls. Separate
controls reject an expired target sample and fail on renewal error. This is a
model-clock test, not a claim of 610 measured production seconds.

Principal review accepts the correction for a fresh hosted successor. The
earlier hosted controls remain valid transport observations; their service
hash precedes this correction, so another hosted short control is required
before actual 72-hour dispatch.

## Consequences

No external durable environment or extra credential is required for this
experiment. Pod eviction or hard runner loss can destroy local state and fail
the campaign; the test does not claim to resume across ARC pod replacement.
Completed evidence frames remain in GitHub logs when they were delivered.
Log masking, truncation or unavailable download fails transport validation.
The collector publishes partial failures without treating them as acceptance.
Actual Prefect integration remains the separate short gate from ADR-0028.
DS-052a remains a separate lane with no additional delivery percentage.
