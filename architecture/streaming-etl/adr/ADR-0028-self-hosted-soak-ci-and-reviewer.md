# ADR-0028: Existing self-hosted runners for continuous soak CI

- Status: `progress`
- Date: 2026-10-26
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0027](ADR-0027-short-acceptance-and-separate-long-soak.md), [ADR-0026](ADR-0026-release-compatibility-and-disaster-recovery.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-052a in [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

The user requested a separate 72-hour CI test and an automatic reviewer, then
instructed us to use the three existing GitHub self-hosted runners. A single
72-hour job exceeds the 24-hour lifetime of its repository token. Sequential
jobs can refresh credentials but must observe the same persistent workload.
Scheduling them freely across three machines would lose access to the original
Docker daemon and local evidence. A runner name is not automatically a routing
label. The current GitHub integration returns HTTP 403 when listing runners;
repository push permission does not establish runner-administration permission.

## Theory

One detached workload can continue across 18 sequential four-hour monitor jobs
on an existing persistent runner. Each job receives fresh CI credentials,
checks the same source and machine identity, and archives bounded public
evidence. A deterministic independent reviewer can reject missing segments,
insufficient measured duration, state corruption, cursor regression, missing
scheduled faults, and declared resource-budget violations.

## Proposed solution and agreement

Use the existing runners. Do not register a runner or provision a new machine.
The [main workflow](../../../.github/workflows/streaming-etl-soak.yml) offers a
short smoke profile and a manual `72h` profile. The smoke uses the standard
`self-hosted`, `Linux`, and `X64` labels and performs start, fault injection,
review, artifact upload, and cleanup within one job.

The long profile additionally requires an existing custom label unique to one
persistent runner. All jobs request that label. Generic labels are rejected
before bootstrap; runtime identity checks independently reject movement to a
different host. We must verify a real label before dispatching the long profile.
An explicit dependency chain, rather than matrix scheduling order, runs the
[18 monitor jobs](../../../.github/workflows/streaming-etl-soak-segment.yml).
Each monitor lasts four hours with a 270-minute job timeout. The continuous
service and synchronous PostgreSQL pair remain outside the checkout and CI
temporary directory at `/var/tmp/ariadne-soak/gh-<run-id>-<run-attempt>`.
Rerunning all jobs creates a fresh campaign; it cannot silently reuse a cleaned
container set or old completed-segment evidence.

The harness executes ordinary Python yields through
`source -> A -> {B,C} -> J -> D`, with periodic barriers, bounded retention,
release fencing, owner loss, and two scheduled workload-process failures. This
campaign covers that PostgreSQL-backed DAG. Actual Prefect integration retains
its separate short acceptance evidence; this campaign does not claim 72 hours
of Prefect execution.

Every job checks out the original commit with persisted Git credentials
disabled. Docker executes a copied immutable source image, with pinned base
images and frozen dependencies. The CI identity has `contents: read`; no PAT or
language-model credential is required. The reviewer is deterministic code.
Only the explicit public artifact whitelist plus a bounded `ci-status.json`
diagnostic are uploaded. That diagnostic contains run/attempt identifiers,
runner name and job outcome, including when bootstrap failed before a manifest
existed. Credentials and the
complete persistent state directory are never uploaded.

The final job reviews evidence even after an earlier monitor fails. Success
cleans up only this campaign's resources. Failure stops its workload and retains
its databases for investigation. The separate
[cleanup workflow](../../../.github/workflows/streaming-etl-soak-cleanup.yml)
handles retained or cancelled campaigns on their owning host. Cancellation can
interrupt final cleanup, so the operator must verify cleanup explicitly.

## Stories

- As a developer, I want to use an existing self-hosted runner without creating
  another machine or requiring additional runtime secrets.
- As a user, I want a real 72-hour test to continue across CI credential windows.
- As a system, I want a reviewer to reject incomplete evidence and preserve
  failure artifacts without leaking credentials.

## Acceptance criteria

- As a developer, I can run the smoke profile on one of the existing runners
  and retrieve a public report naming the actual selected runner.
- As a system, I can execute exactly 18 sequential monitor jobs against one
  source identity, one machine, one service container, and one persistent DB pair.
- As a user, I got a `long_soak_passed=true` result only after at least 259200
  measured seconds, all linked segments, the declared faults, and all invariants
  pass.
- As a system, I can fail on source or host drift, missing records, inconsistent
  managed state, cursor regression, expired authority, and resource violations.
- As a developer, I can inspect smoke and long results separately, and can clean
  up only the resources belonging to an explicit campaign ID.
- As a user, I can see whether the workflow was only prepared locally, published,
  smoke-tested on GitHub, or actually completed its long campaign.

## Evidence

The preserved [initial local smoke](../evidence/postgres-long-soak-smoke-2026-10-25.json)
proves control-path recovery and reports `long_soak_passed=false`. Runner
portability changes replace dependency bootstrapping with a pinned official uv
binary and allow partial public failure evidence while retaining strict review.
Current execution and publication results are tracked in the
[CI operator guide](../soak_ci.md). No long-duration acceptance is claimed.

The [runner-portability smoke](../evidence/postgres-long-soak-runner-smoke-2026-10-26.json)
passes on the pinned uv-only image, including active process loss, five owner
faults, four independent oracles, sanitized Docker errors and credential-free
export. Five reviewer/cleanup regressions pass. A local smoke cannot establish
GitHub runner readiness or the clean-checkout gate. The Principal reviewed the
three workflow files and found no remaining blocking defect; actionlint and the
18-job dependency-chain check pass. Checkout and artifact actions are pinned to
the verified v7.0.1 and v7.0.2 commit SHAs respectively.

## Consequences

The separate campaign occupies one existing host for at least 72 hours. Other
jobs on the host can affect measured capacity; a unique routing label does not
reserve CPU or disk. Queue delays over the declared observation-gap limit cause
review failure even if the detached workload remains alive. Retained failure
databases require explicit cleanup. This ADR remains `progress` until its
actual-run acceptance criteria have evidence, and contributes no percentage to
the current short delivery under ADR-0027.
