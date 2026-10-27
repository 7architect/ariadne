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
Use project names and neutral wording for branches, commits and documentation.
The delivery branch is `streaming-etl-adrs`; CI branch filters and current
documentation follow its name. Historical evidence remains unchanged.
The [main workflow](../../../.github/workflows/streaming-etl-soak.yml) offers a
short smoke profile and a manual `72h` profile. The user confirmed the runner
inventory: `anacreon-runners` is an ARC ephemeral scale set with no additional
labels; `k8s-runner` is the static runner with `self-hosted`, `Linux`, and `X64`.
The smoke uses `runs-on: anacreon-runners` and performs start, fault injection,
review, artifact upload, and cleanup within one job, so no state must survive
replacement of an ARC runner.

The initial unique-custom-label requirement is superseded by that confirmed
inventory. The long profile uses the existing static runner's standard labels
and explicitly verifies `RUNNER_NAME=k8s-runner` before bootstrap, every monitor,
review, and manual cleanup. Runtime identity checks independently reject host
or Docker drift. No new label or runner registration is required. An ephemeral
ARC job cannot own this long campaign across job boundaries.
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

## Repository ownership and runner access

The user clarified that the ARC scale set belongs to the `Anacreonsystems`
organization. At the initial observation, `7architect/ariadne` belonged to a personal
account; matching a runner label does not grant access to another owner's
organization runners. This explains why the requested selector alone does not
establish runner eligibility.

The user proposed transferring the repository to the organization. Preflight
confirmed the organization login, the published delivery branch, and no public
target repository named `Anacreonsystems/ariadne`. The attempted repository
transfer API returned HTTP 403 `Resource not accessible by integration`.
That API attempt did not transfer the repository or change the local remote. Repository administration
permission for the integration is distinct from the connected user's reported
repository admin permission.

The owner can transfer through repository Settings, General, Danger Zone,
Transfer ownership, selecting `Anacreonsystems` and keeping `ariadne`. After a
successful transfer, verify the new owner and branch, update the Git remote,
and check that the organization's runner group permits this repository. Transfer
alone does not override a selected-repository runner-group policy. Existing
accepted short-profile evidence remains valid; hosted CI and DS-052a remain open.

On 2026-10-26 the user completed the transfer. The destination API and the old
URL both resolve to `Anacreonsystems/ariadne`, repository ID `1425414142`.
The `main` and `streaming-etl-adrs` tips were preserved, and the local Git remote
now points to the organization. Rerunning
[smoke 40343803001](https://github.com/Anacreonsystems/ariadne/actions/runs/40343803001)
created attempt 2. It still had no assigned `anacreon-runners` host when the
separate static-analysis failure was investigated on 2026-10-27. Transfer is
verified; runner execution remains unverified.

## CI source hygiene and immutable evidence

The pull request's
[static-analysis run](https://github.com/Anacreonsystems/ariadne/actions/runs/40349113436)
failed in `pre-commit`, while its type-check job passed. The pinned Ruff 0.15.19
found 142 issues and would reformat 44 files. Other failed hooks found double
backticks in active Python documentation and two truncated words in captured
crash logs. These findings are independent of the queued streaming smoke.

Active prototype code must pass the same pinned hooks as the repository.
Formatting, import cleanup, explicit test imports and readable test variable
names repair those failures. The protocol's literal sink tier `I` and the two
imports after a standalone test-path bootstrap retain narrowly explained local
lint annotations. No active prototype directory is excluded.

Two historical probe scripts are exceptions to Ruff check/format:
`prefect-storage-discovery-baseline-2026-10-26.py` and
`prefect-storage-discovery-configured-2026-10-26.py` inside `evidence/`.
Their executed bytes are bound to the
[probe report](../evidence/prefect-storage-discovery-probe-2026-10-26.json).
The exclusion names those exact files and date; future evidence scripts remain
checked. Codespell excludes only raw `evidence/*.log` process output, where a
crash can truncate a word. Evidence JSON, Markdown and active sources remain
checked. Historical reports, scripts and logs must retain their original bytes.

As a developer, I want the published branch to pass repository hooks without
rewriting previously executed evidence. As a system, I want changed source bytes
to receive a successor frozen-source acceptance report rather than inheriting
the old report's hashes.

Acceptance for this repair: as a developer, I can run `pre-commit run --all-files`
twice without a further edit; as a system, I got passing SQLite/source/soak
regressions, a new complete PostgreSQL gate with no source drift, and preserved
hashes for every pre-existing evidence file. The original short acceptance
remains historical evidence; the new gate must bind the current source tree.
The [repair report](../evidence/ci-static-analysis-repair-2026-10-27.json)
records observed checks and publication status. Hosted CI and actual 72-hour
acceptance remain separate observations.

The published repair at `6d03a45e28c009f384c09cdbcfb3057432ea2e99`
passed both `Pre-commit checks` and `Type Checks` in
[run 40484377266](https://github.com/Anacreonsystems/ariadne/actions/runs/40484377266).
The obsolete pre-repair queued smoke was cancelled before publication. The
[replacement smoke](https://github.com/Anacreonsystems/ariadne/actions/runs/40484370723)
requests `anacreon-runners` and was still queued at that observation.
Static-analysis success does not establish streaming-runner access.

The [first successor gate](../evidence/postgres-ci-style-regression-2026-10-27.json)
completed all 164 PostgreSQL tests in 759.190 seconds with one error and no
source drift. `test_heartbeat_and_same_owner_recovery` completed its first
sync turn, then the second async turn's `deliver_next` raised
`StaleOwner: owner lease expired`. The fixture starts with a 50 ms consumer
lease and renews with a 120 ms invocation lease. An
[isolated exact-test retry](../evidence/ci-heartbeat-isolated-2026-10-27.log)
passed in 1.108 seconds. Concurrent hook/UI work and slow-loop warnings were
observed during the first gate; they do not prove the only possible cause.
The failed full report and its raw log are retained. A serial full rerun uses
the same source hashes and lease limits, with no concurrent local hook/UI/test
commands. This repair does not change timing thresholds to obtain a pass.

The [serial successor](../evidence/postgres-ci-style-regression-serial-2026-10-27.json)
then passed all 164 tests in 748.077 seconds, including the exact heartbeat
test, without skips or source drift. Its 126 Python/HTML hashes equal the
failed trial's hashes and match published commit
`6d03a45e28c009f384c09cdbcfb3057432ea2e99`. The synchronous `remote_apply`
profile and documentation checks pass; no unclosed-socket or ResourceWarning
was recorded. All 197 SQLite/source/soak regressions and two complete pinned
pre-commit passes also succeed. These observations establish the selected
source-hygiene successor; they do not establish execution under arbitrary
event-loop starvation or actual long-duration runner acceptance.

The [Principal review](../evidence/ci-source-hygiene-principal-review-2026-10-27.json)
accepts this repair and current-source short regression with no blockers.
The [source-hygiene manifest](../evidence/ci-source-hygiene-manifest-2026-10-27.json)
records all 48 changed Python files: 35 retain the exact AST, and the remaining
13 differ only by documented import, docstring and test-local name cleanup.
Every pre-existing evidence artifact retains its hash. The
[final documentation graph](../evidence/adr-graph-ci-repair-2026-10-27.json)
checks links and retained statuses after publication references were added.
The earned short experimental percentage remains 100%; this ADR remains
`progress` for its actual-run and long-duration criteria.

## ARC filesystem follow-up

After the user changed runner-group access, the queued workflow received
`anacreon-runners-runner-s2jvf`. It was assigned during cancellation of the old
queued attempt; that attempt was cancelled and its owned cleanup succeeded.
Fresh attempt 2 ran on `anacreon-runners-runner-km72x`. Python/Docker checks,
database bootstrap and image build completed, but the host never observed
`service-status.json` and readiness timed out. The
[failed raw log](../evidence/github-arc-smoke-failed-40484370723-attempt2.log)
and [repair record](../evidence/ci-arc-bind-mount-repair-2026-10-27.json)
retain the result. Artifact metadata exists; downloading its archive through the
current connection returned HTTP 403. The failed attempt is not acceptance.

The theory is a Docker-in-Docker filesystem boundary: the runner's `/var/tmp`
can differ from the daemon's directory with the same absolute name. A local
daemon shared with the development environment does not establish that an ARC
sidecar sees those files. Missing host status alone does not prove the theory;
a bidirectional bind-mount control is required.

The smoke now selects `$RUNNER_TEMP/ariadne-soak`, inside the ARC shared work
volume, and publishes that root through `GITHUB_ENV` for every following step.
The 72-hour profile and its separate monitor/cleanup jobs retain
`/var/tmp/ariadne-soak` on the static host. Ephemeral smoke paths cannot be used
for persistence between long-campaign jobs.

Before provisioning, a pinned Python container must read a fresh host nonce
and atomically write a matching response which the host verifies. The selected
root must pass. Smoke also probes the former `/var/tmp` root as a bounded,
nonfatal control; its failure is not required. Only an invisible legacy root
with a passing selected root establishes that boundary for the actual attempt.
The probe uses the existing sanitized Docker boundary and removes only its
own unique container/directory. Its public record contains outcomes and root
classes, not nonces or credentials. The later `ci-status.json` includes the
record after `export-public` recreates the artifact directory.

As a developer, I want the one-job ARC smoke to share its campaign files with
Docker and fail before provisioning when the selected mount cannot round-trip.
Acceptance: as a system, I got a passing selected-root probe, actual readiness,
scheduled process-loss recovery, a passing deterministic review, public export
and successful owned cleanup from a fresh ARC run. Workflow lint and local
checks prepare this repair; actual hosted evidence is required to close it.
Core/CLI bytes, lease limits and the selected short-delivery percentage do not
change. This ADR and the 72-hour acceptance remain `progress`.

## Consequences

The separate campaign occupies the existing static host for at least 72 hours.
Other jobs on the host can affect measured capacity; a routing selector does not
reserve CPU or disk. Queue delays over the declared observation-gap limit cause
review failure even if the detached workload remains alive. Retained failure
databases require explicit cleanup. This ADR remains `progress` until its
actual-run acceptance criteria have evidence, and contributes no percentage to
the current short delivery under ADR-0027.
