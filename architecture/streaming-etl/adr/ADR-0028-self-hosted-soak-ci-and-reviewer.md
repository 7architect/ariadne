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
verified; runner execution was not yet verified at that stage.

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

## Smoke resource policy and long-duration rates

The [fresh ARC observation](../evidence/ci-arc-smoke-observed-40577088311.json)
at commit `c1e5d114e0b2ef7cb89798b45dd9bcfe38e04a55` confirms the filesystem
boundary: the former root failed with `source_path_missing=true`, while the
selected shared root passed both directions. Readiness and the watch segment
then passed with 22 source records, seven owner faults, two process boots,
four passing oracles, complete credential-free export and owned cleanup.
The [raw log](../evidence/github-arc-smoke-resource-failed-40577088311.log)
preserves the actual reviewer FAIL: `unexplained_primary_growth` and
`unexplained_schema_growth`. This run is not reclassified as a smoke PASS.

The measured interval was 64.324871 seconds. Maxima were 71,009,267 primary
data bytes, 6,537,216 schema bytes and 38,304,481 service RSS bytes. The old
reviewer applied hourly latter-half slope limits to this cold startup window.
Finite PostgreSQL page allocations over seconds cannot establish a sustained
hourly growth trend. Retrying or increasing the numeric limits would not
resolve that mismatch between the short and long acceptance profiles.

Agreement: smoke enforces the existing absolute budgets and every existing
identity, durability, cursor, checksum, fault, oracle and capacity check. It
continues to calculate and report all growth rates descriptively. Non-smoke
retains both absolute budgets and the unchanged hourly growth limits. Reports
declare an explicit `resource_policy` and keep `long_soak_passed=false` for
smoke.

Absolute limits remain 8 GiB primary data, 512 MiB schema relations and
768 MiB service RSS. Long-profile growth limits remain 32 MiB/hour primary,
8 MiB/hour schema and 16 MiB/hour RSS. None of these numeric limits increased.
As a developer, I want cold smoke resource observations to describe their
measured duration. As a user, I want long-duration resource limits to remain
mandatory for the long campaign.

Acceptance: as a system, I can pass eight bounded cold-allocation samples in
smoke while still reporting their high hourly extrapolations, reject the same
growth rates in non-smoke review, reject each absolute-budget overrun in smoke,
and expose the exact policy in the report. The eight-test reviewer suite
includes those controls: the
[before log](../evidence/ci-smoke-resource-policy-before-2026-10-27.log)
reproduces the defect and the
[after log](../evidence/ci-smoke-resource-policy-after-2026-10-27.log)
passes. A fresh ARC campaign must still pass the changed reviewer on its own
source identity; historical campaigns are not re-reviewed as new acceptance.

## Hosted ARC smoke acceptance

[Run 40578404646](https://github.com/Anacreonsystems/ariadne/actions/runs/40578404646)
at commit `270f5de15617016c6d093049f2d3c7ca97d315e6` passes on
`anacreon-runners-runner-xjl9q`. The
[observation decoded from exact job logs](../evidence/ci-arc-smoke-observed-40578404646.json)
binds the run, source identity, probe, segment, deterministic review, export and
job-step outcomes. The selected root passes in both directions; the former
root fails with Docker exit 125 and `source_path_missing=true`.

The run observed 65.122762 seconds, 22 source documents, seven scheduled owner
faults, two process boots after the injected process loss, no unexpected
container restarts and four passing independent oracles. The reviewer reports
`smoke_passed=true`, `long_soak_passed=false`, no violations and the explicit
`smoke_absolute_caps` policy. Export reports `complete=true` and
`credentials_exported=false`; artifact upload and owned cleanup succeed.

The [raw successful log](../evidence/github-arc-smoke-passed-40578404646.log)
and [Principal acceptance](../evidence/ci-arc-smoke-principal-review-2026-10-27.json)
close the ARC smoke repair without reclassifying earlier failed runs. The
artifact's server metadata/digest is preserved, but its ZIP download through
the current connection returns HTTP 403. This acceptance uses retrieved job
logs, the public exporter result and successful upload metadata; it does not
claim independent inspection of ZIP bytes.

The [final evidence graph](../evidence/adr-graph-arc-smoke-2026-10-27.json)
checks the new links and retained statuses. All 126 Python/HTML sources from
the accepted PostgreSQL gate still match; the isolated reviewer policy has its
own regression controls and fresh hosted execution. ARC access and the one-job
smoke are verified. The static runner's real 18-job/72-hour campaign remains
unstarted, so this ADR and DS-052a remain `progress` with no percentage added.

## Main-branch dispatch observation (2026-10-27)

The user merged PR #1 at `3776be1382d3d047485c6f96a14f9a5c1a1cb458`.
Its tree exactly matches the accepted branch tip
`24e49ec87cbcfa20a42f3a53fba2b54d10ff18c0`; no implementation changes were
introduced by the merge. The workflow is now active on the default branch.

The separate [`72h` dispatch](https://github.com/Anacreonsystems/ariadne/actions/runs/40601620025)
was accepted at 2026-10-27 23:21:54 UTC with campaign ID
`gh-40601620025-1`. The [dispatch observation](../evidence/ci-long-soak-dispatch-40601620025.json)
records a queued prepare job requesting `self-hosted`, with no runner assigned
at that observation. Bootstrap still requires the actual runner name to be
`k8s-runner`, a shared persistent state root and the existing Docker checks.

As a user, I want the merged source to receive its separate long-duration
validation on the existing persistent runner. As a developer, I can inspect
the exact source commit, campaign ID and queued job; this dispatch satisfies
traceability only. Queue time does not count toward the 259200-second workload.
Actual startup, all 18 observations and the final independent reviewer remain
open acceptance criteria. This ADR and DS-052a remain `progress` with no
additional percentage. See the [operator guide](../soak_ci.md),
[task lane](../todo.md) and [sprint board](../sprint_board.md).
The [dispatch evidence graph](../evidence/adr-graph-long-dispatch-40601620025.json)
checks the retained statuses and local documentation links.

## ARC retrigger and long-profile boundary (2026-10-28)

The user requested a retrigger and confirmed `anacreon-runners`. The existing
long-profile run was still queued, with no assigned runner and no executed
steps. It was [cancelled before bootstrap](../evidence/ci-long-soak-cancelled-40601620025.json)
to release the workflow concurrency group. The earlier dispatch observation
remains unchanged. A fresh
[smoke dispatch](https://github.com/Anacreonsystems/ariadne/actions/runs/40650444560)
uses the exact merged source and requests `anacreon-runners`.

As a user, I want a retrigger to use the confirmed available runner. As a
developer, I can distinguish this smoke from the unexecuted long campaign.
Neither cancellation nor a passing replacement smoke satisfies the 72-hour
acceptance criteria; DS-052a remains `progress` with no additional percentage.

Principal assessment: the current 18-job design requires persistent Docker and
state across jobs; changing its selector to an ephemeral ARC scale set would
violate that contract. A single 72-hour ARC job is a possible separate design,
but its pod/Docker lifetime, disk budget, job timeout and late evidence
publication have not been verified for this scale set. Repository-token expiry
alone does not prove that the separate artifact-runtime credentials must fail;
late upload must be demonstrated rather than assumed. No long-job guard,
duration, resource threshold or evidence criterion was weakened for this
retrigger. A future ARC long-profile change needs its own validated execution
and evidence-lifetime contract.

The [replacement observation](../evidence/ci-arc-smoke-observed-40650444560.json)
and [raw job log](../evidence/github-arc-smoke-retrigger-40650444560.log)
record `success` on `anacreon-runners-runner-vf9jp` at the exact merged commit.
The measured workload lasted 64.830406 seconds and completed 22 source
documents, seven owner faults and two process boots after the injected process
loss. All four independent oracles, the reviewer, public export, artifact
upload and owned cleanup passed. `smoke_passed=true`, `long_soak_passed=false`
and `violations=[]` preserve the profile boundary. Public report contents were
retrieved from exact job logs; artifact metadata was retrieved, but its archive
was not independently downloaded. The
[retrigger evidence graph](../evidence/adr-graph-arc-retrigger-2026-10-28.json)
checks retained statuses and links.

## Consequences

The separate campaign occupies the existing static host for at least 72 hours.
Other jobs on the host can affect measured capacity; a routing selector does not
reserve CPU or disk. Queue delays over the declared observation-gap limit cause
review failure even if the detached workload remains alive. Retained failure
databases require explicit cleanup. This ADR remains `progress` until its
actual-run acceptance criteria have evidence, and contributes no percentage to
the current short delivery under ADR-0027.
