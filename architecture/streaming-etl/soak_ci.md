# Streaming ETL soak CI operator guide

Decisions: [ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md) and the
current [ARC execution contract](adr/ADR-0030-continuous-arc-soak-and-evidence-collection.md).
The [harness](prototypes/ci_soak/README.md) runs one persistent workload. The
[short-delivery agreement](adr/ADR-0027-short-acceptance-and-separate-long-soak.md)
remains in force.

## Existing runner requirements

Use the existing inventory: `anacreon-runners` is the ARC ephemeral scale set;
`k8s-runner` is the optional static runner. Both profiles require Linux x64,
Python 3.11 or newer, a working local Docker socket, outbound image/dependency
access and a writable state root shared bidirectionally with Docker.

ARC uses `$RUNNER_TEMP/ariadne-soak` for both profiles. Its long campaign stays
in one pod and one job. The explicit static backend uses
`/var/tmp/ariadne-soak` across jobs and verifies `RUNNER_NAME=k8s-runner`.
The harness verifies machine and Docker identity throughout either campaign.
Containers can consume up to 5 GiB RAM and five CPU equivalents in total.
The long admission probe requires at least 20 GiB free on the observed state
filesystem and temporary Docker volume. These are campaign prerequisites,
not a production capacity claim or protection against later node pressure.

## Start a short check

The [workflow](../../.github/workflows/streaming-etl-soak.yml) defaults to
`profile=smoke`, routed to `anacreon-runners`. A matching push on
`streaming-etl-adrs` also requests this profile. The original job performs
start/recovery/review/export/cleanup, while a dependent fresh ARC job verifies
and publishes reconstructed public evidence from the original job log.
The original reviewer must report `smoke_passed=true` and
`long_soak_passed=false`; the collector must also pass. A successful short
control does not close DS-052a or establish long-duration capacity.

## Start the separate long campaign

Choose `profile=72h`, `execution=arc` (the default). Manual dispatch requires
the workflow to be available on the default branch; an existing workflow can
then be dispatched at the reviewed feature-branch ref. Every job checks out
the exact triggering SHA with persisted Git credentials disabled.

Bootstrap copies committed source into an immutable Docker image. Eighteen
sequential four-hour monitor steps run inside one 4500-minute ARC job. The
service and PostgreSQL pair continue through step boundaries. Scheduled process
losses occur in segments 5 and 11. The original pod runs the independent final
review with unchanged duration, source/state checksums, faults, synchronous
replication, authority, pressure, and absolute/rate resource checks.

The early partial artifact tests initial publication. Each segment and the
final review emit bounded, compressed public frames to the job log. The
original long job performs no late repository API or artifact operation.
A dependent ARC job gets fresh `actions: read` credentials, downloads the
completed original job log, verifies frame integrity, source identity,
segment chain and original verdict, and uploads `streaming-soak-collected-*`.
Both the original job and collector/publication must pass. Read
`transport-review.json` and `review.json` in that artifact.

The whitelist contains `manifest.json`, `segments/segment-*.json`, `review.json`
and `review.md`; the collector adds `transport-review.json`. Native short
artifacts also include bounded `ci-status.json` diagnostics. Failed runs
publish available partial evidence with a failed transport verdict. Never
upload the private state directory, `credentials.json`, or the unfiltered
original job log.

`execution=static` retains the 18-job chain and `streaming-soak-final-*`
artifact. That option requires `k8s-runner`; it does not route to ARC.

## Failure, retry and cleanup

Campaign ID is `gh-<GitHub run ID>-<GitHub run attempt>`. Rerun all jobs for a
fresh campaign. Individual completed segments are immutable; an ARC pod cannot
resume a prior pod's campaign. Queue time is not measured workload duration.

The original ARC job attempts public partial export and owned Docker cleanup
after its final frame, including on failure. TERM/INT also attempts partial
export and stops its service. Hard pod loss can destroy state and prevent
finalization; this fails the campaign. The fresh collector preserves public
frames when it can retrieve the job log. Missing, altered, oversized or
truncated evidence and final artifact failure all prevent acceptance.

For the static backend, success cleans owned resources; failure stops the
workload and retains databases. Cancellation can prevent finalization. Use the
[cleanup workflow](../../.github/workflows/streaming-etl-soak-cleanup.yml) with
the exact campaign ID on its owning static runner. It removes only that
campaign's named resources and credentials, without global Docker cleanup.

## Initial execution status (2026-10-26)

- Workflow source, the explicit 18-job chain and the prototype dependencies are
  committed and published on `streaming-etl-adrs` at commit
  `2f5902d5297f475f2835c4fdf8ce6db73aee287d`.
- The three new workflow files pass actionlint v1.7.12.
- The [current local Docker smoke](evidence/postgres-long-soak-runner-smoke-2026-10-26.json)
  passes after active process loss, five owner faults and four independent oracle
  checks. All five reviewer/cleanup regressions pass. This does not establish
  GitHub runner readiness or a clean CI checkout.
- Principal workflow review passed without a blocker. Checkout v7.0.1 and
  upload-artifact v7.0.2 are pinned to verified full commit SHAs.
- GitHub runner listing and runner permission reads return HTTP 403 through the
  current integration. Existing-runner routing uses labels and needs no runner
  registration or administration API.
- GitHub accepted the workflow and created
  [smoke run 40288079824](https://github.com/7architect/ariadne/actions/runs/40288079824).
  Its prepare job initially waits for `self-hosted`, `Linux`, and `X64` labels;
  no runner has yet been assigned at the time this observation was recorded.
- The user confirmed the two runner types; routing now uses ARC for smoke and
  the static runner with an explicit name check for the long campaign.
- A passing GitHub smoke and actual 72-hour execution are not yet evidenced.
  `long_soak_passed` remains false. Historical observations are
  archived in the [CI setup report](evidence/postgres-soak-ci-setup-2026-10-26.json).

## Confirmed routing follow-up

The ARC scale set belongs to `Anacreonsystems`, while the repository initially
belonged to the personal account `7architect`. The requested runner name alone
cannot grant cross-owner access. A user-proposed transfer to
`Anacreonsystems/ariadne` was attempted through the GitHub API and rejected with
HTTP 403 `Resource not accessible by integration`; ownership was not changed.

For manual transfer, open repository Settings, General, Danger Zone, Transfer
ownership and select `Anacreonsystems`. Then verify the destination and delivery
branch, update the local remote, and allow the repository in the organization's
runner group if that group restricts repository access. Do not rewrite historical
test reports or claim hosted CI passed as a consequence of transfer alone.

After the user supplied the runner inventory,
[run 40335123178](https://github.com/7architect/ariadne/actions/runs/40335123178)
requests the exact ARC selector `anacreon-runners`. At the last observation it
is queued without an assigned runner. The old static-selector run was
cancelled. Repository eligibility/runner availability remains an external
CI follow-up; the local short release gate is independently complete. The
72-hour profile still targets the static runner, verifies `k8s-runner`, and
has not been dispatched.

## Transfer and static-analysis repair follow-up

The user completed the transfer on 2026-10-26. The verified repository is now
[`Anacreonsystems/ariadne`](https://github.com/Anacreonsystems/ariadne), with the
same repository ID and preserved branch tips. The local remote follows the new
owner. [Smoke attempt 2](https://github.com/Anacreonsystems/ariadne/actions/runs/40343803001)
was queued without an assigned ARC host at the 2026-10-27 investigation.

The failed pull-request check was
[Static analysis](https://github.com/Anacreonsystems/ariadne/actions/runs/40349113436),
specifically repository hooks; its type-check job passed. The
[repair report](evidence/ci-static-analysis-repair-2026-10-27.json) distinguishes
local hook/regression results, publication and hosted check outcomes.
[ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md) documents the exact
immutable-file exceptions and the required successor source-hash gate.

The repair's
[static-analysis run](https://github.com/Anacreonsystems/ariadne/actions/runs/40484377266)
passed both pre-commit and type checking at commit
`6d03a45e28c009f384c09cdbcfb3057432ea2e99`. The obsolete queued smoke was
cancelled before publication; its
[replacement](https://github.com/Anacreonsystems/ariadne/actions/runs/40484370723)
was queued for `anacreon-runners` without an assigned host at that observation.
The 72-hour profile has not been dispatched.

## ARC bind-mount readiness

The user changed runner-group access and actual ARC jobs were assigned.
Attempt 2 reached Docker/database/image bootstrap but failed to publish status
to the host; the [repair record](evidence/ci-arc-bind-mount-repair-2026-10-27.json)
preserves its failure and the filesystem hypothesis.

The one-job smoke uses `$RUNNER_TEMP/ariadne-soak` in the ARC shared work volume.
The static 72-hour profile continues to use `/var/tmp/ariadne-soak` across jobs.
Before bootstrap, the workflow verifies host-to-container and container-to-host
file visibility with an ephemeral pinned Python container. A selected-root
failure stops provisioning. The former `/var/tmp` path is a nonfatal smoke
control; neither outcome is assumed in advance.

Read `SOAK_BIND_MOUNT_PROBE_JSON` in the job log and `bind_mount_probe` inside
the exported `ci-status.json`. The public record contains root classes and
bounded outcomes without the nonce. It is copied after public export so the
export directory reset cannot remove it. The deterministic smoke review and
owned cleanup must still pass; a mount probe alone does not earn acceptance.

The [first repaired-root ARC run](evidence/ci-arc-smoke-observed-40577088311.json)
confirmed that boundary and completed recovery/export/cleanup, but its reviewer
failed on hourly growth extrapolations from a cold minute-long run. It remains
a failed observation. The reviewer now declares its resource policy explicitly:
smoke enforces all existing absolute caps and reports rates descriptively;
non-smoke also enforces the unchanged hourly rate caps. All other checks remain
mandatory. See [ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md) for
measurements, controls and fresh-run acceptance requirements.

## Verified ARC smoke (2026-10-27)

[Run 40578404646](https://github.com/Anacreonsystems/ariadne/actions/runs/40578404646)
passes on `anacreon-runners-runner-xjl9q`: shared-root probe, readiness,
scheduled process loss, watch segment, four independent oracles, reviewer,
complete credential-free export and owned cleanup all pass. The
[retrieved observation](evidence/ci-arc-smoke-observed-40578404646.json) and
[Principal review](evidence/ci-arc-smoke-principal-review-2026-10-27.json)
record the source identity and measured limits. The selected shared root is
visible; the old `/var/tmp` root is not visible to the ARC Docker daemon.

The reviewer explicitly reports `smoke_passed=true` and
`long_soak_passed=false`. Earlier readiness/growth failures remain preserved.
The ZIP archive could not be downloaded through the current connection
(HTTP 403); the retrieved job logs, exporter result and uploaded-artifact
metadata provide the acceptance evidence. Actual 72-hour execution on the
static runner remains a separate open step.

## Long-profile dispatch after merge (2026-10-27)

PR #1 is merged and the workflow is active on `main` at
`3776be1382d3d047485c6f96a14f9a5c1a1cb458`. The
[72-hour workflow](https://github.com/Anacreonsystems/ariadne/actions/runs/40601620025)
was manually dispatched with campaign ID `gh-40601620025-1`. Its prepare job
was queued for `self-hosted` without an assigned runner in the
[recorded observation](evidence/ci-long-soak-dispatch-40601620025.json).
The existing `k8s-runner` name guard and persistent-root checks still apply.

Do not start a duplicate campaign or rerun all jobs merely because this one
is queued. Assignment is verified from the actual job, and measured workload
duration begins at service startup, not dispatch time. This is a dispatch
record, not a long-duration PASS. Follow [ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md)
for acceptance and retained evidence.

## User-requested ARC retrigger (2026-10-28)

The user requested a fresh trigger and confirmed `anacreon-runners`. The
[unassigned long run](evidence/ci-long-soak-cancelled-40601620025.json) was
cancelled before bootstrap, releasing the workflow concurrency group. Its
initial dispatch record remains unchanged. The fresh
[main-branch smoke](https://github.com/Anacreonsystems/ariadne/actions/runs/40650444560)
passed on `anacreon-runners-runner-vf9jp`, including process recovery, all four
oracles, review, public export, artifact upload and owned cleanup. Its
[retrieved observation](evidence/ci-arc-smoke-observed-40650444560.json)
reports `smoke_passed=true` and `long_soak_passed=false`.

The current 18-job long profile still requires the existing persistent static
runner. Retrying it cannot route it to the distinct ARC scale-set selector.
An ARC long profile requires a separately verified workload/state and late
evidence-publication contract; changing only `runs-on` does not establish it.
See the [Principal assessment in ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md).
