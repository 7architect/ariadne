# Streaming ETL soak CI operator guide

Decision: [ADR-0028](adr/ADR-0028-self-hosted-soak-ci-and-reviewer.md).
The [harness](prototypes/ci_soak/README.md) runs one persistent workload; the
separate [short-delivery agreement](adr/ADR-0027-short-acceptance-and-separate-long-soak.md)
remains in force.

## Existing runner requirements

Use one of the three existing self-hosted runners. The selected profile requires
Linux x64, Python 3.11 or newer, a working local Docker socket accessible to the
runner account, outbound image/dependency access, and writable
`/var/tmp/ariadne-soak`. The job must run on a persistent host. The configured
containers can consume up to 5 GiB RAM and five CPU equivalents in total; allow
at least 20 GiB free disk for images, two PostgreSQL volumes and evidence. These
are campaign prerequisites, not a production capacity claim.

The long profile requires a custom label that selects exactly one existing
runner. Enter its real label, not its display name. The standard labels alone
cannot pin one of three machines. The harness records and verifies the runner,
machine and Docker daemon identity throughout the campaign.

## Start a short check

The [Streaming ETL soak workflow](../../.github/workflows/streaming-etl-soak.yml)
defaults to `profile=smoke` and `runner_label=self-hosted`. A push of the workflow
or harness on `codex/streaming-etl-adrs` also requests this smoke profile. It
selects an available existing Linux x64 runner and performs the complete short
start/recovery/review/export/cleanup sequence within that job.

The result must say `smoke_passed=true` and `long_soak_passed=false`. A successful
smoke does not close DS-052a or establish long-duration capacity.

## Start the separate long campaign

After the workflow is available for manual dispatch, choose `profile=72h` and
the existing unique runner label. Manual dispatch availability normally requires
the workflow to exist on the repository's default branch. Publishing a feature
branch alone does not establish that availability; the branch push smoke is a
separate trigger.

Bootstrap copies the exact committed source into an immutable Docker image.
The workflow then runs monitor segments 0 through 17 in an explicit dependency
chain. Each four-hour job obtains fresh GitHub credentials and uploads public
evidence. The service and database pair continue through job boundaries.
Scheduled process-loss injections occur in segments 5 and 11. Final review
checks actual elapsed duration, source/state checksums, faults, synchronous
replication, authority, pressure and measured resource limits.

Read the final job summary and the `streaming-soak-final-*` artifact. The harness
public files are `manifest.json`, `segments/segment-*.json`, `review.json`, and
`review.md`. CI adds a bounded `ci-status.json` containing only run/attempt
identifiers, runner name and job outcome. Failed or interrupted runs can export incomplete public evidence;
the reviewer still fails them. Never upload the full persistent directory or
`credentials.json`.

## Failure, retry and cleanup

The campaign ID is `gh-<GitHub run ID>-<GitHub run attempt>`. Rerun all jobs to
start a fresh campaign; old evidence is never counted as a fresh long test.
Individual completed segments are immutable within a campaign. Source and
machine identity must match. Retrying an individual monitor after the final
job stopped the workload cannot resume the campaign: rerun all jobs instead.
Time lost between segments remains visible to the reviewer.

On success the final job removes this campaign's Docker resources. On failure
it stops the workload but retains databases and public evidence. Cancellation
can prevent finalization. Use
[Streaming ETL soak cleanup](../../.github/workflows/streaming-etl-soak-cleanup.yml)
with the owning host's unique label and explicit campaign ID, then check the
job result. It removes only named resources from that campaign manifest and its
credentials file; it does not prune Docker globally or touch unrelated tests.

## Current execution status

- Workflow source, the explicit 18-job chain and the prototype dependencies are
  committed and published on `codex/streaming-etl-adrs` at commit
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
- Unique-label selection, a passing GitHub smoke, and actual 72-hour execution
  are not yet evidenced. `long_soak_passed` remains false. Observations are
  archived in the [CI setup report](evidence/postgres-soak-ci-setup-2026-10-26.json).
