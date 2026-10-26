# ADR-0029: Short capacity calibration and release scope

- Status: `accepted`
- Date: 2026-10-26
- Owners: Runtime maintainers, architecture review
- Depends on: [ADR-0027](ADR-0027-short-acceptance-and-separate-long-soak.md), [ADR-0018](ADR-0018-postgresql-authoritative-data-plane.md), [ADR-0025](ADR-0025-prefect-adapter-and-operational-projections.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-051a…d in [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

The short native Prefect test measures 100 small documents and retained storage
but does not hold a branch back while ingress continues. It cannot establish
catch-up capacity. A historical storage-only throughput result cannot be used
as the capacity of the current end-to-end graph. A recovery estimate fitted to
the same recovery interval that validates it would be circular evidence.

## Theory

A declared short workload can calibrate net service capacity on one earlier
backlog, then predict recovery from a later backlog while ingress continues.
Hard record and byte credits determine admission saturation independently of
physical disk allocation. Schema relation growth and WAL insertion are distinct
observations; neither alone establishes retained WAL or filesystem exhaustion.

## Proposed solution and agreement

Run the actual Prefect source and two long-lived branch tasks on the synchronous
PostgreSQL profile. Use 20 deterministic 64 KiB documents at declared ingress
one document per second. B performs terminal RETURN, and C yields uppercase
output. B's first bounded stall establishes an independent calibration window.
A later stall at input 10 consumes the four-record admission limit. Release B
and observe catch-up before the source closes. Periodic maintenance returns
credits through the normal barrier/retirement protocol.

Before evaluating the later recovery, use the earlier calibration:

```text
net_service = calibrated_branch_completion_rate - calibrated_live_ingress
recovery_time_to_target = (later_backlog - declared_target_backlog) / net_service
record_credit_time = remaining_record_credits / declared_ingress
byte_credit_time = remaining_byte_credits / (declared_ingress * payload_bytes)
conditional_headroom_time = declared_headroom / positive_observed_growth
```

Nonpositive net service is a refusal to predict recovery. The measured later
recovery validates the calibration with a declared timing tolerance; it is not
used as the prediction denominator. The relation-headroom estimate and the
hypothetical retained-WAL estimate must be published separately. WAL insertion
is not disk residence. A selected 1 GiB headroom is a model input, not a claim
about available production space or time until its actual exhaustion.

The declared target backlog is one in-flight document. A continuously moving
source need not expose a stable instant with exactly zero backlog to a polling
observer. The later backlog must decrease by at least two while accepted input
advances; after this finite source finishes, both branches must complete all
20 inputs. This separates steady-state catch-up from final drain.

Declare independent bounds for payload accounting, JSON encoding, row count
and physical measurements. A 64 KiB bytea encoded into JSON can use more than
64 KiB, so the small-document test's JSON budget is not reused blindly. The
selected live partition payload budget is 768 KiB; the JSON observation budget
is 4 MiB. These are profile limits, not global platform defaults.

## Stories

- As a developer, I want stalled-branch recovery to be tested through real
  Prefect tasks, PostgreSQL state and normal maintenance.
- As an operator, I want a capacity estimate to identify its calibration,
  assumptions, units and measured error.
- As a user, I want short acceptance to publish its actual scope without
  replacing a long-duration or production-capacity result.

## Acceptance criteria

- As a system, I got at least three pending branch inputs and the hard four
  admission credits consumed while the source remains open.
- As a system, I can resume the held branch and catch up while new input is
  admitted, reducing backlog by at least two to at most one in-flight document,
  with positive independently calibrated and observed net service.
- As an operator, I can compare the later measured recovery with the earlier
  prediction, including the declared tolerance and credit-saturation timing.
- As a developer, I got all 20 inputs completed by both branches, bounded rows,
  JSON and payload accounting, and six independent oracle checks passing.
- As an operator, I can distinguish relation allocation, WAL insertion and
  conditional headroom estimates from actual filesystem/WAL-retention behavior.
- As a user, I got source hashes, exact commands, raw evidence and a final
  review; no short result is labelled a passed 72-hour test.

## Evidence

The initial full [R5-A candidate](../evidence/postgres-r5a-candidate-2026-10-26.json)
passes 159/159 tests without source drift. It predates this capacity experiment.
Its scope and the [recovery runbook](../recovery_runbook.md) remain separate.
Capacity integration, measurement, the successor complete gate and independent
controls now pass for DS-051a…d. Rejected trials remain evidence.

The [focused successor](../evidence/postgres-capacity-acceptance-short-final4-2026-10-26.json)
passes 5/5 checks, including the actual workload and six independent oracles,
with no source drift or unclosed-socket warnings. It retains every rejected
trial by hash. Measured pending input changes from three at saturation to one
while accepted input advances; both branches finish all 20 documents and final
partition credits return to zero. Calibration predicts 3.032 seconds for the
declared backlog reduction; the observed 6.189 seconds remains within the
predeclared two-times-plus-0.5-second tolerance. The raw observation and its
error are published rather than labelled an exact production forecast.

The configured one-second source delay is an ingress ceiling, not a guaranteed
one-record-per-second service SLO. The report retains actual admitted rate and
sample-boundary limitations. If admission was already at its hard limit at the
baseline, saturation ETA is zero. Relation allocation and WAL insertion remain
separately labelled conditional headroom estimates.

The [first full successor](../evidence/postgres-r5b-final-2026-10-26.json) is
rejected and interrupted after its capacity cadence assertion failed. It is not
a completed regression result. Its failure exposed that sampler timestamps
cannot enforce an exact publication-rate ceiling. The successor measurement
uses the durable admission publication timestamp for each accepted sequence.

The focused experiment exposed a live invocation racing join retirement. The
successor uses a typed `JoinRetirementBusy` refusal and retries the same completed
checkpoint's retirement after the invocation finishes. Other protocol errors
remain fatal; no input progress or retirement frontier is invented. A
deterministic regression checks exact-checkpoint retry.

The [SDK storage-discovery diagnosis](../evidence/prefect-storage-discovery-probe-2026-10-26.json)
reproduces an unclosed synchronous HTTP client with a plain Prefect flow/task
and no streaming platform. The selected fixture now persists a LocalFileSystem
block and explicitly configures it through the public SDK. That bypasses repeated
automatic server-default discovery while adapter result persistence remains
disabled. The configured minimal probe has no socket warning. Prefect core is
unchanged; native long-duration resource stability remains unverified.

## Consequences

Completion means the declared short experimental delivery is accepted, not
general production readiness. Technical ADRs retain their broader unresolved
criteria. Long-duration behavior, general connector isolation, production
filesystem sizing and unsupported recovery combinations remain explicit limits.
The separate [DS-052a campaign](../soak_ci.md) contributes no percentage to this
short delivery.

## Accepted short-scope evidence

The [final architecture review](../evidence/postgres-r5b-architecture-review-2026-10-26.json)
records `PASS_SHORT_EXPERIMENTAL_100`, verifies all 126 current source hashes
against the full gate, and closes the four DS-051 acceptance tasks. The
[final documentation check](../evidence/adr-graph-final-2026-10-26.json) validates
the accepted status entries and local evidence links.

The [complete successor gate](../evidence/postgres-r5b-final2-2026-10-26.json)
passes 164/164 tests with no source drift, synchronous PostgreSQL durability,
and no unclosed-socket warnings. The
[independent controls](../evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
pass 7/7 hostile checks with unchanged sources. The
[environment inventory](../evidence/postgres-short-acceptance-environment-2026-10-26.json)
records actual dependency and image versions. Acceptance is limited to the
selected experimental scope in this ADR; it does not establish general
production readiness or a passed 72-hour campaign.
