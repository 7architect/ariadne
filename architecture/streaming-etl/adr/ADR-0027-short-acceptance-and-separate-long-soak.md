# ADR-0027: Short acceptance and a separate long soak

- Status: `accepted`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0012](ADR-0012-protocol-epoch-2-delivery-gates.md), [ADR-0026](ADR-0026-release-compatibility-and-disaster-recovery.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-051a…d and separate DS-052a in [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

The previous plan made an actual 72-hour soak a blocking final acceptance
criterion. This duration was added by the architecture plan. It was not a
technology requirement or an original user constraint, and it unnecessarily
coupled completion of the experimental implementation to a three-day wait.

## Theory

A short, explicit acceptance profile can establish the measured behavior of
the experimental implementation. A separate long soak can subsequently test
slow resource accumulation and sustained behavior. Their results must retain
their actual duration and scope.

## Decision and agreement

On 2026-10-25 the user explicitly selected: “Короткая приёмка сейчас, 72 часа —
отдельная проверка”. This replaces the blocking 72-hour criterion in the current
delivery plan. DS-051a now runs short acceptance with declared workload, fault
boundaries, capacity measurements and independent oracles. DS-052a retains the
actual 72-hour soak as a separate follow-up with no contribution to the current
42-sprint percentage. Previous plans and evidence remain in the graph.

The short gate still requires real SQL/HTTP/Prefect behavior, complete regression
checks, release/restore fencing, resource bounds and an honest credential-boundary
review. A historical 60-second storage benchmark cannot establish the capacity
of a different end-to-end graph. Publish the measured profile and its limits.
No accelerated clock, shorter run or expectation of future success is reported
as a passed 72-hour soak.

Acceptance is a composition of independent real profiles, not a claim that one
execution exercises all boundaries. The native Prefect 100-document run uses
the R4 attempt fixture; release/session, signed backup restoration, physical HA
and closed-network operator isolation have separate tests and evidence. The
capacity/catch-up profile must exercise a stalled branch while ingress continues
and report backlog, observed net recovery, pressure and resource measurements.
Historical storage-only throughput cannot be substituted for that profile.

## Stories

- As a developer, I want to complete and inspect the experimental streaming
  implementation without waiting three days for a separate duration test.
- As a user, I want the reported guarantee to identify the actual tested workload
  and duration.
- As a system, I want the separate long soak to retain its unresolved acceptance
  criteria and evidence links.

## Acceptance criteria

- As a developer, I can reproduce the short release profile from archived
  commands, configuration, hashes and results.
- As a user, I got no 72-hour PASS claim from a shorter test.
- As a system, I retain DS-052a as a separate follow-up and keep its result
  independent of completion of the current experimental delivery.
- As a user, I can see the remaining production limits, including any unsupported
  worker execution or external connector security profile.

## Evidence

The initial [R5-A candidate](../evidence/postgres-r5a-candidate-2026-10-26.json)
passes 159/159 PostgreSQL tests, with frozen sources, the synchronous durability
profile and documentation checks. The successor short capacity profile is
recorded in [ADR-0029](ADR-0029-short-capacity-calibration-and-release-scope.md).
The final complete regression and independent controls have passed for
this selected short-delivery agreement. The separate long campaign remains
[DS-052a](../soak_ci.md), without a long-duration PASS claim.

## Consequences

The current delivery can finish after short acceptance. Long-duration behavior
remains unverified until DS-052a actually runs and passes. Existing ADRs with
broader production acceptance criteria keep their evidence-based status.

## Accepted short-scope evidence

The [final architecture review](../evidence/postgres-r5b-architecture-review-2026-10-26.json)
confirms that the short delivery is complete and the separate long campaign
remains explicitly unpassed.

The [complete successor gate](../evidence/postgres-r5b-final2-2026-10-26.json)
passes 164/164 tests with no source drift, synchronous PostgreSQL durability,
and no unclosed-socket warnings. The
[independent controls](../evidence/postgres-r5b-independent-controls-review-2026-10-26.json)
pass 7/7 hostile checks with unchanged sources. The
[environment inventory](../evidence/postgres-short-acceptance-environment-2026-10-26.json)
records actual dependency and image versions. Acceptance is limited to the
selected experimental scope in this ADR; it does not establish general
production readiness or a passed 72-hour campaign.
