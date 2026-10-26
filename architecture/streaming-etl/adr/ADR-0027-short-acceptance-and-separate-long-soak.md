# ADR-0027: Short acceptance and a separate long soak

- Status: `planning`
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

## Consequences

The current delivery can finish after short acceptance. Long-duration behavior
remains unverified until DS-052a actually runs and passes. Existing ADRs with
broader production acceptance criteria keep their evidence-based status.
