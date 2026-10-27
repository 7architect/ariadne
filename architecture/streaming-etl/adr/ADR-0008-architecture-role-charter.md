# ADR-0008: ETL Architect / HighLoad Principal role charter

- Status: `planning`
- Date: 2026-10-19
- Owner: ETL Architect / HighLoad Principal
- Governs: [ADR graph](README.md), [failure evidence](ADR-0009-failure-injection-evidence.md)

## Problem

The streaming initiative spans distributed correctness, storage, load behavior,
and integration with Prefect. An unrestricted architecture role would dilute
ownership and make decisions hard to review.

## Theory

A narrowly scoped ETL Architect / HighLoad Principal provides a consistent safety
and performance review without taking ownership of unrelated product areas.

## Agreement

The role owns architecture review for:

- stateful streaming ETL and DAG execution;
- checkpoints, replay, recovery domains, and fencing;
- durable channels, state backends, backpressure, and capacity planning;
- durable-yield completion, consumer progress, and client-effect boundaries;
- failure injection, performance prototypes, and production acceptance evidence.

The role does not own UI design, branding, general Prefect features, business
workflow semantics, or connector product prioritization. It can block a guarantee
claim that lacks protocol or load evidence. Product and implementation owners
still decide scope and delivery.

The role reviews every status transition to `planning`, `progress`, `accepted`,
or `rejected`, but an ADR reaches `accepted` only with production code and all
acceptance evidence.

## Stories

- As a developer, I want one accountable reviewer for streaming correctness and
  load assumptions.
- As a user, I want reliability claims reviewed against explicit failure cases.
- As a system, I want safety invariants tested independently from integration
  code.

## Acceptance criteria

- As a developer, I can identify the architect review and unresolved objections
  for every runtime ADR.
- As a user, I got guarantee language that distinguishes safety, liveness, and
  transient visibility.
- As a system, I can trace every high-load target to a benchmark or pending
  experiment.

## Initial review record

The first review rejected silent replay conflicts, mutable completed snapshots,
mutable checkpoint participant sets, and reuse of multi-sink transaction IDs with
different intent. Counterexample tests were added before these ADRs advanced.

