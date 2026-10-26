# ADR-0093: Implicit regeneration of missing durable output

- Status: `rejected`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal
- Refines: [ADR-0003](ADR-0003-branch-and-join-recovery.md)
- Superseded by the explicit boundary contract in [ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md)
- Related: [ADR-0023](ADR-0023-bounded-retention-and-replay-horizons.md)

## Problem and theory

The planner can include an upstream producer when a boundary is unavailable.
The rejected theory was that this alone also repairs a physically missing or
corrupt retained output. That would require an additional reconstruction
protocol; expanding the set of workers does not publish missing bytes.

## Proposed solution and counterexample

For A→{B,C}→J→D, remove or corrupt a retained C→J payload after a completed
checkpoint. The original planner expanded the recovery region to include C.
The executor then replayed C and required its historical output identity and
payload to match the retained platform witness. That check correctly rejected
the absent/corrupt record, leaving an already fenced region unable to finish.

A planner-only test that sealed the expanded region was therefore insufficient
execution evidence. A declared non-replayable edge with intact bytes is a
different supported case: the producer can restore from its retained mailbox
and verify its existing output while the larger region recovers.

## Agreement

Reject implicit repair of durable storage loss in this protocol epoch. Detect
missing/corrupt retained records, invalid roots and missing log authority before
publishing a recovery fence. Return `UnrecoverableBoundary` and preserve the
previous worker generations and control history. Policy-driven expansion remains
available when the required retained evidence is valid.

A future reconstruction protocol would need its own ADR, authenticated retained
identity, deterministic byte verification, atomic publication, stale-owner
fencing, downstream visibility rules and crash evidence. This decision does not
implement such a protocol or an external source rewind adapter.

## Stories

- As a user, I want a recovery refusal to name the unavailable retained boundary.
- As a developer, I want to distinguish an expanded recovery plan from proven
  reconstruction of its input and output bytes.
- As a system, I want unsupported recovery to leave ownership and authority
  unchanged.

## Acceptance criteria and rejection evidence

- As a system, I cannot demonstrate automatic byte reconstruction merely by
  expanding the plan; replay requires the original platform output witness.
- As a developer, I can inspect that failing code path in
  [postgres_recovery_regions.py](../prototypes/postgres_recovery_regions.py) and
  [postgres_regional_recovery.py](../prototypes/postgres_regional_recovery.py).
- As a user, I got this unsupported case recorded explicitly rather than counted
  as successful recovery.

The replacement refusal behavior is validated under
[ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md); its final
gate remains responsible for the executable regression and Principal review.
