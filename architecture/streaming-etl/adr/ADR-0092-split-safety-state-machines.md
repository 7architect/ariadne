# ADR-0092: Split lifecycle and progress state machines

- Status: `rejected`
- Date: 2026-10-20
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md), [ADR-0004](ADR-0004-multi-sink-commit.md), [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Superseded by: [ADR-0002](ADR-0002-checkpoints-and-recovery-domains.md) integrated protocol evidence
- Rejected prototype: [safety_contracts.py](../prototypes/safety_contracts.py)
- Replacement prototype: [integrated_protocol.py](../prototypes/integrated_protocol.py)

## Problem

The first Sprint 1 prototype added stable IDs, a checkpoint lifecycle, a combined
progress record, capability negotiation, fault names, and an oracle. Its 23 tests
passed, but review found that those pieces could disagree while each local test
remained green.

## Theory

A standalone checkpoint lifecycle plus a separate combined progress store can
jointly establish checkpoint, sink-decision, and source-acknowledgement safety.
Generic injection callbacks and an observation-only oracle are sufficient
evidence for the sprint contract.

## Proposed agreement

Keep lifecycle and combined progress in independent durable stores and establish
the safety claim by composing their local invariants.

## Agreement

The theory is rejected. A protocol has one durable authority for lifecycle and
commit decision. Capability claims are validated from the capability a
participant declares, independent of the guarantee requested by the deployment.
Failure evidence must run the actual protocol, reopen durable state, recover
external effects, and then invoke an independent oracle.

Source progress is a vector of opaque positions keyed by source partition.
Checkpoint sequences and recovery queries are scoped by job and graph version.
Fencing uses a participant-generation manifest plus a coordinator epoch.

## Counterexamples

- A sink could declare tier `T` without prepare/recover, idempotency, or fencing
  when the deployment requested only tier `B`; negotiation then reported a false
  tier `T` result.
- The first store selected `COMMIT_DECIDED` for recovery while its oracle accepted
  only `COMPLETED`, so the system and its verifier disagreed.
- Lifecycle and combined decision lived in separate tables with no atomic link.
- A global numeric source position and global checkpoint sequence mixed jobs,
  graphs, and partitions.
- The fault test exercised injector callbacks but did not recover the protocol or
  evaluate invariants at every named point.
- The oracle had no expected sink/source manifest, so missing required outputs
  could be invisible.

## Stories

- As a developer, I want failed protocol theories retained with reproducible
  counterexamples so I do not repeat them in production code.
- As a user, I want a green test count to represent end-to-end behavior rather
  than isolated helper behavior.
- As a system, I want one authoritative transition and decision lineage.

## Acceptance criteria

- As a developer, I can reproduce the false tier declaration and store/oracle
  disagreement from the rejected design.
- As a system, I got replacement tests for CAS transitions, participant fencing,
  source vectors, expected sink completeness, and every named crash point.
- As a user, I can trace the rejected theory to the replacement evidence without
  deleting the historical prototype.

## Evidence

[test_integrated_protocol.py](../prototypes/test_integrated_protocol.py) contains
the replacement capability matrix, exhaustive phase table, two-connection CAS
test, job/graph/partition scope test, unsafe controls, a 27-point boundary matrix,
and a 27-case legal phase-edge crash matrix. This ADR remains `rejected`
permanently; later production work may change the replacement ADRs without
rewriting this outcome.
