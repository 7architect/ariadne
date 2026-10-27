# ADR-0000: Decision lifecycle and evidence

- Status: `progress`
- Date: 2026-10-19
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Related: [ADR graph](README.md), [todo](../todo.md), [sprint board](../sprint_board.md)

## Problem

Streaming correctness depends on failure semantics that are easy to state and
hard to prove. Replacing rejected documents erases why a theory failed and makes
the same unsafe design likely to return.

## Theory

An append-only decision graph, explicit status transitions, executable protocol
prototypes, and production acceptance evidence make architectural progress
auditable.

## Decision

Every material runtime decision is recorded in an ADR and linked to its
dependencies and dependants. ADRs are never deleted. A later decision supersedes
an earlier one by linking to it and changing its status when appropriate.

Status transitions use the definitions in the [ADR graph](README.md). In line
with the original requested workflow, `accepted` requires implemented code or a
prototype appropriate to the ADR's explicit scope and evidence for every
acceptance criterion in that ADR. Accepting an experimental scope does not
establish that a production system is complete. An ADR claiming production
scope must require production code and deployment evidence in its own criteria.

Before an ADR can become `accepted`, it must contain the problem, theory, agreed
decision, user/system/developer stories, acceptance criteria, evidence, failure
modes, consequences, and links to related ADRs. Earlier states may retain these
as explicit gaps while the theory is being discussed or tested.

## Stories

- As a developer, I want to trace a task to the decision that created it.
- As a user, I want published guarantees to identify their tested limits.
- As a system, I want incompatible decisions to be detected before deployment.

## Acceptance criteria

- As a developer, I can navigate from every streaming task to at least one ADR.
- As a user, I can distinguish planned guarantees from implemented guarantees.
- As a system, I can validate ADR status values and links in CI.
- As a developer, I got retained evidence for every rejected or superseded theory.

## Evidence required for `accepted`

- CI validates allowed statuses, required sections, and non-broken local links.
- The implementation PR template requires ADR and acceptance-evidence links.
- At least one ADR completes the full `pending → planning → progress → accepted`
  lifecycle without being rewritten or deleted.

## Consequences

Documentation work becomes part of the delivery definition. Status changes must
be evidence-based and reviewed like code changes.

## Prototype graph validation

[validate_adr_graph.py](../prototypes/validate_adr_graph.py) checks every retained
ADR's allowed status, agreement with the graph index, required sections for an
accepted ADR, and all local Markdown links. The current
[validation report](../evidence/adr-graph-2026-10-25.json) passes. The PostgreSQL
release harness runs the same check as a gate condition. This is a reproducible
local/release check; repository-hosted CI and the first fully production-accepted
ADR remain open acceptance items.

