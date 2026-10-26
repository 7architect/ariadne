# ADR-0026: Release compatibility and disaster recovery

- Status: `progress`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0023](ADR-0023-bounded-retention-and-replay-horizons.md), [ADR-0024](ADR-0024-control-plane-authority-and-lifecycle.md), [ADR-0025](ADR-0025-prefect-adapter-and-operational-projections.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-050a…d, W37–W39 in [todo](../todo.md) and [sprint board](../sprint_board.md)

## Problem

Process replacement over live storage does not prove release compatibility or
recovery from a database backup. A restored copy can coexist with a newer live
writer. An older worker can interpret a changed schema incorrectly. A completed
terminal write whose response was lost must remain one platform result when
the client reconnects after a network failure.

## Theory

Explicit compatibility contracts, transactional additive migrations, fenced
cutover, and recovery against independently checked durable authority can keep
upgrades and restores inside the existing protocol. Rejecting an unsupported
combination before DATA is a valid result; silently accepting it is not.

## Proposed solution for discussion

Keep all experiments outside Prefect core. Use actual PostgreSQL connections,
real process loss and an actual dump/restore or isolated physical copy. Declare
the source and target schema/runtime versions, supported read/write ranges,
and code/state manifests before a rollout starts. A migration must either commit
its schema and compatibility marker together or leave the old state usable.
Do not present an arbitrary new diagnostic column as evidence of a complete
historical platform migration.

Prefer a narrow, executable upgrade from the selected control-plane baseline
to the actual attempt/maintenance integration. Preserve existing source IDs,
logical run, managed state and completed coordinates. Older incompatible
writers must fail before changing those authorities. Document whether the
rejection relies on trusted runtime code or a database privilege boundary;
credential isolation is separately verified by DS-051c.

Rollback means reconnecting a compatible older application while retaining
durable data, or explicitly refusing an incompatible downgrade. It must never
erase a completed yield, rewind an externally observed source identity or
reactivate an old owner. Application rollback is different from restoring an
earlier database backup.

Before a restored database becomes writable, fence the previous execution
infrastructure and rotate its authority. State the backup recovery point and
which later acknowledged records are outside that point. A local backup does
not provide zero data loss after the backup time. Verify restored channel,
object, control, attempt, checkpoint and join state with independent oracles.
Resume the first incomplete input from the restored authority, subject to the
client's external-effect idempotency responsibility.

The fault campaign should cut a real database connection before a terminal
transaction and after commit before the response, stop an execution process,
interrupt a maintenance checkpoint/retirement operation, and corrupt or remove
an ObjectRef in a disposable copy. Supported recovery must converge; unavailable
authority must yield the documented boundary error. Host-loss evidence uses an
isolated PostgreSQL pair, never the shared test primary or another task's data.

## Stories

- As a developer, I want to declare supported runtime/schema combinations and
  upgrade the actual integration without reinterpreting completed turns.
- As a user, I want an interrupted upgrade or lost response to preserve my
  platform outputs and input positions.
- As an operator, I want a tested backup cutover procedure with an explicit
  recovery point, fencing steps and recovery limits.
- As a system, I want incompatible writers and missing replay authority to fail
  before they can create misleading progress.

## Acceptance criteria

- As a developer, I can run the supported old/new combination against a real
  schema, and I got a deterministic pre-DATA rejection for an incompatible one.
- As a system, I got all migration effects and the release marker together or
  none after injected interruption, with unchanged durable stream coordinates.
- As a user, I got one platform terminal result after reconnecting across an
  unknown transaction outcome, with external-effect limitations documented.
- As an operator, I can restore an actual backup into an isolated target,
  verify its exact recovery point, fence old actors and resume its incomplete
  suffix without claiming recovery of data newer than the backup.
- As a system, I reject a corrupt/missing retained ObjectRef and cannot advance
  a recovery cursor by inventing a replacement payload.
- As an operator, I can follow the linked runbook and reproduce its restore,
  drain and fault-handling checks from archived commands and evidence.

## Agreement and evidence

ETL/HighLoad Principal reviewed the proposal on 2026-10-25. The direction is
agreed for planning, with dependent implementation blocked until R4-B closes.
No R5-A readiness is earned by this proposal.
The actual 72-hour soak belongs to DS-051a and cannot be replaced by simulated
clock advancement, a shorter load test or an expectation of future success.

The agreed release design uses a durable release head, migration ledger and
runtime sessions. A live session binds epoch/schema/features/runtime identity;
every protected authority write must resolve it against the current release
fence inside the same transaction. A connect-time compatibility check alone is
insufficient: an old connection can remain alive across cutover. Additive EXPAND
may retain both versions only when their write semantics match. Semantic
CUTOVER revokes incompatible sessions atomically; CONTRACT waits for their
leases and retained recovery horizon. A session ID supplied through a GUC is a
lookup key, not permission to invent authority; hostile database credentials
remain DS-051c's separate boundary.

The migration experiment must use the actual pre-attempt control-plane schema
and install the selected attempt/maintenance tables and hooks transactionally,
with an immutable from/to/DDL-digest identity. It must exercise an old connection
that checked compatibility before cutover, forged/expired sessions, feature
drift, interrupted DDL and incompatible downgrade.

The selected backup experiment is a schema backup with an exported PostgreSQL
snapshot and a manifest of its LSN, exact stream/control/checkpoint/attempt cuts
and digest. Restore goes to an isolated target. Its writable cutover requires
executable exclusive authority and proof that the old execution endpoint or
credentials are fenced. A supplied boolean assertion is not proof. The experiment
does not establish whole-cluster disaster recovery or zero RPO; infrastructure
failover beyond the selected profile must stay an explicit limitation.

The follow-up Principal discussion fixes the writable-restore boundary more
strictly. A live backup may be inspected read-only with an explicit RPO, but it
must not be activated for writing if DATA or completed cuts advanced after its
snapshot and no supported source rewind can recover them. The supported writable
path first quiesces release authority while leaving the logical run ACTIVE:
`ACTIVE → QUIESCED` atomically under the release-head lock. The selected prototype
first requires no active invocation, RESTORING/global/regional recovery or
incomplete maintenance mutation. A busy state returns `ReleaseQuiesceBusy`
without changing head/session authority. Only then does quiescence increment
the fence and reject old sessions. Every writer holds the same head lock through
its transaction. Hold the exporting REPEATABLE READ transaction open through
`pg_dump --snapshot`. Durable pending DATA may remain in the snapshot and is the
suffix to resume. A two-phase QUIESCING protocol was considered but deferred:
blocking all writes before waiting would strand persisted invocations, while
selectively allowing their completion needs separate protocol/crash evidence.

After the dump, a narrowly scoped source-finalization operation must verify
unchanged DATA/cursor/checkpoint cuts, cancel the original logical run, revoke
its actors/capabilities and make the original release `CUTOVER_RETIRED`.
Quiesced authority admits no unrelated mutation. A separate control registry
outside the backed-up schema then issues one immutable restore grant, bound to
the logical deployment/revision/run, backup digest, source and actual target
PostgreSQL system identifiers, target schema and exact cuts. Concurrent targets
must yield one grant winner and zero losing-target activation/session rows.

The restored copy contains an ACTIVE logical run behind a QUIESCED release
guard. It remains unwritable until a storage-checked local witness of the bound
grant admits a fresh target generation/session, followed by replacement of
attempt/source/worker authority. This does not reopen a CANCELLED logical run.
An exact same-target activation retry is idempotent; a different target cannot
reuse the grant. The grant has no timeout takeover: losing its winning target
makes that backup non-reactivatable elsewhere in this selected profile. Registry
unavailability fails closed. This is a deliberate availability limit and not a
general distributed failover protocol.

## Failure modes and consequences

Direct DDL remains a privileged administrative action. A compatibility check
inside a trusted Python client is not a hostile-worker security boundary.
Restoring stale database state while a previous writer remains live creates
split authority; it must be rejected operationally and by the selected fence.
Backup recovery may replay client effects already visible externally. Those
effects still require client-owned idempotency under
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md).
