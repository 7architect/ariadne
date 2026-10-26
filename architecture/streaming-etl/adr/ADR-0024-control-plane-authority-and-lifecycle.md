# ADR-0024: Control-plane projections, leadership and run lifecycle

- Status: `progress`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, orchestration maintainers
- Depends on: [ADR-0001](ADR-0001-runtime-boundary.md), [ADR-0022](ADR-0022-postgresql-routed-join-and-regional-recovery.md), [ADR-0023](ADR-0023-bounded-retention-and-replay-horizons.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-040a…d, W31–W33 in [todo](../todo.md) and [sprint board](../sprint_board.md)

## Problem

A durable stream can recover correctly while its orchestration API displays an
invented cursor, accepts a command from an expired leader, or declares a run
drained while a downstream operator still has work. Tenant identity and run
lifecycle must participate in the same authority graph as the existing workers
and channels. A second independent progress ledger would create conflicting
answers after a server crash.

## Theory

The control plane owns desired lifecycle and command identity. Durable channel,
worker and checkpoint tables own actual progress. Read projections from the
authoritative backend and serialize lifecycle changes with its revision lock.
Lease epochs fence control writers; durable commands make lost acknowledgements
retryable through a replacement server.

## Proposed solution and agreement

Implement an isolated Python HTTP server and PostgreSQL control service outside
Prefect core. Reuse the frozen R3 data plane. The server stores tenant/deployment
bindings, scoped capability-token hashes, leader leases, run intent and command
outcomes. It reads worker owners, input cursors, barriers, recoveries and pressure
from the existing authority. It never copies or advances an input cursor merely
to satisfy an API request.

Bind each run to one immutable revision and each physical partition to its run
scope. Reject another tenant or conflicting deployment identity before a data
or control mutation. Random bearer capabilities are trusted bootstrap in this
prototype; plaintext tokens must not be persisted or returned in projections.

Each lifecycle/maintenance control command validates the current tenant/capability and live leader
epoch, freezes an exact request identity and publishes its result in one
transaction. An exact retry returns the previous result; a reused command ID
with different content is a conflict. Stale leaders may serve read projections
but cannot change lifecycle or worker authority.

Capability validity is rechecked inside the mutation transaction under a shared
row lock, including expiry and revocation. The leader row stays locked from its
epoch check through command commit, so a concurrent takeover cannot slip between
those points. One canonical DRAIN and one canonical CANCEL command bound the
command ledger; a different request ID for an already published intent is a
conflict. Exact retries of the original command retain their original result.

Deployment binding claims exclusive ownership of **all** source and output
partitions, including intermediate and leaf output. The ownership row records
tenant, revision and SOURCE/OUTPUT role. Protecting sources alone would allow
two otherwise distinct tenants to mix output records in one physical partition.

### Source admission and bounded control metadata

Source admission/publication uses its separate source capability and frozen
reservation rather than controller leadership. The prototype permits exactly
one structured producer per owned source partition, bounding producer summaries.
Legacy consumer bindings are the exact installed source-consumer manifest;
legacy consumers of intermediate/output partitions are explicitly unsupported.
The native R3 operator graph retains its own complete revision-bound inputs.

Reserve exact source identity, ObjectRef metadata and structured producer
coordinates while admission is open. The durable reservation contributes to the
drain cut and has hard record/declared-payload-byte credits. Native source append
validates the reservation; an AFTER INSERT trigger marks it PUBLISHED in the
same transaction as DATA. The HTTP publication route uses that same authority,
so response loss cannot leave accepted DATA with an uncommitted control receipt.

Retire a PUBLISHED admission only after the existing R3 metadata summary covers
its exact source sequence. Advance the bounded producer horizon and return the
admission credits atomically. ADMITTED reservations pin their work; they are not
silently discarded. A producer coordinate below the retired horizon receives an
explicit expired result. Arbitrary old opaque request IDs do not gain indefinite
deduplication beyond that horizon, consistently with
[ADR-0020](ADR-0020-bounded-metadata-and-idempotency-horizon.md).

### Drain

Close source admission and capture admitted source watermarks under the same
lock used by admission. Every already accepted record remains processable.
Drained is a serialized fixed point: every participant input has completed its
current DATA and ordered-log tail, there is no active invocation or blocked/pending mailbox,
every admitted checkpoint has completed or aborted, and no recovery generation
is restoring. A source count cannot be compared
directly to a leaf count: joins, fan-out and terminal `return` change that
relationship. A restarted leader reconciles the same durable intent.

### Cancel

Publish cancellation and fence affected worker generations atomically. Old
client Python may still finish an external action, but its terminal platform
write must be rejected. New claims or steps must not reactivate the cancelled
run. External-effect idempotency remains client-owned under
[ADR-0011](ADR-0011-yield-completion-and-client-effects.md).

Cancellation after drain intent must close every reserved publication; an old
drain cut is not authorization to append after CANCELLED. DRAINED is terminal
and a later CANCEL receives an explicit conflict. Authorized R3 metadata
retirement can still reclaim a completed prefix after a terminal fence. An
unprocessed cancelled suffix remains pinned within the hard capacities; a
separate discard policy would be needed to purge it. This preserves replayable
bytes instead of silently treating cancellation as completion.

### Integration boundary

Activation freezes `execution_mode`: NATIVE_JOIN or LEGACY_SOURCE. Native mode
requires the complete installed participant/input/configuration manifest, sealed
regional topology and every enabled control guard. Seeing any one installed
worker is insufficient: A may finish its source while an uninstalled B still
has all downstream work, producing a false DRAINED result.

The supported native bootstrap order is full R3 protocol schema and operator
installation, regional topology binding, control-plane installation, then
deployment/run activation. Missing guards fail closed. Runtime registration is
sealed after activation. The legacy mode has no native workers and binds the
exact installed source-consumer manifest; it does not acquire native graph
semantics by installing more components later. Protocol DDL is trusted
provisioning, not a worker operation; the later credential gate enforces that
separate boundary.

These lifecycle states are streaming control intent. In the following R4-B
adapter they must be reconciled with actual Prefect flow/run states through the
Prefect API. This prototype does not replace Prefect's orchestration authority
or claim that its internal lifecycle rows are already Prefect run states.

## Stories

- As a developer, I want to query run, partition, owner, input cursor, barrier,
  pressure and recovery reason from one authorized API.
- As a user, I want a server replacement to preserve the same progress and the
  result of a command whose response was lost.
- As a user, I want drain to finish admitted work and cancel to fence further
  platform publication with an explicit boundary.
- As a system, I want expired leaders and another tenant's capabilities to be
  unable to control this run or its channels.

## Acceptance criteria

- As a developer, I can compare a real HTTP projection with the independent
  PostgreSQL authority and get the exact durable owner/cursor/checkpoint state.
- As a system, I reject cursor-only control writes, deployment identity drift,
  stale leader mutations, forged/revoked or insufficient capabilities and
  cross-tenant resource access without changing durable progress.
- As a user, I got one command outcome after duplicate submission, response loss,
  abrupt server death and fresh-server lease takeover.
- As a user, I can keep processing admitted DATA during drain; later source DATA
  is rejected, and the complete DAG reaches DRAINED only at its true fixed point.
- As a system, I reject a cancelled worker's terminal output and any later claim
  that attempts to reactivate the cancelled run.
- As a developer, I can reproduce lifecycle recovery through real sockets and
  fresh processes, with independent authority checks and no Prefect core edits.

## Review questions and limits

Verify admission/control lock order against R3 revision→worker serialization.
Check fan-out, join and `return` in the drain proof. Test in-flight client Python
across cancellation and takeover. Every control projection must expose the
actual source of its state rather than a private optimistic counter.

The prototype uses a trusted provisioning boundary and one revision-bound run
scope. External IAM, transport deployment, credential rotation, infrastructure
security review and production operations remain later acceptance work. Direct
worker database credentials require the separate DS-051c review; HTTP tenant
authorization alone is not evidence for that boundary.

## R4-A prototype evidence and gate result

[postgres_control_plane.py](../prototypes/postgres_control_plane.py) stores
intent, capability, leader and bounded admission authority.
[postgres_control_plane_server.py](../prototypes/postgres_control_plane_server.py)
serves real JSON HTTP sockets; [the client](../prototypes/postgres_control_plane_client.py)
uses those sockets. The [independent oracle](../prototypes/postgres_control_plane_oracle.py)
imports no mutation implementation and verifies scope, exact manifests, durable
credits, lifecycle fixed points and projected authority fields.

[The focused cases](../prototypes/test_postgres_control_plane.py) cover
revocation/expiry, stale leader serialization, concurrent tenant output
ownership, exact legacy/native runtime coverage, missing guards, reserved
source publication across drain/cancel, native regional cancellation, legal
post-terminal metadata maintenance and real process death after command commit
before HTTP response. Fault injection requires an explicit isolated-server
flag and is disabled in the normal server.

[The HTTP composition test](../prototypes/test_postgres_control_plane_streaming.py)
executes 100 real source admission/publication → native join → checkpoint →
retirement cycles. Native full-schema bootstrap is included. The whole-schema
census retains 62 rows at cycles 25/50/75/100 and measures
25,194 / 25,207 / 25,220 / 25,303 JSON bytes under a declared
512-row/256-KiB profile. Hot admissions return to zero; structured producer
retries below the retained horizon fail explicitly. This is logical storage
evidence, not a physical-disk or new throughput claim.

[The final candidate](../evidence/postgres-r4a-candidate2-2026-10-25.json) passes
110/110 tests in 444.602 seconds on PostgreSQL 17.11 `remote_apply` with a
synchronous streaming replica. Its raw log and 92 source hashes are bound to
the report with no source drift. The earlier 109-test candidate is retained,
but it did not cover the partial-native/bootstrap review findings and awarded
no readiness.

[The independent Principal PASS](../evidence/postgres-r4a-principal-2026-10-25.json)
verified the report/log/source hashes and reran five decisive controls. It
closes DS-040a…d and W31–W33 at **83%** on 2026-10-25. Those hashes identify this
gate's implementation; later adapter changes require their own successor gate.

## Evidence required for `accepted`

The DS-040a…d prototype gate, actual Prefect integration, production authorization
and HA deployment, operational tests and final release acceptance must all be
linked. The R4-A prototype gate has earned 83%; this ADR remains `progress`
until the broader production evidence exists.
