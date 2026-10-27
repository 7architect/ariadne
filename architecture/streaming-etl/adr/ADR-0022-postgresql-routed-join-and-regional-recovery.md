# ADR-0022: Routed barriers, durable join mailboxes, and regional recovery

- Status: `progress`
- Date: 2026-10-25
- Owners: ETL Architect / HighLoad Principal, runtime maintainers
- Depends on: [ADR-0021](ADR-0021-postgresql-coordinator-checkpoints.md), [ADR-0015](ADR-0015-independent-branches-and-durable-join-buffer.md), [ADR-0011](ADR-0011-yield-completion-and-client-effects.md)
- Refines: [ADR-0003](ADR-0003-branch-and-join-recovery.md)
- Validated by: [ADR-0009](ADR-0009-failure-injection-evidence.md)
- Delivery: DS-031a…d in [todo](../todo.md) and [sprint board](../sprint_board.md)

## Problem

The R3-A coordinator binds input vectors to source barriers. A multi-stage graph
also needs the barriers produced by intermediate operators. Without routed
authority, a join can form a coherent local vector that belongs to no graph
checkpoint. Source replay roots alone do not protect an intermediate edge.

An early barrier must pause application on its input while other inputs continue.
The post-barrier tail needs durable admission and deterministic drain order.
Worker loss between admission, generator execution, and terminal publication
must preserve the same first-incomplete input and reject a stale completion.

Regional recovery adds another constraint: for A→{B,C}→J→D, restarting B must
restore B/J/D while C keeps its owner, state, and cursor. J rereads retained C
output. If a required boundary is unavailable, recovery expands upstream with
an exact reason and stops explicitly at a non-rewindable source.

## Theory

Bind each physical partition to one producer and freeze its consumer edges in
the deployment revision. Route each checkpoint from source injection through
participant acknowledgements and output barriers. A participant snapshot binds
the exact cuts of its own incoming edges, regardless of source depth.

Persist admission order separately from application order. A terminal generator
observation creates output, managed state, immutable turn, and that input's DATA
cursor in one transaction. The checkpoint snapshot includes only pre-barrier
turns. Pending post-barrier DATA drains in durable arrival order after alignment.

## Proposed solution and agreement

- Fixed acyclic graphs, static partitions, one producer per partition, fan-out
  through independent participant input progress, one active aligned checkpoint.
- Immutable `revision_edges` derive from revision participant manifests; graph
  registration rejects undeclared producers, duplicate producers, and cycles.
- Source and intermediate output barriers share the partition log with DATA.
  Each participant output barrier receives a sealed retention root.
- Each input stores separate `scanned`, `completed`, and `blocked_checkpoint`.
  Immutable receipts retain ObjectRef identity and arrival order. Immutable turns
  retain terminal state/output identity and application order.
- Execution has three phases: durable invocation claim, client Python outside
  any DB transaction, then a fenced terminal transaction. A replacement worker
  can retry the claim; old invocation tokens cannot publish.
- Snapshot/ACK/output BARRIER/local alignment form one transaction by passing
  the existing connection into coordinator acknowledgement.
- Local alignment releases the buffer before global `COMPLETED`. Abort also
  releases buffered DATA; aborted log barriers remain and are skipped safely.
- Logical record identity excludes recovery generation. Client effects remain
  subject to client idempotency and may repeat during replay.

## Stories

- As a developer, I want to inspect every edge producer, input cut, mailbox
  receipt, terminal witness, snapshot high-water, and buffer drain ordinal.
- As a user, I want a fast branch to deliver each document immediately and a
  slow branch to recover from its own first incomplete input.
- As a system, I want the join snapshot to exclude post-barrier DATA and every
  terminal DATA cursor to have its matching state/output witness.
- As a user, I want B/J/D recovery while C stays live, with visible explanations
  when recovery has to include upstream producers.
- As an operator, I want hard edge, buffer, metadata, and recovery-backlog
  budgets with explicit pressure and no dropped or skipped input.

## Acceptance criteria

- As a developer, I can trace source→A→{B,C}→J→D checkpoint authority through
  exact producer edges and inspect protected intermediate replay roots.
- As a user, I got fast DATA1/2 and slow DATA1/2 before the output BARRIER, then
  fast DATA3/4 in persisted order without waiting for global completion.
- As a system, I reject mixed checkpoints, rebound inputs, cursor-only progress,
  stale worker/coordinator epochs, buffer reorder, early output barriers, and
  snapshot cuts substituted by current live cursors.
- As a system, I got continued DATA after a partially aligned checkpoint abort.
- As a system, I survive abrupt process loss around receipt/claim, terminal
  publication, barrier observation, snapshot/output barrier, abort, and replay.
- As a user, I got the same managed state and stable platform output identities
  after B/J/D generation replacement while C keeps its epoch and processes new
  input.
- As a system, I select one completed compatible graph checkpoint, freeze exact
  boundary targets, verify payload bytes, and require full replay before READY.
- As a developer, I can explain recursive recovery expansion and inspect an
  explicit unrecoverable boundary when the source cannot rewind.
- As an operator, I can see edge/buffer age and capacity, recover with live
  ingress within the declared budget, and resume after credit returns.

## Prototype discussion and current evidence

[postgres_join_runtime.py](../prototypes/postgres_join_runtime.py) is the first
live PostgreSQL join implementation. [postgres_join_oracle.py](../prototypes/postgres_join_oracle.py)
derives state-chain, terminal prefix, input authority, buffer, snapshot, output
barrier, and replay invariants without importing the runtime.
[test_postgres_join_runtime.py](../prototypes/test_postgres_join_runtime.py)
currently covers noncommutative skew, exact snapshot/drain, bounded buffer,
partial abort release, fresh connection restore validation, client execution
without DB locks, stale-terminal rejection, and a five-participant routed DAG.
It also covers async yield/return, a real 12-boundary subprocess crash matrix,
failed pre-barrier retry behind buffered input, forged terminal INSERT,
coherently rehashed ObjectRef corruption, last-buffer-slot concurrency, and
byte-only saturation. The join and coordinator oracles read the same fixtures
independently.

[postgres_recovery_regions.py](../prototypes/postgres_recovery_regions.py)
and [its tests](../prototypes/test_postgres_recovery_regions.py) bind a completed
checkpoint, protected edge cuts, payload-verified boundaries, recursive
expansion, and explicit unrecoverable sources. External source rewind execution
is outside the supported durable-channel recovery contract.

[test_postgres_regional_expansion.py](../prototypes/test_postgres_regional_expansion.py)
now tests execution as well as planning. A declared non-replayable C→J boundary
expands B/J/D to B/C/J/D; fresh handles restore all four generations, preserve A,
activate the region atomically and process document 5 through the tail. A
recursive expansion reaching a non-rewindable SOURCE refuses recovery without
changing worker ownership, authority ordinals or recovery-plan rows. Both cases
pass the join, coordinator and stream oracles. The fixture deliberately retains
the payload bytes: it proves region expansion and exact replay, not repair of
missing or corrupt storage outside the retained horizon.

Principal review rejected the assumption that physical missing/corrupt retained
output could be repaired merely by expanding the plan. The counterexample and
replacement boundary contract are preserved in
[ADR-0093](ADR-0093-implicit-regeneration-of-missing-durable-output.md). A named
pre-fence refusal must also cover a declared non-replayable edge whose bytes are
damaged; its policy flag cannot bypass physical integrity checks.

[postgres_regional_recovery.py](../prototypes/postgres_regional_recovery.py)
and [its tests](../prototypes/test_postgres_regional_recovery.py) compose that
plan with atomic worker-generation fencing for B/J/D. Healthy C keeps its owner,
epoch and state and accepts new data. Replay checks stable outputs without new
appends. All regional workers remain `RESTORING` until deferred storage guards
permit one atomic region-wide activation. Generic global recovery APIs reject a
regional plan; its exact generation manifest is independently checked.
Append-only recovery-owner attempts now provide lease renewal and atomic
takeover after replay-process loss without changing the frozen plan or targets.
A four-boundary `os._exit(86)` campaign reopens fresh processes, rejects stale
attempts, resumes replay, and activates the complete region. Independent oracle
checks bind every attempt ordinal, epoch chain, request digest and worker set.

[postgres_dag_pressure.py](../prototypes/postgres_dag_pressure.py) projects edge
lag, record/byte/metadata/log saturation, pending mailbox records, buffer usage,
and alignment age from a read-only transaction. It reserves no credit. The
partition append and terminal transactions enforce actual admission.

## Task and acceptance traceability

| Task | Implemented contract | Executable evidence |
| --- | --- | --- |
| DS-031a | Ordered routed alignment, durable admission/application order, terminal publication, abort release and exact snapshot/replay | [Join runtime cases](../prototypes/test_postgres_join_runtime.py), including twelve real process-loss boundaries and three independent oracles |
| DS-031b | Fenced regional worker generations, append-only recovery owner attempts, full-region READY/activation and healthy sibling progress | [Regional recovery cases](../prototypes/test_postgres_regional_recovery.py), including four real recovery-process losses |
| DS-031c | Executable policy-driven expansion inside the verified retained horizon; named atomic refusal for unsupported source rewind or invalid physical replay authority | [Planner cases](../prototypes/test_postgres_recovery_regions.py), [expanded-region execution and internal-output refusal](../prototypes/test_postgres_regional_expansion.py), and [rejected repair theory](ADR-0093-implicit-regeneration-of-missing-durable-output.md) |
| DS-031d | Hard record/byte/mailbox limits, observable pressure, returned credits, bounded integrated archive and live DAG catch-up | [Pressure/credit cases](../prototypes/test_postgres_join_runtime.py), [100-cycle composition and catch-up](../prototypes/test_postgres_streaming_retention.py), and [retention contract](ADR-0023-bounded-retention-and-replay-horizons.md) |

This table identifies the evidence required by the gate; it does not by itself
award task completion or a readiness percentage.

Principal design review rejected the original source-input-only spike as gate
evidence. It also identified generator execution under DB locks, permanent
blocking after abort, missing progress guards, permanently pinned input roots,
and historical replay verification without restored regional generations.
The spike now separates invocation/terminal transactions, handles abort, adds
progress guards, and routes intermediate barriers. Those changes require an
independent hostile review before any task or readiness increment is earned.

Further hostile review found that UPDATE guards could trust a forged terminal
INSERT. Admission now validates receipts against source log/ObjectRef identity,
turns against a durable invocation token/history and output witness, barriers
against source or upstream participant authority, and alignments against the
exact snapshot ACK. A second control rehashed forged receipt metadata and exposed
a false negative in the oracle; independent ObjectRef/byte cross-checks now
reject it. A third control removed a regional generation; the coordinator
oracle now requires the exact region manifest. Focused Principal re-review
passed this safety slice. The complete gate was then reviewed against the final
frozen sources and acceptance evidence below.

## R3-B gate result and remaining limits

The standalone `restore` path verifies historical mailbox replay. The regional
module separately creates and activates fenced regional generations.
[ADR-0023](ADR-0023-bounded-retention-and-replay-horizons.md) now advances input
roots and retires hot join history through exact snapshot/summary manifests.
Coordinator/log, regional and stream metadata archival pass the integrated
bounded-history gate. Declared-unavailable internal boundaries have executable
expansion coverage; invalid physical replay authority and non-rewindable sources
fail before a fence is published. Reconstruction of missing durable payloads
and an external source rewind adapter remain unsupported.

On 2026-10-25 the [final frozen-source report](../evidence/postgres-r3b-final2-2026-10-25.json)
passed 93/93 PostgreSQL tests in 336.425 seconds on PostgreSQL 17.11 with
`remote_apply` and a synchronous streaming replica. All 39 recorded source
hashes match the current tree and source drift is empty. The integrated
100-cycle census retained exactly 64 rows at cycles 25/50/75/100 and at most
30,452 JSON bytes, within its declared 512-row/256-KiB profile. This is logical
storage evidence for the tested graph, not a physical-disk or throughput claim.

The [independent ETL/HighLoad Principal review](../evidence/postgres-r3b-principal-2026-10-25.json)
rechecked the report hash, every source hash, all DS-031a…d criteria, and four
key regression controls. It recorded PASS with no remaining blockers in this
supported scope. W28–W30 are complete and earned readiness is **75%**.

Server, Prefect integration, authorization, upgrades, infrastructure chaos, and
the real 72-hour soak remain subsequent gates in
[ADR-0012](ADR-0012-protocol-epoch-2-delivery-gates.md). A shortened test cannot
count as elapsed soak time.

## Evidence required for `accepted`

Production runtime/server integration, complete regional recovery acceptance,
bounded retention under load, upgrades, chaos, operational drill, and final
Principal release acceptance. Prototype tests alone keep this ADR `progress`.
