# Bounded protocol models

`BarrierAlignment.tla` models two ordered checkpoints, per-input barrier IDs and
cuts, pre/post-barrier cursors, durable-buffer membership, snapshot identity,
high-water ordering, restore, and two ownership epochs. `BarrierAlignment.cfg`
must satisfy type, same-checkpoint cut, post-barrier exclusion, monotonic
publication, and stale-writer invariants.

Checkpoint IDs in this bounded model are dense ordinals of successful alignment
epochs. External IDs skipped by an aborted checkpoint are renumbered before they
enter the model; the Python protocol only requires external IDs to increase.

Two mutation controls demonstrate that the invariants are non-vacuous:

- `BarrierAlignmentUnsafeMixed.cfg` admits different checkpoint IDs on the two
  inputs and publishes their mixed cut; it must violate `SameCheckpointCut`.
- `BarrierAlignmentUnsafe.cfg` removes the owner-epoch guard from a write; it
  must violate `FencingSafety`.

Run with TLC 1.8.0:

```shell
java -cp /path/to/tla2tools.jar tlc2.TLC -config BarrierAlignment.cfg BarrierAlignment.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -config BarrierAlignmentUnsafeMixed.cfg BarrierAlignment.tla
java -cp /path/to/tla2tools.jar tlc2.TLC -config BarrierAlignmentUnsafe.cfg BarrierAlignment.tla
```

For CI, `verify_tla.py` runs all three commands and asserts both the success and
the named unsafe counterexamples:

```shell
TLA2TOOLS_JAR=/path/to/tla2tools.jar python verify_tla.py
```

With TLC 1.8.0 the safe configuration generated 9,471 states, found 2,132
distinct states, reached depth 17, and reported no invariant violation. The
mixed-checkpoint control violated `SameCheckpointCut` at depth 4; the missing-
fence control violated `FencingSafety` at depth 2.

The safe run is bounded safety evidence. It does not establish liveness, lease
timing, distributed-storage correctness, unbounded checkpoints, or production
behavior.

The R3-B rerun is archived in
[barrier-tla-r3b-2026-10-25.json](../evidence/barrier-tla-r3b-2026-10-25.json).
`verify_tla.py --output <path>` records all three raw outputs and hashes of the
model, configurations, runner, and TLC JAR.

For [ADR-0022](../adr/ADR-0022-postgresql-routed-join-and-regional-recovery.md),
`Data` maps to per-input admission/application, `Barrier` and `PublishSnapshot`
map to ordered `_barrier` alignment, and `Takeover`/`AcceptWrite` map to worker
epoch admission. This preserves the existing bounded invariants only. The model
does not cover SQL transactions, invocation tokens, checkpoint abort, metadata
retirement, regional owner attempts, or catch-up liveness; their evidence comes
from the PostgreSQL hostile and subprocess tests.
