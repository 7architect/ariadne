-------------------------- MODULE BarrierAlignment --------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS Inputs, MaxCheckpoint, MaxEpoch, MaxPosition

CheckpointIds == 1..MaxCheckpoint
Epochs == 1..MaxEpoch
Positions == 0..MaxPosition
ZeroInputMap == [input \in Inputs |-> 0]
ZeroSnapshotMap == [checkpoint \in CheckpointIds |-> ZeroInputMap]

VARIABLES
    ownerEpoch,
    activeCheckpoint,
    highWater,
    barriers,
    barrierCuts,
    cursors,
    postBarrierInputs,
    snapshots,
    snapshotBarrierIds,
    snapshotBarrierCuts,
    snapshotStateCuts,
    acceptedWriteEpoch

vars ==
    <<ownerEpoch, activeCheckpoint, highWater, barriers, barrierCuts, cursors,
      postBarrierInputs, snapshots, snapshotBarrierIds, snapshotBarrierCuts,
      snapshotStateCuts, acceptedWriteEpoch>>

Init ==
    /\ ownerEpoch = 1
    /\ activeCheckpoint = 0
    /\ highWater = 0
    /\ barriers = ZeroInputMap
    /\ barrierCuts = ZeroInputMap
    /\ cursors = ZeroInputMap
    /\ postBarrierInputs = {}
    /\ snapshots = {}
    /\ snapshotBarrierIds = ZeroSnapshotMap
    /\ snapshotBarrierCuts = ZeroSnapshotMap
    /\ snapshotStateCuts = ZeroSnapshotMap
    /\ acceptedWriteEpoch = 0

Data(input, epoch) ==
    /\ input \in Inputs
    /\ epoch = ownerEpoch
    /\ cursors[input] < MaxPosition
    /\ cursors' = [cursors EXCEPT ![input] = @ + 1]
    /\ IF activeCheckpoint # 0 /\ barriers[input] = activeCheckpoint
          THEN postBarrierInputs' = postBarrierInputs \cup {input}
          ELSE UNCHANGED postBarrierInputs
    /\ UNCHANGED <<ownerEpoch, activeCheckpoint, highWater, barriers,
                    barrierCuts, snapshots, snapshotBarrierIds,
                    snapshotBarrierCuts, snapshotStateCuts,
                    acceptedWriteEpoch>>

Barrier(input, checkpoint, epoch) ==
    /\ input \in Inputs
    /\ checkpoint \in CheckpointIds
    /\ epoch = ownerEpoch
    /\ barriers[input] = 0
    /\ IF activeCheckpoint = 0
          THEN /\ checkpoint = highWater + 1
               /\ activeCheckpoint' = checkpoint
          ELSE /\ checkpoint = activeCheckpoint
               /\ UNCHANGED activeCheckpoint
    /\ barriers' = [barriers EXCEPT ![input] = checkpoint]
    /\ barrierCuts' = [barrierCuts EXCEPT ![input] = cursors[input]]
    /\ UNCHANGED <<ownerEpoch, highWater, cursors, postBarrierInputs,
                    snapshots, snapshotBarrierIds, snapshotBarrierCuts,
                    snapshotStateCuts, acceptedWriteEpoch>>

PublishSnapshot ==
    /\ activeCheckpoint \in CheckpointIds
    /\ \A input \in Inputs: barriers[input] = activeCheckpoint
    /\ activeCheckpoint = highWater + 1
    /\ snapshots' = snapshots \cup {activeCheckpoint}
    /\ snapshotBarrierIds' =
          [snapshotBarrierIds EXCEPT ![activeCheckpoint] = barriers]
    /\ snapshotBarrierCuts' =
          [snapshotBarrierCuts EXCEPT ![activeCheckpoint] = barrierCuts]
    /\ snapshotStateCuts' =
          [snapshotStateCuts EXCEPT ![activeCheckpoint] = barrierCuts]
    /\ highWater' = activeCheckpoint
    /\ activeCheckpoint' = 0
    /\ barriers' = ZeroInputMap
    /\ barrierCuts' = ZeroInputMap
    /\ postBarrierInputs' = {}
    /\ UNCHANGED <<ownerEpoch, cursors, acceptedWriteEpoch>>

Restore(checkpoint, epoch) ==
    /\ checkpoint \in snapshots
    /\ epoch = ownerEpoch
    /\ cursors' = snapshotStateCuts[checkpoint]
    /\ activeCheckpoint' = 0
    /\ barriers' = ZeroInputMap
    /\ barrierCuts' = ZeroInputMap
    /\ postBarrierInputs' = {}
    /\ UNCHANGED <<ownerEpoch, highWater, snapshots, snapshotBarrierIds,
                    snapshotBarrierCuts, snapshotStateCuts,
                    acceptedWriteEpoch>>

Takeover ==
    /\ ownerEpoch < MaxEpoch
    /\ ownerEpoch' = ownerEpoch + 1
    /\ acceptedWriteEpoch' = 0
    /\ UNCHANGED <<activeCheckpoint, highWater, barriers, barrierCuts,
                    cursors, postBarrierInputs, snapshots, snapshotBarrierIds,
                    snapshotBarrierCuts, snapshotStateCuts>>

AcceptWrite(epoch) ==
    /\ epoch \in Epochs
    /\ epoch = ownerEpoch
    /\ acceptedWriteEpoch' = epoch
    /\ UNCHANGED <<ownerEpoch, activeCheckpoint, highWater, barriers,
                    barrierCuts, cursors, postBarrierInputs, snapshots,
                    snapshotBarrierIds, snapshotBarrierCuts,
                    snapshotStateCuts>>

Next ==
    \/ \E input \in Inputs, epoch \in Epochs: Data(input, epoch)
    \/ \E input \in Inputs, checkpoint \in CheckpointIds, epoch \in Epochs:
          Barrier(input, checkpoint, epoch)
    \/ PublishSnapshot
    \/ \E checkpoint \in CheckpointIds, epoch \in Epochs:
          Restore(checkpoint, epoch)
    \/ Takeover
    \/ \E epoch \in Epochs: AcceptWrite(epoch)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ ownerEpoch \in Epochs
    /\ activeCheckpoint \in 0..MaxCheckpoint
    /\ highWater \in 0..MaxCheckpoint
    /\ barriers \in [Inputs -> 0..MaxCheckpoint]
    /\ barrierCuts \in [Inputs -> Positions]
    /\ cursors \in [Inputs -> Positions]
    /\ postBarrierInputs \subseteq Inputs
    /\ snapshots \subseteq CheckpointIds
    /\ snapshotBarrierIds \in [CheckpointIds -> [Inputs -> 0..MaxCheckpoint]]
    /\ snapshotBarrierCuts \in [CheckpointIds -> [Inputs -> Positions]]
    /\ snapshotStateCuts \in [CheckpointIds -> [Inputs -> Positions]]
    /\ acceptedWriteEpoch \in 0..MaxEpoch

SameCheckpointCut ==
    \A checkpoint \in snapshots:
        \A input \in Inputs:
            snapshotBarrierIds[checkpoint][input] = checkpoint

SnapshotExcludesPostBarrier ==
    \A checkpoint \in snapshots:
        snapshotStateCuts[checkpoint] = snapshotBarrierCuts[checkpoint]

MonotonicPublication == snapshots = 1..highWater

FencingSafety ==
    acceptedWriteEpoch = 0 \/ acceptedWriteEpoch = ownerEpoch

(* The alignment mutation admits different checkpoint IDs on different inputs
   and only publishes once all inputs have some barrier. *)
BarrierWithoutAlignment(input, checkpoint, epoch) ==
    /\ input \in Inputs
    /\ checkpoint \in CheckpointIds
    /\ epoch = ownerEpoch
    /\ barriers[input] = 0
    /\ activeCheckpoint' =
          IF activeCheckpoint = 0 THEN checkpoint ELSE activeCheckpoint
    /\ barriers' = [barriers EXCEPT ![input] = checkpoint]
    /\ barrierCuts' = [barrierCuts EXCEPT ![input] = cursors[input]]
    /\ UNCHANGED <<ownerEpoch, highWater, cursors, postBarrierInputs,
                    snapshots, snapshotBarrierIds, snapshotBarrierCuts,
                    snapshotStateCuts, acceptedWriteEpoch>>

PublishMixedSnapshot ==
    /\ activeCheckpoint \in CheckpointIds
    /\ \A input \in Inputs: barriers[input] # 0
    /\ \E left, right \in Inputs: barriers[left] # barriers[right]
    /\ snapshots' = snapshots \cup {activeCheckpoint}
    /\ snapshotBarrierIds' =
          [snapshotBarrierIds EXCEPT ![activeCheckpoint] = barriers]
    /\ snapshotBarrierCuts' =
          [snapshotBarrierCuts EXCEPT ![activeCheckpoint] = barrierCuts]
    /\ snapshotStateCuts' =
          [snapshotStateCuts EXCEPT ![activeCheckpoint] = cursors]
    /\ highWater' = activeCheckpoint
    /\ activeCheckpoint' = 0
    /\ barriers' = ZeroInputMap
    /\ barrierCuts' = ZeroInputMap
    /\ postBarrierInputs' = {}
    /\ UNCHANGED <<ownerEpoch, cursors, acceptedWriteEpoch>>

UnsafeAlignmentNext ==
    \/ \E input \in Inputs, epoch \in Epochs: Data(input, epoch)
    \/ \E input \in Inputs, checkpoint \in CheckpointIds, epoch \in Epochs:
          BarrierWithoutAlignment(input, checkpoint, epoch)
    \/ PublishMixedSnapshot
    \/ Takeover
    \/ \E epoch \in Epochs: AcceptWrite(epoch)

UnsafeAlignmentSpec == Init /\ [][UnsafeAlignmentNext]_vars

UnsafeAcceptWrite(epoch) ==
    /\ epoch \in Epochs
    /\ acceptedWriteEpoch' = epoch
    /\ UNCHANGED <<ownerEpoch, activeCheckpoint, highWater, barriers,
                    barrierCuts, cursors, postBarrierInputs, snapshots,
                    snapshotBarrierIds, snapshotBarrierCuts,
                    snapshotStateCuts>>

UnsafeFencingNext ==
    \/ \E input \in Inputs, epoch \in Epochs: Data(input, epoch)
    \/ \E input \in Inputs, checkpoint \in CheckpointIds, epoch \in Epochs:
          Barrier(input, checkpoint, epoch)
    \/ PublishSnapshot
    \/ Takeover
    \/ \E epoch \in Epochs: UnsafeAcceptWrite(epoch)

UnsafeFencingSpec == Init /\ [][UnsafeFencingNext]_vars

=============================================================================
