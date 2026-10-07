# Deployment rolling update - TLA+ model

`DeploymentRollout.tla` models one Deployment rolling from an old ReplicaSet to
a new one. `MC_DeploymentRollout.tla` binds constants for Apalache.

## What it is read from

| Model | Code |
| --- | --- |
| `Controller` (one atomic pass) | `rolloutRolling`, `reconcileNewReplicaSet`, `reconcileOldReplicaSets`, `cleanupUnhealthyReplicas`, `scaleDownOldReplicaSetsForRollingUpdate` (`pkg/controller/deployment/rolling.go`) |
| first `Controller` pass | `getNewReplicaSet` creates the new RS already sized by `NewRSNewReplicas`, then the same pass runs `reconcileOldReplicaSets` (`sync.go`) |
| `Surge`, `MaxUnavailable`, `MinAvailable` | `ResolveFenceposts`, `MaxUnavailable` (ceil surge, floor unavailable, 0/0 -> unavailable 1, cap at N) |
| `NewRSNewReplicas` | `NewRSNewReplicas` (scales raw `maxSurge`, rounding up) |
| `Old/NewCreate`, `Old/NewDelete` | ReplicaSet controller `manageReplicas`; not-ready pods are deleted first |
| `Old/NewStatus` | ReplicaSet `calculateStatus`: `availableReplicas` counts active (non-terminating) ready pods, published with lag |
| `Old/NewReady`, `Old/NewFail`, `Old/NewGone` | kubelet: pods become ready, ready pods fail (bounded by `MaxFail`), terminating pods disappear |

`Init` draws N in 1..MaxN and `maxSurge`/`maxUnavailable` from ints
0..MaxN+1 and percents in `PctValues` (both literally zero, and
`maxUnavailable` > 100%, are excluded as validation does). Ints cover every
resolved (surge, unavailable) pair; percents exercise the rounding.

## Results

| Property | Result |
| --- | --- |
| 1a. `oldSpec + newSpec <= N + maxSurge` (sum of RS `spec.replicas`) | **Proved** (Apalache, inductive: `IndInv`, any N >= 1, any input, lagging status, failures). TLC also holds for N <= 4. |
| 1b. active pods `<= N + maxSurge` | **Violated** (TLC). See below. |
| 1c. all pod objects incl. terminating `<= N + maxSurge` | **Violated** (TLC), same trace. |
| 2. ready pods `>= N - maxUnavailable` (no failures) | Holds within bounds: N <= 4, all inputs, fresh status (107,777 states) and lagging status (604,215 states). |
| 2. same, with one involuntary failure | Violated trivially: an old pod fails before the controller does anything. Not a controller property. |
| 2'. ready pods `>= N - maxUnavailable - failures` | Holds within bounds: 1 failure, N <= 4 fresh status (419,669 states); N <= 3 lagging status (620,445 states). |
| 3. `<>[]Complete` under weak fairness | Holds within bounds: N <= 4 fresh status no failures; N <= 3 fresh status 1 failure; N <= 2 lagging status 1 failure. |

### The surge counterexample (1b/1c)

N = 1, maxSurge = 0, maxUnavailable = 1:

1. Init: old RS spec 1, one ready old pod.
2. Controller (creates new RS at 0, reconcileOld): old RS spec -> 0. The old pod
   is still running - the ReplicaSet controller has not deleted it yet.
3. Controller: `NewRSNewReplicas` sees `0 + 0 < 1 + 0` and scales new RS -> 1.
4. ReplicaSet controller creates the new pod before deleting the old one:
   two active pods, bound is 1.

This is the code, not the model: `NewRSNewReplicas` and `reconcileOldReplicaSets`
count `spec.replicas` (`GetReplicaCountForReplicaSets`), never pods. maxSurge
bounds what the Deployment controller asks for; pods follow when the
ReplicaSet controller gets to them, and terminating pods are not counted
at all (`TerminatingReplicas` is reported in status, not used by the rolling
logic). Whether this is a bug is a reading of the API docs, not something the
model decides.

### Why availability holds with lagging status

`cleanupUnhealthyReplicas` computes `spec - status.availableReplicas`; when the
old RS has been scaled down but its status still counts the doomed pods, that
is negative, the "invalid request to scale down" error fires, and
`reconcileOldReplicaSets` returns `(false, nil)`. That error path is what stops
the controller from scaling down again on a stale count.

`Mutant_NoCleanupGuard.tla` checks this: substitute
`ReconcileOld <- ReconcileOldNoGuard` (cleanup treats a negative count as 0
and carries on) and TLC breaks `AvailBound` with no failures at all - N = 3,
maxSurge = 3, maxUnavailable = 1: the first pass sets old 3 -> 2, new -> 3;
one old pod is deleted, one new pod becomes ready and is published; old status
still says 3, so the next pass scales old 2 -> 0; the two remaining old pods go
and 1 pod is available against a floor of 2. With the real error path, the
same model holds. The guard works because it errors, not because it was
written as one; a refactor that "fixes" the negative count would remove it.

## Bounds and what is left out

- TLC results hold within the stated bounds, not in general. Larger runs
  (N = 4 with lagging status and a failure, 2 failures) exceeded the checking
  service's limit.
- One old ReplicaSet (no multi-RS rollovers), N constant (no scaling event
  mid-rollout, no `proportional scaling`), no pause/rollback,
  `minReadySeconds = 0`, `progressDeadlineSeconds` ignored.
- The controller reads its own `spec.replicas` writes immediately; only RS
  status lags.
- Pod creation/deletion is one pod per step; quota or admission failures on
  create are not modelled.
- A failed pod is "active, not ready" (crash loop); evicted / `Failed`-phase
  pods replaced by the RS controller are not modelled separately.

## Replay against the Go controller

`.grounds/conformance/deployment_rolling_update.vectors.json` holds 9 runs
(TLC, N <= 2, lagging status, one failure): new-RS scale-up, old-RS
scale-down, cleanup of an unhealthy old pod, stale status blocking a
scale-down, availability at the floor, both surge overshoots, a full rollout to
completion, and an old pod recovering. `NewDelete`, `OldCreate` and `NewGone`
cannot happen in a single forward rollout (the new RS is never scaled down, the
old RS never up) - they are there for rollback and scaling, which are not
modelled.

`action_taken` labels every step. A harness would drive the real
`DeploymentController.rolloutRolling` against a fake clientset:

- `oldSpec`/`newSpec` <- `ReplicaSet.Spec.Replicas`; `oSt`/`nSt` <-
  `ReplicaSet.Status.AvailableReplicas` set in the informer cache;
- `oR/oU/oT`, `nR/nU/nT` <- pods with Ready condition / without / with
  `DeletionTimestamp`, owned by each RS;
- `Controller` -> one `syncDeployment` call; RS-controller and kubelet steps
  -> direct edits of the fake objects (or a real `ReplicaSetController` sync
  for `*Create/*Delete/*Status`);
- after every step compare the RS specs the fake client recorded with the
  vector's `oldSpec`/`newSpec`.

No harness exists yet; the proof and bounded results are about the model only
until one runs.
