------------------------------ MODULE DeploymentRollout ------------------------------
(***************************************************************************)
(* One Deployment rolling from an old ReplicaSet to a new one.             *)
(*                                                                         *)
(* Read from:                                                              *)
(*   pkg/controller/deployment/rolling.go       rolloutRolling,            *)
(*       reconcileNewReplicaSet, reconcileOldReplicaSets,                  *)
(*       cleanupUnhealthyReplicas, scaleDownOldReplicaSetsForRollingUpdate *)
(*   pkg/controller/deployment/sync.go          getNewReplicaSet           *)
(*   pkg/controller/deployment/util/deployment_util.go                     *)
(*       ResolveFenceposts, MaxUnavailable, NewRSNewReplicas               *)
(*   pkg/controller/replicaset                  manageReplicas,            *)
(*       calculateStatus (active pods only, not-ready deleted first)       *)
(*                                                                         *)
(* Actors, interleaved:                                                    *)
(*   - the Deployment controller: one atomic rolloutRolling pass, reading  *)
(*     RS spec.replicas and RS status.availableReplicas;                   *)
(*   - the ReplicaSet controller: creates/deletes one pod at a time toward *)
(*     spec.replicas (not-ready pods deleted first), and, separately,      *)
(*     publishes status.availableReplicas (which can lag);                 *)
(*   - the kubelet: pods become ready, ready pods fail (bounded budget),   *)
(*     terminating pods go away.                                           *)
(*                                                                         *)
(* Pods are anonymous, so they are counted per RS and per condition.       *)
(* minReadySeconds = 0: available = ready.                                 *)
(***************************************************************************)
EXTENDS Integers

CONSTANTS
    \* @type: Int;
    MaxN,          \* desired replicas range over 1..MaxN
    \* @type: Set(Int);
    PctValues,     \* percent values tried for maxSurge / maxUnavailable
    \* @type: Int;
    MaxFail,       \* how many times a ready pod may fail (involuntary disruption)
    \* @type: Bool;
    StaleStatus    \* TRUE: the controller reads RS status that lags the pods

VARIABLES
    \* The Deployment's spec: replicas, maxSurge, maxUnavailable (never change).
    \* @type: Int;
    n,
    \* @type: { k: Str, v: Int };
    sIn,
    \* @type: { k: Str, v: Int };
    uIn,
    \* Has the pass that creates the new RS run.
    \* @type: Bool;
    created,
    \* RS spec.replicas.
    \* @type: Int;
    oldSpec,
    \* @type: Int;
    newSpec,
    \* Old RS pods: ready, active-not-ready, terminating.
    \* @type: Int;
    oR,
    \* @type: Int;
    oU,
    \* @type: Int;
    oT,
    \* New RS pods: ready, active-not-ready, terminating.
    \* @type: Int;
    nR,
    \* @type: Int;
    nU,
    \* @type: Int;
    nT,
    \* RS status.availableReplicas as published.
    \* @type: Int;
    oSt,
    \* @type: Int;
    nSt,
    \* Failures so far.
    \* @type: Int;
    fails,
    \* Label of the last step, for replay.
    \* @type: Str;
    action_taken

params == <<n, sIn, uIn>>
\* @type: <<Int, Int, Int, Int, Int, Int>>;
pods   == <<oR, oU, oT, nR, nU, nT>>
vars   ==<<params, created, oldSpec, newSpec, pods, oSt, nSt, fails, action_taken>>

Min(a, b) == IF a < b THEN a ELSE b

(* IntOrString: an int, or a percent. Validation rejects maxSurge and     *)
(* maxUnavailable both literally zero, and maxUnavailable above 100%.      *)
Inputs == [k : {"int"}, v : 0..(MaxN + 1)] \cup [k : {"pct"}, v : PctValues]
\* @type: ({ k: Str, v: Int }) => Bool;
Zero(x) == x.v = 0

\* intstr.GetScaledValueFromIntOrPercent
\* @type: ({ k: Str, v: Int }, Int, Bool) => Int;
Scaled(x, total, roundUp) ==
    IF x.k = "int" THEN x.v
    ELSE IF roundUp THEN (x.v * total + 99) \div 100
    ELSE (x.v * total) \div 100

\* ResolveFenceposts
RawSurge == Scaled(sIn, n, TRUE)
RawUnav  == Scaled(uIn, n, FALSE)
Surge    == RawSurge
ResUnav  == IF RawSurge = 0 /\ RawUnav = 0 THEN 1 ELSE RawUnav
\* MaxUnavailable caps at spec.replicas
MaxUnavailable == IF ResUnav > n THEN n ELSE ResUnav
MinAvailable   == n - MaxUnavailable

\* What the controller sees as available.
OldAvailSeen == IF StaleStatus THEN oSt ELSE oR
NewAvailSeen == IF StaleStatus THEN nSt ELSE nR

\* NewRSNewReplicas (RollingUpdate branch); maxSurge is scaled raw, as in the code.
NewRSNewReplicas(os, ns) ==
    LET all == os + ns
        maxTotal == n + Scaled(sIn, n, TRUE)
    IN IF all >= maxTotal THEN ns
       ELSE ns + Min(maxTotal - all, n - ns)

(* reconcileOldReplicaSets with one old RS: the new spec.replicas of the  *)
(* old RS. oa/na are the status counts the controller reads.              *)
ReconcileOld(os, ns, oa, na) ==
    IF os = 0 THEN os
    ELSE
      LET maxScaledDown == (os + ns) - MinAvailable - (ns - na)
      IN IF maxScaledDown <= 0 THEN os
         \* cleanupUnhealthyReplicas: spec < status.available asks to scale
         \* "down" to a larger number; it returns an error and
         \* reconcileOldReplicaSets returns (false, nil) - no scale-down.
         ELSE IF os # oa /\ os - oa < 0 THEN os
         ELSE
           LET c    == IF os = oa THEN 0 ELSE Min(maxScaledDown, os - oa)
               os1  == os - c
               \* scaleDownOldReplicaSetsForRollingUpdate: status of all RSs
               avail == oa + na
           IN IF avail <= MinAvailable THEN os1
              ELSE os1 - Min(os1, avail - MinAvailable)

(* One rolloutRolling pass. The first pass is getNewReplicaSet creating   *)
(* the new RS already sized by NewRSNewReplicas, so reconcileNew is a      *)
(* no-op and the same pass continues to reconcileOld (new RS status 0).    *)
\* @type: (Int, Int) => <<Int, Int>>;
ControllerNext(os, ns) ==
    IF ~created THEN
        LET ns1 == NewRSNewReplicas(os, 0)
        IN <<ReconcileOld(os, ns1, OldAvailSeen, 0), ns1>>
    ELSE IF ns > n THEN <<os, n>>
    ELSE IF ns # n /\ NewRSNewReplicas(os, ns) # ns
         THEN <<os, NewRSNewReplicas(os, ns)>>
    ELSE <<ReconcileOld(os, ns, OldAvailSeen, NewAvailSeen), ns>>

Controller ==
    LET nxt == ControllerNext(oldSpec, newSpec)
    IN /\ \/ ~created
          \/ nxt[1] # oldSpec
          \/ nxt[2] # newSpec
       /\ created' = TRUE
       /\ oldSpec' = nxt[1]
       /\ newSpec' = nxt[2]
       /\ action_taken' = "Controller"
       /\ UNCHANGED <<params, pods, oSt, nSt, fails>>

----------------------------------------------------------------------------
(* ReplicaSet controller: manageReplicas, one pod per step. *)

OldCreate ==
    /\ oR + oU < oldSpec
    /\ oU' = oU + 1
    /\ action_taken' = "OldCreate"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oT, nR, nU, nT, oSt, nSt, fails>>

OldDelete ==
    /\ oR + oU > oldSpec
    /\ IF oU > 0 THEN oU' = oU - 1 /\ UNCHANGED oR
                 ELSE oR' = oR - 1 /\ UNCHANGED oU
    /\ oT' = oT + 1
    /\ action_taken' = "OldDelete"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, nR, nU, nT, oSt, nSt, fails>>

NewCreate ==
    /\ nR + nU < newSpec
    /\ nU' = nU + 1
    /\ action_taken' = "NewCreate"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oU, oT, nR, nT, oSt, nSt, fails>>

NewDelete ==
    /\ nR + nU > newSpec
    /\ IF nU > 0 THEN nU' = nU - 1 /\ UNCHANGED nR
                 ELSE nR' = nR - 1 /\ UNCHANGED nU
    /\ nT' = nT + 1
    /\ action_taken' = "NewDelete"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oU, oT, oSt, nSt, fails>>

\* RS calculateStatus: availableReplicas over active pods, published later.
OldStatus ==
    /\ StaleStatus /\ oSt # oR
    /\ oSt' = oR
    /\ action_taken' = "OldStatus"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, pods, nSt, fails>>

NewStatus ==
    /\ StaleStatus /\ nSt # nR
    /\ nSt' = nR
    /\ action_taken' = "NewStatus"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, pods, oSt, fails>>

----------------------------------------------------------------------------
(* Kubelet / environment. *)

OldReady ==
    /\ oU > 0
    /\ oU' = oU - 1 /\ oR' = oR + 1
    /\ action_taken' = "OldReady"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oT, nR, nU, nT, oSt, nSt, fails>>

NewReady ==
    /\ nU > 0
    /\ nU' = nU - 1 /\ nR' = nR + 1
    /\ action_taken' = "NewReady"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oU, oT, nT, oSt, nSt, fails>>

OldFail ==
    /\ fails < MaxFail /\ oR > 0
    /\ oR' = oR - 1 /\ oU' = oU + 1 /\ fails' = fails + 1
    /\ action_taken' = "OldFail"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oT, nR, nU, nT, oSt, nSt>>

NewFail ==
    /\ fails < MaxFail /\ nR > 0
    /\ nR' = nR - 1 /\ nU' = nU + 1 /\ fails' = fails + 1
    /\ action_taken' = "NewFail"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oU, oT, nT, oSt, nSt>>

OldGone ==
    /\ oT > 0 /\ oT' = oT - 1
    /\ action_taken' = "OldGone"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oU, nR, nU, nT, oSt, nSt, fails>>

NewGone ==
    /\ nT > 0 /\ nT' = nT - 1
    /\ action_taken' = "NewGone"
    /\ UNCHANGED <<params, created, oldSpec, newSpec, oR, oU, oT, nR, nU, oSt, nSt, fails>>

----------------------------------------------------------------------------

\* The rollout starts from a fully available old RS of n pods.
Init ==
    /\ n \in 1..MaxN
    /\ sIn \in Inputs
    /\ uIn \in Inputs
    /\ ~(Zero(sIn) /\ Zero(uIn))
    /\ ~(uIn.k = "pct" /\ uIn.v > 100)
    /\ created = FALSE
    /\ oldSpec = n /\ newSpec = 0
    /\ oR = n /\ oU = 0 /\ oT = 0
    /\ nR = 0 /\ nU = 0 /\ nT = 0
    /\ oSt = n /\ nSt = 0
    /\ fails = 0
    /\ action_taken = "Init"

Next ==
    \/ Controller
    \/ OldCreate \/ OldDelete \/ NewCreate \/ NewDelete
    \/ OldStatus \/ NewStatus
    \/ OldReady \/ NewReady \/ OldFail \/ NewFail
    \/ OldGone \/ NewGone

Fairness ==
    /\ WF_vars(Controller)
    /\ WF_vars(OldCreate) /\ WF_vars(OldDelete)
    /\ WF_vars(NewCreate) /\ WF_vars(NewDelete)
    /\ WF_vars(OldStatus) /\ WF_vars(NewStatus)
    /\ WF_vars(OldReady) /\ WF_vars(NewReady)
    /\ WF_vars(OldGone) /\ WF_vars(NewGone)

Spec == Init /\ [][Next]_vars /\ Fairness

----------------------------------------------------------------------------
(* Properties. "Total pods" has three readings; the code bounds the first. *)

TypeOK ==
    /\ newSpec \in 0..n /\ oldSpec \in 0..n
    /\ fails \in 0..MaxFail

\* 1a. Sum of RS spec.replicas (what NewRSNewReplicas bounds).
SpecSurgeBound   == oldSpec + newSpec <= n + Surge
\* 1b. Active pods (no deletionTimestamp).
ActiveSurgeBound == oR + oU + nR + nU <= n + Surge
\* 1c. All pod objects, terminating included.
AllSurgeBound    == oR + oU + oT + nR + nU + nT <= n + Surge

\* 2. Available pods. Fails trivially once a ready pod may fail (MaxFail > 0):
\* no controller prevents an involuntary disruption.
AvailBound == oR + nR >= MinAvailable
\* 2'. With failures: the controller's own scale-downs never cost availability
\* beyond what the failures took - each failure lowers the floor by one.
AvailBoundModFailures == oR + nR >= MinAvailable - fails

\* 3. Rollout complete.
Complete ==
    /\ created /\ newSpec = n /\ oldSpec = 0
    /\ nR = n /\ nU = 0 /\ oR + oU + oT + nT = 0

EventuallyComplete       == <>Complete
EventuallyAlwaysComplete == <>[]Complete

----------------------------------------------------------------------------
(* Inductive invariant for SpecSurgeBound, for Apalache. Deliberately not  *)
(* bounded by MaxN or PctValues: any n >= 1, any non-negative inputs.      *)
Actions == {"Init", "Controller", "OldCreate", "OldDelete", "NewCreate",
            "NewDelete", "OldStatus", "NewStatus", "OldReady", "NewReady",
            "OldFail", "NewFail", "OldGone", "NewGone"}

IndInv ==
    /\ n >= 1
    /\ sIn.k \in {"int", "pct"} /\ sIn.v >= 0
    /\ uIn.k \in {"int", "pct"} /\ uIn.v >= 0
    /\ created \in BOOLEAN
    /\ oldSpec >= 0 /\ newSpec >= 0 /\ newSpec <= n
    /\ oR >= 0 /\ oU >= 0 /\ oT >= 0 /\ nR >= 0 /\ nU >= 0 /\ nT >= 0
    /\ oSt >= 0 /\ nSt >= 0
    /\ fails >= 0 /\ fails <= MaxFail
    /\ action_taken \in Actions
    /\ SpecSurgeBound

\* Any state of the right types that satisfies IndInv.
IndInit ==
    /\ n \in Int
    /\ \E sk \in {"int", "pct"}, sv \in Int : sIn = [k |-> sk, v |-> sv]
    /\ \E uk \in {"int", "pct"}, uv \in Int : uIn = [k |-> uk, v |-> uv]
    /\ created \in BOOLEAN
    /\ oldSpec \in Int /\ newSpec \in Int
    /\ oR \in Int /\ oU \in Int /\ oT \in Int
    /\ nR \in Int /\ nU \in Int /\ nT \in Int
    /\ oSt \in Int /\ nSt \in Int
    /\ fails \in Int
    /\ action_taken \in Actions
    /\ IndInv

=============================================================================
