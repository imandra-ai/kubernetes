---- MODULE PVBinder ----
\* PV controller binder: claim worker(s) and the volume worker interleaved with
\* users, an external provisioner, informer lag (assume cache) and
\* resourceVersion optimistic concurrency. Model of
\* pkg/controller/volume/persistentvolume/pv_controller.go (syncClaim,
\* syncVolume, bind, reclaimVolume, deleteVolumeOperation/isVolumeReleased).
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS Vols, Claims, CWorkers, MaxUid, MaxRv, Budget, AdminEdits

None == "none"

VARIABLES pv, pvc, pvC, pvcC, cw, delq, rvc, nextUid, budget, dataLoss, rel

vars == <<pv, pvc, pvC, pvcC, cw, delq, rvc, nextUid, budget, dataLoss, rel>>

NoPV == [ex |-> FALSE, ref |-> None, ruid |-> 0, phase |-> "Pending", bbc |-> FALSE, dyn |-> FALSE, policy |-> "Retain", rv |-> 0]
NoPVC == [ex |-> FALSE, uid |-> 0, vol |-> None, bc |-> FALSE, bbc |-> FALSE, phase |-> "Pending", rv |-> 0]
Idle == [pc |-> "idle", c |-> None, v |-> None, cl |-> NoPVC]
NoRel == [on |-> FALSE, ref |-> None, ruid |-> 0]

Init ==
  /\ pv = [v \in Vols |-> NoPV]
  /\ pvc = [c \in Claims |-> NoPVC]
  /\ pvC = pv
  /\ pvcC = pvc
  /\ cw = [w \in CWorkers |-> Idle]
  /\ delq = {}
  /\ rvc = 0
  /\ nextUid = 1
  /\ budget = Budget
  /\ dataLoss = FALSE
  /\ rel = [v \in Vols |-> NoRel]

\* ---------------- helpers ----------------
\* storagehelpers.IsVolumeBoundToClaim
BoundTo(p, c, cl) == p.ref = c /\ (p.ruid = 0 \/ p.ruid = cl.uid)

\* Update succeeds iff the resourceVersion read is still current; the assume
\* cache sees the controller's own write at once.
WritePV(v, readRv, new) ==
  /\ pv[v].ex /\ pv[v].rv = readRv
  /\ LET n == [new EXCEPT !.rv = rvc + 1] IN
       /\ pv' = [pv EXCEPT ![v] = n]
       /\ pvC' = [pvC EXCEPT ![v] = n]
  /\ rvc' = rvc + 1

WritePVC(c, readRv, new) ==
  /\ pvc[c].ex /\ pvc[c].rv = readRv
  /\ LET n == [new EXCEPT !.rv = rvc + 1] IN
       /\ pvc' = [pvc EXCEPT ![c] = n]
       /\ pvcC' = [pvcC EXCEPT ![c] = n]
  /\ rvc' = rvc + 1

\* history: claimRef a PV had when it entered Released with Retain
RelTrack(v, np) ==
  rel' = [rel EXCEPT ![v] =
     IF np.phase = "Released" /\ np.policy = "Retain" /\ ~rel[v].on
     THEN [on |-> TRUE, ref |-> np.ref, ruid |-> np.ruid] ELSE rel[v]]

\* ---------------- claim worker: syncClaim ----------------
\* bind write 1 (PV) is taken with the decision made on the possibly stale
\* cache; write 2 (PVC) is a separate action, so anything can happen between.
NewPVFor(p, c, cl) == [p EXCEPT !.ref = c, !.ruid = cl.uid, !.bbc = p.bbc \/ ~BoundTo(p, c, cl), !.phase = "Bound"]
NewPVCFor(cl, v) == [cl EXCEPT !.vol = v, !.bbc = cl.bbc \/ cl.vol # v, !.bc = TRUE, !.phase = "Bound"]

StartBind(w, c, cl, v) ==
  LET p == pvC[v] IN
  LET np == NewPVFor(p, c, cl) IN
  IF np = p
  THEN /\ cw' = [cw EXCEPT ![w] = [pc |-> "pvc", c |-> c, v |-> v, cl |-> cl]]
       /\ UNCHANGED <<pv, pvC, rvc>>
  ELSE \/ /\ WritePV(v, p.rv, np)
          /\ cw' = [cw EXCEPT ![w] = [pc |-> "pvc", c |-> c, v |-> v, cl |-> cl]]
       \/ /\ ~(pv[v].ex /\ pv[v].rv = p.rv)   \* conflict: nothing saved, retried later
          /\ UNCHANGED <<pv, pvC, rvc, cw>>

FinishBind(w) ==
  /\ cw[w].pc = "pvc"
  /\ LET c == cw[w].c
         cl == cw[w].cl
         ncl == NewPVCFor(cl, cw[w].v) IN
     IF ncl = cl THEN UNCHANGED <<pvc, pvcC, rvc>>
     ELSE IF pvc[c].ex /\ pvc[c].rv = cl.rv THEN WritePVC(c, cl.rv, ncl)
     ELSE UNCHANGED <<pvc, pvcC, rvc>>
  /\ cw' = [cw EXCEPT ![w] = Idle]
  /\ UNCHANGED <<pv, pvC, delq, nextUid, budget, dataLoss, rel>>

MarkLost(c, cl) ==
  IF cl.phase = "Lost" THEN UNCHANGED <<pvc, pvcC, rvc>>
  ELSE IF pvc[c].ex /\ pvc[c].rv = cl.rv THEN WritePVC(c, cl.rv, [cl EXCEPT !.phase = "Lost"])
  ELSE UNCHANGED <<pvc, pvcC, rvc>>

SyncClaim(w, c) ==
  /\ cw[w].pc = "idle"
  /\ \A w2 \in CWorkers : cw[w2].c # c     \* a workqueue never hands one key to two workers
  /\ pvcC[c].ex
  /\ LET cl == pvcC[c] IN
     IF ~cl.bc THEN
       IF cl.vol = None THEN
         LET pre == {v \in Vols : pvC[v].ex /\ BoundTo(pvC[v], c, cl)}
             free == {v \in Vols : pvC[v].ex /\ pvC[v].ref = None /\ pvC[v].phase = "Available"} IN
         IF pre # {} THEN \E v \in pre : StartBind(w, c, cl, v) /\ UNCHANGED <<pvc, pvcC>>
         ELSE IF free # {} THEN \E v \in free : StartBind(w, c, cl, v) /\ UNCHANGED <<pvc, pvcC>>
         ELSE FALSE   \* stays Pending; provisioning is the ProvisionPV action
       ELSE
         LET v == cl.vol IN
         IF ~pvC[v].ex THEN FALSE
         ELSE IF pvC[v].ref = None \/ BoundTo(pvC[v], c, cl) THEN StartBind(w, c, cl, v) /\ UNCHANGED <<pvc, pvcC>>
         ELSE FALSE   \* Pending (or the "should never happen" error); no write
     ELSE
       IF cl.vol = None \/ ~pvC[cl.vol].ex THEN MarkLost(c, cl) /\ UNCHANGED <<pv, pvC, cw>>
       ELSE IF pvC[cl.vol].ref = None \/ pvC[cl.vol].ruid = cl.uid THEN StartBind(w, c, cl, cl.vol) /\ UNCHANGED <<pvc, pvcC>>
       ELSE MarkLost(c, cl) /\ UNCHANGED <<pv, pvC, cw>>
  /\ UNCHANGED <<delq, nextUid, budget, dataLoss, rel>>

\* ---------------- volume worker: syncVolume ----------------
SetPV(v, p, np) ==
  IF np = p THEN UNCHANGED <<pv, pvC, rvc, rel>>
  ELSE IF pv[v].ex /\ pv[v].rv = p.rv THEN WritePV(v, p.rv, np) /\ RelTrack(v, np)
  ELSE UNCHANGED <<pv, pvC, rvc, rel>>

Released(p) == IF p.phase \in {"Released", "Failed"} THEN p ELSE [p EXCEPT !.phase = "Released"]

Reclaim(v, p) ==
  /\ SetPV(v, p, Released(p))
  /\ delq' = IF p.policy = "Delete" /\ pv'[v].ex /\ pv'[v].phase = Released(p).phase THEN delq \cup {v} ELSE delq

SyncVolume(v) ==
  /\ pvC[v].ex
  /\ LET p == pvC[v] IN
     IF p.ref = None \/ p.ruid = 0 THEN SetPV(v, p, [p EXCEPT !.phase = "Available"]) /\ UNCHANGED delq
     ELSE
       LET cc == pvcC[p.ref]
           api == pvc[p.ref]
           \* cache first; on a miss re-read the API unless Released/Failed;
           \* on a UID mismatch re-read the API
           found == IF cc.ex THEN (IF cc.uid = p.ruid THEN TRUE ELSE api.ex /\ api.uid = p.ruid)
                    ELSE IF p.phase \notin {"Released", "Failed"} THEN api.ex /\ api.uid = p.ruid
                    ELSE FALSE
           cl == IF cc.ex /\ cc.uid = p.ruid THEN cc ELSE api IN
       IF ~found THEN Reclaim(v, p)
       ELSE IF cl.vol = None THEN UNCHANGED <<pv, pvC, rvc, rel, delq>>
       ELSE IF cl.vol = v THEN SetPV(v, p, [p EXCEPT !.phase = "Bound"]) /\ UNCHANGED delq
       ELSE IF p.dyn /\ p.policy = "Delete" THEN Reclaim(v, p)
       ELSE SetPV(v, p, IF p.bbc THEN [p EXCEPT !.ref = None, !.ruid = 0, !.bbc = FALSE, !.phase = "Available"]
                         ELSE [p EXCEPT !.ruid = 0, !.phase = "Available"]) /\ UNCHANGED delq
  /\ UNCHANGED <<pvc, pvcC, cw, nextUid, budget, dataLoss>>

\* deleteVolumeOperation: re-read the PV from the API, isVolumeReleased against
\* the claim CACHE, then delete storage and the PV object
DeleteOp(v) ==
  /\ v \in delq
  /\ delq' = delq \ {v}
  /\ LET p == pv[v]
         released == p.ex /\ p.ref # None /\ p.ruid # 0 /\
                     (~pvcC[p.ref].ex \/ pvcC[p.ref].uid # p.ruid \/ (pvcC[p.ref].vol # None /\ pvcC[p.ref].vol # v)) IN
     IF released THEN
       /\ dataLoss' = (dataLoss \/ \E c \in Claims : pvc[c].ex /\ pvc[c].vol = v /\ pvc[c].uid = p.ruid)
       /\ pv' = [pv EXCEPT ![v] = NoPV]
       /\ rel' = [rel EXCEPT ![v] = NoRel]
     ELSE UNCHANGED <<pv, dataLoss, rel>>
  /\ UNCHANGED <<pvc, pvC, pvcC, cw, rvc, nextUid, budget>>

\* ---------------- informers: deliver the API's current object ----------------
InformPV(v) == /\ pvC[v] # pv[v] /\ pvC' = [pvC EXCEPT ![v] = pv[v]]
               /\ UNCHANGED <<pv, pvc, pvcC, cw, delq, rvc, nextUid, budget, dataLoss, rel>>
InformPVC(c) == /\ pvcC[c] # pvc[c] /\ pvcC' = [pvcC EXCEPT ![c] = pvc[c]]
                /\ UNCHANGED <<pv, pvc, pvC, cw, delq, rvc, nextUid, budget, dataLoss, rel>>

\* ---------------- users / environment (bounded by Budget) ----------------
User == budget > 0 /\ budget' = budget - 1

CreateClaim(c, v) ==
  /\ User /\ ~pvc[c].ex /\ nextUid <= MaxUid
  /\ pvc' = [pvc EXCEPT ![c] = [ex |-> TRUE, uid |-> nextUid, vol |-> v, bc |-> FALSE, bbc |-> FALSE, phase |-> "Pending", rv |-> rvc + 1]]
  /\ rvc' = rvc + 1 /\ nextUid' = nextUid + 1
  /\ UNCHANGED <<pv, pvC, pvcC, cw, delq, dataLoss, rel>>

DeleteClaim(c) ==
  /\ User /\ pvc[c].ex
  /\ pvc' = [pvc EXCEPT ![c] = NoPVC]
  /\ UNCHANGED <<pv, pvC, pvcC, cw, delq, rvc, nextUid, dataLoss, rel>>

CreatePV(v, pol) ==
  /\ User /\ ~pv[v].ex
  /\ pv' = [pv EXCEPT ![v] = [NoPV EXCEPT !.ex = TRUE, !.policy = pol, !.rv = rvc + 1]]
  /\ rvc' = rvc + 1
  /\ UNCHANGED <<pvc, pvC, pvcC, cw, delq, nextUid, dataLoss, rel>>

\* external provisioner: PV pre-bound to the claim's UID, provisioned-by, Delete
ProvisionPV(v, c) ==
  /\ User /\ ~pv[v].ex /\ pvc[c].ex /\ pvc[c].vol = None /\ ~pvc[c].bc
  /\ pv' = [pv EXCEPT ![v] = [NoPV EXCEPT !.ex = TRUE, !.ref = c, !.ruid = pvc[c].uid, !.dyn = TRUE, !.policy = "Delete", !.rv = rvc + 1]]
  /\ rvc' = rvc + 1
  /\ UNCHANGED <<pvc, pvC, pvcC, cw, delq, nextUid, dataLoss, rel>>

\* pv-protection: a Bound PV is removed only once it is no longer Bound
DeletePV(v) ==
  /\ User /\ pv[v].ex /\ pv[v].phase # "Bound"
  /\ pv' = [pv EXCEPT ![v] = NoPV]
  /\ rel' = [rel EXCEPT ![v] = NoRel]
  /\ UNCHANGED <<pvc, pvC, pvcC, cw, delq, rvc, nextUid, dataLoss>>

\* documented manual reclaim: an admin removes spec.claimRef
ClearClaimRef(v) ==
  /\ AdminEdits /\ User /\ pv[v].ex /\ pv[v].ref # None
  /\ pv' = [pv EXCEPT ![v] = [@ EXCEPT !.ref = None, !.ruid = 0, !.rv = rvc + 1]]
  /\ rvc' = rvc + 1
  /\ UNCHANGED <<pvc, pvC, pvcC, cw, delq, nextUid, dataLoss, rel>>

Next ==
  \/ \E w \in CWorkers, c \in Claims : SyncClaim(w, c)
  \/ \E w \in CWorkers : FinishBind(w)
  \/ \E v \in Vols : SyncVolume(v) \/ DeleteOp(v) \/ InformPV(v) \/ DeletePV(v) \/ ClearClaimRef(v)
  \/ \E v \in Vols, pol \in {"Retain", "Delete"} : CreatePV(v, pol)
  \/ \E v \in Vols, c \in Claims : ProvisionPV(v, c)
  \/ \E c \in Claims : InformPVC(c) \/ DeleteClaim(c)
  \/ \E c \in Claims, v \in Vols \cup {None} : CreateClaim(c, v)

Spec == Init /\ [][Next]_vars

StateBound == rvc <= MaxRv

\* ---------------- properties ----------------
NoDoubleBound ==
  \A c1, c2 \in Claims : c1 # c2 =>
    ~(pvc[c1].ex /\ pvc[c2].ex /\ pvc[c1].phase = "Bound" /\ pvc[c2].phase = "Bound"
      /\ pvc[c1].vol # None /\ pvc[c1].vol = pvc[c2].vol)

BoundPairs ==
  \A c \in Claims : (pvc[c].ex /\ pvc[c].phase = "Bound" /\ pvc[c].vol # None /\ pv[pvc[c].vol].ex)
     => (pv[pvc[c].vol].ref = c /\ pv[pvc[c].vol].ruid = pvc[c].uid)

BoundClaimHasVolume ==
  \A c \in Claims : (pvc[c].ex /\ pvc[c].phase = "Bound") => (pvc[c].vol # None /\ pv[pvc[c].vol].ex)

NoDataLoss == ~dataLoss

RetainNotRebound ==
  \A v \in Vols : (rel[v].on /\ pv[v].ex) => (pv[v].ref = rel[v].ref /\ pv[v].ruid = rel[v].ruid /\ pv[v].phase = "Released")

====
