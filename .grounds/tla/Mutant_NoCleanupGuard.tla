------------------------------ MODULE Mutant_NoCleanupGuard ------------------------------
(* Mutation check, not the code: cleanupUnhealthyReplicas without its     *)
(* "invalid request to scale down" error, i.e. a stale status that counts *)
(* more available pods than spec.replicas is cleaned up as zero and the   *)
(* scale-down goes ahead. Substitute it with                              *)
(*   CONSTANT ReconcileOld <- ReconcileOldNoGuard                         *)
(* to see what that error path protects.                                  *)
EXTENDS DeploymentRollout

ReconcileOldNoGuard(os, ns, oa, na) ==
    IF os = 0 THEN os
    ELSE
      LET maxScaledDown == (os + ns) - MinAvailable - (ns - na)
      IN IF maxScaledDown <= 0 THEN os
         ELSE
           LET c    == IF os <= oa THEN 0 ELSE Min(maxScaledDown, os - oa)
               os1  == os - c
               avail == oa + na
           IN IF avail <= MinAvailable THEN os1
              ELSE os1 - Min(os1, avail - MinAvailable)

=============================================================================
