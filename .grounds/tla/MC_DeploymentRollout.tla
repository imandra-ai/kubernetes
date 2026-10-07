------------------------------ MODULE MC_DeploymentRollout ------------------------------
(* Constant bindings for Apalache. IndInv does not use MaxN or PctValues, *)
(* so the inductive proof is for every n >= 1 and every input value.      *)
EXTENDS DeploymentRollout

OVERRIDE_MaxN == 4
OVERRIDE_PctValues == {0, 25, 50, 75, 100}
OVERRIDE_MaxFail == 1
OVERRIDE_StaleStatus == TRUE

=============================================================================
