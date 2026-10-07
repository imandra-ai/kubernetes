# HPA replica-count edge cases (ImandraX via Grounds)

Scope: `pkg/controller/podautoscaler/replica_calculator.go` (calcPlainMetricReplicas,
GetResourceReplicas, getUsageRatioReplicaCount, Tolerances.isWithin, ceilToInt32) and
`horizontal.go` (convertDesiredReplicasWithRules / calculateScaleUpLimit,
stabilizeRecommendationWithBehaviors, convertDesiredReplicasWithBehaviorRate,
calculateScaleUpLimitWithScalingRules, calculateScaleDownLimitWithBehaviors, the zero-replica
and min/max pre-clamps in reconcileAutoscaler). No Kubernetes code was changed.

## Model

- Ratios are **exact rationals** (IML `real`); float64 is not modelled. `math.Ceil` is an exact
  ceiling, `big.Int.Quo` is truncation, `ceilToInt32` clamps to the int32 rails.
  `GetResourceUtilizationRatio` truncates utilization to an int percent before dividing by the
  target; `GetMetricUsageRatio` truncates the average. Both truncations are modelled.
- Pods are summarised by counts: ready pods with a metric (and their sum), Pending/unready pods,
  pods with no metric. Extraneous metrics (for pods not in the lister) are not modelled.
- The calculator's decision logic is also stated as a linear core `decide` (ratio, new ratio and
  the two ceil proposals as inputs). Z3 cannot close the nonlinear goals of the full model
  (division by a variable target, `ratio * podCount` under floor/ceil), so proofs are done on the
  core for *all* values of the computed quantities, and counterexamples are made concrete on the
  full model with some parameters fixed.
- Behaviors: scale events and recommendations are lists of `(value, age-in-seconds)`; an item
  counts iff `age < period/window` (the code's `timestamp.After(now - period)`).
- Percent policies exact: up `ceil(start*(100+v)/100)`, down `trunc(start*(100-v)/100)`.

## Properties

| # | Property (docs / API promise) | Result |
|---|---|---|
| P1 | Within the tolerance band nothing changes (no missing/unready pods) | **Proved** (core) |
| P2 | Within the band nothing changes, with missing/unready pods, *if the fill is conservative* | **Proved** (core) |
| P2' | ... unconditionally | Refuted (core) -> the fill is not always conservative, see F6 |
| P3 | Missing metrics / unready pods never reverse the measured direction | **Proved** for every fill value (core) |
| P4 | Fast path (no missing/unready) never scales opposite to the ratio | **Refuted**: current 10, 3 ready pods at 2x target -> 6 (F4) |
| P5 | Object/External Value metrics never scale opposite to the ratio | **Refuted**: ratio 2, 0 ready pods -> 0 (F5) |
| P6 | Behaviors: result in [min, max] when min <= current <= max | **Proved** |
| P6' | ... for any current | Refuted at current > max (unreachable: reconcile pre-clamps) |
| P7 | selectPolicy Disabled => no change in that direction | **Proved** (within convertDesiredReplicasWithBehaviorRate) |
| P8 | Pods policy: replicas added within the period <= value | **Refuted**: a scale-down of 357 within the period lets 663 be added under a 306-pod policy (F9) |
| P9 | Scale-up possible whenever enabled (scale from zero) | **Refuted**: Percent-only, or selectPolicy Min over Pods+Percent, from 0 -> stays 0 (F8) |
| P10 | Stabilization: a recommendation older than the window never wins | **Proved** (by induction, with two lemmas) |
| P11 | Stabilization only moves toward current (between current and desired) | **Proved** |
| P12 | Legacy normalization: result in [min, max] | Refuted at current 0, min 5 -> 4; reconcileAutoscaler re-clamps to min on that path |
| P13 | currentReplicas 0 set by a user => HPA does nothing (HPAScaleToZero on) | **Refuted** on a 3-step trace (F10) |

## Decompositions (regions)

- `plain_replicas` (calcPlainMetricReplicas): 154 regions under sign constraints. ImandraX could
  not produce sample points (nonlinear), so conformance there uses 14 hand-picked vectors.
  Surprising regions: ratio exactly 1.0 with missing pods (missing pods are then dropped from
  `len(metrics)`, neither 0% nor 100%); the int32 rail regions (a `ceilToInt32` clamp to
  2147483647 is reachable with a large custom metric).
- `rate_limit_core`: 19 regions; `rate_limit` (with lists, limits as basis): 19 regions under a
  realistic domain. Surprising: region 1/17 — from currentReplicas 0 the scale-up limit can be
  <= 0 and the result stays 0; region 15 — desired > max with current == max and a limit below
  current returns max (== current).
- `rules_normalize`: 9 regions; region 8 (current <= 2, desired >= 5) returns 4 regardless of
  minReplicas (the "scale up to at most 4" floor of calculateScaleUpLimit).
- `stabilize`: 4 regions (region 4, min-window > max-window, is infeasible: min <= desired <= max).
- `usage_ratio_replicas`: 7 regions; currentReplicas 0 => `ceil(ratio)` ignoring tolerance and
  ready pods; current > 0 and 0 ready pods => 0.

## Conformance (`go test ./pkg/controller/podautoscaler/ -run TestGroundsConformance -v`)

| Vectors file | Agree | Disagree | Disagreements |
|---|---|---|---|
| plain_replicas | 10 | 4 | c3, c4 (ceil of float product), c5, c6 (band edge) |
| usage_ratio_replicas | 13 | 1 | c6 (ceil of float product) |
| rules_normalize | 11 | 0 | |
| rate_limit_core | 19 | 0 | |
| rate_limit | 31 | 2 | c1, c2 (Percent policy float) |
| stabilize | 9 | 0 | |

All 7 disagreements are float64-vs-exact findings at boundary inputs chosen for that purpose;
none is a modelling error. Note: this package's `horizontal_test.go` `init()` sets
`scaleUpLimitFactor = 8`; the binding restores production's 2.0 for the rules vectors (so the
existing tests never exercise the production scale-up factor).

`TestGroundsFindings` pins the code's actual behaviour on every finding (passes).

## Findings

| # | Edge case | Region / counterexample | What the code does | Verdict |
|---|---|---|---|---|
| F1 | `ceil(ratio * pods)` when the exact product is an integer | 25 pods at 14 vs target 50 (ratio 0.28); also 50 pods at 88 vs 80 | 0.28*25 = 7.000000000000001 -> **8** (exact 7) | Doc-worthy: off by one extra replica, conservative direction; docs give the formula as exact `ceil` |
| F2 | ratio exactly at `1 - tolerance` | flag tolerance 0.7, utilization 30 vs 100 | 1-0.7 = 0.30000000000000004 -> outside band -> scales 10 -> **3**; same tolerance as a `700m` Quantity is 0.7000000000000001 -> no change | Doc-worthy: band edges are not closed as documented, and depend on how the tolerance was written |
| F3 | ratio exactly at `1 + tolerance` | tolerance 0.36, utilization 136 vs 100 | 1+0.36 = 1.3599999999999999 -> scales 10 -> **14** | Doc-worthy (as F2) |
| F4 | fast path uses readyPodCount, not currentReplicas, with no direction guard | spec.replicas 10, 3 pods exist (e.g. quota), each at 2x target | proposes **6** (scale down while overloaded); 10 ready pods at 0.5 with spec 4 -> **5** (scale up while under target, e.g. during a surge rollout) | Doc-worthy surprise: docs give `ceil(currentReplicas * ratio)`; the slow path has a direction guard, the fast path does not |
| F5 | Object/External Value metric with no Ready pod | ratio 5 (5x target), current 10, all pods unready | `ceil(5 * 0)` = **0**; normalization takes it to minReplicas (or to 0 with minReplicas 0 / HPAScaleToZero) after the stabilization window | Likely bug: overload that fails readiness makes the HPA scale *down* |
| F6 | missing-pod fill truncated per pod | target 150%, 1 ready pod 135% (ratio 0.9, in band), 10 missing pods requesting 3m | fill = 3*150/100 = 4 (133%), new ratio 0.893 -> **10** from 11 | Minor bug: rounding goes the non-conservative way; only matters for tiny requests at the band edge |
| F7 | Percent policy computed in float64 | 10% of 100 -> **111**; 30% of 90 down -> **62** | one replica beyond the policy limit each way | Doc-worthy (minor): the policy limit is exceeded by one |
| F8 | scale from zero with behaviors | HPAScaleToZero, minReplicas 0, scaleUp Percent-only, or selectPolicy Min over the default Pods 4 + Percent 100 | limit = ceil(0*x) = 0 -> stays **0** forever (reconcile only re-clamps when minReplicas != 0) | Bug (beta feature): scale-from-zero silently disabled by a valid policy |
| F9 | Pods policy counts net change | 4 pods/60s, scaled down 5 within the last 60s | limit 10-0+5+4 = **19**: 9 pods added in 60s | By design (code computes from periodStartReplicas) but doc-worthy: "at most N pods per period" is net, not added |
| F10 | stale ScaledToZero condition | HPA scales 1 -> 0 (condition True); user scales to max; HPA reconciles with no rescale (condition left True); user scales to 0 | HPA scales back up from 0, ignoring the manual zero | Bug: the condition is only refreshed on a rescale despite the "never stale" comment (model-level; not bound to Go) |
| F11 | legacy path from zero with minReplicas > 4 | current 0, min 5, desired 43 | convertDesiredReplicasWithRules returns 4 (< min); reconcile re-clamps to 5 | By design (caller handles it) |
| F12 | Disabled vs min/max | selectPolicy Disabled, current < min or > max | reconcile moves to min/max before behaviors run | By design: Disabled governs metric-driven scaling only |

## Not established

- Full-model proofs (P1-P3 on `plain_replicas` / `resource_replicas` directly) are Z3-unknown
  (nonlinear); the proofs are on the decision core plus the refactoring argument that the full
  model calls the core with the computed values.
- No region of `plain_replicas` got a sample point; conformance there rests on 14 chosen vectors.
- GetResourceReplicas has no vectors file; F6 is checked by a direct Go test.
- F10 is a model-level trace; the reconcile loop was not driven in Go.
