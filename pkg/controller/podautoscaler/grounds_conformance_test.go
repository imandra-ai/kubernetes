/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package podautoscaler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	metricsclient "k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
)

// The vectors are produced by ImandraX from an IML model of the replica
// calculation and normalization in which float64 ratios are exact rationals.
// A disagreement on a vector chosen at a float64 boundary is therefore a
// float-vs-exact finding, not necessarily a modelling error.
const groundsVectorsDir = "../../../.grounds/conformance"

const groundsNamespace = "grounds"

type groundsVector struct {
	ID       string                     `json:"id"`
	Input    map[string]json.RawMessage `json:"input"`
	Expected json.RawMessage            `json:"expected"`
	Why      string                     `json:"why"`
}

type groundsVectorsFile struct {
	Function string          `json:"function"`
	Vectors  []groundsVector `json:"vectors"`
}

func loadGroundsVectors(t *testing.T, file, name string) groundsVectorsFile {
	t.Helper()
	path := filepath.Join(groundsVectorsDir, file)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var f groundsVectorsFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	if f.Function != name {
		t.Fatalf("%s holds vectors for %q, want %q", path, f.Function, name)
	}
	return f
}

func gInt(t *testing.T, raw json.RawMessage) int64 {
	t.Helper()
	var n json.Number
	if err := json.Unmarshal(raw, &n); err == nil {
		v, err := strconv.ParseInt(n.String(), 10, 64)
		if err != nil {
			t.Fatalf("integer %s does not fit int64: %v", n, err)
		}
		return v
	}
	var bi struct {
		Int string `json:"$int"`
	}
	if err := json.Unmarshal(raw, &bi); err != nil || bi.Int == "" {
		t.Fatalf("not an integer: %s", raw)
	}
	v, err := strconv.ParseInt(bi.Int, 10, 64)
	if err != nil {
		t.Fatalf("integer %s does not fit int64: %v", bi.Int, err)
	}
	return v
}

func gInt32(t *testing.T, raw json.RawMessage) int32 {
	t.Helper()
	v := gInt(t, raw)
	if v != int64(int32(v)) {
		t.Fatalf("integer %d does not fit int32", v)
	}
	return int32(v)
}

// gReal returns the float64 nearest to the exact rational, which is what
// float64(n)/float64(d) gives the code for the values used here.
func gReal(t *testing.T, raw json.RawMessage) float64 {
	t.Helper()
	var r struct {
		Real string `json:"$real"`
	}
	if err := json.Unmarshal(raw, &r); err != nil || r.Real == "" {
		t.Fatalf("not a real: %s", raw)
	}
	q, ok := new(big.Rat).SetString(r.Real)
	if !ok {
		t.Fatalf("bad rational %q", r.Real)
	}
	f, _ := q.Float64()
	return f
}

func gCtor(t *testing.T, raw json.RawMessage) (string, []json.RawMessage) {
	t.Helper()
	var c struct {
		C    string            `json:"$c"`
		Args []json.RawMessage `json:"args"`
	}
	if err := json.Unmarshal(raw, &c); err != nil || c.C == "" {
		t.Fatalf("not a variant: %s", raw)
	}
	return c.C, c.Args
}

func gList(t *testing.T, raw json.RawMessage) []map[string]json.RawMessage {
	t.Helper()
	var l []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &l); err != nil {
		t.Fatalf("not a list of records: %s", raw)
	}
	return l
}

func groundsPod(name string, phase v1.PodPhase, ready bool, request string) *v1.Pod {
	status := v1.ConditionFalse
	if ready {
		status = v1.ConditionTrue
	}
	start := metav1.NewTime(time.Now().Add(-time.Hour))
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: groundsNamespace},
		Spec: v1.PodSpec{Containers: []v1.Container{{
			Name: "c",
			Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse(request),
			}},
		}}},
		Status: v1.PodStatus{
			Phase:     phase,
			StartTime: &start,
			Conditions: []v1.PodCondition{{
				Type:               v1.PodReady,
				Status:             status,
				LastTransitionTime: start,
			}},
		},
	}
}

func groundsCalculator(t *testing.T, pods []*v1.Pod, mc metricsclient.MetricsClient) *ReplicaCalculator {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, p := range pods {
		if err := indexer.Add(p); err != nil {
			t.Fatalf("adding pod %s: %v", p.Name, err)
		}
	}
	return NewReplicaCalculator(mc, corelisters.NewPodLister(indexer), defaultTestingCPUInitializationPeriod, defaultTestingDelayOfInitialReadinessStatus)
}

// runPlain builds n_ready running pods carrying metrics that sum to sum_ready,
// n_unready Pending pods and n_missing running pods without a metric.
func runPlain(t *testing.T, current int32, target int64, tol Tolerances, nReady, nUnready, nMissing int, sum int64) (int32, error) {
	t.Helper()
	var pods []*v1.Pod
	metrics := metricsclient.PodMetricsInfo{}
	for i := 0; i < nReady; i++ {
		name := fmt.Sprintf("ready-%d", i)
		pods = append(pods, groundsPod(name, v1.PodRunning, true, "1"))
		// only the total matters to GetMetricUsageRatio
		v := int64(0)
		if i == 0 {
			v = sum
		}
		metrics[name] = metricsclient.PodMetric{Value: v, Timestamp: time.Now(), Window: time.Minute}
	}
	for i := 0; i < nUnready; i++ {
		pods = append(pods, groundsPod(fmt.Sprintf("pending-%d", i), v1.PodPending, false, "1"))
	}
	for i := 0; i < nMissing; i++ {
		pods = append(pods, groundsPod(fmt.Sprintf("missing-%d", i), v1.PodRunning, true, "1"))
	}
	rc := groundsCalculator(t, pods, nil)
	got, _, err := rc.calcPlainMetricReplicas(metrics, current, target, tol, groundsNamespace, labels.Everything(), "")
	return got, err
}

func groundsSelect(t *testing.T, raw json.RawMessage) autoscalingv2.ScalingPolicySelect {
	t.Helper()
	c, _ := gCtor(t, raw)
	switch c {
	case "SelMax":
		return autoscalingv2.MaxChangePolicySelect
	case "SelMin":
		return autoscalingv2.MinChangePolicySelect
	case "SelDisabled":
		return autoscalingv2.DisabledPolicySelect
	}
	t.Fatalf("unknown select policy %q", c)
	return ""
}

func groundsPolicies(t *testing.T, raw json.RawMessage) []autoscalingv2.HPAScalingPolicy {
	t.Helper()
	var out []autoscalingv2.HPAScalingPolicy
	for _, p := range gList(t, raw) {
		c, _ := gCtor(t, p["ptype"])
		typ := autoscalingv2.PodsScalingPolicy
		if c == "Percent" {
			typ = autoscalingv2.PercentScalingPolicy
		}
		out = append(out, autoscalingv2.HPAScalingPolicy{Type: typ, Value: gInt32(t, p["value"]), PeriodSeconds: gInt32(t, p["period"])})
	}
	return out
}

// groundsEvents turns (change, age-in-seconds) into events stamped now-age;
// the code reads time.Now() later, so age == period lands outside the period
// exactly as the model's strict age < period says.
func groundsEvents(t *testing.T, raw json.RawMessage, now time.Time) []timestampedScaleEvent {
	t.Helper()
	var out []timestampedScaleEvent
	for _, e := range gList(t, raw) {
		out = append(out, timestampedScaleEvent{
			replicaChange: gInt32(t, e["change"]),
			timestamp:     now.Add(-time.Duration(gInt(t, e["age"])) * time.Second),
		})
	}
	return out
}

func newGroundsController() *HorizontalController {
	return &HorizontalController{
		recommendations: map[string][]timestampedRecommendation{},
		scaleUpEvents:   map[string][]timestampedScaleEvent{},
		scaleDownEvents: map[string][]timestampedScaleEvent{},
	}
}

func checkPlainVector(t *testing.T, v groundsVector) {
	t.Helper()
	in := v.Input
	got, err := runPlain(t,
		gInt32(t, in["current"]), gInt(t, in["target"]),
		Tolerances{scaleDown: gReal(t, in["tol_down"]), scaleUp: gReal(t, in["tol_up"])},
		int(gInt(t, in["n_ready"])), int(gInt(t, in["n_unready"])), int(gInt(t, in["n_missing"])),
		gInt(t, in["sum_ready"]))
	c, args := gCtor(t, v.Expected)
	switch c {
	case "Err":
		if err == nil {
			t.Errorf("model: Err; code: %d, nil", got)
		}
	case "Ok":
		want := gInt32(t, args[0])
		if err != nil || got != want {
			t.Errorf("model: Ok %d; code: %d, %v", want, got, err)
		}
	default:
		t.Fatalf("unknown result %q", c)
	}
}

func TestGroundsConformance(t *testing.T) {
	t.Run("plain_replicas", func(t *testing.T) {
		f := loadGroundsVectors(t, "plain_replicas.vectors.json", "plain_replicas")
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) { checkPlainVector(t, v) })
		}
	})

	// Integer neighbours of the boundaries of the real-relaxed decomposition (plain_real).
	t.Run("plain_replicas_int_boundaries", func(t *testing.T) {
		f := loadGroundsVectors(t, "plain_replicas_int_boundaries.vectors.json", "plain_replicas")
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) { checkPlainVector(t, v) })
		}
	})

	t.Run("usage_ratio_replicas", func(t *testing.T) {
		f := loadGroundsVectors(t, "usage_ratio_replicas.vectors.json", "usage_ratio_replicas")
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				in := v.Input
				ready := int(gInt(t, in["ready"]))
				var pods []*v1.Pod
				for i := 0; i < ready; i++ {
					pods = append(pods, groundsPod(fmt.Sprintf("ready-%d", i), v1.PodRunning, true, "1"))
				}
				// getReadyPodsCount errors on an empty pod list; the model's
				// ready=0 means pods exist but none is ready.
				pods = append(pods, groundsPod("not-ready", v1.PodRunning, false, "1"))
				rc := groundsCalculator(t, pods, nil)
				got, _, err := rc.getUsageRatioReplicaCount(gInt32(t, in["current"]), gReal(t, in["ratio"]),
					Tolerances{scaleDown: gReal(t, in["tol_down"]), scaleUp: gReal(t, in["tol_up"])},
					groundsNamespace, labels.Everything())
				want := gInt32(t, v.Expected)
				if err != nil || got != want {
					t.Errorf("model: %d; code: %d, %v", want, got, err)
				}
			})
		}
	})

	t.Run("rules_normalize", func(t *testing.T) {
		f := loadGroundsVectors(t, "rules_normalize.vectors.json", "rules_normalize")
		// this package's test init() raises the factor to 8; the model encodes production's 2
		saved := scaleUpLimitFactor
		scaleUpLimitFactor = 2.0
		defer func() { scaleUpLimitFactor = saved }()
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				in := v.Input
				got, _, _ := convertDesiredReplicasWithRules(gInt32(t, in["current"]), gInt32(t, in["desired"]), gInt32(t, in["minr"]), gInt32(t, in["maxr"]))
				if want := gInt32(t, v.Expected); got != want {
					t.Errorf("model: %d; code: %d", want, got)
				}
			})
		}
	})

	t.Run("rate_limit_core", func(t *testing.T) {
		f := loadGroundsVectors(t, "rate_limit_core.vectors.json", "rate_limit_core")
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				in := v.Input
				current := gInt32(t, in["current"])
				// A single Pods policy with no recorded events makes the
				// limit exactly current +/- value, so it can carry any up_lim/down_lim.
				sel := autoscalingv2.MaxChangePolicySelect
				up := &autoscalingv2.HPAScalingRules{SelectPolicy: &sel, Policies: []autoscalingv2.HPAScalingPolicy{
					{Type: autoscalingv2.PodsScalingPolicy, Value: gInt32(t, in["up_lim"]) - current, PeriodSeconds: 60}}}
				down := &autoscalingv2.HPAScalingRules{SelectPolicy: &sel, Policies: []autoscalingv2.HPAScalingPolicy{
					{Type: autoscalingv2.PodsScalingPolicy, Value: current - gInt32(t, in["down_lim"]), PeriodSeconds: 60}}}
				got, _, _ := newGroundsController().convertDesiredReplicasWithBehaviorRate(NormalizationArg{
					Key: "k", ScaleUpBehavior: up, ScaleDownBehavior: down,
					MinReplicas: gInt32(t, in["minr"]), MaxReplicas: gInt32(t, in["maxr"]),
					CurrentReplicas: current, DesiredReplicas: gInt32(t, in["desired"]),
				})
				if want := gInt32(t, v.Expected); got != want {
					t.Errorf("model: %d; code: %d", want, got)
				}
			})
		}
	})

	t.Run("rate_limit", func(t *testing.T) {
		f := loadGroundsVectors(t, "rate_limit.vectors.json", "rate_limit")
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				in := v.Input
				now := time.Now()
				a := newGroundsController()
				a.scaleUpEvents["k"] = groundsEvents(t, in["ups"], now)
				a.scaleDownEvents["k"] = groundsEvents(t, in["downs"], now)
				upSel, downSel := groundsSelect(t, in["up_sel"]), groundsSelect(t, in["down_sel"])
				got, _, _ := a.convertDesiredReplicasWithBehaviorRate(NormalizationArg{
					Key:               "k",
					ScaleUpBehavior:   &autoscalingv2.HPAScalingRules{SelectPolicy: &upSel, Policies: groundsPolicies(t, in["up_ps"])},
					ScaleDownBehavior: &autoscalingv2.HPAScalingRules{SelectPolicy: &downSel, Policies: groundsPolicies(t, in["down_ps"])},
					MinReplicas:       gInt32(t, in["minr"]), MaxReplicas: gInt32(t, in["maxr"]),
					CurrentReplicas: gInt32(t, in["current"]), DesiredReplicas: gInt32(t, in["desired"]),
				})
				if want := gInt32(t, v.Expected); got != want {
					t.Errorf("model: %d; code: %d", want, got)
				}
			})
		}
	})

	t.Run("stabilize", func(t *testing.T) {
		f := loadGroundsVectors(t, "stabilize.vectors.json", "stabilize")
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				in := v.Input
				now := time.Now()
				a := newGroundsController()
				for _, r := range gList(t, in["recs"]) {
					a.recommendations["k"] = append(a.recommendations["k"], timestampedRecommendation{
						recommendation: gInt32(t, r["rec_replicas"]),
						timestamp:      now.Add(-time.Duration(gInt(t, r["rec_age"])) * time.Second),
					})
				}
				upW, downW := gInt32(t, in["up_w"]), gInt32(t, in["down_w"])
				got, _, _ := a.stabilizeRecommendationWithBehaviors(NormalizationArg{
					Key:               "k",
					ScaleUpBehavior:   &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: &upW},
					ScaleDownBehavior: &autoscalingv2.HPAScalingRules{StabilizationWindowSeconds: &downW},
					CurrentReplicas:   gInt32(t, in["current"]), DesiredReplicas: gInt32(t, in["desired"]),
				})
				if want := gInt32(t, v.Expected); got != want {
					t.Errorf("model: %d; code: %d", want, got)
				}
			})
		}
	})
}

type groundsResourceMetrics struct {
	metricsclient.MetricsClient
	metrics metricsclient.PodMetricsInfo
}

func (g groundsResourceMetrics) GetResourceMetric(_ context.Context, _ v1.ResourceName, _ string, _ labels.Selector, _ string) (metricsclient.PodMetricsInfo, time.Time, error) {
	out := metricsclient.PodMetricsInfo{}
	for k, v := range g.metrics {
		out[k] = v
	}
	return out, time.Now(), nil
}

// TestGroundsFindings pins what the code does on the edge cases ImandraX found,
// so a change in behaviour shows up. Each case states the exact-arithmetic answer.
func TestGroundsFindings(t *testing.T) {
	def := Tolerances{scaleDown: 0.1, scaleUp: 0.1}

	t.Run("ceil of a float product just above an integer adds a replica", func(t *testing.T) {
		// 25 pods at 14 against a target of 50: exactly 0.28*25 = 7, float64 gives 7.000000000000001.
		got, err := runPlain(t, 25, 50, def, 25, 0, 0, 25*14)
		if err != nil || got != 8 {
			t.Errorf("got %d, %v; want 8 (exact arithmetic: 7)", got, err)
		}
	})

	t.Run("ratio exactly at 1-tolerance scales down", func(t *testing.T) {
		// 1-0.7 is 0.30000000000000004 in float64, so ratio 0.3 falls outside the band.
		got, err := runPlain(t, 10, 100, Tolerances{scaleDown: 0.7, scaleUp: 0.1}, 10, 0, 0, 300)
		if err != nil || got != 3 {
			t.Errorf("got %d, %v; want 3 (exact arithmetic: no change, 10)", got, err)
		}
		// The same tolerance written as a per-HPA Quantity rounds the other way,
		// so the band edge depends on where the tolerance came from.
		q := resource.MustParse("700m")
		qf := q.AsApproximateFloat64()
		if qf != 0.7000000000000001 {
			t.Errorf("700m as float64: got %v; want 0.7000000000000001", qf)
		}
		got, err = runPlain(t, 10, 100, Tolerances{scaleDown: qf, scaleUp: 0.1}, 10, 0, 0, 300)
		if err != nil || got != 10 {
			t.Errorf("Quantity tolerance: got %d, %v; want 10", got, err)
		}
	})

	t.Run("ratio exactly at 1+tolerance scales up", func(t *testing.T) {
		// 1+0.36 is 1.3599999999999999 in float64, below ratio 1.36.
		got, err := runPlain(t, 10, 100, Tolerances{scaleDown: 0.1, scaleUp: 0.36}, 10, 0, 0, 1360)
		if err != nil || got != 14 {
			t.Errorf("got %d, %v; want 14 (exact arithmetic: no change, 10)", got, err)
		}
	})

	t.Run("fast path ignores currentReplicas: overload scales down", func(t *testing.T) {
		// 3 pods exist for spec.replicas=10, each at twice the target.
		got, err := runPlain(t, 10, 100, def, 3, 0, 0, 600)
		if err != nil || got != 6 {
			t.Errorf("got %d, %v; want 6", got, err)
		}
	})

	t.Run("object/external metric with no ready pod proposes 0", func(t *testing.T) {
		rc := groundsCalculator(t, []*v1.Pod{groundsPod("p", v1.PodRunning, false, "1")}, nil)
		got, _, err := rc.getUsageRatioReplicaCount(10, 5.0, def, groundsNamespace, labels.Everything())
		if err != nil || got != 0 {
			t.Errorf("got %d, %v; want 0 at five times the target", got, err)
		}
	})

	t.Run("missing pods filled below target on a scale-down", func(t *testing.T) {
		// target 150%: one ready pod at 135% (ratio 0.9, inside the band); ten
		// missing pods requesting 3m are filled with 3*150/100 = 4 (133%), not 4.5.
		pods := []*v1.Pod{groundsPod("ready", v1.PodRunning, true, "100m")}
		metrics := metricsclient.PodMetricsInfo{"ready": {Value: 135, Timestamp: time.Now(), Window: time.Minute}}
		for i := 0; i < 10; i++ {
			pods = append(pods, groundsPod(fmt.Sprintf("missing-%d", i), v1.PodRunning, true, "3m"))
		}
		rc := groundsCalculator(t, pods, groundsResourceMetrics{metrics: metrics})
		got, _, _, _, err := rc.GetResourceReplicas(context.Background(), 11, 150, v1.ResourceMemory, def, groundsNamespace, labels.Everything(), "")
		if err != nil || got != 10 {
			t.Errorf("got %d, %v; want 10 although the measured ratio is within tolerance", got, err)
		}
	})

	t.Run("percent policies round past the limit", func(t *testing.T) {
		sel := autoscalingv2.MaxChangePolicySelect
		up := &autoscalingv2.HPAScalingRules{SelectPolicy: &sel, Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 10, PeriodSeconds: 60}}}
		if got := calculateScaleUpLimitWithScalingRules(100, nil, nil, up); got != 111 {
			t.Errorf("10%% of 100: got %d; want 111 (exact arithmetic: 110)", got)
		}
		down := &autoscalingv2.HPAScalingRules{SelectPolicy: &sel, Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 30, PeriodSeconds: 60}}}
		if got := calculateScaleDownLimitWithBehaviors(90, nil, nil, down); got != 62 {
			t.Errorf("30%% of 90: got %d; want 62 (exact arithmetic: 63)", got)
		}
	})

	t.Run("percent-only or selectPolicy Min never scales up from zero", func(t *testing.T) {
		maxSel, minSel := autoscalingv2.MaxChangePolicySelect, autoscalingv2.MinChangePolicySelect
		for name, rules := range map[string]*autoscalingv2.HPAScalingRules{
			"percent only": {SelectPolicy: &maxSel, Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}},
			"min of defaults": {SelectPolicy: &minSel, Policies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 15},
				{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15}}},
		} {
			got, reason, _ := newGroundsController().convertDesiredReplicasWithBehaviorRate(NormalizationArg{
				Key: "k", ScaleUpBehavior: rules, ScaleDownBehavior: rules,
				MinReplicas: 0, MaxReplicas: 10, CurrentReplicas: 0, DesiredReplicas: 5,
			})
			if got != 0 {
				t.Errorf("%s: got %d (%s); want 0", name, got, reason)
			}
		}
	})

	t.Run("a scale-down in the period buys extra scale-up", func(t *testing.T) {
		sel := autoscalingv2.MaxChangePolicySelect
		up := &autoscalingv2.HPAScalingRules{SelectPolicy: &sel, Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 60}}}
		downEvents := []timestampedScaleEvent{{replicaChange: 5, timestamp: time.Now().Add(-30 * time.Second)}}
		if got := calculateScaleUpLimitWithScalingRules(10, nil, downEvents, up); got != 19 {
			t.Errorf("got %d; want 19 (9 pods added within 60s under a 4 pods/60s policy)", got)
		}
	})
}

// groundsIntBoundaryShifts are the chosen integer boundary points (by index)
// where ImandraX's integer model and its real relaxation disagree after
// ceiling. Every one traces to GetMetricUsageRatio truncating the average to
// an int64 before dividing by the target.
const groundsIntBoundaryShifts = 55

// TestGroundsIntBoundaryShifts compares, at each integer boundary point, the
// integer model (what the code computes) with the real relaxation evaluated at
// the same point (exact average, exact proposal). Both outputs come from
// ImandraX; a difference means the integer point lands in another region than
// its real neighbour.
func TestGroundsIntBoundaryShifts(t *testing.T) {
	ints := loadGroundsVectors(t, "plain_replicas_int_boundaries.vectors.json", "plain_replicas")
	reals := loadGroundsVectors(t, "plain_real.vectors.json", "plain_real")
	realAt := map[string]groundsVector{}
	for _, v := range reals.Vectors {
		realAt[strings.TrimPrefix(v.ID, "plain_real_")] = v
	}
	shifts := 0
	for _, iv := range ints.Vectors {
		key := strings.TrimPrefix(iv.ID, "plain_replicas_")
		rv, ok := realAt[key]
		if !ok {
			t.Fatalf("no real-model evaluation for %s", iv.ID)
		}
		ic, iargs := gCtor(t, iv.Expected)
		rc, rargs := gCtor(t, rv.Expected)
		if ic != "Ok" || rc != "OkR" {
			if ic == "Ok" || rc == "OkR" {
				t.Errorf("%s: integer %s, real %s", iv.ID, ic, rc)
			}
			continue
		}
		in := gInt(t, iargs[0])
		var r struct {
			Real string `json:"$real"`
		}
		if err := json.Unmarshal(rargs[0], &r); err != nil {
			t.Fatalf("%s: bad real result %s", rv.ID, rargs[0])
		}
		q, _ := new(big.Rat).SetString(r.Real)
		c := new(big.Int).Div(q.Num(), q.Denom())
		if new(big.Rat).SetInt(c).Cmp(q) < 0 {
			c.Add(c, big.NewInt(1))
		}
		if c.Int64() != in {
			shifts++
			t.Logf("%s: integer model %d, real relaxation %s (ceil %d) at %s", iv.ID, in, r.Real, c.Int64(), string(mustJSON(iv.Input)))
		}
	}
	t.Logf("%d of %d integer boundary points shift", shifts, len(ints.Vectors))
	if groundsIntBoundaryShifts >= 0 && shifts != groundsIntBoundaryShifts {
		t.Errorf("got %d shifted points, pinned %d", shifts, groundsIntBoundaryShifts)
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
