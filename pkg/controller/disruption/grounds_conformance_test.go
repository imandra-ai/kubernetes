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

package disruption

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/ktesting"
)

// Binds the ImandraX model "PDB disruption arithmetic" (pdb_status, pod_observe)
// to trySync. Vectors are generated from the model's region decomposition and
// live in .grounds/conformance; they must not be edited by hand.

const groundsConformanceDir = "../../../.grounds/conformance"

type groundsVector struct {
	ID       string          `json:"id"`
	Input    json.RawMessage `json:"input"`
	Expected json.RawMessage `json:"expected"`
}

type groundsVariant struct {
	C    string            `json:"$c"`
	Args []json.RawMessage `json:"args"`
}

func loadGroundsVectors(t *testing.T, file string) []groundsVector {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(groundsConformanceDir, file))
	if err != nil {
		t.Fatalf("reading vectors %s: %v", file, err)
	}
	var vf struct {
		Vectors []groundsVector `json:"vectors"`
	}
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatalf("decoding vectors %s: %v", file, err)
	}
	if len(vf.Vectors) == 0 {
		t.Fatalf("no vectors in %s", file)
	}
	return vf.Vectors
}

func toInt32(t *testing.T, name string, v int64) int32 {
	t.Helper()
	if v < math.MinInt32 || v > math.MaxInt32 {
		t.Fatalf("%s=%d does not fit the int32 the API uses", name, v)
	}
	return int32(v)
}

// groundsIntOrString maps the model's ios (IntV n | Pct p) to the API value.
func groundsIntOrString(t *testing.T, raw json.RawMessage) intstr.IntOrString {
	t.Helper()
	var v groundsVariant
	if err := json.Unmarshal(raw, &v); err != nil || len(v.Args) != 1 {
		t.Fatalf("bad ios %s: %v", raw, err)
	}
	var n int64
	if err := json.Unmarshal(v.Args[0], &n); err != nil {
		t.Fatalf("bad ios arg %s: %v", raw, err)
	}
	switch v.C {
	case "IntV":
		return intstr.FromInt32(toInt32(t, "IntV", n))
	case "Pct":
		return intstr.FromString(fmt.Sprintf("%d%%", n))
	}
	t.Fatalf("unknown ios constructor %q", v.C)
	return intstr.IntOrString{}
}

// TestGroundsConformancePdbStatus drives the whole sync: one ReplicaSet of the
// given scale owning n_pods selected pods, the first cur of them Ready.
func TestGroundsConformancePdbStatus(t *testing.T) {
	for _, v := range loadGroundsVectors(t, "pdb_status.vectors.json") {
		t.Run(v.ID, func(t *testing.T) {
			var in struct {
				S     groundsVariant `json:"s"`
				NPods int64          `json:"n_pods"`
				Scale int64          `json:"scale"`
				Cur   int64          `json:"cur"`
			}
			if err := json.Unmarshal(v.Input, &in); err != nil {
				t.Fatalf("bad input: %v", err)
			}
			var want struct {
				Expected int64 `json:"expected"`
				Desired  int64 `json:"desired"`
				Allowed  int64 `json:"allowed"`
			}
			if err := json.Unmarshal(v.Expected, &want); err != nil {
				t.Fatalf("bad expected: %v", err)
			}

			_, ctx := ktesting.NewTestContext(t)
			dc, ps := newFakeDisruptionController(ctx)

			pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(0))
			pdb.Spec.MinAvailable = nil
			switch in.S.C {
			case "MinAvail":
				val := groundsIntOrString(t, in.S.Args[0])
				pdb.Spec.MinAvailable = &val
			case "MaxUnavail":
				val := groundsIntOrString(t, in.S.Args[0])
				pdb.Spec.MaxUnavailable = &val
			case "NoBudget":
			default:
				t.Fatalf("unknown spec constructor %q", in.S.C)
			}
			add(t, dc.pdbStore, pdb)

			rs, _ := newReplicaSet(t, toInt32(t, "scale", in.Scale))
			add(t, dc.rsStore, rs)
			for i := int64(0); i < in.NPods; i++ {
				pod, _ := newPod(t, fmt.Sprintf("pod-%d", i))
				updatePodOwnerToRs(t, pod, rs)
				if i >= in.Cur {
					pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}
				}
				add(t, dc.podStore, pod)
			}

			if err := dc.sync(ctx, pdbName); err != nil {
				t.Fatalf("sync: %v", err)
			}
			got := ps.Get(pdbName).Status
			if int64(got.ExpectedPods) != want.Expected || int64(got.DesiredHealthy) != want.Desired ||
				int64(got.DisruptionsAllowed) != want.Allowed || int64(got.CurrentHealthy) != in.Cur {
				t.Errorf("model {expected=%d desired=%d allowed=%d currentHealthy=%d}, code {expected=%d desired=%d allowed=%d currentHealthy=%d}",
					want.Expected, want.Desired, want.Allowed, in.Cur,
					got.ExpectedPods, got.DesiredHealthy, got.DisruptionsAllowed, got.CurrentHealthy)
			}
		})
	}
}

// TestGroundsConformancePodObserve checks, for a single pod, whether trySync
// counts it in currentHealthy and whether it keeps it in status.disruptedPods.
func TestGroundsConformancePodObserve(t *testing.T) {
	for _, v := range loadGroundsVectors(t, "pod_observe.vectors.json") {
		t.Run(v.ID, func(t *testing.T) {
			var in struct {
				P struct {
					Deleting     bool           `json:"deleting"`
					Ready        bool           `json:"ready"`
					DisruptedAge groundsVariant `json:"disrupted_age"`
				} `json:"p"`
			}
			if err := json.Unmarshal(v.Input, &in); err != nil {
				t.Fatalf("bad input: %v", err)
			}
			var want struct {
				Tuple []bool `json:"$tuple"`
			}
			if err := json.Unmarshal(v.Expected, &want); err != nil || len(want.Tuple) != 2 {
				t.Fatalf("bad expected %s: %v", v.Expected, err)
			}

			_, ctx := ktesting.NewTestContext(t)
			now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
			dc, ps := newFakeDisruptionControllerWithTime(ctx, now)
			dc.recheckQueue = workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[string]{Name: "pdb_queue"})
			defer dc.recheckQueue.ShutDown()

			pdb, pdbName := newMinAvailablePodDisruptionBudget(t, intstr.FromInt32(0))
			switch in.P.DisruptedAge.C {
			case "Some":
				var age int64
				if err := json.Unmarshal(in.P.DisruptedAge.Args[0], &age); err != nil {
					t.Fatalf("bad age: %v", err)
				}
				pdb.Status.DisruptedPods = map[string]metav1.Time{"p": {Time: now.Add(-time.Duration(age) * time.Second)}}
			case "None":
			default:
				t.Fatalf("unknown option constructor %q", in.P.DisruptedAge.C)
			}
			add(t, dc.pdbStore, pdb)

			pod, _ := newPod(t, "p")
			if !in.P.Ready {
				pod.Status.Conditions = []v1.PodCondition{{Type: v1.PodReady, Status: v1.ConditionFalse}}
			}
			if in.P.Deleting {
				pod.DeletionTimestamp = &metav1.Time{Time: now}
			}
			add(t, dc.podStore, pod)

			if err := dc.sync(ctx, pdbName); err != nil {
				t.Fatalf("sync: %v", err)
			}
			st := ps.Get(pdbName).Status
			_, kept := st.DisruptedPods["p"]
			healthy := st.CurrentHealthy == 1
			if healthy != want.Tuple[0] || kept != want.Tuple[1] {
				t.Errorf("model (counted healthy=%v, kept in disruptedPods=%v), code (%v, %v)",
					want.Tuple[0], want.Tuple[1], healthy, kept)
			}
		})
	}
}
