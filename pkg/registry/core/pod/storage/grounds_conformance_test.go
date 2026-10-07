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

package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/kubernetes/fake"
	podapi "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/policy"
)

// Binds the ImandraX model "PDB disruption arithmetic" (evict) to
// EvictionREST.Create. Vectors are generated from the model's region
// decomposition and live in .grounds/conformance; they must not be edited by hand.

// groundsStore deletes on the first attempt, unlike mockStore, whose
// name-dependent conflicts would change the pod between attempts.
type groundsStore struct {
	mockStore
}

func (s *groundsStore) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	s.deleteCount++
	return s.pod, true, nil
}

type groundsVariant struct {
	C    string            `json:"$c"`
	Args []json.RawMessage `json:"args"`
}

func groundsInt32(t *testing.T, name string, v int64) int32 {
	t.Helper()
	if v < math.MinInt32 || v > math.MaxInt32 {
		t.Fatalf("%s=%d does not fit int32", name, v)
	}
	return int32(v)
}

func TestGroundsConformanceEvict(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("../../../../../.grounds/conformance", "evict.vectors.json"))
	if err != nil {
		t.Fatalf("reading vectors: %v", err)
	}
	var vf struct {
		Vectors []struct {
			ID    string `json:"id"`
			Input struct {
				Ph       groundsVariant `json:"ph"`
				Deleting bool           `json:"deleting"`
				Ready    bool           `json:"ready"`
				NPdbs    int64          `json:"n_pdbs"`
				Pol      groundsVariant `json:"pol"`
				S        struct {
					ObservedGen int64 `json:"observed_gen"`
					Generation  int64 `json:"generation"`
					Allowed     int64 `json:"st_allowed"`
					Current     int64 `json:"st_current"`
					Desired     int64 `json:"st_desired"`
					NDisrupted  int64 `json:"n_disrupted"`
				} `json:"s"`
			} `json:"input"`
			Expected groundsVariant `json:"expected"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatalf("decoding vectors: %v", err)
	}
	if len(vf.Vectors) == 0 {
		t.Fatal("no vectors")
	}

	for _, v := range vf.Vectors {
		t.Run(v.ID, func(t *testing.T) {
			in := v.Input
			want := v.Expected.C
			if want == "Decremented" {
				var k int64
				if err := json.Unmarshal(v.Expected.Args[0], &k); err != nil {
					t.Fatalf("bad Decremented arg: %v", err)
				}
				want = fmt.Sprintf("Decremented %d", k)
			}

			var policyPtr *policyv1.UnhealthyPodEvictionPolicyType
			switch in.Pol.C {
			case "DefaultPolicy":
			case "IfHealthyBudget":
				policyPtr = unhealthyPolicyPtr(policyv1.IfHealthyBudget)
			case "AlwaysAllow":
				policyPtr = unhealthyPolicyPtr(policyv1.AlwaysAllow)
			default:
				t.Fatalf("unknown policy %q", in.Pol.C)
			}

			var pdbs []runtime.Object
			for i := int64(0); i < in.NPdbs; i++ {
				pdb := &policyv1.PodDisruptionBudget{
					ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pdb-%d", i), Namespace: metav1.NamespaceDefault, Generation: in.S.Generation},
					Spec: policyv1.PodDisruptionBudgetSpec{
						Selector:                   &metav1.LabelSelector{MatchLabels: map[string]string{"a": "true"}},
						UnhealthyPodEvictionPolicy: policyPtr,
					},
					Status: policyv1.PodDisruptionBudgetStatus{
						ObservedGeneration: in.S.ObservedGen,
						DisruptionsAllowed: groundsInt32(t, "st_allowed", in.S.Allowed),
						CurrentHealthy:     groundsInt32(t, "st_current", in.S.Current),
						DesiredHealthy:     groundsInt32(t, "st_desired", in.S.Desired),
					},
				}
				if in.S.NDisrupted > 0 {
					pdb.Status.DisruptedPods = makeDisruptedPods(int(in.S.NDisrupted))
				}
				pdbs = append(pdbs, pdb)
			}

			pod := validNewPod()
			pod.Name = "g"
			pod.Labels = map[string]string{"a": "true"}
			pod.Spec.NodeName = "foo"
			pod.ResourceVersion = "100"
			switch in.Ph.C {
			case "Pending":
				pod.Status.Phase = api.PodPending
			case "Running":
				pod.Status.Phase = api.PodRunning
			case "Succeeded":
				pod.Status.Phase = api.PodSucceeded
			case "Failed":
				pod.Status.Phase = api.PodFailed
			case "Unknown":
				pod.Status.Phase = api.PodUnknown
			default:
				t.Fatalf("unknown phase %q", in.Ph.C)
			}
			readyStatus := api.ConditionFalse
			if in.Ready {
				readyStatus = api.ConditionTrue
			}
			podapi.UpdatePodCondition(&pod.Status, &api.PodCondition{Type: api.PodReady, Status: readyStatus})
			if in.Deleting {
				now := metav1.Now()
				pod.DeletionTimestamp = &now
			}

			ms := &groundsStore{mockStore{pod: pod}}
			client := fake.NewSimpleClientset(pdbs...)
			evictionRest := newEvictionStorage(ms, client.PolicyV1())
			testContext := genericapirequest.WithRequestInfo(
				genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault),
				&genericapirequest.RequestInfo{APIGroup: "", APIVersion: "v1", Resource: "pods"})
			eviction := &policy.Eviction{ObjectMeta: metav1.ObjectMeta{Name: "g", Namespace: metav1.NamespaceDefault}, DeleteOptions: metav1.NewDeleteOptions(0)}

			res, err := evictionRest.Create(testContext, "g", eviction, nil, &metav1.CreateOptions{})

			listed, updated := false, false
			for _, a := range client.Actions() {
				switch a.GetVerb() {
				case "list":
					listed = true
				case "update":
					updated = true
				}
			}

			var got string
			switch {
			case err != nil && apierrors.IsTooManyRequests(err):
				if strings.Contains(errToString(err), "is still being processed by the server") {
					got = "Stale429"
				} else {
					got = "Budget429"
				}
			case err != nil && apierrors.IsForbidden(err):
				msg := errToString(err)
				switch {
				case strings.Contains(msg, "disruptions allowed is negative"):
					got = "ForbiddenNegative"
				case strings.Contains(msg, "DisruptedPods map too big"):
					got = "ForbiddenTooManyDisrupted"
				default:
					t.Fatalf("unclassified forbidden error: %v", err)
				}
			case err != nil:
				t.Fatalf("unclassified error: %v", err)
			default:
				status, ok := res.(*metav1.Status)
				if !ok {
					t.Fatalf("expected *metav1.Status, got %T", res)
				}
				switch {
				case status.Status == metav1.StatusFailure && status.Code == 500:
					got = "MultiplePdbs500"
				case !listed:
					got = "DeletedIgnoringPdb"
				case updated:
					after, gerr := client.PolicyV1().PodDisruptionBudgets(metav1.NamespaceDefault).Get(context.TODO(), "pdb-0", metav1.GetOptions{})
					if gerr != nil {
						t.Fatalf("reading PDB after eviction: %v", gerr)
					}
					got = fmt.Sprintf("Decremented %d", after.Status.DisruptionsAllowed)
				case in.NPdbs == 0:
					got = "DeletedNoPdb"
				default:
					got = "DeletedUnhealthyNoDecrement"
				}
			}

			deleted := got == "DeletedIgnoringPdb" || got == "DeletedNoPdb" || got == "DeletedUnhealthyNoDecrement" || strings.HasPrefix(got, "Decremented")
			if deleted != (ms.deleteCount == 1) {
				t.Errorf("outcome %s but pod deleted %d times", got, ms.deleteCount)
			}
			if got != want {
				t.Errorf("model %s, code %s (err=%v)", want, got, err)
			}
		})
	}
}
