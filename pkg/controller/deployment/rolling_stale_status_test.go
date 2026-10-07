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

package deployment

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
)

// staleRS is one ReplicaSet of the simulated world. ready is the number of its
// pods that are really available; status is what the ReplicaSet controller last
// wrote, which is what the deployment controller reads.
type staleRS struct {
	name   string
	image  string
	age    int // minutes after a fixed base; larger is newer
	spec   int32
	ready  int32
	status int32
}

// realAvailable is what remains available once the ReplicaSet controller has
// deleted the excess: it removes not-ready pods first, so an RS keeps
// min(spec, ready) available pods. Status plays no part.
func realAvailable(rss []staleRS) int32 {
	total := int32(0)
	for _, r := range rss {
		total += min(r.spec, r.ready)
	}
	return total
}

func staleDeployment(replicas, surge, unavailable int32, image string) *apps.Deployment {
	maxSurge := intstr.FromInt32(surge)
	maxUnavailable := intstr.FromInt32(unavailable)
	d := newDeployment("foo", replicas, nil, &maxSurge, &maxUnavailable, map[string]string{"foo": "bar"})
	d.Spec.Template.Spec.Containers[0].Image = image
	d.Annotations[deploymentutil.RevisionAnnotation] = "2"
	return d
}

// syncOnce runs one syncDeployment over the world as the controller's caches
// would see it, and returns the world with the spec.replicas it wrote. The
// desired-replicas annotation is what tells a scaling event from a rollout, so
// it is the replica count the deployment had before this sync.
func syncOnce(t *testing.T, d *apps.Deployment, annotated int32, world []staleRS) []staleRS {
	t.Helper()
	_, ctx := ktesting.NewTestContext(t)
	maxReplicas := annotated + deploymentutil.MaxSurge(*d)

	rsObjs := make([]*apps.ReplicaSet, 0, len(world))
	objs := []runtime.Object{d}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i, w := range world {
		r := newReplicaSet(d, w.name, w.spec)
		r.CreationTimestamp = metav1.NewTime(base.Add(time.Duration(w.age) * time.Minute))
		r.Spec.Template = *d.Spec.Template.DeepCopy()
		r.Spec.Template.Spec.Containers[0].Image = w.image
		r.Annotations = map[string]string{
			deploymentutil.RevisionAnnotation:        strconv.Itoa(i + 1),
			deploymentutil.DesiredReplicasAnnotation: strconv.Itoa(int(annotated)),
			deploymentutil.MaxReplicasAnnotation:     strconv.Itoa(int(maxReplicas)),
		}
		r.Status.Replicas = max(w.spec, w.status)
		r.Status.ReadyReplicas = w.status
		r.Status.AvailableReplicas = w.status
		rsObjs = append(rsObjs, r)
		objs = append(objs, r)
	}

	client := fake.NewClientset(objs...)
	factory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
	dc, err := NewDeploymentController(ctx, factory.Apps().V1().Deployments(), factory.Apps().V1().ReplicaSets(), factory.Core().V1().Pods(), client)
	if err != nil {
		t.Fatalf("creating deployment controller: %v", err)
	}
	dc.eventRecorder = &record.FakeRecorder{}
	dc.dListerSynced, dc.rsListerSynced, dc.podListerSynced = alwaysReady, alwaysReady, alwaysReady
	if err := factory.Apps().V1().Deployments().Informer().GetIndexer().Add(d); err != nil {
		t.Fatal(err)
	}
	for _, r := range rsObjs {
		if err := factory.Apps().V1().ReplicaSets().Informer().GetIndexer().Add(r); err != nil {
			t.Fatal(err)
		}
	}

	if err := dc.syncDeployment(ctx, d.Namespace+"/"+d.Name); err != nil {
		t.Fatalf("syncDeployment: %v", err)
	}

	out := make([]staleRS, len(world))
	for i, w := range world {
		got, err := client.AppsV1().ReplicaSets(d.Namespace).Get(ctx, w.name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("reading back %s: %v", w.name, err)
		}
		w.spec = *got.Spec.Replicas
		out[i] = w
	}
	list, err := client.AppsV1().ReplicaSets(d.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != len(world) {
		t.Fatalf("controller created a ReplicaSet; the world must already hold the new one")
	}
	return out
}

// settle is the ReplicaSet controller catching up: status reflects the pods
// left after deletion.
func settle(world []staleRS) []staleRS {
	out := make([]staleRS, len(world))
	for i, w := range world {
		w.ready = min(w.spec, w.ready)
		w.status = w.ready
		out[i] = w
	}
	return out
}

func describe(world []staleRS) string {
	s := ""
	for _, w := range world {
		s += fmt.Sprintf(" %s(spec=%d ready=%d status=%d)", w.name, w.spec, w.ready, w.status)
	}
	return s
}

// exploreStale runs rolling syncs from world; before each, any subset of the
// ReplicaSets may have settled, and the new ReplicaSet's pods may have become
// ready. It returns a trace to the first state whose real availability falls
// below floor while the status the controller read did not say so.
func exploreStale(t *testing.T, d *apps.Deployment, floor int32, world []staleRS, depth int) string {
	if depth == 0 {
		return ""
	}
	for mask := 0; mask < 1<<len(world); mask++ {
		for _, ready := range []bool{false, true} {
			in := make([]staleRS, len(world))
			copy(in, world)
			for i := range in {
				if mask&(1<<i) != 0 {
					in[i] = settle(in[i : i+1])[0]
				}
			}
			if ready {
				nw := &in[len(in)-1]
				if nw.ready >= nw.spec {
					continue
				}
				nw.ready = nw.spec
				nw.status = nw.ready
			}
			out := syncOnce(t, d, *d.Spec.Replicas, in)
			if realAvailable(in) >= floor && realAvailable(out) < floor {
				return fmt.Sprintf(" | sync on%s -> %s real %d", describe(in), describe(out), realAvailable(out))
			}
			if trace := exploreStale(t, d, floor, out, depth-1); trace != "" {
				return fmt.Sprintf(" | sync on%s -> %s", describe(in), describe(out)) + trace
			}
		}
	}
	return ""
}

// TestSearchStaleScaleEvent enumerates small fresh rollout states, applies a
// scaling event, then rolling syncs on the status the RS controller left, and
// reports where only staleness takes real availability below the floor.
//
// STALE_SEARCH=transient: any fresh rollout state, one stale sync (finds 26).
// STALE_SEARCH=idle: only states a sync leaves unchanged, then up to 3 syncs
// with any order of settling and new pods turning ready (finds 0). Minutes.
func TestSearchStaleScaleEvent(t *testing.T) {
	mode := os.Getenv("STALE_SEARCH")
	if mode != "transient" && mode != "idle" {
		t.Skip("set STALE_SEARCH=transient or STALE_SEARCH=idle to run the search")
	}
	found := 0
	for d0 := int32(2); d0 <= 5; d0++ {
		for d1 := int32(1); d1 <= d0+2; d1++ {
			if d1 == d0 {
				continue
			}
			for surge := int32(0); surge <= 2; surge++ {
				for mu := int32(0); mu <= 2; mu++ {
					if surge == 0 && mu == 0 {
						continue
					}
					for n0 := int32(1); n0 <= d0; n0++ {
						for o0 := int32(1); n0+o0 <= d0+surge; o0++ {
							for rn := int32(0); rn <= n0; rn++ {
								for ro := int32(0); ro <= o0; ro++ {
									if rn+ro < d0-mu {
										continue
									}
									before := []staleRS{
										{name: "foo-old", image: "v1", age: 1, spec: o0, ready: ro, status: ro},
										{name: "foo-new", image: "v2", age: 2, spec: n0, ready: rn, status: rn},
									}
									d1d := staleDeployment(d1, surge, mu, "v2")
									floor := d1 - deploymentutil.MaxUnavailable(*d1d)
									if mode == "idle" {
										// Only states the controller would sit in: a sync changes nothing.
										idle := syncOnce(t, staleDeployment(d0, surge, mu, "v2"), d0, before)
										if idle[0].spec != o0 || idle[1].spec != n0 {
											continue
										}
										if d1 == 1 {
											d0d := staleDeployment(d0, surge, mu, "v2")
											if trace := exploreStale(t, d0d, d0-deploymentutil.MaxUnavailable(*d0d), before, 3); trace != "" {
												found++
												t.Logf("idle, no scaling event, d %d surge %d mu %d: before%s%s", d0, surge, mu, describe(before), trace)
											}
										}
									}
									afterScale := syncOnce(t, staleDeployment(d1, surge, mu, "v2"), d0, before)
									if realAvailable(afterScale) < floor {
										continue
									}
									if mode == "idle" {
										if trace := exploreStale(t, d1d, floor, afterScale, 3); trace != "" {
											found++
											t.Logf("idle d %d->%d surge %d mu %d floor %d: before%s | after scale%s%s", d0, d1, surge, mu, floor, describe(before), describe(afterScale), trace)
										}
										continue
									}
									stale := syncOnce(t, d1d, d1, afterScale)
									if realAvailable(stale) >= floor {
										continue
									}
									fresh := syncOnce(t, d1d, d1, settle(afterScale))
									if realAvailable(fresh) < floor {
										continue
									}
									found++
									if found <= 10 {
										t.Logf("d %d->%d surge %d mu %d floor %d: before%s | after scale%s | stale sync%s real %d | settled sync%s real %d",
											d0, d1, surge, mu, floor, describe(before), describe(afterScale), describe(stale), realAvailable(stale), describe(fresh), realAvailable(fresh))
									}
								}
							}
						}
					}
				}
			}
		}
	}
	t.Logf("found %d", found)
}

// TestRollingSyncOnStaleStatusAfterScaleEvent: a scaling event (kubectl scale,
// an HPA) during a rollout makes scale() lower the new ReplicaSet. The update
// enqueues the deployment again, and the next sync is a rolling sync that reads
// the status the ReplicaSet controller wrote - which, being computed from the
// pods listed before it deleted any, still counts the removed pods as available.
//
// The starting state is transient: the new ReplicaSet's last pod has just
// become ready and the old one has not been scaled for it yet. The scaling
// event has to be handled in that same sync (the workqueue coalesces both
// updates into one key); from a state the controller rests in, no sequence of
// up to three syncs breaks the floor (see TestSearchStaleScaleEvent).
//
// This test documents the defect as the code stands: it passes while the
// stale sync breaks maxUnavailable, and fails once that is fixed.
func TestRollingSyncOnStaleStatusAfterScaleEvent(t *testing.T) {
	const surge, unavailable = 1, 0
	before := []staleRS{
		{name: "foo-old", image: "v1", age: 1, spec: 2, ready: 2, status: 2},
		{name: "foo-new", image: "v2", age: 2, spec: 3, ready: 3, status: 3},
	}
	const floor = int32(3 - unavailable)

	// 4 -> 3: proportional scaling takes the new ReplicaSet 3 -> 2.
	afterScale := syncOnce(t, staleDeployment(3, surge, unavailable, "v2"), 4, before)
	if got := realAvailable(afterScale); got < floor {
		t.Fatalf("scaling event alone breaks the floor (%d < %d):%s", got, floor, describe(afterScale))
	}
	if afterScale[0].spec != 2 || afterScale[1].spec != 2 {
		t.Fatalf("scaling event: want old=2 new=2, got%s", describe(afterScale))
	}

	// The annotations now say 3, so the next sync is a rolling one.
	d := staleDeployment(3, surge, unavailable, "v2")
	fresh := syncOnce(t, d, 3, settle(afterScale))
	stale := syncOnce(t, d, 3, afterScale)
	t.Logf("settled status:%s -> real available %d", describe(fresh), realAvailable(fresh))
	t.Logf("stale status:%s -> real available %d", describe(stale), realAvailable(stale))

	if got := realAvailable(fresh); got < floor {
		t.Errorf("settled status: real available %d below floor %d", got, floor)
	}
	// new reads 2 spec / 3 available: maxScaledDown = 4 - 3 - (2-3) = 2 and
	// the available count is 5, so the old ReplicaSet goes 2 -> 0 while only 2
	// new pods remain.
	if got := realAvailable(stale); got != 2 || stale[0].spec != 0 {
		t.Errorf("stale status: want old scaled to 0 and real available 2 (floor %d, maxUnavailable 0), got%s real %d", floor, describe(stale), got)
	}
}
