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
	"context"
	"fmt"
	"testing"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2/ktesting"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/ptr"
)

// TestScaleDuringRolloutKeepsMaxUnavailable scales a deployment down in the
// same sync that sees a new pod become ready. Proportional scaling lowers the
// new ReplicaSet; the following rolling sync reads that ReplicaSet's status,
// which the ReplicaSet controller computes from the pods it listed before
// deleting the excess, and so still counts the deleted pod as available.
//
// Nothing marks pods ready after the scaling event, so the state the
// controllers settle in is final: the old ReplicaSet should keep one pod
// (3 available with maxUnavailable 0); if it is scaled to 0, only 2 remain.
// Whether the stale read happens is a race between the two controllers, so
// the scenario is repeated.
func TestScaleDuringRolloutKeepsMaxUnavailable(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-scale-during-rollout", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	const runs = 10
	broken := 0
	for i := range runs {
		name := fmt.Sprintf("d%d", i)
		tester, oldRS, newRS := stageRollout(ctx, t, c, ns.Name, name)

		// Paused, the controller keeps the replica counts while the third pod
		// becomes ready, so the scaling event is sure to be handled before the
		// rolling sync that would scale the old ReplicaSet down.
		var err error
		if tester.deployment, err = tester.updateDeployment(pauseFn); err != nil {
			t.Fatalf("pausing: %v", err)
		}
		if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
			t.Fatal(err)
		}
		markReadyUpTo(ctx, t, c, ns.Name, newRS, 3)
		waitForRS(ctx, t, c, tester, func(r *apps.ReplicaSet) bool {
			return r.UID == newRS.UID && r.Status.AvailableReplicas == 3
		})
		waitForSpecs(ctx, t, c, ns.Name, oldRS.Name, newRS.Name, 2, 3)

		if tester.deployment, err = tester.updateDeployment(func(u *apps.Deployment) {
			u.Spec.Replicas = ptr.To[int32](3)
			u.Spec.Paused = false
		}); err != nil {
			t.Fatalf("scaling and resuming: %v", err)
		}
		available, oldSpec := settleAfterScale(ctx, t, tester, oldRS, newRS)
		t.Logf("run %d: old ReplicaSet settled at %d, real available %d (floor 3)", i, oldSpec, available)
		if available < 3 {
			broken++
		}
	}
	if broken > 0 {
		t.Errorf("%d of %d runs settled below replicas - maxUnavailable (3 - 0): stale ReplicaSet status let the old ReplicaSet be scaled down too far", broken, runs)
	}
}

// TestScaleRightAfterPodReadyKeepsMaxUnavailable is the unstaged version: no
// pause, just the scale subresource set 4 -> 3 (what kubectl scale or an HPA
// sends) at a fixed delay after the third new pod is marked ready. Whether the
// deployment controller handles the readiness (scaling old 2 -> 1) or the
// scaling event first is left to the controllers.
func TestScaleRightAfterPodReadyKeepsMaxUnavailable(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	closeFn, rm, dc, informers, c := dcSetup(ctx, t)
	defer closeFn()
	ns := framework.CreateNamespaceOrDie(c, "test-scale-after-ready", t)
	defer framework.DeleteNamespaceOrDie(c, ns, t)
	stopControllers := runControllersAndInformers(t, rm, dc, informers)
	defer stopControllers()

	const runs = 20
	var report []string
	total := 0
	for _, delay := range []time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond} {
		broken := 0
		for i := range runs {
			name := fmt.Sprintf("d%d-%d", delay.Milliseconds(), i)
			tester, oldRS, newRS := stageRollout(ctx, t, c, ns.Name, name)

			markReadyUpTo(ctx, t, c, ns.Name, newRS, 3)
			time.Sleep(delay)
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				s, err := c.AppsV1().Deployments(ns.Name).GetScale(ctx, name, metav1.GetOptions{})
				if err != nil {
					return err
				}
				s.Spec.Replicas = 3
				_, err = c.AppsV1().Deployments(ns.Name).UpdateScale(ctx, name, s, metav1.UpdateOptions{})
				return err
			})
			if err != nil {
				t.Fatalf("scaling %s to 3: %v", name, err)
			}
			if tester.deployment, err = c.AppsV1().Deployments(ns.Name).Get(ctx, name, metav1.GetOptions{}); err != nil {
				t.Fatal(err)
			}

			available, oldSpec := settleAfterScale(ctx, t, tester, oldRS, newRS)
			t.Logf("delay %v run %d: old ReplicaSet settled at %d, real available %d (floor 3)", delay, i, oldSpec, available)
			if available < 3 {
				broken++
			}
		}
		total += broken
		report = append(report, fmt.Sprintf("delay %v: %d of %d", delay, broken, runs))
	}
	t.Logf("runs below the floor: %v", report)
	if total > 0 {
		t.Errorf("runs settled below replicas - maxUnavailable (3 - 0): %v", report)
	}
}

// stageRollout creates a 4-replica deployment (maxSurge 1, maxUnavailable 0),
// rolls it to a new template, and walks the rollout to old 2/2 and new 3 with
// 2 ready, one pod at a time.
func stageRollout(ctx context.Context, t *testing.T, c clientset.Interface, ns, name string) (*deploymentTester, *apps.ReplicaSet, *apps.ReplicaSet) {
	t.Helper()
	d := newDeployment(name, ns, 4)
	d.Spec.Selector.MatchLabels = map[string]string{"name": name}
	d.Spec.Template.Labels = map[string]string{"name": name}
	d.Spec.Strategy.RollingUpdate = &apps.RollingUpdateDeployment{
		MaxSurge:       ptr.To(intstr.FromInt32(1)),
		MaxUnavailable: ptr.To(intstr.FromInt32(0)),
	}
	d, err := c.AppsV1().Deployments(ns).Create(ctx, d, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("creating deployment: %v", err)
	}
	tester := &deploymentTester{t: t, c: c, deployment: d}
	if err := tester.waitForDeploymentRevisionAndImage("1", fakeImage); err != nil {
		t.Fatal(err)
	}
	if err := tester.waitForDeploymentCompleteAndMarkPodsReady(); err != nil {
		t.Fatal(err)
	}
	oldRS, err := tester.expectNewReplicaSet()
	if err != nil {
		t.Fatal(err)
	}

	if tester.deployment, err = tester.updateDeployment(func(u *apps.Deployment) {
		u.Spec.Template.Spec.Containers[0].Image = "v2"
	}); err != nil {
		t.Fatalf("starting rollout: %v", err)
	}
	newRS := waitForRS(ctx, t, c, tester, func(r *apps.ReplicaSet) bool { return r.UID != oldRS.UID })

	// Walk the rollout to old 2/2, new 3 with 2 ready, one pod at a time.
	for _, step := range []struct{ old, new int32 }{{4, 1}, {3, 2}, {2, 3}} {
		waitForSpecs(ctx, t, c, ns, oldRS.Name, newRS.Name, step.old, step.new)
		if step.new < 3 {
			markOneReady(ctx, t, c, ns, newRS)
		}
	}
	markReadyUpTo(ctx, t, c, ns, newRS, 2)
	return tester, oldRS, newRS
}

// settleAfterScale waits until the controllers have handled the scaling event
// and stopped moving, then returns the real available pods and the old
// ReplicaSet's replicas. Nothing marks pods ready afterwards, so the settled
// state is final: old at 1 keeps 3 available, old at 0 leaves 2.
func settleAfterScale(ctx context.Context, t *testing.T, tester *deploymentTester, oldRS, newRS *apps.ReplicaSet) (int32, int32) {
	t.Helper()
	c, ns := tester.c, tester.deployment.Namespace
	if err := tester.waitForObservedDeployment(tester.deployment.Generation); err != nil {
		t.Fatal(err)
	}

	var last string
	stableSince := time.Now()
	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		o, err := c.AppsV1().ReplicaSets(ns).Get(ctx, oldRS.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		n, err := c.AppsV1().ReplicaSets(ns).Get(ctx, newRS.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		now := fmt.Sprintf("%d/%d/%d", *o.Spec.Replicas, *n.Spec.Replicas, n.Status.Replicas+o.Status.Replicas)
		if now != last {
			last, stableSince = now, time.Now()
		}
		return *n.Spec.Replicas == 3 && time.Since(stableSince) > 2*time.Second, nil
	})
	if err != nil {
		t.Fatalf("waiting for the controllers to settle (old/new/pods %s): %v", last, err)
	}

	o, err := c.AppsV1().ReplicaSets(ns).Get(ctx, oldRS.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return realAvailablePods(ctx, t, c, ns, tester.deployment.Name), *o.Spec.Replicas
}

func waitForRS(ctx context.Context, t *testing.T, c clientset.Interface, tester *deploymentTester, ok func(*apps.ReplicaSet) bool) *apps.ReplicaSet {
	t.Helper()
	var found *apps.ReplicaSet
	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		list, err := c.AppsV1().ReplicaSets(tester.deployment.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: metav1.FormatLabelSelector(tester.deployment.Spec.Selector),
		})
		if err != nil {
			return false, err
		}
		for i := range list.Items {
			if ok(&list.Items[i]) {
				found = &list.Items[i]
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("waiting for ReplicaSet: %v", err)
	}
	return found
}

func waitForSpecs(ctx context.Context, t *testing.T, c clientset.Interface, ns, oldName, newName string, old, new int32) {
	t.Helper()
	var got string
	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		o, err := c.AppsV1().ReplicaSets(ns).Get(ctx, oldName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		n, err := c.AppsV1().ReplicaSets(ns).Get(ctx, newName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		got = fmt.Sprintf("old=%d new=%d", *o.Spec.Replicas, *n.Spec.Replicas)
		return *o.Spec.Replicas == old && *n.Spec.Replicas == new, nil
	})
	if err != nil {
		t.Fatalf("waiting for old=%d new=%d, have %s: %v", old, new, got, err)
	}
}

func rsPods(ctx context.Context, t *testing.T, c clientset.Interface, ns string, rs *apps.ReplicaSet) []v1.Pod {
	t.Helper()
	list, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: metav1.FormatLabelSelector(rs.Spec.Selector)})
	if err != nil {
		t.Fatalf("listing pods: %v", err)
	}
	var out []v1.Pod
	for _, p := range list.Items {
		if p.DeletionTimestamp == nil && metav1.IsControlledBy(&p, rs) {
			out = append(out, p)
		}
	}
	return out
}

func markOneReady(ctx context.Context, t *testing.T, c clientset.Interface, ns string, rs *apps.ReplicaSet) {
	t.Helper()
	ready := 0
	for _, p := range rsPods(ctx, t, c, ns, rs) {
		if podutil.IsPodReady(&p) {
			ready++
		}
	}
	markReadyUpTo(ctx, t, c, ns, rs, ready+1)
}

// markReadyUpTo makes pods of rs ready until want of them are, waiting for the
// ReplicaSet controller to have created them.
func markReadyUpTo(ctx context.Context, t *testing.T, c clientset.Interface, ns string, rs *apps.ReplicaSet, want int) {
	t.Helper()
	err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true, func(ctx context.Context) (bool, error) {
		pods := rsPods(ctx, t, c, ns, rs)
		ready := 0
		for i := range pods {
			if podutil.IsPodReady(&pods[i]) {
				ready++
			}
		}
		for i := range pods {
			if ready >= want {
				return true, nil
			}
			if podutil.IsPodReady(&pods[i]) {
				continue
			}
			if err := markPodReady(c, ns, &pods[i]); err != nil {
				return false, nil
			}
			ready++
		}
		return ready >= want, nil
	})
	if err != nil {
		t.Fatalf("marking %d pods of %s ready: %v", want, rs.Name, err)
	}
}

// realAvailablePods counts pods that are ready and not being deleted -
// ReplicaSet status is what is under test, so it is not consulted.
func realAvailablePods(ctx context.Context, t *testing.T, c clientset.Interface, ns, name string) int32 {
	t.Helper()
	list, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "name=" + name})
	if err != nil {
		t.Fatalf("listing pods: %v", err)
	}
	n := int32(0)
	for i := range list.Items {
		p := &list.Items[i]
		if p.DeletionTimestamp == nil && podutil.IsPodAvailable(p, 0, metav1.Now()) {
			n++
		}
	}
	return n
}
