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

package persistentvolume

// Binds the region vectors of the IML model of one controller step
// (.grounds/conformance/*.vectors.json) to the real syncClaim, syncVolume and
// findBestMatchForClaim. Each vector is loaded into the fake API server and the
// controller cache, the step runs once, and the resulting objects, events,
// error and scheduled operation are projected back to the model's terms.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2/ktesting"
)

const (
	groundsDriver    = "example.com/csi"
	groundsVectorDir = "../../../../.grounds/conformance"
)

// ---- model values, as the vectors files encode them ----

type gVar struct {
	C    string            `json:"$c"`
	Args []json.RawMessage `json:"args"`
}

type gRef struct {
	Name int64 `json:"cr_name"`
	UID  int64 `json:"cr_uid"`
}

type gPVJSON struct {
	Name     int64 `json:"v_name"`
	Ref      gVar  `json:"v_ref"`
	Phase    gVar  `json:"v_phase"`
	BBC      bool  `json:"v_bbc"`
	Dyn      bool  `json:"v_dyn"`
	Policy   gVar  `json:"v_policy"`
	Deleting bool  `json:"v_deleting"`
	Cap      int64 `json:"v_cap"`
	Class    int64 `json:"v_class"`
	Block    bool  `json:"v_block"`
	RWX      bool  `json:"v_rwx"`
	Labeled  bool  `json:"v_labeled"`
}

type gPVCJSON struct {
	Name     int64 `json:"c_name"`
	UID      int64 `json:"c_uid"`
	Vol      int64 `json:"c_vol"`
	Phase    gVar  `json:"c_phase"`
	BC       bool  `json:"c_bc"`
	BBC      bool  `json:"c_bbc"`
	Req      int64 `json:"c_req"`
	Class    int64 `json:"c_class"`
	Block    bool  `json:"c_block"`
	RWX      bool  `json:"c_rwx"`
	Selector bool  `json:"c_selector"`
	SelNode  bool  `json:"c_selnode"`
}

// gPV / gPVC are the comparable forms; Ref is nil for "no claimRef".
type gPV struct {
	Name     int64
	Ref      *gRef
	Phase    string
	BBC      bool
	Dyn      bool
	Policy   string
	Deleting bool
	Cap      int64
	Class    int64
	Block    bool
	RWX      bool
	Labeled  bool
}

type gPVC struct {
	Name     int64
	UID      int64
	Vol      int64
	Phase    string
	BC       bool
	BBC      bool
	Req      int64
	Class    int64
	Block    bool
	RWX      bool
	Selector bool
	SelNode  bool
}

type gWorld struct {
	V1, V2 *gPV
	C1, C2 *gPVC
}

type gResult struct {
	W      gWorld
	Events []string
	Err    bool
	Op     string
	Enq    bool
}

type gVector struct {
	ID       string                     `json:"id"`
	Input    map[string]json.RawMessage `json:"input"`
	Expected json.RawMessage            `json:"expected"`
}

// loadGroundsVectors reads the region vectors (<name>.vectors.json, kept as
// generated) and the vectors at hand-picked counterexample inputs
// (<name>.chosen.json, expected values still computed by the model).
func loadGroundsVectors(t *testing.T, name string) []gVector {
	t.Helper()
	var all []gVector
	for _, file := range []string{name + ".vectors.json", name + ".chosen.json"} {
		data, err := os.ReadFile(filepath.Join(groundsVectorDir, file))
		if err != nil {
			t.Fatalf("reading vectors %s: %v", file, err)
		}
		var f struct {
			Vectors []gVector `json:"vectors"`
		}
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("parsing vectors %s: %v", file, err)
		}
		if len(f.Vectors) == 0 {
			t.Fatalf("no vectors in %s", file)
		}
		all = append(all, f.Vectors...)
	}
	return all
}

func mustJSON(t *testing.T, raw json.RawMessage, into interface{}) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decoding %s: %v", string(raw), err)
	}
}

// decodeOption returns the payload of {"$c":"Some","args":[x]} or nil for None.
func decodeOption(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()
	var o gVar
	mustJSON(t, raw, &o)
	switch o.C {
	case "None":
		return nil
	case "Some":
		return o.Args[0]
	}
	t.Fatalf("not an option: %s", string(raw))
	return nil
}

func decodePV(t *testing.T, raw json.RawMessage) *gPV {
	var j gPVJSON
	mustJSON(t, raw, &j)
	pv := &gPV{Name: j.Name, Phase: j.Phase.C, BBC: j.BBC, Dyn: j.Dyn, Policy: j.Policy.C,
		Deleting: j.Deleting, Cap: j.Cap, Class: j.Class, Block: j.Block, RWX: j.RWX, Labeled: j.Labeled}
	if j.Ref.C == "Some" {
		var r gRef
		mustJSON(t, j.Ref.Args[0], &r)
		pv.Ref = &r
	}
	return pv
}

func decodePVC(t *testing.T, raw json.RawMessage) *gPVC {
	var j gPVCJSON
	mustJSON(t, raw, &j)
	return &gPVC{Name: j.Name, UID: j.UID, Vol: j.Vol, Phase: j.Phase.C, BC: j.BC, BBC: j.BBC,
		Req: j.Req, Class: j.Class, Block: j.Block, RWX: j.RWX, Selector: j.Selector, SelNode: j.SelNode}
}

func decodeOptPV(t *testing.T, raw json.RawMessage) *gPV {
	if p := decodeOption(t, raw); p != nil {
		return decodePV(t, p)
	}
	return nil
}

func decodeOptPVC(t *testing.T, raw json.RawMessage) *gPVC {
	if p := decodeOption(t, raw); p != nil {
		return decodePVC(t, p)
	}
	return nil
}

func decodeResult(t *testing.T, raw json.RawMessage) gResult {
	var j struct {
		W struct {
			V1, V2, C1, C2 json.RawMessage
		} `json:"w"`
		Events []gVar `json:"events"`
		Err    bool   `json:"err"`
		Op     gVar   `json:"op"`
		Enq    bool   `json:"enq"`
	}
	mustJSON(t, raw, &j)
	r := gResult{
		W: gWorld{V1: decodeOptPV(t, j.W.V1), V2: decodeOptPV(t, j.W.V2),
			C1: decodeOptPVC(t, j.W.C1), C2: decodeOptPVC(t, j.W.C2)},
		Events: []string{}, Err: j.Err, Op: j.Op.C, Enq: j.Enq,
	}
	for _, e := range j.Events {
		r.Events = append(r.Events, e.C)
	}
	return r
}

// ---- model value -> API object ----

func gNum(prefix string, n int64) string { return fmt.Sprintf("%s-%d", prefix, n) }

func gUID(n int64) types.UID {
	if n == 0 {
		return ""
	}
	return types.UID(gNum("uid", n))
}

func gClassName(n int64) string {
	if n == 0 {
		return ""
	}
	return gNum("class", n)
}

func gParse(prefix, s string) int64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimPrefix(s, prefix+"-"), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("unexpected name %q for prefix %q", s, prefix))
	}
	return n
}

var gVolPhases = map[string]v1.PersistentVolumePhase{
	"VPending": v1.VolumePending, "VAvailable": v1.VolumeAvailable, "VBound": v1.VolumeBound,
	"VReleased": v1.VolumeReleased, "VFailed": v1.VolumeFailed,
}

var gClaimPhases = map[string]v1.PersistentVolumeClaimPhase{
	"CPending": v1.ClaimPending, "CBound": v1.ClaimBound, "CLost": v1.ClaimLost,
}

var gPolicies = map[string]v1.PersistentVolumeReclaimPolicy{
	"Retain": v1.PersistentVolumeReclaimRetain, "Delete": v1.PersistentVolumeReclaimDelete,
	"Recycle": v1.PersistentVolumeReclaimRecycle,
}

func gModes(rwx bool) []v1.PersistentVolumeAccessMode {
	if rwx {
		return []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce, v1.ReadWriteMany}
	}
	return []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
}

func gMode(block bool) *v1.PersistentVolumeMode {
	m := v1.PersistentVolumeFilesystem
	if block {
		m = v1.PersistentVolumeBlock
	}
	return &m
}

func toPV(m *gPV) *v1.PersistentVolume {
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: gNum("pv", m.Name), ResourceVersion: "1"},
		Spec: v1.PersistentVolumeSpec{
			Capacity: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(m.Cap, resource.BinarySI)},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{Driver: groundsDriver, VolumeHandle: gNum("handle", m.Name)},
			},
			AccessModes:                   gModes(m.RWX),
			PersistentVolumeReclaimPolicy: gPolicies[m.Policy],
			StorageClassName:              gClassName(m.Class),
			VolumeMode:                    gMode(m.Block),
		},
		Status: v1.PersistentVolumeStatus{Phase: gVolPhases[m.Phase]},
	}
	if m.Ref != nil {
		pv.Spec.ClaimRef = &v1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1",
			Namespace: testNamespace, Name: gNum("pvc", m.Ref.Name), UID: gUID(m.Ref.UID)}
	}
	ann := map[string]string{}
	if m.BBC {
		ann[storagehelpers.AnnBoundByController] = "yes"
	}
	if m.Dyn {
		ann[storagehelpers.AnnDynamicallyProvisioned] = groundsDriver
	}
	if len(ann) > 0 {
		pv.Annotations = ann
	}
	if m.Deleting {
		ts := metav1.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
		pv.DeletionTimestamp = &ts
	}
	if m.Labeled {
		pv.Labels = map[string]string{"grounds": "yes"}
	}
	return pv
}

func toPVC(m *gPVC) *v1.PersistentVolumeClaim {
	class := gClassName(m.Class)
	c := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: gNum("pvc", m.Name), Namespace: testNamespace,
			UID: gUID(m.UID), ResourceVersion: "1"},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: gModes(m.RWX),
			Resources: v1.VolumeResourceRequirements{
				Requests: v1.ResourceList{v1.ResourceStorage: *resource.NewQuantity(m.Req, resource.BinarySI)},
			},
			StorageClassName: &class,
			VolumeMode:       gMode(m.Block),
		},
		Status: v1.PersistentVolumeClaimStatus{Phase: gClaimPhases[m.Phase]},
	}
	if m.Vol != 0 {
		c.Spec.VolumeName = gNum("pv", m.Vol)
	}
	ann := map[string]string{}
	if m.BC {
		ann[storagehelpers.AnnBindCompleted] = "yes"
	}
	if m.BBC {
		ann[storagehelpers.AnnBoundByController] = "yes"
	}
	if m.SelNode {
		ann[storagehelpers.AnnSelectedNode] = "node-1"
	}
	if len(ann) > 0 {
		c.Annotations = ann
	}
	if m.Selector {
		c.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"grounds": "yes"}}
	}
	return c
}

// ---- API object -> model value ----

func fromPV(pv *v1.PersistentVolume) *gPV {
	if pv == nil {
		return nil
	}
	m := &gPV{Name: gParse("pv", pv.Name), BBC: metav1.HasAnnotation(pv.ObjectMeta, storagehelpers.AnnBoundByController),
		Dyn:      metav1.HasAnnotation(pv.ObjectMeta, storagehelpers.AnnDynamicallyProvisioned),
		Deleting: pv.DeletionTimestamp != nil, Class: gParse("class", pv.Spec.StorageClassName),
		Block:   pv.Spec.VolumeMode != nil && *pv.Spec.VolumeMode == v1.PersistentVolumeBlock,
		Labeled: pv.Labels["grounds"] == "yes"}
	q := pv.Spec.Capacity[v1.ResourceStorage]
	m.Cap = q.Value()
	for k, v := range gVolPhases {
		if v == pv.Status.Phase {
			m.Phase = k
		}
	}
	for k, v := range gPolicies {
		if v == pv.Spec.PersistentVolumeReclaimPolicy {
			m.Policy = k
		}
	}
	for _, am := range pv.Spec.AccessModes {
		if am == v1.ReadWriteMany {
			m.RWX = true
		}
	}
	if r := pv.Spec.ClaimRef; r != nil {
		uid := int64(0)
		if r.UID != "" {
			uid = gParse("uid", string(r.UID))
		}
		m.Ref = &gRef{Name: gParse("pvc", r.Name), UID: uid}
	}
	return m
}

func fromPVC(c *v1.PersistentVolumeClaim) *gPVC {
	if c == nil {
		return nil
	}
	m := &gPVC{Name: gParse("pvc", c.Name), UID: gParse("uid", string(c.UID)), Vol: gParse("pv", c.Spec.VolumeName),
		BC:       metav1.HasAnnotation(c.ObjectMeta, storagehelpers.AnnBindCompleted),
		BBC:      metav1.HasAnnotation(c.ObjectMeta, storagehelpers.AnnBoundByController),
		SelNode:  metav1.HasAnnotation(c.ObjectMeta, storagehelpers.AnnSelectedNode),
		Block:    c.Spec.VolumeMode != nil && *c.Spec.VolumeMode == v1.PersistentVolumeBlock,
		Selector: c.Spec.Selector != nil}
	q := c.Spec.Resources.Requests[v1.ResourceStorage]
	m.Req = q.Value()
	if c.Spec.StorageClassName != nil {
		m.Class = gParse("class", *c.Spec.StorageClassName)
	}
	for k, v := range gClaimPhases {
		if v == c.Status.Phase {
			m.Phase = k
		}
	}
	for _, am := range c.Spec.AccessModes {
		if am == v1.ReadWriteMany {
			m.RWX = true
		}
	}
	return m
}

var gEventNames = map[string]string{
	"WaitForFirstConsumer": "EvWaitForFirstConsumer", "FailedBinding": "EvFailedBinding",
	"VolumeMismatch": "EvVolumeMismatch", "ClaimLost": "EvClaimLost", "ClaimMisbound": "EvClaimMisbound",
}

var gOpNames = map[string]string{"provision": "OpProvision", "recycle": "OpRecycle", "delete": "OpDelete"}

// ---- harness ----

type groundsHarness struct {
	ctx    context.Context
	client *fake.Clientset
	ctrl   *PersistentVolumeController
}

func newGroundsHarness(t *testing.T, w gWorld) *groundsHarness {
	_, ctx := ktesting.NewTestContext(t)
	client := &fake.Clientset{}
	ctrl, err := newTestController(ctx, client, nil, true)
	if err != nil {
		t.Fatalf("constructing controller: %v", err)
	}
	reactor := newVolumeReactor(ctx, client, ctrl, nil, nil, nil)
	for _, m := range []*gPV{w.V1, w.V2} {
		if m == nil {
			continue
		}
		pv := toPV(m)
		if err := ctrl.volumes.Indexer().Add(pv); err != nil {
			t.Fatal(err)
		}
		reactor.AddVolume(pv)
	}
	for _, m := range []*gPVC{w.C1, w.C2} {
		if m == nil {
			continue
		}
		c := toPVC(m)
		if err := ctrl.claims.Indexer().Add(c); err != nil {
			t.Fatal(err)
		}
		reactor.AddClaim(c)
	}
	immediate, wffc := storage.VolumeBindingImmediate, storage.VolumeBindingWaitForFirstConsumer
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, sc := range []*storage.StorageClass{
		{ObjectMeta: metav1.ObjectMeta{Name: gClassName(1)}, Provisioner: groundsDriver, VolumeBindingMode: &immediate},
		{ObjectMeta: metav1.ObjectMeta{Name: gClassName(2)}, Provisioner: groundsDriver, VolumeBindingMode: &wffc},
	} {
		if err := indexer.Add(sc); err != nil {
			t.Fatal(err)
		}
	}
	ctrl.classLister = storagelisters.NewStorageClassLister(indexer)
	return &groundsHarness{ctx: ctx, client: client, ctrl: ctrl}
}

func (h *groundsHarness) getPV(t *testing.T, n int64) *v1.PersistentVolume {
	pv, err := h.client.CoreV1().PersistentVolumes().Get(h.ctx, gNum("pv", n), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		t.Fatalf("get pv: %v", err)
	}
	return pv
}

func (h *groundsHarness) getPVC(t *testing.T, n int64) *v1.PersistentVolumeClaim {
	c, err := h.client.CoreV1().PersistentVolumeClaims(testNamespace).Get(h.ctx, gNum("pvc", n), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	return c
}

// observe reads the API objects and drains the events emitted so far.
func (h *groundsHarness) observe(t *testing.T) gResult {
	r := gResult{W: gWorld{V1: fromPV(h.getPV(t, 1)), V2: fromPV(h.getPV(t, 2)),
		C1: fromPVC(h.getPVC(t, 1)), C2: fromPVC(h.getPVC(t, 2))}, Events: []string{}, Op: "NoOp"}
	events := h.ctrl.eventRecorder.(*record.FakeRecorder).Events
	for {
		select {
		case e := <-events:
			reason := strings.Fields(e)[1]
			name, ok := gEventNames[reason]
			if !ok {
				name = "unmodelled:" + reason
			}
			r.Events = append(r.Events, name)
			continue
		default:
		}
		break
	}
	return r
}

// step runs one sync. An operation (provision/recycle/delete) runs
// asynchronously after the sync decided to schedule it; the state is taken
// when it is scheduled, which is the end of the step the model describes.
func (h *groundsHarness) step(t *testing.T, sync func() error) gResult {
	var atOp *gResult
	h.ctrl.preOperationHook = func(opName string) {
		r := h.observe(t)
		r.Op = gOpNames[strings.SplitN(opName, "-", 2)[0]]
		atOp = &r
	}
	err := sync()
	h.ctrl.runningOperations.WaitForCompletion()
	var r gResult
	if atOp != nil {
		r = *atOp
	} else {
		r = h.observe(t)
	}
	r.Err = err != nil
	r.Enq = h.ctrl.claimQueue.Len() > 0
	return r
}

func compareGrounds(t *testing.T, want, got gResult) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		wj, _ := json.MarshalIndent(want, "", " ")
		gj, _ := json.MarshalIndent(got, "", " ")
		t.Errorf("model and code disagree\nmodel: %s\ncode:  %s", wj, gj)
	}
}

func TestGroundsConformanceSyncClaim(t *testing.T) {
	for _, vec := range loadGroundsVectors(t, "sync_claim_11") {
		t.Run(vec.ID, func(t *testing.T) {
			c := decodePVC(t, vec.Input["c"])
			w := gWorld{V1: decodeOptPV(t, vec.Input["v"]), C1: c}
			h := newGroundsHarness(t, w)
			got := h.step(t, func() error { return h.ctrl.syncClaim(h.ctx, toPVC(c)) })
			compareGrounds(t, decodeResult(t, vec.Expected), got)
		})
	}
}

func TestGroundsConformanceSyncVolume(t *testing.T) {
	for _, vec := range loadGroundsVectors(t, "sync_volume_11") {
		t.Run(vec.ID, func(t *testing.T) {
			v := decodePV(t, vec.Input["v"])
			w := gWorld{V1: v, C1: decodeOptPVC(t, vec.Input["c"])}
			h := newGroundsHarness(t, w)
			got := h.step(t, func() error { return h.ctrl.syncVolume(h.ctx, toPV(v)) })
			compareGrounds(t, decodeResult(t, vec.Expected), got)
		})
	}
}

// The order of volumes inside one access-mode group comes from a set in the
// informer index, so it is not fixed: the model gives the match for each
// order, and every run of the code must produce one of them.
func TestGroundsConformanceFindBestMatch(t *testing.T) {
	const runs = 30
	for _, vec := range loadGroundsVectors(t, "find_best_both") {
		t.Run(vec.ID, func(t *testing.T) {
			a, b, c := decodePV(t, vec.Input["a"]), decodePV(t, vec.Input["b"]), decodePVC(t, vec.Input["c"])
			var tuple struct {
				T []json.RawMessage `json:"$tuple"`
			}
			mustJSON(t, vec.Expected, &tuple)
			allowed := map[string]bool{}
			for _, o := range tuple.T {
				name := "none"
				if pv := decodeOptPV(t, o); pv != nil {
					name = gNum("pv", pv.Name)
				}
				allowed[name] = true
			}
			seen := map[string]bool{}
			for i := 0; i < runs; i++ {
				h := newGroundsHarness(t, gWorld{V1: a, V2: b, C1: c})
				claim := toPVC(c)
				delay, err := storagehelpers.IsDelayBindingMode(claim, h.ctrl.classLister)
				if err != nil {
					t.Fatalf("delay binding mode: %v", err)
				}
				pv, err := h.ctrl.volumes.findBestMatchForClaim(claim, delay)
				if err != nil {
					t.Fatalf("findBestMatchForClaim: %v", err)
				}
				name := "none"
				if pv != nil {
					name = pv.Name
				}
				seen[name] = true
			}
			for name := range seen {
				if !allowed[name] {
					t.Errorf("code matched %s; model allows %v (seen %v)", name, allowed, seen)
				}
			}
		})
	}
}
