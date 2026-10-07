/*
Copyright The Kubernetes Authors.

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

package cronjob

// Binds the Grounds models of the CronJob controller (.grounds/models) to the
// real code: the IML step model's region vectors, the schedule abstraction
// against robfig/cron, and the edge cases the models found.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
)

// groundsJobControl is a by-name job store: Replace must delete the specific
// active jobs, which the single-Job fakeJobControl cannot express.
type groundsJobControl struct {
	jobs      map[string]*batchv1.Job
	createErr error
	deleteErr error
	created   []string
	deleted   []string
	attempted bool
}

func (g *groundsJobControl) GetJob(namespace, name string) (*batchv1.Job, error) {
	if j, ok := g.jobs[name]; ok {
		return j, nil
	}
	return nil, errors.NewNotFound(schema.GroupResource{Group: "batch", Resource: "jobs"}, name)
}

func (g *groundsJobControl) CreateJob(namespace string, job *batchv1.Job) (*batchv1.Job, error) {
	g.attempted = true
	if g.createErr != nil {
		return nil, g.createErr
	}
	if _, ok := g.jobs[job.Name]; ok {
		return nil, errors.NewAlreadyExists(schema.GroupResource{Group: "batch", Resource: "jobs"}, job.Name)
	}
	j := job.DeepCopy()
	j.Namespace = namespace
	j.UID = types.UID("uid-" + job.Name)
	g.jobs[job.Name] = j
	g.created = append(g.created, job.Name)
	return j, nil
}

func (g *groundsJobControl) DeleteJob(namespace string, name string) error {
	if g.deleteErr != nil {
		return g.deleteErr
	}
	delete(g.jobs, name)
	g.deleted = append(g.deleted, name)
	return nil
}

var groundsCJUID = types.UID("grounds-cj-uid")

func groundsCronJob(schedule string, tz *string) *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{Name: "cj", Namespace: "ns", UID: groundsCJUID},
		Spec: batchv1.CronJobSpec{
			Schedule: schedule,
			TimeZone: tz,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{}},
			},
		},
	}
}

func groundsOwnedJob(cj *batchv1.CronJob, name string, running bool) *batchv1.Job {
	j := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: cj.Namespace, UID: types.UID("uid-" + name),
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cj, controllerKind)},
		},
	}
	if !running {
		j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	}
	return j
}

func groundsRef(j *batchv1.Job) corev1.ObjectReference {
	return corev1.ObjectReference{Kind: "Job", APIVersion: "batch/v1", Name: j.Name, Namespace: j.Namespace, UID: j.UID}
}

type groundsRun struct {
	requeue   *time.Duration
	err       error
	cj        *batchv1.CronJob
	jc        *groundsJobControl
	events    []string
	hasReason func(string) bool
}

func groundsSync(t *testing.T, cj *batchv1.CronJob, jobs []*batchv1.Job, jc *groundsJobControl, now time.Time) groundsRun {
	t.Helper()
	recorder := record.NewFakeRecorder(1000)
	jm := ControllerV2{
		jobControl:     jc,
		cronJobControl: &fakeCJControl{CronJob: cj.DeepCopy()},
		recorder:       recorder,
		now:            func() time.Time { return now },
	}
	cjCopy := cj.DeepCopy()
	rq, _, err := jm.syncCronJob(context.TODO(), cjCopy, jobs)
	close(recorder.Events)
	var evs []string
	for e := range recorder.Events {
		evs = append(evs, e)
	}
	r := groundsRun{requeue: rq, err: err, cj: cjCopy, jc: jc, events: evs}
	r.hasReason = func(reason string) bool {
		for _, e := range evs {
			if strings.Contains(e, " "+reason+" ") {
				return true
			}
		}
		return false
	}
	return r
}

func unixSec(s int64) time.Time { return time.Unix(s, 0).UTC() }

// ---------------------------------------------------------------------------
// Vectors from the IML model (.grounds/models/cronjob_step.iml, step_minutely).

type groundsVectorsFile struct {
	Vectors []struct {
		ID       string          `json:"id"`
		Input    json.RawMessage `json:"input"`
		Expected json.RawMessage `json:"expected"`
	} `json:"vectors"`
}

// IML values as exported by Grounds: options and variants are {"$c": Name, "args": [...]}.
type imlC struct {
	C    string            `json:"$c"`
	Args []json.RawMessage `json:"args"`
}

func imlOptInt(t *testing.T, raw json.RawMessage) *int64 {
	t.Helper()
	var c imlC
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("option: %v in %s", err, raw)
	}
	if c.C == "None" {
		return nil
	}
	var v int64
	if err := json.Unmarshal(c.Args[0], &v); err != nil {
		t.Fatalf("option arg: %v in %s", err, raw)
	}
	return &v
}

func imlCtor(t *testing.T, raw json.RawMessage) imlC {
	t.Helper()
	var c imlC
	if err := json.Unmarshal(raw, &c); err != nil {
		var s string
		if err2 := json.Unmarshal(raw, &s); err2 == nil {
			return imlC{C: s}
		}
		t.Fatalf("variant: %v in %s", err, raw)
	}
	return c
}

type stepInput struct {
	Now          int64           `json:"now"`
	Creation     int64           `json:"creation"`
	Lst          json.RawMessage `json:"lst"`
	Deadline     json.RawMessage `json:"deadline"`
	Suspend      bool            `json:"suspend"`
	Policy       json.RawMessage `json:"policy"`
	Deleting     bool            `json:"deleting"`
	TzValid      bool            `json:"tz_valid"`
	Active       int64           `json:"active"`
	NameInActive bool            `json:"name_in_active"`
	DeleteOk     bool            `json:"delete_ok"`
	Create       json.RawMessage `json:"create"`
}

type stepOutput struct {
	Outcome   json.RawMessage `json:"outcome"`
	Deleted   int64           `json:"deleted"`
	NewLst    json.RawMessage `json:"new_lst"`
	NewActive int64           `json:"new_active"`
	Requeue   json.RawMessage `json:"requeue"`
	Many      bool            `json:"many"`
}

// floorMinute is the model's prev_fire for "* * * * *": the scheduled time a
// sync at now would pick, used only to name the jobs a vector says collide.
func floorMinute(s int64) int64 {
	m := s % 60
	if m < 0 {
		m += 60
	}
	return s - m
}

type stepObserved struct {
	outcome   string
	deleted   int64
	newLst    *int64
	newActive int64
	requeue   *int64
	many      bool
}

func runStepVector(t *testing.T, in stepInput) stepObserved {
	t.Helper()
	cj := groundsCronJob("* * * * *", nil)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(in.Creation)}
	if l := imlOptInt(t, in.Lst); l != nil {
		cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(*l)}
	}
	if d := imlOptInt(t, in.Deadline); d != nil {
		cj.Spec.StartingDeadlineSeconds = d
	}
	cj.Spec.Suspend = ptr.To(in.Suspend)
	switch imlCtor(t, in.Policy).C {
	case "Allow":
		cj.Spec.ConcurrencyPolicy = batchv1.AllowConcurrent
	case "Forbid":
		cj.Spec.ConcurrencyPolicy = batchv1.ForbidConcurrent
	case "Replace":
		cj.Spec.ConcurrencyPolicy = batchv1.ReplaceConcurrent
	}
	if in.Deleting {
		cj.DeletionTimestamp = &metav1.Time{Time: unixSec(in.Now)}
	}
	if !in.TzValid {
		cj.Spec.TimeZone = ptr.To("Not/AZone")
	}
	jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
	var jobs []*batchv1.Job
	for k := int64(0); k < in.Active; k++ {
		name := fmt.Sprintf("cj-active-%d", k)
		if k == 0 && in.NameInActive {
			name = getJobName(cj, unixSec(floorMinute(in.Now)))
		}
		j := groundsOwnedJob(cj, name, true)
		jc.jobs[name] = j
		jobs = append(jobs, j)
		cj.Status.Active = append(cj.Status.Active, groundsRef(j))
	}
	collide := getJobName(cj, unixSec(floorMinute(in.Now)))
	switch imlCtor(t, in.Create).C {
	case "CreateErr":
		jc.createErr = errors.NewInternalError(fmt.Errorf("grounds: create fails"))
	case "ExistsForeign":
		if _, ok := jc.jobs[collide]; !ok {
			jc.jobs[collide] = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: collide, Namespace: "ns", UID: "foreign"}}
		}
	case "ExistsOwned":
		if _, ok := jc.jobs[collide]; !ok {
			jc.jobs[collide] = groundsOwnedJob(cj, collide, false)
		}
	}
	if !in.DeleteOk {
		jc.deleteErr = errors.NewInternalError(fmt.Errorf("grounds: delete fails"))
	}
	nowT := unixSec(in.Now)
	r := groundsSync(t, cj, jobs, jc, nowT)

	obs := stepObserved{deleted: int64(len(jc.deleted)), newActive: int64(len(r.cj.Status.Active)), many: r.hasReason("TooManyMissedTimes")}
	if r.cj.Status.LastScheduleTime != nil {
		v := r.cj.Status.LastScheduleTime.Unix()
		obs.newLst = &v
	}
	if r.requeue != nil {
		v := nowT.Add(*r.requeue).Add(-nextScheduleDelta)
		if v.Nanosecond() != 0 {
			t.Fatalf("requeue %v is not a whole second", v)
		}
		s := v.Unix()
		obs.requeue = &s
	}
	switch {
	case r.hasReason("SuccessfulCreate"):
		obs.outcome = "Created"
	case r.err != nil && r.hasReason("FailedDelete"):
		obs.outcome = "ReplaceFailed"
	case r.err != nil && r.hasReason("FailedCreate"):
		obs.outcome = "CreateFailed"
	case r.err != nil:
		obs.outcome = "Error:" + r.err.Error()
	case r.hasReason("UnknownTimeZone"):
		obs.outcome = "BadTZ"
	case r.hasReason("MissSchedule"):
		obs.outcome = "TooLate"
	case r.hasReason("InvalidSchedule"):
		obs.outcome = "InvalidSchedule"
	case r.hasReason("JobAlreadyActive"):
		obs.outcome = "ForbidSkip"
	case jc.attempted && obs.newLst != nil && (cj.Status.LastScheduleTime == nil || *obs.newLst != cj.Status.LastScheduleTime.Unix()):
		obs.outcome = "Adopted"
	case jc.attempted && r.requeue == nil:
		obs.outcome = "ExistsForeignStop"
	case r.requeue == nil && cj.DeletionTimestamp != nil:
		obs.outcome = "Deleting"
	case r.requeue == nil:
		obs.outcome = "Suspended"
	default:
		// NoUnmet and AlreadyProcessed emit nothing and differ only in why; observed alike.
		obs.outcome = "Requeued"
	}
	return obs
}

func modelStepObserved(t *testing.T, out stepOutput) stepObserved {
	t.Helper()
	o := stepObserved{deleted: out.Deleted, newLst: imlOptInt(t, out.NewLst), newActive: out.NewActive, requeue: imlOptInt(t, out.Requeue), many: out.Many}
	o.outcome = imlCtor(t, out.Outcome).C
	if o.outcome == "NoUnmet" || o.outcome == "AlreadyProcessed" {
		o.outcome = "Requeued"
	}
	return o
}

func fmtOpt(p *int64) string {
	if p == nil {
		return "None"
	}
	return fmt.Sprint(*p)
}

func (o stepObserved) String() string {
	return fmt.Sprintf("{%s deleted=%d lst=%s active=%d requeue=%s many=%t}", o.outcome, o.deleted, fmtOpt(o.newLst), o.newActive, fmtOpt(o.requeue), o.many)
}

func groundsVectors(t *testing.T, file string) groundsVectorsFile {
	t.Helper()
	path := filepath.Join("..", "..", "..", ".grounds", "conformance", file)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var vf groundsVectorsFile
	if err := json.Unmarshal(b, &vf); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	if len(vf.Vectors) == 0 {
		t.Fatalf("%s has no vectors", path)
	}
	return vf
}

func TestGroundsSyncStepVectors(t *testing.T) {
	vf := groundsVectors(t, "cronjob_sync_step.vectors.json")
	for _, v := range vf.Vectors {
		t.Run(v.ID, func(t *testing.T) {
			var args map[string]json.RawMessage
			if err := json.Unmarshal(v.Input, &args); err != nil {
				t.Fatalf("input: %v", err)
			}
			var in stepInput
			if err := json.Unmarshal(args["i"], &in); err != nil {
				t.Fatalf("input i: %v in %s", err, v.Input)
			}
			var out stepOutput
			if err := json.Unmarshal(v.Expected, &out); err != nil {
				t.Fatalf("expected: %v", err)
			}
			want := modelStepObserved(t, out)
			got := runStepVector(t, in)
			if got.String() != want.String() {
				t.Errorf("input %s\n model: %v\n code:  %v", v.Input, want, got)
			}
		})
	}
}

// mr_minutely(base, now, deadline) = mostRecentScheduleTime(cj, now, "* * * * *", true)
// with base the lastScheduleTime.
func TestGroundsMostRecentVectors(t *testing.T) {
	s, err := cron.ParseStandard("* * * * *")
	if err != nil {
		t.Fatal(err)
	}
	missedName := map[missedSchedulesType]string{noneMissed: "NoneMissed", fewMissed: "FewMissed", manyMissed: "ManyMissed"}
	vf := groundsVectors(t, "most_recent_minutely.vectors.json")
	for _, v := range vf.Vectors {
		t.Run(v.ID, func(t *testing.T) {
			var in struct {
				Base     int64           `json:"base"`
				Now      int64           `json:"now"`
				Deadline json.RawMessage `json:"deadline"`
			}
			if err := json.Unmarshal(v.Input, &in); err != nil {
				t.Fatal(err)
			}
			var want struct {
				Earliest int64           `json:"earliest"`
				Recent   json.RawMessage `json:"recent"`
				Missed   json.RawMessage `json:"missed"`
				Err      bool            `json:"err"`
			}
			if err := json.Unmarshal(v.Expected, &want); err != nil {
				t.Fatal(err)
			}
			cj := groundsCronJob("* * * * *", nil)
			cj.CreationTimestamp = metav1.Time{Time: unixSec(in.Base - 3600)}
			cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(in.Base)}
			cj.Spec.StartingDeadlineSeconds = imlOptInt(t, in.Deadline)
			earliest, recent, missed, err := mostRecentScheduleTime(cj, unixSec(in.Now), s, true)
			got := fmt.Sprintf("earliest=%d recent=%s missed=%s err=%t", earliest.Unix(), fmtOpt(timeOpt(recent)), missedName[missed], err != nil)
			exp := fmt.Sprintf("earliest=%d recent=%s missed=%s err=%t", want.Earliest, fmtOpt(imlOptInt(t, want.Recent)), imlCtor(t, want.Missed).C, want.Err)
			if got != exp {
				t.Errorf("input %s\n model: %s\n code:  %s", v.Input, exp, got)
			}
		})
	}
}

func timeOpt(t *time.Time) *int64 {
	if t == nil {
		return nil
	}
	if t.Nanosecond() != 0 {
		panic(fmt.Sprintf("schedule time %v is not a whole second", t))
	}
	v := t.Unix()
	return &v
}

// ---------------------------------------------------------------------------
// The schedule abstraction against robfig/cron.

// bruteMostRecent is ground truth from robfig itself: walk Next from earliest.
func bruteMostRecent(s cron.Schedule, earliest, now time.Time) *time.Time {
	var last *time.Time
	for x := s.Next(earliest); !x.After(now); x = s.Next(x) {
		x := x
		last = &x
	}
	return last
}

// The model's next_fire/prev_fire for Every(60,0) and Pair(86400,0,3600) equal
// robfig's Next on "* * * * *" and "0 0,1 * * *" (UTC).
func TestGroundsScheduleAbstraction(t *testing.T) {
	cases := []struct {
		spec   string
		period int64
		offset int64 // 0: Every
	}{
		{"* * * * *", 60, 0},
		{"0 0,1 * * *", 86400, 3600},
	}
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Unix()
	for _, c := range cases {
		s, err := cron.ParseStandard(c.spec)
		if err != nil {
			t.Fatal(err)
		}
		next := func(x int64) int64 {
			b := x - ((x%c.period)+c.period)%c.period
			if c.offset == 0 {
				return b + c.period
			}
			if x-b < c.offset {
				return b + c.offset
			}
			return b + c.period
		}
		for x := start - 3*c.period; x < start+3*c.period; x += c.period / 60 {
			for _, d := range []int64{-1, 0, 1} {
				y := x + d
				if got, want := s.Next(unixSec(y)).Unix(), next(y); got != want {
					t.Errorf("%s: Next(%d) robfig=%d model=%d", c.spec, y, got, want)
				}
			}
		}
	}
}

// mostRecentScheduleTime against robfig ground truth: on uniform schedules it
// agrees; on non-uniform ones (weekdays, two fires a day, DST in a timeZone,
// month lengths) it returns nil where a schedule was missed.
func TestGroundsMostRecentVsRobfig(t *testing.T) {
	const agree, drops, report = "agree", "drops", "report"
	specs := []struct {
		spec   string
		expect string
		days   int // window after lastScheduleTime; short for dense specs, the brute walk is O(fires)
	}{
		{"* * * * *", agree, 3},
		{"0 * * * *", agree, 10},
		{"0 9 * * *", agree, 40},
		{"0 9 * * 1-5", drops, 40},
		{"0 0,1 * * *", drops, 40},
		{"TZ=America/New_York 0 9 * * *", drops, 40}, // 23h/25h days around the change
		// a drop needs three estimated misses, beyond this window
		{"0 0 1 * *", report, 40},
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, sp := range specs {
		s, err := cron.ParseStandard(sp.spec)
		if err != nil {
			t.Fatal(err)
		}
		mismatches, total := 0, 0
		var first string
		// lastScheduleTime on a fire; now at whole days after it and 5h past them
		lst := base
		for i := 0; i < 400; i++ {
			lst = s.Next(lst)
			if i%13 != 0 {
				continue
			}
			for k := 0; k < 2*sp.days; k++ {
				h := 24*(k/2+1) + 5*(k%2)
				now := lst.Add(time.Duration(h) * time.Hour)
				cj := groundsCronJob(sp.spec, nil)
				cj.CreationTimestamp = metav1.Time{Time: lst.Add(-time.Hour)}
				cj.Status.LastScheduleTime = &metav1.Time{Time: lst}
				_, got, _, err := mostRecentScheduleTime(cj, now, s, false)
				if err != nil {
					t.Fatalf("%s: %v", sp.spec, err)
				}
				want := bruteMostRecent(s, lst, now)
				total++
				if (got == nil) != (want == nil) || (got != nil && !got.Equal(*want)) {
					mismatches++
					if first == "" {
						first = fmt.Sprintf("lst=%v now=%v code=%v robfig=%v", lst, now, got, want)
					}
				}
			}
		}
		t.Logf("%-32s %4d/%d mismatches; first: %s", sp.spec, mismatches, total, first)
		if sp.expect == agree && mismatches > 0 {
			t.Errorf("%s: uniform schedule should agree with robfig", sp.spec)
		}
		if sp.expect == drops && mismatches == 0 {
			t.Errorf("%s: expected the missed-count estimate to drop a missed schedule somewhere", sp.spec)
		}
	}
}

// ---------------------------------------------------------------------------
// Edge cases found by the models, pinned on the real code.

// Weekday schedule, controller down Thu 08:59 .. Mon 08:00: Thu and Fri were
// missed, Allow, no deadline - yet nothing is started on recovery.
func TestGroundsWeekdayCatchUpDropped(t *testing.T) {
	cj := groundsCronJob("0 9 * * 1-5", nil)
	lst := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC) // Wed
	cj.CreationTimestamp = metav1.Time{Time: lst.Add(-time.Hour)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: lst}
	now := time.Date(2026, 10, 12, 8, 0, 0, 0, time.UTC) // Mon
	r := groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, now)
	if len(r.jc.created) != 0 {
		t.Fatalf("expected the dropped catch-up (no job), got %v", r.jc.created)
	}
	s, _ := cron.ParseStandard("0 9 * * 1-5")
	if want := bruteMostRecent(s, lst, now); want == nil || !want.Equal(time.Date(2026, 10, 9, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("robfig ground truth: Fri 09:00 was missed, got %v", want)
	}
	if r.requeue == nil || *r.requeue != time.Hour+nextScheduleDelta {
		t.Fatalf("requeue %v, want next fire Mon 09:00", r.requeue)
	}
}

// Daily 09:00 America/New_York, last run Tue Oct 27, controller back Sun Nov 1
// 08:00 EST (the day DST ends): Sat Oct 31 09:00 EDT was missed and is not
// started, because the day after it is 25h long.
func TestGroundsDSTCatchUpDropped(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skip("no tzdata")
	}
	cj := groundsCronJob("0 9 * * *", ptr.To("America/New_York"))
	lst := time.Date(2026, 10, 27, 13, 0, 0, 0, time.UTC)
	cj.CreationTimestamp = metav1.Time{Time: lst.Add(-time.Hour)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: lst}
	now := time.Date(2026, 11, 1, 13, 0, 0, 0, time.UTC)
	r := groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, now)
	if len(r.jc.created) != 0 {
		t.Fatalf("expected the dropped catch-up, got %v", r.jc.created)
	}
	s, _ := cron.ParseStandard("TZ=America/New_York 0 9 * * *")
	if want := bruteMostRecent(s, lst, now); want == nil || !want.Equal(time.Date(2026, 10, 31, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("robfig ground truth: Oct 31 09:00 EDT was missed, got %v", want)
	}
}

// The model's counterexample for "the cut-off never drops the most recent
// schedule": Pair(86400,0,3600), lst = 3600, now = 446400 (101 estimated misses).
func TestGroundsManyMissedDropsMostRecent(t *testing.T) {
	s, _ := cron.ParseStandard("0 0,1 * * *")
	cj := groundsCronJob("0 0,1 * * *", nil)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(3600)}
	_, mr, missed, err := mostRecentScheduleTime(cj, unixSec(446400), s, true)
	if err != nil || mr != nil || missed != manyMissed {
		t.Fatalf("got mr=%v missed=%v err=%v, want nil/many/nil", mr, missed, err)
	}
	if want := bruteMostRecent(s, unixSec(3600), unixSec(446400)); want == nil || want.Unix() != 435600 {
		t.Fatalf("robfig: most recent should be 435600, got %v", want)
	}
}

// startingDeadlineSeconds: 0 passes validation and never starts a job, silently.
func TestGroundsDeadlineZeroNeverRuns(t *testing.T) {
	cj := groundsCronJob("* * * * *", nil)
	cj.Spec.StartingDeadlineSeconds = ptr.To[int64](0)
	lst := time.Date(2026, 10, 7, 9, 0, 0, 0, time.UTC)
	cj.CreationTimestamp = metav1.Time{Time: lst.Add(-time.Hour)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: lst}
	for _, now := range []time.Time{lst.Add(time.Minute), lst.Add(time.Minute + nextScheduleDelta), lst.Add(time.Hour)} {
		r := groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, now)
		if len(r.jc.created) != 0 || len(r.events) != 0 || r.requeue == nil {
			t.Fatalf("now=%v: created=%v events=%v requeue=%v", now, r.jc.created, r.events, r.requeue)
		}
	}
}

// A schedule missed by exactly startingDeadlineSeconds is not started, though
// the tooLate check (scheduled + deadline < now) would have allowed it.
func TestGroundsDeadlineBoundaryExclusive(t *testing.T) {
	cj := groundsCronJob("* * * * *", nil)
	cj.Spec.StartingDeadlineSeconds = ptr.To[int64](30)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(60)}
	r := groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, unixSec(150))
	if len(r.jc.created) != 0 {
		t.Fatalf("fire at 120 with deadline 30 at now=150: created %v", r.jc.created)
	}
	r = groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, unixSec(149))
	if len(r.jc.created) != 1 {
		t.Fatalf("now=149 should start the 120 fire")
	}
}

// Fall-back day: "30 1 * * *" in America/New_York fires at 01:30 EDT and again
// at 01:30 EST; the controller starts two jobs for one wall-clock time.
func TestGroundsDSTFallBackRunsTwice(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no tzdata")
	}
	cj := groundsCronJob("30 1 * * *", ptr.To("America/New_York"))
	first := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC) // 01:30 EDT
	cj.CreationTimestamp = metav1.Time{Time: first.Add(-24 * time.Hour)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: first}
	now := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC).Add(nextScheduleDelta) // 01:30 EST
	jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
	r := groundsSync(t, cj, nil, jc, now)
	if len(jc.created) != 1 {
		t.Fatalf("expected a second job on the fall-back day, got %v", jc.created)
	}
	ann := jc.jobs[jc.created[0]].Annotations[batchv1.CronJobScheduledTimestampAnnotation]
	if ann != "2026-11-01T01:30:00-05:00" {
		t.Fatalf("annotation %q", ann)
	}
	if first.In(ny).Format(time.RFC3339) != "2026-11-01T01:30:00-04:00" {
		t.Fatal("first run is not 01:30 EDT")
	}
	_ = r
}

// Spring-forward day: "30 2 * * *" in America/New_York has no 02:30; the
// controller requeues from Mar 7 straight to Mar 9 - no run on Mar 8.
func TestGroundsDSTSpringForwardSkipsDay(t *testing.T) {
	if _, err := time.LoadLocation("America/New_York"); err != nil {
		t.Skip("no tzdata")
	}
	cj := groundsCronJob("30 2 * * *", ptr.To("America/New_York"))
	lst := time.Date(2026, 3, 7, 7, 30, 0, 0, time.UTC) // 02:30 EST
	cj.CreationTimestamp = metav1.Time{Time: lst.Add(-time.Hour)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: lst}
	now := lst.Add(nextScheduleDelta)
	r := groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, now)
	want := time.Date(2026, 3, 9, 6, 30, 0, 0, time.UTC) // 02:30 EDT
	if r.requeue == nil || !now.Add(*r.requeue).Equal(want.Add(nextScheduleDelta)) {
		t.Fatalf("requeue %v, want %v", r.requeue, want)
	}
}

// "@every 30s" passes validation; two fires share a minute and so a job name.
// The second fire of each minute is never run.
func TestGroundsSubMinuteNameCollision(t *testing.T) {
	cj := groundsCronJob("@every 30s", nil)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(60)}
	jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
	prev := groundsOwnedJob(cj, getJobName(cj, unixSec(60)), false) // finished, out of active
	jc.jobs[prev.Name] = prev
	r := groundsSync(t, cj, []*batchv1.Job{prev}, jc, unixSec(90))
	if len(jc.created) != 0 || r.cj.Status.LastScheduleTime.Unix() != 90 || len(r.cj.Status.Active) != 1 || r.cj.Status.Active[0].Name != prev.Name {
		t.Fatalf("created=%v lst=%v active=%v", jc.created, r.cj.Status.LastScheduleTime, r.cj.Status.Active)
	}
}

// A Job the CronJob does not own holding the deterministic name: syncCronJob
// returns no requeue and no error - the CronJob is not looked at again until
// an informer event or resync.
func TestGroundsForeignNameCollisionStopsRequeue(t *testing.T) {
	cj := groundsCronJob("* * * * *", nil)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(60)}
	jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
	name := getJobName(cj, unixSec(120))
	jc.jobs[name] = &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "foreign"}}
	r := groundsSync(t, cj, nil, jc, unixSec(125))
	if r.requeue != nil || r.err != nil {
		t.Fatalf("requeue=%v err=%v", r.requeue, r.err)
	}
}

// TLA+ counterexample for "one job per scheduled time": the status write after
// creating the job is lost, the job finishes, the next sync's history cleanup
// (limit 0) deletes it, and the same scheduled time is created again.
func TestGroundsLostStatusWriteRerunsWithHistoryZero(t *testing.T) {
	cj := groundsCronJob("* * * * *", nil)
	cj.Spec.SuccessfulJobsHistoryLimit = ptr.To[int32](0)
	cj.Spec.FailedJobsHistoryLimit = ptr.To[int32](0)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(60)}
	jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
	now := unixSec(120).Add(nextScheduleDelta)

	// sync 1: job for 120 created; its status write is lost (cj not updated)
	r := groundsSync(t, cj, nil, jc, now)
	if len(jc.created) != 1 {
		t.Fatalf("sync 1 created %v", jc.created)
	}
	name := jc.created[0]
	_ = r
	// job finishes before the retry
	done := jc.jobs[name].DeepCopy()
	done.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	jc.jobs[name] = done

	// sync 2 = sync(): cleanupFinishedJobs then syncCronJob on the persisted cj
	recorder := record.NewFakeRecorder(1000)
	jm := ControllerV2{jobControl: jc, cronJobControl: &fakeCJControl{CronJob: cj.DeepCopy()}, recorder: recorder,
		now: func() time.Time { return now.Add(2 * time.Second) }}
	cjCopy := cj.DeepCopy()
	jobs := []*batchv1.Job{done}
	jm.cleanupFinishedJobs(context.TODO(), cjCopy, jobs)
	if _, _, err := jm.syncCronJob(context.TODO(), cjCopy, jobs); err != nil {
		t.Fatal(err)
	}
	if len(jc.created) != 2 || jc.created[1] != name {
		t.Fatalf("expected %s created twice, got %v (deleted %v)", name, jc.created, jc.deleted)
	}
}

// TLA+ counterexample for Forbid: the status write after creating job A is
// lost; at the next fire job A is running but not in status.active, so it is
// reported as UnexpectedJob and a second job starts under Forbid.
func TestGroundsForbidOverlapAfterLostWrite(t *testing.T) {
	for _, policy := range []batchv1.ConcurrencyPolicy{batchv1.ForbidConcurrent, batchv1.ReplaceConcurrent} {
		cj := groundsCronJob("* * * * *", nil)
		cj.Spec.ConcurrencyPolicy = policy
		cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
		cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(60)}
		jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
		groundsSync(t, cj, nil, jc, unixSec(120).Add(nextScheduleDelta)) // write lost
		a := jc.jobs[jc.created[0]]
		r := groundsSync(t, cj, []*batchv1.Job{a}, jc, unixSec(180).Add(nextScheduleDelta))
		if len(jc.created) != 2 || len(jc.deleted) != 0 || !r.hasReason("UnexpectedJob") {
			t.Fatalf("%s: created=%v deleted=%v events=%v", policy, jc.created, jc.deleted, r.events)
		}
	}
}

// Clock going backwards: nothing created, status untouched, requeue for the
// first fire after lastScheduleTime.
func TestGroundsClockBackwards(t *testing.T) {
	cj := groundsCronJob("* * * * *", nil)
	cj.CreationTimestamp = metav1.Time{Time: unixSec(0)}
	cj.Status.LastScheduleTime = &metav1.Time{Time: unixSec(6000)}
	r := groundsSync(t, cj, nil, &groundsJobControl{jobs: map[string]*batchv1.Job{}}, unixSec(100))
	if len(r.jc.created) != 0 || r.requeue == nil || *r.requeue != 5960*time.Second+nextScheduleDelta {
		t.Fatalf("created=%v requeue=%v", r.jc.created, r.requeue)
	}
}

// History limits: removeOldestJobs keeps min(n, limit); a failed delete keeps more.
func TestGroundsHistoryLimit(t *testing.T) {
	for _, c := range []struct {
		n, limit int
		failDel  bool
		want     int
	}{{5, 3, false, 3}, {2, 3, false, 2}, {5, 0, false, 0}, {5, 3, true, 5}} {
		cj := groundsCronJob("* * * * *", nil)
		cj.Spec.SuccessfulJobsHistoryLimit = ptr.To(int32(c.limit))
		jc := &groundsJobControl{jobs: map[string]*batchv1.Job{}}
		if c.failDel {
			jc.deleteErr = errors.NewInternalError(fmt.Errorf("grounds"))
		}
		var js []*batchv1.Job
		for i := 0; i < c.n; i++ {
			j := groundsOwnedJob(cj, fmt.Sprintf("cj-%d", i), false)
			j.Status.StartTime = &metav1.Time{Time: unixSec(int64(i * 60))}
			jc.jobs[j.Name] = j
			js = append(js, j)
		}
		jm := ControllerV2{jobControl: jc, recorder: record.NewFakeRecorder(1000)}
		jm.cleanupFinishedJobs(context.TODO(), cj, js)
		if len(jc.jobs) != c.want {
			t.Errorf("n=%d limit=%d failDel=%t: kept %d, want %d", c.n, c.limit, c.failDel, len(jc.jobs), c.want)
		}
	}
}
