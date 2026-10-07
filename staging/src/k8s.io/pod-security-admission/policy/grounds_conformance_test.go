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

package policy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

// The region vectors come from an IML model of this package (see .grounds/REGION-MAP.md).
// This test binds each vector to the real checks: it builds the PodSpec the vector
// describes, runs the real CheckPodFn (or the real Evaluator) and compares the verdict
// with the model's.

const groundsVectorsDir = "../../../../../.grounds/conformance"

type gVectorsFile struct {
	Function string `json:"function"`
	Vectors  []struct {
		ID       string                     `json:"id"`
		Input    map[string]json.RawMessage `json:"input"`
		Expected json.RawMessage            `json:"expected"`
	} `json:"vectors"`
}

// gOpt is an IML option or variant: {"$c": "None"}, {"$c": "Some", "args": [x]}.
type gOpt struct {
	C    string            `json:"$c"`
	Args []json.RawMessage `json:"args"`
}

func (o gOpt) some(t *testing.T, into interface{}) bool {
	t.Helper()
	switch o.C {
	case "None", "":
		return false
	case "Some":
		if len(o.Args) != 1 {
			t.Fatalf("Some with %d args", len(o.Args))
		}
		if err := json.Unmarshal(o.Args[0], into); err != nil {
			t.Fatalf("decoding option payload %s: %v", o.Args[0], err)
		}
		return true
	default:
		t.Fatalf("unexpected option constructor %q", o.C)
		return false
	}
}

func (o gOpt) boolPtr(t *testing.T) *bool {
	var b bool
	if o.some(t, &b) {
		return &b
	}
	return nil
}

func (o gOpt) int64Ptr(t *testing.T) *int64 {
	var i int64
	if o.some(t, &i) {
		return &i
	}
	return nil
}

func (o gOpt) stringPtr(t *testing.T) *string {
	var s string
	if o.some(t, &s) {
		return &s
	}
	return nil
}

type gSELinux struct {
	Type string `json:"se_type"`
	User string `json:"se_user"`
	Role string `json:"se_role"`
}

type gContainer struct {
	SC           bool     `json:"sc"`
	Privileged   gOpt     `json:"privileged"`
	Caps         bool     `json:"caps"`
	CapAdd       []string `json:"cap_add"`
	CapDrop      []string `json:"cap_drop"`
	ProcMount    gOpt     `json:"proc_mount"`
	RunAsNonRoot gOpt     `json:"run_as_non_root"`
	RunAsUser    gOpt     `json:"run_as_user"`
	Seccomp      gOpt     `json:"seccomp"`
	SELinux      gOpt     `json:"selinux"`
	APE          gOpt     `json:"ape"`
	AppArmor     gOpt     `json:"apparmor"`
	HostProcess  gOpt     `json:"host_process"`
	HostPorts    []int64  `json:"host_ports"`
	ProbeHosts   []string `json:"probe_hosts"`
	SeccompAnnot gOpt     `json:"seccomp_annot"`
}

type gVolume struct {
	HostPath bool `json:"v_hostpath"`
	Allowed  bool `json:"v_allowed"`
	Other    bool `json:"v_other"`
}

type gPod struct {
	HostNetwork     bool         `json:"host_network"`
	HostPID         bool         `json:"host_pid"`
	HostIPC         bool         `json:"host_ipc"`
	HostUsers       gOpt         `json:"host_users"`
	Windows         bool         `json:"windows"`
	PSC             bool         `json:"psc"`
	PRunAsNonRoot   gOpt         `json:"p_run_as_non_root"`
	PRunAsUser      gOpt         `json:"p_run_as_user"`
	PSeccomp        gOpt         `json:"p_seccomp"`
	PSELinux        gOpt         `json:"p_selinux"`
	PAppArmor       gOpt         `json:"p_apparmor"`
	PHostProcess    gOpt         `json:"p_host_process"`
	Sysctls         []string     `json:"sysctls"`
	Volumes         []gVolume    `json:"volumes"`
	PodSeccompAnnot gOpt         `json:"pod_seccomp_annot"`
	AppArmorAnnots  []string     `json:"apparmor_annots"`
	InitCs          []gContainer `json:"init_cs"`
	Cs              []gContainer `json:"cs"`
	EphCs           []gContainer `json:"eph_cs"`
}

// imlParser reads the values ImandraX prints when it cannot render them as JSON
// ({"$iml": "{sc = false; privileged = Some false; ...}"}) into the JSON shape of the
// other vectors, so both decode the same way.
type imlParser struct {
	s   string
	pos int
}

func (p *imlParser) skip() {
	for p.pos < len(p.s) && strings.ContainsRune(" \t\r\n", rune(p.s[p.pos])) {
		p.pos++
	}
}

func (p *imlParser) peek() byte {
	p.skip()
	if p.pos >= len(p.s) {
		return 0
	}
	return p.s[p.pos]
}

func (p *imlParser) expect(c byte) error {
	if p.peek() != c {
		return fmt.Errorf("expected %q at %d in %q", c, p.pos, p.s)
	}
	p.pos++
	return nil
}

func (p *imlParser) ident() string {
	p.skip()
	start := p.pos
	for p.pos < len(p.s) {
		c := p.s[p.pos]
		if c == '_' || c == '.' || c == '\'' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			p.pos++
			continue
		}
		break
	}
	return p.s[start:p.pos]
}

// startsAtom reports whether a constructor argument follows.
func (p *imlParser) startsAtom() bool {
	c := p.peek()
	return c == '"' || c == '[' || c == '{' || c == '(' || c == '-' || (c >= '0' && c <= '9') ||
		(c >= 'A' && c <= 'Z') || strings.HasPrefix(p.s[p.pos:], "true") || strings.HasPrefix(p.s[p.pos:], "false")
}

func (p *imlParser) value() (interface{}, error) {
	if c := p.peek(); c >= 'A' && c <= 'Z' {
		name := p.ident()
		if !p.startsAtom() {
			return map[string]interface{}{"$c": name}, nil
		}
		arg, err := p.atom()
		if err != nil {
			return nil, err
		}
		return map[string]interface{}{"$c": name, "args": []interface{}{arg}}, nil
	}
	return p.atom()
}

func (p *imlParser) atom() (interface{}, error) {
	switch c := p.peek(); {
	case c == '"':
		start := p.pos
		p.pos++
		for p.pos < len(p.s) && p.s[p.pos] != '"' {
			if p.s[p.pos] == '\\' {
				p.pos++
			}
			p.pos++
		}
		p.pos++
		s, err := strconv.Unquote(p.s[start:p.pos])
		if err != nil {
			return nil, fmt.Errorf("string %s: %w", p.s[start:p.pos], err)
		}
		return s, nil
	case c == '(':
		p.pos++
		v, err := p.value()
		if err != nil {
			return nil, err
		}
		return v, p.expect(')')
	case c == '[':
		p.pos++
		items := []interface{}{}
		for p.peek() != ']' {
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			items = append(items, v)
			if p.peek() == ';' {
				p.pos++
			}
		}
		p.pos++
		return items, nil
	case c == '{':
		p.pos++
		rec := map[string]interface{}{}
		for p.peek() != '}' {
			name := p.ident()
			if err := p.expect('='); err != nil {
				return nil, err
			}
			v, err := p.value()
			if err != nil {
				return nil, err
			}
			rec[name] = v
			if p.peek() == ';' {
				p.pos++
			}
		}
		p.pos++
		return rec, nil
	case c == '-' || (c >= '0' && c <= '9'):
		start := p.pos
		p.pos++
		for p.pos < len(p.s) && p.s[p.pos] >= '0' && p.s[p.pos] <= '9' {
			p.pos++
		}
		return json.Number(p.s[start:p.pos]), nil
	default:
		switch word := p.ident(); word {
		case "true":
			return true, nil
		case "false":
			return false, nil
		default:
			return nil, fmt.Errorf("unexpected %q at %d in %q", word, p.pos, p.s)
		}
	}
}

func imlToJSON(raw json.RawMessage) (json.RawMessage, error) {
	var wrapped struct {
		IML *string `json:"$iml"`
	}
	if json.Unmarshal(raw, &wrapped) != nil || wrapped.IML == nil {
		return raw, nil
	}
	p := &imlParser{s: *wrapped.IML}
	v, err := p.value()
	if err != nil {
		return nil, err
	}
	if p.peek() != 0 {
		return nil, fmt.Errorf("trailing input at %d in %q", p.pos, p.s)
	}
	return json.Marshal(v)
}

func decode(t *testing.T, raw json.RawMessage, into interface{}) {
	t.Helper()
	if raw == nil {
		t.Fatalf("missing input")
	}
	raw, err := imlToJSON(raw)
	if err != nil {
		t.Fatalf("reading IML value: %v", err)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatalf("decoding %s: %v", raw, err)
	}
}

func selinuxOpts(t *testing.T, o gOpt) *corev1.SELinuxOptions {
	var s gSELinux
	if o.some(t, &s) {
		return &corev1.SELinuxOptions{Type: s.Type, User: s.User, Role: s.Role}
	}
	return nil
}

// probe/lifecycle host slots a model container's probe_hosts are spread over, in order.
func setProbeHost(t *testing.T, c *corev1.Container, slot int, host string) {
	t.Helper()
	httpGet := &corev1.HTTPGetAction{Host: host}
	tcp := &corev1.TCPSocketAction{Host: host}
	probe := func(p **corev1.Probe) *corev1.Probe {
		if *p == nil {
			*p = &corev1.Probe{}
		}
		return *p
	}
	if slot >= 6 && c.Lifecycle == nil {
		c.Lifecycle = &corev1.Lifecycle{}
	}
	handler := func(h **corev1.LifecycleHandler) *corev1.LifecycleHandler {
		if *h == nil {
			*h = &corev1.LifecycleHandler{}
		}
		return *h
	}
	switch slot {
	case 0:
		probe(&c.LivenessProbe).HTTPGet = httpGet
	case 1:
		probe(&c.LivenessProbe).TCPSocket = tcp
	case 2:
		probe(&c.ReadinessProbe).HTTPGet = httpGet
	case 3:
		probe(&c.ReadinessProbe).TCPSocket = tcp
	case 4:
		probe(&c.StartupProbe).HTTPGet = httpGet
	case 5:
		probe(&c.StartupProbe).TCPSocket = tcp
	case 6:
		handler(&c.Lifecycle.PostStart).HTTPGet = httpGet
	case 7:
		handler(&c.Lifecycle.PostStart).TCPSocket = tcp
	case 8:
		handler(&c.Lifecycle.PreStop).HTTPGet = httpGet
	case 9:
		handler(&c.Lifecycle.PreStop).TCPSocket = tcp
	default:
		t.Fatalf("model container has more probe hosts than the 10 probe/lifecycle host fields")
	}
}

func buildContainer(t *testing.T, gc gContainer, name string, annotations map[string]string) corev1.Container {
	t.Helper()
	c := corev1.Container{Name: name, Image: "image"}
	if gc.SC {
		sc := &corev1.SecurityContext{
			Privileged:               gc.Privileged.boolPtr(t),
			RunAsNonRoot:             gc.RunAsNonRoot.boolPtr(t),
			RunAsUser:                gc.RunAsUser.int64Ptr(t),
			AllowPrivilegeEscalation: gc.APE.boolPtr(t),
			SELinuxOptions:           selinuxOpts(t, gc.SELinux),
		}
		if gc.Caps {
			caps := &corev1.Capabilities{}
			for _, a := range gc.CapAdd {
				caps.Add = append(caps.Add, corev1.Capability(a))
			}
			for _, d := range gc.CapDrop {
				caps.Drop = append(caps.Drop, corev1.Capability(d))
			}
			sc.Capabilities = caps
		}
		if pm := gc.ProcMount.stringPtr(t); pm != nil {
			pmt := corev1.ProcMountType(*pm)
			sc.ProcMount = &pmt
		}
		if s := gc.Seccomp.stringPtr(t); s != nil {
			sc.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileType(*s)}
		}
		if a := gc.AppArmor.stringPtr(t); a != nil {
			sc.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileType(*a)}
		}
		if hp := gc.HostProcess.boolPtr(t); hp != nil {
			sc.WindowsOptions = &corev1.WindowsSecurityContextOptions{HostProcess: hp}
		}
		c.SecurityContext = sc
	}
	for i, hp := range gc.HostPorts {
		c.Ports = append(c.Ports, corev1.ContainerPort{Name: fmt.Sprintf("p%d", i), ContainerPort: 8080, HostPort: int32(hp)})
	}
	for i, h := range gc.ProbeHosts {
		setProbeHost(t, &c, i, h)
	}
	if v := gc.SeccompAnnot.stringPtr(t); v != nil {
		annotations[annotationKeyContainerPrefix+name] = *v
	}
	return c
}

func buildPod(t *testing.T, gp gPod) (*metav1.ObjectMeta, *corev1.PodSpec) {
	t.Helper()
	meta := &metav1.ObjectMeta{Name: "pod", Annotations: map[string]string{}}
	spec := &corev1.PodSpec{
		HostNetwork: gp.HostNetwork,
		HostPID:     gp.HostPID,
		HostIPC:     gp.HostIPC,
		HostUsers:   gp.HostUsers.boolPtr(t),
	}
	if gp.Windows {
		spec.OS = &corev1.PodOS{Name: corev1.Windows}
	}
	if gp.PSC {
		psc := &corev1.PodSecurityContext{
			RunAsNonRoot:   gp.PRunAsNonRoot.boolPtr(t),
			RunAsUser:      gp.PRunAsUser.int64Ptr(t),
			SELinuxOptions: selinuxOpts(t, gp.PSELinux),
		}
		if s := gp.PSeccomp.stringPtr(t); s != nil {
			psc.SeccompProfile = &corev1.SeccompProfile{Type: corev1.SeccompProfileType(*s)}
		}
		if a := gp.PAppArmor.stringPtr(t); a != nil {
			psc.AppArmorProfile = &corev1.AppArmorProfile{Type: corev1.AppArmorProfileType(*a)}
		}
		if hp := gp.PHostProcess.boolPtr(t); hp != nil {
			psc.WindowsOptions = &corev1.WindowsSecurityContextOptions{HostProcess: hp}
		}
		for _, s := range gp.Sysctls {
			psc.Sysctls = append(psc.Sysctls, corev1.Sysctl{Name: s, Value: "1"})
		}
		spec.SecurityContext = psc
	}
	for i, v := range gp.Volumes {
		vol := corev1.Volume{Name: fmt.Sprintf("vol-%d", i)}
		if v.HostPath {
			vol.HostPath = &corev1.HostPathVolumeSource{Path: "/"}
		}
		if v.Allowed {
			vol.EmptyDir = &corev1.EmptyDirVolumeSource{}
		}
		if v.Other {
			vol.NFS = &corev1.NFSVolumeSource{Server: "nfs", Path: "/"}
		}
		spec.Volumes = append(spec.Volumes, vol)
	}
	if v := gp.PodSeccompAnnot.stringPtr(t); v != nil {
		meta.Annotations[annotationKeyPod] = *v
	}
	for i, v := range gp.AppArmorAnnots {
		meta.Annotations[fmt.Sprintf("%saa-%d", corev1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix, i)] = v
	}
	for i, c := range gp.InitCs {
		spec.InitContainers = append(spec.InitContainers, buildContainer(t, c, fmt.Sprintf("init-%d", i), meta.Annotations))
	}
	for i, c := range gp.Cs {
		spec.Containers = append(spec.Containers, buildContainer(t, c, fmt.Sprintf("c-%d", i), meta.Annotations))
	}
	for i, c := range gp.EphCs {
		ec := buildContainer(t, c, fmt.Sprintf("eph-%d", i), meta.Annotations)
		spec.EphemeralContainers = append(spec.EphemeralContainers, corev1.EphemeralContainer{EphemeralContainerCommon: corev1.EphemeralContainerCommon(ec)})
	}
	return meta, spec
}

// checkAt picks the CheckPodFn the registry would use at policy v1.<minor>; below the first
// MinimumVersion it returns the first one, as the model's run_check does.
func checkAt(c Check, minor int) CheckPodFn {
	fn := c.Versions[0].CheckPod
	v := api.MajorMinorVersion(1, minor)
	for _, vc := range c.Versions {
		if !v.Older(vc.MinimumVersion) {
			fn = vc.CheckPod
		}
	}
	return fn
}

// the model's step functions are a check run on its pod with c as the only container.
func onePod(p gPod, c gContainer) gPod {
	p.InitCs, p.Cs, p.EphCs = nil, []gContainer{c}, nil
	return p
}

var modelCheckIDs = map[string]CheckID{
	"Privileged":            "privileged",
	"Caps_baseline":         checkCapabilitiesBaselineID,
	"Host_namespaces":       "hostNamespaces",
	"Host_path":             checkHostPathVolumesID,
	"Host_ports":            "hostPorts",
	"Host_probes":           "hostProbesAndHostLifecycle",
	"Proc_mount":            "procMount",
	"Seccomp_baseline":      checkSeccompBaselineID,
	"Selinux":               "seLinuxOptions",
	"Sysctls":               "sysctls",
	"Apparmor":              "appArmorProfile",
	"Windows_host_process":  "windowsHostProcess",
	"Caps_restricted":       "capabilities_restricted",
	"Proc_mount_restricted": "procMount_restricted",
	"Restricted_volumes":    "restrictedVolumes",
	"Run_as_non_root":       "runAsNonRoot",
	"Run_as_user":           "runAsUser",
	"Seccomp_restricted":    "seccompProfile_restricted",
	"Ape":                   "allowPrivilegeEscalation",
}

// activeIDs names the checks the real registry runs for a level at v1.<minor>, by matching
// each registered CheckPodFn against the version of each check that applies there.
func activeIDs(t *testing.T, fns []CheckPodFn, minor int) []string {
	t.Helper()
	var ids []string
	for _, fn := range fns {
		ptr := reflect.ValueOf(fn).Pointer()
		var match []string
		for _, c := range DefaultChecks() {
			// a check not yet introduced at this version cannot be the one registered here
			if v := api.MajorMinorVersion(1, minor); v.Older(c.Versions[0].MinimumVersion) {
				continue
			}
			if reflect.ValueOf(checkAt(c, minor)).Pointer() == ptr {
				match = append(match, string(c.ID))
			}
		}
		if len(match) != 1 {
			t.Fatalf("registered check at v1.%d matches %d checks: %v", minor, len(match), match)
		}
		ids = append(ids, match[0])
	}
	sort.Strings(ids)
	return ids
}

func newGroundsEvaluator(t *testing.T) *checkRegistry {
	t.Helper()
	e, err := NewEvaluator(DefaultChecks(), nil)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func admitReal(t *testing.T, e *checkRegistry, lvl gOpt, ver gOpt, meta *metav1.ObjectMeta, spec *corev1.PodSpec) bool {
	t.Helper()
	var level api.Level
	switch lvl.C {
	case "L_privileged":
		level = api.LevelPrivileged
	case "L_baseline":
		level = api.LevelBaseline
	case "L_restricted":
		level = api.LevelRestricted
	default:
		t.Fatalf("unexpected level %q", lvl.C)
	}
	var version api.Version
	switch ver.C {
	case "Latest":
		version = api.LatestVersion()
	case "V":
		var m int
		decode(t, ver.Args[0], &m)
		version = api.MajorMinorVersion(1, m)
	default:
		t.Fatalf("unexpected version %q", ver.C)
	}
	return AggregateCheckResults(e.EvaluatePod(api.LevelVersion{Level: level, Version: version}, meta, spec)).Allowed
}

func runVector(t *testing.T, e *checkRegistry, function string, in map[string]json.RawMessage) interface{} {
	t.Helper()
	var (
		m int
		p gPod
		c gContainer
		v gVolume
	)
	if raw, ok := in["m"]; ok {
		decode(t, raw, &m)
	} else {
		m = 37 // single-version checks: any version
	}
	if raw, ok := in["p"]; ok {
		decode(t, raw, &p)
	}
	if raw, ok := in["c"]; ok {
		decode(t, raw, &c)
	}
	if function == "host_path_vol_step" || function == "restricted_vol_step" {
		decode(t, in["v"], &v)
	}
	run := func(check Check, pod gPod) bool {
		meta, spec := buildPod(t, pod)
		return checkAt(check, m)(meta, spec).Allowed
	}
	switch function {
	case "privileged_step":
		return run(CheckPrivileged(), onePod(p, c))
	case "caps_baseline_step":
		return run(CheckCapabilitiesBaseline(), onePod(p, c))
	case "caps_restricted_step":
		return run(CheckCapabilitiesRestricted(), onePod(p, c))
	case "host_namespaces_step":
		return run(CheckHostNamespaces(), p)
	case "host_path_vol_step":
		return run(CheckHostPathVolumes(), gPod{Volumes: []gVolume{v}})
	case "restricted_vol_step":
		return run(CheckRestrictedVolumes(), gPod{Volumes: []gVolume{v}})
	case "host_ports_step":
		return run(CheckHostPorts(), onePod(p, c))
	case "host_probes_step":
		return run(CheckHostProbesAndHostLifecycle(), onePod(p, c))
	case "proc_mount_baseline_step":
		return run(CheckProcMountBaseline(), onePod(p, c))
	case "proc_mount_restricted_step":
		return run(CheckProcMountRestricted(), onePod(p, c))
	case "run_as_non_root_step":
		return run(CheckRunAsNonRoot(), onePod(p, c))
	case "run_as_user_step":
		return run(CheckRunAsUser(), onePod(p, c))
	case "seccomp_baseline_step":
		return run(CheckSeccompBaseline(), onePod(p, c))
	case "seccomp_baseline_v1_0":
		m = 0
		return run(CheckSeccompBaseline(), onePod(p, c))
	case "seccomp_baseline_v1_19":
		m = 19
		return run(CheckSeccompBaseline(), onePod(p, c))
	case "seccomp_restricted_step":
		return run(CheckSeccompProfileRestricted(), onePod(p, c))
	case "seccomp_restricted_v1_19":
		m = 19
		return run(CheckSeccompProfileRestricted(), onePod(p, c))
	case "seccomp_restricted_v1_25":
		m = 25
		return run(CheckSeccompProfileRestricted(), onePod(p, c))
	case "valid_seccomp_annot", "apparmor_annot_ok":
		var s string
		decode(t, in["v"], &s)
		if function == "valid_seccomp_annot" {
			return validSeccompAnnotationValue(s)
		}
		return allowedAnnotationValue(s)
	case "selinux_step":
		return run(CheckSELinuxOptions(), onePod(p, c))
	case "ape_step":
		return run(CheckAllowPrivilegeEscalation(), onePod(p, c))
	case "apparmor_step":
		return run(CheckAppArmorProfile(), onePod(p, c))
	case "windows_host_process_step":
		return run(CheckWindowsHostProcess(), onePod(p, c))
	case "sysctl_allowed":
		var s string
		decode(t, in["s"], &s)
		return run(CheckSysctls(), gPod{PSC: true, Sysctls: []string{s}})
	case "admit":
		var lvl, ver gOpt
		decode(t, in["lvl"], &lvl)
		decode(t, in["v"], &ver)
		meta, spec := buildPod(t, p)
		return admitReal(t, e, lvl, ver, meta, spec)
	case "baseline_ids":
		return activeIDs(t, e.baselineChecks[api.MajorMinorVersion(1, m)], m)
	case "restricted_ids":
		return activeIDs(t, e.restrictedChecks[api.MajorMinorVersion(1, m)], m)
	}
	t.Fatalf("no binding for model function %q", function)
	return nil
}

func expectedValue(t *testing.T, function string, raw json.RawMessage) interface{} {
	t.Helper()
	if function == "baseline_ids" || function == "restricted_ids" {
		var ids []gOpt
		decode(t, raw, &ids)
		var out []string
		for _, id := range ids {
			goID, ok := modelCheckIDs[id.C]
			if !ok {
				t.Fatalf("unknown model check id %q", id.C)
			}
			out = append(out, string(goID))
		}
		sort.Strings(out)
		return out
	}
	var b bool
	decode(t, raw, &b)
	return b
}

func loadGroundsVectors(t *testing.T) []gVectorsFile {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(groundsVectorsDir, "psa.*.vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatalf("no vectors under %s", groundsVectorsDir)
	}
	var files []gVectorsFile
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var f gVectorsFile
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		files = append(files, f)
	}
	return files
}

func TestGroundsConformance(t *testing.T) {
	e := newGroundsEvaluator(t)
	for _, f := range loadGroundsVectors(t) {
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				want := expectedValue(t, f.Function, v.Expected)
				got := runVector(t, e, f.Function, v.Input)
				if !reflect.DeepEqual(got, want) {
					t.Errorf("%s: model %v, code %v", f.Function, want, got)
				}
			})
		}
	}
}

func gSome(v interface{}) gOpt {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return gOpt{C: "Some", Args: []json.RawMessage{raw}}
}

// The hand-picked pods below are the IML constants of the same names; their verdicts were
// computed by ImandraX on the model (ground evaluation), since its vector evaluator cannot run
// String.prefix at chosen inputs.
func edgeCasePods() map[string]gPod {
	baseC := gContainer{SC: true, Caps: true, CapDrop: []string{"ALL"}, RunAsNonRoot: gSome(true), Seccomp: gSome("RuntimeDefault"), APE: gSome(false)}
	base := gPod{Cs: []gContainer{baseC}}
	with := func(f func(p *gPod)) gPod {
		p := base
		p.Cs = []gContainer{baseC}
		f(&p)
		return p
	}
	withC := func(f func(c *gContainer)) gPod {
		c := baseC
		f(&c)
		return with(func(p *gPod) { p.Cs = []gContainer{c} })
	}
	hostnet := with(func(p *gPod) { p.HostNetwork = true })
	rnrPodTrue := with(func(p *gPod) {
		p.PSC, p.PRunAsNonRoot = true, gSome(true)
		c := baseC
		c.RunAsNonRoot = gOpt{}
		p.Cs = []gContainer{c}
	})
	rnrContainerFalse := rnrPodTrue
	rnrContainerFalse.Cs = []gContainer{withC(func(c *gContainer) { c.RunAsNonRoot = gSome(false) }).Cs[0]}
	return map[string]gPod{
		"base_pod": base,
		"cex_win_caps": with(func(p *gPod) {
			p.Windows = true
			c := baseC
			c.CapAdd, c.CapDrop = []string{"SYS_ADMIN"}, nil
			p.Cs = []gContainer{c}
		}),
		"cex_win_seccomp": with(func(p *gPod) {
			p.Windows, p.PSC, p.PSeccomp = true, true, gSome("Unconfined")
			c := baseC
			c.Seccomp = gOpt{}
			p.Cs = []gContainer{c}
		}),
		"cex_two_sources": with(func(p *gPod) { p.Volumes = []gVolume{{HostPath: true, Allowed: true}} }),
		"cex_rnr_root":    withC(func(c *gContainer) { c.RunAsUser = gSome(0) }),
		"cex_rnr_root_userns": func() gPod {
			p := withC(func(c *gContainer) { c.RunAsUser = gSome(0) })
			p.HostUsers = gSome(false)
			return p
		}(),
		"annot_unconfined": with(func(p *gPod) { p.PodSeccompAnnot = gSome("unconfined") }),
		"sysctl_new":       with(func(p *gPod) { p.PSC, p.Sysctls = true, []string{"net.ipv4.tcp_notsent_lowat"} }),
		"sysctl_slash":     with(func(p *gPod) { p.PSC, p.Sysctls = true, []string{"net/ipv4/tcp_syncookies"} }),
		"engine":           withC(func(c *gContainer) { c.SELinux = gSome(gSELinux{Type: "container_engine_t"}) }),
		"userns_unmasked": func() gPod {
			p := withC(func(c *gContainer) { c.ProcMount = gSome("Unmasked") })
			p.HostUsers = gSome(false)
			return p
		}(),
		"pod_root_overridden": func() gPod {
			p := withC(func(c *gContainer) { c.RunAsUser = gSome(1000) })
			p.PSC, p.PRunAsUser = true, gSome(0)
			return p
		}(),
		"nil_sc":              withC(func(c *gContainer) { c.SC = false }),
		"eph_privileged":      with(func(p *gPod) { c := baseC; c.Privileged = gSome(true); p.EphCs = []gContainer{c} }),
		"init_hostport":       with(func(p *gPod) { c := baseC; c.HostPorts = []int64{8080}; p.InitCs = []gContainer{c} }),
		"rnr_pod_true":        rnrPodTrue,
		"rnr_container_false": rnrContainerFalse,
		"hostnet":             hostnet,
		"worst": func() gPod {
			p := withC(func(c *gContainer) { c.Privileged = gSome(true) })
			p.HostNetwork = true
			return p
		}(),
		"caps_all_lower":            withC(func(c *gContainer) { c.CapDrop = []string{"all"} }),
		"empty_seccomp_annot":       with(func(p *gPod) { p.PodSeccompAnnot = gSome("") }),
		"apparmor_empty_annot":      with(func(p *gPod) { p.AppArmorAnnots = []string{""} }),
		"apparmor_unconfined_annot": with(func(p *gPod) { p.AppArmorAnnots = []string{"unconfined"} }),
		"probe_host":                withC(func(c *gContainer) { c.ProbeHosts = []string{"10.0.0.1"} }),
		"caps_sys_admin":            withC(func(c *gContainer) { c.CapAdd = []string{"SYS_ADMIN"} }),
	}
}

func TestGroundsEdgeCases(t *testing.T) {
	e := newGroundsEvaluator(t)
	pods := edgeCasePods()
	latest := gOpt{C: "Latest"}
	v := func(m int) gOpt { return gOpt{C: "V", Args: []json.RawMessage{json.RawMessage(strconv.Itoa(m))}} }
	lvl := func(l string) gOpt { return gOpt{C: l} }
	const b, r, p = "L_baseline", "L_restricted", "L_privileged"
	cases := []struct {
		pod   string
		level string
		ver   gOpt
		model bool
	}{
		{"base_pod", r, latest, true},
		{"cex_win_caps", b, latest, false},
		{"cex_win_caps", r, latest, true},
		{"cex_win_caps", r, v(24), false},
		{"cex_win_seccomp", b, latest, false},
		{"cex_win_seccomp", r, latest, true},
		{"cex_two_sources", b, latest, false},
		{"cex_two_sources", r, latest, true},
		{"worst", p, latest, true},
		{"hostnet", b, v(100), false},
		{"cex_rnr_root", r, v(22), true},
		{"cex_rnr_root", r, v(23), false},
		{"cex_rnr_root_userns", r, latest, true},
		{"cex_rnr_root_userns", r, v(34), false},
		{"annot_unconfined", b, v(18), false},
		{"annot_unconfined", b, v(19), true},
		{"annot_unconfined", r, v(18), false},
		{"annot_unconfined", r, v(19), true},
		{"sysctl_new", b, v(36), false},
		{"sysctl_new", b, v(37), true},
		{"engine", b, v(30), false},
		{"engine", b, v(31), true},
		{"userns_unmasked", b, v(34), false},
		{"userns_unmasked", b, v(35), true},
		{"userns_unmasked", r, v(35), false},
		{"probe_host", b, v(33), true},
		{"probe_host", b, latest, false},
		{"caps_sys_admin", r, v(21), false},
		{"empty_seccomp_annot", b, v(0), false},
		{"sysctl_slash", b, latest, false},
		{"pod_root_overridden", r, latest, false},
		{"nil_sc", r, latest, false},
		{"nil_sc", b, latest, true},
		{"eph_privileged", b, latest, false},
		{"init_hostport", b, latest, false},
		{"rnr_pod_true", r, latest, true},
		{"rnr_container_false", r, latest, false},
		{"caps_all_lower", r, latest, false},
		{"apparmor_empty_annot", b, latest, true},
		{"apparmor_unconfined_annot", b, latest, false},
	}
	for i, c := range cases {
		pod, ok := pods[c.pod]
		if !ok {
			t.Fatalf("no pod named %q", c.pod)
		}
		t.Run(fmt.Sprintf("edge_%d_%s", i, c.pod), func(t *testing.T) {
			meta, spec := buildPod(t, pod)
			if got := admitReal(t, e, lvl(c.level), c.ver, meta, spec); got != c.model {
				t.Errorf("%s at %s %s: model %v, code %v", c.pod, c.level, c.ver.C, c.model, got)
			}
		})
	}
}

// TestGroundsContainerKindIrrelevant replays every admit vector with all containers moved to
// initContainers and then to ephemeralContainers: the verdict must not change.
func TestGroundsContainerKindIrrelevant(t *testing.T) {
	e := newGroundsEvaluator(t)
	for _, f := range loadGroundsVectors(t) {
		if f.Function != "admit" {
			continue
		}
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				var p gPod
				var lvl, ver gOpt
				decode(t, v.Input["p"], &p)
				decode(t, v.Input["lvl"], &lvl)
				decode(t, v.Input["v"], &ver)
				all := append(append(append([]gContainer{}, p.InitCs...), p.Cs...), p.EphCs...)
				verdict := func(q gPod) bool {
					meta, spec := buildPod(t, q)
					return admitReal(t, e, lvl, ver, meta, spec)
				}
				regular, init, eph := p, p, p
				regular.InitCs, regular.Cs, regular.EphCs = nil, all, nil
				init.InitCs, init.Cs, init.EphCs = all, nil, nil
				eph.InitCs, eph.Cs, eph.EphCs = nil, nil, all
				want := verdict(regular)
				if got := verdict(init); got != want {
					t.Errorf("as init containers: %v, as regular containers: %v", got, want)
				}
				if got := verdict(eph); got != want {
					t.Errorf("as ephemeral containers: %v, as regular containers: %v", got, want)
				}
			})
		}
	}
}
