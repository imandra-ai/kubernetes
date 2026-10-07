# Pod Security Admission — region map

Scope: `staging/src/k8s.io/pod-security-admission/policy` (every `check_*.go`, `registry.go`,
`visitor.go`, `helpers.go`) at commit `7a651b56`. No Kubernetes code was changed.

How this was built: an IML model of the pod's security-relevant fields and of every versioned
check (subject *Pod Security Admission policy* on Grounds), decomposed by ImandraX one check at a
time, with the level/version decision decomposed on top. Each region's sample pod was turned into
a real `PodSpec` and run through the real Go check (or the real `Evaluator`) by
`staging/src/k8s.io/pod-security-admission/policy/grounds_conformance_test.go`:
**204/204 region vectors, 40/40 hand-picked edge cases and 7/7 container-kind replays agree**
(`go test ./policy/ -run TestGrounds`, run from `staging/src/k8s.io/pod-security-admission`).
Vectors: `.grounds/conformance/psa.*.vectors.json`.

Words used below:

- *set* means the pointer is non-nil; *unset* means nil. A container with no `securityContext`
  has every container-level field unset.
- *effective* value of a container = its own value if set, else the pod's.
- "all containers" always means init + regular + ephemeral containers (`visitContainers`).
- `vN` = policy version v1.N. `latest` and any version newer than v1.37 behave exactly like v1.37
  (the evaluator clamps to its highest registered version; v1.37 is the sysctls check).

---

## 1. The level decision (`EvaluatePod` + `AggregateCheckResults`)

| Region | Condition | Decision |
|---|---|---|
| L1 | level = privileged | **admit**, no check runs |
| L2 | level = baseline, version ≤ v1.37 | admit iff every baseline check of that version admits |
| L3 | level = baseline, `latest` | as L2 at v1.37 |
| L4 | level = baseline, version ≥ v1.38 | as L2 at v1.37 |
| L5 | level = restricted, version ≤ v1.37 | admit iff every restricted check of that version admits (restricted checks + baseline checks not overridden) |
| L6 | level = restricted, `latest` | as L5 at v1.37 |
| L7 | level = restricted, version ≥ v1.38 | as L5 at v1.37 |

Any level string other than privileged/baseline is evaluated as restricted (fail-closed; not
modelled — `ParseLevel` never produces one).

### Which checks run, by version (`populate` / `inflateVersions`)

Baseline (2 regions):

| Versions | Checks |
|---|---|
| v1.0 – v1.33 | privileged, capabilities_baseline, hostNamespaces, hostPathVolumes, hostPorts, procMount, seccompProfile_baseline, seLinuxOptions, sysctls, appArmorProfile, windowsHostProcess |
| v1.34+ | the above + hostProbesAndHostLifecycle |

Restricted (7 regions; *overridden* baseline checks drop out):

| Versions | Baseline checks kept | Restricted checks added |
|---|---|---|
| v1.0 – v1.7 | all except hostPathVolumes | restrictedVolumes (overrides hostPathVolumes), runAsNonRoot |
| v1.8 – v1.18 | same | + allowPrivilegeEscalation |
| v1.19 – v1.21 | minus seccompProfile_baseline | + seccompProfile_restricted |
| v1.22 | minus capabilities_baseline | + capabilities_restricted |
| v1.23 – v1.33 | same | + runAsUser |
| v1.34 | + hostProbesAndHostLifecycle | same |
| v1.35+ | minus procMount (baseline) | + procMount_restricted |

---

## 2. Region map per level

### Privileged

Every pod is admitted (region L1).

### Baseline — a pod is **admitted** when all of these hold

1. No container (any kind) has `privileged: true`.
2. Every container's `capabilities.add` contains only the 13 default capabilities
   (`AUDIT_WRITE CHOWN DAC_OVERRIDE FOWNER FSETID KILL MKNOD NET_BIND_SERVICE SETFCAP SETGID SETPCAP SETUID SYS_CHROOT`),
   spelled exactly so (no `CAP_` prefix, upper case). Unset or empty `add` is fine.
3. `hostNetwork`, `hostPID`, `hostIPC` are all false.
4. No volume has a `hostPath` source.
5. No container port has a non-zero `hostPort`.
6. v1.34+: no probe or lifecycle `httpGet`/`tcpSocket` sets a non-empty `host`.
7. procMount: before v1.35 every set `procMount` is exactly `"Default"`; from v1.35 the same,
   unless the pod sets `hostUsers: false`, in which case any value is admitted.
8. seccomp:
   - v1.0 – v1.18: the pod annotation `seccomp.security.alpha.kubernetes.io/pod` and each
     container's `container.seccomp.security.alpha.kubernetes.io/<name>` annotation, if present,
     is `runtime/default`, `docker/default` or starts with `localhost/`. The `seccompProfile`
     fields are not looked at.
   - v1.19+: every *set* `seccompProfile.type` (pod and containers) is `RuntimeDefault` or
     `Localhost`. Unset is fine. Annotations are no longer looked at.
9. seLinuxOptions (pod and every container, when set): `type` is empty, `container_t`,
   `container_init_t`, `container_kvm_t` (v1.31+: also `container_engine_t`); `user` and `role`
   are empty. `level` is unrestricted.
10. Every pod sysctl name is in the allowed set of that version, matched exactly:
    v1.0 `kernel.shm_rmid_forced`, `net.ipv4.ip_local_port_range`, `net.ipv4.tcp_syncookies`,
    `net.ipv4.ping_group_range`, `net.ipv4.ip_unprivileged_port_start`; v1.27 + `net.ipv4.ip_local_reserved_ports`;
    v1.29 + `net.ipv4.tcp_keepalive_time`, `tcp_fin_timeout`, `tcp_keepalive_intvl`, `tcp_keepalive_probes`;
    v1.32 + `net.ipv4.tcp_rmem`, `tcp_wmem`; v1.37 + `net.ipv4.tcp_slow_start_after_idle`, `tcp_notsent_lowat`.
11. AppArmor: every *set* `appArmorProfile.type` (pod and containers) is `RuntimeDefault` or
    `Localhost`; every `container.apparmor.security.beta.kubernetes.io/*` annotation is empty,
    `runtime/default` or starts with `localhost/`.
12. `windowsOptions.hostProcess` is not `true` on the pod or on any container.

Baseline **rejects** a pod in exactly the complementary regions: any one of 1–12 failing is
enough, and each check names itself in the forbidden reason.

### Restricted — a pod is **admitted** when everything baseline requires holds (minus the
overridden checks, which are replaced by stricter ones), and additionally

13. Volumes: every volume has one of configMap, csi, downwardAPI, emptyDir, ephemeral, image,
    persistentVolumeClaim, projected, secret (replaces 4).
14. runAsNonRoot: the pod does not set `runAsNonRoot: false`, and every container's effective
    `runAsNonRoot` is `true`. v1.35+: skipped entirely when `hostUsers: false`.
15. v1.8+: every container sets `allowPrivilegeEscalation: false` explicitly. v1.25+: skipped for
    `os.name: windows` pods.
16. v1.19+: seccomp — the pod's `seccompProfile.type` is not set to a bad value, and every
    container's effective type is `RuntimeDefault` or `Localhost` (unset everywhere is rejected).
    Replaces 8. v1.25+: skipped for windows pods.
17. v1.22+: capabilities — every container has a `capabilities` block whose `drop` contains the
    exact string `"ALL"` and whose `add` contains only `NET_BIND_SERVICE`. Replaces 2.
    v1.25+: skipped for windows pods.
18. v1.23+: runAsUser — neither the pod nor any container sets `runAsUser: 0`.
    v1.35+: skipped when `hostUsers: false`.
19. v1.35+: procMount — every set `procMount` is `"Default"`, regardless of `hostUsers`
    (replaces 7, which is the relaxed version).

Restricted **rejects** in the complementary regions.

---

## 3. Per-check regions (one decomposition each, one step: a pod with a single container)

`c.sc` = container has a securityContext; `p.psc` = pod has a securityContext.

| Check (Go) | Regions | Admits | Rejects |
|---|---|---|---|
| `privileged_1_0` | 2 | no sc; sc with privileged unset/false | sc with privileged true |
| `capabilitiesBaseline_1_0` | 3 (+ element set of 13) | no sc; no capabilities; add ⊆ defaults | any add outside the defaults (exact string) |
| `capabilitiesRestricted_1_22/_1_25` | 9 | windows ∧ ≥v1.25; caps set ∧ drop ∋ "ALL" ∧ add ⊆ {NET_BIND_SERVICE} | no sc; no capabilities; drop lacks "ALL"; any other add |
| `hostNamespaces_1_0` | 3 | none of hostNetwork/PID/IPC | any of them |
| `hostPathVolumes_1_0` | 1 per volume | no hostPath source | hostPath source (even next to another source) |
| `hostPorts_1_0` | 1 per container | every hostPort = 0 | any non-zero hostPort |
| `hostProbesAndHostLifecycleV1Dot34` | 1 per container | every probe/lifecycle host empty | any non-empty host |
| `procMount_1_0` | 3 | no sc; procMount unset; "Default" | any other string ("Unmasked", "default", "") |
| `procMount1_35baseline` | 7 | as above, or hostUsers=false | as above unless hostUsers=false |
| `restrictedVolumes_1_0` | 1 per volume | has an allowed source | no allowed source (incl. a sourceless volume, reported "unknown") |
| `runAsNonRoot1_0/1_35` | 15 | effective true for every container; or v1.35+ ∧ hostUsers=false | pod false; container false; unset at both levels |
| `runAsUser1_23/1_35` | 11 | no 0 anywhere; or v1.35+ ∧ hostUsers=false | pod 0 (even if every container overrides); container 0 |
| `seccompProfileBaseline_1_0` | 5 (+3 element) | annotations absent or valid | an invalid pod or container annotation, including `""` |
| `seccompProfileBaseline_1_19` | 10 | every set type valid | a set type other than RuntimeDefault/Localhost |
| `seccompProfileRestricted_1_19` | 10 | every effective type valid | pod type bad; container type bad; unset at both levels |
| `seccompProfileRestricted_1_25` | 11 | as 1_19, or windows | as 1_19 for non-windows pods |
| `seLinuxOptions` (1_0/1_31) | 10 | unset; type in allowed set, user/role empty | any set option with a bad type, or user/role |
| `sysctls` (5 versions) | 5 version bands | names in the band's set | anything else (exact match) |
| `allowPrivilegeEscalation_1_8/_1_25` | 5 | explicit false; windows ∧ ≥v1.25 | unset (incl. no sc) or true |
| `appArmorProfile_1_0` | 13 (+3 element) | set types valid and annotations valid | a bad type at pod or container level; a bad annotation |
| `windowsHostProcess_1_0` | 5 | hostProcess not true anywhere | true at pod or container level |

Element-level regions (strings): `validSeccompAnnotationValue` 3 (`runtime/default`, `docker/default`,
`localhost/` prefix test), `allowedAnnotationValue` 3 (empty, `runtime/default`, prefix test).

---

## 4. Properties checked

| Property | Result |
|---|---|
| restricted admits ⇒ baseline admits | **Refuted** in general (3 counterexamples, §5 E1–E3); **proved** for every version v1.0–v1.37 (so also `latest` and future versions) for pods that pass API validation (one volume source per volume; windows pods set no capabilities and no seccompProfile) |
| an unsafe pod-level value cannot be hidden by container overrides, and an unset container value cannot dodge an unsafe pod-level one | **Proved** for runAsNonRoot, runAsUser, seccomp (baseline and restricted), seLinuxOptions, AppArmor, hostProcess: every admitted container's *effective* value is safe, and an unsafe pod-level value is always rejected |
| init / ephemeral containers get no more leeway | **Proved** for the shared per-container predicate (any predicate gives the same verdict whether a container is init, regular or ephemeral), and per check for privileged, both capabilities checks, hostPorts, hostProbes, procMount, runAsUser, both seccomp-baseline versions, allowPrivilegeEscalation, AppArmor, hostProcess; **tested** against the real evaluator for every admit vector (7/7). Not machine-proved for runAsNonRoot, seccomp-restricted, seLinux (prover limitation, see gaps) |
| runAsNonRoot + runAsUser 0 always rejected under restricted | **Refuted**: admitted at v1.0–v1.22 (E4) and at v1.35+ with `hostUsers: false` (E5). **Proved**: rejected at v1.23–v1.34 for every pod, and at v1.35+ for pods not in a user namespace |
| a newer version never admits what an older one rejected | **Proved** for every step v1.N → v1.N+1 (N = 0…36), both levels, for pods that touch none of the documented relaxations (seccomp annotations, `os.name: windows`, `hostUsers: false`, newly allowed sysctls, `container_engine_t`). Each relaxation has a counterexample in §5 and is documented in the check's comment |

---

## 5. Edge cases

| # | Edge case | Region / counterexample | What the code does | Verdict |
|---|---|---|---|---|
| E1 | Windows pod adding `SYS_ADMIN` (no drop) | `os.name: windows`, restricted ≥ v1.25 | restricted **admits** (capabilities_restricted exempts windows and overrides capabilities_baseline); baseline rejects | doc-worthy: restricted ⊄ baseline; unreachable through the API server (`validateWindows` forbids capabilities), comment in the check says it relies on that |
| E2 | Windows pod with pod-level `seccompProfile: Unconfined` | restricted ≥ v1.25 | restricted **admits**, baseline rejects (same override pattern) | doc-worthy, same reasoning as E1 |
| E3 | One volume with both `configMap` and `hostPath` | restricted, any version | restrictedVolumes sees configMap first and admits; hostPathVolumes is overridden; baseline rejects | doc-worthy: API validation rejects multi-source volumes, so unreachable in an API server, but the restricted check's switch is not a superset of the baseline check it overrides |
| E4 | `runAsNonRoot: true` with `runAsUser: 0` | restricted v1.0 – v1.22 | admitted (runAsUser check starts at v1.23); kubelet refuses to start it | by design (documented version history) |
| E5 | same, with `hostUsers: false` | restricted v1.35+ | admitted (KEP-127 relaxation) | by design (documented in the check) |
| E6 | Pod `runAsUser: 0`, every container `runAsUser: 1000` | restricted v1.23+ | rejected although no container runs as 0 | by design (fail-safe: an ephemeral container added later inherits the pod value); doc-worthy |
| E7 | Pod `runAsNonRoot: true`, container unset | restricted | admitted (inherits) | by design (documented) |
| E8 | Container has no securityContext | baseline: admitted; restricted: rejected (allowPrivilegeEscalation, capabilities, runAsNonRoot, seccomp all need explicit values) | — | by design |
| E9 | `drop: ["all"]` (lower case) | restricted v1.22+ | rejected — `"ALL"` is matched exactly | doc-worthy: conservative, but container runtimes accept lower case |
| E10 | `add: ["CAP_NET_BIND_SERVICE"]`, `add: ["chown"]`, `add: ["CAP_CHOWN"]` | restricted / baseline | rejected — exact match against unprefixed upper-case names | doc-worthy (conservative) |
| E11 | Empty capability lists (`capabilities: {}`) | baseline: admitted; restricted: rejected (no `ALL` in drop) | — | by design |
| E12 | Sysctl `net/ipv4/tcp_syncookies` (slash form) or `Kernel.shm_rmid_forced` | baseline, any version | rejected — exact match against dotted names | doc-worthy: the slash form is a valid way to name a safe sysctl, and is rejected |
| E13 | procMount `"default"`, `""`, any non-`Default` value | baseline < v1.35, restricted v1.35+ | rejected | by design (allow-list of one) |
| E14 | procMount `Unmasked` with `hostUsers: false` | baseline v1.34 rejects, v1.35 admits; restricted v1.35 rejects | — | by design (documented relaxation) |
| E15 | Pod seccomp annotation `unconfined` | baseline v1.18 rejects, v1.19+ admits; restricted v1.18 rejects, v1.19+ admits if fields are valid | annotations stop being checked at v1.19 | by design (documented; the field supersedes the annotation) |
| E16 | Empty seccomp annotation `""` | baseline < v1.19 | rejected, while an empty AppArmor annotation is admitted | doc-worthy (inconsistent, harmless) |
| E17 | `container_engine_t` SELinux type | baseline v1.30 rejects, v1.31 admits | — | by design (documented) |
| E18 | Sysctl `net.ipv4.tcp_notsent_lowat` | baseline v1.36 rejects, v1.37 admits | — | by design (documented) |
| E19 | Probe `host: 10.0.0.1` | baseline v1.33 admits, v1.34 rejects | — | by design (new check) |
| E20 | Privileged ephemeral container; init container with a hostPort | baseline | rejected, exactly as a regular container | by design (`visitContainers`); several check comments list only containers/initContainers |
| E21 | Version `latest`, `v1.100` | any | evaluated as v1.37 | by design |
| E22 | Privileged level | any pod | admitted without running checks | by design |

---

## 6. Assumptions and limits

- Versions are `v1.m`, `m ≥ 0` (what `ParseVersion` yields). A zero `api.Version{}` or a major
  other than 1 has no registered checks and would admit everything; not reachable through
  `ParseVersion`, not modelled.
- Annotations are abstracted per key class; probe/lifecycle hosts are flattened to one list per
  container; a volume is three bits (hostPath / allowed source / other source).
- Only the `Allowed` bit is modelled, not the forbidden reason/detail strings.
- Restricted ⊆ baseline and the no-bypass results rest on API validation only where stated.
