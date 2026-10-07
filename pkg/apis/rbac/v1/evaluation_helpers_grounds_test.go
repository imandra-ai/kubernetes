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

package v1

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

// The vectors come from an ImandraX model of these matchers in which a Go
// string is a list of ints, one per byte. Region vectors cover every branch of
// the model; "chosen" vectors pin the boundary cases (e.g. '*/*', '/healthz*',
// '**', ResourceNames [""]).
const groundsVectorsDir = "../../../../.grounds/conformance"

type groundsVector struct {
	ID       string                     `json:"id"`
	Input    map[string]json.RawMessage `json:"input"`
	Expected bool                       `json:"expected"`
}

type groundsVectorFile struct {
	Function string          `json:"function"`
	Vectors  []groundsVector `json:"vectors"`
}

func loadGroundsVectors(t *testing.T, name string) groundsVectorFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(groundsVectorsDir, name+".vectors.json"))
	if err != nil {
		t.Fatalf("reading vectors for %s: %v", name, err)
	}
	var f groundsVectorFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decoding vectors for %s: %v", name, err)
	}
	if f.Function != name || len(f.Vectors) == 0 {
		t.Fatalf("vectors file for %s holds function %q with %d vectors", name, f.Function, len(f.Vectors))
	}
	return f
}

func groundsBytes(t *testing.T, codes []int) string {
	t.Helper()
	b := make([]byte, len(codes))
	for i, c := range codes {
		// The model's ints stand for bytes; anything else cannot be a Go string byte.
		if c < 0 || c > 255 {
			t.Fatalf("vector byte %d out of range 0..255", c)
		}
		b[i] = byte(c)
	}
	return string(b)
}

func groundsString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var codes []int
	if err := json.Unmarshal(raw, &codes); err != nil {
		t.Fatalf("decoding string %s: %v", raw, err)
	}
	return groundsBytes(t, codes)
}

func groundsStrings(t *testing.T, raw json.RawMessage) []string {
	t.Helper()
	var lists [][]int
	if err := json.Unmarshal(raw, &lists); err != nil {
		t.Fatalf("decoding string list %s: %v", raw, err)
	}
	out := make([]string, len(lists))
	for i, l := range lists {
		out[i] = groundsBytes(t, l)
	}
	return out
}

func TestGroundsConformance(t *testing.T) {
	matchers := map[string]func(t *testing.T, in map[string]json.RawMessage) bool{
		"verb_matches": func(t *testing.T, in map[string]json.RawMessage) bool {
			return VerbMatches(&rbacv1.PolicyRule{Verbs: groundsStrings(t, in["verbs"])}, groundsString(t, in["v"]))
		},
		"api_group_matches": func(t *testing.T, in map[string]json.RawMessage) bool {
			return APIGroupMatches(&rbacv1.PolicyRule{APIGroups: groundsStrings(t, in["groups"])}, groundsString(t, in["g"]))
		},
		"resource_matches": func(t *testing.T, in map[string]json.RawMessage) bool {
			return ResourceMatches(&rbacv1.PolicyRule{Resources: groundsStrings(t, in["rs"])}, groundsString(t, in["combined"]), groundsString(t, in["sub"]))
		},
		"resource_name_matches": func(t *testing.T, in map[string]json.RawMessage) bool {
			return ResourceNameMatches(&rbacv1.PolicyRule{ResourceNames: groundsStrings(t, in["names"])}, groundsString(t, in["n"]))
		},
		"non_resource_url_matches": func(t *testing.T, in map[string]json.RawMessage) bool {
			return NonResourceURLMatches(&rbacv1.PolicyRule{NonResourceURLs: groundsStrings(t, in["urls"])}, groundsString(t, in["url"]))
		},
	}
	for name, call := range matchers {
		f := loadGroundsVectors(t, name)
		for _, v := range f.Vectors {
			t.Run(v.ID, func(t *testing.T) {
				if got := call(t, v.Input); got != v.Expected {
					t.Errorf("%s(%s): code returned %v, model expects %v", name, groundsInputJSON(v.Input), got, v.Expected)
				}
			})
		}
	}
}

func groundsInputJSON(in map[string]json.RawMessage) string {
	b, _ := json.Marshal(in)
	return string(b)
}
