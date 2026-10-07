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

package rbac

import (
	"encoding/json"
	"os"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// Vectors from an ImandraX model of RuleAllows; strings are byte lists.
const groundsRuleAllowsVectors = "../../../../../.grounds/conformance/rule_allows.vectors.json"

type groundsByteString []int

type groundsRequest struct {
	IsResource  bool              `json:"is_resource"`
	Verb        groundsByteString `json:"verb"`
	APIGroup    groundsByteString `json:"api_group"`
	Resource    groundsByteString `json:"resource"`
	Subresource groundsByteString `json:"subresource"`
	Name        groundsByteString `json:"name"`
	Path        groundsByteString `json:"path"`
}

type groundsRule struct {
	Verbs           []groundsByteString `json:"verbs"`
	APIGroups       []groundsByteString `json:"api_groups"`
	Resources       []groundsByteString `json:"resources"`
	ResourceNames   []groundsByteString `json:"resource_names"`
	NonResourceURLs []groundsByteString `json:"non_resource_urls"`
}

type groundsRuleAllowsVector struct {
	ID    string `json:"id"`
	Input struct {
		Req  groundsRequest `json:"req"`
		Rule groundsRule    `json:"r"`
	} `json:"input"`
	Expected bool `json:"expected"`
}

func (s groundsByteString) toString(t *testing.T) string {
	t.Helper()
	b := make([]byte, len(s))
	for i, c := range s {
		// The model's ints stand for bytes; anything else cannot be a Go string byte.
		if c < 0 || c > 255 {
			t.Fatalf("vector byte %d out of range 0..255", c)
		}
		b[i] = byte(c)
	}
	return string(b)
}

func groundsToStrings(t *testing.T, l []groundsByteString) []string {
	t.Helper()
	out := make([]string, len(l))
	for i, s := range l {
		out[i] = s.toString(t)
	}
	return out
}

func TestGroundsConformanceRuleAllows(t *testing.T) {
	data, err := os.ReadFile(groundsRuleAllowsVectors)
	if err != nil {
		t.Fatalf("reading RuleAllows vectors: %v", err)
	}
	var f struct {
		Function string                    `json:"function"`
		Vectors  []groundsRuleAllowsVector `json:"vectors"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decoding RuleAllows vectors: %v", err)
	}
	if f.Function != "rule_allows" || len(f.Vectors) == 0 {
		t.Fatalf("vectors file holds function %q with %d vectors, want rule_allows", f.Function, len(f.Vectors))
	}
	for _, v := range f.Vectors {
		t.Run(v.ID, func(t *testing.T) {
			req := v.Input.Req
			attrs := authorizer.AttributesRecord{
				ResourceRequest: req.IsResource,
				Verb:            req.Verb.toString(t),
				APIGroup:        req.APIGroup.toString(t),
				Resource:        req.Resource.toString(t),
				Subresource:     req.Subresource.toString(t),
				Name:            req.Name.toString(t),
				Path:            req.Path.toString(t),
			}
			rule := rbacv1.PolicyRule{
				Verbs:           groundsToStrings(t, v.Input.Rule.Verbs),
				APIGroups:       groundsToStrings(t, v.Input.Rule.APIGroups),
				Resources:       groundsToStrings(t, v.Input.Rule.Resources),
				ResourceNames:   groundsToStrings(t, v.Input.Rule.ResourceNames),
				NonResourceURLs: groundsToStrings(t, v.Input.Rule.NonResourceURLs),
			}
			if got := RuleAllows(attrs, &rule); got != v.Expected {
				t.Errorf("RuleAllows(%+v, %s): code returned %v, model expects %v", attrs, rule.String(), got, v.Expected)
			}
		})
	}
}
