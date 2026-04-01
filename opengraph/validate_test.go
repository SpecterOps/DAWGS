// Copyright 2025 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package opengraph

import (
	"strings"
	"testing"
)

func TestValidate_EmptyGraph(t *testing.T) {
	doc := Document{Graph: Graph{}}
	if err := Validate(doc); err != nil {
		t.Fatalf("empty graph should be valid, got: %v", err)
	}
}

func TestValidate_ValidGraph(t *testing.T) {
	doc := Document{
		Graph: Graph{
			Nodes: []Node{
				{ID: "a", Kinds: []string{"Person"}},
				{ID: "b", Kinds: []string{"Person"}},
			},
			Edges: []Edge{
				{StartID: "a", EndID: "b", Kind: "KNOWS"},
			},
		},
	}
	if err := Validate(doc); err != nil {
		t.Fatalf("valid graph should pass, got: %v", err)
	}
}

func TestValidate_DuplicateNodeID(t *testing.T) {
	doc := Document{
		Graph: Graph{
			Nodes: []Node{
				{ID: "a"},
				{ID: "a"},
			},
		},
	}
	err := Validate(doc)
	if err == nil {
		t.Fatal("expected error for duplicate node ID")
	}
	if !strings.Contains(err.Error(), "duplicate node ID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_EmptyNodeID(t *testing.T) {
	doc := Document{
		Graph: Graph{Nodes: []Node{{ID: ""}}},
	}
	err := Validate(doc)
	if err == nil {
		t.Fatal("expected error for empty node ID")
	}
	if !strings.Contains(err.Error(), "empty ID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_EmptyEdgeKind(t *testing.T) {
	doc := Document{
		Graph: Graph{
			Nodes: []Node{{ID: "a"}, {ID: "b"}},
			Edges: []Edge{{StartID: "a", EndID: "b", Kind: ""}},
		},
	}
	err := Validate(doc)
	if err == nil {
		t.Fatal("expected error for empty edge kind")
	}
	if !strings.Contains(err.Error(), "empty kind") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_EmptyEdgeStartID(t *testing.T) {
	doc := Document{
		Graph: Graph{
			Nodes: []Node{{ID: "a"}},
			Edges: []Edge{{StartID: "", EndID: "a", Kind: "REL"}},
		},
	}
	err := Validate(doc)
	if err == nil {
		t.Fatal("expected error for empty start_id")
	}
	if !strings.Contains(err.Error(), "empty start_id") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_UnknownEdgeReference(t *testing.T) {
	doc := Document{
		Graph: Graph{
			Nodes: []Node{{ID: "a"}},
			Edges: []Edge{{StartID: "a", EndID: "missing", Kind: "REL"}},
		},
	}
	err := Validate(doc)
	if err == nil {
		t.Fatal("expected error for unknown node reference")
	}
	if !strings.Contains(err.Error(), "unknown end node") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_MultipleEdgeErrors(t *testing.T) {
	doc := Document{
		Graph: Graph{
			Nodes: []Node{{ID: "a"}},
			Edges: []Edge{
				{StartID: "", EndID: "", Kind: ""},
				{StartID: "a", EndID: "nope", Kind: "REL"},
			},
		},
	}
	err := Validate(doc)
	if err == nil {
		t.Fatal("expected errors")
	}
	// errors.Join produces newline-separated errors
	errStr := err.Error()
	if !strings.Contains(errStr, "empty kind") {
		t.Fatalf("expected empty kind error, got: %v", err)
	}
	if !strings.Contains(errStr, "unknown end node") {
		t.Fatalf("expected unknown node error, got: %v", err)
	}
}
