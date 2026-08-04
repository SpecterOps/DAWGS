// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package testutil

import (
	"fmt"

	"github.com/specterops/dawgs/opengraph"
)

const ReconciliationScaleDataset = "generated_reconciliation"

// GeneratedNodeListParam resolves optional fixture IDs followed by a
// deterministic prefix/count sequence. It keeps high-cardinality database-ID
// parameters out of handwritten JSON.
type GeneratedNodeListParam struct {
	Prefix  string   `json:"prefix"`
	Count   int      `json:"count"`
	Include []string `json:"include,omitempty"`
}

// FixtureNames returns stable, zero-padded fixture IDs.
func FixtureNames(prefix string, count int) []string {
	if count < 0 {
		count = 0
	}

	width := len(fmt.Sprintf("%d", max(count-1, 0)))
	if width < 2 {
		width = 2
	}

	values := make([]string, count)
	for idx := range count {
		values[idx] = fmt.Sprintf("%s-%0*d", prefix, width, idx)
	}
	return values
}

// NewReconciliationScaleFixture returns the deterministic graphbench fixture
// for the ingestion reconciliation forms. fanout controls the degree of the
// REC-08 detach-delete target.
func NewReconciliationScaleFixture(fanout int) *opengraph.Graph {
	if fanout < 1 {
		fanout = 128
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "source", Kinds: []string{"Source"}, Properties: map[string]any{"objectid": "source"}},
			{ID: "sink", Kinds: []string{"Destination"}, Properties: map[string]any{"objectid": "sink"}},
			{ID: "inbound-target", Kinds: []string{"ADEntity", "Group"}, Properties: map[string]any{"objectid": "rec-in"}},
			{ID: "outbound-target", Kinds: []string{"ADEntity", "Computer"}, Properties: map[string]any{"objectid": "rec-out"}},
			{ID: "list-target", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "rec-list"}},
			{ID: "template", Kinds: []string{"CertTemplate"}, Properties: map[string]any{"objectid": "template"}},
			{ID: "agent", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "agent"}},
			{ID: "delete-target", Kinds: []string{"ADEntity", "Group"}, Properties: map[string]any{"objectid": "delete-target"}},
			{ID: "survivor", Kinds: []string{"ADEntity", "User"}, Properties: map[string]any{"objectid": "survivor"}},
		},
		Edges: []opengraph.Edge{
			{StartID: "source", EndID: "inbound-target", Kind: "RecKind01", Properties: map[string]any{"marker": "rec-01-a"}},
			{StartID: "source", EndID: "inbound-target", Kind: "RecKind30", Properties: map[string]any{"marker": "rec-01-b"}},
			{StartID: "outbound-target", EndID: "sink", Kind: "RecKind01", Properties: map[string]any{"marker": "rec-02-a"}},
			{StartID: "outbound-target", EndID: "sink", Kind: "RecKind30", Properties: map[string]any{"marker": "rec-02-b"}},
			{StartID: "source", EndID: "list-target", Kind: "ADReconcile", Properties: map[string]any{"marker": "rec-04-a"}},
			{StartID: "source", EndID: "list-target", Kind: "ADReconcile", Properties: map[string]any{"marker": "rec-04-b"}},
			{StartID: "agent", EndID: "template", Kind: "DelegatedEnrollmentAgent", Properties: map[string]any{"marker": "rec-06-a"}},
			{StartID: "agent", EndID: "template", Kind: "DelegatedEnrollmentAgent", Properties: map[string]any{"marker": "rec-06-b"}},
			{StartID: "source", EndID: "survivor", Kind: "Survivor", Properties: map[string]any{"marker": "survivor"}},
		},
	}

	for _, templateID := range FixtureNames("scale-template", 2_000) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         templateID,
			Kinds:      []string{"CertTemplate"},
			Properties: map[string]any{"objectid": templateID},
		})
	}

	// Ensure every relationship kind in the 30-kind disjunction is registered,
	// while anchoring each decoy away from the REC-01/REC-02 target endpoints.
	for idx := 2; idx < 30; idx++ {
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    "source",
			EndID:      "survivor",
			Kind:       fmt.Sprintf("RecKind%02d", idx),
			Properties: map[string]any{"marker": fmt.Sprintf("kind-decoy-%02d", idx)},
		})
	}

	for idx := range fanout {
		neighborID := fmt.Sprintf("detach-neighbor-%04d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         neighborID,
			Kinds:      []string{"ADEntity"},
			Properties: map[string]any{"objectid": neighborID},
		})

		startID, endID := "delete-target", neighborID
		if idx%2 == 0 {
			startID, endID = neighborID, "delete-target"
		}
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    startID,
			EndID:      endID,
			Kind:       "Incident",
			Properties: map[string]any{"marker": fmt.Sprintf("incident-%04d", idx)},
		})
	}

	fixture.Edges = append(fixture.Edges, opengraph.Edge{
		StartID:    "delete-target",
		EndID:      "delete-target",
		Kind:       "Incident",
		Properties: map[string]any{"marker": "incident-self"},
	})
	return fixture
}
