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

package integration

import (
	"fmt"

	"github.com/specterops/dawgs/opengraph"
)

// defaultRegressionFanout is the relationship fanout used when a regression
// fixture does not request an explicit size.
const defaultRegressionFanout = 32

// FixtureNames returns deterministic fixture identifiers without committing
// large handwritten lists to the corpus.
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

// FixtureKinds returns deterministic synthetic kind names for list-cardinality
// tests.
func FixtureKinds(count int) []string {
	if count < 0 {
		count = 0
	}

	kinds := make([]string, count)
	for idx := range count {
		kinds[idx] = fmt.Sprintf("RegressionKind%02d", idx+1)
	}

	return kinds
}

// NewReconciliationFixture builds the reusable reconciliation fixture. It includes
// typed and multi-kind endpoints, duplicate relationship kinds, missing and
// explicit-null properties, timestamps, both directions, and a deterministic
// high-degree anchor. A non-positive fanout selects a small production-like
// default.
func NewReconciliationFixture(fanout int) *opengraph.Graph {
	if fanout <= 0 {
		fanout = defaultRegressionFanout
	}

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{
				ID:         "anchor",
				Kinds:      []string{"ADEntity", "Computer", "Entity"},
				Properties: map[string]any{"objectid": "anchor-id", "lastcollected": "2026-01-02T00:00:00Z", "name": "anchor"},
			},
			{
				ID:         "typed-end",
				Kinds:      []string{"ADEntity", "Group", "Entity"},
				Properties: map[string]any{"objectid": "typed-end-id", "lastcollected": "2026-01-03T00:00:00Z", "name": "typed-end"},
			},
			{
				ID:         "missing-lastseen",
				Kinds:      []string{"ADEntity", "Entity"},
				Properties: map[string]any{"objectid": "missing-id"},
			},
			{
				ID:         "null-lastseen",
				Kinds:      []string{"ADEntity", "Entity"},
				Properties: map[string]any{"objectid": "null-id", "lastseen": nil},
			},
		},
		Edges: []opengraph.Edge{
			{
				StartID:    "anchor",
				EndID:      "typed-end",
				Kind:       "MemberOf",
				Properties: map[string]any{"lastseen": "2026-01-01T00:00:00Z", "isprimarygroup": false, "marker": "duplicate-a"},
			},
			{
				StartID:    "anchor",
				EndID:      "typed-end",
				Kind:       "MemberOf",
				Properties: map[string]any{"lastseen": "2026-01-04T00:00:00Z", "isprimarygroup": true, "marker": "duplicate-b"},
			},
			{
				StartID:    "typed-end",
				EndID:      "anchor",
				Kind:       "MemberOf",
				Properties: map[string]any{"marker": "reverse"},
			},
			{
				StartID:    "anchor",
				EndID:      "missing-lastseen",
				Kind:       "HasSession",
				Properties: map[string]any{"marker": "missing-lastseen"},
			},
			{
				StartID:    "anchor",
				EndID:      "null-lastseen",
				Kind:       "HasSession",
				Properties: map[string]any{"lastseen": nil, "marker": "null-lastseen"},
			},
		},
	}

	for _, fixtureID := range FixtureNames("fanout", fanout) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         fixtureID,
			Kinds:      []string{"ADEntity", "Entity", "User"},
			Properties: map[string]any{"objectid": fixtureID, "name": fixtureID},
		})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    "anchor",
			EndID:      fixtureID,
			Kind:       "FanoutEdge",
			Properties: map[string]any{"lastseen": "2026-01-01T00:00:00Z"},
		})
	}

	return fixture
}
