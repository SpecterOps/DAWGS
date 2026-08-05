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
	"strings"

	"github.com/specterops/dawgs/opengraph"
)

const (
	ShortestPathScaleDataset = "generated_shortest_paths"
	ADCSScaleDataset         = "generated_adcs"
)

type ShortestPathScaleConfig struct {
	Depth  int
	Fanout int
}

// NewShortestPathScaleFixture builds deterministic linear, diamond, dead-end,
// cycle, wrong-direction, and disconnected shapes around a bound endpoint
// pair. Fanout controls parallel dead ends without changing the unique linear
// route's requested depth.
func NewShortestPathScaleFixture(config ShortestPathScaleConfig) *opengraph.Graph {
	depth := max(config.Depth, 1)
	fanout := max(config.Fanout, 1)
	fixture := &opengraph.Graph{}

	fixture.Nodes = append(fixture.Nodes,
		opengraph.Node{ID: "sp-start", Kinds: []string{"ShortestNode"}, Properties: map[string]any{"role": "start"}},
		opengraph.Node{ID: "sp-end", Kinds: []string{"ShortestNode"}, Properties: map[string]any{"role": "end"}},
		opengraph.Node{ID: "sp-disconnected", Kinds: []string{"ShortestNode"}},
		opengraph.Node{ID: "sp-wrong-direction", Kinds: []string{"ShortestNode"}},
	)

	previous := "sp-start"
	for level := 1; level < depth; level++ {
		next := fmt.Sprintf("sp-linear-%02d", level)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: next, Kinds: []string{"ShortestNode"}})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: previous, EndID: next, Kind: "Traverse"})
		previous = next
	}
	fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: previous, EndID: "sp-end", Kind: "Traverse"})
	fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: "sp-end", EndID: "sp-wrong-direction", Kind: "Traverse"})

	for idx := range fanout {
		deadEnd := fmt.Sprintf("sp-dead-%04d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: deadEnd, Kinds: []string{"ShortestNode"}})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: "sp-start", EndID: deadEnd, Kind: "Traverse"})
	}

	fixture.Nodes = append(fixture.Nodes,
		opengraph.Node{ID: "sp-diamond-left", Kinds: []string{"ShortestNode"}},
		opengraph.Node{ID: "sp-diamond-right", Kinds: []string{"ShortestNode"}},
		opengraph.Node{ID: "sp-diamond-end", Kinds: []string{"ShortestNode"}},
		opengraph.Node{ID: "sp-cycle-a", Kinds: []string{"ShortestNode"}},
		opengraph.Node{ID: "sp-cycle-b", Kinds: []string{"ShortestNode"}},
	)
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{StartID: "sp-start", EndID: "sp-diamond-left", Kind: "Traverse"},
		opengraph.Edge{StartID: "sp-start", EndID: "sp-diamond-right", Kind: "Traverse"},
		opengraph.Edge{StartID: "sp-diamond-left", EndID: "sp-diamond-end", Kind: "TypedTraverse"},
		opengraph.Edge{StartID: "sp-diamond-right", EndID: "sp-diamond-end", Kind: "TypedTraverse"},
		opengraph.Edge{StartID: "sp-start", EndID: "sp-cycle-a", Kind: "Traverse"},
		opengraph.Edge{StartID: "sp-cycle-a", EndID: "sp-cycle-b", Kind: "Traverse"},
		opengraph.Edge{StartID: "sp-cycle-b", EndID: "sp-cycle-a", Kind: "Traverse"},
	)

	return fixture
}

type ADCSScaleConfig struct {
	MemberOfDepth       int
	Fanout              int
	ValidSuffixEvery    int
	PropertyPayloadSize int
}

// NewADCSScaleFixture builds a deterministic MemberOf fanout feeding a shared
// ADCS suffix. It also emits independent wrong-kind, wrong-direction,
// wrong-endpoint-kind, and disconnected suffix decoys.
func NewADCSScaleFixture(config ADCSScaleConfig) *opengraph.Graph {
	depth := max(config.MemberOfDepth, 0)
	fanout := max(config.Fanout, 1)
	validEvery := max(config.ValidSuffixEvery, 1)
	payload := strings.Repeat("x", max(config.PropertyPayloadSize, 0))

	fixture := &opengraph.Graph{Nodes: []opengraph.Node{
		{ID: "adcs-root", Kinds: []string{"Group"}, Properties: map[string]any{"objectid": "generated-adcs-root", "payload": payload}},
		{ID: "adcs-ca", Kinds: []string{"EnterpriseCA"}, Properties: map[string]any{"payload": payload}},
		{ID: "adcs-store", Kinds: []string{"NTAuthStore"}},
		{ID: "adcs-domain", Kinds: []string{"Domain"}},
		{ID: "adcs-wrong-endpoint", Kinds: []string{"Group"}},
		{ID: "adcs-disconnected", Kinds: []string{"Group"}},
	}}
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{StartID: "adcs-root", EndID: "adcs-ca", Kind: "Enroll", Properties: map[string]any{"payload": payload}},
		opengraph.Edge{StartID: "adcs-ca", EndID: "adcs-store", Kind: "TrustedForNTAuth"},
		opengraph.Edge{StartID: "adcs-store", EndID: "adcs-domain", Kind: "NTAuthStoreFor"},
	)

	if depth > 0 {
		for branch := range fanout {
			previous := "adcs-root"
			for level := 1; level <= depth; level++ {
				next := fmt.Sprintf("adcs-branch-%04d-level-%02d", branch, level)
				fixture.Nodes = append(fixture.Nodes, opengraph.Node{ID: next, Kinds: []string{"Group"}, Properties: map[string]any{"payload": payload}})
				fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: previous, EndID: next, Kind: "MemberOf"})
				previous = next
			}
			if branch%validEvery == 0 {
				fixture.Edges = append(fixture.Edges, opengraph.Edge{StartID: previous, EndID: "adcs-ca", Kind: "Enroll"})
			}
		}
	}

	decoySource := "adcs-root"
	if depth > 0 {
		decoySource = "adcs-branch-0000-level-01"
	}
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{StartID: decoySource, EndID: "adcs-ca", Kind: "WrongEnrollKind"},
		opengraph.Edge{StartID: "adcs-ca", EndID: decoySource, Kind: "Enroll"},
		opengraph.Edge{StartID: decoySource, EndID: "adcs-wrong-endpoint", Kind: "Enroll"},
		opengraph.Edge{StartID: "adcs-disconnected", EndID: "adcs-ca", Kind: "Enroll"},
	)

	return fixture
}
