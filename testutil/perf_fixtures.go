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
	// ShortestPathScaleDataset identifies the generated shortest-path fixture.
	ShortestPathScaleDataset = "generated_shortest_paths"

	// FixedSuffixExpansionScaleDataset identifies the generated fixed-suffix
	// expansion fixture.
	FixedSuffixExpansionScaleDataset = "generated_fixed_suffix_expansion"
)

// ShortestPathScaleConfig controls the depth and dead-end fanout of the
// generated shortest-path fixture.
type ShortestPathScaleConfig struct {
	// Depth sets the length of the fixture's unique linear route.
	Depth int

	// Fanout sets the number of dead ends attached to the route's start.
	Fanout int
}

// NewShortestPathScaleFixture builds deterministic linear, diamond, dead-end,
// cycle, parallel-edge, self-loop, wrong-direction, and disconnected shapes
// around a bound endpoint pair. Fanout controls parallel dead ends without
// changing the unique linear route's requested depth.
func NewShortestPathScaleFixture(config ShortestPathScaleConfig) *opengraph.Graph {
	depth := max(config.Depth, 1)
	fanout := max(config.Fanout, 1)
	fixture := &opengraph.Graph{}

	fixture.Nodes = append(fixture.Nodes,
		opengraph.Node{
			ID:         "sp-start",
			Kinds:      []string{"ShortestNode"},
			Properties: map[string]any{"role": "start"},
		},
		opengraph.Node{
			ID:         "sp-end",
			Kinds:      []string{"ShortestNode"},
			Properties: map[string]any{"role": "end"},
		},
		opengraph.Node{
			ID:    "sp-disconnected",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-wrong-direction",
			Kinds: []string{"ShortestNode"},
		},
	)

	previous := "sp-start"
	for level := 1; level < depth; level++ {
		next := fmt.Sprintf("sp-linear-%02d", level)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    next,
			Kinds: []string{"ShortestNode"},
		})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID: previous,
			EndID:   next,
			Kind:    "Traverse",
		})
		previous = next
	}
	fixture.Edges = append(fixture.Edges, opengraph.Edge{
		StartID: previous,
		EndID:   "sp-end",
		Kind:    "Traverse",
	})
	fixture.Edges = append(fixture.Edges, opengraph.Edge{
		StartID: "sp-end",
		EndID:   "sp-wrong-direction",
		Kind:    "Traverse",
	})

	for idx := range fanout {
		deadEnd := fmt.Sprintf("sp-dead-%04d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    deadEnd,
			Kinds: []string{"ShortestNode"},
		})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID: "sp-start",
			EndID:   deadEnd,
			Kind:    "Traverse",
		})
	}

	fixture.Nodes = append(fixture.Nodes,
		opengraph.Node{
			ID:    "sp-diamond-left",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-diamond-right",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-diamond-end",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-cycle-a",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-cycle-b",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-parallel-end",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-self-loop",
			Kinds: []string{"ShortestNode"},
		},
		opengraph.Node{
			ID:    "sp-self-loop-exit",
			Kinds: []string{"ShortestNode"},
		},
	)
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{
			StartID: "sp-start",
			EndID:   "sp-diamond-left",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID: "sp-start",
			EndID:   "sp-diamond-right",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID: "sp-diamond-left",
			EndID:   "sp-diamond-end",
			Kind:    "TypedTraverse",
		},
		opengraph.Edge{
			StartID: "sp-diamond-right",
			EndID:   "sp-diamond-end",
			Kind:    "TypedTraverse",
		},
		opengraph.Edge{
			StartID: "sp-start",
			EndID:   "sp-cycle-a",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID: "sp-cycle-a",
			EndID:   "sp-cycle-b",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID: "sp-cycle-b",
			EndID:   "sp-cycle-a",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID:    "sp-start",
			EndID:      "sp-parallel-end",
			Kind:       "Traverse",
			Properties: map[string]any{"logical_key": "sp-parallel-0"},
		},
		opengraph.Edge{
			StartID:    "sp-start",
			EndID:      "sp-parallel-end",
			Kind:       "TypedTraverse",
			Properties: map[string]any{"logical_key": "sp-parallel-1"},
		},
		opengraph.Edge{
			StartID: "sp-start",
			EndID:   "sp-self-loop",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID: "sp-self-loop",
			EndID:   "sp-self-loop",
			Kind:    "Traverse",
		},
		opengraph.Edge{
			StartID: "sp-self-loop",
			EndID:   "sp-self-loop-exit",
			Kind:    "Traverse",
		},
	)

	return fixture
}

// FixedSuffixExpansionScaleConfig controls expansion work, suffix density,
// decoys, and payload size in a fixed-suffix fixture.
type FixedSuffixExpansionScaleConfig struct {
	// ExpansionDepth sets the number of Expand hops in each branch.
	ExpansionDepth int

	// Fanout sets the number of expansion branches rooted at the fixture root.
	Fanout int

	// ValidSuffixEvery attaches a suffix to every nth legacy branch.
	ValidSuffixEvery int

	// PropertyPayloadSize sets the length of synthetic payload properties.
	PropertyPayloadSize int

	// ExactReachableSuffixSources decouples reachable suffix density from the
	// legacy modulus control. Nil preserves ValidSuffixEvery behavior; zero is
	// an exact zero and is therefore materially different from nil.
	ExactReachableSuffixSources *int

	// ReachableSuffixDepths restricts suffix attachment to the listed expansion
	// depths when nonempty.
	ReachableSuffixDepths []int

	// DisconnectedSuffixSources sets the number of suffix sources unreachable
	// from any expansion root.
	DisconnectedSuffixSources int

	// ReverseFanIn sets the number of decoy Expand edges entering a productive
	// branch boundary.
	ReverseFanIn int

	// SuffixPathsPerBoundary sets the number of distinct suffix paths attached
	// to each selected boundary.
	SuffixPathsPerBoundary int

	// RootMatchCount sets the number of roots matching the fixture root key.
	RootMatchCount int

	// RootHasZeroDepthSuffix controls whether the primary root has a suffix;
	// nil preserves the enabled default.
	RootHasZeroDepthSuffix *bool
}

// NewFixedSuffixExpansionScaleFixture builds a deterministic expansion fanout
// feeding a shared fixed suffix. It also emits independent wrong-kind,
// wrong-direction, wrong-endpoint-kind, and disconnected suffix decoys.
func NewFixedSuffixExpansionScaleFixture(config FixedSuffixExpansionScaleConfig) *opengraph.Graph {
	if config.ExactReachableSuffixSources == nil && len(config.ReachableSuffixDepths) == 0 && config.DisconnectedSuffixSources == 0 && config.ReverseFanIn == 0 && config.SuffixPathsPerBoundary == 0 && config.RootMatchCount == 0 && config.RootHasZeroDepthSuffix == nil {
		return newLegacyFixedSuffixExpansionScaleFixture(config)
	}
	depth := max(config.ExpansionDepth, 0)
	fanout := max(config.Fanout, 1)
	validEvery := max(config.ValidSuffixEvery, 1)
	reachableSources := -1
	if config.ExactReachableSuffixSources != nil {
		reachableSources = min(max(*config.ExactReachableSuffixSources, 0), fanout)
	}
	suffixPaths := max(config.SuffixPathsPerBoundary, 1)
	rootCount := max(config.RootMatchCount, 1)
	rootHasSuffix := true
	if config.RootHasZeroDepthSuffix != nil {
		rootHasSuffix = *config.RootHasZeroDepthSuffix
	}
	payload := strings.Repeat("x", max(config.PropertyPayloadSize, 0))

	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{
				ID:    "fse-terminal",
				Kinds: []string{"SuffixTerminal"},
			},
			{
				ID:    "fse-wrong-endpoint",
				Kinds: []string{"ExpansionNode"},
			},
		},
	}
	for rootIdx := range rootCount {
		rootID := "fse-root"
		if rootIdx > 0 {
			rootID = fmt.Sprintf("fse-root-%02d", rootIdx)
		}
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         rootID,
			Kinds:      []string{"ExpansionRoot"},
			Properties: map[string]any{"root_key": "generated-fse-root", "payload": payload},
		})
	}
	addSuffix := func(source, key string) {
		for pathIdx := range suffixPaths {
			headID := fmt.Sprintf("fse-head-%s-%02d", key, pathIdx)
			middleID := fmt.Sprintf("fse-middle-%s-%02d", key, pathIdx)
			fixture.Nodes = append(fixture.Nodes,
				opengraph.Node{
					ID:         headID,
					Kinds:      []string{"SuffixHead"},
					Properties: map[string]any{"payload": payload},
				},
				opengraph.Node{
					ID:    middleID,
					Kinds: []string{"SuffixMiddle"},
				},
			)
			fixture.Edges = append(fixture.Edges,
				opengraph.Edge{
					StartID:    source,
					EndID:      headID,
					Kind:       "EnterSuffix",
					Properties: map[string]any{"payload": payload, "logical_key": key + ":enter"},
				},
				opengraph.Edge{
					StartID:    headID,
					EndID:      middleID,
					Kind:       "ContinueSuffix",
					Properties: map[string]any{"logical_key": key + ":continue"},
				},
				opengraph.Edge{
					StartID:    middleID,
					EndID:      "fse-terminal",
					Kind:       "CompleteSuffix",
					Properties: map[string]any{"logical_key": key + ":complete"},
				},
			)
		}
	}
	if rootHasSuffix {
		addSuffix("fse-root", "root")
	}

	productiveBoundary := "fse-root"
	if depth > 0 {
		for branch := range fanout {
			previous := "fse-root"
			for level := 1; level <= depth; level++ {
				next := fmt.Sprintf("fse-branch-%04d-level-%02d", branch, level)
				fixture.Nodes = append(fixture.Nodes, opengraph.Node{
					ID:         next,
					Kinds:      []string{"ExpansionNode"},
					Properties: map[string]any{"payload": payload},
				})
				fixture.Edges = append(fixture.Edges, opengraph.Edge{
					StartID:    previous,
					EndID:      next,
					Kind:       "Expand",
					Properties: map[string]any{"logical_key": fmt.Sprintf("branch-%04d-level-%02d", branch, level)},
				})
				previous = next
			}
			reachable := branch%validEvery == 0
			if reachableSources >= 0 {
				reachable = branch < reachableSources
			}
			if reachable && (len(config.ReachableSuffixDepths) == 0 || containsInt(config.ReachableSuffixDepths, depth)) {
				addSuffix(previous, fmt.Sprintf("branch-%04d-depth-%02d", branch, depth))
				if branch == 0 {
					productiveBoundary = previous
				}
			}
		}
	}
	for idx := range max(config.DisconnectedSuffixSources, 0) {
		source := fmt.Sprintf("fse-disconnected-%05d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    source,
			Kinds: []string{"ExpansionNode"},
		})
		addSuffix(source, fmt.Sprintf("disconnected-%05d", idx))
	}
	for idx := range max(config.ReverseFanIn, 0) {
		source := fmt.Sprintf("fse-fanin-%05d", idx)
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:    source,
			Kinds: []string{"ExpansionNode"},
		})
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    source,
			EndID:      productiveBoundary,
			Kind:       "Expand",
			Properties: map[string]any{"logical_key": fmt.Sprintf("fanin-%05d", idx)},
		})
	}

	decoySource := "fse-root"
	if depth > 0 {
		decoySource = "fse-branch-0000-level-01"
	}
	fixture.Nodes = append(fixture.Nodes,
		opengraph.Node{
			ID:    "fse-decoy-head",
			Kinds: []string{"SuffixHead"},
		},
		opengraph.Node{
			ID:    "fse-decoy-middle",
			Kinds: []string{"SuffixMiddle"},
		},
	)
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{
			StartID: decoySource,
			EndID:   "fse-decoy-head",
			Kind:    "WrongEnterSuffix",
		},
		opengraph.Edge{
			StartID: "fse-decoy-head",
			EndID:   decoySource,
			Kind:    "EnterSuffix",
		},
		opengraph.Edge{
			StartID: decoySource,
			EndID:   "fse-wrong-endpoint",
			Kind:    "EnterSuffix",
		},
	)

	return fixture
}

// newLegacyFixedSuffixExpansionScaleFixture builds the original shared-suffix
// topology used when no independent population controls are configured.
func newLegacyFixedSuffixExpansionScaleFixture(config FixedSuffixExpansionScaleConfig) *opengraph.Graph {
	depth := max(config.ExpansionDepth, 0)
	fanout := max(config.Fanout, 1)
	validEvery := max(config.ValidSuffixEvery, 1)
	payload := strings.Repeat("x", max(config.PropertyPayloadSize, 0))
	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{
				ID:         "fse-root",
				Kinds:      []string{"ExpansionRoot"},
				Properties: map[string]any{"root_key": "generated-fse-root", "payload": payload},
			},
			{
				ID:         "fse-head",
				Kinds:      []string{"SuffixHead"},
				Properties: map[string]any{"payload": payload},
			},
			{
				ID:    "fse-middle",
				Kinds: []string{"SuffixMiddle"},
			},
			{
				ID:    "fse-terminal",
				Kinds: []string{"SuffixTerminal"},
			},
			{
				ID:    "fse-wrong-endpoint",
				Kinds: []string{"ExpansionNode"},
			},
			{
				ID:    "fse-disconnected",
				Kinds: []string{"ExpansionNode"},
			},
		},
	}
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{
			StartID:    "fse-root",
			EndID:      "fse-head",
			Kind:       "EnterSuffix",
			Properties: map[string]any{"payload": payload},
		},
		opengraph.Edge{
			StartID: "fse-head",
			EndID:   "fse-middle",
			Kind:    "ContinueSuffix",
		},
		opengraph.Edge{
			StartID: "fse-middle",
			EndID:   "fse-terminal",
			Kind:    "CompleteSuffix",
		},
	)
	if depth > 0 {
		for branch := range fanout {
			previous := "fse-root"
			for level := 1; level <= depth; level++ {
				next := fmt.Sprintf("fse-branch-%04d-level-%02d", branch, level)
				fixture.Nodes = append(fixture.Nodes, opengraph.Node{
					ID:         next,
					Kinds:      []string{"ExpansionNode"},
					Properties: map[string]any{"payload": payload},
				})
				fixture.Edges = append(fixture.Edges, opengraph.Edge{
					StartID: previous,
					EndID:   next,
					Kind:    "Expand",
				})
				previous = next
			}
			if branch%validEvery == 0 {
				fixture.Edges = append(fixture.Edges, opengraph.Edge{
					StartID: previous,
					EndID:   "fse-head",
					Kind:    "EnterSuffix",
				})
			}
		}
	}
	decoySource := "fse-root"
	if depth > 0 {
		decoySource = "fse-branch-0000-level-01"
	}
	fixture.Edges = append(fixture.Edges,
		opengraph.Edge{
			StartID: decoySource,
			EndID:   "fse-head",
			Kind:    "WrongEnterSuffix",
		},
		opengraph.Edge{
			StartID: "fse-head",
			EndID:   decoySource,
			Kind:    "EnterSuffix",
		},
		opengraph.Edge{
			StartID: decoySource,
			EndID:   "fse-wrong-endpoint",
			Kind:    "EnterSuffix",
		},
		opengraph.Edge{
			StartID: "fse-disconnected",
			EndID:   "fse-head",
			Kind:    "EnterSuffix",
		},
	)
	return fixture
}

// containsInt reports whether target occurs in values.
func containsInt(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
