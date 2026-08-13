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
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

// TestShortestPathScaleFixtureIsDeterministicAndCardinalityExact verifies the
// legacy shortest-path fixture is stable and emits the expected topology.
func TestShortestPathScaleFixtureIsDeterministicAndCardinalityExact(t *testing.T) {
	config := ShortestPathScaleConfig{
		Depth:  16,
		Fanout: 10,
	}
	first := NewShortestPathScaleFixture(config)
	second := NewShortestPathScaleFixture(config)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
	require.Len(t, first.Nodes, 4+(config.Depth-1)+config.Fanout+8)
	require.Len(t, first.Edges, config.Depth+1+config.Fanout+12)

	var parallel, selfLoops int
	for _, edge := range first.Edges {
		if edge.StartID == "sp-start" && edge.EndID == "sp-parallel-end" {
			parallel++
		}
		if edge.StartID == "sp-self-loop" && edge.EndID == "sp-self-loop" {
			selfLoops++
		}
	}
	require.Equal(t, 2, parallel)
	require.Equal(t, 1, selfLoops)
}

// TestEndpointSeededExpansionFixtureIsDeterministicAndSeparatesWorkClasses verifies productive, nonmatching, and ineligible lanes remain distinct.
func TestEndpointSeededExpansionFixtureIsDeterministicAndSeparatesWorkClasses(t *testing.T) {
	config := EndpointSeededExpansionScaleConfig{
		Depth:                   3,
		MatchingEndpoints:       2,
		OtherEndpoints:          1,
		MatchingEligibleLanes:   2,
		OtherEligibleLanes:      1,
		MatchingIneligibleLanes: 1,
		ParallelEdges:           1,
		AddCycle:                true,
		PropertyPayloadSize:     8,
	}
	first := NewEndpointSeededExpansionScaleFixture(config)
	second := NewEndpointSeededExpansionScaleFixture(config)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
	require.NotEmpty(t, first.Nodes)
	require.NotEmpty(t, first.Edges)
	require.NoError(t, ValidateEndpointSeededExpansionScaleConfig(config))
	require.Error(t, ValidateEndpointSeededExpansionScaleConfig(EndpointSeededExpansionScaleConfig{}))
	config.ParallelEdges = 2
	require.ErrorContains(t, ValidateEndpointSeededExpansionScaleConfig(config), "uniquely keys edges")
}

// TestShortestPathScaleV2FixtureIsDeterministicAndTopologyExact verifies the
// configurable fixture is stable and assigns unique logical edge keys.
func TestShortestPathScaleV2FixtureIsDeterministicAndTopologyExact(t *testing.T) {
	config := ShortestPathScaleV2Config{
		Depth:                    3,
		ForwardRootFanOut:        2,
		ReverseRootFanIn:         2,
		IntermediateFanOut:       1,
		IntermediateReverseFanIn: 4,
		FanInLevel:               2,
		ParallelKindCount:        3,
		ParallelTargetCount:      2,
		DiamondWidth:             2,
		DisconnectedWidth:        3,
		PropertyPayloadSize:      8,
		AddCycle:                 true,
		AddSelfLoop:              true,
	}
	first := NewShortestPathScaleV2Fixture(config)
	second := NewShortestPathScaleV2Fixture(config)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
	require.Len(t, first.Nodes, 32)
	require.Len(t, first.Edges, 33)

	logicalKeys := map[string]bool{}
	for _, edge := range first.Edges {
		key, ok := edge.Properties["logical_key"].(string)
		require.True(t, ok)
		require.NotEmpty(t, key)
		require.False(t, logicalKeys[key], key)
		logicalKeys[key] = true
	}
}

// TestShortestPathScaleV2ConfigurationRejectsImpossibleShapes verifies invalid
// dimensions and inconsistent fan-in controls are rejected.
func TestShortestPathScaleV2ConfigurationRejectsImpossibleShapes(t *testing.T) {
	for _, config := range []ShortestPathScaleV2Config{
		{
			Depth: -1,
		},
		{
			Depth: 65,
		},
		{
			Depth:      3,
			FanInLevel: 2,
		},
		{
			Depth:                    3,
			IntermediateReverseFanIn: 1,
			FanInLevel:               3,
		},
		{
			ParallelKindCount: 1,
		},
		{
			ParallelTargetCount: 1,
		},
	} {
		require.Error(t, ValidateShortestPathScaleV2Config(config))
	}
	require.NoError(t, ValidateShortestPathScaleV2Config(ShortestPathScaleV2Config{}))
}

// TestFixedSuffixExpansionScaleFixtureIsDeterministicAndCoversDecoys verifies
// the legacy suffix topology remains stable and includes wrong-kind edges.
func TestFixedSuffixExpansionScaleFixtureIsDeterministicAndCoversDecoys(t *testing.T) {
	config := FixedSuffixExpansionScaleConfig{
		ExpansionDepth:      4,
		Fanout:              10,
		ValidSuffixEvery:    2,
		PropertyPayloadSize: 32,
	}
	first := NewFixedSuffixExpansionScaleFixture(config)
	second := NewFixedSuffixExpansionScaleFixture(config)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
	require.Len(t, first.Nodes, 6+config.ExpansionDepth*config.Fanout)
	require.Len(t, first.Edges, 3+config.ExpansionDepth*config.Fanout+5+4)

	_, edgeKinds := first.Kinds()
	require.Contains(t, edgeKinds.Strings(), "WrongEnterSuffix")
}

// TestFixedSuffixExpansionScaleFixtureV2ControlsSuffixPopulationsIndependently verifies reachable, disconnected, and reverse-fan-in populations can vary
// without changing one another.
func TestFixedSuffixExpansionScaleFixtureV2ControlsSuffixPopulationsIndependently(t *testing.T) {
	reachable := 0
	zeroDepth := false
	fixture := NewFixedSuffixExpansionScaleFixture(FixedSuffixExpansionScaleConfig{
		ExpansionDepth:              2,
		Fanout:                      4,
		ExactReachableSuffixSources: &reachable,
		DisconnectedSuffixSources:   3,
		ReverseFanIn:                2,
		SuffixPathsPerBoundary:      2,
		RootMatchCount:              1,
		RootHasZeroDepthSuffix:      &zeroDepth,
	})

	var enterSuffix, expand int
	for _, edge := range fixture.Edges {
		switch edge.Kind {
		case "EnterSuffix":
			enterSuffix++
		case "Expand":
			expand++
		}
	}
	require.Equal(t, 8, enterSuffix)
	require.Equal(t, 10, expand)
	nodeIDs := make([]string, 0, len(fixture.Nodes))
	for _, node := range fixture.Nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	require.NotContains(t, nodeIDs, "fse-disconnected")
	require.Contains(t, nodeIDs, "fse-disconnected-00002")
}

// TestFixedSuffixExpansionScaleFixtureV3ControlsRootMultiplicity verifies that
// matching root rows vary without multiplying the primary root's fanout or
// suffix population.
func TestFixedSuffixExpansionScaleFixtureV3ControlsRootMultiplicity(t *testing.T) {
	reachable := 1
	zeroDepth := false
	config := FixedSuffixExpansionScaleConfig{
		ExpansionDepth:              2,
		Fanout:                      2,
		ExactReachableSuffixSources: &reachable,
		SuffixPathsPerBoundary:      1,
		RootMatchCount:              3,
		RootHasZeroDepthSuffix:      &zeroDepth,
	}
	require.NoError(t, ValidateFixedSuffixExpansionScaleV3Config(config))

	fixture := NewFixedSuffixExpansionScaleFixture(config)
	matchingRoots := 0
	for _, node := range fixture.Nodes {
		if slices.Contains(node.Kinds, "ExpansionRoot") && node.Properties["root_key"] == "generated-fse-root" {
			matchingRoots++
		}
	}
	require.Equal(t, 3, matchingRoots)

	rootExpandEdges := 0
	for _, edge := range fixture.Edges {
		if edge.Kind == "Expand" && edge.StartID == "fse-root" {
			rootExpandEdges++
		}
	}
	require.Equal(t, 2, rootExpandEdges)
}

// TestFixedSuffixExpansionScaleFixtureV3ProductiveBoundaryControls verifies
// all cycle/self-loop combinations and the stable, relationship-distinct
// topology emitted for each enabled control.
func TestFixedSuffixExpansionScaleFixtureV3ProductiveBoundaryControls(t *testing.T) {
	for _, testCase := range []struct {
		// name retains the name while anonymous record is assembled or evaluated.
		name string
		// cycle indicates whether cycle applies.
		cycle bool
		// selfLoop indicates whether self loop applies.
		selfLoop bool
		// wantEdges retains the want edges while anonymous record is assembled or evaluated.
		wantEdges int
	}{
		{name: "neither"},
		{
			name:      "cycle",
			cycle:     true,
			wantEdges: 2,
		},
		{
			name:      "self-loop",
			selfLoop:  true,
			wantEdges: 1,
		},
		{
			name:      "both",
			cycle:     true,
			selfLoop:  true,
			wantEdges: 3,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			reachable := 0
			zeroDepth := true
			config := FixedSuffixExpansionScaleConfig{
				ExpansionDepth:                2,
				Fanout:                        1,
				ExactReachableSuffixSources:   &reachable,
				SuffixPathsPerBoundary:        1,
				RootMatchCount:                1,
				RootHasZeroDepthSuffix:        &zeroDepth,
				AddProductiveBoundaryCycle:    testCase.cycle,
				AddProductiveBoundarySelfLoop: testCase.selfLoop,
			}
			require.NoError(t, ValidateFixedSuffixExpansionScaleV3Config(config))

			fixture := NewFixedSuffixExpansionScaleFixture(config)
			controlEdges := map[string]opengraph.Edge{}
			for _, edge := range fixture.Edges {
				logicalKey, _ := edge.Properties["logical_key"].(string)
				if strings.HasPrefix(logicalKey, "productive-boundary-") {
					controlEdges[logicalKey] = edge
				}
			}
			require.Len(t, controlEdges, testCase.wantEdges)
			if testCase.cycle {
				require.Equal(t, "fse-productive-boundary-cycle", controlEdges["productive-boundary-cycle-enter"].EndID)
				require.Equal(t, "fse-root", controlEdges["productive-boundary-cycle-return"].EndID)
			}
			if testCase.selfLoop {
				selfLoop := controlEdges["productive-boundary-self-loop"]
				require.Equal(t, "fse-root", selfLoop.StartID)
				require.Equal(t, selfLoop.StartID, selfLoop.EndID)
			}
		})
	}
}

// TestFixedSuffixExpansionScaleV3ConfigurationRejectsUnproductiveControls
// verifies that topology and fan-in controls cannot be attached to a boundary
// with no generated suffix.
func TestFixedSuffixExpansionScaleV3ConfigurationRejectsUnproductiveControls(t *testing.T) {
	reachable := 0
	zeroDepth := false
	base := FixedSuffixExpansionScaleConfig{
		ExpansionDepth:              2,
		Fanout:                      1,
		ExactReachableSuffixSources: &reachable,
		SuffixPathsPerBoundary:      1,
		RootMatchCount:              1,
		RootHasZeroDepthSuffix:      &zeroDepth,
	}
	require.NoError(t, ValidateFixedSuffixExpansionScaleV3Config(base))

	withCycle := base
	withCycle.AddProductiveBoundaryCycle = true
	require.Error(t, ValidateFixedSuffixExpansionScaleV3Config(withCycle))
	withSelfLoop := base
	withSelfLoop.AddProductiveBoundarySelfLoop = true
	require.Error(t, ValidateFixedSuffixExpansionScaleV3Config(withSelfLoop))
	withFanIn := base
	withFanIn.ReverseFanIn = 1
	require.Error(t, ValidateFixedSuffixExpansionScaleV3Config(withFanIn))
}
