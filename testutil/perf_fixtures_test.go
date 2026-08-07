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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShortestPathScaleFixtureIsDeterministicAndCardinalityExact(t *testing.T) {
	config := ShortestPathScaleConfig{Depth: 16, Fanout: 10}
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

func TestADCSScaleFixtureIsDeterministicAndCoversDecoys(t *testing.T) {
	config := ADCSScaleConfig{MemberOfDepth: 4, Fanout: 10, ValidSuffixEvery: 2, PropertyPayloadSize: 32}
	first := NewADCSScaleFixture(config)
	second := NewADCSScaleFixture(config)
	firstJSON, err := json.Marshal(first)
	require.NoError(t, err)
	secondJSON, err := json.Marshal(second)
	require.NoError(t, err)
	require.Equal(t, firstJSON, secondJSON)
	require.Len(t, first.Nodes, 6+config.MemberOfDepth*config.Fanout)
	require.Len(t, first.Edges, 3+config.MemberOfDepth*config.Fanout+5+4)

	_, edgeKinds := first.Kinds()
	require.Contains(t, edgeKinds.Strings(), "WrongEnrollKind")
}

func TestADCSScaleFixtureV2ControlsSuffixPopulationsIndependently(t *testing.T) {
	reachable := 0
	zeroDepth := false
	fixture := NewADCSScaleFixture(ADCSScaleConfig{
		MemberOfDepth: 2, Fanout: 4, ExactReachableSuffixSources: &reachable,
		DisconnectedSuffixSources: 3, ReverseFanIn: 2, SuffixPathsPerBoundary: 2,
		RootMatchCount: 1, RootHasZeroDepthSuffix: &zeroDepth,
	})

	var enroll, memberOf int
	for _, edge := range fixture.Edges {
		switch edge.Kind {
		case "Enroll":
			enroll++
		case "MemberOf":
			memberOf++
		}
	}
	require.Equal(t, 8, enroll)
	require.Equal(t, 10, memberOf)
	nodeIDs := make([]string, 0, len(fixture.Nodes))
	for _, node := range fixture.Nodes {
		nodeIDs = append(nodeIDs, node.ID)
	}
	require.NotContains(t, nodeIDs, "adcs-disconnected")
	require.Contains(t, nodeIDs, "adcs-disconnected-00002")
}
