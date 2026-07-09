// Copyright 2026 Specter Ops, Inc.
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

package main

import (
	"os"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/stretchr/testify/require"
)

func TestBaseScenariosDeclareExpectedRows(t *testing.T) {
	scenarios := baseScenarios(opengraph.IDMap{
		"n1": graph.ID(1),
		"n2": graph.ID(2),
		"n3": graph.ID(3),
	})

	requireExpectedRows(t, scenarios, "Match Nodes", "base", 3)
	requireExpectedRows(t, scenarios, "Match Edges", "base", 2)
	requireExpectedRows(t, scenarios, "Shortest Paths", "n1 -> n3", 1)
	requireExpectedRows(t, scenarios, "Traversal", "n1", 2)
	requireExpectedRows(t, scenarios, "Match Return", "n1", 1)
	requireExpectedRows(t, scenarios, "Filter By Kind", "NodeKind1", 2)
	requireExpectedRows(t, scenarios, "Filter By Kind", "NodeKind2", 2)
}

func TestTraversalShapesDatasetIsValid(t *testing.T) {
	file, err := os.Open("../../integration/testdata/traversal_shapes.json")
	require.NoError(t, err)
	defer file.Close()

	doc, err := opengraph.ParseDocument(file)
	require.NoError(t, err)
	require.Len(t, doc.Graph.Nodes, 45)
	require.Len(t, doc.Graph.Edges, 41)
}

func TestTraversalShapesScenariosDeclareExpectedRows(t *testing.T) {
	scenarios := traversalShapesScenarios(traversalShapesIDMap())

	requireExpectedRows(t, scenarios, "Match Nodes", traversalShapesDataset, 45)
	requireExpectedRows(t, scenarios, "Match Edges", traversalShapesDataset, 41)
	requireExpectedRows(t, scenarios, "Traversal Depth", "chain depth 1", 1)
	requireExpectedRows(t, scenarios, "Traversal Depth", "chain depth 3", 3)
	requireExpectedRows(t, scenarios, "Traversal Depth", "chain depth 10", 10)
	requireExpectedRows(t, scenarios, "Traversal Depth", "fanout depth 1", 3)
	requireExpectedRows(t, scenarios, "Traversal Depth", "fanout depth 2", 9)
	requireExpectedRows(t, scenarios, "Traversal Depth", "fanout depth 3", 15)
	requireExpectedRows(t, scenarios, "Traversal Cycle", "bounded cycle", 4)
	requireExpectedRows(t, scenarios, "Traversal Dead End", "chain terminal", 0)
	requireExpectedRows(t, scenarios, "Edge Kind Traversal", "Allowed", 3)
	requireExpectedRows(t, scenarios, "Edge Kind Traversal", "all kinds", 6)
	requireExpectedRows(t, scenarios, "Shortest Paths", "diamond many paths", 3)
	requireExpectedRows(t, scenarios, "Shortest Paths", "disconnected", 0)
}

func TestDefaultDatasetsIncludeTraversalShapes(t *testing.T) {
	require.Contains(t, defaultDatasets, traversalShapesDataset)
	require.Contains(t, defaultDatasets, "adcs_fanout")
}

func TestValidateScenarioRows(t *testing.T) {
	scenario := Scenario{
		Section:      "Traversal",
		Dataset:      "base",
		Label:        "n1",
		ExpectedRows: expectRows(2),
	}

	require.NoError(t, validateScenarioRows(scenario, 2))
	require.ErrorContains(t, validateScenarioRows(scenario, 1), "Traversal/n1 on base expected 2 rows, got 1")
}

func traversalShapesIDMap() opengraph.IDMap {
	ids := []string{
		"c0", "c10",
		"f0",
		"d0", "d4",
		"y0",
		"x0", "x1",
		"s0",
	}

	idMap := opengraph.IDMap{}
	for idx, id := range ids {
		idMap[id] = graph.ID(idx + 1)
	}

	return idMap
}

func requireExpectedRows(t *testing.T, scenarios []Scenario, section, label string, expectedRows int64) {
	t.Helper()

	for _, scenario := range scenarios {
		if scenario.Section == section && scenario.Label == label {
			require.NotNil(t, scenario.ExpectedRows)
			require.Equal(t, expectedRows, *scenario.ExpectedRows)
			return
		}
	}

	require.Failf(t, "scenario not found", "%s/%s", section, label)
}
