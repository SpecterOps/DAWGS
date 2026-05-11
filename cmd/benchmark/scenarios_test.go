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

func requireExpectedRows(t *testing.T, scenarios []Scenario, section, label string, expectedRows int) {
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
