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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFixtureNamesAreDeterministic(t *testing.T) {
	require.Equal(t, []string{"id-00", "id-01", "id-02"}, FixtureNames("id", 3))
	require.Equal(t, []string{"RegressionKind01", "RegressionKind02"}, FixtureKinds(2))
	require.Equal(t, FixtureNames("id", 1_000), FixtureNames("id", 1_000))
	require.Empty(t, FixtureNames("id", -1))
}

func TestNewReconciliationFixtureIncludesRequiredShapes(t *testing.T) {
	fixture := NewReconciliationFixture(4)
	require.Len(t, fixture.Nodes, 8)
	require.Len(t, fixture.Edges, 9)
	require.Equal(t, "fanout-00", fixture.Nodes[4].ID)
	require.Equal(t, "fanout-03", fixture.Nodes[7].ID)
	require.Equal(t, "FanoutEdge", fixture.Edges[5].Kind)
	require.Equal(t, "fanout-03", fixture.Edges[8].EndID)

	nodeKinds, edgeKinds := fixture.Kinds()
	require.Contains(t, nodeKinds.Strings(), "Computer")
	require.Contains(t, nodeKinds.Strings(), "Group")
	require.Contains(t, edgeKinds.Strings(), "MemberOf")
	require.Contains(t, edgeKinds.Strings(), "HasSession")
}
