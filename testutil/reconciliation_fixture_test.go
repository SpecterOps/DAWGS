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
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNewReconciliationScaleFixture(t *testing.T) {
	fixture := NewReconciliationScaleFixture(8)
	nodeKinds, edgeKinds := fixture.Kinds()

	require.Len(t, fixture.Nodes, 2_017)
	require.Len(t, fixture.Edges, 46)
	require.Contains(t, nodeKinds, graph.StringKind("ADEntity"))
	for idx := 1; idx <= 30; idx++ {
		require.Contains(t, edgeKinds, graph.StringKind(fmt.Sprintf("RecKind%02d", idx)))
	}
}

func TestFixtureNamesAreDeterministic(t *testing.T) {
	require.Equal(t, []string{"item-00", "item-01", "item-02"}, FixtureNames("item", 3))
	require.Equal(t, FixtureNames("item", 2_000), FixtureNames("item", 2_000))
	require.Empty(t, FixtureNames("item", -1))
}

func TestNewTrustPruningScaleFixtureIncludesDenseAndDecoyShapes(t *testing.T) {
	fixture := NewTrustPruningScaleFixture(8)
	nodeKinds, edgeKinds := fixture.Kinds()

	require.Len(t, fixture.Nodes, 56)
	require.Len(t, fixture.Edges, 83)
	require.Contains(t, nodeKinds, graph.StringKind("Domain"))
	require.Contains(t, nodeKinds, graph.StringKind("PruneCandidate"))
	require.Contains(t, nodeKinds, graph.StringKind("PruneBatchNode"))
	require.Contains(t, edgeKinds, graph.StringKind("SameForestTrust"))
	require.Contains(t, edgeKinds, graph.StringKind("CrossForestTrust"))
	require.Contains(t, edgeKinds, graph.StringKind("HasSession"))
	require.Contains(t, edgeKinds, graph.StringKind("PruneBatch"))
	require.Contains(t, edgeKinds, graph.StringKind("MetaIncludes"))
}

func TestNewHopScaleFixtureIncludesDenseAndLargeListShapes(t *testing.T) {
	fixture := NewHopScaleFixture(32)
	nodeKinds, edgeKinds := fixture.Kinds()

	require.Len(t, fixture.Nodes, 1_132)
	require.Len(t, fixture.Edges, 1_220)
	require.Contains(t, nodeKinds, graph.StringKind("HopTemplate"))
	require.Contains(t, nodeKinds, graph.StringKind("HopIDEndpoint"))
	for idx := 1; idx <= 30; idx++ {
		require.Contains(t, edgeKinds, graph.StringKind(fmt.Sprintf("HopKind%02d", idx)))
	}
	require.Contains(t, edgeKinds, graph.StringKind("HopSetEdge"))
}

func TestNewScanLookupScaleFixtureIncludesWideAndLargeListShapes(t *testing.T) {
	fixture := NewScanLookupScaleFixture(32)
	nodeKinds, edgeKinds := fixture.Kinds()

	require.Len(t, fixture.Nodes, 3_198)
	require.Len(t, fixture.Edges, 1_352)
	require.Contains(t, nodeKinds, graph.StringKind("ADBase"))
	require.Contains(t, nodeKinds, graph.StringKind("AZRole"))
	require.Contains(t, nodeKinds, graph.StringKind("Hydrate"))
	require.Contains(t, edgeKinds, graph.StringKind("ScanPostProcessed"))
	require.Contains(t, edgeKinds, graph.StringKind("Contains"))
	for idx := 1; idx <= 9; idx++ {
		require.Contains(t, edgeKinds, graph.StringKind(fmt.Sprintf("ADCSEdge%02d", idx)))
	}
}
