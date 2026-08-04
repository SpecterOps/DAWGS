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
