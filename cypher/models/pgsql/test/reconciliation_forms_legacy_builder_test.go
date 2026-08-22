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

package test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// TestLegacyBuilderPostgreSQL_ReconciliationForms verifies migrated relationship reconciliation reads and deletes across kind-set sizes.
func TestLegacyBuilderPostgreSQL_ReconciliationForms(t *testing.T) {
	reconciliationKinds := func(count int) graph.Kinds {
		kinds := make(graph.Kinds, count)
		for idx := range count {
			kinds[idx] = graph.StringKind(fmt.Sprintf("RegressionKind%02d", idx+1))
		}
		return kinds
	}

	assertRelationshipDelete := func(t *testing.T, formatted string) {
		t.Helper()
		selection := strings.Index(formatted, "select ")
		deletion := strings.Index(formatted, "delete from edge e1 using s0")
		require.NotEqual(t, -1, selection)
		require.Greater(t, deletion, selection, "selection must precede mutation: %s", formatted)
		require.Contains(t, formatted, "where (s0.e0).id = e1.id")
	}

	for _, count := range []int{1, 2, 9, 30} {
		kinds := reconciliationKinds(count)

		t.Run(fmt.Sprintf("REC-01 inbound %d kinds", count), func(t *testing.T) {
			formatted, translation := translateLegacyQuery(t,
				query.Where(query.And(
					query.Kind(query.End(), graph.StringKind("RegressionKind31")),
					query.Equals(query.EndProperty("objectid"), "target-id"),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Delete(query.Relationship()),
			)

			assertRelationshipDelete(t, formatted)
			require.Contains(t, formatted, "n1.id = e0.end_id")
			require.Contains(t, formatted, "n1.properties -> 'objectid'")
			require.Contains(t, formatted, fmt.Sprintf("array [%s]::int2[]", sequentialKindIDs(33, count)))
			require.Equal(t, map[string]any{"pi0": "target-id"}, translation.Parameters)
		})

		t.Run(fmt.Sprintf("REC-02 outbound %d kinds", count), func(t *testing.T) {
			formatted, translation := translateLegacyQuery(t,
				query.Where(query.And(
					query.Kind(query.Start(), graph.StringKind("RegressionKind31")),
					query.Equals(query.StartProperty("objectid"), "target-id"),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Delete(query.Relationship()),
			)

			assertRelationshipDelete(t, formatted)
			require.Contains(t, formatted, "n0.id = e0.start_id")
			require.Contains(t, formatted, "n0.properties -> 'objectid'")
			require.Contains(t, formatted, fmt.Sprintf("array [%s]::int2[]", sequentialKindIDs(33, count)))
			require.Equal(t, map[string]any{"pi0": "target-id"}, translation.Parameters)
		})
	}

	testCases := map[string]struct {
		// criteria contains the legacy query-builder inputs for the case.
		criteria []graph.Criteria
		// fragments lists SQL fragments that the translation must contain.
		fragments []string
		// parameters is the exact parameter map expected from translation.
		parameters map[string]any
		// read reports whether the case reads rather than deletes a relationship.
		read bool
	}{
		"REC-03 inbound primary group": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.End(), graph.StringKind("RegressionKind31")),
					query.Equals(query.EndProperty("objectid"), "group-id"),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind32")),
					query.Equals(query.RelationshipProperty("isprimarygroup"), false),
				)),
				query.Delete(query.Relationship()),
			},
			fragments:  []string{"n1.id = e0.end_id", "e0.properties -> 'isprimarygroup'", "delete from edge e1 using s0"},
			parameters: map[string]any{"pi0": "group-id", "pi1": false},
		},
		"REC-03 outbound primary group": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.Start(), graph.StringKind("RegressionKind31")),
					query.Equals(query.StartProperty("objectid"), "computer-id"),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind32")),
					query.Equals(query.RelationshipProperty("isprimarygroup"), true),
				)),
				query.Delete(query.Relationship()),
			},
			fragments:  []string{"n0.id = e0.start_id", "e0.properties -> 'isprimarygroup'", "delete from edge e1 using s0"},
			parameters: map[string]any{"pi0": "computer-id", "pi1": true},
		},
		"REC-04 object ID list relationship delete": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind32")),
					query.Kind(query.End(), graph.StringKind("RegressionKind31")),
					query.In(query.EndProperty("objectid"), []string{"target-1", "target-2"}),
				)),
				query.Delete(query.Relationship()),
			},
			fragments:  []string{"n1.id = e0.end_id", "n1.properties ->> 'objectid'", "delete from edge e1 using s0"},
			parameters: map[string]any{"pi0": []string{"target-1", "target-2"}},
		},
		"REC-05 delegated enrollment discovery": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.In(query.EndProperty("objectid"), []string{"ca-1", "ca-2"}),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind32")),
					query.Kind(query.Start(), graph.StringKind("RegressionKind31")),
				)),
				query.Returning(query.Relationship(), query.Start()),
			},
			fragments:  []string{"select s0.e0 as r, s0.n0 as s", "n1.properties ->> 'objectid'"},
			parameters: map[string]any{"pi0": []string{"ca-1", "ca-2"}},
			read:       true,
		},
		"REC-06 delegated enrollment delete by IDs": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.End(), graph.StringKind("RegressionKind31")),
					query.InIDs(query.EndID(), graph.ID(101), graph.ID(202)),
					query.KindIn(query.Relationship(), graph.StringKind("RegressionKind32")),
				)),
				query.Delete(query.Relationship()),
			},
			fragments:  []string{"n1.id = any", "delete from edge e1 using s0"},
			parameters: map[string]any{"pi0": []uint64{101, 202}},
		},
		"REC-07 HostsCAService relationship delete": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.End(), graph.StringKind("RegressionKind31")),
					query.Equals(query.EndProperty("objectid"), "ca-id"),
					query.KindIn(query.Relationship(), graph.StringKind("RegressionKind32")),
				)),
				query.Delete(query.Relationship()),
			},
			fragments:  []string{"n1.id = e0.end_id", "delete from edge e1 using s0"},
			parameters: map[string]any{"pi0": "ca-id"},
		},
		"REC-08 AD entity detach delete": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.Node(), graph.StringKind("RegressionKind31")),
					query.In(query.NodeProperty("objectid"), []string{"target-1", "target-2"}),
				)),
				query.Delete(query.Node()),
			},
			fragments:  []string{"n0.properties ->> 'objectid'", "delete from node n1 using s0", "where (s0.n0).id = n1.id"},
			parameters: map[string]any{"pi0": []string{"target-1", "target-2"}},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			formatted, translation := translateLegacyQuery(t, testCase.criteria...)
			for _, fragment := range testCase.fragments {
				require.Contains(t, formatted, fragment)
			}
			if !testCase.read {
				selection := strings.Index(formatted, "select ")
				deletion := strings.Index(formatted, "delete from ")
				require.Greater(t, deletion, selection, "selection must precede mutation: %s", formatted)
			}
			require.Equal(t, testCase.parameters, translation.Parameters)
		})
	}
}

// sequentialKindIDs formats count consecutive kind IDs beginning at start for SQL-fragment assertions.
func sequentialKindIDs(first, count int) string {
	ids := make([]string, count)
	for idx := range count {
		ids[idx] = fmt.Sprint(first + idx)
	}
	return strings.Join(ids, ", ")
}
