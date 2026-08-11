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
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// TestLegacyBuilderPostgreSQL_StandaloneHopForms verifies migrated one-hop queries preserve anchors, direction, kinds, and projections.
func TestLegacyBuilderPostgreSQL_StandaloneHopForms(t *testing.T) {
	hopKinds := func(count int) graph.Kinds {
		kinds := make(graph.Kinds, count)
		for idx := range count {
			kinds[idx] = graph.StringKind(fmt.Sprintf("RegressionKind%02d", idx+1))
		}
		return kinds
	}

	t.Run("HOP-01 exact and one-element IN start anchors", func(t *testing.T) {
		for name, anchor := range map[string]graph.Criteria{
			"exact": query.Equals(query.StartID(), graph.ID(101)),
			"in":    query.InIDs(query.StartID(), graph.ID(101)),
		} {
			t.Run(name, func(t *testing.T) {
				formatted, translation := translateLegacyQuery(t,
					query.Where(query.And(anchor, query.Kind(query.Relationship(), graph.StringKind("RegressionKind01")))),
					query.Returning(query.Relationship(), query.End()),
				)
				require.Contains(t, formatted, "n0.id = e0.start_id")
				require.Contains(t, formatted, "e0.kind_id = any (array [33]::int2[])")
				require.Contains(t, formatted, "select s0.e0 as r, s0.n1 as e")
				if name == "exact" {
					require.Equal(t, map[string]any{"pi0": uint64(101)}, translation.Parameters)
				} else {
					require.Equal(t, map[string]any{"pi0": []uint64{101}}, translation.Parameters)
				}
			})
		}
	})

	t.Run("HOP-02 end anchor and inbound projection", func(t *testing.T) {
		formatted, translation := translateLegacyQuery(t,
			query.Where(query.And(
				query.Equals(query.EndID(), graph.ID(202)),
				query.Kind(query.Relationship(), graph.StringKind("RegressionKind01")),
			)),
			query.Returning(query.Relationship(), query.Start()),
		)
		require.Contains(t, formatted, "n1.id = e0.end_id")
		require.Contains(t, formatted, "select s0.e0 as r, s0.n0 as s")
		require.Equal(t, map[string]any{"pi0": uint64(202)}, translation.Parameters)
	})

	for _, count := range []int{2, 5, 9, 30} {
		kinds := hopKinds(count)
		kindIDs := sequentialKindIDs(33, count)

		t.Run(fmt.Sprintf("HOP-03 outbound %d kinds", count), func(t *testing.T) {
			formatted, translation := translateLegacyQuery(t,
				query.Where(query.And(
					query.InIDs(query.StartID(), graph.ID(101)),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Returning(query.Relationship(), query.End()),
			)
			require.Contains(t, formatted, fmt.Sprintf("array [%s]::int2[]", kindIDs))
			require.Contains(t, formatted, "n0.id = any")
			require.Contains(t, formatted, "select s0.e0 as r, s0.n1 as e")
			require.Equal(t, map[string]any{"pi0": []uint64{101}}, translation.Parameters)
		})

		t.Run(fmt.Sprintf("HOP-03 inbound %d kinds", count), func(t *testing.T) {
			formatted, translation := translateLegacyQuery(t,
				query.Where(query.And(
					query.InIDs(query.EndID(), graph.ID(202)),
					query.KindIn(query.Relationship(), kinds...),
				)),
				query.Returning(query.Relationship(), query.Start()),
			)
			require.Contains(t, formatted, fmt.Sprintf("array [%s]::int2[]", kindIDs))
			require.Contains(t, formatted, "n1.id = any")
			require.Contains(t, formatted, "select s0.e0 as r, s0.n0 as s")
			require.Equal(t, map[string]any{"pi0": []uint64{202}}, translation.Parameters)
		})
	}

	testCases := map[string]struct {
		// criteria contains the legacy query-builder inputs for the case.
		criteria []graph.Criteria
		// fragments lists SQL fragments that the translation must contain.
		fragments []string
		// parameters is the exact parameter map expected from translation.
		parameters map[string]any
	}{
		"HOP-04 endpoint kind disjunction": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind51")),
					query.KindIn(query.End(), graph.StringKind("RegressionKind52"), graph.StringKind("RegressionKind53")),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{"n1.kind_ids operator (pg_catalog.&&) array [84, 85]::int2[]", "e0.kind_id = any (array [83]::int2[])", "select s0.e0 as r, s0.n1 as e"},
			parameters: map[string]any{"pi0": []uint64{101}},
		},
		"HOP-05 endpoint IDs through variable spelling": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Equals(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind54")),
					query.InIDs(query.End(), graph.ID(202), graph.ID(303)),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{"n0.id = @pi0", "n1.id = any", "e0.kind_id = any (array [86]::int2[])", "select s0.e0 as r, s0.n1 as e"},
			parameters: map[string]any{"pi0": uint64(101), "pi1": []uint64{202, 303}},
		},
		"HOP-05 endpoint IDs through identity-function spelling": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.Start(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind54")),
					query.InIDs(query.EndID(), graph.ID(202), graph.ID(303)),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{"n0.id = any", "n1.id = any", "e0.kind_id = any (array [86]::int2[])", "select s0.e0 as r, s0.n1 as e"},
			parameters: map[string]any{"pi0": []uint64{101}, "pi1": []uint64{202, 303}},
		},
		"HOP-06 scalar endpoint properties": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Equals(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind55")),
					query.Equals(query.EndProperty("enabled"), true),
					query.Equals(query.EndProperty("score"), 7),
					query.Equals(query.EndProperty("name"), "target"),
					query.Equals(query.EndProperty("isassignabletorole"), "true"),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{"n1.properties -> 'enabled'", "n1.properties -> 'score'", "n1.properties -> 'name'", "n1.properties -> 'isassignabletorole'", "e0.kind_id = any (array [87]::int2[])"},
			parameters: map[string]any{"pi0": uint64(101), "pi1": true, "pi2": 7, "pi3": "target", "pi4": "true"},
		},
		"HOP-07 nested production branches": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Equals(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind56")),
					query.Kind(query.End(), graph.StringKind("RegressionKind57")),
					query.Or(
						query.And(
							query.Equals(query.EndProperty("requiresmanagerapproval"), false),
							query.GreaterThan(query.EndProperty("schemaversion"), 1),
							query.Equals(query.EndProperty("authorizedsignatures"), 0),
							query.Equals(query.EndProperty("authenticationenabled"), true),
						),
						query.And(
							query.Equals(query.EndProperty("requiresmanagerapproval"), false),
							query.Equals(query.EndProperty("schemaversion"), 1),
							query.Equals(query.EndProperty("authenticationenabled"), true),
						),
					),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{" or ", "n1.kind_ids operator (pg_catalog.&&) array [89]::int2[]", "n1.properties -> 'schemaversion'", "n1.properties -> 'authorizedsignatures'", "e0.kind_id = any (array [88]::int2[])"},
			parameters: map[string]any{"pi0": uint64(101), "pi1": false, "pi2": 1, "pi3": 0, "pi4": true, "pi5": false, "pi6": 1, "pi7": true},
		},
		"HOP-08 collection and scalar OR": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Equals(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind58")),
					query.Or(
						query.Equals(query.EndProperty("schannelauthenticationenabled"), true),
						query.Equals(query.Size(query.EndProperty("effectiveekus")), 0),
						query.InInverted(query.EndProperty("effectiveekus"), "1.3.6.1.5.5.7.3.2"),
					),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{" or ", "jsonb_array_length", "jsonb_to_text_array", "e0.kind_id = any (array [90]::int2[])"},
			parameters: map[string]any{"pi0": uint64(101), "pi1": true, "pi2": 0, "pi3": "1.3.6.1.5.5.7.3.2"},
		},
		"HOP-09 two-sided ID lists": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.StartID(), graph.ID(101), graph.ID(202)),
					query.InIDs(query.EndID(), graph.ID(303), graph.ID(404)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind59")),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{"n0.id = any", "n1.id = any", "e0.kind_id = any (array [91]::int2[])", "select s0.e0 as r, s0.n1 as e"},
			parameters: map[string]any{"pi0": []uint64{101, 202}, "pi1": []uint64{303, 404}},
		},
		"HOP-10 outbound full direction": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind60")),
					query.Kind(query.End(), graph.StringKind("RegressionKind52")),
					query.Equals(query.EndProperty("active"), true),
				)),
				query.Returning(query.Relationship(), query.End()),
			},
			fragments:  []string{"n1.kind_ids operator (pg_catalog.&&) array [84]::int2[]", "n1.properties -> 'active'", "select s0.e0 as r, s0.n1 as e"},
			parameters: map[string]any{"pi0": []uint64{101}, "pi1": true},
		},
		"HOP-10 inbound full direction": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.EndID(), graph.ID(202)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind60")),
					query.Kind(query.Start(), graph.StringKind("RegressionKind51")),
					query.Equals(query.StartProperty("active"), true),
				)),
				query.Returning(query.Relationship(), query.Start()),
			},
			fragments:  []string{"n0.kind_ids operator (pg_catalog.&&) array [83]::int2[]", "n0.properties -> 'active'", "select s0.e0 as r, s0.n0 as s"},
			parameters: map[string]any{"pi0": []uint64{202}, "pi1": true},
		},
		"HOP-10 start node projection": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.EndID(), graph.ID(202)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind60")),
				)),
				query.Returning(query.Start()),
			},
			fragments:  []string{"select s0.n0 as s"},
			parameters: map[string]any{"pi0": []uint64{202}},
		},
		"HOP-10 end ID relationship projection": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.InIDs(query.StartID(), graph.ID(101)),
					query.Kind(query.Relationship(), graph.StringKind("RegressionKind60")),
				)),
				query.Returning(query.EndID(), query.Relationship()),
			},
			fragments:  []string{"select s0.n1 as \"id(e)\", s0.e0 as r"},
			parameters: map[string]any{"pi0": []uint64{101}},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			formatted, translation := translateLegacyQuery(t, testCase.criteria...)
			for _, fragment := range testCase.fragments {
				require.Contains(t, formatted, fragment)
			}
			require.Equal(t, testCase.parameters, translation.Parameters)
		})
	}
}
