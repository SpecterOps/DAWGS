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
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestLegacyBuilderPostgreSQL_TrustAndPruningForms(t *testing.T) {
	threshold := time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC)

	testCases := map[string]struct {
		criteria   []graph.Criteria
		fragments  []string
		parameters map[string]any
	}{
		"TRUST-01 SameForestTrust ID projection": {
			criteria: trustPruningCriteria("RegressionKind40", "RegressionKind41", query.RelationshipID()),
			fragments: []string{
				"n0.kind_ids operator (pg_catalog.&&) array [72]::int2[]",
				"n1.kind_ids operator (pg_catalog.&&) array [72]::int2[]",
				"e0.kind_id = any (array [73]::int2[])",
				"e0.properties -> 'lastseen'",
				"n0.properties -> 'lastcollected'",
				"n1.properties -> 'lastcollected'",
				"select (s0.e0).id",
			},
			parameters: map[string]any{},
		},
		"TRUST-02 CrossForestTrust full projection": {
			criteria: trustPruningCriteria("RegressionKind40", "RegressionKind42", query.Relationship()),
			fragments: []string{
				"e0.kind_id = any (array [74]::int2[])",
				"select s0.e0 as r",
			},
			parameters: map[string]any{},
		},
		"TRUST-03 branch-local derived trust kinds": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.Start(), graph.StringKind("RegressionKind40")),
					query.Kind(query.End(), graph.StringKind("RegressionKind40")),
					query.Or(
						query.And(
							query.Equals(query.StartID(), graph.ID(101)),
							query.Equals(query.EndID(), graph.ID(202)),
							query.KindIn(query.Relationship(), graph.StringKind("RegressionKind43")),
						),
						query.And(
							query.Equals(query.StartID(), graph.ID(202)),
							query.Equals(query.EndID(), graph.ID(101)),
							query.KindIn(query.Relationship(), graph.StringKind("RegressionKind44")),
						),
					),
				)),
				query.Returning(query.RelationshipID()),
			},
			fragments: []string{
				" or ",
				"n0.id = @pi0",
				"n1.id = @pi1",
				"n0.id = @pi2",
				"n1.id = @pi3",
				"e0.kind_id = any (array [75]::int2[])",
				"e0.kind_id = any (array [76]::int2[])",
			},
			parameters: map[string]any{"pi0": uint64(101), "pi1": uint64(202), "pi2": uint64(202), "pi3": uint64(101)},
		},
		"PRUNE-01 relationship TTL": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Not(query.KindIn(query.Relationship(), graph.StringKind("RegressionKind45"), graph.StringKind("RegressionKind46"))),
					query.Before(query.RelationshipProperty("lastseen"), threshold),
				)),
				query.Returning(query.RelationshipID()),
			},
			fragments:  []string{"not (e0.kind_id = any (array [77, 78]::int2[]))", "e0.properties ->> 'lastseen'", "select (s0.e0).id"},
			parameters: map[string]any{"pi0": threshold},
		},
		"PRUNE-02 HasSession TTL": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.KindIn(query.Relationship(), graph.StringKind("HasSession")),
					query.Or(
						query.Not(query.Exists(query.RelationshipProperty("lastseen"))),
						query.Before(query.RelationshipProperty("lastseen"), threshold),
					),
				)),
				query.Returning(query.RelationshipID()),
			},
			fragments:  []string{"not ((e0.properties ? 'lastseen'", " or ", "e0.kind_id = any (array [7]::int2[])"},
			parameters: map[string]any{"pi0": threshold},
		},
		"PRUNE-03 node TTL": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Not(query.KindIn(query.Node(), graph.StringKind("RegressionKind48"), graph.StringKind("RegressionKind49"))),
					query.Or(
						query.Not(query.Exists(query.NodeProperty("lastseen"))),
						query.Before(query.NodeProperty("lastseen"), threshold),
					),
				)),
				query.Returning(query.NodeID()),
			},
			fragments:  []string{"not (n0.kind_ids operator (pg_catalog.&&) array [80, 81]::int2[])", "not ((n0.properties ? 'lastseen'", "select (s0.n0).id"},
			parameters: map[string]any{"pi0": threshold},
		},
		"PRUNE-04 orphan SID prefix": {
			criteria: []graph.Criteria{
				query.Where(query.And(
					query.Not(query.KindIn(query.Node(), graph.StringKind("RegressionKind48"), graph.StringKind("RegressionKind49"))),
					query.Not(query.Exists(query.NodeProperty("name"))),
					query.StringStartsWith(query.NodeProperty("objectid"), "S-1-5"),
				)),
				query.Returning(query.NodeID()),
			},
			fragments:  []string{"not ((n0.properties ? 'name'", "cypher_starts_with", "select (s0.n0).id"},
			parameters: map[string]any{"pi0": "S-1-5"},
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

func trustPruningCriteria(domainKind, relationshipKind string, projection graph.Criteria) []graph.Criteria {
	return []graph.Criteria{
		query.Where(query.And(
			query.Kind(query.Start(), graph.StringKind(domainKind)),
			query.Kind(query.End(), graph.StringKind(domainKind)),
			query.Kind(query.Relationship(), graph.StringKind(relationshipKind)),
			query.Or(
				query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
				query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
			),
		)),
		query.Returning(projection),
	}
}
