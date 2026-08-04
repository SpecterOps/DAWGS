// Copyright 2026 Specter Ops, Inc.
//
// Licensed under the Apache License, Version 2.0
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func phase5RegressionKinds(numbers ...int) graph.Kinds {
	kinds := make(graph.Kinds, len(numbers))
	for idx, number := range numbers {
		kinds[idx] = graph.StringKind("RegressionKind" + phase5TwoDigits(number))
	}
	return kinds
}

func phase5TwoDigits(value int) string {
	if value < 10 {
		return "0" + string(rune('0'+value))
	}
	return string(rune('0'+value/10)) + string(rune('0'+value%10))
}

func assertPhase5Translation(t *testing.T, criteria []graph.Criteria, fragments ...string) {
	t.Helper()
	formatted, _ := translateLegacyQuery(t, criteria...)
	for _, fragment := range fragments {
		require.Contains(t, formatted, fragment)
	}
}

func TestLegacyBuilderPostgreSQL_Phase5RelationshipScans(t *testing.T) {
	t.Run("SCAN-01 base endpoints and relationship ID", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.KindIn(query.Start(), phase5RegressionKinds(61, 62)...),
				query.Kind(query.Relationship(), phase5RegressionKinds(63)[0]),
				query.KindIn(query.End(), phase5RegressionKinds(61, 62)...),
			)),
			query.Returning(query.RelationshipID()),
		}, "n0.kind_ids", "n1.kind_ids", "array [93, 94]::int2[]", "e0.kind_id = any (array [95]::int2[])", "select (s0.e0).id")
	})

	t.Run("SCAN-02 excludes Meta endpoints and hydrates relationships", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Not(query.KindIn(query.Start(), phase5RegressionKinds(64, 65)...)),
				query.KindIn(query.Relationship(), phase5RegressionKinds(66, 67)...),
				query.Not(query.KindIn(query.End(), phase5RegressionKinds(64, 65)...)),
			)),
			query.Returning(query.Relationship()),
		}, "not", "array [96, 97]::int2[]", "array [98, 99]::int2[]", "select s0.e0 as r")
	})

	t.Run("SCAN-03 exists relationship property and ID", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Not(query.KindIn(query.Start(), phase5RegressionKinds(64, 65)...)),
				query.Kind(query.Relationship(), phase5RegressionKinds(68)[0]),
				query.Exists(query.RelationshipProperty("lastseen")),
				query.Not(query.KindIn(query.End(), phase5RegressionKinds(64, 65)...)),
			)),
			query.Returning(query.RelationshipID()),
		}, "e0.properties ? 'lastseen'", "not (e0.properties -> 'lastseen')", "array [100]::int2[]", "select (s0.e0).id")
	})

	for _, relationshipKind := range []int{70, 71} {
		t.Run("SCAN-04 raw ownership representative "+phase5TwoDigits(relationshipKind), func(t *testing.T) {
			assertPhase5Translation(t, []graph.Criteria{
				query.Where(query.And(
					query.Kind(query.Relationship(), phase5RegressionKinds(relationshipKind)[0]),
					query.Kind(query.Start(), phase5RegressionKinds(69)[0]),
				)),
				query.Returning(query.Relationship()),
			}, "n0.kind_ids", "array [101]::int2[]", "select s0.e0 as r")
		})
	}

	nineKinds := phase5RegressionKinds(72, 73, 74, 75, 76, 77, 78, 79, 80)
	t.Run("SCAN-05 nine relationship kinds bound end", func(t *testing.T) {
		formatted, translation := translateLegacyQuery(t,
			query.Where(query.And(
				query.Kind(query.Start(), phase5RegressionKinds(69)[0]),
				query.KindIn(query.Relationship(), nineKinds...),
				query.Equals(query.EndID(), graph.ID(202)),
			)),
			query.Returning(query.Relationship(), query.Start()),
		)
		require.Contains(t, formatted, "array [104, 105, 106, 107, 108, 109, 110, 111, 112]::int2[]")
		require.Contains(t, formatted, "select s0.e0 as r, s0.n0 as s")
		require.Equal(t, map[string]any{"pi0": uint64(202)}, translation.Parameters)
	})

	t.Run("SCAN-06 FetchKinds column order", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Relationship(), phase5RegressionKinds(82)[0]),
				query.Kind(query.End(), phase5RegressionKinds(81)[0]),
			)),
			query.Returning(query.StartID(), query.RelationshipID(), query.KindsOf(query.Relationship()), query.EndID()),
		}, "select (s0.n0).id as \"id(s)\", (s0.e0).id as \"id(r)\", kind_name((s0.e0).kind_id)::text as \"type(r)\", (s0.n1).id as \"id(e)\"")
	})

	t.Run("SCAN-07 directed start and end IDs", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.KindIn(query.Relationship(), phase5RegressionKinds(83, 84)...)),
			query.Returning(query.StartID(), query.EndID()),
		}, "array [115, 116]::int2[]", "select (s0.n0).id as \"id(s)\", (s0.n1).id as \"id(e)\"")
	})

	t.Run("SCAN-08 scenario A and B", func(t *testing.T) {
		for name, testCase := range map[string]struct {
			endKinds graph.Kinds
			relKinds graph.Kinds
		}{
			"scenario A": {relKinds: phase5RegressionKinds(87, 88, 89, 90, 91, 92)},
			"scenario B": {endKinds: phase5RegressionKinds(81), relKinds: phase5RegressionKinds(87, 88, 89, 90, 91)},
		} {
			t.Run(name, func(t *testing.T) {
				criteria := []graph.Criteria{
					query.KindIn(query.Start(), phase5RegressionKinds(85, 86, 81)...),
					query.InIDs(query.EndID(), graph.ID(202), graph.ID(303)),
					query.KindIn(query.Relationship(), testCase.relKinds...),
				}
				if len(testCase.endKinds) > 0 {
					criteria = append(criteria, query.KindIn(query.End(), testCase.endKinds...))
				}
				assertPhase5Translation(t, []graph.Criteria{
					query.Where(query.And(criteria...)),
					query.Returning(query.StartID()),
				}, "n0.kind_ids", "n1.id = any", "select (s0.n0).id")
			})
		}
	})
}

func TestLegacyBuilderPostgreSQL_Phase5Lookups(t *testing.T) {
	t.Run("LOOKUP-01 ID and full-node projections", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.KindIn(query.Node(), phase5RegressionKinds(85, 86)...)),
			query.Returning(query.NodeID()),
		}, "array [117, 118]::int2[]", "select (s0.n0).id")
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.Kind(query.Node(), phase5RegressionKinds(93)[0])),
			query.Returning(query.Node()),
		}, "array [125]::int2[]", "select s0.n0 as n")
	})

	t.Run("LOOKUP-02 equalities and limit", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Node(), phase5RegressionKinds(81)[0]),
				query.Equals(query.NodeProperty("objectid"), "S-1-5-21"),
			)),
			query.Returning(query.Node()),
			query.Limit(1),
		}, "n0.properties -> 'objectid'", "select s0.n0 as n", "limit 1")
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Equals(query.NodeProperty("name"), "dc.example.test"),
				query.Equals(query.NodeProperty("enabled"), true),
			)),
			query.Returning(query.NodeID()),
		}, "n0.properties -> 'name'", "n0.properties -> 'enabled'", "select (s0.n0).id")
	})

	t.Run("LOOKUP-03 boolean two-column projection", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Node(), phase5RegressionKinds(81)[0]),
				query.Equals(query.NodeProperty("hasura"), true),
			)),
			query.Returning(query.NodeID(), query.NodeProperty("hasura")),
		}, "select (s0.n0).id as \"id(n)\", ((s0.n0).properties -> 'hasura') as \"n.hasura\"")
	})

	t.Run("LOOKUP-04 prefix suffix and equality", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Node(), phase5RegressionKinds(94)[0]),
				query.StringStartsWith(query.NodeProperty("distinguishedname"), "CN=ADMINSDHOLDER,"),
				query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
			)),
			query.Returning(query.Node()),
		}, "cypher_starts_with", "n0.properties -> 'domainsid'")
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.Or(
				query.StringEndsWith(query.NodeProperty("objectid"), "-S-1"),
				query.StringEndsWith(query.NodeProperty("objectid"), "-S-2"),
			)),
			query.Returning(query.NodeID()),
		}, "cypher_ends_with", " or ")
	})

	t.Run("LOOKUP-05 case-insensitive strings preserve literals", func(t *testing.T) {
		formatted, translation := translateLegacyQuery(t,
			query.Where(query.CaseInsensitiveStringStartsWith(query.NodeProperty("name"), "Remote Desktop Users%_")),
			query.Returning(query.NodeID()),
		)
		require.Contains(t, formatted, "lower")
		require.Contains(t, formatted, "cypher_starts_with")
		require.Equal(t, map[string]any{"pi0": "remote desktop users%_"}, translation.Parameters)
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.CaseInsensitiveStringContains(query.NodeProperty("objectid"), "Approver_GUID")),
			query.Returning(query.Node()),
		}, "lower", "cypher_contains")
	})

	t.Run("LOOKUP-06 required and excluded kind groups", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.KindIn(query.Node(), phase5RegressionKinds(85, 86)...),
				query.Kind(query.Node(), phase5RegressionKinds(69)[0]),
				query.StringEndsWith(query.NodeProperty("objectid"), "-512"),
				query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
			)),
			query.Returning(query.Node()),
		}, "array [117, 118]::int2[]", "array [101]::int2[]", "cypher_ends_with")
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Node(), phase5RegressionKinds(69)[0]),
				query.Not(query.KindIn(query.Node(), phase5RegressionKinds(85, 98)...)),
				query.StringEndsWith(query.NodeProperty("objectid"), "-512"),
			)),
			query.Returning(query.Node()),
		}, "not", "array [117, 130]::int2[]")
	})

	t.Run("LOOKUP-07 missing property", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.Not(query.Exists(query.NodeProperty("name")))),
			query.Returning(query.Node()),
		}, "n0.properties ? 'name'", "not (n0.properties -> 'name')", "not")
	})

	t.Run("LOOKUP-08 nullable approver disjunction", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Node(), phase5RegressionKinds(95)[0]),
				query.Equals(query.NodeProperty("tenantid"), "tenant-1"),
				query.Equals(query.NodeProperty("approvalrequired"), true),
				query.Or(
					query.IsNotNull(query.NodeProperty("userapprovers")),
					query.IsNotNull(query.NodeProperty("groupapprovers")),
				),
			)),
			query.Returning(query.Node()),
		}, "n0.properties ? 'userapprovers'", "n0.properties ? 'groupapprovers'", " or ")
	})

	t.Run("LOOKUP-09 duplicate ID list hydration", func(t *testing.T) {
		formatted, translation := translateLegacyQuery(t,
			query.Where(query.InIDs(query.NodeID(), graph.ID(101), graph.ID(202), graph.ID(101))),
			query.Returning(query.Node()),
		)
		require.Contains(t, formatted, "n0.id = any")
		require.Contains(t, formatted, "select s0.n0 as n")
		require.Equal(t, map[string]any{"pi0": []uint64{101, 202, 101}}, translation.Parameters)
	})

	t.Run("LOOKUP-10 nested negated flags", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Kind(query.Node(), phase5RegressionKinds(86)[0]),
				query.Not(query.And(query.Exists(query.NodeProperty("gmsa")), query.Equals(query.NodeProperty("gmsa"), true))),
				query.Not(query.And(query.Exists(query.NodeProperty("msa")), query.Equals(query.NodeProperty("msa"), true))),
				query.InIDs(query.NodeID(), graph.ID(101), graph.ID(202)),
			)),
			query.Returning(query.Node()),
		}, "not", "n0.properties -> 'gmsa'", "n0.properties -> 'msa'", "n0.id = any")
	})

	t.Run("LOOKUP-11 tenant adjacency and endpoint property list", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Kind(query.Relationship(), phase5RegressionKinds(97)[0]),
				query.KindIn(query.End(), phase5RegressionKinds(95, 96)...),
				query.In(query.EndProperty("roletemplateid"), []string{"role-a", "role-b"}),
			)),
			query.Returning(query.End()),
		}, "n0.id = @pi0", "array [127, 128]::int2[]", "n1.properties ->> 'roletemplateid'", "select s0.n1 as e")
	})

	t.Run("LOOKUP-12 exact edge key and First", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.And(
				query.Equals(query.StartID(), graph.ID(101)),
				query.Equals(query.EndID(), graph.ID(202)),
				query.Kind(query.Relationship(), phase5RegressionKinds(83)[0]),
			)),
			query.Returning(query.Relationship()),
			query.Limit(1),
		}, "n0.id = @pi0", "n1.id = @pi1", "array [115]::int2[]", "select s0.e0 as r", "limit 1")
	})

	t.Run("LOOKUP-13 suffix with bound opposite endpoint projections", func(t *testing.T) {
		for _, projection := range []graph.Criteria{query.Returning(query.Start()), query.Returning(query.StartID())} {
			assertPhase5Translation(t, []graph.Criteria{
				query.Where(query.And(
					query.StringEndsWith(query.StartProperty("objectid"), "-555"),
					query.Kind(query.Relationship(), phase5RegressionKinds(82)[0]),
					query.Equals(query.EndID(), graph.ID(202)),
				)),
				projection,
			}, "cypher_ends_with", "n1.id = @pi1")
		}
	})

	t.Run("LOOKUP-14 descending property order", func(t *testing.T) {
		assertPhase5Translation(t, []graph.Criteria{
			query.Where(query.Kind(query.Node(), phase5RegressionKinds(99)[0])),
			query.Returning(query.Node()),
			query.OrderBy(query.Order(query.NodeProperty("name"), query.Descending())),
		}, "select s0.n0 as n", "order by", "desc")
	})

	t.Run("LOOKUP-16 typed and untyped four-property equalities", func(t *testing.T) {
		for name, kindCriteria := range map[string]graph.Criteria{
			"typed":   query.Kind(query.Node(), phase5RegressionKinds(81)[0]),
			"untyped": query.And(),
		} {
			t.Run(name, func(t *testing.T) {
				assertPhase5Translation(t, []graph.Criteria{
					query.Where(query.And(
						kindCriteria,
						query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
						query.Equals(query.NodeProperty("isdc"), true),
						query.Equals(query.NodeProperty("ldapavailable"), true),
						query.Equals(query.NodeProperty("ldapsigning"), false),
					)),
					query.Returning(query.NodeID()),
				}, "n0.properties -> 'domainsid'", "n0.properties -> 'isdc'", "n0.properties -> 'ldapavailable'", "n0.properties -> 'ldapsigning'", "select (s0.n0).id")
			})
		}
	})
}
