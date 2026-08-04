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

//go:build manual_integration

package integration

import (
	"sort"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestPhase5LegacyBuilderIntegration(t *testing.T) {
	wideFixture := phase4TemplateFixture(t, "SCAN-01 through SCAN-04 wide relationship filters")
	anchoredFixture := phase4TemplateFixture(t, "SCAN-05 through SCAN-08 anchored scans and projections")
	basicFixture := phase4TemplateFixture(t, "LOOKUP-01 through LOOKUP-08 node predicates and projections")
	advancedFixture := phase4TemplateFixture(t, "LOOKUP-09 through LOOKUP-14 and LOOKUP-16 advanced lookups")
	countFixture := phase4TemplateFixture(t, "LOOKUP-15 dense graph counts")

	var nodeKinds, edgeKinds graph.Kinds
	for _, fixture := range []*opengraph.Graph{wideFixture, anchoredFixture, basicFixture, advancedFixture, countFixture} {
		nextNodeKinds, nextEdgeKinds := fixture.Kinds()
		nodeKinds = nodeKinds.Add(nextNodeKinds...)
		edgeKinds = edgeKinds.Add(nextEdgeKinds...)
	}
	db, ctx := SetupDBWithKindsNoGraphCleanup(t, nodeKinds, edgeKinds)
	ClearGraph(t, db, ctx)
	session := &Session{DB: db, Ctx: ctx}

	t.Run("SCAN-01 base endpoints and relationship IDs", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, wideFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.KindIn(query.Start(), graph.StringKind("ADBase"), graph.StringKind("AZBase")),
				query.Kind(query.Relationship(), graph.StringKind("PostProcessed")),
				query.KindIn(query.End(), graph.StringKind("ADBase"), graph.StringKind("AZBase")),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			ids, err := ops.FetchRelationshipIDs(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, ids, 4)
			return nil
		})
	})

	t.Run("SCAN-02 non-Meta relationship hydration", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, wideFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Not(query.KindIn(query.Start(), graph.StringKind("Meta"), graph.StringKind("MetaDetail"))),
				query.KindIn(query.Relationship(), graph.StringKind("TrackerA"), graph.StringKind("TrackerB")),
				query.Not(query.KindIn(query.End(), graph.StringKind("Meta"), graph.StringKind("MetaDetail"))),
			)
		}, phase4AssertRelationshipMarkers(t, []string{"tracker-a", "tracker-b"}))
	})

	t.Run("SCAN-03 present lastseen relationship IDs", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, wideFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Not(query.KindIn(query.Start(), graph.StringKind("Meta"), graph.StringKind("MetaDetail"))),
				query.Kind(query.Relationship(), graph.StringKind("MigratedEdge")),
				query.Exists(query.RelationshipProperty("lastseen")),
				query.Not(query.KindIn(query.End(), graph.StringKind("Meta"), graph.StringKind("MetaDetail"))),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			ids, err := ops.FetchRelationshipIDs(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, ids, 1)
			return nil
		})
	})

	t.Run("SCAN-04 raw ownership representatives", func(t *testing.T) {
		for kind, expected := range map[string]string{"OwnsRaw": "owns", "WriteOwnerRaw": "write-owner"} {
			t.Run(kind, func(t *testing.T) {
				WithLegacyRelationshipQuery(t, session, wideFixture, func(opengraph.IDMap) graph.Criteria {
					return query.And(
						query.Kind(query.Relationship(), graph.StringKind(kind)),
						query.Kind(query.Start(), graph.StringKind("Entity")),
					)
				}, phase4AssertRelationshipMarkers(t, []string{expected}))
			})
		}
	})

	t.Run("SCAN-05 consolidated nine-kind inbound scan", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, anchoredFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Start(), graph.StringKind("Entity")),
				query.KindIn(query.Relationship(), phase5ADCSKinds()...),
				query.Equals(query.EndID(), idMap["target"]),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			seenKinds := map[graph.Kind]int{}
			err := ops.ForEachStartNode(relationshipQuery, func(relationship *graph.Relationship, node *graph.Node) error {
				require.True(t, node.Kinds.ContainsOneOf(graph.StringKind("Entity")))
				seenKinds[relationship.Kind]++
				return nil
			})
			require.NoError(t, err)
			require.Len(t, seenKinds, 9)
			for _, count := range seenKinds {
				require.Equal(t, 1, count)
			}
			return nil
		})
	})

	t.Run("SCAN-06 FetchKinds contract", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, anchoredFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Relationship(), graph.StringKind("LocalToComputer")),
				query.Kind(query.End(), graph.StringKind("Computer")),
			)
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			return relationshipQuery.FetchKinds(func(cursor graph.Cursor[graph.RelationshipKindsResult]) error {
				var results []graph.RelationshipKindsResult
				for result := range cursor.Chan() {
					results = append(results, result)
				}
				require.NoError(t, cursor.Error())
				require.Len(t, results, 1)
				require.Equal(t, idMap["source-01"], results[0].StartID)
				require.Equal(t, idMap["target"], results[0].EndID)
				require.Equal(t, graph.StringKind("LocalToComputer"), results[0].Kind)
				require.NotZero(t, results[0].ID)
				return nil
			})
		})
	})

	t.Run("SCAN-07 directed endpoint pairs", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, anchoredFixture, func(opengraph.IDMap) graph.Criteria {
			return query.KindIn(query.Relationship(), graph.StringKind("MemberOf"), graph.StringKind("MemberOfLocalGroup"))
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			return relationshipQuery.FetchTriples(func(cursor graph.Cursor[graph.RelationshipTripleResult]) error {
				count := 0
				duplicatePairCount := 0
				for result := range cursor.Chan() {
					count++
					if result.StartID == idMap["source-01"] && result.EndID == idMap["target"] {
						duplicatePairCount++
					}
				}
				require.NoError(t, cursor.Error())
				require.Equal(t, 3, count)
				require.Equal(t, 2, duplicatePairCount)
				return nil
			})
		})
	})

	t.Run("SCAN-08 both ESC scenarios", func(t *testing.T) {
		for _, testCase := range []struct {
			name      string
			scenarioB bool
			expected  int
		}{
			{name: "scenario A", expected: 3},
			{name: "scenario B", scenarioB: true, expected: 2},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				WithLegacyRelationshipQuery(t, session, anchoredFixture, func(idMap opengraph.IDMap) graph.Criteria {
					criteria := []graph.Criteria{
						query.KindIn(query.Start(), graph.StringKind("Group"), graph.StringKind("User"), graph.StringKind("Computer")),
						query.InIDs(query.EndID(), idMap["victim-computer"], idMap["victim-other"], idMap["victim-unused"]),
					}
					if testCase.scenarioB {
						criteria = append(criteria,
							query.Kind(query.End(), graph.StringKind("Computer")),
							query.KindIn(query.Relationship(), graph.StringKind("GenericAll"), graph.StringKind("GenericWrite"), graph.StringKind("Owns"), graph.StringKind("WriteOwner"), graph.StringKind("WriteDACL")),
						)
					} else {
						criteria = append(criteria, query.KindIn(query.Relationship(), graph.StringKind("GenericAll"), graph.StringKind("GenericWrite"), graph.StringKind("Owns"), graph.StringKind("WriteOwner"), graph.StringKind("WriteDACL"), graph.StringKind("WritePublicInformation")))
					}
					return query.And(criteria...)
				}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
					ids, err := ops.FetchStartNodeIDs(relationshipQuery)
					require.NoError(t, err)
					require.Len(t, ids, testCase.expected)
					return nil
				})
			})
		}
	})

	t.Run("LOOKUP-01 kind scans and hydration", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, basicFixture, func(opengraph.IDMap) graph.Criteria {
			return query.KindIn(query.Node(), graph.StringKind("Group"), graph.StringKind("User"))
		}, func(nodeQuery graph.NodeQuery, _ opengraph.IDMap) error {
			nodes, err := ops.FetchNodes(nodeQuery)
			require.NoError(t, err)
			require.Len(t, nodes, 8)
			return nil
		})
	})

	t.Run("LOOKUP-02 equality First", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, basicFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Computer")),
				query.Equals(query.NodeProperty("objectid"), "S-1-5-21-100"),
			)
		}, func(nodeQuery graph.NodeQuery, _ opengraph.IDMap) error {
			node, err := nodeQuery.Limit(1).First()
			require.NoError(t, err)
			require.NotNil(t, node)
			return nil
		})
	})

	t.Run("LOOKUP-03 boolean projection order and type", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, basicFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Computer")),
				query.Equals(query.NodeProperty("hasura"), true),
			)
		}, func(nodeQuery graph.NodeQuery, _ opengraph.IDMap) error {
			return nodeQuery.Query(func(results graph.Result) error {
				count := 0
				for results.Next() {
					var id graph.ID
					var hasURA bool
					require.NoError(t, results.Scan(&id, &hasURA))
					require.NotZero(t, id)
					require.True(t, hasURA)
					count++
				}
				require.NoError(t, results.Error())
				require.Equal(t, 1, count)
				return nil
			}, query.Returning(query.NodeID(), query.NodeProperty("hasura")))
		})
	})

	t.Run("LOOKUP-04 case-sensitive prefix", func(t *testing.T) {
		phase5AssertNodeIDs(t, session, basicFixture, []string{"adminsdholder"}, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Container")),
				query.StringStartsWith(query.NodeProperty("distinguishedname"), "CN=ADMINSDHOLDER,CN=SYSTEM,"),
				query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
			)
		})
	})

	t.Run("LOOKUP-05 case-insensitive contains candidates", func(t *testing.T) {
		phase5AssertNodeIDs(t, session, basicFixture, []string{"ci-contains-exact", "ci-contains-substring"}, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Entity")),
				query.CaseInsensitiveStringContains(query.NodeProperty("objectid"), "Approver_GUID"),
			)
		})
	})

	t.Run("LOOKUP-06 required and excluded kinds", func(t *testing.T) {
		phase5AssertNodeIDs(t, session, basicFixture, []string{"entity-only"}, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Entity")),
				query.Not(query.KindIn(query.Node(), graph.StringKind("Group"), graph.StringKind("LocalGroup"))),
				query.StringEndsWith(query.NodeProperty("objectid"), "-512"),
			)
		})
	})

	t.Run("LOOKUP-07 missing and null properties", func(t *testing.T) {
		phase5AssertNodeIDs(t, session, basicFixture, []string{"name-missing", "name-null"}, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Lookup")),
				query.Not(query.Exists(query.NodeProperty("name"))),
			)
		})
	})

	t.Run("LOOKUP-08 nullable approver disjunction", func(t *testing.T) {
		phase5AssertNodeIDs(t, session, basicFixture, []string{"role-both", "role-group", "role-user"}, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("AZRole")),
				query.Equals(query.NodeProperty("tenantid"), "tenant-1"),
				query.Equals(query.NodeProperty("approvalrequired"), true),
				query.Or(
					query.IsNotNull(query.NodeProperty("userapprovers")),
					query.IsNotNull(query.NodeProperty("groupapprovers")),
				),
			)
		})
	})

	t.Run("LOOKUP-09 duplicate ID list hydration", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, advancedFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.InIDs(query.NodeID(), idMap["hydrate-a"], idMap["hydrate-a"], idMap["hydrate-b"])
		}, func(nodeQuery graph.NodeQuery, idMap opengraph.IDMap) error {
			nodes, err := ops.FetchNodes(nodeQuery)
			require.NoError(t, err)
			require.Equal(t, []string{"hydrate-a", "hydrate-b"}, phase5FixtureIDs(t, idMap, phase5NodeIDs(nodes)))
			return nil
		})
	})

	t.Run("LOOKUP-10 nested negated account flags", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, advancedFixture, func(idMap opengraph.IDMap) graph.Criteria {
			ids := make([]graph.ID, 0, 16)
			for _, first := range []string{"m", "n", "f", "t"} {
				for _, second := range []string{"m", "n", "f", "t"} {
					ids = append(ids, idMap["flags-"+first+second])
				}
			}
			return query.And(
				query.Kind(query.Node(), graph.StringKind("User")),
				query.Not(query.And(query.Exists(query.NodeProperty("gmsa")), query.Equals(query.NodeProperty("gmsa"), true))),
				query.Not(query.And(query.Exists(query.NodeProperty("msa")), query.Equals(query.NodeProperty("msa"), true))),
				query.InIDs(query.NodeID(), ids...),
			)
		}, func(nodeQuery graph.NodeQuery, _ opengraph.IDMap) error {
			nodes, err := ops.FetchNodes(nodeQuery)
			require.NoError(t, err)
			require.Len(t, nodes, 9)
			return nil
		})
	})

	t.Run("LOOKUP-11 tenant adjacency property list", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, advancedFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), idMap["tenant"]),
				query.Kind(query.Relationship(), graph.StringKind("Contains")),
				query.KindIn(query.End(), graph.StringKind("AZRole"), graph.StringKind("AZServicePrincipal")),
				query.In(query.EndProperty("roletemplateid"), []string{"role-a", "role-b", "role-multi"}),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			nodes, err := ops.FetchEndNodes(relationshipQuery)
			require.NoError(t, err)
			require.Equal(t, 3, nodes.Len())
			return nil
		})
	})

	t.Run("LOOKUP-12 exact edge key First", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, advancedFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), idMap["edge-start"]),
				query.Equals(query.EndID(), idMap["edge-end"]),
				query.Kind(query.Relationship(), graph.StringKind("MemberOf")),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			relationship, err := relationshipQuery.Limit(1).First()
			require.NoError(t, err)
			marker, err := relationship.Properties.Get("marker").String()
			require.NoError(t, err)
			require.Equal(t, "exact-edge", marker)
			return nil
		})
	})

	t.Run("LOOKUP-13 suffix and bound endpoint", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, advancedFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.StringEndsWith(query.StartProperty("objectid"), "-555"),
				query.Kind(query.Relationship(), graph.StringKind("LocalToComputer")),
				query.Equals(query.EndID(), idMap["local-target"]),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			nodes, err := ops.FetchStartNodes(relationshipQuery)
			require.NoError(t, err)
			require.Equal(t, 2, nodes.Len())
			return nil
		})
	})

	t.Run("LOOKUP-14 descending node property", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, advancedFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Node(), graph.StringKind("Domain")),
				query.Exists(query.NodeProperty("name")),
			)
		}, func(nodeQuery graph.NodeQuery, _ opengraph.IDMap) error {
			var names []string
			err := nodeQuery.OrderBy(query.Order(query.NodeProperty("name"), query.Descending())).Fetch(func(cursor graph.Cursor[*graph.Node]) error {
				for node := range cursor.Chan() {
					name, err := node.Properties.Get("name").String()
					require.NoError(t, err)
					names = append(names, name)
				}
				return cursor.Error()
			})
			require.NoError(t, err)
			require.Equal(t, []string{"Gamma", "Beta", "Beta", "Alpha"}, names)
			return nil
		})
	})

	t.Run("LOOKUP-15 direct sequential counts", func(t *testing.T) {
		for _, testCase := range []struct {
			family        string
			expectedNodes int64
			expectedEdges int64
		}{
			{family: "LOOKUP-15 empty graph counts"},
			{family: "LOOKUP-15 node-only graph counts", expectedNodes: 3},
			{family: "LOOKUP-15 edge-bearing graph counts", expectedNodes: 2, expectedEdges: 1},
			{family: "LOOKUP-15 dense graph counts", expectedNodes: 4, expectedEdges: 6},
		} {
			t.Run(testCase.family, func(t *testing.T) {
				fixture := phase4TemplateFixture(t, testCase.family)
				err := session.WithRollbackFixture(t, fixture, false, func(tx graph.Transaction, _ opengraph.IDMap) error {
					nodeCount, err := tx.Nodes().Count()
					require.NoError(t, err)
					edgeCount, err := tx.Relationships().Count()
					require.NoError(t, err)
					require.Equal(t, testCase.expectedNodes, nodeCount)
					require.Equal(t, testCase.expectedEdges, edgeCount)
					return nil
				})
				require.NoError(t, err)
			})
		}
	})

	t.Run("LOOKUP-16 four-property LDAP and LDAPS forms", func(t *testing.T) {
		for _, testCase := range []struct {
			name       string
			kind       graph.Kind
			available  string
			protection string
			expected   string
		}{
			{name: "typed LDAP", kind: graph.StringKind("Computer"), available: "ldapavailable", protection: "ldapsigning", expected: "ntlm-ldap-good"},
			{name: "untyped LDAPS", available: "ldapsavailable", protection: "epa", expected: "ntlm-ldaps-good"},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				phase5AssertNodeIDs(t, session, advancedFixture, []string{testCase.expected}, func(opengraph.IDMap) graph.Criteria {
					criteria := []graph.Criteria{
						query.Equals(query.NodeProperty("domainsid"), "S-1-5-21"),
						query.Equals(query.NodeProperty("isdc"), true),
						query.Equals(query.NodeProperty(testCase.available), true),
						query.Equals(query.NodeProperty(testCase.protection), false),
					}
					if testCase.kind != nil {
						criteria = append([]graph.Criteria{query.Kind(query.Node(), testCase.kind)}, criteria...)
					}
					return query.And(criteria...)
				})
			})
		}
	})
}

func phase5ADCSKinds() graph.Kinds {
	kinds := make(graph.Kinds, 9)
	for idx := range kinds {
		kinds[idx] = graph.StringKind("ADCSEdge0" + string(rune('1'+idx)))
	}
	return kinds
}

func phase5NodeIDs(nodes []*graph.Node) []graph.ID {
	ids := make([]graph.ID, len(nodes))
	for idx, node := range nodes {
		ids[idx] = node.ID
	}
	return ids
}

func phase5FixtureIDs(t *testing.T, idMap opengraph.IDMap, ids []graph.ID) []string {
	t.Helper()
	fixtureIDs := make([]string, len(ids))
	for idx, id := range ids {
		fixtureIDs[idx] = phase1FixtureID(t, idMap, id)
	}
	sort.Strings(fixtureIDs)
	return fixtureIDs
}

func phase5AssertNodeIDs(t *testing.T, session *Session, fixture *opengraph.Graph, expected []string, criteria func(opengraph.IDMap) graph.Criteria) {
	t.Helper()
	WithLegacyNodeQuery(t, session, fixture, criteria, func(nodeQuery graph.NodeQuery, idMap opengraph.IDMap) error {
		ids, err := ops.FetchNodeIDs(nodeQuery)
		require.NoError(t, err)
		require.Equal(t, expected, phase5FixtureIDs(t, idMap, ids))
		return nil
	})
}
