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

//go:build manual_integration

package integration

import (
	"fmt"
	"sort"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestPhase4LegacyBuilderIntegration(t *testing.T) {
	anchorFixture := phase4TemplateFixture(t, "HOP-01 through HOP-03 anchored direction and relationship-kind cardinality")
	idFixture := phase4TemplateFixture(t, "HOP-04 and HOP-05 endpoint kinds and ID constraints")
	predicateFixture := phase4TemplateFixture(t, "HOP-06 through HOP-08 scalar nested and collection endpoint predicates")
	projectionFixture := phase4TemplateFixture(t, "HOP-09 and HOP-10 two-sided sets and directional projections")

	var nodeKinds, edgeKinds graph.Kinds
	for _, fixture := range []*opengraph.Graph{anchorFixture, idFixture, predicateFixture, projectionFixture} {
		nextNodeKinds, nextEdgeKinds := fixture.Kinds()
		nodeKinds = nodeKinds.Add(nextNodeKinds...)
		edgeKinds = edgeKinds.Add(nextEdgeKinds...)
	}
	db, ctx := SetupDBWithKindsNoGraphCleanup(t, nodeKinds, edgeKinds)
	ClearGraph(t, db, ctx)
	session := &Session{DB: db, Ctx: ctx}

	t.Run("HOP-01 outbound full direction", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, anchorFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), idMap["out-one"]),
				query.Kind(query.Relationship(), graph.StringKind("HopKind01")),
			)
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			return relationshipQuery.FetchDirection(graph.DirectionInbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
				results := phase4DirectionalResults(t, cursor)
				require.Len(t, results, 1)
				require.Equal(t, idMap["out-one-target"], results[0].Node.ID)
				return nil
			})
		})
	})

	t.Run("HOP-02 inbound full direction", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, anchorFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.EndID(), idMap["in-one"]),
				query.Kind(query.Relationship(), graph.StringKind("HopKind01")),
			)
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			return relationshipQuery.FetchDirection(graph.DirectionOutbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
				results := phase4DirectionalResults(t, cursor)
				require.Len(t, results, 1)
				require.Equal(t, idMap["in-one-source"], results[0].Node.ID)
				return nil
			})
		})
	})

	t.Run("HOP-03 thirty kinds preserve anchor orientation", func(t *testing.T) {
		kinds := make(graph.Kinds, 30)
		for idx := range kinds {
			kinds[idx] = graph.StringKind(fmt.Sprintf("HopKind%02d", idx+1))
		}
		WithLegacyRelationshipQuery(t, session, anchorFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.InIDs(query.StartID(), idMap["kind-center"]),
				query.KindIn(query.Relationship(), kinds...),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			relationships, err := ops.FetchRelationships(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, relationships, 30)
			require.NotContains(t, phase4RelationshipMarkers(t, relationships), "out-disallowed")
			return nil
		})
	})

	t.Run("HOP-04 endpoint kind disjunction", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, idFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.InIDs(query.StartID(), idMap["root"]),
				query.Kind(query.Relationship(), graph.StringKind("HopTypedEdge")),
				query.KindIn(query.End(), graph.StringKind("HopEndA"), graph.StringKind("HopEndB")),
			)
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			nodes, err := ops.FetchEndNodes(relationshipQuery)
			require.NoError(t, err)
			require.Equal(t, 3, nodes.Len())
			require.True(t, nodes.ContainsID(idMap["typed-a"]))
			require.True(t, nodes.ContainsID(idMap["typed-b"]))
			require.True(t, nodes.ContainsID(idMap["typed-multi"]))
			return nil
		})
	})

	t.Run("HOP-05 endpoint IDs and traversal anchor contradiction", func(t *testing.T) {
		for _, testCase := range []struct {
			name        string
			allowedRoot string
			expected    []string
		}{
			{name: "matching", allowedRoot: "root", expected: []string{"id-a", "id-b"}},
			{name: "contradictory", allowedRoot: "other-root", expected: nil},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				WithLegacyRelationshipQuery(t, session, idFixture, func(idMap opengraph.IDMap) graph.Criteria {
					return query.And(
						query.Equals(query.StartID(), idMap["root"]),
						query.InIDs(query.Start(), idMap[testCase.allowedRoot]),
						query.InIDs(query.EndID(), idMap["id-a"], idMap["id-b"]),
						query.Kind(query.Relationship(), graph.StringKind("HopIDEdge")),
					)
				}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
					relationships, err := ops.FetchRelationships(relationshipQuery)
					require.NoError(t, err)
					require.Equal(t, testCase.expected, phase4RelationshipMarkers(t, relationships))
					return nil
				})
			})
		}
	})

	t.Run("HOP-06 scalar property", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, predicateFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), idMap["root"]),
				query.Kind(query.Relationship(), graph.StringKind("HopPropertyEdge")),
				query.Equals(query.EndProperty("enabled"), true),
				query.Equals(query.EndProperty("score"), 7),
				query.Equals(query.EndProperty("value"), "alpha"),
				query.Equals(query.EndProperty("isassignabletorole"), "true"),
			)
		}, phase4AssertRelationshipMarkers(t, []string{"scalar-match"}))
	})

	t.Run("HOP-07 nested branch-local predicate", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, predicateFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), idMap["root"]),
				query.Kind(query.Relationship(), graph.StringKind("HopNestedEdge")),
				query.Kind(query.End(), graph.StringKind("HopTemplate")),
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
			)
		}, phase4AssertRelationshipMarkers(t, []string{"nested-v1", "nested-v2"}))
	})

	t.Run("HOP-08 collection OR scalar predicate", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, predicateFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), idMap["root"]),
				query.Kind(query.Relationship(), graph.StringKind("HopCollectionEdge")),
				query.Or(
					query.Equals(query.EndProperty("schannelauthenticationenabled"), true),
					query.Equals(query.Size(query.EndProperty("effectiveekus")), 0),
					query.InInverted(query.EndProperty("effectiveekus"), "1.3.6.1.5.5.7.3.2"),
				),
			)
		}, phase4AssertRelationshipMarkers(t, []string{"collection-client", "collection-empty", "collection-scalar"}))
	})

	t.Run("HOP-09 two-sided ID lists", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, projectionFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.InIDs(query.StartID(), idMap["s1"], idMap["s2"]),
				query.InIDs(query.EndID(), idMap["e1"], idMap["e2"]),
				query.Kind(query.Relationship(), graph.StringKind("HopSetEdge")),
			)
		}, phase4AssertRelationshipMarkers(t, []string{"s1-e1", "s1-e2", "s2-e1", "s2-e2"}))
	})

	t.Run("HOP-10 both full directional projections", func(t *testing.T) {
		t.Run("outbound", func(t *testing.T) {
			WithLegacyRelationshipQuery(t, session, projectionFixture, func(idMap opengraph.IDMap) graph.Criteria {
				return query.And(
					query.InIDs(query.StartID(), idMap["s1"]),
					query.Kind(query.Relationship(), graph.StringKind("HopProjectionEdge")),
					query.Kind(query.End(), graph.StringKind("HopProjectionEnd")),
					query.Equals(query.EndProperty("active"), true),
				)
			}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
				return relationshipQuery.FetchDirection(graph.DirectionInbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
					results := phase4DirectionalResults(t, cursor)
					require.Len(t, results, 1)
					require.Equal(t, idMap["e1"], results[0].Node.ID)
					return nil
				})
			})
		})

		t.Run("inbound", func(t *testing.T) {
			WithLegacyRelationshipQuery(t, session, projectionFixture, func(idMap opengraph.IDMap) graph.Criteria {
				return query.And(
					query.InIDs(query.EndID(), idMap["e1"]),
					query.Kind(query.Relationship(), graph.StringKind("HopProjectionEdge")),
					query.Kind(query.Start(), graph.StringKind("HopProjectionStart")),
					query.Equals(query.StartProperty("active"), true),
				)
			}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
				return relationshipQuery.FetchDirection(graph.DirectionOutbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
					results := phase4DirectionalResults(t, cursor)
					require.Len(t, results, 1)
					require.Equal(t, idMap["s1"], results[0].Node.ID)
					return nil
				})
			})
		})
	})
}

func phase4TemplateFixture(t *testing.T, familyName string) *opengraph.Graph {
	t.Helper()
	for _, templateFile := range loadCypherTemplateFiles(t) {
		for _, family := range templateFile.Families {
			if family.Name == familyName {
				return family.Fixture
			}
		}
	}
	t.Fatalf("template family %q not found", familyName)
	return nil
}

func phase4DirectionalResults(t *testing.T, cursor graph.Cursor[graph.DirectionalResult]) []graph.DirectionalResult {
	t.Helper()
	var results []graph.DirectionalResult
	for result := range cursor.Chan() {
		results = append(results, result)
	}
	require.NoError(t, cursor.Error())
	return results
}

func phase4RelationshipMarkers(t *testing.T, relationships []*graph.Relationship) []string {
	t.Helper()
	markers := make([]string, 0, len(relationships))
	for _, relationship := range relationships {
		marker, err := relationship.Properties.Get("marker").String()
		require.NoError(t, err)
		markers = append(markers, marker)
	}
	sort.Strings(markers)
	return markers
}

func phase4AssertRelationshipMarkers(t *testing.T, expected []string) func(graph.RelationshipQuery, opengraph.IDMap) error {
	t.Helper()
	return func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
		relationships, err := ops.FetchRelationships(relationshipQuery)
		require.NoError(t, err)
		require.Equal(t, expected, phase4RelationshipMarkers(t, relationships))
		return nil
	}
}
