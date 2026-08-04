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
	"sort"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestPhase1LegacyBuilderIntegration(t *testing.T) {
	logicFixture := phase1LogicFixture()
	projectionFixture := phase1ProjectionFixture()
	logicNodeKinds, logicEdgeKinds := logicFixture.Kinds()
	projectionNodeKinds, projectionEdgeKinds := projectionFixture.Kinds()

	db, ctx := SetupDBWithKindsNoGraphCleanup(
		t,
		logicNodeKinds.Add(projectionNodeKinds...),
		logicEdgeKinds.Add(projectionEdgeKinds...),
	)
	ClearGraph(t, db, ctx)
	session := &Session{DB: db, Ctx: ctx}

	t.Run("LOGIC-01 branch-local relationship kinds", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, logicFixture, func(idMap opengraph.IDMap) graph.Criteria {
			forwardID := idMap["direction-forward"]
			reverseID := idMap["direction-reverse"]
			return query.And(
				query.Kind(query.Start(), graph.StringKind("LogicDomain")),
				query.Kind(query.End(), graph.StringKind("LogicDomain")),
				query.Or(
					query.And(
						query.Equals(query.StartID(), forwardID),
						query.Equals(query.EndID(), reverseID),
						query.KindIn(query.Relationship(), graph.StringKind("LogicKindA")),
					),
					query.And(
						query.Equals(query.StartID(), reverseID),
						query.Equals(query.EndID(), forwardID),
						query.KindIn(query.Relationship(), graph.StringKind("LogicKindB")),
					),
				),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			var ids []graph.ID
			err := relationshipQuery.FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
				for id := range cursor.Chan() {
					ids = append(ids, id)
				}
				return cursor.Error()
			})
			require.NoError(t, err)
			require.Len(t, ids, 2, "both invalid kind/direction combinations must remain excluded")
			require.NotEqual(t, ids[0], ids[1])
			return nil
		})
	})

	t.Run("LOGIC-02 cross-binding temporal disjunction", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, logicFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Relationship(), graph.StringKind("LogicStaleTrust")),
				query.Or(
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
					query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
				),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			var markers []string
			err := relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
				for relationship := range cursor.Chan() {
					marker, err := relationship.Properties.Get("marker").String()
					require.NoError(t, err)
					markers = append(markers, marker)
				}
				return cursor.Error()
			})
			require.NoError(t, err)
			sort.Strings(markers)
			require.Equal(t, []string{"older-both", "older-end-only", "older-start-only"}, markers)
			return nil
		})
	})

	t.Run("LOGIC-03 scoped negation and null-aware age predicate", func(t *testing.T) {
		threshold := time.Date(2026, time.January, 3, 0, 0, 0, 0, time.UTC)
		WithLegacyNodeQuery(t, session, logicFixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Not(query.KindIn(query.Node(), graph.StringKind("LogicProtected"))),
				query.Or(
					query.Not(query.Exists(query.NodeProperty("lastseen"))),
					query.Before(query.NodeProperty("lastseen"), threshold),
				),
			)
		}, func(nodeQuery graph.NodeQuery, idMap opengraph.IDMap) error {
			var fixtureIDs []string
			err := nodeQuery.FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
				for id := range cursor.Chan() {
					fixtureIDs = append(fixtureIDs, phase1FixtureID(t, idMap, id))
				}
				return cursor.Error()
			})
			require.NoError(t, err)
			sort.Strings(fixtureIDs)
			require.Equal(t, []string{"candidate-missing", "candidate-null", "candidate-older", "direction-forward", "direction-reverse", "early-a", "early-b", "equal-a", "equal-b", "late-a", "late-b"}, fixtureIDs)
			return nil
		})
	})

	t.Run("LOGIC-05 projection order and Go result types", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, projectionFixture, func(idMap opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Relationship(), graph.StringKind("LogicProjectionEdge")),
				query.Equals(query.StartID(), idMap["projection-start"]),
			)
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			err := relationshipQuery.FetchDirection(graph.DirectionInbound, func(cursor graph.Cursor[graph.DirectionalResult]) error {
				results := make([]graph.DirectionalResult, 0, 1)
				for result := range cursor.Chan() {
					results = append(results, result)
				}
				require.NoError(t, cursor.Error())
				require.Len(t, results, 1)
				require.IsType(t, &graph.Relationship{}, results[0].Relationship)
				require.IsType(t, &graph.Node{}, results[0].Node)
				require.Equal(t, idMap["projection-end"], results[0].Node.ID)
				return nil
			})
			require.NoError(t, err)

			err = relationshipQuery.Query(func(result graph.Result) error {
				require.True(t, result.Next())
				var nodeID, relationshipID graph.ID
				var nodeKinds graph.Kinds
				var relationshipKind graph.Kind
				require.NoError(t, result.Scan(&nodeID, &nodeKinds, &relationshipID, &relationshipKind))
				require.Equal(t, idMap["projection-end"], nodeID)
				require.Equal(t, graph.StringKind("LogicProjectionEdge"), relationshipKind)
				require.Contains(t, nodeKinds, graph.StringKind("LogicProjectionEnd"))
				require.NotZero(t, relationshipID)
				require.False(t, result.Next())
				return result.Error()
			}, query.Returning(
				query.EndID(),
				query.KindsOf(query.End()),
				query.RelationshipID(),
				query.KindsOf(query.Relationship()),
			))
			require.NoError(t, err)

			err = relationshipQuery.FetchTriples(func(cursor graph.Cursor[graph.RelationshipTripleResult]) error {
				triples := make([]graph.RelationshipTripleResult, 0, 1)
				for triple := range cursor.Chan() {
					triples = append(triples, triple)
				}
				require.NoError(t, cursor.Error())
				require.Len(t, triples, 1)
				require.Equal(t, []graph.RelationshipTripleResult{{
					ID:      triples[0].ID,
					StartID: idMap["projection-start"],
					EndID:   idMap["projection-end"],
				}}, triples)
				return nil
			})
			require.NoError(t, err)

			err = relationshipQuery.FetchIDs(func(cursor graph.Cursor[graph.ID]) error {
				ids := make([]graph.ID, 0, 1)
				for id := range cursor.Chan() {
					ids = append(ids, id)
				}
				require.NoError(t, cursor.Error())
				require.Len(t, ids, 1)
				return nil
			})
			require.NoError(t, err)

			err = relationshipQuery.Fetch(func(cursor graph.Cursor[*graph.Relationship]) error {
				relationships := make([]*graph.Relationship, 0, 1)
				for relationship := range cursor.Chan() {
					relationships = append(relationships, relationship)
				}
				require.NoError(t, cursor.Error())
				require.Len(t, relationships, 1)
				require.IsType(t, &graph.Relationship{}, relationships[0])
				return nil
			})
			require.NoError(t, err)
			return nil
		})
	})
}

func phase1LogicFixture() *opengraph.Graph {
	day := func(day int) time.Time {
		return time.Date(2026, time.January, day, 0, 0, 0, 0, time.UTC)
	}

	return &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "direction-forward", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"name": "forward"}},
			{ID: "direction-reverse", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"name": "reverse"}},
			{ID: "early-a", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(2)}},
			{ID: "early-b", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(2)}},
			{ID: "equal-a", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(3)}},
			{ID: "equal-b", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(3)}},
			{ID: "late-a", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(4)}},
			{ID: "late-b", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(4)}},
			{ID: "late-b-newer", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(4), "lastseen": day(4)}},
			{ID: "late-b-missing", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(4), "lastseen": day(4)}},
			{ID: "late-b-null", Kinds: []string{"LogicDomain"}, Properties: map[string]any{"lastcollected": day(4), "lastseen": day(4)}},
			{ID: "candidate-missing", Kinds: []string{"LogicCandidate"}, Properties: map[string]any{}},
			{ID: "candidate-null", Kinds: []string{"LogicCandidate"}, Properties: map[string]any{"lastseen": nil}},
			{ID: "candidate-older", Kinds: []string{"LogicCandidate"}, Properties: map[string]any{"lastseen": day(2)}},
			{ID: "candidate-equal", Kinds: []string{"LogicCandidate"}, Properties: map[string]any{"lastseen": day(3)}},
			{ID: "candidate-newer", Kinds: []string{"LogicCandidate"}, Properties: map[string]any{"lastseen": day(4)}},
			{ID: "protected-missing", Kinds: []string{"LogicProtected"}, Properties: map[string]any{}},
			{ID: "protected-null", Kinds: []string{"LogicProtected"}, Properties: map[string]any{"lastseen": nil}},
			{ID: "protected-older", Kinds: []string{"LogicProtected"}, Properties: map[string]any{"lastseen": day(2)}},
			{ID: "multi-kind-protected", Kinds: []string{"LogicCandidate", "LogicProtected"}, Properties: map[string]any{"lastseen": day(2)}},
		},
		Edges: []opengraph.Edge{
			{StartID: "direction-forward", EndID: "direction-reverse", Kind: "LogicKindA", Properties: map[string]any{"marker": "valid-forward"}},
			{StartID: "direction-reverse", EndID: "direction-forward", Kind: "LogicKindB", Properties: map[string]any{"marker": "valid-reverse"}},
			{StartID: "direction-forward", EndID: "direction-reverse", Kind: "LogicKindB", Properties: map[string]any{"marker": "invalid-forward-kind"}},
			{StartID: "direction-reverse", EndID: "direction-forward", Kind: "LogicKindA", Properties: map[string]any{"marker": "invalid-reverse-kind"}},
			{StartID: "late-a", EndID: "early-a", Kind: "LogicStaleTrust", Properties: map[string]any{"lastseen": day(3), "marker": "older-start-only"}},
			{StartID: "early-a", EndID: "late-a", Kind: "LogicStaleTrust", Properties: map[string]any{"lastseen": day(3), "marker": "older-end-only"}},
			{StartID: "late-a", EndID: "late-b", Kind: "LogicStaleTrust", Properties: map[string]any{"lastseen": day(3), "marker": "older-both"}},
			{StartID: "equal-a", EndID: "equal-b", Kind: "LogicStaleTrust", Properties: map[string]any{"lastseen": day(3), "marker": "equal"}},
			{StartID: "late-a", EndID: "late-b-newer", Kind: "LogicStaleTrust", Properties: map[string]any{"lastseen": day(5), "marker": "newer"}},
			{StartID: "late-a", EndID: "late-b-missing", Kind: "LogicStaleTrust", Properties: map[string]any{"marker": "missing"}},
			{StartID: "late-a", EndID: "late-b-null", Kind: "LogicStaleTrust", Properties: map[string]any{"lastseen": nil, "marker": "null"}},
		},
	}
}

func phase1ProjectionFixture() *opengraph.Graph {
	return &opengraph.Graph{
		Nodes: []opengraph.Node{
			{ID: "projection-start", Kinds: []string{"LogicProjectionStart"}, Properties: map[string]any{"name": "start"}},
			{ID: "projection-end", Kinds: []string{"LogicProjectionEnd", "LogicProjectionEntity"}, Properties: map[string]any{"name": "end"}},
		},
		Edges: []opengraph.Edge{
			{StartID: "projection-start", EndID: "projection-end", Kind: "LogicProjectionEdge", Properties: map[string]any{"marker": "projection"}},
		},
	}
}

func phase1FixtureID(t *testing.T, idMap opengraph.IDMap, id graph.ID) string {
	t.Helper()
	for fixtureID, databaseID := range idMap {
		if databaseID == id {
			return fixtureID
		}
	}
	t.Fatalf("database ID %d is absent from fixture ID map", id)
	return ""
}
