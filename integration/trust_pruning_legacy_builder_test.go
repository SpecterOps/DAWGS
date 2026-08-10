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
	"context"
	"sort"
	"testing"
	"time"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

func TestLegacyBuilderTrustAndPruningSelectors(t *testing.T) {
	fixture := trustPruningFixture()
	nodeKinds, edgeKinds := fixture.Kinds()
	db, ctx := SetupDBWithKindsNoGraphCleanup(t, nodeKinds, edgeKinds)
	ClearGraph(t, db, ctx)
	session := &Session{
		DB:  db,
		Ctx: ctx,
	}
	threshold := regressionDay(3)

	t.Run("TRUST-01 SameForestTrust IDs", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, fixture, func(opengraph.IDMap) graph.Criteria {
			return trustPruningCriteria("SameForestTrust")
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			ids, err := ops.FetchRelationshipIDs(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, ids, 1)
			return nil
		})
	})

	t.Run("TRUST-02 CrossForestTrust hydration", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, fixture, func(opengraph.IDMap) graph.Criteria {
			return trustPruningCriteria("CrossForestTrust")
		}, func(relationshipQuery graph.RelationshipQuery, idMap opengraph.IDMap) error {
			relationships, err := ops.FetchRelationships(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, relationships, 1)
			require.Equal(t, idMap["late-a"], relationships[0].StartID)
			require.Equal(t, idMap["early"], relationships[0].EndID)
			require.Equal(t, graph.StringKind("CrossForestTrust"), relationships[0].Kind)
			marker, err := relationships[0].Properties.Get("marker").String()
			require.NoError(t, err)
			require.Equal(t, "cross-old", marker)
			return nil
		})
	})

	t.Run("TRUST-03 directional derived IDs", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, fixture, func(idMap opengraph.IDMap) graph.Criteria {
			return directionalTrustCriteria(idMap, "late-a", "late-b")
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			relationships, err := ops.FetchRelationships(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, relationships, 2)
			markers := make([]string, 0, len(relationships))
			for _, relationship := range relationships {
				marker, err := relationship.Properties.Get("marker").String()
				require.NoError(t, err)
				markers = append(markers, marker)
			}
			sort.Strings(markers)
			require.Equal(t, []string{"valid-forward-abuse", "valid-reverse-spoof"}, markers)
			return nil
		})
	})

	t.Run("TRUST-03 reverse driving trust relationship", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, fixture, func(idMap opengraph.IDMap) graph.Criteria {
			return directionalTrustCriteria(idMap, "late-b", "late-a")
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			relationships, err := ops.FetchRelationships(relationshipQuery)
			require.NoError(t, err)
			require.Len(t, relationships, 2)
			require.Equal(t, []string{"invalid-forward-spoof", "invalid-reverse-abuse"}, trustPruningRelationshipMarkers(t, relationships))
			return nil
		})
	})

	t.Run("PRUNE-01 protected kinds and old relationships", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, fixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Not(query.KindIn(query.Relationship(), graph.StringKind("HasSession"), graph.StringKind("MetaIncludes"))),
				query.Before(query.RelationshipProperty("lastseen"), threshold),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			relationships, err := ops.FetchRelationships(relationshipQuery)
			require.NoError(t, err)
			require.Equal(t, []string{"candidate-old"}, trustPruningRelationshipMarkers(t, relationships))
			return nil
		})
	})

	t.Run("PRUNE-02 HasSession missing null or old", func(t *testing.T) {
		WithLegacyRelationshipQuery(t, session, fixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Kind(query.Relationship(), graph.StringKind("HasSession")),
				query.Or(
					query.Not(query.Exists(query.RelationshipProperty("lastseen"))),
					query.Before(query.RelationshipProperty("lastseen"), threshold),
				),
			)
		}, func(relationshipQuery graph.RelationshipQuery, _ opengraph.IDMap) error {
			relationships, err := ops.FetchRelationships(relationshipQuery)
			require.NoError(t, err)
			require.Equal(t, []string{"session-missing", "session-null", "session-old"}, trustPruningRelationshipMarkers(t, relationships))
			return nil
		})
	})

	t.Run("PRUNE-03 protected kinds and missing null or old nodes", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, fixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Not(query.KindIn(query.Node(), pruningProtectedNodeKinds()...)),
				query.Or(
					query.Not(query.Exists(query.NodeProperty("lastseen"))),
					query.Before(query.NodeProperty("lastseen"), threshold),
				),
			)
		}, func(nodeQuery graph.NodeQuery, idMap opengraph.IDMap) error {
			ids, err := ops.FetchNodeIDs(nodeQuery)
			require.NoError(t, err)
			require.Equal(t, []string{"candidate-missing", "candidate-null", "candidate-old", "orphan-empty", "orphan-missing", "orphan-null", "orphan-wrong-prefix"}, trustPruningFixtureIDs(t, idMap, ids))
			return nil
		})
	})

	t.Run("PRUNE-04 orphan SID nodes", func(t *testing.T) {
		WithLegacyNodeQuery(t, session, fixture, func(opengraph.IDMap) graph.Criteria {
			return query.And(
				query.Not(query.KindIn(query.Node(), pruningProtectedNodeKinds()...)),
				query.Not(query.Exists(query.NodeProperty("name"))),
				query.StringStartsWith(query.NodeProperty("objectid"), "S-1-5"),
			)
		}, func(nodeQuery graph.NodeQuery, idMap opengraph.IDMap) error {
			ids, err := ops.FetchNodeIDs(nodeQuery)
			require.NoError(t, err)
			require.Equal(t, []string{"orphan-missing", "orphan-null"}, trustPruningFixtureIDs(t, idMap, ids))
			return nil
		})
	})
}

func TestDirectBatchPruning(t *testing.T) {
	fixture := batchPruningFixture(32)
	nodeKinds, edgeKinds := fixture.Kinds()
	db, ctx := SetupDBWithKinds(t, CleanupGraph, nodeKinds, edgeKinds)

	loadFixture := func(t *testing.T) opengraph.IDMap {
		t.Helper()
		ClearGraph(t, db, ctx)
		idMap, err := opengraph.WriteGraph(ctx, db, fixture)
		require.NoError(t, err)
		return idMap
	}

	t.Run("PRUNE-05 empty single and many relationships", func(t *testing.T) {
		for _, testCase := range []struct {
			name      string
			criteria  graph.CriteriaProvider
			expected  int
			remaining int64
		}{
			{
				name:      "empty",
				criteria:  func() graph.Criteria { return query.Equals(query.RelationshipProperty("marker"), "absent") },
				expected:  0,
				remaining: 3,
			},
			{
				name:      "single",
				criteria:  func() graph.Criteria { return query.Equals(query.RelationshipProperty("marker"), "single") },
				expected:  1,
				remaining: 2,
			},
			{
				name:      "many",
				criteria:  func() graph.Criteria { return query.Kind(query.Relationship(), graph.StringKind("PruneDelete")) },
				expected:  3,
				remaining: 0,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				loadFixture(t)
				deleted, err := pruneRelationshipsInBatches(ctx, db, testCase.criteria, nil)
				require.NoError(t, err)
				require.Equal(t, testCase.expected, deleted)
				require.Equal(t, testCase.remaining, countByCypher(t, ctx, db, "MATCH ()-[r:PruneDelete]->() RETURN count(r)"))
				require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:PruneSurvivor]->() RETURN count(r)"))
			})
		}
	})

	t.Run("PRUNE-05 relationship absent after selection is harmless", func(t *testing.T) {
		loadFixture(t)
		deleted, err := pruneRelationshipsInBatches(ctx, db, func() graph.Criteria {
			return query.Equals(query.RelationshipProperty("marker"), "single")
		}, func(ids []graph.ID) error {
			return db.BatchOperation(ctx, func(batch graph.Batch) error {
				return batch.DeleteRelationship(ids[0])
			})
		})
		require.NoError(t, err)
		require.Equal(t, 1, deleted, "the production workflow counts accepted delete attempts")
		require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH ()-[r:PruneDelete]->() RETURN count(r)"))
	})

	t.Run("PRUNE-06 empty single many and high-degree nodes", func(t *testing.T) {
		for _, testCase := range []struct {
			name               string
			criteria           graph.CriteriaProvider
			expected           int
			expectedCandidates int64
			expectedIncidents  int64
		}{
			{
				name:               "empty",
				criteria:           func() graph.Criteria { return query.Equals(query.NodeProperty("objectid"), "absent") },
				expected:           0,
				expectedCandidates: 3,
				expectedIncidents:  34,
			},
			{
				name:               "single",
				criteria:           func() graph.Criteria { return query.Equals(query.NodeProperty("objectid"), "single") },
				expected:           1,
				expectedCandidates: 2,
				expectedIncidents:  34,
			},
			{
				name:               "many including high degree",
				criteria:           func() graph.Criteria { return query.Equals(query.NodeProperty("remove"), true) },
				expected:           2,
				expectedCandidates: 1,
				expectedIncidents:  1,
			},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				loadFixture(t)
				deleted, err := pruneNodesInBatches(ctx, db, testCase.criteria, nil)
				require.NoError(t, err)
				require.Equal(t, testCase.expected, deleted)
				require.Equal(t, testCase.expectedCandidates, countByCypher(t, ctx, db, "MATCH (n:PruneDeleteNode) RETURN count(n)"))
				require.Equal(t, testCase.expectedIncidents, countByCypher(t, ctx, db, "MATCH ()-[r:PruneIncident]->() RETURN count(r)"))
			})
		}
	})

	t.Run("PRUNE-06 node absent after selection is harmless", func(t *testing.T) {
		loadFixture(t)
		deleted, err := pruneNodesInBatches(ctx, db, func() graph.Criteria {
			return query.Equals(query.NodeProperty("objectid"), "single")
		}, func(ids []graph.ID) error {
			return db.BatchOperation(ctx, func(batch graph.Batch) error {
				return batch.DeleteNode(ids[0])
			})
		})
		require.NoError(t, err)
		require.Equal(t, 1, deleted)
		require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH (n:PruneDeleteNode) RETURN count(n)"))
	})
}

func BenchmarkDirectBatchPruning(b *testing.B) {
	fixture := testutil.NewTrustPruningScaleFixture(2_000)
	nodeKinds, edgeKinds := fixture.Kinds()
	session := Open(b, Options{
		ExtraNodeKinds: nodeKinds,
		ExtraEdgeKinds: edgeKinds,
		CleanupMode:    CloseOnly,
	})

	resetFixture := func(b *testing.B) {
		b.Helper()
		if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
			return tx.Nodes().Delete()
		}); err != nil {
			b.Fatalf("clear benchmark graph: %v", err)
		}
		if _, err := opengraph.WriteGraph(session.Ctx, session.DB, fixture); err != nil {
			b.Fatalf("load benchmark fixture: %v", err)
		}
	}

	b.Run("PRUNE-05 relationship ID selection and batch delete", func(b *testing.B) {
		b.ReportAllocs()
		for idx := 0; idx < b.N; idx++ {
			b.StopTimer()
			resetFixture(b)
			b.StartTimer()
			deleted, err := pruneRelationshipsInBatches(session.Ctx, session.DB, func() graph.Criteria {
				return query.Kind(query.Relationship(), graph.StringKind("PruneBatch"))
			}, nil)
			if err != nil {
				b.Fatalf("prune relationships: %v", err)
			}
			if deleted != 2_000 {
				b.Fatalf("deleted relationships: got %d, want 2000", deleted)
			}
		}
	})

	b.Run("PRUNE-06 node ID selection high-degree cascade and batch delete", func(b *testing.B) {
		b.ReportAllocs()
		for idx := 0; idx < b.N; idx++ {
			b.StopTimer()
			resetFixture(b)
			b.StartTimer()
			deleted, err := pruneNodesInBatches(session.Ctx, session.DB, func() graph.Criteria {
				return query.Equals(query.NodeProperty("remove"), true)
			}, nil)
			if err != nil {
				b.Fatalf("prune nodes: %v", err)
			}
			if deleted != 1_001 {
				b.Fatalf("deleted nodes: got %d, want 1001", deleted)
			}
		}
	})
}

func trustPruningCriteria(kind string) graph.Criteria {
	return query.And(
		query.Kind(query.Start(), graph.StringKind("Domain")),
		query.Kind(query.End(), graph.StringKind("Domain")),
		query.KindIn(query.Relationship(), graph.StringKind(kind)),
		query.Or(
			query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.StartProperty("lastcollected")),
			query.BeforeGraphQuery(query.RelationshipProperty("lastseen"), query.EndProperty("lastcollected")),
		),
	)
}

func directionalTrustCriteria(idMap opengraph.IDMap, forward, reverse string) graph.Criteria {
	forwardID := idMap[forward]
	reverseID := idMap[reverse]
	return query.And(
		query.Kind(query.Start(), graph.StringKind("Domain")),
		query.Kind(query.End(), graph.StringKind("Domain")),
		query.Or(
			query.And(
				query.Equals(query.StartID(), forwardID),
				query.Equals(query.EndID(), reverseID),
				query.KindIn(query.Relationship(), graph.StringKind("AbuseTGTDelegation")),
			),
			query.And(
				query.Equals(query.StartID(), reverseID),
				query.Equals(query.EndID(), forwardID),
				query.KindIn(query.Relationship(), graph.StringKind("SpoofSIDHistory")),
			),
		),
	)
}

func pruningProtectedNodeKinds() graph.Kinds {
	return graph.Kinds{
		graph.StringKind("Domain"),
		graph.StringKind("Tenant"),
		graph.StringKind("Meta"),
		graph.StringKind("MetaIncludes"),
		graph.StringKind("MigrationData"),
	}
}

func trustPruningRelationshipMarkers(t *testing.T, relationships []*graph.Relationship) []string {
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

func trustPruningFixtureIDs(t *testing.T, idMap opengraph.IDMap, ids []graph.ID) []string {
	t.Helper()
	fixtureIDs := make([]string, 0, len(ids))
	for _, id := range ids {
		fixtureIDs = append(fixtureIDs, regressionFixtureID(t, idMap, id))
	}
	sort.Strings(fixtureIDs)
	return fixtureIDs
}

func trustPruningFixture() *opengraph.Graph {
	return &opengraph.Graph{
		Nodes: []opengraph.Node{
			{
				ID:         "early",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(2)},
			},
			{
				ID:         "late-a",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "late-b",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "candidate-rel-equal",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "candidate-rel-new",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "candidate-rel-missing",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "candidate-rel-null",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "session-null",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "session-old",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "session-equal",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "session-new",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(4)},
			},
			{
				ID:         "equal-a",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(3)},
			},
			{
				ID:         "equal-b",
				Kinds:      []string{"Domain"},
				Properties: map[string]any{"lastcollected": regressionDay(3)},
			},
			{
				ID:         "wrong-end",
				Kinds:      []string{"Computer"},
				Properties: map[string]any{"lastcollected": regressionDay(4), "lastseen": regressionDay(4)},
			},
			{
				ID:         "candidate-missing",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{},
			},
			{
				ID:         "candidate-null",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"lastseen": nil},
			},
			{
				ID:         "candidate-old",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"lastseen": regressionDay(2)},
			},
			{
				ID:         "candidate-equal",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"lastseen": regressionDay(3)},
			},
			{
				ID:         "candidate-new",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"lastseen": regressionDay(4)},
			},
			{
				ID:         "orphan-missing",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"objectid": "S-1-5-100"},
			},
			{
				ID:         "orphan-null",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"name": nil, "objectid": "S-1-5-101"},
			},
			{
				ID:         "orphan-empty",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"name": "", "objectid": "S-1-5-102"},
			},
			{
				ID:         "orphan-wrong-prefix",
				Kinds:      []string{"CandidateNode"},
				Properties: map[string]any{"objectid": "X-1-5-103"},
			},
			{
				ID:         "orphan-protected",
				Kinds:      []string{"CandidateNode", "Domain"},
				Properties: map[string]any{"objectid": "S-1-5-104"},
			},
		},
		Edges: []opengraph.Edge{
			{
				StartID:    "late-a",
				EndID:      "early",
				Kind:       "SameForestTrust",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "same-old"},
			},
			{
				StartID:    "equal-a",
				EndID:      "equal-b",
				Kind:       "SameForestTrust",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "same-equal"},
			},
			{
				StartID:    "late-a",
				EndID:      "wrong-end",
				Kind:       "SameForestTrust",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "same-wrong-end"},
			},
			{
				StartID:    "late-a",
				EndID:      "early",
				Kind:       "CrossForestTrust",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "cross-old"},
			},
			{
				StartID:    "equal-a",
				EndID:      "equal-b",
				Kind:       "CrossForestTrust",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "cross-equal"},
			},
			{
				StartID:    "late-a",
				EndID:      "late-b",
				Kind:       "AbuseTGTDelegation",
				Properties: map[string]any{"marker": "valid-forward-abuse"},
			},
			{
				StartID:    "late-b",
				EndID:      "late-a",
				Kind:       "SpoofSIDHistory",
				Properties: map[string]any{"marker": "valid-reverse-spoof"},
			},
			{
				StartID:    "late-a",
				EndID:      "late-b",
				Kind:       "SpoofSIDHistory",
				Properties: map[string]any{"marker": "invalid-forward-spoof"},
			},
			{
				StartID:    "late-b",
				EndID:      "late-a",
				Kind:       "AbuseTGTDelegation",
				Properties: map[string]any{"marker": "invalid-reverse-abuse"},
			},
			{
				StartID:    "late-a",
				EndID:      "late-b",
				Kind:       "CandidateRel",
				Properties: map[string]any{"lastseen": regressionDay(2), "marker": "candidate-old"},
			},
			{
				StartID:    "late-a",
				EndID:      "candidate-rel-equal",
				Kind:       "CandidateRel",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "candidate-equal"},
			},
			{
				StartID:    "late-a",
				EndID:      "candidate-rel-new",
				Kind:       "CandidateRel",
				Properties: map[string]any{"lastseen": regressionDay(4), "marker": "candidate-new"},
			},
			{
				StartID:    "late-a",
				EndID:      "candidate-rel-missing",
				Kind:       "CandidateRel",
				Properties: map[string]any{"marker": "candidate-missing"},
			},
			{
				StartID:    "late-a",
				EndID:      "candidate-rel-null",
				Kind:       "CandidateRel",
				Properties: map[string]any{"lastseen": nil, "marker": "candidate-null"},
			},
			{
				StartID:    "late-a",
				EndID:      "late-b",
				Kind:       "HasSession",
				Properties: map[string]any{"marker": "session-missing"},
			},
			{
				StartID:    "late-a",
				EndID:      "session-null",
				Kind:       "HasSession",
				Properties: map[string]any{"lastseen": nil, "marker": "session-null"},
			},
			{
				StartID:    "late-a",
				EndID:      "session-old",
				Kind:       "HasSession",
				Properties: map[string]any{"lastseen": regressionDay(2), "marker": "session-old"},
			},
			{
				StartID:    "late-a",
				EndID:      "session-equal",
				Kind:       "HasSession",
				Properties: map[string]any{"lastseen": regressionDay(3), "marker": "session-equal"},
			},
			{
				StartID:    "late-a",
				EndID:      "session-new",
				Kind:       "HasSession",
				Properties: map[string]any{"lastseen": regressionDay(4), "marker": "session-new"},
			},
			{
				StartID:    "late-a",
				EndID:      "late-b",
				Kind:       "MetaIncludes",
				Properties: map[string]any{"lastseen": regressionDay(2), "marker": "meta-includes-old"},
			},
		},
	}
}

func batchPruningFixture(fanout int) *opengraph.Graph {
	fixture := &opengraph.Graph{
		Nodes: []opengraph.Node{
			{
				ID:         "rel-a",
				Kinds:      []string{"PruneEndpoint"},
				Properties: map[string]any{"name": "rel-a"},
			},
			{
				ID:         "rel-b",
				Kinds:      []string{"PruneEndpoint"},
				Properties: map[string]any{"name": "rel-b"},
			},
			{
				ID:         "rel-c",
				Kinds:      []string{"PruneEndpoint"},
				Properties: map[string]any{"name": "rel-c"},
			},
			{
				ID:         "single",
				Kinds:      []string{"PruneDeleteNode"},
				Properties: map[string]any{"objectid": "single", "remove": true},
			},
			{
				ID:         "high",
				Kinds:      []string{"PruneDeleteNode"},
				Properties: map[string]any{"objectid": "high", "remove": true},
			},
			{
				ID:         "survivor",
				Kinds:      []string{"PruneDeleteNode"},
				Properties: map[string]any{"objectid": "survivor", "remove": false},
			},
		},
		Edges: []opengraph.Edge{
			{
				StartID:    "rel-a",
				EndID:      "rel-b",
				Kind:       "PruneDelete",
				Properties: map[string]any{"marker": "single"},
			},
			{
				StartID:    "rel-a",
				EndID:      "rel-c",
				Kind:       "PruneDelete",
				Properties: map[string]any{"marker": "many-a"},
			},
			{
				StartID:    "rel-b",
				EndID:      "rel-a",
				Kind:       "PruneDelete",
				Properties: map[string]any{"marker": "many-b"},
			},
			{
				StartID:    "rel-a",
				EndID:      "rel-b",
				Kind:       "PruneSurvivor",
				Properties: map[string]any{"marker": "survivor"},
			},
			{
				StartID:    "survivor",
				EndID:      "rel-a",
				Kind:       "PruneIncident",
				Properties: map[string]any{"marker": "survivor-incident"},
			},
			{
				StartID:    "high",
				EndID:      "high",
				Kind:       "PruneIncident",
				Properties: map[string]any{"marker": "high-self"},
			},
		},
	}

	for idx, neighborID := range FixtureNames("neighbor", fanout) {
		fixture.Nodes = append(fixture.Nodes, opengraph.Node{
			ID:         neighborID,
			Kinds:      []string{"PruneNeighbor"},
			Properties: map[string]any{"name": neighborID},
		})
		startID, endID := "high", neighborID
		if idx%2 == 0 {
			startID, endID = neighborID, "high"
		}
		fixture.Edges = append(fixture.Edges, opengraph.Edge{
			StartID:    startID,
			EndID:      endID,
			Kind:       "PruneIncident",
			Properties: map[string]any{"marker": neighborID},
		})
	}
	return fixture
}

func pruneRelationshipsInBatches(ctx context.Context, db graph.Database, criteria graph.CriteriaProvider, afterSelect func([]graph.ID) error) (int, error) {
	var ids []graph.ID
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		ids, err = ops.FetchRelationshipIDs(tx.Relationships().Filterf(criteria))
		return err
	}); err != nil {
		return 0, err
	}
	if afterSelect != nil {
		if err := afterSelect(ids); err != nil {
			return 0, err
		}
	}

	deleted := 0
	if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		for _, id := range ids {
			if err := batch.DeleteRelationship(id); err != nil {
				return err
			}

			deleted++
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return deleted, nil
}

func pruneNodesInBatches(ctx context.Context, db graph.Database, criteria graph.CriteriaProvider, afterSelect func([]graph.ID) error) (int, error) {
	var ids []graph.ID
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		ids, err = ops.FetchNodeIDs(tx.Nodes().Filterf(criteria))
		return err
	}); err != nil {
		return 0, err
	}
	if afterSelect != nil {
		if err := afterSelect(ids); err != nil {
			return 0, err
		}
	}

	deleted := 0
	if err := db.BatchOperation(ctx, func(batch graph.Batch) error {
		for _, id := range ids {
			if err := batch.DeleteNode(id); err != nil {
				return err
			}

			deleted++
		}
		return nil
	}); err != nil {
		return 0, err
	}

	return deleted, nil
}

func regressionDay(day int) time.Time {
	return time.Date(2026, time.January, day, 0, 0, 0, 0, time.UTC)
}
