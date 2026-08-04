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
	"fmt"
	"math"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/opengraph"
	"github.com/specterops/dawgs/ops"
	"github.com/specterops/dawgs/query"
	"github.com/specterops/dawgs/testutil"
	"github.com/stretchr/testify/require"
)

const (
	phase6ObjectID = "objectid"
	phase6LastSeen = "lastseen"
)

var (
	phase6DeleteRelationshipKind  = graph.StringKind("WriteDeleteRelationship")
	phase6CreateRelationshipKind  = graph.StringKind("WriteCreateRelationship")
	phase6CreateRelationshipOther = graph.StringKind("WriteCreateRelationshipOther")
	phase6UpsertNodeKind          = graph.StringKind("WriteUpsertNode")
	phase6UpsertNodeKindA         = graph.StringKind("WriteUpsertNodeA")
	phase6UpsertNodeKindB         = graph.StringKind("WriteUpsertNodeB")
	phase6UpsertNodeKindC         = graph.StringKind("WriteUpsertNodeC")
	phase6UpsertRelationshipKind  = graph.StringKind("WriteUpsertRelationship")
	phase6UpsertRelationshipOther = graph.StringKind("WriteUpsertRelationshipOther")
	phase6EnsureRelationshipKind  = graph.StringKind("WriteEnsureRelationship")
	phase6EntityKind              = graph.StringKind("Entity")
	phase6GroupKind               = graph.StringKind("Group")
	phase6UnrelatedKind           = graph.StringKind("WriteUnrelated")
	phase6SuffixKind              = graph.StringKind("WriteSuffix")
	phase6MissingKind             = graph.StringKind("WriteMissing")
	phase6ScanKind                = graph.StringKind("WriteKindScan")
	phase6EndpointKind            = graph.StringKind("WriteEndpoint")
	phase6BoundarySizes           = []int{0, 1, 1_000, 1_999, 2_000, 2_001, 4_001, 8_001}
)

func TestPhase6DeleteRelationshipBoundariesAndSurvivors(t *testing.T) {
	db, ctx := phase6Setup(t)

	for _, size := range phase6BoundarySizes {
		t.Run(fmt.Sprintf("WRITE-01 size %d", size), func(t *testing.T) {
			_, _ = phase6LoadDirectWriteFixture(t, ctx, db, size)
			ids := phase6FetchRelationshipIDs(t, ctx, db, func() graph.Criteria {
				return query.And(
					query.Kind(query.Relationship(), phase6DeleteRelationshipKind),
					query.Equals(query.RelationshipProperty("deletebatch"), true),
				)
			})
			require.Len(t, ids, size)

			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for _, id := range ids {
					if err := batch.DeleteRelationship(id); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(2_000)))

			require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteDeleteRelationship]->() RETURN count(r)"))
			require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteDeleteRelationship]->() WHERE r.marker = 'same-kind-survivor' RETURN count(r)"))
			require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteSurvivor]->() RETURN count(r)"))
			require.Equal(t, int64(size), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpdateRelationship]->() RETURN count(r)"))
			require.Equal(t, phase6IncidentCount(size), countByCypher(t, ctx, db, "MATCH ()-[r:WriteIncident]->() RETURN count(r)"))
		})
	}

	t.Run("WRITE-01 duplicate and missing IDs are harmless", func(t *testing.T) {
		phase6LoadDirectWriteFixture(t, ctx, db, 3)
		ids := phase6FetchRelationshipIDs(t, ctx, db, func() graph.Criteria {
			return query.And(
				query.Kind(query.Relationship(), phase6DeleteRelationshipKind),
				query.Equals(query.RelationshipProperty("deletebatch"), true),
			)
		})
		require.Len(t, ids, 3)

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			for _, id := range []graph.ID{ids[0], ids[0], graph.ID(math.MaxInt64 - 7)} {
				if err := batch.DeleteRelationship(id); err != nil {
					return err
				}
			}
			return nil
		}, graph.WithBatchSize(2)))

		require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteDeleteRelationship]->() RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteDeleteRelationship]->() WHERE r.marker = 'same-kind-survivor' RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteSurvivor]->() RETURN count(r)"))
	})
}

func TestPhase6DeleteNodeBoundariesAndCascades(t *testing.T) {
	db, ctx := phase6Setup(t)

	for _, size := range phase6BoundarySizes {
		t.Run(fmt.Sprintf("WRITE-02 size %d", size), func(t *testing.T) {
			_, idMap := phase6LoadDirectWriteFixture(t, ctx, db, size)
			ids := make([]graph.ID, 0, size)
			for _, targetName := range testutil.FixtureNames("write-target", size) {
				ids = append(ids, idMap[targetName])
			}

			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for _, id := range ids {
					if err := batch.DeleteNode(id); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(2_000)))

			require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH (n:WriteEndpoint) RETURN count(n)"))
			require.Equal(t, int64(0), countByCypher(t, ctx, db, "MATCH (n:WriteDeleteNode) RETURN count(n)"))
			require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteDeleteRelationship]->() RETURN count(r)"))
			require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteSurvivor]->() RETURN count(r)"))
		})
	}

	t.Run("WRITE-02 duplicate missing isolated self low high and mixed directions", func(t *testing.T) {
		_, idMap := phase6LoadDirectWriteFixture(t, ctx, db, 8)
		targetIDs := testutil.FixtureNames("write-target", 8)
		isolated := phase6CreateNode(t, ctx, db, phase6Properties(phase6ObjectID, "write-isolated"), graph.StringKind("WriteDeleteNode"))

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			for _, targetName := range targetIDs {
				if err := batch.DeleteNode(idMap[targetName]); err != nil {
					return err
				}
			}
			if err := batch.DeleteNode(isolated.ID); err != nil {
				return err
			}
			if err := batch.DeleteNode(idMap[targetIDs[0]]); err != nil {
				return err
			}
			return batch.DeleteNode(graph.ID(math.MaxInt64 - 11))
		}, graph.WithBatchSize(3)))

		require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH (n:WriteEndpoint) RETURN count(n)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteDeleteRelationship]->() RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteSurvivor]->() RETURN count(r)"))
		require.Equal(t, int64(0), countByCypher(t, ctx, db, "MATCH ()-[r:WriteIncident]->() RETURN count(r)"))
	})
}

func TestPhase6CreateRelationshipConflictMerge(t *testing.T) {
	db, ctx := phase6Setup(t)
	ClearGraph(t, db, ctx)
	a, b, c := phase6CreateEndpoints(t, ctx, db, "create-a", "create-b", "create-c")

	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		updates := []struct {
			start, end graph.ID
			kind       graph.Kind
			properties *graph.Properties
		}{
			{a.ID, b.ID, phase6CreateRelationshipKind, phase6Properties("firstseen", "2026-01-01T00:00:00Z", "custom", "first", "preserved", "yes")},
			{a.ID, b.ID, phase6CreateRelationshipKind, phase6Properties("lastseen", "2026-01-02T00:00:00Z", "custom", "within")},
			{a.ID, b.ID, phase6CreateRelationshipKind, phase6Properties("custom", "last", "nullable", nil)},
			{b.ID, a.ID, phase6CreateRelationshipKind, phase6Properties("marker", "reverse")},
			{a.ID, b.ID, phase6CreateRelationshipOther, phase6Properties("marker", "other-kind")},
			{a.ID, c.ID, phase6CreateRelationshipKind, graph.NewProperties()},
		}
		for _, update := range updates {
			if err := batch.CreateRelationshipByIDs(update.start, update.end, update.kind, update.properties); err != nil {
				return err
			}
		}
		return nil
	}, graph.WithBatchSize(2)))

	primary := phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6CreateRelationshipKind)
	require.Equal(t, "2026-01-01T00:00:00Z", phase6StringProperty(t, primary.Properties, "firstseen"))
	require.Equal(t, "2026-01-02T00:00:00Z", phase6StringProperty(t, primary.Properties, phase6LastSeen))
	require.Equal(t, "last", phase6StringProperty(t, primary.Properties, "custom"))
	require.Equal(t, "yes", phase6StringProperty(t, primary.Properties, "preserved"))
	// Neo4j removes a property set to null while PostgreSQL retains a JSONB null
	// key. The shared graph API exposes nil in both cases.
	require.Nil(t, primary.Properties.Get("nullable").Any())
	require.NotNil(t, phase6FetchRelationship(t, ctx, db, b.ID, a.ID, phase6CreateRelationshipKind))
	require.NotNil(t, phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6CreateRelationshipOther))
	require.Empty(t, phase6FetchRelationship(t, ctx, db, a.ID, c.ID, phase6CreateRelationshipKind).Properties.MapOrEmpty())
	require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationship]->() RETURN count(r)"))
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationshipOther]->() RETURN count(r)"))

	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.CreateRelationshipByIDs(a.ID, b.ID, phase6CreateRelationshipKind, phase6Properties(
			phase6LastSeen, "2026-01-03T00:00:00Z",
			"retry", "yes",
		))
	}))
	primary = phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6CreateRelationshipKind)
	require.Equal(t, "2026-01-03T00:00:00Z", phase6StringProperty(t, primary.Properties, phase6LastSeen))
	require.Equal(t, "last", phase6StringProperty(t, primary.Properties, "custom"))
	require.Equal(t, "yes", phase6StringProperty(t, primary.Properties, "retry"))
	require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationship]->() RETURN count(r)"))
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationshipOther]->() RETURN count(r)"))
}

func TestPhase6UpdateNodeBySemanticsAndBoundaries(t *testing.T) {
	db, ctx := phase6Setup(t)

	for _, size := range []int{1_000, 1_999, 2_000, 2_001} {
		t.Run(fmt.Sprintf("WRITE-04 size %d", size), func(t *testing.T) {
			ClearGraph(t, db, ctx)
			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for idx := range size {
					if err := batch.UpdateNodeBy(phase6NodeUpdate(
						fmt.Sprintf("node-boundary-%04d", idx),
						phase6UpsertNodeKind,
						phase6Properties(phase6LastSeen, "2026-01-02T00:00:00Z", "ordinal", idx),
					)); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(2_000)))
			require.Equal(t, int64(size), countByCypher(t, ctx, db, "MATCH (n:WriteUpsertNode) RETURN count(n)"))
			first := phase6FetchNodeByObjectID(t, ctx, db, "node-boundary-0000")
			require.Equal(t, "2026-01-02T00:00:00Z", phase6StringProperty(t, first.Properties, phase6LastSeen))
		})
	}

	t.Run("WRITE-04 insert update duplicates retry lastseen and kind merge", func(t *testing.T) {
		ClearGraph(t, db, ctx)
		existing := phase6CreateNode(t, ctx, db, phase6Properties(
			phase6ObjectID, "node-existing",
			phase6LastSeen, "2026-01-01T00:00:00Z",
			"preserved", "yes",
		), phase6UpsertNodeKindA)

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			updates := []graph.NodeUpdate{
				phase6NodeUpdate("node-new", phase6UpsertNodeKindA, phase6Properties(phase6LastSeen, "2026-01-01T00:00:00Z", "custom", "first")),
				phase6NodeUpdate("node-new", phase6UpsertNodeKindB, phase6Properties(phase6LastSeen, "2026-01-02T00:00:00Z", "custom", "within")),
				phase6NodeUpdate("node-new", phase6UpsertNodeKindC, phase6Properties(phase6LastSeen, "2026-01-03T00:00:00Z", "custom", "last")),
				phase6NodeUpdate("node-existing", phase6UpsertNodeKindB, phase6Properties(phase6LastSeen, "2026-01-02T00:00:00Z", "changed", true)),
			}
			for _, update := range updates {
				if err := batch.UpdateNodeBy(update); err != nil {
					return err
				}
			}
			return nil
		}, graph.WithBatchSize(2)))

		inserted := phase6FetchNodeByObjectID(t, ctx, db, "node-new")
		require.Equal(t, "2026-01-03T00:00:00Z", phase6StringProperty(t, inserted.Properties, phase6LastSeen))
		require.Equal(t, "last", phase6StringProperty(t, inserted.Properties, "custom"))
		require.True(t, inserted.Kinds.ContainsOneOf(phase6UpsertNodeKindA))
		require.True(t, inserted.Kinds.ContainsOneOf(phase6UpsertNodeKindB))
		require.True(t, inserted.Kinds.ContainsOneOf(phase6UpsertNodeKindC))

		updated := phase6FetchNodeByObjectID(t, ctx, db, "node-existing")
		require.Equal(t, existing.ID, updated.ID)
		require.Equal(t, "yes", phase6StringProperty(t, updated.Properties, "preserved"))
		require.True(t, updated.Properties.Get("changed").Any().(bool))

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateNodeBy(phase6NodeUpdate("node-new", phase6UpsertNodeKind, phase6Properties(
				phase6LastSeen, "2026-01-04T00:00:00Z",
				"retry", "yes",
			)))
		}))
		inserted = phase6FetchNodeByObjectID(t, ctx, db, "node-new")
		require.Equal(t, "2026-01-04T00:00:00Z", phase6StringProperty(t, inserted.Properties, phase6LastSeen))
		require.Equal(t, "yes", phase6StringProperty(t, inserted.Properties, "retry"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'node-new' RETURN count(n)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'node-existing' RETURN count(n)"))
	})
}

func TestPhase6UpdateRelationshipBySemanticsAndBoundaries(t *testing.T) {
	db, ctx := phase6Setup(t)

	for _, size := range []int{1_000, 1_999, 2_000, 2_001} {
		t.Run(fmt.Sprintf("WRITE-05 size %d", size), func(t *testing.T) {
			ClearGraph(t, db, ctx)
			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for idx := range size {
					if err := batch.UpdateRelationshipBy(phase6RelationshipUpdate(
						fmt.Sprintf("rel-source-%04d", idx),
						fmt.Sprintf("rel-target-%04d", idx),
						phase6UpsertRelationshipKind,
						phase6Properties(phase6LastSeen, "2026-01-02T00:00:00Z", "ordinal", idx),
					)); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(2_000)))
			require.Equal(t, int64(size*2), countByCypher(t, ctx, db, "MATCH (n:WriteEndpoint) RETURN count(n)"))
			require.Equal(t, int64(size), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"))
		})
	}

	t.Run("WRITE-05 endpoint upsert duplicate retry reverse kind and property merge", func(t *testing.T) {
		ClearGraph(t, db, ctx)
		a := phase6CreateNode(t, ctx, db, phase6Properties(phase6ObjectID, "rel-a", "preserved", "start"), phase6EndpointKind)
		b := phase6CreateNode(t, ctx, db, phase6Properties(phase6ObjectID, "rel-b", "preserved", "end"), phase6EndpointKind)

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			updates := []graph.RelationshipUpdate{
				phase6RelationshipUpdate("rel-a", "rel-b", phase6UpsertRelationshipKind, phase6Properties(phase6LastSeen, "2026-01-01T00:00:00Z", "custom", "first", "preserved", "yes")),
				phase6RelationshipUpdate("rel-a", "rel-b", phase6UpsertRelationshipKind, phase6Properties(phase6LastSeen, "2026-01-02T00:00:00Z", "custom", "within")),
				phase6RelationshipUpdate("rel-a", "rel-b", phase6UpsertRelationshipKind, phase6Properties(phase6LastSeen, "2026-01-03T00:00:00Z", "custom", "last")),
				phase6RelationshipUpdate("rel-b", "rel-a", phase6UpsertRelationshipKind, phase6Properties("marker", "reverse")),
				phase6RelationshipUpdate("rel-a", "rel-b", phase6UpsertRelationshipOther, phase6Properties("marker", "other-kind")),
				phase6RelationshipUpdate("rel-missing-a", "rel-missing-b", phase6UpsertRelationshipKind, phase6Properties("marker", "missing-endpoints")),
			}
			for _, update := range updates {
				if err := batch.UpdateRelationshipBy(update); err != nil {
					return err
				}
			}
			return nil
		}, graph.WithBatchSize(2)))

		primary := phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6UpsertRelationshipKind)
		require.Equal(t, "2026-01-03T00:00:00Z", phase6StringProperty(t, primary.Properties, phase6LastSeen))
		require.Equal(t, "last", phase6StringProperty(t, primary.Properties, "custom"))
		require.Equal(t, "yes", phase6StringProperty(t, primary.Properties, "preserved"))
		require.NotNil(t, phase6FetchRelationship(t, ctx, db, b.ID, a.ID, phase6UpsertRelationshipKind))
		require.NotNil(t, phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6UpsertRelationshipOther))
		missingStart := phase6FetchNodeByObjectID(t, ctx, db, "rel-missing-a")
		missingEnd := phase6FetchNodeByObjectID(t, ctx, db, "rel-missing-b")
		require.NotNil(t, phase6FetchRelationship(t, ctx, db, missingStart.ID, missingEnd.ID, phase6UpsertRelationshipKind))
		require.Equal(t, int64(4), countByCypher(t, ctx, db, "MATCH (n:WriteEndpoint) RETURN count(n)"))
		require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationshipOther]->() RETURN count(r)"))
		require.Equal(t, "start", phase6StringProperty(t, phase6FetchNodeByObjectID(t, ctx, db, "rel-a").Properties, "preserved"))
		require.Equal(t, "end", phase6StringProperty(t, phase6FetchNodeByObjectID(t, ctx, db, "rel-b").Properties, "preserved"))

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateRelationshipBy(phase6RelationshipUpdate("rel-a", "rel-b", phase6UpsertRelationshipKind, phase6Properties(
				phase6LastSeen, "2026-01-04T00:00:00Z",
				"retry", "yes",
			)))
		}))
		primary = phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6UpsertRelationshipKind)
		require.Equal(t, "2026-01-04T00:00:00Z", phase6StringProperty(t, primary.Properties, phase6LastSeen))
		require.Equal(t, "last", phase6StringProperty(t, primary.Properties, "custom"))
		require.Equal(t, "yes", phase6StringProperty(t, primary.Properties, "retry"))
		require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationshipOther]->() RETURN count(r)"))
	})
}

func TestPhase6ReadThenCreateOrUpdateRelationship(t *testing.T) {
	db, ctx := phase6Setup(t)
	ClearGraph(t, db, ctx)
	a, b, _ := phase6CreateEndpoints(t, ctx, db, "ensure-a", "ensure-b", "ensure-unused")

	// A reverse-direction relationship is a decoy, not an existing exact key.
	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(b.ID, a.ID, phase6EnsureRelationshipKind, phase6Properties("marker", "reverse"))
		return err
	}))

	createdID, created, err := phase6EnsureRelationship(ctx, db, a.ID, b.ID, phase6EnsureRelationshipKind, phase6Properties(
		phase6LastSeen, "2026-01-01T00:00:00Z",
		"custom", "created",
	))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH ()-[r:WriteEnsureRelationship]->() RETURN count(r)"))

	updatedID, created, err := phase6EnsureRelationship(ctx, db, a.ID, b.ID, phase6EnsureRelationshipKind, phase6Properties(
		phase6LastSeen, "2026-01-02T00:00:00Z",
		"custom", "updated",
		"newproperty", "yes",
	))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, createdID, updatedID)

	repeatedID, created, err := phase6EnsureRelationship(ctx, db, a.ID, b.ID, phase6EnsureRelationshipKind, phase6Properties(
		phase6LastSeen, "2026-01-02T00:00:00Z",
		"custom", "updated",
		"newproperty", "yes",
	))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, createdID, repeatedID)
	require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH ()-[r:WriteEnsureRelationship]->() RETURN count(r)"))

	relationship := phase6FetchRelationship(t, ctx, db, a.ID, b.ID, phase6EnsureRelationshipKind)
	require.Equal(t, "2026-01-02T00:00:00Z", phase6StringProperty(t, relationship.Properties, phase6LastSeen))
	require.Equal(t, "updated", phase6StringProperty(t, relationship.Properties, "custom"))
	require.Equal(t, "yes", phase6StringProperty(t, relationship.Properties, "newproperty"))
	reverse := phase6FetchRelationship(t, ctx, db, b.ID, a.ID, phase6EnsureRelationshipKind)
	require.Equal(t, "reverse", phase6StringProperty(t, reverse.Properties, "marker"))
}

func TestPhase6FullNodeUpdateAfterSelectors(t *testing.T) {
	db, ctx := phase6Setup(t)
	ClearGraph(t, db, ctx)

	suffix := phase6CreateNode(t, ctx, db, phase6Properties(
		phase6ObjectID, "S-1-5-21-512",
		"name", "old suffix name",
		"preserved", "suffix",
	), phase6EntityKind, phase6SuffixKind, phase6UnrelatedKind)
	missing := phase6CreateNode(t, ctx, db, phase6Properties(
		phase6ObjectID, "missing-name",
		"preserved", "missing",
	), phase6EntityKind, phase6MissingKind, phase6UnrelatedKind)
	scan := phase6CreateNode(t, ctx, db, phase6Properties(
		phase6ObjectID, "kind-scan",
		"name", "old scan name",
		"preserved", "scan",
	), phase6EntityKind, phase6ScanKind, phase6UnrelatedKind)
	phase6CreateNode(t, ctx, db, phase6Properties(
		phase6ObjectID, "S-1-5-21-513",
		"name", "decoy",
	), phase6EntityKind, phase6UnrelatedKind)

	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		selectedSuffix, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.And(
				query.Kind(query.Node(), phase6SuffixKind),
				query.StringEndsWith(query.NodeProperty(phase6ObjectID), "-512"),
			)
		}).First()
		if err != nil {
			return err
		}
		selectedSuffix.Properties.Set("name", "new suffix name")
		if err := tx.UpdateNode(selectedSuffix); err != nil {
			return err
		}

		selectedMissing, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.And(
				query.Kind(query.Node(), phase6MissingKind),
				query.Not(query.Exists(query.NodeProperty("name"))),
			)
		}).First()
		if err != nil {
			return err
		}
		selectedMissing.AddKinds(phase6GroupKind)
		if err := tx.UpdateNode(selectedMissing); err != nil {
			return err
		}

		selectedScan, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.Kind(query.Node(), phase6ScanKind)
		}).First()
		if err != nil {
			return err
		}
		selectedScan.Properties.Set("name", "new scan name")
		selectedScan.AddKinds(phase6GroupKind)
		return tx.UpdateNode(selectedScan)
	}))

	updatedSuffix := phase6FetchNodeByID(t, ctx, db, suffix.ID)
	require.Equal(t, "new suffix name", phase6StringProperty(t, updatedSuffix.Properties, "name"))
	require.Equal(t, "suffix", phase6StringProperty(t, updatedSuffix.Properties, "preserved"))
	require.True(t, updatedSuffix.Kinds.ContainsOneOf(phase6UnrelatedKind))
	require.False(t, updatedSuffix.Kinds.ContainsOneOf(phase6GroupKind))

	updatedMissing := phase6FetchNodeByID(t, ctx, db, missing.ID)
	require.False(t, updatedMissing.Properties.Exists("name"))
	require.Equal(t, "missing", phase6StringProperty(t, updatedMissing.Properties, "preserved"))
	require.True(t, updatedMissing.Kinds.ContainsOneOf(phase6GroupKind))
	require.True(t, updatedMissing.Kinds.ContainsOneOf(phase6UnrelatedKind))

	updatedScan := phase6FetchNodeByID(t, ctx, db, scan.ID)
	require.Equal(t, "new scan name", phase6StringProperty(t, updatedScan.Properties, "name"))
	require.Equal(t, "scan", phase6StringProperty(t, updatedScan.Properties, "preserved"))
	require.True(t, updatedScan.Kinds.ContainsOneOf(phase6GroupKind))
	require.True(t, updatedScan.Kinds.ContainsOneOf(phase6UnrelatedKind))
}

func TestPhase6ExactKeyMissThenCreateNode(t *testing.T) {
	db, ctx := phase6Setup(t)
	ClearGraph(t, db, ctx)

	_, err := phase6FindNodeByObjectID(ctx, db, "well-known-new")
	require.Error(t, err)
	require.True(t, graph.IsErrNotFound(err), "selector must report an exact-key miss before the driver create")

	completeProperties := phase6Properties(
		phase6ObjectID, "well-known-new",
		"name", "Well Known Group",
		"domainsid", "S-1-5-21",
		"domainfqdn", "example.test",
		phase6LastSeen, "2026-01-01T00:00:00Z",
	)
	created, wasCreated, err := phase6GetOrCreateGroup(ctx, db, completeProperties)
	require.NoError(t, err)
	require.True(t, wasCreated)
	require.True(t, created.Kinds.ContainsOneOf(phase6EntityKind))
	require.True(t, created.Kinds.ContainsOneOf(phase6GroupKind))
	require.Equal(t, "Well Known Group", phase6StringProperty(t, created.Properties, "name"))
	require.Equal(t, "S-1-5-21", phase6StringProperty(t, created.Properties, "domainsid"))
	require.Equal(t, "example.test", phase6StringProperty(t, created.Properties, "domainfqdn"))

	selectorHit, err := phase6FindNodeByObjectID(ctx, db, "well-known-new")
	require.NoError(t, err)
	require.Equal(t, created.ID, selectorHit.ID)

	repeated, wasCreated, err := phase6GetOrCreateGroup(ctx, db, completeProperties)
	require.NoError(t, err)
	require.False(t, wasCreated)
	require.Equal(t, created.ID, repeated.ID)
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'well-known-new' RETURN count(n)"))

	existing := phase6CreateNode(t, ctx, db, phase6Properties(
		phase6ObjectID, "well-known-existing",
		"name", "Existing",
		"preserved", "yes",
	), phase6EntityKind, phase6UnrelatedKind)
	existingResult, wasCreated, err := phase6GetOrCreateGroup(ctx, db, phase6Properties(
		phase6ObjectID, "well-known-existing",
		"name", "replacement ignored",
	))
	require.NoError(t, err)
	require.False(t, wasCreated)
	require.Equal(t, existing.ID, existingResult.ID)
	require.True(t, existingResult.Kinds.ContainsOneOf(phase6GroupKind))
	require.True(t, existingResult.Kinds.ContainsOneOf(phase6UnrelatedKind))
	require.Equal(t, "yes", phase6StringProperty(t, existingResult.Properties, "preserved"))
	require.Equal(t, "Existing", phase6StringProperty(t, existingResult.Properties, "name"))
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'well-known-existing' RETURN count(n)"))
}

func BenchmarkPhase6MutationSafeDirectWrites(b *testing.B) {
	session := Open(b, Options{
		Schema:      phase6Schema(),
		CleanupMode: CleanupGraph,
	})

	for _, size := range []int{1_000, 2_000, 2_001} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.Run("WRITE-01 DeleteRelationship", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					phase6ClearBenchmarkGraph(b, session)
					if _, err := opengraph.WriteGraph(session.Ctx, session.DB, testutil.NewDirectWriteScaleFixture(size)); err != nil {
						b.Fatalf("load fixture: %v", err)
					}
					ids, err := phase6RelationshipIDs(session.Ctx, session.DB, func() graph.Criteria {
						return query.And(
							query.Kind(query.Relationship(), phase6DeleteRelationshipKind),
							query.Equals(query.RelationshipProperty("deletebatch"), true),
						)
					})
					if err != nil {
						b.Fatalf("select relationship IDs: %v", err)
					}
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for _, id := range ids {
							if err := batch.DeleteRelationship(id); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("delete relationships: %v", err)
					}
					b.StopTimer()
					if remaining, err := phase6Count(session.Ctx, session.DB, "MATCH ()-[r:WriteDeleteRelationship]->() RETURN count(r)"); err != nil || remaining != 1 {
						b.Fatalf("remaining relationships: got %d, err %v", remaining, err)
					}
				}
			})

			b.Run("WRITE-02 DeleteNode cascade", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					phase6ClearBenchmarkGraph(b, session)
					idMap, err := opengraph.WriteGraph(session.Ctx, session.DB, testutil.NewDirectWriteScaleFixture(size))
					if err != nil {
						b.Fatalf("load fixture: %v", err)
					}
					ids := make([]graph.ID, 0, size)
					for _, name := range testutil.FixtureNames("write-target", size) {
						ids = append(ids, idMap[name])
					}
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for _, id := range ids {
							if err := batch.DeleteNode(id); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("delete nodes: %v", err)
					}
					b.StopTimer()
					if remaining, err := phase6Count(session.Ctx, session.DB, "MATCH (n:WriteDeleteNode) RETURN count(n)"); err != nil || remaining != 0 {
						b.Fatalf("remaining nodes: got %d, err %v", remaining, err)
					}
					if survivors, err := phase6Count(session.Ctx, session.DB, "MATCH ()-[r:WriteSurvivor]->() RETURN count(r)"); err != nil || survivors != 1 {
						b.Fatalf("survivor relationships: got %d, err %v", survivors, err)
					}
				}
			})

			b.Run("WRITE-03 CreateRelationship conflict merge", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					phase6ClearBenchmarkGraph(b, session)
					idMap, err := opengraph.WriteGraph(session.Ctx, session.DB, testutil.NewDirectWriteScaleFixture(size))
					if err != nil {
						b.Fatalf("load fixture: %v", err)
					}
					rootID := idMap["write-root"]
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for idx, name := range testutil.FixtureNames("write-target", size) {
							if err := batch.CreateRelationshipByIDs(rootID, idMap[name], phase6CreateRelationshipKind, phase6Properties("ordinal", idx, "custom", "first")); err != nil {
								return err
							}
							if err := batch.CreateRelationshipByIDs(rootID, idMap[name], phase6CreateRelationshipKind, phase6Properties("custom", "last")); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("create relationships: %v", err)
					}
					b.StopTimer()
					if created, err := phase6Count(session.Ctx, session.DB, "MATCH ()-[r:WriteCreateRelationship]->() RETURN count(r)"); err != nil || created != int64(size) {
						b.Fatalf("created relationships: got %d, want %d, err %v", created, size, err)
					}
					if merged, err := phase6Count(session.Ctx, session.DB, "MATCH ()-[r:WriteCreateRelationship]->() WHERE r.custom = 'last' RETURN count(r)"); err != nil || merged != int64(size) {
						b.Fatalf("merged relationships: got %d, want %d, err %v", merged, size, err)
					}
				}
			})

			b.Run("WRITE-04 UpdateNodeBy", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					phase6ClearBenchmarkGraph(b, session)
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for idx := range size {
							if err := batch.UpdateNodeBy(phase6NodeUpdate(fmt.Sprintf("bench-node-%04d", idx), phase6UpsertNodeKind, phase6Properties("ordinal", idx))); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("update nodes: %v", err)
					}
					b.StopTimer()
					if updated, err := phase6Count(session.Ctx, session.DB, "MATCH (n:WriteUpsertNode) RETURN count(n)"); err != nil || updated != int64(size) {
						b.Fatalf("updated nodes: got %d, want %d, err %v", updated, size, err)
					}
				}
			})

			b.Run("WRITE-05 UpdateRelationshipBy", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					phase6ClearBenchmarkGraph(b, session)
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for idx := range size {
							if err := batch.UpdateRelationshipBy(phase6RelationshipUpdate(
								fmt.Sprintf("bench-source-%04d", idx),
								fmt.Sprintf("bench-target-%04d", idx),
								phase6UpsertRelationshipKind,
								phase6Properties("ordinal", idx),
							)); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("update relationships: %v", err)
					}
					b.StopTimer()
					if updated, err := phase6Count(session.Ctx, session.DB, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"); err != nil || updated != int64(size) {
						b.Fatalf("updated relationships: got %d, want %d, err %v", updated, size, err)
					}
				}
			})
		})
	}
}

func phase6Setup(t *testing.T) (graph.Database, context.Context) {
	t.Helper()
	session := Open(t, Options{
		Schema:      phase6Schema(),
		CleanupMode: CleanupGraph,
	})
	return session.DB, session.Ctx
}

func phase6Schema() *graph.Schema {
	nodeKinds, edgeKinds := phase6Kinds()
	graphSchema := graph.Graph{
		Name:  "integration_test",
		Nodes: nodeKinds,
		Edges: edgeKinds,
		NodeConstraints: []graph.Constraint{{
			Field: phase6ObjectID,
			Type:  graph.BTreeIndex,
		}},
	}
	return &graph.Schema{
		Graphs:       []graph.Graph{graphSchema},
		DefaultGraph: graphSchema,
	}
}

func phase6Kinds() (graph.Kinds, graph.Kinds) {
	fixtureNodeKinds, fixtureEdgeKinds := testutil.NewDirectWriteScaleFixture(2).Kinds()
	nodeKinds := fixtureNodeKinds.Add(
		phase6UpsertNodeKind,
		phase6UpsertNodeKindA,
		phase6UpsertNodeKindB,
		phase6UpsertNodeKindC,
		phase6EntityKind,
		phase6GroupKind,
		phase6UnrelatedKind,
		phase6SuffixKind,
		phase6MissingKind,
		phase6ScanKind,
	)
	edgeKinds := fixtureEdgeKinds.Add(
		phase6CreateRelationshipKind,
		phase6CreateRelationshipOther,
		phase6UpsertRelationshipKind,
		phase6UpsertRelationshipOther,
		phase6EnsureRelationshipKind,
	)
	return nodeKinds, edgeKinds
}

func phase6LoadDirectWriteFixture(t *testing.T, ctx context.Context, db graph.Database, size int) (*opengraph.Graph, opengraph.IDMap) {
	t.Helper()
	ClearGraph(t, db, ctx)
	fixture := testutil.NewDirectWriteScaleFixture(size)
	idMap, err := opengraph.WriteGraph(ctx, db, fixture)
	require.NoError(t, err)
	return fixture, idMap
}

func phase6CreateEndpoints(t *testing.T, ctx context.Context, db graph.Database, objectIDs ...string) (*graph.Node, *graph.Node, *graph.Node) {
	t.Helper()
	require.Len(t, objectIDs, 3)
	created := make([]*graph.Node, 0, len(objectIDs))
	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, objectID := range objectIDs {
			node, err := tx.CreateNode(phase6Properties(phase6ObjectID, objectID), phase6EndpointKind)
			if err != nil {
				return err
			}
			created = append(created, node)
		}
		return nil
	}))
	return created[0], created[1], created[2]
}

func phase6CreateNode(t *testing.T, ctx context.Context, db graph.Database, properties *graph.Properties, kinds ...graph.Kind) *graph.Node {
	t.Helper()
	var created *graph.Node
	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		created, err = tx.CreateNode(properties, kinds...)
		return err
	}))
	return created
}

func phase6Properties(keyValues ...any) *graph.Properties {
	properties := graph.NewProperties()
	for idx := 0; idx < len(keyValues); idx += 2 {
		properties.Set(keyValues[idx].(string), keyValues[idx+1])
	}
	return properties
}

func phase6IncidentCount(targets int) int64 {
	switch targets {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return int64(targets + 1)
	}
}

func phase6NodeUpdate(objectID string, kind graph.Kind, properties *graph.Properties) graph.NodeUpdate {
	properties = properties.Clone().Set(phase6ObjectID, objectID)
	return graph.NodeUpdate{
		Node:               graph.PrepareNode(properties, kind),
		IdentityProperties: []string{phase6ObjectID},
	}
}

func phase6RelationshipUpdate(startObjectID, endObjectID string, kind graph.Kind, properties *graph.Properties) graph.RelationshipUpdate {
	return graph.RelationshipUpdate{
		Start: graph.PrepareNode(
			phase6Properties(phase6ObjectID, startObjectID),
			phase6EndpointKind,
		),
		StartIdentityProperties: []string{phase6ObjectID},
		End: graph.PrepareNode(
			phase6Properties(phase6ObjectID, endObjectID),
			phase6EndpointKind,
		),
		EndIdentityProperties: []string{phase6ObjectID},
		Relationship:          graph.PrepareRelationship(properties, kind),
	}
}

func phase6FetchRelationshipIDs(t *testing.T, ctx context.Context, db graph.Database, criteria graph.CriteriaProvider) []graph.ID {
	t.Helper()
	ids, err := phase6RelationshipIDs(ctx, db, criteria)
	require.NoError(t, err)
	return ids
}

func phase6RelationshipIDs(ctx context.Context, db graph.Database, criteria graph.CriteriaProvider) ([]graph.ID, error) {
	var ids []graph.ID
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		ids, err = ops.FetchRelationshipIDs(tx.Relationships().Filterf(criteria))
		return err
	})
	return ids, err
}

func phase6FetchRelationship(t *testing.T, ctx context.Context, db graph.Database, startID, endID graph.ID, kind graph.Kind) *graph.Relationship {
	t.Helper()
	var relationship *graph.Relationship
	require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		relationship, err = tx.Relationships().Filterf(func() graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), startID),
				query.Equals(query.EndID(), endID),
				query.Kind(query.Relationship(), kind),
			)
		}).First()
		return err
	}))
	return relationship
}

func phase6FetchNodeByObjectID(t *testing.T, ctx context.Context, db graph.Database, objectID string) *graph.Node {
	t.Helper()
	node, err := phase6FindNodeByObjectID(ctx, db, objectID)
	require.NoError(t, err)
	return node
}

func phase6FindNodeByObjectID(ctx context.Context, db graph.Database, objectID string) (*graph.Node, error) {
	var node *graph.Node
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		node, err = tx.Nodes().Filterf(func() graph.Criteria {
			return query.Equals(query.NodeProperty(phase6ObjectID), objectID)
		}).First()
		return err
	})
	return node, err
}

func phase6FetchNodeByID(t *testing.T, ctx context.Context, db graph.Database, id graph.ID) *graph.Node {
	t.Helper()
	var node *graph.Node
	require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		node, err = tx.Nodes().Filter(query.Equals(query.NodeID(), id)).First()
		return err
	}))
	return node
}

func phase6StringProperty(t *testing.T, properties *graph.Properties, key string) string {
	t.Helper()
	value, err := properties.Get(key).String()
	require.NoError(t, err)
	return value
}

func phase6EnsureRelationship(ctx context.Context, db graph.Database, startID, endID graph.ID, kind graph.Kind, properties *graph.Properties) (graph.ID, bool, error) {
	var (
		id      graph.ID
		created bool
	)
	err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		relationship, err := tx.Relationships().Filterf(func() graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), startID),
				query.Equals(query.EndID(), endID),
				query.Kind(query.Relationship(), kind),
			)
		}).First()
		if err != nil && !graph.IsErrNotFound(err) {
			return err
		}
		if graph.IsErrNotFound(err) {
			createdRelationship, err := tx.CreateRelationshipByIDs(startID, endID, kind, properties)
			if err != nil {
				return err
			}
			id = createdRelationship.ID
			created = true
			return nil
		}

		relationship.Properties.Merge(properties)
		id = relationship.ID
		return tx.UpdateRelationship(relationship)
	})
	return id, created, err
}

func phase6GetOrCreateGroup(ctx context.Context, db graph.Database, properties *graph.Properties) (*graph.Node, bool, error) {
	objectID, err := properties.Get(phase6ObjectID).String()
	if err != nil {
		return nil, false, err
	}

	var (
		result  *graph.Node
		created bool
	)
	err = db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		existing, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.Equals(query.NodeProperty(phase6ObjectID), objectID)
		}).First()
		if err != nil && !graph.IsErrNotFound(err) {
			return err
		}
		if graph.IsErrNotFound(err) {
			result, err = tx.CreateNode(properties.Clone(), phase6EntityKind, phase6GroupKind)
			created = err == nil
			return err
		}

		result = existing
		if !result.Kinds.ContainsOneOf(phase6GroupKind) {
			result.AddKinds(phase6GroupKind)
			return tx.UpdateNode(result)
		}
		return nil
	})
	return result, created, err
}

func phase6ClearBenchmarkGraph(b *testing.B, session *Session) {
	b.Helper()
	if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	}); err != nil {
		b.Fatalf("clear benchmark graph: %v", err)
	}
}

func phase6Count(ctx context.Context, db graph.Database, cypher string) (int64, error) {
	var count int64
	err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()
		if !result.Next() {
			return result.Error()
		}
		if err := result.Scan(&count); err != nil {
			return err
		}
		return result.Error()
	})
	return count, err
}
