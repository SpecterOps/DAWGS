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
	directWriteObjectID = "objectid"
	directWriteLastSeen = "lastseen"
)

var (
	directWriteDeleteRelationshipKind  = graph.StringKind("WriteDeleteRelationship")
	directWriteCreateRelationshipKind  = graph.StringKind("WriteCreateRelationship")
	directWriteCreateRelationshipOther = graph.StringKind("WriteCreateRelationshipOther")
	directWriteUpsertNodeKind          = graph.StringKind("WriteUpsertNode")
	directWriteUpsertNodeKindA         = graph.StringKind("WriteUpsertNodeA")
	directWriteUpsertNodeKindB         = graph.StringKind("WriteUpsertNodeB")
	directWriteUpsertNodeKindC         = graph.StringKind("WriteUpsertNodeC")
	directWriteUpsertRelationshipKind  = graph.StringKind("WriteUpsertRelationship")
	directWriteUpsertRelationshipOther = graph.StringKind("WriteUpsertRelationshipOther")
	directWriteEnsureRelationshipKind  = graph.StringKind("WriteEnsureRelationship")
	directWriteEntityKind              = graph.StringKind("Entity")
	directWriteGroupKind               = graph.StringKind("Group")
	directWriteUnrelatedKind           = graph.StringKind("WriteUnrelated")
	directWriteSuffixKind              = graph.StringKind("WriteSuffix")
	directWriteMissingKind             = graph.StringKind("WriteMissing")
	directWriteScanKind                = graph.StringKind("WriteKindScan")
	directWriteEndpointKind            = graph.StringKind("WriteEndpoint")
	directWriteBoundarySizes           = []int{0, 1, 1_000, 1_999, 2_000, 2_001, 4_001, 8_001}
)

func TestDirectWriteDeleteRelationshipBoundariesAndSurvivors(t *testing.T) {
	db, ctx := directWriteSetup(t)

	for _, size := range directWriteBoundarySizes {
		t.Run(fmt.Sprintf("WRITE-01 size %d", size), func(t *testing.T) {
			_, _ = directWriteLoadDirectWriteFixture(t, ctx, db, size)
			ids := directWriteFetchRelationshipIDs(t, ctx, db, func() graph.Criteria {
				return query.And(
					query.Kind(query.Relationship(), directWriteDeleteRelationshipKind),
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
			require.Equal(t, directWriteIncidentCount(size), countByCypher(t, ctx, db, "MATCH ()-[r:WriteIncident]->() RETURN count(r)"))
		})
	}

	t.Run("WRITE-01 duplicate and missing IDs are harmless", func(t *testing.T) {
		directWriteLoadDirectWriteFixture(t, ctx, db, 3)
		ids := directWriteFetchRelationshipIDs(t, ctx, db, func() graph.Criteria {
			return query.And(
				query.Kind(query.Relationship(), directWriteDeleteRelationshipKind),
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

func TestDirectWriteDeleteNodeBoundariesAndCascades(t *testing.T) {
	db, ctx := directWriteSetup(t)

	for _, size := range directWriteBoundarySizes {
		t.Run(fmt.Sprintf("WRITE-02 size %d", size), func(t *testing.T) {
			_, idMap := directWriteLoadDirectWriteFixture(t, ctx, db, size)
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
		_, idMap := directWriteLoadDirectWriteFixture(t, ctx, db, 8)
		targetIDs := testutil.FixtureNames("write-target", 8)
		isolated := directWriteCreateNode(t, ctx, db, directWriteProperties(directWriteObjectID, "write-isolated"), graph.StringKind("WriteDeleteNode"))

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

func TestDirectWriteCreateRelationshipConflictMerge(t *testing.T) {
	db, ctx := directWriteSetup(t)
	ClearGraph(t, db, ctx)
	a, b, c := directWriteCreateEndpoints(t, ctx, db, "create-a", "create-b", "create-c")

	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		updates := []struct {
			start      graph.ID
			end        graph.ID
			kind       graph.Kind
			properties *graph.Properties
		}{
			{
				start:      a.ID,
				end:        b.ID,
				kind:       directWriteCreateRelationshipKind,
				properties: directWriteProperties("firstseen", "2026-01-01T00:00:00Z", "custom", "first", "preserved", "yes"),
			},
			{
				start:      a.ID,
				end:        b.ID,
				kind:       directWriteCreateRelationshipKind,
				properties: directWriteProperties("lastseen", "2026-01-02T00:00:00Z", "custom", "within"),
			},
			{
				start:      a.ID,
				end:        b.ID,
				kind:       directWriteCreateRelationshipKind,
				properties: directWriteProperties("custom", "last", "nullable", nil),
			},
			{
				start:      b.ID,
				end:        a.ID,
				kind:       directWriteCreateRelationshipKind,
				properties: directWriteProperties("marker", "reverse"),
			},
			{
				start:      a.ID,
				end:        b.ID,
				kind:       directWriteCreateRelationshipOther,
				properties: directWriteProperties("marker", "other-kind"),
			},
			{
				start:      a.ID,
				end:        c.ID,
				kind:       directWriteCreateRelationshipKind,
				properties: graph.NewProperties(),
			},
		}
		for _, update := range updates {
			if err := batch.CreateRelationshipByIDs(update.start, update.end, update.kind, update.properties); err != nil {
				return err
			}
		}
		return nil
	}, graph.WithBatchSize(2)))

	primary := directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteCreateRelationshipKind)
	require.Equal(t, "2026-01-01T00:00:00Z", directWriteStringProperty(t, primary.Properties, "firstseen"))
	require.Equal(t, "2026-01-02T00:00:00Z", directWriteStringProperty(t, primary.Properties, directWriteLastSeen))
	require.Equal(t, "last", directWriteStringProperty(t, primary.Properties, "custom"))
	require.Equal(t, "yes", directWriteStringProperty(t, primary.Properties, "preserved"))
	// Neo4j removes a property set to null while PostgreSQL retains a JSONB null
	// key. The shared graph API exposes nil in both cases.
	require.Nil(t, primary.Properties.Get("nullable").Any())
	require.NotNil(t, directWriteFetchRelationship(t, ctx, db, b.ID, a.ID, directWriteCreateRelationshipKind))
	require.NotNil(t, directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteCreateRelationshipOther))
	require.Empty(t, directWriteFetchRelationship(t, ctx, db, a.ID, c.ID, directWriteCreateRelationshipKind).Properties.MapOrEmpty())
	require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationship]->() RETURN count(r)"))
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationshipOther]->() RETURN count(r)"))

	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.CreateRelationshipByIDs(a.ID, b.ID, directWriteCreateRelationshipKind, directWriteProperties(
			directWriteLastSeen, "2026-01-03T00:00:00Z",
			"retry", "yes",
		))
	}))
	primary = directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteCreateRelationshipKind)
	require.Equal(t, "2026-01-03T00:00:00Z", directWriteStringProperty(t, primary.Properties, directWriteLastSeen))
	require.Equal(t, "last", directWriteStringProperty(t, primary.Properties, "custom"))
	require.Equal(t, "yes", directWriteStringProperty(t, primary.Properties, "retry"))
	require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationship]->() RETURN count(r)"))
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteCreateRelationshipOther]->() RETURN count(r)"))
}

func TestDirectWriteUpdateNodeBySemanticsAndBoundaries(t *testing.T) {
	db, ctx := directWriteSetup(t)

	for _, size := range []int{1_000, 1_999, 2_000, 2_001} {
		t.Run(fmt.Sprintf("WRITE-04 size %d", size), func(t *testing.T) {
			ClearGraph(t, db, ctx)
			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for idx := range size {
					if err := batch.UpdateNodeBy(directWriteNodeUpdate(
						fmt.Sprintf("node-boundary-%04d", idx),
						directWriteUpsertNodeKind,
						directWriteProperties(directWriteLastSeen, "2026-01-02T00:00:00Z", "ordinal", idx),
					)); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(2_000)))
			require.Equal(t, int64(size), countByCypher(t, ctx, db, "MATCH (n:WriteUpsertNode) RETURN count(n)"))
			first := directWriteFetchNodeByObjectID(t, ctx, db, "node-boundary-0000")
			require.Equal(t, "2026-01-02T00:00:00Z", directWriteStringProperty(t, first.Properties, directWriteLastSeen))
		})
	}

	t.Run("WRITE-04 insert update duplicates retry lastseen and kind merge", func(t *testing.T) {
		ClearGraph(t, db, ctx)
		existing := directWriteCreateNode(t, ctx, db, directWriteProperties(
			directWriteObjectID, "node-existing",
			directWriteLastSeen, "2026-01-01T00:00:00Z",
			"preserved", "yes",
		), directWriteUpsertNodeKindA)

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			updates := []graph.NodeUpdate{
				directWriteNodeUpdate("node-new", directWriteUpsertNodeKindA, directWriteProperties(directWriteLastSeen, "2026-01-01T00:00:00Z", "custom", "first")),
				directWriteNodeUpdate("node-new", directWriteUpsertNodeKindB, directWriteProperties(directWriteLastSeen, "2026-01-02T00:00:00Z", "custom", "within")),
				directWriteNodeUpdate("node-new", directWriteUpsertNodeKindC, directWriteProperties(directWriteLastSeen, "2026-01-03T00:00:00Z", "custom", "last")),
				directWriteNodeUpdate("node-existing", directWriteUpsertNodeKindB, directWriteProperties(directWriteLastSeen, "2026-01-02T00:00:00Z", "changed", true)),
			}
			for _, update := range updates {
				if err := batch.UpdateNodeBy(update); err != nil {
					return err
				}
			}
			return nil
		}, graph.WithBatchSize(2)))

		inserted := directWriteFetchNodeByObjectID(t, ctx, db, "node-new")
		require.Equal(t, "2026-01-03T00:00:00Z", directWriteStringProperty(t, inserted.Properties, directWriteLastSeen))
		require.Equal(t, "last", directWriteStringProperty(t, inserted.Properties, "custom"))
		require.True(t, inserted.Kinds.ContainsOneOf(directWriteUpsertNodeKindA))
		require.True(t, inserted.Kinds.ContainsOneOf(directWriteUpsertNodeKindB))
		require.True(t, inserted.Kinds.ContainsOneOf(directWriteUpsertNodeKindC))

		updated := directWriteFetchNodeByObjectID(t, ctx, db, "node-existing")
		require.Equal(t, existing.ID, updated.ID)
		require.Equal(t, "yes", directWriteStringProperty(t, updated.Properties, "preserved"))
		require.True(t, updated.Properties.Get("changed").Any().(bool))

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateNodeBy(directWriteNodeUpdate("node-new", directWriteUpsertNodeKind, directWriteProperties(
				directWriteLastSeen, "2026-01-04T00:00:00Z",
				"retry", "yes",
			)))
		}))
		inserted = directWriteFetchNodeByObjectID(t, ctx, db, "node-new")
		require.Equal(t, "2026-01-04T00:00:00Z", directWriteStringProperty(t, inserted.Properties, directWriteLastSeen))
		require.Equal(t, "yes", directWriteStringProperty(t, inserted.Properties, "retry"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'node-new' RETURN count(n)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'node-existing' RETURN count(n)"))
	})
}

func TestDirectWriteUpdateRelationshipBySemanticsAndBoundaries(t *testing.T) {
	db, ctx := directWriteSetup(t)

	for _, size := range []int{1_000, 1_999, 2_000, 2_001} {
		t.Run(fmt.Sprintf("WRITE-05 size %d", size), func(t *testing.T) {
			ClearGraph(t, db, ctx)
			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for idx := range size {
					if err := batch.UpdateRelationshipBy(directWriteRelationshipUpdate(
						fmt.Sprintf("rel-source-%04d", idx),
						fmt.Sprintf("rel-target-%04d", idx),
						directWriteUpsertRelationshipKind,
						directWriteProperties(directWriteLastSeen, "2026-01-02T00:00:00Z", "ordinal", idx),
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
		a := directWriteCreateNode(t, ctx, db, directWriteProperties(directWriteObjectID, "rel-a", "preserved", "start"), directWriteEndpointKind)
		b := directWriteCreateNode(t, ctx, db, directWriteProperties(directWriteObjectID, "rel-b", "preserved", "end"), directWriteEndpointKind)

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			updates := []graph.RelationshipUpdate{
				directWriteRelationshipUpdate("rel-a", "rel-b", directWriteUpsertRelationshipKind, directWriteProperties(directWriteLastSeen, "2026-01-01T00:00:00Z", "custom", "first", "preserved", "yes")),
				directWriteRelationshipUpdate("rel-a", "rel-b", directWriteUpsertRelationshipKind, directWriteProperties(directWriteLastSeen, "2026-01-02T00:00:00Z", "custom", "within")),
				directWriteRelationshipUpdate("rel-a", "rel-b", directWriteUpsertRelationshipKind, directWriteProperties(directWriteLastSeen, "2026-01-03T00:00:00Z", "custom", "last")),
				directWriteRelationshipUpdate("rel-b", "rel-a", directWriteUpsertRelationshipKind, directWriteProperties("marker", "reverse")),
				directWriteRelationshipUpdate("rel-a", "rel-b", directWriteUpsertRelationshipOther, directWriteProperties("marker", "other-kind")),
				directWriteRelationshipUpdate("rel-missing-a", "rel-missing-b", directWriteUpsertRelationshipKind, directWriteProperties("marker", "missing-endpoints")),
			}
			for _, update := range updates {
				if err := batch.UpdateRelationshipBy(update); err != nil {
					return err
				}
			}
			return nil
		}, graph.WithBatchSize(2)))

		primary := directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteUpsertRelationshipKind)
		require.Equal(t, "2026-01-03T00:00:00Z", directWriteStringProperty(t, primary.Properties, directWriteLastSeen))
		require.Equal(t, "last", directWriteStringProperty(t, primary.Properties, "custom"))
		require.Equal(t, "yes", directWriteStringProperty(t, primary.Properties, "preserved"))
		require.NotNil(t, directWriteFetchRelationship(t, ctx, db, b.ID, a.ID, directWriteUpsertRelationshipKind))
		require.NotNil(t, directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteUpsertRelationshipOther))
		missingStart := directWriteFetchNodeByObjectID(t, ctx, db, "rel-missing-a")
		missingEnd := directWriteFetchNodeByObjectID(t, ctx, db, "rel-missing-b")
		require.NotNil(t, directWriteFetchRelationship(t, ctx, db, missingStart.ID, missingEnd.ID, directWriteUpsertRelationshipKind))
		require.Equal(t, int64(4), countByCypher(t, ctx, db, "MATCH (n:WriteEndpoint) RETURN count(n)"))
		require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationshipOther]->() RETURN count(r)"))
		require.Equal(t, "start", directWriteStringProperty(t, directWriteFetchNodeByObjectID(t, ctx, db, "rel-a").Properties, "preserved"))
		require.Equal(t, "end", directWriteStringProperty(t, directWriteFetchNodeByObjectID(t, ctx, db, "rel-b").Properties, "preserved"))

		require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
			return batch.UpdateRelationshipBy(directWriteRelationshipUpdate("rel-a", "rel-b", directWriteUpsertRelationshipKind, directWriteProperties(
				directWriteLastSeen, "2026-01-04T00:00:00Z",
				"retry", "yes",
			)))
		}))
		primary = directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteUpsertRelationshipKind)
		require.Equal(t, "2026-01-04T00:00:00Z", directWriteStringProperty(t, primary.Properties, directWriteLastSeen))
		require.Equal(t, "last", directWriteStringProperty(t, primary.Properties, "custom"))
		require.Equal(t, "yes", directWriteStringProperty(t, primary.Properties, "retry"))
		require.Equal(t, int64(3), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"))
		require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationshipOther]->() RETURN count(r)"))
	})
}

func TestDirectWriteReadThenCreateOrUpdateRelationship(t *testing.T) {
	db, ctx := directWriteSetup(t)
	ClearGraph(t, db, ctx)
	a, b, _ := directWriteCreateEndpoints(t, ctx, db, "ensure-a", "ensure-b", "ensure-unused")

	// A reverse-direction relationship is a decoy, not an existing exact key.
	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		_, err := tx.CreateRelationshipByIDs(b.ID, a.ID, directWriteEnsureRelationshipKind, directWriteProperties("marker", "reverse"))
		return err
	}))

	createdID, created, err := directWriteEnsureRelationship(ctx, db, a.ID, b.ID, directWriteEnsureRelationshipKind, directWriteProperties(
		directWriteLastSeen, "2026-01-01T00:00:00Z",
		"custom", "created",
	))
	require.NoError(t, err)
	require.True(t, created)
	require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH ()-[r:WriteEnsureRelationship]->() RETURN count(r)"))

	updatedID, created, err := directWriteEnsureRelationship(ctx, db, a.ID, b.ID, directWriteEnsureRelationshipKind, directWriteProperties(
		directWriteLastSeen, "2026-01-02T00:00:00Z",
		"custom", "updated",
		"newproperty", "yes",
	))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, createdID, updatedID)

	repeatedID, created, err := directWriteEnsureRelationship(ctx, db, a.ID, b.ID, directWriteEnsureRelationshipKind, directWriteProperties(
		directWriteLastSeen, "2026-01-02T00:00:00Z",
		"custom", "updated",
		"newproperty", "yes",
	))
	require.NoError(t, err)
	require.False(t, created)
	require.Equal(t, createdID, repeatedID)
	require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH ()-[r:WriteEnsureRelationship]->() RETURN count(r)"))

	relationship := directWriteFetchRelationship(t, ctx, db, a.ID, b.ID, directWriteEnsureRelationshipKind)
	require.Equal(t, "2026-01-02T00:00:00Z", directWriteStringProperty(t, relationship.Properties, directWriteLastSeen))
	require.Equal(t, "updated", directWriteStringProperty(t, relationship.Properties, "custom"))
	require.Equal(t, "yes", directWriteStringProperty(t, relationship.Properties, "newproperty"))
	reverse := directWriteFetchRelationship(t, ctx, db, b.ID, a.ID, directWriteEnsureRelationshipKind)
	require.Equal(t, "reverse", directWriteStringProperty(t, reverse.Properties, "marker"))
}

func TestDirectWriteFullNodeUpdateAfterSelectors(t *testing.T) {
	db, ctx := directWriteSetup(t)
	ClearGraph(t, db, ctx)

	suffix := directWriteCreateNode(t, ctx, db, directWriteProperties(
		directWriteObjectID, "S-1-5-21-512",
		"name", "old suffix name",
		"preserved", "suffix",
	), directWriteEntityKind, directWriteSuffixKind, directWriteUnrelatedKind)
	missing := directWriteCreateNode(t, ctx, db, directWriteProperties(
		directWriteObjectID, "missing-name",
		"preserved", "missing",
	), directWriteEntityKind, directWriteMissingKind, directWriteUnrelatedKind)
	scan := directWriteCreateNode(t, ctx, db, directWriteProperties(
		directWriteObjectID, "kind-scan",
		"name", "old scan name",
		"preserved", "scan",
	), directWriteEntityKind, directWriteScanKind, directWriteUnrelatedKind)
	directWriteCreateNode(t, ctx, db, directWriteProperties(
		directWriteObjectID, "S-1-5-21-513",
		"name", "decoy",
	), directWriteEntityKind, directWriteUnrelatedKind)

	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		selectedSuffix, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.And(
				query.Kind(query.Node(), directWriteSuffixKind),
				query.StringEndsWith(query.NodeProperty(directWriteObjectID), "-512"),
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
				query.Kind(query.Node(), directWriteMissingKind),
				query.Not(query.Exists(query.NodeProperty("name"))),
			)
		}).First()
		if err != nil {
			return err
		}
		selectedMissing.AddKinds(directWriteGroupKind)
		if err := tx.UpdateNode(selectedMissing); err != nil {
			return err
		}

		selectedScan, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.Kind(query.Node(), directWriteScanKind)
		}).First()
		if err != nil {
			return err
		}
		selectedScan.Properties.Set("name", "new scan name")
		selectedScan.AddKinds(directWriteGroupKind)
		return tx.UpdateNode(selectedScan)
	}))

	updatedSuffix := directWriteFetchNodeByID(t, ctx, db, suffix.ID)
	require.Equal(t, "new suffix name", directWriteStringProperty(t, updatedSuffix.Properties, "name"))
	require.Equal(t, "suffix", directWriteStringProperty(t, updatedSuffix.Properties, "preserved"))
	require.True(t, updatedSuffix.Kinds.ContainsOneOf(directWriteUnrelatedKind))
	require.False(t, updatedSuffix.Kinds.ContainsOneOf(directWriteGroupKind))

	updatedMissing := directWriteFetchNodeByID(t, ctx, db, missing.ID)
	require.False(t, updatedMissing.Properties.Exists("name"))
	require.Equal(t, "missing", directWriteStringProperty(t, updatedMissing.Properties, "preserved"))
	require.True(t, updatedMissing.Kinds.ContainsOneOf(directWriteGroupKind))
	require.True(t, updatedMissing.Kinds.ContainsOneOf(directWriteUnrelatedKind))

	updatedScan := directWriteFetchNodeByID(t, ctx, db, scan.ID)
	require.Equal(t, "new scan name", directWriteStringProperty(t, updatedScan.Properties, "name"))
	require.Equal(t, "scan", directWriteStringProperty(t, updatedScan.Properties, "preserved"))
	require.True(t, updatedScan.Kinds.ContainsOneOf(directWriteGroupKind))
	require.True(t, updatedScan.Kinds.ContainsOneOf(directWriteUnrelatedKind))
}

func TestDirectWriteExactKeyMissThenCreateNode(t *testing.T) {
	db, ctx := directWriteSetup(t)
	ClearGraph(t, db, ctx)

	_, err := directWriteFindNodeByObjectID(ctx, db, "well-known-new")
	require.Error(t, err)
	require.True(t, graph.IsErrNotFound(err), "selector must report an exact-key miss before the driver create")

	completeProperties := directWriteProperties(
		directWriteObjectID, "well-known-new",
		"name", "Well Known Group",
		"domainsid", "S-1-5-21",
		"domainfqdn", "example.test",
		directWriteLastSeen, "2026-01-01T00:00:00Z",
	)
	created, wasCreated, err := directWriteGetOrCreateGroup(ctx, db, completeProperties)
	require.NoError(t, err)
	require.True(t, wasCreated)
	require.True(t, created.Kinds.ContainsOneOf(directWriteEntityKind))
	require.True(t, created.Kinds.ContainsOneOf(directWriteGroupKind))
	require.Equal(t, "Well Known Group", directWriteStringProperty(t, created.Properties, "name"))
	require.Equal(t, "S-1-5-21", directWriteStringProperty(t, created.Properties, "domainsid"))
	require.Equal(t, "example.test", directWriteStringProperty(t, created.Properties, "domainfqdn"))

	selectorHit, err := directWriteFindNodeByObjectID(ctx, db, "well-known-new")
	require.NoError(t, err)
	require.Equal(t, created.ID, selectorHit.ID)

	repeated, wasCreated, err := directWriteGetOrCreateGroup(ctx, db, completeProperties)
	require.NoError(t, err)
	require.False(t, wasCreated)
	require.Equal(t, created.ID, repeated.ID)
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'well-known-new' RETURN count(n)"))

	existing := directWriteCreateNode(t, ctx, db, directWriteProperties(
		directWriteObjectID, "well-known-existing",
		"name", "Existing",
		"preserved", "yes",
	), directWriteEntityKind, directWriteUnrelatedKind)
	existingResult, wasCreated, err := directWriteGetOrCreateGroup(ctx, db, directWriteProperties(
		directWriteObjectID, "well-known-existing",
		"name", "replacement ignored",
	))
	require.NoError(t, err)
	require.False(t, wasCreated)
	require.Equal(t, existing.ID, existingResult.ID)
	require.True(t, existingResult.Kinds.ContainsOneOf(directWriteGroupKind))
	require.True(t, existingResult.Kinds.ContainsOneOf(directWriteUnrelatedKind))
	require.Equal(t, "yes", directWriteStringProperty(t, existingResult.Properties, "preserved"))
	require.Equal(t, "Existing", directWriteStringProperty(t, existingResult.Properties, "name"))
	require.Equal(t, int64(1), countByCypher(t, ctx, db, "MATCH (n) WHERE n.objectid = 'well-known-existing' RETURN count(n)"))
}

func BenchmarkMutationSafeDirectWrites(b *testing.B) {
	session := Open(b, Options{
		Schema:      directWriteSchema(),
		CleanupMode: CleanupGraph,
	})

	for _, size := range []int{1_000, 2_000, 2_001} {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			b.Run("WRITE-01 DeleteRelationship", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					directWriteClearBenchmarkGraph(b, session)
					if _, err := opengraph.WriteGraph(session.Ctx, session.DB, testutil.NewDirectWriteScaleFixture(size)); err != nil {
						b.Fatalf("load fixture: %v", err)
					}
					ids, err := directWriteRelationshipIDs(session.Ctx, session.DB, func() graph.Criteria {
						return query.And(
							query.Kind(query.Relationship(), directWriteDeleteRelationshipKind),
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
					if remaining, err := directWriteCount(session.Ctx, session.DB, "MATCH ()-[r:WriteDeleteRelationship]->() RETURN count(r)"); err != nil || remaining != 1 {
						b.Fatalf("remaining relationships: got %d, err %v", remaining, err)
					}
				}
			})

			b.Run("WRITE-02 DeleteNode cascade", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					directWriteClearBenchmarkGraph(b, session)
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
					if remaining, err := directWriteCount(session.Ctx, session.DB, "MATCH (n:WriteDeleteNode) RETURN count(n)"); err != nil || remaining != 0 {
						b.Fatalf("remaining nodes: got %d, err %v", remaining, err)
					}
					if survivors, err := directWriteCount(session.Ctx, session.DB, "MATCH ()-[r:WriteSurvivor]->() RETURN count(r)"); err != nil || survivors != 1 {
						b.Fatalf("survivor relationships: got %d, err %v", survivors, err)
					}
				}
			})

			b.Run("WRITE-03 CreateRelationship conflict merge", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					directWriteClearBenchmarkGraph(b, session)
					idMap, err := opengraph.WriteGraph(session.Ctx, session.DB, testutil.NewDirectWriteScaleFixture(size))
					if err != nil {
						b.Fatalf("load fixture: %v", err)
					}
					rootID := idMap["write-root"]
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for idx, name := range testutil.FixtureNames("write-target", size) {
							if err := batch.CreateRelationshipByIDs(rootID, idMap[name], directWriteCreateRelationshipKind, directWriteProperties("ordinal", idx, "custom", "first")); err != nil {
								return err
							}
							if err := batch.CreateRelationshipByIDs(rootID, idMap[name], directWriteCreateRelationshipKind, directWriteProperties("custom", "last")); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("create relationships: %v", err)
					}
					b.StopTimer()
					if created, err := directWriteCount(session.Ctx, session.DB, "MATCH ()-[r:WriteCreateRelationship]->() RETURN count(r)"); err != nil || created != int64(size) {
						b.Fatalf("created relationships: got %d, want %d, err %v", created, size, err)
					}
					if merged, err := directWriteCount(session.Ctx, session.DB, "MATCH ()-[r:WriteCreateRelationship]->() WHERE r.custom = 'last' RETURN count(r)"); err != nil || merged != int64(size) {
						b.Fatalf("merged relationships: got %d, want %d, err %v", merged, size, err)
					}
				}
			})

			b.Run("WRITE-04 UpdateNodeBy", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					directWriteClearBenchmarkGraph(b, session)
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for idx := range size {
							if err := batch.UpdateNodeBy(directWriteNodeUpdate(fmt.Sprintf("bench-node-%04d", idx), directWriteUpsertNodeKind, directWriteProperties("ordinal", idx))); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("update nodes: %v", err)
					}
					b.StopTimer()
					if updated, err := directWriteCount(session.Ctx, session.DB, "MATCH (n:WriteUpsertNode) RETURN count(n)"); err != nil || updated != int64(size) {
						b.Fatalf("updated nodes: got %d, want %d, err %v", updated, size, err)
					}
				}
			})

			b.Run("WRITE-05 UpdateRelationshipBy", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					directWriteClearBenchmarkGraph(b, session)
					b.StartTimer()
					if err := session.DB.BatchOperation(session.Ctx, func(batch graph.Batch) error {
						for idx := range size {
							if err := batch.UpdateRelationshipBy(directWriteRelationshipUpdate(
								fmt.Sprintf("bench-source-%04d", idx),
								fmt.Sprintf("bench-target-%04d", idx),
								directWriteUpsertRelationshipKind,
								directWriteProperties("ordinal", idx),
							)); err != nil {
								return err
							}
						}
						return nil
					}, graph.WithBatchSize(2_000)); err != nil {
						b.Fatalf("update relationships: %v", err)
					}
					b.StopTimer()
					if updated, err := directWriteCount(session.Ctx, session.DB, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"); err != nil || updated != int64(size) {
						b.Fatalf("updated relationships: got %d, want %d, err %v", updated, size, err)
					}
				}
			})
		})
	}
}

func directWriteSetup(t *testing.T) (graph.Database, context.Context) {
	t.Helper()
	session := Open(t, Options{
		Schema:      directWriteSchema(),
		CleanupMode: CleanupGraph,
	})
	return session.DB, session.Ctx
}

func directWriteSchema() *graph.Schema {
	nodeKinds, edgeKinds := directWriteKinds()
	graphSchema := graph.Graph{
		Name:  "integration_test",
		Nodes: nodeKinds,
		Edges: edgeKinds,
		NodeConstraints: []graph.Constraint{{
			Field: directWriteObjectID,
			Type:  graph.BTreeIndex,
		}},
	}
	return &graph.Schema{
		Graphs:       []graph.Graph{graphSchema},
		DefaultGraph: graphSchema,
	}
}

func directWriteKinds() (graph.Kinds, graph.Kinds) {
	fixtureNodeKinds, fixtureEdgeKinds := testutil.NewDirectWriteScaleFixture(2).Kinds()
	nodeKinds := fixtureNodeKinds.Add(
		directWriteUpsertNodeKind,
		directWriteUpsertNodeKindA,
		directWriteUpsertNodeKindB,
		directWriteUpsertNodeKindC,
		directWriteEntityKind,
		directWriteGroupKind,
		directWriteUnrelatedKind,
		directWriteSuffixKind,
		directWriteMissingKind,
		directWriteScanKind,
	)
	edgeKinds := fixtureEdgeKinds.Add(
		directWriteCreateRelationshipKind,
		directWriteCreateRelationshipOther,
		directWriteUpsertRelationshipKind,
		directWriteUpsertRelationshipOther,
		directWriteEnsureRelationshipKind,
	)
	return nodeKinds, edgeKinds
}

func directWriteLoadDirectWriteFixture(t *testing.T, ctx context.Context, db graph.Database, size int) (*opengraph.Graph, opengraph.IDMap) {
	t.Helper()
	ClearGraph(t, db, ctx)
	fixture := testutil.NewDirectWriteScaleFixture(size)
	idMap, err := opengraph.WriteGraph(ctx, db, fixture)
	require.NoError(t, err)
	return fixture, idMap
}

func directWriteCreateEndpoints(t *testing.T, ctx context.Context, db graph.Database, objectIDs ...string) (*graph.Node, *graph.Node, *graph.Node) {
	t.Helper()
	require.Len(t, objectIDs, 3)
	created := make([]*graph.Node, 0, len(objectIDs))
	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, objectID := range objectIDs {
			node, err := tx.CreateNode(directWriteProperties(directWriteObjectID, objectID), directWriteEndpointKind)
			if err != nil {
				return err
			}
			created = append(created, node)
		}
		return nil
	}))
	return created[0], created[1], created[2]
}

func directWriteCreateNode(t *testing.T, ctx context.Context, db graph.Database, properties *graph.Properties, kinds ...graph.Kind) *graph.Node {
	t.Helper()
	var created *graph.Node
	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		created, err = tx.CreateNode(properties, kinds...)
		return err
	}))
	return created
}

func directWriteProperties(keyValues ...any) *graph.Properties {
	properties := graph.NewProperties()
	for idx := 0; idx < len(keyValues); idx += 2 {
		properties.Set(keyValues[idx].(string), keyValues[idx+1])
	}
	return properties
}

func directWriteIncidentCount(targets int) int64 {
	switch targets {
	case 0:
		return 0
	case 1:
		return 1
	default:
		return int64(targets + 1)
	}
}

func directWriteNodeUpdate(objectID string, kind graph.Kind, properties *graph.Properties) graph.NodeUpdate {
	properties = properties.Clone().Set(directWriteObjectID, objectID)
	return graph.NodeUpdate{
		Node:               graph.PrepareNode(properties, kind),
		IdentityProperties: []string{directWriteObjectID},
	}
}

func directWriteRelationshipUpdate(startObjectID, endObjectID string, kind graph.Kind, properties *graph.Properties) graph.RelationshipUpdate {
	return graph.RelationshipUpdate{
		Start: graph.PrepareNode(
			directWriteProperties(directWriteObjectID, startObjectID),
			directWriteEndpointKind,
		),
		StartIdentityProperties: []string{directWriteObjectID},
		End: graph.PrepareNode(
			directWriteProperties(directWriteObjectID, endObjectID),
			directWriteEndpointKind,
		),
		EndIdentityProperties: []string{directWriteObjectID},
		Relationship:          graph.PrepareRelationship(properties, kind),
	}
}

func directWriteFetchRelationshipIDs(t *testing.T, ctx context.Context, db graph.Database, criteria graph.CriteriaProvider) []graph.ID {
	t.Helper()
	ids, err := directWriteRelationshipIDs(ctx, db, criteria)
	require.NoError(t, err)
	return ids
}

func directWriteRelationshipIDs(ctx context.Context, db graph.Database, criteria graph.CriteriaProvider) ([]graph.ID, error) {
	var ids []graph.ID
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		ids, err = ops.FetchRelationshipIDs(tx.Relationships().Filterf(criteria))
		return err
	}); err != nil {
		return nil, err
	}

	return ids, nil
}

func directWriteFetchRelationship(t *testing.T, ctx context.Context, db graph.Database, startID, endID graph.ID, kind graph.Kind) *graph.Relationship {
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

func directWriteFetchNodeByObjectID(t *testing.T, ctx context.Context, db graph.Database, objectID string) *graph.Node {
	t.Helper()
	node, err := directWriteFindNodeByObjectID(ctx, db, objectID)
	require.NoError(t, err)
	return node
}

func directWriteFindNodeByObjectID(ctx context.Context, db graph.Database, objectID string) (*graph.Node, error) {
	var node *graph.Node
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		node, err = tx.Nodes().Filterf(func() graph.Criteria {
			return query.Equals(query.NodeProperty(directWriteObjectID), objectID)
		}).First()
		return err
	}); err != nil {
		return nil, err
	}

	return node, nil
}

func directWriteFetchNodeByID(t *testing.T, ctx context.Context, db graph.Database, id graph.ID) *graph.Node {
	t.Helper()
	var node *graph.Node
	require.NoError(t, db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		var err error
		node, err = tx.Nodes().Filter(query.Equals(query.NodeID(), id)).First()
		return err
	}))
	return node
}

func directWriteStringProperty(t *testing.T, properties *graph.Properties, key string) string {
	t.Helper()
	value, err := properties.Get(key).String()
	require.NoError(t, err)
	return value
}

func directWriteEnsureRelationship(ctx context.Context, db graph.Database, startID, endID graph.ID, kind graph.Kind, properties *graph.Properties) (graph.ID, bool, error) {
	var (
		id      graph.ID
		created bool
	)
	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if relationship, err := tx.Relationships().Filterf(func() graph.Criteria {
			return query.And(
				query.Equals(query.StartID(), startID),
				query.Equals(query.EndID(), endID),
				query.Kind(query.Relationship(), kind),
			)
		}).First(); err != nil {
			if !graph.IsErrNotFound(err) {
				return err
			}

			if createdRelationship, err := tx.CreateRelationshipByIDs(startID, endID, kind, properties); err != nil {
				return err
			} else {
				id = createdRelationship.ID
				created = true
				return nil
			}
		} else {
			relationship.Properties.Merge(properties)
			id = relationship.ID
			return tx.UpdateRelationship(relationship)
		}
	}); err != nil {
		return 0, false, err
	}

	return id, created, nil
}

func directWriteGetOrCreateGroup(ctx context.Context, db graph.Database, properties *graph.Properties) (*graph.Node, bool, error) {
	objectID, err := properties.Get(directWriteObjectID).String()
	if err != nil {
		return nil, false, err
	}

	var (
		result  *graph.Node
		created bool
	)
	if err := db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		if existing, err := tx.Nodes().Filterf(func() graph.Criteria {
			return query.Equals(query.NodeProperty(directWriteObjectID), objectID)
		}).First(); err != nil {
			if !graph.IsErrNotFound(err) {
				return err
			}

			if createdNode, err := tx.CreateNode(properties.Clone(), directWriteEntityKind, directWriteGroupKind); err != nil {
				return err
			} else {
				result = createdNode
				created = true
				return nil
			}
		} else {
			result = existing
			if !result.Kinds.ContainsOneOf(directWriteGroupKind) {
				result.AddKinds(directWriteGroupKind)
				return tx.UpdateNode(result)
			}

			return nil
		}
	}); err != nil {
		return nil, false, err
	}

	return result, created, nil
}

func directWriteClearBenchmarkGraph(b *testing.B, session *Session) {
	b.Helper()
	if err := session.DB.WriteTransaction(session.Ctx, func(tx graph.Transaction) error {
		return tx.Nodes().Delete()
	}); err != nil {
		b.Fatalf("clear benchmark graph: %v", err)
	}
}

func directWriteCount(ctx context.Context, db graph.Database, cypher string) (int64, error) {
	var count int64
	if err := db.ReadTransaction(ctx, func(tx graph.Transaction) error {
		result := tx.Query(cypher, nil)
		defer result.Close()

		if !result.Next() {
			return result.Error()
		}
		if err := result.Scan(&count); err != nil {
			return err
		}
		return result.Error()
	}); err != nil {
		return 0, err
	}

	return count, nil
}
