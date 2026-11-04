//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestBatchIdentitySeparatesKindOperations(t *testing.T) {
	db, ctx := directWriteSetup(t)
	add := directWriteCreateNode(t, ctx, db, directWriteProperties("objectid", "batch-add"), directWriteUpsertNodeKind)
	remove := directWriteCreateNode(t, ctx, db, directWriteProperties("objectid", "batch-remove"), directWriteUpsertNodeKind, directWriteUpsertNodeKindA)
	add.AddKinds(directWriteUpsertNodeKindA)
	remove.DeleteKinds(directWriteUpsertNodeKindA)
	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		return batch.UpdateNodes([]*graph.Node{add, remove})
	}))
	require.True(t, directWriteFetchNodeByID(t, ctx, db, add.ID).Kinds.ContainsOneOf(directWriteUpsertNodeKindA))
	require.False(t, directWriteFetchNodeByID(t, ctx, db, remove.ID).Kinds.ContainsOneOf(directWriteUpsertNodeKindA))
}

func TestBatchIdentitySeparatesRelationshipEndpoints(t *testing.T) {
	db, ctx := directWriteSetup(t)
	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		for _, row := range [][3]string{{"1", "23", "first"}, {"12", "3", "second"}, {"1", "23", "last"}} {
			if err := batch.UpdateRelationshipBy(directWriteRelationshipUpdate(row[0], row[1], directWriteUpsertRelationshipKind, directWriteProperties("marker", row[2]))); err != nil {
				return err
			}
		}
		return nil
	}))
	for _, row := range [][3]string{{"1", "23", "last"}, {"12", "3", "second"}} {
		start := directWriteFetchNodeByObjectID(t, ctx, db, row[0])
		end := directWriteFetchNodeByObjectID(t, ctx, db, row[1])
		rel := directWriteFetchRelationship(t, ctx, db, start.ID, end.ID, directWriteUpsertRelationshipKind)
		require.Equal(t, row[2], rel.Properties.Get("marker").Any())
	}
	require.Equal(t, int64(2), countByCypher(t, ctx, db, "MATCH ()-[r:WriteUpsertRelationship]->() RETURN count(r)"))
}

func TestBatchNodePropertyDeletions(t *testing.T) {
	db, ctx := directWriteSetup(t)
	for _, single := range []bool{false, true} {
		t.Run(map[bool]string{false: "grouped submissions", true: "individual submissions"}[single], func(t *testing.T) {
			ClearGraph(t, db, ctx)
			nodes := []*graph.Node{}
			for _, name := range []string{"delete", "reset", "unchanged", "multiple"} {
				nodes = append(nodes, directWriteCreateNode(t, ctx, db,
					directWriteProperties("objectid", name, "keep", true, "old", "value", "punctuation`key", "extra"), directWriteUpsertNodeKind))
			}
			nodes[0].Properties.Delete("old").Delete("absent").Set("new", true)
			nodes[1].Properties.Delete("old").Set("old", "reset")
			nodes[3].Properties.Delete("old").Delete("punctuation`key")
			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				if single {
					for _, node := range nodes {
						if err := batch.UpdateNodes([]*graph.Node{node}); err != nil {
							return err
						}
					}
					return nil
				}
				return batch.UpdateNodes(nodes)
			}))
			for idx, node := range nodes {
				stored := directWriteFetchNodeByID(t, ctx, db, node.ID)
				require.True(t, stored.Properties.Get("keep").Any().(bool))
				require.Equal(t, idx != 0 && idx != 3, stored.Properties.Exists("old"))
				require.Equal(t, idx != 3, stored.Properties.Exists("punctuation`key"))
				require.False(t, stored.Properties.Exists("absent"))
				if idx == 0 {
					require.Equal(t, true, stored.Properties.Get("new").Any())
				}
				if idx == 1 {
					require.Equal(t, "reset", stored.Properties.Get("old").Any())
				}
			}
		})
	}
}

func TestBatchNodeUpsertDeletedKinds(t *testing.T) {
	db, ctx := directWriteSetup(t)
	target := directWriteCreateNode(t, ctx, db, directWriteProperties("objectid", "upsert-remove", "preserved", "yes"), directWriteUpsertNodeKind, directWriteUpsertNodeKindA, directWriteUpsertNodeKindB)
	survivor := directWriteCreateNode(t, ctx, db, directWriteProperties("objectid", "upsert-survivor"), directWriteUpsertNodeKind, directWriteUpsertNodeKindA)
	require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
		update := directWriteNodeUpdate("upsert-remove", directWriteUpsertNodeKindC, directWriteProperties("changed", true))
		update.Node.DeleteKinds(directWriteUpsertNodeKindA)
		if err := batch.UpdateNodeBy(update); err != nil {
			return err
		}
		return batch.UpdateNodeBy(directWriteNodeUpdate("upsert-survivor", directWriteUpsertNodeKindB, graph.NewProperties()))
	}))
	stored := directWriteFetchNodeByObjectID(t, ctx, db, "upsert-remove")
	require.Equal(t, target.ID, stored.ID)
	require.Equal(t, "yes", stored.Properties.Get("preserved").Any())
	require.Equal(t, true, stored.Properties.Get("changed").Any())
	require.True(t, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKind))
	require.True(t, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKindB))
	require.True(t, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKindC))
	require.False(t, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKindA), "identity upsert must apply tracked kind removals")
	require.True(t, directWriteFetchNodeByID(t, ctx, db, survivor.ID).Kinds.ContainsOneOf(directWriteUpsertNodeKindA))
}

func TestBatchNodeUpsertKindRemovalCoalescing(t *testing.T) {
	db, ctx := directWriteSetup(t)
	for _, removeLast := range []bool{false, true} {
		t.Run(map[bool]string{false: "remove then add", true: "add then remove"}[removeLast], func(t *testing.T) {
			ClearGraph(t, db, ctx)
			target := directWriteCreateNode(t, ctx, db, directWriteProperties("objectid", "coalesce"), directWriteUpsertNodeKind, directWriteUpsertNodeKindA)
			add := directWriteNodeUpdate("coalesce", directWriteUpsertNodeKindA, graph.NewProperties())
			remove := directWriteNodeUpdate("coalesce", directWriteUpsertNodeKind, graph.NewProperties())
			remove.Node.DeleteKinds(directWriteUpsertNodeKindA)
			updates := []graph.NodeUpdate{remove, add}
			if removeLast {
				updates = []graph.NodeUpdate{add, remove}
			}
			require.NoError(t, db.BatchOperation(ctx, func(batch graph.Batch) error {
				for _, update := range updates {
					if err := batch.UpdateNodeBy(update); err != nil {
						return err
					}
				}
				return nil
			}, graph.WithBatchSize(1)))
			stored := directWriteFetchNodeByID(t, ctx, db, target.ID)
			require.Equal(t, !removeLast, stored.Kinds.ContainsOneOf(directWriteUpsertNodeKindA))
		})
	}
}
