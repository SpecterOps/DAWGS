package query_test

import (
	"testing"

	query "github.com/specterops/dawgs/drivers/pg/query"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestNodeUpdateBatchIdentityCoalescing(t *testing.T) {
	batch := query.NewNodeUpdateBatch()
	add := func(a, b any, marker string) *query.Future[graph.ID] {
		future, err := batch.Add(graph.NodeUpdate{
			Node:         graph.PrepareNode(graph.AsProperties(map[string]any{"a": a, "b": b, "marker": marker})),
			IdentityKind: graph.StringKind("Base"), IdentityProperties: []string{"b", "a"},
		})
		require.NoError(t, err)
		return future
	}
	first := add("1", "23", "first")
	second := add("12", "3", "second")
	typed := add(int64(1), "23", "typed")
	require.NotSame(t, first, second)
	require.NotSame(t, first, typed)
	require.Same(t, first, add("1", "23", "last"))
	require.Len(t, batch.Updates, 3)
	markers := map[string]bool{}
	for _, update := range batch.Updates {
		markers[update.Node.Properties.Get("marker").Any().(string)] = true
	}
	require.Equal(t, map[string]bool{"last": true, "second": true, "typed": true}, markers)
}

func TestRelationshipUpdateBatchIdentityCoalescing(t *testing.T) {
	batch := query.NewRelationshipUpdateBatch()
	add := func(start, end, marker string) {
		require.NoError(t, batch.Add(graph.RelationshipUpdate{
			Relationship: graph.NewRelationship(0, 0, 0, graph.AsProperties(map[string]any{"marker": marker}), graph.StringKind("R")),
			Start:        graph.PrepareNode(graph.AsProperties(map[string]any{"id": start})), StartIdentityProperties: []string{"id"},
			End: graph.PrepareNode(graph.AsProperties(map[string]any{"id": end})), EndIdentityProperties: []string{"id"},
		}))
	}
	add("1", "23", "first")
	add("12", "3", "second")
	add("1", "23", "last")
	require.Len(t, batch.NodeUpdates.Updates, 4)
	require.Len(t, batch.Updates, 2)
	for _, update := range batch.Updates {
		require.Contains(t, []string{"last", "second"}, update.Relationship.Properties.Get("marker").Any())
		require.NotSame(t, update.StartID, update.EndID)
	}
}
