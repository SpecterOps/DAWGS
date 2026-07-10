package container_test

import (
	"testing"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func collectAdjacentEdges(ts container.Triplestore, node uint64, direction graph.Direction) []container.Edge {
	var edges []container.Edge

	ts.EachAdjacentEdge(node, direction, func(next container.Edge) bool {
		edges = append(edges, next)
		return true
	})

	return edges
}

func collectEdges(ts container.Triplestore) []container.Edge {
	var edges []container.Edge

	ts.EachEdge(func(next container.Edge) bool {
		edges = append(edges, next)
		return true
	})

	return edges
}

func TestTriplestoreProjectionFiltersDeletedEndNodesFromAdjacentEdges(t *testing.T) {
	ts := container.NewTriplestore()
	ts.AddTriple(10, 1, 2)
	ts.AddTriple(11, 2, 3)

	projection := ts.Projection(cardinality.NewBitmap64With(3), cardinality.NewBitmap64())

	require.Equal(t, uint64(1), projection.NumEdges())
	require.Equal(t, []container.Edge{{ID: 10, Start: 1, End: 2}}, collectEdges(projection))
	require.Empty(t, collectAdjacentEdges(projection, 3, graph.DirectionInbound))
	require.Empty(t, collectAdjacentEdges(projection, 2, graph.DirectionOutbound))
	require.Equal(t, []container.Edge{{ID: 10, Start: 1, End: 2}}, collectAdjacentEdges(projection, 1, graph.DirectionOutbound))
}
