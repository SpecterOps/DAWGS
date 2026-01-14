package container_test

import (
	"testing"

	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestCSRDigraph(t *testing.T) {
	var (
		digraph  = container.NewCSRGraph()
		expected = map[uint64][]uint64{
			1: []uint64{2, 3, 4, 5, 6, 7},
			2: []uint64{3, 4, 5, 6, 7},
			3: []uint64{4, 5, 6, 7},
			4: []uint64{},
			5: []uint64{6},
			6: []uint64{},
			7: []uint64{},
		}
	)

	digraph.AddEdge(2, 1)
	digraph.AddEdge(3, 2)
	digraph.AddEdge(4, 3)
	digraph.AddEdge(5, 3)
	digraph.AddEdge(6, 5)
	digraph.AddEdge(7, 3)

	for expectedNode, expectedReach := range expected {
		actualReach := container.Reach(digraph, expectedNode, graph.DirectionInbound).Slice()
		require.Equal(t, expectedReach, actualReach)
	}
}
