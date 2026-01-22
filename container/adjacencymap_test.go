package container_test

import (
	"testing"

	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

func TestAdjacencyMapDigraph(t *testing.T) {
	var (
		digraph  = container.NewAdjacencyMapGraph()
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

func BenchmarkAdjacencyMapDigraphAdjacency(b *testing.B) {
	const (
		maxNodes    = 100_000
		numAdjacent = 100
	)

	// Create a test graph with many nodes and edges
	adj := make(map[uint64][]uint64)
	for i := uint64(0); i < maxNodes; i++ {
		adj[i] = make([]uint64, numAdjacent)
		for j := 0; j < numAdjacent; j++ {
			adj[i][j] = (i + uint64(j)) % 1000
		}
	}

	csrGraph := container.BuildGraph(container.NewAdjacencyMapGraph, adj)

	// Use a simple delegate function for testing
	delegate := func(adjacent uint64) bool {
		return adjacent > 500
	}

	b.ResetTimer()

	node := uint64(0)

	for b.Loop() {
		csrGraph.EachAdjacentNode(node, graph.DirectionOutbound, delegate)

		if node += 1; node >= maxNodes {
			node = 0
		}
	}
}
