package test

import (
	"sync/atomic"

	"github.com/specterops/dawgs/graph"
)

var (
	// idSequence supplies process-local fixture IDs through atomic increments.
	idSequence int64 = 0
)

// nextID atomically reserves the next fixture identifier.
func nextID() graph.ID {
	return graph.ID(atomic.AddInt64(&idSequence, 1))
}

// Node creates a fixture node with a fresh identifier and an empty property bag.
func Node(kinds ...graph.Kind) *graph.Node {
	return graph.NewNode(nextID(), graph.NewProperties(), kinds...)
}

// Edge creates a fixture relationship between the supplied endpoints with fresh identity and properties.
func Edge(start, end *graph.Node, kind graph.Kind) *graph.Relationship {
	return graph.NewRelationship(nextID(), start.ID, end.ID, graph.NewProperties(), kind)
}
