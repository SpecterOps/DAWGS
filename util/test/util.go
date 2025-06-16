package test

import (
	"sync/atomic"

	"github.com/specterops/dawgs/graph"
)

var (
	idSequence int64 = 0
)

func nextID() graph.ID {
	return graph.ID(atomic.AddInt64(&idSequence, 1))
}

func Node(kinds ...graph.Kind) *graph.Node {
	return graph.NewNode(nextID(), graph.NewProperties(), kinds...)
}

func Edge(start, end *graph.Node, kind graph.Kind) *graph.Relationship {
	return graph.NewRelationship(nextID(), start.ID, end.ID, graph.NewProperties(), kind)
}
