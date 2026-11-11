package util

import "github.com/specterops/dawgs/container"

// BuildGraph adds each source and destination node and each directed edge to a fresh builder, then builds its graph.
func BuildGraph(constructor func() container.DigraphBuilder, adj map[uint64][]uint64) container.DirectedGraph {
	builder := constructor()

	for src, outs := range adj {
		builder.AddNode(src)

		for _, dst := range outs {
			builder.AddNode(dst)
			builder.AddEdge(src, dst)
		}
	}

	return builder.Build()
}
