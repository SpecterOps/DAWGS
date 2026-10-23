package algo

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cache"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

func BenchmarkReachabilityCache(b *testing.B) {
	for _, shape := range []string{"chain", "diamonds"} {
		builder := container.NewCSRDigraphBuilder()
		const numNodes = uint64(512)
		for node := uint64(0); node < numNodes; node++ {
			builder.AddNode(node)
			if node+1 < numNodes {
				builder.AddEdge(node, node+1)
			}
			if shape == "diamonds" && node%3 == 0 && node+3 < numNodes {
				builder.AddEdge(node, node+2)
				builder.AddEdge(node+1, node+3)
			}
		}
		components := NewComponentGraph(context.Background(), builder.Build())
		root, _ := components.ContainingComponent(0)
		for _, workload := range []string{"cold_root", "warm_root", "all_roots"} {
			b.Run(shape+"/"+workload, func(b *testing.B) {
				newCache := func() *ReachabilityCache {
					return &ReachabilityCache{
						components:             components,
						inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](64),
						outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](64),
					}
				}
				reach := newCache()
				reach.componentReachDFS(root, graph.DirectionOutbound)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					switch workload {
					case "cold_root":
						newCache().componentReachDFS(root, graph.DirectionOutbound)
					case "warm_root":
						reach.componentReachDFS(root, graph.DirectionOutbound)
					case "all_roots":
						for member := uint64(0); member < numNodes; member++ {
							component, _ := components.ContainingComponent(member)
							reach.componentReachDFS(component, graph.DirectionOutbound)
						}
					}
				}
			})
		}
	}
}
