package algo

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/specterops/dawgs/cache"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
)

// BenchmarkReachabilityCache measures reachability queries across the configured graph and cache workloads.
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
					return newReachabilityCache(components, 64)
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

// BenchmarkReachabilityCacheSharedTail models the membership-shaped workload
// behind overlapping reachability queries: many distinct roots that all reach
// a long shared tail. numRoots roots each have a single edge into a chain of
// chainLen components, giving numNodes total nodes to match
// BenchmarkReachabilityCache's scale. Overlapping roots measure how well
// complete intermediate results collapse repeated expansion across queries.
func BenchmarkReachabilityCacheSharedTail(b *testing.B) {
	const (
		numRoots = uint64(128)
		chainLen = uint64(384)
		numNodes = numRoots + chainLen
	)

	builder := container.NewCSRDigraphBuilder()
	for node := uint64(0); node < numNodes; node++ {
		builder.AddNode(node)
	}
	for root := uint64(0); root < numRoots; root++ {
		builder.AddEdge(root, numRoots)
	}
	for node := numRoots; node+1 < numNodes; node++ {
		builder.AddEdge(node, node+1)
	}

	components := NewComponentGraph(context.Background(), builder.Build())

	members := make([]uint64, 0, numNodes)
	for member := uint64(0); member < numNodes; member++ {
		members = append(members, member)
	}
	reversed := slices.Clone(members)
	slices.Reverse(reversed)

	for _, workload := range []string{"cold_root", "all_roots", "all_roots_reverse", "all_roots_capacity_2"} {
		b.Run(workload, func(b *testing.B) {
			capacity := 64
			if workload == "all_roots_capacity_2" {
				capacity = 2
			}

			newCache := func() *ReachabilityCache {
				return newReachabilityCache(components, capacity)
			}

			reach := newCache()
			rootComponent, _ := components.ContainingComponent(0)
			reach.componentReachDFS(rootComponent, graph.DirectionOutbound)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				switch workload {
				case "cold_root":
					newCache().componentReachDFS(rootComponent, graph.DirectionOutbound)
				case "all_roots", "all_roots_capacity_2":
					for _, member := range members {
						component, _ := components.ContainingComponent(member)
						reach.componentReachDFS(component, graph.DirectionOutbound)
					}
				case "all_roots_reverse":
					for _, member := range reversed {
						component, _ := components.ContainingComponent(member)
						reach.componentReachDFS(component, graph.DirectionOutbound)
					}
				}
			}

			b.StopTimer()
			if workload != "cold_root" {
				stats := reach.Stats()
				b.ReportMetric(float64(stats.Hits())/float64(b.N), "hits/op")
				b.ReportMetric(float64(stats.Misses())/float64(b.N), "misses/op")
			}
		})
	}
}

// BenchmarkReachabilityCacheMemoEviction measures cold traversal work after the local memo exceeds its capacity.
//
// Keep a production-configured cold workload above the traversal memo floor.
// Increasing diamond depth must not multiply expansion after memo eviction.
func BenchmarkReachabilityCacheMemoEviction(b *testing.B) {
	for _, levels := range []int{2, 6, 10} {
		b.Run(fmt.Sprintf("levels_%d", levels), func(b *testing.B) {
			digraph, root := sharedLeavesDiamondGraph(levels, graph.DirectionOutbound)
			components := NewComponentGraph(context.Background(), digraph)
			component, _ := components.ContainingComponent(root)
			capacity := int(float64(digraph.NumNodes()) * .15)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				newReachabilityCache(components, capacity).componentReachDFS(component, graph.DirectionOutbound)
			}
		})
	}
}

// BenchmarkReachabilityCacheSharedTailParallel measures the concurrent access
// patterns of a production-configured cache: sharded sieves, a bounded
// traversal memo and in-flight coordination, all exercised through
// NewReachabilityCache. warm_hits measures steady-state lookup traffic across
// worker goroutines; churn_capacity_8 forces constant misses, republishing and
// eviction under the same concurrency.
func BenchmarkReachabilityCacheSharedTailParallel(b *testing.B) {
	const (
		numRoots = uint64(128)
		chainLen = uint64(384)
		numNodes = numRoots + chainLen
	)

	newDigraph := func() container.DirectedGraph {
		builder := container.NewCSRDigraphBuilder()
		for node := uint64(0); node < numNodes; node++ {
			builder.AddNode(node)
		}
		for root := uint64(0); root < numRoots; root++ {
			builder.AddEdge(root, numRoots)
		}
		for node := numRoots; node+1 < numNodes; node++ {
			builder.AddEdge(node, node+1)
		}
		return builder.Build()
	}

	for _, workload := range []string{"warm_hits", "churn_capacity_8", "warm_hits_unsharded"} {
		b.Run(workload, func(b *testing.B) {
			capacity := 64
			if workload == "churn_capacity_8" {
				capacity = 8
			}

			var reach *ReachabilityCache
			if workload == "warm_hits_unsharded" {
				// A/B control: identical shape without sharding, memo cap or
				// in-flight coordination, mirroring struct-literal benches.
				reach = &ReachabilityCache{
					components:             NewComponentGraph(context.Background(), newDigraph()),
					inboundComponentReach:  cache.NewSieve[uint64, cardinality.Duplex[uint64]](capacity),
					outboundComponentReach: cache.NewSieve[uint64, cardinality.Duplex[uint64]](capacity),
				}
			} else {
				reach = NewReachabilityCache(context.Background(), newDigraph(), capacity)
			}
			rootComponent, _ := reach.components.ContainingComponent(0)
			reach.componentReachDFS(rootComponent, graph.DirectionOutbound)

			b.ReportAllocs()
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for member := uint64(0); pb.Next(); member++ {
					component, _ := reach.components.ContainingComponent(member % numNodes)
					reach.componentReachDFS(component, graph.DirectionOutbound)
				}
			})

			b.StopTimer()
			stats := reach.Stats()
			b.ReportMetric(float64(stats.Hits())/float64(b.N), "hits/op")
			b.ReportMetric(float64(stats.Misses())/float64(b.N), "misses/op")
		})
	}
}
