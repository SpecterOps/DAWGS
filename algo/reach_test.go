package algo_test

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/specterops/dawgs/algo"
	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/container/util"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Test the reachability cache tool.
// Graph:
// 0 → 1 → 2 → 0   (cycle A)
// 3 → 4 → 5 → 3   (cycle B)
// 6 → 7 → 8 → 6   (cycle C)
// 9 → 10 → 11 → 9   (cycle D)
// 2 → 3           (bridge from A to B)
// 5 → 6           (bridge from B to C)
// 8 → 9           (bridge from C to D)
func TestReachabilityCache(t *testing.T) {
	var (
		digraph = util.BuildGraph(container.NewCSRDigraphBuilder, map[uint64][]uint64{
			0:  {1},
			1:  {2},
			2:  {0, 3},
			3:  {4},
			4:  {5},
			5:  {3, 6},
			6:  {7},
			7:  {8},
			8:  {6, 9},
			9:  {10},
			10: {11},
			11: {9},
		})

		expected = map[uint64][]uint64{
			0:  []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			1:  []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			2:  []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			3:  []uint64{3, 4, 5, 6, 7, 8, 9, 10, 11},
			4:  []uint64{3, 4, 5, 6, 7, 8, 9, 10, 11},
			5:  []uint64{3, 4, 5, 6, 7, 8, 9, 10, 11},
			6:  []uint64{6, 7, 8, 9, 10, 11},
			7:  []uint64{6, 7, 8, 9, 10, 11},
			8:  []uint64{6, 7, 8, 9, 10, 11},
			9:  []uint64{9, 10, 11},
			10: []uint64{9, 10, 11},
			11: []uint64{9, 10, 11},
		}

		ctx          = context.Background()
		reachability = algo.NewReachabilityCache(ctx, digraph, 1500)
	)

	// Validate directional reach via the component graph
	require.True(t, reachability.CanReach(0, 11, graph.DirectionBoth))

	require.True(t, reachability.CanReach(0, 5, graph.DirectionOutbound))
	require.False(t, reachability.CanReach(0, 5, graph.DirectionInbound))

	require.True(t, reachability.CanReach(5, 0, graph.DirectionInbound))
	require.False(t, reachability.CanReach(5, 0, graph.DirectionOutbound))

	// Validate reach rollup
	for expectedNode, expectedReach := range expected {
		actualReach := reachability.ReachOfComponentContainingMember(expectedNode, graph.DirectionOutbound).Slice()
		require.Equalf(t, expectedReach, actualReach, "Reach for node %d is invalid", expectedNode)
	}
}

// This oracle walks the original graph without using SCCs or the reach cache.
func expectedReach(adj map[uint64][]uint64, root uint64, direction graph.Direction) []uint64 {
	if _, known := adj[root]; !known {
		return []uint64{}
	}

	neighbors := map[uint64][]uint64{}
	for start, ends := range adj {
		for _, end := range ends {
			if direction != graph.DirectionInbound {
				neighbors[start] = append(neighbors[start], end)
			}
			if direction != graph.DirectionOutbound {
				neighbors[end] = append(neighbors[end], start)
			}
		}
	}

	seen := map[uint64]bool{root: true}
	pending := []uint64{root}
	for len(pending) > 0 {
		next := pending[0]
		pending = pending[1:]
		for _, neighbor := range neighbors[next] {
			if !seen[neighbor] {
				seen[neighbor] = true
				pending = append(pending, neighbor)
			}
		}
	}

	result := make([]uint64, 0, len(seen))
	for member := range seen {
		result = append(result, member)
	}
	slices.Sort(result)
	return result
}

func requireReach(t *testing.T, reach *algo.ReachabilityCache, adj map[uint64][]uint64, member uint64, direction graph.Direction) {
	t.Helper()
	expected := expectedReach(adj, member, direction)
	require.Equalf(t, expected, reach.ReachOfComponentContainingMember(member, direction).Slice(), "member %d, direction %v", member, direction)

	combined := cardinality.NewBitmap64()
	for _, component := range reach.ReachSliceOfComponentContainingMember(member, direction) {
		combined.Or(component)
	}
	require.Equalf(t, expected, combined.Slice(), "slice for member %d, direction %v", member, direction)
}

func TestReachabilityCacheDiamondTail(t *testing.T) {
	for _, direction := range []graph.Direction{graph.DirectionOutbound, graph.DirectionInbound} {
		t.Run(fmt.Sprint(direction), func(t *testing.T) {
			adj := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}
			if direction == graph.DirectionInbound {
				reversed := map[uint64][]uint64{}
				for start, ends := range adj {
					if _, ok := reversed[start]; !ok {
						reversed[start] = nil
					}
					for _, end := range ends {
						reversed[end] = append(reversed[end], start)
					}
				}
				adj = reversed
			}

			digraph := util.BuildGraph(container.NewCSRDigraphBuilder, adj)
			warm := algo.NewReachabilityCache(context.Background(), digraph, 16)
			requireReach(t, warm, adj, 1, direction)
			for _, member := range []uint64{2, 3} {
				// Both branches must include the shared tail after warming the root.
				requireReach(t, warm, adj, member, direction)
				cold := algo.NewReachabilityCache(context.Background(), digraph, 16)
				require.Equal(t, cold.ReachOfComponentContainingMember(member, direction).Slice(), warm.ReachOfComponentContainingMember(member, direction).Slice())
			}
		})
	}
}

func TestReachabilityCacheCompletedEntryReuse(t *testing.T) {
	adj := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}
	digraph := util.BuildGraph(container.NewCSRDigraphBuilder, adj)
	reach := algo.NewReachabilityCache(context.Background(), digraph, 16)

	// A complete tail can short-circuit a larger traversal without being
	// changed by that traversal's union into its own local result.
	requireReach(t, reach, adj, 4, graph.DirectionOutbound)
	hits := reach.Stats().Hits()
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, reach.ReachOfComponentContainingMember(1, graph.DirectionOutbound).Slice())
	require.Greater(t, reach.Stats().Hits(), hits)
	requireReach(t, reach, adj, 4, graph.DirectionOutbound)

	// Capacity one forces the root out when a different component is queried.
	reach = algo.NewReachabilityCache(context.Background(), digraph, 1)
	requireReach(t, reach, adj, 1, graph.DirectionOutbound)
	requireReach(t, reach, adj, 3, graph.DirectionOutbound)
	misses := reach.Stats().Misses()
	requireReach(t, reach, adj, 1, graph.DirectionOutbound)
	require.Greater(t, reach.Stats().Misses(), misses)
	require.Equal(t, int64(1), reach.Stats().Size())
}

func TestReachabilityCacheQueryOrderAndEviction(t *testing.T) {
	fixtures := map[string]map[uint64][]uint64{
		"diamond": {1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}},
		// Each diamond vertex is an SCC; a disconnected cycle and isolated
		// vertex check that neither direction leaks unrelated members.
		"scc_diamond": {0: {1}, 1: {0, 2, 4}, 2: {3}, 3: {2, 6}, 4: {5}, 5: {4, 6}, 6: {7}, 7: {6, 8}, 8: {}, 9: {10}, 10: {9}, 1 << 40: {}},
		"shortcuts":   {0: {0, 1, 2, 3}, 1: {2, 3}, 2: {3}, 3: {4}, 4: {}},
		"empty":       {},
	}
	for name, adj := range fixtures {
		for _, capacity := range []int{0, 1, 2, 64} {
			t.Run(fmt.Sprintf("%s/capacity_%d", name, capacity), func(t *testing.T) {
				digraph := util.BuildGraph(container.NewCSRDigraphBuilder, adj)
				reach := algo.NewReachabilityCache(context.Background(), digraph, capacity)
				members := make([]uint64, 0, len(adj)+1)
				for member := range adj {
					members = append(members, member)
				}
				members = append(members, 999) // Unknown member.
				slices.Sort(members)
				random := rand.New(rand.NewSource(73))
				for permutation := 0; permutation < 8; permutation++ {
					for _, member := range members {
						for _, direction := range []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound, graph.DirectionBoth} {
							requireReach(t, reach, adj, member, direction)
						}
					}
					random.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
				}
				require.LessOrEqual(t, reach.Stats().Size(), int64(reach.Stats().Capacity))
			})
		}
	}
}

func TestReachabilityCacheGeneratedGraphs(t *testing.T) {
	random := rand.New(rand.NewSource(73))
	for fixture := 0; fixture < 24; fixture++ {
		t.Run(fmt.Sprint(fixture), func(t *testing.T) {
			adj := map[uint64][]uint64{}
			for start := uint64(0); start < 10; start++ {
				adj[start] = nil
				for end := uint64(0); end < 10; end++ {
					// Alternate DAGs and cyclic graphs with branch convergence.
					if (fixture%2 != 0 || end > start) && random.Intn(5) == 0 {
						adj[start] = append(adj[start], end)
					}
				}
			}
			digraph := util.BuildGraph(container.NewCSRDigraphBuilder, adj)
			reach := algo.NewReachabilityCache(context.Background(), digraph, 1+fixture%4)
			for round := 0; round < 3; round++ {
				for _, member := range random.Perm(10) {
					for _, direction := range []graph.Direction{graph.DirectionOutbound, graph.DirectionInbound, graph.DirectionBoth} {
						requireReach(t, reach, adj, uint64(member), direction)
					}
				}
			}
		})
	}
}

func TestReachabilityCacheConcurrentQueries(t *testing.T) {
	adj := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {4}}
	digraph := util.BuildGraph(container.NewCSRDigraphBuilder, adj)
	reach := algo.NewReachabilityCache(context.Background(), digraph, 2)
	for worker := 0; worker < 8; worker++ {
		t.Run(fmt.Sprint(worker), func(t *testing.T) {
			t.Parallel()
			for round := 0; round < 20; round++ {
				for member := range adj {
					for _, direction := range []graph.Direction{graph.DirectionInbound, graph.DirectionOutbound, graph.DirectionBoth} {
						requireReach(t, reach, adj, member, direction)
					}
				}
			}
		})
	}
}

func TestReachabilityCacheResultIsolationAndSetOperations(t *testing.T) {
	adj := map[uint64][]uint64{1: {2, 3}, 2: {4}, 3: {4}, 4: {5}, 5: {}}
	digraph := util.BuildGraph(container.NewCSRDigraphBuilder, adj)
	reach := algo.NewReachabilityCache(context.Background(), digraph, 16)
	reach.ReachOfComponentContainingMember(1, graph.DirectionOutbound).Clear()
	requireReach(t, reach, adj, 1, graph.DirectionOutbound)

	for _, member := range []uint64{2, 3} {
		union := cardinality.NewBitmap64With(99)
		reach.OrReach(member, graph.DirectionOutbound, union)
		require.Equal(t, []uint64{4, 5, 99}, union.Slice())
		xor := cardinality.NewBitmap64With(4, 99)
		reach.XorReach(member, graph.DirectionOutbound, xor)
		require.Equal(t, []uint64{5, 99}, xor.Slice())
		requireReach(t, reach, adj, member, graph.DirectionOutbound)
	}
}
