package algo_test

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/algo"
	"github.com/specterops/dawgs/container"
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
		digraph = container.BuildGraph(container.NewCSRGraph, map[uint64][]uint64{
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
