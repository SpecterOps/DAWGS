package algo

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/container"
	"github.com/specterops/dawgs/graph"
	"github.com/stretchr/testify/require"
)

// Test a simple chain of verticies. In this example, each vertex is its own SCC.
//
// Graph:
// 0 -> 1 -> 2 -> 3
func TestSCC_Chain(t *testing.T) {
	var (
		digraph = container.BuildCSRGraph(map[uint64][]uint64{
			0: {1},
			1: {2},
			2: {3},
		})

		ctx                     = context.Background()
		sccs, nodeToSCCIndex    = StronglyConnectedComponents(ctx, digraph)
		expectedNodeToComponent = map[uint64]uint64{
			0: 3,
			1: 2,
			2: 1,
			3: 0,
		}
	)

	require.Equalf(t, 4, len(sccs), "expected 4 SCCs, got %d", len(sccs))

	// Each SCC must contain exactly one node
	for component, members := range sccs {
		require.Equalf(t, uint64(1), members.Cardinality(), "SCC %d expected size 1, got %d", component, members.Cardinality())
	}

	require.Equal(t, expectedNodeToComponent, nodeToSCCIndex)
}

// Test a simple cycle. In this example, there should be two disconnected components.
//
// Graph:
// 0 -> 0
// 1
func TestSCC_SimpleCycle(t *testing.T) {
	var (
		digraph = container.BuildCSRGraph(map[uint64][]uint64{
			0: {0}, // self‑loop component
			1: {},  // isolated vertex – must be present as a key!
		})

		ctx             = context.Background()
		sccs, nodeToIdx = StronglyConnectedComponents(ctx, digraph)
	)

	require.Equalf(t, 2, len(sccs), "expected 2 components, got %d", len(sccs))

	// The self‑loop should be a component of size 1 (Tarjan treats it as strongly connected)
	require.Equalf(t, uint64(1), sccs[nodeToIdx[0]].Cardinality(), "self‑loop node not alone in its component")
	require.Equalf(t, uint64(1), sccs[nodeToIdx[1]].Cardinality(), "isolated node not alone in its component")
}

// Test two cycles with a bridge (a “figure‑8” graph). There should be one component that contains
// all verticies.
//
// Graph:
// 0 -> 1 -> 2 -> 3
// 1 -> 3
// 2 -> 3
// 3 -> 1
func TestSCC_FigureEight(t *testing.T) {
	var (
		digraph = container.BuildCSRGraph(map[uint64][]uint64{
			0: {1},
			1: {2, 3},
			2: {0},
			3: {1},
		})

		ctx             = context.Background()
		sccs, nodeToIdx = StronglyConnectedComponents(ctx, digraph)
	)

	// The whole graph is one SCC with 4 members
	require.Equalf(t, 1, len(sccs), "expected 1 SCC, got %d", len(sccs))
	require.Equalf(t, uint64(4), sccs[0].Cardinality(), "expected component size 4, got %d", sccs[0].Cardinality())

	// All nodes must map to component 0.
	digraph.EachNode(func(node uint64) bool {
		if component, ok := nodeToIdx[node]; !ok || component != 0 {
			t.Fatalf("node %d expected in component 0, got %d (exists=%v)", node, component, ok)
		}

		return true
	})
}

// Test component graph construction including edge deduplication and directionality.
//
// Graph:
// 0 → 1 → 2 → 0   (cycle A)
// 3 → 4 → 5 → 3   (cycle B)
// 2 → 3           (bridge from A to B)
func TestComponentGraph_EdgeDeduplication(t *testing.T) {
	var (
		digraph = container.BuildCSRGraph(map[uint64][]uint64{
			0: {1},
			1: {2},
			2: {0, 3},
			3: {4},
			4: {5},
			5: {3},
		})

		ctx            = context.Background()
		componentGraph = NewComponentGraph(ctx, digraph)
	)

	// Expect exactly two components
	if len(componentGraph.componentMembers) != 2 {
		t.Fatalf("expected 2 components, got %d", len(componentGraph.componentMembers))
	}

	// Component IDs are 0 and 1 (order depends on traversal)
	var outEdges []uint64

	// Verify that the component digraph contains a *single* edge from component A to B
	componentGraph.Digraph().EachAdjacentNode(0, graph.DirectionOutbound, func(v uint64) bool {
		outEdges = append(outEdges, v)
		return true
	})

	if len(outEdges) == 0 {
		// The edge could be reversed depending on which component got ID 0, so test both possibilities
		componentGraph.Digraph().EachAdjacentNode(0, graph.DirectionInbound, func(v uint64) bool {
			outEdges = append(outEdges, v)
			return true
		})
	}

	require.Equalf(t, 1, len(outEdges), "expected exactly one inter‑component edge, got %v", outEdges)
	require.Equalf(t, uint64(1), outEdges[0], "expected exactly one inter‑component edge, got %v", outEdges)

	var (
		startComp, _ = componentGraph.ContainingComponent(0)
		endComp, _   = componentGraph.ContainingComponent(5)
	)

	// The component containing origin vertex 0 should reach component containing origin vertex 5
	require.Truef(t, componentGraph.ComponentReachable(startComp, endComp, graph.DirectionOutbound), "expected component %d to reach component %d", startComp, endComp)
}

// Test histogram counting.
//
// Graph:
// 0 -> 1 -> 2 -> 0
// 3 -> 4 -> 5 -> 3
// 6 -> 7 -> 8 -> 6
func TestComponentHistogram(t *testing.T) {
	var (
		digraph = container.BuildCSRGraph(map[uint64][]uint64{
			0: {1},
			1: {2},
			2: {0},
			3: {4},
			4: {5},
			5: {3},
			6: {7},
			7: {8},
			8: {6},
		})

		ctx                = context.Background()
		componentGraph     = NewComponentGraph(ctx, digraph)
		componentHistogram = componentGraph.ComponentHistogram([]uint64{0, 1, 2, 3, 4, 5, 6, 7, 8})
	)

	require.Equalf(t, 3, len(componentHistogram), "expected 3 components, got %d", len(componentHistogram))

	for _, count := range componentHistogram {
		require.Equalf(t, uint64(3), count, "each component should have count 3, got %d", count)
	}
}
