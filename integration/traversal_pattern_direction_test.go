//go:build manual_integration

package integration

import (
	"context"
	"sync"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/traversal"
	"github.com/stretchr/testify/require"
)

// The DAWGS-074 regression verifies complete BreadthFirst pattern traversals
// whose steps change direction. Each fixture is committed before traversal
// because BreadthFirst executes its own read transactions across workers.

var (
	traversalPatternNodeKind = graph.StringKind("TraversalPatternNode")
	traversalPatternEdgeKind = graph.StringKind("TraversalPatternEdge")
)

func traversalPatternSchema() *graph.Schema {
	graphSchema := graph.Graph{
		Name:  "integration_test",
		Nodes: graph.Kinds{traversalPatternNodeKind},
		Edges: graph.Kinds{traversalPatternEdgeKind},
	}

	return &graph.Schema{
		Graphs:       []graph.Graph{graphSchema},
		DefaultGraph: graphSchema,
	}
}

// traversalPatternFixture clears the graph and commits nodes named by the
// fixture keys plus one directed edge per pair. Edge IDs are returned in the
// fixture's declared order.
func traversalPatternFixture(t *testing.T, db graph.Database, ctx context.Context, names []string, edges [][2]string) (map[string]*graph.Node, []graph.ID) {
	t.Helper()

	ClearGraph(t, db, ctx)

	var (
		created = make(map[string]*graph.Node, len(names))
		edgeIDs = make([]graph.ID, 0, len(edges))
	)

	require.NoError(t, db.WriteTransaction(ctx, func(tx graph.Transaction) error {
		for _, name := range names {
			node, err := tx.CreateNode(graph.NewProperties(), traversalPatternNodeKind)
			if err != nil {
				return err
			}

			created[name] = node
		}

		for _, edge := range edges {
			relationship, err := tx.CreateRelationshipByIDs(created[edge[0]].ID, created[edge[1]].ID, traversalPatternEdgeKind, graph.NewProperties())
			if err != nil {
				return err
			}

			edgeIDs = append(edgeIDs, relationship.ID)
		}

		return nil
	}))

	return created, edgeIDs
}

func traversalPatternRun(t *testing.T, db graph.Database, ctx context.Context, root *graph.Node, build func(delegate traversal.PatternMatchDelegate) traversal.Driver) []graph.Path {
	t.Helper()

	var (
		mutex = &sync.Mutex{}
		paths []graph.Path
	)

	driver := build(func(terminal *graph.PathSegment) error {
		// BreadthFirst completes terminals across parallel workers, so the
		// delegate must synchronize its collection.
		mutex.Lock()
		defer mutex.Unlock()
		paths = append(paths, terminal.Path())
		return nil
	})

	require.NoError(t, traversal.New(db, 2).BreadthFirst(ctx, traversal.Plan{
		Root:   root,
		Driver: driver,
	}))

	return paths
}

func requireSingleTerminal(t *testing.T, paths []graph.Path, nodeIDs []graph.ID, edgeIDs []graph.ID) {
	t.Helper()

	require.Len(t, paths, 1)

	gotNodes := make([]graph.ID, 0, len(paths[0].Nodes))
	for _, node := range paths[0].Nodes {
		gotNodes = append(gotNodes, node.ID)
	}

	gotEdges := make([]graph.ID, 0, len(paths[0].Edges))
	for _, edge := range paths[0].Edges {
		gotEdges = append(gotEdges, edge.ID)
	}

	require.Equal(t, nodeIDs, gotNodes)
	require.Equal(t, edgeIDs, gotEdges)
}

func TestTraversalPatternDirectionChange(t *testing.T) {
	session := Open(t, Options{
		Schema:      traversalPatternSchema(),
		CleanupMode: CleanupGraph,
	})

	t.Run("outbound then inbound", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c"},
			[][2]string{{"a", "b"}, {"c", "b"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				InboundWithDepth(1, 1).
				Do(delegate)
		})

		requireSingleTerminal(t, paths,
			[]graph.ID{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID},
			edgeIDs,
		)
	})

	t.Run("inbound then outbound", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c"},
			[][2]string{{"b", "a"}, {"b", "c"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				InboundWithDepth(1, 1).
				OutboundWithDepth(1, 1).
				Do(delegate)
		})

		requireSingleTerminal(t, paths,
			[]graph.ID{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID},
			edgeIDs,
		)
	})

	t.Run("same direction control", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c"},
			[][2]string{{"a", "b"}, {"b", "c"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				OutboundWithDepth(1, 1).
				Do(delegate)
		})

		requireSingleTerminal(t, paths,
			[]graph.ID{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID},
			edgeIDs,
		)
	})

	t.Run("alternating three steps", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c", "d"},
			[][2]string{{"a", "b"}, {"c", "b"}, {"c", "d"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				InboundWithDepth(1, 1).
				OutboundWithDepth(1, 1).
				Do(delegate)
		})

		requireSingleTerminal(t, paths,
			[]graph.ID{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID, nodes["d"].ID},
			edgeIDs,
		)
	})

	t.Run("required second step without match", func(t *testing.T) {
		nodes, _ := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b"},
			[][2]string{{"a", "b"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				InboundWithDepth(1, 1).
				Do(delegate)
		})

		require.Empty(t, paths)
	})
}
