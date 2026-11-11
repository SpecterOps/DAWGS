//go:build manual_integration

package integration

import (
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/traversal"
	"github.com/stretchr/testify/require"
)

// The DAWGS-095 regression verifies complete BreadthFirst pattern traversals
// whose following step is optional. The pattern driver formerly eager-fetched
// the optional following expansion and also preserved the boundary segment,
// so the optional expansion ran twice and each of its terminals reached the
// delegate twice. These checks pin exact callback cardinality on both
// backends; consumer-side deduplication cannot substitute for it.

// requirePathSet compares the returned paths against the expected node/edge ID
// sequences independent of traversal order: BreadthFirst completes terminals
// across parallel workers.
func requirePathSet(t *testing.T, paths []graph.Path, expected [][]graph.ID) {
	t.Helper()

	format := func(path graph.Path) []graph.ID {
		ids := make([]graph.ID, 0, len(path.Nodes)+len(path.Edges))
		for _, node := range path.Nodes {
			ids = append(ids, node.ID)
		}
		for _, edge := range path.Edges {
			ids = append(ids, edge.ID)
		}
		return ids
	}

	observed := make([][]graph.ID, 0, len(paths))
	for _, path := range paths {
		observed = append(observed, format(path))
	}

	require.ElementsMatch(t, expected, observed)
}

func TestTraversalPatternOptionalStep(t *testing.T) {
	session := Open(t, Options{
		Schema:      traversalPatternSchema(),
		CleanupMode: CleanupGraph,
	})

	t.Run("optional second step with match", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c"},
			[][2]string{{"a", "b"}, {"b", "c"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				OutboundWithDepth(0, 0).
				Do(delegate)
		})

		requirePathSet(t, paths, [][]graph.ID{
			{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID, edgeIDs[0], edgeIDs[1]},
		})
	})

	t.Run("optional second step without match", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b"},
			[][2]string{{"a", "b"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				OutboundWithDepth(0, 0).
				Do(delegate)
		})

		requirePathSet(t, paths, [][]graph.ID{
			{nodes["a"].ID, nodes["b"].ID, edgeIDs[0]},
		})
	})

	t.Run("optional second step with multiple matches", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c1", "c2"},
			[][2]string{{"a", "b"}, {"b", "c1"}, {"b", "c2"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				OutboundWithDepth(0, 0).
				Do(delegate)
		})

		requirePathSet(t, paths, [][]graph.ID{
			{nodes["a"].ID, nodes["b"].ID, nodes["c1"].ID, edgeIDs[0], edgeIDs[1]},
			{nodes["a"].ID, nodes["b"].ID, nodes["c2"].ID, edgeIDs[0], edgeIDs[2]},
		})
	})

	t.Run("optional middle step with required tail", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c", "d"},
			[][2]string{{"a", "b"}, {"b", "c"}, {"c", "d"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				OutboundWithDepth(0, 0).
				OutboundWithDepth(1, 1).
				Do(delegate)
		})

		// The optional middle step either skips its expansion at b or matches
		// b→c; both branches continue through the required tail exactly once.
		requirePathSet(t, paths, [][]graph.ID{
			{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID, edgeIDs[0], edgeIDs[1]},
			{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID, nodes["d"].ID, edgeIDs[0], edgeIDs[1], edgeIDs[2]},
		})
	})

	t.Run("optional direction-changed second step", func(t *testing.T) {
		nodes, edgeIDs := traversalPatternFixture(t, session.DB, session.Ctx,
			[]string{"a", "b", "c"},
			[][2]string{{"a", "b"}, {"c", "b"}},
		)

		paths := traversalPatternRun(t, session.DB, session.Ctx, nodes["a"], func(delegate traversal.PatternMatchDelegate) traversal.Driver {
			return traversal.NewPattern().
				OutboundWithDepth(1, 1).
				InboundWithDepth(0, 1).
				Do(delegate)
		})

		requirePathSet(t, paths, [][]graph.ID{
			{nodes["a"].ID, nodes["b"].ID, nodes["c"].ID, edgeIDs[0], edgeIDs[1]},
		})
	})
}
