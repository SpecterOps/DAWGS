package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// preparedCompactBuilderForTest builds an all-shortest AST with stable parameter names for cache comparisons.
func preparedCompactBuilderForTest(t *testing.T, start, end graph.ID, kinds ...graph.Kind) preparedRegularQuery {
	t.Helper()
	criteria := []graph.Criteria{query.Equals(query.StartID(), start), query.Equals(query.EndID(), end)}
	if len(kinds) > 0 {
		criteria = append(criteria, query.KindIn(query.Relationship(), kinds...))
	}
	builder := query.NewBuilderWithCriteria(query.Where(query.And(criteria...)), query.Returning(query.Path()))
	regular, err := builder.Build(true)
	require.NoError(t, err)
	prepared, err := prepareRegularQuery(regular)
	require.NoError(t, err)
	return prepared
}

// TestCompactBuilderCompilationSeparatesSnapshotModeAndPreservesWarmBindings verifies legacy and compact cache entries remain distinct while warm parameters rebind correctly.
func TestCompactBuilderCompilationSeparatesSnapshotModeAndPreservesWarmBindings(t *testing.T) {
	manager := NewSchemaManager(nil, 0)
	t.Cleanup(func() { manager.parseCache.Close(); manager.compilationCache.Close() })
	first := preparedCompactBuilderForTest(t, 11, 12)
	second := preparedCompactBuilderForTest(t, 21, 22)
	legacy, err := manager.compileWithMapperAndCompactSearch(context.Background(), first.source, first.parameters, 7, &first.identity, first.translationQuery, manager, false)
	require.NoError(t, err)
	require.NotContains(t, legacy.statement, "all_shortest_paths_dag_v2")
	compact, err := manager.compileWithMapperAndCompactSearch(context.Background(), first.source, first.parameters, 7, &first.identity, first.translationQuery, manager, true)
	require.NoError(t, err)
	require.Contains(t, compact.statement, "all_shortest_paths_dag_v2")
	before := manager.compilationCache.Stats()
	warm, err := manager.compileWithMapperAndCompactSearch(context.Background(), second.source, second.parameters, 7, &second.identity, second.translationQuery, manager, true)
	require.NoError(t, err)
	require.Equal(t, compact.statement, warm.statement)
	require.Greater(t, manager.compilationCache.Stats().Hits, before.Hits)
	coldManager := NewSchemaManager(nil, 0)
	t.Cleanup(func() { coldManager.parseCache.Close(); coldManager.compilationCache.Close() })
	cold, err := coldManager.compileWithMapperAndCompactSearch(context.Background(), second.source, second.parameters, 7, &second.identity, second.translationQuery, coldManager, true)
	require.NoError(t, err)
	require.Equal(t, cold.statement, warm.statement)
	require.Equal(t, cold.bindings, warm.bindings)
	legacyAgain, err := manager.compileWithMapperAndCompactSearch(context.Background(), second.source, second.parameters, 7, &second.identity, second.translationQuery, manager, false)
	require.NoError(t, err)
	require.Equal(t, legacy.statement, legacyAgain.statement)
}
