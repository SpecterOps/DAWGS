package pg

import (
	"context"
	"testing"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

// preparedCompactEndpointInequalityBuilder retains a direct comparison between the two endpoint identity expressions.
func preparedCompactEndpointInequalityBuilder(t *testing.T, start, end graph.ID) preparedRegularQuery {
	t.Helper()
	builder := query.NewBuilderWithCriteria(
		query.Where(query.And(
			query.Equals(query.StartID(), start),
			query.Equals(query.EndID(), end),
			cypher.NewComparison(query.StartID(), cypher.OperatorNotEquals, query.EndID()),
		)),
		query.Returning(query.Path()),
	)
	regular, err := builder.Build(true)
	require.NoError(t, err)
	prepared, err := prepareRegularQuery(regular)
	require.NoError(t, err)
	return prepared
}

// TestCompactBuilderEndpointInequalityRebindsWarmEndpoints checks that equal endpoint values do not change qualification or leak through cached bindings.
func TestCompactBuilderEndpointInequalityRebindsWarmEndpoints(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	manager := NewSchemaManager(nil, 0)
	t.Cleanup(func() {
		manager.parseCache.Close()
		manager.compilationCache.Close()
	})
	var previousSQL string
	for _, endpoints := range [][2]graph.ID{{11, 12}, {11, 11}, {21, 22}} {
		prepared := preparedCompactEndpointInequalityBuilder(t, endpoints[0], endpoints[1])
		translationQuery, err := prepared.translationQuery()
		require.NoError(t, err)
		require.True(t, translate.UsesBudgetedAllShortestPaths(translationQuery))

		before := manager.compilationCache.Stats()
		compiled, err := manager.compileWithMapperAndCompactSearch(context.Background(), prepared.source, prepared.parameters, 7, &prepared.identity, prepared.translationQuery, manager, true)
		require.NoError(t, err)
		require.Contains(t, compiled.statement, "all_shortest_paths_dag_v2")
		if previousSQL != "" {
			require.Equal(t, previousSQL, compiled.statement)
			require.Greater(t, manager.compilationCache.Stats().Hits, before.Hits)
		}
		previousSQL = compiled.statement

		coldManager := NewSchemaManager(nil, 0)
		t.Cleanup(func() {
			coldManager.parseCache.Close()
			coldManager.compilationCache.Close()
		})
		cold, err := coldManager.compileWithMapperAndCompactSearch(context.Background(), prepared.source, prepared.parameters, 7, &prepared.identity, prepared.translationQuery, coldManager, true)
		require.NoError(t, err)
		require.Equal(t, cold.statement, compiled.statement)
		require.Equal(t, cold.bindings, compiled.bindings)
	}
}
