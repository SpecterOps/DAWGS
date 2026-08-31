package pg

import (
	"context"
	"testing"
	"time"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/query"
	"github.com/stretchr/testify/require"
)

func TestPrepareRegularQueryUsesStableNamesAndDoesNotMutateBuilder(t *testing.T) {
	firstBuilder := query.NewBuilder(nil)
	firstBuilder.Apply(
		query.Where(query.Equals(query.NodeProperty("objectid"), graph.ID(1))),
		query.Returning(query.Node()),
	)

	first, err := firstBuilder.Build(false)
	require.NoError(t, err)

	firstPrepared, err := prepareRegularQuery(first)
	require.NoError(t, err)
	require.Same(t, first, firstPrepared.query)

	secondBuilder := query.NewBuilder(nil)
	secondBuilder.Apply(
		query.Where(query.Equals(query.NodeProperty("objectid"), graph.ID(2))),
		query.Returning(query.Node()),
	)

	second, err := secondBuilder.Build(false)
	require.NoError(t, err)

	secondPrepared, err := prepareRegularQuery(second)
	require.NoError(t, err)

	require.Equal(t, firstPrepared.source, secondPrepared.source)
	require.Equal(t, map[string]any{
		builderParameterPrefix + "0": graph.ID(1),
	}, firstPrepared.parameters)
	require.Equal(t, map[string]any{
		builderParameterPrefix + "0": graph.ID(2),
	}, secondPrepared.parameters)
	require.NotContains(t, firstPrepared.source, "1")
	require.NotContains(t, secondPrepared.source, "2")

	// Re-preparing the original builder query proves that preparation did not
	// leak request-local generated names into it.
	preparedAgain, err := prepareRegularQuery(first)
	require.NoError(t, err)
	require.Equal(t, firstPrepared.source, preparedAgain.source)
}

func TestPrepareRegularQueryRedactsLiteralsInSQLComments(t *testing.T) {
	builder := query.NewBuilder(nil)
	builder.Apply(query.Returning(query.Literal("sensitive-value")))

	regularQuery, err := builder.Build(false)
	require.NoError(t, err)
	prepared, err := prepareRegularQuery(regularQuery)
	require.NoError(t, err)
	require.Contains(t, prepared.source, "sensitive-value")
	require.NotContains(t, prepared.commentSource, "sensitive-value")
	require.Contains(t, prepared.commentSource, "$STRIPPED")

	commented := commentRegularQuery(prepared.commentSource, "select 1")
	require.NotContains(t, commented, "sensitive-value")
	require.Contains(t, commented, "$STRIPPED")
}

func TestBuilderPreparedQueriesShareTranslationCacheKey(t *testing.T) {
	cache := newTranslationCache(1)
	first := preparedRegularQuery{
		source: "MATCH (n) WHERE n.id = $__dawgs_builder_p0 RETURN n",
		parameters: map[string]any{
			builderParameterPrefix + "0": graph.ID(1),
		},
	}
	second := preparedRegularQuery{
		source: first.source,
		parameters: map[string]any{
			builderParameterPrefix + "0": graph.ID(2),
		},
	}
	key := cache.Key(first.source, 1, first.parameters)

	_, bindings, err := cache.GetOrBuild(key, first.parameters, cacheableBuild(
		"select @p0",
		map[string]any{
			"p0": uint64(1),
		},
		map[string]string{
			"p0": builderParameterPrefix + "0",
		},
	))
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"p0": uint64(1),
	}, bindings)

	builds := 0
	_, bindings, err = cache.GetOrBuild(cache.Key(second.source, 1, second.parameters), second.parameters, func() (string, translationCacheBuildResult, error) {
		builds++
		return "", translationCacheBuildResult{}, nil
	})
	require.NoError(t, err)
	require.Zero(t, builds)
	require.Equal(t, map[string]any{
		"p0": uint64(2),
	}, bindings)
}

func buildPreparedNodeLookup(t *testing.T, id graph.ID) preparedRegularQuery {
	t.Helper()
	builder := query.NewBuilder(nil)
	builder.Apply(
		query.Where(query.Equals(query.NodeProperty("objectid"), id)),
		query.Returning(query.Node()),
	)
	regularQuery, err := builder.Build(false)
	require.NoError(t, err)
	prepared, err := prepareRegularQuery(regularQuery)
	require.NoError(t, err)
	return prepared
}

func TestCompileRegularQueryCachesBuilderShapeAndRebindsValues(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 2,
	})
	first := buildPreparedNodeLookup(t, graph.ID(1))
	second := buildPreparedNodeLookup(t, graph.ID(2))

	firstSQL, firstBindings, err := manager.compileRegularQuery(context.Background(), first, 7)
	require.NoError(t, err)
	secondSQL, secondBindings, err := manager.compileRegularQuery(context.Background(), second, 7)
	require.NoError(t, err)

	require.Equal(t, firstSQL, secondSQL)
	require.NotEqual(t, firstBindings, secondBindings)
	require.Len(t, firstBindings, 1)
	require.Len(t, secondBindings, 1)
	for name, firstValue := range firstBindings {
		require.Equal(t, uint64(1), firstValue)
		require.Equal(t, uint64(2), secondBindings[name])
	}
	stats := manager.translationCache.Stats()
	require.Equal(t, int64(1), stats.Misses)
	require.Equal(t, int64(1), stats.Hits)
	require.Equal(t, int64(1), stats.Insertions)
}

func compilePreparedBuilderQuery(t *testing.T, builder *query.Builder) preparedRegularQuery {
	t.Helper()

	regularQuery, err := builder.Build(false)
	require.NoError(t, err)
	prepared, err := prepareRegularQuery(regularQuery)
	require.NoError(t, err)
	return prepared
}

// requireWarmBindingsMatchCold compiles one builder request to populate a cache,
// then verifies that a same-shape request receives the exact bindings it would
// have received from a fresh translation. This protects the complete builder
// preparation -> cache -> translation path rather than only cache internals.
func requireWarmBindingsMatchCold(t *testing.T, first, second preparedRegularQuery) {
	t.Helper()
	setOptimizedTranslationForTest(t, true)

	warmManager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 8,
	})
	coldManager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 0,
	})

	firstSQL, firstBindings, err := warmManager.compileRegularQuery(context.Background(), first, 7)
	require.NoError(t, err)
	warmSQL, warmBindings, err := warmManager.compileRegularQuery(context.Background(), second, 7)
	require.NoError(t, err)
	coldSQL, coldBindings, err := coldManager.compileRegularQuery(context.Background(), second, 7)
	require.NoError(t, err)

	require.Equal(t, first.source, second.source)
	require.Equal(t, firstSQL, warmSQL)
	require.Equal(t, coldSQL, warmSQL)
	require.Equal(t, coldBindings, warmBindings)
	require.NotEqual(t, firstBindings, warmBindings, "a cache hit must not retain the first request's values")
	require.Equal(t, int64(1), warmManager.translationCache.Stats().Misses)
	require.Equal(t, int64(1), warmManager.translationCache.Stats().Hits)
}

func TestCompileRegularQueryWarmBindingsMatchColdForMultipleParameters(t *testing.T) {
	timestamp := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	testCases := []struct {
		name   string
		first  func() *query.Builder
		second func() *query.Builder
	}{
		{
			name: "nested scalar predicates preserve order",
			first: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Where(query.And(
					query.Equals(query.NodeProperty("id"), graph.ID(101)),
					query.Or(
						query.StringContains(query.NodeProperty("name"), "first-name"),
						query.GreaterThan(query.NodeProperty("rank"), int64(303)),
					),
					query.Equals(query.NodeProperty("enabled"), true),
				)), query.Returning(query.Node()))
				return builder
			},
			second: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Where(query.And(
					query.Equals(query.NodeProperty("id"), graph.ID(202)),
					query.Or(
						query.StringContains(query.NodeProperty("name"), "second-name"),
						query.GreaterThan(query.NodeProperty("rank"), int64(404)),
					),
					query.Equals(query.NodeProperty("enabled"), false),
				)), query.Returning(query.Node()))
				return builder
			},
		},
		{
			name: "ID collections and temporal values",
			first: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Where(query.And(
					query.InIDs(query.Node(), graph.ID(1), graph.ID(3)),
					query.Before(query.NodeProperty("seen"), timestamp),
				)), query.Returning(query.Node()))
				return builder
			},
			second: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Where(query.And(
					query.InIDs(query.Node(), graph.ID(2), graph.ID(4), graph.ID(6)),
					query.Before(query.NodeProperty("seen"), timestamp.Add(time.Hour)),
				)), query.Returning(query.Node()))
				return builder
			},
		},
		{
			name: "floating point values",
			first: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Where(query.Equals(query.NodeProperty("score"), 1.25)), query.Returning(query.Node()))
				return builder
			},
			second: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Where(query.Equals(query.NodeProperty("score"), 9.75)), query.Returning(query.Node()))
				return builder
			},
		},
		{
			name: "map properties",
			first: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Create(query.NodePattern(nil, query.Parameter(map[string]any{"name": "first", "rank": int64(1)}))))
				return builder
			},
			second: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(query.Create(query.NodePattern(nil, query.Parameter(map[string]any{"name": "second", "rank": int64(2)}))))
				return builder
			},
		},
		{
			name: "set values and filter values",
			first: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(
					query.Where(query.Equals(query.NodeProperty("id"), graph.ID(11))),
					query.Updatef(func() graph.Criteria { return query.SetProperties(query.Node(), map[string]any{"value": "first"}) }),
				)
				return builder
			},
			second: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(
					query.Where(query.Equals(query.NodeProperty("id"), graph.ID(22))),
					query.Updatef(func() graph.Criteria { return query.SetProperties(query.Node(), map[string]any{"value": "second"}) }),
				)
				return builder
			},
		},
		{
			name: "relationship set values and filter values",
			first: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(
					query.Where(query.InIDs(query.Relationship(), graph.ID(31))),
					query.Updatef(func() graph.Criteria {
						return query.SetProperties(query.Relationship(), map[string]any{"value": "first"})
					}),
				)
				return builder
			},
			second: func() *query.Builder {
				builder := query.NewBuilder(nil)
				builder.Apply(
					query.Where(query.InIDs(query.Relationship(), graph.ID(32))),
					query.Updatef(func() graph.Criteria {
						return query.SetProperties(query.Relationship(), map[string]any{"value": "second"})
					}),
				)
				return builder
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			first := compilePreparedBuilderQuery(t, testCase.first())
			second := compilePreparedBuilderQuery(t, testCase.second())
			requireWarmBindingsMatchCold(t, first, second)
		})
	}
}

func TestCompileRegularQueryPartitionsParameterTypesAndStructuralLiterals(t *testing.T) {
	manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 8,
	})

	stringBuilder := query.NewBuilder(nil)
	stringBuilder.Apply(query.Where(query.Equals(query.NodeProperty("value"), "one")), query.Returning(query.Node()))
	stringQuery := compilePreparedBuilderQuery(t, stringBuilder)
	intBuilder := query.NewBuilder(nil)
	intBuilder.Apply(query.Where(query.Equals(query.NodeProperty("value"), int64(1))), query.Returning(query.Node()))
	intQuery := compilePreparedBuilderQuery(t, intBuilder)
	require.Equal(t, stringQuery.source, intQuery.source)
	require.NotEqual(t, manager.translationCache.Key(stringQuery.source, 7, stringQuery.parameters), manager.translationCache.Key(intQuery.source, 7, intQuery.parameters))

	limitOne := query.NewBuilder(nil)
	limitOne.Apply(query.Where(query.Equals(query.NodeProperty("value"), "one")), query.Limit(1), query.Returning(query.Node()))
	limitTwo := query.NewBuilder(nil)
	limitTwo.Apply(query.Where(query.Equals(query.NodeProperty("value"), "one")), query.Limit(2), query.Returning(query.Node()))
	limitOneQuery := compilePreparedBuilderQuery(t, limitOne)
	limitTwoQuery := compilePreparedBuilderQuery(t, limitTwo)
	require.NotEqual(t, limitOneQuery.source, limitTwoQuery.source)
	require.NotEqual(t, manager.translationCache.Key(limitOneQuery.source, 7, limitOneQuery.parameters), manager.translationCache.Key(limitTwoQuery.source, 7, limitTwoQuery.parameters))

	emptyIDs := map[string]any{"ids": []graph.ID{}}
	populatedIDs := map[string]any{"ids": []graph.ID{1, 2}}
	strings := map[string]any{"ids": []string{"1", "2"}}
	require.Equal(t, manager.translationCache.Key("RETURN $ids", 7, emptyIDs), manager.translationCache.Key("RETURN $ids", 7, populatedIDs))
	require.NotEqual(t, manager.translationCache.Key("RETURN $ids", 7, populatedIDs), manager.translationCache.Key("RETURN $ids", 7, strings))
}

func TestPrepareRegularQueryRenamesExplicitAndRepeatedParameterSymbols(t *testing.T) {
	build := func(value string) preparedRegularQuery {
		parameter := cypher.NewParameter(builderParameterPrefix+"99", value)
		builder := query.NewBuilder(nil)
		builder.Apply(
			query.Where(query.And(
				query.Equals(query.NodeProperty("first"), parameter),
				query.Equals(query.NodeProperty("second"), parameter),
			)),
			query.Returning(query.Node()),
		)
		return compilePreparedBuilderQuery(t, builder)
	}

	first := build("first")
	second := build("second")
	require.NotContains(t, first.source, builderParameterPrefix+"99")
	require.Equal(t, first.source, second.source)
	require.Equal(t, map[string]any{
		builderParameterPrefix + "0": "first",
		builderParameterPrefix + "1": "first",
	}, first.parameters)
	requireWarmBindingsMatchCold(t, first, second)
}

func TestCompileRegularQueryDisabledCacheDoesNotRetainTranslation(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 0,
	})

	_, _, err := manager.compileRegularQuery(context.Background(), buildPreparedNodeLookup(t, graph.ID(1)), 7)
	require.NoError(t, err)
	_, _, err = manager.compileRegularQuery(context.Background(), buildPreparedNodeLookup(t, graph.ID(2)), 7)
	require.NoError(t, err)
	stats := manager.translationCache.Stats()
	require.Zero(t, stats.Misses)
	require.Zero(t, stats.Size)
	require.Equal(t, int64(2), stats.Bypasses)
}

func TestCompileRegularQueryUnoptimizedBypassesAndPreservesWarmEntries(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 2,
	})

	first := buildPreparedNodeLookup(t, graph.ID(1))
	_, _, err := manager.compileRegularQuery(context.Background(), first, 7)
	require.NoError(t, err)
	warmStats := manager.translationCache.Stats()

	setOptimizedTranslationForTest(t, false)
	_, _, err = manager.compileRegularQuery(context.Background(), buildPreparedNodeLookup(t, graph.ID(2)), 7)
	require.NoError(t, err)
	disabledStats := manager.translationCache.Stats()
	require.Equal(t, warmStats.Hits, disabledStats.Hits)
	require.Equal(t, warmStats.Misses, disabledStats.Misses)
	require.Equal(t, warmStats.Insertions, disabledStats.Insertions)
	require.Equal(t, warmStats.Bypasses+1, disabledStats.Bypasses)
	require.Equal(t, warmStats.UnoptimizedCompilations+1, disabledStats.UnoptimizedCompilations)

	setOptimizedTranslationForTest(t, true)
	_, _, err = manager.compileRegularQuery(context.Background(), buildPreparedNodeLookup(t, graph.ID(3)), 7)
	require.NoError(t, err)
	require.Equal(t, warmStats.Hits+1, manager.translationCache.Stats().Hits)
}

func TestCompileTextUnoptimizedBypassesCache(t *testing.T) {
	setOptimizedTranslationForTest(t, true)
	manager := NewSchemaManagerWithOptions(nil, 0, DriverOptions{
		TranslationCacheEntries: 2,
	})

	_, _, err := manager.compileText(context.Background(), "MATCH (n) RETURN n", nil, 7)
	require.NoError(t, err)
	warmStats := manager.translationCache.Stats()

	setOptimizedTranslationForTest(t, false)
	_, _, err = manager.compileText(context.Background(), "MATCH (n) RETURN n", nil, 7)
	require.NoError(t, err)
	disabledStats := manager.translationCache.Stats()
	require.Equal(t, warmStats.Hits, disabledStats.Hits)
	require.Equal(t, warmStats.Misses, disabledStats.Misses)
	require.Equal(t, warmStats.Bypasses+1, disabledStats.Bypasses)
	require.Equal(t, warmStats.UnoptimizedCompilations+1, disabledStats.UnoptimizedCompilations)
}
