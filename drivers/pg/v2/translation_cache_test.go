package v2

import (
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/stretchr/testify/require"
)

func newTestCache(t *testing.T, capacity int) (*connectionCacheProvider, *connectionTranslationCache, *pgx.Conn) {
	t.Helper()
	provider, err := newConnectionCacheProvider(Config{TranslationCacheEntries: capacity})
	require.NoError(t, err)
	conn := &pgx.Conn{}
	provider.registerConnection(conn)
	cache, ok := provider.CacheForConnection(conn).(*connectionTranslationCache)
	require.True(t, ok)
	return provider, cache, conn
}

func cacheableBuild(sql string, value any) func() (translate.Result, string, error) {
	return func() (translate.Result, string, error) {
		return translate.Result{
			Parameters:       map[string]any{"i0": value},
			ParameterSources: map[string]string{"i0": "id"},
		}, sql, nil
	}
}

// TestConnectionTranslationCacheRebindsCurrentValues verifies cached SQL does
// not retain caller parameter values.
func TestConnectionTranslationCacheRebindsCurrentValues(t *testing.T) {
	_, cache, _ := newTestCache(t, 2)

	sql, parameters, err := cache.TranslateWithPolicy(" MATCH (n) WHERE id(n) = $id RETURN n ", 1, map[string]any{"id": int64(1)}, "incumbent", cacheableBuild("select @i0", int64(1)))
	require.NoError(t, err)
	require.Equal(t, "select @i0", sql)
	require.Equal(t, int64(1), parameters["i0"])

	sql, parameters, err = cache.TranslateWithPolicy("MATCH (n) WHERE id(n) = $id RETURN n", 1, map[string]any{"id": int64(2)}, "incumbent", cacheableBuild("wrong", int64(999)))
	require.NoError(t, err)
	require.Equal(t, "select @i0", sql)
	require.Equal(t, int64(2), parameters["i0"])
	require.Equal(t, TranslationCacheStats{Hits: 1, Misses: 1, Insertions: 1, Entries: 1, Capacity: 2}, cache.statsSnapshot())
}

// TestConnectionTranslationCachePartitionsInputs verifies all inputs that can
// change SQL occupy independent entries.
func TestConnectionTranslationCachePartitionsInputs(t *testing.T) {
	provider, cache, _ := newTestCache(t, 8)
	builds := 0
	build := func() (translate.Result, string, error) {
		builds++
		return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
	}

	for _, request := range []struct {
		query  string
		graph  int32
		params map[string]any
		policy string
	}{
		{"RETURN $value", 1, map[string]any{"value": int64(1)}, "one"},
		{"RETURN $value", 2, map[string]any{"value": int64(1)}, "one"},
		{"RETURN $value", 1, map[string]any{"value": "1"}, "one"},
		{"RETURN $value", 1, map[string]any{"other": int64(1)}, "one"},
		{"RETURN $value", 1, map[string]any{"value": int64(1)}, "two"},
	} {
		_, _, err := cache.TranslateWithPolicy(request.query, request.graph, request.params, request.policy, build)
		require.NoError(t, err)
	}
	provider.advanceSchemaGeneration()
	_, _, err := cache.TranslateWithPolicy("RETURN $value", 1, map[string]any{"value": int64(1)}, "one", build)
	require.NoError(t, err)

	require.Equal(t, 6, builds)
}

func TestConnectionTranslationCacheCapacityAndBypasses(t *testing.T) {
	t.Run("capacity is enforced", func(t *testing.T) {
		_, cache, _ := newTestCache(t, 1)
		build := func() (translate.Result, string, error) {
			return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
		}
		_, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "one", build)
		require.NoError(t, err)
		_, _, err = cache.TranslateWithPolicy("RETURN 2", 1, nil, "one", build)
		require.NoError(t, err)
		stats := cache.statsSnapshot()
		require.Equal(t, 1, stats.Entries)
		require.Equal(t, uint64(1), stats.Evictions)
	})

	t.Run("zero capacity bypasses retention", func(t *testing.T) {
		_, cache, _ := newTestCache(t, 0)
		builds := 0
		build := func() (translate.Result, string, error) {
			builds++
			return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
		}
		for range 2 {
			_, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "one", build)
			require.NoError(t, err)
		}
		require.Equal(t, 2, builds)
		require.Equal(t, TranslationCacheStats{Misses: 2, Bypasses: 2}, cache.statsSnapshot())
	})
}

func TestConnectionTranslationCacheRejectsUncacheableResultsAndClonesSources(t *testing.T) {
	_, cache, _ := newTestCache(t, 2)
	sources := map[string]string{"i0": "id"}
	builds := 0
	build := func() (translate.Result, string, error) {
		builds++
		return translate.Result{Parameters: map[string]any{"i0": int64(1)}, ParameterSources: sources}, "select @i0", nil
	}

	_, _, err := cache.TranslateWithPolicy("RETURN $id", 1, map[string]any{"id": int64(1)}, "one", build)
	require.NoError(t, err)
	sources["i0"] = "mutated"
	_, parameters, err := cache.TranslateWithPolicy("RETURN $id", 1, map[string]any{"id": int64(2)}, "one", build)
	require.NoError(t, err)
	require.Equal(t, int64(2), parameters["i0"])
	require.Equal(t, 1, builds)

	uncacheable := func() (translate.Result, string, error) {
		return translate.Result{Parameters: map[string]any{"i0": int64(1)}}, "select @i0", nil
	}
	_, _, err = cache.TranslateWithPolicy("RETURN 2", 1, nil, "one", uncacheable)
	require.NoError(t, err)
	stats := cache.statsSnapshot()
	require.Equal(t, 1, stats.Entries)
	require.Equal(t, uint64(1), stats.Bypasses)
}

func TestConnectionTranslationCacheDoesNotRetainFailures(t *testing.T) {
	_, cache, _ := newTestCache(t, 2)
	expected := errors.New("translation failed")
	_, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "one", func() (translate.Result, string, error) {
		return translate.Result{}, "partial", expected
	})
	require.ErrorIs(t, err, expected)
	require.Zero(t, cache.statsSnapshot().Entries)
}

func TestConnectionCacheProviderSeparatesAndRetiresPhysicalConnections(t *testing.T) {
	provider, err := newConnectionCacheProvider(Config{TranslationCacheEntries: 2})
	require.NoError(t, err)
	first, second := &pgx.Conn{}, &pgx.Conn{}
	provider.registerConnection(first)
	provider.registerConnection(second)
	firstCache := provider.CacheForConnection(first).(*connectionTranslationCache)
	secondCache := provider.CacheForConnection(second).(*connectionTranslationCache)
	require.NotSame(t, firstCache, secondCache)

	_, _, err = firstCache.TranslateWithPolicy("RETURN 1", 1, nil, "one", func() (translate.Result, string, error) {
		return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
	})
	require.NoError(t, err)
	require.Zero(t, secondCache.statsSnapshot().Entries)

	provider.removeConnection(first)
	provider.removeConnection(first)
	require.Nil(t, provider.CacheForConnection(first))
	_, _, err = firstCache.TranslateWithPolicy("RETURN 1", 1, nil, "one", func() (translate.Result, string, error) {
		return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
	})
	require.NoError(t, err)

	stats := provider.stats()
	require.Equal(t, 1, stats.LiveConnections)
	require.Equal(t, uint64(1), stats.RetiredConnections)
	require.Len(t, stats.Connections, 1)
	require.Len(t, provider.states, 1) // first was deleted; the second remains in the registry.
}

func TestConnectionCacheProviderRejectsNegativeCapacity(t *testing.T) {
	provider, err := newConnectionCacheProvider(Config{TranslationCacheEntries: -1})
	require.Nil(t, provider)
	require.ErrorContains(t, err, "must not be negative")
}

func TestConnectionCacheProviderCloseDropsStateAndPreventsResurrection(t *testing.T) {
	provider, cache, conn := newTestCache(t, 2)
	_, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "one", func() (translate.Result, string, error) {
		return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
	})
	require.NoError(t, err)
	provider.close()
	provider.close()

	require.Nil(t, provider.CacheForConnection(conn))
	stats := provider.stats()
	require.Zero(t, stats.LiveConnections)
	require.Equal(t, uint64(1), stats.RetiredConnections)
	require.Empty(t, stats.Connections)
	require.Nil(t, provider.states)

	_, _, err = cache.TranslateWithPolicy("RETURN 1", 1, nil, "one", func() (translate.Result, string, error) {
		return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, "select 1", nil
	})
	require.NoError(t, err)
	require.Zero(t, cache.statsSnapshot().Entries)
}

func TestConnectionCacheProviderStatsAndCleanupAreRaceSafe(t *testing.T) {
	provider, _, conn := newTestCache(t, 2)
	const workers = 8
	var group sync.WaitGroup
	group.Add(workers + 1)
	for range workers {
		go func() {
			defer group.Done()
			for range 100 {
				_ = provider.stats()
			}
		}()
	}
	go func() {
		defer group.Done()
		provider.removeConnection(conn)
	}()
	group.Wait()

	require.Nil(t, provider.CacheForConnection(conn))
}

// TestConnectionWorkspaceReadinessTracksGenerationAndFailures verifies that a
// physical connection skips only successfully initialized workspaces and
// becomes unready after a schema-generation change or retirement.
func TestConnectionWorkspaceReadinessTracksGenerationAndFailures(t *testing.T) {
	provider, _, conn := newTestCache(t, 2)
	var calls int
	initialize := func() error {
		calls++
		return nil
	}

	require.NoError(t, provider.ensureWorkspaceForConnection(conn, initialize))
	require.NoError(t, provider.ensureWorkspaceForConnection(conn, initialize))
	stats := provider.stats()
	require.Equal(t, 1, calls)
	require.Equal(t, uint64(1), stats.TraversalWorkspace.Initializations)
	require.Equal(t, uint64(1), stats.TraversalWorkspace.Reuses)
	require.True(t, stats.Connections[0].TraversalWorkspace.Ready)

	provider.advanceSchemaGeneration()
	stats = provider.stats()
	require.False(t, stats.Connections[0].TraversalWorkspace.Ready)
	require.NoError(t, provider.ensureWorkspaceForConnection(conn, initialize))
	require.Equal(t, 2, calls)

	provider.removeConnection(conn)
	require.Nil(t, provider.CacheForConnection(conn))
	require.NoError(t, provider.ensureWorkspaceForConnection(conn, initialize))
	require.Equal(t, 3, calls)
}

func TestConnectionWorkspaceReadinessDoesNotMarkFailuresReady(t *testing.T) {
	provider, _, conn := newTestCache(t, 2)
	expected := errors.New("workspace setup failed")
	require.ErrorIs(t, provider.ensureWorkspaceForConnection(conn, func() error { return expected }), expected)
	stats := provider.stats()
	require.Equal(t, uint64(1), stats.TraversalWorkspace.Failures)
	require.False(t, stats.Connections[0].TraversalWorkspace.Ready)

	require.NoError(t, provider.ensureWorkspaceForConnection(conn, func() error { return nil }))
	stats = provider.stats()
	require.Equal(t, uint64(1), stats.TraversalWorkspace.Initializations)
	require.True(t, stats.Connections[0].TraversalWorkspace.Ready)
}
