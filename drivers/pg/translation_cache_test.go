package pg

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/pgsql/translate"
	"github.com/specterops/dawgs/drivers/pg/pgutil"
	"github.com/stretchr/testify/require"
)

// TestCypherTranslationCacheReturnsZeroValuesOnBuildError verifies failed builds do not leak partial SQL or parameter maps.
func TestCypherTranslationCacheReturnsZeroValuesOnBuildError(t *testing.T) {
	cache := newCypherTranslationCache(2)
	expectedErr := errors.New("translation failed")

	sql, parameters, err := cache.Translate("RETURN 1", 1, nil, func() (translate.Result, string, error) {
		return translate.Result{
			Parameters: map[string]any{"partial": true},
		}, "partial sql", expectedErr
	})

	require.ErrorIs(t, err, expectedErr)
	require.Empty(t, sql)
	require.Nil(t, parameters)
	require.Zero(t, cache.Stats().Entries)
}

// TestCypherTranslationCacheRebindsTranslatedListParameters verifies a cached list translation uses values from the current caller.
func TestCypherTranslationCacheRebindsTranslatedListParameters(t *testing.T) {
	cache := newCypherTranslationCache(2)
	const cypherQuery = `MATCH (n) WHERE n.objectid IN $object_ids RETURN n`
	mapper := pgutil.NewInMemoryKindMapper()
	builds := 0

	translateWith := func(parameters map[string]any) (string, map[string]any, error) {
		parsed, err := frontend.ParseCypher(frontend.NewContext(), cypherQuery)
		require.NoError(t, err)
		return cache.Translate(cypherQuery, translate.DefaultGraphID, parameters, func() (translate.Result, string, error) {
			builds++
			result, err := translate.Translate(context.Background(), parsed, mapper, parameters, translate.DefaultGraphID)
			if err != nil {
				return translate.Result{}, "", err
			}
			sql, err := translate.Translated(result)
			if err != nil {
				return translate.Result{}, "", err
			}

			return result, sql, nil
		})
	}

	_, first, err := translateWith(map[string]any{"object_ids": []any{}})
	require.NoError(t, err)
	_, second, err := translateWith(map[string]any{"object_ids": []any{"selected"}})
	require.NoError(t, err)
	_, third, err := translateWith(map[string]any{"object_ids": []any{"other"}})
	require.NoError(t, err)
	require.Equal(t, 2, builds)
	require.NotEqual(t, first, second)
	require.NotEqual(t, second, third)
}

// TestCypherTranslationCacheRebindsNamedParameters verifies generated SQL parameter names map back to fresh named values.
func TestCypherTranslationCacheRebindsNamedParameters(t *testing.T) {
	cache := newCypherTranslationCache(2)
	var builds int
	build := func(value int64) func() (translate.Result, string, error) {
		return func() (translate.Result, string, error) {
			builds++
			return translate.Result{
				Parameters:       map[string]any{"i0": value},
				ParameterSources: map[string]string{"i0": "id"},
			}, "select @i0", nil
		}
	}

	sql, parameters, err := cache.Translate(" MATCH (n) WHERE id(n) = $id RETURN n ", 1, map[string]any{"id": int64(1)}, build(1))
	require.NoError(t, err)
	require.Equal(t, "select @i0", sql)
	require.Equal(t, int64(1), parameters["i0"])

	sql, parameters, err = cache.Translate("MATCH (n) WHERE id(n) = $id RETURN n", 1, map[string]any{"id": int64(2)}, build(999))
	require.NoError(t, err)
	require.Equal(t, "select @i0", sql)
	require.Equal(t, int64(2), parameters["i0"])
	require.Equal(t, 1, builds)
	require.Equal(t, TranslationCacheStats{
		Hits:    1,
		Misses:  1,
		Entries: 1,
	}, cache.Stats())
}

// TestCypherTranslationCacheSeparatesGraphAndParameterTypes verifies graph identity and negotiated types partition cache entries.
func TestCypherTranslationCacheSeparatesGraphAndParameterTypes(t *testing.T) {
	cache := newCypherTranslationCache(4)
	var builds int
	build := func() (translate.Result, string, error) {
		builds++
		return translate.Result{
			Parameters:       map[string]any{},
			ParameterSources: map[string]string{},
		}, "select 1", nil
	}

	_, _, err := cache.Translate("RETURN $value", 1, map[string]any{"value": int64(1)}, build)
	require.NoError(t, err)
	_, _, err = cache.Translate("RETURN $value", 2, map[string]any{"value": int64(1)}, build)
	require.NoError(t, err)
	_, _, err = cache.Translate("RETURN $value", 1, map[string]any{"value": "1"}, build)
	require.NoError(t, err)
	require.Equal(t, 3, builds)
}

// TestCypherTranslationCacheSeparatesProductionPolicies verifies disabling a
// canary cannot reuse SQL compiled under an earlier selector generation.
func TestCypherTranslationCacheSeparatesProductionPolicies(t *testing.T) {
	cache := newCypherTranslationCache(4)
	builds := 0
	build := func(sql string) func() (translate.Result, string, error) {
		return func() (translate.Result, string, error) {
			builds++
			return translate.Result{Parameters: map[string]any{}, ParameterSources: map[string]string{}}, sql, nil
		}
	}

	first, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "candidate-g1", build("candidate"))
	require.NoError(t, err)
	incumbent, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "production-incumbent-v1", build("incumbent"))
	require.NoError(t, err)
	again, _, err := cache.TranslateWithPolicy("RETURN 1", 1, nil, "candidate-g1", build("wrong"))
	require.NoError(t, err)

	require.Equal(t, "candidate", first)
	require.Equal(t, "incumbent", incumbent)
	require.Equal(t, "candidate", again)
	require.Equal(t, 2, builds)
}

// TestTranslationParameterTypeKeyIsDelimiterSafe verifies length-prefixed name and type components cannot collide.
func TestTranslationParameterTypeKeyIsDelimiterSafe(t *testing.T) {
	first := translationParameterTypeKey(map[string]any{
		"a": int64(1),
		"b": "value",
	})
	second := translationParameterTypeKey(map[string]any{
		"a=int8;b": "value",
	})
	require.NotEqual(t, first, second)
}

// TestCypherTranslationCacheRejectsMissingParameterSources verifies incomplete source metadata bypasses retention.
func TestCypherTranslationCacheRejectsMissingParameterSources(t *testing.T) {
	cache := newCypherTranslationCache(2)
	var builds int
	build := func() (translate.Result, string, error) {
		builds++
		return translate.Result{
			Parameters:       map[string]any{"i0": int64(1)},
			ParameterSources: map[string]string{"i0": "required"},
		}, "select @i0", nil
	}

	for range 2 {
		_, _, err := cache.Translate("RETURN $required", 1, map[string]any{"other": int64(1)}, build)
		require.NoError(t, err)
	}

	require.Equal(t, 2, builds)
	require.Zero(t, cache.Stats().Entries)
	require.Equal(t, uint64(2), cache.Stats().Bypasses)
}

// TestCachedTranslationBindingFailsClosedOnMissingSource verifies a cache hit errors rather than binding an absent caller value.
func TestCachedTranslationBindingFailsClosedOnMissingSource(t *testing.T) {
	value := cypherTranslationCacheValue{
		parameterSources: map[string]string{"i0": "required"},
	}
	_, err := value.bind(map[string]any{"other": int64(1)})
	require.ErrorContains(t, err, "missing parameter source")
}

// TestCypherTranslationCacheBypassesGeneratedParameters verifies translations with non-source parameters are rebuilt for each caller.
func TestCypherTranslationCacheBypassesGeneratedParameters(t *testing.T) {
	cache := newCypherTranslationCache(2)
	var builds int
	build := func() (translate.Result, string, error) {
		builds++
		return translate.Result{
			Parameters: map[string]any{"pi0": "insert into traversal_pair_filter ..."},
		}, "select @pi0", nil
	}

	for range 2 {
		_, _, err := cache.Translate("MATCH p = shortestPath((a)-[*]->(b)) RETURN p", 1, nil, build)
		require.NoError(t, err)
	}
	require.Equal(t, 2, builds)
	require.Equal(t, uint64(2), cache.Stats().Bypasses)
	require.Zero(t, cache.Stats().Entries)
}

// TestCypherTranslationCacheCoalescesConcurrentMisses verifies equivalent concurrent requests share one cacheable build.
func TestCypherTranslationCacheCoalescesConcurrentMisses(t *testing.T) {
	cache := newCypherTranslationCache(2)
	const workers = 16
	var builds atomic.Int64
	start := make(chan struct{})
	release := make(chan struct{})
	build := func() (translate.Result, string, error) {
		if builds.Add(1) == 1 {
			close(start)
		}
		<-release
		return translate.Result{
			Parameters:       map[string]any{},
			ParameterSources: map[string]string{},
		}, "select 1", nil
	}

	var group sync.WaitGroup
	group.Add(workers)
	errs := make([]error, workers)
	for idx := 0; idx < workers; idx++ {
		go func(index int) {
			defer group.Done()
			_, _, errs[index] = cache.Translate("MATCH (n) RETURN n", 1, nil, build)
		}(idx)
	}
	<-start
	close(release)
	group.Wait()

	for _, err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int64(1), builds.Load())
	require.Equal(t, uint64(workers-1), cache.Stats().Hits+cache.Stats().CoalescedMisses)
}

// TestCypherTranslationCacheDoesNotShareUncacheableParametersWithWaiters verifies waiters rebuild results whose values cannot be rebound safely.
func TestCypherTranslationCacheDoesNotShareUncacheableParametersWithWaiters(t *testing.T) {
	cache := newCypherTranslationCache(2)
	start := make(chan struct{})
	release := make(chan struct{})
	var builds atomic.Int64
	build := func(value string, wait bool) func() (translate.Result, string, error) {
		return func() (translate.Result, string, error) {
			builds.Add(1)
			if wait {
				close(start)
				<-release
			}
			return translate.Result{
				Parameters: map[string]any{"pi0": value},
			}, "select @pi0", nil
		}
	}

	var (
		first, second       map[string]any
		firstErr, secondErr error
	)

	done := make(chan struct{})
	go func() {
		_, first, firstErr = cache.Translate("RETURN 1", 1, nil, build("first", true))
		close(done)
	}()
	<-start
	secondDone := make(chan struct{})
	go func() {
		_, second, secondErr = cache.Translate("RETURN 1", 1, nil, build("second", false))
		close(secondDone)
	}()
	close(release)
	<-done
	<-secondDone

	require.NoError(t, firstErr)
	require.NoError(t, secondErr)
	require.Equal(t, "first", first["pi0"])
	require.Equal(t, "second", second["pi0"])
	require.Equal(t, int64(2), builds.Load())
}
