package pg

import (
	"strings"
	"sync"
	"testing"

	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/stretchr/testify/require"
)

func TestCypherParseCacheReusesTrimmedQuery(t *testing.T) {
	cache := newCypherParseCache(2)

	first, hit, err := cache.Parse("  MATCH (n) RETURN n  ")
	require.NoError(t, err)
	require.False(t, hit)

	second, hit, err := cache.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.True(t, hit)
	require.Same(t, first, second)
}

func TestCypherParseCacheEvictsLeastRecentlyUsedQuery(t *testing.T) {
	cache := newCypherParseCache(2)

	_, _, err := cache.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	second, _, err := cache.Parse("MATCH (n) RETURN id(n)")
	require.NoError(t, err)
	_, hit, err := cache.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.True(t, hit)
	_, _, err = cache.Parse("MATCH (n) RETURN count(n)")
	require.NoError(t, err)

	reparsed, hit, err := cache.Parse("MATCH (n) RETURN id(n)")
	require.NoError(t, err)
	require.False(t, hit)
	require.NotSame(t, second, reparsed)
}

func TestCypherParseCacheDoesNotRetainErrorsOrOversizedQueries(t *testing.T) {
	cache := newCypherParseCache(2)

	_, hit, err := cache.Parse("MATCH (")
	require.Error(t, err)
	require.False(t, hit)
	_, hit, err = cache.Parse("MATCH (")
	require.Error(t, err)
	require.False(t, hit)
	require.Empty(t, cache.entries)

	oversized := "MATCH (n) RETURN n // " + strings.Repeat("x", maxCachedCypherQueryBytes)
	_, hit, err = cache.Parse(oversized)
	require.NoError(t, err)
	require.False(t, hit)
	require.Empty(t, cache.entries)

	padded := strings.Repeat(" ", maxCachedCypherQueryBytes) + "MATCH (n) RETURN n"
	_, hit, err = cache.Parse(padded)
	require.NoError(t, err)
	require.False(t, hit)
	require.Empty(t, cache.entries)
	require.Equal(t, uint64(2), cache.Stats().Bypasses)
}

func TestCypherParseCacheCoalescesConcurrentMissesAndSupportsConcurrentOptimization(t *testing.T) {
	cache := newCypherParseCache(2)
	const workers = 32

	queries := make([]any, workers)
	errors := make([]error, workers)
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	for idx := 0; idx < workers; idx++ {
		go func(index int) {
			defer waitGroup.Done()
			query, _, err := cache.Parse("MATCH (n) WHERE id(n) = $id RETURN n")
			if err == nil {
				_, err = optimize.Optimize(query)
			}
			errors[index] = err
			queries[index] = query
		}(idx)
	}
	waitGroup.Wait()

	for _, err := range errors {
		require.NoError(t, err)
	}
	for idx := 1; idx < len(queries); idx++ {
		require.Same(t, queries[0], queries[idx])
	}
	require.Len(t, cache.entries, 1)
	require.Equal(t, uint64(workers-1), cache.Stats().Hits+cache.Stats().CoalescedMisses)
}

func TestCypherParseCacheSupportsConcurrentDifferentKeys(t *testing.T) {
	cache := newCypherParseCache(64)
	const workers = 32
	var waitGroup sync.WaitGroup
	errors := make([]error, workers)
	waitGroup.Add(workers)
	for idx := 0; idx < workers; idx++ {
		go func(index int) {
			defer waitGroup.Done()
			_, _, errors[index] = cache.Parse("MATCH (n) RETURN n // key " + strings.Repeat("x", index))
		}(idx)
	}
	waitGroup.Wait()
	for _, err := range errors {
		require.NoError(t, err)
	}
	require.Equal(t, uint64(workers), cache.Stats().Misses)
	require.Equal(t, workers, cache.Stats().Entries)
}

func TestCypherParseCacheStatsAndCloseReleaseEntries(t *testing.T) {
	cache := newCypherParseCache(1)
	_, _, err := cache.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	_, hit, err := cache.Parse("MATCH (n) RETURN n")
	require.NoError(t, err)
	require.True(t, hit)
	_, _, err = cache.Parse("MATCH (n) RETURN id(n)")
	require.NoError(t, err)
	require.Equal(t, ParseCacheStats{Hits: 1, Misses: 2, Evictions: 1, Entries: 1}, cache.Stats())

	cache.Close()
	require.Zero(t, cache.Stats().Entries)
	require.Nil(t, cache.entries)
	_, hit, err = cache.Parse("MATCH (n) RETURN id(n)")
	require.NoError(t, err)
	require.False(t, hit)
	require.Equal(t, uint64(1), cache.Stats().Bypasses)
}

func BenchmarkCypherParseCache(b *testing.B) {
	const query = "MATCH (n) WHERE id(n) = $id RETURN n"
	b.Run("uncached", func(b *testing.B) {
		for idx := 0; idx < b.N; idx++ {
			cache := newCypherParseCache(0)
			_, _, err := cache.Parse(query)
			require.NoError(b, err)
		}
	})
	b.Run("cached", func(b *testing.B) {
		cache := newCypherParseCache(1)
		_, _, err := cache.Parse(query)
		require.NoError(b, err)
		b.ResetTimer()
		for idx := 0; idx < b.N; idx++ {
			_, hit, err := cache.Parse(query)
			require.NoError(b, err)
			require.True(b, hit)
		}
	})
}
