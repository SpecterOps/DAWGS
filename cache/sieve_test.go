package cache_test

import (
	"testing"

	"github.com/specterops/dawgs/cache"
	"github.com/stretchr/testify/require"
)

func TestSieve_PutGet(t *testing.T) {
	type testValue struct {
		id   int
		data string
	}

	var (
		sieve    = cache.NewSieve[int, testValue](10)
		expected = testValue{id: 1, data: "one"}
	)

	sieve.Put(1, expected)
	fetched, exists := sieve.Get(1)

	require.True(t, exists)
	require.Equal(t, expected, fetched)
}

func TestSieve_UpdateExistingKey(t *testing.T) {
	sieve := cache.NewSieve[string, int](5)

	sieve.Put("k", 10)
	sieve.Put("k", 20)

	fetched, exists := sieve.Get("k")

	require.True(t, exists)
	require.Equal(t, 20, fetched)
}

func TestSieve_EvictWhenFull(t *testing.T) {
	var (
		sieve      = cache.NewSieve[int, string](3)
		assertions = map[int]string{
			2: "two",
			3: "three",
			4: "four",
		}
	)

	sieve.Put(1, "one")
	sieve.Put(2, "two")
	sieve.Put(3, "three")
	sieve.Put(4, "four")

	_, exists := sieve.Get(1)
	require.Falsef(t, exists, "first element must evict")

	for key, expected := range assertions {
		fetched, exists := sieve.Get(key)

		require.True(t, exists)
		require.Equal(t, expected, fetched)
	}
}

func TestSieve_EvictRespectsVisitedFlag(t *testing.T) {
	var (
		sieve      = cache.NewSieve[int, string](3)
		assertions = map[int]string{
			1: "one",
			3: "three",
			4: "four",
		}
	)

	// 1 will become the oldest (tail)
	sieve.Put(1, "one")
	sieve.Put(2, "two")

	// 3 is the newest (head)
	sieve.Put(3, "three")

	// Access key 1 – this marks it as visited.
	_, exists := sieve.Get(1)
	require.True(t, exists)

	// Adding a new entry forces eviction.
	sieve.Put(4, "four")

	// Key 2 must have been evicted.
	_, exists = sieve.Get(2)
	require.False(t, exists, "key two must be evicted")

	for key, expected := range assertions {
		fetched, exists := sieve.Get(key)

		require.True(t, exists)
		require.Equal(t, expected, fetched)
	}
}
