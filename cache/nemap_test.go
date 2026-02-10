package cache_test

import (
	"testing"

	"github.com/specterops/dawgs/cache"
	"github.com/stretchr/testify/require"
)

func TestNonExpiringMapCache_PutGet(t *testing.T) {
	type testValue struct {
		id   int
		data string
	}

	var (
		nemap    = cache.NewNonExpiringMapCache[int, testValue](10)
		expected = testValue{id: 1, data: "one"}
	)

	nemap.Put(1, expected)
	fetched, exists := nemap.Get(1)

	require.True(t, exists)
	require.Equal(t, expected, fetched)
}

func TestNonExpiringMapCache_UpdateExistingKey(t *testing.T) {
	nemap := cache.NewNonExpiringMapCache[string, int](5)

	nemap.Put("k", 10)
	nemap.Put("k", 20)

	fetched, exists := nemap.Get("k")

	require.True(t, exists)
	require.Equal(t, 20, fetched)
}
