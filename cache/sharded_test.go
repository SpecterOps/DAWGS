package cache_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/specterops/dawgs/cache"
	"github.com/stretchr/testify/require"
)

func TestShardedSieve_PutGet(t *testing.T) {
	sharded := cache.NewShardedSieve[int, string](10, 4, func(key int) uint64 { return uint64(key) })

	sharded.Put(1, "one")
	fetched, exists := sharded.Get(1)

	require.True(t, exists)
	require.Equal(t, "one", fetched)
}

func TestShardedSieve_MissingKeyCountsMiss(t *testing.T) {
	sharded := cache.NewShardedSieve[int, string](10, 4, func(key int) uint64 { return uint64(key) })

	_, exists := sharded.Get(99)
	require.False(t, exists)

	_, exists = sharded.Get(99)
	require.False(t, exists)

	require.Equal(t, int64(2), sharded.Stats().Misses())
	require.Equal(t, int64(0), sharded.Stats().Hits())
}

func TestShardedSieve_CapacityIsNeverInflated(t *testing.T) {
	// Shard counts beyond the total capacity collapse so each shard keeps at
	// least one entry without exceeding the requested capacity. Capacities at
	// or below zero floor at one, matching NewSieve.
	for _, tc := range []struct {
		capacity  int
		effective int
		shards    int
	}{
		{capacity: 0, effective: 1, shards: 16},
		{capacity: -3, effective: 1, shards: 8},
		{capacity: 1, effective: 1, shards: 16},
		{capacity: 2, effective: 2, shards: 16},
		{capacity: 3, effective: 3, shards: 2},
		{capacity: 3, effective: 3, shards: 16},
		{capacity: 10, effective: 10, shards: 4},
		{capacity: 10, effective: 10, shards: 1},
		{capacity: 17, effective: 17, shards: 16},
	} {
		t.Run(fmt.Sprintf("capacity_%d_shards_%d", tc.capacity, tc.shards), func(t *testing.T) {
			sharded := cache.NewShardedSieve[uint64, uint64](tc.capacity, tc.shards, func(key uint64) uint64 { return key })
			require.Equal(t, tc.effective, sharded.Stats().Capacity)

			// Fill far beyond capacity: the cache must retain at most its
			// capacity while still answering for newly inserted keys.
			for key := uint64(0); key < 128; key++ {
				sharded.Put(key, key)
				require.LessOrEqual(t, sharded.Stats().Size(), int64(sharded.Stats().Capacity))
			}

			fresh, exists := sharded.Get(127)
			require.True(t, exists)
			require.Equal(t, uint64(127), fresh)
		})
	}
}

func TestShardedSieve_UpdateExistingKey(t *testing.T) {
	sharded := cache.NewShardedSieve[string, int](4, 4, func(key string) uint64 { return uint64(len(key)) })

	sharded.Put("k", 10)
	sharded.Put("k", 20)

	fetched, exists := sharded.Get("k")
	require.True(t, exists)
	require.Equal(t, 20, fetched)
	require.Equal(t, int64(1), sharded.Stats().Size())
}

func TestShardedSieve_Delete(t *testing.T) {
	sharded := cache.NewShardedSieve[uint64, uint64](8, 4, func(key uint64) uint64 { return key })

	sharded.Put(1, 1)
	sharded.Delete(1)

	_, exists := sharded.Get(1)
	require.False(t, exists)
	require.Equal(t, int64(0), sharded.Stats().Size())
}

func TestShardedSieve_NilShardFunction(t *testing.T) {
	sharded := cache.NewShardedSieve[uint64, uint64](8, 4, nil)

	sharded.Put(1, 1)
	fetched, exists := sharded.Get(1)

	require.True(t, exists)
	require.Equal(t, uint64(1), fetched)
}

func TestShardedSieve_ConcurrentAccess(t *testing.T) {
	sharded := cache.NewShardedSieve[uint64, uint64](64, 8, func(key uint64) uint64 { return key })

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()

			for key := uint64(0); key < 512; key++ {
				shardKey := uint64(worker)*512 + key
				sharded.Put(shardKey, shardKey)

				if fetched, exists := sharded.Get(shardKey); exists {
					if fetched != shardKey {
						return
					}
				}
			}
		}(worker)
	}
	wg.Wait()

	require.LessOrEqual(t, sharded.Stats().Size(), int64(sharded.Stats().Capacity))
	require.Greater(t, sharded.Stats().Hits(), int64(0))
}
