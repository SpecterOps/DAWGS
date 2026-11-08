package cache

import (
	"container/list"
	"sync/atomic"
)

// ShardedSieve distributes a fixed total capacity over independent Sieve
// shards. Lookups, insertions and statistics touch only the shard selected by
// the caller's hash function, so concurrent access spreads lock and counter
// traffic across shards instead of serializing on a single mutex and a single
// set of statistic words.
//
// The shard count is rounded up to a power of two and then capped at the
// largest power of two not exceeding the total capacity, so every shard holds
// at least one entry and the sum of shard capacities equals the requested
// total. Eviction (second chance) operates within each shard; entries do not
// migrate between shards.
type ShardedSieve[K comparable, V any] struct {
	shards  []*Sieve[K, V]
	shardFn func(key K) uint64
	mask    uint64
}

// NewShardedSieve creates a sharded second-chance cache with the supplied
// total capacity distributed across shards. The shardFn maps keys to hash
// values; only the low bits are used. A nil shardFn routes every key to one
// shard. A capacity of zero or less yields a single shard of capacity one,
// matching NewSieve.
func NewShardedSieve[K comparable, V any](totalCapacity int, shards int, shardFn func(key K) uint64) Cache[K, V] {
	if totalCapacity <= 0 {
		totalCapacity = 1
	}

	shards = nextPowerOfTwo(shards)
	if maxShards := previousPowerOfTwo(totalCapacity); shards > maxShards {
		shards = maxShards
	}

	if shardFn == nil {
		shardFn = func(key K) uint64 { return 0 }
	}

	sharded := &ShardedSieve[K, V]{
		shards:  make([]*Sieve[K, V], shards),
		shardFn: shardFn,
		mask:    uint64(shards - 1),
	}

	// Distribute the total capacity across shards without inflating it: the
	// first remainder shards hold one extra entry each.
	perShard := totalCapacity / shards
	remainder := totalCapacity % shards

	for idx := range sharded.shards {
		capacity := perShard
		if idx < remainder {
			capacity++
		}

		sharded.shards[idx] = &Sieve[K, V]{
			store: make(map[K]*entry[K, V], capacity),
			queue: list.New(),
			stats: NewStats(capacity),
		}
	}

	return sharded
}

func nextPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}

	power := 1
	for power < value {
		power <<= 1
	}

	return power
}

// previousPowerOfTwo returns the largest power of two not exceeding value.
func previousPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}

	power := 1
	for power*2 <= value {
		power <<= 1
	}

	return power
}

func (s *ShardedSieve[K, V]) shard(key K) *Sieve[K, V] {
	return s.shards[s.shardFn(key)&s.mask]
}

// Put adds or updates the value associated with key in the key's shard.
func (s *ShardedSieve[K, V]) Put(key K, value V) {
	s.shard(key).Put(key, value)
}

// Get retrieves the value associated with key from the key's shard.
func (s *ShardedSieve[K, V]) Get(key K) (V, bool) {
	return s.shard(key).Get(key)
}

// Delete removes the entry identified by key from the key's shard.
func (s *ShardedSieve[K, V]) Delete(key K) {
	s.shard(key).Delete(key)
}

// Stats returns aggregated statistics across all shards. The returned value
// snapshots the shard counters at the moment of the call.
func (s *ShardedSieve[K, V]) Stats() Stats {
	combined := Stats{
		hits:     &atomic.Int64{},
		misses:   &atomic.Int64{},
		size:     &atomic.Int64{},
		Capacity: 0,
	}

	for _, shard := range s.shards {
		shardStats := shard.Stats()

		combined.hits.Add(shardStats.Hits())
		combined.misses.Add(shardStats.Misses())
		combined.size.Add(shardStats.Size())
		combined.Capacity += shardStats.Capacity
	}

	return combined
}
