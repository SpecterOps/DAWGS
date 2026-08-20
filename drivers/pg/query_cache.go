package pg

import (
	"container/list"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
)

const (
	// defaultCypherParseCacheEntries is the maximum number of parsed ASTs
	// retained when no cache capacity is configured.
	defaultCypherParseCacheEntries = 256

	// targetCypherParseCacheEntriesPerShard keeps unrelated query shapes from
	// contending on one LRU lock while retaining a bounded shared AST cache.
	targetCypherParseCacheEntriesPerShard = 16

	// MaxCachedCypherQueryBytes excludes oversized query strings from reusable
	// parse and translation caches while still allowing them to execute.
	MaxCachedCypherQueryBytes = 64 * 1024
)

// cypherParseCacheEntry pairs an immutable parsed AST with the normalized query text used as its LRU key.
type cypherParseCacheEntry struct {
	// query is the normalized, cloned cache key.
	query string

	// parsed is the immutable parser result shared by cache hits.
	parsed *cypher.RegularQuery
}

// cypherParseCall publishes one in-flight parse result to callers waiting on the same query.
type cypherParseCall struct {
	// done closes after parsed and err have been published.
	done chan struct{}

	// parsed is the AST produced by the coalesced parse.
	parsed *cypher.RegularQuery

	// err is the parser failure, if any, shared with waiters.
	err error
}

// cypherParseCacheShard owns one bounded LRU and its single-flight parse
// calls. A query hashes to exactly one shard, so identical query text still
// shares one immutable AST while unrelated text can proceed independently.
type cypherParseCacheShard struct {
	lock sync.Mutex

	capacity int
	entries  map[string]*list.Element
	lru      *list.List
	pending  map[string]*cypherParseCall
	stats    ParseCacheStats
}

// cypherParseCache retains immutable parser output in independently locked
// shards. Translation is safe to run concurrently against a cached query
// because the optimizer copies the Cypher AST before applying rules or
// lowering it.
type cypherParseCache struct {
	// capacity is the exact aggregate maximum number of completed parses.
	capacity int

	// shards partition query text and each contain a local LRU.
	shards []cypherParseCacheShard

	// closed prevents completed or future parses from being retained. It is
	// atomic so hot cache hits do not need a driver-wide lifecycle lock.
	closed atomic.Bool
}

// ParseCacheStats contains aggregate, query-text-free diagnostics. It is a
// snapshot; counters are scoped to one driver instance and reset only when the
// driver is reconstructed.
type ParseCacheStats struct {
	// Hits counts lookups served from completed cache entries.
	Hits uint64 `json:"hits"`

	// Misses counts queries parsed by the caller that established a pending entry.
	Misses uint64 `json:"misses"`

	// Bypasses counts queries parsed without retention because caching was unavailable or disallowed.
	Bypasses uint64 `json:"bypasses"`

	// Evictions counts least-recently-used translations removed at capacity.
	Evictions uint64 `json:"evictions"`

	// CoalescedMisses counts callers that waited for an existing parse of the same query.
	CoalescedMisses uint64 `json:"coalesced_misses"`

	// Entries is the number of completed parses retained when the snapshot was taken.
	Entries int `json:"entries"`

	// Pending is the number of in-flight parses when the snapshot was taken.
	Pending int `json:"pending"`

	// Shards is the number of independently locked cache partitions.
	Shards int `json:"shards"`
}

// newCypherParseCache initializes a bounded shared AST cache with independently locked shards.
func newCypherParseCache(capacity int) *cypherParseCache {
	shardCount := parseCacheShardCount(capacity)
	cache := &cypherParseCache{
		capacity: capacity,
		shards:   make([]cypherParseCacheShard, shardCount),
	}
	for index := range cache.shards {
		shardCapacity := capacity / shardCount
		if index < capacity%shardCount {
			shardCapacity++
		}
		cache.shards[index] = cypherParseCacheShard{
			capacity: shardCapacity,
			entries:  make(map[string]*list.Element, shardCapacity),
			lru:      list.New(),
			pending:  map[string]*cypherParseCall{},
		}
	}
	return cache
}

func parseCacheShardCount(capacity int) int {
	if capacity <= 0 {
		return 1
	}
	count := capacity / targetCypherParseCacheEntriesPerShard
	if count < 1 {
		return 1
	}
	if count > targetCypherParseCacheEntriesPerShard {
		return targetCypherParseCacheEntriesPerShard
	}
	return count
}

func (s *cypherParseCache) shardForQuery(query string) *cypherParseCacheShard {
	return &s.shards[xxhash.Sum64String(query)%uint64(len(s.shards))]
}

// Parse returns an immutable Cypher AST and reports whether it came from a completed or coalesced cache hit.
func (s *cypherParseCache) Parse(input string) (*cypher.RegularQuery, bool, error) {
	query := strings.TrimSpace(input)
	// Bound the caller-owned input rather than only the trimmed view. A short
	// query padded with a very large amount of whitespace must not retain that
	// backing allocation through an LRU key.
	if s == nil {
		parsed, err := parseCypher(query)
		return parsed, false, err
	}

	shard := s.shardForQuery(query)
	shard.lock.Lock()
	if s.closed.Load() || s.capacity <= 0 || len(input) > MaxCachedCypherQueryBytes {
		shard.stats.Bypasses++
		shard.lock.Unlock()
		parsed, err := parseCypher(query)
		return parsed, false, err
	}
	if element, found := shard.entries[query]; found {
		shard.stats.Hits++
		shard.lru.MoveToFront(element)
		parsed := element.Value.(cypherParseCacheEntry).parsed
		shard.lock.Unlock()
		return parsed, true, nil
	}
	if call, found := shard.pending[query]; found {
		shard.stats.CoalescedMisses++
		shard.lock.Unlock()
		<-call.done
		if call.err != nil {
			return nil, false, call.err
		}
		return call.parsed, true, nil
	}

	// Lookups do not retain the caller's string. Clone only a true miss before
	// using it as a pending/cache key so the zero-allocation hit path remains
	// intact.
	query = strings.Clone(query)
	shard.stats.Misses++
	call := &cypherParseCall{done: make(chan struct{})}
	shard.pending[query] = call
	shard.lock.Unlock()

	parsed, err := parseCypher(query)

	shard.lock.Lock()
	call.parsed = parsed
	call.err = err
	if err == nil && !s.closed.Load() {
		element := shard.lru.PushFront(cypherParseCacheEntry{
			query:  query,
			parsed: parsed,
		})
		shard.entries[query] = element
		if shard.lru.Len() > shard.capacity {
			evicted := shard.lru.Back()
			shard.lru.Remove(evicted)
			delete(shard.entries, evicted.Value.(cypherParseCacheEntry).query)
			shard.stats.Evictions++
		}
	}
	delete(shard.pending, query)
	close(call.done)
	shard.lock.Unlock()

	if err != nil {
		return nil, false, err
	}
	return parsed, false, nil
}

func parseCypher(query string) (*cypher.RegularQuery, error) {
	return frontend.ParseCypher(frontend.NewContext(), query)
}

// Stats returns a consistent aggregate snapshot of all shard counters and occupancy.
func (s *cypherParseCache) Stats() ParseCacheStats {
	if s == nil {
		return ParseCacheStats{}
	}
	stats := ParseCacheStats{Shards: len(s.shards)}
	for index := range s.shards {
		shard := &s.shards[index]
		shard.lock.Lock()
		stats.Hits += shard.stats.Hits
		stats.Misses += shard.stats.Misses
		stats.Bypasses += shard.stats.Bypasses
		stats.Evictions += shard.stats.Evictions
		stats.CoalescedMisses += shard.stats.CoalescedMisses
		stats.Entries += len(shard.entries)
		stats.Pending += len(shard.pending)
		shard.lock.Unlock()
	}
	return stats
}

// Close prevents future retention and releases every cached query/AST
// reference. In-flight parses wake their waiters normally but do not repopulate
// the cache after closure.
func (s *cypherParseCache) Close() {
	if s == nil || s.closed.Swap(true) {
		return
	}
	for index := range s.shards {
		shard := &s.shards[index]
		shard.lock.Lock()
		shard.entries = nil
		shard.lru.Init()
		shard.lock.Unlock()
	}
}
