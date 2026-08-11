package pg

import (
	"container/list"
	"strings"
	"sync"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
)

const (
	// defaultCypherParseCacheEntries is the maximum number of parsed ASTs
	// retained when no cache capacity is configured.
	defaultCypherParseCacheEntries = 256

	// maxCachedCypherQueryBytes excludes oversized query strings from the parse
	// cache while still allowing them to be parsed.
	maxCachedCypherQueryBytes = 64 * 1024
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

// cypherParseCache retains immutable parser output. Translation is safe to run
// concurrently against a cached query because the optimizer copies the Cypher
// AST before applying rules or lowering it.
type cypherParseCache struct {
	// lock protects cache entries, pending calls, closure state, and counters.
	lock sync.Mutex

	// capacity is the maximum number of completed parses retained in entries.
	capacity int

	// entries indexes completed parses by normalized query text.
	entries map[string]*list.Element

	// lru orders completed entries from most to least recently used.
	lru *list.List

	// pending coalesces concurrent misses for the same normalized query.
	pending map[string]*cypherParseCall

	// closed prevents completed or future parses from being retained.
	closed bool

	// stats accumulates cache activity for this cache instance.
	stats ParseCacheStats
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

	// Evictions counts least-recently-used entries removed at capacity.
	Evictions uint64 `json:"evictions"`

	// CoalescedMisses counts callers that waited for an existing parse of the same query.
	CoalescedMisses uint64 `json:"coalesced_misses"`

	// Entries is the number of completed parses retained when the snapshot was taken.
	Entries int `json:"entries"`

	// Pending is the number of in-flight parses when the snapshot was taken.
	Pending int `json:"pending"`
}

// newCypherParseCache initializes an empty LRU parse cache with the requested capacity.
func newCypherParseCache(capacity int) *cypherParseCache {
	return &cypherParseCache{
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
		lru:      list.New(),
		pending:  map[string]*cypherParseCall{},
	}
}

// Parse returns an immutable Cypher AST and reports whether it came from a completed or coalesced cache hit.
func (s *cypherParseCache) Parse(input string) (*cypher.RegularQuery, bool, error) {
	query := strings.TrimSpace(input)
	// Bound the caller-owned input rather than only the trimmed view. A short
	// query padded with a very large amount of whitespace must not retain that
	// backing allocation through an LRU key.
	if s == nil {
		parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
		if err != nil {
			return nil, false, err
		}

		return parsed, false, nil
	}

	s.lock.Lock()
	if s.closed || s.capacity <= 0 || len(input) > maxCachedCypherQueryBytes {
		s.stats.Bypasses++
		s.lock.Unlock()
		parsed, err := frontend.ParseCypher(frontend.NewContext(), query)
		if err != nil {
			return nil, false, err
		}

		return parsed, false, nil
	}
	if element, found := s.entries[query]; found {
		s.stats.Hits++
		s.lru.MoveToFront(element)
		parsed := element.Value.(cypherParseCacheEntry).parsed
		s.lock.Unlock()
		return parsed, true, nil
	}
	if call, found := s.pending[query]; found {
		s.stats.CoalescedMisses++
		s.lock.Unlock()
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
	s.stats.Misses++
	call := &cypherParseCall{done: make(chan struct{})}
	s.pending[query] = call
	s.lock.Unlock()

	parsed, err := frontend.ParseCypher(frontend.NewContext(), query)

	s.lock.Lock()
	call.parsed = parsed
	call.err = err
	if err == nil && !s.closed {
		element := s.lru.PushFront(cypherParseCacheEntry{
			query:  query,
			parsed: parsed,
		})
		s.entries[query] = element
		if s.lru.Len() > s.capacity {
			evicted := s.lru.Back()
			s.lru.Remove(evicted)
			delete(s.entries, evicted.Value.(cypherParseCacheEntry).query)
			s.stats.Evictions++
		}
	}
	delete(s.pending, query)
	close(call.done)
	s.lock.Unlock()

	if err != nil {
		return nil, false, err
	}

	return parsed, false, nil
}

// Stats returns a consistent snapshot of counters and current cache occupancy.
func (s *cypherParseCache) Stats() ParseCacheStats {
	if s == nil {
		return ParseCacheStats{}
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	stats := s.stats
	stats.Entries = len(s.entries)
	stats.Pending = len(s.pending)
	return stats
}

// Close prevents future retention and releases every cached query/AST
// reference. In-flight parses wake their waiters normally but do not repopulate
// the cache after closure.
func (s *cypherParseCache) Close() {
	if s == nil {
		return
	}
	s.lock.Lock()
	s.closed = true
	s.entries = nil
	s.lru.Init()
	s.lock.Unlock()
}
