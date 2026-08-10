package pg

import (
	"container/list"
	"strings"
	"sync"

	"github.com/specterops/dawgs/cypher/frontend"
	"github.com/specterops/dawgs/cypher/models/cypher"
)

const (
	defaultCypherParseCacheEntries = 256
	maxCachedCypherQueryBytes      = 64 * 1024
)

type cypherParseCacheEntry struct {
	query  string
	parsed *cypher.RegularQuery
}

type cypherParseCall struct {
	done   chan struct{}
	parsed *cypher.RegularQuery
	err    error
}

// cypherParseCache retains immutable parser output. Translation is safe to run
// concurrently against a cached query because the optimizer copies the Cypher
// AST before applying rules or lowering it.
type cypherParseCache struct {
	lock     sync.Mutex
	capacity int
	entries  map[string]*list.Element
	lru      *list.List
	pending  map[string]*cypherParseCall
	closed   bool
	stats    ParseCacheStats
}

// ParseCacheStats contains aggregate, query-text-free diagnostics. It is a
// snapshot; counters are scoped to one driver instance and reset only when the
// driver is reconstructed.
type ParseCacheStats struct {
	Hits            uint64 `json:"hits"`
	Misses          uint64 `json:"misses"`
	Bypasses        uint64 `json:"bypasses"`
	Evictions       uint64 `json:"evictions"`
	CoalescedMisses uint64 `json:"coalesced_misses"`
	Entries         int    `json:"entries"`
	Pending         int    `json:"pending"`
}

func newCypherParseCache(capacity int) *cypherParseCache {
	return &cypherParseCache{
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
		lru:      list.New(),
		pending:  map[string]*cypherParseCall{},
	}
}

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
