package v2

import (
	"strings"
	"sync"

	dawgscache "github.com/specterops/dawgs/cache"
)

// sharedTemplateCache is a bounded V2-driver-wide L2 containing only
// immutable shortest-path SQL templates and source metadata. L1 remains the
// connection-local cache; the shared tier removes duplicate compilation when
// a pool expands or rotates connections.
type sharedTemplateCache struct {
	lock     sync.Mutex
	capacity int
	sieve    dawgscache.Cache[translationKey, translationEntry]
	stats    SharedTemplateStats
}

func newSharedTemplateCache(capacity int) *sharedTemplateCache {
	cache := &sharedTemplateCache{capacity: capacity, stats: SharedTemplateStats{Capacity: capacity}}
	if capacity > 0 {
		cache.sieve = dawgscache.NewSieve[translationKey, translationEntry](capacity)
	}
	return cache
}

func (s *sharedTemplateCache) get(key translationKey) (translationEntry, bool) {
	if s == nil || s.capacity == 0 {
		return translationEntry{}, false
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	entry, found := s.sieve.Get(key)
	if found {
		s.stats.Hits++
	} else {
		s.stats.Misses++
	}
	return entry, found
}

func (s *sharedTemplateCache) put(key translationKey, entry translationEntry) {
	if s == nil || s.capacity == 0 {
		return
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if _, exists := s.sieve.Get(key); exists {
		return
	}
	if s.sieve.Stats().Size() >= int64(s.capacity) {
		s.stats.Evictions++
	}
	key.query = strings.Clone(key.query)
	s.sieve.Put(key, entry)
	s.stats.Insertions++
}

func (s *sharedTemplateCache) snapshot() SharedTemplateStats {
	if s == nil {
		return SharedTemplateStats{}
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	stats := s.stats
	if s.sieve != nil {
		stats.Entries = int(s.sieve.Stats().Size())
	}
	return stats
}
