package cache

import (
	"sync"
)

// NonExpiringMapCache
type NonExpiringMapCache[K comparable, V any] struct {
	store  map[K]V
	stats  Stats
	rwLock sync.RWMutex
}

func NewNonExpiringMapCache[K comparable, V any](capacity int) Cache[K, V] {
	return &NonExpiringMapCache[K, V]{
		store: make(map[K]V, capacity),
		stats: NewStats(capacity),
	}
}

func (s *NonExpiringMapCache[K, V]) Put(key K, value V) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	if _, exists := s.store[key]; exists {
		s.store[key] = value
	} else if int(s.stats.Size()) < s.stats.Capacity {
		s.store[key] = value
		s.stats.Put()
	}
}

func (s *NonExpiringMapCache[K, V]) Get(key K) (V, bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()

	value, hasValue := s.store[key]

	if hasValue {
		s.stats.Hit()
	} else {
		s.stats.Miss()
	}

	return value, hasValue
}

func (s *NonExpiringMapCache[K, V]) Delete(key K) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	_, exists := s.store[key]

	if exists {
		delete(s.store, key)
		s.stats.Delete()
	}
}

func (s *NonExpiringMapCache[K, V]) Stats() Stats {
	return s.stats
}
