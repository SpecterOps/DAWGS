package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// entry holds the key and value of a cache entry.
type entry[K comparable, V any] struct {
	key     K
	value   V
	visited atomic.Bool
	element *list.Element
}

type Sieve[K comparable, V any] struct {
	rwLock sync.RWMutex
	store  map[K]*entry[K, V]
	queue  *list.List
	hand   *list.Element
	stats  Stats
}

func NewSieve[K comparable, V any](capacity int) Cache[K, V] {
	return &Sieve[K, V]{
		store: make(map[K]*entry[K, V], capacity),
		queue: list.New(),
		stats: NewStats(capacity),
	}
}

func (s *Sieve[K, V]) Stats() Stats {
	return s.stats
}

func (s *Sieve[K, V]) putEntry(key K, value V) {
	// Evict first if needed
	if s.queue.Len() >= int(s.stats.Capacity) {
		s.evict()
	}

	newEntry := &entry[K, V]{
		key:     key,
		value:   value,
		element: s.queue.PushFront(key),
	}

	s.store[key] = newEntry
	s.Stats().Put()
}

func (s *Sieve[K, V]) Put(key K, value V) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	if existingEntry, exists := s.store[key]; exists {
		// Update the entry values
		existingEntry.value = value
		existingEntry.visited.Store(true)
	} else {
		s.putEntry(key, value)
	}
}

func (s *Sieve[K, V]) Get(key K) (V, bool) {
	s.rwLock.RLock()
	defer s.rwLock.RUnlock()

	if entry, exists := s.store[key]; exists {
		s.stats.Hit()

		entry.visited.Store(true)
		return entry.value, true
	}

	s.stats.Miss()

	var emptyV V
	return emptyV, false
}

func (s *Sieve[K, V]) removeEntry(e *entry[K, V]) {
	s.queue.Remove(e.element)
	delete(s.store, e.key)

	s.stats.Delete()
}

func (s *Sieve[K, V]) Delete(key K) {
	s.rwLock.Lock()
	defer s.rwLock.Unlock()

	if entry, exists := s.store[key]; exists {
		if entry.element == s.hand {
			s.hand = s.hand.Prev()
		}

		s.removeEntry(entry)
	}
}

func (s *Sieve[K, V]) evict() {
	hand := s.hand

	// if nil assign it to the tail element in the list
	if hand == nil {
		hand = s.queue.Back()
	}

	entry := s.store[hand.Value.(K)]

	for entry.visited.Load() {
		entry.visited.Store(false)

		if hand = hand.Prev(); hand == nil {
			hand = s.queue.Back()
		}

		entry = s.store[hand.Value.(K)]
	}

	s.hand = hand.Prev()
	s.removeEntry(entry)
}
