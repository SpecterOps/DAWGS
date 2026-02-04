package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// entry holds the key and value of a cache entry.
//
// The generic parameters K and V represent the key and value types
// respectively. An entry also tracks whether it has been visited
// during the most recent clock‑hand sweep via the atomic `visited`
// flag and stores a pointer to its position in the eviction queue.
type entry[K comparable, V any] struct {
	key     K
	value   V
	visited atomic.Bool
	element *list.Element
}

// Sieve implements a clock‑hand (second‑chance) cache with a fixed
// capacity. It is safe for concurrent use by multiple goroutines.
//
// The cache maintains:
//   - a map (`store`) from keys to entries for O(1) lookups,
//   - a doubly‑linked list (`queue`) that orders entries from most
//     recently inserted (front) to least recently inserted (back),
//   - a “hand” pointer that walks the list during eviction, giving
//     each entry a second chance if it has been visited since the
//     previous sweep.
//
// Statistics about cache usage are collected in the embedded `stats`
// field.
type Sieve[K comparable, V any] struct {
	rwLock sync.RWMutex
	store  map[K]*entry[K, V]
	queue  *list.List
	hand   *list.Element
	stats  Stats
}

// NewSieve creates a new Sieve cache with the supplied capacity.
//
// If `capacity` is less than or equal to zero, a capacity of one is
// used to prevent function calls from panicking. The returned value
// implements the `Cache[K,V]` interface.
func NewSieve[K comparable, V any](capacity int) Cache[K, V] {
	// Do not panic on an invalid capacity but ensure that the cache remains functional
	if capacity <= 0 {
		capacity = 1
	}

	return &Sieve[K, V]{
		store: make(map[K]*entry[K, V], capacity),
		queue: list.New(),
		stats: NewStats(capacity),
	}
}

// Stats returns a copy of the cache’s current statistics.
//
// The returned `Stats` value can be inspected without holding the
// cache lock; it contains counters for hits, misses, puts, and
// deletions as well as the configured capacity.
func (s *Sieve[K, V]) Stats() Stats {
	return s.stats
}

// putEntry inserts a new key/value pair into the cache without acquiring
// the lock.
//
// It first evicts an entry if the cache is already at capacity, then
// creates a fresh `entry`, pushes its key onto the front of the
// eviction queue, stores the entry in the map, and records a put in
// the statistics.
//
// This method is intended to be called only from other methods that
// already hold `s.rwLock`.
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

// Put adds or updates the value associated with `key`.
//
// If the key already exists, its value is replaced and the entry is
// marked as visited (so it will survive the next eviction sweep). If
// the key is new, a fresh entry is inserted, possibly triggering an
// eviction when the cache is full.
//
// The operation holds an exclusive lock for the duration of the update.
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

// Get retrieves the value associated with `key`.
//
// If the key is present, the entry’s visited flag is set, a hit is
// recorded, and the cached value is returned with `true`. If the key
// is absent, a miss is recorded and the zero value of V is returned
// with `false`.
//
// The operation holds a read lock while accessing the map.
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

// removeEntry deletes `e` from both the eviction queue and the store map,
// and updates the statistics to reflect a deletion.
//
// This helper assumes that the caller already holds the appropriate lock.
func (s *Sieve[K, V]) removeEntry(e *entry[K, V]) {
	s.queue.Remove(e.element)
	delete(s.store, e.key)

	s.stats.Delete()
}

// Delete removes the entry identified by `key` from the cache.
//
// If the entry being removed is currently pointed to by the clock hand,
// the hand is moved one step backward so that eviction can continue
// correctly. The method records a deletion in the statistics.
//
// The operation holds an exclusive lock for the duration of the removal.
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

// evict removes a single entry from the cache using the clock‑hand
// (second‑chance) algorithm.
//
// The hand walks backwards through the list (from most recent to
// least recent). For each visited entry the visited flag is cleared
// and the hand moves on, giving the entry a “second chance”. The
// first entry encountered whose visited flag is false is evicted.
// After eviction the hand is positioned one element before the removed
// entry so that the next eviction continues where this one left off.
//
// This method assumes the caller already holds the exclusive lock.
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
