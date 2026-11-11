package cache

import "sync/atomic"

// Stats holds shared atomic cache counters and a fixed entry capacity.
// Copying Stats shares the counters; Combined produces independent counters.
type Stats struct {
	// hits counts successful lookups.
	hits *atomic.Int64
	// misses counts lookups without a cached entry.
	misses *atomic.Int64
	// size tracks the current number of entries.
	size *atomic.Int64
	// Capacity is the configured maximum entry count.
	Capacity int
}

// Combined snapshots and sums two sets of counters into independent statistics.
func (s Stats) Combined(other Stats) Stats {
	var (
		hits     = &atomic.Int64{}
		misses   = &atomic.Int64{}
		size     = &atomic.Int64{}
		capacity = s.Capacity + other.Capacity
	)

	hits.Add(s.Hits())
	hits.Add(other.Hits())

	misses.Add(s.Misses())
	misses.Add(other.Misses())

	size.Add(s.Size())
	size.Add(other.Size())

	return Stats{
		hits:     hits,
		misses:   misses,
		size:     size,
		Capacity: capacity,
	}
}

// Miss atomically records one lookup without a cached entry.
func (s Stats) Miss() {
	s.misses.Add(1)
}

// Misses atomically reads the number of unsuccessful lookups.
func (s Stats) Misses() int64 {
	return s.misses.Load()
}

// Hit atomically records one successful lookup.
func (s Stats) Hit() {
	s.hits.Add(1)
}

// Hits atomically reads the number of successful lookups.
func (s Stats) Hits() int64 {
	return s.hits.Load()
}

// Size atomically reads the current entry count.
func (s Stats) Size() int64 {
	return s.size.Load()
}

// Put atomically increments the tracked entry count.
func (s Stats) Put() {
	s.size.Add(1)
}

// Delete atomically decrements the tracked entry count.
func (s Stats) Delete() {
	s.size.Add(-1)
}

// NewStats creates zeroed, independently owned counters for the supplied capacity.
func NewStats(capacity int) Stats {
	return Stats{
		hits:     &atomic.Int64{},
		misses:   &atomic.Int64{},
		size:     &atomic.Int64{},
		Capacity: capacity,
	}
}

// Cache stores values by key and exposes hit, miss, and entry counters.
type Cache[K comparable, V any] interface {
	// Put inserts or replaces the value associated with key.
	Put(key K, value V)
	// Get returns the value and whether key is present.
	Get(key K) (V, bool)
	// Delete removes key if present.
	Delete(key K)
	// Stats returns the cache counters and configured capacity.
	Stats() Stats
}
