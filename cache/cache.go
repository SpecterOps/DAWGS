package cache

import "sync/atomic"

type Stats struct {
	hits     *atomic.Int64
	misses   *atomic.Int64
	size     *atomic.Int64
	Capacity int
}

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

func (s Stats) Miss() {
	s.misses.Add(1)
}

func (s Stats) Misses() int64 {
	return s.misses.Load()
}

func (s Stats) Hit() {
	s.hits.Add(1)
}

func (s Stats) Hits() int64 {
	return s.hits.Load()
}

func (s Stats) Size() int64 {
	return s.size.Load()
}

func (s Stats) Put() {
	s.size.Add(1)
}

func (s Stats) Delete() {
	s.size.Add(-1)
}

func NewStats(capacity int) Stats {
	return Stats{
		hits:     &atomic.Int64{},
		misses:   &atomic.Int64{},
		size:     &atomic.Int64{},
		Capacity: capacity,
	}
}

type Cache[K comparable, V any] interface {
	Put(key K, value V)
	Get(key K) (V, bool)
	Delete(key K)
	Stats() Stats
}
