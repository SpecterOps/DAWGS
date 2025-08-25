package changelog

import (
	"fmt"
	"sync"
)

type cache struct {
	data  map[uint64]uint64
	mutex *sync.Mutex
	stats CacheStats
}

func newChangeCache() cache {
	return cache{
		data:  make(map[uint64]uint64),
		mutex: &sync.Mutex{},
		stats: CacheStats{},
	}
}

type CacheStats struct {
	Hits   uint64 // unchanged
	Misses uint64 // new or modified
}

// shouldSubmit compares the proposed change against the cached snapshot.
// It returns true if the change is new or modified and should be submitted
// downstream. If the change is identical to the cached version, it returns false.
func (s *cache) shouldSubmit(change Change) (bool, error) {
	idHash := change.IdentityKey()
	dataHash, err := change.Hash()
	if err != nil {
		return false, fmt.Errorf("hash proposed change: %w", err)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// try to diff against the storedHash snapshot
	if storedHash, ok := s.data[idHash]; ok {
		if storedHash == dataHash {
			s.stats.Hits++
			return false, nil // unchanged
		}
	}

	// new or modified -> update cache to the new snapshot
	s.data[idHash] = dataHash
	s.stats.Misses++
	return true, nil
}

func (s *cache) Stats() CacheStats {
	return s.stats
}

func (s *cache) ResetStats() CacheStats {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	old := s.stats
	s.stats = CacheStats{} // zero it out
	return old
}
