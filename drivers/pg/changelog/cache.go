package changelog

import (
	"fmt"
	"sync"
)

type changeCache struct {
	data  map[uint64]uint64
	mutex *sync.RWMutex
}

func newChangeCache() changeCache {
	return changeCache{
		data:  make(map[uint64]uint64),
		mutex: &sync.RWMutex{},
	}
}

func (s *changeCache) get(key uint64) (uint64, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *changeCache) put(key uint64, value uint64) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[key] = value
}

// checkCache attempts to resolve the proposed change using the cached snapshot.
// it returns true if the proposedChange should be written to the graph
func (s *changeCache) checkCache(proposedChange *NodeChange) (bool, error) {
	idHash := proposedChange.IdentityKey()
	dataHash, err := proposedChange.Hash()
	if err != nil {
		return false, fmt.Errorf("hash proposed change: %w", err)
	}

	// try to diff against the storedHash snapshot
	storedHash, ok := s.get(idHash)
	if !ok {
		s.put(idHash, dataHash)
		return true, nil
	}

	if storedHash == dataHash { // hash equal -> no change
		return false, nil
	}

	// hashes not equal, there is a change
	// update cache to the new snapshot so next call can short-circuit
	s.put(idHash, dataHash)
	return true, nil
}
