package changestream

import (
	"bytes"
	"fmt"
	"sync"
)

// todo: use golang-LRU cache
// time multiple ingest runs with no cache, size 1_000, 100_000
type changeCache struct {
	data  map[string]*NodeChange
	mutex *sync.RWMutex
}

func newChangeCache() changeCache {
	return changeCache{
		data:  make(map[string]*NodeChange),
		mutex: &sync.RWMutex{},
	}
}

func (s *changeCache) get(key string) (*NodeChange, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *changeCache) put(key string, value *NodeChange) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.data[key] = value
}

// checkCache attempts to resolve the proposed change using the cached snapshot.
// It MUTATES proposedChange when it can fully resolve (NoChange or Modified) and returns handled=true.
// On cache miss (no entry), it returns handled=false and does not mutate proposedChange.
func (s *changeCache) checkCache(proposedChange *NodeChange) (bool, error) {
	key := proposedChange.IdentityKey()

	proposedHash, err := proposedChange.Hash()
	if err != nil {
		return false, fmt.Errorf("hash proposed change: %w", err)
	}

	// try to diff against the cached snapshot
	cached, ok := s.get(key)
	if !ok {
		return false, nil // let caller hit DB
	}

	if prevHash, err := cached.Hash(); err != nil {
		return false, nil
	} else if bytes.Equal(prevHash, proposedHash) { // hash equal -> NoChange
		proposedChange.changeType = ChangeTypeNoChange
		return true, nil
	}

	// hash differs -> modified,
	// diff against cached properties
	oldProps := cached.Properties
	newProps := proposedChange.Properties
	proposedChange.Properties = DiffProps(oldProps, newProps)

	proposedChange.changeType = ChangeTypeModified

	// Update cache to the new snapshot so next call can short-circuit
	s.put(key, proposedChange)

	return true, nil
}
