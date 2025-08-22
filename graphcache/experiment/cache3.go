package experiment

import (
	"github.com/OneOfOne/xxhash"
	"github.com/specterops/dawgs/graph"
)

type BasicMap struct {
	store    map[uint64][]byte
	digester *xxhash.XXHash64
}

func NewBasicMap() *BasicMap {
	return &BasicMap{
		store:    make(map[uint64][]byte),
		digester: xxhash.New64(),
	}
}

func (s *BasicMap) Stat() int {
	return len(s.store)
}

func (s *BasicMap) Compact() error {
	return nil
}

func (s *BasicMap) PutNode(entityID string, node *graph.Node) ([]byte, error) {
	if entityIDHash, err := digestEntityIDHash(s.digester, entityID); err != nil {
		return nil, err
	} else if entityHash, err := digestNodeHash(s.digester, node); err != nil {
		return nil, err
	} else {
		s.store[entityIDHash] = entityHash
		return entityHash, nil
	}
}
