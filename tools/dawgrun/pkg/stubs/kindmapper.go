// Package stubs has various bits and bobs to stub out internal behaviors
package stubs

import (
	"context"
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

var (
	ErrNoSuchKind   = errors.New("no such kind")
	ErrNoSuchKindID = errors.New("no such kind with ID")
)

type KindMap map[int16]graph.Kind

func (s KindMap) Invert() map[graph.Kind]int16 {
	inverse := make(map[graph.Kind]int16)
	for kindID, kind := range s {
		inverse[kind] = kindID
	}

	return inverse
}

type DumbKindMapper struct {
	idToKind KindMap
	kindToID map[graph.Kind]int16
	lastID   int16
}

var _ pgsql.KindMapper = (*DumbKindMapper)(nil)

func EmptyMapper() *DumbKindMapper {
	return &DumbKindMapper{
		idToKind: make(KindMap),
		kindToID: make(map[graph.Kind]int16),
		lastID:   -1,
	}
}

func MapperFromKindMap(kindMap KindMap) *DumbKindMapper {
	idToKind := make(KindMap, len(kindMap))
	lastID := int16(-1)
	for id, kind := range kindMap {
		idToKind[id] = kind
		if id > lastID {
			lastID = id
		}
	}

	return &DumbKindMapper{
		idToKind: idToKind,
		kindToID: idToKind.Invert(),
		lastID:   lastID,
	}
}

func (s *DumbKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	kindIDs := make([]int16, len(kinds))
	for idx, kind := range kinds {
		if mappedKindID, ok := s.kindToID[kind]; !ok {
			return nil, fmt.Errorf("%w: %v", ErrNoSuchKind, kind)
		} else {
			kindIDs[idx] = mappedKindID
		}
	}
	return kindIDs, nil
}

// AssertKinds tries to return IDs of `graph.Kind`s that are already known, inserting any kinds not known
// into the schema.
func (s *DumbKindMapper) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	kindIDs := make([]int16, 0)
	for _, kind := range kinds {
		if mappedKindID, ok := s.kindToID[kind]; !ok {
			newID := s.lastID + 1
			s.lastID += 1
			s.kindToID[kind] = newID
			s.idToKind[newID] = kind
			kindIDs = append(kindIDs, newID)
		} else {
			kindIDs = append(kindIDs, mappedKindID)
		}
	}

	return kindIDs, nil
}

func (s *DumbKindMapper) GetKindByID(id int16) (graph.Kind, error) {
	if kind, ok := s.idToKind[id]; !ok {
		return nil, fmt.Errorf("%w: %d", ErrNoSuchKindID, id)
	} else {
		return kind, nil
	}
}

func (s *DumbKindMapper) GetIDByKind(kind graph.Kind) (int16, error) {
	if kindID, ok := s.kindToID[kind]; !ok {
		return -1, fmt.Errorf("%w: %v", ErrNoSuchKind, kind)
	} else {
		return kindID, nil
	}
}
