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

func (km KindMap) Invert() map[graph.Kind]int16 {
	inverse := make(map[graph.Kind]int16)
	for kindID, kind := range km {
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
	return &DumbKindMapper{
		idToKind: kindMap,
		kindToID: kindMap.Invert(),
		lastID:   -1,
	}
}

func (k *DumbKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	return k.AssertKinds(ctx, kinds)
}

// AssertKinds tries to return IDs of `graph.Kind`s that are already known, inserting any kinds not known
// into the schema.
func (k *DumbKindMapper) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	kindIDs := make([]int16, 0)
	for _, kind := range kinds {
		if mappedKindID, ok := k.kindToID[kind]; !ok {
			newID := k.lastID + 1
			k.lastID += 1
			k.kindToID[kind] = newID
			k.idToKind[newID] = kind
			kindIDs = append(kindIDs, newID)
		} else {
			kindIDs = append(kindIDs, mappedKindID)
		}
	}

	return kindIDs, nil
}

func (k *DumbKindMapper) GetKindByID(id int16) (graph.Kind, error) {
	if kind, ok := k.idToKind[id]; !ok {
		return nil, fmt.Errorf("%w: %d", ErrNoSuchKindID, id)
	} else {
		return kind, nil
	}
}

func (k *DumbKindMapper) GetIDByKind(kind graph.Kind) (int16, error) {
	if kindID, ok := k.kindToID[kind]; !ok {
		return -1, fmt.Errorf("%w: %v", ErrNoSuchKind, kind)
	} else {
		return kindID, nil
	}
}
