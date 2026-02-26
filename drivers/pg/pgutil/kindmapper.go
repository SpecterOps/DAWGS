package pgutil

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

type InMemoryKindMapper struct {
	nextKindID int16
	KindToID   map[graph.Kind]int16
	IDToKind   map[int16]graph.Kind
}

func NewInMemoryKindMapper() *InMemoryKindMapper {
	return &InMemoryKindMapper{
		nextKindID: int16(1),
		KindToID:   map[graph.Kind]int16{},
		IDToKind:   map[int16]graph.Kind{},
	}
}

func (s *InMemoryKindMapper) MapKindID(ctx context.Context, kindID int16) (graph.Kind, error) {
	if kind, hasKind := s.IDToKind[kindID]; hasKind {
		return kind, nil
	}

	return nil, fmt.Errorf("kind not found for id %d", kindID)
}

func (s *InMemoryKindMapper) MapKindIDs(ctx context.Context, kindIDs []int16) (graph.Kinds, error) {
	kinds := make(graph.Kinds, len(kindIDs))

	for idx, kindID := range kindIDs {
		if kind, err := s.MapKindID(ctx, kindID); err != nil {
			return nil, err
		} else {
			kinds[idx] = kind
		}
	}

	return kinds, nil
}

func (s *InMemoryKindMapper) MapKind(ctx context.Context, kind graph.Kind) (int16, error) {
	if id, hasID := s.KindToID[kind]; hasID {
		return id, nil
	}

	return 0, fmt.Errorf("no id found for kind %s", kind)
}

func (s *InMemoryKindMapper) mapKinds(kinds graph.Kinds) ([]int16, graph.Kinds) {
	var (
		ids     = make([]int16, 0, len(kinds))
		missing = make(graph.Kinds, 0, len(kinds))
	)

	for _, kind := range kinds {
		if id, found := s.KindToID[kind]; !found {
			missing = append(missing, kind)
		} else {
			ids = append(ids, id)
		}
	}

	return ids, missing
}

func (s *InMemoryKindMapper) MapKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	if ids, missing := s.mapKinds(kinds); len(missing) > 0 {
		return nil, fmt.Errorf("missing kinds: %v", missing)
	} else {
		return ids, nil
	}
}

func (s *InMemoryKindMapper) AssertKinds(ctx context.Context, kinds graph.Kinds) ([]int16, error) {
	ids, missing := s.mapKinds(kinds)

	for _, kind := range missing {
		ids = append(ids, s.Put(kind))
	}

	return ids, nil
}

func (s *InMemoryKindMapper) Put(kind graph.Kind) int16 {
	if kindID, ok := s.KindToID[kind]; ok {
		return kindID
	}

	kindID := s.nextKindID
	s.nextKindID += 1

	s.KindToID[kind] = kindID
	s.IDToKind[kindID] = kind

	return kindID
}
