package dawgs

import (
	"strconv"

	"github.com/specterops/dawgs/graph"
)

// Resolver maps collection source node IDs to IDs assigned by the target
// database. Canonical decimal IDs use a compact numeric map; all other IDs
// retain their exact string representation.
type Resolver struct {
	numeric  map[uint64]graph.ID
	fallback map[string]graph.ID
}

func NewResolver(expected int64) *Resolver {
	capacity := int(expected)
	if int64(capacity) != expected || capacity < 0 {
		capacity = 0
	}

	return &Resolver{numeric: make(map[uint64]graph.ID, capacity)}
}

func (s *Resolver) Put(sourceID string, destinationID graph.ID) bool {
	if numericID, ok := canonicalNumericSourceID(sourceID); ok {
		if _, exists := s.numeric[numericID]; exists {
			return false
		}
		s.numeric[numericID] = destinationID
		return true
	}

	if s.fallback == nil {
		s.fallback = make(map[string]graph.ID)
	}
	if _, exists := s.fallback[sourceID]; exists {
		return false
	}
	s.fallback[sourceID] = destinationID
	return true
}

func (s *Resolver) Resolve(sourceID string) (graph.ID, bool) {
	if numericID, ok := canonicalNumericSourceID(sourceID); ok {
		resolved, found := s.numeric[numericID]
		return resolved, found
	}

	resolved, found := s.fallback[sourceID]
	return resolved, found
}

func canonicalNumericSourceID(sourceID string) (uint64, bool) {
	if sourceID == "" {
		return 0, false
	}

	value, err := strconv.ParseUint(sourceID, 10, 64)
	if err != nil || strconv.FormatUint(value, 10) != sourceID {
		return 0, false
	}

	return value, true
}
