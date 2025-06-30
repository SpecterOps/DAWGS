package v1compat

import (
	"fmt"

	"github.com/specterops/dawgs/cardinality"
	"github.com/specterops/dawgs/database"
	"github.com/specterops/dawgs/util/size"
)

// IndexedSlice is a structure maps a comparable key to a value that implements size.Sizable.
type IndexedSlice[K comparable, V any] struct {
	index  map[K]int
	values []V
	size   size.Size
}

func NewIndexedSlice[K comparable, V any]() *IndexedSlice[K, V] {
	return &IndexedSlice[K, V]{
		index: make(map[K]int),
		size:  0,
	}
}

func (s *IndexedSlice[K, V]) Keys() []K {
	keys := make([]K, 0, len(s.index))

	for key := range s.index {
		keys = append(keys, key)
	}

	return keys
}

func (s *IndexedSlice[K, V]) Values() []V {
	return s.values
}

func (s *IndexedSlice[K, V]) Merge(other *IndexedSlice[K, V]) {
	for key, idx := range other.index {
		s.Put(key, other.values[idx])
	}
}

// Len returns the number of values stored.
func (s *IndexedSlice[K, V]) Len() int {
	return len(s.values)
}

// SizeOf returns the relative size of the IndexedSlice instance.
func (s *IndexedSlice[K, V]) SizeOf() size.Size {
	return s.size
}

func (s *IndexedSlice[K, V]) Get(key K) V {
	if valueIdx, hasValue := s.index[key]; hasValue {
		return s.values[valueIdx]
	}

	var empty V
	return empty
}

func (s *IndexedSlice[K, V]) Has(key K) bool {
	_, hasValue := s.index[key]
	return hasValue
}

func (s *IndexedSlice[K, V]) GetOr(key K, defaultConstructor func() V) V {
	if valueIdx, hasValue := s.index[key]; hasValue {
		return s.values[valueIdx]
	}

	defaultValue := defaultConstructor()

	s.Put(key, defaultValue)
	return defaultValue
}

// CheckedGet returns a tuple containing the value and a boolean representing if a value was found for the
// given key.
func (s *IndexedSlice[K, V]) CheckedGet(key K) (V, bool) {
	if valueIdx, hasValue := s.index[key]; hasValue {
		return s.values[valueIdx], true
	}

	var empty V
	return empty, false
}

// GetAll returns all found values for a given slice of keys. Any keys that do not have stored values
// in this IndexedSlice are returned as the second value of the tuple return for this function.
func (s *IndexedSlice[K, V]) GetAll(keys []K) ([]V, []K) {
	var (
		values      = make([]V, 0, len(keys))
		missingKeys = make([]K, 0, len(keys))
	)

	for _, key := range keys {
		if valueIdx, hasValue := s.index[key]; hasValue {
			values = append(values, s.values[valueIdx])
		} else {
			missingKeys = append(missingKeys, key)
		}
	}

	return values, missingKeys
}

// GetAllIndexed returns all found values for a given slice of keys. Any keys that do not have stored values
// in this IndexedSlice are returned as the second value of the tuple return for this function.
func (s *IndexedSlice[K, V]) GetAllIndexed(keys []K) (*IndexedSlice[K, V], []K) {
	var (
		values      = NewIndexedSlice[K, V]()
		missingKeys = make([]K, 0, len(keys))
	)

	for _, key := range keys {
		if valueIdx, hasValue := s.index[key]; hasValue {
			values.Put(key, s.values[valueIdx])
		} else {
			missingKeys = append(missingKeys, key)
		}
	}

	return values, missingKeys
}

func sizeOf(value any) size.Size {
	if sizeable, typeOK := value.(size.Sizable); typeOK {
		return sizeable.SizeOf()
	}

	return size.Of(value)
}

// Put inserts the given value with the given key.
func (s *IndexedSlice[K, V]) Put(key K, value V) {
	s.size += sizeOf(value)

	if valueIdx, hasValue := s.index[key]; hasValue {
		s.size -= sizeOf(s.values[valueIdx])
		s.values[valueIdx] = value
	} else {
		s.values = append(s.values, value)
		s.index[key] = len(s.values) - 1
	}
}

func (s *IndexedSlice[K, V]) Each(delegate func(key K, value V) bool) {
	for id, idx := range s.index {
		if !delegate(id, s.values[idx]) {
			break
		}
	}
}

// DuplexToGraphIDs takes a Duplex provider and returns a slice of graph IDs.
func DuplexToGraphIDs[T uint32 | uint64](provider cardinality.Duplex[T]) []ID {
	ids := make([]ID, 0, provider.Cardinality())

	provider.Each(func(value T) bool {
		ids = append(ids, ID(value))
		return true
	})

	return ids

}

func AnyToV2DB(instance any) (database.Instance, error) {
	switch typedInstance := instance.(type) {
	case *v1Wrapper:
		return typedInstance.v2DB, nil
	case database.Instance:
		return typedInstance, nil
	default:
		return nil, fmt.Errorf("unsupported instance type: %T", instance)
	}
}

func AnyToV2Driver(session any) (database.Driver, error) {
	switch typedSession := session.(type) {
	case *driverTransactionWrapper:
		return typedSession.driver, nil
	case *driverBatchWrapper:
		return typedSession.driver, nil
	case database.Driver:
		return typedSession, nil
	default:
		return nil, fmt.Errorf("unsupported session type: %T", session)
	}
}
