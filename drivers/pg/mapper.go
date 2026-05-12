package pg

import (
	"context"

	"github.com/specterops/dawgs/graph"
)

const (
	minKindID = -1 << 15
	maxKindID = 1<<15 - 1
)

func mapKindIDs(ctx context.Context, kindMapper KindMapper, kindIDs []int16) (graph.Kinds, bool) {
	if len(kindIDs) == 0 {
		return graph.Kinds{}, true
	}

	if mappedKinds, err := kindMapper.MapKindIDs(ctx, kindIDs); err == nil {
		return mappedKinds, true
	}

	return nil, false
}

func asKindID(value any) (int16, bool) {
	switch typedValue := value.(type) {
	case int:
		if typedValue < minKindID || typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	case int8:
		return int16(typedValue), true
	case int16:
		return typedValue, true
	case int32:
		if typedValue < minKindID || typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	case int64:
		if typedValue < minKindID || typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	case uint:
		if typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	case uint8:
		return int16(typedValue), true
	case uint16:
		if typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	case uint32:
		if typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	case uint64:
		if typedValue > maxKindID {
			return 0, false
		}

		return int16(typedValue), true
	default:
		return 0, false
	}
}

func mapAnyKinds(ctx context.Context, kindMapper KindMapper, values []any) (graph.Kinds, bool) {
	if len(values) == 0 {
		return graph.Kinds{}, true
	}

	var (
		kindIDs     []int16
		kindStrings []string
	)

	for _, value := range values {
		if kindString, typeOK := value.(string); typeOK {
			if kindIDs != nil {
				return nil, false
			}

			kindStrings = append(kindStrings, kindString)
		} else if kindID, typeOK := asKindID(value); typeOK {
			if kindStrings != nil {
				return nil, false
			}

			kindIDs = append(kindIDs, kindID)
		} else {
			return nil, false
		}
	}

	if kindStrings != nil {
		return graph.StringsToKinds(kindStrings), true
	}

	return mapKindIDs(ctx, kindMapper, kindIDs)
}

func mapKinds(ctx context.Context, kindMapper KindMapper, untypedValue any) (graph.Kinds, bool) {
	switch typedValue := untypedValue.(type) {
	case []any:
		return mapAnyKinds(ctx, kindMapper, typedValue)

	case []int16:
		return mapKindIDs(ctx, kindMapper, typedValue)

	case []string:
		return graph.StringsToKinds(typedValue), true
	}

	return nil, false
}

func newMapFunc(ctx context.Context, kindMapper KindMapper) graph.MapFunc {
	return func(value, target any) bool {
		switch typedTarget := target.(type) {
		case *graph.Relationship:
			if compositeMap, typeOK := value.(map[string]any); typeOK {
				edge := edgeComposite{}

				if edge.TryMap(compositeMap) {
					if err := edge.ToRelationship(ctx, kindMapper, typedTarget); err == nil {
						return true
					}
				}
			}

		case *graph.Node:
			if compositeMap, typeOK := value.(map[string]any); typeOK {
				node := nodeComposite{}

				if node.TryMap(compositeMap) {
					if err := node.ToNode(ctx, kindMapper, typedTarget); err == nil {
						return true
					}
				}
			}

		case *graph.Path:
			if compositeMap, typeOK := value.(map[string]any); typeOK {
				path := pathComposite{}

				if path.TryMap(compositeMap) {
					if err := path.ToPath(ctx, kindMapper, typedTarget); err == nil {
						return true
					}
				}
			}

		case *graph.Kind:
			if kindID, typeOK := value.(int16); typeOK {
				if kind, err := kindMapper.MapKindID(ctx, kindID); err == nil {
					*typedTarget = kind
					return true
				}
			}

		case *graph.Kinds:
			if mappedKinds, typeOK := mapKinds(ctx, kindMapper, value); typeOK {
				*typedTarget = mappedKinds
				return true
			}
		}

		return false
	}
}

func NewValueMapper(ctx context.Context, kindMapper KindMapper) graph.ValueMapper {
	return graph.NewValueMapper(newMapFunc(ctx, kindMapper))
}
