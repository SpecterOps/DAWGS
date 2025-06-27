package v2

import (
	"context"

	"github.com/specterops/dawgs/drivers/pg"

	"github.com/specterops/dawgs/graph"
)

func mapKinds(ctx context.Context, kindMapper pg.KindMapper, untypedValue any) (graph.Kinds, bool) {
	var (
		// The default assumption is that the untyped value contains a type that can be mapped from
		validType = true
		kindIDs   []int16
	)

	switch typedValue := untypedValue.(type) {
	case []any:
		kindIDs = make([]int16, len(typedValue))

		for idx, untypedElement := range typedValue {
			if typedElement, typeOK := untypedElement.(int16); typeOK {
				kindIDs[idx] = typedElement
			}
		}

	case []int16:
		kindIDs = typedValue

	default:
		// This is not a valid type to map to graph Kinds
		validType = false
	}

	// Guard to prevent unnecessary thrashing of critical sections if there are no kind IDs to resolve
	if len(kindIDs) > 0 {
		// Ignoring the error here is intentional. Failure to map the kinds here does not imply a fatal error.
		if mappedKinds, err := kindMapper.MapKindIDs(ctx, kindIDs); err == nil {
			return mappedKinds, true
		}
	}

	// Return validType here in case there was a type match (in which case the mapper succeeded) but the type did not
	// contain a valid kind ID to map to
	return nil, validType
}

func newMapFunc(ctx context.Context, kindMapper pg.KindMapper) graph.MapFunc {
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

func newValueMapper(ctx context.Context, kindMapper pg.KindMapper) graph.ValueMapper {
	return graph.NewValueMapper(newMapFunc(ctx, kindMapper))
}
