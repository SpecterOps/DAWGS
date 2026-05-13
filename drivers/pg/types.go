package pg

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

type edgeComposite struct {
	ID         int64
	StartID    int64
	EndID      int64
	KindID     int16
	Properties map[string]any
}

func castSlice[T any](raw any) ([]T, error) {
	switch rawSlice := raw.(type) {
	case []T:
		return append([]T(nil), rawSlice...), nil

	case []any:
		sliceCopy := make([]T, len(rawSlice))
		for idx, rawValue := range rawSlice {
			if typedValue, typeOK := rawValue.(T); !typeOK {
				var empty T
				return nil, fmt.Errorf("expected type %T but received %T", empty, rawValue)
			} else {
				sliceCopy[idx] = typedValue
			}
		}

		return sliceCopy, nil

	default:
		return nil, fmt.Errorf("expected raw slice type []%T or []any but received %T", *new(T), raw)
	}
}

func castMapValueAsSliceOf[T any](compositeMap map[string]any, key string) ([]T, error) {
	if src, hasKey := compositeMap[key]; !hasKey {
		return nil, fmt.Errorf("composite map does not contain expected key %s", key)
	} else {
		return castSlice[T](src)
	}
}

func castAndAssignMapValue[T any](compositeMap map[string]any, key string, dst *T) error {
	if src, hasKey := compositeMap[key]; !hasKey {
		return fmt.Errorf("composite map does not contain expected key %s", key)
	} else {
		switch typedSrc := src.(type) {
		case int8:
			switch typedDst := any(dst).(type) {
			case *int8:
				*typedDst = typedSrc
			case *int16:
				*typedDst = int16(typedSrc)
			case *int32:
				*typedDst = int32(typedSrc)
			case *int64:
				*typedDst = int64(typedSrc)
			case *int:
				*typedDst = int(typedSrc)
			default:
				return fmt.Errorf("unable to cast and assign value type: %T", src)
			}

		case int16:
			switch typedDst := any(dst).(type) {
			case *int16:
				*typedDst = typedSrc
			case *int32:
				*typedDst = int32(typedSrc)
			case *int64:
				*typedDst = int64(typedSrc)
			case *int:
				*typedDst = int(typedSrc)
			default:
				return fmt.Errorf("unable to cast and assign value type: %T", src)
			}

		case int32:
			switch typedDst := any(dst).(type) {
			case *int32:
				*typedDst = typedSrc
			case *int64:
				*typedDst = int64(typedSrc)
			case *int:
				*typedDst = int(typedSrc)
			default:
				return fmt.Errorf("unable to cast and assign value type: %T", src)
			}

		case int64:
			switch typedDst := any(dst).(type) {
			case *int64:
				*typedDst = typedSrc
			case *int:
				*typedDst = int(typedSrc)
			default:
				return fmt.Errorf("unable to cast and assign value type: %T", src)
			}

		case int:
			switch typedDst := any(dst).(type) {
			case *int64:
				*typedDst = int64(typedSrc)
			case *int:
				*typedDst = typedSrc
			default:
				return fmt.Errorf("unable to cast and assign value type: %T", src)
			}

		case T:
			*dst = typedSrc

		default:
			return fmt.Errorf("unable to cast and assign value type: %T", src)
		}
	}

	return nil
}

func nodeCompositesFromRaw(raw any) ([]nodeComposite, error) {
	rawNodes, typeOK := raw.([]any)
	if !typeOK {
		return nil, fmt.Errorf("expected raw node composite array type []any but received %T", raw)
	}

	nodes := make([]nodeComposite, 0, len(rawNodes))
	for _, rawNode := range rawNodes {
		compositeMap, typeOK := rawNode.(map[string]any)
		if !typeOK {
			return nil, fmt.Errorf("unexpected type for raw node: %T", rawNode)
		}

		var node nodeComposite
		if err := node.FromMap(compositeMap); err != nil {
			return nil, err
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func edgeCompositesFromRaw(raw any) ([]edgeComposite, error) {
	rawEdges, typeOK := raw.([]any)
	if !typeOK {
		return nil, fmt.Errorf("expected raw edge composite array type []any but received %T", raw)
	}

	edges := make([]edgeComposite, 0, len(rawEdges))
	for _, rawEdge := range rawEdges {
		compositeMap, typeOK := rawEdge.(map[string]any)
		if !typeOK {
			return nil, fmt.Errorf("unexpected type for raw edge: %T", rawEdge)
		}

		var edge edgeComposite
		if err := edge.FromMap(compositeMap); err != nil {
			return nil, err
		}

		edges = append(edges, edge)
	}

	return edges, nil
}

func (s *edgeComposite) TryMap(compositeMap map[string]any) bool {
	return s.FromMap(compositeMap) == nil
}

func (s *edgeComposite) FromMap(compositeMap map[string]any) error {
	if err := castAndAssignMapValue(compositeMap, "id", &s.ID); err != nil {
		return err
	}

	if err := castAndAssignMapValue(compositeMap, "start_id", &s.StartID); err != nil {
		return err
	}

	if err := castAndAssignMapValue(compositeMap, "end_id", &s.EndID); err != nil {
		return err
	}

	if err := castAndAssignMapValue(compositeMap, "kind_id", &s.KindID); err != nil {
		return err
	}

	if err := castAndAssignMapValue(compositeMap, "properties", &s.Properties); err != nil {
		return err
	}

	return nil
}

func (s *edgeComposite) ToRelationship(ctx context.Context, kindMapper KindMapper, relationship *graph.Relationship) error {
	if kind, err := kindMapper.MapKindID(ctx, s.KindID); err != nil {
		return err
	} else {
		relationship.Kind = kind
	}

	relationship.ID = graph.ID(s.ID)
	relationship.StartID = graph.ID(s.StartID)
	relationship.EndID = graph.ID(s.EndID)
	relationship.Properties = graph.AsProperties(s.Properties)

	return nil
}

type nodeComposite struct {
	ID         int64
	KindIDs    []int16
	Properties map[string]any
}

func (s *nodeComposite) TryMap(compositeMap map[string]any) bool {
	return s.FromMap(compositeMap) == nil
}

func (s *nodeComposite) FromMap(compositeMap map[string]any) error {
	if err := castAndAssignMapValue(compositeMap, "id", &s.ID); err != nil {
		return err
	}

	if kindIDs, err := castMapValueAsSliceOf[int16](compositeMap, "kind_ids"); err != nil {
		return err
	} else {
		s.KindIDs = kindIDs
	}

	if err := castAndAssignMapValue(compositeMap, "properties", &s.Properties); err != nil {
		return err
	}

	return nil
}

func (s *nodeComposite) ToNode(ctx context.Context, kindMapper KindMapper, node *graph.Node) error {
	if kinds, err := kindMapper.MapKindIDs(ctx, s.KindIDs); err != nil {
		return err
	} else {
		node.Kinds = kinds
	}

	node.ID = graph.ID(s.ID)
	node.Properties = graph.AsProperties(s.Properties)

	return nil
}

type pathComposite struct {
	Nodes []nodeComposite
	Edges []edgeComposite
}

func (s *pathComposite) TryMap(compositeMap map[string]any) bool {
	return s.FromMap(compositeMap) == nil
}

func (s *pathComposite) FromMap(compositeMap map[string]any) error {
	if rawNodes, hasNodes := compositeMap["nodes"]; hasNodes {
		if nodes, err := nodeCompositesFromRaw(rawNodes); err != nil {
			return err
		} else {
			s.Nodes = append(s.Nodes, nodes...)
		}
	}

	if rawEdges, hasEdges := compositeMap["edges"]; hasEdges {
		if edges, err := edgeCompositesFromRaw(rawEdges); err != nil {
			return err
		} else {
			s.Edges = append(s.Edges, edges...)
		}
	}

	return nil
}

func (s *pathComposite) ToPath(ctx context.Context, kindMapper KindMapper, path *graph.Path) error {
	path.Nodes = make([]*graph.Node, len(s.Nodes))

	for idx, pgNode := range s.Nodes {
		dawgsNode := &graph.Node{}

		if err := pgNode.ToNode(ctx, kindMapper, dawgsNode); err != nil {
			return err
		}

		path.Nodes[idx] = dawgsNode
	}

	path.Edges = make([]*graph.Relationship, len(s.Edges))

	for idx, pgEdge := range s.Edges {
		dawgsRelationship := &graph.Relationship{}

		if err := pgEdge.ToRelationship(ctx, kindMapper, dawgsRelationship); err != nil {
			return err
		}

		path.Edges[idx] = dawgsRelationship
	}

	return nil
}
