package pg

import (
	"context"
	"fmt"

	"github.com/specterops/dawgs/graph"
)

// edgeComposite is the ordered Go representation of PostgreSQL's edge composite type.
type edgeComposite struct {
	// ID is the database identifier of the decoded relationship.
	ID int64

	// StartID is the database identifier of the relationship's start node.
	StartID int64

	// EndID is the database identifier of the relationship's end node.
	EndID int64

	// KindID is the PostgreSQL int2 identifier of the relationship kind.
	KindID int16

	// Properties contains the relationship's decoded JSON property values.
	Properties map[string]any
}

// ScanNull rejects a null edge because the owned scalar representation has no null state.
func (s *edgeComposite) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into %T", s)
}

// ScanIndex returns the destination for a PostgreSQL edge field in schema order.
func (s *edgeComposite) ScanIndex(index int) any {
	switch index {
	case 0:
		return &s.ID
	case 1:
		return &s.StartID
	case 2:
		return &s.EndID
	case 3:
		return &s.KindID
	case 4:
		return &s.Properties
	default:
		return fmt.Errorf("%T only has 5 fields: index %d is out of bounds", s, index)
	}
}

// castSlice copies either a typed slice or a pgx []any representation into []T.
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

// castMapValueAsSliceOf retrieves key from a fallback composite map and converts its value to []T.
func castMapValueAsSliceOf[T any](compositeMap map[string]any, key string) ([]T, error) {
	if src, hasKey := compositeMap[key]; !hasKey {
		return nil, fmt.Errorf("composite map does not contain expected key %s", key)
	} else {
		return castSlice[T](src)
	}
}

// castAndAssignMapValue assigns a fallback composite-map field to dst, allowing lossless widening of integer values.
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

// nodeCompositesFromRaw converts typed or pgx fallback arrays into owned node composites.
func nodeCompositesFromRaw(raw any) ([]nodeComposite, error) {
	switch rawNodes := raw.(type) {
	case []nodeComposite:
		return rawNodes, nil
	case []any:
		nodes := make([]nodeComposite, len(rawNodes))
		for idx, rawNode := range rawNodes {
			if node, typeOK := nodeCompositeFromRaw(rawNode); !typeOK {
				return nil, fmt.Errorf("unexpected type for raw node at index %d: %T", idx, rawNode)
			} else {
				nodes[idx] = node
			}
		}

		return nodes, nil
	default:
		return nil, fmt.Errorf("expected raw node composite array type []nodeComposite or []any but received %T", raw)
	}
}

// edgeCompositesFromRaw converts typed or pgx fallback arrays into owned edge composites.
func edgeCompositesFromRaw(raw any) ([]edgeComposite, error) {
	switch rawEdges := raw.(type) {
	case []edgeComposite:
		return rawEdges, nil
	case []any:
		edges := make([]edgeComposite, len(rawEdges))
		for idx, rawEdge := range rawEdges {
			if edge, typeOK := edgeCompositeFromRaw(rawEdge); !typeOK {
				return nil, fmt.Errorf("unexpected type for raw edge at index %d: %T", idx, rawEdge)
			} else {
				edges[idx] = edge
			}
		}

		return edges, nil
	default:
		return nil, fmt.Errorf("expected raw edge composite array type []edgeComposite or []any but received %T", raw)
	}
}

// edgeCompositeFromRaw accepts an owned edge value, pointer, or pgx fallback map.
func edgeCompositeFromRaw(raw any) (edgeComposite, bool) {
	switch typedRaw := raw.(type) {
	case edgeComposite:
		return typedRaw, true
	case *edgeComposite:
		if typedRaw != nil {
			return *typedRaw, true
		}
	case map[string]any:
		var edge edgeComposite
		if edge.TryMap(typedRaw) {
			return edge, true
		}
	}

	return edgeComposite{}, false
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

// nodeComposite is the ordered Go representation of PostgreSQL's node composite type.
type nodeComposite struct {
	// ID is the database identifier of the decoded node.
	ID int64

	// KindIDs contains the PostgreSQL int2 identifiers of the node's kinds.
	KindIDs []int16

	// Properties contains the node's decoded JSON property values.
	Properties map[string]any
}

// ScanNull rejects a null node because the owned scalar representation has no null state.
func (s *nodeComposite) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into %T", s)
}

// ScanIndex returns the destination for a PostgreSQL node field in schema order.
func (s *nodeComposite) ScanIndex(index int) any {
	switch index {
	case 0:
		return &s.ID
	case 1:
		return &s.KindIDs
	case 2:
		return &s.Properties
	default:
		return fmt.Errorf("%T only has 3 fields: index %d is out of bounds", s, index)
	}
}

// nodeCompositeFromRaw accepts an owned node value, pointer, or pgx fallback map.
func nodeCompositeFromRaw(raw any) (nodeComposite, bool) {
	switch typedRaw := raw.(type) {
	case nodeComposite:
		return typedRaw, true
	case *nodeComposite:
		if typedRaw != nil {
			return *typedRaw, true
		}
	case map[string]any:
		var node nodeComposite
		if node.TryMap(typedRaw) {
			return node, true
		}
	}

	return nodeComposite{}, false
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

// pathComposite is the ordered Go representation of PostgreSQL's path composite type.
type pathComposite struct {
	// Nodes contains the path's decoded nodes in traversal order.
	Nodes []nodeComposite

	// Edges contains the path's decoded relationships in traversal order.
	Edges []edgeComposite
}

// ScanNull rejects a null path because the owned scalar representation has no null state.
func (s *pathComposite) ScanNull() error {
	return fmt.Errorf("cannot scan NULL into %T", s)
}

// ScanIndex returns the destination for a PostgreSQL path field in schema order.
func (s *pathComposite) ScanIndex(index int) any {
	switch index {
	case 0:
		return &s.Nodes
	case 1:
		return &s.Edges
	default:
		return fmt.Errorf("%T only has 2 fields: index %d is out of bounds", s, index)
	}
}

// pathCompositeFromRaw accepts an owned path value, pointer, or pgx fallback map.
func pathCompositeFromRaw(raw any) (pathComposite, bool) {
	switch typedRaw := raw.(type) {
	case pathComposite:
		return typedRaw, true
	case *pathComposite:
		if typedRaw != nil {
			return *typedRaw, true
		}
	case map[string]any:
		var path pathComposite
		if path.TryMap(typedRaw) {
			return path, true
		}
	}

	return pathComposite{}, false
}

func (s *pathComposite) TryMap(compositeMap map[string]any) bool {
	return s.FromMap(compositeMap) == nil
}

// FromMap populates a path composite from pgx's fallback map representation of its node and edge arrays.
func (s *pathComposite) FromMap(compositeMap map[string]any) error {
	if rawNodes, hasNodes := compositeMap["nodes"]; hasNodes {
		if nodes, err := nodeCompositesFromRaw(rawNodes); err != nil {
			return err
		} else {
			s.Nodes = nodes
		}
	}

	if rawEdges, hasEdges := compositeMap["edges"]; hasEdges {
		if edges, err := edgeCompositesFromRaw(rawEdges); err != nil {
			return err
		} else {
			s.Edges = edges
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
