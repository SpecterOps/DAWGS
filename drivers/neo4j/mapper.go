package neo4j

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/specterops/dawgs/graph"
)

func AsTime(value any) (time.Time, bool) {
	switch typedValue := value.(type) {
	case dbtype.Time:
		return typedValue.Time(), true

	case dbtype.LocalTime:
		return typedValue.Time(), true

	case dbtype.Date:
		return typedValue.Time(), true

	case dbtype.LocalDateTime:
		return typedValue.Time(), true

	default:
		return graph.AsTime(value)
	}
}

func mapNodeList(rawValue any) ([]*graph.Node, bool) {
	rawNodes, typeOK := rawValue.([]any)
	if !typeOK {
		return nil, false
	}

	nodes := make([]*graph.Node, len(rawNodes))
	for idx, rawNode := range rawNodes {
		node, typeOK := rawNode.(dbtype.Node)
		if !typeOK {
			return nil, false
		}

		nodes[idx] = newNode(node)
	}

	return nodes, true
}

func mapRelationshipList(rawValue any) ([]*graph.Relationship, bool) {
	rawRelationships, typeOK := rawValue.([]any)
	if !typeOK {
		return nil, false
	}

	relationships := make([]*graph.Relationship, len(rawRelationships))
	for idx, rawRelationship := range rawRelationships {
		relationship, typeOK := rawRelationship.(dbtype.Relationship)
		if !typeOK {
			return nil, false
		}

		relationships[idx] = newRelationship(relationship)
	}

	return relationships, true
}

func mapValue(rawValue, target any) bool {
	switch typedTarget := target.(type) {
	case *time.Time:
		if value, typeOK := AsTime(rawValue); typeOK {
			*typedTarget = value
			return true
		}

	case *dbtype.Relationship:
		if value, typeOK := rawValue.(dbtype.Relationship); typeOK {
			*typedTarget = value
			return true
		}

	case *graph.Relationship:
		if value, typeOK := rawValue.(dbtype.Relationship); typeOK {
			*typedTarget = *newRelationship(value)
			return true
		}

	case *dbtype.Node:
		if value, typeOK := rawValue.(dbtype.Node); typeOK {
			*typedTarget = value
			return true
		}

	case *graph.Node:
		if value, typeOK := rawValue.(dbtype.Node); typeOK {
			*typedTarget = *newNode(value)
			return true
		}

	case *[]*graph.Node:
		if nodes, typeOK := mapNodeList(rawValue); typeOK {
			*typedTarget = nodes
			return true
		}

	case *[]graph.Node:
		if nodes, typeOK := mapNodeList(rawValue); typeOK {
			nodeValues := make([]graph.Node, len(nodes))
			for idx, node := range nodes {
				if node != nil {
					nodeValues[idx] = *node
				}
			}

			*typedTarget = nodeValues
			return true
		}

	case *graph.Path:
		if value, typeOK := rawValue.(dbtype.Path); typeOK {
			*typedTarget = newPath(value)
			return true
		}

	case *[]*graph.Relationship:
		if relationships, typeOK := mapRelationshipList(rawValue); typeOK {
			*typedTarget = relationships
			return true
		}

	case *[]graph.Relationship:
		if relationships, typeOK := mapRelationshipList(rawValue); typeOK {
			relationshipValues := make([]graph.Relationship, len(relationships))
			for idx, relationship := range relationships {
				if relationship != nil {
					relationshipValues[idx] = *relationship
				}
			}

			*typedTarget = relationshipValues
			return true
		}
	}

	return false
}

func NewValueMapper() graph.ValueMapper {
	return graph.NewValueMapper(mapValue)
}
