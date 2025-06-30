package v2

import (
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/specterops/dawgs/graph"
)

func asTime(value any) (time.Time, bool) {
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

func newNode(internalNode neo4j.Node) *graph.Node {
	var propertiesInst = internalNode.Props

	if propertiesInst == nil {
		propertiesInst = make(map[string]any)
	}

	return graph.NewNode(graph.ID(internalNode.Id), graph.AsProperties(propertiesInst), graph.StringsToKinds(internalNode.Labels)...)
}

func newRelationship(internalRelationship neo4j.Relationship) *graph.Relationship {
	propertiesInst := internalRelationship.Props

	if propertiesInst == nil {
		propertiesInst = make(map[string]any)
	}

	return graph.NewRelationship(
		graph.ID(internalRelationship.Id),
		graph.ID(internalRelationship.StartId),
		graph.ID(internalRelationship.EndId),
		graph.AsProperties(propertiesInst),
		graph.StringKind(internalRelationship.Type),
	)
}

func newPath(internalPath neo4j.Path) graph.Path {
	path := graph.Path{}

	for _, node := range internalPath.Nodes {
		path.Nodes = append(path.Nodes, newNode(node))
	}

	for _, relationship := range internalPath.Relationships {
		path.Edges = append(path.Edges, newRelationship(relationship))
	}

	return path
}

func mapValue(rawValue, target any) bool {
	switch typedTarget := target.(type) {
	case *time.Time:
		if value, typeOK := asTime(rawValue); typeOK {
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

	case *graph.Path:
		if value, typeOK := rawValue.(dbtype.Path); typeOK {
			*typedTarget = newPath(value)
			return true
		}
	}

	return false
}

func newValueMapper() graph.ValueMapper {
	return graph.NewValueMapper(mapValue)
}
