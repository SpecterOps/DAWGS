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

	case *graph.Path:
		if value, typeOK := rawValue.(dbtype.Path); typeOK {
			*typedTarget = newPath(value)
			return true
		}
	}

	return false
}

func NewValueMapper() graph.ValueMapper {
	return graph.NewValueMapper(mapValue)
}
