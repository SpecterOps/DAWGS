package pgd

import (
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

// Any builds an ANY expression with the requested cast type and retained operand.
func Any(expr pgsql.Expression, castType pgsql.DataType) *pgsql.AnyExpression {
	return pgsql.NewAnyExpression(expr, castType)
}

// Column qualifies a column identifier with its relation or record identifier.
func Column(root, column pgsql.Identifier) pgsql.CompoundIdentifier {
	return pgsql.CompoundIdentifier{root, column}
}

// EntityID selects the entity identifier column of the supplied relation.
func EntityID(identifier pgsql.Identifier) pgsql.CompoundIdentifier {
	return Column(identifier, pgsql.ColumnID)
}

// StartID selects the relationship start-node identifier column.
func StartID(identifier pgsql.Identifier) pgsql.CompoundIdentifier {
	return Column(identifier, pgsql.ColumnStartID)
}

// EndID selects the relationship end-node identifier column.
func EndID(identifier pgsql.Identifier) pgsql.CompoundIdentifier {
	return Column(identifier, pgsql.ColumnEndID)
}

// Properties selects the JSON property column of the supplied entity.
func Properties(identifier pgsql.Identifier) pgsql.CompoundIdentifier {
	return Column(identifier, pgsql.ColumnProperties)
}
