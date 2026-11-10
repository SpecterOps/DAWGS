package pgd

import "github.com/specterops/dawgs/cypher/models/pgsql"

// Not builds SQL logical negation while retaining the operand expression.
func Not(operand pgsql.Expression) *pgsql.UnaryExpression {
	return pgsql.NewUnaryExpression(pgsql.OperatorNot, operand)
}

// And builds a SQL conjunction while retaining both operand expressions.
func And(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorAnd, rOperand)
}

// LessThan builds a strict SQL ordering comparison of the supplied operands.
func LessThan(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorLessThan, rOperand)
}

// LessThanOrEqualTo builds an inclusive SQL upper-bound comparison.
func LessThanOrEqualTo(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorLessThanOrEqualTo, rOperand)
}

// GreaterThan builds a strict SQL lower-bound comparison.
func GreaterThan(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorGreaterThan, rOperand)
}

// GreaterThanOrEqualTo builds an inclusive SQL lower-bound comparison.
func GreaterThanOrEqualTo(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorGreaterThanOrEqualTo, rOperand)
}

// Equals builds SQL equality, including PostgreSQL's null propagation.
func Equals(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorEquals, rOperand)
}

// Concatenate builds the SQL concatenation operator for the supplied operands.
func Concatenate(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorConcatenate, rOperand)
}

// EdgeHasKind compares the edge kind_id column with a typed kind identifier.
func EdgeHasKind(edge pgsql.Identifier, kindID int16) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(
		Column(edge, pgsql.ColumnKindID),
		pgsql.OperatorEquals,
		IntLiteral(kindID),
	)
}

// PropertyLookup builds a text-valued JSON property lookup on the owner's properties column.
func PropertyLookup(owner pgsql.Identifier, propertyName string) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(
		Properties(owner),
		pgsql.OperatorJSONTextField,
		pgsql.PropertyKey{Literal: TextLiteral(propertyName)},
	)
}

// Add builds SQL addition while retaining both operand expressions.
func Add(lOperand, rOperand pgsql.Expression) *pgsql.BinaryExpression {
	return pgsql.NewBinaryExpression(lOperand, pgsql.OperatorAdd, rOperand)
}
