package pgd

import "github.com/specterops/dawgs/cypher/models/pgsql"

// ExpressionArrayLiteral wraps the expression slice as an array literal without copying its elements.
func ExpressionArrayLiteral(values ...pgsql.Expression) pgsql.ArrayLiteral {
	return pgsql.ArrayLiteral{
		Values: values,
	}
}

// IntLiteral selects the PostgreSQL integer type corresponding to the Go integer width.
func IntLiteral[T int | int16 | int32 | int64](literal T) pgsql.Literal {
	var dataType = pgsql.UnknownDataType

	switch any(literal).(type) {
	case int:
		dataType = pgsql.Int
	case int16:
		dataType = pgsql.Int2
	case int32:
		dataType = pgsql.Int4
	case int64:
		dataType = pgsql.Int8
	}

	return pgsql.NewLiteral(literal, dataType)
}

// TextLiteral wraps literal as a PostgreSQL text literal for the renderer to quote.
func TextLiteral(literal string) pgsql.Literal {
	return pgsql.NewLiteral(literal, pgsql.Text)
}
