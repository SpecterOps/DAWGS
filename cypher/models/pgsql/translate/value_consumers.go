package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
)

// functionJSONArgument recognizes values whose runtime type must survive until
// the consumer. Direct properties and materialized WITH aliases share this path.
func (s *Translator) functionJSONArgument(argument pgsql.Expression) (pgsql.Expression, bool, error) {
	if isPropertyLookup(unwrapParenthetical(argument)) {
		value, err := arithmeticJSONOperand(argument)
		return value, true, err
	}
	dataType, err := s.inferExpressionType(argument)
	return argument, dataType == pgsql.JSONB, err
}

// functionTextArgument extracts the scalar value rather than JSON's serialization
// of it. The helper distinguishes explicit toString conversion from functions
// that require a string, and rejects unsupported runtime types.
func (s *Translator) functionTextArgument(argument pgsql.Expression, function string) (pgsql.Expression, error) {
	// Keep the established direct-property text path visible to functional
	// indexes. Carried values need runtime extraction; these lookups already
	// have a native text representation at their consumer.
	if function != "toString" {
		if property, ok := expressionToPropertyLookupBinaryExpression(argument); ok {
			copy := *property
			copy.Operator = pgsql.OperatorJSONTextField
			return &copy, nil
		}
	}
	value, dynamic, err := s.functionJSONArgument(argument)
	if err != nil {
		return nil, err
	}
	if dynamic {
		return pgsql.FunctionCall{
			Function:   pgsql.FunctionCypherText,
			Parameters: []pgsql.Expression{value, pgsql.NewLiteral(function, pgsql.Text)},
			CastType:   pgsql.Text,
		}, nil
	}
	dataType, err := s.inferExpressionType(argument)
	if err != nil {
		return nil, err
	}
	if function == "toString" && dataType.IsArrayType() {
		return nil, fmt.Errorf("toString does not support a list value")
	}
	return argument, nil
}

// scalarCastArgument unwraps a carried JSON scalar before native conversions.
// Conversion policy (for example toInteger rounding) remains with that consumer.
func (s *Translator) scalarCastArgument(argument pgsql.Expression) (pgsql.Expression, error) {
	value, dynamic, err := s.functionJSONArgument(argument)
	if err != nil {
		return nil, err
	}
	if dynamic {
		return pgsql.NewParenthetical(pgsql.NewBinaryExpression(value, "#>>", pgsql.ArrayLiteral{CastType: pgsql.TextArray})), nil
	}
	return argument, nil
}

// jsonBooleanOperand preserves SQL null semantics and rejects non-Boolean JSON.
func jsonBooleanOperand(argument pgsql.Expression) pgsql.Expression {
	return pgsql.NewTypeCast(pgsql.FunctionCall{Function: pgsql.FunctionNullIf,
		Parameters: []pgsql.Expression{argument, jsonNullLiteral()}, CastType: pgsql.JSONB}, pgsql.Boolean)
}

func (s *Translator) booleanArgument(argument pgsql.Expression) (pgsql.Expression, error) {
	dataType, err := s.inferExpressionType(argument)
	if err != nil {
		return nil, err
	}
	if dataType == pgsql.JSONB {
		return jsonBooleanOperand(argument), nil
	}
	return argument, nil
}
