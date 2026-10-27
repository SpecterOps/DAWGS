package translate

import "github.com/specterops/dawgs/cypher/models/pgsql"

// requiresDynamicArithmetic keeps the native typed path for known operands and
// property/literal arithmetic. Two dynamic values need runtime dispatch; a JSONB
// result also needs dispatch when used in a subsequent arithmetic operation.
func requiresDynamicArithmetic(expression *pgsql.BinaryExpression) (bool, error) {
	if !expression.Operator.IsIn(pgsql.OperatorAdd, pgsql.OperatorSubtract, pgsql.OperatorMultiply, pgsql.OperatorDivide, "%", "^") {
		return false, nil
	}
	left := unwrapParenthetical(expression.LOperand)
	right := unwrapParenthetical(expression.ROperand)
	leftType, err := InferExpressionType(left)
	if err != nil {
		return false, err
	}
	rightType, err := InferExpressionType(right)
	if err != nil {
		return false, err
	}
	return leftType == pgsql.JSONB || rightType == pgsql.JSONB || (isPropertyLookup(left) && isPropertyLookup(right)), nil
}

// arithmeticJSONOperand preserves stored JSON types instead of extracting text.
func arithmeticJSONOperand(expression pgsql.Expression) (pgsql.Expression, error) {
	expression = concreteEmptyArray(unwrapParenthetical(expression))
	if literal, ok := expression.(pgsql.Literal); ok && literal.Null {
		return pgsql.NewTypeCast(literal, pgsql.JSONB), nil
	}
	if lookup, ok := expressionToPropertyLookupBinaryExpression(expression); ok {
		copy := *lookup
		copy.Operator = pgsql.OperatorJSONField
		return nullifyJSONPropertyLookup(&copy), nil
	}
	dataType, err := InferExpressionType(expression)
	if err != nil {
		return nil, err
	}
	if dataType == pgsql.JSONB {
		return expression, nil
	}
	if !dataType.IsKnown() {
		switch expression.(type) {
		case pgsql.Parameter, *pgsql.Parameter:
			// Untyped parameters represent null. A polymorphic to_jsonb call
			// cannot infer their SQL type without this explicit null type.
			return pgsql.NewTypeCast(expression, pgsql.JSONB), nil
		}
	}
	// A scope-bound scalar (for example an UNWIND column) can lack an AST
	// hint while still having a concrete SQL type. Let PostgreSQL resolve that
	// column type instead of emitting an invalid cast to unknown.
	var value pgsql.Expression = expression
	if dataType.IsKnown() {
		value = pgsql.NewTypeCast(expression, dataType)
	}
	if dataType.MatchesOneOf(pgsql.Float4, pgsql.Float8, pgsql.Numeric) {
		// to_jsonb(float8) can spell 2.0 as 2. Keep a fractional digit so the
		// next dynamic operation can distinguish float from integer division.
		value = pgsql.NewBinaryExpression(pgsql.NewTypeCast(pgsql.NewTypeCast(expression, pgsql.Text), pgsql.Numeric), pgsql.OperatorAdd, pgsql.NewLiteral(0.0, pgsql.Numeric))
	}
	return pgsql.FunctionCall{
		Function:   pgsql.FunctionToJSONB,
		Parameters: []pgsql.Expression{value},
		CastType:   pgsql.JSONB,
	}, nil
}

// rewriteArithmeticComparison compares a runtime arithmetic result with JSON
// values, retaining numeric equality/order without casting it to text.
func rewriteArithmeticComparison(expression *pgsql.BinaryExpression) (bool, error) {
	if !expression.Operator.IsIn(pgsql.OperatorEquals, pgsql.OperatorNotEquals, pgsql.OperatorCypherNotEquals,
		pgsql.OperatorGreaterThan, pgsql.OperatorGreaterThanOrEqualTo, pgsql.OperatorLessThan, pgsql.OperatorLessThanOrEqualTo) {
		return false, nil
	}
	// ANY is SQL comparison syntax, not an array value to serialize. Its
	// scalar side is already converted by typed collection membership.
	for _, operand := range []pgsql.Expression{expression.LOperand, expression.ROperand} {
		switch unwrapParenthetical(operand).(type) {
		case pgsql.AnyExpression, *pgsql.AnyExpression:
			return false, nil
		}
	}
	isDynamicJSON := func(operand pgsql.Expression) bool {
		dataType, err := InferExpressionType(unwrapParenthetical(operand))
		return err == nil && dataType == pgsql.JSONB
	}
	if !isDynamicJSON(expression.LOperand) && !isDynamicJSON(expression.ROperand) {
		return false, nil
	}
	left, err := arithmeticJSONOperand(expression.LOperand)
	if err != nil {
		return false, err
	}
	right, err := arithmeticJSONOperand(expression.ROperand)
	if err != nil {
		return false, err
	}
	expression.LOperand, expression.ROperand = left, right
	return true, nil
}
