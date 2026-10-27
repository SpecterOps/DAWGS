package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func isOrderedComparison(operator pgsql.Operator) bool {
	return operator.IsIn(pgsql.OperatorLessThan, pgsql.OperatorLessThanOrEqualTo, pgsql.OperatorGreaterThan, pgsql.OperatorGreaterThanOrEqualTo)
}

func isStringPredicate(operator pgsql.Operator) bool {
	return operator.IsIn(pgsql.OperatorCypherStartsWith, pgsql.OperatorCypherEndsWith, pgsql.OperatorCypherContains, pgsql.OperatorCypherRegexMatch)
}

// rewriteTypedOrdering preserves the JSON value types of dynamic operands. SQL's
// JSONB total ordering is unsuitable: Cypher returns null across unlike types
// and compares lists lexicographically, including nullable element comparisons.
func rewriteTypedOrdering(expression *pgsql.BinaryExpression) (bool, error) {
	if !isOrderedComparison(expression.Operator) {
		return false, nil
	}
	dynamic := false
	for _, operand := range []pgsql.Expression{expression.LOperand, expression.ROperand} {
		dataType, err := InferExpressionType(unwrapParenthetical(operand))
		if err != nil {
			return false, err
		}
		// Temporal values have an existing native representation and cast path.
		if dataType.MatchesOneOf(pgsql.Date, pgsql.TimestampWithTimeZone, pgsql.TimestampWithoutTimeZone, pgsql.TimeWithTimeZone, pgsql.TimeWithoutTimeZone, pgsql.Interval) {
			return false, nil
		}
		dynamic = dynamic || isPropertyLookup(unwrapParenthetical(operand)) || dataType == pgsql.JSONB
	}
	if !dynamic {
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
	expression.LOperand = pgsql.FunctionCall{
		Function: pgsql.FunctionCypherCompare, Parameters: []pgsql.Expression{left, right}, CastType: pgsql.Int4,
	}
	expression.ROperand = pgsql.NewLiteral(0, pgsql.Int4)
	return true, nil
}

// stringPredicateOperand returns a text operand and, for dynamic values, its
// string-type condition. Direct properties retain bare ->> extraction so LIKE
// predicates remain eligible for existing text indexes.
func stringPredicateOperand(operand pgsql.Expression) (pgsql.Expression, pgsql.Expression, error) {
	operand = unwrapParenthetical(operand)
	if property, ok := expressionToPropertyLookupBinaryExpression(operand); ok {
		copy := *property
		copy.Operator = pgsql.OperatorJSONTextField
		return &copy, jsonbStringTypeCheck(property), nil
	}
	dataType, err := InferExpressionType(operand)
	if err != nil {
		return nil, nil, err
	}
	if dataType == pgsql.JSONB || !dataType.IsKnown() {
		if dataType != pgsql.JSONB {
			// UNWIND columns can lack a scope hint. Their concrete SQL type
			// still lets to_jsonb retain the value's runtime type.
			operand, err = arithmeticJSONOperand(operand)
			if err != nil {
				return nil, nil, err
			}
		}
		return pgsql.NewBinaryExpression(operand, "#>>", pgsql.ArrayLiteral{CastType: pgsql.TextArray}),
			pgsql.NewBinaryExpression(jsonbTypeof(operand), pgsql.OperatorEquals, pgsql.NewLiteral("string", pgsql.Text)), nil
	}
	if dataType.IsKnown() && dataType != pgsql.Text {
		// Non-string operands, including null, yield null rather than their text form.
		return pgsql.NewTypeCast(pgsql.NullLiteral(), pgsql.Text), nil, nil
	}
	return operand, nil, nil
}

// nullableStringPredicate retains null for every non-string value, even when the
// textual predicate would be false. In a positive WHERE PostgreSQL simplifies
// the null branch away and can still use the bare text LIKE predicate as an index
// condition. CASE around the whole predicate would hide that condition.
func nullableStringPredicate(predicate, condition pgsql.Expression) pgsql.Expression {
	if condition == nil {
		return predicate
	}
	return pgsql.NewParenthetical(pgsql.NewBinaryExpression(
		pgsql.NewParenthetical(pgsql.NewBinaryExpression(condition, pgsql.OperatorAnd, predicate)),
		pgsql.OperatorOr,
		pgsql.NewParenthetical(pgsql.NewBinaryExpression(
			pgsql.UnaryExpression{Operator: pgsql.OperatorNot, Operand: pgsql.NewParenthetical(condition)},
			pgsql.OperatorAnd, pgsql.NullLiteral(),
		)),
	))
}

func typedStringPredicate(expression *pgsql.BinaryExpression) (pgsql.Expression, error) {
	left, leftCondition, err := stringPredicateOperand(expression.LOperand)
	if err != nil {
		return nil, err
	}
	right, rightCondition, err := stringPredicateOperand(expression.ROperand)
	if err != nil {
		return nil, err
	}
	// Retain the current literal escaping contract. Functional-left literal
	// escaping and regex matching scope are separate migration work items.
	if expression.Operator != pgsql.OperatorCypherRegexMatch && isPropertyLookup(unwrapParenthetical(expression.LOperand)) {
		if literal, ok := right.(pgsql.Literal); ok && !literal.Null {
			right, err = rewriteStringWildCardLiteral(right)
			if err != nil {
				return nil, err
			}
		}
	}
	var predicate pgsql.Expression
	if expression.Operator == pgsql.OperatorCypherRegexMatch {
		predicate = pgsql.NewBinaryExpression(left, pgsql.OperatorRegexMatch, right)
	} else {
		function := pgsql.FunctionCypherContains
		switch expression.Operator {
		case pgsql.OperatorCypherStartsWith:
			function = pgsql.FunctionCypherStartsWith
		case pgsql.OperatorCypherEndsWith:
			function = pgsql.FunctionCypherEndsWith
		}
		if literal, ok := right.(pgsql.Literal); ok && literal.TypeHint() == pgsql.Text && !literal.Null {
			text, ok := literal.Value.(string)
			if !ok {
				return nil, fmt.Errorf("expected a string literal but received type: %T", literal.Value)
			}
			switch function {
			case pgsql.FunctionCypherStartsWith:
				text += "%"
			case pgsql.FunctionCypherEndsWith:
				text = "%" + text
			default:
				text = "%" + text + "%"
			}
			pattern := pgsql.NewLiteral(text, pgsql.Text)
			predicate = caseInsensitiveLikeExpression(pgsql.NewBinaryExpression(left, pgsql.OperatorLike, pattern), pattern)
		} else {
			predicate, err = cypherStringPredicateExpression(function, left, right)
			if err != nil {
				return nil, err
			}
		}
	}
	return nullableStringPredicate(nullableStringPredicate(predicate, leftCondition), rightCondition), nil
}
