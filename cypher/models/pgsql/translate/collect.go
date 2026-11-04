package translate

import "github.com/specterops/dawgs/cypher/models/pgsql"

func exposeCollectArrayOperands(scope *Scope, expression *pgsql.BinaryExpression) error {
	if expression.Operator != pgsql.OperatorAdd {
		return nil
	}
	operands := []*pgsql.Expression{&expression.LOperand, &expression.ROperand}
	types := [2]pgsql.DataType{}
	for i, operand := range operands {
		dataType, err := InferExpressionType(*operand)
		if err != nil {
			return err
		}
		types[i] = dataType
		if identifier, ok := unwrapParenthetical(*operand).(pgsql.Identifier); ok {
			if dataType, found := scope.LookupDataType(identifier); found {
				types[i] = dataType
			}
		}
	}
	if types[0].IsArrayType() && types[1].IsArrayType() && (types[0] == pgsql.JSONBArray || types[1] == pgsql.JSONBArray) {
		for i, operand := range operands {
			if _, ok := unwrapParenthetical(*operand).(pgsql.Identifier); ok {
				*operand = pgsql.NewTypeCast(pgsql.NewParenthetical(*operand), types[i])
			}
		}
	}
	return nil
}

// Property collections contain JSON values. Convert the other list element by
// element when concatenating with a native SQL array; casting the whole array
// would reinterpret strings as serialized JSON and lose their value types.
func rewriteCollectArrayConcatenation(expression *pgsql.BinaryExpression) error {
	if expression.Operator != pgsql.OperatorAdd {
		return nil
	}
	left, err := InferExpressionType(expression.LOperand)
	if err != nil {
		return err
	}
	right, err := InferExpressionType(expression.ROperand)
	if err != nil {
		return err
	}
	if !left.IsArrayType() || !right.IsArrayType() || (left != pgsql.JSONBArray && right != pgsql.JSONBArray) {
		return nil
	}
	for _, operand := range []struct {
		value    *pgsql.Expression
		dataType pgsql.DataType
	}{{&expression.LOperand, left}, {&expression.ROperand, right}} {
		if operand.dataType == pgsql.JSONBArray {
			continue
		}
		if literal, ok := unwrapParenthetical(*operand.value).(pgsql.ArrayLiteral); ok {
			values := make([]pgsql.Expression, len(literal.Values))
			for i, element := range literal.Values {
				value, err := arithmeticJSONOperand(element)
				if err != nil {
					return err
				}
				values[i] = value
			}
			*operand.value = pgsql.ArrayLiteral{Values: values, CastType: pgsql.JSONBArray}
			continue
		}
		value, err := arithmeticJSONOperand(pgsql.FunctionCall{Function: pgsql.FunctionUnnest,
			Parameters: []pgsql.Expression{*operand.value}, CastType: operand.dataType.ArrayBaseType()})
		if err != nil {
			return err
		}
		*operand.value = pgsql.NewTypeCast(pgsql.ArrayExpression{Expression: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{pgsql.AliasedExpression{Expression: value}},
		}}}, pgsql.JSONBArray)
	}
	return nil
}
