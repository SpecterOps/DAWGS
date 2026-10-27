package translate

import "github.com/specterops/dawgs/cypher/models/pgsql"

func membershipOperandType(scope *Scope, expression pgsql.Expression) (pgsql.DataType, pgsql.DataType, error) {
	if identifier, ok := unwrapParenthetical(expression).(pgsql.Identifier); ok {
		if binding, found := scope.Lookup(identifier); found {
			return bindingExpressionType(binding), binding.collectedEntityType, nil
		}
	}
	dataType, err := InferExpressionType(expression)
	return dataType, pgsql.UnsetDataType, err
}

// rewriteEntityMembership compares identities only within the same Cypher entity
// type. An int8[] used for ID-only collection storage is not a list of numbers.
func rewriteEntityMembership(scope *Scope, expression *pgsql.BinaryExpression) (pgsql.Expression, bool, error) {
	leftType, _, err := membershipOperandType(scope, expression.LOperand)
	if err != nil {
		return nil, false, err
	}
	rightType, entityType, err := membershipOperandType(scope, expression.ROperand)
	if err != nil {
		return nil, false, err
	}
	if rightType == pgsql.NodeCompositeArray || rightType == pgsql.EdgeCompositeArray {
		entityType = rightType.ArrayBaseType()
	}
	leftEntity := leftType == pgsql.NodeComposite || leftType == pgsql.EdgeComposite
	if !rightType.IsArrayType() || (!leftEntity && !entityType.IsKnown()) {
		return nil, false, nil
	}
	if leftEntity && leftType == entityType && rightType != pgsql.Int8Array {
		// Direct aggregates already carry typed composites. Keep their native
		// comparison: moving an outer aggregate into the ID-extraction subquery
		// would change its grouping scope.
		if aggregated, err := ContainsAggregateFunction(expression); err != nil {
			return nil, false, err
		} else if aggregated {
			return nil, false, nil
		}
	}

	left := expression.LOperand
	if leftEntity {
		if identifier, ok := unwrapParenthetical(left).(pgsql.Identifier); ok {
			left = pgsql.CompoundIdentifier{identifier, pgsql.ColumnID}
		} else if leftType == entityType {
			left = pgsql.RowColumnReference{Identifier: left, Column: pgsql.ColumnID}
		}
	} else if leftType == pgsql.JSONB || isPropertyLookup(unwrapParenthetical(left)) {
		left, err = arithmeticJSONOperand(left)
		if err != nil {
			return nil, false, err
		}
	}
	right := expression.ROperand
	if leftEntity && leftType == entityType {
		if rightType != pgsql.Int8Array {
			const element pgsql.Identifier = "_membership_entity"
			right = pgsql.ArrayExpression{Expression: pgsql.Query{Body: pgsql.Select{
				Projection: pgsql.Projection{pgsql.RowColumnReference{Identifier: element, Column: pgsql.ColumnID}},
				From: []pgsql.FromClause{{Source: pgsql.AliasedExpression{
					Expression: pgsql.FunctionCall{Function: pgsql.FunctionUnnest, Parameters: []pgsql.Expression{right}},
					Alias:      pgsql.AsOptionalIdentifier(element),
				}}},
			}}}
		}
		membership := pgsql.NewBinaryExpression(left, pgsql.OperatorEquals, pgsql.NewAnyExpression(right, pgsql.Int8Array))
		if rightType != pgsql.Int8Array {
			return pgsql.Case{
				Conditions: []pgsql.Expression{pgsql.NewBinaryExpression(expression.ROperand, pgsql.OperatorIs, pgsql.NullLiteral())},
				Then:       []pgsql.Expression{pgsql.NewTypeCast(pgsql.NullLiteral(), pgsql.Boolean)},
				Else:       membership,
			}, true, nil
		}
		return membership, true, nil
	}

	// Different types cannot match. Preserve the nullable IN truth table,
	// including null IN [] = false and a null element in a nonmatching list.
	nullCondition := pgsql.NewBinaryExpression(
		pgsql.NewBinaryExpression(left, pgsql.OperatorIs, pgsql.NullLiteral()), pgsql.OperatorOr,
		pgsql.NewBinaryExpression(right, pgsql.OperatorIs, pgsql.NullLiteral()))
	if rightType != pgsql.Int8Array || !entityType.IsKnown() {
		// ID-only collect already removes nulls. Other arrays may contain them.
		nullCondition = pgsql.NewBinaryExpression(nullCondition, pgsql.OperatorOr,
			pgsql.NewBinaryExpression(pgsql.FunctionCall{Function: "array_position",
				Parameters: []pgsql.Expression{right, pgsql.NullLiteral()}}, pgsql.OperatorIsNot, pgsql.NullLiteral()))
	}
	return pgsql.Case{
		Conditions: []pgsql.Expression{
			pgsql.NewBinaryExpression(pgsql.FunctionCall{Function: pgsql.FunctionCardinality,
				Parameters: []pgsql.Expression{right}}, pgsql.OperatorEquals, pgsql.NewLiteral(0, pgsql.Int)),
			nullCondition,
		},
		Then: []pgsql.Expression{pgsql.NewLiteral(false, pgsql.Boolean), pgsql.NewTypeCast(pgsql.NullLiteral(), pgsql.Boolean)},
		Else: pgsql.NewLiteral(false, pgsql.Boolean),
	}, true, nil
}
