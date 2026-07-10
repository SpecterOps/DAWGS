package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func pathCompositeEdgesExpression(scope *Scope, pathBinding *BoundIdentifier) (pgsql.Expression, error) {
	var edgeArrayReferences []pgsql.Expression

	for _, dependency := range pathBinding.Dependencies {
		switch dependency.DataType {
		case pgsql.ExpansionPath:
			if edgeArrayReference, err := expansionPathEdgeArrayExpression(scope, dependency); err != nil {
				return nil, err
			} else {
				edgeArrayReferences = append(edgeArrayReferences, edgeArrayReference)
			}

		case pgsql.EdgeComposite:
			edgeReference := bindingFrameReference(scope, dependency)
			edgeArrayReferences = append(edgeArrayReferences, pgsql.ArrayLiteral{
				Values:   []pgsql.Expression{edgeReference},
				CastType: pgsql.EdgeCompositeArray,
			})

		case pgsql.PathEdge:
			edgeArrayReferences = append(edgeArrayReferences, pathEdgeArrayExpression(scope, dependency))

		default:
			// Path bindings also depend on their node endpoints. Those are not part of relationships(p).
		}
	}

	if edgeArrayExpression := concatenatePathCompositeParts(edgeArrayReferences); edgeArrayExpression != nil {
		return edgeArrayExpression, nil
	}

	return pgsql.ArrayLiteral{CastType: pgsql.EdgeCompositeArray}, nil
}

func pathCompositeEdgeIDArrayExpression(scope *Scope, pathBinding *BoundIdentifier) (pgsql.Expression, error) {
	var edgeIDArrayReferences []pgsql.Expression

	for _, dependency := range pathBinding.Dependencies {
		switch dependency.DataType {
		case pgsql.ExpansionPath:
			edgeIDArrayReferences = append(edgeIDArrayReferences, pathBindingReference(scope, dependency))

		case pgsql.EdgeComposite:
			edgeIDArrayReferences = append(edgeIDArrayReferences, pgsql.ArrayLiteral{
				Values: []pgsql.Expression{
					pathCompositeColumnReference(scope, dependency, pgsql.ColumnID),
				},
				CastType: pgsql.Int8Array,
			})

		case pgsql.PathEdge:
			edgeIDArrayReferences = append(edgeIDArrayReferences, pgsql.ArrayLiteral{
				Values: []pgsql.Expression{
					pathEdgeIDReference(scope, dependency),
				},
				CastType: pgsql.Int8Array,
			})

		default:
			// Path bindings also depend on their node endpoints. Those are not part of relationships(p).
		}
	}

	if edgeIDArrayExpression := concatenatePathCompositeParts(edgeIDArrayReferences); edgeIDArrayExpression != nil {
		return edgeIDArrayExpression, nil
	}

	return pgsql.ArrayLiteral{
		CastType: pgsql.Int8Array,
	}, nil
}

func (s *Translator) buildPathEdgeIDArrayFutures() error {
	for _, future := range s.query.CurrentPart().pathEdgeIDArrayFutures {
		if edgeIDArrayExpression, err := pathCompositeEdgeIDArrayExpression(s.scope, future.Data); err != nil {
			return err
		} else {
			future.SyntaxNode = edgeIDArrayExpression
		}
	}

	return nil
}

func resolvePathCompositeFieldReference(scope *Scope, reference pgsql.RowColumnReference) (pgsql.Expression, bool, error) {
	identifier, isIdentifier := unwrapParenthetical(reference.Identifier).(pgsql.Identifier)
	if !isIdentifier {
		return nil, false, nil
	}

	binding, bound := scope.Lookup(identifier)
	if !bound {
		binding, bound = scope.AliasedLookup(identifier)
	}
	if !bound || binding.DataType != pgsql.PathComposite {
		return nil, false, nil
	}

	if binding.LastProjection != nil {
		return pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{binding.LastProjection.Binding.Identifier, binding.Identifier},
			Column:     reference.Column,
		}, true, nil
	}

	switch reference.Column {
	case pgsql.ColumnEdges:
		expression, err := pathCompositeEdgesExpression(scope, binding)
		return expression, true, err
	case pgsql.ColumnNodes:
		if expression, err := expressionForPathComposite(binding, scope); err != nil {
			return nil, false, err
		} else {
			return pgsql.RowColumnReference{
				Identifier: expression,
				Column:     reference.Column,
			}, true, nil
		}
	default:
		return nil, false, fmt.Errorf("unsupported path composite field reference: %s", reference.Column)
	}
}

func resolvePathCompositeFieldReferencesInProjection(scope *Scope, projection pgsql.Projection) (pgsql.Projection, error) {
	rewritten := make(pgsql.Projection, len(projection))

	for idx, item := range projection {
		expression, isExpression := item.(pgsql.Expression)
		if !isExpression {
			rewritten[idx] = item
			continue
		}

		resolved, err := resolvePathCompositeFieldReferences(scope, expression)
		if err != nil {
			return nil, err
		}

		selectItem, isSelectItem := resolved.(pgsql.SelectItem)
		if !isSelectItem {
			return nil, fmt.Errorf("resolved projection item is not selectable: %T", resolved)
		}

		rewritten[idx] = selectItem
	}

	return rewritten, nil
}

func resolvePathCompositeFieldReferencesInFromClause(scope *Scope, fromClause pgsql.FromClause) (pgsql.FromClause, error) {
	if resolvedSource, err := resolvePathCompositeFieldReferences(scope, fromClause.Source); err != nil {
		return pgsql.FromClause{}, err
	} else {
		fromClause.Source = resolvedSource
	}

	for idx, join := range fromClause.Joins {
		if resolvedTable, err := resolvePathCompositeFieldReferences(scope, join.Table); err != nil {
			return pgsql.FromClause{}, err
		} else {
			join.Table = resolvedTable
		}

		if join.JoinOperator.Constraint != nil {
			if resolvedConstraint, err := resolvePathCompositeFieldReferences(scope, join.JoinOperator.Constraint); err != nil {
				return pgsql.FromClause{}, err
			} else {
				join.JoinOperator.Constraint = resolvedConstraint
			}
		}

		fromClause.Joins[idx] = join
	}

	return fromClause, nil
}

func resolvePathCompositeFieldReferencesInFromClauses(scope *Scope, fromClauses []pgsql.FromClause) ([]pgsql.FromClause, error) {
	rewritten := make([]pgsql.FromClause, len(fromClauses))

	for idx, fromClause := range fromClauses {
		resolved, err := resolvePathCompositeFieldReferencesInFromClause(scope, fromClause)
		if err != nil {
			return nil, err
		}

		rewritten[idx] = resolved
	}

	return rewritten, nil
}

func resolvePathCompositeFieldReferences(scope *Scope, expression pgsql.Expression) (pgsql.Expression, error) {
	switch typedExpression := expression.(type) {
	case nil:
		return nil, nil

	case pgsql.Identifier:
		if binding, bound := scope.Lookup(typedExpression); !bound {
			if aliasedBinding, aliasBound := scope.AliasedLookup(typedExpression); aliasBound {
				binding = aliasedBinding
				bound = true
			}

			if !bound || binding.DataType != pgsql.PathComposite {
				return expression, nil
			}

			return expressionForPathComposite(binding, scope)
		} else if binding.DataType == pgsql.PathComposite {
			return expressionForPathComposite(binding, scope)
		}

		return expression, nil

	case pgsql.RowColumnReference:
		if resolved, rewritten, err := resolvePathCompositeFieldReference(scope, typedExpression); rewritten || err != nil {
			return resolved, err
		}

		if resolvedIdentifier, err := resolvePathCompositeFieldReferences(scope, typedExpression.Identifier); err != nil {
			return nil, err
		} else {
			typedExpression.Identifier = resolvedIdentifier
		}

		return typedExpression, nil

	case pgsql.TypeCast:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case pgsql.FunctionCall:
		for idx, parameter := range typedExpression.Parameters {
			if resolved, err := resolvePathCompositeFieldReferences(scope, parameter); err != nil {
				return nil, err
			} else {
				typedExpression.Parameters[idx] = resolved
			}
		}

		return typedExpression, nil

	case *pgsql.FunctionCall:
		if typedExpression == nil {
			return nil, nil
		}

		resolved, err := resolvePathCompositeFieldReferences(scope, *typedExpression)
		if err != nil {
			return nil, err
		}

		functionCall := resolved.(pgsql.FunctionCall)
		return &functionCall, nil

	case pgsql.AliasedExpression:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case *pgsql.AliasedExpression:
		if typedExpression == nil {
			return nil, nil
		}

		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case pgsql.ArrayExpression:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case pgsql.ArraySlice:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
		}

		if typedExpression.Lower != nil {
			if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Lower); err != nil {
				return nil, err
			} else {
				typedExpression.Lower = resolved
			}
		}

		if typedExpression.Upper != nil {
			if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Upper); err != nil {
				return nil, err
			} else {
				typedExpression.Upper = resolved
			}
		}

		return typedExpression, nil

	case *pgsql.ArraySlice:
		if typedExpression == nil {
			return nil, nil
		}

		resolved, err := resolvePathCompositeFieldReferences(scope, *typedExpression)
		if err != nil {
			return nil, err
		}

		arraySlice := resolved.(pgsql.ArraySlice)
		return &arraySlice, nil

	case pgsql.ArrayLiteral:
		for idx, value := range typedExpression.Values {
			if resolved, err := resolvePathCompositeFieldReferences(scope, value); err != nil {
				return nil, err
			} else {
				typedExpression.Values[idx] = resolved
			}
		}

		return typedExpression, nil

	case pgsql.ArrayIndex:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
		}

		for idx, index := range typedExpression.Indexes {
			if resolved, err := resolvePathCompositeFieldReferences(scope, index); err != nil {
				return nil, err
			} else {
				typedExpression.Indexes[idx] = resolved
			}
		}

		return typedExpression, nil

	case *pgsql.ArrayIndex:
		if typedExpression == nil {
			return nil, nil
		}

		resolved, err := resolvePathCompositeFieldReferences(scope, *typedExpression)
		if err != nil {
			return nil, err
		}

		arrayIndex := resolved.(pgsql.ArrayIndex)
		return &arrayIndex, nil

	case pgsql.AnyExpression:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case *pgsql.AnyExpression:
		if typedExpression == nil {
			return nil, nil
		}

		resolved, err := resolvePathCompositeFieldReferences(scope, *typedExpression)
		if err != nil {
			return nil, err
		}

		anyExpression := resolved.(pgsql.AnyExpression)
		return &anyExpression, nil

	case pgsql.AllExpression:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case *pgsql.UnaryExpression:
		if typedExpression == nil {
			return nil, nil
		}

		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Operand); err != nil {
			return nil, err
		} else {
			typedExpression.Operand = resolved
			return typedExpression, nil
		}

	case pgsql.UnaryExpression:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Operand); err != nil {
			return nil, err
		} else {
			typedExpression.Operand = resolved
			return typedExpression, nil
		}

	case *pgsql.BinaryExpression:
		if typedExpression == nil {
			return nil, nil
		}

		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.LOperand); err != nil {
			return nil, err
		} else {
			typedExpression.LOperand = resolved
		}

		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.ROperand); err != nil {
			return nil, err
		} else {
			typedExpression.ROperand = resolved
		}

		return typedExpression, nil

	case pgsql.BinaryExpression:
		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.LOperand); err != nil {
			return nil, err
		} else {
			typedExpression.LOperand = resolved
		}

		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.ROperand); err != nil {
			return nil, err
		} else {
			typedExpression.ROperand = resolved
		}

		return typedExpression, nil

	case *pgsql.Parenthetical:
		if typedExpression == nil {
			return nil, nil
		}

		if resolved, err := resolvePathCompositeFieldReferences(scope, typedExpression.Expression); err != nil {
			return nil, err
		} else {
			typedExpression.Expression = resolved
			return typedExpression, nil
		}

	case pgsql.Select:
		if resolvedProjection, err := resolvePathCompositeFieldReferencesInProjection(scope, typedExpression.Projection); err != nil {
			return nil, err
		} else {
			typedExpression.Projection = resolvedProjection
		}

		if resolvedFrom, err := resolvePathCompositeFieldReferencesInFromClauses(scope, typedExpression.From); err != nil {
			return nil, err
		} else {
			typedExpression.From = resolvedFrom
		}

		if resolvedWhere, err := resolvePathCompositeFieldReferences(scope, typedExpression.Where); err != nil {
			return nil, err
		} else {
			typedExpression.Where = resolvedWhere
		}

		for idx, groupBy := range typedExpression.GroupBy {
			if resolved, err := resolvePathCompositeFieldReferences(scope, groupBy); err != nil {
				return nil, err
			} else {
				typedExpression.GroupBy[idx] = resolved
			}
		}

		if resolvedHaving, err := resolvePathCompositeFieldReferences(scope, typedExpression.Having); err != nil {
			return nil, err
		} else {
			typedExpression.Having = resolvedHaving
		}

		return typedExpression, nil

	default:
		return expression, nil
	}
}
