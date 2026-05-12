package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/walk"
)

func rewriteCompositeTypeFieldReference(scopeIdentifier pgsql.Identifier, compositeReference pgsql.CompoundIdentifier) pgsql.RowColumnReference {
	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{scopeIdentifier, compositeReference.Root()},
		Column:     compositeReference[1],
	}
}

func rewriteIdentifierScopeReference(scope *Scope, identifier pgsql.Identifier) (pgsql.SelectItem, error) {
	if !pgsql.IsReservedIdentifier(identifier) {
		if binding, bound := scope.Lookup(identifier); bound {
			if binding.LastProjection != nil {
				return pgsql.CompoundIdentifier{binding.LastProjection.Binding.Identifier, identifier}, nil
			}
		}
	}

	// Return the original identifier if no rewrite is needed
	return identifier, nil
}

func rewriteCompoundIdentifierScopeReference(scope *Scope, identifier pgsql.CompoundIdentifier) (pgsql.SelectItem, error) {
	if binding, bound := scope.Lookup(identifier[0]); bound {
		if binding.LastProjection != nil {
			return pgsql.RowColumnReference{
				Identifier: pgsql.CompoundIdentifier{binding.LastProjection.Binding.Identifier, binding.Identifier},
				Column:     identifier[1],
			}, nil
		}
	}

	// Return the original identifier if no rewrite is needed
	return identifier, nil
}

func rewriteExpressionScopeReference(scope *Scope, expression pgsql.Expression) (pgsql.Expression, bool, error) {
	switch typedExpression := expression.(type) {
	case pgsql.Identifier:
		rewritten, err := rewriteIdentifierScopeReference(scope, typedExpression)
		return rewritten, true, err

	case pgsql.CompoundIdentifier:
		rewritten, err := rewriteCompoundIdentifierScopeReference(scope, typedExpression)
		return rewritten, true, err

	default:
		return expression, false, nil
	}
}

type FrameBindingRewriter struct {
	walk.Visitor[pgsql.SyntaxNode]

	scope *Scope
}

func (s *FrameBindingRewriter) rewriteArraySlice(slice *pgsql.ArraySlice) error {
	if slice == nil {
		return nil
	}

	if rewritten, didRewrite, err := rewriteExpressionScopeReference(s.scope, slice.Expression); err != nil {
		return err
	} else if didRewrite {
		slice.Expression = rewritten
	}

	if slice.Lower != nil {
		if rewritten, didRewrite, err := rewriteExpressionScopeReference(s.scope, slice.Lower); err != nil {
			return err
		} else if didRewrite {
			slice.Lower = rewritten
		}
	}

	if slice.Upper != nil {
		if rewritten, didRewrite, err := rewriteExpressionScopeReference(s.scope, slice.Upper); err != nil {
			return err
		} else if didRewrite {
			slice.Upper = rewritten
		}
	}

	return nil
}

func (s *FrameBindingRewriter) enter(node pgsql.SyntaxNode) error {
	switch typedExpression := node.(type) {
	case pgsql.Projection:
		for idx, projection := range typedExpression {
			switch typedProjection := projection.(type) {
			case pgsql.Identifier:
				if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedProjection); err != nil {
					return err
				} else {
					typedExpression[idx] = rewritten
				}

			case pgsql.CompoundIdentifier:
				if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedProjection); err != nil {
					return err
				} else {
					typedExpression[idx] = rewritten
				}

			case pgsql.ArraySlice:
				if err := s.rewriteArraySlice(&typedProjection); err != nil {
					return err
				}
				typedExpression[idx] = typedProjection

			case *pgsql.ArraySlice:
				if err := s.rewriteArraySlice(typedProjection); err != nil {
					return err
				}
			}
		}

	case pgsql.CompositeValue:
		for idx, value := range typedExpression.Values {
			switch typedValue := value.(type) {
			case pgsql.Identifier:
				if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedValue); err != nil {
					return err
				} else {
					typedExpression.Values[idx] = rewritten
				}

			case pgsql.CompoundIdentifier:
				if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedValue); err != nil {
					return err
				} else {
					typedExpression.Values[idx] = rewritten
				}

			case pgsql.ArraySlice:
				if err := s.rewriteArraySlice(&typedValue); err != nil {
					return err
				}
				typedExpression.Values[idx] = typedValue

			case *pgsql.ArraySlice:
				if err := s.rewriteArraySlice(typedValue); err != nil {
					return err
				}
			}
		}

	case pgsql.FunctionCall:
		for idx, parameter := range typedExpression.Parameters {
			switch typedParameter := parameter.(type) {
			case pgsql.Identifier:
				if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedParameter); err != nil {
					return err
				} else {
					// This is being done in case the top-level parameter is a value-type
					typedExpression.Parameters[idx] = rewritten
				}

			case pgsql.CompoundIdentifier:
				if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedParameter); err != nil {
					return err
				} else {
					// This is being done in case the top-level parameter is a value-type
					typedExpression.Parameters[idx] = rewritten
				}
			// quantifier
			case pgsql.BinaryExpression:
				switch typedLOperand := typedParameter.LOperand.(type) {
				case pgsql.Identifier:
					if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedLOperand); err != nil {
						return err
					} else {
						typedParameter.LOperand = rewritten
					}
				case pgsql.CompoundIdentifier:
					if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedLOperand); err != nil {
						return err
					} else {
						typedParameter.LOperand = rewritten
					}
				default:
					return fmt.Errorf("unknown quantifier loperand expression: %v", typedLOperand)
				}
				typedExpression.Parameters[idx] = typedParameter

			case pgsql.ArraySlice:
				if err := s.rewriteArraySlice(&typedParameter); err != nil {
					return err
				}
				typedExpression.Parameters[idx] = typedParameter

			case *pgsql.ArraySlice:
				if err := s.rewriteArraySlice(typedParameter); err != nil {
					return err
				}
			}
		}

	case *pgsql.OrderBy:
		switch typedOrderByExpression := typedExpression.Expression.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedOrderByExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedOrderByExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedOrderByExpression); err != nil {
				return err
			}
			typedExpression.Expression = typedOrderByExpression

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedOrderByExpression); err != nil {
				return err
			}
		}

	case *pgsql.ArrayIndex:
		switch typedArrayIndexInnerExpression := typedExpression.Expression.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedArrayIndexInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedArrayIndexInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedArrayIndexInnerExpression); err != nil {
				return err
			}
			typedExpression.Expression = typedArrayIndexInnerExpression

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedArrayIndexInnerExpression); err != nil {
				return err
			}
		}

		for idx, indexExpression := range typedExpression.Indexes {
			switch typedIndexExpression := indexExpression.(type) {
			case pgsql.Identifier:
				if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedIndexExpression); err != nil {
					return err
				} else {
					typedExpression.Indexes[idx] = rewritten
				}

			case pgsql.CompoundIdentifier:
				if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedIndexExpression); err != nil {
					return err
				} else {
					typedExpression.Indexes[idx] = rewritten
				}

			case pgsql.ArraySlice:
				if err := s.rewriteArraySlice(&typedIndexExpression); err != nil {
					return err
				}
				typedExpression.Indexes[idx] = typedIndexExpression

			case *pgsql.ArraySlice:
				if err := s.rewriteArraySlice(typedIndexExpression); err != nil {
					return err
				}
			}
		}

	case pgsql.ArraySlice:
		return s.rewriteArraySlice(&typedExpression)

	case *pgsql.ArraySlice:
		return s.rewriteArraySlice(typedExpression)

	case *pgsql.Parenthetical:
		switch typedInnerExpression := typedExpression.Expression.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedInnerExpression); err != nil {
				return err
			}
			typedExpression.Expression = typedInnerExpression

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedInnerExpression); err != nil {
				return err
			}
		}

	case *pgsql.AliasedExpression:
		switch typedInnerExpression := typedExpression.Expression.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedInnerExpression); err != nil {
				return err
			}
			typedExpression.Expression = typedInnerExpression

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedInnerExpression); err != nil {
				return err
			}
		}

	case *pgsql.AnyExpression:
		switch typedInnerExpression := typedExpression.Expression.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedInnerExpression); err != nil {
				return err
			} else {
				typedExpression.Expression = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedInnerExpression); err != nil {
				return err
			}
			typedExpression.Expression = typedInnerExpression

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedInnerExpression); err != nil {
				return err
			}
		}

	case *pgsql.UnaryExpression:
		switch typedOperand := typedExpression.Operand.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedOperand); err != nil {
				return err
			} else {
				typedExpression.Operand = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedOperand); err != nil {
				return err
			} else {
				typedExpression.Operand = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedOperand); err != nil {
				return err
			}
			typedExpression.Operand = typedOperand

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedOperand); err != nil {
				return err
			}
		}

	case *pgsql.BinaryExpression:
		switch typedLOperand := typedExpression.LOperand.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedLOperand); err != nil {
				return err
			} else {
				typedExpression.LOperand = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedLOperand); err != nil {
				return err
			} else {
				typedExpression.LOperand = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedLOperand); err != nil {
				return err
			}
			typedExpression.LOperand = typedLOperand

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedLOperand); err != nil {
				return err
			}
		}

		switch typedROperand := typedExpression.ROperand.(type) {
		case pgsql.Identifier:
			if rewritten, err := rewriteIdentifierScopeReference(s.scope, typedROperand); err != nil {
				return err
			} else {
				typedExpression.ROperand = rewritten
			}

		case pgsql.CompoundIdentifier:
			if rewritten, err := rewriteCompoundIdentifierScopeReference(s.scope, typedROperand); err != nil {
				return err
			} else {
				typedExpression.ROperand = rewritten
			}

		case pgsql.ArraySlice:
			if err := s.rewriteArraySlice(&typedROperand); err != nil {
				return err
			}
			typedExpression.ROperand = typedROperand

		case *pgsql.ArraySlice:
			if err := s.rewriteArraySlice(typedROperand); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *FrameBindingRewriter) Enter(node pgsql.SyntaxNode) {
	if err := s.enter(node); err != nil {
		s.SetError(err)
	}
}

func (s *FrameBindingRewriter) exit(node pgsql.SyntaxNode) error {
	switch node.(type) {
	}

	return nil
}
func (s *FrameBindingRewriter) Exit(node pgsql.SyntaxNode) {
	if err := s.exit(node); err != nil {
		s.SetError(err)
	}
}

func RewriteFrameBindings(scope *Scope, expression pgsql.Expression) error {
	if expression == nil {
		return nil
	}

	return walk.PgSQL(expression, &FrameBindingRewriter{
		Visitor: walk.NewVisitor[pgsql.SyntaxNode](),
		scope:   scope,
	})
}
