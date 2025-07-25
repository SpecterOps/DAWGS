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

type FrameBindingRewriter struct {
	walk.Visitor[pgsql.SyntaxNode]

	scope *Scope
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
			}
		}

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
