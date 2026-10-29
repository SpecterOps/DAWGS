package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) prepareUnwindTarget(variable *cypher.Variable) (*BoundIdentifier, bool, error) {
	if _, isUnwindTarget := s.unwindTargets[variable]; !isUnwindTarget {
		return nil, false, nil
	}

	delete(s.unwindTargets, variable)

	cypherIdentifier := pgsql.Identifier(variable.Symbol)
	if existingBinding, bound := s.scope.AliasedLookup(cypherIdentifier); bound {
		return nil, true, fmt.Errorf("unwind target %s shadows existing binding %s", cypherIdentifier, existingBinding.Identifier)
	}

	binding, err := s.scope.DefineNew(pgsql.UnsetDataType)
	if err != nil {
		return nil, true, err
	}

	s.scope.Alias(cypherIdentifier, binding)
	return binding, true, nil
}

func unwindFromClauses(clauses []UnwindClause) []pgsql.FromClause {
	fromClauses := make([]pgsql.FromClause, 0, len(clauses))

	for _, clause := range clauses {
		fromClauses = append(fromClauses, pgsql.FromClause{
			Source: pgsql.AliasedExpression{
				Expression: pgsql.FunctionCall{
					Function:   pgsql.FunctionUnnest,
					Parameters: []pgsql.Expression{clause.Expression},
				},
				Alias: pgsql.AsOptionalIdentifier(clause.Binding.Identifier),
			},
		})
	}

	return fromClauses
}

// concreteEmptyArray supplies a PostgreSQL type before an unconstrained empty
// array is materialized or passed to a polymorphic function. No element can be
// observed, so the fallback does not change the list's values.
func concreteEmptyArray(expression pgsql.Expression) pgsql.Expression {
	switch typedExpression := expression.(type) {
	case pgsql.ArrayLiteral:
		if len(typedExpression.Values) == 0 && typedExpression.CastType == pgsql.AnyArray {
			typedExpression.CastType = pgsql.Int8Array
		}
		return typedExpression
	case *pgsql.Parenthetical:
		typedExpression.Expression = concreteEmptyArray(typedExpression.Expression)
	}
	return expression
}

// unwindArray preserves dynamic property/list element types. Cypher treats a
// non-list scalar as one row and a null input as no rows; nested lists stay
// intact until another UNWIND consumes them.
func (s *Translator) unwindArray(expression pgsql.Expression) (pgsql.Expression, pgsql.DataType, error) {
	value, dynamic, err := s.functionJSONArgument(expression)
	if err != nil {
		return nil, pgsql.UnknownDataType, err
	}
	if dynamic {
		value = pgsql.FunctionCall{Function: pgsql.FunctionNullIf,
			Parameters: []pgsql.Expression{value, jsonNullLiteral()}, CastType: pgsql.JSONB}
		list := pgsql.Case{
			Conditions: []pgsql.Expression{
				pgsql.NewBinaryExpression(jsonbTypeof(value), pgsql.OperatorEquals, pgsql.NewLiteral("array", pgsql.Text)),
				pgsql.NewBinaryExpression(value, pgsql.OperatorIs, pgsql.NullLiteral()),
			},
			Then: []pgsql.Expression{value, pgsql.NewTypeCast(pgsql.NewLiteral("[]", pgsql.Text), pgsql.JSONB)},
			Else: pgsql.FunctionCall{Function: pgsql.FunctionJSONBBuildArray, Parameters: []pgsql.Expression{value}, CastType: pgsql.JSONB},
		}
		return pgsql.NewTypeCast(pgsql.ArrayExpression{Expression: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{pgsql.AliasedExpression{Expression: pgsql.FunctionCall{
				Function: pgsql.FunctionJSONBArrayElements, Parameters: []pgsql.Expression{list},
			}}},
		}}}, pgsql.JSONBArray), pgsql.JSONBArray, nil
	}
	dataType, err := s.inferExpressionType(expression)
	return concreteEmptyArray(expression), dataType, err
}

func (s *Translator) translateUnwind(unwind *cypher.Unwind) error {
	if variableIdentifier, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if arrayExpression, err := s.treeTranslator.PopOperand(); err != nil {
		return err
	} else if arrayExpression, arrayType, err := s.unwindArray(arrayExpression); err != nil {
		return err
	} else if err := RewriteFrameBindings(s.scope, arrayExpression); err != nil {
		return err
	} else if identifier, isIdentifier := variableIdentifier.(pgsql.Identifier); !isIdentifier {
		return fmt.Errorf("expected identifier for unwind variable but got %T", variableIdentifier)
	} else if binding, isBound := s.scope.Lookup(identifier); !isBound {
		return fmt.Errorf("unable to lookup unwind variable binding %s", identifier)
	} else {
		arrayExpression = concreteEmptyArray(arrayExpression)
		if arrayType.IsArrayType() && arrayType.ArrayBaseType().IsKnown() {
			binding.DataType = arrayType.ArrayBaseType()
		}
		if s.scope.CurrentFrame() == nil {
			// A leading UNWIND has no MATCH/CREATE frame to attach to, but
			// later clauses still need a frame that can export the unwind binding.
			frame, err := s.scope.PushFrame()
			if err != nil {
				return err
			}

			frame.Synthetic = true
			s.query.CurrentPart().Frame = frame
		}

		s.scope.Declare(binding.Identifier)
		s.scope.CurrentFrame().Export(binding.Identifier)

		s.query.CurrentPart().AddUnwindClause(UnwindClause{
			Expression: arrayExpression,
			Binding:    binding,
		})
	}

	return nil
}

// materializeUpdateUnwinds makes pending UNWIND rows available to SET/REMOVE,
// including their multiplicity and zero-row behavior when there is no WITH.
func (s *Translator) materializeUpdateUnwinds() error {
	part := s.query.CurrentPart()
	if len(part.unwindClauses) == 0 || part.mutations.Updates.Len() == 0 {
		return nil
	}
	origin := s.scope.CurrentFrame()
	source := origin
	if source == part.Frame {
		source = source.Previous
	}
	frame, err := s.scope.PushFrame()
	if err != nil {
		return err
	}
	projection, err := buildCarryProjection(s.scope, origin.Exported, source)
	if err != nil {
		return err
	}
	var from []pgsql.FromClause
	if source != nil && !source.Synthetic {
		from = append(from, frameReference(source))
	}
	from = append(from, unwindFromClauses(part.ConsumeUnwindClauses())...)
	s.addCTE(frame, pgsql.Select{Projection: projection, From: from})
	for _, identifier := range origin.Exported.Slice() {
		binding, _ := s.scope.Lookup(identifier)
		binding.MaterializedBy(frame)
	}
	return nil
}
