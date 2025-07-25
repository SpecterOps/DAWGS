package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) prepareFilterExpression(filterExpression *cypher.FilterExpression) error {

	quantifierExpressionTree := NewExpressionTreeTranslator(s.kindMapper)
	s.query.CurrentPart().stashedExpressionTreeTranslator = s.treeTranslator
	s.treeTranslator = quantifierExpressionTree

	if bi, err := s.scope.DefineNew(pgsql.AnyArray); err != nil {
		return err
	} else if identifier, hasCypherBinding, err := extractIdentifierFromCypherExpression(filterExpression.Specifier); err != nil {
		return err
	} else if !hasCypherBinding {
		return fmt.Errorf("filter expression must have a cypher identifier")
	} else {
		s.scope.Alias(identifier, bi)
		if aliasedIdentifier, bound := s.scope.AliasedLookup(identifier); !bound {
			return fmt.Errorf("filter expression must have an aliased identifier")
		} else if s.query.CurrentPart().currentPattern.Parts != nil || s.query.CurrentPart().CurrentProjection() != nil {
			s.query.CurrentPart().quantifierIdentifiers.Add(aliasedIdentifier.Identifier)
		} else {
			return fmt.Errorf("quantifiers are not supported without a pattern or projection expression")
		}
	}

	return nil
}

func (s *Translator) translateFilterExpression(filterExpression *cypher.FilterExpression) error {
	var (
		iDInColFromClause = s.query.CurrentPart().ConsumeFromClauses()[0]
	)

	if identifier, hasCypherBinding, err := extractIdentifierFromCypherExpression(filterExpression.Specifier); err != nil {
		return err
	} else if !hasCypherBinding {
		return fmt.Errorf("filter expression must have a cypher identifier")
	} else {
		_, bound := s.scope.AliasedLookup(identifier)
		if !bound {
			return fmt.Errorf("filter expression variable must be bound")
		}

		if constraints, err := s.treeTranslator.ConsumeAllConstraints(); err != nil {
			return err
		} else {
			nestedQuery := &pgsql.Parenthetical{
				Expression: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgsql.FunctionCall{
							Function:   pgsql.FunctionCount,
							Parameters: []pgsql.Expression{pgsql.WildcardIdentifier},
							CastType:   pgsql.Int,
						},
					},
					From:  []pgsql.FromClause{iDInColFromClause},
					Where: constraints.Expression.AsExpression(),
				},
			}
			// push nested query on stashed expression tree translator
			s.query.CurrentPart().stashedExpressionTreeTranslator.treeBuilder.PushOperand(nestedQuery)
			s.treeTranslator = s.query.CurrentPart().stashedExpressionTreeTranslator
		}
	}

	return nil
}

func (s *Translator) translateIDInCollection(idInCol *cypher.IDInCollection) error {
	if quantifierArray, err := s.treeTranslator.PopOperand(); err != nil {
		s.SetError(err)
	} else if identifier, hasCypherBinding, err := extractIdentifierFromCypherExpression(idInCol); err != nil {
		s.SetError(err)
	} else if !hasCypherBinding {
		return fmt.Errorf("filter expression variable must have a cypher identifier binding")
	} else if array, bound := s.scope.AliasedLookup(identifier); !bound {
		return fmt.Errorf("filter expression variable must be bound")
	} else {
		var functionParameters []pgsql.Expression
		switch quantifierArrayExpression := quantifierArray.AsExpression().(type) {
		// Property lookup, n.properties -> usedencryptionkey
		case pgsql.BinaryExpression:
			// All property look-up operators are converted to JSON Text field Operators during translation,
			// this needs to be changed back so we can properly convert it to a Postgres text array which can then be used in an unnest function
			quantifierArrayExpression.Operator = pgsql.OperatorJSONField
			functionParameters = []pgsql.Expression{
				pgsql.FunctionCall{
					Function: pgsql.FunctionJSONBToTextArray,
					Parameters: []pgsql.Expression{
						quantifierArrayExpression,
					},
				},
			}
		// native postgres array eg: collect(x) as quantifier_array ... ANY(y in quantifier_array...
		case pgsql.Identifier:
			functionParameters = []pgsql.Expression{quantifierArrayExpression}
		default:
			return fmt.Errorf("unknown cypher array type %s", quantifierArrayExpression)
		}

		fromClause := pgsql.FromClause{
			Source: pgsql.AliasedExpression{
				Expression: pgsql.FunctionCall{
					Function:   pgsql.FunctionUnnest,
					Parameters: functionParameters,
				},
				Alias: models.Optional[pgsql.Identifier]{Value: array.Identifier, Set: true},
			},
		}

		s.query.CurrentPart().stashedQuantifierArray = functionParameters
		s.query.CurrentPart().AddFromClause(fromClause)
	}
	return nil
}

func (s *Translator) buildQuantifier(cypherQuantifierExpression *cypher.Quantifier) error {
	var (
		fullQuantifierBinaryExpression *pgsql.BinaryExpression
		quantifierExpression           pgsql.Expression
		quantifierOperator             pgsql.Operator
	)
	if filterExpression, err := s.treeTranslator.PopOperand(); err != nil {
		s.SetError(err)
	} else {
		switch cypherQuantifierExpression.Type {
		case cypher.QuantifierTypeAny:
			quantifierExpression = pgsql.Literal{Value: 1, CastType: pgsql.Int}
			quantifierOperator = pgsql.OperatorGreaterThanOrEqualTo
		case cypher.QuantifierTypeNone:
			quantifierExpression = pgsql.Literal{Value: 0, CastType: pgsql.Int}
			quantifierOperator = pgsql.OperatorEquals
		case cypher.QuantifierTypeSingle:
			quantifierExpression = pgsql.Literal{Value: 1, CastType: pgsql.Int}
			quantifierOperator = pgsql.OperatorEquals
		case cypher.QuantifierTypeAll:
			quantifierExpression = pgsql.FunctionCall{
				Function:   pgsql.FunctionArrayLength,
				Parameters: []pgsql.Expression{s.query.CurrentPart().stashedQuantifierArray[0], pgsql.Literal{Value: 1, CastType: pgsql.Int}},
			}
			quantifierOperator = pgsql.OperatorEquals
		default:
			return fmt.Errorf("unknown quantifier type: %v", cypherQuantifierExpression.Type)
		}

		fullQuantifierBinaryExpression = &pgsql.BinaryExpression{
			Operator: quantifierOperator,
			LOperand: filterExpression,
			ROperand: quantifierExpression,
		}

		s.treeTranslator.PushOperand(pgsql.NewTypeCast(fullQuantifierBinaryExpression, pgsql.Boolean))
	}
	return nil
}
