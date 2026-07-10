package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
)

func (s *Translator) enterQuantifier() {
	if !s.query.HasParts() {
		return
	}

	currentPart := s.query.CurrentPart()
	target := optimize.QuantifierTarget{
		QueryPartIndex:  len(s.query.Parts) - 1,
		QuantifierIndex: currentPart.quantifierIndex,
	}

	currentPart.quantifierIndex++
	s.quantifierTargets = append(s.quantifierTargets, target)
}

func (s *Translator) exitQuantifier() {
	if len(s.quantifierTargets) == 0 {
		return
	}

	s.quantifierTargets = s.quantifierTargets[:len(s.quantifierTargets)-1]
}

func (s *Translator) currentQuantifierTarget() (optimize.QuantifierTarget, bool) {
	if len(s.quantifierTargets) == 0 {
		return optimize.QuantifierTarget{}, false
	}

	return s.quantifierTargets[len(s.quantifierTargets)-1], true
}

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
		currentPart = s.query.CurrentPart()
		fromClauses = currentPart.ConsumeFromClauses()
	)

	if len(fromClauses) == 0 {
		return fmt.Errorf("filter expression has no collection source")
	}

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
			var nestedQuery pgsql.Expression

			if currentPart.stashedQuantifierUseExists {
				nestedQuery = pgsql.ExistsExpression{
					Subquery: pgsql.Subquery{
						Query: pgsql.Query{
							Body: pgsql.Select{
								Projection: []pgsql.SelectItem{pgsql.NewLiteral(1, pgsql.Int)},
								From:       fromClauses,
								Where:      constraints.Expression.AsExpression(),
							},
						},
					},
				}
			} else {
				nestedQuery = &pgsql.Parenthetical{
					Expression: pgsql.Select{
						Projection: []pgsql.SelectItem{
							pgsql.FunctionCall{
								Function:   pgsql.FunctionCount,
								Parameters: []pgsql.Expression{pgsql.WildcardIdentifier},
								CastType:   pgsql.Int,
							},
						},
						From:  fromClauses,
						Where: constraints.Expression.AsExpression(),
					},
				}
			}

			// push nested query on stashed expression tree translator
			s.query.CurrentPart().stashedExpressionTreeTranslator.treeBuilder.PushOperand(nestedQuery)
			s.treeTranslator = s.query.CurrentPart().stashedExpressionTreeTranslator
		}
	}

	return nil
}

func relationshipsPathIdentifier(expression pgsql.Expression) (pgsql.Identifier, bool) {
	switch typedExpression := unwrapParenthetical(expression).(type) {
	case pgsql.TypeCast:
		return relationshipsPathIdentifier(typedExpression.Expression)

	case pgsql.RowColumnReference:
		if typedExpression.Column != pgsql.ColumnEdges {
			return "", false
		}

		identifier, isIdentifier := unwrapParenthetical(typedExpression.Identifier).(pgsql.Identifier)
		return identifier, isIdentifier

	default:
		return "", false
	}
}

func (s *Translator) translatePathRelationshipPredicateSource(array *BoundIdentifier, quantifierArrayExpression pgsql.Expression) (bool, error) {
	if s.query.CurrentPart().currentPattern == nil || s.query.CurrentPart().currentPattern.Parts == nil {
		return false, nil
	}

	target, hasTarget := s.currentQuantifierTarget()
	if !hasTarget {
		return false, nil
	}

	if _, hasDecision := s.pathRelationshipPredicateDecisions[target]; !hasDecision {
		return false, nil
	}

	pathIdentifier, isRelationshipsPath := relationshipsPathIdentifier(quantifierArrayExpression)
	if !isRelationshipsPath {
		return false, nil
	}

	pathBinding, bound := pathCompositeBinding(s.scope, pathIdentifier)
	if !bound {
		return false, nil
	}

	edgeIDs := pgsql.NewFuture(pathBinding, pgsql.Int8Array)

	array.DataType = pgsql.EdgeComposite
	s.query.CurrentPart().stashedQuantifierArray = []pgsql.Expression{edgeIDs}
	s.query.CurrentPart().stashedQuantifierUseExists = true
	s.query.CurrentPart().AddPathEdgeIDArrayFuture(edgeIDs)
	s.query.CurrentPart().AddFromClause(pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.TableEdge.AsCompoundIdentifier(),
			Binding: pgsql.AsOptionalIdentifier(array.Identifier),
		},
	})

	if err := s.treeTranslator.AddTranslationConstraint(
		pgsql.AsIdentifierSet(array.Identifier),
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{array.Identifier, pgsql.ColumnID},
			pgsql.OperatorEquals,
			pgsql.NewAnyExpression(edgeIDs, pgsql.Int8Array),
		),
	); err != nil {
		return true, err
	}

	s.recordLowering(optimize.LoweringPathRelationshipPredicate)
	return true, nil
}

func (s *Translator) translateQuantifierArrayExpression(quantifierArrayExpression pgsql.Expression) ([]pgsql.Expression, pgsql.DataType, error) {
	switch typedQuantifierArrayExpression := quantifierArrayExpression.(type) {
	case *pgsql.BinaryExpression:
		if pgsql.OperatorIsPropertyLookup(typedQuantifierArrayExpression.Operator) {
			// Property look-up operators are converted to JSON text field operators during translation.
			// Change this back so the JSON array can be converted to a Postgres text array for unnest(...).
			typedQuantifierArrayExpression.Operator = pgsql.OperatorJSONField
			return []pgsql.Expression{
				pgsql.FunctionCall{
					Function: pgsql.FunctionJSONBToTextArray,
					Parameters: []pgsql.Expression{
						typedQuantifierArrayExpression,
					},
				},
			}, pgsql.TextArray, nil
		}

	case pgsql.BinaryExpression:
		if pgsql.OperatorIsPropertyLookup(typedQuantifierArrayExpression.Operator) {
			typedQuantifierArrayExpression.Operator = pgsql.OperatorJSONField
			return []pgsql.Expression{
				pgsql.FunctionCall{
					Function: pgsql.FunctionJSONBToTextArray,
					Parameters: []pgsql.Expression{
						typedQuantifierArrayExpression,
					},
				},
			}, pgsql.TextArray, nil
		}

		if inferredCollectionType, err := s.inferArrayExpressionType(&typedQuantifierArrayExpression); err != nil {
			return nil, pgsql.UnsetDataType, err
		} else {
			return []pgsql.Expression{typedQuantifierArrayExpression}, inferredCollectionType, nil
		}
	}

	if inferredCollectionType, err := s.inferArrayExpressionType(quantifierArrayExpression); err != nil {
		return nil, pgsql.UnsetDataType, err
	} else {
		return []pgsql.Expression{quantifierArrayExpression}, inferredCollectionType, nil
	}
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
		var (
			functionParameters []pgsql.Expression
			collectionType     pgsql.DataType
		)

		quantifierArrayExpression := quantifierArray.AsExpression()
		if lowered, err := s.translatePathRelationshipPredicateSource(array, quantifierArrayExpression); err != nil {
			return err
		} else if lowered {
			return nil
		}

		if translatedParameters, inferredCollectionType, err := s.translateQuantifierArrayExpression(quantifierArrayExpression); err != nil {
			return err
		} else {
			functionParameters = translatedParameters
			collectionType = inferredCollectionType
		}

		array.DataType = collectionType.ArrayBaseType()

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
		nullInputCheck                 pgsql.Expression
	)
	if filterExpression, err := s.treeTranslator.PopOperand(); err != nil {
		s.SetError(err)
	} else {
		if s.query.CurrentPart().stashedQuantifierUseExists {
			s.query.CurrentPart().stashedQuantifierUseExists = false

			switch cypherQuantifierExpression.Type {
			case cypher.QuantifierTypeAny:
				s.treeTranslator.PushOperand(pgsql.NewTypeCast(filterExpression, pgsql.Boolean))
				return nil

			case cypher.QuantifierTypeNone:
				negatedExpression := filterExpression
				if existsExpression, isExistsExpression := filterExpression.(pgsql.ExistsExpression); isExistsExpression {
					existsExpression.Negated = true
					negatedExpression = existsExpression
				} else {
					negatedExpression = pgsql.NewUnaryExpression(pgsql.OperatorNot, filterExpression)
				}

				s.treeTranslator.PushOperand(pgsql.NewTypeCast(pgsql.NewBinaryExpression(
					negatedExpression,
					pgsql.OperatorAnd,
					pgsql.NewBinaryExpression(
						s.query.CurrentPart().stashedQuantifierArray[0],
						pgsql.OperatorIsNot,
						pgsql.NullLiteral(),
					),
				), pgsql.Boolean))
				return nil

			default:
				return fmt.Errorf("path relationship predicate lowering does not support quantifier type: %v", cypherQuantifierExpression.Type)
			}
		}

		switch cypherQuantifierExpression.Type {
		case cypher.QuantifierTypeAny:
			quantifierExpression = pgsql.Literal{Value: 1, CastType: pgsql.Int}
			quantifierOperator = pgsql.OperatorGreaterThanOrEqualTo
		case cypher.QuantifierTypeNone:
			quantifierExpression = pgsql.Literal{Value: 0, CastType: pgsql.Int}
			quantifierOperator = pgsql.OperatorEquals
			nullInputCheck = pgsql.NewBinaryExpression(
				s.query.CurrentPart().stashedQuantifierArray[0],
				pgsql.OperatorIsNot,
				pgsql.NullLiteral(),
			)
		case cypher.QuantifierTypeSingle:
			quantifierExpression = pgsql.Literal{Value: 1, CastType: pgsql.Int}
			quantifierOperator = pgsql.OperatorEquals
		case cypher.QuantifierTypeAll:
			quantifierExpression = pgsql.FunctionCall{
				Function:   pgsql.FunctionCardinality,
				Parameters: []pgsql.Expression{s.query.CurrentPart().stashedQuantifierArray[0]},
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

		if nullInputCheck != nil {
			fullQuantifierBinaryExpression = pgsql.NewBinaryExpression(
				fullQuantifierBinaryExpression,
				pgsql.OperatorAnd,
				nullInputCheck,
			)
		}

		s.treeTranslator.PushOperand(pgsql.NewTypeCast(fullQuantifierBinaryExpression, pgsql.Boolean))
	}
	return nil
}
