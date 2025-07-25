package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models/cypher"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateSetItem(setItem *cypher.SetItem) error {
	if operator, err := translateCypherAssignmentOperator(setItem.Operator); err != nil {
		return err
	} else {
		switch operator {
		case pgsql.OperatorAssignment:
			if rightOperand, err := s.treeTranslator.PopOperand(); err != nil {
				return err
			} else if leftOperand, err := s.treeTranslator.PopOperand(); err != nil {
				return err
			} else if leftPropertyLookup, err := decomposePropertyLookup(leftOperand); err != nil {
				return err
			} else {
				return s.query.CurrentPart().mutations.AddPropertyAssignment(s.scope, leftPropertyLookup, operator, rightOperand)
			}

		case pgsql.OperatorKindAssignment:
			if rightOperand, err := s.treeTranslator.PopOperand(); err != nil {
				return err
			} else if leftOperand, err := s.treeTranslator.PopOperand(); err != nil {
				return err
			} else if targetIdentifier, isIdentifier := leftOperand.(pgsql.Identifier); !isIdentifier {
				return fmt.Errorf("expected an identifier for kind assignment left operand but got: %T", leftOperand)
			} else if kindList, isKindListLiteral := rightOperand.(pgsql.KindListLiteral); !isKindListLiteral {
				return fmt.Errorf("expected an identifier for kind list right operand but got: %T", rightOperand)
			} else {
				return s.query.CurrentPart().mutations.AddKindAssignment(s.scope, targetIdentifier, kindList.Values)
			}

		default:
			return fmt.Errorf("unsupported set item operator: %s", operator)
		}
	}
}

func (s *Translator) translateUpdates() error {
	currentQueryPart := s.query.CurrentPart()

	// Do not translate the update statements until all are collected
	currentQueryPart.numUpdatingClauses -= 1

	if currentQueryPart.numUpdatingClauses > 0 {
		return nil
	}

	for _, updateClause := range currentQueryPart.mutations.Updates.Values() {
		if stepFrame, err := s.scope.PushFrame(); err != nil {
			return err
		} else {
			updateClause.Frame = stepFrame

			joinConstraint := &Constraint{
				Dependencies: pgsql.AsIdentifierSet(updateClause.TargetBinding.Identifier, updateClause.UpdateBinding.Identifier),
				Expression: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{updateClause.TargetBinding.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{updateClause.UpdateBinding.Identifier, pgsql.ColumnID},
				),
			}

			if err := RewriteFrameBindings(s.scope, joinConstraint.Expression); err != nil {
				return err
			}

			updateClause.JoinConstraint = joinConstraint.Expression

			if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
				return err
			} else {
				// Zip through all projected identifiers and update their last projected frame
				for _, binding := range boundProjections.Bindings {
					binding.MaterializedBy(stepFrame)
				}

				for _, selectItem := range boundProjections.Items {
					switch typedProjection := selectItem.(type) {
					case *pgsql.AliasedExpression:
						if !typedProjection.Alias.Set {
							return fmt.Errorf("expected aliased expression to have an alias set")
						} else if typedProjection.Alias.Value == updateClause.TargetBinding.Identifier {
							// This is the projection being replaced by the assignment
							if rewrittenProjections, err := buildProjection(updateClause.TargetBinding.Identifier, updateClause.UpdateBinding, s.scope, s.scope.ReferenceFrame()); err != nil {
								return err
							} else {
								updateClause.Projection = append(updateClause.Projection, rewrittenProjections...)
							}
						} else {
							updateClause.Projection = append(updateClause.Projection, typedProjection)
						}

					default:
						return fmt.Errorf("expected aliased expression as projection but got: %T", selectItem)
					}
				}
			}

		}
	}

	return s.buildUpdates()
}

func (s *Translator) buildUpdates() error {
	for _, identifierMutation := range s.query.CurrentPart().mutations.Updates.Values() {
		sqlUpdate := pgsql.Update{
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{identifierMutation.Frame.Previous.Binding.Identifier},
				}},
			},
		}

		switch identifierMutation.UpdateBinding.DataType {
		case pgsql.NodeComposite:
			sqlUpdate.Table = pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(identifierMutation.UpdateBinding.Identifier),
			}

		case pgsql.EdgeComposite:
			sqlUpdate.Table = pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(identifierMutation.UpdateBinding.Identifier),
			}

		default:
			return fmt.Errorf("invalid identifier data type for update: %s", identifierMutation.UpdateBinding.Identifier)
		}

		var (
			kindAssignments      pgsql.Expression
			kindRemovals         pgsql.Expression
			propertyAssignments  pgsql.Expression
			propertyRemovals     pgsql.Expression
			kindColumnIdentifier = pgsql.ColumnKindID
		)

		if identifierMutation.UpdateBinding.DataType.MatchesOneOf(pgsql.NodeComposite, pgsql.ExpansionRootNode, pgsql.ExpansionTerminalNode) {
			kindColumnIdentifier = pgsql.ColumnKindIDs
		}

		if len(identifierMutation.KindAssignments) > 0 {
			if kindIDs, err := s.kindMapper.MapKinds(identifierMutation.KindAssignments); err != nil {
				s.SetError(fmt.Errorf("failed to translate kinds: %w", err))
			} else {
				arrayLiteral := pgsql.ArrayLiteral{
					Values:   make([]pgsql.Expression, len(kindIDs)),
					CastType: pgsql.Int2Array,
				}

				for idx, kindID := range kindIDs {
					arrayLiteral.Values[idx] = pgsql.NewLiteral(kindID, pgsql.Int2)
				}

				kindAssignments = arrayLiteral.AsExpression()
			}
		}

		if len(identifierMutation.KindRemovals) > 0 {
			if kindIDs, err := s.kindMapper.MapKinds(identifierMutation.KindRemovals); err != nil {
				s.SetError(fmt.Errorf("failed to translate kinds: %w", err))
			} else {
				arrayLiteral := pgsql.ArrayLiteral{
					Values:   make([]pgsql.Expression, len(kindIDs)),
					CastType: pgsql.Int2Array,
				}

				for idx, kindID := range kindIDs {
					arrayLiteral.Values[idx] = pgsql.NewLiteral(kindID, pgsql.Int2)
				}

				kindRemovals = arrayLiteral.AsExpression()
			}
		}

		if identifierMutation.PropertyAssignments.Len() > 0 {
			jsonObjectFunction := pgsql.FunctionCall{
				Function: pgsql.FunctionJSONBBuildObject,
				CastType: pgsql.JSONB,
			}

			for _, propertyAssignment := range identifierMutation.PropertyAssignments.Values() {
				if err := RewriteFrameBindings(s.scope, propertyAssignment.ValueExpression); err != nil {
					return err
				}

				if propertyLookup, isPropertyLookup := expressionToPropertyLookupBinaryExpression(propertyAssignment.ValueExpression); isPropertyLookup {
					// Ensure that property lookups in JSONB build functions use the JSONB field type
					propertyLookup.Operator = pgsql.OperatorJSONField
				}

				jsonObjectFunction.Parameters = append(jsonObjectFunction.Parameters,
					pgsql.NewLiteral(propertyAssignment.Field, pgsql.Text),
					propertyAssignment.ValueExpression,
				)
			}

			propertyAssignments = jsonObjectFunction.AsExpression()
		}

		if identifierMutation.Removals.Len() > 0 {
			fieldRemovalArray := pgsql.ArrayLiteral{
				CastType: pgsql.TextArray,
			}

			for _, propertyRemoval := range identifierMutation.Removals.Values() {
				fieldRemovalArray.Values = append(fieldRemovalArray.Values, pgsql.NewLiteral(propertyRemoval.Field, pgsql.Text))
			}

			propertyRemovals = fieldRemovalArray.AsExpression()
		}

		if kindAssignments != nil {
			if err := RewriteFrameBindings(s.scope, kindAssignments); err != nil {
				return err
			}

			if kindRemovals != nil {
				sqlUpdate.Assignments = append(sqlUpdate.Assignments,
					pgsql.NewBinaryExpression(
						kindColumnIdentifier,
						pgsql.OperatorAssignment,
						pgsql.FunctionCall{
							Function: pgsql.FunctionIntArrayUnique,
							Parameters: []pgsql.Expression{
								pgsql.FunctionCall{
									Function: pgsql.FunctionIntArraySort,
									Parameters: []pgsql.Expression{
										pgsql.NewBinaryExpression(
											pgsql.NewBinaryExpression(
												pgsql.CompoundIdentifier{identifierMutation.UpdateBinding.Identifier, kindColumnIdentifier},
												pgsql.OperatorSubtract,
												kindRemovals,
											),
											pgsql.OperatorConcatenate,
											kindAssignments,
										),
									},
									CastType: pgsql.Int2Array,
								},
							},
							CastType: pgsql.Int2Array,
						},
					),
				)
			} else {
				sqlUpdate.Assignments = append(sqlUpdate.Assignments,
					pgsql.NewBinaryExpression(
						kindColumnIdentifier,
						pgsql.OperatorAssignment,
						pgsql.FunctionCall{
							Function: pgsql.FunctionIntArrayUnique,
							Parameters: []pgsql.Expression{
								pgsql.FunctionCall{
									Function: pgsql.FunctionIntArraySort,
									Parameters: []pgsql.Expression{
										pgsql.NewBinaryExpression(
											pgsql.CompoundIdentifier{identifierMutation.UpdateBinding.Identifier, kindColumnIdentifier},
											pgsql.OperatorConcatenate,
											kindAssignments,
										),
									},
									CastType: pgsql.Int2Array,
								},
							},
							CastType: pgsql.Int2Array,
						},
					),
				)
			}
		} else if kindRemovals != nil {
			sqlUpdate.Assignments = append(sqlUpdate.Assignments, pgsql.NewBinaryExpression(
				kindColumnIdentifier,
				pgsql.OperatorAssignment,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{identifierMutation.UpdateBinding.Identifier, kindColumnIdentifier},
					pgsql.OperatorSubtract,
					kindRemovals,
				),
			))
		}

		if propertyAssignments != nil {
			if err := RewriteFrameBindings(s.scope, propertyAssignments); err != nil {
				return err
			}

			if propertyRemovals != nil {
				sqlUpdate.Assignments = append(sqlUpdate.Assignments, pgsql.NewBinaryExpression(
					pgsql.ColumnProperties,
					pgsql.OperatorAssignment,
					pgsql.NewBinaryExpression(
						pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{identifierMutation.UpdateBinding.Identifier, pgsql.ColumnProperties},
							pgsql.OperatorSubtract,
							propertyRemovals,
						),
						pgsql.OperatorConcatenate,
						propertyAssignments,
					),
				))
			} else {
				sqlUpdate.Assignments = append(sqlUpdate.Assignments, pgsql.NewBinaryExpression(
					pgsql.ColumnProperties,
					pgsql.OperatorAssignment,
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{identifierMutation.UpdateBinding.Identifier, pgsql.ColumnProperties},
						pgsql.OperatorConcatenate,
						propertyAssignments,
					),
				))
			}
		} else if propertyRemovals != nil {
			sqlUpdate.Assignments = append(sqlUpdate.Assignments, pgsql.NewBinaryExpression(
				pgsql.ColumnProperties,
				pgsql.OperatorAssignment,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{identifierMutation.UpdateBinding.Identifier, pgsql.ColumnProperties},
					pgsql.OperatorSubtract,
					propertyRemovals,
				),
			))
		}

		sqlUpdate.Returning = identifierMutation.Projection
		sqlUpdate.Where = identifierMutation.JoinConstraint

		s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: identifierMutation.Frame.Binding.Identifier,
			},
			Query: pgsql.Query{
				Body: sqlUpdate,
			},
		})
	}

	return nil
}
