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

	if err := s.buildCreates(); err != nil {
		return err
	}

	for _, updateClause := range currentQueryPart.mutations.Updates.Values() {
		writeBinding, err := s.scope.DefineNew(pgsql.Scope)
		if err != nil {
			return err
		}
		stepFrame, err := s.scope.PushFrame()
		if err != nil {
			return err
		}
		updateClause.Frame = stepFrame
		sourceFrame, hasSource := s.previousValidFrame(stepFrame)
		if !hasSource {
			return fmt.Errorf("update %s has no input frame", updateClause.TargetBinding.Identifier)
		}

		updateClause.JoinConstraint = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{updateClause.TargetBinding.Identifier, pgsql.ColumnID},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{updateClause.UpdateBinding.Identifier, pgsql.ColumnID},
		)
		if err := RewriteFrameBindings(s.scope, updateClause.JoinConstraint); err != nil {
			return err
		}
		boundProjections, err := buildVisibleProjections(s.scope)
		if err != nil {
			return err
		}

		// RETURNING has one row per physical target, not per input row. Only
		// return the updated entity, then attach it to the original row stream.
		// A left join also retains rows whose mutation target is null.
		fresh := pgsql.CompoundIdentifier{writeBinding.Identifier, updateClause.UpdateBinding.Identifier}
		for _, selectItem := range boundProjections.Items {
			projection, ok := selectItem.(*pgsql.AliasedExpression)
			if !ok || !projection.Alias.Set {
				return fmt.Errorf("expected aliased update projection but got: %T", selectItem)
			}
			binding, _ := s.scope.Lookup(projection.Alias.Value)
			updateClause.Projection = append(updateClause.Projection, refreshUpdatedEntityProjection(projection, binding, updateClause.UpdateBinding, fresh))
		}

		// Resolve assignment expressions before advancing the bindings to the
		// projection frame, so each target reads the complete preceding stream.
		if err := s.buildUpdate(updateClause, sourceFrame, writeBinding); err != nil {
			return err
		}
		s.addCTE(stepFrame, pgsql.Select{
			Projection: updateClause.Projection,
			From: []pgsql.FromClause{{
				Source: sourceFrame.Binding.Identifier,
				Joins: []pgsql.Join{{
					Table: writeBinding.Identifier,
					JoinOperator: pgsql.JoinOperator{
						JoinType: pgsql.JoinTypeLeftOuter,
						Constraint: pgsql.NewBinaryExpression(
							pgsql.RowColumnReference{Identifier: pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier, updateClause.TargetBinding.Identifier}, Column: pgsql.ColumnID},
							pgsql.OperatorEquals,
							pgsql.RowColumnReference{Identifier: fresh, Column: pgsql.ColumnID},
						),
					},
				}},
			}},
		})
		for _, binding := range boundProjections.Bindings {
			binding.MaterializedBy(stepFrame)
		}
	}

	return nil
}

// refreshUpdatedEntityProjection updates every name for the mutated entity
// while preserving unrelated entities and values captured before the mutation.
func refreshUpdatedEntityProjection(projection *pgsql.AliasedExpression, binding, updated *BoundIdentifier, fresh pgsql.Expression) *pgsql.AliasedExpression {
	if binding == nil || updated == nil || binding.IDOnly {
		return projection
	}
	family := bindingExpressionType(binding)
	if family != bindingExpressionType(updated) || (family != pgsql.NodeComposite && family != pgsql.EdgeComposite) {
		return projection
	}
	refreshed := *projection
	refreshed.Expression = pgsql.Case{
		Conditions: []pgsql.Expression{pgsql.NewBinaryExpression(
			pgsql.RowColumnReference{Identifier: projection.Expression, Column: pgsql.ColumnID},
			pgsql.OperatorEquals,
			pgsql.RowColumnReference{Identifier: fresh, Column: pgsql.ColumnID},
		)},
		Then: []pgsql.Expression{fresh},
		Else: projection.Expression,
	}
	return &refreshed
}

func (s *Translator) buildUpdate(identifierMutation *Update, sourceFrame *Frame, writeBinding *BoundIdentifier) error {
	sqlUpdate := pgsql.Update{
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier},
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

	returning, err := buildProjection(identifierMutation.UpdateBinding.Identifier, identifierMutation.UpdateBinding, s.scope, nil)
	if err != nil {
		return err
	}
	sqlUpdate.Returning = returning
	sqlUpdate.Where = identifierMutation.JoinConstraint

	s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: writeBinding.Identifier,
		},
		Query: pgsql.Query{
			Body: sqlUpdate,
		},
	})

	return nil
}
