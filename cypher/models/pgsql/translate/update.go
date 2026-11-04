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

	if err := s.materializeUpdateUnwinds(); err != nil {
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
		properties pgsql.Expression
		kinds      pgsql.Expression
		addsKinds  bool
	)
	kindColumn := pgsql.ColumnKindID
	if identifierMutation.UpdateBinding.DataType == pgsql.NodeComposite {
		kindColumn = pgsql.ColumnKindIDs
	}

	for _, operation := range identifierMutation.operations {
		switch operation.kind {
		case setProperty:
			if err := RewriteFrameBindings(s.scope, operation.value); err != nil {
				return err
			}
			if propertyLookup, ok := expressionToPropertyLookupBinaryExpression(operation.value); ok {
				// Copy property values as JSONB, preserving their value types.
				propertyLookup.Operator = pgsql.OperatorJSONField
			}
			assignment := pgsql.FunctionCall{
				Function: pgsql.FunctionJSONBBuildObject,
				CastType: pgsql.JSONB,
				Parameters: []pgsql.Expression{
					pgsql.NewLiteral(operation.field, pgsql.Text), operation.value,
				},
			}
			properties = appendUpdateOperation(properties, identifierMutation.UpdateBinding.Identifier, pgsql.ColumnProperties, pgsql.OperatorConcatenate, assignment)

		case removeProperty:
			removal := pgsql.ArrayLiteral{
				CastType: pgsql.TextArray,
				Values:   []pgsql.Expression{pgsql.NewLiteral(operation.field, pgsql.Text)},
			}
			properties = appendUpdateOperation(properties, identifierMutation.UpdateBinding.Identifier, pgsql.ColumnProperties, pgsql.OperatorSubtract, removal)

		case addKinds, removeKinds:
			kindIDs, err := s.kindMapper.MapKinds(operation.kinds)
			if err != nil {
				return fmt.Errorf("failed to translate kinds: %w", err)
			}
			values := pgsql.ArrayLiteral{CastType: pgsql.Int2Array}
			for _, kindID := range kindIDs {
				values.Values = append(values.Values, pgsql.NewLiteral(kindID, pgsql.Int2))
			}
			operator := pgsql.OperatorSubtract
			if operation.kind == addKinds {
				operator = pgsql.OperatorConcatenate
				addsKinds = true
			}
			kinds = appendUpdateOperation(kinds, identifierMutation.UpdateBinding.Identifier, kindColumn, operator, values)
		}
	}

	if kinds != nil {
		if addsKinds {
			kinds = pgsql.FunctionCall{
				Function: pgsql.FunctionIntArrayUnique,
				Parameters: []pgsql.Expression{pgsql.FunctionCall{
					Function:   pgsql.FunctionIntArraySort,
					Parameters: []pgsql.Expression{kinds},
					CastType:   pgsql.Int2Array,
				}},
				CastType: pgsql.Int2Array,
			}
		}
		sqlUpdate.Assignments = append(sqlUpdate.Assignments, pgsql.NewBinaryExpression(kindColumn, pgsql.OperatorAssignment, kinds))
	}
	if properties != nil {
		sqlUpdate.Assignments = append(sqlUpdate.Assignments, pgsql.NewBinaryExpression(pgsql.ColumnProperties, pgsql.OperatorAssignment, properties))
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

// appendUpdateOperation folds operations from left to right. Parentheses keep
// PostgreSQL operator precedence from reordering concatenation and subtraction.
func appendUpdateOperation(previous pgsql.Expression, binding, column pgsql.Identifier, operator pgsql.Operator, value pgsql.Expression) pgsql.Expression {
	if previous == nil {
		previous = pgsql.CompoundIdentifier{binding, column}
	} else {
		// Keep consecutive assignments/removals in one JSON object or array.
		// A change of operator creates a boundary that must remain ordered.
		if binary, ok := previous.(*pgsql.BinaryExpression); ok && binary.Operator == operator {
			switch right := binary.ROperand.(type) {
			case pgsql.FunctionCall:
				if next, ok := value.(pgsql.FunctionCall); ok && right.Function == next.Function {
					right.Parameters = append(right.Parameters, next.Parameters...)
					binary.ROperand = right
					return binary
				}
			case pgsql.ArrayLiteral:
				if next, ok := value.(pgsql.ArrayLiteral); ok && right.CastType == next.CastType {
					right.Values = append(right.Values, next.Values...)
					binary.ROperand = right
					return binary
				}
			}
		}
		previous = pgsql.NewParenthetical(previous)
	}
	return pgsql.NewBinaryExpression(previous, operator, value)
}
