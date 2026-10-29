package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

func (s *Translator) translateDelete(scope *Scope, cypherDelete *cypher.Delete) error {
	for range cypherDelete.Expressions {
		if expression, err := s.treeTranslator.PopOperand(); err != nil {
			return err
		} else {
			switch typedExpression := expression.(type) {
			case pgsql.Identifier:
				if deleteFrame, err := scope.PushFrame(); err != nil {
					return err
				} else if deletion, err := s.query.CurrentPart().mutations.AddDeletion(scope, typedExpression, deleteFrame); err != nil {
					return err
				} else {
					deletion.Detach = cypherDelete.Detach
				}
			default:
				return fmt.Errorf("unsupported delete expression: %T", expression)
			}
		}
	}

	return nil
}

// deletionSource skips mutation-only frames, which do not export input rows.
func (s *Translator) deletionSource(deletion *Delete) (*Frame, error) {
	frames := map[*Frame]bool{}
	for _, candidate := range s.query.CurrentPart().mutations.Deletions.Values() {
		frames[candidate.Frame] = true
	}
	frame, ok := s.previousValidFrame(deletion.Frame)
	for ok && frames[frame] {
		frame, ok = s.previousValidFrame(frame)
	}
	if !ok {
		return nil, fmt.Errorf("deletion %s has no input frame", deletion.TargetBinding.Identifier)
	}
	return frame, nil
}

// deletionIDs describes the entire input stream, including targets in other
// rows. NULL optional bindings must not turn the exclusion test into UNKNOWN.
func (s *Translator) deletionIDs(nodes bool) (pgsql.Expression, error) {
	var result pgsql.Expression = pgsql.ArrayLiteral{CastType: pgsql.Int8Array}
	for _, deletion := range s.query.CurrentPart().mutations.Deletions.Values() {
		if nodes && (deletion.TargetBinding.DataType != pgsql.NodeComposite || !deletion.Detach) ||
			!nodes && deletion.TargetBinding.DataType != pgsql.EdgeComposite {
			continue
		}
		frame, err := s.deletionSource(deletion)
		if err != nil {
			return nil, err
		}
		id := pgsql.RowColumnReference{
			Identifier: pgsql.CompoundIdentifier{frame.Binding.Identifier, deletion.TargetBinding.Identifier},
			Column:     pgsql.ColumnID,
		}
		ids := pgsql.ArrayExpression{Expression: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{id},
			From:       []pgsql.FromClause{{Source: pgsql.TableReference{Name: pgsql.CompoundIdentifier{frame.Binding.Identifier}}}},
			Where:      pgsql.NewBinaryExpression(id, pgsql.OperatorIsNot, pgsql.NullLiteral()),
		}}}
		result = pgsql.NewBinaryExpression(result, pgsql.OperatorConcatenate, ids)
	}
	return result, nil
}

func (s *Translator) buildDeletions(scope *Scope) error {
	for _, identifierDeletion := range s.query.CurrentPart().mutations.Deletions.Values() {
		sourceFrame, err := s.deletionSource(identifierDeletion)
		if err != nil {
			return err
		}
		var (
			sqlDelete = pgsql.Delete{
				Using: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier},
					},
				}},
			}

			joinConstraint = &Constraint{
				Dependencies: pgsql.AsIdentifierSet(identifierDeletion.TargetBinding.Identifier, identifierDeletion.UpdateBinding.Identifier),
				Expression: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{identifierDeletion.TargetBinding.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{identifierDeletion.UpdateBinding.Identifier, pgsql.ColumnID},
				),
			}
		)

		switch identifierDeletion.UpdateBinding.DataType {
		case pgsql.NodeComposite:
			sqlDelete.From = append(sqlDelete.From, pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(identifierDeletion.UpdateBinding.Identifier),
			})

		case pgsql.EdgeComposite:
			sqlDelete.From = append(sqlDelete.From, pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(identifierDeletion.UpdateBinding.Identifier),
			})

		default:
			return fmt.Errorf("invalid identifier data type for deletion: %s", identifierDeletion.UpdateBinding.Identifier)
		}

		if err := rewriteConstraintIdentifierReferences(s.scope, identifierDeletion.Frame, []*Constraint{joinConstraint}); err != nil {
			return err
		}

		sqlDelete.Where = joinConstraint.Expression
		if identifierDeletion.UpdateBinding.DataType == pgsql.NodeComposite && !identifierDeletion.Detach {
			// This error-producing check must consume only fully matched rows.
			// Keep it on the input side of the join and prevent input inlining,
			// so a different join plan cannot check unrelated stored nodes.
			for index := range s.query.CurrentPart().Model.CommonTableExpressions.Expressions {
				cte := &s.query.CurrentPart().Model.CommonTableExpressions.Expressions[index]
				if cte.Alias.Name == sourceFrame.Binding.Identifier {
					cte.Materialized = &pgsql.Materialized{Materialized: true}
				}
			}
			edgeIDs, err := s.deletionIDs(false)
			if err != nil {
				return err
			}
			detachIDs, err := s.deletionIDs(true)
			if err != nil {
				return err
			}
			sqlDelete.Where = pgsql.NewBinaryExpression(sqlDelete.Where, pgsql.OperatorAnd, pgsql.FunctionCall{
				Function: "cypher_assert_node_deletable",
				Parameters: []pgsql.Expression{
					pgsql.NewLiteral(s.graphID, pgsql.Int4),
					pgsql.RowColumnReference{
						Identifier: pgsql.CompoundIdentifier{sourceFrame.Binding.Identifier, identifierDeletion.TargetBinding.Identifier},
						Column:     pgsql.ColumnID,
					},
					edgeIDs, detachIDs,
				},
				CastType: pgsql.Boolean,
			})
		}

		s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: identifierDeletion.Frame.Binding.Identifier,
			},
			Query: pgsql.Query{
				Body: sqlDelete,
			},
		})
	}

	return nil
}

func (s *Translator) translateRemoveItem(removeItem *cypher.RemoveItem) error {
	if removeItem.KindMatcher != nil {
		if variable, isVariable := removeItem.KindMatcher.Reference.(*cypher.Variable); !isVariable {
			return fmt.Errorf("expected variable for kind matcher reference but found type: %T", removeItem.KindMatcher.Reference)
		} else if binding, resolved := s.scope.LookupString(variable.Symbol); !resolved {
			return fmt.Errorf("unable to find identifier %s", variable.Symbol)
		} else {
			return s.query.CurrentPart().mutations.AddKindRemoval(s.scope, binding.Identifier, removeItem.KindMatcher.Kinds)
		}
	}

	if removeItem.Property != nil {
		if propertyLookupExpression, err := s.treeTranslator.PopOperand(); err != nil {
			return err
		} else if propertyLookup, err := decomposePropertyLookup(propertyLookupExpression); err != nil {
			return err
		} else {
			return s.query.CurrentPart().mutations.AddPropertyRemoval(s.scope, propertyLookup)
		}
	}

	return nil
}
