package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	cypher "github.com/specterops/dawgs/cypher/models/cypher"
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
				} else if _, err := s.query.CurrentPart().mutations.AddDeletion(scope, typedExpression, deleteFrame); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported delete expression: %T", expression)
			}
		}
	}

	return nil
}

func (s *Translator) buildDeletions(scope *Scope) error {
	for _, identifierDeletion := range s.query.CurrentPart().mutations.Deletions.Values() {
		var (
			sqlDelete = pgsql.Delete{
				Using: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: pgsql.CompoundIdentifier{identifierDeletion.Frame.Previous.Binding.Identifier},
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
				Binding: models.ValueOptional(identifierDeletion.UpdateBinding.Identifier),
			})

		case pgsql.EdgeComposite:
			sqlDelete.From = append(sqlDelete.From, pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.ValueOptional(identifierDeletion.UpdateBinding.Identifier),
			})

		default:
			return fmt.Errorf("invalid identifier data type for deletion: %s", identifierDeletion.UpdateBinding.Identifier)
		}

		if err := rewriteConstraintIdentifierReferences(s.scope, identifierDeletion.Frame, []*Constraint{joinConstraint}); err != nil {
			return err
		}

		sqlDelete.Where = models.ValueOptional(joinConstraint.Expression)

		s.query.CurrentPart().Model.AddCTE(pgsql.CommonTableExpression{
			Alias: pgsql.TableAlias{
				Name: scope.CurrentFrameBinding().Identifier,
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
