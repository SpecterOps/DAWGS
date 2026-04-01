package translate

import (
	"errors"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/format"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

const translateDefaultMaxTraversalDepth int64 = 15

func expansionEdgeJoinCondition(traversalStep *TraversalStep) (pgsql.Expression, error) {
	return pgd.Equals(
		pgd.EntityID(traversalStep.LeftNode.Identifier),
		traversalStep.Expansion.EdgeStartColumn,
	), nil
}

func expansionConstraints(traversalStep *TraversalStep) pgsql.Expression {
	expansionModel := traversalStep.Expansion

	return pgd.And(
		pgd.LessThan(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionDepth),
			pgd.IntLiteral(expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth)),
		),
		pgd.Not(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionIsCycle),
		),
	)
}

var (
	ErrUnsupportedExpansionDirection = errors.New("unsupported expansion direction")
)

type ExpansionBuilder struct {
	PrimerStatement     pgsql.Select
	RecursiveStatement  pgsql.Select
	ProjectionStatement pgsql.Select

	queryParameters map[string]any
	traversalStep   *TraversalStep
	model           *Expansion
}

func NewExpansionBuilder(queryParameters map[string]any, traversalStep *TraversalStep) (*ExpansionBuilder, error) {
	if traversalStep.Expansion == nil {
		return nil, errors.New("traversal step must have expansion set")
	}

	return &ExpansionBuilder{
		queryParameters: queryParameters,
		traversalStep:   traversalStep,
		model:           traversalStep.Expansion,
	}, nil
}

func nextFrontInsert(body pgsql.SetExpression) pgsql.Insert {
	return pgsql.Insert{
		Table: pgsql.TableReference{
			Name: expansionNextFront.AsCompoundIdentifier(),
		},
		Shape: expansionColumns(),
		Source: &pgsql.Query{
			Body: body,
		},
	}
}

func deframeExpression(expression pgsql.Expression) pgsql.Expression {
	if expression == nil {
		return nil
	}

	switch typedExpression := expression.(type) {
	case pgsql.RowColumnReference:
		if compound, ok := typedExpression.Identifier.(pgsql.CompoundIdentifier); ok && len(compound) >= 2 {
			// Drop the frame prefix and keep only the leaf identifier + column.
			return pgsql.CompoundIdentifier{compound[len(compound)-1], typedExpression.Column}
		}

		return expression

	case *pgsql.BinaryExpression:
		return &pgsql.BinaryExpression{
			Operator: typedExpression.Operator,
			LOperand: deframeExpression(typedExpression.LOperand),
			ROperand: deframeExpression(typedExpression.ROperand),
		}

	case *pgsql.Parenthetical:
		return &pgsql.Parenthetical{
			Expression: deframeExpression(typedExpression.Expression),
		}

	default:
		return expression
	}
}

func (s *ExpansionBuilder) prepareForwardFrontPrimerQuery(expansionModel *Expansion) pgsql.Select {
	var (
		primerNodeConstraints   = expansionModel.PrimerNodeConstraints
		primerNodeJoinCondition = expansionModel.PrimerNodeJoinCondition
		nextQuery               = pgsql.Select{
			Where: expansionModel.EdgeConstraints,
		}
	)

	if s.traversalStep.LeftNodeBound {
		primerNodeConstraints = deframeExpression(primerNodeConstraints)
		primerNodeJoinCondition = pgd.Equals(pgd.Column(s.traversalStep.LeftNode.Identifier, pgsql.ColumnID), pgd.StartID(s.traversalStep.Edge.Identifier))
	}

	nextQuery.Where = pgsql.OptionalAnd(primerNodeConstraints, nextQuery.Where)

	nextQuery.Projection = []pgsql.SelectItem{
		s.model.EdgeStartColumn,
		s.model.EdgeEndColumn,
		pgd.IntLiteral(1),
	}

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		nextQuery.Projection = append(nextQuery.Projection, expansionModel.TerminalNodeSatisfactionProjection)
	} else {
		nextQuery.Projection = append(nextQuery.Projection, pgsql.ExistsExpression{
			Subquery: pgsql.Subquery{
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: []pgsql.SelectItem{
							pgd.IntLiteral(1),
						},
						From: []pgsql.FromClause{{
							Source: pgsql.TableReference{
								Name: pgsql.TableEdge.AsCompoundIdentifier(),
							},
						}},
						Where: pgd.Equals(
							expansionModel.EdgeEndIdentifier,
							expansionModel.EdgeStartColumn,
						),
					},
				},
			},
			Negated: false,
		})
	}

	nextQuery.Projection = append(nextQuery.Projection,
		pgd.Equals(
			pgd.StartID(s.traversalStep.Edge.Identifier),
			pgd.EndID(s.traversalStep.Edge.Identifier),
		),
		pgd.ExpressionArrayLiteral(
			pgd.EntityID(s.traversalStep.Edge.Identifier),
		),
	)

	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
			Binding: models.OptionalValue(s.traversalStep.Edge.Identifier),
		},
	}

	if primerNodeConstraints != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: deframeExpression(primerNodeJoinCondition),
			},
		})
	}

	if expansionModel.TerminalNodeConstraints != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	return nextQuery
}

func (s *ExpansionBuilder) prepareForwardFrontRecursiveQuery(expansionModel *Expansion) pgsql.Select {
	nextQuery := pgsql.Select{
		Where: expansionModel.EdgeConstraints,
	}

	nextQuery.Projection = []pgsql.SelectItem{
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
		s.model.EdgeEndColumn,
		pgd.Add(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionDepth),
			pgd.IntLiteral(1)),
	}

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		nextQuery.Projection = append(nextQuery.Projection, expansionModel.TerminalNodeSatisfactionProjection)
	} else {
		nextQuery.Projection = append(nextQuery.Projection, pgsql.ExistsExpression{
			Subquery: pgsql.Subquery{
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: []pgsql.SelectItem{
							pgd.IntLiteral(1),
						},
						From: []pgsql.FromClause{{
							Source: pgsql.TableReference{
								Name: pgsql.TableEdge.AsCompoundIdentifier(),
							},
						}},
						Where: pgd.Equals(
							expansionModel.EdgeEndIdentifier,
							expansionModel.EdgeStartColumn,
						),
					},
				},
			},
			Negated: false,
		})
	}

	nextQuery.Projection = append(nextQuery.Projection, pgd.Equals(
		pgd.EntityID(s.traversalStep.Edge.Identifier),
		pgd.Any(pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath), pgsql.ExpansionPath),
	))

	nextQuery.Projection = append(nextQuery.Projection, pgd.Concatenate(
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath),
		pgd.EntityID(s.traversalStep.Edge.Identifier),
	))

	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionForwardFront},
			Binding: models.OptionalValue(expansionModel.Frame.Binding.Identifier),
		},

		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(s.traversalStep.Edge.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					s.model.EdgeStartColumn,
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}

	if expansionModel.TerminalNodeConstraints != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	return nextQuery
}

func (s *ExpansionBuilder) prepareBackwardFrontPrimerQuery(expansionModel *Expansion) pgsql.Select {
	nextQuery := pgsql.Select{
		Where: pgsql.OptionalAnd(expansionModel.TerminalNodeConstraints, expansionModel.EdgeConstraints),
	}

	nextQuery.Projection = []pgsql.SelectItem{
		s.model.EdgeEndColumn,
		s.model.EdgeStartColumn,
		pgd.IntLiteral(1),
	}

	if expansionModel.PrimerNodeSatisfactionProjection != nil {
		nextQuery.Projection = append(nextQuery.Projection, expansionModel.PrimerNodeSatisfactionProjection)
	} else {
		nextQuery.Projection = append(nextQuery.Projection, pgsql.ExistsExpression{
			Subquery: pgsql.Subquery{
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: []pgsql.SelectItem{
							pgd.IntLiteral(1),
						},
						From: []pgsql.FromClause{{
							Source: pgsql.TableReference{
								Name: pgsql.TableEdge.AsCompoundIdentifier(),
							},
						}},
						Where: pgd.Equals(
							expansionModel.EdgeStartIdentifier,
							expansionModel.EdgeEndColumn,
						),
					},
				},
			},
			Negated: false,
		})
	}

	nextQuery.Projection = append(nextQuery.Projection,
		pgd.Equals(
			pgd.StartID(s.traversalStep.Edge.Identifier),
			pgd.EndID(s.traversalStep.Edge.Identifier),
		),
		pgd.ExpressionArrayLiteral(
			pgd.EntityID(s.traversalStep.Edge.Identifier),
		),
	)

	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
			Binding: models.OptionalValue(s.traversalStep.Edge.Identifier),
		},
	}

	if expansionModel.PrimerNodeConstraints != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.PrimerNodeJoinCondition,
			},
		})
	}

	if expansionModel.TerminalNodeConstraints != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	return nextQuery
}

func (s *ExpansionBuilder) prepareBackwardFrontRecursiveQuery(expansionModel *Expansion) pgsql.Select {
	nextQuery := pgsql.Select{
		Where: expansionModel.EdgeConstraints,
	}

	nextQuery.Projection = []pgsql.SelectItem{
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
		s.model.EdgeStartColumn,
		pgd.Add(
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionDepth),
			pgd.IntLiteral(1)),
	}

	if expansionModel.PrimerNodeSatisfactionProjection != nil {
		nextQuery.Projection = append(nextQuery.Projection, expansionModel.PrimerNodeSatisfactionProjection)
	} else {
		nextQuery.Projection = append(nextQuery.Projection, pgsql.ExistsExpression{
			Subquery: pgsql.Subquery{
				Query: pgsql.Query{
					Body: pgsql.Select{
						Projection: []pgsql.SelectItem{
							pgd.IntLiteral(1),
						},
						From: []pgsql.FromClause{{
							Source: pgsql.TableReference{
								Name: pgsql.TableEdge.AsCompoundIdentifier(),
							},
						}},
						Where: pgd.Equals(
							expansionModel.EdgeStartIdentifier,
							expansionModel.EdgeEndColumn,
						),
					},
				},
			},
			Negated: false,
		})
	}

	nextQuery.Projection = append(nextQuery.Projection, pgd.Equals(
		pgd.EntityID(s.traversalStep.Edge.Identifier),
		pgd.Any(pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath), pgsql.ExpansionPath),
	))

	nextQuery.Projection = append(nextQuery.Projection, pgd.Concatenate(
		pgd.EntityID(s.traversalStep.Edge.Identifier),
		pgd.Column(expansionModel.Frame.Binding.Identifier, expansionPath),
	))

	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionBackwardFront},
			Binding: models.OptionalValue(expansionModel.Frame.Binding.Identifier),
		},

		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(s.traversalStep.Edge.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					s.model.EdgeEndColumn,
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}

	if expansionModel.PrimerNodeConstraints != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: s.traversalStep.Expansion.PrimerNodeJoinCondition,
			},
		})
	}

	nextQuery.From = []pgsql.FromClause{nextQueryFrom}
	return nextQuery
}

func shortestPathSearchCTE(functionName pgsql.Identifier, expansionModel *Expansion, harnessParameters []pgsql.Expression) pgsql.CommonTableExpression {
	var (
		innerQuery = pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{
					pgsql.Wildcard{},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.FunctionCall{
						Function:   functionName,
						Parameters: harnessParameters,
					},
				}},
			},
		}
	)

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  expansionModel.Frame.Binding.Identifier,
			Shape: expansionColumns(),
		},
		Query: innerQuery,
	}
}

func (s *ExpansionBuilder) buildShortestPathsHarnessCall(harnessFunctionName pgsql.Identifier) (pgsql.Query, error) {
	var (
		expansionModel             = s.traversalStep.Expansion
		forwardFrontPrimerQuery    = s.prepareForwardFrontPrimerQuery(expansionModel)
		forwardFrontRecursiveQuery = s.prepareForwardFrontRecursiveQuery(expansionModel)
		projectionQuery            pgsql.Select
	)

	projectionQuery.Projection = expansionModel.Projection

	// Select the expansion components for the projection statement
	projectionQuery.From = []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				),
			},
		}, {
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}}

	// If the traversal's left node was already bound in a prior frame, that frame must appear in the
	// projection's FROM clause so that columns like s0.e0 and s0.n0 are in scope.
	if s.traversalStep.LeftNodeBound && s.traversalStep.Frame.Previous != nil {
		prevFrameID := s.traversalStep.Frame.Previous.Binding.Identifier

		// Prepend as a comma-join so it does not interfere with the explicit JOIN chain.
		projectionQuery.From = append([]pgsql.FromClause{{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{prevFrameID},
			},
		}}, projectionQuery.From...)

		// (s0.n1).id = s2.root_id
		projectionQuery.Where = pgsql.NewBinaryExpression(
			pgsql.RowColumnReference{
				Identifier: pgsql.CompoundIdentifier{prevFrameID, s.traversalStep.LeftNode.Identifier},
				Column:     pgsql.ColumnID,
			},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
		)
	}

	if harnessParameters, err := s.shortestPathsParameters(expansionModel, forwardFrontPrimerQuery, forwardFrontRecursiveQuery); err != nil {
		return pgsql.Query{}, err
	} else {
		query := pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
			Body:                   projectionQuery,
		}

		query.AddCTE(shortestPathSearchCTE(harnessFunctionName, expansionModel, harnessParameters))
		return query, nil
	}
}

func (s *ExpansionBuilder) BuildShortestPathsRoot() (pgsql.Query, error) {
	return s.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalSPHarness)
}

func (s *ExpansionBuilder) BuildAllShortestPathsRoot() (pgsql.Query, error) {
	return s.buildShortestPathsHarnessCall(pgsql.FunctionUnidirectionalASPHarness)
}

func (s *ExpansionBuilder) BuildBiDirectionalAllShortestPathsRoot() (pgsql.Query, error) {
	var (
		expansionModel              = s.traversalStep.Expansion
		forwardFrontPrimerQuery     = s.prepareForwardFrontPrimerQuery(expansionModel)
		forwardFrontRecursiveQuery  = s.prepareForwardFrontRecursiveQuery(expansionModel)
		backwardFrontPrimerQuery    = s.prepareBackwardFrontPrimerQuery(expansionModel)
		backwardFrontRecursiveQuery = s.prepareBackwardFrontRecursiveQuery(expansionModel)
		projectionQuery             pgsql.Select
	)

	projectionQuery.Projection = expansionModel.Projection

	// Select the expansion components for the projection statement
	projectionQuery.From = []pgsql.FromClause{{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				),
			},
		}, {
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(s.traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	}}

	// If the traversal's left node was already bound in a prior frame, that frame must appear in the
	// projection's FROM clause so that columns like s0.e0 and s0.n0 are in scope.
	if s.traversalStep.LeftNodeBound && s.traversalStep.Frame.Previous != nil {
		prevFrameID := s.traversalStep.Frame.Previous.Binding.Identifier

		// Prepend as a comma-join so it does not interfere with the explicit JOIN chain.
		projectionQuery.From = append([]pgsql.FromClause{{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{prevFrameID},
			},
		}}, projectionQuery.From...)

		// (s0.n1).id = s2.root_id
		projectionQuery.Where = pgsql.NewBinaryExpression(
			pgsql.RowColumnReference{
				Identifier: pgsql.CompoundIdentifier{prevFrameID, s.traversalStep.LeftNode.Identifier},
				Column:     pgsql.ColumnID,
			},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
		)
	}

	if harnessParameters, err := s.bidirectionalAllShortestPathsParameters(expansionModel, forwardFrontPrimerQuery, forwardFrontRecursiveQuery, backwardFrontPrimerQuery, backwardFrontRecursiveQuery); err != nil {
		return pgsql.Query{}, err
	} else {
		query := pgsql.Query{
			CommonTableExpressions: &pgsql.With{},
			Body:                   projectionQuery,
		}

		query.AddCTE(shortestPathSearchCTE(pgsql.FunctionBidirectionalASPHarness, expansionModel, harnessParameters))
		return query, nil
	}
}

func (s *ExpansionBuilder) shortestPathsParameters(expansionModel *Expansion, forwardFrontPrimerQuery pgsql.Select, forwardFrontRecursiveQuery pgsql.Select) ([]pgsql.Expression, error) {
	var (
		harnessParameters []pgsql.Expression
		formatFragment    = func(query pgsql.Select) (string, error) {
			return format.Statement(
				nextFrontInsert(query),
				format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters))
		}
	)

	if formattedQuery, err := formatFragment(forwardFrontPrimerQuery); err != nil {
		return nil, err
	} else {
		// Put this in the translation's parameter bag which is transmitted down to the DB
		s.queryParameters[expansionModel.PrimerQueryParameter.Identifier.String()] = formattedQuery

		// Track this as a function parameter for the harness
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.PrimerQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(forwardFrontRecursiveQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.RecursiveQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.RecursiveQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	return append(harnessParameters, pgsql.NewLiteral(expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth), pgsql.Int)), nil
}

func (s *ExpansionBuilder) bidirectionalAllShortestPathsParameters(expansionModel *Expansion, forwardFrontPrimerQuery pgsql.Select, forwardFrontRecursiveQuery pgsql.Select, backwardFrontPrimerQuery pgsql.Select, backwardFrontRecursiveQuery pgsql.Select) ([]pgsql.Expression, error) {
	var (
		harnessParameters []pgsql.Expression
		formatFragment    = func(query pgsql.Select) (string, error) {
			return format.Statement(
				nextFrontInsert(query),
				format.NewOutputBuilder().WithMaterializedParameters(s.queryParameters))
		}
	)

	if formattedQuery, err := formatFragment(forwardFrontPrimerQuery); err != nil {
		return nil, err
	} else {
		// Put this in the translation's parameter bag which is transmitted down to the DB
		s.queryParameters[expansionModel.PrimerQueryParameter.Identifier.String()] = formattedQuery

		// Track this as a function parameter for the harness
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.PrimerQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(forwardFrontRecursiveQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.RecursiveQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.RecursiveQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(backwardFrontPrimerQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.BackwardPrimerQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.BackwardPrimerQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	if formattedQuery, err := formatFragment(backwardFrontRecursiveQuery); err != nil {
		return nil, err
	} else {
		s.queryParameters[expansionModel.BackwardRecursiveQueryParameter.Identifier.String()] = formattedQuery
		harnessParameters = append(harnessParameters, &pgsql.Parameter{
			Identifier: expansionModel.BackwardRecursiveQueryParameter.Identifier,
			CastType:   pgsql.Text,
		})
	}

	return append(harnessParameters, pgsql.NewLiteral(expansionModel.Options.MaxDepth.GetOr(translateDefaultMaxTraversalDepth), pgsql.Int)), nil
}

func (s *ExpansionBuilder) Build(expansionIdentifier pgsql.Identifier) pgsql.Query {
	query := pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive: true,
		},
		Body: s.ProjectionStatement,
	}

	query.AddCTE(pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  expansionIdentifier,
			Shape: expansionColumns(),
		},
		Query: pgsql.Query{
			Body: pgsql.SetOperation{
				LOperand: s.PrimerStatement,
				ROperand: s.RecursiveStatement,
				Operator: pgsql.OperatorUnion,
			},
		},
	})

	return query
}

func (s *Translator) buildExpansionPatternRoot(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder) (pgsql.Query, error) {
	var (
		traversalStep  = traversalStepContext.CurrentStep
		expansionModel = traversalStep.Expansion
	)

	// Determine local scope of the primer: the edge and both nodes.
	primerLocal, primerExternal := partitionConstraintByLocality(
		expansionModel.PrimerNodeConstraints,
		pgsql.AsIdentifierSet(
			traversalStep.LeftNode.Identifier,
			traversalStep.Edge.Identifier,
			traversalStep.RightNode.Identifier,
		),
	)

	// External terms reference a prior CTE (e.g. s0.i0). Cross-join it into the
	// primer so it is in scope for the base case WHERE clause.
	if primerExternal != nil && traversalStep.Frame.Previous != nil {
		expansion.PrimerStatement.From = append([]pgsql.FromClause{{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
			},
		}}, expansion.PrimerStatement.From...)
	}

	expansion.PrimerStatement.Where = pgsql.OptionalAnd(
		pgsql.OptionalAnd(primerLocal, primerExternal),
		expansionModel.EdgeConstraints,
	)

	expansion.ProjectionStatement.Projection = expansionModel.Projection
	expansion.RecursiveStatement.Where = pgsql.OptionalAnd(expansionModel.EdgeConstraints, expansionModel.RecursiveConstraints)
	expansion.PrimerStatement.Projection = s.buildExpansionPrimerProjection(traversalStep)

	if projection, err := s.buildExpansionRecursiveProjection(traversalStep); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.RecursiveStatement.Projection = projection
	}

	// Craft the from clause
	nextQueryFrom := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
			Binding: models.OptionalValue(traversalStep.Edge.Identifier),
		},
	}

	// If the left node was already bound at time of translation connect this expansion to the
	// previously materialized node
	if traversalStep.LeftNodeBound {
		nextQueryFrom = pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.OptionalValue(traversalStep.Edge.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnStartID},
						pgsql.OperatorEquals,
						rewriteCompositeTypeFieldReference(
							traversalStep.Frame.Previous.Binding.Identifier,
							pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID},
						)),
				},
			}},
		}
	} else if expansionModel.PrimerNodeConstraints != nil {
		// Primer node constraints require a join of of the left node
		nextQueryFrom = pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(traversalStep.Edge.Identifier),
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.LeftNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: traversalStep.Expansion.PrimerNodeJoinCondition,
				},
			}},
		}
	}

	// If there are terminal node constraints then the right node must be joined
	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		nextQueryFrom.Joins = append(nextQueryFrom.Joins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: traversalStep.Expansion.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, nextQueryFrom)

	// Build recursive step joins. The terminal node join is only added when the
	// expansion carries terminal-node constraints, which are the only cases where
	// node columns appear in the recursive body.
	recursiveJoins := []pgsql.Join{{
		Table: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
			Binding: models.OptionalValue(traversalStep.Edge.Identifier),
		},
		JoinOperator: pgsql.JoinOperator{
			JoinType: pgsql.JoinTypeInner,
			Constraint: pgsql.NewBinaryExpression(
				expansionModel.EdgeStartColumn,
				pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
			),
		},
	}}

	if expansionModel.TerminalNodeConstraints != nil {
		recursiveJoins = append(recursiveJoins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.RecursiveStatement.From = append(expansion.RecursiveStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
		},
		Joins: recursiveJoins,
	})

	// The current query part may not have a frame associated with it if is a single part query component
	if traversalStep.Frame.Previous != nil && (s.query.CurrentPart().Frame == nil || traversalStep.Frame.Previous.Binding.Identifier != s.query.CurrentPart().Frame.Binding.Identifier) {
		expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
				Binding: models.EmptyOptional[pgsql.Identifier](),
			},
		})
	}

	// Select the expansion components for the projection statement
	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				),
			},
		}, {
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.RightNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	})

	if projectionConstraints, err := s.buildExpansionProjectionConstraints(traversalStepContext); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.ProjectionStatement.Where = projectionConstraints
	}

	return expansion.Build(expansionModel.Frame.Binding.Identifier), nil
}

func (s *Translator) buildExpansionPatternStep(traversalStepContext TraversalStepContext, expansion *ExpansionBuilder) (pgsql.Query, error) {
	var (
		traversalStep  = traversalStepContext.CurrentStep
		expansionModel = traversalStep.Expansion
	)

	expansion.ProjectionStatement.Projection = expansionModel.Projection
	expansion.PrimerStatement.Where = pgsql.OptionalAnd(expansionModel.PrimerNodeConstraints, expansionModel.EdgeConstraints)
	expansion.RecursiveStatement.Where = pgsql.OptionalAnd(expansionModel.EdgeConstraints, expansionModel.RecursiveConstraints)
	expansion.PrimerStatement.Projection = s.buildExpansionPrimerProjection(traversalStep)

	if projection, err := s.buildExpansionRecursiveProjection(traversalStep); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.RecursiveStatement.Projection = projection
	}

	expansion.PrimerStatement.From = append(expansion.PrimerStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(traversalStep.Edge.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.EdgeJoinCondition,
			},
		}, {
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.ExpansionNodeJoinCondition,
			},
		}},
	})

	// Build recursive step joins. The terminal node join is only added when the
	// expansion carries terminal-node constraints, which are the only cases where
	// node columns appear in the recursive body.
	recursiveJoins := []pgsql.Join{{
		Table: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
			Binding: models.OptionalValue(traversalStep.Edge.Identifier),
		},
		JoinOperator: pgsql.JoinOperator{
			JoinType: pgsql.JoinTypeInner,
			Constraint: pgsql.NewBinaryExpression(
				expansionModel.EdgeStartColumn,
				pgsql.OperatorEquals,
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
			),
		},
	}}

	// If there are terminal node constraints then the right node must be joined
	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		recursiveJoins = append(recursiveJoins, pgsql.Join{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: expansionModel.ExpansionNodeJoinCondition,
			},
		})
	}

	expansion.RecursiveStatement.From = append(expansion.RecursiveStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
		},
		Joins: recursiveJoins,
	})

	// Select the expansion components for the projection statement
	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{traversalStep.Frame.Previous.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
	})

	expansion.ProjectionStatement.From = append(expansion.ProjectionStatement.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier},
			Binding: models.EmptyOptional[pgsql.Identifier](),
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.LeftNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				),
			},
		}, {
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.RightNode.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionNextID},
				),
			},
		}},
	})

	if projectionConstraints, err := s.buildExpansionProjectionConstraints(traversalStepContext); err != nil {
		return pgsql.Query{}, err
	} else {
		expansion.ProjectionStatement.Where = projectionConstraints
	}

	return expansion.Build(expansionModel.Frame.Binding.Identifier), nil
}

func (s *Translator) buildExpansionPrimerProjection(traversalStep *TraversalStep) []pgsql.SelectItem {
	expansionModel := traversalStep.Expansion

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		return []pgsql.SelectItem{
			expansionModel.EdgeStartColumn,
			expansionModel.EdgeEndColumn,
			pgsql.NewLiteral(1, pgsql.Int),
			expansionModel.TerminalNodeSatisfactionProjection,
			pgsql.NewBinaryExpression(
				expansionModel.EdgeStartColumn,
				pgsql.OperatorEquals,
				expansionModel.EdgeEndColumn,
			),
			pgsql.ArrayLiteral{
				Values: []pgsql.Expression{
					pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
				},
			},
		}
	} else {
		return []pgsql.SelectItem{
			expansionModel.EdgeStartColumn,
			expansionModel.EdgeEndColumn,
			pgsql.NewLiteral(1, pgsql.Int),
			pgsql.NewLiteral(false, pgsql.Boolean),
			pgsql.NewBinaryExpression(
				expansionModel.EdgeStartColumn,
				pgsql.OperatorEquals,
				expansionModel.EdgeEndColumn,
			),
			pgsql.ArrayLiteral{
				Values: []pgsql.Expression{
					pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
				},
			},
		}
	}
}

func (s *Translator) buildExpansionRecursiveProjection(traversalStep *TraversalStep) ([]pgsql.SelectItem, error) {
	expansionModel := traversalStep.Expansion

	if expansionModel.TerminalNodeSatisfactionProjection != nil {
		// Split up constraints that can not be satisfied by the local scope of the expansion. This is done to ensure
		// that cross-entity references and other extra-scope comparisons are added external to the expansion frame.
		localSatisfiedConstraint, externalSatisfiedConstraint := partitionConstraintByLocality(
			pgsql.Expression(expansionModel.TerminalNodeSatisfactionProjection),
			pgsql.AsIdentifierSet(
				traversalStep.LeftNode.Identifier,
				traversalStep.Edge.Identifier,
				traversalStep.RightNode.Identifier,
			),
		)

		// Store the external constraints to be inserted during the final projection and where clause
		expansionModel.DeferredNodeSatisfactionConstraint = externalSatisfiedConstraint

		if satisfiedSelectItem, err := pgsql.As[pgsql.SelectItem](localSatisfiedConstraint); err != nil {
			return nil, err
		} else {
			return []pgsql.SelectItem{
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
				expansionModel.EdgeEndColumn,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionDepth},
					pgsql.OperatorAdd,
					pgsql.NewLiteral(1, pgsql.Int),
				),
				satisfiedSelectItem,
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
					pgsql.OperatorEquals,
					pgsql.NewAnyExpression(pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionPath}, pgsql.ExpansionPath),
				),
				pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionPath},
					pgsql.OperatorConcatenate,
					pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
				),
			}, nil
		}
	} else {
		return []pgsql.SelectItem{
			pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionRootID},
			expansionModel.EdgeEndColumn,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionDepth},
				pgsql.OperatorAdd,
				pgsql.NewLiteral(1, pgsql.Int),
			),
			pgsql.NewLiteral(false, pgsql.Boolean),
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
				pgsql.OperatorEquals,
				pgsql.NewAnyExpression(pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionPath}, pgsql.ExpansionPath),
			),
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionPath},
				pgsql.OperatorConcatenate,
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID},
			),
		}, nil
	}
}

func (s *Translator) buildExpansionProjectionConstraints(traversalStepContext TraversalStepContext) (pgsql.Expression, error) {
	var (
		currentStep           = traversalStepContext.CurrentStep
		previousStep          = traversalStepContext.PreviousStep
		expansionModel        = currentStep.Expansion
		projectionConstraints pgsql.Expression
		constraints           *Constraint
		err                   error
		joinCondition         pgsql.Expression
	)

	if previousStep != nil {
		joinCondition = pgd.Equals(
			pgsql.RowColumnReference{
				Identifier: pgsql.CompoundIdentifier{previousStep.Frame.Binding.Identifier, currentStep.LeftNode.Identifier},
				Column:     pgsql.ColumnID,
			},
			pgd.Column(expansionModel.Frame.Binding.Identifier, expansionRootID),
		)
	}

	if constraints, err = s.treeTranslator.ConsumeConstraintsFromVisibleSet(expansionModel.Frame.Visible); err != nil {
		return projectionConstraints, err
	} else {
		// Constraints that target the terminal node may crop up here where it's finally in scope. Additionally,
		// only accept paths that are marked satisfied from the recursive descent CTE
		if expansionModel.TerminalNodeSatisfactionProjection != nil {
			expressions := []pgsql.Expression{
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionSatisfied},
				constraints.Expression,
				joinCondition,
			}

			if projectionConstraints, err = ConjoinExpressions(s.kindMapper, expressions); err != nil {
				return projectionConstraints, err
			}

			// Append any deferred (non-local) constraints onto the projection constraints
			if expansionModel.DeferredNodeSatisfactionConstraint != nil {
				projectionConstraints = pgsql.OptionalAnd(projectionConstraints, expansionModel.DeferredNodeSatisfactionConstraint)
			}
		} else {
			if projectionConstraints, err = ConjoinExpressions(s.kindMapper, []pgsql.Expression{constraints.Expression, joinCondition}); err != nil {
				return projectionConstraints, err
			}
		}
	}

	// Check for min-path depth as this will also filter the final expansion projection
	if expansionModel.Options.MinDepth.Set && expansionModel.Options.MinDepth.Value > 1 {
		projectionConstraints = pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{expansionModel.Frame.Binding.Identifier, expansionDepth},
				pgsql.OperatorGreaterThanOrEqualTo,
				pgsql.NewLiteral(expansionModel.Options.MinDepth.Value, pgsql.Int),
			),
			projectionConstraints,
		)
	}

	return projectionConstraints, nil
}

func (s *Translator) translateTraversalPatternPartWithExpansion(isFirstTraversalStep bool, traversalStep *TraversalStep) error {
	expansionModel := traversalStep.Expansion

	// Translate the expansion's constraints - this has the side effect of making the pattern identifiers visible in
	// the current scope frame
	if err := s.translateExpansionConstraints(isFirstTraversalStep, traversalStep, expansionModel); err != nil {
		return err
	}

	// Export the path from the traversal's scope
	traversalStep.Frame.Export(expansionModel.PathBinding.Identifier)

	// Push a new frame that contains currently projected scope from the expansion recursive CTE
	if expansionFrame, err := s.scope.PushFrame(); err != nil {
		return err
	} else {
		expansionModel.Frame = expansionFrame
	}

	if expansionModel.TerminalNodeConstraints != nil {
		if terminalCriteriaProjection, err := pgsql.As[pgsql.SelectItem](expansionModel.TerminalNodeConstraints); err != nil {
			return err
		} else {
			expansionModel.TerminalNodeSatisfactionProjection = terminalCriteriaProjection
		}
	}

	// Expansion edge join condition
	expansionModel.RecursiveConstraints = expansionConstraints(traversalStep)

	if err := RewriteFrameBindings(s.scope, expansionModel.RecursiveConstraints); err != nil {
		return err
	}

	// Remove the previous projections of the root and terminal node to reproject them after expansion
	traversalStep.LeftNode.Dematerialize()
	traversalStep.RightNode.Dematerialize()

	if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
		return err
	} else {
		// Zip through all projected identifiers and update their last projected frame
		for _, binding := range boundProjections.Bindings {
			binding.MaterializedBy(expansionModel.Frame)
		}

		expansionModel.Projection = boundProjections.Items
	}

	if err := s.scope.PopFrame(); err != nil {
		return err
	}

	if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
		return err
	} else {
		// Zip through all projected identifiers and update their last projected frame
		for _, binding := range boundProjections.Bindings {
			binding.MaterializedBy(traversalStep.Frame)
		}

		traversalStep.Projection = boundProjections.Items
	}

	if expansionModel.Options.FindShortestPath || expansionModel.Options.FindAllShortestPaths {
		if err := s.translateShortestPathTraversal(expansionModel); err != nil {
			return err
		}
	}

	return nil
}

func (s *Translator) translateExpansionConstraints(isFirstTraversalStep bool, step *TraversalStep, expansionModel *Expansion) error {
	if constraints, err := consumePatternConstraints(isFirstTraversalStep, recursivePattern, step, s.treeTranslator); err != nil {
		return err
	} else {
		// If one side of the expansion has constraints but the other does not this may be an opportunity to reorder the traversal
		// to start with tighter search bounds
		if err := constraints.OptimizePatternConstraintBalance(s.scope, step); err != nil {
			return err
		}

		// Left node
		if leftNodeJoinCondition, err := leftNodeTraversalStepConstraint(step); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, leftNodeJoinCondition); err != nil {
			return err
		} else {
			expansionModel.PrimerNodeJoinCondition = leftNodeJoinCondition
		}

		if constraints.LeftNode.Expression != nil {
			if err := RewriteFrameBindings(s.scope, constraints.LeftNode.Expression); err != nil {
				return err
			}

			expansionModel.PrimerNodeConstraints = constraints.LeftNode.Expression

			if primerCriteriaProjection, err := pgsql.As[pgsql.SelectItem](expansionModel.PrimerNodeConstraints); err != nil {
				return err
			} else {
				expansionModel.PrimerNodeSatisfactionProjection = primerCriteriaProjection
			}
		}

		// Expansion edge constraints
		if constraints.Edge.Expression != nil {
			expansionModel.EdgeConstraints = constraints.Edge.Expression

			if err := RewriteFrameBindings(s.scope, expansionModel.EdgeConstraints); err != nil {
				return err
			}
		}

		if !isFirstTraversalStep {
			if edgeJoinCondition, err := expansionEdgeJoinCondition(step); err != nil {
				return err
			} else if err := RewriteFrameBindings(s.scope, edgeJoinCondition); err != nil {
				return err
			} else {
				expansionModel.EdgeJoinCondition = edgeJoinCondition
			}
		}

		// Right node
		if rightNodeJoinCondition, err := rightNodeTraversalStepJoinCondition(step); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, rightNodeJoinCondition); err != nil {
			return err
		} else {
			expansionModel.ExpansionNodeJoinCondition = rightNodeJoinCondition
		}

		if constraints.RightNode.Expression != nil {
			if err := RewriteFrameBindings(s.scope, constraints.RightNode.Expression); err != nil {
				return err
			} else {
				expansionModel.TerminalNodeConstraints = constraints.RightNode.Expression
			}
		}
	}

	return nil
}

func (s *Translator) translateShortestPathTraversal(expansionModel *Expansion) error {
	// If this query is a shortest-path look up, the translator will have to use a function harness for
	// traversal. As such, query fragments for the traversal harness will have to be passed by the parameters
	// defined below.
	if primerQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
		return err
	} else {
		expansionModel.PrimerQueryParameter = primerQueryParameter
	}

	if recursiveQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
		return err
	} else {
		expansionModel.RecursiveQueryParameter = recursiveQueryParameter
	}

	// Bidirectional BFS searches require an additional set of query fragments to represent the backward traversal
	// front of the search.
	if expansionModel.CanExecuteBidirectionalSearch() {
		if reversePrimerQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
			return err
		} else {
			expansionModel.BackwardPrimerQueryParameter = reversePrimerQueryParameter
		}

		if reverseRecursiveQueryParameter, err := s.scope.DefineNew(pgsql.ParameterIdentifier); err != nil {
			return err
		} else {
			expansionModel.BackwardRecursiveQueryParameter = reverseRecursiveQueryParameter
		}
	}

	return nil
}

func (s *Translator) translateNonTraversalPatternPart(part *PatternPart) error {
	if nextFrame, err := s.scope.PushFrame(); err != nil {
		return err
	} else {
		part.NodeSelect.Frame = nextFrame

		nextFrame.Export(part.NodeSelect.Binding.Identifier)

		set := nextFrame.Known().Copy()
		if s.query.CurrentPart().quantifierIdentifiers != nil && s.query.CurrentPart().quantifierIdentifiers.Len() > 0 {
			set = set.MergeSet(s.query.CurrentPart().quantifierIdentifiers)
		}
		if constraint, err := s.treeTranslator.ConsumeConstraintsFromVisibleSet(set); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, constraint.Expression); err != nil {
			return err
		} else {
			part.NodeSelect.Constraints = constraint.Expression
		}

		if boundProjections, err := buildVisibleProjections(s.scope); err != nil {
			return err
		} else {
			// Zip through all projected identifiers and update their last projected frame
			for _, binding := range boundProjections.Bindings {
				binding.MaterializedBy(nextFrame)
			}

			part.NodeSelect.Select.Projection = boundProjections.Items
		}
	}

	return nil
}
