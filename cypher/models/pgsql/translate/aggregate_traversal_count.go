package translate

import (
	"fmt"
	"reflect"

	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/walk"
	"github.com/specterops/dawgs/graph"
)

const (
	aggregateCandidateSourcesCTE pgsql.Identifier = "candidate_sources"
	aggregateTraversalCTE        pgsql.Identifier = "traversal"
	aggregateTerminalNodesCTE    pgsql.Identifier = "terminal_nodes"
	aggregateTerminalHitsCTE     pgsql.Identifier = "terminal_hits"
	aggregateRankedCTE           pgsql.Identifier = "ranked"

	aggregateSourceAlias   pgsql.Identifier = "source_node"
	aggregateEdgeAlias     pgsql.Identifier = "e"
	aggregateTerminalAlias pgsql.Identifier = "terminal_node"

	aggregateRootID pgsql.Identifier = "root_id"
	aggregateNextID pgsql.Identifier = "next_id"
	aggregateDepth  pgsql.Identifier = "depth"
	aggregatePath   pgsql.Identifier = "path"
	aggregateNodeID pgsql.Identifier = "id"
)

func (s *Translator) translateAggregateTraversalCount(query *cypher.RegularQuery, plan optimize.LoweringPlan) (bool, error) {
	if len(plan.AggregateTraversalCount) == 0 {
		return false, nil
	}

	shape, ok := optimize.AggregateTraversalCountShapeForQuery(query)
	if !ok || shape.Target != plan.AggregateTraversalCount[0].Target {
		return false, nil
	}

	statement, err := s.aggregateTraversalCountQuery(shape)
	if err != nil {
		return false, err
	}

	s.translation.Statement = statement
	s.recordLowering(optimize.LoweringAggregateTraversalCount)
	return true, nil
}

func (s *Translator) newAggregatePredicateTranslator() *Translator {
	translator := NewTranslator(s.ctx, s.kindMapper.kindMapper, s.parameters, s.graphID)
	translator.scope.generator = s.scope.generator

	return translator
}

func (s *Translator) aggregateTraversalCountQuery(shape optimize.AggregateTraversalCountShape) (pgsql.Query, error) {
	predicateTranslator := s.newAggregatePredicateTranslator()

	candidateSources, err := s.buildAggregateCandidateSourcesCTE(shape, predicateTranslator)
	if err != nil {
		return pgsql.Query{}, err
	}

	traversal, err := s.buildAggregateTraversalCTE(shape)
	if err != nil {
		return pgsql.Query{}, err
	}

	terminalNodes, err := s.buildAggregateTerminalNodesCTE(shape, predicateTranslator)
	if err != nil {
		return pgsql.Query{}, err
	}

	if err := s.mergeAggregatePredicateParameters(predicateTranslator); err != nil {
		return pgsql.Query{}, err
	}

	terminalHits, err := s.buildAggregateTerminalHitsCTE(shape)
	if err != nil {
		return pgsql.Query{}, err
	}

	projection := pgsql.Projection{
		pgsql.AliasedExpression{
			Expression: aggregateNodeComposite(aggregateSourceAlias),
			Alias:      pgsql.AsOptionalIdentifier(pgsql.Identifier(shape.ReturnSourceAlias)),
		},
	}
	if shape.ReturnCount {
		projection = append(projection, pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{aggregateRankedCTE, pgsql.Identifier(shape.CountAlias)},
			Alias:      pgsql.AsOptionalIdentifier(pgsql.Identifier(shape.ReturnCountAlias)),
		})
	}

	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive: true,
			Expressions: []pgsql.CommonTableExpression{
				candidateSources,
				traversal,
				terminalNodes,
				terminalHits,
				s.buildAggregateRankedCTE(shape),
			},
		},
		Body: pgsql.Select{
			Projection: projection,
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: aggregateRankedCTE.AsCompoundIdentifier(),
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.TableNode.AsCompoundIdentifier(),
						Binding: pgsql.AsOptionalIdentifier(aggregateSourceAlias),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType: pgsql.JoinTypeInner,
						Constraint: pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{aggregateSourceAlias, pgsql.ColumnID},
							pgsql.OperatorEquals,
							pgsql.CompoundIdentifier{aggregateRankedCTE, aggregateRootID},
						),
					},
				}},
			}},
		},
		OrderBy: []*pgsql.OrderBy{{
			Expression: pgsql.CompoundIdentifier{aggregateRankedCTE, pgsql.Identifier(shape.CountAlias)},
			Ascending:  false,
		}},
	}, nil
}

func (s *Translator) buildAggregateCandidateSourcesCTE(shape optimize.AggregateTraversalCountShape, predicateTranslator *Translator) (pgsql.CommonTableExpression, error) {
	whereClause, err := s.aggregateSourceWhere(shape, predicateTranslator)
	if err != nil {
		return pgsql.CommonTableExpression{}, err
	}

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  aggregateCandidateSourcesCTE,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{aggregateRootID}),
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{
					pgsql.AliasedExpression{
						Expression: pgsql.CompoundIdentifier{aggregateSourceAlias, pgsql.ColumnID},
						Alias:      pgsql.AsOptionalIdentifier(aggregateRootID),
					},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name:    pgsql.TableNode.AsCompoundIdentifier(),
						Binding: pgsql.AsOptionalIdentifier(aggregateSourceAlias),
					},
				}},
				Where: whereClause,
			},
		},
	}, nil
}

func (s *Translator) buildAggregateTraversalCTE(shape optimize.AggregateTraversalCountShape) (pgsql.CommonTableExpression, error) {
	edgeKindConstraint, err := s.aggregateEdgeKindConstraint(aggregateEdgeAlias, shape.RelationshipKinds)
	if err != nil {
		return pgsql.CommonTableExpression{}, err
	}

	sourceColumn, nextColumn := aggregateTraversalColumns(shape.Direction)
	var (
		primerJoin = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{aggregateEdgeAlias, sourceColumn},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{aggregateCandidateSourcesCTE, aggregateRootID},
		)
		recursiveJoin = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{aggregateEdgeAlias, sourceColumn},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateNextID},
		)
	)

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: aggregateTraversalCTE,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{
				aggregateRootID,
				aggregateNextID,
				aggregateDepth,
				aggregatePath,
			}),
		},
		Query: pgsql.Query{
			Body: pgsql.SetOperation{
				Operator: pgsql.OperatorUnion,
				All:      true,
				LOperand: pgsql.Select{
					Projection: pgsql.Projection{
						pgsql.CompoundIdentifier{aggregateCandidateSourcesCTE, aggregateRootID},
						pgsql.CompoundIdentifier{aggregateEdgeAlias, nextColumn},
						pgsql.NewLiteral(int64(1), pgsql.Int8),
						pgsql.ArrayLiteral{
							Values: []pgsql.Expression{
								pgsql.CompoundIdentifier{aggregateEdgeAlias, pgsql.ColumnID},
							},
							CastType: pgsql.Int8Array,
						},
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: aggregateCandidateSourcesCTE.AsCompoundIdentifier(),
						},
						Joins: []pgsql.Join{{
							Table: pgsql.TableReference{
								Name:    pgsql.TableEdge.AsCompoundIdentifier(),
								Binding: pgsql.AsOptionalIdentifier(aggregateEdgeAlias),
							},
							JoinOperator: pgsql.JoinOperator{
								JoinType:   pgsql.JoinTypeInner,
								Constraint: primerJoin,
							},
						}},
					}},
					Where: edgeKindConstraint,
				},
				ROperand: pgsql.Select{
					Projection: pgsql.Projection{
						pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateRootID},
						pgsql.CompoundIdentifier{aggregateEdgeAlias, nextColumn},
						pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateDepth},
							pgsql.OperatorAdd,
							pgsql.NewLiteral(int64(1), pgsql.Int8),
						),
						pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregatePath},
							pgsql.OperatorConcatenate,
							pgsql.CompoundIdentifier{aggregateEdgeAlias, pgsql.ColumnID},
						),
					},
					From: []pgsql.FromClause{{
						Source: pgsql.TableReference{
							Name: aggregateTraversalCTE.AsCompoundIdentifier(),
						},
						Joins: []pgsql.Join{{
							Table: pgsql.LateralSubquery{
								Query: pgsql.Query{
									Body: pgsql.Select{
										Projection: pgsql.Projection{
											pgsql.CompoundIdentifier{aggregateEdgeAlias, pgsql.ColumnID},
											pgsql.CompoundIdentifier{aggregateEdgeAlias, pgsql.ColumnStartID},
											pgsql.CompoundIdentifier{aggregateEdgeAlias, pgsql.ColumnEndID},
										},
										From: []pgsql.FromClause{{
											Source: pgsql.TableReference{
												Name:    pgsql.TableEdge.AsCompoundIdentifier(),
												Binding: pgsql.AsOptionalIdentifier(aggregateEdgeAlias),
											},
										}},
										Where: pgsql.OptionalAnd(
											pgsql.OptionalAnd(
												recursiveJoin,
												pgsql.NewBinaryExpression(
													pgsql.CompoundIdentifier{aggregateEdgeAlias, pgsql.ColumnID},
													pgsql.OperatorNotEquals,
													pgsql.NewAllExpression(pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregatePath}),
												),
											),
											edgeKindConstraint,
										),
									},
									Offset: pgsql.NewLiteral(0, pgsql.Int),
								},
								Binding: pgsql.AsOptionalIdentifier(aggregateEdgeAlias),
							},
							JoinOperator: pgsql.JoinOperator{
								JoinType:   pgsql.JoinTypeInner,
								Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
							},
						}},
					}},
					Where: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateDepth},
						pgsql.OperatorLessThan,
						pgsql.NewLiteral(shape.MaxDepth, pgsql.Int8),
					),
				},
			},
		},
	}, nil
}

func (s *Translator) buildAggregateTerminalHitsCTE(shape optimize.AggregateTraversalCountShape) (pgsql.CommonTableExpression, error) {
	terminalWhere := pgsql.Expression(nil)

	if shape.MinDepth > 1 {
		terminalWhere = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateDepth},
			pgsql.OperatorGreaterThanOrEqualTo,
			pgsql.NewLiteral(shape.MinDepth, pgsql.Int8),
		)
	}

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name:  aggregateTerminalHitsCTE,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{aggregateRootID}),
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{
					pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateRootID},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: aggregateTraversalCTE.AsCompoundIdentifier(),
					},
					Joins: []pgsql.Join{{
						Table: pgsql.TableReference{
							Name: aggregateTerminalNodesCTE.AsCompoundIdentifier(),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{aggregateTerminalNodesCTE, aggregateNodeID},
								pgsql.OperatorEquals,
								pgsql.CompoundIdentifier{aggregateTraversalCTE, aggregateNextID},
							),
						},
					}},
				}},
				Where: terminalWhere,
			},
		},
	}, nil
}

func (s *Translator) buildAggregateTerminalNodesCTE(shape optimize.AggregateTraversalCountShape, predicateTranslator *Translator) (pgsql.CommonTableExpression, error) {
	terminalWhere, err := s.aggregateTerminalWhere(shape, predicateTranslator)
	if err != nil {
		return pgsql.CommonTableExpression{}, err
	}

	return pgsql.CommonTableExpression{
		Materialized: &pgsql.Materialized{Materialized: true},
		Alias: pgsql.TableAlias{
			Name:  aggregateTerminalNodesCTE,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{aggregateNodeID}),
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{
					pgsql.CompoundIdentifier{aggregateTerminalAlias, pgsql.ColumnID},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name:    pgsql.TableNode.AsCompoundIdentifier(),
						Binding: pgsql.AsOptionalIdentifier(aggregateTerminalAlias),
					},
				}},
				Where: terminalWhere,
			},
		},
	}, nil
}

func (s *Translator) buildAggregateRankedCTE(shape optimize.AggregateTraversalCountShape) pgsql.CommonTableExpression {
	countAlias := pgsql.Identifier(shape.CountAlias)

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: aggregateRankedCTE,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{
				aggregateRootID,
				countAlias,
			}),
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{
					pgsql.CompoundIdentifier{aggregateTerminalHitsCTE, aggregateRootID},
					pgsql.AliasedExpression{
						Expression: pgsql.FunctionCall{
							Function:   pgsql.FunctionCount,
							Parameters: []pgsql.Expression{pgsql.Wildcard{}},
							CastType:   pgsql.Int8,
						},
						Alias: pgsql.AsOptionalIdentifier(countAlias),
					},
				},
				From: []pgsql.FromClause{{
					Source: pgsql.TableReference{
						Name: aggregateTerminalHitsCTE.AsCompoundIdentifier(),
					},
				}},
				GroupBy: []pgsql.Expression{
					pgsql.CompoundIdentifier{aggregateTerminalHitsCTE, aggregateRootID},
				},
			},
			OrderBy: []*pgsql.OrderBy{{
				Expression: countAlias,
				Ascending:  false,
			}},
			Limit: pgsql.NewLiteral(shape.Limit, pgsql.Int8),
		},
	}
}

func (s *Translator) aggregateSourceWhere(shape optimize.AggregateTraversalCountShape, predicateTranslator *Translator) (pgsql.Expression, error) {
	sourceKindConstraint, err := s.aggregateNodeKindConstraint(aggregateSourceAlias, shape.SourceKinds)
	if err != nil {
		return nil, err
	}

	sourcePredicate, err := s.aggregateSourcePredicate(shape, predicateTranslator)
	if err != nil {
		return nil, err
	}

	return pgsql.OptionalAnd(sourcePredicate, sourceKindConstraint), nil
}

func (s *Translator) mergeAggregatePredicateParameters(translator *Translator) error {
	for key, value := range translator.translation.Parameters {
		if existingValue, hasExisting := s.translation.Parameters[key]; hasExisting && !reflect.DeepEqual(existingValue, value) {
			return fmt.Errorf("aggregate traversal parameter collision for %s", key)
		}

		s.translation.Parameters[key] = value
	}

	return nil
}

func (s *Translator) aggregateTerminalWhere(shape optimize.AggregateTraversalCountShape, predicateTranslator *Translator) (pgsql.Expression, error) {
	terminalKindConstraint, err := s.aggregateNodeKindConstraint(aggregateTerminalAlias, shape.TerminalKinds)
	if err != nil {
		return nil, err
	}

	terminalPredicate, err := s.aggregateBindingPredicate(predicateTranslator, shape.TerminalMatch, shape.TerminalSymbol, aggregateTerminalAlias)
	if err != nil {
		return nil, err
	}

	return pgsql.OptionalAnd(terminalPredicate, terminalKindConstraint), nil
}

func (s *Translator) aggregateSourcePredicate(shape optimize.AggregateTraversalCountShape, predicateTranslator *Translator) (pgsql.Expression, error) {
	return s.aggregateBindingPredicate(predicateTranslator, shape.SourceMatch, shape.SourceSymbol, aggregateSourceAlias)
}

func (s *Translator) aggregateBindingPredicate(translator *Translator, match *cypher.Match, symbol string, alias pgsql.Identifier) (pgsql.Expression, error) {
	if match == nil || match.Where == nil {
		return nil, nil
	}

	binding := translator.scope.Define(alias, pgsql.NodeComposite)
	translator.scope.Alias(pgsql.Identifier(symbol), binding)

	if err := walk.Cypher(match.Where, translator); err != nil {
		return nil, err
	}

	sourceConstraints, err := translator.treeTranslator.ConsumeConstraintsFromVisibleSet(pgsql.AsIdentifierSet(alias))
	if err != nil {
		return nil, err
	}

	remainingConstraints, err := translator.treeTranslator.ConsumeAllConstraints()
	if err != nil {
		return nil, err
	}
	if remainingConstraints.Expression != nil {
		return nil, fmt.Errorf("unsupported aggregate traversal predicate dependencies: %v", remainingConstraints.Dependencies.Slice())
	}

	return sourceConstraints.Expression, nil
}

func (s *Translator) aggregateNodeKindConstraint(alias pgsql.Identifier, kinds graph.Kinds) (pgsql.Expression, error) {
	if len(kinds) == 0 {
		return nil, nil
	}

	kindIDs, err := s.kindMapper.MapKinds(kinds)
	if err != nil {
		return nil, err
	}

	kindIDsLiteral, err := pgsql.AsLiteral(kindIDs)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{alias, pgsql.ColumnKindIDs},
		pgsql.OperatorPGArrayLHSContainsRHS,
		kindIDsLiteral,
	), nil
}

func (s *Translator) aggregateEdgeKindConstraint(alias pgsql.Identifier, kinds graph.Kinds) (pgsql.Expression, error) {
	if len(kinds) == 0 {
		return nil, nil
	}

	kindIDs, err := s.kindMapper.MapKinds(kinds)
	if err != nil {
		return nil, err
	}

	return pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{alias, pgsql.ColumnKindID},
		pgsql.OperatorEquals,
		pgsql.NewAnyExpressionHinted(pgsql.NewLiteral(kindIDs, pgsql.Int2Array)),
	), nil
}

func aggregateTraversalColumns(direction graph.Direction) (pgsql.Identifier, pgsql.Identifier) {
	switch direction {
	case graph.DirectionInbound:
		return pgsql.ColumnEndID, pgsql.ColumnStartID
	default:
		return pgsql.ColumnStartID, pgsql.ColumnEndID
	}
}

func aggregateNodeComposite(alias pgsql.Identifier) pgsql.CompositeValue {
	return pgsql.CompositeValue{
		Values: []pgsql.Expression{
			pgsql.CompoundIdentifier{alias, pgsql.ColumnID},
			pgsql.CompoundIdentifier{alias, pgsql.ColumnKindIDs},
			pgsql.CompoundIdentifier{alias, pgsql.ColumnProperties},
		},
		DataType: pgsql.NodeComposite,
	}
}
