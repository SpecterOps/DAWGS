package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/graph"
)

func (s *Translator) preparePatternPredicate(predicate *cypher.PatternPredicate) error {
	currentQueryPart := s.query.CurrentPart()

	// Stash the match pattern
	currentQueryPart.StashCurrentPattern()

	// All pattern predicates must be relationship patterns
	newPatternPart := currentQueryPart.currentPattern.NewPart()
	newPatternPart.IsTraversal = true
	if target, hasTarget := s.patternPredicateTargets[predicate]; hasTarget {
		newPatternPart.Target = target
		newPatternPart.HasTarget = true
	}

	return nil
}

func (s *Translator) buildOptimizedRelationshipExistPredicate(part *PatternPart, traversalStep *TraversalStep) (pgsql.Expression, error) {
	whereClause := pgsql.NewBinaryExpression(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnStartID},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID}),
		pgsql.OperatorOr,
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnEndID},
			pgsql.OperatorEquals,
			pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID}),
	)

	if err := RewriteFrameBindings(s.scope, whereClause); err != nil {
		return nil, err
	}

	// explain analyze select * from node n0 where not exists(select 1 from edge e0 where e0.start_id = n0.id or e0.end_id = n0.id);
	return pgsql.ExistsExpression{
		Subquery: pgsql.Subquery{
			Query: pgsql.Query{
				Body: pgsql.Select{
					Projection: []pgsql.SelectItem{
						pgsql.NewLiteral(1, pgsql.Int),
					},
					From: []pgsql.FromClause{
						{
							Source: pgsql.TableReference{
								Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
								Binding: models.OptionalValue(traversalStep.Edge.Identifier),
							},
						},
					},
					Where: whereClause,
				},
			},
		},
	}, nil
}

func (s *Translator) translatePatternPredicate() error {
	var (
		currentQueryPart = s.query.CurrentPart()
		patternPredicate = currentQueryPart.ConsumeCurrentPattern()
		predicateFuture  = pgsql.NewFuture[*Pattern](patternPredicate, pgsql.Boolean)
	)

	// Restore the previous match pattern as the current match pattern
	currentQueryPart.RestoreStashedPattern()

	if numPatternParts := len(patternPredicate.Parts); numPatternParts < 1 || numPatternParts > 1 {
		return fmt.Errorf("expected exactly one pattern part for pattern predicate but found: %d", numPatternParts)
	}

	// Push this as an expression for rendering constraints
	s.treeTranslator.PushOperand(predicateFuture)

	// Track this as a predicate to revisit while rendering the patterns
	currentQueryPart.AddPatternPredicateFuture(predicateFuture)
	return nil
}

func (s *Translator) usePatternPredicateExistencePlacement(patternPart *PatternPart, traversalStep *TraversalStep) (bool, error) {
	if patternPart == nil || !patternPart.HasTarget || traversalStep == nil || traversalStep.Direction != graph.DirectionBoth {
		return false, nil
	}

	decision, hasDecision := s.patternPredicateDecisions[patternPart.Target.TraversalStep(0)]
	if !hasDecision || decision.Mode != optimize.PatternPredicatePlacementExistence {
		return false, nil
	}

	traversalStepIdentifiers := pgsql.AsIdentifierSet(
		traversalStep.LeftNode.Identifier,
		traversalStep.Edge.Identifier,
		traversalStep.RightNode.Identifier,
	)

	if hasGlobalConstraints, err := s.treeTranslator.HasAnyConstraints(traversalStepIdentifiers); err != nil {
		return false, err
	} else if hasGlobalConstraints {
		return false, nil
	}

	if hasPredicateConstraints, err := patternPart.Constraints.HasConstraints(traversalStepIdentifiers); err != nil {
		return false, err
	} else if hasPredicateConstraints {
		return false, nil
	}

	return true, nil
}

// buildPatternPredicates is used by translateMatch to resolve deferred pattern predicate
// futures collected for the current MATCH/OPTIONAL MATCH query part's WHERE expressions
func (s *Translator) buildPatternPredicates() error {
	for _, predicateFuture := range s.query.CurrentPart().patternPredicates {
		var (
			lastFrame *Frame

			patternPart = predicateFuture.Data.Parts[0]
			subQuery    = pgsql.Query{
				CommonTableExpressions: &pgsql.With{},
			}
		)

		if len(patternPart.TraversalSteps) == 1 {
			traversalStep := patternPart.TraversalSteps[0]
			if useExistencePlacement, err := s.usePatternPredicateExistencePlacement(patternPart, traversalStep); err != nil {
				return err
			} else if useExistencePlacement {
				if predicateExpression, err := s.buildOptimizedRelationshipExistPredicate(patternPart, traversalStep); err != nil {
					return err
				} else {
					predicateFuture.SyntaxNode = predicateExpression
					s.recordLowering(optimize.LoweringPredicatePlacement)
				}

				return nil
			}
		}

		if err := s.translateTraversalPatternPart(patternPart, true, true); err != nil {
			return err
		}

		for idx, traversalStep := range patternPart.TraversalSteps {
			if traversalStep.Expansion != nil {
				return fmt.Errorf("expansion in pattern predicate not supported")
			}

			if idx > 0 {
				if traversalStepQuery, err := s.buildTraversalPatternStep(traversalStep.Frame, traversalStep); err != nil {
					return err
				} else {
					subQuery.AddCTE(pgsql.CommonTableExpression{
						Alias: pgsql.TableAlias{
							Name: traversalStep.Frame.Binding.Identifier,
						},
						Query: traversalStepQuery,
					})
				}
			} else {
				var (
					traversalStepQuery pgsql.Query
					err                error
				)
				if traversalStep.Direction != graph.DirectionBoth && (traversalStep.LeftNodeBound || traversalStep.RightNodeBound) {
					traversalStepQuery, err = s.buildTraversalPatternRootWithOuterCorrelation(traversalStep.Frame, traversalStep)
				} else {
					traversalStepQuery, err = s.buildTraversalPatternRoot(traversalStep.Frame, traversalStep)
				}

				if err != nil {
					return err
				} else {
					subQuery.AddCTE(pgsql.CommonTableExpression{
						Alias: pgsql.TableAlias{
							Name: traversalStep.Frame.Binding.Identifier,
						},
						Query: traversalStepQuery,
					})
				}
			}

			lastFrame = traversalStep.Frame
		}

		subQuery.Body = pgsql.Select{
			Projection: pgsql.Projection{
				pgsql.NewBinaryExpression(
					pgsql.FunctionCall{
						Function:   pgsql.FunctionCount,
						Parameters: []pgsql.Expression{pgsql.WildcardIdentifier},
					},
					pgsql.OperatorGreaterThan,
					pgsql.NewLiteral(0, pgsql.Int),
				),
			},

			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{lastFrame.Binding.Identifier},
				},
			}},
		}

		predicateFuture.SyntaxNode = pgsql.Subquery{
			Query: subQuery,
		}
	}

	return nil
}
