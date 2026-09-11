package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/graph"
)

// projectedNodeIDReference returns the scalar ID expression exposed for node by frame.
func projectedNodeIDReference(frameIdentifier pgsql.Identifier, binding *BoundIdentifier) pgsql.Expression {
	if binding != nil && binding.IDOnly {
		return pgsql.CompoundIdentifier{frameIdentifier, binding.Identifier}
	}

	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{frameIdentifier, binding.Identifier},
		Column:     pgsql.ColumnID,
	}
}

// boundEndpointIDReference returns the previous-frame scalar ID for a bound traversal endpoint.
func boundEndpointIDReference(frame *Frame, binding *BoundIdentifier) pgsql.Expression {
	return projectedNodeIDReference(frame.Binding.Identifier, binding)
}

// sourceTargetForTraversalStep returns optimizer coordinates for a step that originated in the source query.
func sourceTargetForTraversalStep(part *PatternPart, stepIndex int) (optimize.TraversalStepTarget, bool) {
	if part == nil || stepIndex < 0 || stepIndex >= len(part.TraversalSteps) {
		return optimize.TraversalStepTarget{}, false
	}

	if traversalStep := part.TraversalSteps[stepIndex]; traversalStep != nil && traversalStep.HasSourceTarget {
		return traversalStep.SourceTarget, true
	}

	if !part.HasTarget {
		return optimize.TraversalStepTarget{}, false
	}

	return part.Target.TraversalStep(stepIndex), true
}

// shortestPathExecutorDecision returns the planned physical executor for a source traversal step.
func (s *Translator) shortestPathExecutorDecision(part *PatternPart, stepIndex int) (optimize.ShortestPathExecutorDecision, bool) {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return optimize.ShortestPathExecutorDecision{}, false
	}
	decision, hasDecision := s.shortestPathExecutorDecisions[target]
	return decision, hasDecision
}

// decisionIsForcedShortest reports whether tooling forced a non-incumbent shortest-path executor.
func decisionIsForcedShortest(translator *Translator, target optimize.TraversalStepTarget) bool {
	if translator == nil {
		return false
	}
	decision, found := translator.shortestPathExecutorDecisions[target]
	return found && decision.SelectionMode == "forced_tool"
}

// traversalStepIsFirstForSourceTarget reports whether step is the first translated step for its source target.
func traversalStepIsFirstForSourceTarget(part *PatternPart, stepIndex int) bool {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget || stepIndex == 0 {
		return true
	}

	previousTarget, previousHasTarget := sourceTargetForTraversalStep(part, stepIndex-1)
	return !previousHasTarget || previousTarget != target
}

// traversalStepIsLastForSourceTarget reports whether step is the final translated step for its source target.
func traversalStepIsLastForSourceTarget(part *PatternPart, stepIndex int) bool {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget || stepIndex+1 >= len(part.TraversalSteps) {
		return true
	}

	nextTarget, nextHasTarget := sourceTargetForTraversalStep(part, stepIndex+1)
	return !nextHasTarget || nextTarget != target
}

// shouldUseExpandInto reports whether a planned bound-endpoint traversal applies to this source step.
func (s *Translator) shouldUseExpandInto(part *PatternPart, stepIndex int, traversalStep *TraversalStep) bool {
	if traversalStep == nil || traversalStep.Expansion != nil || !traversalStep.LeftNodeBound || !traversalStep.RightNodeBound {
		return false
	}

	if target, hasTarget := sourceTargetForTraversalStep(part, stepIndex); hasTarget {
		if _, hasDecision := s.expandIntoDecisions[target]; hasDecision {
			return true
		}

		return false
	}

	return true
}

// traversalDirectionDecision returns the planned direction choice for a source traversal step.
func (s *Translator) traversalDirectionDecision(part *PatternPart, stepIndex int) (optimize.TraversalDirectionDecision, bool) {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return optimize.TraversalDirectionDecision{}, false
	}

	decision, hasDecision := s.traversalDirectionDecisions[target]
	return decision, hasDecision
}

// applyPatternConstraintBalance swaps endpoint constraints and reverses path state when the plan flips traversal direction.
func (s *Translator) applyPatternConstraintBalance(part *PatternPart, stepIndex int, constraints *PatternConstraints, traversalStep *TraversalStep) error {
	if decision, hasDecision := s.traversalDirectionDecision(part, stepIndex); hasDecision {
		if decision.Flip {
			if traversalStep.LeftNodeBound {
				if traversalStep.Expansion == nil || !traversalStep.hasPreviousFrameBinding() {
					return nil
				}
			} else if traversalStep.RightNodeBound && !traversalStep.hasPreviousFrameBinding() {
				return nil
			}

			traversalStep.FlipNodes()
			constraints.FlipNodes()
			s.recordLowering(optimize.LoweringTraversalDirection)
		}

		return nil
	}

	if flipped, err := constraints.OptimizePatternConstraintBalance(s.scope, traversalStep); err != nil {
		return err
	} else if flipped {
		s.recordLowering(optimize.LoweringTraversalDirection)
	}

	return nil
}

// shortestPathStrategyDecision returns the planned unidirectional or bidirectional strategy for a source step.
func (s *Translator) shortestPathStrategyDecision(part *PatternPart, stepIndex int) (optimize.ShortestPathStrategyDecision, bool) {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return optimize.ShortestPathStrategyDecision{}, false
	}

	decision, hasDecision := s.shortestPathStrategyDecisions[target]
	return decision, hasDecision
}

// useBidirectionalShortestPathStrategy reports whether a qualified plan selects bidirectional search for step.
func (s *Translator) useBidirectionalShortestPathStrategy(part *PatternPart, stepIndex int, traversalStep *TraversalStep) (bool, error) {
	if decision, hasDecision := s.shortestPathStrategyDecision(part, stepIndex); hasDecision {
		if decision.Strategy != optimize.ShortestPathStrategyBidirectional {
			return false, nil
		}

		if canExecute, err := traversalStep.CanExecutePairAwareBidirectionalSearch(s.scope); err != nil {
			return false, err
		} else if canExecute {
			s.recordLowering(optimize.LoweringShortestPathStrategy)
			return true, nil
		}

		return false, nil
	}

	if canExecute, err := traversalStep.CanExecutePairAwareBidirectionalSearch(s.scope); err != nil {
		return false, err
	} else if canExecute {
		s.recordLowering(optimize.LoweringShortestPathStrategy)
		return true, nil
	}

	return false, nil
}

// shortestPathFilterDecisionsForStep returns every planned filter materialization for a source traversal step.
func (s *Translator) shortestPathFilterDecisionsForStep(part *PatternPart, stepIndex int) []optimize.ShortestPathFilterDecision {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return nil
	}

	return s.shortestPathFilterDecisions[target]
}

// applyShortestPathFilterMaterialization enables terminal or endpoint-pair filters selected for the source step.
func (s *Translator) applyShortestPathFilterMaterialization(part *PatternPart, stepIndex int, traversalStep *TraversalStep, expansionModel *Expansion) {
	for _, decision := range s.shortestPathFilterDecisionsForStep(part, stepIndex) {
		switch decision.Mode {
		case optimize.ShortestPathFilterTerminal:
			if canMaterializeTerminalFilterForStep(traversalStep, expansionModel) {
				expansionModel.UseMaterializedTerminalFilter = true
				s.recordLowering(optimize.LoweringShortestPathFilter)
			}

		case optimize.ShortestPathFilterEndpointPair:
			if expansionModel.UseBidirectionalSearch && canMaterializeEndpointPairFilterForStep(traversalStep, expansionModel) {
				expansionModel.UseMaterializedEndpointPairFilter = true
				s.recordLowering(optimize.LoweringShortestPathFilter)
			}
		}
	}
}

// hasLimitPushdownDecision reports whether target has the requested limit-pushdown mode.
func (s *Translator) hasLimitPushdownDecision(part *PatternPart, stepIndex int, mode optimize.LimitPushdownMode) bool {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return true
	}

	for _, decision := range s.limitPushdownDecisions[target] {
		if decision.Mode == mode {
			return true
		}
	}

	return false
}

// allowLimitPushdownForStep authorizes the step's frame to consume a matching planned limit internally.
func (s *Translator) allowLimitPushdownForStep(part *PatternPart, stepIndex int, traversalStep *TraversalStep) {
	if traversalStep == nil || traversalStep.Frame == nil {
		return
	}
	if traversalStep.Expansion != nil && traversalStep.Expansion.Options.FindAllShortestPaths {
		return
	}

	mode := optimize.LimitPushdownTraversalCTE
	if traversalStep.Expansion != nil &&
		traversalStep.Expansion.Options.FindShortestPath &&
		!traversalStep.Expansion.Options.FindAllShortestPaths {
		mode = optimize.LimitPushdownShortestPathHarness
	}

	if s.hasLimitPushdownDecision(part, stepIndex, mode) {
		s.query.CurrentPart().AllowLimitPushdown(traversalStep.Frame.Binding.Identifier)
	}
}

// buildBoundEndpointTraversalPattern emits a one-hop join between two endpoints already visible in the previous frame.
func (s *Translator) buildBoundEndpointTraversalPattern(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if partFrame == nil || partFrame.Previous == nil {
		return pgsql.Query{}, errors.New("expected previous frame for bound endpoint traversal")
	}

	var (
		previousFrame  = partFrame.Previous
		edgeConstraint = pgsql.OptionalAnd(
			traversalStep.EdgeJoinCondition,
			traversalStep.RightNodeJoinCondition,
		)
		nextSelect = pgsql.Select{
			Projection: traversalStep.Projection,
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
				},
				Joins: []pgsql.Join{{
					Table: pgsql.TableReference{
						Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
						Binding: models.OptionalValue(traversalStep.Edge.Identifier),
					},
					JoinOperator: pgsql.JoinOperator{
						JoinType:   pgsql.JoinTypeInner,
						Constraint: edgeConstraint,
					},
				}},
			}},
		}
	)
	if traversalStep.Direction == graph.DirectionBoth {
		edgeConstraint = buildDirectionlessPairwiseEdgeConstraintForRefs(
			boundEndpointIDReference(previousFrame, traversalStep.LeftNode),
			boundEndpointIDReference(previousFrame, traversalStep.RightNode),
			traversalStep.Edge.Identifier,
		)
		nextSelect.From[0].Joins[0].JoinOperator.Constraint = edgeConstraint
	}
	if referencesUnwind, err := expressionReferencesUnwindBinding(edgeConstraint, s.query.CurrentPart().unwindClauses); err != nil {
		return pgsql.Query{}, err
	} else if referencesUnwind {
		// An UNWIND alias is appended as a comma source after this builder
		// returns. PostgreSQL JOIN ... ON cannot see a later comma source, while
		// WHERE can see the complete FROM list. Keep the exact pair predicate
		// and edge scan together in that shared scope.
		edgeJoin := nextSelect.From[0].Joins[0]
		nextSelect.From[0].Joins = nil
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{Source: edgeJoin.Table})
		nextSelect.Where = pgsql.OptionalAnd(edgeConstraint, nextSelect.Where)
	}

	nextSelect.Where = pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.RightNodeConstraints, nextSelect.Where)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

// buildTraversalPatternRootWithOuterCorrelation constructs a traversal pattern root, preserving the correlation to
// the outer query part's context
func (s *Translator) buildTraversalPatternRootWithOuterCorrelation(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.Direction == graph.DirectionBoth {
		return s.buildDirectionlessTraversalPatternRootWithOuterCorrelation(traversalStep)
	}

	var (
		// Partition right-node constraints: only locally-scoped terms go into JOIN ON.
		// Constraints that reference comma-connected CTEs (e.g. s0.i0 from a prior WITH)
		// must remain in WHERE — they are out of scope inside an explicit JOIN chain.
		rightJoinLocal, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

		nextSelect = pgsql.Select{
			Projection: traversalStep.Projection,
		}
	)

	if traversalStep.LeftNodeBound && traversalStep.RightNodeBound {
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(traversalStep.Edge.Identifier),
			},
		})

		// Both nodes of the traversal are fully bound by the outer query and the frame bindings
		// will have been rewritten to reference the outer CTEs here, so we don't need any JOINs
		// and can use those conditions inside of the inner WHERE to correlate the result set.
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.RightNodeConstraints, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.LeftNodeJoinCondition, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.RightNodeJoinCondition, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)

		return pgsql.Query{
			Body: nextSelect,
		}, nil
	} else if traversalStep.LeftNodeBound {
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(traversalStep.Edge.Identifier),
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition),
				},
			}},
		})

		nextSelect.Where = pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.LeftNodeJoinCondition, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

		return pgsql.Query{
			Body: nextSelect,
		}, nil
	} else if traversalStep.RightNodeBound {
		// Right node was already materialized in a previous frame.
		//
		// We have to promote that frame to the explicit JOIN root so that RightNodeJoinCondition can reference
		// it in the ON clause. PostgreSQL forbids referencing a comma-joined table inside a subsequent
		// explicit JOIN's ON clause.
		leftJoinLocal, leftJoinExternal := partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)

		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
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
					Constraint: pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition),
				},
			}},
		})

		nextSelect.Where = pgsql.OptionalAnd(rightJoinLocal, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.RightNodeJoinCondition, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(leftJoinExternal, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

		return pgsql.Query{
			Body: nextSelect,
		}, nil
	} else {
		// There is nothing to do to preserve outer bounds correlation - do the unbound traversal step
		return s.buildTraversalPatternRoot(partFrame, traversalStep)
	}
}

// buildTraversalPatternRoot emits the first node source, constraints, and projection for a traversal pattern.
func (s *Translator) buildTraversalPatternRoot(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.Direction == graph.DirectionBoth {
		return s.buildDirectionlessTraversalPatternRoot(traversalStep)
	}

	// Dual-bound fixed hops must always use the exact pair join. The optimizer
	// decision records and measures this shape, but correctness must not depend
	// on that analysis recognizing every supported binding source.
	if traversalStep.UseExpandInto || (traversalStep.LeftNodeBound && traversalStep.RightNodeBound) {
		return s.buildBoundEndpointTraversalPattern(partFrame, traversalStep)
	}

	var (
		// Partition right-node constraints: only locally-scoped terms go into JOIN ON.
		// Constraints that reference comma-connected CTEs (e.g. s0.i0 from a prior WITH)
		// must remain in WHERE — they are out of scope inside an explicit JOIN chain.
		rightJoinLocal, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

		nextSelect = pgsql.Select{
			Projection: traversalStep.Projection,
		}
	)

	if traversalStep.LeftNodeBound {
		if partFrame.Previous == nil {
			return pgsql.Query{}, fmt.Errorf("left node is marked as bound but there is no previous frame to reference")
		}

		// prevFrame is the JOIN root here (not comma-connected), so LeftNodeConstraints
		// can safely reference it. No partitioning needed for this branch.
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{partFrame.Previous.Binding.Identifier},
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.OptionalValue(traversalStep.Edge.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, traversalStep.LeftNodeJoinCondition),
				},
			}, {
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition),
				},
			}},
		})
	} else if traversalStep.RightNodeBound && partFrame.Previous == nil {
		// Self-referential pattern: the right node reuses the left node's variable (e.g. (u)-[]->(u)).
		// There is no previous frame to promote as a FROM source. Join only the left node table and
		// push the right-node join condition into WHERE so that start_id and end_id both reference
		// the same node.
		leftJoinLocal, leftJoinExternal := partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)

		if previousFrame, hasPrevious := s.previousFrameTraversalSource(traversalStep); hasPrevious {
			nextSelect.From = append(nextSelect.From, pgsql.FromClause{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
				},
			})
		}

		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
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
					Constraint: pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition),
				},
			}},
		})

		// The right node's join condition (e.g. n0.id = e0.end_id) goes to WHERE since
		// both endpoints reference the same node binding.
		nextSelect.Where = pgsql.OptionalAnd(traversalStep.RightNodeJoinCondition, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(leftJoinExternal, nextSelect.Where)
	} else if traversalStep.RightNodeBound {
		// Right node was already materialized in a previous frame.
		//
		// We have to promote that frame to the explicit JOIN root so that RightNodeJoinCondition can reference
		// it in the ON clause. PostgreSQL forbids referencing a comma-joined table inside a subsequent
		// explicit JOIN's ON clause.
		leftJoinLocal, leftJoinExternal := partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)

		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{partFrame.Previous.Binding.Identifier},
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.OptionalValue(traversalStep.Edge.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition),
				},
			}, {
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.LeftNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition),
				},
			}},
		})

		nextSelect.Where = pgsql.OptionalAnd(leftJoinExternal, nextSelect.Where)
	} else {
		// In this branch prevFrame is comma-separated, so only {e0, n1} are in scope
		// for n1's JOIN ON condition.
		leftJoinLocal, leftJoinExternal := partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)

		if previousFrame, hasPrevious := s.previousFrameTraversalSource(traversalStep); hasPrevious {
			nextSelect.From = append(nextSelect.From, pgsql.FromClause{
				Source: pgsql.TableReference{
					Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
				},
			})
		}

		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
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
					Constraint: pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition),
				},
			}, {
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition),
				},
			}},
		})

		// External left-node constraints go into WHERE.
		nextSelect.Where = pgsql.OptionalAnd(leftJoinExternal, nextSelect.Where)
	}

	// For an inner join, PostgreSQL's optimizer can push start and end predicates into the join if they're part
	// of the where clause below, but it requires additional planning work and may not do so reliably when multiple
	// CTEs are involved or the planner's cost model is off.
	//
	// Emitting them directly in the JOIN ON constraint makes the intent unambiguous and enables the planner to
	// apply the GIN kind index during the join, before materializing the intermediate result.
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

// buildTraversalPatternStep emits one relationship join, terminal node join, constraints, and projection frame.
func (s *Translator) buildTraversalPatternStep(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	// Keep the dual-bound semantic fallback independent of optimizer coverage;
	// otherwise a missed decision can introduce an uncorrelated terminal-node
	// join and multiply the outer bag.
	if traversalStep.UseExpandInto || (traversalStep.LeftNodeBound && traversalStep.RightNodeBound) {
		return s.buildBoundEndpointTraversalPattern(partFrame, traversalStep)
	}

	nextSelect := pgsql.Select{
		Projection: traversalStep.Projection,
	}

	if partFrame.Previous != nil {
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{partFrame.Previous.Binding.Identifier},
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
					Binding: models.OptionalValue(traversalStep.Edge.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: traversalStep.EdgeJoinCondition,
				},
			}, {
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(traversalStep.RightNodeConstraints, traversalStep.RightNodeJoinCondition),
				},
			}},
		})
	} else {
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(traversalStep.Edge.Identifier),
			},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{
					Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
					Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
				},
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.OptionalAnd(traversalStep.RightNodeConstraints, traversalStep.RightNodeJoinCondition),
				},
			}},
		})
	}

	// Append only edge constraints to the where clause.
	//
	// For an inner join, PostgreSQL's optimizer can push start and end predicates into the join if they're part
	// of the where clause below, but it requires additional planning work and may not do so reliably when multiple
	// CTEs are involved or the planner's cost model is off.
	//
	// Emitting them directly in the JOIN ON constraint makes the intent unambiguous and enables the planner to
	// apply the GIN kind index during the join, before materializing the intermediate result.
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

// translateTraversalPatternPart prepares source targets, constraints, and state for translating one pattern part.
func (s *Translator) translateTraversalPatternPart(part *PatternPart, isolatedProjection bool, allowProjectionPruning bool) error {
	var scopeSnapshot *Scope

	if isolatedProjection {
		scopeSnapshot = s.scope.Snapshot()
	}

	for idx, traversalStep := range part.TraversalSteps {
		if traversalStep.UseExpandInto = s.shouldUseExpandInto(part, idx, traversalStep); traversalStep.UseExpandInto {
			s.recordLowering(optimize.LoweringExpandIntoDetection)
		}

		// The optimizer reversed this pattern's element order and relationship directions so the
		// traversal is driven from the terminal endpoint inward. Each expansion accumulates its
		// edges in that reversed walk order, so mark the step path-reversed to restore the
		// original within-segment edge order for a bound path.
		if part.PathDirectionReversed && traversalStep.Expansion != nil {
			traversalStep.PathReversed = true
		}

		s.prepareProjectionPruning(part, idx, traversalStep)

		if traversalStepFrame, err := s.scope.PushFrame(); err != nil {
			return err
		} else {
			// Assign the new scope frame to the traversal step
			traversalStep.Frame = traversalStepFrame
		}

		if traversalStep.Expansion != nil {
			if err := s.translateTraversalPatternPartWithExpansion(part, idx, idx == 0, traversalStep, allowProjectionPruning); err != nil {
				return err
			}
		} else if part.AllShortestPaths || part.ShortestPath {
			return fmt.Errorf("expected shortest path search to utilize variable expansion: ()-[*..]->()")
		} else if err := s.translateTraversalPatternPartWithoutExpansion(part, idx, traversalStep, allowProjectionPruning); err != nil {
			return err
		}
	}

	if applied, err := s.applyExpansionSuffixPushdown(part); err != nil {
		return err
	} else if applied > 0 {
		s.recordLowering(optimize.LoweringExpansionSuffixPushdown)
	}

	if isolatedProjection {
		s.scope = scopeSnapshot
	}

	return nil
}

// applyExpansionSuffixPushdown attaches planned fixed-suffix predicates and records any applied predicate placement.
func (s *Translator) applyExpansionSuffixPushdown(part *PatternPart) (int, error) {
	if part == nil || !part.HasTarget {
		return applyExpansionSuffixPushdown(part)
	}

	var applied int
	for stepIndex := range part.TraversalSteps {
		target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
		if !hasTarget {
			continue
		}

		decisions := s.suffixPushdownDecisions[target]

		if len(decisions) == 0 {
			continue
		}

		for _, decision := range decisions {
			if decision.SuffixLength <= 0 ||
				!decision.ApplySupplemental ||
				decision.SuffixStartStep <= target.StepIndex ||
				decision.SuffixEndStep < decision.SuffixStartStep ||
				decision.SuffixEndStep-decision.SuffixStartStep+1 != decision.SuffixLength {
				continue
			}

			var (
				suffixStartTarget = target
				suffixEndTarget   = target
			)
			suffixStartTarget.StepIndex = decision.SuffixStartStep
			suffixEndTarget.StepIndex = decision.SuffixEndStep

			suffixStartIndex, suffixEndIndex := -1, -1
			for candidateIndex := range part.TraversalSteps {
				candidateTarget, candidateHasTarget := sourceTargetForTraversalStep(part, candidateIndex)
				if !candidateHasTarget {
					continue
				}

				if candidateTarget == suffixStartTarget && suffixStartIndex < 0 {
					suffixStartIndex = candidateIndex
				}
				if candidateTarget == suffixEndTarget {
					suffixEndIndex = candidateIndex
				}
			}
			if suffixStartIndex < 0 || suffixEndIndex < suffixStartIndex {
				continue
			}

			var (
				currentStep = part.TraversalSteps[stepIndex]
				suffixSteps = part.TraversalSteps[suffixStartIndex : suffixEndIndex+1]
			)

			if candidateApplied, err := applyExpansionSuffixPushdownCandidate(currentStep, suffixSteps); err != nil {
				return applied, err
			} else if candidateApplied {
				if len(decision.PredicateAttachments) > 0 {
					s.recordLowering(optimize.LoweringPredicatePlacement)
				}

				applied++
			}
		}
	}

	return applied, nil
}

// traversalStepHasContinuation reports whether another translated step follows in the pattern part.
func traversalStepHasContinuation(part *PatternPart, stepIndex int) bool {
	return part != nil && stepIndex+1 < len(part.TraversalSteps)
}

// fieldRequirementAllowsIDOnly reports whether all external uses of symbol can consume a scalar entity ID.
func fieldRequirementAllowsIDOnly(decision optimize.FieldRequirementDecision) bool {
	observesID := false
	for _, use := range decision.Uses {
		for _, field := range use.Fields {
			if !use.Internal && field == optimize.FieldRequirementEntityID {
				observesID = true
			}

			if !use.Internal && field != optimize.FieldRequirementEntityID {
				return false
			}

			if field == optimize.FieldRequirementFullEntity || field == optimize.FieldRequirementFullPath {
				return false
			}
		}
	}

	return observesID
}

// fieldRequirementAllowsIDOnlyContinuation reports whether later pattern use can continue from scalar ID state.
func fieldRequirementAllowsIDOnlyContinuation(decision optimize.FieldRequirementDecision) bool {
	for _, use := range decision.Uses {
		for _, field := range use.Fields {
			if field == optimize.FieldRequirementFullEntity || field == optimize.FieldRequirementFullPath {
				return false
			}

			if !use.Internal && field != optimize.FieldRequirementEntityID {
				return false
			}
		}
	}

	return true
}

// traversalStepContinuesFromBinding reports whether the next step starts from binding.
func traversalStepContinuesFromBinding(part *PatternPart, stepIndex int, binding *BoundIdentifier) bool {
	if part == nil || binding == nil || stepIndex < 0 || stepIndex+1 >= len(part.TraversalSteps) {
		return false
	}

	currentStep := part.TraversalSteps[stepIndex]
	nextStep := part.TraversalSteps[stepIndex+1]

	return currentStep != nil && nextStep != nil &&
		currentStep.RightNode == binding && nextStep.LeftNode == binding
}

// applyIDOnlyNodeProjection replaces an eligible node composite projection with its scalar ID.
func (s *Translator) applyIDOnlyNodeProjection(part *PatternPart, stepIndex int, binding *BoundIdentifier) bool {
	if part == nil || binding == nil || !part.HasTarget {
		return false
	}

	var (
		isContinuation = traversalStepContinuesFromBinding(part, stepIndex, binding)
		isTerminal     = !traversalStepHasContinuation(part, stepIndex)
	)
	if !isContinuation && !isTerminal {
		return false
	}

	if part.PatternBinding != nil {
		for _, pathSymbol := range s.scope.Symbols(part.PatternBinding) {
			if decision, found := s.fieldRequirementDecisions[part.Target.QueryPartIndex][pathSymbol.String()]; found {
				for _, field := range decision.Fields {
					if field == optimize.FieldRequirementFullPath {
						return false
					}
				}
			}
		}
	}

	foundDecision := false
	for _, symbol := range s.scope.Symbols(binding) {
		if decision, found := s.fieldRequirementDecisions[part.Target.QueryPartIndex][symbol.String()]; found {
			foundDecision = true
			allowsIDOnly := fieldRequirementAllowsIDOnly(decision)
			if isContinuation {
				allowsIDOnly = fieldRequirementAllowsIDOnlyContinuation(decision)
			}

			if !allowsIDOnly {
				return false
			}
		}
	}
	if foundDecision {
		binding.IDOnly = true
		return true
	}

	// Anonymous or otherwise unobserved intermediate nodes have no source-level
	// field-requirement decision. Their identity is still required to join the
	// next relationship, so carry that identity as a scalar between steps.
	if isContinuation && !foundDecision {
		binding.IDOnly = true
		return true
	}

	return false
}

// relationshipIDReference returns the scalar relationship ID exposed by a composite or ID-only binding.
func relationshipIDReference(scope *Scope, binding *BoundIdentifier) pgsql.Expression {
	if binding != nil && binding.DataType == pgsql.EdgeComposite {
		return pathCompositeColumnReference(scope, binding, pgsql.ColumnID)
	}

	return pathEdgeIDReference(scope, binding)
}

// relationshipIDNotInPath builds the edge-uniqueness predicate for a relationship and accumulated path.
func relationshipIDNotInPath(edgeID, pathIDs pgsql.Expression) pgsql.Expression {
	return pgsql.NewBinaryExpression(
		edgeID,
		pgsql.OperatorNotEquals,
		pgsql.NewAllExpression(pathIDs),
	)
}

// previousRelationshipUniquenessConstraint excludes a relationship ID already used by a prior fixed step.
func previousRelationshipUniquenessConstraint(scope *Scope, part *PatternPart, stepIndex int, traversalStep *TraversalStep) pgsql.Expression {
	if scope == nil || part == nil || stepIndex <= 0 || traversalStep == nil || traversalStep.Edge == nil {
		return nil
	}

	var (
		currentEdgeID pgsql.Expression = pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnID}
		constraint    pgsql.Expression
	)

	for _, previousStep := range part.TraversalSteps[:stepIndex] {
		if previousStep == nil || previousStep.Edge == nil {
			continue
		}

		if previousStep.Expansion != nil {
			if previousStep.Expansion.PathBinding != nil {
				constraint = pgsql.OptionalAnd(
					constraint,
					relationshipIDNotInPath(currentEdgeID, pathBindingReference(scope, previousStep.Expansion.PathBinding)),
				)
			}

			continue
		}

		constraint = pgsql.OptionalAnd(
			constraint,
			pgsql.NewBinaryExpression(
				currentEdgeID,
				pgsql.OperatorNotEquals,
				relationshipIDReference(scope, previousStep.Edge),
			),
		)
	}

	return constraint
}

// expansionPreviousRelationshipUniquenessConstraint enforces Cypher relationship uniqueness for an
// expansion step against any preceding fixed steps. Where previousRelationshipUniquenessConstraint
// handles a fixed step that follows an expansion, this handles the mirrored ordering (a fixed step
// that precedes an expansion, e.g. after the optimizer reverses a pattern) by requiring that none
// of the preceding fixed relationships appear in the expansion's accumulated path.
func expansionPreviousRelationshipUniquenessConstraint(scope *Scope, part *PatternPart, stepIndex int, traversalStep *TraversalStep) pgsql.Expression {
	if scope == nil || part == nil || stepIndex <= 0 || traversalStep == nil ||
		traversalStep.Expansion == nil || traversalStep.Expansion.Frame == nil {
		return nil
	}

	var (
		pathIDs = pgsql.CompoundIdentifier{traversalStep.Expansion.Frame.Binding.Identifier, expansionPath}

		constraint pgsql.Expression
	)

	for _, previousStep := range part.TraversalSteps[:stepIndex] {
		if previousStep == nil || previousStep.Edge == nil || previousStep.Expansion != nil {
			continue
		}

		constraint = pgsql.OptionalAnd(
			constraint,
			relationshipIDNotInPath(relationshipIDReference(scope, previousStep.Edge), pathIDs),
		)
	}

	return constraint
}

// projectionPruningDecision returns the planned omitted fields for a source traversal step.
func (s *Translator) projectionPruningDecision(part *PatternPart, stepIndex int) (optimize.ProjectionPruningDecision, bool) {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return optimize.ProjectionPruningDecision{}, false
	}

	decision, hasDecision := s.projectionPruningDecisions[target]
	return decision, hasDecision
}

// prepareProjectionPruning builds the SQL model fragment responsible for prepare projection pruning.
func (s *Translator) prepareProjectionPruning(part *PatternPart, stepIndex int, traversalStep *TraversalStep) {
	decision, hasDecision := s.projectionPruningDecision(part, stepIndex)
	if !hasDecision || traversalStep == nil {
		return
	}

	if decision.OmitLeftNode && traversalStepIsFirstForSourceTarget(part, stepIndex) {
		traversalStep.ProjectionPruning.LeftNode = traversalStep.LeftNode
	}

	if decision.OmitRelationship {
		traversalStep.ProjectionPruning.Relationship = traversalStep.Edge
	}

	if decision.OmitRightNode && traversalStepIsLastForSourceTarget(part, stepIndex) {
		traversalStep.ProjectionPruning.RightNode = traversalStep.RightNode
	}

	if decision.OmitPathBinding && traversalStep.Expansion != nil {
		traversalStep.ProjectionPruning.PathBinding = traversalStep.Expansion.PathBinding
	}
}

// latePathMaterializationDecision returns the requested deferred materialization mode for target.
func (s *Translator) latePathMaterializationDecision(part *PatternPart, stepIndex int, mode optimize.LatePathMaterializationMode) (optimize.LatePathMaterializationDecision, bool) {
	target, hasTarget := sourceTargetForTraversalStep(part, stepIndex)
	if !hasTarget {
		return optimize.LatePathMaterializationDecision{}, false
	}

	for _, decision := range s.latePathDecisions[target] {
		if decision.Mode == mode {
			return decision, true
		}
	}

	return optimize.LatePathMaterializationDecision{}, false
}

// applyPathEdgeIDMaterialization replaces a path binding with ordered edge-ID state for later hydration.
func (s *Translator) applyPathEdgeIDMaterialization(part *PatternPart, stepIndex int, traversalStep *TraversalStep) bool {
	if traversalStep == nil ||
		traversalStep.Edge == nil ||
		traversalStep.Edge.DataType != pgsql.EdgeComposite {
		return false
	}

	if _, hasDecision := s.latePathMaterializationDecision(part, stepIndex, optimize.LatePathMaterializationPathEdgeID); !hasDecision {
		return false
	}

	traversalStep.Edge.DataType = pgsql.PathEdge
	return true
}

// unexportFrameBinding removes binding and its alias from a frame's exported identifiers.
func unexportFrameBinding(frame *Frame, identifier pgsql.Identifier) bool {
	if frame == nil {
		return false
	}

	exported := frame.Exported.Contains(identifier)
	frame.Unexport(identifier)
	return exported
}

// traversalStepBindingBound reports whether binding is an endpoint or relationship already bound for step.
func traversalStepBindingBound(traversalStep *TraversalStep, binding *BoundIdentifier) bool {
	if traversalStep == nil || binding == nil {
		return false
	}

	if traversalStep.LeftNode == binding {
		return traversalStep.LeftNodeBound
	}

	if traversalStep.RightNode == binding {
		return traversalStep.RightNodeBound
	}

	return false
}

// unexportPrunedNodeBinding removes a pruned node and its aliases unless another step still requires the binding.
func unexportPrunedNodeBinding(traversalStep *TraversalStep, binding *BoundIdentifier) bool {
	if binding == nil || traversalStepBindingBound(traversalStep, binding) {
		return false
	}

	return unexportFrameBinding(traversalStep.Frame, binding.Identifier)
}

// pruneTraversalStepProjectionExports removes planned node, relationship, and path exports from a fixed step.
func pruneTraversalStepProjectionExports(part *PatternPart, stepIndex int, traversalStep *TraversalStep) bool {
	var applied bool

	applied = unexportPrunedNodeBinding(traversalStep, traversalStep.ProjectionPruning.LeftNode) || applied
	if traversalStep.ProjectionPruning.Relationship != nil && !traversalStepHasContinuation(part, stepIndex) {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.ProjectionPruning.Relationship.Identifier) || applied
	}
	applied = unexportPrunedNodeBinding(traversalStep, traversalStep.ProjectionPruning.RightNode) || applied

	return applied
}

// pruneExpansionStepProjectionExports removes planned node, relationship, and path exports from an expansion step.
func pruneExpansionStepProjectionExports(part *PatternPart, stepIndex int, traversalStep *TraversalStep) bool {
	if traversalStep == nil || traversalStep.Expansion == nil {
		return false
	}

	var applied bool
	if traversalStep.ProjectionPruning.Relationship != nil {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.ProjectionPruning.Relationship.Identifier) || applied
	}

	if traversalStep.ProjectionPruning.PathBinding != nil && !traversalStepHasContinuation(part, stepIndex) {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.ProjectionPruning.PathBinding.Identifier) || applied
	}

	return applied
}

// translateTraversalPatternPartWithoutExpansion emits each fixed step, applying pruning and scalar-ID continuation where qualified.
func (s *Translator) translateTraversalPatternPartWithoutExpansion(part *PatternPart, stepIndex int, traversalStep *TraversalStep, allowProjectionPruning bool) error {
	isFirstTraversalStep := stepIndex == 0

	if constraints, err := consumePatternConstraints(isFirstTraversalStep, nonRecursivePattern, traversalStep, s.treeTranslator); err != nil {
		return err
	} else {
		if isFirstTraversalStep {
			if err := s.applyPatternConstraintBalance(part, stepIndex, &constraints, traversalStep); err != nil {
				return err
			}
		}

		s.recordPredicatePlacementConsumption(part, stepIndex, traversalStep, constraints)

		if isFirstTraversalStep {
			hasPreviousFrame := traversalStep.Frame.Previous != nil

			if hasPreviousFrame {
				// Pull the implicitly joined result set's visibility to avoid violating SQL expectation on explicit vs
				// implicit join order
				for _, knownIdentifier := range traversalStep.Frame.Known().Slice() {
					if binding, bound := s.scope.Lookup(knownIdentifier); !bound {
						return errors.New("unknown traversal step identifier: " + knownIdentifier.String())
					} else if binding.LastProjection == traversalStep.Frame.Previous {
						traversalStep.Frame.Stash(binding.Identifier)
					}
				}
			}

			if err := RewriteFrameBindings(s.scope, constraints.LeftNode.Expression); err != nil {
				return err
			} else {
				traversalStep.LeftNodeConstraints = constraints.LeftNode.Expression
			}

			if leftNodeJoinCondition, err := leftNodeTraversalStepConstraint(traversalStep); err != nil {
				return err
			} else if err := RewriteFrameBindings(s.scope, leftNodeJoinCondition); err != nil {
				return err
			} else {
				traversalStep.LeftNodeJoinCondition = leftNodeJoinCondition
			}

			if hasPreviousFrame {
				traversalStep.Frame.RestoreStashed()
			}
		}

		traversalStep.Frame.Export(traversalStep.Edge.Identifier)

		if edgeJoinCondition, err := rightEdgeConstraint(traversalStep); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, edgeJoinCondition); err != nil {
			return err
		} else {
			traversalStep.EdgeJoinCondition = edgeJoinCondition
		}

		if err := RewriteFrameBindings(s.scope, constraints.Edge.Expression); err != nil {
			return err
		} else {
			traversalStep.EdgeConstraints = constraints.Edge
		}
		traversalStep.EdgeConstraints.Expression = pgsql.OptionalAnd(
			traversalStep.EdgeConstraints.Expression,
			previousRelationshipUniquenessConstraint(s.scope, part, stepIndex, traversalStep),
		)

		traversalStep.Frame.Export(traversalStep.RightNode.Identifier)

		if err := RewriteFrameBindings(s.scope, constraints.RightNode.Expression); err != nil {
			return err
		} else {
			traversalStep.RightNodeConstraints = constraints.RightNode.Expression
		}

		if rightNodeJoinCondition, err := rightNodeTraversalStepJoinCondition(traversalStep); err != nil {
			return err
		} else if err := RewriteFrameBindings(s.scope, rightNodeJoinCondition); err != nil {
			return err
		} else {
			traversalStep.RightNodeJoinCondition = rightNodeJoinCondition
		}
	}

	if allowProjectionPruning {
		if s.applyPathEdgeIDMaterialization(part, stepIndex, traversalStep) {
			s.recordLowering(optimize.LoweringLatePathMaterialization)
		}

		_, hasDecision := s.projectionPruningDecision(part, stepIndex)
		if hasDecision && pruneTraversalStepProjectionExports(part, stepIndex, traversalStep) {
			s.recordLowering(optimize.LoweringProjectionPruning)
		}
	}

	leftNodeIDOnly := s.applyIDOnlyNodeProjection(part, stepIndex, traversalStep.LeftNode)
	rightNodeIDOnly := s.applyIDOnlyNodeProjection(part, stepIndex, traversalStep.RightNode)
	if leftNodeIDOnly || rightNodeIDOnly {
		s.recordLowering(optimize.LoweringFieldRequirements)
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

	return nil
}
