package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

func (s *Translator) buildDirectionlessTraversalPatternRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.LeftNodeBound && traversalStep.RightNodeBound {
		// Both sides are bound, build a strict pairwise join on the edge
		return s.buildPairwiseDirectionlessTraversalPatternRoot(traversalStep)
	} else if traversalStep.LeftNodeBound || traversalStep.RightNodeBound {
		// One of the traversal step nodes is bound by the outer query, so
		// generate internal constraints to "bind" the inner and outer queries
		return buildSingleBoundDirectionlessTraversalPlan(traversalStep).build()
	}

	// Neither side of the traversal is bound
	return s.buildUnboundDirectionlessTraversalPatternRoot(traversalStep)
}

// buildUnboundDirectionlessTraversalPatternRoot builds a traversal pattern for an UNBOUND path predicate
func (s *Translator) buildUnboundDirectionlessTraversalPatternRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	var (
		// Partition node constraints
		rightJoinLocal, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

		leftJoinLocal, leftJoinExternal = partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)

		nextSelect = pgsql.Select{
			Projection: traversalStep.Projection,
		}
	)

	// Reach into the previous frame, if any, to hoist forward its exports
	if previousFrame, hasPrevious := s.previousValidFrame(traversalStep.Frame); hasPrevious {
		nextSelect.From = append(nextSelect.From, pgsql.FromClause{
			Source: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
			},
		})
	}

	nextSelect.From = append(nextSelect.From, pgsql.FromClause{
		// FROM edge eN
		Source: pgsql.TableReference{
			Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
			Binding: models.OptionalValue(traversalStep.Edge.Identifier),
		},
		Joins: []pgsql.Join{{
			// INNER JOIN node nN
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(traversalStep.LeftNode.Identifier),
			},
			// ON <leftJoinLocal> AND <LeftNodeJoinCondition>
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition),
			},
		}, {
			// INNER JOIN node nN
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

	// For an inner join, PostgreSQL's optimizer can push start and end predicates into the join if they're part
	// of the where clause below, but it requires additional planning work and may not do so reliably when multiple
	// CTEs are involved or the planner's cost model is off.
	//
	// Emitting them directly in the JOIN ON constraint makes the intent unambiguous and enables the planner to
	// apply the GIN kind index during the join, before materializing the intermediate result.
	nextSelect.Where = pgsql.OptionalAnd(leftJoinExternal, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

	// AND (n0.id <> n1.id) - ensures edges are properly constrained to the specified nodes
	nextSelect.Where = pgsql.OptionalAnd(
		s.buildNodeInequalityAssertion(traversalStep),
		nextSelect.Where,
	)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

func (s *Translator) buildNodeInequalityAssertion(traversalStep *TraversalStep) pgsql.Expression {
	return pgsql.NewParenthetical(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID},
			pgsql.OperatorCypherNotEquals,
			pgsql.CompoundIdentifier{traversalStep.RightNode.Identifier, pgsql.ColumnID},
		),
	)
}

// singleBoundDirectionlessTraversalPlan is an internal representation of a disambiguated
// single-node-bound directionless traversal
type singleBoundDirectionlessTraversalPlan struct {
	edgeJoinConstraint      pgsql.Expression
	nodeJoinConstraint      pgsql.Expression
	whereConstraint         pgsql.Expression
	nodeJoinBinding         pgsql.Identifier
	boundNodeIdentifier     pgsql.Identifier
	unboundNodeIdentifier   pgsql.Identifier
	pivotEdgeIdentifier     pgsql.Identifier
	previousFrameIdentifier pgsql.Identifier
	projection              []pgsql.SelectItem
}

func buildSingleBoundDirectionlessTraversalPlan(traversalStep *TraversalStep) *singleBoundDirectionlessTraversalPlan {
	var (
		// Partition node constraints
		rightJoinLocal, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

		leftJoinLocal, leftJoinExternal = partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)
	)

	plan := &singleBoundDirectionlessTraversalPlan{
		pivotEdgeIdentifier:     traversalStep.Edge.Identifier,
		previousFrameIdentifier: traversalStep.Frame.Previous.Binding.Identifier,
		projection:              traversalStep.Projection,
	}
	if traversalStep.LeftNodeBound {
		plan.edgeJoinConstraint = pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, traversalStep.LeftNodeJoinCondition)
		plan.nodeJoinBinding = traversalStep.RightNode.Identifier
		plan.nodeJoinConstraint = pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition)
		plan.whereConstraint = pgsql.OptionalAnd(rightJoinExternal, traversalStep.EdgeConstraints.Expression)
		plan.boundNodeIdentifier = traversalStep.LeftNode.Identifier
		plan.unboundNodeIdentifier = traversalStep.RightNode.Identifier
	} else if traversalStep.RightNodeBound {
		plan.edgeJoinConstraint = pgsql.OptionalAnd(traversalStep.RightNodeConstraints, traversalStep.RightNodeJoinCondition)
		plan.nodeJoinBinding = traversalStep.LeftNode.Identifier
		plan.nodeJoinConstraint = pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition)
		plan.whereConstraint = pgsql.OptionalAnd(leftJoinExternal, traversalStep.EdgeConstraints.Expression)
		plan.boundNodeIdentifier = traversalStep.RightNode.Identifier
		plan.unboundNodeIdentifier = traversalStep.LeftNode.Identifier
	}

	return plan
}

// buildSingleBoundDirectionlessTraversalPatternRoot generates a join oriented for a single-sided outside-bound node
func (plan *singleBoundDirectionlessTraversalPlan) build() (pgsql.Query, error) {
	nextSelect := pgsql.Select{
		Projection: plan.projection,
	}

	// The selected node was already materialized in the previous frame. Promote that frame and join only the terminal node here.
	//
	// prevFrame is the join root so NodeConstraints can safely reference it in the edge ON clause without partitioning.
	nextSelect.From = append(nextSelect.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{plan.previousFrameIdentifier},
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(plan.pivotEdgeIdentifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				// Constraint: pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, traversalStep.LeftNodeJoinCondition),
				Constraint: plan.edgeJoinConstraint,
			},
		}, {
			Table: pgsql.TableReference{
				Name: pgsql.CompoundIdentifier{pgsql.TableNode},
				// Binding: models.OptionalValue(traversalStep.RightNode.Identifier),
				Binding: models.OptionalValue(plan.nodeJoinBinding),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType: pgsql.JoinTypeInner,
				// Constraint: pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition),
				Constraint: plan.nodeJoinConstraint,
			},
		}},
	})

	// these got zooped up into the plan's initial creation
	// nextSelect.Where = pgsql.OptionalAnd(plan.traversalStep.EdgeConstraints.Expression, plan.nextSelect.Where)
	// nextSelect.Where = pgsql.OptionalAnd(plan.whereConstraint, nextSelect.Where)
	nextSelect.Where = plan.whereConstraint

	// selected node is not joined here, so the guard must reference the bound node through the previous frame
	nextSelect.Where = pgsql.OptionalAnd(
		pgsql.NewParenthetical(
			pgsql.NewBinaryExpression(
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{plan.previousFrameIdentifier, plan.boundNodeIdentifier},
					Column:     pgsql.ColumnID,
				},
				pgsql.OperatorCypherNotEquals,
				pgsql.CompoundIdentifier{plan.unboundNodeIdentifier, pgsql.ColumnID},
			),
		),
		nextSelect.Where,
	)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

func buildDirectionlessPairwiseEdgeConstraint(traversalStep *TraversalStep) pgsql.Expression {
	prevFrameID := traversalStep.Frame.Previous.Binding.Identifier

	// ((sN.nLeft).id = (eN).start_id AND (sN.nRight).id = (eN).end_id)
	leftToRight := pgsql.NewParenthetical(
		pgsql.NewBinaryExpression(
			pgsql.NewBinaryExpression(
				// (sN.nLeft).id
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{prevFrameID, traversalStep.LeftNode.Identifier},
					Column:     pgsql.ColumnID,
				},
				pgsql.OperatorEquals,
				// (eN).start_id
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnStartID},
			),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				// (sN.nRight).id
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{prevFrameID, traversalStep.RightNode.Identifier},
					Column:     pgsql.ColumnID,
				},
				pgsql.OperatorEquals,
				// (eN).end_id
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnEndID},
			),
		),
	)

	// ((sN.nRight).id = (eN).start_id AND (sN.nLeft).id = (eN).end_id)
	rightToLeft := pgsql.NewParenthetical(
		pgsql.NewBinaryExpression(
			pgsql.NewBinaryExpression(
				// (sN.nRight).id
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{prevFrameID, traversalStep.RightNode.Identifier},
					Column:     pgsql.ColumnID,
				},
				pgsql.OperatorEquals,
				// (eN).start_id
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnStartID},
			),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				// (sN.nLeft).id
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{prevFrameID, traversalStep.LeftNode.Identifier},
					Column:     pgsql.ColumnID,
				},
				pgsql.OperatorEquals,
				// (eN).end_id
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnEndID},
			),
		),
	)

	return pgsql.NewParenthetical(
		pgsql.NewBinaryExpression(
			leftToRight, pgsql.OperatorOr, rightToLeft,
		),
	)
}

func (s *Translator) buildPairwiseDirectionlessTraversalPatternRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	var (
		// Partition node constraints
		// TODO FIXME
		_, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

		// TODO FIXME
		_, leftJoinExternal = partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)

		nextSelect = pgsql.Select{
			Projection: traversalStep.Projection,
		}
	)

	pairwiseEdgeConstraint := buildDirectionlessPairwiseEdgeConstraint(traversalStep)
	nextSelect.From = append(nextSelect.From, pgsql.FromClause{
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
				Constraint: pairwiseEdgeConstraint,
			},
		}},
	})

	// Dual-bound: both endpoint external constraints apply.
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(leftJoinExternal, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

	// Keep inequality semantics consistent with existing directionless behavior.
	nextSelect.Where = pgsql.OptionalAnd(
		pgsql.NewParenthetical(
			pgsql.NewBinaryExpression(
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{
						traversalStep.Frame.Previous.Binding.Identifier,
						traversalStep.LeftNode.Identifier,
					},
					Column: pgsql.ColumnID,
				},
				pgsql.OperatorCypherNotEquals,
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{
						traversalStep.Frame.Previous.Binding.Identifier,
						traversalStep.RightNode.Identifier,
					},
					Column: pgsql.ColumnID,
				},
			),
		),
		nextSelect.Where,
	)
	return pgsql.Query{Body: nextSelect}, nil
}

// buildTraversalPatternRootWithOuterCorrelation constructs a traversal pattern root, preserving the correlation to
// the outer query part's context
func (s *Translator) buildTraversalPatternRootWithOuterCorrelation(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.Direction == graph.DirectionBoth {
		return s.buildDirectionlessTraversalPatternRoot(traversalStep)
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

func (s *Translator) buildTraversalPatternRoot(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.Direction == graph.DirectionBoth {
		return s.buildDirectionlessTraversalPatternRoot(traversalStep)
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

		if previousFrame, hasPrevious := s.previousValidFrame(traversalStep.Frame); hasPrevious {
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

func (s *Translator) buildTraversalPatternStep(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
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

func (s *Translator) translateTraversalPatternPart(part *PatternPart, isolatedProjection bool) error {
	var scopeSnapshot *Scope

	if isolatedProjection {
		scopeSnapshot = s.scope.Snapshot()
	}

	for idx, traversalStep := range part.TraversalSteps {
		if traversalStepFrame, err := s.scope.PushFrame(); err != nil {
			return err
		} else {
			// Assign the new scope frame to the traversal step
			traversalStep.Frame = traversalStepFrame
		}

		if traversalStep.Expansion != nil {
			if err := s.translateTraversalPatternPartWithExpansion(idx == 0, traversalStep); err != nil {
				return err
			}
		} else if part.AllShortestPaths || part.ShortestPath {
			return fmt.Errorf("expected shortest path search to utilize variable expansion: ()-[*..]->()")
		} else if err := s.translateTraversalPatternPartWithoutExpansion(part, idx, traversalStep); err != nil {
			return err
		}
	}

	if isolatedProjection {
		s.scope = scopeSnapshot
	}

	return nil
}

func patternBindingDependsOn(queryPart *QueryPart, part *PatternPart, binding *BoundIdentifier) bool {
	if queryPart == nil || part == nil || part.PatternBinding == nil || binding == nil {
		return false
	}

	if !queryPart.ReferencesBinding(part.PatternBinding) {
		return false
	}

	for _, dependency := range part.PatternBinding.Dependencies {
		if dependency.Identifier == binding.Identifier {
			return true
		}
	}

	return false
}

func traversalStepProjectsBinding(queryPart *QueryPart, part *PatternPart, stepIndex int, binding *BoundIdentifier) bool {
	if binding == nil {
		return false
	}

	// Keep aliases referenced by later clauses and bindings needed to materialize
	// a referenced path pattern. Everything else can stay internal to this step.
	if (binding.Alias.Set && queryPart.ReferencesBinding(binding)) || patternBindingDependsOn(queryPart, part, binding) {
		return true
	}

	if stepIndex+1 < len(part.TraversalSteps) {
		// A multi-hop pattern needs the right node from this step as the next
		// step's left node even when the user never projects it.
		nextStep := part.TraversalSteps[stepIndex+1]
		return nextStep.LeftNode != nil && nextStep.LeftNode.Identifier == binding.Identifier
	}

	return false
}

func pruneTraversalStepProjectionExports(queryPart *QueryPart, part *PatternPart, stepIndex int, traversalStep *TraversalStep) {
	// Bound endpoints already exist in an outer frame. Only unexport unbound
	// values that later clauses and continuation steps cannot observe.
	if !traversalStep.LeftNodeBound && !traversalStepProjectsBinding(queryPart, part, stepIndex, traversalStep.LeftNode) {
		traversalStep.Frame.Unexport(traversalStep.LeftNode.Identifier)
	}

	if !traversalStepProjectsBinding(queryPart, part, stepIndex, traversalStep.Edge) {
		traversalStep.Frame.Unexport(traversalStep.Edge.Identifier)
	}

	if !traversalStep.RightNodeBound && !traversalStepProjectsBinding(queryPart, part, stepIndex, traversalStep.RightNode) {
		traversalStep.Frame.Unexport(traversalStep.RightNode.Identifier)
	}
}

func (s *Translator) translateTraversalPatternPartWithoutExpansion(part *PatternPart, stepIndex int, traversalStep *TraversalStep) error {
	isFirstTraversalStep := stepIndex == 0

	if constraints, err := consumePatternConstraints(isFirstTraversalStep, nonRecursivePattern, traversalStep, s.treeTranslator); err != nil {
		return err
	} else {
		if isFirstTraversalStep {
			if err := constraints.OptimizePatternConstraintBalance(s.scope, traversalStep); err != nil {
				return err
			}

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

	pruneTraversalStepProjectionExports(s.query.CurrentPart(), part, stepIndex, traversalStep)

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
