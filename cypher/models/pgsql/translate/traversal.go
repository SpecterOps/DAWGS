package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/cypher"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/graph"
)

func boundEndpointIDReference(frame *Frame, binding *BoundIdentifier) pgsql.RowColumnReference {
	return pgsql.RowColumnReference{
		Identifier: pgsql.CompoundIdentifier{frame.Binding.Identifier, binding.Identifier},
		Column:     pgsql.ColumnID,
	}
}

func boundEndpointInequality(frame *Frame, traversalStep *TraversalStep) pgsql.Expression {
	return pgsql.NewParenthetical(
		pgsql.NewBinaryExpression(
			boundEndpointIDReference(frame, traversalStep.LeftNode),
			pgsql.OperatorCypherNotEquals,
			boundEndpointIDReference(frame, traversalStep.RightNode),
		),
	)
}

func (s *Translator) buildBoundEndpointTraversalPattern(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if partFrame == nil || partFrame.Previous == nil {
		return pgsql.Query{}, errors.New("expected previous frame for bound endpoint traversal")
	}

	var (
		previousFrame = partFrame.Previous
		nextSelect    = pgsql.Select{
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
						JoinType: pgsql.JoinTypeInner,
						Constraint: pgsql.OptionalAnd(
							traversalStep.EdgeJoinCondition,
							traversalStep.RightNodeJoinCondition,
						),
					},
				}},
			}},
		}
	)

	nextSelect.Where = pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(traversalStep.RightNodeConstraints, nextSelect.Where)

	if traversalStep.Direction == graph.DirectionBoth && traversalStep.LeftNode.Identifier != traversalStep.RightNode.Identifier {
		nextSelect.Where = pgsql.OptionalAnd(boundEndpointInequality(previousFrame, traversalStep), nextSelect.Where)
	}

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

func (s *Translator) buildDirectionlessTraversalPatternRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.LeftNodeBound && traversalStep.RightNodeBound {
		return s.buildBoundEndpointTraversalPattern(traversalStep.Frame, traversalStep)
	}

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

	if traversalStep.LeftNodeBound {
		if traversalStep.Frame.Previous == nil {
			return pgsql.Query{}, fmt.Errorf("left node is marked as bound but there is no previous frame to reference")
		}

		// Left node was already materialized in the previous frame. Promote that frame and join only the terminal node here.
		//
		// prevFrame is the join root so LeftNodeConstraints can safely reference it in the edge ON clause without partitioning.
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

		nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
		nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

		// left node is not joined here, so the guard must reference the bound node through the previous frame
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
					pgsql.CompoundIdentifier{traversalStep.RightNode.Identifier, pgsql.ColumnID},
				),
			),
			nextSelect.Where,
		)

		return pgsql.Query{
			Body: nextSelect,
		}, nil
	}

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
		pgsql.NewParenthetical(
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{traversalStep.LeftNode.Identifier, pgsql.ColumnID},
				pgsql.OperatorCypherNotEquals,
				pgsql.CompoundIdentifier{traversalStep.RightNode.Identifier, pgsql.ColumnID})),
		nextSelect.Where)
	return pgsql.Query{
		Body: nextSelect,
	}, nil
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

func (s *Translator) buildTraversalPatternRoot(partFrame *Frame, traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.Direction == graph.DirectionBoth {
		return s.buildDirectionlessTraversalPatternRoot(traversalStep)
	}

	if traversalStep.LeftNodeBound && traversalStep.RightNodeBound {
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
	if traversalStep.LeftNodeBound && traversalStep.RightNodeBound {
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

func (s *Translator) translateTraversalPatternPart(part *PatternPart, isolatedProjection bool, allowProjectionPruning bool) error {
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

func (s *Translator) applyExpansionSuffixPushdown(part *PatternPart) (int, error) {
	if part == nil || !part.HasTarget {
		return applyExpansionSuffixPushdown(part)
	}

	var applied int
	for stepIndex := range part.TraversalSteps {
		target := part.Target.TraversalStep(stepIndex)
		decisions := s.suffixPushdownDecisions[target]
		if len(decisions) == 0 {
			if stepIndex+1 >= len(part.TraversalSteps) {
				continue
			}

			currentStep := part.TraversalSteps[stepIndex]
			suffixSteps := part.TraversalSteps[stepIndex+1:]
			if candidateApplied, err := applyExpansionSuffixPushdownCandidate(currentStep, suffixSteps); err != nil {
				return applied, err
			} else if candidateApplied {
				applied++
			}

			continue
		}

		for _, decision := range decisions {
			if decision.SuffixLength <= 0 || stepIndex+decision.SuffixLength >= len(part.TraversalSteps) {
				continue
			}

			currentStep := part.TraversalSteps[stepIndex]
			suffixSteps := part.TraversalSteps[stepIndex+1 : stepIndex+1+decision.SuffixLength]
			if candidateApplied, err := applyExpansionSuffixPushdownCandidate(currentStep, suffixSteps); err != nil {
				return applied, err
			} else if candidateApplied {
				applied++
			}
		}
	}

	return applied, nil
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

func (s *Translator) projectionPruningDecision(part *PatternPart, stepIndex int) (optimize.ProjectionPruningDecision, bool) {
	if part == nil || !part.HasTarget {
		return optimize.ProjectionPruningDecision{}, false
	}

	decision, hasDecision := s.projectionPruningDecisions[part.Target.TraversalStep(stepIndex)]
	return decision, hasDecision
}

func (s *Translator) hasLatePathMaterialization(part *PatternPart, stepIndex int, mode optimize.LatePathMaterializationMode) bool {
	if part == nil || !part.HasTarget {
		return false
	}

	for _, decision := range s.latePathDecisions[part.Target.TraversalStep(stepIndex)] {
		if decision.Mode == mode {
			return true
		}
	}

	return false
}

func projectionPruningDecisionReferencesBinding(decision optimize.ProjectionPruningDecision, binding *BoundIdentifier) bool {
	if binding == nil {
		return false
	}

	sourceIdentifier := binding.Identifier
	if binding.Alias.Set {
		sourceIdentifier = binding.Alias.Value
	}

	for _, symbol := range decision.ReferencedSymbols {
		if symbol == cypher.TokenLiteralAsterisk || pgsql.Identifier(symbol) == sourceIdentifier {
			return true
		}
	}

	return false
}

func projectionPruningDecisionPatternDependsOn(part *PatternPart, binding *BoundIdentifier, decision optimize.ProjectionPruningDecision) bool {
	if !decision.PatternBindingReferenced || part == nil || part.PatternBinding == nil || binding == nil {
		return false
	}

	for _, dependency := range part.PatternBinding.Dependencies {
		if dependency.Identifier == binding.Identifier {
			return true
		}
	}

	return false
}

func traversalStepProjectsBindingByDecision(part *PatternPart, stepIndex int, binding *BoundIdentifier, decision optimize.ProjectionPruningDecision) bool {
	if binding == nil {
		return false
	}

	if projectionPruningDecisionReferencesBinding(decision, binding) || projectionPruningDecisionPatternDependsOn(part, binding, decision) {
		return true
	}

	if stepIndex+1 < len(part.TraversalSteps) {
		nextStep := part.TraversalSteps[stepIndex+1]
		return nextStep.LeftNode != nil && nextStep.LeftNode.Identifier == binding.Identifier
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

func unexportFrameBinding(frame *Frame, identifier pgsql.Identifier) bool {
	if frame == nil {
		return false
	}

	exported := frame.Exported.Contains(identifier)
	frame.Unexport(identifier)
	return exported
}

func pruneTraversalStepProjectionExports(queryPart *QueryPart, part *PatternPart, stepIndex int, traversalStep *TraversalStep, decision optimize.ProjectionPruningDecision, hasDecision bool, allowFallback bool) bool {
	var applied bool

	if hasDecision {
		if traversalStep.LeftNode != nil && !traversalStep.LeftNodeBound && !traversalStepProjectsBindingByDecision(part, stepIndex, traversalStep.LeftNode, decision) {
			applied = unexportFrameBinding(traversalStep.Frame, traversalStep.LeftNode.Identifier) || applied
		}

		if traversalStep.Edge != nil && !traversalStepProjectsBindingByDecision(part, stepIndex, traversalStep.Edge, decision) {
			applied = unexportFrameBinding(traversalStep.Frame, traversalStep.Edge.Identifier) || applied
		}

		if traversalStep.RightNode != nil && !traversalStep.RightNodeBound && !traversalStepProjectsBindingByDecision(part, stepIndex, traversalStep.RightNode, decision) {
			applied = unexportFrameBinding(traversalStep.Frame, traversalStep.RightNode.Identifier) || applied
		}

		return applied
	}

	if !allowFallback {
		return false
	}

	// Bound endpoints already exist in an outer frame. Only unexport unbound
	// values that later clauses and continuation steps cannot observe.
	if traversalStep.LeftNode != nil && !traversalStep.LeftNodeBound && !traversalStepProjectsBinding(queryPart, part, stepIndex, traversalStep.LeftNode) {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.LeftNode.Identifier) || applied
	}

	if traversalStep.Edge != nil &&
		traversalStep.Edge.DataType == pgsql.EdgeComposite &&
		!queryPart.ReferencesBinding(traversalStep.Edge) &&
		patternBindingDependsOn(queryPart, part, traversalStep.Edge) {
		traversalStep.Edge.DataType = pgsql.PathEdge
		applied = true
	}

	if traversalStep.Edge != nil && !traversalStepProjectsBinding(queryPart, part, stepIndex, traversalStep.Edge) {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.Edge.Identifier) || applied
	}

	if traversalStep.RightNode != nil && !traversalStep.RightNodeBound && !traversalStepProjectsBinding(queryPart, part, stepIndex, traversalStep.RightNode) {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.RightNode.Identifier) || applied
	}

	return applied
}

func pruneExpansionStepProjectionExports(queryPart *QueryPart, part *PatternPart, traversalStep *TraversalStep, decision optimize.ProjectionPruningDecision, hasDecision bool, allowFallback bool) bool {
	if traversalStep == nil || traversalStep.Expansion == nil {
		return false
	}

	var applied bool
	if hasDecision {
		if traversalStep.Edge != nil && !projectionPruningDecisionReferencesBinding(decision, traversalStep.Edge) {
			applied = unexportFrameBinding(traversalStep.Frame, traversalStep.Edge.Identifier) || applied
		}

		if traversalStep.Expansion.PathBinding != nil && !projectionPruningDecisionPatternDependsOn(part, traversalStep.Expansion.PathBinding, decision) {
			applied = unexportFrameBinding(traversalStep.Frame, traversalStep.Expansion.PathBinding.Identifier) || applied
		}

		return applied
	}

	if !allowFallback {
		return false
	}

	// Variable-length relationship bindings materialize to edge-composite
	// arrays. A path binding can be rebuilt later from the compact expansion
	// path ID array, so keep the edge array only when the relationship binding
	// itself is observable.
	if traversalStep.Edge != nil && !queryPart.ReferencesBinding(traversalStep.Edge) {
		applied = unexportFrameBinding(traversalStep.Frame, traversalStep.Edge.Identifier) || applied
	}

	pathBinding := traversalStep.Expansion.PathBinding
	if pathBinding != nil && !patternBindingDependsOn(queryPart, part, pathBinding) {
		applied = unexportFrameBinding(traversalStep.Frame, pathBinding.Identifier) || applied
	}

	return applied
}

func (s *Translator) translateTraversalPatternPartWithoutExpansion(part *PatternPart, stepIndex int, traversalStep *TraversalStep, allowProjectionPruning bool) error {
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

	if allowProjectionPruning {
		if traversalStep.Edge != nil &&
			traversalStep.Edge.DataType == pgsql.EdgeComposite &&
			s.hasLatePathMaterialization(part, stepIndex, optimize.LatePathMaterializationPathEdgeID) {
			traversalStep.Edge.DataType = pgsql.PathEdge
			s.recordLowering(optimize.LoweringLatePathMaterialization)
		}

		decision, hasDecision := s.projectionPruningDecision(part, stepIndex)
		allowFallback := !hasDecision
		if pruneTraversalStepProjectionExports(s.query.CurrentPart(), part, stepIndex, traversalStep, decision, hasDecision, allowFallback) {
			s.recordLowering(optimize.LoweringProjectionPruning)
		}
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
