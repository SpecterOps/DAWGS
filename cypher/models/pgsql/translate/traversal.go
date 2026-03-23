package translate

import (
	"errors"
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/graph"
)

func (s *Translator) buildDirectionlessTraversalPatternRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
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
		} else if err := s.translateTraversalPatternPartWithoutExpansion(idx == 0, traversalStep); err != nil {
			return err
		}
	}

	if isolatedProjection {
		s.scope = scopeSnapshot
	}

	return nil
}

func (s *Translator) translateTraversalPatternPartWithoutExpansion(isFirstTraversalStep bool, traversalStep *TraversalStep) error {
	if constraints, err := consumePatternConstraints(isFirstTraversalStep, nonRecursivePattern, traversalStep, s.treeTranslator); err != nil {
		return err
	} else {
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
