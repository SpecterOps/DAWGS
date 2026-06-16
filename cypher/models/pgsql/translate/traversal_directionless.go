package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
)

//
// DIRECTIONLESS TRAVERSALS WITHOUT OUTER CORRELATION
//

// buildDirectionlessTraversalPatternRoot constructs query parts covering an undirected traversal without taking outer
// correlation into consideration for situations where an outer correlation is not necessary (ie., MATCHing on an undirected traversal pattern,
// traversal patterns where neither endpoint is bound).
func (s *Translator) buildDirectionlessTraversalPatternRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.UseExpandInto {
		return s.buildBoundEndpointTraversalPattern(traversalStep.Frame, traversalStep)
	}

	if traversalStep.LeftNodeBound && traversalStep.RightNodeBound {
		// Both sides are bound, build a strict pairwise join on the edge
		return s.buildPairwiseDirectionlessTraversalPatternRoot(traversalStep)
	} else if traversalStep.LeftNodeBound || traversalStep.RightNodeBound {
		// One of the traversal step nodes is bound by the outer query, so
		// generate internal constraints to "bind" the inner and outer queries
		return s.buildSingleBoundDirectionlessTraversalRoot(traversalStep)
	}

	return s.buildUnboundDirectionlessTraversalPatternRoot(traversalStep)
}

func buildDirectionlessPairwiseEdgeConstraint(traversalStep *TraversalStep) pgsql.Expression {
	prevFrame := traversalStep.Frame.Previous

	// ((sN.nLeft).id = (eN).start_id AND (sN.nRight).id = (eN).end_id)
	leftToRight := pgsql.NewParenthetical(
		pgsql.NewBinaryExpression(
			pgsql.NewBinaryExpression(
				// (sN.nLeft).id
				boundEndpointIDReference(prevFrame, traversalStep.LeftNode),
				pgsql.OperatorEquals,
				// (eN).start_id
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnStartID},
			),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				// (sN.nRight).id
				boundEndpointIDReference(prevFrame, traversalStep.RightNode),
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
				boundEndpointIDReference(prevFrame, traversalStep.RightNode),
				pgsql.OperatorEquals,
				// (eN).start_id
				pgsql.CompoundIdentifier{traversalStep.Edge.Identifier, pgsql.ColumnStartID},
			),
			pgsql.OperatorAnd,
			pgsql.NewBinaryExpression(
				// (sN.nLeft).id
				boundEndpointIDReference(prevFrame, traversalStep.LeftNode),
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
		_, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

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

	// Only apply endpoint inequality when the bound nodes are different, to allow for self-referential relationships
	if traversalStep.LeftNode.Identifier != traversalStep.RightNode.Identifier {
		nextSelect.Where = pgsql.OptionalAnd(boundEndpointInequality(traversalStep.Frame.Previous, traversalStep), nextSelect.Where)
	}

	return pgsql.Query{Body: nextSelect}, nil
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

// buildSingleBoundDirectionlessTraversalRoot checks that the bound previous frame exists and generates the binding
// parameters necessary to generate the SQL query for a single-bound undirected traversal.
func (s *Translator) buildSingleBoundDirectionlessTraversalRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	referenceFrame := traversalStep.Frame
	previousFrame, hasPreviousFrame := s.previousValidFrame(referenceFrame)

	// If left node is bound and there is no previous frame, this is a bug in the bounds generation
	if traversalStep.LeftNodeBound && !hasPreviousFrame {
		return pgsql.Query{}, fmt.Errorf("left node is marked as bound but there is no previous frame to reference")
	}

	// Special-case for self-referential right-bound form
	if traversalStep.RightNodeBound && !hasPreviousFrame {
		return s.buildSelfReferentialDirectionlessTraversalRoot(traversalStep)
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

		edgeJoinConstraint    pgsql.Expression
		nodeJoinConstraint    pgsql.Expression
		whereConstraint       pgsql.Expression
		nodeJoinBinding       pgsql.Identifier
		boundNodeIdentifier   pgsql.Identifier
		unboundNodeIdentifier pgsql.Identifier

		pivotEdgeIdentifier = traversalStep.Edge.Identifier
		projection          = traversalStep.Projection
	)

	if traversalStep.LeftNodeBound {
		edgeJoinConstraint = pgsql.OptionalAnd(traversalStep.LeftNodeConstraints, traversalStep.LeftNodeJoinCondition)
		nodeJoinBinding = traversalStep.RightNode.Identifier
		nodeJoinConstraint = pgsql.OptionalAnd(rightJoinLocal, traversalStep.RightNodeJoinCondition)
		whereConstraint = pgsql.OptionalAnd(rightJoinExternal, traversalStep.EdgeConstraints.Expression)
		boundNodeIdentifier = traversalStep.LeftNode.Identifier
		unboundNodeIdentifier = traversalStep.RightNode.Identifier
	} else if traversalStep.RightNodeBound {
		edgeJoinConstraint = pgsql.OptionalAnd(traversalStep.RightNodeConstraints, traversalStep.RightNodeJoinCondition)
		nodeJoinBinding = traversalStep.LeftNode.Identifier
		nodeJoinConstraint = pgsql.OptionalAnd(leftJoinLocal, traversalStep.LeftNodeJoinCondition)
		whereConstraint = pgsql.OptionalAnd(leftJoinExternal, traversalStep.EdgeConstraints.Expression)
		boundNodeIdentifier = traversalStep.RightNode.Identifier
		unboundNodeIdentifier = traversalStep.LeftNode.Identifier
	}

	nextSelect := pgsql.Select{
		Projection: projection,
	}

	// The selected node was already materialized in the previous frame. Promote that frame and join only the terminal node here.
	//
	// prevFrame is the join root so NodeConstraints can safely reference it in the edge ON clause without partitioning.
	nextSelect.From = append(nextSelect.From, pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier},
		},
		Joins: []pgsql.Join{{
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableEdge},
				Binding: models.OptionalValue(pivotEdgeIdentifier),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: edgeJoinConstraint,
			},
		}, {
			Table: pgsql.TableReference{
				Name:    pgsql.CompoundIdentifier{pgsql.TableNode},
				Binding: models.OptionalValue(nodeJoinBinding),
			},
			JoinOperator: pgsql.JoinOperator{
				JoinType:   pgsql.JoinTypeInner,
				Constraint: nodeJoinConstraint,
			},
		}},
	})

	nextSelect.Where = whereConstraint

	// selected node is not joined here, so the guard must reference the bound node through the previous frame
	nextSelect.Where = pgsql.OptionalAnd(
		pgsql.NewParenthetical(
			pgsql.NewBinaryExpression(
				pgsql.RowColumnReference{
					Identifier: pgsql.CompoundIdentifier{previousFrame.Binding.Identifier, boundNodeIdentifier},
					Column:     pgsql.ColumnID,
				},
				pgsql.OperatorCypherNotEquals,
				pgsql.CompoundIdentifier{unboundNodeIdentifier, pgsql.ColumnID},
			),
		),
		nextSelect.Where,
	)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

func (s *Translator) buildSelfReferentialDirectionlessTraversalRoot(traversalStep *TraversalStep) (pgsql.Query, error) {
	var (
		// Partition node constraints
		_, rightJoinExternal = partitionConstraintByLocality(
			traversalStep.RightNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.RightNode.Identifier, traversalStep.Edge.Identifier),
		)

		leftJoinLocal, leftJoinExternal = partitionConstraintByLocality(
			traversalStep.LeftNodeConstraints,
			pgsql.AsIdentifierSet(traversalStep.LeftNode.Identifier, traversalStep.Edge.Identifier),
		)
		nextSelect = pgsql.Select{
			From:       []pgsql.FromClause{},
			Projection: traversalStep.Projection,
		}
	)

	// Self-referential pattern: the right node reuses the left node's variable (e.g. (u)-[]-(u)).
	// There is no previous frame to promote as a FROM source. Join only the left node table and
	// push the right-node join condition into WHERE so that start_id and end_id both reference
	// the same node.
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

	nextSelect.Where = pgsql.OptionalAnd(traversalStep.EdgeConstraints.Expression, nextSelect.Where)
	nextSelect.Where = pgsql.OptionalAnd(rightJoinExternal, nextSelect.Where)

	return pgsql.Query{
		Body: nextSelect,
	}, nil
}

//
// UNDIRECTED TRAVERSALS **WITH** OUTER CORRELATION
//

func (s *Translator) buildDirectionlessTraversalPatternRootWithOuterCorrelation(traversalStep *TraversalStep) (pgsql.Query, error) {
	if traversalStep.UseExpandInto {
		return s.buildBoundEndpointTraversalPattern(traversalStep.Frame, traversalStep)
	}

	return pgsql.Query{}, fmt.Errorf("not implemented")
}
