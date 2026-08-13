package translate

import (
	"errors"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/specterops/dawgs/graph"
)

const (
	aspI1Distance             pgsql.Identifier = "asp_i1_distance"
	aspI1Direct               pgsql.Identifier = "asp_i1_direct"
	aspI1Preflight            pgsql.Identifier = "asp_i1_preflight"
	aspI1PreflightBounded     pgsql.Identifier = "asp_i1_preflight_bounded"
	aspI1DistanceBounded      pgsql.Identifier = "asp_i1_distance_bounded"
	aspI1Target               pgsql.Identifier = "asp_i1_target"
	aspI1Predecessor          pgsql.Identifier = "asp_i1_predecessor"
	aspI1PredecessorBounded   pgsql.Identifier = "asp_i1_predecessor_bounded"
	aspI1Paths                pgsql.Identifier = "asp_i1_paths"
	aspI1PathsBounded         pgsql.Identifier = "asp_i1_paths_bounded"
	aspI1Shortest             pgsql.Identifier = "asp_i1_shortest"
	aspI1Admission            pgsql.Identifier = "asp_i1_admission"
	aspI1Decision             pgsql.Identifier = "asp_i1_decision"
	aspI1CandidateMarker      pgsql.Identifier = "asp_i1_candidate_marker"
	aspI1FallbackMarker       pgsql.Identifier = "asp_i1_fallback_marker"
	aspI1CandidateBody        pgsql.Identifier = "asp_i1_candidate_body"
	aspI1FallbackBody         pgsql.Identifier = "asp_i1_fallback_body"
	aspI1CandidateRows        pgsql.Identifier = "asp_i1_candidate_rows"
	aspI1FallbackRows         pgsql.Identifier = "asp_i1_fallback_rows"
	aspI1NodeID               pgsql.Identifier = "node_id"
	aspI1PredecessorID        pgsql.Identifier = "predecessor_id"
	aspI1EdgeID               pgsql.Identifier = "edge_id"
	aspI1UseCandidate         pgsql.Identifier = "use_candidate"
	aspI1UseFallback          pgsql.Identifier = "use_fallback"
	aspI1Overflow             pgsql.Identifier = "overflow"
	aspI1NoPath               pgsql.Identifier = "no_path"
	aspI1RuntimeReceipt       pgsql.Identifier = "runtime_receipt"
	aspI1RuntimeAttestationFn pgsql.Identifier = "record_requested_traversal_runtime_attestation_v1"
	aspI1ColumnSizeFn         pgsql.Identifier = "pg_column_size"
)

func aspI1Aliased(expression pgsql.Expression, alias pgsql.Identifier) pgsql.SelectItem {
	return &pgsql.AliasedExpression{Expression: expression, Alias: models.OptionalValue(alias)}
}

func aspI1Table(alias, binding pgsql.Identifier) pgsql.TableReference {
	return pgsql.TableReference{Name: alias.AsCompoundIdentifier(), Binding: models.OptionalValue(binding)}
}

func aspI1CanonicalProjection(source pgsql.Identifier) pgsql.Projection {
	return pgsql.Projection{
		aspI1Aliased(pgsql.CompoundIdentifier{source, expansionRootID}, expansionRootID),
		aspI1Aliased(pgsql.CompoundIdentifier{source, expansionNextID}, expansionNextID),
		aspI1Aliased(pgsql.CompoundIdentifier{source, expansionDepth}, expansionDepth),
		aspI1Aliased(pgsql.CompoundIdentifier{source, expansionSatisfied}, expansionSatisfied),
		aspI1Aliased(pgsql.CompoundIdentifier{source, expansionIsCycle}, expansionIsCycle),
		aspI1Aliased(pgsql.CompoundIdentifier{source, expansionPath}, expansionPath),
	}
}

func aspI1OverflowAny(overflows ...pgsql.Expression) pgsql.Expression {
	var result pgsql.Expression
	for _, overflow := range overflows {
		if result == nil {
			result = overflow
		} else {
			result = pgsql.NewBinaryExpression(result, pgsql.OperatorOr, overflow)
		}
	}
	return result
}

func aspI1OutputBytes(source pgsql.Identifier) pgsql.Subquery {
	return pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.FunctionCall{
			Function: pgsql.FunctionCoalesce,
			Parameters: []pgsql.Expression{
				pgsql.FunctionCall{
					Function: pgsql.FunctionSum,
					Parameters: []pgsql.Expression{pgsql.FunctionCall{
						Function:   aspI1ColumnSizeFn,
						Parameters: []pgsql.Expression{pgsql.CompoundIdentifier{source, expansionPath}},
					}},
				},
				pgsql.NewLiteral(int64(0), pgsql.Int8),
			},
			CastType: pgsql.Int8,
		}},
		From: []pgsql.FromClause{tableFrom(source)},
	}}}
}

func aspI1Marker(alias pgsql.Identifier, selected pgsql.Identifier) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: alias},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{aspI1Aliased(pgsql.NewLiteral(true, pgsql.Boolean), orientationArmExecuted)},
			From:       []pgsql.FromClause{tableFrom(aspI1Decision)},
			Where:      pgsql.CompoundIdentifier{aspI1Decision, selected},
		}},
	}
}

type inlinePredecessorDAGMode struct {
	identity   optimize.ShortestPathExecutor
	fallback   optimize.ShortestPathExecutor
	oneWitness bool
}

// BuildInlineAllShortestPathsDAGRoot emits the guarded ASP-I1 predecessor-DAG statement.
func (s *ExpansionBuilder) BuildInlineAllShortestPathsDAGRoot() (pgsql.Query, error) {
	return s.buildInlinePredecessorDAGRoot(inlinePredecessorDAGMode{
		identity: optimize.ShortestPathExecutorASPI1DAG,
		fallback: optimize.ShortestPathExecutorASPA1DAG,
	})
}

// BuildInlineCanonicalShortestPathRoot emits one guarded canonical witness and
// invokes compact S4 exactly once if any candidate resource sentinel overflows.
func (s *ExpansionBuilder) BuildInlineCanonicalShortestPathRoot() (pgsql.Query, error) {
	return s.buildInlinePredecessorDAGRoot(inlinePredecessorDAGMode{
		identity:   optimize.ShortestPathExecutorI1CanonicalPredecessorWitness,
		fallback:   optimize.ShortestPathExecutorS4CanonicalWitness,
		oneWitness: true,
	})
}

// buildInlinePredecessorDAGRoot shares guarded minimum-distance and predecessor
// primitives between the ASP enumerator and the singleton canonical witness.
// Recursive producers are consumed only through materialized cap+1 relations;
// complementary markers prevent candidate/fallback row mixing.
func (s *ExpansionBuilder) buildInlinePredecessorDAGRoot(mode inlinePredecessorDAGMode) (pgsql.Query, error) {
	const validatedEndpoints pgsql.Identifier = "singleton_endpoints"

	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UsesSingletonEndpointPair() {
		return pgsql.Query{}, errors.New(string(mode.identity) + " requires one validated endpoint pair")
	}
	if expansionModel.Options.MinDepth.GetOr(1) != 1 || !expansionModel.Options.MaxDepth.Set || expansionModel.Options.MaxDepth.Value < 1 || expansionModel.Options.MaxDepth.Value > 64 {
		return pgsql.Query{}, errors.New(string(mode.identity) + " requires min depth 1 and bounded max depth <= 64")
	}
	if s.traversalStep.Direction != graph.DirectionOutbound && s.traversalStep.Direction != graph.DirectionInbound {
		return pgsql.Query{}, errors.New(string(mode.identity) + " requires a directed traversal")
	}
	for _, limit := range []int64{
		expansionModel.ShortestPathStateLimit,
		expansionModel.ShortestPathPredecessorLimit,
		expansionModel.ShortestPathEnumerationLimit,
		expansionModel.ShortestPathOutputBytesLimit,
	} {
		if limit <= 0 {
			return pgsql.Query{}, errors.New(string(mode.identity) + " requires positive bounded limits")
		}
	}

	endpointCTE := singletonEndpointValidationCTE(s.traversalStep, expansionModel)
	endpointSelect := endpointCTE.Query.Body.(pgsql.Select)
	endpointSelect.Where = pgsql.OptionalAnd(endpointSelect.Where, shortestPathSelfEndpointGuardCase(
		pgd.EntityID(s.traversalStep.LeftNode.Identifier),
		pgd.EntityID(s.traversalStep.RightNode.Identifier),
	))
	endpointCTE.Query.Body = endpointSelect

	// Exact one/two-hop preflights prevent the recursive distance producer from
	// exploring an irrelevant tail when the target is already shallow. The
	// preflight itself is consumed through the enumeration cap+1 sentinel so a
	// large parallel-edge result falls back before exposing partial rows.
	firstEdge := s.traversalStep.Edge.Identifier
	secondEdge := pgsql.Identifier("asp_i1_preflight_edge_2")
	edgeScope := func(alias pgsql.Identifier) pgsql.Expression {
		var scope pgsql.Expression = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{alias, pgsql.ColumnGraphID}, pgsql.OperatorEquals, pgsql.NewLiteral(s.graphID, pgsql.Int4),
		)
		if len(expansionModel.RelationshipKindIDs) > 0 {
			scope = pgsql.OptionalAnd(scope, pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{alias, pgsql.ColumnKindID}, pgsql.OperatorEquals,
				pgsql.NewAnyExpressionHinted(pgsql.NewLiteral(append([]int16(nil), expansionModel.RelationshipKindIDs...), pgsql.Int2Array)),
			))
		}
		return scope
	}
	startColumn, endColumn := pgsql.ColumnStartID, pgsql.ColumnEndID
	if s.traversalStep.Direction == graph.DirectionInbound {
		startColumn, endColumn = endColumn, startColumn
	}
	directWhere := pgsql.OptionalAnd(edgeScope(firstEdge), pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{firstEdge, startColumn}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID}),
		pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{firstEdge, endColumn}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID}),
	))
	direct := pgsql.Select{
		Projection: pgsql.Projection{
			aspI1Aliased(pgsql.NewLiteral(int64(1), pgsql.Int8), expansionDepth),
			aspI1Aliased(pgsql.ArrayLiteral{Values: []pgsql.Expression{pgsql.CompoundIdentifier{firstEdge, pgsql.ColumnID}}, CastType: pgsql.Int8Array}, expansionPath),
		},
		From:  []pgsql.FromClause{tableFrom(validatedEndpoints), {Source: expansionEdgeTableReference(firstEdge)}},
		Where: directWhere,
	}
	directCTE := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1Direct, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionDepth, expansionPath})},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: direct},
	}
	directExists := pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)}, From: []pgsql.FromClause{tableFrom(aspI1Direct)},
	}, Limit: pgsql.NewLiteral(int64(1), pgsql.Int8)}}}
	secondJoin := pgsql.OptionalAnd(edgeScope(secondEdge), pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{secondEdge, startColumn}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{firstEdge, endColumn}),
		pgsql.NewLiteral(true, pgsql.Boolean),
	))
	twoHop := pgsql.Select{
		Projection: pgsql.Projection{
			aspI1Aliased(pgsql.NewLiteral(int64(2), pgsql.Int8), expansionDepth),
			aspI1Aliased(pgsql.ArrayLiteral{Values: []pgsql.Expression{
				pgsql.CompoundIdentifier{firstEdge, pgsql.ColumnID}, pgsql.CompoundIdentifier{secondEdge, pgsql.ColumnID},
			}, CastType: pgsql.Int8Array}, expansionPath),
		},
		From: []pgsql.FromClause{tableFrom(validatedEndpoints), {Source: expansionEdgeTableReference(firstEdge), Joins: []pgsql.Join{{
			Table: expansionEdgeTableReference(secondEdge), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: secondJoin},
		}}}},
		Where: pgsql.OptionalAnd(
			pgd.Not(directExists),
			pgsql.OptionalAnd(edgeScope(firstEdge), pgsql.OptionalAnd(
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{firstEdge, startColumn}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID}),
				pgsql.OptionalAnd(
					pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{secondEdge, endColumn}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID}),
					pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{firstEdge, pgsql.ColumnID}, pgsql.OperatorNotEquals, pgsql.CompoundIdentifier{secondEdge, pgsql.ColumnID}),
				),
			)),
		),
	}
	preflight := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: aspI1Preflight, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionDepth, expansionPath})},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: pgsql.Select{
				Projection: pgsql.Projection{
					pgsql.CompoundIdentifier{aspI1Direct, expansionDepth},
					pgsql.CompoundIdentifier{aspI1Direct, expansionPath},
				},
				From: []pgsql.FromClause{tableFrom(aspI1Direct)},
			},
			ROperand: twoHop, Operator: pgsql.OperatorUnion, All: true,
		}},
	}
	preflightBounded := boundedTraversalStateProbe(
		aspI1PreflightBounded, aspI1Preflight, []pgsql.Identifier{expansionDepth, expansionPath}, expansionModel.ShortestPathEnumerationLimit,
	)
	preflightOverflow := boundedProbeOverflow(aspI1PreflightBounded, expansionModel.ShortestPathEnumerationLimit)
	preflightExists := pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)}, From: []pgsql.FromClause{tableFrom(aspI1PreflightBounded)},
	}, Limit: pgsql.NewLiteral(int64(1), pgsql.Int8)}}}

	anchor := pgsql.Select{
		Projection: pgsql.Projection{
			pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
			pgsql.NewLiteral(int64(0), pgsql.Int8),
		},
		From:  []pgsql.FromClause{tableFrom(validatedEndpoints)},
		Where: pgd.Not(preflightExists),
	}
	recursive := pgsql.Select{
		Projection: pgsql.Projection{
			expansionModel.EdgeEndColumn,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{aspI1Distance, expansionDepth},
				pgsql.OperatorAdd,
				pgsql.NewLiteral(int64(1), pgsql.Int8),
			),
		},
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: aspI1Distance.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						expansionModel.EdgeStartColumn,
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{aspI1Distance, aspI1NodeID},
					),
				},
			}},
		}},
		Where: pgsql.OptionalAnd(
			expansionModel.EdgeConstraints,
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{aspI1Distance, expansionDepth},
				pgsql.OperatorLessThan,
				pgsql.NewLiteral(expansionModel.Options.MaxDepth.Value, pgsql.Int8),
			),
		),
	}

	distance := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: aspI1Distance, Shape: pgsql.NewRecordShape([]pgsql.Identifier{aspI1NodeID, expansionDepth})},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: anchor,
			ROperand: recursive,
			Operator: pgsql.OperatorUnion,
		}},
	}
	distanceBounded := boundedTraversalStateProbe(
		aspI1DistanceBounded,
		aspI1Distance,
		[]pgsql.Identifier{aspI1NodeID, expansionDepth},
		expansionModel.ShortestPathStateLimit,
	)
	stateOverflow := boundedProbeOverflow(aspI1DistanceBounded, expansionModel.ShortestPathStateLimit)

	target := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1Target, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionDepth})},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{pgsql.CompoundIdentifier{aspI1DistanceBounded, expansionDepth}},
				From:       []pgsql.FromClause{tableFrom(aspI1DistanceBounded)},
				Where: pgsql.OptionalAnd(
					pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{aspI1DistanceBounded, aspI1NodeID},
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
					),
					pgd.Not(stateOverflow),
				),
			},
			OrderBy: []*pgsql.OrderBy{{Expression: pgsql.CompoundIdentifier{aspI1DistanceBounded, expansionDepth}, Ascending: true}},
			Limit:   pgsql.NewLiteral(int64(1), pgsql.Int8),
		},
	}
	// The endpoint relation is correlated through a scalar subquery so target
	// retains a single FROM source and a stable materialization shape.
	targetSelect := target.Query.Body.(pgsql.Select)
	targetTerminal := pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID}},
		From:       []pgsql.FromClause{tableFrom(validatedEndpoints)},
	}}}
	targetSelect.Where = pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{aspI1DistanceBounded, aspI1NodeID},
			pgsql.OperatorEquals,
			targetTerminal,
		),
		pgd.Not(stateOverflow),
	)
	target.Query.Body = targetSelect

	child, prior := pgsql.Identifier("asp_i1_child"), pgsql.Identifier("asp_i1_prior")
	predecessorEdgeConstraint := pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{prior, expansionDepth},
			pgsql.OperatorEquals,
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{child, expansionDepth}, pgsql.OperatorSubtract, pgsql.NewLiteral(int64(1), pgsql.Int8)),
		),
		expansionModel.EdgeConstraints,
	)
	if s.traversalStep.Direction == graph.DirectionOutbound {
		predecessorEdgeConstraint = pgsql.OptionalAnd(predecessorEdgeConstraint,
			pgsql.OptionalAnd(
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnStartID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{prior, aspI1NodeID}),
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnEndID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{child, aspI1NodeID}),
			),
		)
	} else {
		predecessorEdgeConstraint = pgsql.OptionalAnd(predecessorEdgeConstraint,
			pgsql.OptionalAnd(
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnEndID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{prior, aspI1NodeID}),
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnStartID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{child, aspI1NodeID}),
			),
		)
	}

	predecessor := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: aspI1Predecessor, Shape: pgsql.NewRecordShape([]pgsql.Identifier{
			aspI1NodeID, expansionDepth, aspI1PredecessorID, aspI1EdgeID,
		})},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{
				pgsql.CompoundIdentifier{child, aspI1NodeID},
				pgsql.CompoundIdentifier{child, expansionDepth},
				pgsql.CompoundIdentifier{prior, aspI1NodeID},
				pgsql.CompoundIdentifier{s.traversalStep.Edge.Identifier, pgsql.ColumnID},
			},
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{Name: aspI1Target.AsCompoundIdentifier()},
				Joins: []pgsql.Join{
					{Table: aspI1Table(aspI1DistanceBounded, child), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.OptionalAnd(
						pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{child, expansionDepth}, pgsql.OperatorGreaterThan, pgsql.NewLiteral(int64(0), pgsql.Int8)),
						pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{child, expansionDepth}, pgsql.OperatorLessThanOrEqualTo, pgsql.CompoundIdentifier{aspI1Target, expansionDepth}),
					)}},
					{Table: aspI1Table(aspI1DistanceBounded, prior), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewLiteral(true, pgsql.Boolean)}},
					{Table: expansionEdgeTableReference(s.traversalStep.Edge.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: predecessorEdgeConstraint}},
				},
			}},
		}},
	}
	predecessorBounded := boundedTraversalStateProbe(
		aspI1PredecessorBounded,
		aspI1Predecessor,
		[]pgsql.Identifier{aspI1NodeID, expansionDepth, aspI1PredecessorID, aspI1EdgeID},
		expansionModel.ShortestPathPredecessorLimit,
	)
	predecessorOverflow := boundedProbeOverflow(aspI1PredecessorBounded, expansionModel.ShortestPathPredecessorLimit)

	pathAnchor := pgsql.Select{
		Projection: pgsql.Projection{
			pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
			pgsql.CompoundIdentifier{aspI1Target, expansionDepth},
			pgsql.ArrayLiteral{CastType: pgsql.Int8Array},
		},
		From: []pgsql.FromClause{
			tableFrom(aspI1Target),
			tableFrom(validatedEndpoints),
		},
		Where: pgd.Not(pgsql.NewParenthetical(aspI1OverflowAny(stateOverflow, predecessorOverflow))),
	}
	pathRecursive := pgsql.Select{
		Projection: pgsql.Projection{
			pgsql.CompoundIdentifier{aspI1PredecessorBounded, aspI1PredecessorID},
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{aspI1Paths, expansionDepth}, pgsql.OperatorSubtract, pgsql.NewLiteral(int64(1), pgsql.Int8)),
			pgsql.NewBinaryExpression(
				pgsql.ArrayLiteral{Values: []pgsql.Expression{pgsql.CompoundIdentifier{aspI1PredecessorBounded, aspI1EdgeID}}, CastType: pgsql.Int8Array},
				pgsql.OperatorConcatenate,
				pgsql.CompoundIdentifier{aspI1Paths, expansionPath},
			),
		},
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: aspI1Paths.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: pgsql.TableReference{Name: aspI1PredecessorBounded.AsCompoundIdentifier()},
				JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.OptionalAnd(
					pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{aspI1PredecessorBounded, aspI1NodeID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{aspI1Paths, aspI1NodeID}),
					pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{aspI1PredecessorBounded, expansionDepth}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{aspI1Paths, expansionDepth}),
				)},
			}},
		}},
	}
	paths := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: aspI1Paths, Shape: pgsql.NewRecordShape([]pgsql.Identifier{aspI1NodeID, expansionDepth, expansionPath})},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: pathAnchor,
			ROperand: pathRecursive,
			Operator: pgsql.OperatorUnion,
			All:      true,
		}},
	}
	pathsBounded := boundedTraversalStateProbe(
		aspI1PathsBounded,
		aspI1Paths,
		[]pgsql.Identifier{aspI1NodeID, expansionDepth, expansionPath},
		expansionModel.ShortestPathEnumerationLimit,
	)
	enumerationOverflow := boundedProbeOverflow(aspI1PathsBounded, expansionModel.ShortestPathEnumerationLimit)

	shortest := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1Shortest, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionDepth, expansionPath})},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{
				pgsql.CompoundIdentifier{aspI1Target, expansionDepth},
				pgsql.CompoundIdentifier{aspI1PathsBounded, expansionPath},
			},
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{Name: aspI1PathsBounded.AsCompoundIdentifier()},
				Joins:  []pgsql.Join{{Table: pgsql.TableReference{Name: aspI1Target.AsCompoundIdentifier()}, JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewLiteral(true, pgsql.Boolean)}}},
			}},
			Where: pgsql.OptionalAnd(
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{aspI1PathsBounded, aspI1NodeID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID}),
				pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{aspI1PathsBounded, expansionDepth}, pgsql.OperatorEquals, pgsql.NewLiteral(int64(0), pgsql.Int8)),
			),
		}},
	}
	if mode.oneWitness {
		shortestSelect := shortest.Query.Body.(pgsql.Select)
		// ORDER BY at a UNION boundary may reference only the set output name,
		// not a source relation that belongs to one operand.
		shortest.Query.OrderBy = []*pgsql.OrderBy{{Expression: pgsql.CompoundIdentifier{expansionPath}, Ascending: true}}
		shortest.Query.Limit = pgsql.NewLiteral(int64(1), pgsql.Int8)
		shortest.Query.Body = shortestSelect
	}
	shortestSelect := shortest.Query.Body.(pgsql.Select)
	shortestSelect.From = append(shortestSelect.From, tableFrom(validatedEndpoints))
	shortest.Query.Body = shortestSelect
	shortest.Query.Body = pgsql.SetOperation{
		LOperand: pgsql.Select{
			Projection: pgsql.Projection{
				pgsql.CompoundIdentifier{aspI1PreflightBounded, expansionDepth},
				pgsql.CompoundIdentifier{aspI1PreflightBounded, expansionPath},
			},
			From: []pgsql.FromClause{tableFrom(aspI1PreflightBounded)},
		},
		ROperand: shortest.Query.Body.(pgsql.Select), Operator: pgsql.OperatorUnion, All: true,
	}

	bytesOverflow := pgsql.NewBinaryExpression(
		aspI1OutputBytes(aspI1Shortest),
		pgsql.OperatorGreaterThan,
		pgsql.NewLiteral(expansionModel.ShortestPathOutputBytesLimit, pgsql.Int8),
	)
	overflow := aspI1OverflowAny(preflightOverflow, stateOverflow, predecessorOverflow, enumerationOverflow, bytesOverflow)
	useCandidate := pgd.Not(pgsql.NewParenthetical(overflow))
	noPath := pgd.Not(pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{
		Body: pgsql.Select{
			Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)},
			From:       []pgsql.FromClause{tableFrom(aspI1Shortest)},
		},
		Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
	}}})
	admission := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1Admission},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{
			aspI1Aliased(overflow, aspI1Overflow),
			aspI1Aliased(noPath, aspI1NoPath),
		}}},
	}
	admissionOverflow := pgsql.CompoundIdentifier{aspI1Admission, aspI1Overflow}
	admissionNoPath := pgsql.CompoundIdentifier{aspI1Admission, aspI1NoPath}
	useCandidate = pgd.Not(admissionOverflow)
	candidateBranch := "inline_predecessor_dag"
	noPathBranch := "inline_no_path"
	fallbackBranch := "exact_a1_fallback"
	if mode.oneWitness {
		candidateBranch = "inline_canonical_witness"
		noPathBranch = "inline_canonical_no_path"
		fallbackBranch = "exact_s4_fallback"
	}
	branch := pgsql.Case{
		Conditions: []pgsql.Expression{admissionOverflow, admissionNoPath},
		Then: []pgsql.Expression{
			pgsql.NewLiteral(fallbackBranch, pgsql.Text),
			pgsql.NewLiteral(noPathBranch, pgsql.Text),
		},
		Else: pgsql.NewLiteral(candidateBranch, pgsql.Text),
	}
	runtimeExecutor := pgsql.Case{
		Conditions: []pgsql.Expression{admissionOverflow},
		Then:       []pgsql.Expression{pgsql.NewLiteral(string(mode.fallback), pgsql.Text)},
		Else:       pgsql.NewLiteral(string(mode.identity), pgsql.Text),
	}
	decision := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1Decision},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{
			aspI1Aliased(useCandidate, aspI1UseCandidate),
			aspI1Aliased(admissionOverflow, aspI1UseFallback),
			aspI1Aliased(pgsql.FunctionCall{
				Function: aspI1RuntimeAttestationFn,
				Parameters: []pgsql.Expression{
					branch,
					admissionOverflow,
					runtimeExecutor,
				},
			}, aspI1RuntimeReceipt),
		}, From: []pgsql.FromClause{tableFrom(aspI1Admission)}}},
	}

	candidateProjection := pgsql.Projection{
		aspI1Aliased(pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID}, expansionRootID),
		aspI1Aliased(pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID}, expansionNextID),
		aspI1Aliased(pgsql.CompoundIdentifier{aspI1Shortest, expansionDepth}, expansionDepth),
		aspI1Aliased(pgsql.NewLiteral(true, pgsql.Boolean), expansionSatisfied),
		aspI1Aliased(pgsql.NewLiteral(false, pgsql.Boolean), expansionIsCycle),
		aspI1Aliased(pgsql.CompoundIdentifier{aspI1Shortest, expansionPath}, expansionPath),
	}
	candidateQuery := pgsql.Query{Body: pgsql.Select{
		Projection: candidateProjection,
		From: []pgsql.FromClause{
			tableFrom(validatedEndpoints),
			tableFrom(aspI1Shortest),
		},
	}}
	candidateBody, err := gateQueryBehindMarker(aspI1CandidateMarker, aspI1CandidateBody, candidateQuery, candidateProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	candidateRows := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1CandidateRows, Shape: expansionColumns()},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: candidateBody},
	}

	fallbackFunction := pgsql.FunctionAllShortestPathsDAG
	if mode.oneWitness {
		fallbackFunction = pgsql.FunctionShortestPathCompact
	}
	fallbackProjection := aspI1CanonicalProjection(fallbackFunction)
	fallbackParameters := []pgsql.Expression{
		pgsql.NewLiteral(s.graphID, pgsql.Int4),
		pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
		pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
		pgsql.NewLiteral(int64(1), pgsql.Int4),
		pgsql.NewLiteral(expansionModel.Options.MaxDepth.Value, pgsql.Int4),
		pgsql.NewLiteral(append([]int16(nil), expansionModel.RelationshipKindIDs...), pgsql.Int2Array),
		pgsql.NewLiteral(s.traversalStep.Direction == graph.DirectionInbound, pgsql.Boolean),
	}
	if mode.oneWitness {
		fallbackParameters = append(fallbackParameters, pgsql.NewLiteral(expansionModel.ShortestPathStateLimit, pgsql.Int8))
	}
	fallbackQuery := pgsql.Query{Body: pgsql.Select{
		Projection: fallbackProjection,
		From: []pgsql.FromClause{
			tableFrom(validatedEndpoints),
			{Source: pgsql.FunctionCall{
				Function:   fallbackFunction,
				Parameters: fallbackParameters,
			}},
		},
	}}
	fallbackBody, err := gateQueryBehindMarker(aspI1FallbackMarker, aspI1FallbackBody, fallbackQuery, fallbackProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	fallbackRows := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: aspI1FallbackRows, Shape: expansionColumns()},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: fallbackBody},
	}

	stateID := expansionModel.Frame.Binding.Identifier
	search := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: stateID, Shape: expansionColumns()},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: pgsql.Select{Projection: aspI1CanonicalProjection(aspI1CandidateRows), From: []pgsql.FromClause{tableFrom(aspI1CandidateRows)}},
			ROperand: pgsql.Select{Projection: aspI1CanonicalProjection(aspI1FallbackRows), From: []pgsql.FromClause{tableFrom(aspI1FallbackRows)}},
			Operator: pgsql.OperatorUnion,
			All:      true,
		}},
	}

	projection := pgsql.Select{
		Projection: expansionModel.Projection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{stateID, expansionRootID},
				)}},
				{Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier), JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
					pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{stateID, expansionNextID},
				)}},
			},
		}},
	}
	if mode.oneWitness {
		const (
			hydrated      pgsql.Identifier = "m0_hydrated"
			hydratedNodes pgsql.Identifier = "nodes"
			hydratedEdges pgsql.Identifier = "edges"
			hydratedCount pgsql.Identifier = "hydrated_count"
		)
		pathIDs := pgsql.CompoundIdentifier{stateID, expansionPath}
		hydration := shortestPathM0Hydration(stateID, s.traversalStep.Direction)
		path := pgsql.CompositeValue{DataType: pgsql.PathComposite, Values: []pgsql.Expression{
			pgsql.NewBinaryExpression(
				pgsql.ArrayLiteral{Values: []pgsql.Expression{shortestPathNodeComposite(s.traversalStep.LeftNode.Identifier)}, CastType: pgsql.NodeCompositeArray},
				pgsql.OperatorConcatenate,
				pgsql.FunctionCall{Function: pgsql.FunctionCoalesce, Parameters: []pgsql.Expression{pgsql.CompoundIdentifier{hydrated, hydratedNodes}, pgsql.ArrayLiteral{CastType: pgsql.NodeCompositeArray}}},
			),
			pgsql.FunctionCall{Function: pgsql.FunctionCoalesce, Parameters: []pgsql.Expression{pgsql.CompoundIdentifier{hydrated, hydratedEdges}, pgsql.ArrayLiteral{CastType: pgsql.EdgeCompositeArray}}},
		}}
		projection.Projection = shortestPathM0Projection(projection.Projection, stateID, path)
		projection.From[0].Joins = append(projection.From[0].Joins, pgsql.Join{Table: hydration, JoinOperator: pgsql.JoinOperator{
			JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
		}})
		projection.Where = pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{hydrated, hydratedCount}, pgsql.OperatorEquals,
			pgsql.FunctionCall{Function: pgsql.FunctionCardinality, Parameters: []pgsql.Expression{pathIDs}},
		)
	}

	query := pgsql.Query{CommonTableExpressions: &pgsql.With{Recursive: true}, Body: projection}
	for _, cte := range []pgsql.CommonTableExpression{
		endpointCTE,
		directCTE,
		preflight,
		preflightBounded,
		distance,
		distanceBounded,
		target,
		predecessor,
		predecessorBounded,
		paths,
		pathsBounded,
		shortest,
		admission,
		decision,
		aspI1Marker(aspI1CandidateMarker, aspI1UseCandidate),
		aspI1Marker(aspI1FallbackMarker, aspI1UseFallback),
		candidateRows,
		fallbackRows,
		search,
	} {
		query.AddCTE(cte)
	}
	return query, nil
}
