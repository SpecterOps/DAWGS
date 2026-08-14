// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"errors"

	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
	"github.com/specterops/dawgs/graph"
)

const (
	spI2Distance             pgsql.Identifier = "sp_i2_distance"
	spI2DistanceBounded      pgsql.Identifier = "sp_i2_distance_bounded"
	spI2Target               pgsql.Identifier = "sp_i2_target"
	spI2Admission            pgsql.Identifier = "sp_i2_admission"
	spI2Decision             pgsql.Identifier = "sp_i2_decision"
	spI2CandidateMarker      pgsql.Identifier = "sp_i2_candidate_marker"
	spI2FallbackMarker       pgsql.Identifier = "sp_i2_fallback_marker"
	spI2CandidateBody        pgsql.Identifier = "sp_i2_candidate_body"
	spI2FallbackBody         pgsql.Identifier = "sp_i2_fallback_body"
	spI2CandidateRows        pgsql.Identifier = "sp_i2_candidate_rows"
	spI2FallbackRows         pgsql.Identifier = "sp_i2_fallback_rows"
	spI2NodeID               pgsql.Identifier = "node_id"
	spI2Overflow             pgsql.Identifier = "overflow"
	spI2UseCandidate         pgsql.Identifier = "use_candidate"
	spI2UseFallback          pgsql.Identifier = "use_fallback"
	spI2RuntimeReceipt       pgsql.Identifier = "runtime_receipt"
	spI2RuntimeAttestationFn pgsql.Identifier = "record_requested_traversal_runtime_attestation_v1"
)

// BuildInlineGuardedShortestDistanceRoot emits reverse-physical, ID-only
// minimum-distance discovery. The bounded candidate remains invisible until
// independent total-state and per-level frontier gates pass; overflow invokes
// exact compact S4 in the same top-level statement.
func (s *ExpansionBuilder) BuildInlineGuardedShortestDistanceRoot() (pgsql.Query, error) {
	return s.buildInlineGuardedShortestDistanceRoot(optimize.ShortestPathExecutorI2GuardedDistance, false)
}

// buildInlineGuardedShortestDistanceRoot retains the byte-stable V1 rendering
// when consolidatedAdmission is false. V2 uses the same proven recursive and
// fallback semantics while replacing only admission/target orchestration.
func (s *ExpansionBuilder) buildInlineGuardedShortestDistanceRoot(runtimeIdentity optimize.ShortestPathExecutor, consolidatedAdmission bool) (pgsql.Query, error) {
	const validatedEndpoints pgsql.Identifier = "singleton_endpoints"

	expansionModel := s.traversalStep.Expansion
	if !expansionModel.UsesSingletonEndpointPair() {
		return pgsql.Query{}, errors.New("SP-I2-C-D requires one validated endpoint pair")
	}
	if expansionModel.Options.MinDepth.GetOr(1) != 1 || !expansionModel.Options.MaxDepth.Set || expansionModel.Options.MaxDepth.Value < 1 || expansionModel.Options.MaxDepth.Value > 64 {
		return pgsql.Query{}, errors.New("SP-I2-C-D requires min depth 1 and bounded max depth <= 64")
	}
	if s.traversalStep.Direction != graph.DirectionOutbound && s.traversalStep.Direction != graph.DirectionInbound {
		return pgsql.Query{}, errors.New("SP-I2-C-D requires a directed traversal")
	}
	if expansionModel.ShortestPathStateLimit <= 0 || expansionModel.ShortestPathFrontierLimit <= 0 {
		return pgsql.Query{}, errors.New("SP-I2-C-D requires positive state and frontier limits")
	}

	endpointCTE := singletonEndpointValidationCTE(s.traversalStep, expansionModel)
	endpointSelect := endpointCTE.Query.Body.(pgsql.Select)
	endpointSelect.Where = pgsql.OptionalAnd(endpointSelect.Where, shortestPathSelfEndpointGuardCase(
		pgd.EntityID(s.traversalStep.LeftNode.Identifier),
		pgd.EntityID(s.traversalStep.RightNode.Identifier),
	))
	endpointCTE.Query.Body = endpointSelect

	// Search begins at the public terminal and follows the opposite physical
	// adjacency direction toward the public root. This is the canonical escape
	// from logical-inbound hidden fan-in while remaining correct for either
	// directed pattern orientation.
	edge := s.traversalStep.Edge.Identifier
	joinColumn, nextColumn := pgsql.ColumnEndID, pgsql.ColumnStartID
	if s.traversalStep.Direction == graph.DirectionInbound {
		joinColumn, nextColumn = pgsql.ColumnStartID, pgsql.ColumnEndID
	}
	var edgeScope pgsql.Expression = pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{edge, pgsql.ColumnGraphID}, pgsql.OperatorEquals, pgsql.NewLiteral(s.graphID, pgsql.Int4),
	)
	if len(expansionModel.RelationshipKindIDs) > 0 {
		edgeScope = pgsql.OptionalAnd(edgeScope, pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{edge, pgsql.ColumnKindID}, pgsql.OperatorEquals,
			pgsql.NewAnyExpressionHinted(pgsql.NewLiteral(append([]int16(nil), expansionModel.RelationshipKindIDs...), pgsql.Int2Array)),
		))
	}
	anchor := pgsql.Select{
		Projection: pgsql.Projection{
			pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
			pgsql.NewLiteral(int64(0), pgsql.Int8),
		},
		From: []pgsql.FromClause{tableFrom(validatedEndpoints)},
	}
	targetRoot := shortestDistanceEndpointID(validatedEndpoints, expansionRootID)
	recursive := pgsql.Select{
		Projection: pgsql.Projection{
			pgsql.CompoundIdentifier{edge, nextColumn},
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{spI2Distance, expansionDepth}, pgsql.OperatorAdd, pgsql.NewLiteral(int64(1), pgsql.Int8)),
		},
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: spI2Distance.AsCompoundIdentifier()},
			Joins: []pgsql.Join{{
				Table: expansionEdgeTableReference(edge),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{edge, joinColumn}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{spI2Distance, spI2NodeID},
					),
				},
			}},
		}},
		Where: pgsql.OptionalAnd(edgeScope, pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{spI2Distance, expansionDepth}, pgsql.OperatorLessThan,
				pgsql.NewLiteral(expansionModel.Options.MaxDepth.Value, pgsql.Int8),
			),
			// A walk that has reached the requested root already provides a
			// complete distance candidate. Expanding out of it can only create
			// longer walks, and on cycles it needlessly repeats work through the
			// maximum depth.
			pgsql.NewBinaryExpression(
				pgsql.CompoundIdentifier{spI2Distance, spI2NodeID}, pgsql.OperatorNotEquals, targetRoot,
			),
		)),
	}
	distance := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: spI2Distance, Shape: pgsql.NewRecordShape([]pgsql.Identifier{spI2NodeID, expansionDepth})},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: anchor,
			ROperand: recursive,
			Operator: pgsql.OperatorUnion,
		}},
	}
	distanceBounded := boundedTraversalStateProbe(
		spI2DistanceBounded, spI2Distance, []pgsql.Identifier{spI2NodeID, expansionDepth}, expansionModel.ShortestPathStateLimit,
	)
	stateOverflow := boundedProbeOverflow(spI2DistanceBounded, expansionModel.ShortestPathStateLimit)
	var frontierOverflow pgsql.Expression = pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{Body: pgsql.Select{
		Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)},
		From:       []pgsql.FromClause{tableFrom(spI2DistanceBounded)},
		GroupBy:    []pgsql.Expression{pgsql.CompoundIdentifier{spI2DistanceBounded, expansionDepth}},
		Having: pgsql.NewBinaryExpression(
			pgsql.FunctionCall{Function: pgsql.FunctionCount, Parameters: []pgsql.Expression{pgsql.Wildcard{}}, CastType: pgsql.Int8},
			pgsql.OperatorGreaterThan,
			pgsql.NewLiteral(expansionModel.ShortestPathFrontierLimit, pgsql.Int8),
		),
	}, Limit: pgsql.NewLiteral(int64(1), pgsql.Int8)}}}
	frontierGuardDominated := consolidatedAdmission && expansionModel.ShortestPathFrontierLimit >= expansionModel.ShortestPathStateLimit
	if frontierGuardDominated {
		// Every frontier row is also a state row. When the frontier cap is at
		// least the state cap, state admission strictly dominates the frontier
		// check and the depth aggregate is redundant.
		frontierOverflow = pgsql.NewLiteral(false, pgsql.Boolean)
	}
	overflow := aspI1OverflowAny(stateOverflow, frontierOverflow)

	admission := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: spI2Admission},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{
			aspI1Aliased(overflow, spI2Overflow),
		}}},
	}
	if consolidatedAdmission {
		admission.Query.Body = pgsql.Select{Projection: pgsql.Projection{
			aspI1Aliased(overflow, spI2Overflow),
			aspI1Aliased(pgsql.NewLiteral(frontierGuardDominated, pgsql.Boolean), pgsql.Identifier("frontier_guard_dominated")),
			aspI1Aliased(pgsql.NewLiteral(expansionModel.ShortestPathStateLimit, pgsql.Int8), pgsql.Identifier("state_limit")),
			aspI1Aliased(pgsql.NewLiteral(expansionModel.ShortestPathFrontierLimit, pgsql.Int8), pgsql.Identifier("frontier_limit")),
		}}
	}
	admissionOverflow := pgsql.CompoundIdentifier{spI2Admission, spI2Overflow}

	targetWhere := pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{spI2DistanceBounded, spI2NodeID}, pgsql.OperatorEquals, targetRoot),
		pgd.Not(pgsql.NewParenthetical(overflow)),
	)
	targetFrom := []pgsql.FromClause{tableFrom(spI2DistanceBounded)}
	if consolidatedAdmission {
		targetWhere = pgsql.OptionalAnd(
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{spI2DistanceBounded, spI2NodeID}, pgsql.OperatorEquals, targetRoot),
			pgd.Not(admissionOverflow),
		)
		targetFrom = append(targetFrom, tableFrom(spI2Admission))
	}
	target := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: spI2Target, Shape: pgsql.NewRecordShape([]pgsql.Identifier{expansionDepth})},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{pgsql.CompoundIdentifier{spI2DistanceBounded, expansionDepth}},
				From:       targetFrom,
				Where:      targetWhere,
			},
			OrderBy: []*pgsql.OrderBy{{Expression: pgsql.CompoundIdentifier{spI2DistanceBounded, expansionDepth}, Ascending: true}},
			Limit:   pgsql.NewLiteral(int64(1), pgsql.Int8),
		},
	}

	noPath := pgd.Not(pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{
		Body:  pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)}, From: []pgsql.FromClause{tableFrom(spI2Target)}},
		Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
	}}})
	branch := pgsql.Case{
		Conditions: []pgsql.Expression{admissionOverflow, noPath},
		Then: []pgsql.Expression{
			pgsql.NewLiteral("exact_s4_distance_fallback", pgsql.Text),
			pgsql.NewLiteral("inline_canonical_distance_no_path", pgsql.Text),
		},
		Else: pgsql.NewLiteral("inline_canonical_distance", pgsql.Text),
	}
	runtimeExecutor := pgsql.Case{
		Conditions: []pgsql.Expression{admissionOverflow},
		Then:       []pgsql.Expression{pgsql.NewLiteral(string(optimize.ShortestPathExecutorS4CanonicalDistance), pgsql.Text)},
		Else:       pgsql.NewLiteral(string(runtimeIdentity), pgsql.Text),
	}
	decision := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: spI2Decision},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: pgsql.Projection{
				aspI1Aliased(pgd.Not(admissionOverflow), spI2UseCandidate),
				aspI1Aliased(admissionOverflow, spI2UseFallback),
				aspI1Aliased(pgsql.FunctionCall{Function: spI2RuntimeAttestationFn, Parameters: []pgsql.Expression{branch, admissionOverflow, runtimeExecutor}}, spI2RuntimeReceipt),
			},
			From: []pgsql.FromClause{tableFrom(spI2Admission)},
		}},
	}
	marker := func(alias, selected pgsql.Identifier) pgsql.CommonTableExpression {
		return pgsql.CommonTableExpression{
			Alias:        pgsql.TableAlias{Name: alias},
			Materialized: &pgsql.Materialized{Materialized: true},
			Query: pgsql.Query{Body: pgsql.Select{
				Projection: pgsql.Projection{aspI1Aliased(pgsql.NewLiteral(true, pgsql.Boolean), orientationArmExecuted)},
				From:       []pgsql.FromClause{tableFrom(spI2Decision)},
				Where:      pgsql.CompoundIdentifier{spI2Decision, selected},
			}},
		}
	}

	candidateProjection := pgsql.Projection{
		aspI1Aliased(pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID}, expansionRootID),
		aspI1Aliased(pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID}, expansionNextID),
		aspI1Aliased(pgsql.CompoundIdentifier{spI2Target, expansionDepth}, expansionDepth),
		aspI1Aliased(pgsql.NewLiteral(true, pgsql.Boolean), expansionSatisfied),
		aspI1Aliased(pgsql.NewLiteral(false, pgsql.Boolean), expansionIsCycle),
		aspI1Aliased(pgsql.ArrayLiteral{CastType: pgsql.Int8Array}, expansionPath),
	}
	candidateQuery := pgsql.Query{Body: pgsql.Select{
		Projection: candidateProjection,
		From:       []pgsql.FromClause{tableFrom(validatedEndpoints), tableFrom(spI2Target)},
	}}
	candidateBody, err := gateQueryBehindMarker(spI2CandidateMarker, spI2CandidateBody, candidateQuery, candidateProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	candidateRows := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: spI2CandidateRows, Shape: expansionColumns()},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: candidateBody},
	}

	fallbackProjection := aspI1CanonicalProjection(pgsql.FunctionShortestPathCompact)
	fallbackQuery := pgsql.Query{Body: pgsql.Select{
		Projection: fallbackProjection,
		From: []pgsql.FromClause{
			tableFrom(validatedEndpoints),
			{Source: pgsql.FunctionCall{
				Function: pgsql.FunctionShortestPathCompact,
				Parameters: []pgsql.Expression{
					pgsql.NewLiteral(s.graphID, pgsql.Int4),
					pgsql.CompoundIdentifier{validatedEndpoints, expansionRootID},
					pgsql.CompoundIdentifier{validatedEndpoints, expansionTerminalID},
					pgsql.NewLiteral(int64(1), pgsql.Int4),
					pgsql.NewLiteral(expansionModel.Options.MaxDepth.Value, pgsql.Int4),
					pgsql.NewLiteral(append([]int16(nil), expansionModel.RelationshipKindIDs...), pgsql.Int2Array),
					pgsql.NewLiteral(s.traversalStep.Direction == graph.DirectionInbound, pgsql.Boolean),
					pgsql.NewLiteral(expansionModel.ShortestPathStateLimit, pgsql.Int8),
				},
			}},
		},
	}}
	fallbackBody, err := gateQueryBehindMarker(spI2FallbackMarker, spI2FallbackBody, fallbackQuery, fallbackProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	fallbackRows := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: spI2FallbackRows, Shape: expansionColumns()},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: fallbackBody},
	}

	stateID := expansionModel.Frame.Binding.Identifier
	search := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: stateID, Shape: expansionColumns()},
		Query: pgsql.Query{Body: pgsql.SetOperation{
			LOperand: pgsql.Select{Projection: aspI1CanonicalProjection(spI2CandidateRows), From: []pgsql.FromClause{tableFrom(spI2CandidateRows)}},
			ROperand: pgsql.Select{Projection: aspI1CanonicalProjection(spI2FallbackRows), From: []pgsql.FromClause{tableFrom(spI2FallbackRows)}},
			Operator: pgsql.OperatorUnion,
			All:      true,
		}},
	}
	projection := pgsql.Select{
		Projection: expansionModel.Projection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: stateID.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{
					Table: expansionNodeTableReference(s.traversalStep.LeftNode.Identifier),
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{s.traversalStep.LeftNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{stateID, expansionRootID},
					)},
				},
				{
					Table: expansionNodeTableReference(s.traversalStep.RightNode.Identifier),
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{s.traversalStep.RightNode.Identifier, pgsql.ColumnID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{stateID, expansionNextID},
					)},
				},
			},
		}},
	}

	query := pgsql.Query{CommonTableExpressions: &pgsql.With{Recursive: true}, Body: projection}
	query.AddCTE(endpointCTE)
	query.AddCTE(distance)
	query.AddCTE(distanceBounded)
	if consolidatedAdmission {
		query.AddCTE(admission)
		query.AddCTE(target)
	} else {
		query.AddCTE(target)
		query.AddCTE(admission)
	}
	query.AddCTE(decision)
	query.AddCTE(marker(spI2CandidateMarker, spI2UseCandidate))
	query.AddCTE(marker(spI2FallbackMarker, spI2UseFallback))
	query.AddCTE(candidateRows)
	query.AddCTE(fallbackRows)
	query.AddCTE(search)
	return query, nil
}
