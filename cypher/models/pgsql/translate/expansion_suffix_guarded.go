// Copyright 2026 Specter Ops, Inc.
// SPDX-License-Identifier: Apache-2.0

package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

const (
	suffixGuardSuffixOverflow pgsql.Identifier = "suffix_overflow"
	suffixGuardStateOverflow  pgsql.Identifier = "state_overflow"
	suffixGuardUseCandidate   pgsql.Identifier = "use_candidate"
	suffixGuardUseFallback    pgsql.Identifier = "use_fallback"
	suffixGuardRuntimeReceipt pgsql.Identifier = "runtime_receipt"
	suffixGuardAttestationFn  pgsql.Identifier = "record_requested_traversal_runtime_attestation_v1"
	suffixRetryStatusSetting                   = "dawgs.suffix_reverse_retry_status"
)

// suffixReverseGuardIdentifiers assigns stable, policy-specific names to the
// bounded inputs, admission decision, and mutually exclusive execution arms.
// Keeping this namespace separate from orientation makes plan evidence unable
// to attribute this static guard to the rejected topology selector family.
type suffixReverseGuardIdentifiers struct {
	rootPresence    pgsql.Identifier
	suffixProbe     pgsql.Identifier
	boundaries      pgsql.Identifier
	reverse         pgsql.Identifier
	states          pgsql.Identifier
	admission       pgsql.Identifier
	decision        pgsql.Identifier
	candidateMarker pgsql.Identifier
	fallbackMarker  pgsql.Identifier
	candidateBody   pgsql.Identifier
	fallbackBody    pgsql.Identifier
	fallbackRows    pgsql.Identifier
}

func newSuffixReverseGuardIdentifiers(finalFrame pgsql.Identifier) suffixReverseGuardIdentifiers {
	prefix := string(finalFrame) + "_suffix_guard_"
	return suffixReverseGuardIdentifiers{
		rootPresence:    pgsql.Identifier(prefix + "root_presence"),
		suffixProbe:     pgsql.Identifier(prefix + "suffix_probe"),
		boundaries:      pgsql.Identifier(prefix + "boundaries"),
		reverse:         pgsql.Identifier(prefix + "reverse"),
		states:          pgsql.Identifier(prefix + "states"),
		admission:       pgsql.Identifier(prefix + "admission"),
		decision:        pgsql.Identifier(prefix + "decision"),
		candidateMarker: pgsql.Identifier(prefix + "candidate_marker"),
		fallbackMarker:  pgsql.Identifier(prefix + "fallback_marker"),
		candidateBody:   pgsql.Identifier(prefix + "candidate_body"),
		fallbackBody:    pgsql.Identifier(prefix + "fallback_body"),
		fallbackRows:    pgsql.Identifier(prefix + "fallback_rows"),
	}
}

// rewriteTraversalPatternAsSuffixReverseGuard replaces the incumbent frame
// chain with a static full-path reverse candidate and the unchanged incumbent
// behind a bounded, same-statement fallback boundary.
func (s *Translator) rewriteTraversalPatternAsSuffixReverseGuard(part *PatternPart, decision optimize.ExpansionSearchStrategyDecision, firstCTE int) error {
	if (decision.EmittedPolicy != optimize.ExpansionSearchPolicySuffixReverseGuardV1 &&
		decision.EmittedPolicy != optimize.ExpansionSearchPolicySuffixReverseRetryV1 &&
		decision.EmittedPolicy != optimize.ExpansionSearchPolicyTopologyFixedSuffixV1) ||
		decision.ObservationMode != optimize.ExpansionSearchObservationFullPath || part.PatternBinding == nil {
		return fmt.Errorf("suffix reverse guard requires the full-path policy envelope")
	}
	if len(part.TraversalSteps) != decision.SuffixEndStep+1 || decision.SuffixLength != 3 || decision.Target.StepIndex != 0 {
		return fmt.Errorf("suffix reverse guard requires one expansion followed by exactly three terminal suffix steps")
	}
	if decision.ProbeCaps.ReverseSeedRowLimit <= 0 || decision.Admission.StateLimit <= 0 ||
		decision.Admission.FallbackStrategy != optimize.ExpansionSearchStepwiseForward {
		return fmt.Errorf("suffix reverse guard requires positive immutable suffix/state caps and exact stepwise-forward fallback")
	}

	expansionStep := part.TraversalSteps[decision.Target.StepIndex]
	if expansionStep == nil || expansionStep.Expansion == nil || expansionStep.Frame == nil || expansionStep.Frame.Previous == nil ||
		!expansionStep.LeftNodeBound || expansionStep.Edge == nil || expansionStep.LeftNode == nil {
		return fmt.Errorf("suffix reverse guard requires a complete expansion and bound root")
	}
	suffix := part.TraversalSteps[decision.SuffixStartStep : decision.SuffixEndStep+1]
	for _, step := range suffix {
		if step == nil || step.Frame == nil || step.Edge == nil || step.LeftNode == nil || step.RightNode == nil {
			return fmt.Errorf("suffix reverse guard has an incomplete fixed suffix step")
		}
	}

	ctes := s.query.CurrentPart().Model.CommonTableExpressions.Expressions
	if firstCTE < 0 || firstCTE >= len(ctes) {
		return fmt.Errorf("suffix reverse guard did not emit an incumbent frame chain")
	}
	incumbentChain := append([]pgsql.CommonTableExpression(nil), ctes[firstCTE:]...)
	incumbentFinal := incumbentChain[len(incumbentChain)-1]
	if incumbentFinal.Alias.Name != suffix[len(suffix)-1].Frame.Binding.Identifier {
		return fmt.Errorf("suffix reverse guard final frame mismatch: expected %s but found %s", suffix[len(suffix)-1].Frame.Binding.Identifier, incumbentFinal.Alias.Name)
	}
	incumbentSelect, ok := incumbentFinal.Query.Body.(pgsql.Select)
	if !ok {
		return fmt.Errorf("suffix reverse guard final frame must be a select")
	}

	query, err := s.buildSuffixReverseGuardQuery(
		part,
		decision,
		expansionStep,
		suffix,
		expansionStep.Frame.Previous.Binding.Identifier,
		newSuffixReverseGuardIdentifiers(incumbentFinal.Alias.Name),
		incumbentChain,
		incumbentFinal.Alias.Name,
		incumbentSelect.Projection,
	)
	if err != nil {
		return err
	}

	part.PatternBinding.DataType = pgsql.PathComposite
	part.PatternBinding.Dependencies = nil
	part.PatternBinding.MaterializedBy(suffix[len(suffix)-1].Frame)
	s.query.CurrentPart().Model.CommonTableExpressions.Expressions = append(ctes[:firstCTE], pgsql.CommonTableExpression{
		Alias: incumbentFinal.Alias,
		Query: query,
	})
	s.recordExpansionSearchPolicy(decision.Target, decision.EmittedPolicy)
	return nil
}

// buildSuffixReverseGuardQuery performs no topology probes. A bounded suffix
// payload gates reverse seeds, bounded reverse state gates visibility, and one
// materialized decision records the runtime receipt before complementary
// marker-driven candidate/fallback branches execute.
func (s *Translator) buildSuffixReverseGuardQuery(
	part *PatternPart,
	decision optimize.ExpansionSearchStrategyDecision,
	expansionStep *TraversalStep,
	suffix []*TraversalStep,
	rootFrame pgsql.Identifier,
	ids suffixReverseGuardIdentifiers,
	incumbentChain []pgsql.CommonTableExpression,
	incumbentFinal pgsql.Identifier,
	incumbentProjection pgsql.Projection,
) (pgsql.Query, error) {
	_, externalEdgeConstraint := partitionConstraintByLocality(
		expansionStep.Expansion.EdgeConstraints,
		pgsql.AsIdentifierSet(expansionStep.Edge.Identifier),
	)
	if externalEdgeConstraint != nil {
		return pgsql.Query{}, fmt.Errorf("suffix reverse guard relationship predicate is not local")
	}
	suffixIDs := suffixSeededIdentifiers{
		rootPresence: ids.rootPresence,
		suffix:       ids.suffixProbe,
		boundaries:   ids.boundaries,
		reverse:      ids.reverse,
	}
	rootPresence := buildSuffixReverseGuardRootPresence(rootFrame, ids)
	suffixProbe, err := s.buildFixedSuffixProbeCTE(expansionStep, suffix, suffixIDs, decision.ProbeCaps.ReverseSeedRowLimit)
	if err != nil {
		return pgsql.Query{}, err
	}
	boundaries := buildSuffixReverseGuardBoundaries(ids, decision.ProbeCaps.ReverseSeedRowLimit)
	reverse, err := buildSuffixSeededReverseCTE(expansionStep, decision, suffixIDs, "", "", true)
	if err != nil {
		return pgsql.Query{}, err
	}
	states := boundedTraversalStateProbe(
		ids.states,
		ids.reverse,
		[]pgsql.Identifier{fixedSuffixBoundaryID, expansionNextID, expansionDepth, expansionPath, expansionNodePath},
		decision.Admission.StateLimit,
	)
	admission := buildSuffixReverseGuardAdmission(ids, decision.ProbeCaps.ReverseSeedRowLimit, decision.Admission.StateLimit)
	retryOnly := decision.EmittedPolicy == optimize.ExpansionSearchPolicySuffixReverseRetryV1 || decision.EmittedPolicy == optimize.ExpansionSearchPolicyTopologyFixedSuffixV1
	decisionCTE := buildSuffixReverseGuardDecision(ids, retryOnly)
	markers := buildSuffixReverseGuardMarkers(ids)

	candidateProjection, err := suffixSeededFinalProjection(part, expansionStep, suffix, rootFrame, suffixIDs, ids.states, incumbentProjection, nil)
	if err != nil {
		return pgsql.Query{}, err
	}
	candidateProjection = append(candidateProjection, &pgsql.AliasedExpression{
		Expression: suffixSeededOrderedPathComposite(s.graphID, expansionStep, suffix, suffixIDs, ids.states),
		Alias:      models.OptionalValue(part.PatternBinding.Identifier),
	})

	suffixEdgeIDs := pgsql.ArrayLiteral{CastType: pgsql.Int8Array}
	for _, step := range suffix {
		suffixEdgeIDs.Values = append(suffixEdgeIDs.Values, pgsql.CompoundIdentifier{ids.suffixProbe, step.Edge.Identifier})
	}
	candidateWhere := pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{ids.states, expansionDepth},
			pgsql.OperatorGreaterThanOrEqualTo,
			pgsql.NewLiteral(decision.MinimumDepth, pgsql.Int8),
		),
		pgd.Not(pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{ids.states, expansionPath},
			pgsql.OperatorArrayOverlap,
			suffixEdgeIDs,
		)),
	)
	candidateQuery := pgsql.Query{Body: pgsql.Select{
		Projection: candidateProjection,
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{Name: rootFrame.AsCompoundIdentifier()},
			Joins: []pgsql.Join{
				{
					Table: pgsql.TableReference{Name: ids.states.AsCompoundIdentifier()},
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
						projectedNodeIDReference(rootFrame, expansionStep.LeftNode),
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{ids.states, expansionNextID},
					)},
				},
				{
					Table: pgsql.TableReference{Name: ids.suffixProbe.AsCompoundIdentifier()},
					JoinOperator: pgsql.JoinOperator{JoinType: pgsql.JoinTypeInner, Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{ids.suffixProbe, fixedSuffixBoundaryID},
						pgsql.OperatorEquals,
						pgsql.CompoundIdentifier{ids.states, fixedSuffixBoundaryID},
					)},
				},
			},
		}},
		Where: candidateWhere,
	}}
	candidateExecutor := pgsql.Identifier(string(ids.candidateBody) + "_executor")
	candidate, err := gateQueryBehindMarker(ids.candidateMarker, candidateExecutor, candidateQuery, candidateProjection)
	if err != nil {
		return pgsql.Query{}, err
	}

	candidateRows := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.candidateBody},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: candidate},
	}
	candidateOutput, err := suffixReverseGuardOutputSelect(ids.candidateBody, candidateProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	if retryOnly {
		return pgsql.Query{
			CommonTableExpressions: &pgsql.With{
				Recursive: true,
				Expressions: []pgsql.CommonTableExpression{
					rootPresence,
					suffixProbe,
					boundaries,
					reverse,
					states,
					admission,
					decisionCTE,
					markers[0],
					candidateRows,
				},
			},
			Body:  candidateOutput,
			Limit: pgsql.NewLiteral(decision.Admission.OutputRowLimit+1, pgsql.Int8),
		}, nil
	}

	incumbentPath, err := expressionForPathComposite(part.PatternBinding, s.scope)
	if err != nil {
		return pgsql.Query{}, err
	}
	fallbackRows, fallbackProjection, err := buildSuffixReverseGuardFallbackCTE(
		ids.fallbackRows,
		incumbentChain,
		incumbentFinal,
		incumbentProjection,
		pgsql.Projection{&pgsql.AliasedExpression{
			Expression: incumbentPath,
			Alias:      models.OptionalValue(part.PatternBinding.Identifier),
		}},
	)
	if err != nil {
		return pgsql.Query{}, err
	}
	fallbackQuery := pgsql.Query{Body: pgsql.Select{
		Projection: fallbackProjection,
		From:       []pgsql.FromClause{tableFrom(ids.fallbackRows)},
	}}
	fallbackExecutor := pgsql.Identifier(string(ids.fallbackBody) + "_executor")
	fallback, err := gateQueryBehindMarker(ids.fallbackMarker, fallbackExecutor, fallbackQuery, fallbackProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	fallbackOutput := pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.fallbackBody},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        pgsql.Query{Body: fallback},
	}
	fallbackOutputSelect, err := suffixReverseGuardOutputSelect(ids.fallbackBody, fallbackProjection)
	if err != nil {
		return pgsql.Query{}, err
	}

	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive: true,
			Expressions: []pgsql.CommonTableExpression{
				rootPresence,
				suffixProbe,
				boundaries,
				reverse,
				states,
				admission,
				decisionCTE,
				markers[0],
				markers[1],
				fallbackRows,
				candidateRows,
				fallbackOutput,
			},
		},
		Body: pgsql.SetOperation{
			Operator: pgsql.OperatorUnion,
			All:      true,
			LOperand: candidateOutput,
			ROperand: fallbackOutputSelect,
		},
	}, nil
}

func suffixReverseGuardOutputSelect(alias pgsql.Identifier, prototype pgsql.Projection) (pgsql.Select, error) {
	projection := make(pgsql.Projection, 0, len(prototype))
	for _, item := range prototype {
		itemAlias, ok := selectItemAlias(item)
		if !ok {
			return pgsql.Select{}, fmt.Errorf("suffix reverse guard output projection contains an unaliased item %T", item)
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{alias, itemAlias},
			Alias:      models.OptionalValue(itemAlias),
		})
	}
	return pgsql.Select{Projection: projection, From: []pgsql.FromClause{tableFrom(alias)}}, nil
}

func buildSuffixReverseGuardRootPresence(rootFrame pgsql.Identifier, ids suffixReverseGuardIdentifiers) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{Name: ids.rootPresence},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)},
				From:       []pgsql.FromClause{tableFrom(rootFrame)},
			},
			Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
		},
	}
}

// buildSuffixReverseGuardBoundaries produces no reverse seed when the suffix
// cap+1 sentinel exists. Duplicate suffix paths remain in suffixProbe for final
// bag semantics; only recursive starting nodes are deduplicated.
func buildSuffixReverseGuardBoundaries(ids suffixReverseGuardIdentifiers, suffixRowLimit int64) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.boundaries},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Distinct: true,
			Projection: pgsql.Projection{&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{ids.suffixProbe, fixedSuffixBoundaryID},
				Alias:      models.OptionalValue(fixedSuffixBoundaryID),
			}},
			From: []pgsql.FromClause{tableFrom(ids.suffixProbe)},
			Where: pgsql.OptionalAnd(
				pgd.Not(boundedProbeOverflow(ids.suffixProbe, suffixRowLimit)),
				pgsql.ExistsExpression{Subquery: pgsql.Subquery{Query: pgsql.Query{
					Body:  pgsql.Select{Projection: pgsql.Projection{pgsql.NewLiteral(int64(1), pgsql.Int8)}, From: []pgsql.FromClause{tableFrom(ids.rootPresence)}},
					Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
				}}},
			),
		}},
	}
}

func buildSuffixReverseGuardAdmission(ids suffixReverseGuardIdentifiers, suffixRowLimit, stateLimit int64) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.admission},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{Projection: pgsql.Projection{
			aspI1Aliased(boundedProbeOverflow(ids.suffixProbe, suffixRowLimit), suffixGuardSuffixOverflow),
			aspI1Aliased(boundedProbeOverflow(ids.states, stateLimit), suffixGuardStateOverflow),
		}}},
	}
}

// buildSuffixReverseGuardDecision records exactly one requested-runtime
// attestation and exposes complementary booleans consumed by execution markers.
func buildSuffixReverseGuardDecision(ids suffixReverseGuardIdentifiers, retryOnly bool) pgsql.CommonTableExpression {
	suffixOverflow := pgsql.CompoundIdentifier{ids.admission, suffixGuardSuffixOverflow}
	stateOverflow := pgsql.CompoundIdentifier{ids.admission, suffixGuardStateOverflow}
	overflow := pgsql.NewBinaryExpression(suffixOverflow, pgsql.OperatorOr, stateOverflow)
	runtimeExecutor := pgsql.Case{
		Conditions: []pgsql.Expression{overflow},
		Then:       []pgsql.Expression{pgsql.NewLiteral(string(optimize.ExpansionSearchStepwiseForward), pgsql.Text)},
		Else:       pgsql.NewLiteral(string(optimize.ExpansionSearchSuffixSeededReverse), pgsql.Text),
	}
	branch := pgsql.Case{
		Conditions: []pgsql.Expression{suffixOverflow, stateOverflow},
		Then: []pgsql.Expression{
			pgsql.NewLiteral("exact_forward_suffix_overflow", pgsql.Text),
			pgsql.NewLiteral("exact_forward_state_overflow", pgsql.Text),
		},
		Else: pgsql.NewLiteral("suffix_seeded_reverse", pgsql.Text),
	}
	if retryOnly {
		branch = pgsql.Case{
			Conditions: []pgsql.Expression{suffixOverflow, stateOverflow},
			Then: []pgsql.Expression{
				pgsql.NewLiteral("forward_retry_suffix_overflow", pgsql.Text),
				pgsql.NewLiteral("forward_retry_state_overflow", pgsql.Text),
			},
			Else: pgsql.NewLiteral("reverse_complete", pgsql.Text),
		}
	}
	fallbackExecuted := pgsql.Expression(overflow)
	recordedExecutor := pgsql.Expression(runtimeExecutor)
	if retryOnly {
		fallbackExecuted = pgsql.NewLiteral(false, pgsql.Boolean)
		recordedExecutor = pgsql.NewLiteral(string(optimize.ExpansionSearchStepwiseForward), pgsql.Text)
	}
	projection := pgsql.Projection{
		aspI1Aliased(pgd.Not(pgsql.NewParenthetical(overflow)), suffixGuardUseCandidate),
		aspI1Aliased(overflow, suffixGuardUseFallback),
		aspI1Aliased(pgsql.FunctionCall{
			Function: suffixGuardAttestationFn,
			Parameters: []pgsql.Expression{
				branch,
				fallbackExecuted,
				recordedExecutor,
			},
		}, suffixGuardRuntimeReceipt),
	}
	if retryOnly {
		projection = append(projection, aspI1Aliased(pgsql.FunctionCall{
			Function: pgsql.Identifier("set_config"),
			Parameters: []pgsql.Expression{
				pgsql.NewLiteral(suffixRetryStatusSetting, pgsql.Text),
				branch,
				pgsql.NewLiteral(true, pgsql.Boolean),
			},
		}, pgsql.Identifier("retry_status")))
	}
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.decision},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Projection: projection,
			From:       []pgsql.FromClause{tableFrom(ids.admission)},
		}},
	}
}

func buildSuffixReverseGuardMarkers(ids suffixReverseGuardIdentifiers) []pgsql.CommonTableExpression {
	marker := func(alias, selected pgsql.Identifier) pgsql.CommonTableExpression {
		return pgsql.CommonTableExpression{
			Alias:        pgsql.TableAlias{Name: alias},
			Materialized: &pgsql.Materialized{Materialized: true},
			Query: pgsql.Query{Body: pgsql.Select{
				Projection: pgsql.Projection{aspI1Aliased(pgsql.NewLiteral(true, pgsql.Boolean), orientationArmExecuted)},
				From:       []pgsql.FromClause{tableFrom(ids.decision)},
				Where:      pgsql.CompoundIdentifier{ids.decision, selected},
			}},
		}
	}
	return []pgsql.CommonTableExpression{
		marker(ids.candidateMarker, suffixGuardUseCandidate),
		marker(ids.fallbackMarker, suffixGuardUseFallback),
	}
}

// buildSuffixReverseGuardFallbackCTE nests the original frame chain unchanged.
// The materialized CTE sits under the fallback marker's correlated lateral
// boundary, so the incumbent executor is not initialized on admitted runs.
func buildSuffixReverseGuardFallbackCTE(
	alias pgsql.Identifier,
	incumbentChain []pgsql.CommonTableExpression,
	incumbentFinal pgsql.Identifier,
	incumbentProjection pgsql.Projection,
	extraProjection pgsql.Projection,
) (pgsql.CommonTableExpression, pgsql.Projection, error) {
	projection := make(pgsql.Projection, 0, len(incumbentProjection)+len(extraProjection))
	fallback := make(pgsql.Projection, 0, len(incumbentProjection)+len(extraProjection))
	appendItem := func(item pgsql.SelectItem, from pgsql.Identifier) error {
		itemAlias, ok := selectItemAlias(item)
		if !ok {
			return fmt.Errorf("suffix reverse guard incumbent projection contains an unaliased item %T", item)
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{from, itemAlias},
			Alias:      models.OptionalValue(itemAlias),
		})
		fallback = append(fallback, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{alias, itemAlias},
			Alias:      models.OptionalValue(itemAlias),
		})
		return nil
	}
	for _, item := range incumbentProjection {
		if err := appendItem(item, incumbentFinal); err != nil {
			return pgsql.CommonTableExpression{}, nil, err
		}
	}
	for _, item := range extraProjection {
		itemAlias, ok := selectItemAlias(item)
		if !ok {
			return pgsql.CommonTableExpression{}, nil, fmt.Errorf("suffix reverse guard extra incumbent projection contains an unaliased item %T", item)
		}
		projection = append(projection, item)
		fallback = append(fallback, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{alias, itemAlias},
			Alias:      models.OptionalValue(itemAlias),
		})
	}

	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: alias},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{
			CommonTableExpressions: &pgsql.With{Expressions: incumbentChain},
			Body: pgsql.Select{
				Projection: projection,
				From:       []pgsql.FromClause{tableFrom(incumbentFinal)},
			},
		},
	}, fallback, nil
}
