package translate

import (
	"fmt"

	"github.com/specterops/dawgs/cypher/models"
	"github.com/specterops/dawgs/cypher/models/pgsql"
	"github.com/specterops/dawgs/cypher/models/pgsql/optimize"
	"github.com/specterops/dawgs/cypher/models/pgsql/pgd"
)

const (
	// fixedSuffixBoundaryID names the column containing the node where reverse search enters the fixed suffix.
	fixedSuffixBoundaryID pgsql.Identifier = "boundary_id"
)

// suffixSeededIdentifiers names the root-presence, suffix, boundary, and reverse-search CTEs for one rewrite.
type suffixSeededIdentifiers struct {
	// rootPresence names the relation that records whether the bound root produced rows.
	rootPresence pgsql.Identifier
	// suffix names the materialized matches for the fixed terminal suffix.
	suffix pgsql.Identifier
	// boundaries names the distinct suffix-boundary nodes used to seed reverse search.
	boundaries pgsql.Identifier
	// reverse names the recursive relation that searches from each boundary toward the root.
	reverse pgsql.Identifier
}

// newSuffixSeededIdentifiers derives collision-resistant CTE names from the incumbent final frame.
func newSuffixSeededIdentifiers(finalFrame pgsql.Identifier) suffixSeededIdentifiers {
	prefix := string(finalFrame) + "_suffix_seeded_"
	return suffixSeededIdentifiers{
		rootPresence: pgsql.Identifier(prefix + "root_presence"),
		suffix:       pgsql.Identifier(prefix + "suffix"),
		boundaries:   pgsql.Identifier(prefix + "boundaries"),
		reverse:      pgsql.Identifier(prefix + "reverse"),
	}
}

// selectedFixedSuffixDecision returns the first traversal decision that selected suffix-seeded reverse search.
func selectedFixedSuffixDecision(part *PatternPart, decisions map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategyDecision) (optimize.ExpansionSearchStrategyDecision, bool) {
	for _, step := range part.TraversalSteps {
		if step == nil || !step.HasSourceTarget {
			continue
		}
		if decision, found := decisions[step.SourceTarget]; found && decision.SelectedStrategy == optimize.ExpansionSearchSuffixSeededReverse {
			return decision, true
		}
	}

	return optimize.ExpansionSearchStrategyDecision{}, false
}

// selectedGuardedFixedSuffixDecision returns a tool-enabled suffix policy
// without treating its runtime decision as a compile-time selected arm. The
// decision's selection mode distinguishes guarded execution from true shadow.
func selectedGuardedFixedSuffixDecision(part *PatternPart, decisions map[optimize.TraversalStepTarget]optimize.ExpansionSearchStrategyDecision) (optimize.ExpansionSearchStrategyDecision, bool) {
	for _, step := range part.TraversalSteps {
		if step == nil || !step.HasSourceTarget {
			continue
		}
		if decision, found := decisions[step.SourceTarget]; found &&
			decision.Family == "fixed_suffix_expansion" &&
			decision.CandidateStrategy == optimize.ExpansionSearchSuffixSeededReverse &&
			decision.EmittedPolicy == optimize.ExpansionSearchPolicyOrientationProbeV1 {
			return decision, true
		}
	}

	return optimize.ExpansionSearchStrategyDecision{}, false
}

// rewriteTraversalPatternAsSuffixSeededReverse replaces a qualified incumbent frame chain with fixed-suffix reverse search.
func (s *Translator) rewriteTraversalPatternAsSuffixSeededReverse(part *PatternPart, decision optimize.ExpansionSearchStrategyDecision, firstCTE int) error {
	if len(part.TraversalSteps) != decision.SuffixEndStep+1 || decision.SuffixLength != 3 || decision.Target.StepIndex != 0 {
		return fmt.Errorf("forced suffix-seeded reverse target requires one expansion followed by exactly three terminal suffix steps")
	}

	expansionStep := part.TraversalSteps[decision.Target.StepIndex]
	if expansionStep == nil || expansionStep.Expansion == nil || expansionStep.Frame == nil || expansionStep.Frame.Previous == nil || !expansionStep.LeftNodeBound {
		return fmt.Errorf("forced suffix-seeded reverse target requires a bound root materialized by a previous frame")
	}

	suffix := part.TraversalSteps[decision.SuffixStartStep : decision.SuffixEndStep+1]
	for _, step := range suffix {
		if step == nil || step.Frame == nil || step.Edge == nil || step.LeftNode == nil || step.RightNode == nil {
			return fmt.Errorf("forced suffix-seeded reverse target has an incomplete fixed suffix step")
		}
	}

	ctes := s.query.CurrentPart().Model.CommonTableExpressions.Expressions
	if firstCTE < 0 || firstCTE >= len(ctes) {
		return fmt.Errorf("forced suffix-seeded reverse target did not emit an incumbent frame chain")
	}
	incumbentFinal := ctes[len(ctes)-1]
	if incumbentFinal.Alias.Name != suffix[len(suffix)-1].Frame.Binding.Identifier {
		return fmt.Errorf("forced suffix-seeded reverse final frame mismatch: expected %s but found %s", suffix[len(suffix)-1].Frame.Binding.Identifier, incumbentFinal.Alias.Name)
	}

	finalSelect, ok := incumbentFinal.Query.Body.(pgsql.Select)
	if !ok {
		return fmt.Errorf("forced suffix-seeded reverse final frame must be a select")
	}

	ids := newSuffixSeededIdentifiers(incumbentFinal.Alias.Name)
	rootFrame := expansionStep.Frame.Previous.Binding.Identifier
	suffixSeededQuery, err := s.buildSuffixSeededReverseQuery(part, decision, expansionStep, suffix, rootFrame, ids, finalSelect.Projection)
	if err != nil {
		return err
	}

	replacement := pgsql.CommonTableExpression{
		Alias: incumbentFinal.Alias,
		Query: suffixSeededQuery,
	}
	s.query.CurrentPart().Model.CommonTableExpressions.Expressions = append(ctes[:firstCTE], replacement)
	s.recordExpansionSearchStrategy(decision.Target, optimize.ExpansionSearchSuffixSeededReverse)
	return nil
}

// rewriteTraversalPatternAsGuardedSuffixOrientation emits the tool-only
// orientation-probe-v1 policy. Guarded mode wraps the incumbent and reverse
// arm in disjoint runtime gates; shadow mode executes the same bounded probes
// but leaves the incumbent as the only traversal arm.
func (s *Translator) rewriteTraversalPatternAsGuardedSuffixOrientation(part *PatternPart, decision optimize.ExpansionSearchStrategyDecision, firstCTE int) error {
	if len(part.TraversalSteps) != decision.SuffixEndStep+1 || decision.SuffixLength != 3 || decision.Target.StepIndex != 0 {
		return fmt.Errorf("guarded suffix orientation requires one expansion followed by exactly three terminal suffix steps")
	}

	expansionStep := part.TraversalSteps[decision.Target.StepIndex]
	if expansionStep == nil || expansionStep.Expansion == nil || expansionStep.Frame == nil || expansionStep.Frame.Previous == nil || !expansionStep.LeftNodeBound || expansionStep.Edge == nil || expansionStep.LeftNode == nil {
		return fmt.Errorf("guarded suffix orientation requires a complete expansion and bound root")
	}

	suffix := part.TraversalSteps[decision.SuffixStartStep : decision.SuffixEndStep+1]
	for _, step := range suffix {
		if step == nil || step.Frame == nil || step.Edge == nil || step.LeftNode == nil || step.RightNode == nil {
			return fmt.Errorf("guarded suffix orientation has an incomplete fixed suffix step")
		}
	}

	ctes := s.query.CurrentPart().Model.CommonTableExpressions.Expressions
	if firstCTE < 0 || firstCTE >= len(ctes) {
		return fmt.Errorf("guarded suffix orientation did not emit an incumbent frame chain")
	}
	incumbentChain := append([]pgsql.CommonTableExpression(nil), ctes[firstCTE:]...)
	incumbentFinal := incumbentChain[len(incumbentChain)-1]
	if incumbentFinal.Alias.Name != suffix[len(suffix)-1].Frame.Binding.Identifier {
		return fmt.Errorf("guarded suffix orientation final frame mismatch: expected %s but found %s", suffix[len(suffix)-1].Frame.Binding.Identifier, incumbentFinal.Alias.Name)
	}
	incumbentSelect, ok := incumbentFinal.Query.Body.(pgsql.Select)
	if !ok {
		return fmt.Errorf("guarded suffix orientation final frame must be a select")
	}

	ids := newExpansionOrientationIdentifiers(incumbentFinal.Alias.Name)
	rootFrame := expansionStep.Frame.Previous.Binding.Identifier
	var (
		query pgsql.Query
		err   error
	)
	if decision.SelectionMode == "shadow_tool" {
		query, err = s.buildShadowSuffixOrientationQuery(
			decision,
			expansionStep,
			suffix,
			rootFrame,
			ids,
			incumbentChain,
			incumbentFinal.Alias.Name,
			incumbentSelect.Projection,
		)
	} else {
		query, err = s.buildGuardedSuffixOrientationQuery(
			part,
			decision,
			expansionStep,
			suffix,
			rootFrame,
			ids,
			incumbentChain,
			incumbentFinal.Alias.Name,
			incumbentSelect.Projection,
		)
	}
	if err != nil {
		return err
	}

	s.query.CurrentPart().Model.CommonTableExpressions.Expressions = append(ctes[:firstCTE], pgsql.CommonTableExpression{
		Alias: incumbentFinal.Alias,
		Query: query,
	})
	s.recordExpansionSearchPolicy(decision.Target, optimize.ExpansionSearchPolicyOrientationProbeV1)
	return nil
}

// buildShadowSuffixOrientationQuery executes only bounded policy probes and
// the exact incumbent. Named, mutually exclusive marker CTEs preserve the
// policy's would_select_reverse result for plan-derived diagnostic metadata;
// they never dispatch the reverse traversal candidate.
func (s *Translator) buildShadowSuffixOrientationQuery(
	decision optimize.ExpansionSearchStrategyDecision,
	expansionStep *TraversalStep,
	suffix []*TraversalStep,
	rootFrame pgsql.Identifier,
	ids expansionOrientationIdentifiers,
	incumbentChain []pgsql.CommonTableExpression,
	incumbentFinal pgsql.Identifier,
	incumbentProjection pgsql.Projection,
) (pgsql.Query, error) {
	if decision.ProbeCaps.RootRowLimit <= 0 || decision.ProbeCaps.ReverseSeedRowLimit <= 0 || decision.ProbeCaps.DirectionalDegreeRowLimit <= 0 {
		return pgsql.Query{}, fmt.Errorf("shadow suffix orientation requires positive immutable probe caps")
	}

	localEdgeConstraint, externalEdgeConstraint := partitionConstraintByLocality(
		expansionStep.Expansion.EdgeConstraints,
		pgsql.AsIdentifierSet(expansionStep.Edge.Identifier),
	)
	if externalEdgeConstraint != nil {
		return pgsql.Query{}, fmt.Errorf("shadow suffix orientation relationship predicate is not local")
	}

	suffixIDs := suffixSeededIdentifiers{
		rootPresence: ids.rootPresence,
		suffix:       ids.suffixProbe,
		boundaries:   ids.boundaries,
	}
	rootProbe := buildExpansionOrientationRootProbe(rootFrame, expansionStep.LeftNode, ids, decision.ProbeCaps.RootRowLimit)
	rootPresence := buildExpansionOrientationRootPresence(ids)
	suffixProbe, err := s.buildFixedSuffixEvidenceProbeCTE(expansionStep, suffix, suffixIDs, decision.ProbeCaps.ReverseSeedRowLimit)
	if err != nil {
		return pgsql.Query{}, err
	}
	boundaries := buildFixedSuffixBoundariesCTE(suffixIDs)
	forwardDegree := buildExpansionOrientationDegreeProbe(
		ids.forwardDegreeProbe,
		ids.rootProbe,
		orientationRootID,
		expansionStep.Edge.Identifier,
		expansionStep.Expansion.EdgeStartIdentifier,
		localEdgeConstraint,
		decision.ProbeCaps.DirectionalDegreeRowLimit,
	)
	reverseDegree := buildExpansionOrientationDegreeProbe(
		ids.reverseDegreeProbe,
		ids.boundaries,
		fixedSuffixBoundaryID,
		expansionStep.Edge.Identifier,
		expansionStep.Expansion.EdgeEndIdentifier,
		localEdgeConstraint,
		decision.ProbeCaps.DirectionalDegreeRowLimit,
	)
	metrics := buildExpansionOrientationMetrics(ids, decision.ProbeCaps)
	policyDecision := buildExpansionOrientationDecision(ids)
	shadowMarkers := buildExpansionOrientationShadowMarkers(ids)
	incumbent, incumbentOutput, err := buildExpansionOrientationIncumbentCTE(ids, incumbentChain, incumbentFinal, incumbentProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	gatedIncumbent, err := gateQueryBehindMarker(
		ids.executedIncumbent,
		ids.incumbentBody,
		pgsql.Query{Body: pgsql.Select{
			Projection: incumbentOutput,
			From:       []pgsql.FromClause{tableFrom(ids.incumbent)},
		}},
		incumbentOutput,
	)
	if err != nil {
		return pgsql.Query{}, err
	}

	expressions := []pgsql.CommonTableExpression{
		rootProbe,
		rootPresence,
		suffixProbe,
		boundaries,
		forwardDegree,
		reverseDegree,
		metrics,
		policyDecision,
	}
	expressions = append(expressions, shadowMarkers...)
	expressions = append(expressions, incumbent)

	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive:   true,
			Expressions: expressions,
		},
		Body: gatedIncumbent,
	}, nil
}

// buildGuardedSuffixOrientationQuery emits bounded evidence, a versioned
// decision, reverse-state admission, and strictly complementary candidate and
// incumbent branches. No candidate row can pass until every evidence and
// state sentinel proves completeness.
func (s *Translator) buildGuardedSuffixOrientationQuery(
	part *PatternPart,
	decision optimize.ExpansionSearchStrategyDecision,
	expansionStep *TraversalStep,
	suffix []*TraversalStep,
	rootFrame pgsql.Identifier,
	ids expansionOrientationIdentifiers,
	incumbentChain []pgsql.CommonTableExpression,
	incumbentFinal pgsql.Identifier,
	incumbentProjection pgsql.Projection,
) (pgsql.Query, error) {
	if decision.ProbeCaps.RootRowLimit <= 0 || decision.ProbeCaps.ReverseSeedRowLimit <= 0 || decision.ProbeCaps.DirectionalDegreeRowLimit <= 0 || decision.Admission.StateLimit <= 0 {
		return pgsql.Query{}, fmt.Errorf("guarded suffix orientation requires positive immutable probe and admission caps")
	}

	localEdgeConstraint, externalEdgeConstraint := partitionConstraintByLocality(
		expansionStep.Expansion.EdgeConstraints,
		pgsql.AsIdentifierSet(expansionStep.Edge.Identifier),
	)
	if externalEdgeConstraint != nil {
		return pgsql.Query{}, fmt.Errorf("guarded suffix orientation relationship predicate is not local")
	}

	suffixIDs := suffixSeededIdentifiers{
		rootPresence: ids.rootPresence,
		suffix:       ids.suffixProbe,
		boundaries:   ids.boundaries,
		reverse:      ids.reverse,
	}
	rootProbe := buildExpansionOrientationRootProbe(rootFrame, expansionStep.LeftNode, ids, decision.ProbeCaps.RootRowLimit)
	rootPresence := buildExpansionOrientationRootPresence(ids)
	suffixProbe, err := s.buildFixedSuffixProbeCTE(expansionStep, suffix, suffixIDs, decision.ProbeCaps.ReverseSeedRowLimit)
	if err != nil {
		return pgsql.Query{}, err
	}
	boundaries := buildFixedSuffixBoundariesCTE(suffixIDs)
	forwardDegree := buildExpansionOrientationDegreeProbe(
		ids.forwardDegreeProbe,
		ids.rootProbe,
		orientationRootID,
		expansionStep.Edge.Identifier,
		expansionStep.Expansion.EdgeStartIdentifier,
		localEdgeConstraint,
		decision.ProbeCaps.DirectionalDegreeRowLimit,
	)
	reverseDegree := buildExpansionOrientationDegreeProbe(
		ids.reverseDegreeProbe,
		ids.boundaries,
		fixedSuffixBoundaryID,
		expansionStep.Edge.Identifier,
		expansionStep.Expansion.EdgeEndIdentifier,
		localEdgeConstraint,
		decision.ProbeCaps.DirectionalDegreeRowLimit,
	)
	metrics := buildExpansionOrientationMetrics(ids, decision.ProbeCaps)
	policyDecision := buildExpansionOrientationDecision(ids)
	reverseSeed := buildExpansionOrientationReverseSeed(ids)
	reverseIDs := suffixIDs
	reverseIDs.boundaries = ids.reverseSeed
	reverse, err := buildSuffixSeededReverseCTE(expansionStep, decision, reverseIDs, "", "")
	if err != nil {
		return pgsql.Query{}, err
	}
	states := expansionOrientationStateProbe(decision, ids)
	admission := buildExpansionOrientationAdmission(ids, decision.Admission.StateLimit)
	executionMarkers := buildExpansionOrientationExecutionMarkers(ids)
	incumbent, fallbackProjection, err := buildExpansionOrientationIncumbentCTE(ids, incumbentChain, incumbentFinal, incumbentProjection)
	if err != nil {
		return pgsql.Query{}, err
	}
	candidateProjection, err := suffixSeededFinalProjection(part, expansionStep, suffix, rootFrame, suffixIDs, ids.states, incumbentProjection, nil)
	if err != nil {
		return pgsql.Query{}, err
	}

	suffixEdgeIDs := pgsql.ArrayLiteral{CastType: pgsql.Int8Array}
	for _, step := range suffix {
		suffixEdgeIDs.Values = append(suffixEdgeIDs.Values, pgsql.CompoundIdentifier{ids.suffixProbe, step.Edge.Identifier})
	}
	var candidateWhere pgsql.Expression = pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{ids.states, expansionDepth},
		pgsql.OperatorGreaterThanOrEqualTo,
		pgsql.NewLiteral(decision.MinimumDepth, pgsql.Int8),
	)
	candidateWhere = pgsql.OptionalAnd(candidateWhere, pgd.Not(pgsql.NewBinaryExpression(
		pgsql.CompoundIdentifier{ids.states, expansionPath},
		pgsql.OperatorArrayOverlap,
		suffixEdgeIDs,
	)))

	candidate := pgsql.Select{
		Projection: candidateProjection,
		From: []pgsql.FromClause{
			{
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
			},
		},
		Where: candidateWhere,
	}
	candidate, err = gateQueryBehindMarker(
		ids.executedCandidate,
		ids.candidateBody,
		pgsql.Query{Body: candidate},
		candidateProjection,
	)
	if err != nil {
		return pgsql.Query{}, err
	}

	fallback := pgsql.Select{
		Projection: fallbackProjection,
		From:       []pgsql.FromClause{tableFrom(ids.incumbent)},
	}
	fallback, err = gateQueryBehindMarker(
		ids.executedIncumbent,
		ids.incumbentBody,
		pgsql.Query{Body: fallback},
		fallbackProjection,
	)
	if err != nil {
		return pgsql.Query{}, err
	}
	expressions := []pgsql.CommonTableExpression{
		rootProbe,
		rootPresence,
		suffixProbe,
		boundaries,
		forwardDegree,
		reverseDegree,
		metrics,
		policyDecision,
	}
	expressions = append(expressions, reverseSeed...)
	expressions = append(expressions, reverse, states, admission)
	expressions = append(expressions, executionMarkers...)
	expressions = append(expressions, incumbent)

	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive:   true,
			Expressions: expressions,
		},
		Body: pgsql.SetOperation{
			Operator: pgsql.OperatorUnion,
			All:      true,
			LOperand: candidate,
			ROperand: fallback,
		},
	}, nil
}

func buildFixedSuffixBoundariesCTE(ids suffixSeededIdentifiers) pgsql.CommonTableExpression {
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.boundaries},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query: pgsql.Query{Body: pgsql.Select{
			Distinct: true,
			Projection: pgsql.Projection{&pgsql.AliasedExpression{
				Expression: pgsql.CompoundIdentifier{ids.suffix, fixedSuffixBoundaryID},
				Alias:      models.OptionalValue(fixedSuffixBoundaryID),
			}},
			From: []pgsql.FromClause{tableFrom(ids.suffix)},
		}},
	}
}

// buildExpansionOrientationIncumbentCTE nests the original unmodified frame
// chain as the exact fallback. It has no tournament cap and preserves the
// incumbent's projection and bag semantics.
func buildExpansionOrientationIncumbentCTE(
	ids expansionOrientationIdentifiers,
	incumbentChain []pgsql.CommonTableExpression,
	incumbentFinal pgsql.Identifier,
	incumbentProjection pgsql.Projection,
) (pgsql.CommonTableExpression, pgsql.Projection, error) {
	projection := make(pgsql.Projection, 0, len(incumbentProjection))
	fallback := make(pgsql.Projection, 0, len(incumbentProjection))
	for _, item := range incumbentProjection {
		alias, ok := selectItemAlias(item)
		if !ok {
			return pgsql.CommonTableExpression{}, nil, fmt.Errorf("guarded suffix orientation incumbent projection contains an unaliased item %T", item)
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{incumbentFinal, alias},
			Alias:      models.OptionalValue(alias),
		})
		fallback = append(fallback, &pgsql.AliasedExpression{
			Expression: pgsql.CompoundIdentifier{ids.incumbent, alias},
			Alias:      models.OptionalValue(alias),
		})
	}

	incumbentQuery := pgsql.Query{
		CommonTableExpressions: &pgsql.With{Expressions: incumbentChain},
		Body: pgsql.Select{
			Projection: projection,
			From:       []pgsql.FromClause{tableFrom(incumbentFinal)},
		},
	}
	return pgsql.CommonTableExpression{
		Alias:        pgsql.TableAlias{Name: ids.incumbent},
		Materialized: &pgsql.Materialized{Materialized: true},
		Query:        incumbentQuery,
	}, fallback, nil
}

// buildSuffixSeededReverseQuery joins bound roots to reverse states seeded by materialized fixed-suffix matches.
func (s *Translator) buildSuffixSeededReverseQuery(
	part *PatternPart,
	decision optimize.ExpansionSearchStrategyDecision,
	expansionStep *TraversalStep,
	suffix []*TraversalStep,
	rootFrame pgsql.Identifier,
	ids suffixSeededIdentifiers,
	incumbentProjection pgsql.Projection,
) (pgsql.Query, error) {
	rootPresence := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: ids.rootPresence,
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Projection: []pgsql.SelectItem{pgsql.NewLiteral(int64(1), pgsql.Int8)},
				From:       []pgsql.FromClause{tableFrom(rootFrame)},
			},
			Limit: pgsql.NewLiteral(int64(1), pgsql.Int8),
		},
	}

	suffixCTE, err := s.buildFixedSuffixCTE(expansionStep, suffix, ids)
	if err != nil {
		return pgsql.Query{}, err
	}

	boundaries := pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: ids.boundaries,
		},
		Materialized: &pgsql.Materialized{
			Materialized: true,
		},
		Query: pgsql.Query{
			Body: pgsql.Select{
				Distinct: true,
				Projection: []pgsql.SelectItem{&pgsql.AliasedExpression{
					Expression: pgsql.CompoundIdentifier{ids.suffix, fixedSuffixBoundaryID},
					Alias:      models.OptionalValue(fixedSuffixBoundaryID),
				}},
				From: []pgsql.FromClause{tableFrom(ids.suffix)},
			},
		},
	}
	reverse, err := buildSuffixSeededReverseCTE(expansionStep, decision, ids, "", "")
	if err != nil {
		return pgsql.Query{}, err
	}

	projection, err := suffixSeededFinalProjection(part, expansionStep, suffix, rootFrame, ids, ids.reverse, incumbentProjection, nil)
	if err != nil {
		return pgsql.Query{}, err
	}

	suffixEdgeIDs := pgsql.ArrayLiteral{
		CastType: pgsql.Int8Array,
	}
	for _, step := range suffix {
		suffixEdgeIDs.Values = append(suffixEdgeIDs.Values, pgsql.CompoundIdentifier{ids.suffix, step.Edge.Identifier})
	}

	reversePath := pgsql.CompoundIdentifier{ids.reverse, expansionPath}
	finalWhere := pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{ids.reverse, expansionDepth},
			pgsql.OperatorGreaterThanOrEqualTo,
			pgsql.NewLiteral(decision.MinimumDepth, pgsql.Int8),
		),
		pgd.Not(pgsql.NewBinaryExpression(reversePath, pgsql.OperatorArrayOverlap, suffixEdgeIDs)),
	)

	return pgsql.Query{
		CommonTableExpressions: &pgsql.With{
			Recursive: true,
			Expressions: []pgsql.CommonTableExpression{
				rootPresence,
				suffixCTE,
				boundaries,
				reverse,
			},
		},
		Body: pgsql.Select{
			Projection: projection,
			From: []pgsql.FromClause{{
				Source: pgsql.TableReference{
					Name: rootFrame.AsCompoundIdentifier(),
				},
				Joins: []pgsql.Join{
					{
						Table: pgsql.TableReference{
							Name: ids.reverse.AsCompoundIdentifier(),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								projectedNodeIDReference(rootFrame, expansionStep.LeftNode),
								pgsql.OperatorEquals,
								pgsql.CompoundIdentifier{ids.reverse, expansionNextID},
							),
						},
					},
					{
						Table: pgsql.TableReference{
							Name: ids.suffix.AsCompoundIdentifier(),
						},
						JoinOperator: pgsql.JoinOperator{
							JoinType: pgsql.JoinTypeInner,
							Constraint: pgsql.NewBinaryExpression(
								pgsql.CompoundIdentifier{ids.suffix, fixedSuffixBoundaryID},
								pgsql.OperatorEquals,
								pgsql.CompoundIdentifier{ids.reverse, fixedSuffixBoundaryID},
							),
						},
					},
				},
			}},
			Where: finalWhere,
		},
	}, nil
}

// buildFixedSuffixCTE materializes every locally valid fixed-suffix path and its boundary node.
func (s *Translator) buildFixedSuffixCTE(expansionStep *TraversalStep, suffix []*TraversalStep, ids suffixSeededIdentifiers) (pgsql.CommonTableExpression, error) {
	return s.buildFixedSuffixCTEWithOptions(expansionStep, suffix, ids, false, false, 0)
}

// buildFixedSuffixProbeCTE builds a bounded suffix probe used to guard the specialized branch.
func (s *Translator) buildFixedSuffixProbeCTE(expansionStep *TraversalStep, suffix []*TraversalStep, ids suffixSeededIdentifiers, rowLimit int64) (pgsql.CommonTableExpression, error) {
	return s.buildFixedSuffixCTEWithOptions(expansionStep, suffix, ids, false, false, rowLimit)
}

// buildFixedSuffixEvidenceProbeCTE preserves the suffix join and row
// multiplicity used by orientation scoring while projecting only the boundary
// ID needed by the shadow policy. Candidate execution is impossible in shadow
// mode, so materializing edge IDs and node composites would be pure overhead.
func (s *Translator) buildFixedSuffixEvidenceProbeCTE(expansionStep *TraversalStep, suffix []*TraversalStep, ids suffixSeededIdentifiers, rowLimit int64) (pgsql.CommonTableExpression, error) {
	return s.buildFixedSuffixCTEWithOptions(expansionStep, suffix, ids, false, true, rowLimit)
}

// buildFixedSuffixCTEWithOptions builds the fixed-suffix join chain with an
// optional evidence-only projection and row limit.
func (s *Translator) buildFixedSuffixCTEWithOptions(expansionStep *TraversalStep, suffix []*TraversalStep, ids suffixSeededIdentifiers, projectNodeIDs, evidenceOnly bool, rowLimit int64) (pgsql.CommonTableExpression, error) {
	localScope := pgsql.NewIdentifierSet()
	for _, step := range suffix {
		localScope.Add(step.Edge.Identifier)
		localScope.Add(step.LeftNode.Identifier)
		localScope.Add(step.RightNode.Identifier)
	}

	projection := pgsql.Projection{&pgsql.AliasedExpression{
		Expression: pgd.EntityID(suffix[0].LeftNode.Identifier),
		Alias:      models.OptionalValue(fixedSuffixBoundaryID),
	}}
	if !evidenceOnly {
		for _, step := range suffix {
			projection = append(projection, &pgsql.AliasedExpression{
				Expression: pgd.EntityID(step.Edge.Identifier),
				Alias:      models.OptionalValue(step.Edge.Identifier),
			})
		}
		for idx, step := range suffix {
			binding := step.RightNode
			expression := suffixSeededNodeValue(binding)
			if projectNodeIDs {
				expression = pgd.EntityID(binding.Identifier)
			}
			projection = append(projection, &pgsql.AliasedExpression{
				Expression: expression,
				Alias:      models.OptionalValue(binding.Identifier),
			})
			if idx == 0 {
				leftExpression := suffixSeededNodeValue(step.LeftNode)
				if projectNodeIDs {
					leftExpression = pgd.EntityID(step.LeftNode.Identifier)
				}
				projection = append(projection, &pgsql.AliasedExpression{
					Expression: leftExpression,
					Alias:      models.OptionalValue(step.LeftNode.Identifier),
				})
			}
		}
	}

	first := suffix[0]
	from := pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: ids.rootPresence.AsCompoundIdentifier(),
		},
		Joins: []pgsql.Join{
			{
				Table: expansionEdgeTableReference(first.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType:   pgsql.JoinTypeInner,
					Constraint: pgsql.NewLiteral(true, pgsql.Boolean),
				},
			},
			{
				Table: expansionNodeTableReference(first.LeftNode.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						pgd.EntityID(first.LeftNode.Identifier), pgsql.OperatorEquals, pgsql.CompoundIdentifier{first.Edge.Identifier, pgsql.ColumnStartID},
					),
				},
			},
			{
				Table: expansionNodeTableReference(first.RightNode.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						pgd.EntityID(first.RightNode.Identifier), pgsql.OperatorEquals, pgsql.CompoundIdentifier{first.Edge.Identifier, pgsql.ColumnEndID},
					),
				},
			},
		},
	}
	for _, step := range suffix[1:] {
		from.Joins = append(from.Joins,
			pgsql.Join{
				Table: expansionEdgeTableReference(step.Edge.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						pgsql.CompoundIdentifier{step.Edge.Identifier, pgsql.ColumnStartID}, pgsql.OperatorEquals, pgd.EntityID(step.LeftNode.Identifier),
					),
				},
			},
			pgsql.Join{
				Table: expansionNodeTableReference(step.RightNode.Identifier),
				JoinOperator: pgsql.JoinOperator{
					JoinType: pgsql.JoinTypeInner,
					Constraint: pgsql.NewBinaryExpression(
						pgd.EntityID(step.RightNode.Identifier), pgsql.OperatorEquals, pgsql.CompoundIdentifier{step.Edge.Identifier, pgsql.ColumnEndID},
					),
				},
			},
		)
	}

	var boundaryConstraint pgsql.Expression
	if expansionStep.Expansion != nil {
		boundaryConstraint = expansionStep.Expansion.TerminalNodeConstraints
	}
	localBoundaryConstraint, _ := partitionConstraintByLocality(boundaryConstraint, localScope)
	where := localBoundaryConstraint
	suffixRelationships := make([]pgsql.Identifier, 0, len(suffix))
	for _, step := range suffix {
		suffixRelationships = append(suffixRelationships, step.Edge.Identifier)
		localLeftConstraint, _ := partitionConstraintByLocality(step.LeftNodeConstraints, localScope)
		localEdgeConstraint, _ := partitionConstraintByLocality(step.EdgeConstraints.Expression, localScope)
		localRightConstraint, _ := partitionConstraintByLocality(step.RightNodeConstraints, localScope)
		where = pgsql.OptionalAnd(where, localLeftConstraint)
		where = pgsql.OptionalAnd(where, localEdgeConstraint)
		where = pgsql.OptionalAnd(where, localRightConstraint)
	}
	where = pgsql.OptionalAnd(where, pairwiseRelationshipIDUniqueness(suffixRelationships))

	query := pgsql.Query{
		Body: pgsql.Select{
			Projection: projection,
			From:       []pgsql.FromClause{from},
			Where:      where,
		},
	}
	if rowLimit > 0 {
		query.Limit = pgsql.NewLiteral(rowLimit+1, pgsql.Int8)
	}

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: ids.suffix,
		},
		Materialized: &pgsql.Materialized{
			Materialized: true,
		},
		Query: query,
	}, nil
}

// buildSuffixSeededReverseCTE recursively walks from suffix boundaries back toward bound roots without reusing edges.
func buildSuffixSeededReverseCTE(expansionStep *TraversalStep, decision optimize.ExpansionSearchStrategyDecision, ids suffixSeededIdentifiers, gateSource, gateColumn pgsql.Identifier) (pgsql.CommonTableExpression, error) {
	if expansionStep.Edge == nil || expansionStep.RightNode == nil {
		return pgsql.CommonTableExpression{}, fmt.Errorf("forced suffix-seeded reverse expansion step is incomplete")
	}

	emptyPath := pgsql.ArrayLiteral{
		CastType: pgsql.Int8Array,
	}
	seed := pgsql.Select{
		Projection: []pgsql.SelectItem{
			pgsql.CompoundIdentifier{ids.boundaries, fixedSuffixBoundaryID},
			pgsql.CompoundIdentifier{ids.boundaries, fixedSuffixBoundaryID},
			pgsql.NewLiteral(int64(0), pgsql.Int8),
			emptyPath,
		},
		From: []pgsql.FromClause{tableFrom(ids.boundaries)},
	}
	if gateSource != "" && gateColumn != "" {
		seed.From = append(seed.From, tableFrom(gateSource))
		seed.Where = pgsql.CompoundIdentifier{gateSource, gateColumn}
	}

	path := pgsql.CompoundIdentifier{ids.reverse, expansionPath}
	localEdgeConstraint, _ := partitionConstraintByLocality(
		expansionStep.Expansion.EdgeConstraints,
		pgsql.AsIdentifierSet(expansionStep.Edge.Identifier),
	)
	recursiveWhere := pgsql.OptionalAnd(
		pgsql.NewBinaryExpression(
			pgsql.CompoundIdentifier{ids.reverse, expansionDepth},
			pgsql.OperatorLessThan,
			pgsql.NewLiteral(decision.MaximumDepth, pgsql.Int8),
		),
		pgsql.NewBinaryExpression(
			pgd.EntityID(expansionStep.Edge.Identifier),
			pgsql.OperatorNotEquals,
			pgsql.NewAllExpression(path),
		),
	)
	recursiveWhere = pgsql.OptionalAnd(recursiveWhere, localEdgeConstraint)

	recursive := pgsql.Select{
		Projection: []pgsql.SelectItem{
			pgsql.CompoundIdentifier{ids.reverse, fixedSuffixBoundaryID},
			pgsql.CompoundIdentifier{expansionStep.Edge.Identifier, pgsql.ColumnStartID},
			pgsql.NewBinaryExpression(pgsql.CompoundIdentifier{ids.reverse, expansionDepth}, pgsql.OperatorAdd, pgsql.NewLiteral(int64(1), pgsql.Int8)),
			pgsql.FunctionCall{
				Function: pgsql.Identifier("array_prepend"),
				Parameters: []pgsql.Expression{
					pgd.EntityID(expansionStep.Edge.Identifier), path,
				},
				CastType: pgsql.Int8Array,
			},
		},
		From: []pgsql.FromClause{{
			Source: pgsql.TableReference{
				Name: ids.reverse.AsCompoundIdentifier(),
			},
			Joins: []pgsql.Join{
				{
					Table: expansionEdgeTableReference(expansionStep.Edge.Identifier),
					JoinOperator: pgsql.JoinOperator{
						JoinType: pgsql.JoinTypeInner,
						Constraint: pgsql.NewBinaryExpression(
							pgsql.CompoundIdentifier{expansionStep.Edge.Identifier, pgsql.ColumnEndID}, pgsql.OperatorEquals, pgsql.CompoundIdentifier{ids.reverse, expansionNextID},
						),
					},
				},
				{
					Table: expansionNodeTableReference(expansionStep.LeftNode.Identifier),
					JoinOperator: pgsql.JoinOperator{
						JoinType: pgsql.JoinTypeInner,
						Constraint: pgsql.NewBinaryExpression(
							pgd.EntityID(expansionStep.LeftNode.Identifier), pgsql.OperatorEquals, pgsql.CompoundIdentifier{expansionStep.Edge.Identifier, pgsql.ColumnStartID},
						),
					},
				},
			},
		}},
		Where: recursiveWhere,
	}

	return pgsql.CommonTableExpression{
		Alias: pgsql.TableAlias{
			Name: ids.reverse,
			Shape: pgsql.NewRecordShape([]pgsql.Identifier{
				fixedSuffixBoundaryID, expansionNextID, expansionDepth, expansionPath,
			}),
		},
		Query: pgsql.Query{
			Body: pgsql.SetOperation{
				Operator: pgsql.OperatorUnion,
				All:      true,
				LOperand: seed,
				ROperand: recursive,
			},
		},
	}, nil
}

// suffixSeededFinalProjection reconstructs the incumbent projection from root, reverse-state, and suffix columns.
func suffixSeededFinalProjection(
	part *PatternPart,
	expansionStep *TraversalStep,
	suffix []*TraversalStep,
	rootFrame pgsql.Identifier,
	ids suffixSeededIdentifiers,
	reverseStateSource pgsql.Identifier,
	incumbent pgsql.Projection,
	suffixOverrides map[pgsql.Identifier]pgsql.Expression,
) (pgsql.Projection, error) {
	suffixBindings := map[pgsql.Identifier]struct{}{}
	for _, step := range suffix {
		suffixBindings[step.Edge.Identifier] = struct{}{}
		suffixBindings[step.LeftNode.Identifier] = struct{}{}
		suffixBindings[step.RightNode.Identifier] = struct{}{}
	}

	projection := make(pgsql.Projection, 0, len(incumbent))
	for _, item := range incumbent {
		alias, ok := selectItemAlias(item)
		if !ok {
			return nil, fmt.Errorf("forced suffix-seeded reverse final projection contains an unaliased item %T", item)
		}

		var expression pgsql.Expression
		switch {
		case expansionStep.Expansion != nil && expansionStep.Expansion.PathBinding != nil && alias == expansionStep.Expansion.PathBinding.Identifier:
			expression = pgsql.CompoundIdentifier{reverseStateSource, expansionPath}
		case alias == expansionStep.LeftNode.Identifier:
			expression = pgsql.CompoundIdentifier{rootFrame, alias}
		case suffixOverrides[alias] != nil:
			expression = suffixOverrides[alias]
		default:
			if _, found := suffixBindings[alias]; found {
				expression = pgsql.CompoundIdentifier{ids.suffix, alias}
			} else {
				expression = pgsql.CompoundIdentifier{rootFrame, alias}
			}
		}
		projection = append(projection, &pgsql.AliasedExpression{
			Expression: expression,
			Alias:      models.OptionalValue(alias),
		})
	}

	return projection, nil
}

// selectItemAlias returns an explicit alias or the identifier naturally exposed by a select item.
func selectItemAlias(item pgsql.SelectItem) (pgsql.Identifier, bool) {
	switch typed := item.(type) {
	case *pgsql.AliasedExpression:
		return typed.Alias.Value, typed.Alias.Set
	case pgsql.AliasedExpression:
		return typed.Alias.Value, typed.Alias.Set
	default:
		return "", false
	}
}

// suffixSeededNodeValue returns a node's scalar ID or composite value according to its projection representation.
func suffixSeededNodeValue(binding *BoundIdentifier) pgsql.Expression {
	if binding.IDOnly {
		return pgd.EntityID(binding.Identifier)
	}
	return aggregateNodeComposite(binding.Identifier)
}

// tableFrom wraps a relation name as a single PostgreSQL FROM clause.
func tableFrom(identifier pgsql.Identifier) pgsql.FromClause {
	return pgsql.FromClause{
		Source: pgsql.TableReference{
			Name: identifier.AsCompoundIdentifier(),
		},
	}
}
